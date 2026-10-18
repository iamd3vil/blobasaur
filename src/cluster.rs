use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex, Once};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chitchat::{
    Chitchat, ChitchatConfig, ChitchatHandle, ChitchatId, FailureDetectorConfig, spawn_chitchat,
};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tokio::time::interval;
use tracing::{debug, error, info, warn};

/// Strategy for distributing namespace operations across cluster nodes
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum NamespaceDistributionStrategy {
    /// Use namespace for slot calculation (legacy behavior - confines namespace to one node)
    #[allow(dead_code)]
    NamespaceBased,
    /// Use key for slot calculation (distributes namespace across nodes)
    #[default]
    KeyBased,
    /// Use hash tags when present, fallback to key-based
    HashTagAware,
}

use crate::config::ClusterConfig;

/// Redis cluster has 16384 hash slots
pub const REDIS_CLUSTER_SLOTS: u16 = 16384;

/// Represents the state of a cluster node
#[derive(Debug, Clone)]
pub struct ClusterNode {
    pub id: String,
    pub addr: SocketAddr,
    pub slots: HashSet<u16>,
    pub _state: String,
    pub role: NodeRole,
    /// The node id this node replicates, for a replica.
    pub primary: Option<String>,
}

/// What a node gossips about its place in replication.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum NodeRole {
    #[default]
    Primary,
    /// A replica with a shard that isn't a full copy yet; it serves no reads.
    ReplicaSyncing,
    Replica,
}

/// Where a request for a slot should run.
#[derive(Debug, PartialEq, Eq)]
pub enum Route {
    /// This node owns the slot.
    Local,
    /// A live peer owns the slot; holds the full `MOVED <slot> <ip>:<port>` reply.
    Moved(String),
    /// No live node owns the slot.
    ClusterDown,
}

/// A contiguous slot range and the node serving it, as reported by CLUSTER SLOTS.
#[derive(Debug, PartialEq, Eq)]
pub struct SlotRange {
    pub start: u16,
    pub end: u16,
    pub node_id: String,
    pub ip: String,
    pub port: u16,
    /// Live replicas in `replica` role as `(ip, port, node_id)`, by node id.
    pub replicas: Vec<(String, u16, String)>,
}

/// The Redis ip and port this node advertises to clients and peers.
fn local_endpoint(config: &ClusterConfig, redis_addr: SocketAddr) -> (String, u16) {
    if let Some(ref advertise_addr) = config.advertise_addr {
        match advertise_addr.rsplit_once(':') {
            Some((ip, port)) => (ip.to_string(), port.parse().unwrap_or(redis_addr.port())),
            None => (advertise_addr.clone(), redis_addr.port()),
        }
    } else if redis_addr.ip().is_unspecified() {
        ("127.0.0.1".to_string(), redis_addr.port())
    } else {
        (redis_addr.ip().to_string(), redis_addr.port())
    }
}

/// Keeps only the newest generation of each node id.
fn newest_generations<'a>(ids: impl Iterator<Item = &'a ChitchatId>) -> Vec<&'a ChitchatId> {
    let mut newest: HashMap<&str, &ChitchatId> = HashMap::new();
    for id in ids {
        let entry = newest.entry(&id.node_id).or_insert(id);
        if id.generation_id > entry.generation_id {
            *entry = id;
        }
    }
    newest.into_values().collect()
}

/// The other live nodes and what they gossiped. Only live nodes own slots, and
/// only their newest generation counts.
fn live_peers(chitchat: &Chitchat, self_id: &str) -> HashMap<String, ClusterNode> {
    let mut nodes_map = HashMap::new();
    for chitchat_id in newest_generations(chitchat.live_nodes()) {
        if chitchat_id.node_id == self_id {
            continue; // Skip self
        }
        let Some(node_state) = chitchat.node_state(chitchat_id) else {
            continue;
        };

        debug!("Processing node: {}", chitchat_id.node_id);

        if let Some(node_info_value) = node_state.get("node_info") {
            debug!("Node {} has gossip data", chitchat_id.node_id);
            match serde_json::from_str::<NodeGossipData>(node_info_value) {
                Ok(gossip_data) => {
                    if let Ok(addr) = gossip_data.addr.parse::<SocketAddr>() {
                        let cluster_node = ClusterNode {
                            id: chitchat_id.node_id.clone(),
                            addr,
                            slots: gossip_data.owned_slots(),
                            _state: gossip_data.state,
                            role: gossip_data.role,
                            primary: gossip_data.primary,
                        };
                        nodes_map.insert(chitchat_id.node_id.clone(), cluster_node);
                    } else {
                        warn!(
                            "Invalid address format from {}: {}",
                            chitchat_id.node_id, gossip_data.addr
                        );
                    }
                }
                Err(e) => {
                    warn!(
                        "Failed to deserialize gossip data from {}: {} (data: {})",
                        chitchat_id.node_id, e, node_info_value
                    );
                }
            }
        } else {
            debug!("No node_info found for node: {}", chitchat_id.node_id);
        }
    }
    nodes_map
}

/// The largest `node_info` value we gossip with the legacy `slots` list. chitchat
/// drops a value that doesn't fit one 65507-byte datagram, shared with the digest
/// (about 60 bytes per node) and the node record, and asserts one item is at most
/// `u16::MAX` bytes. This leaves about 9.5 KB for those. 9000 slots take under 55 KB.
const GOSSIP_VALUE_MAX: usize = 56_000;

/// Gossip data structure for sharing node information
#[derive(Debug, Clone, Serialize, Deserialize)]
struct NodeGossipData {
    pub addr: String,
    /// Every owned slot. Only for old nodes, which read nothing else; empty when
    /// only the ranges fit `GOSSIP_VALUE_MAX`. Can be dropped in a later release.
    pub slots: Vec<u16>,
    /// Owned slots as inclusive `(start, end)` runs. Read in preference to `slots`;
    /// empty when the list alone fits but not both.
    #[serde(default)]
    pub slot_ranges: Vec<(u16, u16)>,
    pub state: String,
    #[serde(default)]
    pub role: NodeRole,
    #[serde(default)]
    pub primary: Option<String>,
}

impl NodeGossipData {
    /// Encodes `slots` as both a list and ranges if that fits `GOSSIP_VALUE_MAX`,
    /// else as whichever one alone fits, preferring the list old nodes can read.
    /// Errors if neither fits: peers would never receive the value.
    fn new(
        addr: String,
        slots: &HashSet<u16>,
        role: NodeRole,
        primary: Option<String>,
    ) -> Result<Self, String> {
        let mut list: Vec<u16> = slots.iter().copied().collect();
        list.sort_unstable();
        let both = Self {
            addr,
            slots: list,
            slot_ranges: ClusterManager::slot_runs(slots),
            state: "master".to_string(),
            role,
            primary,
        };
        let size = |data: &Self| serde_json::to_string(data).map_or(usize::MAX, |json| json.len());
        if size(&both) <= GOSSIP_VALUE_MAX {
            return Ok(both);
        }
        // Scattered slots: the ranges can be longer than the list.
        let list_only = Self {
            slot_ranges: Vec::new(),
            ..both.clone()
        };
        if size(&list_only) <= GOSSIP_VALUE_MAX {
            return Ok(list_only);
        }
        let ranges_only = Self {
            slots: Vec::new(),
            ..both
        };
        let ranges_size = size(&ranges_only);
        if ranges_size > GOSSIP_VALUE_MAX {
            return Err(format!(
                "this node's {} slots are too scattered to gossip ({} bytes as ranges, limit {})",
                slots.len(),
                ranges_size,
                GOSSIP_VALUE_MAX
            ));
        }
        static WARNED: Once = Once::new();
        WARNED.call_once(|| {
            warn!(
                "This node's slots are too many to gossip as a list: peers older than this version can't see them. Upgrade every peer before giving a node this many slots"
            )
        });
        Ok(ranges_only)
    }

    fn owned_slots(&self) -> HashSet<u16> {
        if self.slot_ranges.is_empty() {
            self.slots.iter().copied().collect()
        } else {
            self.slot_ranges
                .iter()
                .flat_map(|&(start, end)| start..=end)
                .collect()
        }
    }
}

/// A replica's progress: the node it follows and which shards are fully copied.
struct ReplicaState {
    primary: String,
    synced: Vec<bool>,
}

impl ReplicaState {
    fn role(&self) -> NodeRole {
        if self.synced.iter().all(|synced| *synced) {
            NodeRole::Replica
        } else {
            NodeRole::ReplicaSyncing
        }
    }
}

/// Manages cluster state and gossip protocol
pub struct ClusterManager {
    node_id: String,
    nodes: Arc<RwLock<HashMap<String, ClusterNode>>>,
    local_slots: Arc<RwLock<HashSet<u16>>>,
    config: ClusterConfig,
    chitchat_handle: Arc<Option<ChitchatHandle>>,
    local_addr: SocketAddr,
    namespace_strategy: NamespaceDistributionStrategy,
    /// Set on a replica; its role is derived from this.
    replica: Option<Arc<Mutex<ReplicaState>>>,
}

impl ClusterManager {
    /// Create a new cluster manager. It owns and advertises no slots until
    /// [`Self::claim_slots`]. A replica of `replica_of` starts out `replica-syncing`
    /// with `num_shards` unsynced shards.
    pub async fn new(
        config: &ClusterConfig,
        gossip_bind_addr: SocketAddr,
        redis_addr: SocketAddr,
        replica_of: Option<&str>,
        num_shards: usize,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let local_slots = Arc::new(RwLock::new(HashSet::new()));
        let replica = replica_of.map(|primary| {
            Arc::new(Mutex::new(ReplicaState {
                primary: primary.to_string(),
                synced: vec![false; num_shards],
            }))
        });

        let nodes = Arc::new(RwLock::new(HashMap::new()));

        // Set up chitchat for gossip protocol
        let gossip_addr = SocketAddr::new(gossip_bind_addr.ip(), config.port);

        // Peers gossip over UDP on `cluster.port` at the advertised host. Falls back to
        // the bind address when no (or a non-IP) advertise_addr is configured.
        let (local_ip, local_port) = local_endpoint(config, redis_addr);
        let advertise_gossip_addr = config
            .advertise_addr
            .as_ref()
            .and_then(|_| local_ip.parse::<IpAddr>().ok())
            .map_or(gossip_addr, |ip| SocketAddr::new(ip, config.port));

        // The startup time is the generation, so peers drop the state of a previous run.
        let generation_id = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_millis() as u64);

        let chitchat_id = ChitchatId {
            node_id: config.node_id.clone(),
            generation_id,
            gossip_advertise_addr: advertise_gossip_addr,
        };

        let failure_detector_config = FailureDetectorConfig {
            phi_threshold: 8.0,
            initial_interval: Duration::from_millis(1000),
            max_interval: Duration::from_secs(10),
            dead_node_grace_period: Duration::from_secs(60),
            sampling_window_size: 100,
        };

        let chitchat_config = ChitchatConfig {
            chitchat_id: chitchat_id.clone(),
            cluster_id: "blobasaur-cluster".to_string(),
            gossip_interval: Duration::from_millis(config.gossip_interval_ms.unwrap_or(1000)),
            listen_addr: gossip_addr,
            seed_nodes: config.seeds.clone(),
            failure_detector_config,
            marked_for_deletion_grace_period: Duration::from_secs(3600),
            catchup_callback: None,
            extra_liveness_predicate: None,
        };

        // Create initial gossip data for this node
        let initial_gossip_data = NodeGossipData::new(
            format!("{}:{}", local_ip, local_port),
            &HashSet::new(),
            replica_of.map_or(NodeRole::Primary, |_| NodeRole::ReplicaSyncing),
            replica_of.map(str::to_string),
        )?;

        let gossip_data_json = serde_json::to_string(&initial_gossip_data)
            .map_err(|e| format!("Failed to serialize gossip data: {}", e))?;

        let initial_key_values = vec![("node_info".to_string(), gossip_data_json)];

        // Create UDP transport for chitchat
        info!(
            "Starting chitchat gossip protocol on {}:{}",
            gossip_bind_addr.ip(),
            config.port
        );
        info!("Seed nodes: {:?}", config.seeds);
        info!("Advertise address: {:?}", config.advertise_addr);

        let chitchat_handle = spawn_chitchat(
            chitchat_config,
            initial_key_values,
            &chitchat::transport::UdpTransport,
        )
        .await
        .map_err(|e| {
            error!("Failed to spawn chitchat: {}", e);
            format!("Failed to spawn chitchat: {}", e)
        })?;

        let cluster_manager = Self {
            node_id: config.node_id.clone(),
            nodes,
            local_slots,
            config: config.clone(),
            chitchat_handle: Arc::new(Some(chitchat_handle)),
            local_addr: redis_addr,
            namespace_strategy: NamespaceDistributionStrategy::default(),
            replica,
        };

        // Start gossip processing task
        let gossip_manager = cluster_manager.clone();
        tokio::spawn(async move {
            gossip_manager.gossip_task().await;
        });

        info!("Cluster manager initialized for node: {}", config.node_id);

        Ok(cluster_manager)
    }

    /// Background task to process gossip updates
    async fn gossip_task(&self) {
        let mut interval = interval(Duration::from_millis(
            self.config.gossip_interval_ms.unwrap_or(1000),
        ));

        info!("Gossip task started for node: {}", self.node_id);

        loop {
            interval.tick().await;
            debug!("Gossip task tick for node: {}", self.node_id);

            if let Some(handle) = self.chitchat_handle.as_ref() {
                // Get current cluster state from chitchat
                let chitchat_arc = handle.chitchat();
                let chitchat_guard = chitchat_arc.lock().await;

                // Log cluster state for debugging
                let total_nodes = chitchat_guard.node_states().len();
                let live_nodes: Vec<_> =
                    chitchat_guard.live_nodes().map(|id| &id.node_id).collect();
                let dead_nodes: Vec<_> =
                    chitchat_guard.dead_nodes().map(|id| &id.node_id).collect();

                debug!(
                    "Gossip update: {} total nodes, {} live nodes: {:?}, {} dead nodes: {:?}",
                    total_nodes,
                    live_nodes.len(),
                    live_nodes,
                    dead_nodes.len(),
                    dead_nodes
                );

                let nodes_map = live_peers(&chitchat_guard, &self.node_id);

                drop(chitchat_guard);

                // Update the nodes map and log topology changes
                let mut nodes = self.nodes.write().await;

                // Diff the new node set against the current one to detect
                // added, removed, and updated (addr/slots/role changed) nodes
                let mut added = Vec::new();
                let mut updated = Vec::new();
                let mut removed = Vec::new();

                for (id, node) in &nodes_map {
                    match nodes.get(id) {
                        None => added.push((node.id.clone(), node.addr, node.slots.len())),
                        Some(old)
                            if old.addr != node.addr
                                || old.slots != node.slots
                                || old.role != node.role
                                || old.primary != node.primary =>
                        {
                            updated.push((
                                node.id.clone(),
                                old.addr,
                                node.addr,
                                old.slots.len(),
                                node.slots.len(),
                                (old.role, node.role),
                            ));
                        }
                        _ => {}
                    }
                }
                for id in nodes.keys() {
                    if !nodes_map.contains_key(id) {
                        removed.push(id.clone());
                    }
                }

                let topology_stable = added.is_empty()
                    && updated.is_empty()
                    && removed.is_empty()
                    && !nodes_map.is_empty();
                let node_count = nodes_map.len();
                *nodes = nodes_map;
                drop(nodes);

                for (id, addr, slot_count) in added {
                    info!(
                        "Discovered cluster node: {} at {} with {} slots",
                        id, addr, slot_count
                    );
                }
                for (
                    id,
                    old_addr,
                    new_addr,
                    old_slot_count,
                    new_slot_count,
                    (old_role, new_role),
                ) in updated
                {
                    info!(
                        "Cluster node updated: {} (addr {} -> {}, slots {} -> {}, role {:?} -> {:?})",
                        id, old_addr, new_addr, old_slot_count, new_slot_count, old_role, new_role
                    );
                }
                for id in removed {
                    info!("Cluster node left the cluster: {}", id);
                }

                if topology_stable {
                    debug!(
                        "Cluster topology stable for {}: {} nodes",
                        self.node_id, node_count
                    );
                }
            } else {
                warn!(
                    "Gossip task running but chitchat_handle is None for node: {}",
                    self.node_id
                );
            }
        }
    }

    /// Decide where a request for `slot` should run. A synced replica serves reads
    /// of its primary's slots on connections that sent `READONLY`.
    pub async fn route(&self, slot: u16, is_read: bool, conn_readonly: bool) -> Route {
        // A replica owns no slots, so never runs a write itself.
        if self.replica.is_none() && self.local_slots.read().await.contains(&slot) {
            return Route::Local;
        }
        let nodes = self.nodes.read().await;
        if is_read
            && conn_readonly
            && let Some(replica) = &self.replica
        {
            let replica = replica.lock().unwrap();
            if replica.role() == NodeRole::Replica
                && nodes
                    .get(&replica.primary)
                    .is_some_and(|primary| primary.slots.contains(&slot))
            {
                return Route::Local;
            }
        }
        match nodes.values().find(|node| node.slots.contains(&slot)) {
            Some(node) => Route::Moved(format!("MOVED {} {}", slot, node.addr)),
            None => Route::ClusterDown,
        }
    }

    /// The role this node gossips.
    fn role(&self) -> NodeRole {
        self.replica
            .as_ref()
            .map_or(NodeRole::Primary, |replica| replica.lock().unwrap().role())
    }

    /// Replica: records whether `shard` holds a full copy of its primary, and
    /// gossips the role change if that flips it.
    pub async fn set_shard_synced(&self, shard: usize, synced: bool) {
        let Some(replica) = &self.replica else {
            return;
        };
        let changed = {
            let mut replica = replica.lock().unwrap();
            let before = replica.role();
            replica.synced[shard] = synced;
            before != replica.role()
        };
        if changed && let Err(e) = self.update_gossip_data().await {
            error!("Failed to gossip the new role: {}", e);
        }
    }

    /// Owns and advertises the configured slots, unless a live node already
    /// claims any of them. A node that advertised them at once could silently
    /// split slot ownership with a running primary, so first listen to gossip for
    /// a few intervals. Best-effort conflict detection, not fencing: a partitioned
    /// node, or two nodes starting at once, can both pass. Replicas own no slots.
    pub async fn claim_slots(&self) -> Result<(), String> {
        let mut slots = HashSet::new();
        slots.extend(self.config.slots.iter().flatten().copied());
        for range in self.config.slot_ranges.iter().flatten() {
            slots.extend(range.start..=range.end);
        }
        if slots.is_empty() {
            return Ok(());
        }
        if self.replica.is_some() {
            warn!("Ignoring the configured slots: a replica owns none");
            return Ok(());
        }

        let wait = Duration::from_millis(self.config.gossip_interval_ms.unwrap_or(1000)) * 3;
        info!(
            "Listening to gossip for {:?} before claiming {} slots",
            wait,
            slots.len()
        );
        tokio::time::sleep(wait).await;

        if let Some(handle) = self.chitchat_handle.as_ref() {
            let chitchat = handle.chitchat();
            let chitchat = chitchat.lock().await;
            for node in live_peers(&chitchat, &self.node_id).values() {
                let overlap: HashSet<u16> = slots.intersection(&node.slots).copied().collect();
                if !overlap.is_empty() {
                    return Err(format!(
                        "node {} already claims slots {} of this node's {}",
                        node.id,
                        Self::slots_to_ranges(&overlap),
                        Self::slots_to_ranges(&slots)
                    ));
                }
            }
        }

        let count = slots.len();
        *self.local_slots.write().await = slots;
        self.update_gossip_data().await?;
        info!("Cluster node {} claimed {} slots", self.node_id, count);
        Ok(())
    }

    /// The Redis address of a live peer, as last gossiped.
    pub async fn node_addr(&self, node_id: &str) -> Option<SocketAddr> {
        self.nodes.read().await.get(node_id).map(|node| node.addr)
    }

    /// Calculate Redis hash slot for a key
    pub fn calculate_slot(key: &str) -> u16 {
        Self::calculate_slot_with_strategy(key, NamespaceDistributionStrategy::HashTagAware)
    }

    /// Calculate slot with specific distribution strategy
    pub fn calculate_slot_with_strategy(key: &str, strategy: NamespaceDistributionStrategy) -> u16 {
        let hash_key = match strategy {
            NamespaceDistributionStrategy::NamespaceBased => {
                // Legacy behavior - use full key
                key
            }
            NamespaceDistributionStrategy::KeyBased => {
                // Use full key (same as legacy for non-namespace operations)
                key
            }
            NamespaceDistributionStrategy::HashTagAware => {
                // Extract hash tag if present (e.g., "user:{123}:profile" -> "123")
                if let Some(start) = key.find('{') {
                    if let Some(end) = key[start + 1..].find('}') {
                        let tag = &key[start + 1..start + 1 + end];
                        if !tag.is_empty() { tag } else { key }
                    } else {
                        key
                    }
                } else {
                    key
                }
            }
        };

        crc16::State::<crc16::XMODEM>::calculate(hash_key.as_bytes()) % REDIS_CLUSTER_SLOTS
    }

    /// Calculate slot for hash operations (HGET, HSET, etc.)
    pub fn calculate_slot_for_hash(&self, namespace: &str, key: &str) -> u16 {
        match self.namespace_strategy {
            NamespaceDistributionStrategy::NamespaceBased => {
                // Use namespace as the hash key (legacy behavior)
                Self::calculate_slot_with_strategy(namespace, self.namespace_strategy)
            }
            NamespaceDistributionStrategy::KeyBased
            | NamespaceDistributionStrategy::HashTagAware => {
                // Use individual key for distribution; calculate_slot_with_strategy
                // extracts the hash tag for HashTagAware.
                Self::calculate_slot_with_strategy(key, self.namespace_strategy)
            }
        }
    }

    /// Get cluster nodes information for CLUSTER NODES command
    pub async fn get_cluster_nodes(&self) -> String {
        let mut result = Vec::new();
        let nodes = self.nodes.read().await;
        let local_slots = self.local_slots.read().await;

        // Add local node
        let local_slots_ranges = Self::slots_to_ranges(&local_slots);
        let (local_ip, local_port) = local_endpoint(&self.config, self.local_addr);

        // Replicas list the node they follow where primaries list `-`, and no slots.
        let local_info = match self.replica_primary() {
            Some(primary) => format!(
                "{} {}:{} myself,slave {} 0 0 0 connected",
                self.node_id, local_ip, local_port, primary
            ),
            None => format!(
                "{} {}:{} myself,master - 0 0 0 connected {}",
                self.node_id, local_ip, local_port, local_slots_ranges
            ),
        };
        result.push(local_info);

        // Add other nodes
        for node in nodes.values() {
            let node_info = match (node.role, &node.primary) {
                (NodeRole::Primary, _) | (_, None) => format!(
                    "{} {}:{} master - 0 0 0 connected {}",
                    node.id,
                    node.addr.ip(),
                    node.addr.port(),
                    Self::slots_to_ranges(&node.slots)
                ),
                (_, Some(primary)) => format!(
                    "{} {}:{} slave {} 0 0 0 connected",
                    node.id,
                    node.addr.ip(),
                    node.addr.port(),
                    primary
                ),
            };
            result.push(node_info);
        }

        format!("{}\n", result.join("\n"))
    }

    /// The node id this node replicates, if it is a replica.
    fn replica_primary(&self) -> Option<String> {
        Some(self.replica.as_ref()?.lock().unwrap().primary.clone())
    }

    /// Collapse a slot set into sorted, inclusive `(start, end)` runs
    fn slot_runs(slots: &HashSet<u16>) -> Vec<(u16, u16)> {
        let mut sorted: Vec<u16> = slots.iter().copied().collect();
        sorted.sort_unstable();

        let mut runs: Vec<(u16, u16)> = Vec::new();
        for slot in sorted {
            match runs.last_mut() {
                Some((_, end)) if slot - *end == 1 => *end = slot,
                _ => runs.push((slot, slot)),
            }
        }
        runs
    }

    /// Convert slots set to Redis slot ranges format
    fn slots_to_ranges(slots: &HashSet<u16>) -> String {
        Self::slot_runs(slots)
            .into_iter()
            .map(|(start, end)| {
                if start == end {
                    start.to_string()
                } else {
                    format!("{}-{}", start, end)
                }
            })
            .collect::<Vec<_>>()
            .join(" ")
    }

    /// Get the slot ranges of every live node (local and remote) for CLUSTER SLOTS
    pub async fn get_cluster_slots(&self) -> Vec<SlotRange> {
        let (local_ip, local_port) = local_endpoint(&self.config, self.local_addr);
        let nodes = self.nodes.read().await;

        // Replicas serving reads, by the primary they follow.
        let mut replicas: HashMap<String, Vec<(String, u16, String)>> = HashMap::new();
        if self.role() == NodeRole::Replica
            && let Some(primary) = self.replica_primary()
        {
            replicas.entry(primary).or_default().push((
                local_ip.clone(),
                local_port,
                self.node_id.clone(),
            ));
        }
        for node in nodes.values() {
            if let (NodeRole::Replica, Some(primary)) = (node.role, &node.primary) {
                replicas.entry(primary.clone()).or_default().push((
                    node.addr.ip().to_string(),
                    node.addr.port(),
                    node.id.clone(),
                ));
            }
        }
        for list in replicas.values_mut() {
            list.sort_by(|a, b| a.2.cmp(&b.2));
        }

        let mut ranges: Vec<SlotRange> = Self::slot_runs(&*self.local_slots.read().await)
            .into_iter()
            .map(|(start, end)| SlotRange {
                start,
                end,
                node_id: self.node_id.clone(),
                ip: local_ip.clone(),
                port: local_port,
                replicas: replicas.get(&self.node_id).cloned().unwrap_or_default(),
            })
            .collect();

        for node in nodes.values() {
            ranges.extend(
                Self::slot_runs(&node.slots)
                    .into_iter()
                    .map(|(start, end)| SlotRange {
                        start,
                        end,
                        node_id: node.id.clone(),
                        ip: node.addr.ip().to_string(),
                        port: node.addr.port(),
                        replicas: replicas.get(&node.id).cloned().unwrap_or_default(),
                    }),
            );
        }

        ranges.sort_by_key(|range| range.start);
        ranges
    }

    /// Get cluster info for CLUSTER INFO command
    pub async fn get_cluster_info(&self) -> String {
        let nodes = self.nodes.read().await;
        let total_nodes = nodes.len() + 1; // +1 for local node
        let local_slots = self.local_slots.read().await;

        let mut total_assigned_slots = local_slots.len();
        for node in nodes.values() {
            total_assigned_slots += node.slots.len();
        }

        let cluster_state = if total_assigned_slots == REDIS_CLUSTER_SLOTS as usize {
            "ok"
        } else {
            "fail"
        };

        format!(
            "cluster_state:{}\n\
             cluster_slots_assigned:{}\n\
             cluster_slots_ok:{}\n\
             cluster_slots_pfail:0\n\
             cluster_slots_fail:0\n\
             cluster_known_nodes:{}\n\
             cluster_size:{}\n\
             cluster_current_epoch:1\n\
             cluster_my_epoch:1\n\
             cluster_stats_messages_sent:0\n\
             cluster_stats_messages_received:0\n",
            cluster_state, total_assigned_slots, total_assigned_slots, total_nodes, total_nodes
        )
    }

    /// Add slots to this node
    pub async fn add_slots(
        &self,
        slots: Vec<u16>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.replica.is_some() {
            return Err("ERR this node is a replica".into());
        }
        let mut local_slots = self.local_slots.write().await;
        let added: Vec<u16> = slots
            .iter()
            .copied()
            .filter(|slot| local_slots.insert(*slot))
            .collect();
        drop(local_slots);

        if let Err(e) = self.update_gossip_data().await {
            let mut local_slots = self.local_slots.write().await;
            for slot in &added {
                local_slots.remove(slot);
            }
            return Err(format!("ERR {}", e).into());
        }

        info!("Added slots {:?} to local node", slots);
        Ok(())
    }

    /// Remove slots from this node
    pub async fn remove_slots(
        &self,
        slots: Vec<u16>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.replica.is_some() {
            return Err("ERR this node is a replica".into());
        }
        let mut local_slots = self.local_slots.write().await;
        let removed: Vec<u16> = slots
            .iter()
            .copied()
            .filter(|slot| local_slots.remove(slot))
            .collect();
        drop(local_slots);

        if let Err(e) = self.update_gossip_data().await {
            self.local_slots.write().await.extend(removed);
            return Err(format!("ERR {}", e).into());
        }

        info!("Removed slots {:?} from local node", slots);
        Ok(())
    }

    /// Update gossip data with the current slots and role. Errors, leaving the
    /// gossiped value as it was, if the slots can't be encoded within a datagram.
    async fn update_gossip_data(&self) -> Result<(), String> {
        if let Some(handle) = self.chitchat_handle.as_ref() {
            let (local_ip, local_port) = local_endpoint(&self.config, self.local_addr);
            let chitchat_arc = handle.chitchat();
            // Read the state under the chitchat lock, so concurrent updates
            // publish the latest one last.
            let mut chitchat_guard = chitchat_arc.lock().await;
            let gossip_data = NodeGossipData::new(
                format!("{}:{}", local_ip, local_port),
                &*self.local_slots.read().await,
                self.role(),
                self.replica_primary(),
            )?;

            if let Ok(gossip_data_json) = serde_json::to_string(&gossip_data) {
                debug!("Updating gossip data: {}", gossip_data_json);
                chitchat_guard
                    .self_node_state()
                    .set("node_info", &gossip_data_json);
                info!(
                    "Updated gossip data with {} slots, role {:?}",
                    gossip_data.owned_slots().len(),
                    gossip_data.role
                );
            } else {
                error!("Failed to serialize gossip data");
            }
        }
        Ok(())
    }

    /// Get slots owned by this node
    #[cfg(test)]
    pub async fn get_local_slots(&self) -> HashSet<u16> {
        self.local_slots.read().await.clone()
    }

    /// Shutdown the cluster manager
    pub async fn shutdown(&self) {
        if let Some(handle) = self.chitchat_handle.as_ref() {
            let chitchat_arc = handle.chitchat();
            let mut chitchat_guard = chitchat_arc.lock().await;
            chitchat_guard
                .self_node_state()
                .set("node_status", "shutting_down");
            drop(chitchat_guard);
            // Note: We don't call handle.shutdown() as it would consume the handle
        }
        info!("Shutting down cluster manager");
    }
}

// Implement Clone for ClusterManager to allow sharing between tasks
impl Clone for ClusterManager {
    fn clone(&self) -> Self {
        Self {
            node_id: self.node_id.clone(),
            nodes: Arc::clone(&self.nodes),
            local_slots: Arc::clone(&self.local_slots),
            config: self.config.clone(),
            chitchat_handle: Arc::clone(&self.chitchat_handle),
            local_addr: self.local_addr,
            namespace_strategy: self.namespace_strategy,
            replica: self.replica.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ClusterConfig;
    use std::net::SocketAddr;

    #[test]
    fn test_calculate_slot_basic_keys() {
        // Test basic key hashing
        let slot1 = ClusterManager::calculate_slot("mykey");
        let slot2 = ClusterManager::calculate_slot("anotherkey");

        // Slots should be within valid range
        assert!(slot1 < REDIS_CLUSTER_SLOTS);
        assert!(slot2 < REDIS_CLUSTER_SLOTS);

        // Same key should always produce same slot
        assert_eq!(slot1, ClusterManager::calculate_slot("mykey"));
    }

    #[test]
    fn test_calculate_slot_with_hash_tags() {
        // Keys with same hash tag should go to same slot
        let slot1 = ClusterManager::calculate_slot("user:{1000}:profile");
        let slot2 = ClusterManager::calculate_slot("user:{1000}:settings");
        let slot3 = ClusterManager::calculate_slot("user:{1000}:data");

        assert_eq!(slot1, slot2);
        assert_eq!(slot2, slot3);

        // Different hash tags should likely go to different slots
        let slot4 = ClusterManager::calculate_slot("user:{2000}:profile");
        // Note: Different hash tags *might* collide, but it's very unlikely
        assert!(slot4 < REDIS_CLUSTER_SLOTS);
    }

    #[test]
    fn test_calculate_slot_empty_hash_tag() {
        // Empty hash tag should use whole key
        let slot1 = ClusterManager::calculate_slot("user:{}:profile");
        let slot2 = ClusterManager::calculate_slot("user:{}:profile");

        assert_eq!(slot1, slot2);
        assert!(slot1 < REDIS_CLUSTER_SLOTS);
    }

    #[test]
    fn test_calculate_slot_no_hash_tag() {
        // Keys without hash tags should use full key
        let slot1 = ClusterManager::calculate_slot("simple_key");
        let slot2 = ClusterManager::calculate_slot("another_simple_key");

        assert!(slot1 < REDIS_CLUSTER_SLOTS);
        assert!(slot2 < REDIS_CLUSTER_SLOTS);

        // Same key should produce same slot
        assert_eq!(slot1, ClusterManager::calculate_slot("simple_key"));
    }

    #[test]
    fn test_calculate_slot_redis_compatibility() {
        // Test some known Redis slot calculations for compatibility
        // These values are based on Redis cluster slot calculation

        // "key" should map to slot 12539 in Redis
        assert_eq!(ClusterManager::calculate_slot("key"), 12539);

        // "foo" should map to slot 12182 in Redis
        assert_eq!(ClusterManager::calculate_slot("foo"), 12182);

        // "bar" should map to slot 5061 in Redis
        assert_eq!(ClusterManager::calculate_slot("bar"), 5061);
    }

    #[test]
    fn test_slots_to_ranges() {
        // Test slot range formatting
        let mut slots = HashSet::new();

        // Single slot
        slots.insert(1);
        assert_eq!(ClusterManager::slots_to_ranges(&slots), "1");

        // Continuous range
        slots.clear();
        slots.extend([1, 2, 3, 4, 5]);
        assert_eq!(ClusterManager::slots_to_ranges(&slots), "1-5");

        // Multiple ranges
        slots.clear();
        slots.extend([1, 2, 3, 10, 11, 20]);
        let result = ClusterManager::slots_to_ranges(&slots);
        assert!(result.contains("1-3"));
        assert!(result.contains("10-11"));
        assert!(result.contains("20"));

        // Empty set
        slots.clear();
        assert_eq!(ClusterManager::slots_to_ranges(&slots), "");
    }

    #[tokio::test]
    async fn test_cluster_manager_creation() {
        let config = ClusterConfig {
            enabled: true,
            node_id: "test-node".to_string(),
            seeds: vec!["127.0.0.1:7000".to_string()],
            port: 7000,
            slots: Some(vec![0, 1, 2, 3, 4]),
            slot_ranges: None,
            gossip_interval_ms: Some(100),
            advertise_addr: Some("127.0.0.1:7000".to_string()),
        };

        let gossip_bind_addr: SocketAddr = "127.0.0.1:7000".parse().unwrap();
        let redis_addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
        let cluster_manager =
            ClusterManager::new(&config, gossip_bind_addr, redis_addr, None, 1).await;

        // Note: This test might fail in CI due to UDP binding issues
        if let Ok(manager) = cluster_manager {
            assert_eq!(manager.node_id, "test-node");
            assert!(manager.get_local_slots().await.is_empty());
            manager.claim_slots().await.unwrap();

            // Check local slots were set
            let local_slots = manager.get_local_slots().await;
            assert_eq!(local_slots.len(), 5);
            assert!(local_slots.contains(&0));
            assert!(local_slots.contains(&4));
        }

        // Note: Additional namespace distribution tests are in tests/namespace_distribution_test.rs
    }

    /// A manager without gossip, with `remote` nodes already known as live
    fn manager_with(local: &[u16], remote: &[(&str, &str, &[u16])]) -> ClusterManager {
        let config = ClusterConfig {
            enabled: true,
            node_id: "node-a".to_string(),
            seeds: vec![],
            port: 7000,
            slots: None,
            slot_ranges: None,
            gossip_interval_ms: Some(1000),
            advertise_addr: Some("10.0.0.1:6379".to_string()),
        };
        let nodes = remote
            .iter()
            .map(|(id, addr, slots)| {
                (
                    id.to_string(),
                    ClusterNode {
                        id: id.to_string(),
                        addr: addr.parse().unwrap(),
                        slots: slots.iter().copied().collect(),
                        _state: "master".to_string(),
                        role: NodeRole::Primary,
                        primary: None,
                    },
                )
            })
            .collect();
        ClusterManager {
            node_id: config.node_id.clone(),
            nodes: Arc::new(RwLock::new(nodes)),
            local_slots: Arc::new(RwLock::new(local.iter().copied().collect())),
            config,
            chitchat_handle: Arc::new(None),
            local_addr: "0.0.0.0:6379".parse().unwrap(),
            namespace_strategy: NamespaceDistributionStrategy::default(),
            replica: None,
        }
    }

    /// Makes `manager` a replica of `primary` with two shards, `synced` or not.
    fn into_replica(mut manager: ClusterManager, primary: &str, synced: bool) -> ClusterManager {
        manager.replica = Some(Arc::new(Mutex::new(ReplicaState {
            primary: primary.to_string(),
            synced: vec![synced; 2],
        })));
        manager
    }

    async fn add_replica(manager: &ClusterManager, id: &str, addr: &str, of: &str, role: NodeRole) {
        manager.nodes.write().await.insert(
            id.to_string(),
            ClusterNode {
                id: id.to_string(),
                addr: addr.parse().unwrap(),
                slots: HashSet::new(),
                _state: "master".to_string(),
                role,
                primary: Some(of.to_string()),
            },
        );
    }

    #[tokio::test]
    async fn test_route() {
        let manager = manager_with(&[0, 1], &[("node-b", "10.0.0.2:6379", &[10])]);

        assert_eq!(manager.route(1, false, false).await, Route::Local);
        assert_eq!(
            manager.route(10, false, false).await,
            Route::Moved("MOVED 10 10.0.0.2:6379".to_string())
        );
        // Nobody owns slot 5: must not fall through to local execution
        assert_eq!(manager.route(5, false, false).await, Route::ClusterDown);
        // READONLY means nothing to a primary
        assert_eq!(
            manager.route(10, true, true).await,
            Route::Moved("MOVED 10 10.0.0.2:6379".to_string())
        );
    }

    #[tokio::test]
    async fn test_route_on_a_replica() {
        let moved = Route::Moved("MOVED 10 10.0.0.2:6379".to_string());
        let remote: &[(&str, &str, &[u16])] = &[
            ("node-b", "10.0.0.2:6379", &[10]),
            ("node-c", "10.0.0.3:6379", &[20]),
        ];
        let replica = into_replica(manager_with(&[], remote), "node-b", true);

        // Only a READONLY connection reading its primary's slots is served here.
        assert_eq!(replica.route(10, true, true).await, Route::Local);
        assert_eq!(replica.route(10, true, false).await, moved);
        assert_eq!(replica.route(10, false, true).await, moved);
        // Another primary's slot, and a slot nobody owns.
        assert_eq!(
            replica.route(20, true, true).await,
            Route::Moved("MOVED 20 10.0.0.3:6379".to_string())
        );
        assert_eq!(replica.route(5, true, true).await, Route::ClusterDown);

        // Still copying: nothing is served.
        let syncing = into_replica(manager_with(&[], remote), "node-b", false);
        assert_eq!(syncing.route(10, true, true).await, moved);

        // Even with a local slot set, a replica never runs a write itself.
        let rogue = into_replica(manager_with(&[20], remote), "node-b", true);
        assert_ne!(rogue.route(20, false, false).await, Route::Local);

        // The primary left the cluster.
        let orphan = into_replica(manager_with(&[], &remote[1..]), "node-b", true);
        assert_eq!(orphan.route(10, true, true).await, Route::ClusterDown);
    }

    #[tokio::test]
    async fn test_replica_role_follows_synced_shards() {
        let replica = into_replica(manager_with(&[], &[]), "node-b", false);
        assert_eq!(replica.role(), NodeRole::ReplicaSyncing);
        replica.set_shard_synced(0, true).await;
        assert_eq!(replica.role(), NodeRole::ReplicaSyncing);
        replica.set_shard_synced(1, true).await;
        assert_eq!(replica.role(), NodeRole::Replica);
        // A resync of one shard takes it back.
        replica.set_shard_synced(0, false).await;
        assert_eq!(replica.role(), NodeRole::ReplicaSyncing);
        assert_eq!(manager_with(&[], &[]).role(), NodeRole::Primary);
    }

    #[test]
    fn test_gossip_encodes_ranges_and_legacy_slots() {
        let slots: HashSet<u16> = [0, 1, 2, 9].into_iter().collect();
        let data =
            NodeGossipData::new("10.0.0.1:6379".into(), &slots, NodeRole::Primary, None).unwrap();
        assert_eq!(data.slots, vec![0, 1, 2, 9]);
        assert_eq!(data.slot_ranges, vec![(0, 2), (9, 9)]);
        let json = serde_json::to_string(&data).unwrap();
        assert_eq!(
            serde_json::from_str::<NodeGossipData>(&json)
                .unwrap()
                .owned_slots(),
            slots
        );

        // While the value fits a datagram the legacy list is sent too, and an old
        // node reads it; above that only the ranges.
        #[derive(Deserialize)]
        struct Legacy {
            slots: Vec<u16>,
        }
        let all = |n: u16| (0..n).collect::<HashSet<u16>>();
        let at_limit =
            NodeGossipData::new("a:1".into(), &all(9000), NodeRole::Primary, None).unwrap();
        assert_eq!(at_limit.slots.len(), 9000);
        let json = serde_json::to_string(&at_limit).unwrap();
        assert!(json.len() <= GOSSIP_VALUE_MAX, "{} bytes", json.len());
        assert_eq!(
            serde_json::from_str::<Legacy>(&json).unwrap().slots.len(),
            9000
        );
        // The worst case for 9000 slots: all five digits.
        let high: HashSet<u16> = (7384..16384).collect();
        let high = NodeGossipData::new("a:1".into(), &high, NodeRole::Primary, None).unwrap();
        assert_eq!(high.slots.len(), 9000);
        let full = NodeGossipData::new("a:1".into(), &all(16384), NodeRole::Primary, None).unwrap();
        assert!(full.slots.is_empty());
        assert_eq!(full.slot_ranges, vec![(0, 16383)]);
        let json = serde_json::to_string(&full).unwrap();
        assert!(json.len() < 1000, "{} bytes", json.len());
        assert_eq!(
            serde_json::from_str::<NodeGossipData>(&json)
                .unwrap()
                .owned_slots()
                .len(),
            16384
        );
    }

    #[test]
    fn test_gossip_falls_back_to_whichever_encoding_fits() {
        // Every other slot: the ranges are longer than the list, so the list
        // alone is sent (and old nodes can still read it).
        let alternating: HashSet<u16> = (0..16384).step_by(2).collect();
        let data =
            NodeGossipData::new("a:1".into(), &alternating, NodeRole::Primary, None).unwrap();
        assert!(data.slot_ranges.is_empty());
        let json = serde_json::to_string(&data).unwrap();
        assert!(json.len() <= GOSSIP_VALUE_MAX, "{} bytes", json.len());
        assert_eq!(
            serde_json::from_str::<NodeGossipData>(&json)
                .unwrap()
                .owned_slots(),
            alternating
        );
    }

    #[test]
    fn test_gossip_decodes_what_old_nodes_send() {
        // No ranges, role or primary: read the slot list and assume a primary.
        let old = r#"{"addr":"10.0.0.1:6379","slots":[5,6,7],"state":"master"}"#;
        let data: NodeGossipData = serde_json::from_str(old).unwrap();
        assert_eq!(data.owned_slots(), [5, 6, 7].into_iter().collect());
        assert_eq!((data.role, data.primary), (NodeRole::Primary, None));

        // Ranges win over a stale list.
        let new = r#"{"addr":"a:1","slots":[1],"slot_ranges":[[10,12]],"state":"master"}"#;
        let data: NodeGossipData = serde_json::from_str(new).unwrap();
        assert_eq!(data.owned_slots(), [10, 11, 12].into_iter().collect());

        let replica = NodeGossipData::new(
            "a:1".into(),
            &HashSet::new(),
            NodeRole::ReplicaSyncing,
            Some("node-a".into()),
        )
        .unwrap();
        let json = serde_json::to_string(&replica).unwrap();
        assert!(json.contains(r#""role":"replica-syncing""#), "{json}");
        assert!(json.contains(r#""primary":"node-a""#), "{json}");
    }

    #[tokio::test]
    async fn test_get_cluster_slots_lists_local_and_remote_ranges() {
        let manager = manager_with(&[0, 1, 2, 9], &[("node-b", "10.0.0.2:6380", &[5, 6])]);

        let range = |start, end, id: &str, ip: &str, port| SlotRange {
            start,
            end,
            node_id: id.to_string(),
            ip: ip.to_string(),
            port,
            replicas: vec![],
        };
        assert_eq!(
            manager.get_cluster_slots().await,
            vec![
                range(0, 2, "node-a", "10.0.0.1", 6379),
                range(5, 6, "node-b", "10.0.0.2", 6380),
                range(9, 9, "node-a", "10.0.0.1", 6379),
            ]
        );
    }

    #[tokio::test]
    async fn test_topology_lists_replicas() {
        let manager = manager_with(&[0, 1], &[("node-b", "10.0.0.2:6379", &[5, 6])]);
        add_replica(
            &manager,
            "node-b2",
            "10.0.0.4:6379",
            "node-b",
            NodeRole::Replica,
        )
        .await;
        add_replica(
            &manager,
            "node-b1",
            "10.0.0.3:6379",
            "node-b",
            NodeRole::Replica,
        )
        .await;
        add_replica(
            &manager,
            "node-a1",
            "10.0.0.5:6379",
            "node-a",
            NodeRole::ReplicaSyncing,
        )
        .await;

        // Syncing replicas are not listed; the others follow their primary by id.
        let ranges = manager.get_cluster_slots().await;
        assert_eq!(ranges.len(), 2);
        assert_eq!(ranges[0].node_id, "node-a");
        assert!(ranges[0].replicas.is_empty());
        let replica = |ip: &str, id: &str| (ip.to_string(), 6379, id.to_string());
        assert_eq!(
            ranges[1].replicas,
            vec![
                replica("10.0.0.3", "node-b1"),
                replica("10.0.0.4", "node-b2")
            ]
        );

        let nodes = manager.get_cluster_nodes().await;
        assert!(nodes.contains("node-a 10.0.0.1:6379 myself,master - 0 0 0 connected 0-1\n"));
        assert!(nodes.contains("node-b 10.0.0.2:6379 master - 0 0 0 connected 5-6\n"));
        for (id, ip, primary) in [
            ("node-b1", "10.0.0.3", "node-b"),
            ("node-a1", "10.0.0.5", "node-a"),
        ] {
            let line = format!("{id} {ip}:6379 slave {primary} 0 0 0 connected\n");
            assert!(nodes.contains(&line), "{nodes}");
        }
    }

    #[tokio::test]
    async fn test_replica_topology_includes_itself() {
        let manager = into_replica(
            manager_with(&[], &[("node-b", "10.0.0.2:6379", &[5, 6])]),
            "node-b",
            true,
        );
        let ranges = manager.get_cluster_slots().await;
        assert_eq!(
            ranges[0].replicas,
            vec![("10.0.0.1".to_string(), 6379, "node-a".to_string())]
        );
        let nodes = manager.get_cluster_nodes().await;
        assert!(
            nodes.starts_with("node-a 10.0.0.1:6379 myself,slave node-b 0 0 0 connected\n"),
            "{nodes}"
        );

        // Not listed while copying.
        let syncing = into_replica(
            manager_with(&[], &[("node-b", "10.0.0.2:6379", &[5, 6])]),
            "node-b",
            false,
        );
        assert!(syncing.get_cluster_slots().await[0].replicas.is_empty());
    }

    #[test]
    fn test_newest_generations() {
        let addr: SocketAddr = "127.0.0.1:7000".parse().unwrap();
        let ids = [
            ChitchatId::new("a".to_string(), 5, addr),
            ChitchatId::new("a".to_string(), 9, addr),
            ChitchatId::new("a".to_string(), 7, addr),
            ChitchatId::new("b".to_string(), 1, addr),
        ];

        let mut newest: Vec<_> = newest_generations(ids.iter())
            .into_iter()
            .map(|id| (id.node_id.as_str(), id.generation_id))
            .collect();
        newest.sort();
        assert_eq!(newest, vec![("a", 9), ("b", 1)]);
    }

    #[test]
    fn test_local_endpoint() {
        let redis_addr: SocketAddr = "0.0.0.0:6379".parse().unwrap();
        let mut config = manager_with(&[], &[]).config;

        assert_eq!(
            local_endpoint(&config, redis_addr),
            ("10.0.0.1".to_string(), 6379)
        );
        config.advertise_addr = None;
        assert_eq!(
            local_endpoint(&config, redis_addr),
            ("127.0.0.1".to_string(), 6379)
        );
    }

    #[tokio::test]
    async fn test_add_remove_slots() {
        let config = ClusterConfig {
            enabled: true,
            node_id: "test-node".to_string(),
            seeds: vec![],
            port: 7000,
            slots: Some(vec![0, 1, 2]),
            slot_ranges: None,
            gossip_interval_ms: Some(100),
            advertise_addr: Some("127.0.0.1:7000".to_string()),
        };

        let gossip_bind_addr: SocketAddr = "127.0.0.1:7000".parse().unwrap();
        let redis_addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();

        // This test might fail in CI due to UDP binding, so we'll handle the error gracefully
        if let Ok(manager) =
            ClusterManager::new(&config, gossip_bind_addr, redis_addr, None, 1).await
        {
            manager.claim_slots().await.unwrap();
            // Initial slots
            let initial_slots = manager.get_local_slots().await;
            assert_eq!(initial_slots.len(), 3);

            // Add slots
            manager.add_slots(vec![10, 11, 12]).await.unwrap();
            let after_add = manager.get_local_slots().await;
            assert_eq!(after_add.len(), 6);
            assert!(after_add.contains(&10));
            assert!(after_add.contains(&11));
            assert!(after_add.contains(&12));

            // Remove slots
            manager.remove_slots(vec![1, 11]).await.unwrap();
            let after_remove = manager.get_local_slots().await;
            assert_eq!(after_remove.len(), 4);
            assert!(!after_remove.contains(&1));
            assert!(!after_remove.contains(&11));
            assert!(after_remove.contains(&0));
            assert!(after_remove.contains(&2));
            assert!(after_remove.contains(&10));
            assert!(after_remove.contains(&12));
        }
    }
}
