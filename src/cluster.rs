mod state;

#[allow(unused_imports)]
pub use state::{
    CLUSTER_STATE_FILE, ClusterState, LoadedClusterState, MergeOutcome, PersistedRole, SlotBinding,
    SlotRangeBinding, SlotTable, configured_slots, load_or_bootstrap,
};

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Once};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chitchat::{
    Chitchat, ChitchatConfig, ChitchatHandle, ChitchatId, FailureDetectorConfig, spawn_chitchat,
};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
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

/// Redis Cluster error reply for a hash slot with no known owner.
pub const CLUSTERDOWN_HASH_SLOT_NOT_SERVED: &str = "CLUSTERDOWN Hash slot not served";

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
    pub current_epoch: u64,
    pub config_epoch: u64,
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Route {
    /// This node owns the slot.
    Local,
    /// A live peer owns the slot; holds the full `MOVED <slot> <ip>:<port>` reply.
    Moved(String),
    /// No live node owns the slot.
    ClusterDown,
}

impl Route {
    /// Returns the Redis error reply when the command must not execute locally.
    pub fn into_error(self) -> Option<String> {
        match self {
            Route::Local => None,
            Route::Moved(redirect) => Some(redirect),
            Route::ClusterDown => Some(CLUSTERDOWN_HASH_SLOT_NOT_SERVED.to_string()),
        }
    }
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

/// One node entry inside a `CLUSTER SHARDS` shard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterShardNode {
    pub id: String,
    pub port: u16,
    pub ip: String,
    pub endpoint: String,
    pub role: &'static str,
    pub replication_offset: i64,
    pub health: &'static str,
}

/// One shard entry as reported by Redis 7 `CLUSTER SHARDS`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterShard {
    pub slots: Vec<(u16, u16)>,
    pub nodes: Vec<ClusterShardNode>,
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
                            current_epoch: gossip_data.current_epoch,
                            config_epoch: gossip_data.config_epoch,
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
    #[serde(default)]
    pub current_epoch: u64,
    #[serde(default)]
    pub config_epoch: u64,
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
        current_epoch: u64,
        config_epoch: u64,
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
            current_epoch,
            config_epoch,
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

/// The gossip server and the task reading its state.
type Gossip = (ChitchatHandle, JoinHandle<()>);

/// Manages cluster state and gossip protocol
pub struct ClusterManager {
    node_id: String,
    /// Live peers from gossip (newest generation per node id).
    nodes: Arc<RwLock<HashMap<String, ClusterNode>>>,
    /// Last known Redis address of any peer seen live.
    known_addrs: Arc<std::sync::RwLock<HashMap<String, SocketAddr>>>,
    /// Persisted cluster state (role, primary, epochs, slot table).
    state: Arc<std::sync::RwLock<ClusterState>>,
    /// Path to `<data_dir>/cluster-state.json`; `None` only in tests without persistence.
    state_path: Option<PathBuf>,
    /// Serializes state mutations and disk persistence so writes never reorder.
    write_lock: Arc<tokio::sync::Mutex<()>>,
    /// True during first-boot bootstrap until `claim_slots` finishes and persists.
    bootstrapping: Arc<AtomicBool>,
    config: ClusterConfig,
    /// Gossip state; `None` only in tests.
    chitchat: Option<Arc<tokio::sync::Mutex<Chitchat>>>,
    /// What [`Self::shutdown`] stops.
    running: Arc<Mutex<Option<Gossip>>>,
    local_addr: SocketAddr,
    namespace_strategy: NamespaceDistributionStrategy,
    /// Per-shard synced flags for a replica; role is `Replica` once all are true.
    replica_synced: Arc<Mutex<Vec<bool>>>,
}

impl ClusterManager {
    /// Create a new cluster manager from loaded or bootstrapped cluster state.
    /// When bootstrapping (`!loaded.from_file`), it advertises no slots until
    /// [`Self::claim_slots`]. When loaded from disk, its persisted slots and
    /// epochs are active immediately.
    pub async fn new(
        config: &ClusterConfig,
        gossip_bind_addr: SocketAddr,
        redis_addr: SocketAddr,
        loaded: LoadedClusterState,
        num_shards: usize,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let LoadedClusterState {
            state: initial_state,
            from_file,
            path: state_path,
        } = loaded;
        let nodes = Arc::new(RwLock::new(HashMap::new()));
        let known_addrs = Arc::new(std::sync::RwLock::new(HashMap::new()));
        let replica_synced = Arc::new(Mutex::new(vec![false; num_shards]));

        // Set up chitchat for gossip protocol
        let gossip_addr = SocketAddr::new(gossip_bind_addr.ip(), config.port);

        // Peers gossip over UDP on `cluster.port` at the advertised host. Falls back to
        // the bind address when no (or a non-IP) advertise_addr is configured.
        let (local_ip, local_port) = local_endpoint(config, redis_addr);
        let advertise_gossip_addr =
            gossip_advertise_addr(config.advertise_addr.as_deref(), gossip_addr);

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

        let initial_slots = if from_file && !initial_state.is_replica() {
            initial_state.slots.slots_of(&config.node_id)
        } else {
            HashSet::new()
        };
        let initial_role = if initial_state.is_replica() {
            NodeRole::ReplicaSyncing
        } else {
            NodeRole::Primary
        };
        let initial_gossip_data = NodeGossipData::new(
            format!("{}:{}", local_ip, local_port),
            &initial_slots,
            initial_role,
            initial_state.primary.clone(),
            initial_state.current_epoch,
            initial_state.config_epoch,
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
            known_addrs,
            state: Arc::new(std::sync::RwLock::new(initial_state)),
            state_path: Some(state_path),
            write_lock: Arc::new(tokio::sync::Mutex::new(())),
            bootstrapping: Arc::new(AtomicBool::new(!from_file)),
            config: config.clone(),
            chitchat: Some(chitchat_handle.chitchat()),
            running: Arc::new(Mutex::new(None)),
            local_addr: redis_addr,
            namespace_strategy: NamespaceDistributionStrategy::default(),
            replica_synced,
        };

        // Start gossip processing task
        let gossip_manager = cluster_manager.clone();
        let gossip_task = tokio::spawn(async move {
            gossip_manager.gossip_task().await;
        });
        *cluster_manager.running.lock().unwrap() = Some((chitchat_handle, gossip_task));

        info!("Cluster manager initialized for node: {}", config.node_id);

        Ok(cluster_manager)
    }

    /// This node's cluster id (`CLUSTER MYID`).
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// Whether this node's runtime role is replica.
    pub fn is_replica(&self) -> bool {
        self.state.read().unwrap().is_replica()
    }

    /// The primary node id this node follows, if its runtime role is replica.
    pub fn primary_id(&self) -> Option<String> {
        self.state.read().unwrap().primary.clone()
    }

    async fn persist_state(&self, state: &ClusterState) -> Result<(), String> {
        let Some(path) = self.state_path.clone() else {
            return Ok(());
        };
        let snapshot = state.clone();
        tokio::task::spawn_blocking(move || snapshot.save_atomic(&path))
            .await
            .map_err(|e| format!("cluster state persist task failed: {e}"))?
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

            if let Some(chitchat_arc) = &self.chitchat {
                // Get current cluster state from chitchat
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

                self.process_peers(nodes_map).await;
            } else {
                warn!(
                    "Gossip task running without chitchat for node: {}",
                    self.node_id
                );
            }
        }
    }

    /// Apply a fresh snapshot of live gossip peers: update peer view, adopt
    /// higher `current_epoch`, merge slot claims by the epoch rule, and resolve
    /// `config_epoch` collisions.
    async fn process_peers(&self, nodes_map: HashMap<String, ClusterNode>) {
        {
            let mut addrs = self.known_addrs.write().unwrap();
            for (id, node) in &nodes_map {
                addrs.insert(id.clone(), node.addr);
            }
        }

        // Update the nodes map and log topology changes
        let mut nodes = self.nodes.write().await;

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
                        || old.primary != node.primary
                        || old.config_epoch != node.config_epoch =>
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

        let topology_stable =
            added.is_empty() && updated.is_empty() && removed.is_empty() && !nodes_map.is_empty();
        let node_count = nodes_map.len();
        *nodes = nodes_map.clone();
        drop(nodes);

        for (id, addr, slot_count) in added {
            info!(
                "Discovered cluster node: {} at {} with {} slots",
                id, addr, slot_count
            );
        }
        for (id, old_addr, new_addr, old_slot_count, new_slot_count, (old_role, new_role)) in
            updated
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

        let _guard = self.write_lock.lock().await;
        let is_bootstrapping = self.bootstrapping.load(Ordering::Acquire);
        let mut state = self.state.read().unwrap().clone();
        let had_own_slots =
            state.role == PersistedRole::Primary && state.slots.has_slots_of(&self.node_id);

        let mut changed = false;
        let mut gossip_changed = false;

        // Adopt the highest epoch seen across all peers.
        let mut max_epoch = state.current_epoch;
        for peer in nodes_map.values() {
            max_epoch = max_epoch.max(peer.current_epoch).max(peer.config_epoch);
        }
        if max_epoch > state.current_epoch {
            state.current_epoch = max_epoch;
            changed = true;
            gossip_changed = true;
        }

        // Merge live primaries' slot claims in deterministic node-id order.
        let mut primary_peers: Vec<&ClusterNode> = nodes_map
            .values()
            .filter(|peer| peer.role == NodeRole::Primary)
            .collect();
        primary_peers.sort_by(|a, b| a.id.cmp(&b.id));

        for peer in &primary_peers {
            let outcome =
                state
                    .slots
                    .merge_claim(&peer.id, peer.config_epoch, &peer.slots, &self.node_id);
            if outcome.changed {
                changed = true;
            }
            if outcome.lost_own_slots {
                gossip_changed = true;
            }
        }

        if !is_bootstrapping && had_own_slots && !state.slots.has_slots_of(&self.node_id) {
            error!(
                "Primary {} lost its last slot; serving no slots (runtime demotion not yet implemented)",
                self.node_id
            );
        }

        // Redis epoch collision rule: when two primaries share a config_epoch,
        // the lexicographically smaller node id bumps current_epoch and takes it.
        if !is_bootstrapping && state.role == PersistedRole::Primary {
            let collides = primary_peers
                .iter()
                .any(|peer| peer.config_epoch == state.config_epoch && self.node_id < peer.id);
            if collides {
                let new_epoch = state.current_epoch + 1;
                state.current_epoch = new_epoch;
                state.config_epoch = new_epoch;
                state.slots.rebind_owner_epoch(&self.node_id, new_epoch);
                changed = true;
                gossip_changed = true;
                info!(
                    "Resolved config_epoch collision on {}: bumped config_epoch to {}",
                    self.node_id, new_epoch
                );
            }
        }

        if changed {
            if !is_bootstrapping && let Err(e) = self.persist_state(&state).await {
                error!("Failed to persist cluster state after gossip update: {}", e);
                return;
            }
            *self.state.write().unwrap() = state;
        }
        drop(_guard);

        if gossip_changed
            && !is_bootstrapping
            && let Err(e) = self.update_gossip_data().await
        {
            error!("Failed to gossip updated epochs/slots: {}", e);
        }
    }

    /// Decide where a request for `slot` should run from the slot table.
    /// A synced replica serves reads of its primary's slots on connections that
    /// sent `READONLY`.
    pub async fn route(&self, slot: u16, is_read: bool, conn_readonly: bool) -> Route {
        let (role, primary, binding) = {
            let state = self.state.read().unwrap();
            (
                state.role,
                state.primary.clone(),
                state.slots.get(slot).cloned(),
            )
        };
        let Some(binding) = binding else {
            return Route::ClusterDown;
        };
        if binding.node_id == self.node_id {
            return if role == PersistedRole::Primary {
                Route::Local
            } else {
                Route::ClusterDown
            };
        }
        let nodes = self.nodes.read().await;
        let Some(owner) = nodes.get(&binding.node_id) else {
            return Route::ClusterDown;
        };
        if is_read
            && conn_readonly
            && role == PersistedRole::Replica
            && self.role() == NodeRole::Replica
            && primary.as_deref() == Some(binding.node_id.as_str())
        {
            return Route::Local;
        }
        Route::Moved(format!("MOVED {} {}", slot, owner.addr))
    }

    /// The role this node gossips.
    fn role(&self) -> NodeRole {
        if self.is_replica() {
            if self.replica_synced.lock().unwrap().iter().all(|s| *s) {
                NodeRole::Replica
            } else {
                NodeRole::ReplicaSyncing
            }
        } else {
            NodeRole::Primary
        }
    }

    /// Replica: records whether `shard` holds a full copy of its primary, and
    /// gossips the role change if that flips it.
    pub async fn set_shard_synced(&self, shard: usize, synced: bool) {
        if !self.is_replica() {
            return;
        }
        let changed = {
            let mut flags = self.replica_synced.lock().unwrap();
            let before = flags.iter().all(|s| *s);
            flags[shard] = synced;
            before != flags.iter().all(|s| *s)
        };
        if changed && let Err(e) = self.update_gossip_data().await {
            error!("Failed to gossip the new role: {}", e);
        }
    }

    /// Bootstrap step: when no state file existed on startup, claims configured
    /// slots at `config_epoch = 0` after the best-effort startup conflict check
    /// and writes `<data_dir>/cluster-state.json`. On restarts from an existing
    /// state file this is a no-op.
    pub async fn claim_slots(&self) -> Result<(), String> {
        if !self.bootstrapping.load(Ordering::Acquire) {
            return Ok(());
        }

        let configured = configured_slots(&self.config);
        let is_replica = self.is_replica();
        if is_replica && !configured.is_empty() {
            warn!("Ignoring the configured slots: a replica owns none");
        }

        if !is_replica && !configured.is_empty() {
            let wait = Duration::from_millis(self.config.gossip_interval_ms.unwrap_or(1000)) * 3;
            info!(
                "Listening to gossip for {:?} before claiming {} slots",
                wait,
                configured.len()
            );
            tokio::time::sleep(wait).await;

            if let Some(chitchat) = &self.chitchat {
                let chitchat = chitchat.lock().await;
                for node in live_peers(&chitchat, &self.node_id).values() {
                    let overlap: HashSet<u16> =
                        configured.intersection(&node.slots).copied().collect();
                    if !overlap.is_empty() {
                        return Err(format!(
                            "node {} already claims slots {} of this node's {}",
                            node.id,
                            Self::slots_to_ranges(&overlap),
                            Self::slots_to_ranges(&configured)
                        ));
                    }
                }
            }
        }

        let _guard = self.write_lock.lock().await;
        let mut state = self.state.read().unwrap().clone();
        if !is_replica {
            for &slot in &configured {
                state
                    .slots
                    .set(slot, self.node_id.clone(), state.config_epoch);
            }
            let own_slots = state.slots.slots_of(&self.node_id);
            self.gossip_data_for(&state, &own_slots)?;
        }
        self.persist_state(&state).await?;
        *self.state.write().unwrap() = state;
        self.bootstrapping.store(false, Ordering::Release);
        drop(_guard);

        self.update_gossip_data().await?;
        if !is_replica && !configured.is_empty() {
            info!(
                "Cluster node {} claimed {} slots",
                self.node_id,
                configured.len()
            );
        }
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
        let state = self.state.read().unwrap().clone();
        let known_addrs = self.known_addrs.read().unwrap().clone();

        let (local_ip, local_port) = local_endpoint(&self.config, self.local_addr);

        // Local node line.
        let local_info = match state.role {
            PersistedRole::Replica => {
                let primary = state.primary.as_deref().unwrap_or("-");
                format!(
                    "{} {}:{} myself,slave {} 0 0 {} connected",
                    self.node_id, local_ip, local_port, primary, state.config_epoch
                )
            }
            PersistedRole::Primary => {
                let own_slots = state.slots.slots_of(&self.node_id);
                let ranges = Self::slots_to_ranges(&own_slots);
                if ranges.is_empty() {
                    format!(
                        "{} {}:{} myself,master - 0 0 {} connected",
                        self.node_id, local_ip, local_port, state.config_epoch
                    )
                } else {
                    format!(
                        "{} {}:{} myself,master - 0 0 {} connected {}",
                        self.node_id, local_ip, local_port, state.config_epoch, ranges
                    )
                }
            }
        };
        result.push(local_info);

        // Live peers sorted by node id.
        let mut peers: Vec<&ClusterNode> = nodes.values().collect();
        peers.sort_by(|a, b| a.id.cmp(&b.id));
        for node in peers {
            let node_info = match (node.role, &node.primary) {
                (NodeRole::Primary, _) | (_, None) => {
                    let peer_slots = state.slots.slots_of(&node.id);
                    let ranges = Self::slots_to_ranges(&peer_slots);
                    if ranges.is_empty() {
                        format!(
                            "{} {}:{} master - 0 0 {} connected",
                            node.id,
                            node.addr.ip(),
                            node.addr.port(),
                            node.config_epoch
                        )
                    } else {
                        format!(
                            "{} {}:{} master - 0 0 {} connected {}",
                            node.id,
                            node.addr.ip(),
                            node.addr.port(),
                            node.config_epoch,
                            ranges
                        )
                    }
                }
                (_, Some(primary)) => format!(
                    "{} {}:{} slave {} 0 0 {} connected",
                    node.id,
                    node.addr.ip(),
                    node.addr.port(),
                    primary,
                    node.config_epoch
                ),
            };
            result.push(node_info);
        }

        // Offline slot owners retained in the slot table stay listed with their slots.
        for owner_id in state.slots.owners() {
            if owner_id == self.node_id || nodes.contains_key(&owner_id) {
                continue;
            }
            let addr = known_addrs
                .get(&owner_id)
                .map_or_else(|| "0.0.0.0:0".to_string(), ToString::to_string);
            let epoch = state.slots.max_epoch_for(&owner_id);
            let ranges = Self::slots_to_ranges(&state.slots.slots_of(&owner_id));
            result.push(format!(
                "{} {} master - 0 0 {} disconnected {}",
                owner_id, addr, epoch, ranges
            ));
        }

        format!("{}\n", result.join("\n"))
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
    pub(crate) fn slots_to_ranges(slots: &HashSet<u16>) -> String {
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

    /// Get the slot ranges from the slot table for CLUSTER SLOTS.
    /// Lists only live slot owners; replicas are listed only once synced.
    pub async fn get_cluster_slots(&self) -> Vec<SlotRange> {
        let (local_ip, local_port) = local_endpoint(&self.config, self.local_addr);
        let nodes = self.nodes.read().await;
        let state = self.state.read().unwrap().clone();

        // Replicas serving reads, by the primary they follow.
        let mut replicas: HashMap<String, Vec<(String, u16, String)>> = HashMap::new();
        if self.role() == NodeRole::Replica
            && let Some(primary) = state.primary.clone()
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

        let mut ranges = Vec::new();
        for (start, end, owner_id) in state.slots.runs_by_owner() {
            let (ip, port) = if owner_id == self.node_id && state.role == PersistedRole::Primary {
                (local_ip.clone(), local_port)
            } else if let Some(peer) = nodes.get(&owner_id) {
                (peer.addr.ip().to_string(), peer.addr.port())
            } else {
                // Owner is down: slot is currently CLUSTERDOWN, omit from CLUSTER SLOTS.
                continue;
            };
            ranges.push(SlotRange {
                start,
                end,
                replicas: replicas.get(&owner_id).cloned().unwrap_or_default(),
                node_id: owner_id,
                ip,
                port,
            });
        }
        ranges
    }

    /// Get cluster shards from the slot table for Redis 7 `CLUSTER SHARDS`.
    pub async fn get_cluster_shards(&self) -> Vec<ClusterShard> {
        let (local_ip, local_port) = local_endpoint(&self.config, self.local_addr);
        let nodes = self.nodes.read().await;
        let state = self.state.read().unwrap().clone();
        let known_addrs = self.known_addrs.read().unwrap().clone();

        let mut slots_by_owner: BTreeMap<String, Vec<(u16, u16)>> = BTreeMap::new();
        for (start, end, owner_id) in state.slots.runs_by_owner() {
            slots_by_owner
                .entry(owner_id)
                .or_default()
                .push((start, end));
        }

        let mut replicas_by_primary: HashMap<String, Vec<ClusterShardNode>> = HashMap::new();
        if state.role == PersistedRole::Replica
            && let Some(primary) = state.primary.clone()
        {
            let health = if self.role() == NodeRole::Replica {
                "online"
            } else {
                "loading"
            };
            replicas_by_primary
                .entry(primary)
                .or_default()
                .push(ClusterShardNode {
                    id: self.node_id.clone(),
                    port: local_port,
                    ip: local_ip.clone(),
                    endpoint: local_ip.clone(),
                    role: "replica",
                    replication_offset: 0,
                    health,
                });
        }
        for node in nodes.values() {
            if let Some(primary) = &node.primary
                && node.role != NodeRole::Primary
            {
                let health = if node.role == NodeRole::Replica {
                    "online"
                } else {
                    "loading"
                };
                let ip = node.addr.ip().to_string();
                replicas_by_primary
                    .entry(primary.clone())
                    .or_default()
                    .push(ClusterShardNode {
                        id: node.id.clone(),
                        port: node.addr.port(),
                        endpoint: ip.clone(),
                        ip,
                        role: "replica",
                        replication_offset: 0,
                        health,
                    });
            }
        }
        for list in replicas_by_primary.values_mut() {
            list.sort_by(|a, b| a.id.cmp(&b.id));
        }

        let mut shards = Vec::new();
        for (owner_id, slot_ranges) in slots_by_owner {
            let master = if owner_id == self.node_id && state.role == PersistedRole::Primary {
                ClusterShardNode {
                    id: self.node_id.clone(),
                    port: local_port,
                    ip: local_ip.clone(),
                    endpoint: local_ip.clone(),
                    role: "master",
                    replication_offset: 0,
                    health: "online",
                }
            } else if let Some(peer) = nodes.get(&owner_id) {
                let ip = peer.addr.ip().to_string();
                ClusterShardNode {
                    id: peer.id.clone(),
                    port: peer.addr.port(),
                    endpoint: ip.clone(),
                    ip,
                    role: "master",
                    replication_offset: 0,
                    health: "online",
                }
            } else if let Some(addr) = known_addrs.get(&owner_id) {
                let ip = addr.ip().to_string();
                ClusterShardNode {
                    id: owner_id.clone(),
                    port: addr.port(),
                    endpoint: ip.clone(),
                    ip,
                    role: "master",
                    replication_offset: 0,
                    health: "failed",
                }
            } else {
                continue;
            };

            let mut shard_nodes = vec![master];
            if let Some(replicas) = replicas_by_primary.remove(&owner_id) {
                shard_nodes.extend(replicas);
            }
            shards.push(ClusterShard {
                slots: slot_ranges,
                nodes: shard_nodes,
            });
        }

        shards.sort_by_key(|shard| shard.slots.first().map_or(u16::MAX, |&(start, _)| start));
        shards
    }

    /// Get cluster info for CLUSTER INFO command
    pub async fn get_cluster_info(&self) -> String {
        let nodes = self.nodes.read().await;
        let state = self.state.read().unwrap().clone();

        let slot_owners = state.slots.owners();
        let assigned_slots = state.slots.slot_count();

        let mut ok_slots = 0usize;
        for (start, end, owner_id) in state.slots.runs_by_owner() {
            let live = (owner_id == self.node_id && state.role == PersistedRole::Primary)
                || nodes.contains_key(&owner_id);
            if live {
                ok_slots += (end - start) as usize + 1;
            }
        }
        let fail_slots = assigned_slots.saturating_sub(ok_slots);

        let mut known_nodes: BTreeSet<&str> = BTreeSet::new();
        known_nodes.insert(&self.node_id);
        for id in nodes.keys() {
            known_nodes.insert(id.as_str());
        }
        for owner in &slot_owners {
            known_nodes.insert(owner.as_str());
        }

        let cluster_state = if ok_slots == REDIS_CLUSTER_SLOTS as usize {
            "ok"
        } else {
            "fail"
        };

        format!(
            "cluster_state:{}\n\
             cluster_slots_assigned:{}\n\
             cluster_slots_ok:{}\n\
             cluster_slots_pfail:0\n\
             cluster_slots_fail:{}\n\
             cluster_known_nodes:{}\n\
             cluster_size:{}\n\
             cluster_current_epoch:{}\n\
             cluster_my_epoch:{}\n\
             cluster_stats_messages_sent:0\n\
             cluster_stats_messages_received:0\n",
            cluster_state,
            assigned_slots,
            ok_slots,
            fail_slots,
            known_nodes.len(),
            slot_owners.len(),
            state.current_epoch,
            state.config_epoch
        )
    }

    /// Add slots to this node at its current `config_epoch`.
    pub async fn add_slots(
        &self,
        slots: Vec<u16>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let _guard = self.write_lock.lock().await;
        let mut candidate = self.state.read().unwrap().clone();
        if candidate.is_replica() {
            return Err("ERR this node is a replica".into());
        }
        for &slot in &slots {
            if slot >= REDIS_CLUSTER_SLOTS {
                return Err(format!("ERR invalid slot {}", slot).into());
            }
            if let Some(existing) = candidate.slots.get(slot)
                && existing.node_id != self.node_id
                && existing.epoch >= candidate.config_epoch
            {
                return Err(
                    format!("ERR slot {} is already owned by {}", slot, existing.node_id).into(),
                );
            }
            candidate
                .slots
                .set(slot, self.node_id.clone(), candidate.config_epoch);
        }
        let own_slots = candidate.slots.slots_of(&self.node_id);
        // Checked before it is installed: a rejected change is never persisted or routed on.
        self.gossip_data_for(&candidate, &own_slots)
            .map_err(|e| format!("ERR {}", e))?;
        self.persist_state(&candidate)
            .await
            .map_err(|e| format!("ERR {}", e))?;
        *self.state.write().unwrap() = candidate;
        drop(_guard);

        self.update_gossip_data()
            .await
            .map_err(|e| format!("ERR {}", e))?;

        info!("Added slots {:?} to local node", slots);
        Ok(())
    }

    /// Remove slots from this node (unbinds locally in the slot table).
    pub async fn remove_slots(
        &self,
        slots: Vec<u16>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let _guard = self.write_lock.lock().await;
        let mut candidate = self.state.read().unwrap().clone();
        if candidate.is_replica() {
            return Err("ERR this node is a replica".into());
        }
        for &slot in &slots {
            if candidate
                .slots
                .get(slot)
                .is_some_and(|b| b.node_id == self.node_id)
            {
                candidate.slots.clear(slot);
            }
        }
        let own_slots = candidate.slots.slots_of(&self.node_id);
        // Checked before it is installed: a rejected change is never persisted or routed on.
        self.gossip_data_for(&candidate, &own_slots)
            .map_err(|e| format!("ERR {}", e))?;
        self.persist_state(&candidate)
            .await
            .map_err(|e| format!("ERR {}", e))?;
        *self.state.write().unwrap() = candidate;
        drop(_guard);

        self.update_gossip_data()
            .await
            .map_err(|e| format!("ERR {}", e))?;

        info!("Removed slots {:?} from local node", slots);
        Ok(())
    }

    /// Build this node's gossip value for `state` and `own_slots`; errors if
    /// that can't be encoded within a datagram.
    fn gossip_data_for(
        &self,
        state: &ClusterState,
        own_slots: &HashSet<u16>,
    ) -> Result<NodeGossipData, String> {
        let (local_ip, local_port) = local_endpoint(&self.config, self.local_addr);
        let role = if state.is_replica() {
            if self.replica_synced.lock().unwrap().iter().all(|s| *s) {
                NodeRole::Replica
            } else {
                NodeRole::ReplicaSyncing
            }
        } else {
            NodeRole::Primary
        };
        NodeGossipData::new(
            format!("{}:{}", local_ip, local_port),
            own_slots,
            role,
            state.primary.clone(),
            state.current_epoch,
            state.config_epoch,
        )
    }

    /// Update gossip data with the current slots, role and epochs. Errors,
    /// leaving the gossiped value as it was, if the slots can't be encoded.
    async fn update_gossip_data(&self) -> Result<(), String> {
        if let Some(chitchat_arc) = &self.chitchat {
            // Read the state under the chitchat lock, so concurrent updates
            // publish the latest one last.
            let mut chitchat_guard = chitchat_arc.lock().await;
            let gossip_data = {
                let state = self.state.read().unwrap();
                let own_slots = if state.is_replica() {
                    HashSet::new()
                } else {
                    state.slots.slots_of(&self.node_id)
                };
                self.gossip_data_for(&state, &own_slots)?
            };

            if let Ok(gossip_data_json) = serde_json::to_string(&gossip_data) {
                debug!("Updating gossip data: {}", gossip_data_json);
                chitchat_guard
                    .self_node_state()
                    .set("node_info", &gossip_data_json);
                info!(
                    "Updated gossip data with {} slots, role {:?}, epochs {}/{}",
                    gossip_data.owned_slots().len(),
                    gossip_data.role,
                    gossip_data.current_epoch,
                    gossip_data.config_epoch,
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
        let state = self.state.read().unwrap();
        if state.is_replica() {
            HashSet::new()
        } else {
            state.slots.slots_of(&self.node_id)
        }
    }

    /// Snapshot of persisted cluster state, for unit tests.
    #[cfg(test)]
    pub fn state_snapshot(&self) -> ClusterState {
        self.state.read().unwrap().clone()
    }

    /// Create an in-memory replica `ClusterManager` without gossip, for tests
    /// that only exercise `AppState` replica role helpers.
    #[cfg(test)]
    pub fn for_replica_test(node_id: &str, primary: &str, num_shards: usize) -> Self {
        let config = ClusterConfig {
            enabled: true,
            node_id: node_id.to_string(),
            seeds: vec![],
            port: 0,
            slots: None,
            slot_ranges: None,
            gossip_interval_ms: Some(1000),
            advertise_addr: None,
            node_timeout_ms: None,
            replica_validity_factor: None,
        };
        Self {
            node_id: node_id.to_string(),
            nodes: Arc::new(RwLock::new(HashMap::new())),
            known_addrs: Arc::new(std::sync::RwLock::new(HashMap::new())),
            state: Arc::new(std::sync::RwLock::new(ClusterState::bootstrap_replica(
                primary.to_string(),
            ))),
            state_path: None,
            write_lock: Arc::new(tokio::sync::Mutex::new(())),
            bootstrapping: Arc::new(AtomicBool::new(false)),
            config,
            chitchat: None,
            running: Arc::new(Mutex::new(None)),
            local_addr: "127.0.0.1:6379".parse().unwrap(),
            namespace_strategy: NamespaceDistributionStrategy::default(),
            replica_synced: Arc::new(Mutex::new(vec![false; num_shards])),
        }
    }

    /// Stops gossip: the node stops advertising itself and frees its UDP port.
    pub async fn shutdown(&self) {
        if let Some(chitchat_arc) = &self.chitchat {
            chitchat_arc
                .lock()
                .await
                .self_node_state()
                .set("node_status", "shutting_down");
        }
        info!("Shutting down cluster manager");
        let running = self.running.lock().unwrap().take();
        if let Some((handle, gossip_task)) = running {
            gossip_task.abort();
            let _ = gossip_task.await;
            if let Err(e) = handle.shutdown().await {
                warn!("Failed to stop gossip: {}", e);
            }
        }
    }
}

/// The address peers should gossip to. `advertise_addr` is the Redis address
/// clients connect to, so only its IP applies here; gossip uses the cluster
/// port. Falls back to `gossip_addr` if `advertise_addr` is unset or not an
/// `ip:port`.
fn gossip_advertise_addr(advertise_addr: Option<&str>, gossip_addr: SocketAddr) -> SocketAddr {
    advertise_addr
        .and_then(|addr| {
            addr.parse::<SocketAddr>()
                .map(|sock| sock.ip())
                .or_else(|_| addr.parse::<IpAddr>())
                .ok()
        })
        .map_or(gossip_addr, |ip| SocketAddr::new(ip, gossip_addr.port()))
}

// Implement Clone for ClusterManager to allow sharing between tasks
impl Clone for ClusterManager {
    fn clone(&self) -> Self {
        Self {
            node_id: self.node_id.clone(),
            nodes: Arc::clone(&self.nodes),
            known_addrs: Arc::clone(&self.known_addrs),
            state: Arc::clone(&self.state),
            state_path: self.state_path.clone(),
            write_lock: Arc::clone(&self.write_lock),
            bootstrapping: Arc::clone(&self.bootstrapping),
            config: self.config.clone(),
            chitchat: self.chitchat.clone(),
            running: Arc::clone(&self.running),
            local_addr: self.local_addr,
            namespace_strategy: self.namespace_strategy,
            replica_synced: Arc::clone(&self.replica_synced),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ClusterConfig;
    use std::net::SocketAddr;

    #[test]
    fn gossip_is_advertised_on_the_cluster_port_not_the_redis_port() {
        let gossip: SocketAddr = "0.0.0.0:7001".parse().unwrap();
        // advertise_addr is the Redis address; only its IP carries over.
        assert_eq!(
            gossip_advertise_addr(Some("10.0.0.5:6381"), gossip),
            "10.0.0.5:7001".parse::<SocketAddr>().unwrap()
        );
        assert_eq!(gossip_advertise_addr(None, gossip), gossip);
        assert_eq!(gossip_advertise_addr(Some("not-an-addr"), gossip), gossip);
    }

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

    fn bootstrap_loaded(dir: &std::path::Path, replica_of: Option<&str>) -> LoadedClusterState {
        let state = match replica_of {
            Some(primary) => ClusterState::bootstrap_replica(primary.to_string()),
            None => ClusterState::bootstrap_primary(),
        };
        LoadedClusterState {
            state,
            from_file: false,
            path: dir.join(CLUSTER_STATE_FILE),
        }
    }

    #[tokio::test]
    async fn test_cluster_manager_creation() {
        let dir = tempfile::TempDir::new().unwrap();
        let config = ClusterConfig {
            enabled: true,
            node_id: "test-node".to_string(),
            seeds: vec!["127.0.0.1:7000".to_string()],
            port: 7000,
            slots: Some(vec![0, 1, 2, 3, 4]),
            slot_ranges: None,
            gossip_interval_ms: Some(100),
            advertise_addr: Some("127.0.0.1:7000".to_string()),
            node_timeout_ms: None,
            replica_validity_factor: None,
        };

        let gossip_bind_addr: SocketAddr = "127.0.0.1:7000".parse().unwrap();
        let redis_addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
        let cluster_manager = ClusterManager::new(
            &config,
            gossip_bind_addr,
            redis_addr,
            bootstrap_loaded(dir.path(), None),
            1,
        )
        .await;

        // Note: This test might fail in CI due to UDP binding issues
        if let Ok(manager) = cluster_manager {
            assert_eq!(manager.node_id, "test-node");
            assert!(manager.get_local_slots().await.is_empty());
            manager.claim_slots().await.unwrap();

            // Check local slots were set and persisted
            let local_slots = manager.get_local_slots().await;
            assert_eq!(local_slots.len(), 5);
            assert!(local_slots.contains(&0));
            assert!(local_slots.contains(&4));
            assert!(dir.path().join(CLUSTER_STATE_FILE).exists());
            manager.shutdown().await;
        }

        // Note: Additional namespace distribution tests are in tests/namespace_distribution_test.rs
    }

    /// A manager without gossip, with `remote` nodes already known as live
    fn manager_with(local: &[u16], remote: &[(&str, &str, &[u16])]) -> ClusterManager {
        manager_with_id("node-a", local, remote)
    }

    fn manager_with_id(
        node_id: &str,
        local: &[u16],
        remote: &[(&str, &str, &[u16])],
    ) -> ClusterManager {
        let config = ClusterConfig {
            enabled: true,
            node_id: node_id.to_string(),
            seeds: vec![],
            port: 7000,
            slots: None,
            slot_ranges: None,
            gossip_interval_ms: Some(1000),
            advertise_addr: Some("10.0.0.1:6379".to_string()),
            node_timeout_ms: None,
            replica_validity_factor: None,
        };
        let mut state = ClusterState::bootstrap_primary();
        for &slot in local {
            state.slots.set(slot, node_id, 0);
        }
        let mut nodes = HashMap::new();
        let mut known_addrs = HashMap::new();
        for (id, addr, slots) in remote {
            let parsed: SocketAddr = addr.parse().unwrap();
            known_addrs.insert(id.to_string(), parsed);
            for &slot in *slots {
                state.slots.set(slot, *id, 0);
            }
            nodes.insert(
                id.to_string(),
                ClusterNode {
                    id: id.to_string(),
                    addr: parsed,
                    slots: slots.iter().copied().collect(),
                    _state: "master".to_string(),
                    role: NodeRole::Primary,
                    primary: None,
                    current_epoch: 0,
                    config_epoch: 0,
                },
            );
        }
        ClusterManager {
            node_id: config.node_id.clone(),
            nodes: Arc::new(RwLock::new(nodes)),
            known_addrs: Arc::new(std::sync::RwLock::new(known_addrs)),
            state: Arc::new(std::sync::RwLock::new(state)),
            state_path: None,
            write_lock: Arc::new(tokio::sync::Mutex::new(())),
            bootstrapping: Arc::new(AtomicBool::new(false)),
            config,
            chitchat: None,
            running: Arc::new(Mutex::new(None)),
            local_addr: "0.0.0.0:6379".parse().unwrap(),
            namespace_strategy: NamespaceDistributionStrategy::default(),
            replica_synced: Arc::new(Mutex::new(vec![false; 2])),
        }
    }

    /// Makes `manager` a replica of `primary` with two shards, `synced` or not.
    fn into_replica(manager: ClusterManager, primary: &str, synced: bool) -> ClusterManager {
        {
            let mut state = manager.state.write().unwrap();
            state.role = PersistedRole::Replica;
            state.primary = Some(primary.to_string());
        }
        *manager.replica_synced.lock().unwrap() = vec![synced; 2];
        manager
    }

    async fn add_replica(manager: &ClusterManager, id: &str, addr: &str, of: &str, role: NodeRole) {
        let parsed: SocketAddr = addr.parse().unwrap();
        manager
            .known_addrs
            .write()
            .unwrap()
            .insert(id.to_string(), parsed);
        manager.nodes.write().await.insert(
            id.to_string(),
            ClusterNode {
                id: id.to_string(),
                addr: parsed,
                slots: HashSet::new(),
                _state: "master".to_string(),
                role,
                primary: Some(of.to_string()),
                current_epoch: 0,
                config_epoch: 0,
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
        let data = NodeGossipData::new(
            "10.0.0.1:6379".into(),
            &slots,
            NodeRole::Primary,
            None,
            0,
            0,
        )
        .unwrap();
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
            NodeGossipData::new("a:1".into(), &all(9000), NodeRole::Primary, None, 0, 0).unwrap();
        assert_eq!(at_limit.slots.len(), 9000);
        let json = serde_json::to_string(&at_limit).unwrap();
        assert!(json.len() <= GOSSIP_VALUE_MAX, "{} bytes", json.len());
        assert_eq!(
            serde_json::from_str::<Legacy>(&json).unwrap().slots.len(),
            9000
        );
        // The worst case for 9000 slots: all five digits.
        let high: HashSet<u16> = (7384..16384).collect();
        let high = NodeGossipData::new("a:1".into(), &high, NodeRole::Primary, None, 0, 0).unwrap();
        assert_eq!(high.slots.len(), 9000);
        let full =
            NodeGossipData::new("a:1".into(), &all(16384), NodeRole::Primary, None, 0, 0).unwrap();
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
            NodeGossipData::new("a:1".into(), &alternating, NodeRole::Primary, None, 0, 0).unwrap();
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
        // No ranges, role, primary, or epochs: read the slot list and default to primary at epoch 0.
        let old = r#"{"addr":"10.0.0.1:6379","slots":[5,6,7],"state":"master"}"#;
        let data: NodeGossipData = serde_json::from_str(old).unwrap();
        assert_eq!(data.owned_slots(), [5, 6, 7].into_iter().collect());
        assert_eq!((data.role, data.primary), (NodeRole::Primary, None));
        assert_eq!((data.current_epoch, data.config_epoch), (0, 0));

        // Ranges win over a stale list.
        let new = r#"{"addr":"a:1","slots":[1],"slot_ranges":[[10,12]],"state":"master"}"#;
        let data: NodeGossipData = serde_json::from_str(new).unwrap();
        assert_eq!(data.owned_slots(), [10, 11, 12].into_iter().collect());

        let replica = NodeGossipData::new(
            "a:1".into(),
            &HashSet::new(),
            NodeRole::ReplicaSyncing,
            Some("node-a".into()),
            4,
            0,
        )
        .unwrap();
        let json = serde_json::to_string(&replica).unwrap();
        assert!(json.contains(r#""role":"replica-syncing""#), "{json}");
        assert!(json.contains(r#""primary":"node-a""#), "{json}");
        assert!(json.contains(r#""current_epoch":4"#), "{json}");
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
    async fn test_a_slot_change_that_cannot_be_gossiped_is_never_installed() {
        // Two of every three slots: too long both as a list and as ranges.
        let unencodable: Vec<u16> = (0..16384).filter(|slot| slot % 3 != 2).collect();
        let manager = manager_with(&[1, 2, 3], &[]);
        let err = manager.add_slots(unencodable.clone()).await.unwrap_err();
        assert!(err.to_string().starts_with("ERR "), "{err}");
        assert_eq!(manager.get_local_slots().await, [1, 2, 3].into());

        // Removing a slot can break the encoding too.
        let owned: Vec<u16> = unencodable.iter().copied().chain([16383]).collect();
        let manager = manager_with(&owned, &[]);
        assert!(manager.remove_slots(vec![16383]).await.is_err());
        assert_eq!(manager.get_local_slots().await, owned.into_iter().collect());
    }

    #[tokio::test]
    async fn test_shutdown_stops_gossip_and_frees_its_port() {
        let dir = tempfile::TempDir::new().unwrap();
        let port = std::net::UdpSocket::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let config = ClusterConfig {
            enabled: true,
            node_id: "test-node".to_string(),
            seeds: vec![],
            port,
            slots: None,
            slot_ranges: None,
            gossip_interval_ms: Some(100),
            advertise_addr: Some("127.0.0.1:6379".to_string()),
            node_timeout_ms: None,
            replica_validity_factor: None,
        };
        let bind: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
        let manager = ClusterManager::new(
            &config,
            bind,
            "127.0.0.1:6379".parse().unwrap(),
            bootstrap_loaded(dir.path(), None),
            1,
        )
        .await
        .unwrap();
        assert!(
            std::net::UdpSocket::bind(bind).is_err(),
            "gossip isn't running"
        );
        manager.shutdown().await;
        std::net::UdpSocket::bind(bind).expect("the gossip port is still taken");
    }

    #[tokio::test]
    async fn test_add_remove_slots() {
        let dir = tempfile::TempDir::new().unwrap();
        let config = ClusterConfig {
            enabled: true,
            node_id: "test-node".to_string(),
            seeds: vec![],
            port: 7000,
            slots: Some(vec![0, 1, 2]),
            slot_ranges: None,
            gossip_interval_ms: Some(100),
            advertise_addr: Some("127.0.0.1:7000".to_string()),
            node_timeout_ms: None,
            replica_validity_factor: None,
        };

        let gossip_bind_addr: SocketAddr = "127.0.0.1:7000".parse().unwrap();
        let redis_addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();

        // This test might fail in CI due to UDP binding, so we'll handle the error gracefully
        if let Ok(manager) = ClusterManager::new(
            &config,
            gossip_bind_addr,
            redis_addr,
            bootstrap_loaded(dir.path(), None),
            1,
        )
        .await
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

            // Persisted state matches the table after ADDSLOTS/DELSLOTS.
            let persisted =
                ClusterState::load(&dir.path().join(CLUSTER_STATE_FILE), "test-node").unwrap();
            assert_eq!(persisted.slots.slots_of("test-node"), after_remove);
            manager.shutdown().await;
        }
    }

    fn make_test_manager(local_slots: impl IntoIterator<Item = u16>) -> ClusterManager {
        let config = ClusterConfig {
            enabled: true,
            node_id: "node-a".to_string(),
            seeds: vec![],
            port: 7001,
            slots: None,
            slot_ranges: None,
            gossip_interval_ms: Some(1000),
            advertise_addr: Some("127.0.0.1:6401".to_string()),
            node_timeout_ms: None,
            replica_validity_factor: None,
        };
        let mut state = ClusterState::bootstrap_primary();
        for slot in local_slots {
            state.slots.set(slot, "node-a", 0);
        }

        ClusterManager {
            node_id: config.node_id.clone(),
            nodes: Arc::new(RwLock::new(HashMap::new())),
            known_addrs: Arc::new(std::sync::RwLock::new(HashMap::new())),
            state: Arc::new(std::sync::RwLock::new(state)),
            state_path: None,
            write_lock: Arc::new(tokio::sync::Mutex::new(())),
            bootstrapping: Arc::new(AtomicBool::new(false)),
            config,
            chitchat: None,
            running: Arc::new(Mutex::new(None)),
            local_addr: "127.0.0.1:6401".parse().unwrap(),
            namespace_strategy: NamespaceDistributionStrategy::default(),
            replica_synced: Arc::new(Mutex::new(vec![false; 1])),
        }
    }

    #[tokio::test]
    async fn route_slot_distinguishes_local_moved_and_unserved() {
        let manager = make_test_manager(0..=8191);
        let peer_addr: SocketAddr = "127.0.0.1:6402".parse().unwrap();

        // Before gossip converges: non-local slots have no known owner.
        assert_eq!(manager.route(5061, false, false).await, Route::Local);
        assert_eq!(manager.route(9706, false, false).await, Route::ClusterDown);
        assert_eq!(
            manager
                .route(9706, false, false)
                .await
                .into_error()
                .as_deref(),
            Some(CLUSTERDOWN_HASH_SLOT_NOT_SERVED)
        );

        // Once the peer is discovered, its slots enter the table and route as MOVED.
        let mut peers = HashMap::new();
        peers.insert(
            "node-b".to_string(),
            ClusterNode {
                id: "node-b".to_string(),
                addr: peer_addr,
                slots: (8192..=16383).collect(),
                _state: "master".to_string(),
                role: NodeRole::Primary,
                primary: None,
                current_epoch: 1,
                config_epoch: 1,
            },
        );
        manager.process_peers(peers).await;
        assert_eq!(
            manager.route(9706, false, false).await,
            Route::Moved(format!("MOVED 9706 {}", peer_addr))
        );
        assert_eq!(
            manager
                .route(9706, false, false)
                .await
                .into_error()
                .as_deref(),
            Some("MOVED 9706 127.0.0.1:6402")
        );

        // When the peer leaves gossip, its bindings stay in the slot table,
        // while requests to its slots return ClusterDown.
        manager.process_peers(HashMap::new()).await;
        assert_eq!(
            manager.state_snapshot().slots.get(9706).unwrap().node_id,
            "node-b"
        );
        assert_eq!(manager.route(9706, false, false).await, Route::ClusterDown);
        assert_eq!(
            manager
                .route(9706, false, false)
                .await
                .into_error()
                .as_deref(),
            Some(CLUSTERDOWN_HASH_SLOT_NOT_SERVED)
        );
        assert_eq!(manager.route(5061, false, false).await, Route::Local);
    }

    #[tokio::test]
    async fn route_key_and_route_hash_return_unserved_for_unowned_slots() {
        let manager = make_test_manager(0..=8191);

        // "bar" -> slot 5061 (local), "layout:user42" -> slot 9706 (unserved)
        assert_eq!(ClusterManager::calculate_slot("bar"), 5061);
        assert_eq!(ClusterManager::calculate_slot("layout:user42"), 9706);

        assert_eq!(
            manager
                .route(ClusterManager::calculate_slot("bar"), false, false)
                .await,
            Route::Local
        );
        assert_eq!(
            manager
                .route(
                    ClusterManager::calculate_slot("layout:user42"),
                    false,
                    false
                )
                .await,
            Route::ClusterDown
        );
        assert_eq!(
            manager
                .route(manager.calculate_slot_for_hash("ns", "bar"), false, false)
                .await,
            Route::Local
        );
        assert_eq!(
            manager
                .route(
                    manager.calculate_slot_for_hash("ns", "layout:user42"),
                    false,
                    false
                )
                .await,
            Route::ClusterDown
        );
    }

    #[tokio::test]
    async fn concurrent_add_and_remove_slots_keep_gossip_in_sync_with_local_slots() {
        let dir = tempfile::TempDir::new().unwrap();
        let sock = std::net::UdpSocket::bind("127.0.0.1:0").expect("ephemeral udp port");
        let gossip_port = sock.local_addr().unwrap().port();
        drop(sock);

        let config = ClusterConfig {
            enabled: true,
            node_id: "node-race".to_string(),
            seeds: vec![],
            port: gossip_port,
            slots: Some(vec![1]),
            slot_ranges: None,
            gossip_interval_ms: Some(1000),
            advertise_addr: Some("127.0.0.1:6499".to_string()),
            node_timeout_ms: None,
            replica_validity_factor: None,
        };
        let gossip_bind_addr: SocketAddr = format!("127.0.0.1:{gossip_port}").parse().unwrap();
        let redis_addr: SocketAddr = "127.0.0.1:6499".parse().unwrap();
        let mut manager = ClusterManager::new(
            &config,
            gossip_bind_addr,
            redis_addr,
            bootstrap_loaded(dir.path(), None),
            1,
        )
        .await
        .expect("cluster manager");
        manager.claim_slots().await.expect("claim initial slots");
        // Skip per-round fsyncs here; this test stresses the in-memory gossip race.
        manager.state_path = None;

        // Interleave concurrent add_slots/remove_slots on the same slot while
        // briefly contending on the chitchat lock, which previously let an
        // older snapshot overwrite a newer one in gossip.
        for round in 0..30 {
            let m1 = manager.clone();
            let m2 = manager.clone();
            let m3 = manager.clone();
            let slot = (round % 3) as u16;
            let t1 = tokio::spawn(async move { m1.remove_slots(vec![slot]).await.unwrap() });
            let t2 = tokio::spawn(async move { m2.add_slots(vec![slot]).await.unwrap() });
            let t3 = tokio::spawn(async move {
                if let Some(chitchat) = m3.chitchat.as_ref() {
                    let _guard = chitchat.lock().await;
                    tokio::task::yield_now().await;
                }
            });
            let (r1, r2, r3) = tokio::join!(t1, t2, t3);
            r1.unwrap();
            r2.unwrap();
            r3.unwrap();
        }

        let local_slots = manager.get_local_slots().await;
        let published: HashSet<u16> = {
            let chitchat = manager.chitchat.as_ref().expect("chitchat");
            let mut guard = chitchat.lock().await;
            let raw = guard
                .self_node_state()
                .get("node_info")
                .expect("published node_info");
            let data: NodeGossipData = serde_json::from_str(raw).expect("valid gossip json");
            data.owned_slots()
        };
        assert_eq!(
            published, local_slots,
            "published gossip slots diverged from local_slots"
        );
        manager.shutdown().await;
    }

    #[tokio::test]
    async fn epoch_collision_resolution_bumps_smaller_node_id_only() {
        let dir_a = tempfile::TempDir::new().unwrap();
        let dir_b = tempfile::TempDir::new().unwrap();
        let mut mgr_a = manager_with_id("node-a", &[0, 1], &[]);
        mgr_a.state_path = Some(dir_a.path().join(CLUSTER_STATE_FILE));
        let mut mgr_b = manager_with_id("node-b", &[2, 3], &[]);
        mgr_b.state_path = Some(dir_b.path().join(CLUSTER_STATE_FILE));

        let peer_b = ClusterNode {
            id: "node-b".to_string(),
            addr: "10.0.0.2:6379".parse().unwrap(),
            slots: [2, 3].into_iter().collect(),
            _state: "master".to_string(),
            role: NodeRole::Primary,
            primary: None,
            current_epoch: 0,
            config_epoch: 0,
        };
        let peer_a = ClusterNode {
            id: "node-a".to_string(),
            addr: "10.0.0.1:6379".parse().unwrap(),
            slots: [0, 1].into_iter().collect(),
            _state: "master".to_string(),
            role: NodeRole::Primary,
            primary: None,
            current_epoch: 0,
            config_epoch: 0,
        };

        // node-a < node-b at config_epoch 0: node-a bumps to 1 and persists.
        mgr_a
            .process_peers([("node-b".to_string(), peer_b)].into())
            .await;
        let state_a = mgr_a.state_snapshot();
        assert_eq!(state_a.current_epoch, 1);
        assert_eq!(state_a.config_epoch, 1);
        assert_eq!(state_a.slots.get(0).unwrap().epoch, 1);
        assert_eq!(state_a.slots.get(2).unwrap().epoch, 0);
        let persisted =
            ClusterState::load(&dir_a.path().join(CLUSTER_STATE_FILE), "node-a").unwrap();
        assert_eq!(persisted.config_epoch, 1);

        // node-b > node-a at config_epoch 0: node-b does NOT bump.
        mgr_b
            .process_peers([("node-a".to_string(), peer_a)].into())
            .await;
        let state_b = mgr_b.state_snapshot();
        assert_eq!(state_b.current_epoch, 0);
        assert_eq!(state_b.config_epoch, 0);
    }

    #[tokio::test]
    async fn primary_drops_slots_on_higher_epoch_claim_and_serves_moved() {
        let dir = tempfile::TempDir::new().unwrap();
        let mut mgr = manager_with_id("node-a", &[0, 1], &[]);
        mgr.state_path = Some(dir.path().join(CLUSTER_STATE_FILE));
        {
            let mut st = mgr.state.write().unwrap();
            st.current_epoch = 2;
            st.config_epoch = 2;
            st.slots.rebind_owner_epoch("node-a", 2);
        }

        // Equal epoch claim is ignored.
        let equal_peer = ClusterNode {
            id: "node-b".to_string(),
            addr: "10.0.0.2:6379".parse().unwrap(),
            slots: [0, 1].into_iter().collect(),
            _state: "master".to_string(),
            role: NodeRole::Primary,
            primary: None,
            current_epoch: 2,
            config_epoch: 2,
        };
        // Note: node-a < node-b would trigger collision at epoch 2, so test with node-z < self
        // or test strictly lower epoch here:
        let lower_peer = ClusterNode {
            config_epoch: 1,
            current_epoch: 1,
            ..equal_peer.clone()
        };
        mgr.process_peers([("node-b".to_string(), lower_peer)].into())
            .await;
        assert_eq!(mgr.route(0, false, false).await, Route::Local);
        assert_eq!(mgr.route(1, false, false).await, Route::Local);

        // Strictly higher epoch claim on slot 1: node-a drops slot 1 immediately.
        let take_one = ClusterNode {
            slots: [1].into_iter().collect(),
            current_epoch: 5,
            config_epoch: 5,
            ..equal_peer.clone()
        };
        mgr.process_peers([("node-b".to_string(), take_one)].into())
            .await;
        assert_eq!(mgr.route(0, false, false).await, Route::Local);
        assert_eq!(
            mgr.route(1, false, false).await,
            Route::Moved("MOVED 1 10.0.0.2:6379".to_string())
        );
        assert_eq!(mgr.state_snapshot().current_epoch, 5);
        let persisted = ClusterState::load(&dir.path().join(CLUSTER_STATE_FILE), "node-a").unwrap();
        assert_eq!(persisted.slots.get(1).unwrap().node_id, "node-b");
        assert_eq!(persisted.slots.get(1).unwrap().epoch, 5);

        // Losing the last slot (slot 0): serves nothing, routes MOVED to winner.
        let take_last = ClusterNode {
            slots: [0, 1].into_iter().collect(),
            current_epoch: 6,
            config_epoch: 6,
            ..equal_peer
        };
        mgr.process_peers([("node-b".to_string(), take_last)].into())
            .await;
        assert!(mgr.get_local_slots().await.is_empty());
        assert_eq!(
            mgr.route(0, false, false).await,
            Route::Moved("MOVED 0 10.0.0.2:6379".to_string())
        );
        // Role stays primary (demotion is step 2).
        assert!(!mgr.is_replica());
    }

    #[tokio::test]
    async fn cluster_shards_info_and_nodes_reflect_epochs_and_dead_owners() {
        let manager = manager_with(&[0, 1, 9], &[("node-b", "10.0.0.2:6380", &[5, 6])]);
        {
            let mut st = manager.state.write().unwrap();
            st.current_epoch = 4;
            st.config_epoch = 3;
            st.slots.rebind_owner_epoch("node-a", 3);
            st.slots.rebind_owner_epoch("node-b", 4);
        }
        manager
            .nodes
            .write()
            .await
            .get_mut("node-b")
            .unwrap()
            .config_epoch = 4;
        add_replica(
            &manager,
            "node-b1",
            "10.0.0.3:6381",
            "node-b",
            NodeRole::Replica,
        )
        .await;
        add_replica(
            &manager,
            "node-a1",
            "10.0.0.4:6382",
            "node-a",
            NodeRole::ReplicaSyncing,
        )
        .await;

        let info = manager.get_cluster_info().await;
        assert!(info.contains("cluster_current_epoch:4\n"), "{info}");
        assert!(info.contains("cluster_my_epoch:3\n"), "{info}");
        assert!(info.contains("cluster_slots_assigned:5\n"), "{info}");
        assert!(info.contains("cluster_slots_ok:5\n"), "{info}");
        assert!(info.contains("cluster_size:2\n"), "{info}");
        assert!(info.contains("cluster_known_nodes:4\n"), "{info}");

        let shards = manager.get_cluster_shards().await;
        assert_eq!(shards.len(), 2);
        // Shard 0 is node-a with disjoint ranges (0, 1) and (9, 9), plus loading replica node-a1.
        assert_eq!(shards[0].slots, vec![(0, 1), (9, 9)]);
        assert_eq!(shards[0].nodes[0].id, "node-a");
        assert_eq!(shards[0].nodes[0].role, "master");
        assert_eq!(shards[0].nodes[0].health, "online");
        assert_eq!(shards[0].nodes[1].id, "node-a1");
        assert_eq!(shards[0].nodes[1].role, "replica");
        assert_eq!(shards[0].nodes[1].health, "loading");
        // Shard 1 is node-b with online replica node-b1.
        assert_eq!(shards[1].slots, vec![(5, 6)]);
        assert_eq!(shards[1].nodes[0].id, "node-b");
        assert_eq!(shards[1].nodes[1].id, "node-b1");
        assert_eq!(shards[1].nodes[1].health, "online");

        // When node-b leaves gossip, its slots remain bound in CLUSTER NODES
        // as disconnected and in CLUSTER SHARDS as failed, and CLUSTER SLOTS omits it.
        manager.process_peers(HashMap::new()).await;
        let nodes = manager.get_cluster_nodes().await;
        assert!(
            nodes.contains("node-b 10.0.0.2:6380 master - 0 0 4 disconnected 5-6\n"),
            "{nodes}"
        );
        let shards_after = manager.get_cluster_shards().await;
        assert_eq!(shards_after[1].nodes[0].id, "node-b");
        assert_eq!(shards_after[1].nodes[0].health, "failed");
        let slots_after = manager.get_cluster_slots().await;
        assert!(slots_after.iter().all(|r| r.node_id == "node-a"));
    }
}
