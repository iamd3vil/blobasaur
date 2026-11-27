mod election;
mod failure;
mod state;

pub use election::{
    CLUSTER_MF_PAUSE_MULT, CLUSTER_MF_TIMEOUT, ElectionPlan, ElectionStats, FailoverAuthRequest,
    PromotionKind, ShardReplState, VoteCollectionOutcome, collect_votes, election_delay,
    election_timeout, parse_slot_ranges, random_jitter_ms, retry_backoff, send_mfabort,
    send_mfstart, vote_backoff,
};
use failure::{FailureTracker, PeerHeartbeatView};
#[allow(unused_imports)]
pub use state::{
    CLUSTER_STATE_FILE, ClusterState, LoadedClusterState, MergeOutcome, PersistedRole, SlotBinding,
    SlotRangeBinding, SlotTable, configured_slots, is_cluster_state_file, load_or_bootstrap,
};

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Once};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use chitchat::{
    Chitchat, ChitchatConfig, ChitchatHandle, ChitchatId, FailureDetectorConfig, spawn_chitchat,
};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
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
    pub shard_repl: Vec<ShardReplState>,
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

/// Snapshot of a peer from `chitchat.node_states()` (newest generation carrying
/// `node_info`), including its raw chitchat heartbeat and failure reports.
#[derive(Debug, Clone)]
struct PeerSnapshot {
    node: ClusterNode,
    heartbeat_view: PeerHeartbeatView,
}

/// Last known metadata for a peer seen in gossip, kept for `CLUSTER NODES` and
/// `CLUSTER SHARDS` while the peer is disconnected or evicted.
#[derive(Debug, Clone)]
struct KnownPeer {
    addr: SocketAddr,
    role: NodeRole,
    primary: Option<String>,
    config_epoch: u64,
    shard_repl: Vec<ShardReplState>,
}

/// Reads the newest generation carrying `node_info` for every peer in chitchat's
/// state table, regardless of chitchat's own phi failure detector.
fn peer_snapshots(chitchat: &Chitchat, self_id: &str) -> HashMap<String, PeerSnapshot> {
    let ids_with_info = chitchat
        .node_states()
        .iter()
        .filter(|(id, state)| id.node_id != self_id && state.get("node_info").is_some())
        .map(|(id, _)| id);

    let mut snapshots = HashMap::new();
    for chitchat_id in newest_generations(ids_with_info) {
        let Some(node_state) = chitchat.node_state(chitchat_id) else {
            continue;
        };
        let Some(node_info_value) = node_state.get("node_info") else {
            continue;
        };
        match serde_json::from_str::<NodeGossipData>(node_info_value) {
            Ok(gossip_data) => {
                if let Ok(addr) = gossip_data.addr.parse::<SocketAddr>() {
                    let slots = gossip_data.owned_slots();
                    let cluster_node = ClusterNode {
                        id: chitchat_id.node_id.clone(),
                        addr,
                        slots,
                        _state: gossip_data.state,
                        role: gossip_data.role,
                        primary: gossip_data.primary,
                        current_epoch: gossip_data.current_epoch,
                        config_epoch: gossip_data.config_epoch,
                        shard_repl: gossip_data.shard_repl,
                    };
                    let heartbeat_view = PeerHeartbeatView {
                        generation_id: chitchat_id.generation_id,
                        heartbeat: u64::from(node_state.heartbeat()),
                        pfail: gossip_data.pfail,
                        fail: gossip_data.fail,
                    };
                    snapshots.insert(
                        chitchat_id.node_id.clone(),
                        PeerSnapshot {
                            node: cluster_node,
                            heartbeat_view,
                        },
                    );
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
    }
    snapshots
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
                            shard_repl: gossip_data.shard_repl,
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
    /// Node ids this node currently cannot reach (PFAIL).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pfail: Vec<String>,
    /// Node ids this node has marked failed (FAIL).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fail: Vec<String>,
    /// Per-shard replication replid and `caught_up_lsn` for election ranking.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub shard_repl: Vec<ShardReplState>,
}

impl NodeGossipData {
    /// Encodes `slots` as both a list and ranges if that fits `GOSSIP_VALUE_MAX`,
    /// else as whichever one alone fits, preferring the list old nodes can read.
    /// Errors if neither fits: peers would never receive the value.
    #[allow(clippy::too_many_arguments)]
    fn new(
        addr: String,
        slots: &HashSet<u16>,
        role: NodeRole,
        primary: Option<String>,
        current_epoch: u64,
        config_epoch: u64,
        pfail: Vec<String>,
        fail: Vec<String>,
        shard_repl: Vec<ShardReplState>,
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
            pfail,
            fail,
            shard_repl,
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

/// Runtime role transition driven level-triggered from persisted cluster state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoleTransition {
    /// Primary has `demoting_to` set and must finish demoting to a replica.
    Demote,
    /// Replica has `repointing_to` set and must finish re-pointing to the new primary.
    Repoint,
}

/// Non-blocking callback invoked by gossip (and on startup) to spawn a role transition.
pub type RoleTransitionHandler = Arc<dyn Fn(RoleTransition) + Send + Sync>;

/// Failure from [`ClusterManager::promote_takeover`], distinguishing whether
/// `cluster-state.json` already committed `role = primary`.
#[derive(Debug)]
pub enum PromoteError {
    /// Failed before persisting `cluster-state.json`; the node is still a replica on disk.
    BeforeCommit(String),
    /// Persisted `cluster-state.json` as primary, then the per-shard promotion failed.
    AfterCommit(String),
}

/// Pre-check result for `CLUSTER REPLICATE <node-id>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicateCheck {
    AlreadyFollowing,
    FromEmptyPrimary,
    SwitchReplica,
}

/// The gossip server and the task reading its state.
type Gossip = (ChitchatHandle, Option<JoinHandle<()>>);

/// Manages cluster state and gossip protocol
pub struct ClusterManager {
    node_id: String,
    /// Reachable peers from gossip (present in chitchat and not PFAIL).
    nodes: Arc<RwLock<HashMap<String, ClusterNode>>>,
    /// Last known metadata of any peer seen in gossip (retained while disconnected/evicted).
    known_peers: Arc<std::sync::RwLock<HashMap<String, KnownPeer>>>,
    /// Heartbeat-based PFAIL/FAIL detector and minority tracker.
    failure: Arc<std::sync::RwLock<FailureTracker>>,
    /// Persisted cluster state (role, primary, epochs, slot table).
    state: Arc<std::sync::RwLock<ClusterState>>,
    /// Path to `<data_dir>/cluster-state.json`; `None` only in tests without persistence.
    state_path: Option<PathBuf>,
    /// Serializes state mutations and disk persistence so writes never reorder.
    write_lock: Arc<tokio::sync::Mutex<()>>,
    /// True during first-boot bootstrap until `claim_slots` finishes and persists.
    bootstrapping: Arc<AtomicBool>,
    /// Set if a post-commit TAKEOVER failure halts further state writes while
    /// the node shuts down for startup recovery.
    persist_halted: Arc<AtomicBool>,
    config: ClusterConfig,
    /// Gossip state; `None` only in tests.
    chitchat: Option<Arc<tokio::sync::Mutex<Chitchat>>>,
    /// What [`Self::shutdown`] stops.
    running: Arc<Mutex<Option<Gossip>>>,
    /// Cancels `gossip_task` cooperatively between ticks on shutdown.
    gossip_shutdown: CancellationToken,
    /// Non-blocking handler for demotion and replica re-pointing; attached once writers start.
    role_handler: Arc<Mutex<Option<RoleTransitionHandler>>>,
    local_addr: SocketAddr,
    namespace_strategy: NamespaceDistributionStrategy,
    /// Per-shard synced flags for a replica; role is `Replica` once all are true.
    replica_synced: Arc<Mutex<Vec<bool>>>,
    /// Per-shard replication replid and `caught_up_lsn` gossiped for ranking.
    shard_repl: Arc<Mutex<Vec<ShardReplState>>>,
    /// Set when `shard_repl` changes so the next gossip tick republishes it.
    shard_repl_dirty: Arc<AtomicBool>,
    /// Monotonic time each shard last received a frame from its primary.
    last_frame_at: Arc<Mutex<Vec<Option<Instant>>>>,
    /// Monotonic start time of each active per-primary vote backoff (`last_vote_for`).
    vote_backoff_at: Arc<Mutex<HashMap<String, Instant>>>,
    /// Lightweight election and voting counters for `CLUSTER INFO`.
    election_stats: Arc<ElectionStats>,
    /// Wakes the automatic election loop on topology/FAIL/sync updates.
    election_notify: Arc<tokio::sync::Notify>,
    /// Wakes manual failover catch-up waiters on replica `caught_up_lsn` or role updates.
    repl_progress_notify: Arc<tokio::sync::Notify>,
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
        let known_peers = Arc::new(std::sync::RwLock::new(HashMap::new()));
        let gossip_interval = Duration::from_millis(config.gossip_interval_ms.unwrap_or(1000));
        let node_timeout = Duration::from_millis(config.node_timeout_ms());
        let init_now = Instant::now();
        let failure = Arc::new(std::sync::RwLock::new(FailureTracker::new(
            config.node_id.clone(),
            node_timeout,
            init_now,
        )));
        let replica_synced = Arc::new(Mutex::new(vec![false; num_shards]));
        let shard_repl = Arc::new(Mutex::new(vec![ShardReplState::default(); num_shards]));
        let last_frame_at = Arc::new(Mutex::new(vec![None; num_shards]));
        let mut vote_backoff_map = HashMap::new();
        for (failed_primary, &epoch) in &initial_state.last_vote_for {
            if epoch > 0 {
                vote_backoff_map.insert(failed_primary.clone(), init_now);
            }
        }
        let vote_backoff_at = Arc::new(Mutex::new(vote_backoff_map));

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
            initial_interval: gossip_interval.max(Duration::from_millis(50)),
            max_interval: Duration::from_secs(10),
            dead_node_grace_period: Duration::from_secs(60).max(node_timeout.saturating_mul(4)),
            sampling_window_size: 100,
        };

        let chitchat_config = ChitchatConfig {
            chitchat_id: chitchat_id.clone(),
            cluster_id: "blobasaur-cluster".to_string(),
            gossip_interval,
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
            Vec::new(),
            Vec::new(),
            Vec::new(),
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
            known_peers,
            failure,
            state: Arc::new(std::sync::RwLock::new(initial_state)),
            state_path: Some(state_path),
            write_lock: Arc::new(tokio::sync::Mutex::new(())),
            bootstrapping: Arc::new(AtomicBool::new(!from_file)),
            persist_halted: Arc::new(AtomicBool::new(false)),
            config: config.clone(),
            chitchat: Some(chitchat_handle.chitchat()),
            running: Arc::new(Mutex::new(None)),
            gossip_shutdown: CancellationToken::new(),
            role_handler: Arc::new(Mutex::new(None)),
            local_addr: redis_addr,
            namespace_strategy: NamespaceDistributionStrategy::default(),
            replica_synced,
            shard_repl,
            shard_repl_dirty: Arc::new(AtomicBool::new(false)),
            last_frame_at,
            vote_backoff_at,
            election_stats: Arc::new(ElectionStats::default()),
            election_notify: Arc::new(tokio::sync::Notify::new()),
            repl_progress_notify: Arc::new(tokio::sync::Notify::new()),
        };

        // Start gossip processing task
        let gossip_manager = cluster_manager.clone();
        let gossip_task = tokio::spawn(async move {
            gossip_manager.gossip_task().await;
        });
        *cluster_manager.running.lock().unwrap() = Some((chitchat_handle, Some(gossip_task)));

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

    /// Whether `node_id` is currently marked FAIL in this node's failure view.
    pub fn is_failed(&self, node_id: &str) -> bool {
        self.failure.read().unwrap().is_failed(node_id)
    }

    /// Whether `node_id` is currently suspected unreachable (PFAIL) locally.
    #[allow(dead_code)]
    pub fn is_pfail(&self, node_id: &str) -> bool {
        self.failure.read().unwrap().is_pfail(node_id)
    }

    /// Whether this node is fenced in a minority (cannot reach a majority of
    /// slot-owning primaries for `node_timeout_ms`).
    pub fn is_in_minority(&self) -> bool {
        self.failure.read().unwrap().is_in_minority()
    }

    /// Configured cluster node timeout (`node_timeout_ms`).
    pub fn node_timeout(&self) -> Duration {
        Duration::from_millis(self.config.node_timeout_ms())
    }

    /// Notification woken whenever gossip/FAIL/sync state changes.
    pub fn election_notify(&self) -> Arc<tokio::sync::Notify> {
        Arc::clone(&self.election_notify)
    }

    /// Notification woken whenever a replica shard's `caught_up_lsn`, sync state,
    /// or gossip topology changes.
    pub fn repl_progress_notify(&self) -> Arc<tokio::sync::Notify> {
        Arc::clone(&self.repl_progress_notify)
    }

    /// Whether this node is currently a non-demoting primary owning `slot`.
    pub fn owns_slot(&self, slot: u16) -> bool {
        let state = self.state.read().unwrap();
        state.role == PersistedRole::Primary
            && state.demoting_to.is_none()
            && state
                .slots
                .get(slot)
                .is_some_and(|binding| binding.node_id == self.node_id)
    }

    /// Last known Redis address of `node_id` from live gossip or retained peers.
    pub async fn peer_addr(&self, node_id: &str) -> Option<SocketAddr> {
        if let Some(addr) = self.nodes.read().await.get(node_id).map(|n| n.addr) {
            return Some(addr);
        }
        self.known_peers
            .read()
            .unwrap()
            .get(node_id)
            .map(|p| p.addr)
    }

    /// Validate that this primary can accept `BLOBASAUR.MFSTART <replica_id>`.
    pub async fn validate_mfstart(&self, replica_id: &str) -> Result<(), String> {
        if self.persist_halted.load(Ordering::Acquire) {
            return Err("ERR Node is shutting down".to_string());
        }
        if self.bootstrapping.load(Ordering::Acquire) {
            return Err("ERR Node is still bootstrapping".to_string());
        }
        {
            let state = self.state.read().unwrap();
            if state.role != PersistedRole::Primary {
                return Err("ERR Node is not a primary".to_string());
            }
            if state.demoting_to.is_some() {
                return Err("ERR Primary is demoting".to_string());
            }
            if !state.slots.has_slots_of(&self.node_id) {
                return Err("ERR Primary owns no slots".to_string());
            }
        }
        let is_known_replica = {
            let nodes = self.nodes.read().await;
            nodes.get(replica_id).is_some_and(|peer| {
                peer.role != NodeRole::Primary
                    && peer.primary.as_deref() == Some(self.node_id.as_str())
            })
        };
        if !is_known_replica {
            return Err(format!(
                "ERR Node {replica_id} is not a known replica of {}",
                self.node_id
            ));
        }
        Ok(())
    }

    /// True when every shard on this replica is synced under the primary's
    /// `replid` and has `caught_up_lsn >= primary_lsn`.
    pub fn replica_caught_up_to(&self, target: &[(String, u64)]) -> bool {
        let synced = self.replica_synced.lock().unwrap();
        let repl = self.shard_repl.lock().unwrap();
        if synced.len() != target.len() || repl.len() != target.len() || target.is_empty() {
            return false;
        }
        for (idx, (target_replid, target_lsn)) in target.iter().enumerate() {
            if !synced[idx]
                || target_replid.is_empty()
                || repl[idx].replid != *target_replid
                || repl[idx].caught_up_lsn < *target_lsn
            {
                return false;
            }
        }
        true
    }

    /// Snapshot of per-shard `(synced, ShardReplState)` for diagnostics.
    pub fn replica_progress_snapshot(&self) -> Vec<(bool, ShardReplState)> {
        let synced = self.replica_synced.lock().unwrap();
        let repl = self.shard_repl.lock().unwrap();
        synced.iter().copied().zip(repl.iter().cloned()).collect()
    }

    /// Records the current `replid` of `shard` for gossiped ranking/history checks.
    pub fn set_shard_replid(&self, shard: usize, replid: &str) {
        let is_replica = self.is_replica();
        let mut guard = self.shard_repl.lock().unwrap();
        if let Some(entry) = guard.get_mut(shard) {
            let mut changed = false;
            if entry.replid != replid {
                entry.replid = replid.to_string();
                changed = true;
            }
            if !is_replica && entry.caught_up_lsn != 0 {
                entry.caught_up_lsn = 0;
                changed = true;
            }
            if changed {
                self.shard_repl_dirty.store(true, Ordering::Release);
            }
        }
    }

    /// Records that `shard` received a replication frame from its primary at `at`.
    pub fn record_replica_frame(&self, shard: usize, at: Instant) {
        if let Some(slot) = self.last_frame_at.lock().unwrap().get_mut(shard) {
            *slot = Some(at);
        }
    }

    /// Replica: records `(replid, caught_up_lsn, synced, last_frame_at)` for `shard`,
    /// gossiping immediately if overall synced status flipped.
    pub async fn update_replica_shard_progress(
        &self,
        shard: usize,
        replid: &str,
        caught_up_lsn: u64,
        synced: bool,
        at: Instant,
    ) {
        if !self.is_replica() {
            return;
        }
        self.record_replica_frame(shard, at);
        {
            let mut guard = self.shard_repl.lock().unwrap();
            if let Some(entry) = guard.get_mut(shard)
                && (entry.replid != replid || entry.caught_up_lsn != caught_up_lsn)
            {
                entry.replid = replid.to_string();
                entry.caught_up_lsn = caught_up_lsn;
                self.shard_repl_dirty.store(true, Ordering::Release);
            }
        }
        self.set_shard_synced(shard, synced).await;
        self.repl_progress_notify.notify_waiters();
    }

    /// Replica: clears frame time and caught-up LSN for `shard` on `RESYNC` reset.
    pub async fn reset_shard_replica_state(&self, shard: usize, new_replid: &str) {
        if let Some(slot) = self.last_frame_at.lock().unwrap().get_mut(shard) {
            *slot = None;
        }
        {
            let mut guard = self.shard_repl.lock().unwrap();
            if let Some(entry) = guard.get_mut(shard) {
                entry.replid = new_replid.to_string();
                entry.caught_up_lsn = 0;
                self.shard_repl_dirty.store(true, Ordering::Release);
            }
        }
        self.set_shard_synced(shard, false).await;
        self.repl_progress_notify.notify_waiters();
    }

    pub fn record_election_won(&self) {
        self.election_stats
            .elections_won
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_election_failed(&self) {
        self.election_stats
            .elections_failed
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_manual_failover_started(&self) {
        self.election_stats
            .manual_failovers_started
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_manual_failover_succeeded(&self) {
        self.election_stats
            .manual_failovers_succeeded
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_manual_failover_aborted(&self) {
        self.election_stats
            .manual_failovers_aborted
            .fetch_add(1, Ordering::Relaxed);
    }

    async fn persist_state(&self, state: &ClusterState) -> Result<(), String> {
        if self.persist_halted.load(Ordering::Acquire) {
            return Err("cluster state persistence halted during shutdown".to_string());
        }
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
        let gossip_ms = self.config.gossip_interval_ms.unwrap_or(1000);
        let tick_ms = gossip_ms.min((self.config.node_timeout_ms() / 2).max(50));
        let mut interval = interval(Duration::from_millis(tick_ms));

        info!("Gossip task started for node: {}", self.node_id);

        loop {
            tokio::select! {
                _ = self.gossip_shutdown.cancelled() => {
                    info!("Gossip task stopped for node: {}", self.node_id);
                    return;
                }
                _ = interval.tick() => {}
            }
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

                let snapshots = peer_snapshots(&chitchat_guard, &self.node_id);

                drop(chitchat_guard);

                self.process_peer_snapshots(snapshots, Instant::now()).await;
            } else {
                warn!(
                    "Gossip task running without chitchat for node: {}",
                    self.node_id
                );
            }
        }
    }

    /// Register the non-blocking runtime handler for demotion and replica
    /// re-pointing, and immediately trigger it if persisted state has a pending
    /// transition (`demoting_to` or `repointing_to`).
    pub fn set_role_handler(&self, handler: RoleTransitionHandler) {
        *self.role_handler.lock().unwrap() = Some(handler.clone());
        let transition = {
            let state = self.state.read().unwrap();
            Self::pending_transition_for(&state)
        };
        if let Some(transition) = transition {
            handler(transition);
        }
    }

    fn pending_transition_for(state: &ClusterState) -> Option<RoleTransition> {
        match state.role {
            PersistedRole::Primary if state.demoting_to.is_some() => Some(RoleTransition::Demote),
            PersistedRole::Replica if state.repointing_to.is_some() => {
                Some(RoleTransition::Repoint)
            }
            _ => None,
        }
    }

    /// Whether this primary has a pending demotion marker (`demoting_to`).
    pub fn is_demoting(&self) -> bool {
        let state = self.state.read().unwrap();
        state.role == PersistedRole::Primary && state.demoting_to.is_some()
    }

    /// Whether this replica has a pending re-point marker (`repointing_to`).
    pub fn is_repointing(&self) -> bool {
        let state = self.state.read().unwrap();
        state.role == PersistedRole::Replica && state.repointing_to.is_some()
    }

    /// Mark every shard as not synced in memory (stops `READONLY` reads and
    /// switches gossip role to `ReplicaSyncing` on next gossip update).
    pub fn reset_replica_synced(&self) {
        for flag in self.replica_synced.lock().unwrap().iter_mut() {
            *flag = false;
        }
        for slot in self.last_frame_at.lock().unwrap().iter_mut() {
            *slot = None;
        }
        for entry in self.shard_repl.lock().unwrap().iter_mut() {
            entry.caught_up_lsn = 0;
        }
        self.shard_repl_dirty.store(true, Ordering::Release);
        self.repl_progress_notify.notify_waiters();
    }

    /// Publish the current state, role, and epochs to gossip.
    pub async fn publish_gossip(&self) -> Result<(), String> {
        self.update_gossip_data().await
    }

    /// Pre-check for promotion (`TAKEOVER` or won election) before stopping sync tasks.
    pub fn can_promote(&self, kind: &PromotionKind) -> Result<(), String> {
        if self.persist_halted.load(Ordering::Acquire) {
            return Err("ERR Node is shutting down after incomplete takeover".to_string());
        }
        let state = self.state.read().unwrap();
        if !state.is_replica() {
            return Err("ERR You should send CLUSTER FAILOVER to a replica".to_string());
        }
        if state.repointing_to.is_some() {
            return Err("ERR Replica is re-pointing to a new primary".to_string());
        }
        let Some(ref primary) = state.primary else {
            return Err("ERR Replica has no configured primary".to_string());
        };
        match kind {
            PromotionKind::Takeover => {
                if !state.slots.has_slots_of(primary) {
                    return Err("ERR Primary owns no slots to take over".to_string());
                }
            }
            PromotionKind::Elected {
                election_epoch,
                failed_primary,
                primary_config_epoch,
                expected_slots,
                deadline,
            } => {
                if Instant::now() >= *deadline {
                    return Err("ERR Election deadline passed before promotion commit".to_string());
                }
                if primary != failed_primary {
                    return Err(format!(
                        "ERR Replica now follows {primary}, no longer {failed_primary}"
                    ));
                }
                let current_slots = state.slots.slots_of(failed_primary);
                if current_slots.is_empty() || current_slots != *expected_slots {
                    return Err("ERR Failed primary's slots changed before promotion".to_string());
                }
                if state.slots.max_epoch_for(failed_primary) != *primary_config_epoch {
                    return Err(
                        "ERR Failed primary's slot epoch changed before promotion".to_string()
                    );
                }
                if *election_epoch <= *primary_config_epoch {
                    return Err("ERR Election epoch is not newer than primary epoch".to_string());
                }
            }
        }
        Ok(())
    }

    /// Pre-check for `CLUSTER FAILOVER TAKEOVER` before stopping sync tasks.
    pub fn can_takeover(&self) -> Result<(), String> {
        self.can_promote(&PromotionKind::Takeover)
    }

    /// Check whether this node is currently eligible to start an automatic
    /// failover election (FAILOVER.md "Election"):
    /// - replica (not repointing, not bootstrapping, not shutting down);
    /// - its primary is FAIL in this node's view and owns slots in its table;
    /// - every shard is synced under the primary's replids;
    /// - every shard received a frame within `node_timeout_ms * replica_validity_factor`
    ///   before the failure (when factor > 0).
    pub async fn check_election_eligibility(&self) -> Option<String> {
        if self.persist_halted.load(Ordering::Acquire) || self.bootstrapping.load(Ordering::Acquire)
        {
            return None;
        }
        let primary_id = {
            let state = self.state.read().unwrap();
            if !state.is_replica() || state.repointing_to.is_some() {
                return None;
            }
            let primary = state.primary.clone()?;
            if !state.slots.has_slots_of(&primary) {
                return None;
            }
            primary
        };
        let failed_at = self.failure.read().unwrap().failed_at(&primary_id)?;
        {
            let synced = self.replica_synced.lock().unwrap();
            if synced.is_empty() || !synced.iter().all(|s| *s) {
                return None;
            }
        }
        let local_repl = self.shard_repl.lock().unwrap().clone();
        if local_repl.is_empty() || local_repl.iter().any(|s| s.replid.is_empty()) {
            return None;
        }
        if let Some(primary_repl) = self.primary_known_repl(&primary_id).await
            && !election::replids_match(&local_repl, &primary_repl)
        {
            return None;
        }
        let validity_factor = self.config.replica_validity_factor();
        if validity_factor > 0 {
            let max_age = Duration::from_millis(
                self.config
                    .node_timeout_ms()
                    .saturating_mul(validity_factor),
            );
            let failure_time = failed_at
                .checked_sub(self.node_timeout())
                .unwrap_or(failed_at);
            let frames = self.last_frame_at.lock().unwrap();
            if frames.len() != local_repl.len() {
                return None;
            }
            for frame_at in frames.iter() {
                let last = (*frame_at)?;
                if failure_time.saturating_duration_since(last) > max_age {
                    return None;
                }
            }
        }
        Some(primary_id)
    }

    async fn primary_known_repl(&self, primary_id: &str) -> Option<Vec<ShardReplState>> {
        let from_live = {
            let nodes = self.nodes.read().await;
            nodes.get(primary_id).map(|n| n.shard_repl.clone())
        };
        from_live
            .filter(|v| !v.is_empty() && v.iter().all(|s| !s.replid.is_empty()))
            .or_else(|| {
                self.known_peers
                    .read()
                    .unwrap()
                    .get(primary_id)
                    .map(|p| p.shard_repl.clone())
                    .filter(|v| !v.is_empty() && v.iter().all(|s| !s.replid.is_empty()))
            })
    }

    /// Compute this replica's rank among live synced replicas of `primary_id`
    /// that share the primary's per-shard replids. Rank 0 = highest sum of
    /// `caught_up_lsn`, ties broken by smaller `node_id`.
    pub async fn compute_replica_rank(&self, primary_id: &str) -> usize {
        let local_repl = self.shard_repl.lock().unwrap().clone();
        let expected_repl = self
            .primary_known_repl(primary_id)
            .await
            .unwrap_or_else(|| local_repl.clone());
        let self_lsn = election::total_caught_up_lsn(&local_repl);
        let nodes = self.nodes.read().await;
        let failure = self.failure.read().unwrap();
        let peers: Vec<(String, u128)> = nodes
            .values()
            .filter(|peer| {
                peer.role == NodeRole::Replica
                    && peer.primary.as_deref() == Some(primary_id)
                    && !failure.is_failed(&peer.id)
                    && election::replids_match(&peer.shard_repl, &expected_repl)
            })
            .map(|peer| {
                (
                    peer.id.clone(),
                    election::total_caught_up_lsn(&peer.shard_repl),
                )
            })
            .collect();
        election::compute_rank(&self.node_id, self_lsn, &peers)
    }

    /// Start an election attempt: under `write_lock`, validate replica state,
    /// increment `current_epoch`, persist atomically BEFORE sending any vote
    /// requests, and return the [`ElectionPlan`].
    pub async fn prepare_election(&self, manual: bool) -> Result<ElectionPlan, String> {
        let _guard = self.write_lock.lock().await;
        if self.persist_halted.load(Ordering::Acquire) {
            return Err("ERR Node is shutting down after incomplete takeover".to_string());
        }
        if self.bootstrapping.load(Ordering::Acquire) {
            return Err("ERR Node is still bootstrapping".to_string());
        }
        let mut candidate = self.state.read().unwrap().clone();
        if !candidate.is_replica() {
            return Err("ERR You should send CLUSTER FAILOVER to a replica".to_string());
        }
        if candidate.repointing_to.is_some() {
            return Err("ERR Replica is re-pointing to a new primary".to_string());
        }
        let Some(failed_primary) = candidate.primary.clone() else {
            return Err("ERR Replica has no configured primary".to_string());
        };
        if !manual && !self.is_failed(&failed_primary) {
            return Err(format!("ERR Primary {failed_primary} is not marked FAIL"));
        }
        let slots = candidate.slots.slots_of(&failed_primary);
        if slots.is_empty() {
            return Err("ERR Primary owns no slots to take over".to_string());
        }
        let primary_config_epoch = candidate.slots.max_epoch_for(&failed_primary);
        let owners = candidate.slots.owners();
        let quorum = owners.len() / 2 + 1;

        let election_epoch = candidate.current_epoch + 1;
        candidate.current_epoch = election_epoch;
        self.persist_state(&candidate)
            .await
            .map_err(|e| format!("ERR {e}"))?;
        *self.state.write().unwrap() = candidate;

        let targets = {
            let nodes = self.nodes.read().await;
            let known = self.known_peers.read().unwrap();
            let mut targets = Vec::new();
            for owner in &owners {
                if owner == &self.node_id {
                    continue;
                }
                if let Some(addr) = nodes
                    .get(owner)
                    .map(|p| p.addr)
                    .or_else(|| known.get(owner).map(|p| p.addr))
                {
                    targets.push((owner.clone(), addr));
                }
            }
            targets
        };
        drop(_guard);

        self.election_stats
            .elections_started
            .fetch_add(1, Ordering::Relaxed);
        if let Err(e) = self.update_gossip_data().await {
            debug!("Failed to gossip incremented election epoch: {}", e);
        }

        info!(
            candidate = %self.node_id,
            epoch = election_epoch,
            failed_primary = %failed_primary,
            primary_config_epoch,
            owners = owners.len(),
            quorum,
            targets = targets.len(),
            manual,
            "Starting failover election"
        );

        Ok(ElectionPlan {
            candidate: self.node_id.clone(),
            epoch: election_epoch,
            failed_primary,
            primary_config_epoch,
            slots,
            owners,
            targets,
            quorum,
            manual,
        })
    }

    /// Evaluate and cast a vote for `BLOBASAUR.FAILOVER-AUTH` (`Vote` in
    /// `specs/Failover.tla`), persisting `current_epoch`, `last_vote_epoch`, and
    /// `last_vote_for` atomically BEFORE returning `Ok(epoch)`.
    pub async fn handle_failover_auth(&self, req: &FailoverAuthRequest) -> Result<u64, String> {
        self.handle_failover_auth_at(req, Instant::now()).await
    }

    pub(crate) async fn handle_failover_auth_at(
        &self,
        req: &FailoverAuthRequest,
        now: Instant,
    ) -> Result<u64, String> {
        let _guard = self.write_lock.lock().await;
        let refuse = |reason: String| -> Result<u64, String> {
            self.election_stats
                .votes_refused
                .fetch_add(1, Ordering::Relaxed);
            info!(
                voter = %self.node_id,
                candidate = %req.candidate,
                epoch = req.epoch,
                failed_primary = %req.failed_primary,
                manual = req.manual,
                reason = %reason,
                "Refused failover vote"
            );
            Err(reason)
        };

        if self.persist_halted.load(Ordering::Acquire) {
            return refuse("voter is shutting down".to_string());
        }
        if self.bootstrapping.load(Ordering::Acquire) {
            return refuse("voter is still bootstrapping".to_string());
        }

        let state = self.state.read().unwrap().clone();
        if state.role != PersistedRole::Primary || state.demoting_to.is_some() {
            return refuse("voter is not a primary".to_string());
        }
        if !state.slots.has_slots_of(&self.node_id) {
            return refuse("voter owns no slots".to_string());
        }
        if req.epoch < state.current_epoch {
            return refuse(format!(
                "requested epoch {} < current_epoch {}",
                req.epoch, state.current_epoch
            ));
        }
        if req.epoch <= state.last_vote_epoch {
            return refuse(format!(
                "already voted in epoch {} (requested {})",
                state.last_vote_epoch, req.epoch
            ));
        }

        // Candidate must be a replica of `failed_primary` per gossip.
        let is_known_replica = {
            let nodes = self.nodes.read().await;
            nodes.get(&req.candidate).is_some_and(|peer| {
                peer.role != NodeRole::Primary
                    && peer.primary.as_deref() == Some(req.failed_primary.as_str())
            })
        };
        if !is_known_replica {
            return refuse(format!(
                "candidate {} is not a known replica of {} in gossip",
                req.candidate, req.failed_primary
            ));
        }

        // Primary must be FAIL in this node's view unless MANUAL.
        if !req.manual && !self.is_failed(&req.failed_primary) {
            return refuse(format!("primary {} is not marked FAIL", req.failed_primary));
        }

        // Persisted vote backoff: at most one vote per failed primary per 2 * node_timeout_ms.
        let backoff = vote_backoff(self.node_timeout());
        if state
            .last_vote_for
            .get(&req.failed_primary)
            .copied()
            .unwrap_or(0)
            > 0
        {
            let mut backoff_map = self.vote_backoff_at.lock().unwrap();
            match backoff_map.get(&req.failed_primary).copied() {
                Some(voted_at) if now.saturating_duration_since(voted_at) < backoff => {
                    return refuse(format!(
                        "already voted for a replica of {} within vote backoff ({:?})",
                        req.failed_primary, backoff
                    ));
                }
                None => {
                    // Fail-safe if persisted entry existed without an in-memory timestamp.
                    backoff_map.insert(req.failed_primary.clone(), now);
                    return refuse(format!(
                        "already voted for a replica of {} within vote backoff ({:?})",
                        req.failed_primary, backoff
                    ));
                }
                Some(_) => {}
            }
        }

        // Per-slot epoch check: candidate must not be working from an older config.
        if req.slots.is_empty() {
            return refuse("request listed no slots".to_string());
        }
        for &slot in &req.slots {
            if let Some(binding) = state.slots.get(slot)
                && binding.epoch > req.primary_config_epoch
            {
                return refuse(format!(
                    "slot {} has newer epoch {} > primary-config-epoch {}",
                    slot, binding.epoch, req.primary_config_epoch
                ));
            }
        }

        // All checks passed: adopt epoch, record vote & backoff, persist BEFORE replying.
        let mut next = state;
        let epoch_advanced = req.epoch > next.current_epoch;
        next.current_epoch = req.epoch;
        next.last_vote_epoch = req.epoch;
        next.last_vote_for
            .insert(req.failed_primary.clone(), req.epoch);
        if let Err(e) = self.persist_state(&next).await {
            return refuse(format!("failed to persist vote: {e}"));
        }
        *self.state.write().unwrap() = next;
        self.vote_backoff_at
            .lock()
            .unwrap()
            .insert(req.failed_primary.clone(), now);
        self.election_stats
            .votes_granted
            .fetch_add(1, Ordering::Relaxed);
        drop(_guard);

        if epoch_advanced && let Err(e) = self.update_gossip_data().await {
            debug!("Failed to gossip adopted epoch after vote: {}", e);
        }

        info!(
            voter = %self.node_id,
            candidate = %req.candidate,
            epoch = req.epoch,
            failed_primary = %req.failed_primary,
            manual = req.manual,
            "Granted failover vote"
        );
        Ok(req.epoch)
    }

    /// Promotion commit point shared by `CLUSTER FAILOVER TAKEOVER` and won
    /// elections: under `write_lock`, validate preconditions, persist
    /// `role = primary` with the old primary's slots bound to self at the
    /// target epoch, run `on_persisted` (per-shard writer promotion), install
    /// in-memory state, and gossip.
    pub async fn promote<F, Fut>(
        &self,
        kind: PromotionKind,
        on_persisted: F,
    ) -> Result<u64, PromoteError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<(), String>>,
    {
        let _guard = self.write_lock.lock().await;
        if self.persist_halted.load(Ordering::Acquire) {
            return Err(PromoteError::BeforeCommit(
                "ERR Node is shutting down after incomplete takeover".to_string(),
            ));
        }
        let mut candidate = self.state.read().unwrap().clone();
        if !candidate.is_replica() {
            return Err(PromoteError::BeforeCommit(
                "ERR You should send CLUSTER FAILOVER to a replica".to_string(),
            ));
        }
        if candidate.repointing_to.is_some() {
            return Err(PromoteError::BeforeCommit(
                "ERR Replica is re-pointing to a new primary".to_string(),
            ));
        }
        let Some(old_primary) = candidate.primary.clone() else {
            return Err(PromoteError::BeforeCommit(
                "ERR Replica has no configured primary".to_string(),
            ));
        };
        let (taken, new_epoch, election_deadline) = match kind {
            PromotionKind::Takeover => {
                let taken = candidate.slots.slots_of(&old_primary);
                if taken.is_empty() {
                    return Err(PromoteError::BeforeCommit(
                        "ERR Primary owns no slots to take over".to_string(),
                    ));
                }
                let new_epoch = candidate.current_epoch + 1;
                candidate.current_epoch = new_epoch;
                (taken, new_epoch, None)
            }
            PromotionKind::Elected {
                election_epoch,
                failed_primary,
                primary_config_epoch,
                expected_slots,
                deadline,
            } => {
                if old_primary != failed_primary {
                    return Err(PromoteError::BeforeCommit(format!(
                        "ERR Replica now follows {old_primary}, no longer {failed_primary}"
                    )));
                }
                let current_slots = candidate.slots.slots_of(&failed_primary);
                if current_slots.is_empty() || current_slots != expected_slots {
                    return Err(PromoteError::BeforeCommit(
                        "ERR Failed primary's slots changed before promotion".to_string(),
                    ));
                }
                if candidate.slots.max_epoch_for(&failed_primary) != primary_config_epoch {
                    return Err(PromoteError::BeforeCommit(
                        "ERR Failed primary's slot epoch changed before promotion".to_string(),
                    ));
                }
                if election_epoch <= primary_config_epoch {
                    return Err(PromoteError::BeforeCommit(
                        "ERR Election epoch is not newer than primary epoch".to_string(),
                    ));
                }
                candidate.current_epoch = candidate.current_epoch.max(election_epoch);
                (expected_slots, election_epoch, Some(deadline))
            }
        };
        candidate.config_epoch = new_epoch;
        candidate.role = PersistedRole::Primary;
        candidate.primary = None;
        candidate.demoting_to = None;
        candidate.repointing_to = None;
        for &slot in &taken {
            candidate.slots.set(slot, self.node_id.clone(), new_epoch);
        }
        self.gossip_data_for(&candidate, &taken)
            .map_err(|e| PromoteError::BeforeCommit(format!("ERR {e}")))?;

        // Re-check the election deadline right before the commit point so a slow
        // replica-sync stop or lock wait cannot promote after the voter backoff
        // window's timing assumption (`FAILOVER.md` "Timing assumption").
        if let Some(deadline) = election_deadline
            && Instant::now() >= deadline
        {
            return Err(PromoteError::BeforeCommit(
                "ERR Election deadline passed before promotion commit".to_string(),
            ));
        }

        // Commit point: persist cluster state before per-shard promotion or serving.
        self.persist_state(&candidate)
            .await
            .map_err(|e| PromoteError::BeforeCommit(format!("ERR {e}")))?;

        // Run per-shard promotion while holding `write_lock` and before exposing
        // the primary role in memory, so neither writes nor BLOBASAUR.SYNC see
        // primary before every shard has its new replid.
        if let Err(e) = on_persisted().await {
            // Cluster state on disk is already primary; halt further state
            // writes and gossip while the node shuts down cleanly so startup
            // completes promotion through init_shard.
            self.persist_halted.store(true, Ordering::Release);
            self.reset_replica_synced();
            self.gossip_shutdown.cancel();
            return Err(PromoteError::AfterCommit(e));
        }

        *self.state.write().unwrap() = candidate;
        self.reset_replica_synced();
        drop(_guard);

        if let Err(e) = self.update_gossip_data().await {
            error!("Failed to gossip promoted state: {}", e);
        }
        info!(
            "Node {} promoted to primary at epoch {} taking {} slots from {}",
            self.node_id,
            new_epoch,
            taken.len(),
            old_primary
        );
        Ok(new_epoch)
    }

    /// Commit demotion after shard writer barriers and per-shard `Demote`
    /// succeed: clear `demoting_to` and persist `role = replica, primary = winner`
    /// atomically.
    pub async fn finish_demotion(&self) -> Result<Option<String>, String> {
        let _guard = self.write_lock.lock().await;
        let mut candidate = self.state.read().unwrap().clone();
        if candidate.role != PersistedRole::Primary || candidate.slots.has_slots_of(&self.node_id) {
            return Ok(None);
        }
        let Some(winner) = candidate.demoting_to.take() else {
            return Ok(None);
        };
        candidate.role = PersistedRole::Replica;
        candidate.primary = Some(winner.clone());
        candidate.config_epoch = 0;
        candidate.repointing_to = None;
        self.persist_state(&candidate).await?;
        *self.state.write().unwrap() = candidate;
        self.reset_replica_synced();
        drop(_guard);
        info!("Node {} demoted to replica of {}", self.node_id, winner);
        Ok(Some(winner))
    }

    /// Commit replica re-pointing after sync tasks stop and shards are marked
    /// unsynced: clear `repointing_to` and persist `primary = new_primary`
    /// atomically.
    pub async fn finish_repoint(&self) -> Result<Option<String>, String> {
        let _guard = self.write_lock.lock().await;
        let mut candidate = self.state.read().unwrap().clone();
        if candidate.role != PersistedRole::Replica {
            return Ok(None);
        }
        let Some(new_primary) = candidate.repointing_to.take() else {
            return Ok(None);
        };
        let old_primary = candidate.primary.clone().unwrap_or_default();
        candidate.primary = Some(new_primary.clone());
        candidate.config_epoch = 0;
        self.persist_state(&candidate).await?;
        *self.state.write().unwrap() = candidate;
        self.reset_replica_synced();
        drop(_guard);
        info!(
            "Replica {} re-pointed from {} to {}",
            self.node_id, old_primary, new_primary
        );
        Ok(Some(new_primary))
    }

    /// Validate `CLUSTER REPLICATE <target_id>` preconditions.
    pub async fn validate_replicate_target(
        &self,
        target_id: &str,
    ) -> Result<ReplicateCheck, String> {
        if target_id == self.node_id {
            return Err("ERR Can't replicate myself".to_string());
        }
        let nodes = self.nodes.read().await;
        let state = self.state.read().unwrap();
        match nodes.get(target_id) {
            Some(peer) if peer.role != NodeRole::Primary => {
                return Err("ERR I can only replicate a master, not a replica.".to_string());
            }
            Some(_) => {}
            None if state.slots.has_slots_of(target_id) => {}
            None => return Err(format!("ERR Unknown node {target_id}")),
        }
        match state.role {
            PersistedRole::Primary => {
                if state.slots.has_slots_of(&self.node_id) {
                    return Err(
                        "ERR To set a master the node must be empty and without assigned slots."
                            .to_string(),
                    );
                }
                Ok(ReplicateCheck::FromEmptyPrimary)
            }
            PersistedRole::Replica => {
                if state.primary.as_deref() == Some(target_id) {
                    Ok(ReplicateCheck::AlreadyFollowing)
                } else {
                    Ok(ReplicateCheck::SwitchReplica)
                }
            }
        }
    }

    /// Re-validate under `write_lock` and persist `role = replica, primary = target_id`.
    pub async fn commit_replicate(&self, target_id: &str) -> Result<(), String> {
        let _guard = self.write_lock.lock().await;
        if target_id == self.node_id {
            return Err("ERR Can't replicate myself".to_string());
        }
        let nodes = self.nodes.read().await;
        let mut candidate = self.state.read().unwrap().clone();
        match nodes.get(target_id) {
            Some(peer) if peer.role != NodeRole::Primary => {
                return Err("ERR I can only replicate a master, not a replica.".to_string());
            }
            Some(_) => {}
            None if candidate.slots.has_slots_of(target_id) => {}
            None => return Err(format!("ERR Unknown node {target_id}")),
        }
        drop(nodes);

        if candidate.role == PersistedRole::Primary && candidate.slots.has_slots_of(&self.node_id) {
            return Err(
                "ERR To set a master the node must be empty and without assigned slots."
                    .to_string(),
            );
        }
        candidate.role = PersistedRole::Replica;
        candidate.primary = Some(target_id.to_string());
        candidate.config_epoch = 0;
        candidate.demoting_to = None;
        candidate.repointing_to = None;
        self.persist_state(&candidate)
            .await
            .map_err(|e| format!("ERR {e}"))?;
        *self.state.write().unwrap() = candidate;
        self.reset_replica_synced();
        drop(_guard);

        info!("Node {} now replicates {}", self.node_id, target_id);
        Ok(())
    }

    /// Apply a fresh snapshot of chitchat peers at monotonic time `now`:
    /// advance heartbeat-based PFAIL tracking, update the reachable peer view,
    /// adopt higher `current_epoch`, merge slot claims by the epoch rule,
    /// resolve `config_epoch` collisions, and evaluate FAIL / minority state.
    async fn process_peer_snapshots(&self, snapshots: HashMap<String, PeerSnapshot>, now: Instant) {
        let observations: HashMap<String, PeerHeartbeatView> = snapshots
            .iter()
            .map(|(id, snap)| (id.clone(), snap.heartbeat_view.clone()))
            .collect();

        // Phase 1: heartbeat tracking and local PFAIL.
        let initial_owners = self.state.read().unwrap().slots.owners();
        let (progressed, mut failure_outcome, nodes_map) = {
            let mut failure = self.failure.write().unwrap();
            let (progressed, outcome) =
                failure.observe_heartbeats(now, &observations, &initial_owners);
            let reachable: HashMap<String, ClusterNode> = snapshots
                .iter()
                .filter(|(id, _)| !failure.is_pfail(id))
                .map(|(id, snap)| (id.clone(), snap.node.clone()))
                .collect();
            (progressed, outcome, reachable)
        };

        {
            let mut known = self.known_peers.write().unwrap();
            for (id, snap) in &snapshots {
                known.insert(
                    id.clone(),
                    KnownPeer {
                        addr: snap.node.addr,
                        role: snap.node.role,
                        primary: snap.node.primary.clone(),
                        config_epoch: snap.node.config_epoch,
                        shard_repl: snap.node.shard_repl.clone(),
                    },
                );
            }
        }

        // Update the reachable nodes map and log topology changes
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
        if self.persist_halted.load(Ordering::Acquire) {
            return;
        }
        let is_bootstrapping = self.bootstrapping.load(Ordering::Acquire);
        let mut state = self.state.read().unwrap().clone();
        let old_own_slots = if state.role == PersistedRole::Primary {
            state.slots.slots_of(&self.node_id)
        } else {
            HashSet::new()
        };
        let old_demoting_slots = state
            .demoting_to
            .as_deref()
            .map(|w| state.slots.slots_of(w))
            .unwrap_or_default();
        let tracked_primary = if state.role == PersistedRole::Replica {
            state
                .repointing_to
                .clone()
                .or_else(|| state.primary.clone())
        } else {
            None
        };
        let old_tracked_slots = tracked_primary
            .as_deref()
            .map(|p| state.slots.slots_of(p))
            .unwrap_or_default();

        let mut changed = false;
        let mut gossip_changed = false;
        let mut mark_repointing = false;

        // Adopt the highest epoch seen across all reachable peers.
        let mut max_epoch = state.current_epoch;
        for peer in nodes_map.values() {
            max_epoch = max_epoch.max(peer.current_epoch).max(peer.config_epoch);
        }
        if max_epoch > state.current_epoch {
            state.current_epoch = max_epoch;
            changed = true;
            gossip_changed = true;
        }

        // Merge reachable primaries' slot claims in deterministic node-id order.
        let mut primary_peers: Vec<&ClusterNode> = nodes_map
            .values()
            .filter(|peer| peer.role == NodeRole::Primary)
            .collect();
        primary_peers.sort_by(|a, b| a.id.cmp(&b.id));

        let mut last_taker: Option<String> = None;
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
                last_taker = Some(peer.id.clone());
            }
        }

        // Durable markers for level-triggered demotion and replica re-pointing:
        // persisted right alongside the merged slot table so crashes never
        // reopen lost slots and failed attempts retry on subsequent ticks.
        if !is_bootstrapping {
            match state.role {
                PersistedRole::Primary => {
                    if !old_own_slots.is_empty() && !state.slots.has_slots_of(&self.node_id) {
                        let winner = last_taker.or_else(|| {
                            old_own_slots
                                .iter()
                                .filter_map(|&slot| state.slots.get(slot))
                                .filter(|b| b.node_id != self.node_id)
                                .max_by(|a, b| {
                                    a.epoch
                                        .cmp(&b.epoch)
                                        .then_with(|| b.node_id.cmp(&a.node_id))
                                })
                                .map(|b| b.node_id.clone())
                        });
                        if let Some(winner) = winner {
                            warn!(
                                "Primary {} lost its last slot to {}; marking demoting_to",
                                self.node_id, winner
                            );
                            state.demoting_to = Some(winner);
                            changed = true;
                            gossip_changed = true;
                        }
                    } else if let Some(ref cur_winner) = state.demoting_to
                        && let Some(next_winner) = old_demoting_slots
                            .iter()
                            .filter_map(|&slot| state.slots.get(slot))
                            .filter(|b| b.node_id != *cur_winner && b.node_id != self.node_id)
                            .max_by(|a, b| {
                                a.epoch
                                    .cmp(&b.epoch)
                                    .then_with(|| b.node_id.cmp(&a.node_id))
                            })
                            .map(|b| b.node_id.clone())
                    {
                        info!(
                            "Demoting primary {} saw winner {}'s slots move to {}; updating demoting_to",
                            self.node_id, cur_winner, next_winner
                        );
                        state.demoting_to = Some(next_winner);
                        changed = true;
                    }
                }
                PersistedRole::Replica => {
                    if let Some(ref primary) = tracked_primary
                        && let Some(new_primary) = old_tracked_slots
                            .iter()
                            .filter_map(|&slot| state.slots.get(slot))
                            .filter(|b| b.node_id != *primary && b.node_id != self.node_id)
                            .max_by(|a, b| {
                                a.epoch
                                    .cmp(&b.epoch)
                                    .then_with(|| b.node_id.cmp(&a.node_id))
                            })
                            .map(|b| b.node_id.clone())
                    {
                        info!(
                            "Replica {} saw primary {}'s slots move to {}; marking repointing_to",
                            self.node_id, primary, new_primary
                        );
                        state.repointing_to = Some(new_primary);
                        mark_repointing = true;
                        changed = true;
                        gossip_changed = true;
                    }
                }
            }
        }

        // Redis epoch collision rule: when two primaries share a config_epoch,
        // the lexicographically smaller node id bumps current_epoch and takes it.
        if !is_bootstrapping && state.role == PersistedRole::Primary && state.demoting_to.is_none()
        {
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

        // Expire persisted per-primary vote backoffs whose 2 * node_timeout window elapsed.
        if !is_bootstrapping && !state.last_vote_for.is_empty() {
            let backoff = vote_backoff(self.node_timeout());
            let mut backoff_map = self.vote_backoff_at.lock().unwrap();
            let expired: Vec<String> = state
                .last_vote_for
                .keys()
                .filter(|primary| {
                    backoff_map
                        .get(*primary)
                        .is_some_and(|&voted_at| now.saturating_duration_since(voted_at) >= backoff)
                })
                .cloned()
                .collect();
            for primary in expired {
                state.last_vote_for.remove(&primary);
                backoff_map.remove(&primary);
                changed = true;
            }
        }

        let pending_transition = if is_bootstrapping {
            None
        } else {
            Self::pending_transition_for(&state)
        };

        let post_merge_owners = state.slots.owners();
        if changed {
            if !is_bootstrapping && let Err(e) = self.persist_state(&state).await {
                error!("Failed to persist cluster state after gossip update: {}", e);
                return;
            }
            *self.state.write().unwrap() = state;
            if mark_repointing {
                self.reset_replica_synced();
            }
        }
        drop(_guard);

        // Phase 2: evaluate FAIL and minority state against the updated slot table.
        if !is_bootstrapping {
            let mut failure = self.failure.write().unwrap();
            failure.evaluate_failures(
                now,
                &observations,
                &progressed,
                &post_merge_owners,
                &mut failure_outcome,
            );
        }
        if failure_outcome.gossip_changed || self.shard_repl_dirty.swap(false, Ordering::AcqRel) {
            gossip_changed = true;
        }
        for id in &failure_outcome.newly_pfail {
            warn!("Peer {} marked PFAIL on {}", id, self.node_id);
        }
        for id in &failure_outcome.cleared_pfail {
            info!(
                "Peer {} reachable again on {} (cleared PFAIL)",
                id, self.node_id
            );
        }
        for id in &failure_outcome.newly_failed {
            warn!("Peer {} marked FAIL on {}", id, self.node_id);
        }
        for id in &failure_outcome.cleared_failed {
            info!("Peer {} cleared FAIL on {}", id, self.node_id);
        }
        if failure_outcome.entered_minority {
            warn!(
                "Node {} lost majority of slot-owning primaries; fencing requests with CLUSTERDOWN",
                self.node_id
            );
        } else if failure_outcome.exited_minority {
            info!(
                "Node {} regained majority of slot-owning primaries; resuming service",
                self.node_id
            );
        }

        if gossip_changed
            && !is_bootstrapping
            && let Err(e) = self.update_gossip_data().await
        {
            error!("Failed to gossip updated epochs/slots: {}", e);
        }

        if let Some(transition) = pending_transition
            && let Some(handler) = self.role_handler.lock().unwrap().clone()
        {
            handler(transition);
        }
        self.election_notify.notify_one();
        self.repl_progress_notify.notify_waiters();
    }

    #[cfg(test)]
    async fn process_peers(&self, nodes_map: HashMap<String, ClusterNode>) {
        let snapshots = nodes_map
            .into_iter()
            .map(|(id, node)| {
                (
                    id,
                    PeerSnapshot {
                        node,
                        heartbeat_view: PeerHeartbeatView {
                            generation_id: 1,
                            heartbeat: 1,
                            pfail: Vec::new(),
                            fail: Vec::new(),
                        },
                    },
                )
            })
            .collect();
        self.process_peer_snapshots(snapshots, Instant::now()).await;
    }

    /// Decide where a request for `slot` should run from the slot table.
    /// A synced replica serves reads of its primary's slots on connections that
    /// sent `READONLY`.
    pub async fn route(&self, slot: u16, is_read: bool, conn_readonly: bool) -> Route {
        let (role, primary, repointing, binding) = {
            let state = self.state.read().unwrap();
            (
                state.role,
                state.primary.clone(),
                state.repointing_to.is_some(),
                state.slots.get(slot).cloned(),
            )
        };
        let Some(binding) = binding else {
            return Route::ClusterDown;
        };
        if binding.node_id == self.node_id {
            return if role == PersistedRole::Primary && !self.is_in_minority() {
                Route::Local
            } else {
                Route::ClusterDown
            };
        }
        if self.is_failed(&binding.node_id) {
            return Route::ClusterDown;
        }
        let nodes = self.nodes.read().await;
        let Some(owner) = nodes.get(&binding.node_id) else {
            return Route::ClusterDown;
        };
        if is_read
            && conn_readonly
            && role == PersistedRole::Replica
            && !repointing
            && self.role() == NodeRole::Replica
            && primary.as_deref() == Some(binding.node_id.as_str())
        {
            return if self.is_in_minority() {
                Route::ClusterDown
            } else {
                Route::Local
            };
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
        if changed {
            if let Err(e) = self.update_gossip_data().await {
                error!("Failed to gossip the new role: {}", e);
            }
            self.election_notify.notify_one();
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
        let known_peers = self.known_peers.read().unwrap().clone();
        let (pfail_set, fail_set): (HashSet<String>, HashSet<String>) = {
            let failure = self.failure.read().unwrap();
            (
                failure.pfail_list().into_iter().collect(),
                failure.fail_list().into_iter().collect(),
            )
        };

        let (local_ip, local_port) = local_endpoint(&self.config, self.local_addr);
        // In Redis CLUSTER NODES, a replica reports its primary's config_epoch.
        let primary_epoch = |primary_id: &str| -> u64 {
            if primary_id == self.node_id && state.role == PersistedRole::Primary {
                state.config_epoch
            } else if let Some(peer) = nodes.get(primary_id) {
                peer.config_epoch
            } else {
                state
                    .slots
                    .max_epoch_for(primary_id)
                    .max(known_peers.get(primary_id).map_or(0, |p| p.config_epoch))
            }
        };

        // Local node line.
        let local_info = match state.role {
            PersistedRole::Replica => {
                let primary = state.primary.as_deref().unwrap_or("-");
                format!(
                    "{} {}:{} myself,slave {} 0 0 {} connected",
                    self.node_id,
                    local_ip,
                    local_port,
                    primary,
                    primary_epoch(primary)
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

        // Collect all remote peer ids: reachable peers, offline/evicted known
        // peers, retained slot owners, and any nodes marked PFAIL/FAIL.
        let mut peer_ids: BTreeSet<String> = BTreeSet::new();
        peer_ids.extend(nodes.keys().cloned());
        peer_ids.extend(known_peers.keys().cloned());
        peer_ids.extend(state.slots.owners());
        peer_ids.extend(pfail_set.iter().cloned());
        peer_ids.extend(fail_set.iter().cloned());
        peer_ids.remove(&self.node_id);

        for peer_id in peer_ids {
            let is_failed = fail_set.contains(&peer_id);
            let is_pfail = pfail_set.contains(&peer_id);
            let fail_suffix = if is_failed {
                ",fail"
            } else if is_pfail {
                ",fail?"
            } else {
                ""
            };
            let link_state = if nodes.contains_key(&peer_id) && !is_pfail {
                "connected"
            } else {
                "disconnected"
            };

            let line = if let Some(node) = nodes.get(&peer_id) {
                match (node.role, &node.primary) {
                    (NodeRole::Primary, _) | (_, None) => {
                        let peer_slots = state.slots.slots_of(&node.id);
                        let ranges = Self::slots_to_ranges(&peer_slots);
                        if ranges.is_empty() {
                            format!(
                                "{} {}:{} master{} - 0 0 {} {}",
                                node.id,
                                node.addr.ip(),
                                node.addr.port(),
                                fail_suffix,
                                node.config_epoch,
                                link_state,
                            )
                        } else {
                            format!(
                                "{} {}:{} master{} - 0 0 {} {} {}",
                                node.id,
                                node.addr.ip(),
                                node.addr.port(),
                                fail_suffix,
                                node.config_epoch,
                                link_state,
                                ranges,
                            )
                        }
                    }
                    (_, Some(primary)) => format!(
                        "{} {}:{} slave{} {} 0 0 {} {}",
                        node.id,
                        node.addr.ip(),
                        node.addr.port(),
                        fail_suffix,
                        primary,
                        primary_epoch(primary),
                        link_state,
                    ),
                }
            } else {
                let addr = known_peers
                    .get(&peer_id)
                    .map_or_else(|| "0.0.0.0:0".to_string(), |p| p.addr.to_string());
                let peer_slots = state.slots.slots_of(&peer_id);
                if !peer_slots.is_empty() {
                    let epoch = state.slots.max_epoch_for(&peer_id);
                    let ranges = Self::slots_to_ranges(&peer_slots);
                    format!(
                        "{} {} master{} - 0 0 {} {} {}",
                        peer_id, addr, fail_suffix, epoch, link_state, ranges
                    )
                } else if let Some(known) = known_peers.get(&peer_id) {
                    match (known.role, &known.primary) {
                        (NodeRole::Primary, _) | (_, None) => format!(
                            "{} {} master{} - 0 0 {} {}",
                            peer_id, addr, fail_suffix, known.config_epoch, link_state
                        ),
                        (_, Some(primary)) => format!(
                            "{} {} slave{} {} 0 0 {} {}",
                            peer_id,
                            addr,
                            fail_suffix,
                            primary,
                            primary_epoch(primary),
                            link_state,
                        ),
                    }
                } else {
                    format!(
                        "{} {} master{} - 0 0 0 {}",
                        peer_id, addr, fail_suffix, link_state
                    )
                }
            };
            result.push(line);
        }

        format!("{}\n", result.join("\n"))
    }

    /// Collapse a slot set into sorted, inclusive `(start, end)` runs
    pub(crate) fn slot_runs(slots: &HashSet<u16>) -> Vec<(u16, u16)> {
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
    /// Lists only live, non-failed slot owners; replicas are listed only once synced.
    pub async fn get_cluster_slots(&self) -> Vec<SlotRange> {
        let (local_ip, local_port) = local_endpoint(&self.config, self.local_addr);
        let nodes = self.nodes.read().await;
        let state = self.state.read().unwrap().clone();
        let fail_set: HashSet<String> = self
            .failure
            .read()
            .unwrap()
            .fail_list()
            .into_iter()
            .collect();

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
            if fail_set.contains(&node.id) {
                continue;
            }
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
            } else if !fail_set.contains(&owner_id)
                && let Some(peer) = nodes.get(&owner_id)
            {
                (peer.addr.ip().to_string(), peer.addr.port())
            } else {
                // Owner is down or FAIL: slot is currently CLUSTERDOWN, omit from CLUSTER SLOTS.
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
        let known_peers = self.known_peers.read().unwrap().clone();
        let fail_set: HashSet<String> = self
            .failure
            .read()
            .unwrap()
            .fail_list()
            .into_iter()
            .collect();

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
            if fail_set.contains(&node.id) {
                continue;
            }
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
            } else if !fail_set.contains(&owner_id)
                && let Some(peer) = nodes.get(&owner_id)
            {
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
            } else if let Some(addr) = nodes
                .get(&owner_id)
                .map(|p| p.addr)
                .or_else(|| known_peers.get(&owner_id).map(|p| p.addr))
            {
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
        let known_peers = self.known_peers.read().unwrap().clone();
        let (in_minority, pfail_set, fail_set): (bool, HashSet<String>, HashSet<String>) = {
            let failure = self.failure.read().unwrap();
            (
                failure.is_in_minority(),
                failure.pfail_list().into_iter().collect(),
                failure.fail_list().into_iter().collect(),
            )
        };

        let slot_owners = state.slots.owners();
        let assigned_slots = state.slots.slot_count();

        let mut ok_slots = 0usize;
        let mut pfail_slots = 0usize;
        let mut fail_slots = 0usize;
        for (start, end, owner_id) in state.slots.runs_by_owner() {
            let count = (end - start) as usize + 1;
            if owner_id == self.node_id && state.role == PersistedRole::Primary {
                ok_slots += count;
            } else if fail_set.contains(&owner_id) {
                fail_slots += count;
            } else if pfail_set.contains(&owner_id) {
                pfail_slots += count;
            } else if nodes.contains_key(&owner_id) {
                ok_slots += count;
            } else {
                fail_slots += count;
            }
        }

        let mut known_nodes: BTreeSet<&str> = BTreeSet::new();
        known_nodes.insert(&self.node_id);
        for id in nodes.keys() {
            known_nodes.insert(id.as_str());
        }
        for id in known_peers.keys() {
            known_nodes.insert(id.as_str());
        }
        for owner in &slot_owners {
            known_nodes.insert(owner.as_str());
        }

        let cluster_state = if !in_minority && ok_slots == REDIS_CLUSTER_SLOTS as usize {
            "ok"
        } else {
            "fail"
        };

        format!(
            "cluster_state:{}\n\
             cluster_slots_assigned:{}\n\
             cluster_slots_ok:{}\n\
             cluster_slots_pfail:{}\n\
             cluster_slots_fail:{}\n\
             cluster_known_nodes:{}\n\
             cluster_size:{}\n\
             cluster_current_epoch:{}\n\
             cluster_my_epoch:{}\n\
             cluster_elections_started:{}\n\
             cluster_elections_won:{}\n\
             cluster_elections_failed:{}\n\
             cluster_votes_granted:{}\n\
             cluster_votes_refused:{}\n\
             cluster_stats_manual_failover_started:{}\n\
             cluster_stats_manual_failover_succeeded:{}\n\
             cluster_stats_manual_failover_aborted:{}\n\
             cluster_stats_messages_sent:0\n\
             cluster_stats_messages_received:0\n",
            cluster_state,
            assigned_slots,
            ok_slots,
            pfail_slots,
            fail_slots,
            known_nodes.len(),
            slot_owners.len(),
            state.current_epoch,
            state.config_epoch,
            self.election_stats
                .elections_started
                .load(Ordering::Relaxed),
            self.election_stats.elections_won.load(Ordering::Relaxed),
            self.election_stats.elections_failed.load(Ordering::Relaxed),
            self.election_stats.votes_granted.load(Ordering::Relaxed),
            self.election_stats.votes_refused.load(Ordering::Relaxed),
            self.election_stats
                .manual_failovers_started
                .load(Ordering::Relaxed),
            self.election_stats
                .manual_failovers_succeeded
                .load(Ordering::Relaxed),
            self.election_stats
                .manual_failovers_aborted
                .load(Ordering::Relaxed),
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
        if candidate.demoting_to.is_some() {
            return Err("ERR this node is demoting".into());
        }
        for &slot in &slots {
            if slot >= REDIS_CLUSTER_SLOTS {
                return Err(format!("ERR invalid slot {}", slot).into());
            }
            if let Some(existing) = candidate.slots.get(slot)
                && existing.node_id != self.node_id
            {
                return Err(format!("ERR Slot {} is already busy", slot).into());
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
        if candidate.demoting_to.is_some() {
            return Err("ERR this node is demoting".into());
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
        let (pfail, fail) = {
            let failure = self.failure.read().unwrap();
            (failure.pfail_list(), failure.fail_list())
        };
        let shard_repl = {
            let repl = self.shard_repl.lock().unwrap();
            if repl
                .iter()
                .any(|s| !s.replid.is_empty() || s.caught_up_lsn > 0)
            {
                repl.clone()
            } else {
                Vec::new()
            }
        };
        NodeGossipData::new(
            format!("{}:{}", local_ip, local_port),
            own_slots,
            role,
            state.primary.clone(),
            state.current_epoch,
            state.config_epoch,
            pfail,
            fail,
            shard_repl,
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
            known_peers: Arc::new(std::sync::RwLock::new(HashMap::new())),
            failure: Arc::new(std::sync::RwLock::new(FailureTracker::new(
                node_id.to_string(),
                Duration::from_millis(config.node_timeout_ms()),
                Instant::now(),
            ))),
            state: Arc::new(std::sync::RwLock::new(ClusterState::bootstrap_replica(
                node_id.to_string(),
                primary.to_string(),
            ))),
            state_path: None,
            write_lock: Arc::new(tokio::sync::Mutex::new(())),
            bootstrapping: Arc::new(AtomicBool::new(false)),
            persist_halted: Arc::new(AtomicBool::new(false)),
            config,
            chitchat: None,
            running: Arc::new(Mutex::new(None)),
            gossip_shutdown: CancellationToken::new(),
            role_handler: Arc::new(Mutex::new(None)),
            local_addr: "127.0.0.1:6379".parse().unwrap(),
            namespace_strategy: NamespaceDistributionStrategy::default(),
            replica_synced: Arc::new(Mutex::new(vec![false; num_shards])),
            shard_repl: Arc::new(Mutex::new(vec![ShardReplState::default(); num_shards])),
            shard_repl_dirty: Arc::new(AtomicBool::new(false)),
            last_frame_at: Arc::new(Mutex::new(vec![None; num_shards])),
            vote_backoff_at: Arc::new(Mutex::new(HashMap::new())),
            election_stats: Arc::new(ElectionStats::default()),
            election_notify: Arc::new(tokio::sync::Notify::new()),
            repl_progress_notify: Arc::new(tokio::sync::Notify::new()),
        }
    }

    /// Stops the background gossip processing task cooperatively so any
    /// in-progress role transition completes cleanly before writers drain.
    pub async fn stop_gossip_task(&self) {
        self.gossip_shutdown.cancel();
        let task = {
            let mut running = self.running.lock().unwrap();
            running.as_mut().and_then(|(_, task)| task.take())
        };
        if let Some(task) = task {
            let _ = task.await;
        }
    }

    /// Stops gossip: the node stops advertising itself and frees its UDP port.
    pub async fn shutdown(&self) {
        self.stop_gossip_task().await;
        if let Some(chitchat_arc) = &self.chitchat {
            chitchat_arc
                .lock()
                .await
                .self_node_state()
                .set("node_status", "shutting_down");
        }
        info!("Shutting down cluster manager");
        let running = self.running.lock().unwrap().take();
        if let Some((handle, _)) = running
            && let Err(e) = handle.shutdown().await
        {
            warn!("Failed to stop gossip: {}", e);
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
            known_peers: Arc::clone(&self.known_peers),
            failure: Arc::clone(&self.failure),
            state: Arc::clone(&self.state),
            state_path: self.state_path.clone(),
            write_lock: Arc::clone(&self.write_lock),
            bootstrapping: Arc::clone(&self.bootstrapping),
            persist_halted: Arc::clone(&self.persist_halted),
            config: self.config.clone(),
            chitchat: self.chitchat.clone(),
            running: Arc::clone(&self.running),
            gossip_shutdown: self.gossip_shutdown.clone(),
            role_handler: Arc::clone(&self.role_handler),
            local_addr: self.local_addr,
            namespace_strategy: self.namespace_strategy,
            replica_synced: Arc::clone(&self.replica_synced),
            shard_repl: Arc::clone(&self.shard_repl),
            shard_repl_dirty: Arc::clone(&self.shard_repl_dirty),
            last_frame_at: Arc::clone(&self.last_frame_at),
            vote_backoff_at: Arc::clone(&self.vote_backoff_at),
            election_stats: Arc::clone(&self.election_stats),
            election_notify: Arc::clone(&self.election_notify),
            repl_progress_notify: Arc::clone(&self.repl_progress_notify),
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

    fn bootstrap_loaded(
        dir: &std::path::Path,
        node_id: &str,
        replica_of: Option<&str>,
    ) -> LoadedClusterState {
        let state = match replica_of {
            Some(primary) => {
                ClusterState::bootstrap_replica(node_id.to_string(), primary.to_string())
            }
            None => ClusterState::bootstrap_primary(node_id.to_string()),
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
            bootstrap_loaded(dir.path(), "test-node", None),
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
        let mut state = ClusterState::bootstrap_primary(node_id.to_string());
        for &slot in local {
            state.slots.set(slot, node_id, 0);
        }
        let mut nodes = HashMap::new();
        let mut known_peers = HashMap::new();
        let now = Instant::now();
        let mut failure = FailureTracker::new(
            node_id.to_string(),
            Duration::from_millis(config.node_timeout_ms()),
            now,
        );
        for (id, addr, slots) in remote {
            let parsed: SocketAddr = addr.parse().unwrap();
            known_peers.insert(
                id.to_string(),
                KnownPeer {
                    addr: parsed,
                    role: NodeRole::Primary,
                    primary: None,
                    config_epoch: 0,
                    shard_repl: Vec::new(),
                },
            );
            failure.seed_live_peer(id, now);
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
                    shard_repl: Vec::new(),
                },
            );
        }
        ClusterManager {
            node_id: config.node_id.clone(),
            nodes: Arc::new(RwLock::new(nodes)),
            known_peers: Arc::new(std::sync::RwLock::new(known_peers)),
            failure: Arc::new(std::sync::RwLock::new(failure)),
            state: Arc::new(std::sync::RwLock::new(state)),
            state_path: None,
            write_lock: Arc::new(tokio::sync::Mutex::new(())),
            bootstrapping: Arc::new(AtomicBool::new(false)),
            persist_halted: Arc::new(AtomicBool::new(false)),
            config,
            chitchat: None,
            running: Arc::new(Mutex::new(None)),
            gossip_shutdown: CancellationToken::new(),
            role_handler: Arc::new(Mutex::new(None)),
            local_addr: "0.0.0.0:6379".parse().unwrap(),
            namespace_strategy: NamespaceDistributionStrategy::default(),
            replica_synced: Arc::new(Mutex::new(vec![false; 2])),
            shard_repl: Arc::new(Mutex::new(vec![ShardReplState::default(); 2])),
            shard_repl_dirty: Arc::new(AtomicBool::new(false)),
            last_frame_at: Arc::new(Mutex::new(vec![None; 2])),
            vote_backoff_at: Arc::new(Mutex::new(HashMap::new())),
            election_stats: Arc::new(ElectionStats::default()),
            election_notify: Arc::new(tokio::sync::Notify::new()),
            repl_progress_notify: Arc::new(tokio::sync::Notify::new()),
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
        *manager.shard_repl.lock().unwrap() = vec![ShardReplState::default(); 2];
        manager
    }

    async fn add_replica(manager: &ClusterManager, id: &str, addr: &str, of: &str, role: NodeRole) {
        let parsed: SocketAddr = addr.parse().unwrap();
        manager.known_peers.write().unwrap().insert(
            id.to_string(),
            KnownPeer {
                addr: parsed,
                role,
                primary: Some(of.to_string()),
                config_epoch: 0,
                shard_repl: Vec::new(),
            },
        );
        manager
            .failure
            .write()
            .unwrap()
            .seed_live_peer(id, Instant::now());
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
                shard_repl: Vec::new(),
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
            vec![],
            vec![],
            vec![],
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
        let at_limit = NodeGossipData::new(
            "a:1".into(),
            &all(9000),
            NodeRole::Primary,
            None,
            0,
            0,
            vec![],
            vec![],
            vec![],
        )
        .unwrap();
        assert_eq!(at_limit.slots.len(), 9000);
        let json = serde_json::to_string(&at_limit).unwrap();
        assert!(json.len() <= GOSSIP_VALUE_MAX, "{} bytes", json.len());
        assert_eq!(
            serde_json::from_str::<Legacy>(&json).unwrap().slots.len(),
            9000
        );
        // The worst case for 9000 slots: all five digits.
        let high: HashSet<u16> = (7384..16384).collect();
        let high = NodeGossipData::new(
            "a:1".into(),
            &high,
            NodeRole::Primary,
            None,
            0,
            0,
            vec![],
            vec![],
            vec![],
        )
        .unwrap();
        assert_eq!(high.slots.len(), 9000);
        let full = NodeGossipData::new(
            "a:1".into(),
            &all(16384),
            NodeRole::Primary,
            None,
            0,
            0,
            vec![],
            vec![],
            vec![],
        )
        .unwrap();
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
        let data = NodeGossipData::new(
            "a:1".into(),
            &alternating,
            NodeRole::Primary,
            None,
            0,
            0,
            vec![],
            vec![],
            vec![],
        )
        .unwrap();
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
        // No ranges, role, primary, epochs, pfail, or fail: read the slot list and default cleanly.
        let old = r#"{"addr":"10.0.0.1:6379","slots":[5,6,7],"state":"master"}"#;
        let data: NodeGossipData = serde_json::from_str(old).unwrap();
        assert_eq!(data.owned_slots(), [5, 6, 7].into_iter().collect());
        assert_eq!((data.role, data.primary), (NodeRole::Primary, None));
        assert_eq!((data.current_epoch, data.config_epoch), (0, 0));
        assert!(data.pfail.is_empty());
        assert!(data.fail.is_empty());
        assert!(data.shard_repl.is_empty());

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
            vec!["node-b".into()],
            vec!["node-c".into()],
            vec![ShardReplState {
                replid: "abc".into(),
                caught_up_lsn: 42,
            }],
        )
        .unwrap();
        let json = serde_json::to_string(&replica).unwrap();
        assert!(json.contains(r#""role":"replica-syncing""#), "{json}");
        assert!(json.contains(r#""primary":"node-a""#), "{json}");
        assert!(json.contains(r#""current_epoch":4"#), "{json}");
        assert!(json.contains(r#""pfail":["node-b"]"#), "{json}");
        assert!(json.contains(r#""fail":["node-c"]"#), "{json}");
        let roundtrip: NodeGossipData = serde_json::from_str(&json).unwrap();
        assert_eq!(roundtrip.pfail, vec!["node-b"]);
        assert_eq!(roundtrip.fail, vec!["node-c"]);
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
            bootstrap_loaded(dir.path(), "test-node", None),
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
            bootstrap_loaded(dir.path(), "test-node", None),
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
        let mut state = ClusterState::bootstrap_primary("node-a".to_string());
        for slot in local_slots {
            state.slots.set(slot, "node-a", 0);
        }

        ClusterManager {
            node_id: config.node_id.clone(),
            nodes: Arc::new(RwLock::new(HashMap::new())),
            known_peers: Arc::new(std::sync::RwLock::new(HashMap::new())),
            failure: Arc::new(std::sync::RwLock::new(FailureTracker::new(
                config.node_id.clone(),
                Duration::from_millis(config.node_timeout_ms()),
                Instant::now(),
            ))),
            state: Arc::new(std::sync::RwLock::new(state)),
            state_path: None,
            write_lock: Arc::new(tokio::sync::Mutex::new(())),
            bootstrapping: Arc::new(AtomicBool::new(false)),
            persist_halted: Arc::new(AtomicBool::new(false)),
            config,
            chitchat: None,
            running: Arc::new(Mutex::new(None)),
            gossip_shutdown: CancellationToken::new(),
            role_handler: Arc::new(Mutex::new(None)),
            local_addr: "127.0.0.1:6401".parse().unwrap(),
            namespace_strategy: NamespaceDistributionStrategy::default(),
            replica_synced: Arc::new(Mutex::new(vec![false; 1])),
            shard_repl: Arc::new(Mutex::new(vec![ShardReplState::default(); 1])),
            shard_repl_dirty: Arc::new(AtomicBool::new(false)),
            last_frame_at: Arc::new(Mutex::new(vec![None; 1])),
            vote_backoff_at: Arc::new(Mutex::new(HashMap::new())),
            election_stats: Arc::new(ElectionStats::default()),
            election_notify: Arc::new(tokio::sync::Notify::new()),
            repl_progress_notify: Arc::new(tokio::sync::Notify::new()),
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
                shard_repl: Vec::new(),
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
            bootstrap_loaded(dir.path(), "node-race", None),
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
            shard_repl: Vec::new(),
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
            shard_repl: Vec::new(),
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
            shard_repl: Vec::new(),
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

        // Losing the last slot (slot 0): serves nothing, routes MOVED to winner,
        // and persists the merged slot table with durable `demoting_to` marker.
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
        let persisted = ClusterState::load(&dir.path().join(CLUSTER_STATE_FILE), "node-a").unwrap();
        assert_eq!(persisted.role, PersistedRole::Primary);
        assert_eq!(persisted.demoting_to.as_deref(), Some("node-b"));
        assert!(!persisted.slots.has_slots_of("node-a"));
        assert_eq!(persisted.slots.get(0).unwrap().node_id, "node-b");
    }

    #[tokio::test]
    async fn replica_detects_repoint_when_primary_slots_move_at_higher_epoch() {
        let dir = tempfile::TempDir::new().unwrap();
        let mut mgr = into_replica(
            manager_with(&[], &[("node-b", "10.0.0.2:6379", &[0, 1])]),
            "node-b",
            true,
        );
        mgr.state_path = Some(dir.path().join(CLUSTER_STATE_FILE));
        {
            let mut st = mgr.state.write().unwrap();
            st.current_epoch = 2;
            st.slots.rebind_owner_epoch("node-b", 2);
        }
        let winner = ClusterNode {
            id: "node-c".to_string(),
            addr: "10.0.0.3:6379".parse().unwrap(),
            slots: [0, 1].into_iter().collect(),
            _state: "master".to_string(),
            role: NodeRole::Primary,
            primary: None,
            current_epoch: 3,
            config_epoch: 3,
            shard_repl: Vec::new(),
        };
        mgr.process_peers([("node-c".to_string(), winner)].into())
            .await;

        assert_eq!(mgr.role(), NodeRole::ReplicaSyncing);
        let persisted = ClusterState::load(&dir.path().join(CLUSTER_STATE_FILE), "node-a").unwrap();
        assert_eq!(persisted.role, PersistedRole::Replica);
        assert_eq!(persisted.primary.as_deref(), Some("node-b"));
        assert_eq!(persisted.repointing_to.as_deref(), Some("node-c"));
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

        // Replicas report their primary's config_epoch in CLUSTER NODES.
        let live_nodes = manager.get_cluster_nodes().await;
        assert!(
            live_nodes.contains("node-a1 10.0.0.4:6382 slave node-a 0 0 3 connected\n"),
            "{live_nodes}"
        );
        assert!(
            live_nodes.contains("node-b1 10.0.0.3:6381 slave node-b 0 0 4 connected\n"),
            "{live_nodes}"
        );

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

    #[tokio::test]
    async fn add_slots_refuses_slots_bound_to_another_node_even_at_lower_epoch() {
        let manager = manager_with(&[0, 1], &[("node-b", "10.0.0.2:6379", &[10, 11])]);
        {
            let mut st = manager.state.write().unwrap();
            st.current_epoch = 5;
            st.config_epoch = 5;
            st.slots.rebind_owner_epoch("node-a", 5);
            st.slots.rebind_owner_epoch("node-b", 1);
        }

        // Slot 10 is bound to node-b at epoch 1 < node-a's config_epoch 5: must be refused.
        let err = manager.add_slots(vec![2, 10]).await.unwrap_err();
        assert_eq!(err.to_string(), "ERR Slot 10 is already busy");
        // Failed command leaves both slot 2 and slot 10 unchanged.
        assert!(manager.state_snapshot().slots.get(2).is_none());
        assert_eq!(
            manager.state_snapshot().slots.get(10).unwrap().node_id,
            "node-b"
        );
        assert_eq!(manager.state_snapshot().slots.get(10).unwrap().epoch, 1);

        // Self-owned and unbound slots are accepted.
        manager.add_slots(vec![0, 2]).await.unwrap();
        assert_eq!(
            manager.state_snapshot().slots.get(2).unwrap().node_id,
            "node-a"
        );
        assert_eq!(manager.state_snapshot().slots.get(2).unwrap().epoch, 5);
    }

    #[tokio::test]
    async fn replica_self_line_reports_primary_config_epoch() {
        let manager = into_replica(
            manager_with(&[], &[("node-b", "10.0.0.2:6379", &[5, 6])]),
            "node-b",
            true,
        );
        {
            let mut st = manager.state.write().unwrap();
            st.current_epoch = 7;
            st.slots.rebind_owner_epoch("node-b", 7);
        }
        manager
            .nodes
            .write()
            .await
            .get_mut("node-b")
            .unwrap()
            .config_epoch = 7;

        let nodes = manager.get_cluster_nodes().await;
        assert!(
            nodes.starts_with("node-a 10.0.0.1:6379 myself,slave node-b 0 0 7 connected\n"),
            "{nodes}"
        );

        // Even when primary is offline, replica falls back to the slot table's epoch.
        manager.process_peers(HashMap::new()).await;
        let nodes = manager.get_cluster_nodes().await;
        assert!(
            nodes.starts_with("node-a 10.0.0.1:6379 myself,slave node-b 0 0 7 connected\n"),
            "{nodes}"
        );
    }

    #[tokio::test]
    async fn role_transition_handler_is_level_triggered_and_non_blocking() {
        let manager = manager_with(&[0, 1], &[]);
        {
            let mut st = manager.state.write().unwrap();
            st.current_epoch = 1;
            st.config_epoch = 1;
            st.slots.rebind_owner_epoch("node-a", 1);
        }

        let fired = Arc::new(Mutex::new(Vec::new()));
        let fired_cb = Arc::clone(&fired);
        manager.set_role_handler(Arc::new(move |transition| {
            fired_cb.lock().unwrap().push(transition);
        }));
        assert!(fired.lock().unwrap().is_empty());

        let mut peers = HashMap::new();
        peers.insert(
            "node-b".to_string(),
            ClusterNode {
                id: "node-b".to_string(),
                addr: "10.0.0.2:6379".parse().unwrap(),
                slots: [0, 1].into_iter().collect(),
                _state: "master".to_string(),
                role: NodeRole::Primary,
                primary: None,
                current_epoch: 2,
                config_epoch: 2,
                shard_repl: Vec::new(),
            },
        );
        manager.process_peers(peers.clone()).await;
        assert_eq!(*fired.lock().unwrap(), vec![RoleTransition::Demote]);
        assert!(manager.is_demoting());

        // Still demoting on the next tick -> handler fires again (level-triggered).
        manager.process_peers(peers).await;
        assert_eq!(
            *fired.lock().unwrap(),
            vec![RoleTransition::Demote, RoleTransition::Demote]
        );

        // Once finish_demotion clears demoting_to, subsequent ticks do not fire.
        assert_eq!(
            manager.finish_demotion().await.unwrap().as_deref(),
            Some("node-b")
        );
        assert!(!manager.is_demoting());
        manager.process_peers(HashMap::new()).await;
        assert_eq!(fired.lock().unwrap().len(), 2);
    }

    #[allow(clippy::too_many_arguments)]
    fn make_peer_snapshot(
        id: &str,
        addr: &str,
        slots: std::ops::RangeInclusive<u16>,
        epoch: u64,
        generation: u64,
        hb: u64,
        pfail: &[&str],
        fail: &[&str],
    ) -> (String, PeerSnapshot) {
        (
            id.to_string(),
            PeerSnapshot {
                node: ClusterNode {
                    id: id.to_string(),
                    addr: addr.parse().unwrap(),
                    slots: slots.collect(),
                    _state: "master".to_string(),
                    role: NodeRole::Primary,
                    primary: None,
                    current_epoch: epoch,
                    config_epoch: epoch,
                    shard_repl: Vec::new(),
                },
                heartbeat_view: PeerHeartbeatView {
                    generation_id: generation,
                    heartbeat: hb,
                    pfail: pfail.iter().map(|s| s.to_string()).collect(),
                    fail: fail.iter().map(|s| s.to_string()).collect(),
                },
            },
        )
    }

    fn full_cluster_manager(
        node_id: &str,
        local: std::ops::RangeInclusive<u16>,
        timeout_ms: u64,
        t0: Instant,
    ) -> ClusterManager {
        let mut mgr = make_test_manager(local);
        mgr.node_id = node_id.to_string();
        mgr.config.node_id = node_id.to_string();
        mgr.config.node_timeout_ms = Some(timeout_ms);
        {
            let mut st = mgr.state.write().unwrap();
            st.node_id = node_id.to_string();
            let slots = st.slots.slots_of("node-a");
            for s in slots {
                st.slots.set(s, node_id, 1);
            }
            st.current_epoch = 1;
            st.config_epoch = 1;
        }
        *mgr.failure.write().unwrap() =
            FailureTracker::new(node_id.to_string(), Duration::from_millis(timeout_ms), t0);
        mgr
    }

    #[tokio::test]
    async fn cluster_nodes_info_and_routing_reflect_pfail_fail_and_recovery() {
        let t0 = Instant::now();
        let mgr = full_cluster_manager("p1", 0..=5460, 1000, t0);

        let initial: HashMap<_, _> = [
            make_peer_snapshot("p2", "10.0.0.2:6379", 5461..=10922, 2, 1, 1, &[], &[]),
            make_peer_snapshot("p3", "10.0.0.3:6379", 10923..=16383, 3, 1, 1, &[], &[]),
        ]
        .into();
        mgr.process_peer_snapshots(initial, t0).await;

        assert!(!mgr.is_pfail("p3"));
        assert!(!mgr.is_failed("p3"));
        assert_eq!(mgr.route(0, false, false).await, Route::Local);
        assert_eq!(
            mgr.route(12000, false, false).await,
            Route::Moved("MOVED 12000 10.0.0.3:6379".to_string())
        );
        let info = mgr.get_cluster_info().await;
        assert!(info.contains("cluster_state:ok\n"), "{info}");
        assert!(info.contains("cluster_slots_ok:16384\n"), "{info}");
        assert!(info.contains("cluster_slots_pfail:0\n"), "{info}");
        assert!(info.contains("cluster_slots_fail:0\n"), "{info}");

        // p3 stalls for node_timeout; p2 has NOT reported p3 yet -> p3 is PFAIL only.
        let p3_pfail: HashMap<_, _> = [
            make_peer_snapshot("p2", "10.0.0.2:6379", 5461..=10922, 2, 1, 2, &[], &[]),
            make_peer_snapshot("p3", "10.0.0.3:6379", 10923..=16383, 3, 1, 1, &[], &[]),
        ]
        .into();
        mgr.process_peer_snapshots(p3_pfail, t0 + Duration::from_millis(1000))
            .await;
        assert!(mgr.is_pfail("p3"));
        assert!(!mgr.is_failed("p3"));
        let nodes = mgr.get_cluster_nodes().await;
        assert!(
            nodes.contains("p3 10.0.0.3:6379 master,fail? - 0 0 3 disconnected 10923-16383\n"),
            "{nodes}"
        );
        let info = mgr.get_cluster_info().await;
        assert!(info.contains("cluster_slots_pfail:5461\n"), "{info}");
        assert!(info.contains("cluster_slots_fail:0\n"), "{info}");

        // p2 reports p3 PFAIL -> majority (2/3) marks p3 FAIL; slots answer CLUSTERDOWN.
        let p3_fail: HashMap<_, _> = [
            make_peer_snapshot("p2", "10.0.0.2:6379", 5461..=10922, 2, 1, 3, &["p3"], &[]),
            make_peer_snapshot("p3", "10.0.0.3:6379", 10923..=16383, 3, 1, 1, &[], &[]),
        ]
        .into();
        mgr.process_peer_snapshots(p3_fail, t0 + Duration::from_millis(1100))
            .await;
        assert!(mgr.is_failed("p3"));
        assert_eq!(mgr.route(12000, false, false).await, Route::ClusterDown);
        assert_eq!(mgr.route(0, false, false).await, Route::Local);
        let nodes = mgr.get_cluster_nodes().await;
        assert!(
            nodes.contains("p3 10.0.0.3:6379 master,fail - 0 0 3 disconnected 10923-16383\n"),
            "{nodes}"
        );
        let info = mgr.get_cluster_info().await;
        assert!(info.contains("cluster_slots_pfail:0\n"), "{info}");
        assert!(info.contains("cluster_slots_fail:5461\n"), "{info}");

        // p3 restarts (new generation): connected but still `fail` for 2 * node_timeout.
        let p3_back: HashMap<_, _> = [
            make_peer_snapshot("p2", "10.0.0.2:6379", 5461..=10922, 2, 1, 4, &[], &["p3"]),
            make_peer_snapshot("p3", "10.0.0.3:6379", 10923..=16383, 3, 2, 1, &[], &[]),
        ]
        .into();
        mgr.process_peer_snapshots(p3_back, t0 + Duration::from_millis(2000))
            .await;
        assert!(!mgr.is_pfail("p3"));
        assert!(mgr.is_failed("p3"));
        assert_eq!(mgr.route(12000, false, false).await, Route::ClusterDown);
        let nodes = mgr.get_cluster_nodes().await;
        assert!(
            nodes.contains("p3 10.0.0.3:6379 master,fail - 0 0 3 connected 10923-16383\n"),
            "{nodes}"
        );

        // After 2 * node_timeout of reachability (t0 + 4000ms), FAIL clears.
        let p3_mid: HashMap<_, _> = [
            make_peer_snapshot("p2", "10.0.0.2:6379", 5461..=10922, 2, 1, 5, &[], &["p3"]),
            make_peer_snapshot("p3", "10.0.0.3:6379", 10923..=16383, 3, 2, 2, &[], &[]),
        ]
        .into();
        mgr.process_peer_snapshots(p3_mid, t0 + Duration::from_millis(3000))
            .await;
        let p3_clear: HashMap<_, _> = [
            make_peer_snapshot("p2", "10.0.0.2:6379", 5461..=10922, 2, 1, 6, &[], &["p3"]),
            make_peer_snapshot("p3", "10.0.0.3:6379", 10923..=16383, 3, 2, 3, &[], &[]),
        ]
        .into();
        mgr.process_peer_snapshots(p3_clear, t0 + Duration::from_millis(4000))
            .await;
        assert!(!mgr.is_failed("p3"));
        assert_eq!(
            mgr.route(12000, false, false).await,
            Route::Moved("MOVED 12000 10.0.0.3:6379".to_string())
        );
        let info = mgr.get_cluster_info().await;
        assert!(info.contains("cluster_state:ok\n"), "{info}");
    }

    #[tokio::test]
    async fn minority_stops_primary_reads_writes_and_replica_readonly_reads() {
        let t0 = Instant::now();
        let primary = full_cluster_manager("p1", 0..=5460, 1000, t0);
        let peers_all: HashMap<_, _> = [
            make_peer_snapshot("p2", "10.0.0.2:6379", 5461..=10922, 2, 1, 1, &[], &[]),
            make_peer_snapshot("p3", "10.0.0.3:6379", 10923..=16383, 3, 1, 1, &[], &[]),
        ]
        .into();
        primary.process_peer_snapshots(peers_all.clone(), t0).await;
        assert!(!primary.is_in_minority());
        assert_eq!(primary.route(0, false, false).await, Route::Local);
        assert_eq!(primary.route(0, true, false).await, Route::Local);

        // Both p2 and p3 stall for node_timeout -> p1 is in a minority.
        primary
            .process_peer_snapshots(peers_all, t0 + Duration::from_millis(1000))
            .await;
        assert!(primary.is_in_minority());
        assert_eq!(primary.route(0, false, false).await, Route::ClusterDown);
        assert_eq!(primary.route(0, true, false).await, Route::ClusterDown);
        let info = primary.get_cluster_info().await;
        assert!(info.contains("cluster_state:fail\n"), "{info}");

        // p2 comes back -> p1 regains majority and serves again.
        let p2_back: HashMap<_, _> = [
            make_peer_snapshot("p2", "10.0.0.2:6379", 5461..=10922, 2, 1, 2, &[], &[]),
            make_peer_snapshot("p3", "10.0.0.3:6379", 10923..=16383, 3, 1, 1, &[], &[]),
        ]
        .into();
        primary
            .process_peer_snapshots(p2_back, t0 + Duration::from_millis(1200))
            .await;
        assert!(!primary.is_in_minority());
        assert_eq!(primary.route(0, false, false).await, Route::Local);
        assert_eq!(primary.route(0, true, false).await, Route::Local);

        // Synced replica of p1 in the same minority stops serving READONLY reads.
        let replica = into_replica(full_cluster_manager("r1", 0..=0, 1000, t0), "p1", true);
        {
            let mut st = replica.state.write().unwrap();
            st.slots = SlotTable::new();
        }
        let rep_all: HashMap<_, _> = [
            make_peer_snapshot("p1", "10.0.0.1:6379", 0..=5460, 1, 1, 1, &[], &[]),
            make_peer_snapshot("p2", "10.0.0.2:6379", 5461..=10922, 2, 1, 1, &[], &[]),
            make_peer_snapshot("p3", "10.0.0.3:6379", 10923..=16383, 3, 1, 1, &[], &[]),
        ]
        .into();
        replica.process_peer_snapshots(rep_all, t0).await;
        assert_eq!(replica.route(0, true, true).await, Route::Local);

        // p2 and p3 stall while p1 stays reachable -> replica is in a minority.
        let rep_minority: HashMap<_, _> = [
            make_peer_snapshot("p1", "10.0.0.1:6379", 0..=5460, 1, 1, 2, &[], &[]),
            make_peer_snapshot("p2", "10.0.0.2:6379", 5461..=10922, 2, 1, 1, &[], &[]),
            make_peer_snapshot("p3", "10.0.0.3:6379", 10923..=16383, 3, 1, 1, &[], &[]),
        ]
        .into();
        replica
            .process_peer_snapshots(rep_minority, t0 + Duration::from_millis(1000))
            .await;
        assert!(replica.is_in_minority());
        assert_eq!(replica.route(0, true, true).await, Route::ClusterDown);

        // p2 comes back -> replica resumes READONLY reads.
        let rep_recovered: HashMap<_, _> = [
            make_peer_snapshot("p1", "10.0.0.1:6379", 0..=5460, 1, 1, 3, &[], &[]),
            make_peer_snapshot("p2", "10.0.0.2:6379", 5461..=10922, 2, 1, 2, &[], &[]),
            make_peer_snapshot("p3", "10.0.0.3:6379", 10923..=16383, 3, 1, 1, &[], &[]),
        ]
        .into();
        replica
            .process_peer_snapshots(rep_recovered, t0 + Duration::from_millis(1200))
            .await;
        assert!(!replica.is_in_minority());
        assert_eq!(replica.route(0, true, true).await, Route::Local);
    }

    fn reload_test_manager_from_disk(
        path: PathBuf,
        node_id: &str,
        timeout_ms: u64,
        now: Instant,
    ) -> ClusterManager {
        let loaded = ClusterState::load(&path, node_id).unwrap();
        let mut vote_backoff_map = HashMap::new();
        for (failed_primary, &epoch) in &loaded.last_vote_for {
            if epoch > 0 {
                vote_backoff_map.insert(failed_primary.clone(), now);
            }
        }
        let mut mgr = full_cluster_manager(node_id, 0..=0, timeout_ms, now);
        mgr.state_path = Some(path);
        *mgr.state.write().unwrap() = loaded;
        *mgr.vote_backoff_at.lock().unwrap() = vote_backoff_map;
        mgr
    }

    fn auth_req(
        candidate: &str,
        epoch: u64,
        failed_primary: &str,
        primary_config_epoch: u64,
        slots: std::ops::RangeInclusive<u16>,
        manual: bool,
    ) -> FailoverAuthRequest {
        FailoverAuthRequest {
            candidate: candidate.to_string(),
            epoch,
            failed_primary: failed_primary.to_string(),
            primary_config_epoch,
            slots: slots.collect(),
            manual,
        }
    }

    #[tokio::test]
    async fn vote_rules_cover_every_refusal_reason() {
        let dir = tempfile::TempDir::new().unwrap();
        let t0 = Instant::now();
        let mut voter = full_cluster_manager("p2", 5461..=10922, 1000, t0);
        voter.state_path = Some(dir.path().join(CLUSTER_STATE_FILE));
        {
            let mut st = voter.state.write().unwrap();
            st.current_epoch = 4;
            st.config_epoch = 2;
            st.slots.rebind_owner_epoch("p2", 2);
            for s in 0..=5460 {
                st.slots.set(s, "p1", 3);
            }
            for s in 10923..=16383 {
                st.slots.set(s, "p3", 4);
            }
        }
        add_replica(&voter, "r1", "10.0.0.10:6379", "p1", NodeRole::Replica).await;
        add_replica(&voter, "r2", "10.0.0.11:6379", "p1", NodeRole::Replica).await;
        add_replica(&voter, "r3", "10.0.0.12:6379", "p3", NodeRole::Replica).await;

        // 1. Primary not marked FAIL (non-manual) -> refused.
        let err = voter
            .handle_failover_auth_at(&auth_req("r1", 5, "p1", 3, 0..=5460, false), t0)
            .await
            .unwrap_err();
        assert!(err.contains("not marked FAIL"), "{err}");

        // Mark p1 FAIL.
        voter
            .failure
            .write()
            .unwrap()
            .mark_failed_for_test("p1", t0);

        // 2. epoch < current_epoch -> refused.
        let err = voter
            .handle_failover_auth_at(&auth_req("r1", 3, "p1", 3, 0..=5460, false), t0)
            .await
            .unwrap_err();
        assert!(err.contains("current_epoch"), "{err}");

        // 3. Unknown candidate or replica of another primary -> refused.
        let err = voter
            .handle_failover_auth_at(&auth_req("ghost", 5, "p1", 3, 0..=5460, false), t0)
            .await
            .unwrap_err();
        assert!(err.contains("not a known replica"), "{err}");
        let err = voter
            .handle_failover_auth_at(&auth_req("r3", 5, "p1", 3, 0..=5460, false), t0)
            .await
            .unwrap_err();
        assert!(err.contains("not a known replica"), "{err}");

        // 4. Stale primary-config-epoch (slot epoch 3 > requested 2) -> refused.
        let err = voter
            .handle_failover_auth_at(&auth_req("r1", 5, "p1", 2, 0..=5460, false), t0)
            .await
            .unwrap_err();
        assert!(err.contains("newer epoch"), "{err}");

        // 5. All checks pass -> granted, epoch adopted & persisted.
        assert_eq!(
            voter
                .handle_failover_auth_at(&auth_req("r1", 5, "p1", 3, 0..=5460, false), t0)
                .await
                .unwrap(),
            5
        );
        let st = voter.state_snapshot();
        assert_eq!(st.current_epoch, 5);
        assert_eq!(st.last_vote_epoch, 5);
        assert_eq!(st.last_vote_for.get("p1").copied(), Some(5));

        // 6. Duplicate epoch (epoch <= last_vote_epoch) -> refused.
        let err = voter
            .handle_failover_auth_at(&auth_req("r2", 5, "p1", 3, 0..=5460, false), t0)
            .await
            .unwrap_err();
        assert!(err.contains("already voted"), "{err}");

        // 7. Higher epoch within vote backoff (2 * election_timeout = 4000ms for nt=1000ms) -> refused.
        let err = voter
            .handle_failover_auth_at(
                &auth_req("r2", 6, "p1", 3, 0..=5460, false),
                t0 + Duration::from_millis(3999),
            )
            .await
            .unwrap_err();
        assert!(err.contains("vote backoff"), "{err}");

        // 8. Different failed primary with MANUAL (no FAIL needed) still allowed at epoch 6.
        assert_eq!(
            voter
                .handle_failover_auth_at(
                    &auth_req("r3", 6, "p3", 4, 10923..=16383, true),
                    t0 + Duration::from_millis(500),
                )
                .await
                .unwrap(),
            6
        );

        // 9. After vote backoff (4000ms) elapses, p1's backoff expires -> vote at epoch 7 granted.
        assert_eq!(
            voter
                .handle_failover_auth_at(
                    &auth_req("r2", 7, "p1", 3, 0..=5460, false),
                    t0 + Duration::from_millis(4000),
                )
                .await
                .unwrap(),
            7
        );

        // 10. Replica voter -> refused.
        let replica_voter = into_replica(full_cluster_manager("rx", 0..=0, 1000, t0), "p1", true);
        let err = replica_voter
            .handle_failover_auth_at(&auth_req("r1", 8, "p1", 3, 0..=5460, true), t0)
            .await
            .unwrap_err();
        assert!(err.contains("not a primary"), "{err}");

        // 11. Slotless primary voter -> refused.
        let slotless = full_cluster_manager("p-empty", 0..=0, 1000, t0);
        slotless.state.write().unwrap().slots.clear(0);
        let err = slotless
            .handle_failover_auth_at(&auth_req("r1", 8, "p1", 3, 0..=5460, true), t0)
            .await
            .unwrap_err();
        assert!(err.contains("owns no slots"), "{err}");

        // 12. Demoting primary voter -> refused.
        voter.state.write().unwrap().demoting_to = Some("r1".to_string());
        let err = voter
            .handle_failover_auth_at(
                &auth_req("r2", 8, "p1", 3, 0..=5460, true),
                t0 + Duration::from_millis(5000),
            )
            .await
            .unwrap_err();
        assert!(err.contains("not a primary"), "{err}");
    }

    #[tokio::test]
    async fn vote_and_backoff_persist_across_restart() {
        let dir = tempfile::TempDir::new().unwrap();
        let state_path = dir.path().join(CLUSTER_STATE_FILE);
        let t0 = Instant::now();
        let mut voter = full_cluster_manager("p2", 5461..=10922, 1000, t0);
        voter.state_path = Some(state_path.clone());
        {
            let mut st = voter.state.write().unwrap();
            for s in 0..=5460 {
                st.slots.set(s, "p1", 1);
            }
        }
        add_replica(&voter, "r1", "10.0.0.10:6379", "p1", NodeRole::Replica).await;
        add_replica(&voter, "r2", "10.0.0.11:6379", "p1", NodeRole::Replica).await;
        voter
            .failure
            .write()
            .unwrap()
            .mark_failed_for_test("p1", t0);

        // Cast vote at epoch 5; must already be on disk before returning.
        assert_eq!(
            voter
                .handle_failover_auth_at(&auth_req("r1", 5, "p1", 1, 0..=5460, false), t0)
                .await
                .unwrap(),
            5
        );
        let on_disk = ClusterState::load(&state_path, "p2").unwrap();
        assert_eq!(on_disk.current_epoch, 5);
        assert_eq!(on_disk.last_vote_epoch, 5);
        assert_eq!(on_disk.last_vote_for.get("p1").copied(), Some(5));

        // Simulate restart at t0 + 100ms by reloading from disk.
        let t_restart = t0 + Duration::from_millis(100);
        let restarted = reload_test_manager_from_disk(state_path.clone(), "p2", 1000, t_restart);
        add_replica(&restarted, "r1", "10.0.0.10:6379", "p1", NodeRole::Replica).await;
        add_replica(&restarted, "r2", "10.0.0.11:6379", "p1", NodeRole::Replica).await;
        restarted
            .failure
            .write()
            .unwrap()
            .mark_failed_for_test("p1", t_restart);

        // Same epoch is blocked by persisted last_vote_epoch.
        let err = restarted
            .handle_failover_auth_at(
                &auth_req("r2", 5, "p1", 1, 0..=5460, false),
                t_restart + Duration::from_millis(10),
            )
            .await
            .unwrap_err();
        assert!(err.contains("already voted"), "{err}");

        // Next epoch within vote backoff (4000ms for nt=1000ms) is blocked by persisted last_vote_for.
        let err = restarted
            .handle_failover_auth_at(
                &auth_req("r2", 6, "p1", 1, 0..=5460, false),
                t_restart + Duration::from_millis(3999),
            )
            .await
            .unwrap_err();
        assert!(err.contains("vote backoff"), "{err}");

        // Once backoff expires after restart, a vote in epoch 6 is allowed.
        assert_eq!(
            restarted
                .handle_failover_auth_at(
                    &auth_req("r2", 6, "p1", 1, 0..=5460, false),
                    t_restart + Duration::from_millis(4000),
                )
                .await
                .unwrap(),
            6
        );

        // Even with small node_timeout_ms (< 1000ms), vote_backoff enforces the 4s floor
        // (2 * election_timeout >= 4s) so backoff never expires before the election window ends.
        let small_dir = tempfile::TempDir::new().unwrap();
        let mut small_voter = full_cluster_manager("p2", 5461..=10922, 200, t0);
        small_voter.state_path = Some(small_dir.path().join(CLUSTER_STATE_FILE));
        {
            let mut st = small_voter.state.write().unwrap();
            for s in 0..=5460 {
                st.slots.set(s, "p1", 1);
            }
        }
        add_replica(
            &small_voter,
            "r1",
            "10.0.0.10:6379",
            "p1",
            NodeRole::Replica,
        )
        .await;
        add_replica(
            &small_voter,
            "r2",
            "10.0.0.11:6379",
            "p1",
            NodeRole::Replica,
        )
        .await;
        small_voter
            .failure
            .write()
            .unwrap()
            .mark_failed_for_test("p1", t0);
        assert_eq!(
            small_voter
                .handle_failover_auth_at(&auth_req("r1", 5, "p1", 1, 0..=5460, false), t0)
                .await
                .unwrap(),
            5
        );
        let err = small_voter
            .handle_failover_auth_at(
                &auth_req("r2", 6, "p1", 1, 0..=5460, false),
                t0 + Duration::from_millis(3999),
            )
            .await
            .unwrap_err();
        assert!(err.contains("vote backoff"), "{err}");
        assert_eq!(
            small_voter
                .handle_failover_auth_at(
                    &auth_req("r2", 6, "p1", 1, 0..=5460, false),
                    t0 + Duration::from_millis(4000),
                )
                .await
                .unwrap(),
            6
        );
    }

    #[tokio::test]
    async fn concurrent_votes_are_serialized() {
        let dir = tempfile::TempDir::new().unwrap();
        let t0 = Instant::now();
        let mut voter = full_cluster_manager("p2", 5461..=10922, 1000, t0);
        voter.state_path = Some(dir.path().join(CLUSTER_STATE_FILE));
        {
            let mut st = voter.state.write().unwrap();
            for s in 0..=5460 {
                st.slots.set(s, "p1", 1);
            }
        }
        add_replica(&voter, "r1", "10.0.0.10:6379", "p1", NodeRole::Replica).await;
        add_replica(&voter, "r2", "10.0.0.11:6379", "p1", NodeRole::Replica).await;
        voter
            .failure
            .write()
            .unwrap()
            .mark_failed_for_test("p1", t0);

        let req1 = auth_req("r1", 5, "p1", 1, 0..=5460, false);
        let req2 = auth_req("r2", 5, "p1", 1, 0..=5460, false);
        let (res1, res2) = tokio::join!(
            voter.handle_failover_auth_at(&req1, t0),
            voter.handle_failover_auth_at(&req2, t0),
        );
        assert_eq!(
            [res1.is_ok(), res2.is_ok()]
                .into_iter()
                .filter(|ok| *ok)
                .count(),
            1,
            "exactly one concurrent vote must succeed: res1={res1:?}, res2={res2:?}"
        );
    }

    #[tokio::test]
    async fn majority_counting_includes_dead_owner_in_denominator() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        async fn spawn_mock_voter(reply: &'static str) -> SocketAddr {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            tokio::spawn(async move {
                if let Ok((mut stream, _)) = listener.accept().await {
                    let mut buf = [0u8; 512];
                    let _ = stream.read(&mut buf).await;
                    let _ = stream.write_all(reply.as_bytes()).await;
                }
            });
            addr
        }

        // Bind a dead port (dropped immediately so connections fail fast).
        let dead_addr = {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            listener.local_addr().unwrap()
        };

        let owners: BTreeSet<String> = ["p1", "p2", "p3"].into_iter().map(String::from).collect();
        let slots: HashSet<u16> = (0..=5460).collect();

        // 1) 3 owners (p1 dead, p2 votes +VOTE 5, p3 refuses): 1 < quorum 2 -> NoMajority.
        let p2_ok = spawn_mock_voter("+VOTE 5\r\n").await;
        let p3_err = spawn_mock_voter("-ERR primary p1 is not marked FAIL\r\n").await;
        let plan = ElectionPlan {
            candidate: "r1".into(),
            epoch: 5,
            failed_primary: "p1".into(),
            primary_config_epoch: 1,
            slots: slots.clone(),
            owners: owners.clone(),
            targets: vec![
                ("p1".into(), dead_addr),
                ("p2".into(), p2_ok),
                ("p3".into(), p3_err),
            ],
            quorum: 2,
            manual: false,
        };
        let shutdown = CancellationToken::new();
        let outcome = collect_votes(
            &plan,
            Instant::now() + Duration::from_secs(2),
            &shutdown,
            || false,
        )
        .await;
        assert!(
            matches!(outcome, VoteCollectionOutcome::NoMajority { ref votes, quorum: 2 } if votes.len() == 1),
            "{outcome:?}"
        );

        // 2) Wrong epoch reply (+VOTE 4) must not count toward epoch 5.
        let p2_ok = spawn_mock_voter("+VOTE 5\r\n").await;
        let p3_wrong_epoch = spawn_mock_voter("+VOTE 4\r\n").await;
        let plan_wrong = ElectionPlan {
            targets: vec![
                ("p1".into(), dead_addr),
                ("p2".into(), p2_ok),
                ("p3".into(), p3_wrong_epoch),
            ],
            ..plan.clone()
        };
        let outcome = collect_votes(
            &plan_wrong,
            Instant::now() + Duration::from_secs(2),
            &shutdown,
            || false,
        )
        .await;
        assert!(
            matches!(outcome, VoteCollectionOutcome::NoMajority { ref votes, quorum: 2 } if votes.len() == 1),
            "{outcome:?}"
        );

        // 3) Both p2 and p3 vote +VOTE 5 -> 2 >= quorum 2 -> Won!
        let p2_ok = spawn_mock_voter("+VOTE 5\r\n").await;
        let p3_ok = spawn_mock_voter("+VOTE 5\r\n").await;
        let plan_win = ElectionPlan {
            targets: vec![
                ("p1".into(), dead_addr),
                ("p2".into(), p2_ok),
                ("p3".into(), p3_ok),
            ],
            ..plan
        };
        let outcome = collect_votes(
            &plan_win,
            Instant::now() + Duration::from_secs(2),
            &shutdown,
            || false,
        )
        .await;
        assert!(
            matches!(outcome, VoteCollectionOutcome::Won { ref votes } if votes.len() == 2),
            "{outcome:?}"
        );
    }

    #[tokio::test]
    async fn eligibility_and_rank_respect_synced_replids_and_validity_factor() {
        let t0 = Instant::now();
        let mut mgr = into_replica(
            manager_with_id("r-b", &[], &[("p1", "10.0.0.1:6379", &[0, 1])]),
            "p1",
            false,
        );
        mgr.config.node_timeout_ms = Some(1000);
        mgr.config.replica_validity_factor = Some(2); // max_age = 2000ms before failure

        // Primary gossips its 2-shard replids.
        let p1_repl = vec![
            ShardReplState {
                replid: "rep-0".into(),
                caught_up_lsn: 0,
            },
            ShardReplState {
                replid: "rep-1".into(),
                caught_up_lsn: 0,
            },
        ];
        mgr.known_peers
            .write()
            .unwrap()
            .get_mut("p1")
            .unwrap()
            .shard_repl = p1_repl.clone();

        // Mark p1 FAIL at t0 + 5000ms (estimated failure_time = t0 + 4000ms).
        let fail_t = t0 + Duration::from_millis(5000);
        mgr.failure
            .write()
            .unwrap()
            .mark_failed_for_test("p1", fail_t);

        // Unsynced -> ineligible.
        assert_eq!(mgr.check_election_eligibility().await, None);

        // Sync both shards with stale frames (at t0 + 1000ms -> 3000ms before failure_time > 2000ms).
        mgr.update_replica_shard_progress(0, "rep-0", 100, true, t0 + Duration::from_millis(1000))
            .await;
        mgr.update_replica_shard_progress(1, "rep-1", 50, true, t0 + Duration::from_millis(1000))
            .await;
        assert_eq!(mgr.check_election_eligibility().await, None);

        // With replica_validity_factor = 0, staleness check is disabled -> eligible!
        mgr.config.replica_validity_factor = Some(0);
        assert_eq!(
            mgr.check_election_eligibility().await.as_deref(),
            Some("p1")
        );

        // Re-enable factor = 2 and update frames to t0 + 3500ms (500ms before failure_time <= 2000ms).
        mgr.config.replica_validity_factor = Some(2);
        mgr.update_replica_shard_progress(0, "rep-0", 100, true, t0 + Duration::from_millis(3500))
            .await;
        mgr.update_replica_shard_progress(1, "rep-1", 50, true, t0 + Duration::from_millis(3500))
            .await;
        assert_eq!(
            mgr.check_election_eligibility().await.as_deref(),
            Some("p1")
        );

        // Mismatched replid on shard 1 -> ineligible.
        mgr.update_replica_shard_progress(1, "wrong", 50, true, t0 + Duration::from_millis(3500))
            .await;
        assert_eq!(mgr.check_election_eligibility().await, None);
        mgr.update_replica_shard_progress(1, "rep-1", 50, true, t0 + Duration::from_millis(3500))
            .await;

        // Rank test: r-b has total 150.
        // Add r-c with higher total 200 (same replids) -> r-b becomes rank 1.
        // Add r-a with tied total 150 ("r-a" < "r-b") -> r-b becomes rank 2.
        // Add r-wrong with huge total 9999 but mismatched replid -> ignored!
        // Add r-unsynced with huge total 9999 but ReplicaSyncing -> ignored!
        for (id, role, repl) in [
            (
                "r-c",
                NodeRole::Replica,
                vec![
                    ShardReplState {
                        replid: "rep-0".into(),
                        caught_up_lsn: 120,
                    },
                    ShardReplState {
                        replid: "rep-1".into(),
                        caught_up_lsn: 80,
                    },
                ],
            ),
            (
                "r-a",
                NodeRole::Replica,
                vec![
                    ShardReplState {
                        replid: "rep-0".into(),
                        caught_up_lsn: 75,
                    },
                    ShardReplState {
                        replid: "rep-1".into(),
                        caught_up_lsn: 75,
                    },
                ],
            ),
            (
                "r-wrong",
                NodeRole::Replica,
                vec![
                    ShardReplState {
                        replid: "other".into(),
                        caught_up_lsn: 9999,
                    },
                    ShardReplState {
                        replid: "rep-1".into(),
                        caught_up_lsn: 0,
                    },
                ],
            ),
            (
                "r-unsynced",
                NodeRole::ReplicaSyncing,
                vec![
                    ShardReplState {
                        replid: "rep-0".into(),
                        caught_up_lsn: 9999,
                    },
                    ShardReplState {
                        replid: "rep-1".into(),
                        caught_up_lsn: 0,
                    },
                ],
            ),
        ] {
            add_replica(&mgr, id, "10.0.0.20:6379", "p1", role).await;
            mgr.nodes.write().await.get_mut(id).unwrap().shard_repl = repl;
        }
        assert_eq!(mgr.compute_replica_rank("p1").await, 2);
    }

    #[tokio::test]
    async fn elected_promotion_aborts_if_deadline_expires_before_commit() {
        let dir = tempfile::TempDir::new().unwrap();
        let state_path = dir.path().join(CLUSTER_STATE_FILE);
        let mut mgr = into_replica(
            manager_with_id("r1", &[], &[("p1", "10.0.0.1:6379", &[0, 1])]),
            "p1",
            true,
        );
        mgr.state_path = Some(state_path.clone());
        {
            let mut st = mgr.state.write().unwrap();
            st.current_epoch = 5;
            st.slots.rebind_owner_epoch("p1", 1);
            st.save_atomic(&state_path).unwrap();
        }

        let expected_slots: HashSet<u16> = [0, 1].into_iter().collect();

        // 1) Already-expired deadline is rejected by can_promote and promote.
        let expired_kind = PromotionKind::Elected {
            election_epoch: 5,
            failed_primary: "p1".into(),
            primary_config_epoch: 1,
            expected_slots: expected_slots.clone(),
            deadline: Instant::now() - Duration::from_millis(1),
        };
        let err = mgr.can_promote(&expired_kind).unwrap_err();
        assert!(err.contains("deadline"), "{err}");

        let ran_shards = Arc::new(AtomicBool::new(false));
        let ran_flag = ran_shards.clone();
        let res = mgr
            .promote(expired_kind, move || async move {
                ran_flag.store(true, Ordering::SeqCst);
                Ok(())
            })
            .await;
        assert!(
            matches!(res, Err(PromoteError::BeforeCommit(ref e)) if e.contains("deadline")),
            "{res:?}"
        );
        assert!(!ran_shards.load(Ordering::SeqCst));
        assert!(mgr.is_replica());
        let on_disk = ClusterState::load(&state_path, "r1").unwrap();
        assert_eq!(on_disk.role, PersistedRole::Replica);
        assert_eq!(on_disk.primary.as_deref(), Some("p1"));
        assert_eq!(on_disk.slots.get(0).unwrap().node_id, "p1");

        // 2) Deadline valid at can_promote, then expires before commit (simulated
        // by holding write_lock past the deadline as a slow sync stop / apply would).
        let short_deadline = Instant::now() + Duration::from_millis(40);
        let delayed_kind = PromotionKind::Elected {
            election_epoch: 5,
            failed_primary: "p1".into(),
            primary_config_epoch: 1,
            expected_slots,
            deadline: short_deadline,
        };
        assert!(mgr.can_promote(&delayed_kind).is_ok());

        let write_guard = mgr.write_lock.lock().await;
        let mgr_clone = mgr.clone();
        let ran_flag = ran_shards.clone();
        let promote_task = tokio::spawn(async move {
            mgr_clone
                .promote(delayed_kind, move || async move {
                    ran_flag.store(true, Ordering::SeqCst);
                    Ok(())
                })
                .await
        });
        tokio::time::sleep(Duration::from_millis(60)).await;
        drop(write_guard);

        let res = promote_task.await.unwrap();
        assert!(
            matches!(res, Err(PromoteError::BeforeCommit(ref e)) if e.contains("deadline")),
            "{res:?}"
        );
        assert!(!ran_shards.load(Ordering::SeqCst));
        assert!(mgr.is_replica());
        let on_disk = ClusterState::load(&state_path, "r1").unwrap();
        assert_eq!(on_disk.role, PersistedRole::Replica);
        assert_eq!(on_disk.primary.as_deref(), Some("p1"));
        assert_eq!(on_disk.slots.get(0).unwrap().node_id, "p1");
    }

    #[tokio::test]
    async fn mfstart_validation_catch_up_and_cluster_info_counters() {
        let primary = make_test_manager(0..=5460);
        assert!(primary.owns_slot(0));
        assert!(primary.owns_slot(5460));
        assert!(!primary.owns_slot(5461));

        // Unknown replica is rejected.
        assert!(primary.validate_mfstart("r1").await.is_err());

        // Add r1 as replica of self ("node-a") and r2 as replica of another primary.
        {
            let mut nodes = primary.nodes.write().await;
            nodes.insert(
                "r1".to_string(),
                ClusterNode {
                    id: "r1".to_string(),
                    addr: "127.0.0.1:6402".parse().unwrap(),
                    slots: HashSet::new(),
                    _state: "connected".to_string(),
                    role: NodeRole::Replica,
                    primary: Some("node-a".to_string()),
                    current_epoch: 0,
                    config_epoch: 0,
                    shard_repl: Vec::new(),
                },
            );
            nodes.insert(
                "r2".to_string(),
                ClusterNode {
                    id: "r2".to_string(),
                    addr: "127.0.0.1:6403".parse().unwrap(),
                    slots: HashSet::new(),
                    _state: "connected".to_string(),
                    role: NodeRole::Replica,
                    primary: Some("node-2".to_string()),
                    current_epoch: 0,
                    config_epoch: 0,
                    shard_repl: Vec::new(),
                },
            );
        }
        assert!(primary.validate_mfstart("r1").await.is_ok());
        assert!(primary.validate_mfstart("r2").await.is_err());

        // Demoting primary rejects MFSTART and reports owns_slot == false.
        primary.state.write().unwrap().demoting_to = Some("r1".to_string());
        assert!(!primary.owns_slot(0));
        assert!(primary.validate_mfstart("r1").await.is_err());

        // Replica catch-up semantics: requires synced == true, matching non-empty replid,
        // and caught_up_lsn >= target on every shard (including empty shards at LSN 0).
        let replica = into_replica(make_test_manager(0..=0), "p1", false);
        let target = vec![("rep-0".to_string(), 10), ("rep-1".to_string(), 0)];
        assert!(!replica.replica_caught_up_to(&target));

        let now = Instant::now();
        replica
            .update_replica_shard_progress(0, "rep-0", 9, true, now)
            .await;
        replica
            .update_replica_shard_progress(1, "rep-1", 0, true, now)
            .await;
        // Shard 0 is still behind (9 < 10).
        assert!(!replica.replica_caught_up_to(&target));

        // Wrong replid on shard 0 even with higher LSN is not caught up.
        replica
            .update_replica_shard_progress(0, "wrong", 20, true, now)
            .await;
        assert!(!replica.replica_caught_up_to(&target));

        // Exact match on both shards passes.
        replica
            .update_replica_shard_progress(0, "rep-0", 10, true, now)
            .await;
        assert!(replica.replica_caught_up_to(&target));

        // Resetting shard 1 clears synced and blocks catch-up even for LSN 0.
        replica.reset_shard_replica_state(1, "rep-1").await;
        assert!(!replica.replica_caught_up_to(&target));

        // Manual failover counters appear in CLUSTER INFO.
        replica.record_manual_failover_started();
        replica.record_manual_failover_started();
        replica.record_manual_failover_succeeded();
        replica.record_manual_failover_aborted();
        let info = replica.get_cluster_info().await;
        assert!(info.contains("cluster_stats_manual_failover_started:2"));
        assert!(info.contains("cluster_stats_manual_failover_succeeded:1"));
        assert!(info.contains("cluster_stats_manual_failover_aborted:1"));
    }
}
