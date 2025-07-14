use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use chitchat::{ChitchatConfig, ChitchatHandle, ChitchatId, FailureDetectorConfig, spawn_chitchat};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tokio::time::interval;
use tracing::{debug, error, info, warn};

/// Strategy for distributing namespace operations across cluster nodes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NamespaceDistributionStrategy {
    /// Use namespace for slot calculation (legacy behavior - confines namespace to one node)
    #[allow(dead_code)]
    NamespaceBased,
    /// Use key for slot calculation (distributes namespace across nodes)
    KeyBased,
    /// Use hash tags when present, fallback to key-based
    HashTagAware,
}

impl Default for NamespaceDistributionStrategy {
    fn default() -> Self {
        Self::KeyBased
    }
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
}

/// Gossip data structure for sharing node information
#[derive(Debug, Clone, Serialize, Deserialize)]
struct NodeGossipData {
    pub addr: String,
    pub slots: Vec<u16>,
    pub state: String,
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
}

impl ClusterManager {
    /// Create a new cluster manager
    pub async fn new(
        config: &ClusterConfig,
        gossip_bind_addr: SocketAddr,
        redis_addr: SocketAddr,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        // Initialize local slots from either slots array or slot ranges
        let mut slot_set = HashSet::new();

        // Add slots from slots array if present
        if let Some(ref slots) = config.slots {
            slot_set.extend(slots.iter().copied());
        }

        // Add slots from slot ranges if present
        if let Some(ref slot_ranges) = config.slot_ranges {
            for range in slot_ranges {
                for slot in range.start..=range.end {
                    slot_set.insert(slot);
                }
            }
        }

        let local_slots = Arc::new(RwLock::new(slot_set));

        let nodes = Arc::new(RwLock::new(HashMap::new()));

        // Set up chitchat for gossip protocol
        let gossip_addr = SocketAddr::new(gossip_bind_addr.ip(), config.port);

        // Use advertise_addr if provided, otherwise fall back to gossip_addr
        let advertise_gossip_addr = if let Some(ref advertise_addr_str) = config.advertise_addr {
            advertise_addr_str.parse().unwrap_or(gossip_addr)
        } else {
            gossip_addr
        };

        let chitchat_id = ChitchatId {
            node_id: config.node_id.clone(),
            generation_id: 0,
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
        let initial_gossip_data = NodeGossipData {
            addr: format!(
                "{}:{}",
                if redis_addr.ip().is_unspecified() {
                    "127.0.0.1".to_string()
                } else {
                    redis_addr.ip().to_string()
                },
                redis_addr.port()
            ),
            slots: {
                let mut slots = Vec::new();
                // Add slots from slots array if present
                if let Some(ref slot_list) = config.slots {
                    slots.extend(slot_list.iter().copied());
                }
                // Add slots from slot ranges if present
                if let Some(ref slot_ranges) = config.slot_ranges {
                    for range in slot_ranges {
                        for slot in range.start..=range.end {
                            slots.push(slot);
                        }
                    }
                }
                slots
            },
            state: "master".to_string(),
        };

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
        };

        // Start gossip processing task
        let gossip_manager = cluster_manager.clone();
        tokio::spawn(async move {
            gossip_manager.gossip_task().await;
        });

        let total_slots = {
            let slots_count = config.slots.as_ref().map(|s| s.len()).unwrap_or(0);
            let ranges_count: usize = config
                .slot_ranges
                .as_ref()
                .map(|ranges| ranges.iter().map(|r| (r.end - r.start + 1) as usize).sum())
                .unwrap_or(0);
            slots_count + ranges_count
        };

        info!(
            "Cluster manager initialized for node: {} with {} local slots",
            config.node_id, total_slots
        );

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
                let mut nodes_map = HashMap::new();

                // Log cluster state for debugging
                let total_nodes = chitchat_guard.node_states().len();
                let live_nodes: Vec<_> =
                    chitchat_guard.live_nodes().map(|id| &id.node_id).collect();
                let dead_nodes: Vec<_> =
                    chitchat_guard.dead_nodes().map(|id| &id.node_id).collect();

                info!(
                    "Gossip check for {}: {} total nodes, {} live nodes: {:?}, {} dead nodes: {:?}",
                    self.node_id,
                    total_nodes,
                    live_nodes.len(),
                    live_nodes,
                    dead_nodes.len(),
                    dead_nodes
                );

                debug!(
                    "Gossip update: {} total nodes, {} live nodes: {:?}, {} dead nodes: {:?}",
                    total_nodes,
                    live_nodes.len(),
                    live_nodes,
                    dead_nodes.len(),
                    dead_nodes
                );

                // Iterate through all nodes in the cluster
                for (chitchat_id, node_state) in chitchat_guard.node_states() {
                    if chitchat_id.node_id == self.node_id {
                        continue; // Skip self
                    }

                    debug!("Processing node: {}", chitchat_id.node_id);

                    if let Some(node_info_value) = node_state.get("node_info") {
                        debug!("Node {} has gossip data", chitchat_id.node_id);
                        match serde_json::from_str::<NodeGossipData>(node_info_value) {
                            Ok(gossip_data) => {
                                if let Ok(addr) = gossip_data.addr.parse::<SocketAddr>() {
                                    let cluster_node = ClusterNode {
                                        id: chitchat_id.node_id.clone(),
                                        addr,
                                        slots: gossip_data.slots.into_iter().collect(),
                                        _state: gossip_data.state,
                                    };
                                    info!(
                                        "Discovered cluster node: {} at {} with {} slots",
                                        cluster_node.id,
                                        cluster_node.addr,
                                        cluster_node.slots.len()
                                    );
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

                drop(chitchat_guard);

                // Update the nodes map and log changes
                let mut nodes = self.nodes.write().await;
                let prev_count = nodes.len();
                *nodes = nodes_map;
                let new_count = nodes.len();

                if prev_count != new_count {
                    info!(
                        "Cluster membership changed for {}: {} -> {} nodes",
                        self.node_id, prev_count, new_count
                    );
                    debug!("Cluster nodes: {} total", new_count);
                } else if new_count > 0 {
                    debug!(
                        "Cluster membership stable for {}: {} nodes",
                        self.node_id, new_count
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

    /// Get the node responsible for a given slot
    pub async fn get_node_for_slot(&self, slot: u16) -> Option<ClusterNode> {
        let nodes = self.nodes.read().await;

        // First check if we own this slot
        let local_slots = self.local_slots.read().await;
        if local_slots.contains(&slot) {
            return None; // We handle this slot locally
        }

        // Find the node that owns this slot
        for node in nodes.values() {
            if node.slots.contains(&slot) {
                return Some(node.clone());
            }
        }

        None
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
            NamespaceDistributionStrategy::KeyBased => {
                // Use individual key for distribution
                Self::calculate_slot_with_strategy(key, self.namespace_strategy)
            }
            NamespaceDistributionStrategy::HashTagAware => {
                // Check if key has hash tag, otherwise use key
                let combined_key = if key.contains('{') && key.contains('}') {
                    key
                } else {
                    key
                };
                Self::calculate_slot_with_strategy(combined_key, self.namespace_strategy)
            }
        }
    }

    /// Check if we should handle a hash operation locally
    pub async fn should_handle_hash_locally(&self, namespace: &str, key: &str) -> bool {
        let slot = self.calculate_slot_for_hash(namespace, key);
        let local_slots = self.local_slots.read().await;
        local_slots.contains(&slot)
    }

    /// Get redirect response for a hash operation
    pub async fn get_hash_redirect_response(&self, namespace: &str, key: &str) -> Option<String> {
        let slot = self.calculate_slot_for_hash(namespace, key);
        if let Some(node) = self.get_node_for_slot(slot).await {
            Some(format!("MOVED {} {}", slot, node.addr))
        } else {
            None
        }
    }

    /// Get cluster nodes information for CLUSTER NODES command
    pub async fn get_cluster_nodes(&self) -> String {
        let mut result = Vec::new();
        let nodes = self.nodes.read().await;
        let local_slots = self.local_slots.read().await;

        // Add local node
        let local_slots_ranges = Self::slots_to_ranges(&local_slots);
        let local_info = format!(
            "{} {}:{} myself,master - 0 0 0 connected {}",
            self.node_id,
            if self.local_addr.ip().is_unspecified() {
                "127.0.0.1".to_string()
            } else {
                self.local_addr.ip().to_string()
            },
            self.local_addr.port(),
            local_slots_ranges
        );
        result.push(local_info);

        // Add other nodes
        for node in nodes.values() {
            let slots_ranges = Self::slots_to_ranges(&node.slots);
            let node_info = format!(
                "{} {}:{} master - 0 0 0 connected {}",
                node.id,
                node.addr.ip(),
                node.addr.port(),
                slots_ranges
            );
            result.push(node_info);
        }

        result.join("\n")
    }

    /// Convert slots set to Redis slot ranges format
    fn slots_to_ranges(slots: &HashSet<u16>) -> String {
        if slots.is_empty() {
            return String::new();
        }

        let mut sorted_slots: Vec<u16> = slots.iter().copied().collect();
        sorted_slots.sort();

        let mut ranges = Vec::new();
        let mut start = sorted_slots[0];
        let mut end = sorted_slots[0];

        for &slot in &sorted_slots[1..] {
            if slot == end + 1 {
                end = slot;
            } else {
                if start == end {
                    ranges.push(start.to_string());
                } else {
                    ranges.push(format!("{}-{}", start, end));
                }
                start = slot;
                end = slot;
            }
        }

        // Add the last range
        if start == end {
            ranges.push(start.to_string());
        } else {
            ranges.push(format!("{}-{}", start, end));
        }

        ranges.join(" ")
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
             cluster_stats_messages_received:0",
            cluster_state, total_assigned_slots, total_assigned_slots, total_nodes, total_nodes
        )
    }

    /// Check if we should handle a key locally or redirect
    pub async fn should_handle_locally(&self, key: &str) -> bool {
        let slot = Self::calculate_slot(key);
        let local_slots = self.local_slots.read().await;
        local_slots.contains(&slot)
    }

    /// Get redirect response for a key
    pub async fn get_redirect_response(&self, key: &str) -> Option<String> {
        let slot = Self::calculate_slot(key);
        if let Some(node) = self.get_node_for_slot(slot).await {
            Some(format!(
                "MOVED {} {}:{}",
                slot,
                node.addr.ip(),
                node.addr.port()
            ))
        } else {
            None
        }
    }

    /// Add slots to this node
    pub async fn add_slots(
        &self,
        slots: Vec<u16>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut local_slots = self.local_slots.write().await;
        for slot in &slots {
            local_slots.insert(*slot);
        }
        drop(local_slots);

        // Update gossip data
        self.update_gossip_data().await;

        info!("Added slots {:?} to local node", slots);
        Ok(())
    }

    /// Remove slots from this node
    pub async fn remove_slots(
        &self,
        slots: Vec<u16>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut local_slots = self.local_slots.write().await;
        for slot in &slots {
            local_slots.remove(slot);
        }
        drop(local_slots);

        // Update gossip data
        self.update_gossip_data().await;

        info!("Removed slots {:?} from local node", slots);
        Ok(())
    }

    /// Update gossip data with current slot assignments
    async fn update_gossip_data(&self) {
        if let Some(handle) = self.chitchat_handle.as_ref() {
            let updated_slots: Vec<u16> = self.local_slots.read().await.iter().copied().collect();
            let gossip_data = NodeGossipData {
                addr: self
                    .config
                    .advertise_addr
                    .as_deref()
                    .unwrap_or(&format!(
                        "{}:{}",
                        self.local_addr.ip(),
                        self.local_addr.port()
                    ))
                    .to_string(),
                slots: updated_slots,
                state: "master".to_string(),
            };

            if let Ok(gossip_data_json) = serde_json::to_string(&gossip_data) {
                debug!("Updating gossip data: {}", gossip_data_json);
                let chitchat_arc = handle.chitchat();
                let mut chitchat_guard = chitchat_arc.lock().await;
                chitchat_guard
                    .self_node_state()
                    .set("node_info", &gossip_data_json);
                info!("Updated gossip data with {} slots", gossip_data.slots.len());
            } else {
                error!("Failed to serialize gossip data");
            }
        }
    }

    /// Get slots owned by this node
    pub async fn get_local_slots(&self) -> HashSet<u16> {
        self.local_slots.read().await.clone()
    }

    /// Shutdown the cluster manager
    #[allow(dead_code)]
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
            gossip_interval_ms: Some(1000),
            advertise_addr: Some("127.0.0.1:7000".to_string()),
        };

        let gossip_bind_addr: SocketAddr = "127.0.0.1:7000".parse().unwrap();
        let redis_addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
        let cluster_manager = ClusterManager::new(&config, gossip_bind_addr, redis_addr).await;

        // Note: This test might fail in CI due to UDP binding issues
        if cluster_manager.is_ok() {
            let manager = cluster_manager.unwrap();
            assert_eq!(manager.node_id, "test-node");

            // Check local slots were set
            let local_slots = manager.get_local_slots().await;
            assert_eq!(local_slots.len(), 5);
            assert!(local_slots.contains(&0));
            assert!(local_slots.contains(&4));
        }

        // Note: Additional namespace distribution tests are in tests/namespace_distribution_test.rs
    }

    #[tokio::test]
    async fn test_should_handle_locally() {
        let config = ClusterConfig {
            enabled: true,
            node_id: "test-node".to_string(),
            seeds: vec![],
            port: 7000,
            slots: Some(vec![0, 1, 2]),
            slot_ranges: None,
            gossip_interval_ms: Some(1000),
            advertise_addr: Some("127.0.0.1:7000".to_string()),
        };

        let gossip_bind_addr: SocketAddr = "127.0.0.1:7000".parse().unwrap();
        let redis_addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();

        // This test might fail in CI due to UDP binding, so we'll handle the error gracefully
        if let Ok(manager) = ClusterManager::new(&config, gossip_bind_addr, redis_addr).await {
            // Test keys that should be handled locally
            for slot in [0, 1, 2] {
                // Find a key that maps to this slot
                for i in 0..1000 {
                    let test_key = format!("testkey{}", i);
                    if ClusterManager::calculate_slot(&test_key) == slot {
                        assert!(manager.should_handle_locally(&test_key).await);
                        break;
                    }
                }
            }

            // Test a key that definitely won't be in slots 0, 1, 2
            // We'll use a key that we know maps to a different slot
            let remote_key = "key"; // This maps to slot 12539
            assert!(!manager.should_handle_locally(remote_key).await);
        }
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
            gossip_interval_ms: Some(1000),
            advertise_addr: Some("127.0.0.1:7000".to_string()),
        };

        let gossip_bind_addr: SocketAddr = "127.0.0.1:7000".parse().unwrap();
        let redis_addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();

        // This test might fail in CI due to UDP binding, so we'll handle the error gracefully
        if let Ok(manager) = ClusterManager::new(&config, gossip_bind_addr, redis_addr).await {
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
