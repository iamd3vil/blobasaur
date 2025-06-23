use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;

use tokio::sync::RwLock;
use tracing::info;

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

/// Manages cluster state and gossip protocol
pub struct ClusterManager {
    node_id: String,
    nodes: Arc<RwLock<HashMap<String, ClusterNode>>>,
    local_slots: Arc<RwLock<HashSet<u16>>>,
    _config: ClusterConfig,
}

impl ClusterManager {
    /// Create a new cluster manager
    pub async fn new(
        config: &ClusterConfig,
        _bind_addr: SocketAddr,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        // Initialize local slots
        let local_slots = Arc::new(RwLock::new(
            config
                .slots
                .clone()
                .unwrap_or_default()
                .into_iter()
                .collect(),
        ));

        let nodes = Arc::new(RwLock::new(HashMap::new()));

        let cluster_manager = Self {
            node_id: config.node_id.clone(),
            nodes,
            local_slots,
            _config: config.clone(),
        };

        info!("Cluster manager initialized for node: {}", config.node_id);

        Ok(cluster_manager)
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
        // Extract hash tag if present (e.g., "user:{123}:profile" -> "123")
        let hash_key = if let Some(start) = key.find('{') {
            if let Some(end) = key[start + 1..].find('}') {
                let tag = &key[start + 1..start + 1 + end];
                if !tag.is_empty() { tag } else { key }
            } else {
                key
            }
        } else {
            key
        };

        crc16::State::<crc16::XMODEM>::calculate(hash_key.as_bytes()) % REDIS_CLUSTER_SLOTS
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
            self.node_id, "127.0.0.1", 6379, local_slots_ranges
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

        format!(
            "cluster_state:ok\n\
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
            total_assigned_slots, total_assigned_slots, total_nodes, total_nodes
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
        for slot in slots {
            local_slots.insert(slot);
        }
        drop(local_slots);

        info!("Added slots to local node");
        Ok(())
    }

    /// Remove slots from this node
    pub async fn remove_slots(
        &self,
        slots: Vec<u16>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut local_slots = self.local_slots.write().await;
        for slot in slots {
            local_slots.remove(&slot);
        }
        drop(local_slots);

        info!("Removed slots from local node");
        Ok(())
    }

    /// Get slots owned by this node
    pub async fn get_local_slots(&self) -> HashSet<u16> {
        self.local_slots.read().await.clone()
    }

    /// Shutdown the cluster manager
    #[allow(dead_code)]
    pub async fn shutdown(&self) {
        info!("Shutting down cluster manager");
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
            gossip_interval_ms: Some(1000),
        };

        let bind_addr: SocketAddr = "127.0.0.1:7000".parse().unwrap();
        let cluster_manager = ClusterManager::new(&config, bind_addr).await;

        assert!(cluster_manager.is_ok());

        let manager = cluster_manager.unwrap();
        assert_eq!(manager.node_id, "test-node");

        // Check local slots were set
        let local_slots = manager.get_local_slots().await;
        assert_eq!(local_slots.len(), 5);
        assert!(local_slots.contains(&0));
        assert!(local_slots.contains(&4));
    }

    #[tokio::test]
    async fn test_should_handle_locally() {
        let config = ClusterConfig {
            enabled: true,
            node_id: "test-node".to_string(),
            seeds: vec![],
            port: 7000,
            slots: Some(vec![0, 1, 2]),
            gossip_interval_ms: Some(1000),
        };

        let bind_addr: SocketAddr = "127.0.0.1:7000".parse().unwrap();
        let manager = ClusterManager::new(&config, bind_addr).await.unwrap();

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

    #[tokio::test]
    async fn test_add_remove_slots() {
        let config = ClusterConfig {
            enabled: true,
            node_id: "test-node".to_string(),
            seeds: vec![],
            port: 7000,
            slots: Some(vec![0, 1, 2]),
            gossip_interval_ms: Some(1000),
        };

        let bind_addr: SocketAddr = "127.0.0.1:7000".parse().unwrap();
        let manager = ClusterManager::new(&config, bind_addr).await.unwrap();

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
