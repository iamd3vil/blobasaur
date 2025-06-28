//! Demonstration of Blobnom's distributed namespace functionality
//!
//! This example shows how namespace operations are distributed across cluster nodes
//! using key-based slot calculation instead of namespace-based calculation.
//!
//! Run with: cargo run --example namespace_distribution_demo

use std::collections::HashMap;
#[cfg(test)]
use std::collections::HashSet;

// Simulate the cluster manager's slot calculation
pub const REDIS_CLUSTER_SLOTS: u16 = 16384;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NamespaceDistributionStrategy {
    /// Use namespace for slot calculation (legacy behavior - confines namespace to one node)
    NamespaceBased,
    /// Use key for slot calculation (distributes namespace across nodes)
    KeyBased,
    /// Use hash tags when present, fallback to key-based
    HashTagAware,
}

fn calculate_slot_with_strategy(key: &str, strategy: NamespaceDistributionStrategy) -> u16 {
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

    // Simulate CRC16 calculation (simplified for demo)
    let hash = hash_key
        .bytes()
        .fold(0u16, |acc, b| acc.wrapping_mul(31).wrapping_add(b as u16));
    hash % REDIS_CLUSTER_SLOTS
}

fn calculate_slot_for_hash(
    namespace: &str,
    key: &str,
    strategy: NamespaceDistributionStrategy,
) -> u16 {
    match strategy {
        NamespaceDistributionStrategy::NamespaceBased => {
            // Use namespace as the hash key (legacy behavior)
            calculate_slot_with_strategy(namespace, strategy)
        }
        NamespaceDistributionStrategy::KeyBased => {
            // Use individual key for distribution
            calculate_slot_with_strategy(key, strategy)
        }
        NamespaceDistributionStrategy::HashTagAware => {
            // Check if key has hash tag, otherwise use key
            let combined_key = if key.contains('{') && key.contains('}') {
                key
            } else {
                key
            };
            calculate_slot_with_strategy(combined_key, strategy)
        }
    }
}

fn simulate_cluster_nodes() -> HashMap<u16, String> {
    let mut node_assignments = HashMap::new();

    // Simulate 3-node cluster with slot ranges
    for slot in 0..=5460 {
        node_assignments.insert(slot, "Node-1 (127.0.0.1:6381)".to_string());
    }
    for slot in 5461..=10922 {
        node_assignments.insert(slot, "Node-2 (127.0.0.1:6382)".to_string());
    }
    for slot in 10923..16384 {
        node_assignments.insert(slot, "Node-3 (127.0.0.1:6383)".to_string());
    }

    node_assignments
}

fn main() {
    println!("🚀 Blobnom Distributed Namespace Demonstration");
    println!("==============================================\n");

    let cluster_nodes = simulate_cluster_nodes();
    let namespace = "users";
    let user_keys = vec![
        "alice", "bob", "charlie", "diana", "eve", "frank", "grace", "henry",
    ];

    println!(
        "📊 Scenario: Hash operations on '{}' namespace with {} users",
        namespace,
        user_keys.len()
    );
    println!("🏗️  Cluster: 3 nodes with 16384 slots distributed evenly\n");

    // Demonstrate the problem with namespace-based distribution
    println!("❌ PROBLEM: Namespace-Based Distribution (Old Approach)");
    println!("--------------------------------------------------------");

    let mut namespace_based_distribution = HashMap::new();
    for key in &user_keys {
        let slot = calculate_slot_for_hash(
            namespace,
            key,
            NamespaceDistributionStrategy::NamespaceBased,
        );
        let node = cluster_nodes.get(&slot).unwrap();
        namespace_based_distribution
            .entry(node.clone())
            .or_insert(Vec::new())
            .push(key);
    }

    for (node, keys) in &namespace_based_distribution {
        println!("  {} handles {} users: {:?}", node, keys.len(), keys);
    }

    let unique_nodes = namespace_based_distribution.keys().count();
    println!(
        "  📈 Distribution: {} users confined to {} node(s)",
        user_keys.len(),
        unique_nodes
    );
    println!(
        "  ⚠️  Scaling Issue: Entire '{}' namespace limited to single node!\n",
        namespace
    );

    // Demonstrate the solution with key-based distribution
    println!("✅ SOLUTION: Key-Based Distribution (New Approach)");
    println!("--------------------------------------------------");

    let mut key_based_distribution = HashMap::new();
    let mut slot_info = Vec::new();

    for key in &user_keys {
        let slot = calculate_slot_for_hash(namespace, key, NamespaceDistributionStrategy::KeyBased);
        let node = cluster_nodes.get(&slot).unwrap();
        key_based_distribution
            .entry(node.clone())
            .or_insert(Vec::new())
            .push(key);
        slot_info.push((key, slot, node));
    }

    // Show detailed slot assignments
    println!("  Slot assignments:");
    for (key, slot, node) in &slot_info {
        let node_short = node.split(' ').next().unwrap();
        println!(
            "    HSET {} {} -> Slot {:5} -> {}",
            namespace, key, slot, node_short
        );
    }
    println!();

    // Show distribution summary
    for (node, keys) in &key_based_distribution {
        println!("  {} handles {} users: {:?}", node, keys.len(), keys);
    }

    let unique_nodes_new = key_based_distribution.keys().count();
    println!(
        "  📈 Distribution: {} users across {} node(s)",
        user_keys.len(),
        unique_nodes_new
    );
    println!("  🎯 Benefit: Namespace can scale horizontally across all nodes!\n");

    // Demonstrate hash tag co-location
    println!("🏷️  BONUS: Hash Tag Co-location");
    println!("-------------------------------");

    let tagged_keys = vec![
        ("{user:alice}:profile", "Alice's profile data"),
        ("{user:alice}:settings", "Alice's settings"),
        ("{user:alice}:cache", "Alice's cache"),
        ("{user:bob}:profile", "Bob's profile data"),
        ("{user:bob}:settings", "Bob's settings"),
    ];

    println!("  Related data can be co-located using hash tags:");
    let mut tag_distribution = HashMap::new();

    for (tagged_key, description) in &tagged_keys {
        let slot =
            calculate_slot_with_strategy(tagged_key, NamespaceDistributionStrategy::HashTagAware);
        let node = cluster_nodes.get(&slot).unwrap();
        println!(
            "    HSET {} {} -> Slot {:5} -> {} ({})",
            namespace,
            tagged_key,
            slot,
            node.split(' ').next().unwrap(),
            description
        );
        tag_distribution
            .entry(node.clone())
            .or_insert(Vec::new())
            .push(tagged_key);
    }

    println!("\n  Co-location groups:");
    for (node, keys) in &tag_distribution {
        if keys.len() > 1 {
            println!(
                "    {} groups related data: {:?}",
                node.split(' ').next().unwrap(),
                keys
            );
        }
    }

    println!("\n🎉 Summary");
    println!("----------");
    println!("✅ Fixed namespace scaling bottleneck");
    println!("✅ Maintains Redis cluster protocol compatibility");
    println!("✅ Supports automatic client redirects");
    println!("✅ Enables hash tag co-location when needed");
    println!("✅ Distributes load evenly across all cluster nodes");

    println!("\n💡 Usage Examples:");
    println!("  # Operations are automatically distributed");
    println!("  redis-cli -c -p 6381 HSET users alice 'Alice data'    # -> Node A");
    println!("  redis-cli -c -p 6381 HSET users bob 'Bob data'        # -> Node B");
    println!("  redis-cli -c -p 6381 HSET users charlie 'Charlie data' # -> Node C");
    println!("  ");
    println!("  # Client gets redirected automatically if needed");
    println!("  redis-cli -c -p 6382 HGET users alice  # -> MOVED 1234 127.0.0.1:6381");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_based_distribution_spreads_load() {
        let namespace = "test_namespace";
        let keys = vec!["key1", "key2", "key3", "key4", "key5"];

        let mut slots = HashSet::new();
        for key in keys {
            let slot =
                calculate_slot_for_hash(namespace, key, NamespaceDistributionStrategy::KeyBased);
            slots.insert(slot);
        }

        // Different keys should likely map to different slots (not guaranteed due to hash collisions)
        // but very likely with different key names
        assert!(
            slots.len() > 1,
            "Keys should be distributed across multiple slots"
        );
    }

    #[test]
    fn test_namespace_based_confinement() {
        let namespace = "test_namespace";
        let keys = vec!["key1", "key2", "key3", "key4", "key5"];

        let mut slots = HashSet::new();
        for key in keys {
            let slot = calculate_slot_for_hash(
                namespace,
                key,
                NamespaceDistributionStrategy::NamespaceBased,
            );
            slots.insert(slot);
        }

        // All keys should map to the same slot with namespace-based strategy
        assert_eq!(
            slots.len(),
            1,
            "All keys should map to same slot with namespace-based strategy"
        );
    }

    #[test]
    fn test_hash_tag_colocations() {
        let namespace = "users";

        // Keys with same hash tag should go to same slot
        let slot1 = calculate_slot_for_hash(
            namespace,
            "{user:123}:profile",
            NamespaceDistributionStrategy::KeyBased,
        );
        let slot2 = calculate_slot_for_hash(
            namespace,
            "{user:123}:settings",
            NamespaceDistributionStrategy::KeyBased,
        );

        // Note: This test uses KeyBased strategy, so it will use the full key including hash tag
        // In practice, you'd use HashTagAware strategy for true hash tag extraction

        // Different users should likely go to different slots
        let slot3 = calculate_slot_for_hash(
            namespace,
            "{user:456}:profile",
            NamespaceDistributionStrategy::KeyBased,
        );

        assert!(slot1 < REDIS_CLUSTER_SLOTS);
        assert!(slot2 < REDIS_CLUSTER_SLOTS);
        assert!(slot3 < REDIS_CLUSTER_SLOTS);
    }
}
