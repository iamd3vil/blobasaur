# Clustering Guide

This document explains how to set up and use Blobasaur's Redis cluster-compatible clustering functionality.

## Overview

Blobasaur implements Redis cluster protocol compatibility, allowing Redis cluster-aware clients to connect and perform operations. The clustering system uses hash slots (16384 total) to distribute data across nodes, just like Redis Cluster, with automatic node discovery and gossip protocol communication.

## Implementation Status

✅ **Fully Implemented:**
- Redis cluster protocol commands (CLUSTER NODES, CLUSTER INFO, CLUSTER SLOTS, etc.)
- Hash slot calculation using CRC16 (Redis-compatible)
- Automatic slot assignment and management using slot ranges
- MOVED redirection for keys not owned by current node
- Gossip protocol implementation using chitchat for node discovery
- Automatic node discovery and cluster membership management
- Cross-node key operations with automatic redirects
- Cluster state management and health monitoring

🚧 **Not Yet Implemented:**
- Replication support (master-slave)
- Online slot migration/resharding
- Automatic failover

## Configuration

### Slot Range Configuration

Blobasaur uses slot ranges for efficient slot assignment. The total 16384 hash slots are distributed across nodes using range specifications.

### Multi-Node Setup Example

For a 3-node cluster, distribute the 16384 slots using ranges:

**Node 1 Configuration (`config.node1.toml`):**
```toml
# Basic server configuration
data_dir = "data"
num_shards = 4
storage_compression = true
output_compression = false
async_write = true
batch_size = 100
batch_timeout_ms = 10
addr = "0.0.0.0:6381"

# Cluster configuration
[cluster]
enabled = true
node_id = "node-1"
seeds = ["127.0.0.1:7002", "127.0.0.1:7003"]
advertise_addr = "127.0.0.1:6381"
port = 7001
gossip_interval_ms = 1000

# Hash slot ranges for this node (0-5460)
[[cluster.slot_ranges]]
start = 0
end = 5460
```

**Node 2 Configuration (`config.node2.toml`):**
```toml
# Basic server configuration
data_dir = "data"
num_shards = 4
storage_compression = true
output_compression = false
async_write = true
batch_size = 100
batch_timeout_ms = 10
addr = "0.0.0.0:6382"

# Cluster configuration
[cluster]
enabled = true
node_id = "node-2"
seeds = ["127.0.0.1:7001", "127.0.0.1:7003"]
advertise_addr = "127.0.0.1:6382"
port = 7002
gossip_interval_ms = 1000

# Hash slot ranges for this node (5461-10922)
[[cluster.slot_ranges]]
start = 5461
end = 10922
```

**Node 3 Configuration (`config.node3.toml`):**
```toml
# Basic server configuration
data_dir = "data"
num_shards = 4
storage_compression = true
output_compression = false
async_write = true
batch_size = 100
batch_timeout_ms = 10
addr = "0.0.0.0:6383"

# Cluster configuration
[cluster]
enabled = true
node_id = "node-3"
seeds = ["127.0.0.1:7001", "127.0.0.1:7002"]
advertise_addr = "127.0.0.1:6383"
port = 7003
gossip_interval_ms = 1000

# Hash slot ranges for this node (10923-16383)
[[cluster.slot_ranges]]
start = 10923
end = 16383
```

### Port Configuration

Blobasaur uses two types of ports:

- **Redis Protocol Port (`addr`)**: Where clients connect for Redis operations (6381, 6382, 6383)
- **Gossip Port (`port`)**: For inter-node communication and cluster discovery (7001, 7002, 7003)
- **Advertise Address (`advertise_addr`)**: The Redis address that clients should use to connect to this node (uses Redis port, not gossip port)

## Starting the Cluster

### Manual Setup

1. Start each node with its configuration:
```bash
# On node 1
./blobasaur --config config.node1.toml

# On node 2
./blobasaur --config config.node2.toml

# On node 3
./blobasaur --config config.node3.toml
```

### Docker Setup

Use the provided Docker Compose configuration for easy local testing:

```bash
cd dev
docker-compose up -d
```

This starts all three nodes with the correct networking and configuration.

## Verifying Cluster Operation

### Check Cluster Status
```bash
# Check overall cluster health
valkey-cli -c -p 6381 cluster info

# Should show:
# cluster_state:ok
# cluster_known_nodes:3
# cluster_size:3
# cluster_slots_assigned:16384
```

### View Cluster Topology
```bash
# List all nodes and their slot assignments
valkey-cli -c -p 6381 cluster nodes

# Should show something like:
# node-1 127.0.0.1:6381 myself,master - 0 0 0 connected 0-5460
# node-2 127.0.0.1:6382 master - 0 0 0 connected 5461-10922
# node-3 127.0.0.1:6383 master - 0 0 0 connected 10923-16383
```

## Redis Cluster Commands

### Basic Cluster Information
```bash
# Get cluster status
valkey-cli -c -p 6381 CLUSTER INFO

# List cluster nodes
valkey-cli -c -p 6381 CLUSTER NODES

# Show slot assignments
valkey-cli -c -p 6381 CLUSTER SLOTS
```

### Key Operations
```bash
# Get the hash slot for a specific key
valkey-cli -c -p 6381 CLUSTER KEYSLOT mykey

# Set and get keys (automatic redirection)
valkey-cli -c -p 6381 SET mykey "myvalue"
valkey-cli -c -p 6382 GET mykey  # Will redirect if needed
```

## Using with Redis Clients

### Redis CLI with Cluster Support
```bash
# Connect with cluster mode enabled (-c flag)
valkey-cli -c -p 6381

# Now you can use any Redis command and redirects happen automatically
SET user:1000 "John Doe"
GET user:1000
```

### Python (redis-py-cluster)
```python
from redis import RedisCluster

startup_nodes = [
    {"host": "127.0.0.1", "port": 6381},
    {"host": "127.0.0.1", "port": 6382},
    {"host": "127.0.0.1", "port": 6383}
]

rc = RedisCluster(startup_nodes=startup_nodes, decode_responses=True)
rc.set("mykey", "myvalue")
print(rc.get("mykey"))
```

### Node.js (ioredis)
```javascript
const Redis = require('ioredis');

const cluster = new Redis.Cluster([
  { host: '127.0.0.1', port: 6381 },
  { host: '127.0.0.1', port: 6382 },
  { host: '127.0.0.1', port: 6383 }
]);

await cluster.set('mykey', 'myvalue');
const value = await cluster.get('mykey');
```

## Hash Slot Calculation

Blobasaur uses the same hash slot calculation as Redis:

1. Extract the "hash tag" from the key (text between `{` and `}`)
2. If no hash tag exists, use the entire key
3. Calculate CRC16 of the hash key
4. Modulo 16384 to get the slot number

Examples:
- `user:1000` → CRC16("user:1000") % 16384 = slot X
- `{user:1000}:profile` → CRC16("user:1000") % 16384 = slot Y
- `{user:1000}:settings` → CRC16("user:1000") % 16384 = slot Y (same as above)

## Distributed Namespace Operations

### The Namespace Scaling Problem

Traditional Redis clustering has a significant limitation when using hash operations for namespacing: if you use a hash key like `users` to store multiple user records, the entire namespace gets confined to a single cluster node.

**Problem Example (Traditional Approach):**
```bash
# All these operations would go to the SAME node
HSET users alice "Alice's data"     # Slot = hash("users") → Node A
HSET users bob "Bob's data"         # Slot = hash("users") → Node A
HSET users charlie "Charlie's data" # Slot = hash("users") → Node A
```

This creates a scaling bottleneck where popular namespaces become single-node hotspots.
**The Problem**: Traditional hash operations in Redis distribute data based on the hash key (namespace), which means all keys in a namespace go to the same slot. This creates an uneven distribution when namespaces have vastly different key counts.

### Blobasaur's Solution: Key-Based Distribution

Blobasaur solves this by using the **individual key** (not the namespace) for slot calculation in hash operations:

**Distributed Example (Blobasaur Approach):**
```bash
# These operations are distributed across different nodes
HSET users alice "Alice's data"     # Slot = hash("alice") → Node A
HSET users bob "Bob's data"         # Slot = hash("bob") → Node B
HSET users charlie "Charlie's data" # Slot = hash("charlie") → Node C
```

### Benefits of Distributed Namespaces

✅ **Horizontal Scaling**: Large namespaces can utilize all cluster nodes
✅ **Load Balancing**: No single-node bottlenecks for popular namespaces
✅ **Redis Compatibility**: Uses standard Redis cluster redirects
✅ **Transparent Operation**: Existing client code works unchanged

### Demonstration

Run the included example to see the difference:
```bash
cargo run --example namespace_distribution_demo
```

This shows how 8 users in a `users` namespace are distributed across 3 nodes instead of being confined to 1 node.

### Hash Tag Co-location (When Needed)

If you need related data on the same node, use hash tags:
```bash
# Co-locate Alice's data using hash tags
HSET users {user:alice}:profile "profile data"   # All go to same node
HSET users {user:alice}:settings "settings data" # Same node as above
HSET users {user:alice}:cache "cache data"       # Same node as above

# Bob's data goes to a different node
HSET users {user:bob}:profile "profile data"     # Different node
HSET users {user:bob}:settings "settings data"   # Same node as Bob's profile
```

## Automatic Key Redirection

When a client requests a key that doesn't belong to the current node, Blobasaur responds with a MOVED error:

```
-MOVED 3999 127.0.0.1:6382
```

Redis cluster-aware clients (using `-c` flag or cluster libraries) automatically follow these redirections transparently.

## Testing Cross-Node Operations

### Basic Test
```bash
# Set keys from different nodes
valkey-cli -c -p 6381 SET test1 "from node 1"
valkey-cli -c -p 6382 SET test2 "from node 2"
valkey-cli -c -p 6383 SET test3 "from node 3"

# Get keys from different nodes (tests redirection)
valkey-cli -c -p 6381 GET test2  # Should redirect to node 2
valkey-cli -c -p 6382 GET test3  # Should redirect to node 3
valkey-cli -c -p 6383 GET test1  # Should redirect to node 1
```

### Distributed Namespace Test
```bash
# Hash operations are distributed across nodes based on individual keys
valkey-cli -c -p 6381 HSET users alice "Alice's data"     # May redirect to node owning hash("alice")
valkey-cli -c -p 6381 HSET users bob "Bob's data"         # May redirect to node owning hash("bob")
valkey-cli -c -p 6381 HSET users charlie "Charlie's data" # May redirect to node owning hash("charlie")

# Check which node handles each user
valkey-cli -c -p 6381 CLUSTER KEYSLOT alice    # Shows slot for alice
valkey-cli -c -p 6381 CLUSTER KEYSLOT bob      # Shows slot for bob
valkey-cli -c -p 6381 CLUSTER KEYSLOT charlie  # Shows slot for charlie

# Retrieve data (with automatic redirects)
valkey-cli -c -p 6382 HGET users alice         # Redirects if needed
valkey-cli -c -p 6383 HGET users bob           # Redirects if needed
```

### Hash Tag Co-location Test
```bash
# Keys with same hash tag go to same node
valkey-cli -c -p 6381 HSET users "{user:1000}:name" "Alice"
valkey-cli -c -p 6382 HSET users "{user:1000}:email" "alice@example.com"

# Both should be on the same node
valkey-cli -c -p 6383 HGET users "{user:1000}:name"
valkey-cli -c -p 6383 HGET users "{user:1000}:email"
```

## Networking Requirements

- **Redis Protocol Ports:** TCP ports for client connections (6381, 6382, 6383)
- **Gossip Ports:** UDP ports for inter-node cluster communication (7001, 7002, 7003)
- **Firewall:** Ensure both TCP and UDP ports are open between cluster nodes
- **Network Connectivity:** All nodes must be able to reach each other on both port types

## Troubleshooting

### Check Node Discovery
```bash
# Verify all nodes see each other
valkey-cli -c -p 6381 CLUSTER NODES
valkey-cli -c -p 6382 CLUSTER NODES
valkey-cli -c -p 6383 CLUSTER NODES

# All should show the same 3 nodes
```

### Verify Slot Distribution
```bash
# Check that all 16384 slots are assigned
valkey-cli -c -p 6381 CLUSTER INFO | grep cluster_slots_assigned
# Should show: cluster_slots_assigned:16384
```

### Test Key Redirection
```bash
# Check which node handles a specific key
valkey-cli -c -p 6381 CLUSTER KEYSLOT mykey

# Try setting the key and observe redirection
valkey-cli -c -p 6381 SET mykey myvalue
```

### Check Gossip Protocol
```bash
# Look for gossip activity in logs
docker-compose logs | grep -i gossip

# Should see discovery messages like:
# "Discovered cluster node: node-2 at 127.0.0.1:6382 with 5462 slots"
```

## Current Limitations

1. **No Replication:** Each slot is only stored on one node (no master-slave)
2. **No Online Resharding:** Slot assignments are configured statically
3. **No Automatic Failover:** Manual intervention required for node failures
4. **Basic Failure Detection:** Uses gossip protocol timeouts for node failure detection

## Architecture Details

### Gossip Protocol
- Uses chitchat library for UDP-based gossip communication
- Nodes automatically discover each other through seed node configuration
- Cluster membership and slot assignments are propagated via gossip
- Failure detection based on gossip heartbeat timeouts

### Slot Assignment
- Hash slots (0-16383) are statically assigned using slot ranges in configuration
- Each node knows which slots it owns and which nodes own other slots
- Key routing decisions made locally without central coordination

### Client Redirection
- Clients connecting to wrong node get MOVED responses
- Cluster-aware clients automatically follow redirects
- Non-cluster clients can still connect but won't follow redirects

## Best Practices

1. **Even Slot Distribution:** Distribute slots evenly across nodes for balanced load
2. **Network Reliability:** Ensure stable, low-latency network between cluster nodes
3. **Monitoring:** Monitor cluster health using CLUSTER INFO and CLUSTER NODES
4. **Client Configuration:** Use cluster-aware Redis clients for automatic redirection
5. **Testing:** Test cluster behavior thoroughly before production deployment
6. **Backup Strategy:** Implement backup procedures for each node independently
7. **Namespace Design:** Take advantage of distributed namespaces for horizontal scaling:
   - Use simple hash operations (`HSET users alice data`) for automatic distribution
   - Use hash tags (`HSET users {user:alice}:profile data`) only when co-location is needed
   - Avoid single large hashes that would be confined to one node
8. **Key Naming:** Design key names to distribute load evenly across the hash space

## Example Production Deployment

For a production 3-node cluster:

1. **Plan Infrastructure:** 3 servers with reliable networking
2. **Configure Slot Distribution:** 5461, 5462, 5461 slots per node
3. **Network Setup:** Ensure inter-node connectivity on both Redis and gossip ports
4. **Start Nodes:** Start all nodes with correct seed configuration
5. **Verify Cluster:** Check CLUSTER INFO shows cluster_state:ok
6. **Test Operations:** Verify cross-node SET/GET operations work
7. **Client Testing:** Test with your application's Redis client library
8. **Monitor:** Set up monitoring for cluster health and individual node health

The cluster is production-ready for applications requiring horizontal scaling of key-value operations across multiple nodes.
