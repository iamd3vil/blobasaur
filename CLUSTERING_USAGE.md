# Clustering Usage Guide

This document explains how to set up and use Blobnom's Redis cluster-compatible clustering functionality.

## Overview

Blobnom implements Redis cluster protocol compatibility, allowing Redis cluster-aware clients to connect and perform operations. The clustering system uses hash slots (16384 total) to distribute data across nodes, just like Redis Cluster.

## Current Implementation Status

✅ **Implemented:**
- Basic cluster manager structure
- Redis cluster protocol commands (CLUSTER NODES, CLUSTER INFO, etc.)
- Hash slot calculation using CRC16 (Redis-compatible)
- Slot assignment and management
- MOVED redirection for keys not owned by current node
- Configuration system for cluster settings

🚧 **In Development:**
- Actual gossip protocol implementation (currently simplified)
- Automatic node discovery and failure detection
- Cross-node data migration
- Replication support

## Configuration

### Basic Cluster Configuration

Create a configuration file (e.g., `config.cluster.toml`):

```toml
# Basic server settings
data_dir = "data"
num_shards = 4
addr = "0.0.0.0:6379"

# Cluster configuration
[cluster]
enabled = true
node_id = "node-1"
port = 7000
seeds = ["192.168.1.101:7000", "192.168.1.102:7000"]

# Assign hash slots to this node
slots = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]  # Example: first 10 slots
```

### Multi-Node Setup Example

For a 3-node cluster, distribute the 16384 slots:

**Node 1 (node-1):**
```toml
[cluster]
enabled = true
node_id = "node-1"
port = 7000
seeds = ["192.168.1.101:7000", "192.168.1.102:7000"]
slots = [0, 1, 2, ..., 5460]  # Slots 0-5460 (5461 total)
```

**Node 2 (node-2):**
```toml
[cluster]
enabled = true
node_id = "node-2"
port = 7000
seeds = ["192.168.1.100:7000", "192.168.1.102:7000"]
slots = [5461, 5462, ..., 10922]  # Slots 5461-10922 (5462 total)
```

**Node 3 (node-3):**
```toml
[cluster]
enabled = true
node_id = "node-3"
port = 7000
seeds = ["192.168.1.100:7000", "192.168.1.101:7000"]
slots = [10923, 10924, ..., 16383]  # Slots 10923-16383 (5461 total)
```

## Starting the Cluster

1. Start each node with its configuration:
```bash
# On node 1
./blobnom --config config.node1.toml

# On node 2
./blobnom --config config.node2.toml

# On node 3
./blobnom --config config.node3.toml
```

## Redis Cluster Commands

### CLUSTER INFO
Get cluster status information:
```bash
redis-cli -p 6379 CLUSTER INFO
```

### CLUSTER NODES
List all nodes in the cluster:
```bash
redis-cli -p 6379 CLUSTER NODES
```

### CLUSTER SLOTS
Show slot assignments:
```bash
redis-cli -p 6379 CLUSTER SLOTS
```

### CLUSTER KEYSLOT
Get the hash slot for a specific key:
```bash
redis-cli -p 6379 CLUSTER KEYSLOT mykey
```

### CLUSTER ADDSLOTS / CLUSTER DELSLOTS
Manually assign or remove slots:
```bash
# Add slots 100-200 to current node
redis-cli -p 6379 CLUSTER ADDSLOTS 100 101 102 ... 200

# Remove slots from current node
redis-cli -p 6379 CLUSTER DELSLOTS 100 101 102
```

## Using with Redis Clients

### Redis CLI with Cluster Support
```bash
redis-cli -c -p 6379
```

### Python (redis-py-cluster)
```python
from rediscluster import RedisCluster

startup_nodes = [
    {"host": "192.168.1.100", "port": 6379},
    {"host": "192.168.1.101", "port": 6379},
    {"host": "192.168.1.102", "port": 6379}
]

rc = RedisCluster(startup_nodes=startup_nodes, decode_responses=True)
rc.set("mykey", "myvalue")
print(rc.get("mykey"))
```

### Node.js (ioredis)
```javascript
const Redis = require('ioredis');

const cluster = new Redis.Cluster([
  { host: '192.168.1.100', port: 6379 },
  { host: '192.168.1.101', port: 6379 },
  { host: '192.168.1.102', port: 6379 }
]);

await cluster.set('mykey', 'myvalue');
const value = await cluster.get('mykey');
```

## Hash Slot Calculation

Blobnom uses the same hash slot calculation as Redis:

1. Extract the "hash tag" from the key (text between `{` and `}`)
2. If no hash tag, use the entire key
3. Calculate CRC16 of the hash key
4. Modulo 16384 to get the slot number

Examples:
- `user:1000` → CRC16("user:1000") % 16384
- `user:{1000}:profile` → CRC16("1000") % 16384 (hash tag)
- `user:{1000}:settings` → CRC16("1000") % 16384 (same slot as above)

## Key Redirection

When a client requests a key that doesn't belong to the current node, Blobnom responds with a MOVED error:

```
-MOVED 3999 192.168.1.101:6379
```

Redis cluster-aware clients automatically follow these redirections.

## Networking Requirements

- **Redis Protocol Port:** TCP port for client connections (default: 6379)
- **Gossip Port:** UDP port for inter-node communication (configurable)
- **Firewall:** Ensure both ports are open between cluster nodes

## Troubleshooting

### Check Cluster Status
```bash
# Verify node can see other cluster members
redis-cli -p 6379 CLUSTER NODES

# Check cluster health
redis-cli -p 6379 CLUSTER INFO
```

### Verify Slot Distribution
```bash
# Ensure all 16384 slots are assigned
redis-cli -p 6379 CLUSTER SLOTS
```

### Test Key Distribution
```bash
# Check which node handles a specific key
redis-cli -p 6379 CLUSTER KEYSLOT mykey

# Try setting a key and see if it gets redirected
redis-cli -p 6379 SET mykey myvalue
```

## Limitations

1. **No Automatic Failover:** Manual intervention required for node failures
2. **No Replication:** Each slot is only stored on one node
3. **No Online Resharding:** Slot assignments must be configured statically
4. **Simplified Gossip:** Current implementation doesn't use full chitchat protocol

## Future Enhancements

- Full chitchat gossip protocol integration
- Automatic node discovery and failure detection
- Master-slave replication
- Online slot migration
- Automatic cluster rebalancing
- Redis Cluster bus protocol compatibility

## Best Practices

1. **Even Slot Distribution:** Distribute slots evenly across nodes
2. **Network Reliability:** Ensure stable network between cluster nodes  
3. **Monitoring:** Monitor cluster health and node availability
4. **Backup Strategy:** Implement backup procedures for each node
5. **Testing:** Test cluster behavior with client applications before production

## Example Deployment

For a production 3-node cluster:

1. **Plan slot distribution:** 5461, 5462, 5461 slots per node
2. **Configure networking:** Ensure inter-node connectivity
3. **Start nodes sequentially:** Allow each to join the cluster
4. **Verify cluster state:** Check CLUSTER NODES and CLUSTER INFO
5. **Test with clients:** Verify redirection works correctly
6. **Monitor and maintain:** Regular health checks and backups