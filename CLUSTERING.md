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

- Asynchronous replication with manual failover (see [Replication](#replication))

🚧 **Not Yet Implemented:**
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
storage_compression = { enabled = true, algorithm = "zstd", level = 3 }
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
storage_compression = { enabled = true, algorithm = "zstd", level = 3 }
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
storage_compression = { enabled = true, algorithm = "zstd", level = 3 }
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

## Replication

A node can replicate another node's slots. Replication is asynchronous: it gives you a live copy for disaster recovery and replicas that cluster clients can read from. It is not a backup (deletes replicate too), and a failover loses writes the primary acknowledged but had not yet replicated. Promotion is manual. Design and protocol details are in [REPLICATION.md](REPLICATION.md).

### Configuration

A replica is a cluster member that owns no slots and names the primary's `node_id`:

```toml
# config.node1b.toml: replica of node-1
data_dir = "data"
num_shards = 4                                              # same as node-1
storage_compression = { enabled = true, algorithm = "zstd", level = 3 }  # same as node-1
addr = "0.0.0.0:6384"

[cluster]
enabled = true
node_id = "node-1b"
seeds = ["127.0.0.1:7001"]
advertise_addr = "127.0.0.1:6384"
port = 7004
# no slot_ranges: a replica owns no slots

[replication]
replica_of = "node-1"            # node id; its address is found through gossip
tombstone_retention_secs = 86400 # on the primary; see below
```

- `num_shards` and the storage compression algorithm must match the primary. The primary rejects a replica that differs.
- A new replica copies everything from the primary on first start; nothing else to do.
- `tombstone_retention_secs` (default 86400) is how long a primary remembers deletes for disconnected replicas. A replica away for longer does a full resync.
- A replica cannot replicate another replica.

### Reading from replicas

Send `READONLY` on a connection, and a replica serves reads (`GET`, `EXISTS`, `TTL`, `HGET`, `HEXISTS`) for its primary's slots. With go-redis set `ReadOnly: true` (add `RouteByLatency` or `RouteRandomly` to pick among replicas). Writes still go to the primary. Replica reads are eventually consistent, with no read-your-writes guarantee. A replica serves reads only once every shard has finished its first copy.

### Monitoring

The replication metrics are listed in [METRICS.md](METRICS.md). The one to alert on is `blobasaur_replication_staleness_seconds` on replicas (for example `> 60`): it keeps rising if the stream, the primary or the replica's writer stalls. Watch `blobasaur_replication_resyncs_total` for unexpected full copies, and compare `blobasaur_replication_latest_lsn` on the primary with `blobasaur_replication_caught_up_lsn` on the replica for LSN lag.

### Failover

Failover is manual and the operator fences the old primary. Nothing in the cluster stops two nodes from claiming the same slots; a node that starts with slots refuses to if it sees a live node already claiming them, but that check is best effort only.

`BLOBASAUR.WATERMARK` helps decide when a replica is caught up. It returns one `[shard, replid, lsn]` entry per shard:
- On a primary it first waits until every write acknowledged before the call (including `async_write` ones still queued) is committed, then returns the shard's highest LSN.
- On a replica it returns the `caught_up_lsn`, or `not-synced` for a shard without a complete copy.

#### Unplanned (the primary is lost)

1. **Fence the old primary.** Make sure `node-1` is stopped and cannot restart with its old config: stop the unit, block its ports, or take the host down.
2. **Wait for the cluster to see it as dead.** Run `valkey-cli -p 6384 CLUSTER NODES` (any live node) until `node-1` is no longer listed as live. Restarting the replica earlier fails: the startup guard refuses to start while a live node still claims the slots.
3. **Promote the replica.** Edit `config.node1b.toml`: remove `[replication]`'s `replica_of` and add `node-1`'s slots, then restart it:
   ```toml
   [replication]
   tombstone_retention_secs = 86400

   [[cluster.slot_ranges]]
   start = 0
   end = 5460
   ```
   On start it promotes (refusing if any shard never finished its first copy) and generates a new `replid`. Check `valkey-cli -p 6384 CLUSTER NODES` shows it as `master` with the slots.
4. **Repoint other replicas.** For every other replica of `node-1`, set `replica_of = "node-1b"` and restart it. It gets a resync and copies everything again.
5. **Bring `node-1` back (optional).** Give it `replica_of = "node-1b"`, remove its `slot_ranges`, and start it. It resyncs from `node-1b`; writes it never replicated are discarded.

#### Planned (maintenance)

1. Stop client writes to the primary's slots in your application.
2. On the primary, run `valkey-cli -p 6381 BLOBASAUR.WATERMARK` and note each shard's `replid` and `lsn`.
3. On the replica, run `valkey-cli -p 6384 BLOBASAUR.WATERMARK` until every shard shows the same `replid` and an `lsn` at least the primary's. The primary stays up meanwhile, and its idle heartbeats deliver the watermark within about 10 seconds. `not-synced` means the shard is not ready.
4. Stop and fence `node-1`, then continue from step 2 of the unplanned procedure. No acknowledged writes are lost.

#### Restoring a primary from backup

Replication cannot tell that a data directory was restored from an older backup or disk snapshot: it looks like a clean restart, and replicas would keep newer data. Start a restored primary once with `blobasaur serve --new-replid`, which makes its replicas resync.

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

## Upgrade Notes

Nodes gossip their slots as ranges. Nodes from before this encoding read only a
full list of slots, so a node also gossips that list for them as long as its
whole gossip entry stays under 56000 bytes. That fits about 9000 slots, and fewer
when scattered, since the ranges grow too. Past that
(for example a node owning all 16384 slots) only the ranges fit in one gossip
datagram, and the node logs a warning once. An older peer then sees that node as
owning no slots, and runs requests for them locally instead of redirecting.

Upgrade every peer before giving one node an assignment too large for the list.
Rolling upgrades with smaller assignments need no care.

## Current Limitations

1. **Asynchronous Replication:** Replicas can lag, and a failover loses unreplicated writes (see [Replication](#replication))
2. **No Online Resharding:** Slot assignments are configured statically
3. **No Automatic Failover:** Promotion is manual (see [Failover](#failover))
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
