# Clustering Guide

This document explains how to set up and use Blobasaur's Redis cluster-compatible clustering functionality.

## Overview

Blobasaur implements Redis cluster protocol compatibility, allowing Redis cluster-aware clients to connect and perform operations. The clustering system uses hash slots (16384 total) to distribute data across nodes, just like Redis Cluster, with automatic node discovery and gossip protocol communication.

## Implementation Status

✅ **Fully Implemented:**
- Redis cluster protocol commands (`CLUSTER MYID`, `CLUSTER NODES`, `CLUSTER INFO`, `CLUSTER SLOTS`, `CLUSTER SHARDS`, `CLUSTER ADDSLOTS`, `CLUSTER DELSLOTS`, `CLUSTER KEYSLOT`, `CLUSTER FAILOVER [FORCE|TAKEOVER]`, `CLUSTER REPLICATE`)
- Hash slot calculation using CRC16 (Redis-compatible)
- Persisted per-node cluster state (`<data_dir>/cluster-state.json`) with epoch-versioned slot table and Redis-style epoch collision resolution
- Automatic failover elections (rank delay by `caught_up_lsn`, majority voting over `BLOBASAUR.FAILOVER-AUTH` with persisted `last_vote_epoch` and vote backoff), coordinated zero-loss `CLUSTER FAILOVER`, `CLUSTER FAILOVER FORCE`, and `CLUSTER FAILOVER TAKEOVER`
- Runtime promotion, automatic old-primary demotion, automatic replica re-pointing, and runtime `CLUSTER REPLICATE <node-id>`
- Redis-style failure detection (`PFAIL`/`FAIL` over gossip, `fail?`/`fail`/`disconnected` in `CLUSTER NODES`, `cluster_state:fail` in `CLUSTER INFO`) and minority write/read fencing
- MOVED redirection for keys owned by another live node, CLUSTERDOWN for unbound, failed, or minority-fenced slots
- Gossip protocol implementation using chitchat for node discovery
- Automatic node discovery and cluster membership management
- Cross-node key operations with automatic redirects
- Cluster state management and health monitoring
- Asynchronous replication with automatic and manual runtime failover (see [Replication](#replication))

🚧 **Not Yet Implemented:**
- Online slot migration/resharding

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

### Persisted Cluster State and Epochs

Each clustered node persists its cluster metadata to `<data_dir>/cluster-state.json` using atomic writes (write temp file, fsync, rename, fsync directory). The file stores:

- `node_id` (checked against `[cluster].node_id` on startup; a mismatch stops startup)
- `current_epoch`, `last_vote_epoch`, `last_vote_for`
- `role` (`primary` or `replica`) and `primary` (replica only)
- `config_epoch` (primary slot-claim version)
- `slots` (the node's full slot table `slot -> (node_id, config_epoch)`, stored as contiguous ranges)

The `slot_ranges`, `slots`, and `[replication].replica_of` settings in the config file are **bootstrap-only**. On first start (when `cluster-state.json` does not exist), Blobasaur initializes the state file from config (`config_epoch = 0` for a primary, after running the best-effort startup slot-conflict check). On subsequent restarts, `cluster-state.json` wins over the config file; if the config disagrees with the persisted role, primary, or slot assignment, Blobasaur logs a warning and uses the persisted state.

Each primary gossips its `current_epoch` and `config_epoch` along with its slots. Every node adopts the maximum `current_epoch` it sees and updates its slot table only when a primary claims a slot with a strictly higher `config_epoch`. Bindings for nodes that go down stay in the slot table (requests to those slots return `-CLUSTERDOWN`), so a returning old primary at a lower epoch cannot reclaim slots it lost. When two primaries share the same `config_epoch` (such as after bootstrapping at epoch 0), the one with the lexicographically smaller `node_id` increments `current_epoch`, takes it as its new `config_epoch`, persists, and gossips until every primary has a distinct epoch.

Two `[cluster]` failover configuration options are also available:
- `node_timeout_ms` (default `15000`): failure-detection (`PFAIL`) and minority fencing timeout in milliseconds.
- `replica_validity_factor` (default `10`, `0` disables the check): multiplier on `node_timeout_ms` bounding how stale a replica may be when standing for election.

### Failure Detection and Minority Behavior

Each node monitors peers using chitchat's per-node heartbeats against a monotonic clock, independent of chitchat's phi detector:

- **PFAIL (`fail?`):** a peer is marked `PFAIL` locally when its chitchat heartbeat has not advanced for `node_timeout_ms` (or after its state is evicted from chitchat). Unreachable peers appear with `fail?` and `disconnected` in `CLUSTER NODES`, and are gossiped in `pfail`.
- **FAIL (`fail`):** a node is marked `FAIL` when a majority of the slot-owning primaries in the local slot table (`owners / 2 + 1`, counting any down slot owner in `owners`) have reported it in `pfail` or `fail` within `2 * node_timeout_ms`. Nodes gossip `FAIL` marks in `fail`, and any node seeing a peer in a live peer's `fail` list marks it `FAIL` as well. Slots owned by a `FAIL` primary answer `-CLUSTERDOWN`, stay listed against the failed owner in `CLUSTER NODES` with `fail`, and are omitted from `CLUSTER SLOTS`.
- **Clearing FAIL:** `FAIL` clears immediately once the node is reachable again if it is a replica or owns no slots. If it still owns slots, `FAIL` clears after `2 * node_timeout_ms` of continuous reachability with no failover taking its slots.
- **3-primary minimum:** Because quorum is `owners / 2 + 1` including the dead primary, a cluster needs **at least three slot-owning primaries** to agree on `FAIL` (and to tolerate losing one primary without losing quorum).
- **Minority write stop (fencing):** A primary that can reach fewer than a majority of slot-owning primaries (itself included) for `node_timeout_ms` stops serving its own slots—returning `-CLUSTERDOWN` for every read and write—and reports `cluster_state:fail` in `CLUSTER INFO` until a majority is reachable again. A replica in the same situation stops serving `READONLY` reads. A single-primary cluster (`owners = 1`, quorum `1`) is never in a minority.

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
# Get this node's id
valkey-cli -c -p 6381 CLUSTER MYID

# Get cluster status (including cluster_current_epoch and cluster_my_epoch)
valkey-cli -c -p 6381 CLUSTER INFO

# List cluster nodes
valkey-cli -c -p 6381 CLUSTER NODES

# Show slot assignments
valkey-cli -c -p 6381 CLUSTER SLOTS

# Show Redis 7 shard topology
valkey-cli -c -p 6381 CLUSTER SHARDS

# Planned zero-loss failover on a synced replica (returns OK right away, runs in background)
valkey-cli -p 6384 CLUSTER FAILOVER

# Promote a synced replica via a manual election when the primary is down (no FAIL wait)
valkey-cli -p 6384 CLUSTER FAILOVER FORCE

# Promote a synced replica without an election when a majority of primaries is down
valkey-cli -p 6384 CLUSTER FAILOVER TAKEOVER

# Make an empty, slotless primary (or an existing replica) replicate a primary at runtime
valkey-cli -p 6385 CLUSTER REPLICATE node-1
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

A node can replicate another node's slots. Replication is asynchronous: it gives you a live copy for high availability and replicas that cluster clients can read from. It is not a backup (deletes replicate too), and an unplanned failover loses writes the primary acknowledged but had not yet replicated. Failover is automatic in clusters with at least three primaries, or can be triggered manually with `CLUSTER FAILOVER [FORCE|TAKEOVER]`. Design and protocol details are in [REPLICATION.md](REPLICATION.md) and [FAILOVER.md](FAILOVER.md).

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

- **A primary needs a `[replication]` section too**, or it keeps no change index and replicas cannot sync: an empty `[replication]` is enough (or one with only `tombstone_retention_secs`). Without the section a node pays nothing per write for replication, and `BLOBASAUR.SYNC` and `BLOBASAUR.WATERMARK` return an error.
- Turning replication off and on again makes replicas resync. While it is off the node keeps no change index, so on the next start with `[replication]` it rebuilds the index and starts a new history (a new replid on a primary). Every replica of it then copies everything again. A replica that ran without `[replication]` forgets its sync state and resyncs from its primary on the next start, because its data may have drifted in between.
- `num_shards` and the storage compression algorithm must match the primary. The primary rejects a replica that differs.
- A new replica copies everything from the primary on first start; nothing else to do.
- `tombstone_retention_secs` (default 86400) is how long a primary remembers deletes for disconnected replicas. A replica away for longer does a full resync.
- A replica cannot replicate another replica.

### Reading from replicas

Send `READONLY` on a connection, and a replica serves reads (`GET`, `EXISTS`, `TTL`, `HGET`, `HEXISTS`) for its primary's slots. With go-redis set `ReadOnly: true` (add `RouteByLatency` or `RouteRandomly` to pick among replicas). Writes still go to the primary. Replica reads are eventually consistent, with no read-your-writes guarantee. A replica serves reads only once every shard has finished its first copy.

### Monitoring

The replication metrics are listed in [METRICS.md](METRICS.md). The one to alert on is `blobasaur_replication_staleness_seconds` on replicas (for example `> 60`): it keeps rising if the stream, the primary or the replica's writer stalls. Watch `blobasaur_replication_resyncs_total` for unexpected full copies, and compare `blobasaur_replication_latest_lsn` on the primary with `blobasaur_replication_caught_up_lsn` on the replica for LSN lag.

### Failover

Blobasaur supports Redis-Cluster-style automatic failover elections as well as manual `CLUSTER FAILOVER`, `CLUSTER FAILOVER FORCE`, and `CLUSTER FAILOVER TAKEOVER`. Never delete `<data_dir>/cluster-state.json` during failover. Roles, epochs, slot tables, `last_vote_epoch`, and the per-primary vote backoff (`last_vote_for`) are all persisted there so returning primaries demote and restarted voters never double-vote.

#### Automatic failover (3-primary minimum)

Automatic failover requires **at least three slot-owning primaries**. Both `FAIL` agreement and election victory require a majority (`owners / 2 + 1`) of all slot-owning primaries in the slot table, **counting the dead primary**. A 1- or 2-primary cluster can never reach majority when a primary dies.

When a primary is marked `FAIL`:

1. **Eligibility:** A replica stands for election only if its primary is `FAIL` and owns slots, every shard is synced (`synced = 1`) under the primary's replids, and every shard received a replication frame within `node_timeout_ms * replica_validity_factor` before the failure (default `10`; set `0` to disable the staleness check).
2. **Rank delay:** Each eligible replica waits `500 ms + random(0..500 ms) + rank * 1000 ms`, where rank `0` is the replica of that primary with the highest sum of per-shard `caught_up_lsn` (ties broken by node id). If a better replica appears before the timer fires, the slower replica re-arms.
3. **Election:** The candidate increments and persists `current_epoch`, then sends `BLOBASAUR.FAILOVER-AUTH` concurrently to every slot-owning primary over its Redis port. A slot-owning primary votes (`+VOTE <epoch>`) at most once per epoch, only for a known replica of a `FAIL` primary (unless `MANUAL`), at most once per failed primary within twice the election window (`2 * max(2 * node_timeout_ms, 2 s)`, at least `4 s`, persisted in `last_vote_for`), and only if none of the requested slots have a newer epoch in its table. Voters persist `last_vote_epoch` and `last_vote_for` to `cluster-state.json` before replying.
4. **Promotion and convergence:** With a majority of votes within the election window (`max(2 * node_timeout_ms, 2 s)`, measured from before sending the first vote request), the winner stops its sync tasks, re-checks that the election deadline has not passed right before committing `role = primary` at the election epoch in `cluster-state.json` (abandoning the attempt if it ran late), switches every shard to `primary` with a new `replid`, gossips its claim, and begins serving the slots. Sibling replicas re-point to the winner automatically and resync from scratch. When the old primary returns (or a partition heals), it sees its slots bound at a higher epoch, drops them (`MOVED`), drains in-flight writes, demotes to a replica of the winner, and resyncs (discarding any unreplicated writes). If an election does not reach majority or misses its deadline before commit, the replica retries no sooner than `4 * max(2 * node_timeout_ms, 2 s)` (at least `8 s`, twice the vote backoff) after the start of the previous attempt, plus a fresh rank delay and jitter.

**Operational timing assumption:** The persisted vote backoff (`2 * max(2 * node_timeout_ms, 2 s)`, at least `4 s`) and the per-slot epoch check together prevent a stale second replica from taking slots from the first winner before its claim propagates. Because the winner must commit promotion within the first half of that window (`max(2 * node_timeout_ms, 2 s)`) and voters will not vote again for the same failed primary for the full backoff, the remaining half is reserved for gossip to deliver the winner's claim to voters; the retry interval (`4 * max(2 * node_timeout_ms, 2 s)`, at least `8 s`) exceeds `vote_backoff + election_timeout` so split votes clear all voter backoffs before the next round. Keep `gossip_interval_ms` much smaller than `node_timeout_ms` (defaults: `1000 ms` vs `15000 ms`), avoid extreme GC/disk stalls on voters, and preserve `cluster-state.json` across restarts.

#### Planned failover (`CLUSTER FAILOVER`)

Run `CLUSTER FAILOVER` (with no option) on a synced replica to move its primary's slots without losing acknowledged writes:

```bash
valkey-cli -p 6384 CLUSTER FAILOVER
```

The command replies `+OK` right away and runs the handover in the background with a 5 s replica timeout (`CLUSTER_MF_TIMEOUT`), matching Redis Cluster. Writes to the primary's slots can block for up to 10 s (`CLUSTER_MF_TIMEOUT * CLUSTER_MF_PAUSE_MULT`) while the handover is in flight:

1. The replica sends `BLOBASAUR.MFSTART` to its primary. The primary pauses writes to its slots for up to 10 s (reads continue), waits for in-flight write handlers to queue, runs a writer barrier on every shard so every acknowledged write (including queued `async_write` ops) is committed, and replies with each shard's `replid` and committed LSN.
2. The replica waits until every shard's `caught_up_lsn` reaches the primary's LSN under the same `replid`.
3. The replica runs a `MANUAL` election immediately (no `FAIL` mark or rank delay needed) and promotes on a majority of slot-owning primaries before its 5 s deadline.
4. Once the old primary receives the higher-epoch claim over gossip, it ends the pause right away, redirects paused writes with `MOVED` to the new primary, demotes to a replica of the winner, and resyncs. Sibling replicas re-point automatically.

If any step does not finish within 5 s, the replica abandons the failover without starting a late election and sends a best-effort `BLOBASAUR.MFABORT` to release the primary's pause promptly; if that message is lost, the primary's 10 s pause expires on its own. Check logs or `CLUSTER INFO` (`cluster_stats_manual_failover_started`, `cluster_stats_manual_failover_succeeded`, `cluster_stats_manual_failover_aborted`) and `CLUSTER NODES` to verify the outcome.

To check replication lag before maintenance (or during monitoring), `BLOBASAUR.WATERMARK` returns `[shard, replid, lsn]` per shard: the committed LSN after a writer barrier on a primary, and `caught_up_lsn` (or `not-synced`) on a replica.

#### Unplanned manual failover (`FORCE` and `TAKEOVER`)

Use `FORCE` or `TAKEOVER` when the primary is down or unreachable and cannot complete the write-pause handshake:

- **`CLUSTER FAILOVER FORCE`** (sent to a synced replica): skips the primary handshake and immediately runs a `MANUAL` election without waiting for `node_timeout_ms` (`FAIL`) or the rank delay. Voters still enforce majority quorum, single vote per epoch, persisted `last_vote_for` backoff, and the slot-epoch check. If the old primary comes back later, it demotes automatically when it hears the new claim.
- **`CLUSTER FAILOVER TAKEOVER`** (sent to a synced replica): skips failure detection and elections completely, bumps `current_epoch`, and promotes immediately. Use this only when both the primary and a majority of primaries are unavailable. Fencing the old primary is the operator's job, because an unfenced partitioned primary can keep accepting writes until it hears the new claim.

**Reassigning or adding a replica at runtime:** on a replica, or on an empty primary with no assigned slots, run `valkey-cli -p <port> CLUSTER REPLICATE <primary-node-id>` to start replicating `<primary-node-id>` without restarting.

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

**Two-primary clusters and minority fencing:** With minority write fencing enabled, a cluster with exactly two slot-owning primaries has a quorum of 2 (`2 / 2 + 1`). If one primary goes down or is partitioned, the surviving primary can reach only 1 of 2 primaries and will stop serving its own slots (`-CLUSTERDOWN`, `cluster_state:fail`) after `node_timeout_ms`, matching Redis Cluster. Run either a single primary (quorum 1, never in a minority) or at least three primaries so the cluster can survive losing one primary.

## Current Limitations

1. **Asynchronous Replication:** Replicas can lag, and an unplanned failover loses unreplicated writes (see [Replication](#replication))
2. **No Online Resharding:** Slot assignments are configured statically

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
