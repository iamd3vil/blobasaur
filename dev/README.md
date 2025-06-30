# Blobasaur Cluster Development Setup

This directory contains Docker Compose configuration for running a 3-node Blobasaur cluster locally for development and testing.

## Quick Start

1. **Build the binary (optional - will auto-build on first run):**
   ```bash
   just cluster-build
   ```

2. **Start the cluster:**
   ```bash
   just cluster-up
   ```

3. **Initialize slot assignments:**
   ```bash
   just cluster-setup
   ```

4. **Check cluster status:**
   ```bash
   just cluster-status
   ```

## Available Commands

- `just cluster-clean` - Clean target directory (fixes architecture conflicts)
- `just cluster-build` - Pre-build the binary (optional)
- `just cluster-up` - Start the 3-node cluster
- `just cluster-down` - Stop the cluster
- `just cluster-logs` - View cluster logs
- `just cluster-status` - Check cluster health
- `just cluster-setup` - Initialize slot assignments
- `just cluster-reset` - Reset and restart cluster with fresh data

## Cluster Configuration

The cluster consists of 3 nodes:

- **Node 1**: `localhost:6381` (Redis) + `localhost:7001` (Gossip)
- **Node 2**: `localhost:6382` (Redis) + `localhost:7002` (Gossip)  
- **Node 3**: `localhost:6383` (Redis) + `localhost:7003` (Gossip)

Hash slots are distributed as follows:
- Node 1: slots 0-5460 (5461 slots)
- Node 2: slots 5461-10922 (5462 slots)
- Node 3: slots 10923-16383 (5461 slots)

## Testing the Cluster

### Using Redis CLI

Connect to any node and test cluster operations:

```bash
# Connect with cluster support
redis-cli -c -p 6381

# Test setting keys (will auto-redirect to correct node)
SET user:1000 "Alice"
SET user:2000 "Bob"
SET user:3000 "Charlie"

# Check which node handles a specific key
CLUSTER KEYSLOT user:1000

# View cluster topology
CLUSTER NODES
CLUSTER SLOTS
```

### Manual Slot Assignment

If you need to manually assign slots:

```bash
# Add specific slots to a node
redis-cli -p 6381 CLUSTER ADDSLOTS 0 1 2 3 4

# Remove slots from a node
redis-cli -p 6381 CLUSTER DELSLOTS 0 1 2 3 4

# Check current slot assignments
redis-cli -p 6381 CLUSTER SLOTS
```

### Using Redis Cluster Clients

**Python (redis-py-cluster):**
```python
from rediscluster import RedisCluster

nodes = [
    {"host": "localhost", "port": 6381},
    {"host": "localhost", "port": 6382},
    {"host": "localhost", "port": 6383}
]

rc = RedisCluster(startup_nodes=nodes, decode_responses=True)
rc.set("test:key", "test:value")
print(rc.get("test:key"))
```

**Node.js (ioredis):**
```javascript
const Redis = require('ioredis');

const cluster = new Redis.Cluster([
  { host: 'localhost', port: 6381 },
  { host: 'localhost', port: 6382 },
  { host: 'localhost', port: 6383 }
]);

await cluster.set('test:key', 'test:value');
const value = await cluster.get('test:key');
```

## Troubleshooting

### Check if nodes are running
```bash
docker ps | grep blobasaur
```

### View logs for specific node
```bash
docker logs blobasaur-node1
docker logs blobasaur-node2
docker logs blobasaur-node3
```

### Verify cluster connectivity
```bash
# Check if nodes can reach each other
redis-cli -p 6381 CLUSTER NODES
redis-cli -p 6382 CLUSTER NODES
redis-cli -p 6383 CLUSTER NODES
```

### Reset cluster state
```bash
just cluster-reset
```

### Fix architecture conflicts
If you see "cannot execute binary file" errors:
```bash
just cluster-clean
just cluster-up
```

## Development Notes

- Mounts source code and builds once, reusing the binary for fast development
- Automatically handles architecture compatibility (builds inside container if needed)
- Each node has its own data volume to persist data across restarts
- Configurations are mounted read-only from the dev directory
- The cluster uses Docker's internal networking for gossip communication
- All Redis ports are exposed to localhost for easy testing
- Shared cargo cache across containers for faster builds
- If you get "cannot execute binary file" errors, run `just cluster-clean` first

## File Structure

```
dev/
├── README.md              # This file
├── docker-compose.yml     # Docker Compose configuration
├── Dockerfile            # Blobasaur container image
├── config.node1.toml     # Node 1 configuration
├── config.node2.toml     # Node 2 configuration
└── config.node3.toml     # Node 3 configuration
```
