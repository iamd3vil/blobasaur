<div align="right">
  <a href="https://zerodha.tech">
    <img src="https://zerodha.tech/static/images/github-badge.svg" width=140 />
  </a>
</div>

<p align="center">
  <img src="Blobasaur.svg" alt="Blobasaur Logo" width="200"/>
</p>

# Blobasaur

Blobasaur is a high-performance, sharded blob storage server written in Rust. It implements the Redis protocol for client compatibility and uses SQLite as the backend for each shard, providing a simple yet robust solution for storing and retrieving binary large objects (blobs).

## Table of Contents

- [Features](#features)
- [Installation](#installation)
  - [Docker](#docker)
  - [Prerequisites](#prerequisites)
  - [From Source](#from-source)
  - [Cross-compilation](#cross-compilation)
- [Quick Start](#quick-start)
- [CLI Usage](#cli-usage)
- [Configuration](#configuration)
  - [Basic Configuration](#basic-configuration)
  - [Storage Compression](#storage-compression)
  - [Performance Tuning](#performance-tuning)
- [Redis Commands](#redis-commands)
  - [Basic Commands](#basic-commands)
  - [Namespaced Commands](#namespaced-commands)
  - [Using Redis Clients](#using-redis-clients)
- [Shard Migration](#shard-migration)
  - [Overview](#overview)
  - [Usage](#usage)
  - [Migration Process](#migration-process)
  - [Best Practices](#best-practices)
- [Performance Features](#performance-features)
  - [Write Batching](#write-batching)
  - [Asynchronous Writes](#asynchronous-writes)
  - [Storage Compression](#storage-compression-1)
- [Advanced Topics](#advanced-topics)
  - [Database Schema](#database-schema)
  - [TTL and Key Expiration](#ttl-and-key-expiration)
  - [Race Condition Handling](#race-condition-handling)
  - [Redis Cluster Compatibility](#redis-cluster-compatibility)
- [Development](#development)
  - [Project Structure](#project-structure)
  - [Testing](#testing)
  - [Key Dependencies](#key-dependencies)

## Features

- **🚀 High Performance Sharding**: Distributes data across multiple SQLite databases using multi-probe consistent hashing for optimal concurrency and scalability
- **🔄 Shard Migration**: Built-in support for migrating data between different shard configurations with data integrity verification
- **🔌 Redis Protocol Compatible**: Full compatibility with Redis clients using standard commands (`GET`, `SET`, `DEL`, `EXISTS`, `HGET`, `HSET`, `HSETEX`, `HDEL`, `HEXISTS`)
- **⚡ Asynchronous Operations**: Built on Tokio for non-blocking I/O and efficient concurrent request handling
- **💾 SQLite Backend**: Each shard uses its own SQLite database for simple deployment and reliable storage
- **🗜️ Storage Compression**: Configurable compression with multiple algorithms (Gzip, Zstd, Lz4, Brotli)
- **📊 Namespacing**: Hash-based namespacing using `HGET`/`HSET` commands for logical data organization
- **🏷️ Metadata Tracking**: Automatic tracking of creation time, update time, expiration, and version numbers
- **⏰ TTL Support**: Redis-compatible key expiration with `SET EX/PX`, `TTL`, and `EXPIRE` commands plus automatic background cleanup
- **🌐 Redis Cluster Support**: Full cluster protocol support with automatic node discovery and client redirection
- **⚙️ Highly Configurable**: Flexible configuration for shards, compression, batching, and performance tuning

## Installation

### From GitHub Releases

You can download pre-compiled binaries for Linux from the [GitHub Releases page](https://github.com/iamd3vil/blobasaur/releases).

1.  **Download the latest release:**

    For example, to download version `v0.1.0`:
    ```bash
    wget https://github.com/iamd3vil/blobasaur/releases/download/v0.1.0/blobasaur-Linux-musl-x86_64.tar.gz
    ```

2.  **Extract the archive:**
    ```bash
    tar -xvf blobasaur-Linux-musl-x86_64.tar.gz
    ```

3.  **Run the binary:**
    The binary inside the archive is named `blobasaur`. You can run it directly:
    ```bash
    ./blobasaur
    ```

### Docker

Docker images are available on GitHub Container Registry:

```bash
docker pull ghcr.io/iamd3vil/blobasaur:latest
```

Run with a config file:

```bash
docker run -d \
  -p 6379:6379 \
  -v $(pwd)/config.toml:/app/config.toml \
  -v $(pwd)/data:/app/data \
  ghcr.io/iamd3vil/blobasaur:latest \
  blobasaur --config /app/config.toml
```

Available tags:
- `latest` - Latest stable release
- `x.y.z` - Specific version (e.g., `0.2.1`)
- `x.y` - Latest patch for minor version (e.g., `0.2`)
- `main` - Latest build from main branch

### Prerequisites

- **Rust toolchain** (latest stable recommended) - [Install Rust](https://rustup.rs/)
- **`just`** (optional but recommended) - A command runner for development tasks
  ```bash
  cargo install just
  ```
- **`cross`** (optional) - For cross-compilation to different targets
  ```bash
  cargo install cross
  ```

### From Source

You'll need to build from source:

1. **Clone the repository:**
   ```bash
   git clone https://github.com/iamd3vil/blobasaur.git
   cd blobasaur
   ```

2. **Build the project:**
   ```bash
   # Debug build
   cargo build
   # or using just
   just build

   # Release build (recommended for production)
   cargo build --release
   # or using just
   just build-release
   ```

3. **Install the binary:**
   ```bash
   # Install to ~/.cargo/bin
   cargo install --path .

   # Or copy the binary manually
   cp target/release/blobasaur /usr/local/bin/
   ```

### Cross-compilation

For Linux production deployments, you can create a static binary:

```bash
# Build a static Linux MUSL binary
just build-linux

# Binary will be at: target/x86_64-unknown-linux-musl/release/blobasaur
```

## Quick Start

1. **Create a configuration file** (`config.toml`):
   ```toml
   data_dir = "./data"
   num_shards = 4
   addr = "0.0.0.0:6379"
   ```

2. **Run the server:**
   ```bash
   blobasaur
   # or with custom config
   blobasaur --config /path/to/config.toml
   ```

3. **Test with redis-cli:**
   ```bash
   redis-cli -p 6379 SET mykey "Hello, World!"
   redis-cli -p 6379 GET mykey

   # Test TTL functionality
   redis-cli -p 6379 SET session:123 "user_data" EX 60  # Expires in 60 seconds
   redis-cli -p 6379 TTL session:123                    # Check remaining time
   redis-cli -p 6379 EXPIRE mykey 300                   # Set 5 minute expiration
   ```

## CLI Usage

Blobasaur supports multiple modes of operation:

```bash
# Run the server (default behavior)
blobasaur
# or explicitly
blobasaur serve

# Specify a custom config file
blobasaur --config /path/to/config.toml

# Shard migration commands
blobasaur shard migrate <old_shard_count> <new_shard_count>

# Cluster-wide vacuum orchestration
blobasaur shard vacuum --all-shards --mode incremental --budget-mb 128 --nodes 127.0.0.1:6379,127.0.0.1:6380

# Get help
blobasaur --help
```

**Available Commands:**
- `serve` (default): Runs the Blobasaur server
- `shard migrate`: Migrates data between different shard configurations
- `shard vacuum`: Orchestrates node-local shard vacuum across one or more nodes

**Global Options:**
- `--config, -c`: Path to configuration file (default: `config.toml`)
- `--help`: Display help information

## Metrics

Blobasaur exposes a wide range of metrics for monitoring, compatible with Prometheus. You can track command latency, connection counts, cache performance, and much more.

To enable the metrics server, add the following to your `config.toml`:

```toml
[metrics]
enabled = true
addr = "0.0.0.0:9090"
```

For a complete list of available metrics and example usage with Prometheus and Grafana, please see the [METRICS.md](METRICS.md) documentation.

## Configuration

### Basic Configuration

Blobasaur is configured via a `config.toml` file:

```toml
# Required settings
data_dir = "/var/data/blobasaur"    # Directory for SQLite databases
num_shards = 8                      # Number of shards (must be > 0)

# Optional settings
addr = "0.0.0.0:6379"              # Server bind address
async_write = false                 # Enable async writes
batch_size = 1                      # Write batch size
batch_timeout_ms = 0               # Batch timeout in milliseconds
```

### Storage Compression

Configure compression for data at rest:

```toml
[storage_compression]
enabled = true
algorithm = "zstd"  # Options: "none", "gzip", "zstd", "lz4", "brotli"
level = 3           # Compression level (algorithm-specific)
```

**Compression Options:**
- **Zstd**: Best balance of speed and compression ratio (recommended)
- **Lz4**: Fastest compression, lower ratio
- **Gzip**: Good compatibility, moderate performance
- **Brotli**: Best compression ratio, slower

### Performance Tuning

For high-throughput scenarios:

```toml
# High-performance configuration
async_write = true
batch_size = 100
batch_timeout_ms = 10

[storage_compression]
enabled = true
algorithm = "lz4"
level = 1
```

## Redis Commands

### Basic Commands

Blobasaur implements core Redis commands for blob operations:

- **`SET key value [EX seconds] [PX milliseconds]`**: Store or replace a blob with optional TTL
  ```bash
  # Basic SET
  redis-cli SET mykey "Hello, World!"

  # SET with TTL in seconds
  redis-cli SET mykey "Hello, World!" EX 60

  # SET with TTL in milliseconds
  redis-cli SET mykey "Hello, World!" PX 30000
  ```

- **`GET key`**: Retrieve a blob (excludes expired keys)
  ```bash
  redis-cli GET mykey
  ```

- **`DEL key [key ...]`**: Delete one or more blobs (returns count of deleted keys)
  ```bash
  # Delete single key
  redis-cli DEL mykey

  # Delete multiple keys
  redis-cli DEL key1 key2 key3
  ```

- **`EXISTS key`**: Check if a blob exists (excludes expired keys)
  ```bash
  redis-cli EXISTS mykey
  ```

- **`TTL key`**: Get the remaining time to live for a key
  ```bash
  redis-cli TTL mykey
  # Returns:
  # -1 if key exists but has no expiration
  # -2 if key does not exist or has expired
  # positive number: remaining TTL in seconds
  ```

- **`EXPIRE key seconds`**: Set expiration time for an existing key
  ```bash
  redis-cli EXPIRE mykey 120  # Expire in 2 minutes
  # Returns:
  # 1 if expiration was set successfully
  # 0 if key does not exist
  ```

### Namespaced Commands

Use namespaces to organize data into logical groups.

> **Namespace format:** `namespace` must be non-empty and contain only ASCII letters, numbers, and underscore (`[A-Za-z0-9_]`).
> Examples: `users`, `users123`, `users_123`.

- **`HSET namespace key value`**: Store in namespace
  ```bash
  redis-cli HSET users_123 name "John Doe"
  redis-cli HSET users_123 email "john@example.com"
  ```

- **`HSETEX key [options] FIELDS numfields field value [field value ...]`**: Store fields with TTL
  ```bash
  # Set single field with 60 second expiration
  redis-cli HSETEX sessions EX 60 FIELDS 1 user123 "session_data"

  # Set multiple fields with expiration
  redis-cli HSETEX cache EX 300 FIELDS 2 key1 "value1" key2 "value2"

  # Set with millisecond precision
  redis-cli HSETEX temp PX 5000 FIELDS 1 data "temporary"

  # Only set if field doesn't exist (FNX option)
  redis-cli HSETEX users FNX EX 3600 FIELDS 1 newuser "data"

  # Only set if field exists (FXX option)
  redis-cli HSETEX users FXX EX 3600 FIELDS 1 existinguser "updated"
  ```

- **`HGET namespace key`**: Retrieve from namespace
  ```bash
  redis-cli HGET users_123 name
  ```

- **`HDEL namespace key`**: Delete from namespace
  ```bash
  redis-cli HDEL users_123 email
  ```

- **`HEXISTS namespace key`**: Check existence in namespace
  ```bash
  redis-cli HEXISTS users_123 name
  ```

### Using Redis Clients

Any Redis client works with Blobasaur:

```python
# Python example
import redis
r = redis.Redis(host='localhost', port=6379)
r.set('mykey', 'myvalue')
value = r.get('mykey')

# Namespaced operations
r.hset('users_123', 'name', 'John Doe')
name = r.hget('users_123', 'name')

# Namespaced operations with TTL (Redis 8.0 compatible)
# HSETEX syntax: key [options] FIELDS numfields field value [field value ...]
r.execute_command('HSETEX', 'sessions', 'EX', '3600', 'FIELDS', '1', 'user456', 'session_data')

# Set multiple fields with expiration
r.execute_command('HSETEX', 'cache', 'EX', '300', 'FIELDS', '2', 'key1', 'val1', 'key2', 'val2')
```

## Shard Migration

### Overview

Shard migration allows you to change the number of shards in your deployment, enabling you to scale storage and optimize performance.

The migration process:
1. Creates new SQLite databases for the target shard configuration
2. Redistributes existing data according to the new consistent hashing ring
3. Maintains data integrity throughout the process
4. Optionally verifies migration success

### Usage

```bash
# Basic migration from 4 shards to 8 shards
blobasaur shard migrate 4 8

# Migration with custom data directory
blobasaur shard migrate 4 8 --data-dir /custom/path/to/data

# Migration with verification
blobasaur shard migrate 4 8 --verify

# Migration using specific config file
blobasaur --config /path/to/config.toml shard migrate 4 8
```

**Command Options:**
- `old_shard_count`: Current number of shards
- `new_shard_count`: Target number of shards
- `--data-dir, -d`: Override data directory
- `--verify`: Run verification after migration

### Migration Process

1. **Validation**: Checks source shard databases exist and are accessible
2. **Preparation**: Creates new SQLite databases for target configuration
3. **Data Transfer**: Migrates data in batches using new hash ring
4. **Verification** (optional): Compares data integrity between old and new shards

### Best Practices

**Before Migration:**
```bash
# 1. Stop the server
pkill blobasaur

# 2. Backup your data
cp -r /var/data/blobasaur /var/data/blobasaur-backup

# 3. Test on a copy first
cp -r /var/data/blobasaur /tmp/test-migration
```

**Migration Workflow:**
```bash
# 4. Run migration with verification
blobasaur shard migrate 4 8 --verify

# 5. Update config.toml
# Change: num_shards = 4
# To:     num_shards = 8

# 6. Start server with new configuration
blobasaur
```

**Important Considerations:**
- **Downtime Required**: Server must be stopped during migration
- **Disk Space**: Need space for both old and new databases
- **Testing**: Always test migrations on data copies first
- **Recovery**: Keep backups for rollback if needed

## Vacuum Maintenance

- Blobasaur exposes a node-local admin command:
  - `BLOBASAUR.VACUUM SHARD <id|ALL> MODE <incremental|full> BUDGET_MB <n> [DRYRUN]`
- Blobasaur also provides a CLI orchestrator for multi-node runs:
  - `blobasaur shard vacuum --all-shards --mode incremental --budget-mb 128 --nodes <addr1,addr2,...> [--dry-run] [--node-concurrency 1] [--shard-concurrency 1] [--timeout-sec 30]`
- `SHARD ALL` runs serially across local shards and returns per-shard status/results.
- `DRYRUN` is non-mutating and returns per-shard vacuum stats/estimates.
- Default maintenance mode is **incremental vacuum** with conservative defaults (128 MB budget, node concurrency 1, shard concurrency 1).
- The CLI orchestrator continues across node failures and exits non-zero if any node call fails or any shard result is non-`ok`.
- Newly created shard DBs are initialized with `PRAGMA auto_vacuum = INCREMENTAL`.
- On startup, Blobasaur automatically upgrades legacy shard DBs whose persisted `auto_vacuum` is not `INCREMENTAL` by running:
  - `PRAGMA wal_checkpoint(TRUNCATE);`
  - `PRAGMA auto_vacuum = INCREMENTAL;`
  - `VACUUM;`
- Startup fails fast if any shard's auto-upgrade fails, to avoid mixed maintenance state.
- Automatic upgrade is enabled by default and can be disabled with `[sqlite].auto_upgrade_legacy_auto_vacuum = false`.
- Startup upgrade runs with bounded parallelism by default (`[sqlite].auto_upgrade_legacy_auto_vacuum_concurrency = 2`, tuned for common NVMe hosts); set to `1` for HDD/conservative environments.

### Startup Legacy `auto_vacuum` Upgrade Flow

For each configured shard DB (`shard_0.db` through `shard_{num_shards-1}.db`), startup does:

1. Acquire one SQLite connection for the shard upgrade sequence.
2. Read `PRAGMA auto_vacuum`.
3. If mode is already `INCREMENTAL (2)`, continue.
4. If mode is not `INCREMENTAL` and auto-upgrade is disabled, record warning/metrics and continue.
5. If mode is not `INCREMENTAL` and auto-upgrade is enabled, run:
   - `PRAGMA wal_checkpoint(TRUNCATE);`
   - `PRAGMA auto_vacuum = INCREMENTAL;`
   - `VACUUM;`
6. Re-read `PRAGMA auto_vacuum` and require value `2`.
7. On any shard failure, startup exits with error.

Notes:
- Upgrade work is bounded by `[sqlite].auto_upgrade_legacy_auto_vacuum_concurrency` (default `2`).
- This is a one-time per-DB conversion in normal operation.
- Keep sufficient free disk space for `VACUUM` rewrite overhead.

## Performance Features

### Write Batching

Improves throughput by batching multiple operations:

```toml
batch_size = 50          # Operations per batch
batch_timeout_ms = 10    # Max wait time for batch
```

**Benefits:**
- Higher write throughput
- Reduced SQLite transaction overhead
- Better resource utilization

### Asynchronous Writes

Enables immediate responses while writes happen in background:

```toml
async_write = true
```

**Features:**
- Immediate response to clients
- Inflight cache prevents race conditions
- Maintains consistency guarantees

### Storage Compression

Reduces storage requirements and can improve I/O performance:

```toml
[storage_compression]
enabled = true
algorithm = "zstd"
level = 3
```

**Performance Impact:**
- **Zstd**: 20-40% size reduction, minimal CPU overhead
- **Lz4**: 15-25% size reduction, fastest compression
- **Brotli**: 40-60% size reduction, higher CPU usage

## Advanced Topics

### Database Schema

**Default Table (`blobs`):**
```sql
CREATE TABLE blobs (
    key TEXT PRIMARY KEY,
    value BLOB NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    expires_at INTEGER,
    version INTEGER NOT NULL DEFAULT 0
);
```

**Namespaced Tables (`blobs_{namespace}`):**
- Created automatically on first access
- Same schema as default table
- Isolated from other namespaces
- `namespace` must match `[A-Za-z0-9_]+`

### TTL and Key Expiration

**Features:**
- **Redis-Compatible TTL**: Full support for `SET EX/PX`, `TTL`, and `EXPIRE` commands
- **Automatic Expiry**: All read operations (`GET`, `EXISTS`, `TTL`) automatically filter expired keys
- **Background Cleanup**: Per-shard cleanup tasks run every 60 seconds to remove expired keys
- **Efficient Storage**: Uses indexed `expires_at` timestamps for fast expiry queries

**Implementation Details:**
- Expiration timestamps stored as Unix epoch seconds in `expires_at` column
- Database indexes on `expires_at` for efficient cleanup queries
- Background cleanup processes both main `blobs` table and namespaced tables
- Race-free expiry checking: keys are considered expired at query time

**Usage Examples:**
```bash
# Set with 60 second expiration
redis-cli SET session:abc123 "user_data" EX 60

# Check remaining TTL
redis-cli TTL session:abc123

# Add expiration to existing key
redis-cli EXPIRE permanent_key 3600
```

### Race Condition Handling

**The Problem:** Async writes could cause GET requests to miss recently SET data.

**The Solution:** Inflight cache system:
- Tracks pending write operations
- Serves data from cache during async writes
- Automatic cleanup after database commits

### Redis Cluster Compatibility

Full Redis cluster protocol support with:

- **Node Discovery**: Automatic gossip protocol
- **Hash Slots**: 16384 slot distribution
- **Client Redirection**: Automatic MOVED responses
- **Cluster Commands**: `CLUSTER NODES`, `CLUSTER INFO`, etc.

See [config.cluster.example.toml](config.cluster.example.toml) for a complete cluster configuration example with all options documented.

## Development

### Project Structure

```
src/
├── main.rs              # Entry point and CLI handling
├── config.rs            # Configuration management
├── app_state.rs         # Application state and shard routing
├── server.rs            # Redis protocol server
├── shard_manager.rs     # Shard write operations and batching
├── migration.rs         # Shard migration functionality
├── compression.rs       # Storage compression
├── metrics.rs           # Performance metrics
├── http_server.rs       # HTTP metrics endpoint
├── cluster.rs           # Redis cluster protocol
└── redis/               # Redis protocol implementation
    ├── protocol.rs      # RESP parser
    └── integration_tests.rs
```

### Testing

Comprehensive test suite covering:
- **Unit Tests**: RESP protocol parsing and serialization
- **Integration Tests**: Command handling and binary data
- **Protocol Compliance**: Redis compatibility verification

```bash
# Run all tests
cargo test

# Run with output
cargo test -- --nocapture
```

### Key Dependencies

- **[Tokio](https://tokio.rs/)**: Async runtime
- **[SQLx](https://github.com/launchbadge/sqlx)**: Async SQL toolkit
- **[Nom](https://github.com/Geal/nom)**: Parser combinator for RESP protocol
- **[Serde](https://serde.rs/)**: Serialization framework
- **[Miette](https://github.com/zkat/miette)**: Error handling
- **[Tracing](https://tracing.rs/)**: Structured logging
- **[Gumdrop](https://github.com/murarth/gumdrop)**: Command-line parsing

---

## License

This project is licensed under the GNU Affero General Public License v3.0 (AGPL-3.0) - see the [LICENSE](LICENSE) file for details.

The AGPL-3.0 is a copyleft license that requires anyone who distributes the code or runs it on a server to provide the source code to users, including any modifications.
