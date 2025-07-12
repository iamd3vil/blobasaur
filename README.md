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
  - [Race Condition Handling](#race-condition-handling)
  - [Redis Cluster Compatibility](#redis-cluster-compatibility)
- [Development](#development)
  - [Project Structure](#project-structure)
  - [Testing](#testing)
  - [Key Dependencies](#key-dependencies)

## Features

- **🚀 High Performance Sharding**: Distributes data across multiple SQLite databases using multi-probe consistent hashing for optimal concurrency and scalability
- **🔄 Shard Migration**: Built-in support for migrating data between different shard configurations with data integrity verification
- **🔌 Redis Protocol Compatible**: Full compatibility with Redis clients using standard commands (`GET`, `SET`, `DEL`, `EXISTS`, `HGET`, `HSET`, `HDEL`, `HEXISTS`)
- **⚡ Asynchronous Operations**: Built on Tokio for non-blocking I/O and efficient concurrent request handling
- **💾 SQLite Backend**: Each shard uses its own SQLite database for simple deployment and reliable storage
- **🗜️ Storage Compression**: Configurable compression with multiple algorithms (Gzip, Zstd, Lz4, Brotli)
- **📊 Namespacing**: Hash-based namespacing using `HGET`/`HSET` commands for logical data organization
- **🏷️ Metadata Tracking**: Automatic tracking of creation time, update time, expiration, and version numbers
- **🌐 Redis Cluster Support**: Full cluster protocol support with automatic node discovery and client redirection
- **⚙️ Highly Configurable**: Flexible configuration for shards, compression, batching, and performance tuning

## Installation

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
   git clone https://github.com/zerodha/blobasaur.git
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

# Get help
blobasaur --help
```

**Available Commands:**
- `serve` (default): Runs the Blobasaur server
- `shard migrate`: Migrates data between different shard configurations

**Global Options:**
- `--config, -c`: Path to configuration file (default: `config.toml`)
- `--help`: Display help information

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

- **`SET key value`**: Store or replace a blob
  ```bash
  redis-cli SET mykey "Hello, World!"
  ```

- **`GET key`**: Retrieve a blob
  ```bash
  redis-cli GET mykey
  ```

- **`DEL key`**: Delete a blob
  ```bash
  redis-cli DEL mykey
  ```

- **`EXISTS key`**: Check if a blob exists
  ```bash
  redis-cli EXISTS mykey
  ```

### Namespaced Commands

Use namespaces to organize data into logical groups:

- **`HSET namespace key value`**: Store in namespace
  ```bash
  redis-cli HSET users:123 name "John Doe"
  redis-cli HSET users:123 email "john@example.com"
  ```

- **`HGET namespace key`**: Retrieve from namespace
  ```bash
  redis-cli HGET users:123 name
  ```

- **`HDEL namespace key`**: Delete from namespace
  ```bash
  redis-cli HDEL users:123 email
  ```

- **`HEXISTS namespace key`**: Check existence in namespace
  ```bash
  redis-cli HEXISTS users:123 name
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
r.hset('users:123', 'name', 'John Doe')
name = r.hget('users:123', 'name')
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

**Basic Cluster Configuration:**
```toml
[cluster]
enabled = true
node_id = "node1"
seeds = ["127.0.0.1:6380"]
advertise_addr = "127.0.0.1"
port = 6379

[[cluster.slot_ranges]]
start = 0
end = 8191
```

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

[Add your license information here]

## Contributing

[Add contributing guidelines here]
