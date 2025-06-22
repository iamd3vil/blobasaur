<div align="right">
  <a href="https://zerodha.tech">
    <img src="https://zerodha.tech/static/images/github-badge.svg" width=140 />
  </a>
</div>

# Blobnom

Blobnom is a high-performance, sharded blob storage server written in Rust. It implements the Redis protocol for client compatibility and uses SQLite as the backend for each shard, providing a simple yet robust solution for storing and retrieving binary large objects (blobs).

## Features

- **Sharding:** Distributes data across multiple SQLite databases (shards) for improved concurrency and scalability. The shard for a given key is determined by an FNV hash of the key.
- **Redis Protocol:** Implements Redis protocol for client compatibility with the following commands:
  - `GET key`: Retrieve a blob.
  - `SET key value`: Store or replace a blob.
  - `DEL key`: Delete a blob.
  - `EXISTS key`: Check if a blob exists.
  - `HGET namespace key`: Retrieve a blob from a namespace.
  - `HSET namespace key value`: Store or replace a blob in a namespace.
  - `HDEL namespace key`: Delete a blob from a namespace.
  - `HEXISTS namespace key`: Check if a blob exists in a namespace.
- **Asynchronous Operations:** Leverages Tokio and async Rust for non-blocking I/O, ensuring efficient handling of concurrent requests.
- **Configurable:**
  - Number of shards.
  - Data directory for storing SQLite files.
  - Optional storage compression (not yet implemented but planned via config).
  - Optional output compression (not yet implemented but planned via config).
  - Asynchronous write mode for improved response times.
  - Write batching for improved database throughput.
- **SQLite Backend:** Each shard uses its own SQLite database, simplifying deployment and management.
- **Namespacing:** Hash-based namespacing using HGET/HSET commands for organizing data into logical groups.
- **Metadata Tracking:** Each blob includes metadata such as creation time, update time, expiration time, and version number.
- **Graceful Shutdown:** (Implicitly handled by Tokio)

## Getting Started

### Prerequisites

- Rust toolchain (latest stable recommended)
- `just` (a command runner, optional but `Justfile` is provided)
- For cross-compilation (e.g., Linux MUSL builds): `cross`

### Building and Running

1.  **Clone the repository:**

    ```sh
    git clone <repository-url>
    cd blobmom
    ```

2.  **Configuration:**
    Create a `config.toml` file in the root of the project. See the Configuration section below for details. A minimal example:

    ```toml
    data_dir = "blob_data"
    num_shards = 4
    # storage_compression = false # Optional
    # output_compression = false  # Optional
    ```

3.  **Build:**

    - Using Cargo:
      ```sh
      cargo build
      ```
    - Using Just (for convenience):
      ```sh
      just build
      ```
    - For a release build:
      ```sh
      cargo build --release
      # or
      just build-release
      ```

4.  **Run:**
    - Using Cargo:
      ```sh
      cargo run
      ```
    - Using Just:
      ```sh
      just run
      ```
    - To run the release build:
      ```sh
      cargo run --release
      # or
      just run-release
      ```
    
    The server will start by default on `0.0.0.0:6379` (standard Redis port).

### Cross-compilation (Example: Linux MUSL)

The `Justfile` includes targets for cross-compiling, for example, to a static Linux MUSL binary:

```sh
just build-linux
```

This will produce a binary in `target/x86_64-unknown-linux-musl/release/blobnom`.

## Configuration

Blobnom is configured via a `config.toml` file located in the project root.

Key configuration options:

- `data_dir` (String, required): The directory where SQLite database files for each shard will be stored.
- `num_shards` (usize, required): The number of shards to distribute data across. Must be greater than 0.
- `storage_compression` (bool, optional): If `true`, enables compression for data at rest. (Default: `false` if not specified)
- `output_compression` (bool, optional): If `true`, enables compression for HTTP responses. (Default: `false` if not specified)
- `async_write` (bool, optional): If `true`, enables asynchronous write operations where the server responds immediately after queueing the operation instead of waiting for database completion. (Default: `false` if not specified)
- `batch_size` (usize, optional): Maximum number of operations to batch together in a single database transaction. Set to 1 to disable batching. Higher values improve throughput but increase latency. (Default: `1`)
- `batch_timeout_ms` (u64, optional): Maximum time in milliseconds to wait for additional operations before processing a batch. Only relevant when `batch_size > 1`. (Default: `0`)

Example `config.toml`:

```toml
data_dir = "/var/data/blobnom"
num_shards = 8
storage_compression = true
output_compression = false
async_write = true
batch_size = 50
batch_timeout_ms = 10
```

## Redis Commands

Blobnom implements a subset of Redis commands for blob operations:

### Basic Commands

- ### `SET key value`

  - **Description:** Stores or replaces a blob.
  - **Parameters:**
    - `key`: The unique identifier for the blob.
    - `value`: The binary data of the blob.
  - **Responses:**
    - `+OK`: Blob stored successfully (or queued in async mode).
    - `-ERR internal error`: If an error occurs during the operation.

- ### `GET key`

  - **Description:** Retrieves a blob.
  - **Parameters:**
    - `key`: The unique identifier for the blob.
  - **Responses:**
    - Bulk string with blob data if the key exists.
    - `$-1` (null bulk string): If the blob with the given key does not exist.
    - `-ERR database error`: If an error occurs during the operation.

- ### `DEL key`
  - **Description:** Deletes a blob.
  - **Parameters:**
    - `key`: The unique identifier for the blob.
  - **Responses:**
    - `:1`: If the key existed and was deleted.
    - `:0`: If the key did not exist.
    - `-ERR internal error`: If an error occurs during the operation.

- ### `EXISTS key`
  - **Description:** Checks if a blob exists.
  - **Parameters:**
    - `key`: The unique identifier for the blob.
  - **Responses:**
    - `:1`: If the key exists.
    - `:0`: If the key does not exist.
    - `-ERR database error`: If an error occurs during the operation.

### Namespaced Commands

Blobnom supports Redis-style hash operations for namespacing data. Each namespace creates its own isolated table, allowing you to organize your data into logical groups.

- ### `HSET namespace key value`

  - **Description:** Stores or replaces a blob in a namespace.
  - **Parameters:**
    - `namespace`: The namespace identifier (creates table `blobs_namespace`).
    - `key`: The unique identifier for the blob within the namespace.
    - `value`: The binary data of the blob.
  - **Responses:**
    - `+OK`: Blob stored successfully (or queued in async mode).
    - `-ERR internal error`: If an error occurs during the operation.

- ### `HGET namespace key`

  - **Description:** Retrieves a blob from a namespace.
  - **Parameters:**
    - `namespace`: The namespace identifier.
    - `key`: The unique identifier for the blob within the namespace.
  - **Responses:**
    - Bulk string with blob data if the key exists in the namespace.
    - `$-1` (null bulk string): If the blob does not exist in the namespace.
    - `-ERR database error`: If an error occurs during the operation.

- ### `HDEL namespace key`

  - **Description:** Deletes a blob from a namespace.
  - **Parameters:**
    - `namespace`: The namespace identifier.
    - `key`: The unique identifier for the blob within the namespace.
  - **Responses:**
    - `:1`: If the key existed in the namespace and was deleted.
    - `:0`: If the key did not exist in the namespace.
    - `-ERR internal error`: If an error occurs during the operation.

- ### `HEXISTS namespace key`

  - **Description:** Checks if a blob exists in a namespace.
  - **Parameters:**
    - `namespace`: The namespace identifier.
    - `key`: The unique identifier for the blob within the namespace.
  - **Responses:**
    - `:1`: If the key exists in the namespace.
    - `:0`: If the key does not exist in the namespace.
    - `-ERR database error`: If an error occurs during the operation.

### Using Redis Clients

You can use any Redis client to interact with Blobnom:

```bash
# Using redis-cli - Basic operations
redis-cli -p 6379 SET mykey "Hello, World!"
redis-cli -p 6379 GET mykey
redis-cli -p 6379 DEL mykey
redis-cli -p 6379 EXISTS mykey

# Using redis-cli - Namespaced operations
redis-cli -p 6379 HSET users:123 name "John Doe"
redis-cli -p 6379 HSET users:123 email "john@example.com"
redis-cli -p 6379 HGET users:123 name
redis-cli -p 6379 HEXISTS users:123 email
redis-cli -p 6379 HDEL users:123 email

# Using Redis clients in various languages
# Python: redis-py
# Node.js: ioredis or node-redis
# Go: go-redis
# etc.
```

### Example Use Cases for Namespacing

#### User Data Storage
```bash
HSET user:12345 profile '{"name": "John", "age": 30}'
HSET user:12345 preferences '{"theme": "dark", "lang": "en"}'
HGET user:12345 profile
```

#### Session Management
```bash
HSET session:abc123 user_id "12345"
HSET session:abc123 expires_at "1703980800"
HEXISTS session:abc123 user_id
```

#### Configuration Storage
```bash
HSET config:app database_url "sqlite:app.db"
HSET config:app log_level "info"
HGET config:app database_url
```

#### Analytics Data
```bash
HSET metrics:daily:2024-01-01 page_views "1000"
HSET metrics:daily:2024-01-01 unique_users "250"
HGET metrics:daily:2024-01-01 page_views
```

## Project Structure

- `src/main.rs`: Entry point, sets up tracing, configuration, Redis server, and spawns shard writer tasks.
- `src/config.rs`: Handles loading and validation of the `config.toml` file.
- `src/app_state.rs`: Defines the `AppState` structure and logic for determining the shard for a key.
- `src/server.rs`: Implements the Redis protocol server and command handlers.
- `src/redis/`: Redis protocol implementation module.
  - `src/redis/protocol.rs`: Robust RESP protocol parser using nom, with comprehensive error handling.
  - `src/redis/integration_tests.rs`: Extensive integration tests for Redis protocol parsing.
- `src/shard_manager.rs`: Defines the `ShardWriteOperation` enum and the `shard_writer_task` responsible for handling write operations (Set, Delete) for each shard via a message queue.
- `config.toml`: Configuration file (user-created).
- `Justfile`: Contains `just` commands for common development tasks.
- `Cargo.toml`: Rust package manifest.

## Key Dependencies

- [Tokio](https://tokio.rs/): Asynchronous runtime for Rust.
- [SQLx](https://github.com/launchbadge/sqlx): Asynchronous SQL toolkit for Rust, used here with SQLite.
- [Nom](https://crates.io/crates/nom): High-performance parser combinator library for RESP protocol parsing.
- [Serde](https://serde.rs/): Framework for serializing and deserializing Rust data structures.
- [Config](https://crates.io/crates/config): Layered configuration system for Rust applications.
- [Miette](https://crates.io/crates/miette): Fancy diagnostic reporting library.
- [Thiserror](https://crates.io/crates/thiserror): Better error handling with derive macros.
- [Fnv](https://crates.io/crates/fnv): For FNV hashing to determine shard index.
- [Tracing](https://crates.io/crates/tracing): Application-level tracing framework.
- [Chrono](https://crates.io/crates/chrono): Date and time library for handling timestamps.

## Performance Features

### Write Batching

Blobnom supports batching multiple write operations into single database transactions for improved throughput:

- **Benefits**: 2-10x write throughput improvement for high-volume workloads
- **Configuration**: Set `batch_size > 1` to enable batching
- **Timeout**: Use `batch_timeout_ms` to prevent excessive latency
- **Tradeoffs**: Slightly increased latency per operation, better overall throughput

### Asynchronous Writes

When `async_write = true`, the server responds immediately after queueing operations:

- **Benefits**: Lower response times, better user experience
- **Tradeoffs**: Operations may fail after server responds (check logs)
- **Use case**: Write-heavy workloads where eventual consistency is acceptable

### Recommended Configuration

For high-throughput write workloads:
```toml
async_write = true
batch_size = 50
batch_timeout_ms = 10
```

For low-latency, consistency-focused workloads:
```toml
async_write = false
batch_size = 1
```

## Database Schema

The SQLite database schema for each shard includes the following tables:

### Default Table
```sql
CREATE TABLE blobs (
    key TEXT PRIMARY KEY,
    data BLOB,
    created_at INTEGER NOT NULL,    -- Unix timestamp
    updated_at INTEGER NOT NULL,    -- Unix timestamp  
    expires_at INTEGER,             -- Unix timestamp, NULL if no expiration
    version INTEGER NOT NULL DEFAULT 0  -- Incremented on each update
);
```

### Namespaced Tables
Each namespace automatically creates its own table with the naming pattern `blobs_{namespace}`:

```sql
CREATE TABLE blobs_users (
    key TEXT PRIMARY KEY,
    data BLOB,
    created_at INTEGER NOT NULL,    -- Unix timestamp
    updated_at INTEGER NOT NULL,    -- Unix timestamp  
    expires_at INTEGER,             -- Unix timestamp, NULL if no expiration
    version INTEGER NOT NULL DEFAULT 0  -- Incremented on each update
);
```

### Metadata Features

- **Automatic Timestamps**: `created_at` and `updated_at` are automatically managed
- **Versioning**: Each blob update increments the version number starting from 0
- **Expiration**: Blobs with `expires_at` set will be automatically filtered out from GET requests
- **Dynamic Table Creation**: Namespaced tables are created automatically when first accessed
- **Table Caching**: Each shard maintains a cache of known tables to avoid database queries

### Namespace Performance Optimizations

- **Table Existence Caching**: Each shard maintains a HashSet of known tables in memory
- **Dynamic Creation**: Tables are created only when first accessed
- **Batched Operations**: Namespaced operations are batched with regular operations
- **Sharding**: Namespaced data uses the same key-based sharding as regular operations

### Migration Compatibility

Existing GET/SET operations continue to work with the default `blobs` table:
- `GET key` → uses `blobs` table
- `SET key value` → uses `blobs` table
- `HGET namespace key` → uses `blobs_namespace` table
- `HSET namespace key value` → uses `blobs_namespace` table

This allows for gradual migration to namespaced storage without breaking existing applications.

## Testing

Blobnom includes comprehensive test coverage:

- **Unit Tests**: 25 tests covering RESP protocol parsing, serialization, and command validation
- **Integration Tests**: 19 tests covering command handling, binary data, and protocol compliance
- **Total Coverage**: 44 tests ensuring robust Redis protocol implementation

Run tests with:
```sh
cargo test
```

For specific test suites:
```sh
cargo test redis::protocol      # Protocol parser tests
cargo test redis::integration   # Integration tests
```

## Future Enhancements (Ideas)

- Implement actual data compression for `storage_compression` and `output_compression`.
- Add more robust error handling and logging.
- Implement authentication and authorization.
- Add metrics and monitoring.
- Support for different hashing algorithms for sharding.
- Streaming support for very large blobs.
- Replication or durability options beyond single SQLite files.
- Blob expiration cleanup background task.
- Blob listing and search capabilities.
- Additional Redis commands relevant to blob storage (EXPIRE, TTL).
