<div align="right">
  <a href="https://zerodha.tech">
    <img src="https://zerodha.tech/static/images/github-badge.svg" width=140 />
  </a>
</div>

# Blobnom

Blobnom is a high-performance, sharded blob storage server written in Rust. It uses Axum for its HTTP server and SQLite as the backend for each shard, providing a simple yet robust solution for storing and retrieving binary large objects (blobs).

## Features

- **Sharding:** Distributes data across multiple SQLite databases (shards) for improved concurrency and scalability. The shard for a given key is determined by an FNV hash of the key.
- **HTTP API:** Provides a simple RESTful API for blob operations:
  - `GET /blob/{key}`: Retrieve a blob.
  - `POST /blob/{key}`: Store or replace a blob.
  - `DELETE /blob/{key}`: Delete a blob.
- **Asynchronous Operations:** Leverages Tokio and async Rust for non-blocking I/O, ensuring efficient handling of concurrent requests.
- **Configurable:**
  - Number of shards.
  - Data directory for storing SQLite files.
  - Optional storage compression (not yet implemented but planned via config).
  - Optional output compression (not yet implemented but planned via config).
  - Asynchronous write mode for improved API response times.
  - Write batching for improved database throughput.
- **SQLite Backend:** Each shard uses its own SQLite database, simplifying deployment and management.
- **Graceful Shutdown:** (Implicitly handled by Axum and Tokio)

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
      `sh
      cargo run --release
    # or
    just run-release
    `  The server will start by default on`0.0.0.0:3000`.

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
- `async_write` (bool, optional): If `true`, enables asynchronous write operations where the API responds immediately after queueing the operation instead of waiting for database completion. (Default: `false` if not specified)
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

## API Endpoints

All blob operations are performed via the `/blob/{key}` path.

- ### `POST /blob/{key}`

  - **Description:** Stores or replaces a blob.
  - **Path Parameters:**
    - `key` (String): The unique identifier for the blob.
  - **Request Body:** The binary data of the blob.
  - **Responses:**
    - `201 Created`: Blob stored successfully (synchronous mode).
    - `202 Accepted`: Blob queued for storage (asynchronous mode when `async_write = true`).
    - `500 Internal Server Error`: If an error occurs during the operation.

- ### `GET /blob/{key}`

  - **Description:** Retrieves a blob.
  - **Path Parameters:**
    - `key` (String): The unique identifier for the blob.
  - **Responses:**
    - `200 OK`: Returns the blob data in the response body.
    - `404 Not Found`: If the blob with the given key does not exist.
    - `500 Internal Server Error`: If an error occurs during the operation.

- ### `DELETE /blob/{key}`
  - **Description:** Deletes a blob.
  - **Path Parameters:**
    - `key` (String): The unique identifier for the blob.
  - **Responses:**
    - `204 No Content`: Blob deleted successfully (synchronous mode).
    - `202 Accepted`: Blob queued for deletion (asynchronous mode when `async_write = true`).
    - `500 Internal Server Error`: If an error occurs during the operation.

## Project Structure

- `src/main.rs`: Entry point, sets up tracing, configuration, Axum router, and spawns shard writer tasks.
- `src/config.rs`: Handles loading and validation of the `config.toml` file.
- `src/http_server.rs`: Defines the Axum `AppState`, HTTP handlers for blob operations, and logic for determining the shard for a key.
- `src/shard_manager.rs`: Defines the `ShardWriteOperation` enum and the `shard_writer_task` responsible for handling write operations (Set, Delete) for each shard via a message queue.
- `config.toml`: Configuration file (user-created).
- `Justfile`: Contains `just` commands for common development tasks.
- `Cargo.toml`: Rust package manifest.

## Key Dependencies

- [Axum](https://github.com/tokio-rs/axum): Web framework for building HTTP servers.
- [Tokio](https://tokio.rs/): Asynchronous runtime for Rust.
- [SQLx](https://github.com/launchbadge/sqlx): Asynchronous SQL toolkit for Rust, used here with SQLite.
- [Serde](https://serde.rs/): Framework for serializing and deserializing Rust data structures.
- [Config](https://crates.io/crates/config): Layered configuration system for Rust applications.
- [Miette](https://crates.io/crates/miette): Fancy diagnostic reporting library.
- [Fnv](https://crates.io/crates/fnv): For FNV hashing to determine shard index.
- [Tracing](https://crates.io/crates/tracing): Application-level tracing framework.

## Performance Features

### Write Batching

Blobnom supports batching multiple write operations into single database transactions for improved throughput:

- **Benefits**: 2-10x write throughput improvement for high-volume workloads
- **Configuration**: Set `batch_size > 1` to enable batching
- **Timeout**: Use `batch_timeout_ms` to prevent excessive latency
- **Tradeoffs**: Slightly increased latency per operation, better overall throughput

### Asynchronous Writes

When `async_write = true`, the API responds immediately after queueing operations:

- **Benefits**: Lower API response times, better user experience
- **Tradeoffs**: Operations may fail after API responds (check logs)
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

## Future Enhancements (Ideas)

- Implement actual data compression for `storage_compression` and `output_compression`.
- Add more robust error handling and logging.
- Implement authentication and authorization.
- Add metrics and monitoring.
- Support for different hashing algorithms for sharding.
- Streaming support for very large blobs.
- Replication or durability options beyond single SQLite files.
- More comprehensive tests.
