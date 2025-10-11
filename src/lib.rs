//! Blobasaur - A Redis-compatible blob storage system with sharding support
//!
//! This library provides a Redis-compatible interface for storing and retrieving
//! binary data with features like:
//! - Consistent hashing-based sharding
//! - Storage compression
//! - Namespaced operations
//! - Cluster support
//! - Shard migration capabilities

pub mod app_state;
pub mod cluster;
pub mod compression;
pub mod config;
pub mod http_server;
pub mod metrics;
pub mod migration;
pub mod redis;
pub mod server;
pub mod shard_manager;
pub mod storage;

pub use app_state::AppState;
pub use config::Cfg;
pub use migration::MigrationManager;
