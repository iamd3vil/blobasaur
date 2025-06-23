use futures::future::join_all;
use moka::future::Cache;
use sqlx::SqlitePool;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode};
use std::fs;
use std::hash::Hasher;
use std::str::FromStr;
use tokio::sync::mpsc;

// Import ShardWriteOperation from shard_manager
use crate::{config::Cfg, shard_manager::ShardWriteOperation};

pub struct AppState {
    pub cfg: Cfg,
    pub shard_senders: Vec<mpsc::Sender<ShardWriteOperation>>,
    pub db_pools: Vec<SqlitePool>,
    /// Cache for inflight write operations to prevent race conditions in async mode.
    /// When async_write=true, SET operations return OK immediately but the actual
    /// database write happens asynchronously. This cache stores the key-value pairs
    /// for pending writes so that GET requests can return the correct data even
    /// before the write completes, preventing race conditions.
    pub inflight_cache: Cache<String, Vec<u8>>,
    /// Cache for inflight namespaced write operations (namespace:key -> data).
    /// Same as inflight_cache but for HSET/HGET operations. The key format is
    /// "namespace:key" to avoid collisions between namespaces.
    pub inflight_hcache: Cache<String, Vec<u8>>,
}

impl AppState {
    pub async fn new(
        cfg: Cfg,
        shard_receivers_out: &mut Vec<mpsc::Receiver<ShardWriteOperation>>,
    ) -> Self {
        let mut shard_senders_vec = Vec::new();
        // Clear the output vector first to ensure it's empty
        shard_receivers_out.clear();

        for _ in 0..cfg.num_shards {
            let (sender, receiver) = mpsc::channel(cfg.batch_size.unwrap_or(100));
            shard_senders_vec.push(sender);
            shard_receivers_out.push(receiver); // Populate the output vector with receivers
        }

        // Create data directory if it doesn't exist
        fs::create_dir_all(&cfg.data_dir).expect("Failed to create data directory");

        let mut db_pools_futures = vec![];
        for i in 0..cfg.num_shards {
            let data_dir = cfg.data_dir.clone();
            let db_path = format!("{}/shard_{}.db", data_dir, i);

            let mut connect_options =
                SqliteConnectOptions::from_str(&format!("sqlite:{}", db_path))
                    .expect(&format!(
                        "Failed to parse connection string for shard {}",
                        i
                    ))
                    .create_if_missing(true)
                    .journal_mode(SqliteJournalMode::Wal)
                    .busy_timeout(std::time::Duration::from_millis(5000));

            // These PRAGMAs are often set for performance with WAL mode.
            // `synchronous = OFF` is safe except for power loss.
            // `cache_size` is negative to indicate KiB, so -4000 is 4MB.
            // `temp_store = MEMORY` avoids disk I/O for temporary tables.
            connect_options = connect_options
                .pragma("synchronous", "OFF")
                .pragma("cache_size", "-100000") // 4MB cache per shard
                .pragma("temp_store", "MEMORY");

            db_pools_futures.push(sqlx::SqlitePool::connect_with(connect_options))
        }

        let db_pool_results: Vec<Result<SqlitePool, sqlx::Error>> =
            join_all(db_pools_futures).await;
        let db_pools: Vec<SqlitePool> = db_pool_results
            .into_iter()
            .enumerate()
            .map(|(i, res)| {
                res.unwrap_or_else(|e| panic!("Failed to connect to shard {} DB: {}", i, e))
            })
            .collect();

        for (i, pool) in db_pools.iter().enumerate() {
            sqlx::query(
                "CREATE TABLE IF NOT EXISTS blobs (
                    key TEXT PRIMARY KEY,
                    data BLOB,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    expires_at INTEGER,
                    version INTEGER NOT NULL DEFAULT 0
                )",
            )
            .execute(pool)
            .await
            .unwrap_or_else(|e| panic!("Failed to create table in shard {} DB: {}", i, e));
        }

        // Create caches for inflight operations
        // Use a reasonable capacity - adjust based on expected load
        let inflight_cache = Cache::new(10_000);
        let inflight_hcache = Cache::new(10_000);

        AppState {
            cfg,
            shard_senders: shard_senders_vec,
            db_pools,
            inflight_cache,
            inflight_hcache,
        }
    }

    pub fn get_shard(&self, key: &str) -> usize {
        let mut hasher = fnv::FnvHasher::default();
        hasher.write(key.as_bytes());
        let hash = hasher.finish();
        hash as usize % self.cfg.num_shards
    }

    /// Get a namespaced cache key for HGET/HSET operations
    pub fn namespaced_key(&self, namespace: &str, key: &str) -> String {
        format!("{}:{}", namespace, key)
    }
}
