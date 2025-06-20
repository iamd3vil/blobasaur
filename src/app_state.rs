use futures::future::join_all;
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

        AppState {
            cfg,
            shard_senders: shard_senders_vec,
            db_pools,
        }
    }

    pub fn get_shard(&self, key: &str) -> usize {
        let mut hasher = fnv::FnvHasher::default();
        hasher.write(key.as_bytes());
        let hash = hasher.finish();
        hash as usize % self.cfg.num_shards
    }
}
