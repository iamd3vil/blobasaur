#[cfg(feature = "backend-sqlite")]
use futures::future::join_all;
use miette::Result;
use moka::future::Cache;
use mpchash::HashRing;
#[cfg(feature = "backend-sqlite")]
use sqlx::SqlitePool;
#[cfg(feature = "backend-sqlite")]
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode};
use std::fs;
#[cfg(feature = "backend-sqlite")]
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::mpsc;

use crate::compression::{self, Compressor};
// Import ShardWriteOperation from shard_manager
#[cfg(feature = "backend-sqlite")]
use crate::storage::sqlite::SqliteStorage;
#[cfg(feature = "backend-turso")]
use crate::storage::turso::TursoStorage;
use crate::{
    cluster::ClusterManager,
    config::{Cfg, StorageBackend},
    metrics::Metrics,
    shard_manager::ShardWriteOperation,
    storage::Storage,
};
use bytes::Bytes;

#[derive(Hash)]
struct ShardNode(u64);

pub struct AppState {
    pub cfg: Cfg,
    pub shard_senders: Vec<mpsc::Sender<ShardWriteOperation>>,
    pub storage: Vec<Arc<dyn Storage>>,
    /// Cache for inflight write operations to prevent race conditions in async mode.
    /// When async_write=true, SET operations return OK immediately but the actual
    /// database write happens asynchronously. This cache stores the key-value pairs
    /// for pending writes so that GET requests can return the correct data even
    /// before the write completes, preventing race conditions.
    pub inflight_cache: Cache<String, Bytes>,
    /// Cache for inflight namespaced write operations (namespace:key -> data).
    /// Same as inflight_cache but for HSET/HGET operations. The key format is
    /// "namespace:key" to avoid collisions between namespaces.
    pub inflight_hcache: Cache<String, Bytes>,
    /// Cluster manager for Redis cluster protocol
    pub cluster_manager: Option<ClusterManager>,
    pub compressor: Option<Box<dyn Compressor>>,
    /// Metrics collector
    pub metrics: Metrics,

    ring: HashRing<ShardNode>,
}

impl AppState {
    pub async fn new(
        cfg: Cfg,
        shard_receivers_out: &mut Vec<mpsc::Receiver<ShardWriteOperation>>,
    ) -> Result<Self> {
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

        // Validate the number of shards in the data directory
        validate_shard_count(&cfg.data_dir, cfg.num_shards)?;

        let mut storage_handles: Vec<Arc<dyn Storage>> = Vec::new();
        let backend = cfg.backend();

        match backend {
            StorageBackend::Sqlite => {
                #[cfg(not(feature = "backend-sqlite"))]
                {
                    return Err(miette::miette!(
                        "The binary was built without the 'backend-sqlite' feature"
                    ));
                }

                #[cfg(feature = "backend-sqlite")]
                {
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

                        connect_options = connect_options
                            .pragma("synchronous", "OFF")
                            .pragma("cache_size", "-100000")
                            .pragma("temp_store", "MEMORY");

                        db_pools_futures.push(sqlx::SqlitePool::connect_with(connect_options))
                    }

                    let db_pool_results: Vec<Result<SqlitePool, sqlx::Error>> =
                        join_all(db_pools_futures).await;
                    let db_pools: Vec<SqlitePool> = db_pool_results
                        .into_iter()
                        .enumerate()
                        .map(|(i, res)| {
                            res.unwrap_or_else(|e| {
                                panic!("Failed to connect to shard {} DB: {}", i, e)
                            })
                        })
                        .collect();

                    for pool in db_pools.into_iter() {
                        let storage = Arc::new(SqliteStorage::new(pool));
                        storage.ensure_base_schema().await.unwrap_or_else(|e| {
                            panic!("Failed to initialize storage backend: {}", e)
                        });
                        storage_handles.push(storage as Arc<dyn Storage>);
                    }
                }
            }
            StorageBackend::Turso => {
                #[cfg(not(feature = "backend-turso"))]
                {
                    return Err(miette::miette!(
                        "Turso backend requested but the binary was built without the 'backend-turso' feature"
                    ));
                }

                #[cfg(feature = "backend-turso")]
                {
                    for i in 0..cfg.num_shards {
                        let data_dir = cfg.data_dir.clone();
                        let db_path = format!("{}/shard_{}.db", data_dir, i);
                        let storage = Arc::new(TursoStorage::new(&db_path).await.map_err(|e| {
                            miette::miette!("Failed to initialize Turso shard {}: {}", i, e)
                        })?);
                        storage.ensure_base_schema().await.map_err(|e| {
                            miette::miette!(
                                "Failed to initialize Turso schema for shard {}: {}",
                                i,
                                e
                            )
                        })?;
                        storage_handles.push(storage as Arc<dyn Storage>);
                    }
                }
            }
        }

        // Create caches for inflight operations
        // Use a reasonable capacity - adjust based on expected load
        let inflight_cache = Cache::new(10_000);
        let inflight_hcache = Cache::new(10_000);

        // Initialize cluster manager if clustering is enabled
        let cluster_manager = if let Some(ref cluster_config) = cfg.cluster {
            if cluster_config.enabled {
                let gossip_bind_addr = format!("0.0.0.0:{}", cluster_config.port)
                    .parse()
                    .expect("Invalid cluster bind address");

                let redis_addr = cfg
                    .addr
                    .as_deref()
                    .unwrap_or("0.0.0.0:6379")
                    .parse()
                    .expect("Invalid Redis server address");

                match ClusterManager::new(cluster_config, gossip_bind_addr, redis_addr).await {
                    Ok(manager) => {
                        tracing::info!("Cluster manager initialized successfully");
                        Some(manager)
                    }
                    Err(e) => {
                        tracing::error!("Failed to initialize cluster manager: {}", e);
                        None
                    }
                }
            } else {
                None
            }
        } else {
            None
        };

        let compressor = match &cfg.storage_compression {
            Some(comp) if comp.enabled => Some(compression::init_compression(comp.clone())),
            _ => None,
        };

        // Initialize metrics
        let metrics = Metrics::new();

        let ring = HashRing::new();
        for i in 0..cfg.num_shards {
            ring.add(ShardNode(i as u64));
        }

        Ok(AppState {
            cfg,
            shard_senders: shard_senders_vec,
            storage: storage_handles,
            inflight_cache,
            inflight_hcache,
            cluster_manager,
            compressor,
            metrics,
            ring,
        })
    }

    pub fn get_shard(&self, key: &str) -> usize {
        let token = self.ring.node(&key).unwrap();
        token.node().0 as usize
    }

    /// Get a namespaced cache key for HGET/HSET operations
    pub fn namespaced_key(&self, namespace: &str, key: &str) -> String {
        format!("{}:{}", namespace, key)
    }
}

/// Validates that the number of shard files in the data directory matches the expected count
fn validate_shard_count(data_dir: &str, expected_shards: usize) -> Result<()> {
    use std::path::Path;

    let data_path = Path::new(data_dir);

    // If the directory doesn't exist yet, that's fine - it will be created
    if !data_path.exists() {
        return Ok(());
    }

    // If the directory is empty, that's also fine - no validation needed
    if let Ok(entries) = fs::read_dir(data_path) {
        if entries.count() == 0 {
            return Ok(());
        }
    }

    // Count existing shard files
    let mut actual_shards = 0;
    for i in 0..expected_shards {
        let shard_path = data_path.join(format!("shard_{}.db", i));
        if shard_path.exists() {
            actual_shards += 1;
        }
    }

    // Check if we have more shards than expected
    // Look for any shard files beyond the expected range
    let mut extra_shards = Vec::new();
    if let Ok(entries) = fs::read_dir(data_path) {
        for entry in entries.flatten() {
            let file_name = entry.file_name();
            let file_name_str = file_name.to_string_lossy();
            if file_name_str.starts_with("shard_") && file_name_str.ends_with(".db") {
                if let Some(num_str) = file_name_str
                    .strip_prefix("shard_")
                    .and_then(|s| s.strip_suffix(".db"))
                {
                    if let Ok(shard_num) = num_str.parse::<usize>() {
                        if shard_num >= expected_shards {
                            extra_shards.push(shard_num);
                        }
                    }
                }
            }
        }
    }

    // Report any mismatches
    if actual_shards != expected_shards || !extra_shards.is_empty() {
        let mut error_msg = format!(
            "Shard count mismatch: expected {} shards, but found {} existing shards",
            expected_shards, actual_shards
        );

        if !extra_shards.is_empty() {
            error_msg.push_str(&format!(". Found extra shard files: {:?}", extra_shards));
        }

        if actual_shards < expected_shards {
            error_msg.push_str(&format!(
                ". Missing {} shard files.",
                expected_shards - actual_shards
            ));
        }

        return Err(miette::miette!(error_msg));
    }

    Ok(())
}
