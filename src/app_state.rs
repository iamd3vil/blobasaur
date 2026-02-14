use futures::future::join_all;
use miette::{Result, miette};
use moka::future::Cache;
use mpchash::HashRing;
use sqlx::SqlitePool;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use std::fs;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::{Semaphore, mpsc};
use tokio::task::JoinSet;

use crate::compression::{self, Compressor};
// Import ShardWriteOperation from shard_manager
use crate::{
    cluster::ClusterManager, config::Cfg, metrics::Metrics, shard_manager::ShardWriteOperation,
};
use bytes::Bytes;

#[derive(Hash)]
struct ShardNode(u64);

pub struct AppState {
    pub cfg: Cfg,
    pub shard_senders: Vec<mpsc::Sender<ShardWriteOperation>>,
    pub db_pools: Vec<SqlitePool>,
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

        // Initialize metrics early so startup storage checks can emit counters.
        let metrics = Metrics::new();
        let auto_upgrade_legacy_auto_vacuum = cfg.sqlite_auto_upgrade_legacy_auto_vacuum();
        let auto_upgrade_legacy_auto_vacuum_concurrency =
            cfg.sqlite_auto_upgrade_legacy_auto_vacuum_concurrency();

        let mut db_pools_futures = vec![];
        let pool_max_connections = cfg.sqlite_pool_max_connections();
        for i in 0..cfg.num_shards {
            let data_dir = cfg.data_dir.clone();
            let db_path = format!("{}/shard_{}.db", data_dir, i);

            // Get SQLite configuration with defaults
            let cache_size_mb = cfg.sqlite_cache_size_per_connection_mb();
            let busy_timeout_ms = cfg.sqlite_busy_timeout_ms();
            let synchronous = cfg.sqlite_synchronous();
            let mmap_size = cfg.sqlite_mmap_per_connection_bytes();

            let mut connect_options =
                SqliteConnectOptions::from_str(&format!("sqlite:{}", db_path))
                    .expect(&format!(
                        "Failed to parse connection string for shard {}",
                        i
                    ))
                    .create_if_missing(true)
                    .journal_mode(SqliteJournalMode::Wal)
                    .busy_timeout(std::time::Duration::from_millis(busy_timeout_ms));

            // Configure SQLite PRAGMAs for optimal server performance
            connect_options = connect_options
                .pragma("auto_vacuum", "INCREMENTAL")
                .pragma("synchronous", synchronous.as_str())
                .pragma("cache_size", format!("-{}", cache_size_mb * 1024)) // Negative means KiB
                .pragma("temp_store", "MEMORY")
                .pragma("foreign_keys", "true");

            // Enable memory-mapped I/O if configured
            if mmap_size > 0 {
                connect_options = connect_options.pragma("mmap_size", mmap_size.to_string());
            }

            db_pools_futures.push(
                SqlitePoolOptions::new()
                    .max_connections(pool_max_connections)
                    .connect_with(connect_options),
            )
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

        run_startup_auto_vacuum_enforcement(
            &db_pools,
            auto_upgrade_legacy_auto_vacuum,
            auto_upgrade_legacy_auto_vacuum_concurrency,
            &metrics,
        )
        .await?;

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

            // Create index on expires_at for efficient expiry queries
            sqlx::query(
                "CREATE INDEX IF NOT EXISTS idx_expires_at ON blobs(expires_at) WHERE expires_at IS NOT NULL",
            )
            .execute(pool)
            .await
            .unwrap_or_else(|e| panic!("Failed to create expires_at index in shard {} DB: {}", i, e));
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

        let ring = HashRing::new();
        for i in 0..cfg.num_shards {
            ring.add(ShardNode(i as u64));
        }

        Ok(AppState {
            cfg,
            shard_senders: shard_senders_vec,
            db_pools,
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

const SQLITE_AUTO_VACUUM_NONE: i64 = 0;
const SQLITE_AUTO_VACUUM_FULL: i64 = 1;
const SQLITE_AUTO_VACUUM_INCREMENTAL: i64 = 2;

fn sqlite_auto_vacuum_mode_name(mode: i64) -> &'static str {
    match mode {
        SQLITE_AUTO_VACUUM_NONE => "NONE",
        SQLITE_AUTO_VACUUM_FULL => "FULL",
        SQLITE_AUTO_VACUUM_INCREMENTAL => "INCREMENTAL",
        _ => "UNKNOWN",
    }
}

async fn enforce_auto_vacuum_mode(
    pool: &SqlitePool,
    shard_id: usize,
    auto_upgrade_enabled: bool,
    metrics: &Metrics,
) -> Result<()> {
    let started_at = std::time::Instant::now();
    let mut conn = pool.acquire().await.map_err(|error| {
        miette!(
            "failed to acquire SQLite connection for shard {} DB: {}",
            shard_id,
            error
        )
    })?;

    let mode = sqlx::query_scalar::<_, i64>("PRAGMA auto_vacuum")
        .fetch_one(&mut *conn)
        .await
        .map_err(|error| {
            miette!(
                "failed to read PRAGMA auto_vacuum for shard {} DB: {}",
                shard_id,
                error
            )
        })?;

    if mode == SQLITE_AUTO_VACUUM_INCREMENTAL {
        return Ok(());
    }

    tracing::warn!(
        shard_id,
        auto_vacuum_mode = mode,
        auto_vacuum_mode_name = sqlite_auto_vacuum_mode_name(mode),
        "Shard DB auto_vacuum is not INCREMENTAL"
    );

    if !auto_upgrade_enabled {
        metrics.record_sqlite_auto_vacuum_misconfigured();
        metrics
            .record_sqlite_auto_vacuum_upgrade_run("skipped_disabled", Some(started_at.elapsed()));
        return Ok(());
    }

    tracing::info!(
        shard_id,
        from_auto_vacuum_mode = mode,
        from_auto_vacuum_mode_name = sqlite_auto_vacuum_mode_name(mode),
        "Auto-upgrading shard DB auto_vacuum mode to INCREMENTAL"
    );

    let upgrade_result = async {
        sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
            .execute(&mut *conn)
            .await
            .map_err(|error| {
                miette!(
                    "shard {}: wal_checkpoint(TRUNCATE) failed during auto_vacuum upgrade: {}",
                    shard_id,
                    error
                )
            })?;

        sqlx::query("PRAGMA auto_vacuum = INCREMENTAL")
            .execute(&mut *conn)
            .await
            .map_err(|error| {
                miette!(
                    "shard {}: setting PRAGMA auto_vacuum=INCREMENTAL failed: {}",
                    shard_id,
                    error
                )
            })?;

        sqlx::query("VACUUM")
            .execute(&mut *conn)
            .await
            .map_err(|error| {
                miette!(
                    "shard {}: VACUUM failed while applying auto_vacuum upgrade: {}",
                    shard_id,
                    error
                )
            })?;

        let verified_mode = sqlx::query_scalar::<_, i64>("PRAGMA auto_vacuum")
            .fetch_one(&mut *conn)
            .await
            .map_err(|error| {
                miette!(
                    "failed to re-read PRAGMA auto_vacuum for shard {} DB: {}",
                    shard_id,
                    error
                )
            })?;

        if verified_mode != SQLITE_AUTO_VACUUM_INCREMENTAL {
            return Err(miette!(
                "shard {} auto_vacuum upgrade verification failed: expected INCREMENTAL (2), got {} ({})",
                shard_id,
                verified_mode,
                sqlite_auto_vacuum_mode_name(verified_mode)
            ));
        }

        Ok::<(), miette::Error>(())
    }
    .await;

    match upgrade_result {
        Ok(()) => {
            metrics.record_sqlite_auto_vacuum_upgrade_run("ok", Some(started_at.elapsed()));
            tracing::info!(
                shard_id,
                "Auto-upgraded shard DB auto_vacuum mode to INCREMENTAL"
            );
            Ok(())
        }
        Err(error) => {
            metrics.record_sqlite_auto_vacuum_upgrade_run("error", Some(started_at.elapsed()));
            Err(error)
        }
    }
}

async fn run_startup_auto_vacuum_enforcement(
    pools: &[SqlitePool],
    auto_upgrade_enabled: bool,
    concurrency: usize,
    metrics: &Metrics,
) -> Result<()> {
    if pools.is_empty() {
        return Ok(());
    }

    let semaphore = Arc::new(Semaphore::new(concurrency.max(1)));
    let mut join_set = JoinSet::new();

    for (shard_id, pool) in pools.iter().enumerate() {
        let semaphore = semaphore.clone();
        let pool = pool.clone();
        let metrics = metrics.clone();

        join_set.spawn(async move {
            let _permit = semaphore
                .acquire_owned()
                .await
                .map_err(|_| "startup auto_vacuum semaphore closed".to_string())?;

            enforce_auto_vacuum_mode(&pool, shard_id, auto_upgrade_enabled, &metrics)
                .await
                .map_err(|e| e.to_string())
        });
    }

    while let Some(next) = join_set.join_next().await {
        match next {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                join_set.abort_all();
                while join_set.join_next().await.is_some() {}
                return Err(miette!("startup auto_vacuum upgrade failed: {}", error));
            }
            Err(error) => {
                join_set.abort_all();
                while join_set.join_next().await.is_some() {}
                return Err(miette!(
                    "startup auto_vacuum upgrade task join error: {}",
                    error
                ));
            }
        }
    }

    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::{path::Path, str::FromStr};
    use tempfile::TempDir;

    fn test_cfg(data_dir: String, num_shards: usize) -> Cfg {
        Cfg {
            data_dir,
            num_shards,
            storage_compression: None,
            async_write: Some(false),
            batch_size: Some(1),
            batch_timeout_ms: Some(0),
            addr: None,
            cluster: None,
            metrics: None,
            sqlite: None,
        }
    }

    fn test_cfg_with_auto_upgrade(
        data_dir: String,
        num_shards: usize,
        auto_upgrade: bool,
        busy_timeout_ms: Option<u64>,
    ) -> Cfg {
        let mut cfg = test_cfg(data_dir, num_shards);
        cfg.sqlite = Some(crate::config::SqliteConfig {
            cache_size_mb: None,
            busy_timeout_ms,
            synchronous: None,
            mmap_size: None,
            max_connections: Some(1),
            auto_upgrade_legacy_auto_vacuum: Some(auto_upgrade),
            auto_upgrade_legacy_auto_vacuum_concurrency: Some(1),
        });
        cfg
    }

    async fn create_legacy_shard_db(path: &Path) {
        let connect_options =
            SqliteConnectOptions::from_str(&format!("sqlite:{}", path.to_string_lossy()))
                .expect("failed to parse sqlite path")
                .create_if_missing(true)
                .journal_mode(SqliteJournalMode::Wal);

        let pool = SqlitePool::connect_with(connect_options)
            .await
            .expect("failed to create legacy shard DB");

        sqlx::query("PRAGMA auto_vacuum = NONE")
            .execute(&pool)
            .await
            .expect("failed to set auto_vacuum=NONE");
        sqlx::query("VACUUM")
            .execute(&pool)
            .await
            .expect("failed to apply auto_vacuum pragma");

        pool.close().await;
    }

    async fn read_persisted_auto_vacuum_mode(path: &Path) -> i64 {
        let connect_options =
            SqliteConnectOptions::from_str(&format!("sqlite:{}", path.to_string_lossy()))
                .expect("failed to parse sqlite path")
                .create_if_missing(true)
                .journal_mode(SqliteJournalMode::Wal);

        let pool = SqlitePool::connect_with(connect_options)
            .await
            .expect("failed to open shard DB");

        let mode = sqlx::query_scalar::<_, i64>("PRAGMA auto_vacuum")
            .fetch_one(&pool)
            .await
            .expect("failed to read auto_vacuum mode");
        pool.close().await;
        mode
    }

    #[tokio::test]
    async fn app_state_initializes_new_shards_with_incremental_auto_vacuum() {
        let temp_dir = TempDir::new().expect("failed to create temp dir");
        let cfg = test_cfg(temp_dir.path().to_string_lossy().into_owned(), 2);

        let mut receivers = Vec::new();
        let app_state = AppState::new(cfg, &mut receivers)
            .await
            .expect("failed to initialize app state");

        for (shard_id, pool) in app_state.db_pools.iter().enumerate() {
            let mode = sqlx::query_scalar::<_, i64>("PRAGMA auto_vacuum")
                .fetch_one(pool)
                .await
                .expect("failed to read auto_vacuum mode");

            assert_eq!(
                mode, SQLITE_AUTO_VACUUM_INCREMENTAL,
                "shard {} should use INCREMENTAL auto_vacuum",
                shard_id
            );
        }
    }

    #[tokio::test]
    async fn app_state_auto_upgrades_legacy_auto_vacuum_mode_by_default() {
        let temp_dir = TempDir::new().expect("failed to create temp dir");
        let db_path = temp_dir.path().join("shard_0.db");
        create_legacy_shard_db(&db_path).await;

        let cfg = test_cfg(temp_dir.path().to_string_lossy().into_owned(), 1);
        let mut receivers = Vec::new();
        let app_state = AppState::new(cfg, &mut receivers)
            .await
            .expect("app state should auto-upgrade legacy shard");
        drop(app_state);

        let persisted_mode = read_persisted_auto_vacuum_mode(&db_path).await;
        assert_eq!(
            persisted_mode, SQLITE_AUTO_VACUUM_INCREMENTAL,
            "startup should persist auto_vacuum=INCREMENTAL for legacy shard DBs"
        );
    }

    #[tokio::test]
    async fn app_state_can_skip_legacy_auto_vacuum_upgrade_when_disabled() {
        let temp_dir = TempDir::new().expect("failed to create temp dir");
        let db_path = temp_dir.path().join("shard_0.db");
        create_legacy_shard_db(&db_path).await;

        let cfg = test_cfg_with_auto_upgrade(
            temp_dir.path().to_string_lossy().into_owned(),
            1,
            false,
            None,
        );
        let mut receivers = Vec::new();
        let app_state = AppState::new(cfg, &mut receivers)
            .await
            .expect("app state startup should succeed with auto-upgrade disabled");
        drop(app_state);

        let persisted_mode = read_persisted_auto_vacuum_mode(&db_path).await;
        assert_ne!(
            persisted_mode, SQLITE_AUTO_VACUUM_INCREMENTAL,
            "legacy shard DB should remain non-INCREMENTAL when auto-upgrade is disabled"
        );
    }

    #[tokio::test]
    async fn app_state_startup_fails_if_legacy_auto_vacuum_upgrade_fails() {
        let temp_dir = TempDir::new().expect("failed to create temp dir");
        let db_path = temp_dir.path().join("shard_0.db");
        create_legacy_shard_db(&db_path).await;

        let lock_options =
            SqliteConnectOptions::from_str(&format!("sqlite:{}", db_path.to_string_lossy()))
                .expect("failed to parse sqlite path")
                .create_if_missing(true)
                .journal_mode(SqliteJournalMode::Wal);
        let lock_pool = SqlitePool::connect_with(lock_options)
            .await
            .expect("failed to open locking connection");

        sqlx::query("BEGIN EXCLUSIVE")
            .execute(&lock_pool)
            .await
            .expect("failed to hold exclusive lock");

        let cfg = test_cfg_with_auto_upgrade(
            temp_dir.path().to_string_lossy().into_owned(),
            1,
            true,
            Some(1),
        );
        let mut receivers = Vec::new();
        let result = AppState::new(cfg, &mut receivers).await;

        sqlx::query("ROLLBACK")
            .execute(&lock_pool)
            .await
            .expect("failed to release lock");
        lock_pool.close().await;

        assert!(
            result.is_err(),
            "startup must fail fast when legacy auto_vacuum upgrade fails"
        );
    }
}
