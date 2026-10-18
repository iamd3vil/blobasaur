use futures::future::join_all;
use miette::{Result, miette};
use moka::future::Cache;
use mpchash::HashRing;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::{Connection, SqliteConnection, SqlitePool};
use std::fs;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard, Semaphore, mpsc, watch};
use tokio::task::JoinSet;

use crate::compression::{self, Compressor};
// Import ShardWriteOperation from shard_manager
use crate::{
    cluster::ClusterManager,
    config::Cfg,
    metrics::{Metrics, ShardMetrics},
    replication,
    shard_manager::{Pending, ShardSender, ShardWriteOperation, wal_checkpoint_truncate},
};

pub(crate) const CREATE_BLOBS_TABLE: &str = "CREATE TABLE IF NOT EXISTS blobs (
    key TEXT PRIMARY KEY,
    data BLOB,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    expires_at INTEGER,
    version INTEGER NOT NULL DEFAULT 0
)";

/// The change index: one row per key, holding the LSN of its last change.
/// AUTOINCREMENT keeps LSNs from being reused after a row is deleted, and
/// writers use INSERT OR REPLACE so a rewritten key gets a new LSN.
pub(crate) const CREATE_REPL_LOG: &str = "CREATE TABLE IF NOT EXISTS repl_log (
    lsn INTEGER PRIMARY KEY AUTOINCREMENT,
    tbl TEXT NOT NULL,
    key TEXT NOT NULL,
    deleted INTEGER NOT NULL,
    at INTEGER NOT NULL,
    UNIQUE (tbl, key)
)";

pub(crate) const CREATE_REPL_STATE: &str = "CREATE TABLE IF NOT EXISTS repl_state (
    k TEXT PRIMARY KEY,
    v
)";

#[derive(Hash)]
struct ShardNode(u64);

pub struct AppState {
    pub cfg: Cfg,
    pub shard_senders: Vec<ShardSender>,
    pub db_pools: Vec<SqlitePool>,
    pub write_db_pools: Vec<SqlitePool>,
    /// Cache for inflight write operations to prevent race conditions in async mode.
    /// When async_write=true, SET and DEL return immediately but the actual
    /// database write happens asynchronously. This cache stores the pending
    /// value, or a delete marker, so that reads see the write even before it
    /// commits.
    pub inflight_cache: Cache<String, Pending>,
    /// Cache for inflight namespaced write operations (namespace:key -> data).
    /// Same as inflight_cache but for HSET/HGET operations. The key format is
    /// "namespace:key" to avoid collisions between namespaces.
    pub inflight_hcache: Cache<String, Pending>,
    /// Cluster manager for Redis cluster protocol
    pub cluster_manager: Option<ClusterManager>,
    pub compressor: Option<Box<dyn Compressor>>,
    /// Metrics collector
    pub metrics: Metrics,
    /// Per shard, the highest committed `repl_log` LSN. Published by whoever
    /// commits, so subscribers can wake on new changes.
    pub commit_watch: Vec<Arc<watch::Sender<u64>>>,
    /// Per shard, the cursors of the replication streams being served; see
    /// [`replication::ShardPins`].
    pub repl_pins: Vec<Arc<replication::ShardPins>>,
    /// Per shard, on a replica: READONLY reads hold it shared from before their
    /// readiness check until their data is read, and a resync holds it exclusive
    /// while it unsyncs and wipes the shard, so no admitted read sees the wipe.
    replica_gates: Vec<RwLock<()>>,
    /// Per shard, the replid that `blobasaur_replication_info` last reported.
    replids: Vec<Mutex<Option<String>>>,

    ring: HashRing<ShardNode>,
}

impl AppState {
    pub async fn new(
        cfg: Cfg,
        shard_receivers_out: &mut Vec<mpsc::Receiver<ShardWriteOperation>>,
    ) -> Result<Self> {
        if let Some(primary) = cfg.replica_of() {
            let cluster = cfg.cluster.as_ref().filter(|c| c.enabled);
            let Some(cluster) = cluster else {
                return Err(miette!("replication requires [cluster] to be enabled"));
            };
            if cluster.node_id == primary {
                return Err(miette!("replica_of must be another node, not {}", primary));
            }
        }

        let mut shard_senders_vec = Vec::new();
        // Clear the output vector first to ensure it's empty
        shard_receivers_out.clear();

        let queue_capacity = cfg.batch_size.unwrap_or(100);
        for i in 0..cfg.num_shards {
            let (sender, receiver) = mpsc::channel(queue_capacity);
            shard_senders_vec.push(ShardSender::new(sender, ShardMetrics::new(i)));
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

        // auto_vacuum only takes effect on a brand-new DB file, before anything
        // else (e.g. the switch to WAL) initializes it. It isn't a pool connect
        // option because on an existing DB, setting it takes the write lock, and
        // every new pool connection doing that made writer batches fail with
        // SQLITE_BUSY. So create the table once on a short-lived connection that
        // sets it, before the pools open the file. Older shards keep their mode;
        // the enforcement below handles them.
        for i in 0..cfg.num_shards {
            let options =
                build_sqlite_connect_options(&cfg, i).pragma("auto_vacuum", "INCREMENTAL");
            let mut conn = SqliteConnection::connect_with(&options)
                .await
                .map_err(|e| miette!("failed to open shard {} DB: {}", i, e))?;
            sqlx::query(CREATE_BLOBS_TABLE)
                .execute(&mut conn)
                .await
                .map_err(|e| miette!("failed to create table in shard {} DB: {}", i, e))?;
            for ddl in [CREATE_REPL_LOG, CREATE_REPL_STATE] {
                sqlx::query(ddl).execute(&mut conn).await.map_err(|e| {
                    miette!("failed to create change index in shard {} DB: {}", i, e)
                })?;
            }
            conn.close()
                .await
                .map_err(|e| miette!("failed to close shard {} DB: {}", i, e))?;
        }

        // Index pre-existing rows and settle the replid before any writer runs.
        let replica = cfg.replica_of().is_some();
        if !replica {
            let options: Vec<_> = (0..cfg.num_shards)
                .map(|i| build_sqlite_connect_options(&cfg, i))
                .collect();
            replication::ensure_promotable(&options).await?;
        }
        let mut commit_watch = Vec::with_capacity(cfg.num_shards);
        for i in 0..cfg.num_shards {
            let options = build_sqlite_connect_options(&cfg, i);
            let lsn = replication::init_shard(&options, cfg.new_replid, replica)
                .await
                .map_err(|e| miette!("failed to initialize change index of shard {}: {}", i, e))?;
            commit_watch.push(Arc::new(watch::channel(lsn).0));
        }

        let mut db_pools_futures = vec![];
        let mut write_db_pools_futures = vec![];
        let pool_max_connections = cfg.sqlite_pool_max_connections();
        let write_pool_max_connections = cfg.sqlite_write_pool_max_connections();
        for i in 0..cfg.num_shards {
            db_pools_futures.push(
                SqlitePoolOptions::new()
                    .max_connections(pool_max_connections)
                    .connect_with(build_sqlite_connect_options(&cfg, i)),
            );

            write_db_pools_futures.push(
                SqlitePoolOptions::new()
                    .max_connections(write_pool_max_connections)
                    .connect_with(build_sqlite_connect_options(&cfg, i)),
            );
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
        let write_db_pool_results: Vec<Result<SqlitePool, sqlx::Error>> =
            join_all(write_db_pools_futures).await;
        let write_db_pools: Vec<SqlitePool> = write_db_pool_results
            .into_iter()
            .enumerate()
            .map(|(i, res)| {
                res.unwrap_or_else(|e| {
                    panic!(
                        "Failed to connect to shard {} dedicated writer DB: {}",
                        i, e
                    )
                })
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
            sqlx::query(CREATE_BLOBS_TABLE)
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

        // Caches for inflight async ops. An entry must never be evicted before
        // its op commits, or reads would miss an acknowledged write or delete.
        // Each entry belongs to an op that is queued (at most `queue_capacity`
        // per shard) or in the batch being written (at most `batch_size`, which
        // is no larger), so this capacity is never reached.
        let inflight_capacity = (cfg.num_shards * 2 * queue_capacity) as u64;
        let inflight_cache = Cache::new(inflight_capacity);
        let inflight_hcache = Cache::new(inflight_capacity);

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

                let manager = ClusterManager::new(
                    cluster_config,
                    gossip_bind_addr,
                    redis_addr,
                    cfg.replica_of(),
                    cfg.num_shards,
                )
                .await;
                match manager {
                    Ok(manager) => {
                        // A slot conflict stops startup, replication or not.
                        if let Err(e) = manager.claim_slots().await {
                            manager.shutdown().await;
                            return Err(miette!("slot conflict: {}", e));
                        }
                        tracing::info!("Cluster manager initialized successfully");
                        Some(manager)
                    }
                    Err(e) if cfg.replication.is_some() => {
                        return Err(miette!("failed to initialize cluster manager: {}", e));
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

        let repl_pins = (0..cfg.num_shards).map(|_| Arc::default()).collect();
        let replica_gates = (0..cfg.num_shards).map(|_| RwLock::new(())).collect();
        let replids = (0..cfg.num_shards).map(|_| Mutex::new(None)).collect();
        let state = AppState {
            cfg,
            shard_senders: shard_senders_vec,
            db_pools,
            write_db_pools,
            inflight_cache,
            inflight_hcache,
            cluster_manager,
            compressor,
            metrics,
            commit_watch,
            repl_pins,
            replica_gates,
            replids,
            ring,
        };
        // A new replica has none until its first sync.
        for (i, pool) in state.db_pools.iter().enumerate() {
            if let Ok((replid, ..)) = replication::primary_state(pool).await
                && !replid.is_empty()
            {
                state.set_replid(i, &replid);
            }
        }
        Ok(state)
    }

    /// Reports `replid` as the shard's current one in `blobasaur_replication_info`,
    /// and zeroes the series of the one it replaces.
    pub fn set_replid(&self, shard: usize, replid: &str) {
        let mut current = self.replids[shard].lock().unwrap();
        let info = |replid: &str| {
            metrics::gauge!(
                "blobasaur_replication_info",
                "shard" => shard.to_string(),
                "replid" => replid.to_string()
            )
        };
        if let Some(old) = current.as_deref().filter(|old| *old != replid) {
            info(old).set(0.0);
        }
        info(replid).set(1.0);
        *current = Some(replid.to_string());
    }

    /// Records that shard `shard_id` stopped cleanly; see
    /// [`replication::mark_clean_shutdown`]. Its pools must be closed first.
    pub async fn mark_clean_shutdown(&self, shard_id: usize) -> Result<()> {
        replication::mark_clean_shutdown(&build_sqlite_connect_options(&self.cfg, shard_id)).await
    }

    /// Set the gauges that are sampled rather than updated in place: queue
    /// depth and pool connections. Called on each metrics scrape.
    pub fn record_gauges(&self) {
        for (i, sender) in self.shard_senders.iter().enumerate() {
            let shard = i.to_string();
            metrics::gauge!("blobasaur_write_queue_depth", "shard" => shard.clone())
                .set(sender.queue_depth() as f64);
            let role = if self.cfg.replica_of().is_some() {
                "replica"
            } else {
                metrics::gauge!("blobasaur_replication_latest_lsn", "shard" => shard.clone())
                    .set(*self.commit_watch[i].borrow() as f64);
                "primary"
            };
            metrics::gauge!("blobasaur_replication_role", "shard" => shard.clone(), "role" => role)
                .set(1.0);
            for (name, pool) in [
                ("read", &self.db_pools[i]),
                ("write", &self.write_db_pools[i]),
            ] {
                let idle = pool.num_idle();
                let in_use = (pool.size() as usize).saturating_sub(idle);
                for (state, value) in [("idle", idle), ("in_use", in_use)] {
                    metrics::gauge!(
                        "blobasaur_sqlite_pool_connections",
                        "shard" => shard.clone(),
                        "pool" => name,
                        "state" => state
                    )
                    .set(value as f64);
                }
            }
        }
    }

    /// A replica's READONLY read takes this before it is routed, and holds it
    /// until its DB read is done. Primaries and other connections take nothing.
    pub async fn replica_read_gate(
        &self,
        shard: usize,
        conn_readonly: bool,
    ) -> Option<RwLockReadGuard<'_, ()>> {
        if conn_readonly && self.cfg.replica_of().is_some() {
            Some(self.replica_gates[shard].read().await)
        } else {
            None
        }
    }

    /// Held by a resync of `shard` from unsyncing it until the wipe has committed.
    pub async fn replica_reset_gate(&self, shard: usize) -> RwLockWriteGuard<'_, ()> {
        self.replica_gates[shard].write().await
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

fn build_sqlite_connect_options(cfg: &Cfg, shard_id: usize) -> SqliteConnectOptions {
    let db_path = format!("{}/shard_{}.db", cfg.data_dir, shard_id);

    // Get SQLite configuration with defaults
    let cache_size_mb = cfg.sqlite_cache_size_per_connection_mb();
    let busy_timeout_ms = cfg.sqlite_busy_timeout_ms();
    let synchronous = cfg.sqlite_synchronous();
    let mmap_size = cfg.sqlite_mmap_per_connection_bytes();

    let mut connect_options = SqliteConnectOptions::from_str(&format!("sqlite:{}", db_path))
        .unwrap_or_else(|_| panic!("Failed to parse connection string for shard {}", shard_id))
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .busy_timeout(std::time::Duration::from_millis(busy_timeout_ms));

    // No auto_vacuum here: see AppState::new.
    connect_options = connect_options
        .pragma("synchronous", synchronous.as_str())
        .pragma("cache_size", format!("-{}", cache_size_mb * 1024))
        .pragma("foreign_keys", "true");

    if mmap_size > 0 {
        connect_options = connect_options.pragma("mmap_size", mmap_size.to_string());
    }

    connect_options
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

        // The conversion has already succeeded at this point; a busy checkpoint only
        // delays returning the WAL space, so it shouldn't block startup.
        if let Err(error) = wal_checkpoint_truncate(&mut *conn).await {
            tracing::warn!(
                shard_id,
                error,
                "WAL truncate after auto_vacuum upgrade did not complete"
            );
        }

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
    if let Ok(entries) = fs::read_dir(data_path)
        && entries.count() == 0
    {
        return Ok(());
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
            if file_name_str.starts_with("shard_")
                && file_name_str.ends_with(".db")
                && let Some(num_str) = file_name_str
                    .strip_prefix("shard_")
                    .and_then(|s| s.strip_suffix(".db"))
                && let Ok(shard_num) = num_str.parse::<usize>()
                && shard_num >= expected_shards
            {
                extra_shards.push(shard_num);
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
            replication: None,
            cluster: None,
            metrics: None,
            shutdown_timeout_secs: None,
            max_request_size_mb: None,
            cleanup_chunk_size: None,
            cleanup_interval_secs: None,
            new_replid: false,
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
        // Real legacy shards already hold data.
        sqlx::query("CREATE TABLE blobs (key TEXT PRIMARY KEY, data BLOB, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, expires_at INTEGER, version INTEGER NOT NULL DEFAULT 0)")
            .execute(&pool)
            .await
            .expect("failed to create legacy blobs table");

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
    async fn new_pool_connections_do_not_take_the_write_lock() {
        let temp_dir = TempDir::new().expect("failed to create temp dir");
        let cfg = test_cfg_with_auto_upgrade(
            temp_dir.path().to_string_lossy().into_owned(),
            1,
            true,
            Some(100),
        );
        let mut receivers = Vec::new();
        let _app_state = AppState::new(cfg.clone(), &mut receivers)
            .await
            .expect("failed to initialize app state");

        // Stand-in for the shard writer mid-batch.
        let mut writer = SqliteConnection::connect_with(&build_sqlite_connect_options(&cfg, 0))
            .await
            .expect("failed to open writer connection");
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut writer)
            .await
            .expect("failed to take write lock");

        // Pools open connections like this under load and when recycling them.
        // Setting auto_vacuum on connect needed the write lock and failed here.
        let mut fresh = SqliteConnection::connect_with(&build_sqlite_connect_options(&cfg, 0))
            .await
            .expect("opening a connection must not need the write lock");
        let mode = sqlx::query_scalar::<_, i64>("PRAGMA auto_vacuum")
            .fetch_one(&mut fresh)
            .await
            .expect("failed to read auto_vacuum");
        assert_eq!(mode, SQLITE_AUTO_VACUUM_INCREMENTAL);

        sqlx::query("ROLLBACK")
            .execute(&mut writer)
            .await
            .expect("failed to release write lock");
    }

    #[tokio::test]
    async fn replication_info_follows_the_replid() {
        use crate::metrics::rendered_value;
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let _guard = metrics::set_default_local_recorder(&recorder);
        let temp_dir = TempDir::new().expect("failed to create temp dir");
        let cfg = test_cfg(temp_dir.path().to_string_lossy().into_owned(), 1);
        let mut receivers = Vec::new();
        let state = AppState::new(cfg, &mut receivers).await.unwrap();
        let series =
            |replid: &str| format!(r#"blobasaur_replication_info{{shard="0",replid="{replid}"}}"#);

        // A primary reports the replid it started with.
        let stored: String = sqlx::query_scalar("SELECT v FROM repl_state WHERE k = 'replid'")
            .fetch_one(&state.db_pools[0])
            .await
            .unwrap();
        let output = recorder.handle().render();
        assert_eq!(rendered_value(&output, &series(&stored)), 1.0);

        // A new one replaces it, which is zeroed, not removed.
        state.set_replid(0, "next");
        state.set_replid(0, "next");
        let output = recorder.handle().render();
        assert_eq!(rendered_value(&output, &series(&stored)), 0.0);
        assert_eq!(rendered_value(&output, &series("next")), 1.0);
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

        // temp_store=MEMORY would make a full VACUUM build the whole rebuilt DB in RAM.
        for pool in &app_state.write_db_pools {
            let temp_store = sqlx::query_scalar::<_, i64>("PRAGMA temp_store")
                .fetch_one(pool)
                .await
                .expect("failed to read temp_store");
            assert_eq!(
                temp_store, 0,
                "writer connections should use the default temp_store"
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
