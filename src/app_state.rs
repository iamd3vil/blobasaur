use futures::future::join_all;
use miette::{Result, miette};
use moka::future::Cache;
use mpchash::HashRing;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::{Connection, SqliteConnection, SqlitePool};
use std::fs;
use std::hash::{BuildHasher, RandomState};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::time::Duration;
use tokio::sync::{
    Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard, Semaphore, mpsc, oneshot, watch,
};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::compression::{self, Compressor};
// Import ShardWriteOperation from shard_manager
use crate::{
    cluster::{
        ClusterManager, PromoteError, PromotionKind, ReplicateCheck, RoleTransition,
        VoteCollectionOutcome, collect_votes, election_delay, election_timeout, random_jitter_ms,
        retry_backoff,
    },
    config::Cfg,
    metrics::{Metrics, ShardMetrics},
    replication,
    shard_manager::{
        ALL_KEY_TABLES_QUERY, Pending, ShardSender, ShardWriteOperation, wal_checkpoint_truncate,
    },
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
    /// Existence checks HSET, HDEL and DEL make before queueing a write. Kept
    /// off the writer pool, whose one connection the writer holds for a whole
    /// batch, and off the read pool, so reads can't stall writes either.
    pub check_db_pools: Vec<SqlitePool>,
    /// Cached stored-row count per shard (across `blobs` and `blobs_*` tables),
    /// seeded once at startup and updated by each shard's writer and cleanup tasks.
    pub shard_key_counts: Vec<Arc<AtomicI64>>,
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
    /// Held by commands that check a key before writing it; see [`KeyLocks`].
    pub key_locks: KeyLocks,
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
    replids: Vec<std::sync::Mutex<Option<String>>>,
    /// Supervises per-shard replica sync tasks across runtime role changes.
    pub replica_sync: replication::ReplicaSyncSupervisor,
    /// Serializes runtime role transitions (promotion, demotion, re-pointing, REPLICATE).
    role_change_lock: Mutex<()>,
    /// Serializes failover elections (automatic and `CLUSTER FAILOVER FORCE`).
    election_lock: Mutex<()>,
    /// Background automatic failover election task on clustered nodes.
    election_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// Ensures at most one gossip-triggered role transition task runs at a time.
    transition_running: Arc<AtomicBool>,
    /// Tracks spawned gossip-triggered role transition tasks so shutdown waits for them.
    transition_tasks: TaskTracker,
    /// Server shutdown token, attached once the server starts; cancelled if a
    /// post-commit TAKEOVER cannot finish promoting every shard.
    server_shutdown: std::sync::Mutex<Option<CancellationToken>>,
    /// Cancelled whenever this node leaves the primary role, closing active
    /// outbound `BLOBASAUR.SYNC` streams.
    primary_stream_cancel: std::sync::Mutex<CancellationToken>,

    ring: HashRing<ShardNode>,
}

struct TransitionGuard(Arc<AtomicBool>);

impl Drop for TransitionGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

/// Makes a command's existence check and its write one step per key.
///
/// DEL, HDEL and HSET decide their reply, and HSETEX FNX/FXX whether to write at
/// all, from a check made before the write is queued. Without a lock,
/// concurrent commands on the same key all pass the check before any of their
/// writes land. A command holds the lock for each key it checks until its write
/// is queued (async mode) or committed (sync mode). Plain HSETEX, EXPIRE and
/// HEXPIRE hold it too: HSETEX writes the field, and an expiry in the past
/// removes the key.
///
/// Keys map onto a fixed set of mutexes, so unrelated keys sometimes share one.
pub struct KeyLocks {
    stripes: Box<[Mutex<()>]>,
    hasher: RandomState,
}

impl Default for KeyLocks {
    fn default() -> Self {
        Self {
            stripes: (0..Self::STRIPES).map(|_| Mutex::new(())).collect(),
            hasher: RandomState::new(),
        }
    }
}

impl KeyLocks {
    const STRIPES: usize = 4096;

    /// Locks plain keys (DEL, EXPIRE) until the guards are dropped.
    pub async fn lock_keys(&self, keys: &[&str]) -> Vec<MutexGuard<'_, ()>> {
        let hashes = keys.iter().map(|key| self.hasher.hash_one(key));
        self.lock_stripes(hashes.collect()).await
    }

    /// Locks fields of a hash namespace until the guards are dropped. The
    /// namespace is hashed case-sensitively (matching
    /// [`crate::shard_manager::namespace_table`]), and once: a command with
    /// many fields doesn't copy or rehash it per field.
    pub async fn lock_fields(&self, namespace: &str, fields: &[&str]) -> Vec<MutexGuard<'_, ()>> {
        let namespace_hash = self.hasher.hash_one(namespace);
        let hashes = fields
            .iter()
            .map(|field| self.hasher.hash_one((namespace_hash, field)));
        self.lock_stripes(hashes.collect()).await
    }

    /// Each mutex is taken once and in index order, so two commands locking
    /// overlapping sets of keys can't deadlock.
    async fn lock_stripes(&self, hashes: Vec<u64>) -> Vec<MutexGuard<'_, ()>> {
        let mut stripes: Vec<usize> = hashes
            .into_iter()
            .map(|hash| hash as usize % self.stripes.len())
            .collect();
        stripes.sort_unstable();
        stripes.dedup();
        let mut guards = Vec::with_capacity(stripes.len());
        for stripe in stripes {
            guards.push(self.stripes[stripe].lock().await);
        }
        guards
    }
}

impl AppState {
    pub async fn new(
        cfg: Cfg,
        shard_receivers_out: &mut Vec<mpsc::Receiver<ShardWriteOperation>>,
    ) -> Result<Self> {
        if cfg.replica_of().is_some() && !cfg.cluster.as_ref().is_some_and(|c| c.enabled) {
            return Err(miette!("replication requires [cluster] to be enabled"));
        }

        let mut shard_senders_vec = Vec::new();
        // Clear the output vector first to ensure it's empty
        shard_receivers_out.clear();

        let queue_capacity = cfg.write_queue_capacity();
        for i in 0..cfg.num_shards {
            let (sender, receiver) = mpsc::channel(queue_capacity);
            shard_senders_vec.push(ShardSender::new(sender, ShardMetrics::new(i)));
            shard_receivers_out.push(receiver); // Populate the output vector with receivers
        }

        // Create data directory if it doesn't exist
        fs::create_dir_all(&cfg.data_dir).expect("Failed to create data directory");

        // Validate the number of shards in the data directory
        validate_shard_count(&cfg.data_dir, cfg.num_shards)?;

        // Load persisted cluster state if present, or prepare bootstrap state from config.
        // Role and primary come from this state (not the config) for shard init and runtime.
        let loaded_cluster =
            if let Some(cluster_config) = cfg.cluster.as_ref().filter(|c| c.enabled) {
                Some(
                    crate::cluster::load_or_bootstrap(
                        std::path::Path::new(&cfg.data_dir),
                        cluster_config,
                        cfg.replica_of(),
                    )
                    .map_err(|e| miette!("{}", e))?,
                )
            } else {
                None
            };

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
            // Check schema version before opening a writable WAL connection or
            // running application DDL, so a future-version shard is rejected
            // untouched (while still allowing SQLite hot-journal recovery).
            let db_path = format!("{}/shard_{}.db", cfg.data_dir, i);
            if std::path::Path::new(&db_path).exists() {
                let version = crate::shard_manager::probe_shard_schema_version(
                    &db_path,
                    i,
                    cfg.sqlite_busy_timeout_ms(),
                )
                .await?;
                crate::shard_manager::validate_schema_version(version, i)?;
            }

            let options =
                build_sqlite_connect_options(&cfg, i).pragma("auto_vacuum", "INCREMENTAL");
            let mut conn = SqliteConnection::connect_with(&options)
                .await
                .map_err(|e| miette!("failed to open shard {} DB: {}", i, e))?;
            sqlx::query(CREATE_BLOBS_TABLE)
                .execute(&mut conn)
                .await
                .map_err(|e| miette!("failed to create table in shard {} DB: {}", i, e))?;
            crate::shard_manager::migrate_shard_schema(&mut conn, i)
                .await
                .map_err(|e| miette!("failed to migrate schema in shard {} DB: {}", i, e))?;
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
        // Without replication this only marks the index as stale.
        let enabled = cfg.replication_enabled();
        let replica = loaded_cluster
            .as_ref()
            .is_some_and(|loaded| loaded.state.is_replica());
        let is_demoting = loaded_cluster
            .as_ref()
            .is_some_and(|loaded| loaded.state.demoting_to.is_some());
        // Also without replication: a half-copied replica must not serve as a primary.
        if !replica && !is_demoting {
            let options: Vec<_> = (0..cfg.num_shards)
                .map(|i| build_sqlite_connect_options(&cfg, i))
                .collect();
            replication::ensure_promotable(&options).await?;
        }
        let mut commit_watch = Vec::with_capacity(cfg.num_shards);
        for i in 0..cfg.num_shards {
            let options = build_sqlite_connect_options(&cfg, i);
            let lsn =
                replication::init_shard(&options, enabled, cfg.new_replid, replica || is_demoting)
                    .await
                    .map_err(|e| {
                        miette!("failed to initialize change index of shard {}: {}", i, e)
                    })?;
            commit_watch.push(Arc::new(watch::channel(lsn).0));
        }

        let mut db_pools_futures = vec![];
        let mut write_db_pools_futures = vec![];
        let mut check_db_pools_futures = vec![];
        let pool_max_connections = cfg.sqlite_pool_max_connections();
        let write_pool_max_connections = cfg.sqlite_write_pool_max_connections();
        let check_pool_max_connections = cfg.sqlite_check_pool_max_connections();
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

            check_db_pools_futures.push(
                SqlitePoolOptions::new()
                    .max_connections(check_pool_max_connections)
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
        let check_db_pools: Vec<SqlitePool> = join_all(check_db_pools_futures)
            .await
            .into_iter()
            .enumerate()
            .map(|(i, res)| {
                res.unwrap_or_else(|e| panic!("Failed to connect to shard {} check DB: {}", i, e))
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

        // Seed per-shard key counts once at startup across all shards in parallel.
        let count_results = join_all(
            db_pools
                .iter()
                .enumerate()
                .map(|(i, pool)| async move { count_shard_keys(pool, i).await }),
        )
        .await;
        let mut shard_key_counts = Vec::with_capacity(db_pools.len());
        for res in count_results {
            shard_key_counts.push(Arc::new(AtomicI64::new(res?)));
        }

        // Caches for inflight async ops. An entry must never be evicted before
        // its op commits, or reads would miss an acknowledged write or delete.
        // Live entries are already bounded by the per-shard write queues and
        // active batches (`num_shards * (write_queue_capacity + batch_size)`),
        // and each entry is removed once its batch finishes, so no capacity
        // limit is set here.
        let inflight_cache = Cache::builder().build();
        let inflight_hcache = Cache::builder().build();

        // Initialize cluster manager if clustering is enabled
        let cluster_manager = if let (Some(cluster_config), Some(loaded)) =
            (cfg.cluster.as_ref().filter(|c| c.enabled), loaded_cluster)
        {
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
                loaded,
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
        let replids = (0..cfg.num_shards)
            .map(|_| std::sync::Mutex::new(None))
            .collect();
        let state = AppState {
            cfg,
            shard_senders: shard_senders_vec,
            db_pools,
            write_db_pools,
            check_db_pools,
            shard_key_counts,
            inflight_cache,
            inflight_hcache,
            cluster_manager,
            compressor,
            metrics,
            key_locks: KeyLocks::default(),
            commit_watch,
            repl_pins,
            replica_gates,
            replids,
            replica_sync: replication::ReplicaSyncSupervisor::new(),
            role_change_lock: Mutex::new(()),
            election_lock: Mutex::new(()),
            election_task: Mutex::new(None),
            transition_running: Arc::new(AtomicBool::new(false)),
            transition_tasks: TaskTracker::new(),
            server_shutdown: std::sync::Mutex::new(None),
            primary_stream_cancel: std::sync::Mutex::new(CancellationToken::new()),
            ring,
        };
        // A new replica has none until its first sync.
        for (i, pool) in state.db_pools.iter().enumerate().filter(|_| enabled) {
            if let Ok((replid, ..)) = replication::primary_state(pool).await
                && !replid.is_empty()
            {
                state.set_replid(i, &replid);
            }
        }
        if enabled
            && !state.is_replica()
            && let Some(cluster) = &state.cluster_manager
            && let Err(e) = cluster.publish_gossip().await
        {
            tracing::warn!("Failed to gossip initial primary replids: {}", e);
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
        if let Some(cluster) = &self.cluster_manager {
            cluster.set_shard_replid(shard, replid);
        }
    }

    /// Cached stored-key count for one shard.
    pub fn shard_key_count(&self, shard_id: usize) -> u64 {
        self.shard_key_counts[shard_id]
            .load(Ordering::Relaxed)
            .max(0) as u64
    }

    /// Cached total stored-key count across all shards on this node.
    pub fn total_key_count(&self) -> u64 {
        self.shard_key_counts
            .iter()
            .map(|c| c.load(Ordering::Relaxed).max(0) as u64)
            .sum()
    }

    /// Records that shard `shard_id` stopped cleanly; see
    /// [`replication::mark_clean_shutdown`]. Its pools must be closed first.
    pub async fn mark_clean_shutdown(&self, shard_id: usize) -> Result<()> {
        if !self.cfg.replication_enabled() {
            return Ok(());
        }
        replication::mark_clean_shutdown(&build_sqlite_connect_options(&self.cfg, shard_id)).await
    }

    /// Whether this node's runtime role (from cluster state) is replica.
    pub fn is_replica(&self) -> bool {
        self.cluster_manager
            .as_ref()
            .is_some_and(ClusterManager::is_replica)
    }

    /// Node id of the primary this node follows, if its runtime role is replica.
    pub fn replica_of(&self) -> Option<String> {
        self.cluster_manager
            .as_ref()
            .and_then(ClusterManager::primary_id)
    }

    /// Token cancelled when this node leaves the primary role, stopping active
    /// outbound `BLOBASAUR.SYNC` streams.
    pub fn primary_stream_cancel(&self) -> CancellationToken {
        self.primary_stream_cancel.lock().unwrap().clone()
    }

    /// Cancel all active outbound `BLOBASAUR.SYNC` streams and arm a fresh token.
    pub fn cancel_primary_streams(&self) {
        let old = std::mem::replace(
            &mut *self.primary_stream_cancel.lock().unwrap(),
            CancellationToken::new(),
        );
        old.cancel();
    }

    /// Attaches the non-blocking runtime demotion/re-pointing handler to the
    /// cluster manager and starts replica sync tasks if this node is a replica
    /// without a pending re-point.
    pub async fn attach_cluster_runtime(self: &Arc<Self>, shutdown: CancellationToken) {
        *self.server_shutdown.lock().unwrap() = Some(shutdown);
        if let Some(cluster_manager) = &self.cluster_manager {
            let weak = Arc::downgrade(self);
            let handler: crate::cluster::RoleTransitionHandler = Arc::new(move |transition| {
                let Some(state) = weak.upgrade() else {
                    return;
                };
                state.spawn_role_transition(transition);
            });
            cluster_manager.set_role_handler(handler);
        }
        let repointing = self
            .cluster_manager
            .as_ref()
            .is_some_and(ClusterManager::is_repointing);
        if self.cfg.replication_enabled() && self.is_replica() && !repointing {
            self.replica_sync.start(self.clone()).await;
        }
        if self.cluster_manager.is_some() && self.cfg.replication_enabled() {
            let state = self.clone();
            let shutdown = self
                .server_shutdown
                .lock()
                .unwrap()
                .clone()
                .unwrap_or_default();
            let handle = tokio::spawn(async move {
                state.auto_election_loop(shutdown).await;
            });
            *self.election_task.lock().await = Some(handle);
        }
    }

    /// Waits for the background automatic election task to stop on shutdown.
    pub async fn stop_election_task(&self) {
        if let Some(handle) = self.election_task.lock().await.take() {
            let _ = handle.await;
        }
    }

    /// Spawns a role-transition task unless one is already running.
    fn spawn_role_transition(self: &Arc<Self>, transition: RoleTransition) {
        if self
            .transition_running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        let guard = TransitionGuard(self.transition_running.clone());
        let state = self.clone();
        self.transition_tasks.spawn(async move {
            let _guard = guard;
            match transition {
                RoleTransition::Demote => state.complete_demotion().await,
                RoleTransition::Repoint => state.complete_repoint().await,
            }
        });
    }

    /// Prevents new transition tasks from starting and waits for any running
    /// transition to finish before writers shut down.
    pub async fn wait_role_transitions(&self) {
        self.transition_tasks.close();
        self.transition_tasks.wait().await;
    }

    /// Initiates a clean server shutdown.
    fn trigger_shutdown(&self) {
        if let Some(token) = self.server_shutdown.lock().unwrap().as_ref() {
            token.cancel();
        }
    }

    /// Queues a writer barrier on every shard and waits until all of them pass.
    pub async fn barrier_all_shards(&self) -> Result<(), String> {
        let mut receivers = Vec::with_capacity(self.shard_senders.len());
        for (shard, sender) in self.shard_senders.iter().enumerate() {
            let (responder, done) = oneshot::channel();
            sender
                .send(ShardWriteOperation::Barrier { responder })
                .await
                .map_err(|_| format!("shard {shard} writer is gone"))?;
            receivers.push((shard, done));
        }
        for (shard, done) in receivers {
            done.await
                .map_err(|_| format!("shard {shard} writer is gone"))?;
        }
        Ok(())
    }

    /// Sets `repl_state.role = replica` and `synced = 0` on every shard through
    /// its writer queue.
    async fn demote_all_shards(&self) -> Result<(), String> {
        let mut receivers = Vec::with_capacity(self.shard_senders.len());
        for (shard, sender) in self.shard_senders.iter().enumerate() {
            let (responder, done) = oneshot::channel();
            sender
                .send(ShardWriteOperation::Demote { responder })
                .await
                .map_err(|_| format!("shard {shard} writer is gone"))?;
            receivers.push((shard, done));
        }
        for (shard, done) in receivers {
            done.await
                .map_err(|_| format!("shard {shard} writer is gone"))??;
        }
        Ok(())
    }

    /// Checks that every shard DB currently has `repl_state.synced = 1`.
    async fn ensure_all_shards_synced(&self) -> Result<(), String> {
        for (shard, pool) in self.db_pools.iter().enumerate() {
            let synced = replication::is_shard_synced(pool)
                .await
                .map_err(|e| format!("ERR failed to read shard {shard} sync state: {e}"))?;
            if !synced {
                return Err(format!("ERR Cannot promote: shard {shard} is not synced"));
            }
        }
        Ok(())
    }

    /// Returns true if any shard DB currently holds a live (unexpired) row in
    /// `blobs` or any `blobs_*` table. Call after `barrier_all_shards`.
    async fn has_live_keys(&self) -> Result<bool, String> {
        let now = chrono::Utc::now().timestamp();
        for (shard, pool) in self.db_pools.iter().enumerate() {
            let tables: Vec<(String,)> = sqlx::query_as(ALL_KEY_TABLES_QUERY)
                .fetch_all(pool)
                .await
                .map_err(|e| format!("ERR failed to list tables in shard {shard}: {e}"))?;
            for (table,) in tables {
                let escaped = table.replace('"', "\"\"");
                let query = format!(
                    "SELECT 1 FROM \"{escaped}\" WHERE expires_at IS NULL OR expires_at > ? LIMIT 1"
                );
                let found: Option<(i64,)> = sqlx::query_as(&query)
                    .bind(now)
                    .fetch_optional(pool)
                    .await
                    .map_err(|e| {
                        format!("ERR failed to check keys in shard {shard} table {table}: {e}")
                    })?;
                if found.is_some() {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    /// `CLUSTER FAILOVER TAKEOVER`: stop replica sync tasks, persist the
    /// promoted cluster state (commit point), set `repl_state.role = primary`
    /// with a new `replid` per shard through the shard writers, then serve and
    /// gossip.
    pub async fn promote_takeover(self: &Arc<Self>) -> Result<(), String> {
        self.promote_with_kind(PromotionKind::Takeover)
            .await
            .map(|_| ())
    }

    /// `CLUSTER FAILOVER FORCE`: run a manual failover election immediately
    /// (no FAIL mark needed, no rank delay), and promote on majority.
    pub async fn promote_failover_force(self: &Arc<Self>) -> Result<(), String> {
        self.run_election(true).await.map(|_| ())
    }

    /// Run one failover election attempt (automatic when `manual == false`,
    /// `CLUSTER FAILOVER FORCE` when `manual == true`).
    pub async fn run_election(self: &Arc<Self>, manual: bool) -> Result<u64, String> {
        let Some(cluster) = self.cluster_manager.as_ref() else {
            return Err("ERR This instance has cluster support disabled".to_string());
        };
        if !self.cfg.replication_enabled() {
            return Err("ERR replication is not enabled on this node".to_string());
        }

        let _election_guard = self.election_lock.lock().await;
        cluster.can_takeover()?;
        self.ensure_all_shards_synced().await?;

        let plan = cluster.prepare_election(manual).await?;
        let shutdown = self
            .server_shutdown
            .lock()
            .unwrap()
            .clone()
            .unwrap_or_default();
        let timeout = election_timeout(cluster.node_timeout());
        let outcome = collect_votes(&plan, timeout, &shutdown, || {
            !cluster.is_replica()
                || cluster.is_repointing()
                || cluster.primary_id().as_deref() != Some(plan.failed_primary.as_str())
        })
        .await;

        match outcome {
            VoteCollectionOutcome::Won { votes } => {
                tracing::info!(
                    candidate = %plan.candidate,
                    epoch = plan.epoch,
                    failed_primary = %plan.failed_primary,
                    votes = votes.len(),
                    quorum = plan.quorum,
                    manual,
                    "Won failover election; promoting"
                );
                let kind = PromotionKind::Elected {
                    election_epoch: plan.epoch,
                    failed_primary: plan.failed_primary.clone(),
                    primary_config_epoch: plan.primary_config_epoch,
                    expected_slots: plan.slots.clone(),
                };
                match self.promote_with_kind(kind).await {
                    Ok(epoch) => {
                        cluster.record_election_won();
                        Ok(epoch)
                    }
                    Err(e) => {
                        cluster.record_election_failed();
                        tracing::warn!(
                            candidate = %plan.candidate,
                            epoch = plan.epoch,
                            error = %e,
                            "Promotion after winning election failed"
                        );
                        Err(e)
                    }
                }
            }
            VoteCollectionOutcome::NoMajority { votes, quorum } => {
                cluster.record_election_failed();
                tracing::warn!(
                    candidate = %plan.candidate,
                    epoch = plan.epoch,
                    failed_primary = %plan.failed_primary,
                    votes = votes.len(),
                    quorum,
                    manual,
                    "Failover election gave up without majority"
                );
                Err(format!(
                    "ERR Failover election failed: got {} of {} required votes",
                    votes.len(),
                    quorum
                ))
            }
            VoteCollectionOutcome::Aborted => {
                cluster.record_election_failed();
                tracing::info!(
                    candidate = %plan.candidate,
                    epoch = plan.epoch,
                    failed_primary = %plan.failed_primary,
                    manual,
                    "Failover election aborted"
                );
                Err("ERR Failover election aborted".to_string())
            }
        }
    }

    /// Background automatic failover loop: watches eligibility, arms with
    /// `500 ms + random(0..500 ms) + rank * 1000 ms`, re-arms if a better
    /// replica appears, and retries no sooner than `max(4 * node_timeout, 4 s)`
    /// after the start of the previous attempt.
    async fn auto_election_loop(self: Arc<Self>, shutdown: CancellationToken) {
        struct ArmedElection {
            primary: String,
            jitter_ms: u64,
            rank: usize,
            armed_at: std::time::Instant,
            deadline: std::time::Instant,
        }

        let Some(cluster) = self.cluster_manager.as_ref() else {
            return;
        };
        let notify = cluster.election_notify();
        let mut armed: Option<ArmedElection> = None;
        let mut last_attempt: Option<(String, std::time::Instant)> = None;

        while !shutdown.is_cancelled() {
            let now = std::time::Instant::now();
            let Some(primary) = cluster.check_election_eligibility().await else {
                armed = None;
                if let Some((ref prev_primary, _)) = last_attempt
                    && (!cluster.is_failed(prev_primary)
                        || cluster.primary_id().as_deref() != Some(prev_primary.as_str()))
                {
                    last_attempt = None;
                }
                tokio::select! {
                    _ = shutdown.cancelled() => return,
                    _ = notify.notified() => {}
                    _ = tokio::time::sleep(Duration::from_millis(50)) => {}
                }
                continue;
            };

            if let Some((ref prev_primary, prev_start)) = last_attempt
                && prev_primary == &primary
            {
                let retry_at = prev_start + retry_backoff(cluster.node_timeout());
                if now < retry_at {
                    armed = None;
                    let wait = (retry_at - now).min(Duration::from_millis(100));
                    tokio::select! {
                        _ = shutdown.cancelled() => return,
                        _ = notify.notified() => {}
                        _ = tokio::time::sleep(wait) => {}
                    }
                    continue;
                }
            }

            let rank = cluster.compute_replica_rank(&primary).await;
            match armed.as_mut() {
                Some(a) if a.primary == primary => {
                    if rank > a.rank {
                        // A better replica appeared: re-arm from now at the new rank.
                        let jitter_ms = random_jitter_ms();
                        let delay = election_delay(rank, jitter_ms);
                        tracing::info!(
                            node = %cluster.node_id(),
                            primary = %primary,
                            old_rank = a.rank,
                            new_rank = rank,
                            delay_ms = delay.as_millis() as u64,
                            "Better replica appeared; re-arming automatic failover election"
                        );
                        a.jitter_ms = jitter_ms;
                        a.rank = rank;
                        a.armed_at = now;
                        a.deadline = now + delay;
                    } else if rank < a.rank {
                        a.rank = rank;
                        a.deadline = a.armed_at + election_delay(rank, a.jitter_ms);
                    }
                }
                _ => {
                    let jitter_ms = random_jitter_ms();
                    let delay = election_delay(rank, jitter_ms);
                    tracing::info!(
                        node = %cluster.node_id(),
                        primary = %primary,
                        rank,
                        delay_ms = delay.as_millis() as u64,
                        "Armed automatic failover election"
                    );
                    armed = Some(ArmedElection {
                        primary: primary.clone(),
                        jitter_ms,
                        rank,
                        armed_at: now,
                        deadline: now + delay,
                    });
                }
            }

            let deadline = armed.as_ref().unwrap().deadline;
            let armed_rank = armed.as_ref().unwrap().rank;
            if now >= deadline {
                // Recompute rank right before starting; if a better replica appeared, re-arm.
                let final_rank = cluster.compute_replica_rank(&primary).await;
                if final_rank > armed_rank {
                    let a = armed.as_mut().unwrap();
                    let jitter_ms = random_jitter_ms();
                    let delay = election_delay(final_rank, jitter_ms);
                    tracing::info!(
                        node = %cluster.node_id(),
                        primary = %primary,
                        old_rank = armed_rank,
                        new_rank = final_rank,
                        delay_ms = delay.as_millis() as u64,
                        "Better replica appeared before election start; re-arming"
                    );
                    a.jitter_ms = jitter_ms;
                    a.rank = final_rank;
                    a.armed_at = now;
                    a.deadline = now + delay;
                    continue;
                }
                armed = None;
                let attempt_started = std::time::Instant::now();
                last_attempt = Some((primary.clone(), attempt_started));
                match self.run_election(false).await {
                    Ok(_) => {
                        last_attempt = None;
                    }
                    Err(e) => {
                        tracing::debug!(
                            node = %cluster.node_id(),
                            primary = %primary,
                            error = %e,
                            "Automatic failover election attempt did not promote"
                        );
                    }
                }
            } else {
                let wait = (deadline - now).min(Duration::from_millis(100));
                tokio::select! {
                    _ = shutdown.cancelled() => return,
                    _ = notify.notified() => {}
                    _ = tokio::time::sleep(wait) => {}
                }
            }
        }
    }

    async fn promote_with_kind(self: &Arc<Self>, kind: PromotionKind) -> Result<u64, String> {
        let Some(cluster) = self.cluster_manager.as_ref() else {
            return Err("ERR This instance has cluster support disabled".to_string());
        };
        if !self.cfg.replication_enabled() {
            return Err("ERR replication is not enabled on this node".to_string());
        }

        let _role_guard = self.role_change_lock.lock().await;
        cluster.can_promote(&kind)?;
        self.ensure_all_shards_synced().await?;

        // 1. Stop replica sync tasks and wait so no Apply/Reset is queued behind promotion.
        self.replica_sync.stop().await;

        // Re-check synced after stopping sync tasks in case a Reset ran right before stop.
        if let Err(e) = self.ensure_all_shards_synced().await {
            if self.is_replica() {
                self.replica_sync.start(self.clone()).await;
            }
            return Err(e);
        }

        // 2-4. Persist cluster state (commit point), promote every shard through
        // its writer in a synchronous=FULL transaction (retrying failed shards
        // with backoff), then expose & gossip.
        const PROMOTE_SHARD_MAX_ATTEMPTS: usize = 5;
        let shard_senders = &self.shard_senders;
        let result = cluster
            .promote(kind, || async {
                let mut pending: Vec<usize> = (0..shard_senders.len()).collect();
                let mut delay = Duration::from_millis(100);
                let mut last_err = String::new();
                for attempt in 1..=PROMOTE_SHARD_MAX_ATTEMPTS {
                    let mut receivers = Vec::with_capacity(pending.len());
                    for &shard in &pending {
                        let (responder, done) = oneshot::channel();
                        if shard_senders[shard]
                            .send(ShardWriteOperation::Promote { responder })
                            .await
                            .is_err()
                        {
                            last_err = format!("shard {shard} writer is gone");
                            receivers.push((shard, None));
                        } else {
                            receivers.push((shard, Some(done)));
                        }
                    }
                    let mut still_pending = Vec::new();
                    for (shard, done) in receivers {
                        match done {
                            None => still_pending.push(shard),
                            Some(done) => match done.await {
                                Ok(Ok(replid)) => self.set_replid(shard, &replid),
                                Ok(Err(e)) => {
                                    last_err = format!("shard {shard} promote failed: {e}");
                                    still_pending.push(shard);
                                }
                                Err(_) => {
                                    last_err = format!("shard {shard} writer is gone");
                                    still_pending.push(shard);
                                }
                            },
                        }
                    }
                    pending = still_pending;
                    if pending.is_empty() {
                        return Ok(());
                    }
                    tracing::warn!(
                        attempt,
                        max_attempts = PROMOTE_SHARD_MAX_ATTEMPTS,
                        remaining_shards = ?pending,
                        error = %last_err,
                        "Per-shard TAKEOVER promotion failed after cluster state commit; retrying"
                    );
                    if attempt < PROMOTE_SHARD_MAX_ATTEMPTS {
                        tokio::time::sleep(delay).await;
                        delay = (delay * 2).min(Duration::from_secs(2));
                    }
                }
                Err(format!(
                    "ERR Promotion committed in cluster-state.json but shards {:?} failed after {} attempts ({}); shutting down for startup recovery",
                    pending, PROMOTE_SHARD_MAX_ATTEMPTS, last_err
                ))
            })
            .await;

        match result {
            Ok(epoch) => Ok(epoch),
            Err(PromoteError::BeforeCommit(e)) => {
                if self.is_replica() {
                    self.replica_sync.start(self.clone()).await;
                }
                Err(e)
            }
            Err(PromoteError::AfterCommit(e)) => {
                // Past the commit point: never revert to replica or restart sync
                // tasks. Shut down cleanly so restart completes promotion via
                // `replication::init_shard`.
                tracing::error!(
                    "TAKEOVER failed past cluster-state.json commit point: {}. Shutting down cleanly so startup completes promotion.",
                    e
                );
                self.trigger_shutdown();
                Err(e)
            }
        }
    }

    /// Level-triggered demotion: while `role == primary && demoting_to.is_some()`,
    /// drain writer barriers, cancel outbound primary sync streams, mark shards
    /// as unsynced replicas, atomically persist `role = replica, primary = winner`
    /// clearing `demoting_to`, gossip, and start replica sync tasks. Any failure
    /// leaves `demoting_to` set so a later gossip tick retries.
    pub async fn complete_demotion(self: &Arc<Self>) {
        let Some(cluster) = self.cluster_manager.as_ref() else {
            return;
        };
        let _role_guard = self.role_change_lock.lock().await;
        if !cluster.is_demoting() {
            return;
        }

        // 1. Drain everything that passed the route check before slots were dropped.
        if let Err(e) = self.barrier_all_shards().await {
            tracing::error!("Demotion barrier failed (will retry): {}", e);
            return;
        }

        // 2. Close any outbound streams this node was serving as a primary.
        self.cancel_primary_streams();

        // 3. Per shard through the writer: role = replica, synced = 0.
        if let Err(e) = self.demote_all_shards().await {
            tracing::error!(
                "Failed to mark shards as replica during demotion (will retry): {}",
                e
            );
            return;
        }

        // 4. Atomically clear `demoting_to` and persist `role = replica, primary = winner`.
        let winner = match cluster.finish_demotion().await {
            Ok(Some(winner)) => winner,
            Ok(None) => return,
            Err(e) => {
                tracing::error!(
                    "Failed to persist demoted cluster state (will retry): {}",
                    e
                );
                return;
            }
        };

        if let Err(e) = cluster.publish_gossip().await {
            tracing::error!("Failed to gossip demoted role: {}", e);
        }

        // 5. Start replica sync tasks against the new primary.
        if self.cfg.replication_enabled() {
            self.replica_sync.start(self.clone()).await;
        } else {
            tracing::warn!(
                "Demoted to replica of {}, but [replication] is disabled so sync tasks will not start",
                winner
            );
        }
    }

    /// Level-triggered replica re-pointing: while `role == replica &&
    /// repointing_to.is_some()`, stop sync tasks, mark shards unsynced,
    /// atomically persist `primary = new_primary` clearing `repointing_to`,
    /// gossip, and start sync tasks against the new primary.
    pub async fn complete_repoint(self: &Arc<Self>) {
        let Some(cluster) = self.cluster_manager.as_ref() else {
            return;
        };
        let _role_guard = self.role_change_lock.lock().await;
        if !cluster.is_repointing() {
            return;
        }

        self.replica_sync.stop().await;
        cluster.reset_replica_synced();

        if let Err(e) = self.demote_all_shards().await {
            tracing::error!(
                "Failed to mark shards unsynced during re-point (will retry): {}",
                e
            );
            return;
        }

        match cluster.finish_repoint().await {
            Ok(Some(_)) => {}
            Ok(None) => return,
            Err(e) => {
                tracing::error!(
                    "Failed to persist re-pointed cluster state (will retry): {}",
                    e
                );
                return;
            }
        }

        if let Err(e) = cluster.publish_gossip().await {
            tracing::error!("Failed to gossip re-pointed replica state: {}", e);
        }

        if self.cfg.replication_enabled() {
            self.replica_sync.restart(self.clone()).await;
        }
    }

    /// `CLUSTER REPLICATE <node-id>`: make this node a replica of `target_id`
    /// at runtime (Redis semantics).
    pub async fn replicate_node(self: &Arc<Self>, target_id: String) -> Result<(), String> {
        let Some(cluster) = self.cluster_manager.as_ref() else {
            return Err("ERR This instance has cluster support disabled".to_string());
        };
        if !self.cfg.replication_enabled() {
            return Err("ERR replication is not enabled on this node".to_string());
        }

        let _role_guard = self.role_change_lock.lock().await;
        match cluster.validate_replicate_target(&target_id).await? {
            ReplicateCheck::AlreadyFollowing => Ok(()),
            ReplicateCheck::FromEmptyPrimary => {
                self.barrier_all_shards()
                    .await
                    .map_err(|e| format!("ERR {e}"))?;
                if self.has_live_keys().await? {
                    return Err(
                        "ERR To set a master the node must be empty and without assigned slots."
                            .to_string(),
                    );
                }
                cluster.commit_replicate(&target_id).await?;
                self.cancel_primary_streams();
                self.demote_all_shards()
                    .await
                    .map_err(|e| format!("ERR {e}"))?;
                if let Err(e) = cluster.publish_gossip().await {
                    tracing::error!("Failed to gossip replicated state: {}", e);
                }
                self.replica_sync.start(self.clone()).await;
                Ok(())
            }
            ReplicateCheck::SwitchReplica => {
                self.replica_sync.stop().await;
                cluster.reset_replica_synced();
                if let Err(e) = self.demote_all_shards().await {
                    self.replica_sync.restart(self.clone()).await;
                    return Err(format!("ERR {e}"));
                }
                if let Err(e) = cluster.commit_replicate(&target_id).await {
                    self.replica_sync.restart(self.clone()).await;
                    return Err(e);
                }
                if let Err(e) = cluster.publish_gossip().await {
                    tracing::error!("Failed to gossip switched replica state: {}", e);
                }
                self.replica_sync.restart(self.clone()).await;
                Ok(())
            }
        }
    }

    /// Set the gauges that are sampled rather than updated in place: queue
    /// depth, pool connections, and cached per-shard key counts. Called on
    /// each metrics scrape.
    pub fn record_gauges(&self) {
        let is_replica = self.is_replica();
        for (i, sender) in self.shard_senders.iter().enumerate() {
            let shard = i.to_string();
            metrics::gauge!("blobasaur_keys", "shard" => shard.clone())
                .set(self.shard_key_count(i) as f64);
            metrics::gauge!("blobasaur_write_queue_depth", "shard" => shard.clone())
                .set(sender.queue_depth() as f64);
            if self.cfg.replication_enabled() {
                let (active_role, inactive_role) = if is_replica {
                    ("replica", "primary")
                } else {
                    metrics::gauge!("blobasaur_replication_latest_lsn", "shard" => shard.clone())
                        .set(*self.commit_watch[i].borrow() as f64);
                    ("primary", "replica")
                };
                metrics::gauge!(
                    "blobasaur_replication_role",
                    "shard" => shard.clone(),
                    "role" => active_role
                )
                .set(1.0);
                metrics::gauge!(
                    "blobasaur_replication_role",
                    "shard" => shard.clone(),
                    "role" => inactive_role
                )
                .set(0.0);
            }
            for (name, pool) in [
                ("read", &self.db_pools[i]),
                ("write", &self.write_db_pools[i]),
                ("check", &self.check_db_pools[i]),
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
        if conn_readonly && self.is_replica() {
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
        crate::shard_manager::namespaced_key(namespace, key)
    }
}

/// Counts stored rows across `blobs` and every namespaced `blobs_*` table in one
/// shard DB. Uses `SELECT COUNT(*)` which scans each table's primary-key index
/// rather than the blob payloads.
async fn count_shard_keys(pool: &SqlitePool, shard_id: usize) -> Result<i64> {
    let tables: Vec<(String,)> = sqlx::query_as(ALL_KEY_TABLES_QUERY)
        .fetch_all(pool)
        .await
        .map_err(|e| miette!("failed to list tables in shard {} DB: {}", shard_id, e))?;

    let mut total: i64 = 0;
    for (table,) in tables {
        let escaped = table.replace('"', "\"\"");
        let query = format!("SELECT COUNT(*) FROM \"{escaped}\"");
        let count: i64 = sqlx::query_scalar(&query)
            .fetch_one(pool)
            .await
            .map_err(|e| {
                miette!(
                    "failed to count keys in shard {} table {}: {}",
                    shard_id,
                    table,
                    e
                )
            })?;
        total += count;
    }

    Ok(total)
}

fn build_sqlite_connect_options(cfg: &Cfg, shard_id: usize) -> SqliteConnectOptions {
    let db_path = format!("{}/shard_{}.db", cfg.data_dir, shard_id);

    // Get SQLite configuration with defaults
    let cache_size_mb = cfg.sqlite_cache_size_per_connection_mb();
    let busy_timeout_ms = cfg.sqlite_busy_timeout_ms();
    let synchronous = cfg.sqlite_synchronous();
    let mmap_size = cfg.sqlite_mmap_per_connection_bytes();

    let mut connect_options = crate::shard_manager::shard_file_options(&db_path)
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

    if let Some(counts) = crate::migration::pending_shrink(data_path)? {
        return Err(miette!(
            "Shard cleanup is pending. Finish it with `blobasaur shard migrate {counts}` before starting the server"
        ));
    }

    // If the directory is empty (or holds only cluster-state.json and its temp files),
    // no shards exist yet.
    if let Ok(entries) = fs::read_dir(data_path)
        && entries
            .flatten()
            .all(|e| crate::cluster::is_cluster_state_file(&e.file_name()))
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
            "Shard count mismatch: expected {} shards, found {} of them",
            expected_shards, actual_shards
        );

        if !extra_shards.is_empty() {
            extra_shards.sort_unstable();
            error_msg.push_str(&format!(
                ". Found shard files beyond num_shards: {:?} (change the shard count with `blobasaur shard migrate`)",
                extra_shards
            ));
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
            write_queue_capacity: None,
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
        let mut cfg = test_cfg(temp_dir.path().to_string_lossy().into_owned(), 1);
        cfg.replication = Some(crate::config::ReplicationConfig {
            replica_of: None,
            tombstone_retention_secs: None,
        });
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

    #[test]
    fn shard_count_mismatch_lists_extra_shards_in_order() {
        let dir = TempDir::new().unwrap();
        for shard_id in [0, 1, 2, 4, 3] {
            fs::write(dir.path().join(format!("shard_{shard_id}.db")), b"").unwrap();
        }

        let err = validate_shard_count(dir.path().to_str().unwrap(), 3).unwrap_err();

        assert_eq!(
            err.to_string(),
            "Shard count mismatch: expected 3 shards, found 3 of them. Found shard files beyond \
             num_shards: [3, 4] (change the shard count with `blobasaur shard migrate`)"
        );
    }

    #[test]
    fn validate_shard_count_ignores_cluster_state_and_tmp_files() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join(crate::cluster::CLUSTER_STATE_FILE), b"{}").unwrap();
        fs::write(dir.path().join(".cluster-state.json.tmp.42.0"), b"partial").unwrap();

        validate_shard_count(dir.path().to_str().unwrap(), 2).unwrap();

        // Any other file with missing shards is still treated as a mismatch.
        fs::write(dir.path().join("unrelated.db"), b"").unwrap();
        assert!(validate_shard_count(dir.path().to_str().unwrap(), 2).is_err());
    }

    #[tokio::test]
    async fn app_state_seeds_key_counts_from_blobs_and_namespaced_tables() {
        let temp_dir = TempDir::new().expect("failed to create temp dir");
        let cfg = test_cfg(temp_dir.path().to_string_lossy().into_owned(), 2);

        // First boot creates the shard files.
        let mut receivers = Vec::new();
        let initial = AppState::new(cfg.clone(), &mut receivers)
            .await
            .expect("initial boot");
        assert_eq!(initial.shard_key_count(0), 0);
        assert_eq!(initial.shard_key_count(1), 0);
        assert_eq!(initial.total_key_count(), 0);

        // Populate shard 0 with 2 default keys + 1 namespaced key, and shard 1 with 1 default key.
        sqlx::query(
            "INSERT INTO blobs (key, data, created_at, updated_at, expires_at, version)
             VALUES ('k1', x'01', 0, 0, NULL, 0), ('k2', x'02', 0, 0, NULL, 0)",
        )
        .execute(&initial.db_pools[0])
        .await
        .unwrap();
        let users_table = crate::shard_manager::namespace_table("users");
        sqlx::query(&format!(
            "CREATE TABLE {users_table} (
                key TEXT PRIMARY KEY,
                data BLOB,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                expires_at INTEGER,
                version INTEGER NOT NULL DEFAULT 0
             )"
        ))
        .execute(&initial.db_pools[0])
        .await
        .unwrap();
        sqlx::query("INSERT INTO blob_namespaces (namespace, table_name) VALUES ('users', ?)")
            .bind(&users_table)
            .execute(&initial.db_pools[0])
            .await
            .unwrap();
        sqlx::query(&format!(
            "INSERT INTO {users_table} (key, data, created_at, updated_at, expires_at, version)
             VALUES ('u1', x'03', 0, 0, NULL, 0)"
        ))
        .execute(&initial.db_pools[0])
        .await
        .unwrap();
        // An unrelated table whose name would match an unescaped `LIKE 'blobs_%'` wildcard
        // must not be counted.
        sqlx::query("CREATE TABLE blobsXaudit (key TEXT PRIMARY KEY)")
            .execute(&initial.db_pools[0])
            .await
            .unwrap();
        sqlx::query("INSERT INTO blobsXaudit (key) VALUES ('ignored')")
            .execute(&initial.db_pools[0])
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO blobs (key, data, created_at, updated_at, expires_at, version)
             VALUES ('s1', x'04', 0, 0, NULL, 0)",
        )
        .execute(&initial.db_pools[1])
        .await
        .unwrap();
        // Recreate shard 1's default table as uppercase `BLOBS` to verify case-insensitive matching.
        sqlx::query("ALTER TABLE blobs RENAME TO BLOBS_tmp")
            .execute(&initial.db_pools[1])
            .await
            .unwrap();
        sqlx::query("ALTER TABLE BLOBS_tmp RENAME TO BLOBS")
            .execute(&initial.db_pools[1])
            .await
            .unwrap();
        drop(initial);

        // Reboot should seed counts from disk across shards and tables.
        let mut receivers = Vec::new();
        let rebooted = AppState::new(cfg, &mut receivers).await.expect("reboot");
        assert_eq!(rebooted.shard_key_count(0), 3);
        assert_eq!(rebooted.shard_key_count(1), 1);
        assert_eq!(rebooted.total_key_count(), 4);
    }
}
