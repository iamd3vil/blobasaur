use crate::metrics::{Metrics, ShardMetrics};
use crate::redis::protocol::HExpireCondition;
use bytes::Bytes;
use chrono::Utc;
use metrics::Counter;
use miette::{Result as MietteResult, miette};
use moka::future::Cache;
use moka::ops::compute::Op;
use sha2::{Digest, Sha256};
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{Connection, Sqlite, SqliteConnection, SqlitePool, pool::PoolConnection};
use std::collections::{HashSet, VecDeque};
use std::fmt::Write as _;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{mpsc, oneshot};
use tokio::time::{Duration, interval, timeout_at};
use tokio_util::sync::CancellationToken;

// Message type for writer consumers
pub enum ShardWriteOperation {
    Set {
        key: String,
        data: Bytes,
        expires_at: Option<i64>,
        responder: oneshot::Sender<Result<(), String>>,
    },
    SetAsync {
        key: String,
        data: Bytes,
        expires_at: Option<i64>,
        /// Matches the [`Pending::Value`] entry SET left in the cache.
        token: u64,
    },
    Delete {
        key: String,
        responder: oneshot::Sender<Result<(), String>>,
    },
    DeleteAsync {
        key: String,
        /// Matches the [`Pending::Deleted`] marker DEL left in the cache.
        token: u64,
    },
    HSet {
        namespace: String,
        key: String,
        data: Bytes,
        responder: oneshot::Sender<Result<(), String>>,
    },
    HSetAsync {
        namespace: String,
        key: String,
        data: Bytes,
        /// Matches the [`Pending::Value`] entry HSET left in the cache.
        token: u64,
    },
    HSetEx {
        namespace: String,
        key: String,
        data: Bytes,
        expires_at: i64,
        responder: oneshot::Sender<Result<(), String>>,
    },
    HSetExAsync {
        namespace: String,
        key: String,
        data: Bytes,
        expires_at: i64,
        /// Matches the [`Pending::Value`] entry HSETEX left in the cache.
        token: u64,
    },
    HDelete {
        namespace: String,
        key: String,
        responder: oneshot::Sender<Result<(), String>>,
    },
    HDeleteAsync {
        namespace: String,
        key: String,
        /// Matches the [`Pending::Deleted`] marker HDEL left in the cache.
        token: u64,
    },
    Expire {
        key: String,
        expires_at: i64,
        responder: oneshot::Sender<Result<bool, String>>,
    },
    HExpire {
        namespace: String,
        key: String,
        expires_at: i64,
        condition: Option<HExpireCondition>,
        responder: oneshot::Sender<Result<i64, String>>,
    },
    #[allow(dead_code)]
    Vacuum {
        mode: VacuumMode,
        budget_bytes: u64,
        dry_run: bool,
        responder: oneshot::Sender<VacuumResult>,
    },
}

/// A shard's write queue. Every enqueue goes through [`ShardSender::reserve`],
/// so time spent waiting for queue space is always measured.
pub struct ShardSender {
    sender: mpsc::Sender<ShardWriteOperation>,
    pub metrics: ShardMetrics,
}

impl ShardSender {
    pub fn new(sender: mpsc::Sender<ShardWriteOperation>, metrics: ShardMetrics) -> Self {
        Self { sender, metrics }
    }

    pub async fn reserve(
        &self,
    ) -> Result<mpsc::Permit<'_, ShardWriteOperation>, mpsc::error::SendError<()>> {
        let start = std::time::Instant::now();
        let permit = self.sender.reserve().await?;
        self.metrics
            .write_enqueue_wait_seconds
            .record(start.elapsed());
        Ok(permit)
    }

    pub async fn send(&self, op: ShardWriteOperation) -> Result<(), mpsc::error::SendError<()>> {
        self.reserve().await?.send(op);
        Ok(())
    }

    /// Ops queued for the writer, not counting the batch it is processing.
    /// Includes permits reserved but not yet sent, which are held only briefly.
    pub fn queue_depth(&self) -> usize {
        self.sender.max_capacity() - self.sender.capacity()
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VacuumMode {
    Incremental,
    Full,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VacuumStats {
    pub page_size: u64,
    pub page_count: u64,
    pub freelist_count: u64,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct VacuumResult {
    pub mode: VacuumMode,
    pub budget_bytes: u64,
    pub dry_run: bool,
    pub before: Option<VacuumStats>,
    pub after: Option<VacuumStats>,
    pub duration: Duration,
    pub incremental_pages_requested: Option<u64>,
    pub estimated_reclaimed_pages: Option<u64>,
    pub errors: Vec<String>,
}

impl VacuumResult {
    #[allow(dead_code)]
    pub fn is_success(&self) -> bool {
        self.errors.is_empty()
    }
}

/// Everything a shard writer task needs; see [`shard_writer_task`].
pub struct ShardWriter {
    pub shard_id: usize,
    pub pool: SqlitePool,
    pub receiver: mpsc::Receiver<ShardWriteOperation>,
    pub batch_size: usize,
    /// Max time to wait for more ops while a batch is non-empty.
    pub batch_timeout_ms: u64,
    pub inflight_cache: Cache<String, Pending>,
    pub inflight_hcache: Cache<String, Pending>,
    pub metrics: Metrics,
    /// Cancel to make the writer commit everything queued and return.
    pub shutdown: CancellationToken,
}

/// An async write that is acknowledged but not committed yet. Reads check
/// the inflight cache first, so they see it before the DB does.
///
/// Every entry carries a unique token from [`next_pending_token`] matching its
/// queued op, so clearing after commit only removes the entry that op recorded.
#[derive(Clone, Debug)]
pub enum Pending {
    /// A queued SET/HSET/HSETEX. `expires_at` is the expiry a SET writes, which
    /// replaces the key's; TTL reads it. Hash writes leave it `None`.
    Value {
        data: Bytes,
        token: u64,
        expires_at: Option<i64>,
    },
    /// A queued DEL/HDEL: the key reads as absent.
    Deleted(u64),
}

impl Pending {
    fn token(&self) -> u64 {
        match self {
            Pending::Value { token, .. } | Pending::Deleted(token) => *token,
        }
    }
}

/// Queues an async `op` and records its `pending` entry for `key` as one
/// step per key, so the cache always holds the entry of the last op queued
/// for that key. Waits for queue space first: if the writer is gone, nothing
/// is recorded. Uses `and_compute_with`, so it's also ordered against
/// [`clear_pending_write`] on the same key.
pub async fn queue_pending(
    cache: &Cache<String, Pending>,
    sender: &ShardSender,
    key: String,
    pending: Pending,
    op: ShardWriteOperation,
) -> Result<(), mpsc::error::SendError<()>> {
    debug_assert!(
        matches!(
            (&pending, &op),
            (
                Pending::Value { token: pt, .. },
                ShardWriteOperation::SetAsync { token: ot, .. }
                    | ShardWriteOperation::HSetAsync { token: ot, .. }
                    | ShardWriteOperation::HSetExAsync { token: ot, .. }
            ) | (
                Pending::Deleted(pt),
                ShardWriteOperation::DeleteAsync { token: ot, .. }
                    | ShardWriteOperation::HDeleteAsync { token: ot, .. }
            ) if pt == ot
        ),
        "queue_pending op must match pending entry kind and token"
    );
    let permit = sender.reserve().await?;
    let pending_writes = &sender.metrics.async_writes_pending;
    cache
        .entry(key)
        .and_compute_with(|_| async move {
            // Counted only once the op is sent, so a cancelled call can't leak
            // it, and before the writer can see it, so it never goes negative.
            pending_writes.increment(1.0);
            permit.send(op);
            Op::Put(pending)
        })
        .await;
    Ok(())
}

/// Current SQLite schema version stored in `PRAGMA user_version` on each shard.
///
/// Version 0 (legacy): namespaced tables were named `blobs_{namespace}`.
/// Version 1: namespaced tables are named `blobs_{slug}_{sha256_prefix}` so
/// namespaces differing only in case map to distinct SQLite tables, with a
/// `blob_namespaces` lookup table mapping each namespace to its table name.
pub const SHARD_SCHEMA_VERSION: i64 = 1;

pub const CREATE_BLOB_NAMESPACES_TABLE: &str = "CREATE TABLE IF NOT EXISTS blob_namespaces (
    namespace TEXT PRIMARY KEY,
    table_name TEXT NOT NULL UNIQUE
)";

const MAX_NAMESPACE_SLUG_LEN: usize = 32;

/// The table holding `namespace`'s hash fields.
///
/// SQLite matches table names case-insensitively, so a raw `blobs_{namespace}`
/// name collapses namespaces like `Users` and `users` into one table. Suffixing
/// a lowercase readable slug with 128 bits (32 hex chars) of SHA-256 over the
/// exact namespace keeps distinct cases (and arbitrary characters) isolated
/// while remaining a readable, unquoted SQLite identifier.
pub fn namespace_table(namespace: &str) -> String {
    let digest = Sha256::digest(namespace.as_bytes());
    let slug_len = namespace.chars().take(MAX_NAMESPACE_SLUG_LEN).count();
    let mut table = String::with_capacity("blobs_".len() + slug_len + 1 + 32);
    table.push_str("blobs_");
    for ch in namespace.chars().take(MAX_NAMESPACE_SLUG_LEN) {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            table.push(ch.to_ascii_lowercase());
        } else {
            table.push('_');
        }
    }
    table.push('_');
    for byte in &digest[..16] {
        let _ = write!(&mut table, "{byte:02x}");
    }
    table
}

/// The `expires_at` partial index name for a namespaced table.
pub fn namespace_index(table_name: &str) -> String {
    let suffix = strip_blobs_prefix_ignore_ascii_case(table_name).unwrap_or(table_name);
    format!("idx_{suffix}_expires_at")
}

/// Strips a leading `blobs_` prefix ASCII-case-insensitively, preserving the
/// exact case of the suffix (e.g. `BLOBS_Users` -> `Some("Users")`).
pub fn strip_blobs_prefix_ignore_ascii_case(table_name: &str) -> Option<&str> {
    let prefix = table_name.as_bytes().get(..6)?;
    if prefix.eq_ignore_ascii_case(b"blobs_") {
        let suffix = &table_name[6..];
        (!suffix.is_empty()).then_some(suffix)
    } else {
        None
    }
}

fn is_readonly_recovery_error(error: &sqlx::Error) -> bool {
    let sqlx::Error::Database(db) = error else {
        return false;
    };
    // SQLITE_READONLY_RECOVERY (264), SQLITE_READONLY_ROLLBACK (776),
    // SQLITE_READONLY_CANTINIT (1288): hot rollback journal or missing WAL -shm
    // needs a writable connection to recover before any read can proceed.
    db.code()
        .and_then(|c| c.parse::<i32>().ok())
        .is_some_and(|code| matches!(code, 264 | 776 | 1288))
}

/// Probes `PRAGMA user_version` for an existing shard DB without changing its
/// journal mode or running any application DDL.
///
/// Opens read-only first; if SQLite reports a hot rollback journal / WAL
/// recovery requirement (`SQLITE_READONLY_ROLLBACK` / `SQLITE_READONLY_RECOVERY`
/// / `SQLITE_READONLY_CANTINIT`), retries on a plain writable connection with
/// no journal-mode or schema changes so SQLite can finish crash recovery before
/// reading the version.
pub async fn probe_shard_schema_version(
    db_path: &str,
    shard_id: usize,
    busy_timeout_ms: u64,
) -> MietteResult<i64> {
    let ro_options = SqliteConnectOptions::from_str(&format!("sqlite:{db_path}"))
        .map_err(|e| {
            miette!(
                "failed to parse connection string for shard {}: {}",
                shard_id,
                e
            )
        })?
        .read_only(true)
        .busy_timeout(Duration::from_millis(busy_timeout_ms));

    match SqliteConnection::connect_with(&ro_options).await {
        Ok(mut conn) => {
            let res = sqlx::query_scalar::<_, i64>("PRAGMA user_version")
                .fetch_one(&mut conn)
                .await;
            let _ = conn.close().await;
            match res {
                Ok(version) => return Ok(version),
                Err(e) if is_readonly_recovery_error(&e) => {}
                Err(e) => {
                    return Err(miette!(
                        "failed to read user_version for shard {} DB: {}",
                        shard_id,
                        e
                    ));
                }
            }
        }
        Err(e) if is_readonly_recovery_error(&e) => {}
        Err(e) => {
            return Err(miette!(
                "failed to open shard {} DB read-only: {}",
                shard_id,
                e
            ));
        }
    }

    // Hot journal recovery needed: open without create_if_missing, without
    // changing journal_mode, and without any DDL, so SQLite recovers the
    // journal cleanly before reading PRAGMA user_version.
    let recovery_options = SqliteConnectOptions::from_str(&format!("sqlite:{db_path}"))
        .map_err(|e| {
            miette!(
                "failed to parse connection string for shard {}: {}",
                shard_id,
                e
            )
        })?
        .create_if_missing(false)
        .busy_timeout(Duration::from_millis(busy_timeout_ms));
    let mut conn = SqliteConnection::connect_with(&recovery_options)
        .await
        .map_err(|e| {
            miette!(
                "failed to open shard {} DB for journal recovery: {}",
                shard_id,
                e
            )
        })?;
    let version = sqlx::query_scalar::<_, i64>("PRAGMA user_version")
        .fetch_one(&mut conn)
        .await
        .map_err(|e| {
            miette!(
                "failed to read user_version for shard {} DB after recovery: {}",
                shard_id,
                e
            )
        });
    let _ = conn.close().await;
    version
}

/// The inflight cache key of a hash field. Preserves namespace case so `Users`
/// and `users` stay isolated in the cache as they are in SQLite.
pub fn namespaced_key(namespace: &str, key: &str) -> String {
    format!("{namespace}:{key}")
}

pub fn validate_schema_version(version: i64, shard_id: usize) -> MietteResult<()> {
    if !(0..=SHARD_SCHEMA_VERSION).contains(&version) {
        return Err(miette!(
            "shard {} has unsupported schema version {} (supported: 0..={})",
            shard_id,
            version,
            SHARD_SCHEMA_VERSION
        ));
    }
    Ok(())
}

/// Validates that a v1 shard's `blob_namespaces` metadata table exists and
/// matches its physical `blobs_*` tables bidirectionally:
/// - every physical `blobs_*` table has a `blob_namespaces` entry,
/// - every `blob_namespaces` entry points to an existing physical `blobs_*` table,
/// - every `(namespace, table_name)` row satisfies `namespace_table(namespace) == table_name`.
pub async fn validate_v1_shard_metadata(
    conn: &mut SqliteConnection,
    shard_id: usize,
) -> MietteResult<()> {
    let physical_tables: Vec<(String,)> = sqlx::query_as(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'blobs\\_%' ESCAPE '\\'",
    )
    .fetch_all(&mut *conn)
    .await
    .map_err(|e| {
        miette!(
            "failed to list namespace tables while validating shard {}: {}",
            shard_id,
            e
        )
    })?;
    let physical_set: HashSet<String> = physical_tables.into_iter().map(|(t,)| t).collect();

    let rows: Vec<(String, String)> =
        sqlx::query_as("SELECT namespace, table_name FROM blob_namespaces")
            .fetch_all(&mut *conn)
            .await
            .map_err(|e| {
                miette!(
                    "failed to read blob_namespaces on v{} shard {}: {}",
                    SHARD_SCHEMA_VERSION,
                    shard_id,
                    e
                )
            })?;

    let mut mapped_tables: HashSet<String> = HashSet::with_capacity(rows.len());
    for (namespace, table_name) in rows {
        let expected_table = namespace_table(&namespace);
        if expected_table != table_name {
            return Err(miette!(
                "corrupted blob_namespaces entry on shard {}: namespace '{}' maps to '{}' but expected '{}'",
                shard_id,
                namespace,
                table_name,
                expected_table
            ));
        }
        if !physical_set.contains(&table_name) {
            return Err(miette!(
                "orphan blob_namespaces entry on shard {}: namespace '{}' references missing table '{}'",
                shard_id,
                namespace,
                table_name
            ));
        }
        mapped_tables.insert(table_name);
    }

    for table_name in physical_set {
        if !mapped_tables.contains(&table_name) {
            return Err(miette!(
                "missing blob_namespaces entry for table '{}' on v{} shard {}",
                table_name,
                SHARD_SCHEMA_VERSION,
                shard_id
            ));
        }
    }

    Ok(())
}

/// Migrates a shard database to [`SHARD_SCHEMA_VERSION`] in place.
///
/// Rejects unsupported future schema versions (`> SHARD_SCHEMA_VERSION`) and
/// validates v1 `blob_namespaces` metadata bidirectionally before accepting
/// writes. For legacy (version 0) shards, takes an `IMMEDIATE` write lock and
/// re-checks `PRAGMA user_version` under the lock so concurrent startups cannot
/// migrate the same shard twice. Existing `blobs_{namespace}` tables (matched
/// case-insensitively on the `blobs_` prefix) are staged through unique
/// temporary names before moving to their hashed [`namespace_table`] names.
/// Existing `expires_at` indexes are kept without rebuilding (or created if
/// absent), `blob_namespaces` is populated and validated, and
/// `PRAGMA user_version` is bumped to [`SHARD_SCHEMA_VERSION`] atomically.
pub async fn migrate_shard_schema(
    conn: &mut SqliteConnection,
    shard_id: usize,
) -> MietteResult<()> {
    migrate_shard_schema_inner(conn, shard_id, None).await
}

async fn migrate_shard_schema_inner(
    conn: &mut SqliteConnection,
    shard_id: usize,
    after_initial_read: Option<oneshot::Sender<()>>,
) -> MietteResult<()> {
    let current_version: i64 = sqlx::query_scalar("PRAGMA user_version")
        .fetch_one(&mut *conn)
        .await
        .map_err(|e| miette!("failed to read user_version for shard {}: {}", shard_id, e))?;
    validate_schema_version(current_version, shard_id)?;

    if current_version == SHARD_SCHEMA_VERSION {
        // Validate inside a read transaction so sqlite_master and
        // blob_namespaces come from one consistent snapshot even if a writer
        // creates a namespace concurrently.
        let mut read_tx = conn.begin().await.map_err(|e| {
            miette!(
                "failed to begin read tx for v{} metadata validation on shard {}: {}",
                SHARD_SCHEMA_VERSION,
                shard_id,
                e
            )
        })?;
        validate_v1_shard_metadata(&mut read_tx, shard_id).await?;
        read_tx.commit().await.map_err(|e| {
            miette!(
                "failed to close read tx for v{} metadata validation on shard {}: {}",
                SHARD_SCHEMA_VERSION,
                shard_id,
                e
            )
        })?;
        return Ok(());
    }

    if let Some(tx) = after_initial_read {
        let _ = tx.send(());
    }

    let mut tx = conn
        .begin_with("BEGIN IMMEDIATE")
        .await
        .map_err(|e| miette!("failed to begin migration tx for shard {}: {}", shard_id, e))?;

    // Re-read under the write lock so a second concurrent startup cannot
    // re-migrate tables that a racing process just migrated and committed.
    let locked_version: i64 = sqlx::query_scalar("PRAGMA user_version")
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| {
            miette!(
                "failed to re-read user_version under lock for shard {}: {}",
                shard_id,
                e
            )
        })?;
    validate_schema_version(locked_version, shard_id)?;
    if locked_version == SHARD_SCHEMA_VERSION {
        validate_v1_shard_metadata(&mut tx, shard_id).await?;
        tx.commit().await.map_err(|e| {
            miette!(
                "failed to finish no-op migration tx for shard {}: {}",
                shard_id,
                e
            )
        })?;
        return Ok(());
    }

    sqlx::query(CREATE_BLOB_NAMESPACES_TABLE)
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            miette!(
                "failed to create blob_namespaces table for shard {}: {}",
                shard_id,
                e
            )
        })?;

    // Include tables, indexes, views, and triggers so staged temporary table
    // names never collide with any existing SQLite schema object.
    let all_existing_objects: Vec<(String,)> =
        sqlx::query_as("SELECT name FROM sqlite_master WHERE name IS NOT NULL")
            .fetch_all(&mut *tx)
            .await
            .map_err(|e| {
                miette!(
                    "failed to list schema objects for shard {}: {}",
                    shard_id,
                    e
                )
            })?;
    let mut occupied_lower: HashSet<String> = all_existing_objects
        .iter()
        .map(|(name,)| name.to_ascii_lowercase())
        .collect();

    let legacy_tables: Vec<(String,)> = sqlx::query_as(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'blobs\\_%' ESCAPE '\\' ORDER BY name",
    )
    .fetch_all(&mut *tx)
    .await
    .map_err(|e| {
        miette!(
            "failed to list legacy namespace tables for shard {}: {}",
            shard_id,
            e
        )
    })?;

    // Plan all renames up front and stage through unique temporary table names
    // so a legacy table occupying another legacy table's hashed destination
    // (e.g. `users` and `users_<sha256_prefix("users")>`) cannot collide.
    let mut planned: Vec<(String, String, String, String)> = Vec::new();
    let mut tmp_counter: usize = 0;
    for (old_table,) in legacy_tables {
        let Some(namespace) = strip_blobs_prefix_ignore_ascii_case(&old_table) else {
            continue;
        };
        let namespace = namespace.to_string();
        let new_table = namespace_table(&namespace);
        let temp_table = loop {
            let candidate = format!("__blobasaur_migrate_tmp_{tmp_counter}");
            tmp_counter += 1;
            if occupied_lower.insert(candidate.clone()) {
                break candidate;
            }
        };
        planned.push((old_table, namespace, temp_table, new_table));
    }

    // Phase 1: move every legacy table to its temporary name.
    for (old_table, _, temp_table, new_table) in &planned {
        if old_table != new_table {
            let escaped_old = old_table.replace('"', "\"\"");
            let rename_sql = format!("ALTER TABLE \"{escaped_old}\" RENAME TO \"{temp_table}\"");
            sqlx::query(&rename_sql)
                .execute(&mut *tx)
                .await
                .map_err(|e| {
                    miette!(
                        "failed to stage legacy table {} -> {} on shard {}: {}",
                        old_table,
                        temp_table,
                        shard_id,
                        e
                    )
                })?;
        }
    }

    // Phase 2: move every staged table to its final hashed name.
    let mut renamed = 0usize;
    for (old_table, _, temp_table, new_table) in &planned {
        if old_table != new_table {
            let rename_sql = format!("ALTER TABLE \"{temp_table}\" RENAME TO \"{new_table}\"");
            sqlx::query(&rename_sql)
                .execute(&mut *tx)
                .await
                .map_err(|e| {
                    miette!(
                        "failed to finalize table rename {} -> {} on shard {}: {}",
                        old_table,
                        new_table,
                        shard_id,
                        e
                    )
                })?;
            renamed += 1;
            tracing::info!(
                "[Shard {}] Migrated legacy namespace table {} -> {}",
                shard_id,
                old_table,
                new_table
            );
        }
    }

    // Phase 3: keep each migrated table's existing `expires_at` index (SQLite
    // preserves it across ALTER TABLE RENAME without rebuilding), or create one
    // if missing, and record the namespace mapping in `blob_namespaces`.
    for (_, namespace, _, new_table) in &planned {
        let has_expires_index: Option<(i64,)> = sqlx::query_as(
            "SELECT 1 FROM pragma_index_list(?) AS il
             JOIN pragma_index_info(il.name) AS ii
             WHERE il.origin = 'c' AND ii.name = 'expires_at'
             LIMIT 1",
        )
        .bind(new_table)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| {
            miette!(
                "failed to check expires_at index for table {} on shard {}: {}",
                new_table,
                shard_id,
                e
            )
        })?;

        if has_expires_index.is_none() {
            let index_name = namespace_index(new_table);
            let index_sql = format!(
                "CREATE INDEX IF NOT EXISTS {index_name} ON {new_table}(expires_at) WHERE expires_at IS NOT NULL"
            );
            sqlx::query(&index_sql)
                .execute(&mut *tx)
                .await
                .map_err(|e| {
                    miette!(
                        "failed to create expires_at index for {} on shard {}: {}",
                        new_table,
                        shard_id,
                        e
                    )
                })?;
        }

        sqlx::query("INSERT INTO blob_namespaces (namespace, table_name) VALUES (?, ?)")
            .bind(namespace)
            .bind(new_table)
            .execute(&mut *tx)
            .await
            .map_err(|e| {
                miette!(
                    "failed to record namespace {} -> {} on shard {}: {}",
                    namespace,
                    new_table,
                    shard_id,
                    e
                )
            })?;
    }

    // Validate the complete mapping before bumping user_version so pre-existing
    // conflicting or orphan rows in blob_namespaces cannot slip into v1.
    validate_v1_shard_metadata(&mut tx, shard_id).await?;

    sqlx::query(&format!("PRAGMA user_version = {SHARD_SCHEMA_VERSION}"))
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            miette!(
                "failed to update user_version for shard {}: {}",
                shard_id,
                e
            )
        })?;
    tx.commit().await.map_err(|e| {
        miette!(
            "failed to commit schema migration for shard {}: {}",
            shard_id,
            e
        )
    })?;

    if renamed > 0 {
        tracing::info!(
            "[Shard {}] Completed schema migration to version {} ({} legacy tables renamed)",
            shard_id,
            SHARD_SCHEMA_VERSION,
            renamed
        );
    }

    Ok(())
}

/// Whether a query on `table` failed because that table doesn't exist.
/// Namespace tables are created per shard on the first HSET routed there, so a
/// query can hit a shard without the table. That's a miss, not an error. Any
/// other missing table, e.g. one a trigger refers to, is a real error.
pub fn is_missing_table(error: &sqlx::Error, table: &str) -> bool {
    let sqlx::Error::Database(db) = error else {
        return false;
    };
    // A missing table is SQLITE_ERROR (1), or SQLITE_SCHEMA (17) when the
    // schema changed while the statement was prepared, e.g. as another
    // connection creates the table. Checking the code keeps a trigger's RAISE
    // with the same message (SQLITE_CONSTRAINT_TRIGGER) an error.
    let primary_code = db
        .code()
        .and_then(|code| code.parse::<i32>().ok())
        .map(|code| code & 0xff);
    if !matches!(primary_code, Some(1 | 17)) {
        return false;
    }
    let Some(missing) = db.message().strip_prefix("no such table: ") else {
        return false;
    };
    let missing = missing.strip_prefix("main.").unwrap_or(missing);
    missing.eq_ignore_ascii_case(table)
}

/// A fresh token for a [`Pending`] entry and its queued async op.
pub fn next_pending_token() -> u64 {
    static NEXT_TOKEN: AtomicU64 = AtomicU64::new(0);
    NEXT_TOKEN.fetch_add(1, Ordering::Relaxed)
}

/// Whether a queued async op decides `key`'s existence: `Some(true)` for a
/// write, `Some(false)` for a delete, `None` if nothing is queued (ask the DB).
pub async fn pending_exists(cache: &Cache<String, Pending>, key: &str) -> Option<bool> {
    cache
        .get(key)
        .await
        .map(|pending| matches!(pending, Pending::Value { .. }))
}

/// Removes `key`'s entry if it still carries `token`: a newer op on the same
/// key, still queued, must stay visible.
async fn clear_pending_write(cache: &Cache<String, Pending>, key: &str, token: u64) {
    cache
        .entry_by_ref(key)
        .and_compute_with(|entry| async move {
            match entry {
                Some(entry) if entry.value().token() == token => Op::Remove,
                _ => Op::Nop,
            }
        })
        .await;
}

// Enhanced consumer with batching support
pub async fn shard_writer_task(writer: ShardWriter) {
    let ShardWriter {
        shard_id,
        pool,
        mut receiver,
        batch_size,
        batch_timeout_ms,
        inflight_cache,
        inflight_hcache,
        metrics,
        shutdown,
    } = writer;

    let shard_metrics = ShardMetrics::new(shard_id);
    // Load existing namespaced tables into memory
    let mut known_tables = load_existing_tables(&pool, shard_id).await;
    let batch_timeout = Duration::from_millis(batch_timeout_ms);

    tracing::info!(
        "Shard {} writer task started (batch_size={}, timeout={}ms)",
        shard_id,
        batch_size,
        batch_timeout.as_millis()
    );

    let mut batch: VecDeque<ShardWriteOperation> = VecDeque::with_capacity(batch_size);
    let mut pending_maintenance_operation: Option<ShardWriteOperation> = None;

    loop {
        // The next op starts a batch, unless it's a vacuum, which runs alone.
        let next_operation = match pending_maintenance_operation.take() {
            Some(operation) => Some(operation),
            None => tokio::select! {
                biased;
                _ = shutdown.cancelled() => break,
                operation = receiver.recv() => operation,
            },
        };

        match next_operation {
            Some(ShardWriteOperation::Vacuum {
                mode,
                budget_bytes,
                dry_run,
                responder,
            }) => {
                let result =
                    execute_vacuum(shard_id, &pool, mode, budget_bytes, dry_run, &metrics).await;
                let _ = responder.send(result);
                continue;
            }
            Some(operation) => {
                batch.push_back(operation);
            }
            None => break, // Channel closed
        }

        // Fill the batch until it's full or batch_timeout has passed since its
        // first op: take what's queued, then wait for more. A vacuum is an
        // ordering barrier, so it ends the batch and runs next. Shutdown ends
        // the wait: this batch commits, then the drain below takes the rest.
        let deadline = tokio::time::Instant::now() + batch_timeout;
        while batch.len() < batch_size {
            let operation = match receiver.try_recv() {
                Ok(operation) => operation,
                Err(mpsc::error::TryRecvError::Disconnected) => break,
                Err(mpsc::error::TryRecvError::Empty) if batch_timeout.is_zero() => break,
                Err(mpsc::error::TryRecvError::Empty) => {
                    let next_operation = tokio::select! {
                        biased;
                        _ = shutdown.cancelled() => None,
                        result = timeout_at(deadline, receiver.recv()) => result.ok().flatten(),
                    };
                    match next_operation {
                        Some(operation) => operation,
                        None => break, // Timed out, shutting down, or channel closed
                    }
                }
            };

            if matches!(operation, ShardWriteOperation::Vacuum { .. }) {
                pending_maintenance_operation = Some(operation);
                break;
            }
            batch.push_back(operation);
        }

        if !batch.is_empty() {
            let processed_batch_size = batch.len();
            let batch_start = std::time::Instant::now();
            process_batch(
                shard_id,
                &pool,
                &mut batch,
                &mut known_tables,
                &inflight_cache,
                &inflight_hcache,
                &shard_metrics,
            )
            .await;
            let batch_duration = batch_start.elapsed();
            metrics.record_batch_operation(processed_batch_size, batch_duration);
        }
    }

    // Drain: reject new sends, then commit everything already queued without
    // waiting for batch_timeout. Senders whose op made it into the channel were
    // (or will be) acknowledged, so all of it must hit the DB before we return.
    receiver.close();
    let mut drained = 0;
    loop {
        while batch.len() < batch_size {
            match receiver.recv().await {
                // Dropping the responder reports the vacuum as cancelled.
                Some(ShardWriteOperation::Vacuum { .. }) => {}
                Some(operation) => batch.push_back(operation),
                None => break,
            }
        }
        if batch.is_empty() {
            break;
        }

        let processed_batch_size = batch.len();
        drained += processed_batch_size;
        let batch_start = std::time::Instant::now();
        process_batch(
            shard_id,
            &pool,
            &mut batch,
            &mut known_tables,
            &inflight_cache,
            &inflight_hcache,
            &shard_metrics,
        )
        .await;
        let batch_duration = batch_start.elapsed();
        metrics.record_batch_operation(processed_batch_size, batch_duration);
    }

    tracing::info!(
        "Shard {} writer task stopped, drained {} pending ops",
        shard_id,
        drained
    );
}

/// The op's value as an owned `Vec` to bind (empty if it has none): sqlx
/// copies a borrowed blob but takes an owned one as is. Sync ops give theirs
/// up, which costs no copy when the value is the last user of its request
/// buffer. Async ops share theirs with the inflight cache and still need
/// `data.len()` in [`clear_pending_entries`], so copy.
fn take_value(operation: &mut ShardWriteOperation) -> Vec<u8> {
    match operation {
        ShardWriteOperation::Set { data, .. }
        | ShardWriteOperation::HSet { data, .. }
        | ShardWriteOperation::HSetEx { data, .. } => Vec::from(std::mem::take(data)),
        ShardWriteOperation::SetAsync { data, .. }
        | ShardWriteOperation::HSetAsync { data, .. }
        | ShardWriteOperation::HSetExAsync { data, .. } => data.to_vec(),
        _ => Vec::new(),
    }
}

/// Async values a batch must add up to before clearing it also frees them
/// right away. moka keeps removed entries until its next housekeeping, which
/// only runs on later cache activity, so an idle server would hold on to the
/// last large values. Small batches skip it: forcing it after every batch cost
/// ~7% throughput at batch_size = 1.
const FREE_NOW_BYTES: usize = 1024 * 1024;

/// The batch is processed: drop its async ops' inflight cache entries so reads
/// go to the DB, whether or not they committed (a failed op isn't in the DB,
/// and reads shouldn't keep serving it). Only drop an entry the op recorded:
/// a newer op on the same key, still queued, must stay visible.
async fn clear_pending_entries(
    batch: &VecDeque<ShardWriteOperation>,
    inflight_cache: &Cache<String, Pending>,
    inflight_hcache: &Cache<String, Pending>,
) {
    let mut value_bytes = 0;
    for operation in batch.iter() {
        match operation {
            ShardWriteOperation::SetAsync {
                key, data, token, ..
            } => {
                value_bytes += data.len();
                clear_pending_write(inflight_cache, key, *token).await;
            }
            ShardWriteOperation::DeleteAsync { key, token } => {
                clear_pending_write(inflight_cache, key, *token).await;
            }
            ShardWriteOperation::HSetAsync {
                namespace,
                key,
                data,
                token,
            }
            | ShardWriteOperation::HSetExAsync {
                namespace,
                key,
                data,
                token,
                ..
            } => {
                value_bytes += data.len();
                let namespaced_key = namespaced_key(namespace, key);
                clear_pending_write(inflight_hcache, &namespaced_key, *token).await;
            }
            ShardWriteOperation::HDeleteAsync {
                namespace,
                key,
                token,
            } => {
                let namespaced_key = namespaced_key(namespace, key);
                clear_pending_write(inflight_hcache, &namespaced_key, *token).await;
            }
            _ => {}
        }
    }
    if value_bytes >= FREE_NOW_BYTES {
        inflight_cache.run_pending_tasks().await;
        inflight_hcache.run_pending_tasks().await;
    }
}

async fn process_batch(
    shard_id: usize,
    pool: &SqlitePool,
    batch: &mut VecDeque<ShardWriteOperation>,
    known_tables: &mut HashSet<String>,
    inflight_cache: &Cache<String, Pending>,
    inflight_hcache: &Cache<String, Pending>,
    shard_metrics: &ShardMetrics,
) {
    if batch.is_empty() {
        return;
    }

    let batch_size = batch.len();
    tracing::debug!(
        "Processing batch of {} operations for shard {}",
        batch_size,
        shard_id
    );

    // Start transaction
    let mut conn = shard_metrics.acquire(pool, "write").await;
    let tx_start = std::time::Instant::now();
    let mut begin_error = String::new();
    let begun = begin(&mut conn, shard_metrics, tx_start).await;
    let Some(mut tx) = begun.map_err(|e| begin_error = e).ok() else {
        let e = begin_error;
        // Return the connection before the bookkeeping, as on commit.
        drop(conn);
        tracing::error!("[Shard {}] Failed to start transaction: {}", shard_id, e);
        let error_message = format!("Transaction start failed: {}", e);
        clear_pending_entries(batch, inflight_cache, inflight_hcache).await;

        // Send errors to all synchronous operations and clear batch
        for operation in batch.drain(..) {
            match operation {
                ShardWriteOperation::Set { responder, .. }
                | ShardWriteOperation::Delete { responder, .. }
                | ShardWriteOperation::HSet { responder, .. }
                | ShardWriteOperation::HSetEx { responder, .. }
                | ShardWriteOperation::HDelete { responder, .. } => {
                    let _ = responder.send(Err(error_message.clone()));
                }
                ShardWriteOperation::Expire { responder, .. } => {
                    let _ = responder.send(Err(error_message.clone()));
                }
                ShardWriteOperation::HExpire { responder, .. } => {
                    let _ = responder.send(Err(error_message.clone()));
                }
                ShardWriteOperation::Vacuum {
                    mode,
                    budget_bytes,
                    dry_run,
                    responder,
                } => {
                    let _ = responder.send(VacuumResult {
                        mode,
                        budget_bytes,
                        dry_run,
                        before: None,
                        after: None,
                        duration: Duration::ZERO,
                        incremental_pages_requested: None,
                        estimated_reclaimed_pages: None,
                        errors: vec![error_message.clone()],
                    });
                }
                ShardWriteOperation::SetAsync { .. }
                | ShardWriteOperation::DeleteAsync { .. }
                | ShardWriteOperation::HSetAsync { .. }
                | ShardWriteOperation::HSetExAsync { .. }
                | ShardWriteOperation::HDeleteAsync { .. } => {
                    shard_metrics.async_write_failures_total.increment(1);
                    shard_metrics.async_writes_pending.decrement(1.0);
                }
            }
        }
        return;
    };

    // One entry per op, in batch order.
    let mut results: Vec<Result<(), String>> = Vec::with_capacity(batch.len());
    // Tables this batch added to known_tables; see the commit below.
    let mut created_tables: Vec<String> = Vec::new();
    let mut expire_results: Vec<(usize, bool)> = Vec::new();
    let mut hexpire_results: Vec<(usize, i64)> = Vec::new();
    let mut sync_operations: Vec<usize> = Vec::new();

    // Execute all operations in the transaction
    for (idx, operation) in batch.iter_mut().enumerate() {
        let value = take_value(operation);
        let operation = &*operation;
        let result = match operation {
            ShardWriteOperation::Set {
                key, expires_at, ..
            }
            | ShardWriteOperation::SetAsync {
                key, expires_at, ..
            } => {
                if let ShardWriteOperation::Set { .. } = operation {
                    sync_operations.push(idx)
                }

                let now = Utc::now().timestamp();

                // Check if record exists to determine if this is an insert or update
                let exists = sqlx::query("SELECT 1 FROM blobs WHERE key = ?")
                    .bind(key)
                    .fetch_optional(&mut *tx)
                    .await
                    .map(|row| row.is_some())
                    .unwrap_or(false);

                if exists {
                    // Update existing record - update data, updated_at, expires_at, and version
                    sqlx::query("UPDATE blobs SET data = ?, updated_at = ?, expires_at = ?, version = version + 1 WHERE key = ?")
                        .bind(value)
                        .bind(now)
                        .bind(expires_at)
                        .bind(key)
                        .execute(&mut *tx)
                        .await
                        .map(|_| ())
                        .map_err(|e| {
                            tracing::error!("[Shard {}] UPDATE error for key {}: {}", shard_id, key, e);
                            e.to_string()
                        })
                } else {
                    // Insert new record with metadata
                    sqlx::query("INSERT INTO blobs (key, data, created_at, updated_at, expires_at, version) VALUES (?, ?, ?, ?, ?, 0)")
                        .bind(key)
                        .bind(value)
                        .bind(now)
                        .bind(now)
                        .bind(expires_at)
                        .execute(&mut *tx)
                        .await
                        .map(|_| ())
                        .map_err(|e| {
                            tracing::error!("[Shard {}] INSERT error for key {}: {}", shard_id, key, e);
                            e.to_string()
                        })
                }
            }
            ShardWriteOperation::Delete { key, .. }
            | ShardWriteOperation::DeleteAsync { key, .. } => {
                if let ShardWriteOperation::Delete { .. } = operation {
                    sync_operations.push(idx)
                }

                sqlx::query("DELETE FROM blobs WHERE key = ?")
                    .bind(key)
                    .execute(&mut *tx)
                    .await
                    .map(|_| ())
                    .map_err(|e| {
                        tracing::error!("[Shard {}] DELETE error for key {}: {}", shard_id, key, e);
                        e.to_string()
                    })
            }
            ShardWriteOperation::HSet { namespace, key, .. }
            | ShardWriteOperation::HSetAsync { namespace, key, .. } => {
                if let ShardWriteOperation::HSet { .. } = operation {
                    sync_operations.push(idx)
                }

                let table_name = namespace_table(namespace);

                // Ensure table exists
                if let Err(e) = ensure_namespaced_table_exists(
                    &mut tx,
                    namespace,
                    known_tables,
                    &mut created_tables,
                )
                .await
                {
                    tracing::error!("[Shard {}] Failed to ensure table exists: {}", shard_id, e);
                    Err(e)
                } else {
                    let now = Utc::now().timestamp();

                    // Check if record exists to determine if this is an insert or update
                    let query = format!("SELECT 1 FROM {} WHERE key = ?", table_name);
                    let exists = sqlx::query(&query)
                        .bind(key)
                        .fetch_optional(&mut *tx)
                        .await
                        .map(|row| row.is_some())
                        .unwrap_or(false);

                    if exists {
                        // Update existing record. A live TTL is kept; an
                        // expired row is a new field, so its expiry is cleared.
                        let update_query = format!(
                            "UPDATE {} SET data = ?, updated_at = ?, expires_at = CASE WHEN expires_at <= ? THEN NULL ELSE expires_at END, version = version + 1 WHERE key = ?",
                            table_name
                        );
                        sqlx::query(&update_query)
                            .bind(value)
                            .bind(now)
                            .bind(now)
                            .bind(key)
                            .execute(&mut *tx)
                            .await
                            .map(|_| ())
                            .map_err(|e| {
                                tracing::error!(
                                    "[Shard {}] HSET UPDATE error for namespace {} key {}: {}",
                                    shard_id,
                                    namespace,
                                    key,
                                    e
                                );
                                e.to_string()
                            })
                    } else {
                        // Insert new record
                        let insert_query = format!(
                            "INSERT INTO {} (key, data, created_at, updated_at, expires_at, version) VALUES (?, ?, ?, ?, NULL, 0)",
                            table_name
                        );
                        sqlx::query(&insert_query)
                            .bind(key)
                            .bind(value)
                            .bind(now)
                            .bind(now)
                            .execute(&mut *tx)
                            .await
                            .map(|_| ())
                            .map_err(|e| {
                                tracing::error!(
                                    "[Shard {}] HSET INSERT error for namespace {} key {}: {}",
                                    shard_id,
                                    namespace,
                                    key,
                                    e
                                );
                                e.to_string()
                            })
                    }
                }
            }
            ShardWriteOperation::HSetEx {
                namespace,
                key,
                expires_at,
                ..
            }
            | ShardWriteOperation::HSetExAsync {
                namespace,
                key,
                expires_at,
                ..
            } => {
                if let ShardWriteOperation::HSetEx { .. } = operation {
                    sync_operations.push(idx)
                }

                let table_name = namespace_table(namespace);

                // Ensure table exists
                if let Err(e) = ensure_namespaced_table_exists(
                    &mut tx,
                    namespace,
                    known_tables,
                    &mut created_tables,
                )
                .await
                {
                    tracing::error!("[Shard {}] Failed to ensure table exists: {}", shard_id, e);
                    Err(e)
                } else {
                    let now = Utc::now().timestamp();

                    // Check if record exists to determine if this is an insert or update
                    let query = format!("SELECT 1 FROM {} WHERE key = ?", table_name);
                    let exists = sqlx::query(&query)
                        .bind(key)
                        .fetch_optional(&mut *tx)
                        .await
                        .map(|row| row.is_some())
                        .unwrap_or(false);

                    if exists {
                        // Update existing record with expiration
                        let update_query = format!(
                            "UPDATE {} SET data = ?, updated_at = ?, expires_at = ?, version = version + 1 WHERE key = ?",
                            table_name
                        );
                        sqlx::query(&update_query)
                            .bind(value)
                            .bind(now)
                            .bind(*expires_at)
                            .bind(key)
                            .execute(&mut *tx)
                            .await
                            .map(|_| ())
                            .map_err(|e| {
                                tracing::error!(
                                    "[Shard {}] HSETEX UPDATE error for namespace {} key {}: {}",
                                    shard_id,
                                    namespace,
                                    key,
                                    e
                                );
                                e.to_string()
                            })
                    } else {
                        // Insert new record with expiration
                        let insert_query = format!(
                            "INSERT INTO {} (key, data, created_at, updated_at, expires_at, version) VALUES (?, ?, ?, ?, ?, 0)",
                            table_name
                        );
                        sqlx::query(&insert_query)
                            .bind(key)
                            .bind(value)
                            .bind(now)
                            .bind(now)
                            .bind(*expires_at)
                            .execute(&mut *tx)
                            .await
                            .map(|_| ())
                            .map_err(|e| {
                                tracing::error!(
                                    "[Shard {}] HSETEX INSERT error for namespace {} key {}: {}",
                                    shard_id,
                                    namespace,
                                    key,
                                    e
                                );
                                e.to_string()
                            })
                    }
                }
            }
            ShardWriteOperation::HDelete { namespace, key, .. }
            | ShardWriteOperation::HDeleteAsync { namespace, key, .. } => {
                if let ShardWriteOperation::HDelete { .. } = operation {
                    sync_operations.push(idx)
                }

                let table_name = namespace_table(namespace);

                // Asks SQLite rather than known_tables, which can be wrong
                // after a failed commit (see process_batch). A missing table
                // means the field doesn't exist: nothing to delete.
                let delete_query = format!("DELETE FROM {} WHERE key = ?", table_name);
                match sqlx::query(&delete_query).bind(key).execute(&mut *tx).await {
                    Ok(_) => Ok(()),
                    Err(e) if is_missing_table(&e, &table_name) => Ok(()),
                    Err(e) => {
                        tracing::error!(
                            "[Shard {}] HDEL error for namespace {} key {}: {}",
                            shard_id,
                            namespace,
                            key,
                            e
                        );
                        Err(e.to_string())
                    }
                }
            }
            ShardWriteOperation::Expire {
                key, expires_at, ..
            } => {
                sync_operations.push(idx);

                // Update the expires_at field for the key if it exists. An
                // expired row not yet removed by cleanup counts as missing.
                match sqlx::query(
                    "UPDATE blobs SET expires_at = ? WHERE key = ? AND (expires_at IS NULL OR expires_at > ?)",
                )
                .bind(expires_at)
                .bind(key)
                .bind(Utc::now().timestamp())
                .execute(&mut *tx)
                .await
                {
                    Ok(result) => {
                        let rows_affected = result.rows_affected() > 0;
                        expire_results.push((idx, rows_affected));
                        Ok(())
                    }
                    Err(e) => {
                        tracing::error!("[Shard {}] EXPIRE error for key {}: {}", shard_id, key, e);
                        expire_results.push((idx, false));
                        Err(e.to_string())
                    }
                }
            }
            ShardWriteOperation::HExpire {
                namespace,
                key,
                expires_at,
                condition,
                ..
            } => {
                sync_operations.push(idx);
                let (result_code, operation_result) = handle_hexpire_operation(
                    &mut tx,
                    shard_id,
                    namespace,
                    key,
                    *expires_at,
                    *condition,
                )
                .await;
                hexpire_results.push((idx, result_code));
                operation_result
            }
            ShardWriteOperation::Vacuum { .. } => {
                Err("Vacuum operation reached transactional batch unexpectedly".to_string())
            }
        };

        results.push(result);
    }

    // Commit transaction
    let commit_result = tx.commit().await.map_err(|e| {
        tracing::error!("[Shard {}] Transaction commit failed: {}", shard_id, e);
        e.to_string()
    });
    // Return the writer connection before the bookkeeping below.
    drop(conn);
    shard_metrics.record_transaction(commit_result.is_ok(), tx_start.elapsed());
    if let Err(e) = &commit_result {
        shard_metrics.record_sqlite_error("commit", e);
        // Their CREATE TABLE may have rolled back with the batch. Forgetting
        // them makes the next write to their namespace create them again.
        // Some may exist anyway (after e.g. SQLITE_FULL, SQLite rolls back
        // and runs the rest of the batch outside the transaction); then the
        // CREATE TABLE IF NOT EXISTS is a no-op. Nothing else trusts
        // known_tables: HDEL and HEXPIRE ask SQLite.
        for table in &created_tables {
            known_tables.remove(table);
        }
    }
    for e in results.iter().filter_map(|r| r.as_ref().err()) {
        shard_metrics.record_sqlite_error("execute", e);
    }

    clear_pending_entries(batch, inflight_cache, inflight_hcache).await;

    // An op succeeded only if its own statement and the commit both did. A
    // failed statement doesn't abort the transaction, so the commit alone
    // can't tell a client its write was persisted.
    let op_result = |idx: usize| commit_result.clone().and(results[idx].clone());

    // Send responses to synchronous operations
    for (operation_idx, operation) in batch.drain(..).enumerate() {
        match operation {
            ShardWriteOperation::Set { responder, .. }
            | ShardWriteOperation::Delete { responder, .. }
            | ShardWriteOperation::HSet { responder, .. }
            | ShardWriteOperation::HSetEx { responder, .. }
            | ShardWriteOperation::HDelete { responder, .. } => {
                let _ = responder.send(op_result(operation_idx));
            }
            ShardWriteOperation::Expire { responder, .. } => {
                // For expire operations, we need to send the actual result (bool)
                // Find the corresponding expire result using current operation index
                if let Some((_, success)) =
                    expire_results.iter().find(|(idx, _)| *idx == operation_idx)
                {
                    let _ = responder.send(op_result(operation_idx).map(|()| *success));
                } else {
                    let _ = responder.send(Err(
                        "Internal error: could not find expire result".to_string()
                    ));
                }
            }
            ShardWriteOperation::HExpire { responder, .. } => {
                // For hexpire operations, send the per-field result code
                if let Some((_, result_code)) = hexpire_results
                    .iter()
                    .find(|(idx, _)| *idx == operation_idx)
                {
                    let _ = responder.send(op_result(operation_idx).map(|()| *result_code));
                } else {
                    let _ = responder.send(Err(
                        "Internal error: could not find hexpire result".to_string()
                    ));
                }
            }
            ShardWriteOperation::SetAsync { .. }
            | ShardWriteOperation::DeleteAsync { .. }
            | ShardWriteOperation::HSetAsync { .. }
            | ShardWriteOperation::HSetExAsync { .. }
            | ShardWriteOperation::HDeleteAsync { .. } => {
                // Async operations don't need responses, but log commit errors
                if let Err(e) = &commit_result {
                    tracing::error!(
                        "[Shard {}] ASYNC operation failed due to commit error: {}",
                        shard_id,
                        e
                    );
                }
                if op_result(operation_idx).is_err() {
                    shard_metrics.async_write_failures_total.increment(1);
                }
                shard_metrics.async_writes_pending.decrement(1.0);
            }
            ShardWriteOperation::Vacuum {
                mode,
                budget_bytes,
                dry_run,
                responder,
            } => {
                let mut errors = vec![
                    "Vacuum operation reached transactional response path unexpectedly".to_string(),
                ];
                if let Err(e) = &commit_result {
                    errors.push(format!("Commit failed: {}", e));
                }

                let _ = responder.send(VacuumResult {
                    mode,
                    budget_bytes,
                    dry_run,
                    before: None,
                    after: None,
                    duration: Duration::ZERO,
                    incremental_pages_requested: None,
                    estimated_reclaimed_pages: None,
                    errors,
                });
            }
        }
    }
}

/// Begins the batch transaction on the writer connection, if there is one.
/// A failed acquire was already recorded by [`ShardMetrics::acquire`].
///
/// IMMEDIATE takes the write lock up front, waiting up to busy_timeout if
/// another connection holds it. A deferred BEGIN would take it at the first
/// write, after the existence check's read, and SQLite fails that upgrade at
/// once without waiting: the batch's writes, async ones already acked, would
/// be lost.
async fn begin<'c>(
    conn: &'c mut Result<PoolConnection<Sqlite>, sqlx::Error>,
    shard_metrics: &ShardMetrics,
    tx_start: std::time::Instant,
) -> Result<sqlx::Transaction<'c, Sqlite>, String> {
    let conn = conn.as_mut().map_err(|e| e.to_string())?;
    conn.begin_with("BEGIN IMMEDIATE").await.map_err(|e| {
        shard_metrics.record_sqlite_error("begin", &e.to_string());
        shard_metrics.record_transaction(false, tx_start.elapsed());
        e.to_string()
    })
}

async fn handle_hexpire_operation(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    shard_id: usize,
    namespace: &str,
    key: &str,
    expires_at: i64,
    condition: Option<HExpireCondition>,
) -> (i64, Result<(), String>) {
    let table_name = namespace_table(namespace);

    // Treat logically expired fields as non-existent.
    let now = Utc::now().timestamp();
    let select_query = format!(
        "SELECT expires_at FROM {} WHERE key = ? AND (expires_at IS NULL OR expires_at > ?)",
        table_name
    );
    match sqlx::query_as::<_, (Option<i64>,)>(&select_query)
        .bind(key)
        .bind(now)
        .fetch_optional(&mut **tx)
        .await
    {
        // A missing namespace table is a missing field, as in HDEL.
        Ok(None) => (-2, Ok(())),
        Err(e) if is_missing_table(&e, &table_name) => (-2, Ok(())),
        Ok(Some((current_expires_at,))) => {
            // Apply condition check before mutating the row.
            let should_set = match condition {
                None => true,
                Some(HExpireCondition::Nx) => current_expires_at.is_none(),
                Some(HExpireCondition::Xx) => current_expires_at.is_some(),
                Some(HExpireCondition::Gt) => match current_expires_at {
                    None => false, // Non-volatile = infinite TTL, new < infinite
                    Some(current) => expires_at > current,
                },
                Some(HExpireCondition::Lt) => match current_expires_at {
                    None => true, // Non-volatile = infinite TTL, new < infinite
                    Some(current) => expires_at < current,
                },
            };

            if !should_set {
                return (0, Ok(()));
            }

            if expires_at <= now {
                // Immediate/past expiration deletes the field.
                let delete_query = format!("DELETE FROM {} WHERE key = ?", table_name);
                match sqlx::query(&delete_query)
                    .bind(key)
                    .execute(&mut **tx)
                    .await
                {
                    Ok(_) => (2, Ok(())),
                    Err(e) => {
                        tracing::error!(
                            "[Shard {}] HEXPIRE DELETE error for namespace {} key {}: {}",
                            shard_id,
                            namespace,
                            key,
                            e
                        );
                        (-2, Err(e.to_string()))
                    }
                }
            } else {
                let update_query =
                    format!("UPDATE {} SET expires_at = ? WHERE key = ?", table_name);
                match sqlx::query(&update_query)
                    .bind(expires_at)
                    .bind(key)
                    .execute(&mut **tx)
                    .await
                {
                    Ok(_) => (1, Ok(())),
                    Err(e) => {
                        tracing::error!(
                            "[Shard {}] HEXPIRE UPDATE error for namespace {} key {}: {}",
                            shard_id,
                            namespace,
                            key,
                            e
                        );
                        (0, Err(e.to_string()))
                    }
                }
            }
        }
        Err(e) => {
            tracing::error!(
                "[Shard {}] HEXPIRE SELECT error for namespace {} key {}: {}",
                shard_id,
                namespace,
                key,
                e
            );
            (-2, Err(e.to_string()))
        }
    }
}

fn compute_incremental_vacuum_pages(freelist_count: u64, page_size: u64, budget_bytes: u64) -> u64 {
    if page_size == 0 {
        return 0;
    }

    let budget_pages = budget_bytes / page_size;
    freelist_count.min(budget_pages)
}

fn vacuum_mode_name(mode: VacuumMode) -> &'static str {
    match mode {
        VacuumMode::Incremental => "incremental",
        VacuumMode::Full => "full",
    }
}

fn estimate_reclaimed_bytes(
    estimated_reclaimed_pages: Option<u64>,
    before: Option<VacuumStats>,
    after: Option<VacuumStats>,
) -> Option<u64> {
    let page_size = before.or(after).map(|stats| stats.page_size)?;
    estimated_reclaimed_pages.map(|pages| pages.saturating_mul(page_size))
}

fn is_sqlite_busy_error(error: &str) -> bool {
    let normalized = error.to_ascii_lowercase();
    normalized.contains("database is locked")
        || normalized.contains("database table is locked")
        || normalized.contains("database is busy")
        || normalized.contains("sqlite_busy")
}

fn classify_vacuum_error_kinds(errors: &[String]) -> (bool, bool) {
    let mut has_busy = false;
    let mut has_error = false;

    for error in errors {
        if is_sqlite_busy_error(error) {
            has_busy = true;
            continue;
        }

        has_error = true;
    }

    (has_busy, has_error)
}

async fn collect_vacuum_stats(pool: &SqlitePool) -> Result<VacuumStats, String> {
    let page_size = sqlx::query_scalar::<_, i64>("PRAGMA page_size")
        .fetch_one(pool)
        .await
        .map_err(|e| format!("failed to read PRAGMA page_size: {}", e))?;

    let page_count = sqlx::query_scalar::<_, i64>("PRAGMA page_count")
        .fetch_one(pool)
        .await
        .map_err(|e| format!("failed to read PRAGMA page_count: {}", e))?;

    let freelist_count = sqlx::query_scalar::<_, i64>("PRAGMA freelist_count")
        .fetch_one(pool)
        .await
        .map_err(|e| format!("failed to read PRAGMA freelist_count: {}", e))?;

    Ok(VacuumStats {
        page_size: page_size.max(0) as u64,
        page_count: page_count.max(0) as u64,
        freelist_count: freelist_count.max(0) as u64,
    })
}

/// Runs `PRAGMA wal_checkpoint(TRUNCATE)` and treats an incomplete checkpoint as
/// an error. SQLite reports a blocked checkpoint as `busy=1` in the result row
/// rather than as a statement error.
pub(crate) async fn wal_checkpoint_truncate<'e, E>(executor: E) -> Result<(), String>
where
    E: sqlx::Executor<'e, Database = Sqlite>,
{
    let (busy, log_frames, checkpointed_frames) =
        sqlx::query_as::<_, (i64, i64, i64)>("PRAGMA wal_checkpoint(TRUNCATE)")
            .fetch_one(executor)
            .await
            .map_err(|e| format!("wal_checkpoint(TRUNCATE) failed: {}", e))?;

    if busy != 0 {
        return Err(format!(
            "wal_checkpoint(TRUNCATE) incomplete: database is busy (log={}, checkpointed={})",
            log_frames, checkpointed_frames
        ));
    }

    Ok(())
}

async fn execute_vacuum(
    shard_id: usize,
    pool: &SqlitePool,
    mode: VacuumMode,
    budget_bytes: u64,
    dry_run: bool,
    metrics: &Metrics,
) -> VacuumResult {
    let start = std::time::Instant::now();
    let mode_name = vacuum_mode_name(mode);
    let mut errors = Vec::new();

    tracing::info!(
        shard_id,
        mode = mode_name,
        budget_bytes,
        dry_run,
        "Starting shard vacuum operation"
    );

    let before = match collect_vacuum_stats(pool).await {
        Ok(stats) => Some(stats),
        Err(e) => {
            errors.push(e);
            None
        }
    };

    let mut incremental_pages_requested = None;
    if let (VacuumMode::Incremental, Some(before_stats)) = (mode, before) {
        let pages_to_vacuum = compute_incremental_vacuum_pages(
            before_stats.freelist_count,
            before_stats.page_size,
            budget_bytes,
        );
        incremental_pages_requested = Some(pages_to_vacuum);
    }

    if !dry_run {
        match mode {
            VacuumMode::Incremental => {
                if let Some(pages_to_vacuum) = incremental_pages_requested {
                    if pages_to_vacuum > 0 {
                        let query = format!("PRAGMA incremental_vacuum({})", pages_to_vacuum);
                        if let Err(e) = sqlx::query(&query).execute(pool).await {
                            errors.push(format!(
                                "incremental_vacuum({}) failed: {}",
                                pages_to_vacuum, e
                            ));
                        }
                    }
                } else {
                    errors.push(
                        "incremental vacuum skipped because pre-stats were unavailable".to_string(),
                    );
                }
            }
            VacuumMode::Full => {
                if let Err(e) = sqlx::query("VACUUM").execute(pool).await {
                    errors.push(format!("VACUUM failed: {}", e));
                }
            }
        }

        // In WAL mode the main DB file only shrinks once the vacuum's frames are
        // checkpointed, and a full VACUUM leaves a WAL roughly the size of the
        // rebuilt DB. Truncate it so the reclaimed space is returned to disk.
        if let Err(e) = wal_checkpoint_truncate(pool).await {
            errors.push(e);
        }
    }

    let after = match collect_vacuum_stats(pool).await {
        Ok(stats) => Some(stats),
        Err(e) => {
            errors.push(e);
            None
        }
    };

    let estimated_reclaimed_pages = if dry_run {
        match (mode, before) {
            (VacuumMode::Incremental, _) => incremental_pages_requested,
            (VacuumMode::Full, Some(before_stats)) => Some(before_stats.freelist_count),
            (VacuumMode::Full, None) => None,
        }
    } else {
        // Use the page_count delta: a full VACUUM also reclaims fragmented space
        // that never appeared on the freelist, and concurrent expiry deletes can
        // grow the freelist mid-run without changing the file size.
        match (before, after) {
            (Some(before_stats), Some(after_stats)) => Some(
                before_stats
                    .page_count
                    .saturating_sub(after_stats.page_count),
            ),
            _ => None,
        }
    };

    let estimated_reclaimed_bytes =
        estimate_reclaimed_bytes(estimated_reclaimed_pages, before, after);

    let duration = start.elapsed();
    let run_result = if errors.is_empty() { "ok" } else { "error" };

    metrics.record_vacuum_run(mode_name, run_result, Some(duration));

    if let Some(reclaimed_pages) = estimated_reclaimed_pages {
        metrics.record_vacuum_reclaimed_estimate(
            mode_name,
            shard_id,
            reclaimed_pages,
            estimated_reclaimed_bytes,
        );
    }

    let (has_busy, has_error) = classify_vacuum_error_kinds(&errors);
    if has_busy {
        metrics.record_vacuum_shard_failure(shard_id, mode_name, "busy");
    }
    if has_error {
        metrics.record_vacuum_shard_failure(shard_id, mode_name, "error");
    }

    tracing::info!(
        shard_id,
        mode = mode_name,
        budget_bytes,
        dry_run,
        duration_ms = duration.as_millis() as u64,
        pre_stats = ?before,
        post_stats = ?after,
        incremental_pages_requested = ?incremental_pages_requested,
        estimated_reclaimed_pages = ?estimated_reclaimed_pages,
        estimated_reclaimed_bytes = ?estimated_reclaimed_bytes,
        errors = ?errors,
        "Completed shard vacuum operation"
    );

    VacuumResult {
        mode,
        budget_bytes,
        dry_run,
        before,
        after,
        duration,
        incremental_pages_requested,
        estimated_reclaimed_pages,
        errors,
    }
}

// Load existing namespaced tables from the database
async fn load_existing_tables(pool: &SqlitePool, shard_id: usize) -> HashSet<String> {
    let mut tables = HashSet::new();

    // Add the default blobs table
    tables.insert("blobs".to_string());

    match sqlx::query_as::<_, (String,)>(
        "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'blobs\\_%' ESCAPE '\\'",
    )
    .fetch_all(pool)
    .await
    {
        Ok(rows) => {
            for (table_name,) in rows {
                tables.insert(table_name.to_ascii_lowercase());
            }
            tracing::info!(
                "[Shard {}] Loaded {} existing namespaced tables",
                shard_id,
                tables.len() - 1
            );
        }
        Err(e) => {
            tracing::error!("[Shard {}] Failed to load existing tables: {}", shard_id, e);
        }
    }

    tables
}

/// Ensures `namespace`'s table exists, creating it if necessary. A table it
/// creates is added to `known_tables` and to `created`, so the caller can
/// forget it again if the transaction doesn't commit.
async fn ensure_namespaced_table_exists(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    namespace: &str,
    known_tables: &mut HashSet<String>,
    created: &mut Vec<String>,
) -> Result<(), String> {
    let table_name = namespace_table(namespace);
    let table_name = table_name.as_str();
    if known_tables.contains(table_name) {
        return Ok(());
    }

    // Run table, index, and metadata creation inside a savepoint so a failure
    // on any step (e.g. CREATE INDEX failing) rolls back the table creation
    // even if the outer batch transaction commits other operations.
    let mut sp = tx.begin().await.map_err(|e| {
        tracing::error!("Failed to start savepoint for table {}: {}", table_name, e);
        format!("Failed to start savepoint: {}", e)
    })?;

    let init_res: Result<(), String> = async {
        let create_query = format!(
            "CREATE TABLE IF NOT EXISTS {} (
                key TEXT PRIMARY KEY,
                data BLOB,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                expires_at INTEGER,
                version INTEGER NOT NULL DEFAULT 0
            )",
            table_name
        );
        if let Err(e) = sqlx::query(&create_query).execute(&mut *sp).await {
            tracing::error!("Failed to create namespaced table {}: {}", table_name, e);
            return Err(format!("Failed to create table: {}", e));
        }

        // Create index on expires_at for efficient expiry queries
        let index_name = namespace_index(table_name);
        let index_query = format!(
            "CREATE INDEX IF NOT EXISTS {} ON {}(expires_at) WHERE expires_at IS NOT NULL",
            index_name, table_name
        );
        if let Err(e) = sqlx::query(&index_query).execute(&mut *sp).await {
            tracing::error!(
                "Failed to create expires_at index for table {}: {}",
                table_name,
                e
            );
            return Err(format!("Failed to create index: {}", e));
        }

        if let Err(e) = sqlx::query(CREATE_BLOB_NAMESPACES_TABLE)
            .execute(&mut *sp)
            .await
        {
            tracing::error!("Failed to ensure blob_namespaces table: {}", e);
            return Err(format!("Failed to create blob_namespaces table: {}", e));
        }

        if let Err(e) = sqlx::query(
            "INSERT OR IGNORE INTO blob_namespaces (namespace, table_name) VALUES (?, ?)",
        )
        .bind(namespace)
        .bind(table_name)
        .execute(&mut *sp)
        .await
        {
            tracing::error!(
                "Failed to record namespace {} -> {} in blob_namespaces: {}",
                namespace,
                table_name,
                e
            );
            return Err(format!("Failed to record namespace metadata: {}", e));
        }

        Ok(())
    }
    .await;

    match init_res {
        Ok(()) => {
            if let Err(e) = sp.commit().await {
                tracing::error!(
                    "Failed to release savepoint for table {}: {}",
                    table_name,
                    e
                );
                return Err(format!("Failed to release savepoint: {}", e));
            }
            known_tables.insert(table_name.to_string());
            created.push(table_name.to_string());
            tracing::info!(
                "Created namespaced table and expires_at index: {} (namespace: {})",
                table_name,
                namespace
            );
            Ok(())
        }
        Err(err) => {
            let _ = sp.rollback().await;
            Err(err)
        }
    }
}

/// Deletes expired rows from `table` in chunks of `chunk_size`. The
/// cleanup shares the shard's single writer connection, so each chunk is its own
/// statement: the pool hands out connections in FIFO order, letting a waiting
/// writer batch run between chunks instead of stalling behind one long DELETE.
/// Each chunk's rows are added to `deleted` as they go, so a later failing
/// chunk doesn't lose the count.
async fn delete_expired(
    pool: &SqlitePool,
    table: &str,
    now: i64,
    chunk_size: u32,
    deleted_counter: &Counter,
) -> Result<u64, sqlx::Error> {
    let query = format!(
        "DELETE FROM {table} WHERE rowid IN (
            SELECT rowid FROM {table} WHERE expires_at IS NOT NULL AND expires_at <= ? LIMIT ?
        )"
    );
    let mut total = 0;
    loop {
        let deleted = sqlx::query(&query)
            .bind(now)
            .bind(chunk_size)
            .execute(pool)
            .await?
            .rows_affected();
        deleted_counter.increment(deleted);
        total += deleted;
        if deleted < u64::from(chunk_size) {
            return Ok(total);
        }
    }
}

/// Background task to clean up expired keys from a shard
pub async fn shard_cleanup_task(
    shard_id: usize,
    pool: SqlitePool,
    cleanup_interval_secs: u64,
    chunk_size: u32,
) {
    let mut interval = interval(Duration::from_secs(cleanup_interval_secs));
    let shard_metrics = ShardMetrics::new(shard_id);

    tracing::info!(
        "[Shard {}] Starting cleanup task with interval {} seconds",
        shard_id,
        cleanup_interval_secs
    );

    loop {
        interval.tick().await;

        let started = std::time::Instant::now();
        let mut ok = true;
        let now = chrono::Utc::now().timestamp();

        // The main blobs table plus every namespaced "blobs_*" table
        let mut tables = vec!["blobs".to_string()];
        match sqlx::query_as::<_, (String,)>(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'blobs\\_%' ESCAPE '\\'",
        )
        .fetch_all(&pool)
        .await
        {
            Ok(names) => tables.extend(names.into_iter().map(|(name,)| name)),
            Err(e) => {
                tracing::error!(
                    "[Shard {}] Error querying table names for cleanup: {}",
                    shard_id,
                    e
                );
                shard_metrics.record_cleanup_error("discover");
                ok = false;
            }
        }

        for table_name in tables {
            let deleted = &shard_metrics.cleanup_deleted_keys_total;
            match delete_expired(&pool, &table_name, now, chunk_size, deleted).await {
                Ok(deleted_count) => {
                    if deleted_count > 0 {
                        tracing::info!(
                            "[Shard {}] Cleaned up {} expired keys from table {}",
                            shard_id,
                            deleted_count,
                            table_name
                        );
                    }
                }
                Err(e) => {
                    tracing::error!(
                        "[Shard {}] Error during cleanup of table {}: {}",
                        shard_id,
                        table_name,
                        e
                    );
                    shard_metrics.record_cleanup_error("delete");
                    ok = false;
                }
            }
        }
        shard_metrics.record_cleanup(ok, started.elapsed());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
    use std::str::FromStr;
    use tempfile::TempDir;
    use tokio::time::timeout;

    #[test]
    fn take_value_reuses_a_sync_values_request_buffer() {
        // A request split off the connection buffer, which is then replaced, as
        // decode_request and handle_connection do after a large request.
        let mut buf = bytes::BytesMut::with_capacity(4096);
        buf.extend_from_slice(b"*3\r\n$3\r\nSET\r\n");
        let header = buf.len();
        buf.extend_from_slice(&[7u8; 1024]);
        let request = buf.split().freeze();
        drop(buf);
        let data = request.slice(header..);
        drop(request);

        let (responder, _rx) = oneshot::channel();
        let mut op = ShardWriteOperation::Set {
            key: "k".to_string(),
            data,
            expires_at: None,
            responder,
        };
        let value = take_value(&mut op);
        assert_eq!(value, vec![7u8; 1024]);
        // A copy would be a new allocation sized to the value.
        assert_eq!(value.capacity(), 4096, "value was copied");
    }

    #[tokio::test]
    async fn hdel_and_hexpire_dont_trust_known_tables() {
        // A table the writer has no record of, as after a commit that failed
        // although SQLite kept the table (see process_batch).
        let (_temp_dir, pool) = create_test_pool().await;
        let ns_table = namespace_table("ns");
        let trig_table = namespace_table("trig");
        let raise_table = namespace_table("raise");
        for sql in [
            format!(
                "CREATE TABLE {ns_table} (key TEXT PRIMARY KEY, data BLOB, created_at INTEGER NOT NULL,
                 updated_at INTEGER NOT NULL, expires_at INTEGER, version INTEGER NOT NULL DEFAULT 0)"
            ),
            format!(
                "INSERT INTO {ns_table} (key, data, created_at, updated_at) VALUES ('e', x'00', 0, 0)"
            ),
            format!(
                "INSERT INTO {ns_table} (key, data, created_at, updated_at) VALUES ('d', x'00', 0, 0)"
            ),
            // A delete here fails on a missing table other than its own.
            format!(
                "CREATE TABLE {trig_table} (key TEXT PRIMARY KEY, data BLOB, created_at INTEGER NOT NULL,
                 updated_at INTEGER NOT NULL, expires_at INTEGER, version INTEGER NOT NULL DEFAULT 0)"
            ),
            format!(
                "INSERT INTO {trig_table} (key, data, created_at, updated_at) VALUES ('f', x'00', 0, 0)"
            ),
            format!(
                "CREATE TRIGGER audit AFTER DELETE ON {trig_table} BEGIN INSERT INTO missing_audit VALUES (old.key); END"
            ),
            // ...or on a trigger raising the message a missing table gives.
            format!(
                "CREATE TABLE {raise_table} (key TEXT PRIMARY KEY, data BLOB, created_at INTEGER NOT NULL,
                 updated_at INTEGER NOT NULL, expires_at INTEGER, version INTEGER NOT NULL DEFAULT 0)"
            ),
            format!(
                "INSERT INTO {raise_table} (key, data, created_at, updated_at) VALUES ('f', x'00', 0, 0)"
            ),
            format!(
                "CREATE TRIGGER spoof BEFORE DELETE ON {raise_table} BEGIN SELECT RAISE(ABORT, 'no such table: {raise_table}'); END"
            ),
        ] {
            sqlx::query(&sql).execute(&pool).await.unwrap();
        }
        let mut tables = HashSet::from(["blobs".to_string()]);
        let cache = Cache::new(16);
        let hcache = Cache::new(16);

        let (expire_tx, expire_rx) = oneshot::channel();
        let (delete_tx, delete_rx) = oneshot::channel();
        let (missing_tx, missing_rx) = oneshot::channel();
        let (trigger_tx, trigger_rx) = oneshot::channel();
        let (raise_tx, raise_rx) = oneshot::channel();
        let mut batch = VecDeque::from([
            ShardWriteOperation::HExpire {
                namespace: "ns".to_string(),
                key: "e".to_string(),
                expires_at: Utc::now().timestamp() + 60,
                condition: None,
                responder: expire_tx,
            },
            ShardWriteOperation::HDelete {
                namespace: "ns".to_string(),
                key: "d".to_string(),
                responder: delete_tx,
            },
            // A namespace with no table at all is still a plain miss.
            ShardWriteOperation::HExpire {
                namespace: "other".to_string(),
                key: "e".to_string(),
                expires_at: Utc::now().timestamp() + 60,
                condition: None,
                responder: missing_tx,
            },
            ShardWriteOperation::HDelete {
                namespace: "trig".to_string(),
                key: "f".to_string(),
                responder: trigger_tx,
            },
            ShardWriteOperation::HDelete {
                namespace: "raise".to_string(),
                key: "f".to_string(),
                responder: raise_tx,
            },
        ]);
        process_batch(
            0,
            &pool,
            &mut batch,
            &mut tables,
            &cache,
            &hcache,
            &ShardMetrics::new(0),
        )
        .await;

        assert_eq!(expire_rx.await.unwrap(), Ok(1));
        assert_eq!(delete_rx.await.unwrap(), Ok(()));
        assert_eq!(missing_rx.await.unwrap(), Ok(-2));
        let trigger_result = trigger_rx.await.unwrap();
        assert!(
            trigger_result.is_err(),
            "HDEL hid another table's error: {trigger_result:?}"
        );
        let raise_result = raise_rx.await.unwrap();
        assert!(
            raise_result.is_err(),
            "HDEL hid a trigger's error: {raise_result:?}"
        );
        let keys: Vec<(String,)> = sqlx::query_as(&format!("SELECT key FROM {ns_table}"))
            .fetch_all(&pool)
            .await
            .unwrap();
        assert_eq!(keys, [("e".to_string(),)], "HDEL left its field in place");
    }

    async fn create_test_pool() -> (TempDir, SqlitePool) {
        let temp_dir = TempDir::new().expect("failed to create temp dir");
        let db_path = temp_dir.path().join("shard_0.db");

        let connect_options =
            SqliteConnectOptions::from_str(&format!("sqlite:{}", db_path.display()))
                .expect("failed to parse sqlite connection string")
                .create_if_missing(true)
                .journal_mode(SqliteJournalMode::Wal)
                .busy_timeout(std::time::Duration::from_secs(5));

        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect_with(connect_options)
            .await
            .expect("failed to create sqlite pool");

        sqlx::query("PRAGMA auto_vacuum = INCREMENTAL")
            .execute(&pool)
            .await
            .expect("failed to enable incremental auto_vacuum");

        sqlx::query("VACUUM")
            .execute(&pool)
            .await
            .expect("failed to apply auto_vacuum pragma");

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
        .execute(&pool)
        .await
        .expect("failed to create blobs table");

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_expires_at ON blobs(expires_at) WHERE expires_at IS NOT NULL",
        )
        .execute(&pool)
        .await
        .expect("failed to create expires index");

        (temp_dir, pool)
    }

    #[tokio::test]
    async fn delete_expired_removes_only_expired_rows_across_chunks() {
        let (_dir, pool) = create_test_pool().await;
        let now = 1_000;
        let chunk_size = 10;
        let expired = chunk_size * 2 + 5;

        // `expired` rows already expired, plus one live row and one with no expiry
        sqlx::query(
            "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < ?)
             INSERT INTO blobs (key, data, created_at, updated_at, expires_at)
             SELECT 'k' || i, x'00', 0, 0, ? FROM n",
        )
        .bind(expired)
        .bind(now)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO blobs (key, data, created_at, updated_at, expires_at)
             VALUES ('live', x'00', 0, 0, ?), ('forever', x'00', 0, 0, NULL)",
        )
        .bind(now + 1)
        .execute(&pool)
        .await
        .unwrap();

        let deleted = delete_expired(&pool, "blobs", now, chunk_size, &Counter::noop())
            .await
            .unwrap();
        assert_eq!(deleted, u64::from(expired));

        let keys: Vec<(String,)> = sqlx::query_as("SELECT key FROM blobs ORDER BY key")
            .fetch_all(&pool)
            .await
            .unwrap();
        assert_eq!(keys, vec![("forever".into(),), ("live".into(),)]);
    }

    #[test]
    fn compute_incremental_vacuum_pages_respects_budget_and_freelist() {
        assert_eq!(compute_incremental_vacuum_pages(100, 4096, 0), 0);
        assert_eq!(compute_incremental_vacuum_pages(100, 4096, 4095), 0);
        assert_eq!(compute_incremental_vacuum_pages(100, 4096, 4096), 1);
        assert_eq!(compute_incremental_vacuum_pages(100, 4096, 4096 * 200), 100);
        assert_eq!(compute_incremental_vacuum_pages(100, 0, 4096 * 200), 0);
    }

    #[test]
    fn estimate_reclaimed_bytes_uses_known_page_size() {
        let before = Some(VacuumStats {
            page_size: 4096,
            page_count: 100,
            freelist_count: 20,
        });
        let after = Some(VacuumStats {
            page_size: 4096,
            page_count: 95,
            freelist_count: 10,
        });

        assert_eq!(
            estimate_reclaimed_bytes(Some(4), before, after),
            Some(16384)
        );
        assert_eq!(estimate_reclaimed_bytes(None, before, after), None);

        let before_missing = None;
        assert_eq!(
            estimate_reclaimed_bytes(Some(4), before_missing, after),
            Some(16384)
        );
        assert_eq!(estimate_reclaimed_bytes(Some(4), None, None), None);
    }

    #[test]
    fn classify_vacuum_error_kinds_tracks_busy_and_generic_errors() {
        let busy_only = vec!["wal checkpoint failed: database is locked".to_string()];
        assert_eq!(classify_vacuum_error_kinds(&busy_only), (true, false));

        let generic_only = vec!["VACUUM failed: disk I/O error".to_string()];
        assert_eq!(classify_vacuum_error_kinds(&generic_only), (false, true));

        let mixed = vec![
            "incremental_vacuum failed: SQLITE_BUSY".to_string(),
            "failed to read PRAGMA freelist_count".to_string(),
        ];
        assert_eq!(classify_vacuum_error_kinds(&mixed), (true, true));
    }

    fn test_channel(capacity: usize) -> (ShardSender, mpsc::Receiver<ShardWriteOperation>) {
        let (sender, receiver) = mpsc::channel(capacity);
        (ShardSender::new(sender, ShardMetrics::new(0)), receiver)
    }

    async fn queue_set(
        cache: &Cache<String, Pending>,
        sender: &ShardSender,
        key: &str,
        data: Bytes,
    ) -> u64 {
        let token = next_pending_token();
        let op = ShardWriteOperation::SetAsync {
            key: key.to_string(),
            data: data.clone(),
            expires_at: None,
            token,
        };
        queue_pending(
            cache,
            sender,
            key.to_string(),
            Pending::Value {
                data,
                token,
                expires_at: None,
            },
            op,
        )
        .await
        .unwrap();
        token
    }

    async fn queue_del(cache: &Cache<String, Pending>, sender: &ShardSender, key: &str) -> u64 {
        let token = next_pending_token();
        let op = ShardWriteOperation::DeleteAsync {
            key: key.to_string(),
            token,
        };
        queue_pending(cache, sender, key.to_string(), Pending::Deleted(token), op)
            .await
            .unwrap();
        token
    }

    #[tokio::test]
    async fn clearing_a_large_async_value_frees_it() {
        let cache: Cache<String, Pending> = Cache::builder().build();
        let hcache: Cache<String, Pending> = Cache::builder().build();
        let (sender, mut receiver) = test_channel(16);

        let value = Bytes::from(vec![1u8; FREE_NOW_BYTES]);
        queue_set(&cache, &sender, "k", value.clone()).await;
        let batch = VecDeque::from([receiver.try_recv().unwrap()]);
        clear_pending_entries(&batch, &cache, &hcache).await;
        drop(batch);

        // Only the test's own handle is left: the cache let go of its copy.
        // moka frees a removed value through crossbeam-epoch, once every
        // thread in the process has moved past the epoch it was removed in, so
        // other tests running in parallel can delay it. Activity on another
        // cache lets that reclamation run without housekeeping `cache` itself,
        // which is what clear_pending_entries must do.
        let other: Cache<u32, ()> = Cache::new(16);
        for i in 0..1000 {
            if value.is_unique() {
                break;
            }
            other.insert(i % 16, ()).await;
            tokio::task::yield_now().await;
        }
        assert!(value.is_unique(), "cache still holds the value");
    }

    #[tokio::test]
    async fn processed_async_op_only_clears_its_own_cache_entry() {
        let cache: Cache<String, Pending> = Cache::new(16);
        let hcache: Cache<String, Pending> = Cache::new(16);
        let (sender, mut receiver) = test_channel(16);

        let v2 = Bytes::from(vec![2u8; 16]);
        queue_set(&cache, &sender, "k", Bytes::from(vec![1u8; 16])).await;
        queue_set(&cache, &sender, "k", v2.clone()).await;
        queue_del(&cache, &sender, "d").await;
        queue_set(&cache, &sender, "d", Bytes::from_static(b"new")).await;
        queue_del(&cache, &sender, "x").await;
        let second = queue_del(&cache, &sender, "x").await;
        let token = next_pending_token();
        let op = ShardWriteOperation::HDeleteAsync {
            namespace: "ns".to_string(),
            key: "f".to_string(),
            token,
        };
        queue_pending(
            &hcache,
            &sender,
            "ns:f".to_string(),
            Pending::Deleted(token),
            op,
        )
        .await
        .unwrap();

        let mut ops = Vec::new();
        while let Ok(op) = receiver.try_recv() {
            ops.push(op);
        }
        let mut ops = ops.into_iter();
        let mut first_batch = VecDeque::new();
        let mut second_batch = VecDeque::new();
        for _ in 0..3 {
            first_batch.push_back(ops.next().unwrap());
            second_batch.push_back(ops.next().unwrap());
        }
        second_batch.push_back(ops.next().unwrap());

        // Each key's first op is processed while a newer op on it is queued.
        clear_pending_entries(&first_batch, &cache, &hcache).await;
        assert!(
            matches!(cache.get("k").await, Some(Pending::Value { data: v, .. }) if v == v2),
            "newer SET must stay visible"
        );
        assert!(
            matches!(cache.get("d").await, Some(Pending::Value { .. })),
            "SET after DEL must stay visible"
        );
        assert!(
            matches!(cache.get("x").await, Some(Pending::Deleted(t)) if t == second),
            "newer DEL must stay visible"
        );

        // Their own ops clear them.
        clear_pending_entries(&second_batch, &cache, &hcache).await;
        assert_eq!(cache.entry_count() + hcache.entry_count(), 0);
        for key in ["k", "d", "x"] {
            assert!(!cache.contains_key(key));
        }
        assert!(!hcache.contains_key("ns:f"));

        // With the writer gone, nothing is recorded.
        drop(receiver);
        let op = ShardWriteOperation::DeleteAsync {
            key: "gone".to_string(),
            token: 0,
        };
        let result = queue_pending(&cache, &sender, "gone".to_string(), Pending::Deleted(0), op);
        assert!(result.await.is_err());
        assert!(!cache.contains_key("gone"));
    }

    #[tokio::test]
    async fn empty_and_aliased_async_values_only_clear_their_own_cache_entry() {
        let (_temp_dir, pool) = create_test_pool().await;
        let cache: Cache<String, Pending> = Cache::new(16);
        let hcache: Cache<String, Pending> = Cache::new(16);
        let (sender, mut receiver) = test_channel(16);
        let shard_metrics = ShardMetrics::new(0);
        let mut tables = HashSet::new();

        // SET k "", SET k "a", SET k "" using empty Bytes (shared static pointer).
        queue_set(&cache, &sender, "k", Bytes::new()).await;
        queue_set(&cache, &sender, "k", Bytes::from_static(b"a")).await;
        let last_k_token = queue_set(&cache, &sender, "k", Bytes::new()).await;

        // Same aliased non-empty Bytes across an intervening write.
        let shared = Bytes::from_static(b"same");
        queue_set(&cache, &sender, "s", shared.clone()).await;
        queue_set(&cache, &sender, "s", Bytes::from_static(b"mid")).await;
        let last_s_token = queue_set(&cache, &sender, "s", shared.clone()).await;

        // HSET and HSETEX with empty values in inflight_hcache.
        let h0 = next_pending_token();
        queue_pending(
            &hcache,
            &sender,
            "ns:f".to_string(),
            Pending::Value {
                data: Bytes::new(),
                token: h0,
                expires_at: None,
            },
            ShardWriteOperation::HSetAsync {
                namespace: "ns".to_string(),
                key: "f".to_string(),
                data: Bytes::new(),
                token: h0,
            },
        )
        .await
        .unwrap();
        let h1 = next_pending_token();
        queue_pending(
            &hcache,
            &sender,
            "ns:f".to_string(),
            Pending::Value {
                data: Bytes::from_static(b"a"),
                token: h1,
                expires_at: None,
            },
            ShardWriteOperation::HSetAsync {
                namespace: "ns".to_string(),
                key: "f".to_string(),
                data: Bytes::from_static(b"a"),
                token: h1,
            },
        )
        .await
        .unwrap();
        let last_h_token = next_pending_token();
        queue_pending(
            &hcache,
            &sender,
            "ns:f".to_string(),
            Pending::Value {
                data: Bytes::new(),
                token: last_h_token,
                expires_at: None,
            },
            ShardWriteOperation::HSetExAsync {
                namespace: "ns".to_string(),
                key: "f".to_string(),
                data: Bytes::new(),
                expires_at: Utc::now().timestamp() + 60,
                token: last_h_token,
            },
        )
        .await
        .unwrap();

        let mut ops = Vec::new();
        while let Ok(op) = receiver.try_recv() {
            ops.push(op);
        }
        // First batch commits the first two ops for each key (leaving the DB
        // at "a" / "mid"), while the third op for each key stays queued.
        let mut first_batch = VecDeque::from([
            ops.remove(0),
            ops.remove(0),
            ops.remove(1),
            ops.remove(1),
            ops.remove(2),
            ops.remove(2),
        ]);
        let mut second_batch = VecDeque::from(ops);

        process_batch(
            0,
            &pool,
            &mut first_batch,
            &mut tables,
            &cache,
            &hcache,
            &shard_metrics,
        )
        .await;

        // Newer entries must remain in the inflight caches so reads don't fall
        // through to the stale "a" / "mid" values in SQLite.
        assert!(
            matches!(
                cache.get("k").await,
                Some(Pending::Value { data, token, .. }) if data.is_empty() && token == last_k_token
            ),
            "newer empty SET was cleared by older empty SET"
        );
        assert!(
            matches!(
                cache.get("s").await,
                Some(Pending::Value { data, token, .. }) if data == shared && token == last_s_token
            ),
            "newer aliased SET was cleared by older aliased SET"
        );
        assert!(
            matches!(
                hcache.get("ns:f").await,
                Some(Pending::Value { data, token, .. }) if data.is_empty() && token == last_h_token
            ),
            "newer empty HSETEX was cleared by older empty HSET"
        );

        // Once the second batch commits, the caches are cleared and SQLite has
        // the final values.
        process_batch(
            0,
            &pool,
            &mut second_batch,
            &mut tables,
            &cache,
            &hcache,
            &shard_metrics,
        )
        .await;
        assert!(!cache.contains_key("k"));
        assert!(!cache.contains_key("s"));
        assert!(!hcache.contains_key("ns:f"));

        let (k_val,): (Vec<u8>,) = sqlx::query_as("SELECT data FROM blobs WHERE key = 'k'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert!(k_val.is_empty());
        let (s_val,): (Vec<u8>,) = sqlx::query_as("SELECT data FROM blobs WHERE key = 's'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(s_val, b"same");
        let ns_table = namespace_table("ns");
        let (h_val,): (Vec<u8>,) =
            sqlx::query_as(&format!("SELECT data FROM {ns_table} WHERE key = 'f'"))
                .fetch_one(&pool)
                .await
                .unwrap();
        assert!(h_val.is_empty());
    }

    #[tokio::test]
    async fn failed_batch_clears_its_cache_entries() {
        let (_temp_dir, pool) = create_test_pool().await;
        pool.close().await;
        let cache: Cache<String, Pending> = Cache::new(16);
        let (sender, mut receiver) = test_channel(16);
        queue_set(&cache, &sender, "k", Bytes::from_static(b"v")).await;
        queue_del(&cache, &sender, "d").await;
        let mut batch = VecDeque::new();
        while let Ok(op) = receiver.try_recv() {
            batch.push_back(op);
        }

        // The transaction can't start: the ops are dropped, so their entries
        // must go too, or reads would serve a write or delete that never lands.
        process_batch(
            0,
            &pool,
            &mut batch,
            &mut HashSet::new(),
            &cache,
            &Cache::new(16),
            &ShardMetrics::new(0),
        )
        .await;
        assert!(!cache.contains_key("k"));
        assert!(!cache.contains_key("d"));
    }

    #[tokio::test]
    async fn application_metrics_count_async_write_outcomes_once() {
        use crate::metrics::rendered_value;
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let _guard = metrics::set_default_local_recorder(&recorder);
        let value = |series: &str| rendered_value(&recorder.handle().render(), series);

        let (_temp_dir, pool) = create_test_pool().await;
        // Force one statement in the batch to fail while the transaction still commits.
        sqlx::query(
            "CREATE TRIGGER fail_bad BEFORE INSERT ON blobs WHEN NEW.key = 'bad' \
             BEGIN SELECT RAISE(ABORT, 'forced failure'); END",
        )
        .execute(&pool)
        .await
        .unwrap();
        let cache: Cache<String, Pending> = Cache::new(16);
        let hcache: Cache<String, Pending> = Cache::new(16);
        let (sender, mut receiver) = test_channel(16);
        let shard_metrics = ShardMetrics::new(0);

        // Two writes to one key are two pending ops, plus one whose statement fails.
        queue_set(&cache, &sender, "k", Bytes::from_static(b"1")).await;
        queue_set(&cache, &sender, "k", Bytes::from_static(b"2")).await;
        queue_set(&cache, &sender, "bad", Bytes::from_static(b"v")).await;
        assert_eq!(sender.queue_depth(), 3);
        assert_eq!(value(r#"blobasaur_async_writes_pending{shard="0"}"#), 3.0);
        assert_eq!(
            value(r#"blobasaur_write_enqueue_wait_seconds_count{shard="0"}"#),
            3.0
        );

        let mut batch = VecDeque::new();
        while let Ok(op) = receiver.try_recv() {
            batch.push_back(op);
        }
        assert_eq!(sender.queue_depth(), 0);
        let tables = &mut HashSet::new();
        process_batch(
            0,
            &pool,
            &mut batch,
            tables,
            &cache,
            &hcache,
            &shard_metrics,
        )
        .await;

        // The statement failed but the transaction committed.
        let output = recorder.handle().render();
        assert_eq!(
            rendered_value(&output, r#"blobasaur_async_writes_pending{shard="0"}"#),
            0.0
        );
        assert_eq!(
            rendered_value(
                &output,
                r#"blobasaur_async_write_failures_total{shard="0"}"#
            ),
            1.0
        );
        assert_eq!(
            rendered_value(
                &output,
                r#"blobasaur_sqlite_errors_total{shard="0",stage="execute",kind="other"}"#
            ),
            1.0
        );
        assert_eq!(
            rendered_value(
                &output,
                r#"blobasaur_sqlite_transaction_duration_seconds_count{shard="0",result="ok"}"#
            ),
            1.0
        );
        assert_eq!(
            rendered_value(
                &output,
                r#"blobasaur_sqlite_pool_acquire_seconds_count{shard="0",pool="write",result="ok"}"#
            ),
            1.0
        );

        // No connection: the whole batch fails, counted once per op.
        queue_set(&cache, &sender, "k", Bytes::from_static(b"3")).await;
        let mut batch = VecDeque::from([receiver.try_recv().unwrap()]);
        pool.close().await;
        process_batch(
            0,
            &pool,
            &mut batch,
            tables,
            &cache,
            &hcache,
            &shard_metrics,
        )
        .await;
        let output = recorder.handle().render();
        assert_eq!(
            rendered_value(&output, r#"blobasaur_async_writes_pending{shard="0"}"#),
            0.0
        );
        assert_eq!(
            rendered_value(
                &output,
                r#"blobasaur_async_write_failures_total{shard="0"}"#
            ),
            2.0
        );
        assert_eq!(
            rendered_value(
                &output,
                r#"blobasaur_sqlite_errors_total{shard="0",stage="acquire",kind="other"}"#
            ),
            1.0
        );

        // Scrapes don't change counters.
        assert_eq!(
            value(r#"blobasaur_async_write_failures_total{shard="0"}"#),
            2.0
        );
    }

    #[tokio::test]
    async fn application_metrics_count_cleanup_deletes() {
        let counter_value = {
            let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
            let _guard = metrics::set_default_local_recorder(&recorder);
            let (_dir, pool) = create_test_pool().await;
            sqlx::query(
                "INSERT INTO blobs (key, data, created_at, updated_at, expires_at)
                 VALUES ('a', x'00', 0, 0, 1), ('b', x'00', 0, 0, 1), ('c', x'00', 0, 0, NULL)",
            )
            .execute(&pool)
            .await
            .unwrap();
            let counter = ShardMetrics::new(0).cleanup_deleted_keys_total;
            // Chunk size 1: the count accumulates per chunk.
            assert_eq!(
                delete_expired(&pool, "blobs", 10, 1, &counter)
                    .await
                    .unwrap(),
                2
            );
            let missing = delete_expired(&pool, "blobs_missing", 10, 1, &counter).await;
            assert!(missing.is_err());
            crate::metrics::rendered_value(
                &recorder.handle().render(),
                r#"blobasaur_cleanup_deleted_keys_total{shard="0"}"#,
            )
        };
        assert_eq!(counter_value, 2.0);
    }

    #[tokio::test]
    async fn sync_write_reports_its_own_statement_failure() {
        let (_temp_dir, pool) = create_test_pool().await;
        // The INSERT of "blocked" fails, but its transaction still commits.
        sqlx::query(
            "CREATE TRIGGER reject_blocked BEFORE INSERT ON blobs WHEN NEW.key = 'blocked' \
             BEGIN SELECT RAISE(ABORT, 'rejected'); END",
        )
        .execute(&pool)
        .await
        .unwrap();

        let (sender, receiver) = mpsc::channel(8);
        let writer = tokio::spawn(shard_writer_task(ShardWriter {
            shard_id: 0,
            pool: pool.clone(),
            receiver,
            batch_size: 8,
            batch_timeout_ms: 0,
            inflight_cache: Cache::new(16),
            inflight_hcache: Cache::new(16),
            metrics: Metrics::new(),
            shutdown: CancellationToken::new(),
        }));

        let set = |key: &str| {
            let (responder, rx) = oneshot::channel();
            let op = ShardWriteOperation::Set {
                key: key.to_string(),
                data: Bytes::from_static(b"v"),
                expires_at: None,
                responder,
            };
            (op, rx)
        };

        let (op, rx) = set("blocked");
        sender.send(op).await.unwrap();
        let result = rx.await.unwrap();
        assert!(
            result.as_ref().is_err_and(|e| e.contains("rejected")),
            "a write that wasn't persisted must not be acknowledged: {result:?}"
        );

        let (op, rx) = set("after");
        sender.send(op).await.unwrap();
        assert_eq!(rx.await.unwrap(), Ok(()));

        drop(sender);
        writer.await.unwrap();
        let keys: Vec<(String,)> = sqlx::query_as("SELECT key FROM blobs")
            .fetch_all(&pool)
            .await
            .unwrap();
        assert_eq!(keys, vec![("after".to_string(),)]);
    }

    #[tokio::test]
    async fn namespace_table_is_recreated_after_its_creating_batch_fails_to_commit() {
        let (_temp_dir, pool) = create_test_pool().await;
        // Inserting "poison" adds a row with a deferred foreign key to a missing
        // parent, so its batch fails only at COMMIT, after the batch's
        // CREATE TABLE for the namespace has run.
        sqlx::query(
            "CREATE TABLE fk_child (parent_id INTEGER REFERENCES fk_parent(id) \
             DEFERRABLE INITIALLY DEFERRED)",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query("CREATE TABLE fk_parent (id INTEGER PRIMARY KEY)")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "CREATE TRIGGER poison AFTER INSERT ON blobs WHEN NEW.key = 'poison' \
             BEGIN INSERT INTO fk_child VALUES (1); END",
        )
        .execute(&pool)
        .await
        .unwrap();

        let hset = || {
            let (responder, rx) = oneshot::channel();
            let op = ShardWriteOperation::HSet {
                namespace: "ns".to_string(),
                key: "k".to_string(),
                data: Bytes::from_static(b"v"),
                responder,
            };
            (op, rx)
        };
        let (poison_responder, poison_rx) = oneshot::channel();
        let poison = ShardWriteOperation::Set {
            key: "poison".to_string(),
            data: Bytes::from_static(b"v"),
            expires_at: None,
            responder: poison_responder,
        };

        // Queue both before the writer starts so they share one batch.
        let (sender, receiver) = mpsc::channel(8);
        let (first_hset, first_rx) = hset();
        sender.send(first_hset).await.unwrap();
        sender.send(poison).await.unwrap();
        let writer = tokio::spawn(shard_writer_task(ShardWriter {
            shard_id: 0,
            pool: pool.clone(),
            receiver,
            batch_size: 8,
            batch_timeout_ms: 0,
            inflight_cache: Cache::new(16),
            inflight_hcache: Cache::new(16),
            metrics: Metrics::new(),
            shutdown: CancellationToken::new(),
        }));

        let first = first_rx.await.unwrap();
        assert!(
            first.as_ref().is_err_and(|e| e.contains("FOREIGN KEY")),
            "the batch should fail at commit: {first:?}"
        );
        assert!(poison_rx.await.unwrap().is_err());

        // The rolled-back table must be created again, not assumed to exist.
        let (second_hset, second_rx) = hset();
        sender.send(second_hset).await.unwrap();
        assert_eq!(second_rx.await.unwrap(), Ok(()));

        drop(sender);
        writer.await.unwrap();
        let ns_table = namespace_table("ns");
        let rows: Vec<(String,)> = sqlx::query_as(&format!("SELECT key FROM {ns_table}"))
            .fetch_all(&pool)
            .await
            .unwrap();
        assert_eq!(rows, vec![("k".to_string(),)]);
        let meta: Vec<(String, String)> =
            sqlx::query_as("SELECT namespace, table_name FROM blob_namespaces")
                .fetch_all(&pool)
                .await
                .unwrap();
        assert_eq!(meta, vec![("ns".to_string(), ns_table)]);
    }

    #[tokio::test]
    async fn async_write_waits_out_an_outside_write_lock() {
        let (_temp_dir, pool) = create_test_pool().await;
        // Another connection, e.g. a sqlite3 session, holds the write lock.
        let mut lock = pool.acquire().await.unwrap();
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut *lock)
            .await
            .unwrap();

        let (sender, receiver) = mpsc::channel(8);
        let writer = tokio::spawn(shard_writer_task(ShardWriter {
            shard_id: 0,
            pool: pool.clone(),
            receiver,
            batch_size: 8,
            batch_timeout_ms: 0,
            inflight_cache: Cache::new(16),
            inflight_hcache: Cache::new(16),
            metrics: Metrics::new(),
            shutdown: CancellationToken::new(),
        }));

        // Already acked to the client, so it must commit once the lock is gone.
        sender
            .send(ShardWriteOperation::SetAsync {
                key: "k".to_string(),
                data: Bytes::from_static(b"v"),
                expires_at: None,
                token: next_pending_token(),
            })
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        sqlx::query("ROLLBACK").execute(&mut *lock).await.unwrap();
        drop(lock);

        drop(sender);
        writer.await.unwrap();
        let keys: Vec<(String,)> = sqlx::query_as("SELECT key FROM blobs")
            .fetch_all(&pool)
            .await
            .unwrap();
        assert_eq!(keys, vec![("k".to_string(),)]);
    }

    fn spawn_writer(
        pool: &SqlitePool,
        batch_size: usize,
        batch_timeout_ms: u64,
        shutdown: CancellationToken,
    ) -> (
        mpsc::Sender<ShardWriteOperation>,
        tokio::task::JoinHandle<()>,
    ) {
        let (sender, receiver) = mpsc::channel(64);
        let writer = tokio::spawn(shard_writer_task(ShardWriter {
            shard_id: 0,
            pool: pool.clone(),
            receiver,
            batch_size,
            batch_timeout_ms,
            inflight_cache: Cache::new(16),
            inflight_hcache: Cache::new(16),
            metrics: Metrics::new(),
            shutdown,
        }));
        (sender, writer)
    }

    fn sync_set(key: &str) -> (ShardWriteOperation, oneshot::Receiver<Result<(), String>>) {
        let (responder, rx) = oneshot::channel();
        let op = ShardWriteOperation::Set {
            key: key.to_string(),
            data: Bytes::from_static(b"v"),
            expires_at: None,
            responder,
        };
        (op, rx)
    }

    #[tokio::test]
    async fn writer_waits_batch_timeout_for_more_ops_until_the_batch_is_full() {
        let (_temp_dir, pool) = create_test_pool().await;
        let (sender, writer) = spawn_writer(&pool, 2, 60_000, CancellationToken::new());

        let (op, mut first) = sync_set("a");
        sender.send(op).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            first.try_recv().is_err(),
            "the writer should wait for more ops"
        );

        // The batch is full: it commits without waiting out the timeout.
        let (op, second) = sync_set("b");
        sender.send(op).await.unwrap();
        let results = timeout(Duration::from_secs(5), async {
            (first.await.unwrap(), second.await.unwrap())
        })
        .await
        .expect("a full batch should commit right away");
        assert_eq!(results, (Ok(()), Ok(())));

        drop(sender);
        writer.await.unwrap();
    }

    #[tokio::test]
    async fn writer_commits_when_batch_timeout_passes() {
        let (_temp_dir, pool) = create_test_pool().await;
        let (sender, writer) = spawn_writer(&pool, 8, 200, CancellationToken::new());

        let start = std::time::Instant::now();
        let (op, rx) = sync_set("a");
        sender.send(op).await.unwrap();
        assert_eq!(rx.await.unwrap(), Ok(()));
        let elapsed = start.elapsed();
        assert!(
            elapsed >= Duration::from_millis(200) && elapsed < Duration::from_secs(5),
            "the batch should commit once the timeout passes: {elapsed:?}"
        );

        drop(sender);
        writer.await.unwrap();
    }

    #[tokio::test]
    async fn shutdown_ends_the_batch_timeout_wait() {
        let (_temp_dir, pool) = create_test_pool().await;
        let shutdown = CancellationToken::new();
        let (sender, writer) = spawn_writer(&pool, 8, 60_000, shutdown.clone());

        let (op, rx) = sync_set("a");
        sender.send(op).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        shutdown.cancel();
        let result = timeout(Duration::from_secs(5), rx)
            .await
            .expect("shutdown should not wait out the batch timeout");
        assert_eq!(result.unwrap(), Ok(()));
        timeout(Duration::from_secs(5), writer)
            .await
            .expect("writer did not stop")
            .unwrap();
    }

    #[tokio::test]
    async fn writer_drains_queued_ops_on_shutdown() {
        let (_temp_dir, pool) = create_test_pool().await;
        let (sender, receiver) = mpsc::channel(64);
        for i in 0..50 {
            sender
                .send(ShardWriteOperation::SetAsync {
                    key: format!("key-{i}"),
                    data: Bytes::from_static(b"v"),
                    expires_at: None,
                    token: next_pending_token(),
                })
                .await
                .unwrap();
        }

        // Shutdown already requested and the sender is still alive: the
        // writer must commit the queue rather than wait for more ops.
        let shutdown = CancellationToken::new();
        shutdown.cancel();
        timeout(
            Duration::from_secs(5),
            shard_writer_task(ShardWriter {
                shard_id: 0,
                pool: pool.clone(),
                receiver,
                batch_size: 16,
                batch_timeout_ms: 60_000,
                inflight_cache: Cache::new(128),
                inflight_hcache: Cache::new(128),
                metrics: Metrics::new(),
                shutdown,
            }),
        )
        .await
        .expect("writer did not stop");

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM blobs")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 50);
        assert!(
            sender.is_closed(),
            "writer should reject sends after shutdown"
        );
    }

    #[tokio::test]
    async fn vacuum_operation_is_an_ordering_barrier_in_writer_queue() {
        let (_temp_dir, pool) = create_test_pool().await;

        // Seed a large row so deleting it produces freelist pages.
        let now = Utc::now().timestamp();
        let large_payload = vec![7_u8; 256 * 1024];
        sqlx::query(
            "INSERT INTO blobs (key, data, created_at, updated_at, expires_at, version) VALUES (?, ?, ?, ?, NULL, 0)",
        )
        .bind("large-key")
        .bind(&large_payload)
        .bind(now)
        .bind(now)
        .execute(&pool)
        .await
        .expect("failed to insert seed row");

        let (sender, receiver) = mpsc::channel(64);
        let writer_handle = tokio::spawn(shard_writer_task(ShardWriter {
            shard_id: 0,
            pool: pool.clone(),
            receiver,
            batch_size: 64,
            batch_timeout_ms: 10,
            inflight_cache: Cache::new(128),
            inflight_hcache: Cache::new(128),
            metrics: Metrics::new(),
            shutdown: CancellationToken::new(),
        }));

        let (delete_tx, delete_rx) = oneshot::channel();
        sender
            .send(ShardWriteOperation::Delete {
                key: "large-key".to_string(),
                responder: delete_tx,
            })
            .await
            .expect("failed to queue delete");

        let budget_bytes = 32 * 1024 * 1024;
        let (vacuum_tx, vacuum_rx) = oneshot::channel();
        sender
            .send(ShardWriteOperation::Vacuum {
                mode: VacuumMode::Incremental,
                budget_bytes,
                dry_run: false,
                responder: vacuum_tx,
            })
            .await
            .expect("failed to queue vacuum");

        let (set_tx, set_rx) = oneshot::channel();
        sender
            .send(ShardWriteOperation::Set {
                key: "after-vacuum".to_string(),
                data: Bytes::from_static(b"ok"),
                expires_at: None,
                responder: set_tx,
            })
            .await
            .expect("failed to queue post-vacuum write");

        assert_eq!(
            delete_rx.await.expect("delete responder dropped"),
            Ok(()),
            "delete before vacuum should succeed"
        );

        let vacuum_result = vacuum_rx.await.expect("vacuum responder dropped");
        assert!(
            vacuum_result.errors.is_empty(),
            "vacuum should complete without errors: {:?}",
            vacuum_result.errors
        );
        assert_eq!(vacuum_result.mode, VacuumMode::Incremental);

        let before = vacuum_result
            .before
            .expect("vacuum should include pre-stats");
        let after = vacuum_result
            .after
            .expect("vacuum should include post-stats");

        assert!(
            before.freelist_count > 0,
            "delete queued before vacuum should be reflected in pre-vacuum freelist stats"
        );
        assert!(
            after.freelist_count <= before.freelist_count,
            "vacuum should not increase freelist_count"
        );
        assert_eq!(
            vacuum_result.incremental_pages_requested,
            Some(compute_incremental_vacuum_pages(
                before.freelist_count,
                before.page_size,
                budget_bytes
            ))
        );

        assert_eq!(
            set_rx.await.expect("set responder dropped"),
            Ok(()),
            "write queued after vacuum should resume and succeed"
        );

        let post_value = sqlx::query_scalar::<_, Vec<u8>>("SELECT data FROM blobs WHERE key = ?")
            .bind("after-vacuum")
            .fetch_optional(&pool)
            .await
            .expect("failed to read post-vacuum key");
        assert_eq!(post_value, Some(b"ok".to_vec()));

        drop(sender);
        tokio::time::timeout(Duration::from_secs(2), writer_handle)
            .await
            .expect("writer task did not stop")
            .expect("writer task failed");
    }

    /// Pool matching production writer settings: WAL, a single connection.
    async fn create_writer_like_pool(
        db_path: &std::path::Path,
        busy_timeout_ms: u64,
    ) -> SqlitePool {
        let connect_options =
            SqliteConnectOptions::from_str(&format!("sqlite:{}", db_path.display()))
                .expect("failed to parse sqlite connection string")
                .create_if_missing(true)
                .journal_mode(SqliteJournalMode::Wal)
                .busy_timeout(std::time::Duration::from_millis(busy_timeout_ms))
                .pragma("auto_vacuum", "INCREMENTAL");

        SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(connect_options)
            .await
            .expect("failed to create sqlite pool")
    }

    #[tokio::test]
    async fn full_vacuum_truncates_wal_and_reports_page_delta() {
        let temp_dir = TempDir::new().expect("failed to create temp dir");
        let db_path = temp_dir.path().join("shard_0.db");
        let pool = create_writer_like_pool(&db_path, 5000).await;

        sqlx::query("CREATE TABLE t (k INTEGER PRIMARY KEY, d BLOB)")
            .execute(&pool)
            .await
            .expect("failed to create table");
        for k in 0..64_i64 {
            sqlx::query("INSERT INTO t (k, d) VALUES (?, ?)")
                .bind(k)
                .bind(vec![1_u8; 64 * 1024])
                .execute(&pool)
                .await
                .expect("failed to insert row");
        }
        sqlx::query("DELETE FROM t WHERE k % 2 = 0")
            .execute(&pool)
            .await
            .expect("failed to delete rows");

        let result = execute_vacuum(0, &pool, VacuumMode::Full, 1, false, &Metrics::new()).await;
        assert!(
            result.errors.is_empty(),
            "unexpected errors: {:?}",
            result.errors
        );

        let before = result.before.expect("pre-stats");
        let after = result.after.expect("post-stats");
        assert!(
            after.page_count < before.page_count,
            "full vacuum should shrink the DB"
        );
        assert_eq!(
            result.estimated_reclaimed_pages,
            Some(before.page_count - after.page_count)
        );

        let wal_path = temp_dir.path().join("shard_0.db-wal");
        let wal_len = std::fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(0);
        assert_eq!(wal_len, 0, "WAL should be truncated after vacuum");
    }

    #[tokio::test]
    async fn wal_checkpoint_truncate_reports_busy_when_reader_holds_snapshot() {
        let temp_dir = TempDir::new().expect("failed to create temp dir");
        let db_path = temp_dir.path().join("shard_0.db");
        let writer = create_writer_like_pool(&db_path, 50).await;
        let readers = create_writer_like_pool(&db_path, 50).await;

        sqlx::query("CREATE TABLE t (k INTEGER PRIMARY KEY)")
            .execute(&writer)
            .await
            .expect("failed to create table");
        sqlx::query("INSERT INTO t (k) VALUES (1)")
            .execute(&writer)
            .await
            .expect("failed to insert row");

        let mut reader = readers.acquire().await.expect("failed to acquire reader");
        sqlx::query("BEGIN")
            .execute(&mut *reader)
            .await
            .expect("failed to begin read transaction");
        sqlx::query("SELECT COUNT(*) FROM t")
            .execute(&mut *reader)
            .await
            .expect("failed to read");

        sqlx::query("INSERT INTO t (k) VALUES (2)")
            .execute(&writer)
            .await
            .expect("failed to insert row");

        let err = wal_checkpoint_truncate(&writer)
            .await
            .expect_err("checkpoint should be incomplete while a reader holds a snapshot");
        assert!(
            is_sqlite_busy_error(&err),
            "expected busy error, got: {}",
            err
        );

        sqlx::query("COMMIT")
            .execute(&mut *reader)
            .await
            .expect("failed to end read transaction");
        drop(reader);

        wal_checkpoint_truncate(&writer)
            .await
            .expect("checkpoint should complete once the reader is gone");
    }

    #[test]
    fn namespace_table_and_index_distinguish_case_and_keep_valid_identifiers() {
        let lower = namespace_table("users");
        let upper = namespace_table("Users");
        assert_ne!(lower, upper);
        assert!(lower.starts_with("blobs_users_"));
        assert!(upper.starts_with("blobs_users_"));
        assert_eq!(lower.len(), "blobs_users_".len() + 32);
        assert!(
            lower
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
        );
        assert!(
            upper
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
        );
        assert_ne!(namespace_index(&lower), namespace_index(&upper));

        // Long namespaces cap the slug at MAX_NAMESPACE_SLUG_LEN while hashing the full name.
        let long_a = format!("{}a", "x".repeat(40));
        let long_b = format!("{}b", "x".repeat(40));
        let table_a = namespace_table(&long_a);
        let table_b = namespace_table(&long_b);
        assert_ne!(table_a, table_b);
        assert_eq!(
            table_a.len(),
            "blobs_".len() + MAX_NAMESPACE_SLUG_LEN + 1 + 32
        );

        // Non-[a-z0-9_] characters are sanitized in the slug without losing uniqueness.
        assert_ne!(namespace_table("chart-tv"), namespace_table("chart_tv"));
        assert!(namespace_table("chart-tv").starts_with("blobs_chart_tv_"));
    }

    #[tokio::test]
    async fn migrate_shard_schema_renames_legacy_tables_and_is_idempotent() {
        let temp_dir = TempDir::new().expect("failed to create temp dir");
        let db_path = temp_dir.path().join("shard_0.db");
        let options = SqliteConnectOptions::from_str(&format!("sqlite:{}", db_path.display()))
            .unwrap()
            .create_if_missing(true);
        let mut conn = SqliteConnection::connect_with(&options).await.unwrap();

        // Legacy shard (user_version = 0) with two tables: one with an index, one without.
        for sql in [
            "CREATE TABLE blobs (key TEXT PRIMARY KEY, data BLOB, created_at INTEGER NOT NULL,
             updated_at INTEGER NOT NULL, expires_at INTEGER, version INTEGER NOT NULL DEFAULT 0)",
            "CREATE TABLE blobs_users (key TEXT PRIMARY KEY, data BLOB, created_at INTEGER NOT NULL,
             updated_at INTEGER NOT NULL, expires_at INTEGER, version INTEGER NOT NULL DEFAULT 0)",
            "CREATE INDEX idx_users_expires_at ON blobs_users(expires_at) WHERE expires_at IS NOT NULL",
            "INSERT INTO blobs_users (key, data, created_at, updated_at, version) VALUES ('alice', x'0102', 1, 1, 0)",
            "CREATE TABLE blobs_MixedCase (key TEXT PRIMARY KEY, data BLOB, created_at INTEGER NOT NULL,
             updated_at INTEGER NOT NULL, expires_at INTEGER, version INTEGER NOT NULL DEFAULT 0)",
            "INSERT INTO blobs_MixedCase (key, data, created_at, updated_at, version) VALUES ('bob', x'0304', 2, 2, 0)",
        ] {
            sqlx::query(sql).execute(&mut conn).await.unwrap();
        }

        migrate_shard_schema(&mut conn, 0).await.unwrap();

        let version: i64 = sqlx::query_scalar("PRAGMA user_version")
            .fetch_one(&mut conn)
            .await
            .unwrap();
        assert_eq!(version, SHARD_SCHEMA_VERSION);

        let users_table = namespace_table("users");
        let mixed_table = namespace_table("MixedCase");

        let (alice_data,): (Vec<u8>,) = sqlx::query_as(&format!(
            "SELECT data FROM {users_table} WHERE key = 'alice'"
        ))
        .fetch_one(&mut conn)
        .await
        .unwrap();
        assert_eq!(alice_data, vec![1, 2]);

        let (bob_data,): (Vec<u8>,) =
            sqlx::query_as(&format!("SELECT data FROM {mixed_table} WHERE key = 'bob'"))
                .fetch_one(&mut conn)
                .await
                .unwrap();
        assert_eq!(bob_data, vec![3, 4]);

        // Existing index on blobs_users was preserved without creating a duplicate;
        // missing index on blobs_MixedCase was created.
        let users_indexes: Vec<(String,)> = sqlx::query_as(
            "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = ? AND sql LIKE '%expires_at%'",
        )
        .bind(&users_table)
        .fetch_all(&mut conn)
        .await
        .unwrap();
        assert_eq!(users_indexes, vec![("idx_users_expires_at".to_string(),)]);

        let mixed_indexes: Vec<(String,)> = sqlx::query_as(
            "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = ? AND sql LIKE '%expires_at%'",
        )
        .bind(&mixed_table)
        .fetch_all(&mut conn)
        .await
        .unwrap();
        assert_eq!(mixed_indexes, vec![(namespace_index(&mixed_table),)]);

        let meta: Vec<(String, String)> =
            sqlx::query_as("SELECT namespace, table_name FROM blob_namespaces ORDER BY namespace")
                .fetch_all(&mut conn)
                .await
                .unwrap();
        assert_eq!(
            meta,
            vec![
                ("MixedCase".to_string(), mixed_table.clone()),
                ("users".to_string(), users_table.clone()),
            ]
        );

        // Running migration a second time is a no-op and does not double-hash tables.
        migrate_shard_schema(&mut conn, 0).await.unwrap();
        let tables_after: Vec<(String,)> = sqlx::query_as(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'blobs\\_%' ESCAPE '\\' ORDER BY name",
        )
        .fetch_all(&mut conn)
        .await
        .unwrap();
        let mut expected_tables = vec![users_table, mixed_table];
        expected_tables.sort();
        assert_eq!(
            tables_after
                .into_iter()
                .map(|(name,)| name)
                .collect::<Vec<_>>(),
            expected_tables
        );
    }

    #[tokio::test]
    async fn migrate_shard_schema_handles_concurrent_startups_without_double_migrating() {
        let temp_dir = TempDir::new().expect("failed to create temp dir");
        let db_path = temp_dir.path().join("shard_0.db");
        let options = SqliteConnectOptions::from_str(&format!("sqlite:{}", db_path.display()))
            .unwrap()
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .busy_timeout(Duration::from_secs(5));

        let mut setup = SqliteConnection::connect_with(&options).await.unwrap();
        for sql in [
            "CREATE TABLE blobs (key TEXT PRIMARY KEY, data BLOB, created_at INTEGER NOT NULL,
             updated_at INTEGER NOT NULL, expires_at INTEGER, version INTEGER NOT NULL DEFAULT 0)",
            "CREATE TABLE blobs_users (key TEXT PRIMARY KEY, data BLOB, created_at INTEGER NOT NULL,
             updated_at INTEGER NOT NULL, expires_at INTEGER, version INTEGER NOT NULL DEFAULT 0)",
            "INSERT INTO blobs_users (key, data, created_at, updated_at, version) VALUES ('alice', x'aa', 1, 1, 0)",
        ] {
            sqlx::query(sql).execute(&mut setup).await.unwrap();
        }
        setup.close().await.unwrap();

        // Deterministic race: conn_a holds BEGIN IMMEDIATE while conn_b starts
        // migrate_shard_schema_inner, reads user_version = 0, notifies
        // `read_tx`, and then blocks on BEGIN IMMEDIATE. conn_a commits the v1
        // migration only AFTER conn_b's initial v0 read has completed; conn_b
        // must re-read user_version = 1 under the lock and NOT re-hash.
        let mut conn_a = SqliteConnection::connect_with(&options).await.unwrap();
        let mut conn_b = SqliteConnection::connect_with(&options).await.unwrap();

        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut conn_a)
            .await
            .unwrap();

        let (read_tx, read_rx) = oneshot::channel();
        let racer = tokio::spawn(async move {
            migrate_shard_schema_inner(&mut conn_b, 0, Some(read_tx))
                .await
                .unwrap();
            conn_b.close().await.unwrap();
        });
        // Wait until racer has definitely completed its initial user_version = 0 read.
        read_rx.await.unwrap();

        let users_table = namespace_table("users");
        sqlx::query(CREATE_BLOB_NAMESPACES_TABLE)
            .execute(&mut conn_a)
            .await
            .unwrap();
        sqlx::query(&format!("ALTER TABLE blobs_users RENAME TO {users_table}"))
            .execute(&mut conn_a)
            .await
            .unwrap();
        sqlx::query("INSERT INTO blob_namespaces (namespace, table_name) VALUES ('users', ?)")
            .bind(&users_table)
            .execute(&mut conn_a)
            .await
            .unwrap();
        sqlx::query(&format!("PRAGMA user_version = {SHARD_SCHEMA_VERSION}"))
            .execute(&mut conn_a)
            .await
            .unwrap();
        sqlx::query("COMMIT").execute(&mut conn_a).await.unwrap();

        racer.await.unwrap();

        // Table must still be `users_table` (not double-hashed) with original payload.
        let (data,): (Vec<u8>,) = sqlx::query_as(&format!(
            "SELECT data FROM {users_table} WHERE key = 'alice'"
        ))
        .fetch_one(&mut conn_a)
        .await
        .unwrap();
        assert_eq!(data, vec![0xaa]);
        conn_a.close().await.unwrap();
    }

    #[tokio::test]
    async fn migrate_shard_schema_handles_legacy_table_occupying_rename_destination_and_index_name()
    {
        let temp_dir = TempDir::new().expect("failed to create temp dir");
        let db_path = temp_dir.path().join("shard_0.db");
        let options = SqliteConnectOptions::from_str(&format!("sqlite:{}", db_path.display()))
            .unwrap()
            .create_if_missing(true);
        let mut conn = SqliteConnection::connect_with(&options).await.unwrap();

        // `users` hashes to `blobs_users_<hash>`. A legacy shard can validly
        // contain a namespace literally named `users_<hash>` whose legacy table
        // name equals `users`'s destination table name and whose legacy index
        // name equals `users`'s v1 index name.
        let users_table = namespace_table("users");
        let colliding_ns = users_table.strip_prefix("blobs_").unwrap().to_string();
        let colliding_final_table = namespace_table(&colliding_ns);
        assert_ne!(users_table, colliding_final_table);

        for sql in [
            "CREATE TABLE blobs (key TEXT PRIMARY KEY, data BLOB, created_at INTEGER NOT NULL,
             updated_at INTEGER NOT NULL, expires_at INTEGER, version INTEGER NOT NULL DEFAULT 0)",
            "CREATE TABLE blobs_users (key TEXT PRIMARY KEY, data BLOB, created_at INTEGER NOT NULL,
             updated_at INTEGER NOT NULL, expires_at INTEGER, version INTEGER NOT NULL DEFAULT 0)",
            "INSERT INTO blobs_users (key, data, created_at, updated_at, version) VALUES ('k', x'11', 1, 1, 0)",
            &format!(
                "CREATE TABLE {users_table} (key TEXT PRIMARY KEY, data BLOB, created_at INTEGER NOT NULL,
                 updated_at INTEGER NOT NULL, expires_at INTEGER, version INTEGER NOT NULL DEFAULT 0)"
            ),
            &format!(
                "CREATE INDEX idx_{colliding_ns}_expires_at ON {users_table}(expires_at) WHERE expires_at IS NOT NULL"
            ),
            &format!(
                "INSERT INTO {users_table} (key, data, created_at, updated_at, version) VALUES ('k', x'22', 1, 1, 0)"
            ),
        ] {
            sqlx::query(sql).execute(&mut conn).await.unwrap();
        }

        migrate_shard_schema(&mut conn, 0).await.unwrap();

        let (users_val,): (Vec<u8>,) =
            sqlx::query_as(&format!("SELECT data FROM {users_table} WHERE key = 'k'"))
                .fetch_one(&mut conn)
                .await
                .unwrap();
        let (colliding_val,): (Vec<u8>,) = sqlx::query_as(&format!(
            "SELECT data FROM {colliding_final_table} WHERE key = 'k'"
        ))
        .fetch_one(&mut conn)
        .await
        .unwrap();
        assert_eq!(users_val, vec![0x11]);
        assert_eq!(colliding_val, vec![0x22]);
    }

    #[tokio::test]
    async fn migrate_shard_schema_checks_indexed_columns_and_rejects_future_versions() {
        let temp_dir = TempDir::new().expect("failed to create temp dir");
        let db_path = temp_dir.path().join("shard_0.db");
        let options = SqliteConnectOptions::from_str(&format!("sqlite:{}", db_path.display()))
            .unwrap()
            .create_if_missing(true);
        let mut conn = SqliteConnection::connect_with(&options).await.unwrap();

        // A namespace named `expires_at` with an index only on `key` still has
        // "expires_at" in the index's SQL text (via the table name) and must
        // get a real expires_at index during migration.
        for sql in [
            "CREATE TABLE blobs (key TEXT PRIMARY KEY, data BLOB, created_at INTEGER NOT NULL,
             updated_at INTEGER NOT NULL, expires_at INTEGER, version INTEGER NOT NULL DEFAULT 0)",
            "CREATE TABLE blobs_expires_at (key TEXT PRIMARY KEY, data BLOB, created_at INTEGER NOT NULL,
             updated_at INTEGER NOT NULL, expires_at INTEGER, version INTEGER NOT NULL DEFAULT 0)",
            "CREATE INDEX idx_other ON blobs_expires_at(key)",
        ] {
            sqlx::query(sql).execute(&mut conn).await.unwrap();
        }

        migrate_shard_schema(&mut conn, 0).await.unwrap();

        let table = namespace_table("expires_at");
        let expected_idx = namespace_index(&table);
        let indexes: Vec<(String,)> = sqlx::query_as(
            "SELECT name FROM pragma_index_list(?) WHERE origin = 'c' ORDER BY name",
        )
        .bind(&table)
        .fetch_all(&mut conn)
        .await
        .unwrap();
        assert!(
            indexes.iter().any(|(name,)| name == &expected_idx),
            "expected expires_at index {expected_idx} to be created, found {indexes:?}"
        );

        // Unsupported future schema versions must be rejected.
        sqlx::query(&format!(
            "PRAGMA user_version = {}",
            SHARD_SCHEMA_VERSION + 1
        ))
        .execute(&mut conn)
        .await
        .unwrap();
        let err = migrate_shard_schema(&mut conn, 0)
            .await
            .expect_err("future schema version must be rejected");
        assert!(err.to_string().contains("unsupported schema version"));
    }

    #[tokio::test]
    async fn migrate_shard_schema_handles_uppercase_prefix_and_temp_index_collision() {
        let temp_dir = TempDir::new().expect("failed to create temp dir");
        let db_path = temp_dir.path().join("shard_0.db");
        let options = SqliteConnectOptions::from_str(&format!("sqlite:{}", db_path.display()))
            .unwrap()
            .create_if_missing(true);
        let mut conn = SqliteConnection::connect_with(&options).await.unwrap();

        for sql in [
            "CREATE TABLE blobs (key TEXT PRIMARY KEY, data BLOB, created_at INTEGER NOT NULL,
             updated_at INTEGER NOT NULL, expires_at INTEGER, version INTEGER NOT NULL DEFAULT 0)",
            // An existing index occupying the first temporary table name candidate.
            "CREATE INDEX __blobasaur_migrate_tmp_0 ON blobs(key)",
            // Uppercase `BLOBS_` prefix must still migrate and retain the `Users` suffix.
            "CREATE TABLE BLOBS_Users (key TEXT PRIMARY KEY, data BLOB, created_at INTEGER NOT NULL,
             updated_at INTEGER NOT NULL, expires_at INTEGER, version INTEGER NOT NULL DEFAULT 0)",
            "INSERT INTO BLOBS_Users (key, data, created_at, updated_at, version) VALUES ('a', x'fe', 1, 1, 0)",
        ] {
            sqlx::query(sql).execute(&mut conn).await.unwrap();
        }

        migrate_shard_schema(&mut conn, 0).await.unwrap();

        let upper_table = namespace_table("Users");
        let (val,): (Vec<u8>,) =
            sqlx::query_as(&format!("SELECT data FROM {upper_table} WHERE key = 'a'"))
                .fetch_one(&mut conn)
                .await
                .unwrap();
        assert_eq!(val, vec![0xfe]);

        // Dropping blob_namespaces on a v1 shard must fail startup validation instead of silently recreating it.
        sqlx::query("DROP TABLE blob_namespaces")
            .execute(&mut conn)
            .await
            .unwrap();
        let err = migrate_shard_schema(&mut conn, 0)
            .await
            .expect_err("missing blob_namespaces on v1 shard must fail");
        assert!(err.to_string().contains("blob_namespaces"));
    }

    #[tokio::test]
    async fn migrate_shard_schema_rejects_bad_v0_meta() {
        // Pre-existing orphan/conflicting blob_namespaces row on a v0 shard must abort upgrade.
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path().join("shard_0.db");
        let options = SqliteConnectOptions::from_str(&format!("sqlite:{}", db_path.display()))
            .unwrap()
            .create_if_missing(true);
        let mut conn = SqliteConnection::connect_with(&options).await.unwrap();
        for sql in [
            "CREATE TABLE blobs (key TEXT PRIMARY KEY, data BLOB, created_at INTEGER NOT NULL,
             updated_at INTEGER NOT NULL, expires_at INTEGER, version INTEGER NOT NULL DEFAULT 0)",
            CREATE_BLOB_NAMESPACES_TABLE,
            "CREATE TABLE blobs_users (key TEXT PRIMARY KEY, data BLOB, created_at INTEGER NOT NULL,
             updated_at INTEGER NOT NULL, expires_at INTEGER, version INTEGER NOT NULL DEFAULT 0)",
            "INSERT INTO blob_namespaces (namespace, table_name) VALUES ('ghost', 'blobs_ghost_00000000000000000000000000000000')",
        ] {
            sqlx::query(sql).execute(&mut conn).await.unwrap();
        }
        assert!(
            migrate_shard_schema(&mut conn, 0).await.is_err(),
            "pre-existing invalid blob_namespaces row must fail v0 migration"
        );
        let version: i64 = sqlx::query_scalar("PRAGMA user_version")
            .fetch_one(&mut conn)
            .await
            .unwrap();
        assert_eq!(version, 0);
    }

    #[tokio::test]
    async fn failed_ensure_namespaced_table_rolls_back_savepoint_and_keeps_shard_valid() {
        let (_temp_dir, pool) = create_test_pool().await;
        sqlx::query(CREATE_BLOB_NAMESPACES_TABLE)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(&format!("PRAGMA user_version = {SHARD_SCHEMA_VERSION}"))
            .execute(&pool)
            .await
            .unwrap();

        // Pre-create a table occupying the index name that `failns` will try to create,
        // forcing CREATE INDEX inside ensure_namespaced_table_exists to fail after
        // CREATE TABLE succeeds.
        let fail_table = namespace_table("failns");
        let blocking_name = namespace_index(&fail_table);
        sqlx::query(&format!("CREATE TABLE {blocking_name} (id INTEGER)"))
            .execute(&pool)
            .await
            .unwrap();

        let (set_tx, set_rx) = oneshot::channel();
        let (hset_tx, hset_rx) = oneshot::channel();
        let mut batch = VecDeque::from([
            ShardWriteOperation::Set {
                key: "ok".to_string(),
                data: Bytes::from_static(b"1"),
                expires_at: None,
                responder: set_tx,
            },
            ShardWriteOperation::HSet {
                namespace: "failns".to_string(),
                key: "f".to_string(),
                data: Bytes::from_static(b"v"),
                responder: hset_tx,
            },
        ]);
        let mut tables = HashSet::from(["blobs".to_string()]);
        process_batch(
            0,
            &pool,
            &mut batch,
            &mut tables,
            &Cache::new(16),
            &Cache::new(16),
            &ShardMetrics::new(0),
        )
        .await;

        // Outer batch committed `Set("ok")`, while `HSet("failns")` failed AND
        // rolled back its CREATE TABLE savepoint so the shard stays valid for restart.
        assert_eq!(set_rx.await.unwrap(), Ok(()));
        assert!(hset_rx.await.unwrap().is_err());

        let exists: Option<(String,)> =
            sqlx::query_as("SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?")
                .bind(&fail_table)
                .fetch_optional(&pool)
                .await
                .unwrap();
        assert_eq!(
            exists, None,
            "failed namespace creation must not leave an unmapped table behind"
        );

        let mut conn = pool.acquire().await.unwrap();
        validate_v1_shard_metadata(&mut conn, 0)
            .await
            .expect("shard must remain valid for restart after failed HSET");
    }
}
