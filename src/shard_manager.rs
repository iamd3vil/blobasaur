use crate::metrics::{Metrics, ShardMetrics};
use crate::redis::protocol::HExpireCondition;
use crate::replication::{self, Frame};
use bytes::Bytes;
use chrono::Utc;
use metrics::Counter;
use moka::future::Cache;
use moka::ops::compute::Op;
use sqlx::{Connection, Sqlite, SqlitePool, pool::PoolConnection};
use std::borrow::Cow;
use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::{Duration, interval, timeout};
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
    /// Replica only: applies a page of the primary's stream in one transaction.
    Apply {
        frames: Vec<Frame>,
        responder: oneshot::Sender<Result<(), String>>,
    },
    /// Replica only: empties the shard to take a full copy of the primary
    /// whose replication history is `replid`.
    Reset {
        replid: String,
        responder: oneshot::Sender<Result<(), String>>,
    },
    /// Answered once everything queued ahead of it has committed.
    Barrier { responder: oneshot::Sender<()> },
}

impl ShardWriteOperation {
    /// Runs alone, never inside a client batch, so it acts as an ordering barrier.
    fn is_maintenance(&self) -> bool {
        matches!(
            self,
            Self::Vacuum { .. } | Self::Apply { .. } | Self::Reset { .. } | Self::Barrier { .. }
        )
    }
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
    /// Published with the highest LSN after each commit that logged changes.
    pub commit_watch: Arc<watch::Sender<u64>>,
    /// Cancel to make the writer commit everything queued and return.
    pub shutdown: CancellationToken,
}

/// An async write that is acknowledged but not committed yet. Reads check
/// the inflight cache first, so they see it before the DB does.
#[derive(Clone, Debug)]
pub enum Pending {
    Value(Bytes),
    /// A queued DEL/HDEL: the key reads as absent. The token is unique per
    /// delete, so its op clears only its own marker.
    Deleted(u64),
}

impl Pending {
    /// Whether this is the very entry `other` recorded, not just equal data.
    /// Values compare by allocation, so this is cheap even for large values.
    fn is(&self, other: &Pending) -> bool {
        match (self, other) {
            (Pending::Value(a), Pending::Value(b)) => {
                a.as_ptr() == b.as_ptr() && a.len() == b.len()
            }
            (Pending::Deleted(a), Pending::Deleted(b)) => a == b,
            _ => false,
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

/// A fresh token for a [`Pending::Deleted`] marker and its delete op.
pub fn next_delete_token() -> u64 {
    static NEXT_TOKEN: AtomicU64 = AtomicU64::new(0);
    NEXT_TOKEN.fetch_add(1, Ordering::Relaxed)
}

/// Whether a queued async op decides `key`'s existence: `Some(true)` for a
/// write, `Some(false)` for a delete, `None` if nothing is queued (ask the DB).
pub async fn pending_exists(cache: &Cache<String, Pending>, key: &str) -> Option<bool> {
    cache
        .get(key)
        .await
        .map(|pending| matches!(pending, Pending::Value(_)))
}

/// Removes `key`'s entry if it is still the one `written` recorded: a newer
/// op on the same key, still queued, must stay visible.
async fn clear_pending_write(cache: &Cache<String, Pending>, key: &str, written: &Pending) {
    cache
        .entry_by_ref(key)
        .and_compute_with(|entry| async move {
            match entry {
                Some(entry) if entry.value().is(written) => Op::Remove,
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
        commit_watch,
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
        // Collect operations for batching
        if batch.is_empty() {
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
                        execute_vacuum(shard_id, &pool, mode, budget_bytes, dry_run, &metrics)
                            .await;
                    let _ = responder.send(result);
                    continue;
                }
                Some(ShardWriteOperation::Apply { frames, responder }) => {
                    let result = replication::apply(&pool, &mut known_tables, &frames).await;
                    let _ = responder.send(result.map(|lsn| {
                        if let Some(lsn) = lsn {
                            replication::publish(&commit_watch, lsn);
                        }
                    }));
                    continue;
                }
                Some(ShardWriteOperation::Reset { replid, responder }) => {
                    let _ = responder.send(replication::reset(&pool, &replid).await);
                    continue;
                }
                Some(ShardWriteOperation::Barrier { responder }) => {
                    let _ = responder.send(());
                    continue;
                }
                Some(operation) => {
                    batch.push_back(operation);
                }
                None => break, // Channel closed
            }
        } else {
            // If batch has items, wait with timeout for more operations
            let next_operation = tokio::select! {
                biased;
                _ = shutdown.cancelled() => break,
                result = timeout(batch_timeout, receiver.recv()) => result,
            };
            match next_operation {
                Ok(Some(operation)) => {
                    if operation.is_maintenance() {
                        pending_maintenance_operation = Some(operation);
                    } else {
                        batch.push_back(operation);
                    }
                }
                Ok(None) => break, // Channel closed
                Err(_) => {}       // Timeout - process current batch
            }
        }

        // Continue collecting until batch is full or no more immediate operations.
        // Maintenance operations are ordering barriers, so stop collecting when one is observed.
        if pending_maintenance_operation.is_none() {
            while batch.len() < batch_size {
                match receiver.try_recv() {
                    Ok(operation) => {
                        if operation.is_maintenance() {
                            pending_maintenance_operation = Some(operation);
                            break;
                        }

                        batch.push_back(operation);
                    }
                    Err(mpsc::error::TryRecvError::Empty) => break,
                    Err(mpsc::error::TryRecvError::Disconnected) => break,
                }
            }
        }

        if !batch.is_empty() {
            let processed_batch_size = batch.len();
            let batch_start = std::time::Instant::now();
            let committed = process_batch(
                shard_id,
                &pool,
                &mut batch,
                &mut known_tables,
                &inflight_cache,
                &inflight_hcache,
                &shard_metrics,
            )
            .await;
            if let Some(lsn) = committed {
                replication::publish(&commit_watch, lsn);
            }
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
                // Dropping the responder reports it as cancelled.
                Some(op) if op.is_maintenance() => {}
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
        let committed = process_batch(
            shard_id,
            &pool,
            &mut batch,
            &mut known_tables,
            &inflight_cache,
            &inflight_hcache,
            &shard_metrics,
        )
        .await;
        if let Some(lsn) = committed {
            replication::publish(&commit_watch, lsn);
        }
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
/// buffer. Async ops still need theirs to clear the inflight cache, so copy.
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
            ShardWriteOperation::SetAsync { key, data, .. } => {
                value_bytes += data.len();
                clear_pending_write(inflight_cache, key, &Pending::Value(data.clone())).await;
            }
            ShardWriteOperation::DeleteAsync { key, token } => {
                clear_pending_write(inflight_cache, key, &Pending::Deleted(*token)).await;
            }
            ShardWriteOperation::HSetAsync {
                namespace,
                key,
                data,
            }
            | ShardWriteOperation::HSetExAsync {
                namespace,
                key,
                data,
                ..
            } => {
                value_bytes += data.len();
                let namespaced_key = format!("{}:{}", namespace, key);
                clear_pending_write(
                    inflight_hcache,
                    &namespaced_key,
                    &Pending::Value(data.clone()),
                )
                .await;
            }
            ShardWriteOperation::HDeleteAsync {
                namespace,
                key,
                token,
            } => {
                let namespaced_key = format!("{}:{}", namespace, key);
                clear_pending_write(inflight_hcache, &namespaced_key, &Pending::Deleted(*token))
                    .await;
            }
            _ => {}
        }
    }
    if value_bytes >= FREE_NOW_BYTES {
        inflight_cache.run_pending_tasks().await;
        inflight_hcache.run_pending_tasks().await;
    }
}

/// The `repl_log` entry an op's data change needs: table, key, tombstone.
type LogEntry<'a> = (Cow<'a, str>, &'a str, bool);

/// Writes `batch` in one transaction, each op under its own savepoint together
/// with its `repl_log` entry. Returns the highest LSN it committed, if any.
async fn process_batch(
    shard_id: usize,
    pool: &SqlitePool,
    batch: &mut VecDeque<ShardWriteOperation>,
    known_tables: &mut HashSet<String>,
    inflight_cache: &Cache<String, Pending>,
    inflight_hcache: &Cache<String, Pending>,
    shard_metrics: &ShardMetrics,
) -> Option<u64> {
    if batch.is_empty() {
        return None;
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
                | ShardWriteOperation::HDelete { responder, .. }
                | ShardWriteOperation::Apply { responder, .. }
                | ShardWriteOperation::Reset { responder, .. } => {
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
                // Never batched; dropping the responder reports it as cancelled.
                ShardWriteOperation::Barrier { .. } => {}
            }
        }
        return None;
    };

    // One entry per op, in batch order.
    let mut results: Vec<Result<(), String>> = Vec::with_capacity(batch.len());
    let mut expire_results: Vec<(usize, bool)> = Vec::new();
    let mut hexpire_results: Vec<(usize, i64)> = Vec::new();
    let mut sync_operations: Vec<usize> = Vec::new();
    // Namespace tables created by this batch, which `known_tables` already lists.
    let mut added_tables: Vec<String> = Vec::new();
    let mut last_lsn: Option<i64> = None;

    // Execute all operations in the transaction
    for (idx, operation) in batch.iter_mut().enumerate() {
        let value = take_value(operation);
        let operation = &*operation;
        if let Err(e) = sqlx::query("SAVEPOINT op").execute(&mut *tx).await {
            tracing::error!("[Shard {}] SAVEPOINT error: {}", shard_id, e);
            results.push(Err(e.to_string()));
            continue;
        }
        let added_before = added_tables.len();
        let result: Result<Option<LogEntry>, String> = match operation {
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

                let live = Some((Cow::Borrowed("blobs"), key.as_str(), false));
                if exists {
                    // Update existing record - update data, updated_at, expires_at, and version
                    sqlx::query("UPDATE blobs SET data = ?, updated_at = ?, expires_at = ?, version = version + 1 WHERE key = ?")
                        .bind(value)
                        .bind(now)
                        .bind(expires_at)
                        .bind(key)
                        .execute(&mut *tx)
                        .await
                        .map(|_| live)
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
                        .map(|_| live)
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
                    .map(|r| {
                        (r.rows_affected() > 0).then_some((
                            Cow::Borrowed("blobs"),
                            key.as_str(),
                            true,
                        ))
                    })
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

                let table_name = format!("blobs_{}", namespace);

                // Ensure table exists
                if let Err(e) = ensure_namespaced_table_exists(
                    &mut tx,
                    &table_name,
                    known_tables,
                    &mut added_tables,
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
                        // Update existing record
                        let update_query = format!(
                            "UPDATE {} SET data = ?, updated_at = ?, version = version + 1 WHERE key = ?",
                            table_name
                        );
                        sqlx::query(&update_query)
                            .bind(value)
                            .bind(now)
                            .bind(key)
                            .execute(&mut *tx)
                            .await
                            .map(|_| Some((Cow::Owned(table_name), key.as_str(), false)))
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
                            .map(|_| Some((Cow::Owned(table_name), key.as_str(), false)))
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

                let table_name = format!("blobs_{}", namespace);

                // Ensure table exists
                if let Err(e) = ensure_namespaced_table_exists(
                    &mut tx,
                    &table_name,
                    known_tables,
                    &mut added_tables,
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
                            .map(|_| Some((Cow::Owned(table_name), key.as_str(), false)))
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
                            .map(|_| Some((Cow::Owned(table_name), key.as_str(), false)))
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

                let table_name = format!("blobs_{}", namespace);

                // Only delete if table exists
                if known_tables.contains(&table_name) {
                    let delete_query = format!("DELETE FROM {} WHERE key = ?", table_name);
                    sqlx::query(&delete_query)
                        .bind(key)
                        .execute(&mut *tx)
                        .await
                        .map(|r| {
                            (r.rows_affected() > 0).then_some((
                                Cow::Owned(table_name),
                                key.as_str(),
                                true,
                            ))
                        })
                        .map_err(|e| {
                            tracing::error!(
                                "[Shard {}] HDEL error for namespace {} key {}: {}",
                                shard_id,
                                namespace,
                                key,
                                e
                            );
                            e.to_string()
                        })
                } else {
                    // Table doesn't exist, operation succeeds (key doesn't exist)
                    Ok(None)
                }
            }
            ShardWriteOperation::Expire {
                key, expires_at, ..
            } => {
                sync_operations.push(idx);

                // Update the expires_at field for the key if it exists
                match sqlx::query("UPDATE blobs SET expires_at = ? WHERE key = ?")
                    .bind(expires_at)
                    .bind(key)
                    .execute(&mut *tx)
                    .await
                {
                    Ok(result) => {
                        let rows_affected = result.rows_affected() > 0;
                        expire_results.push((idx, rows_affected));
                        Ok(rows_affected.then_some((Cow::Borrowed("blobs"), key.as_str(), false)))
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
                    known_tables,
                    namespace,
                    key,
                    *expires_at,
                    *condition,
                )
                .await;
                hexpire_results.push((idx, result_code));
                // TTL set: live; field deleted: tombstone; nothing changed: no entry.
                operation_result.map(|()| {
                    let table = Cow::Owned(format!("blobs_{}", namespace));
                    match result_code {
                        1 => Some((table, key.as_str(), false)),
                        2 => Some((table, key.as_str(), true)),
                        _ => None,
                    }
                })
            }
            ShardWriteOperation::Vacuum { .. }
            | ShardWriteOperation::Apply { .. }
            | ShardWriteOperation::Reset { .. }
            | ShardWriteOperation::Barrier { .. } => {
                Err("Maintenance operation reached transactional batch unexpectedly".to_string())
            }
        };

        // The log entry joins the op's savepoint: both commit or neither does.
        match finish_op(&mut tx, shard_id, result).await {
            Ok(lsn) => {
                last_lsn = last_lsn.max(lsn);
                results.push(Ok(()));
            }
            Err(e) => {
                for statement in ["ROLLBACK TO op", "RELEASE op"] {
                    if let Err(e) = sqlx::query(statement).execute(&mut *tx).await {
                        tracing::error!("[Shard {}] {} error: {}", shard_id, statement, e);
                    }
                }
                for table in added_tables.drain(added_before..) {
                    known_tables.remove(&table);
                }
                results.push(Err(e));
            }
        }
    }

    // Commit transaction
    let commit_result = tx.commit().await.map_err(|e| {
        tracing::error!("[Shard {}] Transaction commit failed: {}", shard_id, e);
        e.to_string()
    });
    // Return the writer connection before the bookkeeping below.
    drop(conn);
    if commit_result.is_err() {
        for table in &added_tables {
            known_tables.remove(table);
        }
    }
    shard_metrics.record_transaction(commit_result.is_ok(), tx_start.elapsed());
    if let Err(e) = &commit_result {
        shard_metrics.record_sqlite_error("commit", e);
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
            | ShardWriteOperation::HDelete { responder, .. }
            | ShardWriteOperation::Apply { responder, .. }
            | ShardWriteOperation::Reset { responder, .. } => {
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
            // Never batched; dropping the responder reports it as cancelled.
            ShardWriteOperation::Barrier { .. } => {}
        }
    }

    commit_result.ok().and(last_lsn).map(|lsn| lsn as u64)
}

/// Writes the op's `repl_log` entry, if its data change needs one, and releases
/// its savepoint. Returns the entry's LSN. On error the caller rolls back.
async fn finish_op(
    tx: &mut sqlx::Transaction<'_, Sqlite>,
    shard_id: usize,
    result: Result<Option<LogEntry<'_>>, String>,
) -> Result<Option<i64>, String> {
    let lsn = match result? {
        Some((table, key, deleted)) => {
            Some(log_change(tx, &table, key, deleted).await.map_err(|e| {
                tracing::error!(
                    "[Shard {}] repl_log error for {} key {}: {}",
                    shard_id,
                    table,
                    key,
                    e
                );
                e.to_string()
            })?)
        }
        None => None,
    };
    sqlx::query("RELEASE op")
        .execute(&mut **tx)
        .await
        .map_err(|e| e.to_string())?;
    Ok(lsn)
}

/// Records that `key` of `table` changed (or was deleted) and returns the new
/// LSN. INSERT OR REPLACE, not an upsert: the row must get a new LSN.
pub(crate) async fn log_change(
    tx: &mut sqlx::Transaction<'_, Sqlite>,
    table: &str,
    key: &str,
    deleted: bool,
) -> Result<i64, sqlx::Error> {
    sqlx::query("INSERT OR REPLACE INTO repl_log (tbl, key, deleted, at) VALUES (?, ?, ?, ?)")
        .bind(table)
        .bind(key)
        .bind(deleted)
        .bind(Utc::now().timestamp())
        .execute(&mut **tx)
        .await
        .map(|r| r.last_insert_rowid())
}

/// Begins the batch transaction on the writer connection, if there is one.
/// A failed acquire was already recorded by [`ShardMetrics::acquire`].
async fn begin<'c>(
    conn: &'c mut Result<PoolConnection<Sqlite>, sqlx::Error>,
    shard_metrics: &ShardMetrics,
    tx_start: std::time::Instant,
) -> Result<sqlx::Transaction<'c, Sqlite>, String> {
    let conn = conn.as_mut().map_err(|e| e.to_string())?;
    conn.begin().await.map_err(|e| {
        shard_metrics.record_sqlite_error("begin", &e.to_string());
        shard_metrics.record_transaction(false, tx_start.elapsed());
        e.to_string()
    })
}

async fn handle_hexpire_operation(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    shard_id: usize,
    known_tables: &HashSet<String>,
    namespace: &str,
    key: &str,
    expires_at: i64,
    condition: Option<HExpireCondition>,
) -> (i64, Result<(), String>) {
    let table_name = format!("blobs_{}", namespace);

    // HEXPIRE should be a no-op for missing namespaces.
    if !known_tables.contains(&table_name) {
        return (-2, Ok(()));
    }

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
        Ok(None) => (-2, Ok(())),
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
        "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'blobs_%'",
    )
    .fetch_all(pool)
    .await
    {
        Ok(rows) => {
            for (table_name,) in rows {
                tables.insert(table_name);
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

// Ensure a namespaced table exists, creating it if necessary. A table this
// creates is added to `known_tables` at once, and to `added_tables` so the
// caller can take it back out if the transaction doesn't keep it.
pub(crate) async fn ensure_namespaced_table_exists(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    table_name: &str,
    known_tables: &mut HashSet<String>,
    added_tables: &mut Vec<String>,
) -> Result<(), String> {
    if known_tables.contains(table_name) {
        return Ok(());
    }

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

    match sqlx::query(&create_query).execute(&mut **tx).await {
        Ok(_) => {
            // Create index on expires_at for efficient expiry queries
            let index_query = format!(
                "CREATE INDEX IF NOT EXISTS idx_{}_expires_at ON {}(expires_at) WHERE expires_at IS NOT NULL",
                table_name.replace("blobs_", ""),
                table_name
            );

            match sqlx::query(&index_query).execute(&mut **tx).await {
                Ok(_) => {
                    known_tables.insert(table_name.to_string());
                    added_tables.push(table_name.to_string());
                    tracing::info!(
                        "Created namespaced table and expires_at index: {}",
                        table_name
                    );
                    Ok(())
                }
                Err(e) => {
                    tracing::error!(
                        "Failed to create expires_at index for table {}: {}",
                        table_name,
                        e
                    );
                    Err(format!("Failed to create index: {}", e))
                }
            }
        }
        Err(e) => {
            tracing::error!("Failed to create namespaced table {}: {}", table_name, e);
            Err(format!("Failed to create table: {}", e))
        }
    }
}

/// Deletes expired rows from `table` in chunks of `chunk_size`. The
/// cleanup shares the shard's single writer connection, so each chunk is its own
/// transaction: the pool hands out connections in FIFO order, letting a waiting
/// writer batch run between chunks instead of stalling behind one long DELETE.
/// A chunk deletes its rows and logs a tombstone for each, and publishes its
/// LSN once committed. Each chunk's rows are added to `deleted` as they go, so
/// a later failing chunk doesn't lose the count. Stops between chunks once
/// `cancel` is cancelled.
async fn delete_expired(
    pool: &SqlitePool,
    table: &str,
    now: i64,
    chunk_size: u32,
    deleted_counter: &Counter,
    commit_watch: &watch::Sender<u64>,
    cancel: &CancellationToken,
) -> Result<u64, sqlx::Error> {
    let query = format!(
        "DELETE FROM {table} WHERE rowid IN (
            SELECT rowid FROM {table} WHERE expires_at IS NOT NULL AND expires_at <= ? LIMIT ?
        ) RETURNING key"
    );
    let mut total = 0;
    while !cancel.is_cancelled() {
        let mut tx = pool.begin().await?;
        let keys = sqlx::query_scalar::<_, String>(&query)
            .bind(now)
            .bind(chunk_size)
            .fetch_all(&mut *tx)
            .await?;
        let mut last_lsn = None;
        for key in &keys {
            last_lsn = Some(log_change(&mut tx, table, key, true).await?);
        }
        tx.commit().await?;
        if let Some(lsn) = last_lsn {
            replication::publish(commit_watch, lsn as u64);
        }
        let deleted = keys.len() as u64;
        deleted_counter.increment(deleted);
        total += deleted;
        if deleted < u64::from(chunk_size) {
            break;
        }
    }
    Ok(total)
}

/// Background task to clean up expired keys from a shard, and to trim old
/// tombstones from its change index.
#[allow(clippy::too_many_arguments)]
pub async fn shard_cleanup_task(
    shard_id: usize,
    pool: SqlitePool,
    cleanup_interval_secs: u64,
    chunk_size: u32,
    commit_watch: Arc<watch::Sender<u64>>,
    pins: Arc<replication::ShardPins>,
    tombstone_retention_secs: u64,
    cancel: CancellationToken,
) {
    let mut interval = interval(Duration::from_secs(cleanup_interval_secs));
    let shard_metrics = ShardMetrics::new(shard_id);

    tracing::info!(
        "[Shard {}] Starting cleanup task with interval {} seconds",
        shard_id,
        cleanup_interval_secs
    );

    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => break,
            _ = interval.tick() => {}
        }

        let started = std::time::Instant::now();
        let mut ok = true;
        let now = chrono::Utc::now().timestamp();

        // The main blobs table plus every namespaced "blobs_*" table
        let mut tables = vec!["blobs".to_string()];
        match sqlx::query_as::<_, (String,)>(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'blobs_%'",
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
            match delete_expired(
                &pool,
                &table_name,
                now,
                chunk_size,
                deleted,
                &commit_watch,
                &cancel,
            )
            .await
            {
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
        if let Err(e) = replication::trim_tombstones(&pool, &pins, tombstone_retention_secs).await {
            tracing::error!("[Shard {}] Error trimming tombstones: {}", shard_id, e);
            shard_metrics.record_cleanup_error("trim");
            ok = false;
        }
        shard_metrics.record_cleanup(ok, started.elapsed());
    }

    tracing::info!("[Shard {}] Cleanup task stopped", shard_id);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app_state::{CREATE_REPL_LOG, CREATE_REPL_STATE};
    use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
    use std::str::FromStr;
    use tempfile::TempDir;

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

        for ddl in [CREATE_REPL_LOG, CREATE_REPL_STATE] {
            sqlx::query(ddl)
                .execute(&pool)
                .await
                .expect("failed to create change index");
        }

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

        let deleted = delete_expired(
            &pool,
            "blobs",
            now,
            chunk_size,
            &Counter::noop(),
            &watch::channel(0).0,
            &CancellationToken::new(),
        )
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
    ) {
        let op = ShardWriteOperation::SetAsync {
            key: key.to_string(),
            data: data.clone(),
            expires_at: None,
        };
        queue_pending(cache, sender, key.to_string(), Pending::Value(data), op)
            .await
            .unwrap();
    }

    async fn queue_del(cache: &Cache<String, Pending>, sender: &ShardSender, key: &str) -> u64 {
        let token = next_delete_token();
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
        let cache: Cache<String, Pending> = Cache::new(16);
        let hcache: Cache<String, Pending> = Cache::new(16);
        let (sender, mut receiver) = test_channel(16);

        let value = Bytes::from(vec![1u8; FREE_NOW_BYTES]);
        queue_set(&cache, &sender, "k", value.clone()).await;
        let batch = VecDeque::from([receiver.try_recv().unwrap()]);
        clear_pending_entries(&batch, &cache, &hcache).await;
        drop(batch);

        // Only the test's own handle is left: the cache let go of its copy.
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
        let token = next_delete_token();
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
            matches!(cache.get("k").await, Some(Pending::Value(v)) if v == v2),
            "newer SET must stay visible"
        );
        assert!(
            matches!(cache.get("d").await, Some(Pending::Value(_))),
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
        let cache: Cache<String, Pending> = Cache::new(16);
        let hcache: Cache<String, Pending> = Cache::new(16);
        let (sender, mut receiver) = test_channel(16);
        let shard_metrics = ShardMetrics::new(0);

        // Two writes to one key are two pending ops, plus one whose statement
        // fails: a namespace that isn't a valid table name.
        queue_set(&cache, &sender, "k", Bytes::from_static(b"1")).await;
        queue_set(&cache, &sender, "k", Bytes::from_static(b"2")).await;
        let op = ShardWriteOperation::HSetAsync {
            namespace: "bad name".to_string(),
            key: "f".to_string(),
            data: Bytes::from_static(b"v"),
        };
        let pending = Pending::Value(Bytes::from_static(b"v"));
        queue_pending(&hcache, &sender, "bad name:f".to_string(), pending, op)
            .await
            .unwrap();
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
            let (commit_watch, cancel) = (watch::channel(0).0, CancellationToken::new());
            assert_eq!(
                delete_expired(&pool, "blobs", 10, 1, &counter, &commit_watch, &cancel)
                    .await
                    .unwrap(),
                2
            );
            let missing = delete_expired(
                &pool,
                "blobs_missing",
                10,
                1,
                &counter,
                &commit_watch,
                &cancel,
            )
            .await;
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
        // Another connection holds the write lock: the writer's INSERT fails
        // with SQLITE_BUSY, but its (now read-only) transaction still commits.
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
            commit_watch: Arc::new(watch::channel(0).0),
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
            result.as_ref().is_err_and(|e| e.contains("locked")),
            "a write that wasn't persisted must not be acknowledged: {result:?}"
        );

        sqlx::query("ROLLBACK").execute(&mut *lock).await.unwrap();
        drop(lock);
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
    async fn writer_drains_queued_ops_on_shutdown() {
        let (_temp_dir, pool) = create_test_pool().await;
        let (sender, receiver) = mpsc::channel(64);
        for i in 0..50 {
            sender
                .send(ShardWriteOperation::SetAsync {
                    key: format!("key-{i}"),
                    data: Bytes::from_static(b"v"),
                    expires_at: None,
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
                commit_watch: Arc::new(watch::channel(0).0),
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
    async fn barrier_is_answered_after_earlier_async_writes_commit() {
        let (_temp_dir, pool) = create_test_pool().await;
        let (sender, receiver) = mpsc::channel(64);
        let writer = tokio::spawn(shard_writer_task(ShardWriter {
            shard_id: 0,
            pool: pool.clone(),
            receiver,
            batch_size: 64,
            // Without the barrier these would sit in the batch for a minute.
            batch_timeout_ms: 60_000,
            inflight_cache: Cache::new(128),
            inflight_hcache: Cache::new(128),
            metrics: Metrics::new(),
            commit_watch: Arc::new(watch::channel(0).0),
            shutdown: CancellationToken::new(),
        }));
        for i in 0..5 {
            sender
                .send(ShardWriteOperation::SetAsync {
                    key: format!("key-{i}"),
                    data: Bytes::from_static(b"v"),
                    expires_at: None,
                })
                .await
                .unwrap();
        }

        let (responder, done) = oneshot::channel();
        sender
            .send(ShardWriteOperation::Barrier { responder })
            .await
            .unwrap();
        timeout(Duration::from_secs(5), done)
            .await
            .expect("barrier was not answered")
            .unwrap();

        let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM blobs")
            .fetch_one(&pool)
            .await
            .unwrap();
        let lsn: i64 =
            sqlx::query_scalar("SELECT seq FROM sqlite_sequence WHERE name = 'repl_log'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!((rows, lsn), (5, 5));

        drop(sender);
        writer.await.unwrap();
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
            commit_watch: Arc::new(watch::channel(0).0),
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

    type OpResult<T> = oneshot::Receiver<Result<T, String>>;

    fn set(key: &str) -> (ShardWriteOperation, OpResult<()>) {
        let (responder, rx) = oneshot::channel();
        let op = ShardWriteOperation::Set {
            key: key.to_string(),
            data: Bytes::from_static(b"v"),
            expires_at: None,
            responder,
        };
        (op, rx)
    }

    fn del(key: &str) -> (ShardWriteOperation, OpResult<()>) {
        let (responder, rx) = oneshot::channel();
        let key = key.to_string();
        (ShardWriteOperation::Delete { key, responder }, rx)
    }

    fn hset(namespace: &str, key: &str) -> (ShardWriteOperation, OpResult<()>) {
        let (responder, rx) = oneshot::channel();
        let op = ShardWriteOperation::HSet {
            namespace: namespace.to_string(),
            key: key.to_string(),
            data: Bytes::from_static(b"v"),
            responder,
        };
        (op, rx)
    }

    fn hdel(namespace: &str, key: &str) -> (ShardWriteOperation, OpResult<()>) {
        let (responder, rx) = oneshot::channel();
        let (namespace, key) = (namespace.to_string(), key.to_string());
        let op = ShardWriteOperation::HDelete {
            namespace,
            key,
            responder,
        };
        (op, rx)
    }

    fn expire(key: &str, expires_at: i64) -> (ShardWriteOperation, OpResult<bool>) {
        let (responder, rx) = oneshot::channel();
        let key = key.to_string();
        let op = ShardWriteOperation::Expire {
            key,
            expires_at,
            responder,
        };
        (op, rx)
    }

    fn hexpire(
        key: &str,
        expires_at: i64,
        condition: Option<HExpireCondition>,
    ) -> (ShardWriteOperation, OpResult<i64>) {
        let (responder, rx) = oneshot::channel();
        let op = ShardWriteOperation::HExpire {
            namespace: "ns".to_string(),
            key: key.to_string(),
            expires_at,
            condition,
            responder,
        };
        (op, rx)
    }

    /// Runs `ops` as one batch; returns the LSN `process_batch` reports.
    async fn run_batch(
        pool: &SqlitePool,
        known_tables: &mut HashSet<String>,
        ops: Vec<ShardWriteOperation>,
    ) -> Option<u64> {
        process_batch(
            0,
            pool,
            &mut VecDeque::from(ops),
            known_tables,
            &Cache::new(16),
            &Cache::new(16),
            &ShardMetrics::new(0),
        )
        .await
    }

    fn blobs_only() -> HashSet<String> {
        HashSet::from(["blobs".to_string()])
    }

    /// `(tbl, key, deleted)` in LSN order.
    async fn repl_log(pool: &SqlitePool) -> Vec<(String, String, bool)> {
        sqlx::query_as("SELECT tbl, key, deleted FROM repl_log ORDER BY lsn")
            .fetch_all(pool)
            .await
            .unwrap()
    }

    async fn lsn_of(pool: &SqlitePool, tbl: &str, key: &str) -> Option<i64> {
        sqlx::query_scalar("SELECT lsn FROM repl_log WHERE tbl = ? AND key = ?")
            .bind(tbl)
            .bind(key)
            .fetch_optional(pool)
            .await
            .unwrap()
    }

    async fn keys(pool: &SqlitePool, table: &str) -> Vec<String> {
        sqlx::query_scalar(&format!("SELECT key FROM {table}"))
            .fetch_all(pool)
            .await
            .unwrap()
    }

    fn row(tbl: &str, key: &str, deleted: bool) -> (String, String, bool) {
        (tbl.to_string(), key.to_string(), deleted)
    }

    #[tokio::test]
    async fn writes_keep_one_log_row_per_key_with_increasing_lsns() {
        let (_dir, pool) = create_test_pool().await;
        let known = &mut blobs_only();

        let first = run_batch(
            &pool,
            known,
            vec![set("a").0, set("b").0, set("a").0, hset("ns", "f").0],
        )
        .await
        .unwrap();
        let (a, b) = (
            lsn_of(&pool, "blobs", "a").await.unwrap(),
            lsn_of(&pool, "blobs", "b").await.unwrap(),
        );
        assert!(b < a, "a was rewritten after b");
        assert_eq!(first as i64, lsn_of(&pool, "blobs_ns", "f").await.unwrap());

        let second = run_batch(&pool, known, vec![set("b").0]).await.unwrap();
        assert!(second > first);
        assert!(lsn_of(&pool, "blobs", "b").await.unwrap() > a);
        assert_eq!(repl_log(&pool).await.len(), 3);
        assert!(repl_log(&pool).await.iter().all(|(_, _, deleted)| !deleted));
    }

    #[tokio::test]
    async fn deletes_log_a_tombstone_only_when_a_row_was_deleted() {
        let (_dir, pool) = create_test_pool().await;
        let known = &mut blobs_only();
        run_batch(&pool, known, vec![set("a").0, hset("ns", "f").0]).await;

        run_batch(
            &pool,
            known,
            vec![
                del("a").0,
                del("missing").0,
                hdel("ns", "f").0,
                hdel("ns", "missing").0,
                hdel("no-such-namespace", "f").0,
            ],
        )
        .await;
        assert_eq!(
            repl_log(&pool).await,
            vec![row("blobs", "a", true), row("blobs_ns", "f", true)]
        );

        // A tombstone is replaced by a live row when the key comes back.
        run_batch(&pool, known, vec![set("a").0]).await;
        assert_eq!(
            repl_log(&pool).await,
            vec![row("blobs_ns", "f", true), row("blobs", "a", false)]
        );
    }

    #[tokio::test]
    async fn expire_logs_only_when_a_row_was_updated() {
        let (_dir, pool) = create_test_pool().await;
        let known = &mut blobs_only();
        let first = run_batch(&pool, known, vec![set("a").0]).await.unwrap();

        let (expire_a, a) = expire("a", 4_000_000_000);
        let (expire_missing, missing) = expire("missing", 4_000_000_000);
        let second = run_batch(&pool, known, vec![expire_a, expire_missing])
            .await
            .unwrap();
        assert_eq!(
            (a.await.unwrap(), missing.await.unwrap()),
            (Ok(true), Ok(false))
        );
        assert!(second > first);
        assert_eq!(repl_log(&pool).await, vec![row("blobs", "a", false)]);

        // Nothing changed, so nothing was committed to the log.
        assert_eq!(
            run_batch(&pool, known, vec![expire("missing", 1).0]).await,
            None
        );
    }

    #[tokio::test]
    async fn hexpire_results_map_to_log_entries() {
        let (_dir, pool) = create_test_pool().await;
        let known = &mut blobs_only();
        let far = 4_000_000_000;
        run_batch(&pool, known, vec![hset("ns", "f").0]).await;
        let mut lsn = lsn_of(&pool, "blobs_ns", "f").await.unwrap();

        // (op, expected result, whether the log changes)
        let steps = [
            (hexpire("f", far, None), 1, Some(false)),
            (hexpire("f", far + 1, Some(HExpireCondition::Nx)), 0, None),
            (hexpire("missing", far, None), -2, None),
            (hexpire("f", 1, None), 2, Some(true)),
        ];
        for ((op, result), code, logged) in steps {
            let committed = run_batch(&pool, known, vec![op]).await;
            assert_eq!(result.await.unwrap(), Ok(code));
            match logged {
                Some(deleted) => {
                    assert!(committed.unwrap() as i64 > lsn);
                    lsn = lsn_of(&pool, "blobs_ns", "f").await.unwrap();
                    assert_eq!(repl_log(&pool).await, vec![row("blobs_ns", "f", deleted)]);
                }
                None => assert_eq!(committed, None, "result {code} must not log"),
            }
        }
    }

    #[tokio::test]
    async fn a_failed_op_leaves_no_data_or_log_row_and_others_commit() {
        let (_dir, pool) = create_test_pool().await;
        // The log row of a key named "bad" can't be written, after its data was.
        sqlx::query(
            "CREATE TRIGGER fail_bad BEFORE INSERT ON repl_log WHEN NEW.key = 'bad'
             BEGIN SELECT RAISE(ABORT, 'forced failure'); END",
        )
        .execute(&pool)
        .await
        .unwrap();
        let known = &mut blobs_only();

        let (good, good_rx) = set("good");
        let (bad, bad_rx) = set("bad");
        // Creates blobs_ns1, then fails: the table goes and so does its name in
        // `known_tables`, or the next op would fail on the missing table.
        let (bad_hset, bad_hset_rx) = hset("ns1", "bad");
        let (next_hset, next_hset_rx) = hset("ns1", "next");
        let committed = run_batch(&pool, known, vec![good, bad, bad_hset, next_hset]).await;

        assert_eq!(good_rx.await.unwrap(), Ok(()));
        assert_eq!(next_hset_rx.await.unwrap(), Ok(()));
        for rx in [bad_rx, bad_hset_rx] {
            let error = rx.await.unwrap().unwrap_err();
            assert!(error.contains("forced failure"), "{error}");
        }
        assert_eq!(keys(&pool, "blobs").await, vec!["good"]);
        assert_eq!(keys(&pool, "blobs_ns1").await, vec!["next"]);
        assert_eq!(
            repl_log(&pool).await,
            vec![row("blobs", "good", false), row("blobs_ns1", "next", false)]
        );
        assert_eq!(
            committed.unwrap() as i64,
            lsn_of(&pool, "blobs_ns1", "next").await.unwrap()
        );
        assert!(known.contains("blobs_ns1"));
    }

    #[tokio::test]
    async fn known_tables_forgets_a_table_whose_creation_rolled_back() {
        let (_dir, pool) = create_test_pool().await;
        sqlx::query(
            "CREATE TRIGGER fail_bad BEFORE INSERT ON repl_log WHEN NEW.key = 'bad'
             BEGIN SELECT RAISE(ABORT, 'forced failure'); END",
        )
        .execute(&pool)
        .await
        .unwrap();
        let known = &mut blobs_only();

        let (op, rx) = hset("ns1", "bad");
        assert_eq!(run_batch(&pool, known, vec![op]).await, None);
        assert!(rx.await.unwrap().is_err());
        assert_eq!(*known, blobs_only());
        let tables: Vec<String> = sqlx::query_scalar(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'blobs_ns1'",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert!(tables.is_empty());
    }

    #[tokio::test]
    async fn known_tables_forgets_tables_added_by_a_batch_that_failed_to_commit() {
        let (_dir, pool) = create_test_pool().await;
        // A deferred foreign key that the log row of "boom" violates: the op
        // and its savepoint succeed, but the commit is refused.
        sqlx::query("PRAGMA foreign_keys = ON")
            .execute(&pool)
            .await
            .unwrap();
        for ddl in [
            "CREATE TABLE fk_parent (id INTEGER PRIMARY KEY)",
            "CREATE TABLE fk_child (p INTEGER REFERENCES fk_parent(id) DEFERRABLE INITIALLY DEFERRED)",
            "CREATE TRIGGER fail_commit AFTER INSERT ON repl_log WHEN NEW.key = 'boom'
             BEGIN INSERT INTO fk_child VALUES (999); END",
        ] {
            sqlx::query(ddl).execute(&pool).await.unwrap();
        }
        let known = &mut blobs_only();

        let (op, rx) = hset("ns1", "boom");
        assert_eq!(run_batch(&pool, known, vec![op]).await, None);
        let error = rx.await.unwrap().unwrap_err();
        assert!(error.contains("FOREIGN KEY"), "{error}");
        assert_eq!(*known, blobs_only());
        assert!(repl_log(&pool).await.is_empty());
    }

    #[tokio::test]
    async fn writer_publishes_the_highest_committed_lsn() {
        let (_dir, pool) = create_test_pool().await;
        let (sender, receiver) = mpsc::channel(8);
        let commit_watch = Arc::new(watch::channel(0).0);
        let writer = tokio::spawn(shard_writer_task(ShardWriter {
            shard_id: 0,
            pool: pool.clone(),
            receiver,
            batch_size: 8,
            batch_timeout_ms: 0,
            inflight_cache: Cache::new(16),
            inflight_hcache: Cache::new(16),
            metrics: Metrics::new(),
            commit_watch: commit_watch.clone(),
            shutdown: CancellationToken::new(),
        }));

        let mut seen = 0;
        for key in ["a", "b", "a"] {
            let (op, rx) = set(key);
            sender.send(op).await.unwrap();
            assert_eq!(rx.await.unwrap(), Ok(()));
            let published = *commit_watch.borrow();
            assert!(published > seen, "published {published} after {seen}");
            seen = published;
        }
        assert_eq!(seen as i64, lsn_of(&pool, "blobs", "a").await.unwrap());

        // A batch that logs nothing leaves the watermark alone.
        let (op, rx) = del("missing");
        sender.send(op).await.unwrap();
        assert_eq!(rx.await.unwrap(), Ok(()));
        assert_eq!(*commit_watch.borrow(), seen);

        drop(sender);
        writer.await.unwrap();
    }

    #[tokio::test]
    async fn cleanup_logs_tombstones_in_the_chunk_transaction() {
        let (_dir, pool) = create_test_pool().await;
        let known = &mut blobs_only();
        run_batch(&pool, known, vec![set("live").0, hset("ns", "old").0]).await;
        for table in ["blobs", "blobs_ns"] {
            for key in ["old1", "old2", "old3"] {
                sqlx::query(&format!(
                    "INSERT INTO {table} (key, data, created_at, updated_at, expires_at)
                     VALUES (?, x'00', 0, 0, 1)"
                ))
                .bind(key)
                .execute(&pool)
                .await
                .unwrap();
                sqlx::query(
                    "INSERT OR REPLACE INTO repl_log (tbl, key, deleted, at) VALUES (?, ?, 0, 0)",
                )
                .bind(table)
                .bind(key)
                .execute(&pool)
                .await
                .unwrap();
            }
        }
        let live = lsn_of(&pool, "blobs", "live").await.unwrap();
        let commit_watch = watch::channel(0).0;

        // Chunks of 2: two commits for three expired rows.
        let cancel = CancellationToken::new();
        let deleted = delete_expired(
            &pool,
            "blobs",
            10,
            2,
            &Counter::noop(),
            &commit_watch,
            &cancel,
        )
        .await;
        assert_eq!(deleted.unwrap(), 3);
        let log = repl_log(&pool).await;
        let tombstones: Vec<_> = log.iter().filter(|(_, _, deleted)| *deleted).collect();
        assert_eq!(tombstones.len(), 3);
        assert!(tombstones.iter().all(|(tbl, _, _)| tbl == "blobs"));
        let max_lsn: i64 = sqlx::query_scalar("SELECT MAX(lsn) FROM repl_log")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(*commit_watch.borrow() as i64, max_lsn);
        assert!(*commit_watch.borrow() as i64 > live);
        assert_eq!(lsn_of(&pool, "blobs", "live").await, Some(live));

        // The namespaced table's tombstones name it.
        let deleted = delete_expired(
            &pool,
            "blobs_ns",
            10,
            2,
            &Counter::noop(),
            &commit_watch,
            &cancel,
        )
        .await;
        assert_eq!(deleted.unwrap(), 3);
        assert!(lsn_of(&pool, "blobs_ns", "old1").await.is_some());
        assert!(
            repl_log(&pool)
                .await
                .contains(&row("blobs_ns", "old3", true))
        );
        assert!(
            repl_log(&pool)
                .await
                .contains(&row("blobs_ns", "old", false))
        );
    }

    #[tokio::test]
    async fn cleanup_stops_between_chunks_once_cancelled() {
        let (_dir, pool) = create_test_pool().await;
        sqlx::query(
            "INSERT INTO blobs (key, data, created_at, updated_at, expires_at)
             VALUES ('a', x'00', 0, 0, 1)",
        )
        .execute(&pool)
        .await
        .unwrap();
        let cancel = CancellationToken::new();
        cancel.cancel();

        let commit_watch = Arc::new(watch::channel(0).0);
        let deleted = delete_expired(
            &pool,
            "blobs",
            10,
            1,
            &Counter::noop(),
            &commit_watch,
            &cancel,
        )
        .await;
        assert_eq!(deleted.unwrap(), 0);

        // The task itself exits instead of waiting for its next sweep.
        timeout(
            Duration::from_secs(5),
            shard_cleanup_task(
                0,
                pool.clone(),
                3600,
                1,
                commit_watch,
                Arc::default(),
                86400,
                cancel,
            ),
        )
        .await
        .expect("cleanup task did not stop");
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM blobs")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 1);
    }
}
