use crate::metrics::{Metrics, ShardMetrics};
use crate::redis::protocol::HExpireCondition;
use bytes::Bytes;
use chrono::Utc;
use metrics::Counter;
use moka::future::Cache;
use moka::ops::compute::Op;
use sqlx::{Connection, Sqlite, SqlitePool, pool::PoolConnection};
use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{mpsc, oneshot};
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
                    if matches!(operation, ShardWriteOperation::Vacuum { .. }) {
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
        // Vacuum operations are ordering barriers, so stop collecting when one is observed.
        if pending_maintenance_operation.is_none() {
            while batch.len() < batch_size {
                match receiver.try_recv() {
                    Ok(operation) => {
                        if matches!(operation, ShardWriteOperation::Vacuum { .. }) {
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

                let table_name = format!("blobs_{}", namespace);

                // Ensure table exists
                if let Err(e) =
                    ensure_namespaced_table_exists(&mut tx, &table_name, known_tables).await
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

                let table_name = format!("blobs_{}", namespace);

                // Ensure table exists
                if let Err(e) =
                    ensure_namespaced_table_exists(&mut tx, &table_name, known_tables).await
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

                let table_name = format!("blobs_{}", namespace);

                // Only delete if table exists
                if known_tables.contains(&table_name) {
                    let delete_query = format!("DELETE FROM {} WHERE key = ?", table_name);
                    sqlx::query(&delete_query)
                        .bind(key)
                        .execute(&mut *tx)
                        .await
                        .map(|_| ())
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
                    Ok(())
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
                    known_tables,
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

// Ensure a namespaced table exists, creating it if necessary
async fn ensure_namespaced_table_exists(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    table_name: &str,
    known_tables: &mut HashSet<String>,
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
}
