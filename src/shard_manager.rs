use crate::metrics::Metrics;
use crate::redis::protocol::HExpireCondition;
use bytes::Bytes;
use chrono::Utc;
use moka::future::Cache;
use sqlx::SqlitePool;
use std::collections::{HashSet, VecDeque};
use tokio::sync::{mpsc, oneshot};
use tokio::time::{Duration, interval, timeout};

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

// Enhanced consumer with batching support
pub async fn shard_writer_task(
    shard_id: usize,
    pool: SqlitePool,
    mut receiver: mpsc::Receiver<ShardWriteOperation>,
    batch_size: usize,
    batch_timeout_ms: u64,
    inflight_cache: Cache<String, Bytes>,
    inflight_hcache: Cache<String, Bytes>,
    metrics: Metrics,
) {
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
                None => receiver.recv().await,
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
            match timeout(batch_timeout, receiver.recv()).await {
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
            )
            .await;
            let batch_duration = batch_start.elapsed();
            metrics.record_batch_operation(processed_batch_size, batch_duration);
        }
    }

    // Process any remaining operations
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
        )
        .await;
        let batch_duration = batch_start.elapsed();
        metrics.record_batch_operation(processed_batch_size, batch_duration);
    }

    tracing::info!("Shard {} writer task stopped", shard_id);
}

async fn process_batch(
    shard_id: usize,
    pool: &SqlitePool,
    batch: &mut VecDeque<ShardWriteOperation>,
    known_tables: &mut HashSet<String>,
    inflight_cache: &Cache<String, Bytes>,
    inflight_hcache: &Cache<String, Bytes>,
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
    let mut tx = match pool.begin().await {
        Ok(tx) => tx,
        Err(e) => {
            tracing::error!("[Shard {}] Failed to start transaction: {}", shard_id, e);
            let error_message = format!("Transaction start failed: {}", e);

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
                    | ShardWriteOperation::HDeleteAsync { .. } => {}
                }
            }
            return;
        }
    };

    let mut results: Vec<(usize, Result<(), String>)> = Vec::new();
    let mut expire_results: Vec<(usize, bool)> = Vec::new();
    let mut hexpire_results: Vec<(usize, i64)> = Vec::new();
    let mut sync_operations: Vec<usize> = Vec::new();

    // Execute all operations in the transaction
    for (idx, operation) in batch.iter().enumerate() {
        let result = match operation {
            ShardWriteOperation::Set {
                key,
                data,
                expires_at,
                ..
            }
            | ShardWriteOperation::SetAsync {
                key,
                data,
                expires_at,
            } => {
                match operation {
                    ShardWriteOperation::Set { .. } => sync_operations.push(idx),
                    _ => {}
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
                        .bind(&data[..])
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
                        .bind(&data[..])
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
            ShardWriteOperation::Delete { key, .. } | ShardWriteOperation::DeleteAsync { key } => {
                match operation {
                    ShardWriteOperation::Delete { .. } => sync_operations.push(idx),
                    _ => {}
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
            ShardWriteOperation::HSet {
                namespace,
                key,
                data,
                ..
            }
            | ShardWriteOperation::HSetAsync {
                namespace,
                key,
                data,
            } => {
                match operation {
                    ShardWriteOperation::HSet { .. } => sync_operations.push(idx),
                    _ => {}
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
                            .bind(&data[..])
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
                            .bind(&data[..])
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
                data,
                expires_at,
                ..
            }
            | ShardWriteOperation::HSetExAsync {
                namespace,
                key,
                data,
                expires_at,
            } => {
                match operation {
                    ShardWriteOperation::HSetEx { .. } => sync_operations.push(idx),
                    _ => {}
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
                            .bind(&data[..])
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
                            .bind(&data[..])
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
            | ShardWriteOperation::HDeleteAsync { namespace, key } => {
                match operation {
                    ShardWriteOperation::HDelete { .. } => sync_operations.push(idx),
                    _ => {}
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

                let table_name = format!("blobs_{}", namespace);

                // Ensure table exists
                if let Err(e) =
                    ensure_namespaced_table_exists(&mut tx, &table_name, known_tables).await
                {
                    tracing::error!("[Shard {}] Failed to ensure table exists: {}", shard_id, e);
                    hexpire_results.push((idx, -2));
                    Err(e)
                } else {
                    // Check if field exists and get current expires_at
                    let select_query = format!(
                        "SELECT expires_at FROM {} WHERE key = ?",
                        table_name
                    );
                    match sqlx::query_as::<_, (Option<i64>,)>(&select_query)
                        .bind(key)
                        .fetch_optional(&mut *tx)
                        .await
                    {
                        Ok(None) => {
                            // Field does not exist
                            hexpire_results.push((idx, -2));
                            Ok(())
                        }
                        Ok(Some((current_expires_at,))) => {
                            // Check if seconds is 0 or expires_at is in the past -> delete
                            let now = Utc::now().timestamp();
                            if *expires_at <= now {
                                // Delete the field
                                let delete_query = format!(
                                    "DELETE FROM {} WHERE key = ?",
                                    table_name
                                );
                                match sqlx::query(&delete_query)
                                    .bind(key)
                                    .execute(&mut *tx)
                                    .await
                                {
                                    Ok(_) => {
                                        hexpire_results.push((idx, 2));
                                        Ok(())
                                    }
                                    Err(e) => {
                                        tracing::error!(
                                            "[Shard {}] HEXPIRE DELETE error for namespace {} key {}: {}",
                                            shard_id, namespace, key, e
                                        );
                                        hexpire_results.push((idx, -2));
                                        Err(e.to_string())
                                    }
                                }
                            } else {
                                // Apply condition check
                                let should_set = match condition {
                                    None => true,
                                    Some(HExpireCondition::Nx) => current_expires_at.is_none(),
                                    Some(HExpireCondition::Xx) => current_expires_at.is_some(),
                                    Some(HExpireCondition::Gt) => {
                                        match current_expires_at {
                                            None => false, // Non-volatile = infinite TTL, new < infinite
                                            Some(current) => *expires_at > current,
                                        }
                                    }
                                    Some(HExpireCondition::Lt) => {
                                        match current_expires_at {
                                            None => true, // Non-volatile = infinite TTL, new < infinite
                                            Some(current) => *expires_at < current,
                                        }
                                    }
                                };

                                if should_set {
                                    let update_query = format!(
                                        "UPDATE {} SET expires_at = ? WHERE key = ?",
                                        table_name
                                    );
                                    match sqlx::query(&update_query)
                                        .bind(expires_at)
                                        .bind(key)
                                        .execute(&mut *tx)
                                        .await
                                    {
                                        Ok(_) => {
                                            hexpire_results.push((idx, 1));
                                            Ok(())
                                        }
                                        Err(e) => {
                                            tracing::error!(
                                                "[Shard {}] HEXPIRE UPDATE error for namespace {} key {}: {}",
                                                shard_id, namespace, key, e
                                            );
                                            hexpire_results.push((idx, 0));
                                            Err(e.to_string())
                                        }
                                    }
                                } else {
                                    // Condition not met
                                    hexpire_results.push((idx, 0));
                                    Ok(())
                                }
                            }
                        }
                        Err(e) => {
                            tracing::error!(
                                "[Shard {}] HEXPIRE SELECT error for namespace {} key {}: {}",
                                shard_id, namespace, key, e
                            );
                            hexpire_results.push((idx, -2));
                            Err(e.to_string())
                        }
                    }
                }
            }
            ShardWriteOperation::Vacuum { .. } => {
                Err("Vacuum operation reached transactional batch unexpectedly".to_string())
            }
        };

        results.push((idx, result));
    }

    // Commit transaction
    let commit_result = tx.commit().await.map_err(|e| {
        tracing::error!("[Shard {}] Transaction commit failed: {}", shard_id, e);
        e.to_string()
    });

    // Clean up inflight cache entries after successful database commit.
    // This prevents memory leaks and ensures the cache doesn't grow indefinitely.
    // The inflight cache is used to prevent race conditions in async write mode:
    // when a SET returns OK immediately, subsequent GET requests check the cache
    // first before hitting the database. Once the write is committed, we can
    // safely remove the entry from the cache since it's now in the database.
    if commit_result.is_ok() {
        for operation in batch.iter() {
            match operation {
                ShardWriteOperation::SetAsync { key, .. } => {
                    inflight_cache.invalidate(key).await;
                }
                ShardWriteOperation::DeleteAsync { key } => {
                    // Also clean up any SET operations that might have been overridden
                    inflight_cache.invalidate(key).await;
                }
                ShardWriteOperation::HSetAsync { namespace, key, .. } => {
                    let namespaced_key = format!("{}:{}", namespace, key);
                    inflight_hcache.invalidate(&namespaced_key).await;
                }
                ShardWriteOperation::HSetExAsync { namespace, key, .. } => {
                    let namespaced_key = format!("{}:{}", namespace, key);
                    inflight_hcache.invalidate(&namespaced_key).await;
                }
                ShardWriteOperation::HDeleteAsync { namespace, key } => {
                    // Also clean up any HSET operations that might have been overridden
                    let namespaced_key = format!("{}:{}", namespace, key);
                    inflight_hcache.invalidate(&namespaced_key).await;
                }
                _ => {} // Only async operations use the inflight cache
            }
        }
    }

    // Send responses to synchronous operations
    let mut operation_idx = 0;
    for operation in batch.drain(..) {
        match operation {
            ShardWriteOperation::Set { responder, .. }
            | ShardWriteOperation::Delete { responder, .. }
            | ShardWriteOperation::HSet { responder, .. }
            | ShardWriteOperation::HSetEx { responder, .. }
            | ShardWriteOperation::HDelete { responder, .. } => {
                let final_result = match &commit_result {
                    Ok(_) => Ok(()),
                    Err(e) => Err(e.clone()),
                };
                let _ = responder.send(final_result);
            }
            ShardWriteOperation::Expire { responder, .. } => {
                // For expire operations, we need to send the actual result (bool)
                // Find the corresponding expire result using current operation index
                if let Some((_, success)) =
                    expire_results.iter().find(|(idx, _)| *idx == operation_idx)
                {
                    let final_result = match &commit_result {
                        Ok(_) => Ok(*success),
                        Err(e) => Err(e.clone()),
                    };
                    let _ = responder.send(final_result);
                } else {
                    let _ = responder.send(Err(
                        "Internal error: could not find expire result".to_string()
                    ));
                }
            }
            ShardWriteOperation::HExpire { responder, .. } => {
                // For hexpire operations, send the per-field result code
                if let Some((_, result_code)) =
                    hexpire_results.iter().find(|(idx, _)| *idx == operation_idx)
                {
                    let final_result = match &commit_result {
                        Ok(_) => Ok(*result_code),
                        Err(e) => Err(e.clone()),
                    };
                    let _ = responder.send(final_result);
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
        operation_idx += 1;
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
        if let Err(e) = sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
            .execute(pool)
            .await
        {
            errors.push(format!("wal_checkpoint(TRUNCATE) failed: {}", e));
        }

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
        match (before, after) {
            (Some(before_stats), Some(after_stats)) => Some(
                before_stats
                    .freelist_count
                    .saturating_sub(after_stats.freelist_count),
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

/// Background task to clean up expired keys from a shard
pub async fn shard_cleanup_task(shard_id: usize, pool: SqlitePool, cleanup_interval_secs: u64) {
    let mut interval = interval(Duration::from_secs(cleanup_interval_secs));

    tracing::info!(
        "[Shard {}] Starting cleanup task with interval {} seconds",
        shard_id,
        cleanup_interval_secs
    );

    loop {
        interval.tick().await;

        let now = chrono::Utc::now().timestamp();

        // Clean up expired keys from the main blobs table
        match sqlx::query("DELETE FROM blobs WHERE expires_at IS NOT NULL AND expires_at <= ?")
            .bind(now)
            .execute(&pool)
            .await
        {
            Ok(result) => {
                let deleted_count = result.rows_affected();
                if deleted_count > 0 {
                    tracing::info!(
                        "[Shard {}] Cleaned up {} expired keys from blobs table",
                        shard_id,
                        deleted_count
                    );
                }
            }
            Err(e) => {
                tracing::error!(
                    "[Shard {}] Error during cleanup of blobs table: {}",
                    shard_id,
                    e
                );
            }
        }

        // Clean up expired keys from namespaced tables
        // First, get all table names that start with "blobs_"
        let tables_result = sqlx::query_as::<_, (String,)>(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'blobs_%'",
        )
        .fetch_all(&pool)
        .await;

        match tables_result {
            Ok(tables) => {
                for (table_name,) in tables {
                    let query = format!(
                        "DELETE FROM {} WHERE expires_at IS NOT NULL AND expires_at <= ?",
                        table_name
                    );
                    match sqlx::query(&query).bind(now).execute(&pool).await {
                        Ok(result) => {
                            let deleted_count = result.rows_affected();
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
                        }
                    }
                }
            }
            Err(e) => {
                tracing::error!(
                    "[Shard {}] Error querying table names for cleanup: {}",
                    shard_id,
                    e
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
    use std::str::FromStr;
    use tempfile::TempDir;

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
        let writer_handle = tokio::spawn(shard_writer_task(
            0,
            pool.clone(),
            receiver,
            64,
            10,
            Cache::new(128),
            Cache::new(128),
            Metrics::new(),
        ));

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
}
