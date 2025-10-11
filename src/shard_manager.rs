use crate::metrics::Metrics;
use crate::storage::Storage;
use bytes::Bytes;
use chrono::Utc;
use moka::future::Cache;
use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
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
}

// Enhanced consumer with batching support
pub async fn shard_writer_task(
    shard_id: usize,
    store: Arc<dyn Storage>,
    mut receiver: mpsc::Receiver<ShardWriteOperation>,
    batch_size: usize,
    batch_timeout_ms: u64,
    inflight_cache: Cache<String, Bytes>,
    inflight_hcache: Cache<String, Bytes>,
    metrics: Metrics,
) {
    // Load existing namespaced tables into memory
    let mut known_tables = load_existing_tables(store.as_ref(), shard_id).await;
    let batch_timeout = Duration::from_millis(batch_timeout_ms);

    tracing::info!(
        "Shard {} writer task started (batch_size={}, timeout={}ms)",
        shard_id,
        batch_size,
        batch_timeout.as_millis()
    );

    let mut batch: VecDeque<ShardWriteOperation> = VecDeque::with_capacity(batch_size);

    loop {
        // Collect operations for batching
        if batch.is_empty() {
            // If batch is empty, wait indefinitely for first operation
            match receiver.recv().await {
                Some(op) => {
                    batch.push_back(op);
                }
                None => break, // Channel closed
            }
        } else {
            // If batch has items, wait with timeout for more operations
            match timeout(batch_timeout, receiver.recv()).await {
                Ok(Some(operation)) => {
                    batch.push_back(operation);
                }
                Ok(None) => break, // Channel closed
                Err(_) => {}       // Timeout - process current batch
            }
        }

        // Continue collecting until batch is full or no more immediate operations
        while batch.len() < batch_size {
            match receiver.try_recv() {
                Ok(op) => batch.push_back(op),
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => break,
            }
        }

        if !batch.is_empty() {
            let batch_start = std::time::Instant::now();
            process_batch(
                shard_id,
                store.as_ref(),
                &mut batch,
                &mut known_tables,
                &inflight_cache,
                &inflight_hcache,
            )
            .await;
            let batch_duration = batch_start.elapsed();
            metrics.record_batch_operation(batch.len(), batch_duration);
        }
    }

    // Process any remaining operations
    if !batch.is_empty() {
        let batch_start = std::time::Instant::now();
        process_batch(
            shard_id,
            store.as_ref(),
            &mut batch,
            &mut known_tables,
            &inflight_cache,
            &inflight_hcache,
        )
        .await;
        let batch_duration = batch_start.elapsed();
        metrics.record_batch_operation(batch.len(), batch_duration);
    }

    tracing::info!("Shard {} writer task stopped", shard_id);
}

async fn process_batch(
    shard_id: usize,
    store: &dyn Storage,
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

    let mut tx = match store.begin_transaction().await {
        Ok(tx) => tx,
        Err(e) => {
            tracing::error!("[Shard {}] Failed to start transaction: {}", shard_id, e);
            for operation in batch.drain(..) {
                match operation {
                    ShardWriteOperation::Set { responder, .. }
                    | ShardWriteOperation::Delete { responder, .. }
                    | ShardWriteOperation::HSet { responder, .. }
                    | ShardWriteOperation::HSetEx { responder, .. }
                    | ShardWriteOperation::HDelete { responder, .. } => {
                        let _ = responder.send(Err(format!("Transaction start failed: {}", e)));
                    }
                    ShardWriteOperation::Expire { responder, .. } => {
                        let _ = responder.send(Err(format!("Transaction start failed: {}", e)));
                    }
                    _ => {}
                }
            }
            return;
        }
    };

    let mut expire_results: Vec<(usize, bool)> = Vec::new();
    let mut first_error: Option<String> = None;

    for (idx, operation) in batch.iter().enumerate() {
        let result: Result<(), String> = match operation {
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
                let now = Utc::now().timestamp();
                tx.upsert_blob(key, data.as_ref(), *expires_at, now)
                    .await
                    .map_err(|e| {
                        let msg =
                            format!("[Shard {}] Failed to upsert key {}: {}", shard_id, key, e);
                        tracing::error!("{}", msg);
                        msg
                    })
            }
            ShardWriteOperation::Delete { key, .. } | ShardWriteOperation::DeleteAsync { key } => {
                tx.delete_blob(key).await.map_err(|e| {
                    let msg = format!("[Shard {}] DELETE error for key {}: {}", shard_id, key, e);
                    tracing::error!("{}", msg);
                    msg
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
                let table_name = format!("blobs_{}", namespace);
                let ensure = if !known_tables.contains(&table_name) {
                    match tx.ensure_namespace(namespace).await {
                        Ok(_) => {
                            known_tables.insert(table_name.clone());
                            Ok(())
                        }
                        Err(e) => {
                            let msg = format!(
                                "[Shard {}] Failed to ensure namespace {} exists: {}",
                                shard_id, namespace, e
                            );
                            tracing::error!("{}", msg);
                            Err(msg)
                        }
                    }
                } else {
                    Ok(())
                };

                if let Err(err) = ensure {
                    Err(err)
                } else {
                    let now = Utc::now().timestamp();
                    tx.upsert_namespace(namespace, key, data.as_ref(), None, now)
                        .await
                        .map_err(|e| {
                            let msg = format!(
                                "[Shard {}] HSET error for namespace {} key {}: {}",
                                shard_id, namespace, key, e
                            );
                            tracing::error!("{}", msg);
                            msg
                        })
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
                let table_name = format!("blobs_{}", namespace);
                let ensure = if !known_tables.contains(&table_name) {
                    match tx.ensure_namespace(namespace).await {
                        Ok(_) => {
                            known_tables.insert(table_name.clone());
                            Ok(())
                        }
                        Err(e) => {
                            let msg = format!(
                                "[Shard {}] Failed to ensure namespace {} exists: {}",
                                shard_id, namespace, e
                            );
                            tracing::error!("{}", msg);
                            Err(msg)
                        }
                    }
                } else {
                    Ok(())
                };

                if let Err(err) = ensure {
                    Err(err)
                } else {
                    let now = Utc::now().timestamp();
                    tx.upsert_namespace(namespace, key, data.as_ref(), Some(*expires_at), now)
                        .await
                        .map_err(|e| {
                            let msg = format!(
                                "[Shard {}] HSETEX error for namespace {} key {}: {}",
                                shard_id, namespace, key, e
                            );
                            tracing::error!("{}", msg);
                            msg
                        })
                }
            }
            ShardWriteOperation::HDelete { namespace, key, .. }
            | ShardWriteOperation::HDeleteAsync { namespace, key } => {
                let table_name = format!("blobs_{}", namespace);
                if known_tables.contains(&table_name) {
                    tx.delete_namespace(namespace, key).await.map_err(|e| {
                        let msg = format!(
                            "[Shard {}] HDEL error for namespace {} key {}: {}",
                            shard_id, namespace, key, e
                        );
                        tracing::error!("{}", msg);
                        msg
                    })
                } else {
                    Ok(())
                }
            }
            ShardWriteOperation::Expire {
                key, expires_at, ..
            } => match tx.set_blob_expiry(key, *expires_at).await {
                Ok(updated) => {
                    expire_results.push((idx, updated));
                    Ok(())
                }
                Err(e) => {
                    let msg = format!("[Shard {}] EXPIRE error for key {}: {}", shard_id, key, e);
                    tracing::error!("{}", msg);
                    expire_results.push((idx, false));
                    Err(msg)
                }
            },
        };

        if let Err(err) = result {
            if first_error.is_none() {
                first_error = Some(err);
            }
        }
    }

    let transaction_result: Result<(), String> = if let Some(err) = first_error {
        match tx.rollback().await {
            Ok(_) => Err(err),
            Err(e) => {
                tracing::error!("[Shard {}] Transaction rollback failed: {}", shard_id, e);
                Err(e.to_string())
            }
        }
    } else {
        tx.commit().await.map_err(|e| {
            tracing::error!("[Shard {}] Transaction commit failed: {}", shard_id, e);
            e.to_string()
        })
    };

    if transaction_result.is_ok() {
        for operation in batch.iter() {
            match operation {
                ShardWriteOperation::SetAsync { key, .. }
                | ShardWriteOperation::DeleteAsync { key } => {
                    inflight_cache.invalidate(key).await;
                }
                ShardWriteOperation::HSetAsync { namespace, key, .. }
                | ShardWriteOperation::HSetExAsync { namespace, key, .. }
                | ShardWriteOperation::HDeleteAsync { namespace, key } => {
                    let namespaced_key = format!("{}:{}", namespace, key);
                    inflight_hcache.invalidate(&namespaced_key).await;
                }
                _ => {}
            }
        }
    }

    let mut operation_idx = 0;
    for operation in batch.drain(..) {
        match operation {
            ShardWriteOperation::Set { responder, .. }
            | ShardWriteOperation::Delete { responder, .. }
            | ShardWriteOperation::HSet { responder, .. }
            | ShardWriteOperation::HSetEx { responder, .. }
            | ShardWriteOperation::HDelete { responder, .. } => {
                let _ = responder.send(transaction_result.clone());
            }
            ShardWriteOperation::Expire { responder, .. } => {
                if let Some((_, success)) =
                    expire_results.iter().find(|(idx, _)| *idx == operation_idx)
                {
                    let result = transaction_result.clone().map(|_| *success);
                    let _ = responder.send(result);
                } else {
                    let _ = responder.send(Err(
                        "Internal error: could not find expire result".to_string()
                    ));
                }
            }
            ShardWriteOperation::SetAsync { .. }
            | ShardWriteOperation::DeleteAsync { .. }
            | ShardWriteOperation::HSetAsync { .. }
            | ShardWriteOperation::HSetExAsync { .. }
            | ShardWriteOperation::HDeleteAsync { .. } => {
                if let Err(e) = &transaction_result {
                    tracing::error!(
                        "[Shard {}] ASYNC operation failed due to transaction error: {}",
                        shard_id,
                        e
                    );
                }
            }
        }
        operation_idx += 1;
    }
}

async fn load_existing_tables(store: &dyn Storage, shard_id: usize) -> HashSet<String> {
    match store.list_namespace_tables().await {
        Ok(tables) => {
            tracing::info!(
                "[Shard {}] Loaded {} existing namespaced tables",
                shard_id,
                tables.len().saturating_sub(1)
            );
            tables
        }
        Err(e) => {
            tracing::error!("[Shard {}] Failed to load existing tables: {}", shard_id, e);
            let mut tables = HashSet::new();
            tables.insert("blobs".to_string());
            tables
        }
    }
}

/// Background task to clean up expired keys from a shard
pub async fn shard_cleanup_task(
    shard_id: usize,
    store: Arc<dyn Storage>,
    cleanup_interval_secs: u64,
) {
    let mut interval = interval(Duration::from_secs(cleanup_interval_secs));

    tracing::info!(
        "[Shard {}] Starting cleanup task with interval {} seconds",
        shard_id,
        cleanup_interval_secs
    );

    loop {
        interval.tick().await;

        let now = chrono::Utc::now().timestamp();

        match store.delete_expired_main(now).await {
            Ok(deleted) if deleted > 0 => {
                tracing::info!(
                    "[Shard {}] Cleaned up {} expired keys from blobs table",
                    shard_id,
                    deleted
                );
            }
            Ok(_) => {}
            Err(e) => {
                tracing::error!(
                    "[Shard {}] Error during cleanup of blobs table: {}",
                    shard_id,
                    e
                );
            }
        }

        match store.list_namespace_tables().await {
            Ok(tables) => {
                for table_name in tables {
                    if table_name == "blobs" {
                        continue;
                    }
                    if let Some(namespace) = table_name.strip_prefix("blobs_") {
                        match store.delete_expired_namespace(namespace, now).await {
                            Ok(deleted) if deleted > 0 => {
                                tracing::info!(
                                    "[Shard {}] Cleaned up {} expired keys from table {}",
                                    shard_id,
                                    deleted,
                                    table_name
                                );
                            }
                            Ok(_) => {}
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
