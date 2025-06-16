use axum::body::Bytes;
use sqlx::SqlitePool;
use std::collections::VecDeque;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{Duration, timeout};

// Message type for writer consumers
pub enum ShardWriteOperation {
    Set {
        key: String,
        data: Bytes,
        responder: oneshot::Sender<Result<(), String>>,
    },
    SetAsync {
        key: String,
        data: Bytes,
    },
    Delete {
        key: String,
        responder: oneshot::Sender<Result<(), String>>,
    },
    DeleteAsync {
        key: String,
    },
}

// Enhanced consumer with batching support
pub async fn shard_writer_task(
    shard_id: usize,
    pool: SqlitePool,
    mut receiver: mpsc::Receiver<ShardWriteOperation>,
    batch_size: usize,
    batch_timeout_ms: u64,
) {
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
            process_batch(shard_id, &pool, &mut batch).await;
        }
    }

    // Process any remaining operations
    if !batch.is_empty() {
        process_batch(shard_id, &pool, &mut batch).await;
    }

    tracing::info!("Shard {} writer task stopped", shard_id);
}

async fn process_batch(
    shard_id: usize,
    pool: &SqlitePool,
    batch: &mut VecDeque<ShardWriteOperation>,
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
            // Send errors to all sync operations and clear batch
            for operation in batch.drain(..) {
                if let ShardWriteOperation::Set { responder, .. }
                | ShardWriteOperation::Delete { responder, .. } = operation
                {
                    let _ = responder.send(Err(format!("Transaction start failed: {}", e)));
                }
            }
            return;
        }
    };

    let mut results: Vec<(usize, Result<(), String>)> = Vec::new();
    let mut sync_operations: Vec<usize> = Vec::new();

    // Execute all operations in the transaction
    for (idx, operation) in batch.iter().enumerate() {
        let result = match operation {
            ShardWriteOperation::Set { key, data, .. }
            | ShardWriteOperation::SetAsync { key, data } => {
                match operation {
                    ShardWriteOperation::Set { .. } => sync_operations.push(idx),
                    _ => {}
                }

                sqlx::query("INSERT OR REPLACE INTO blobs (key, data) VALUES (?, ?)")
                    .bind(key)
                    .bind(data.as_ref())
                    .execute(&mut *tx)
                    .await
                    .map(|_| ())
                    .map_err(|e| {
                        tracing::error!("[Shard {}] SET error for key {}: {}", shard_id, key, e);
                        e.to_string()
                    })
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
        };

        results.push((idx, result));
    }

    // Commit transaction
    let commit_result = tx.commit().await.map_err(|e| {
        tracing::error!("[Shard {}] Transaction commit failed: {}", shard_id, e);
        e.to_string()
    });

    // Send responses to synchronous operations
    for operation in batch.drain(..) {
        match operation {
            ShardWriteOperation::Set { responder, .. }
            | ShardWriteOperation::Delete { responder, .. } => {
                let final_result = match &commit_result {
                    Ok(_) => Ok(()),
                    Err(e) => Err(e.clone()),
                };
                let _ = responder.send(final_result);
            }
            ShardWriteOperation::SetAsync { .. } | ShardWriteOperation::DeleteAsync { .. } => {
                // Async operations don't need responses, but log commit errors
                if let Err(e) = &commit_result {
                    tracing::error!(
                        "[Shard {}] ASYNC operation failed due to commit error: {}",
                        shard_id,
                        e
                    );
                }
            }
        }
    }
}
