use axum::body::Bytes;
use sqlx::SqlitePool;
use tokio::sync::{mpsc, oneshot};

// Message type for writer consumers
pub enum ShardWriteOperation {
    Set {
        key: String,
        data: Bytes,
        responder: oneshot::Sender<Result<(), String>>,
    },
    Delete {
        key: String,
        responder: oneshot::Sender<Result<(), String>>,
    },
}

// Simplified consumer
pub async fn shard_writer_task(
    shard_id: usize,
    pool: SqlitePool,
    mut receiver: mpsc::Receiver<ShardWriteOperation>,
) {
    tracing::info!("Shard {} writer task started", shard_id);
    while let Some(operation) = receiver.recv().await {
        match operation {
            ShardWriteOperation::Set {
                key,
                data,
                responder,
            } => {
                let res = sqlx::query("INSERT OR REPLACE INTO blobs (key, data) VALUES (?, ?)")
                    .bind(&key)
                    .bind(data.as_ref()) // Convert Bytes to &[u8]
                    .execute(&pool)
                    .await
                    .map(|_| ())
                    .map_err(|e| {
                        tracing::error!("[Shard {}] SET error for key {}: {}", shard_id, key, e);
                        e.to_string()
                    });
                let _ = responder.send(res);
            }
            ShardWriteOperation::Delete { key, responder } => {
                let res = sqlx::query("DELETE FROM blobs WHERE key = ?")
                    .bind(&key)
                    .execute(&pool)
                    .await
                    .map(|_| ())
                    .map_err(|e| {
                        tracing::error!("[Shard {}] DELETE error for key {}: {}", shard_id, key, e);
                        e.to_string()
                    });
                let _ = responder.send(res);
            }
        }
    }
    tracing::info!("Shard {} writer task stopped", shard_id);
}
