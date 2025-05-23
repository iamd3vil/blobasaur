// In http_server.rs or similar

use async_compression::tokio::write::GzipEncoder;
use axum::{
    body::Bytes,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use futures::future::join_all;
use sqlx::SqlitePool;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode}; // Added for SQLite specific options
use std::fs; // Added for directory creation
use std::str::FromStr; // Added for SqliteConnectOptions
use std::{hash::Hasher, sync::Arc};
use tokio::io::AsyncWriteExt; // Required for GzipEncoder
use tokio::sync::{mpsc, oneshot}; // Added mpsc here as well for clarity, though oneshot was the primary addition for responders // Added for explicit type annotation

// Import ShardWriteOperation from shard_manager
use crate::{config::Cfg, shard_manager::ShardWriteOperation};

pub struct AppState {
    pub cfg: Cfg,
    pub shard_senders: Vec<mpsc::Sender<ShardWriteOperation>>,
    pub db_pools: Vec<SqlitePool>,
}

impl AppState {
    pub async fn new(
        cfg: Cfg,
        shard_receivers_out: &mut Vec<mpsc::Receiver<ShardWriteOperation>>,
    ) -> Self {
        let mut shard_senders_vec = Vec::new();
        // Clear the output vector first to ensure it's empty
        shard_receivers_out.clear();

        for _ in 0..cfg.num_shards {
            let (sender, receiver) = mpsc::channel(100);
            shard_senders_vec.push(sender);
            shard_receivers_out.push(receiver); // Populate the output vector with receivers
        }

        // Create data directory if it doesn't exist
        fs::create_dir_all(&cfg.data_dir).expect("Failed to create data directory");

        let mut db_pools_futures = vec![];
        for i in 0..cfg.num_shards {
            let data_dir = cfg.data_dir.clone();
            let db_path = format!("{}/shard_{}.db", data_dir, i);

            let mut connect_options =
                SqliteConnectOptions::from_str(&format!("sqlite:{}", db_path))
                    .expect(&format!(
                        "Failed to parse connection string for shard {}",
                        i
                    ))
                    .create_if_missing(true)
                    .journal_mode(SqliteJournalMode::Wal)
                    .busy_timeout(std::time::Duration::from_millis(5000));

            // These PRAGMAs are often set for performance with WAL mode.
            // `synchronous = OFF` is safe except for power loss.
            // `cache_size` is negative to indicate KiB, so -4000 is 4MB.
            // `temp_store = MEMORY` avoids disk I/O for temporary tables.
            connect_options = connect_options
                .pragma("synchronous", "OFF")
                .pragma("cache_size", "-100000") // 4MB cache per shard
                .pragma("temp_store", "MEMORY");

            db_pools_futures.push(sqlx::SqlitePool::connect_with(connect_options))
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

        for (i, pool) in db_pools.iter().enumerate() {
            sqlx::query("CREATE TABLE IF NOT EXISTS blobs (key TEXT PRIMARY KEY, data BLOB)")
                .execute(pool)
                .await
                .unwrap_or_else(|e| panic!("Failed to create table in shard {} DB: {}", i, e));
        }

        AppState {
            cfg,
            shard_senders: shard_senders_vec,
            db_pools,
        }
    }

    pub fn get_shard(&self, key: &str) -> usize {
        let mut hasher = fnv::FnvHasher::default();
        hasher.write(key.as_bytes());
        let hash = hasher.finish();
        hash as usize % self.cfg.num_shards
    }
}

// ShardWriteOperation enum has been moved to shard_manager.rs

pub async fn get_blob(
    Path(key): Path<String>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let shard_index = state.get_shard(&key);
    let pool = &state.db_pools[shard_index];

    match sqlx::query_as::<_, (Vec<u8>,)>("SELECT data FROM blobs WHERE key = ?")
        .bind(&key)
        .fetch_optional(pool)
        .await
    {
        Ok(Some(row)) => Ok(row.0),
        Ok(None) => Err(StatusCode::NOT_FOUND),
        Err(e) => {
            tracing::error!("Failed to GET blob {}: {}", key, e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

pub async fn set_blob(
    State(state): State<Arc<AppState>>,
    Path(key): Path<String>,
    body: Bytes,
) -> impl IntoResponse {
    let shard_index = state.get_shard(&key);
    let sender = &state.shard_senders[shard_index];

    let (responder_tx, responder_rx) = oneshot::channel();

    let compressed_body;

    // Compress the body is compression is enabled.
    if state.cfg.storage_compression.is_some_and(|v| v) {
        compressed_body = {
            let mut encoder = GzipEncoder::new(Vec::new());
            if let Err(e) = encoder.write_all(body.as_ref()).await {
                tracing::error!("Failed to write to GzipEncoder for key {}: {}", key, e);
                return StatusCode::INTERNAL_SERVER_ERROR;
            }
            if let Err(e) = encoder.shutdown().await {
                // Finish encoding and flush remaining data
                tracing::error!("Failed to shutdown GzipEncoder for key {}: {}", key, e);
                return StatusCode::INTERNAL_SERVER_ERROR;
            }
            Bytes::from(encoder.into_inner())
        };
    } else {
        // If compression is not enabled, use the original body
        compressed_body = body;
    }

    let operation = ShardWriteOperation::Set {
        key,
        data: compressed_body, // Use compressed body
        responder: responder_tx,
    };

    if sender.send(operation).await.is_err() {
        tracing::error!("Failed to send SET operation to shard {}", shard_index);
        return StatusCode::INTERNAL_SERVER_ERROR;
    }

    match responder_rx.await {
        Ok(Ok(())) => StatusCode::CREATED,
        Ok(Err(e)) => {
            tracing::error!("Shard writer failed for SET: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        }
        Err(_) => {
            tracing::error!("Shard writer task cancelled or panicked for SET");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

pub async fn delete_blob(
    State(state): State<Arc<AppState>>,
    Path(key): Path<String>,
) -> impl IntoResponse {
    let shard_index = state.get_shard(&key);
    let sender = &state.shard_senders[shard_index];

    let (responder_tx, responder_rx) = oneshot::channel();

    let operation = ShardWriteOperation::Delete {
        key,
        responder: responder_tx,
    };

    if sender.send(operation).await.is_err() {
        tracing::error!("Failed to send DELETE operation to shard {}", shard_index);
        return StatusCode::INTERNAL_SERVER_ERROR;
    }

    match responder_rx.await {
        Ok(Ok(())) => StatusCode::NO_CONTENT,
        Ok(Err(e)) => {
            tracing::error!("Shard writer failed for DELETE: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        }
        Err(_) => {
            tracing::error!("Shard writer task cancelled or panicked for DELETE");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}
