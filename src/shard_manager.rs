use chrono::Utc;
use moka::future::Cache;
use sqlx::SqlitePool;
use std::collections::{HashSet, VecDeque};
use tokio::sync::{mpsc, oneshot};
use tokio::time::{Duration, timeout};

// Message type for writer consumers
pub enum ShardWriteOperation {
    Set {
        key: String,
        data: Vec<u8>,
        responder: oneshot::Sender<Result<(), String>>,
    },
    SetAsync {
        key: String,
        data: Vec<u8>,
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
        data: Vec<u8>,
        responder: oneshot::Sender<Result<(), String>>,
    },
    HSetAsync {
        namespace: String,
        key: String,
        data: Vec<u8>,
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
}

// Enhanced consumer with batching support
pub async fn shard_writer_task(
    shard_id: usize,
    pool: SqlitePool,
    mut receiver: mpsc::Receiver<ShardWriteOperation>,
    batch_size: usize,
    batch_timeout_ms: u64,
    inflight_cache: Cache<String, Vec<u8>>,
    inflight_hcache: Cache<String, Vec<u8>>,
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
            process_batch(
                shard_id,
                &pool,
                &mut batch,
                &mut known_tables,
                &inflight_cache,
                &inflight_hcache,
            )
            .await;
        }
    }

    // Process any remaining operations
    if !batch.is_empty() {
        process_batch(
            shard_id,
            &pool,
            &mut batch,
            &mut known_tables,
            &inflight_cache,
            &inflight_hcache,
        )
        .await;
    }

    tracing::info!("Shard {} writer task stopped", shard_id);
}

async fn process_batch(
    shard_id: usize,
    pool: &SqlitePool,
    batch: &mut VecDeque<ShardWriteOperation>,
    known_tables: &mut HashSet<String>,
    inflight_cache: &Cache<String, Vec<u8>>,
    inflight_hcache: &Cache<String, Vec<u8>>,
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

                let now = Utc::now().timestamp();

                // Check if record exists to determine if this is an insert or update
                let exists = sqlx::query("SELECT 1 FROM blobs WHERE key = ?")
                    .bind(key)
                    .fetch_optional(&mut *tx)
                    .await
                    .map(|row| row.is_some())
                    .unwrap_or(false);

                if exists {
                    // Update existing record - only update data, updated_at, and version
                    sqlx::query("UPDATE blobs SET data = ?, updated_at = ?, version = version + 1 WHERE key = ?")
                        .bind(&data[..])
                        .bind(now)
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
                    sqlx::query("INSERT INTO blobs (key, data, created_at, updated_at, expires_at, version) VALUES (?, ?, ?, ?, NULL, 0)")
                        .bind(key)
                        .bind(&data[..])
                        .bind(now)
                        .bind(now)
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
    for operation in batch.drain(..) {
        match operation {
            ShardWriteOperation::Set { responder, .. }
            | ShardWriteOperation::Delete { responder, .. }
            | ShardWriteOperation::HSet { responder, .. }
            | ShardWriteOperation::HDelete { responder, .. } => {
                let final_result = match &commit_result {
                    Ok(_) => Ok(()),
                    Err(e) => Err(e.clone()),
                };
                let _ = responder.send(final_result);
            }
            ShardWriteOperation::SetAsync { .. }
            | ShardWriteOperation::DeleteAsync { .. }
            | ShardWriteOperation::HSetAsync { .. }
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
        }
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
            known_tables.insert(table_name.to_string());
            tracing::info!("Created namespaced table: {}", table_name);
            Ok(())
        }
        Err(e) => {
            tracing::error!("Failed to create namespaced table {}: {}", table_name, e);
            Err(format!("Failed to create table: {}", e))
        }
    }
}
