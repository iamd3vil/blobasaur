use crate::metrics::Metrics;
use bytes::Bytes;
use chrono::Utc;
use moka::future::Cache;
use sqlx::SqlitePool;
use std::collections::{HashSet, VecDeque};
use tokio::sync::{mpsc, oneshot};
use tokio::time::{Duration, timeout, interval};

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
                &pool,
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
            &pool,
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
    let mut expire_results: Vec<(usize, bool)> = Vec::new();
    let mut sync_operations: Vec<usize> = Vec::new();

    // Execute all operations in the transaction
    for (idx, operation) in batch.iter().enumerate() {
        let result = match operation {
            ShardWriteOperation::Set { key, data, expires_at, .. }
            | ShardWriteOperation::SetAsync { key, data, expires_at } => {
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
            ShardWriteOperation::Expire { key, expires_at, .. } => {
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
                if let Some((_, success)) = expire_results.iter().find(|(idx, _)| *idx == operation_idx) {
                    let final_result = match &commit_result {
                        Ok(_) => Ok(*success),
                        Err(e) => Err(e.clone()),
                    };
                    let _ = responder.send(final_result);
                } else {
                    let _ = responder.send(Err("Internal error: could not find expire result".to_string()));
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
        }
        operation_idx += 1;
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
pub async fn shard_cleanup_task(
    shard_id: usize,
    pool: SqlitePool,
    cleanup_interval_secs: u64,
) {
    let mut interval = interval(Duration::from_secs(cleanup_interval_secs));
    
    tracing::info!("[Shard {}] Starting cleanup task with interval {} seconds", shard_id, cleanup_interval_secs);
    
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
                    tracing::info!("[Shard {}] Cleaned up {} expired keys from blobs table", shard_id, deleted_count);
                }
            }
            Err(e) => {
                tracing::error!("[Shard {}] Error during cleanup of blobs table: {}", shard_id, e);
            }
        }
        
        // Clean up expired keys from namespaced tables
        // First, get all table names that start with "blobs_"
        let tables_result = sqlx::query_as::<_, (String,)>(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'blobs_%'"
        )
        .fetch_all(&pool)
        .await;
        
        match tables_result {
            Ok(tables) => {
                for (table_name,) in tables {
                    let query = format!("DELETE FROM {} WHERE expires_at IS NOT NULL AND expires_at <= ?", table_name);
                    match sqlx::query(&query)
                        .bind(now)
                        .execute(&pool)
                        .await
                    {
                        Ok(result) => {
                            let deleted_count = result.rows_affected();
                            if deleted_count > 0 {
                                tracing::info!("[Shard {}] Cleaned up {} expired keys from table {}", shard_id, deleted_count, table_name);
                            }
                        }
                        Err(e) => {
                            tracing::error!("[Shard {}] Error during cleanup of table {}: {}", shard_id, table_name, e);
                        }
                    }
                }
            }
            Err(e) => {
                tracing::error!("[Shard {}] Error querying table names for cleanup: {}", shard_id, e);
            }
        }
    }
}
