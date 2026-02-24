use crate::AppState;
use crate::cluster::ClusterManager;
use crate::metrics::Timer;
use crate::redis::{
    HExpireCondition, ParseError, RedisCommand, VacuumCommandMode, VacuumShardTarget,
    parse_command, parse_resp_with_remaining, serialize_frame,
};
use crate::shard_manager::{ShardWriteOperation, VacuumMode, VacuumResult, VacuumStats};
use bytes::Bytes;
use redis_protocol::resp2::types::BytesFrame;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

pub async fn run_redis_server(
    state: Arc<AppState>,
    addr: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind(addr).await?;
    tracing::info!("Blobasaur server listening on {}", addr);

    loop {
        let (stream, addr) = listener.accept().await?;
        tracing::info!("Accepted new connection from {}", addr);
        tracing::debug!("New Blobasaur connection from {}", addr);

        let state_clone = state.clone();
        // Record new connection
        state.metrics.record_connection();

        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, state_clone.clone()).await {
                tracing::error!("Error handling connection from {}: {}", addr, e);
                state_clone.metrics.record_error("connection");
            }
            // Record dropped connection
            state_clone.metrics.record_connection_dropped();
        });
    }
}

async fn handle_connection(
    mut stream: TcpStream,
    state: Arc<AppState>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut buffer = Vec::new();
    let mut temp_buffer = vec![0; 4096];

    loop {
        let n = match stream.read(&mut temp_buffer).await {
            Ok(0) => return Ok(()), // Connection closed
            Ok(n) => n,
            Err(e) => {
                tracing::error!("Failed to read from socket: {}", e);
                return Err(Box::new(e));
            }
        };

        buffer.extend_from_slice(&temp_buffer[..n]);
        tracing::debug!(
            "Read {} bytes from socket, buffer size is now {}",
            n,
            buffer.len()
        );

        // Try to parse complete messages from the buffer
        let mut remaining_data = &buffer[..];

        while !remaining_data.is_empty() {
            match parse_resp_with_remaining(remaining_data) {
                Ok((resp_value, remaining)) => {
                    remaining_data = remaining;

                    // Parse and handle the command
                    match parse_command(resp_value) {
                        Ok(command) => {
                            if let Err(e) = handle_redis_command(&mut stream, &state, command).await
                            {
                                tracing::error!("Error handling command: {}", e);
                                return Err(e);
                            }
                        }
                        Err(ParseError::Invalid(msg)) => {
                            tracing::warn!("Invalid command: {}", msg);
                            let error_resp = BytesFrame::Error(format!("ERR {}", msg).into());
                            stream.write_all(&serialize_frame(&error_resp)).await?;
                        }
                        Err(e) => {
                            tracing::error!("Command parse error: {}", e);
                            let error_resp = BytesFrame::Error("ERR protocol error".into());
                            stream.write_all(&serialize_frame(&error_resp)).await?;
                        }
                    }
                }
                Err(ParseError::Incomplete) => {
                    // Need more data, keep remaining data in buffer
                    break;
                }
                Err(ParseError::Invalid(msg)) => {
                    tracing::warn!("Protocol error: {}", msg);
                    let error_resp = BytesFrame::Error(format!("ERR {}", msg).into());
                    stream.write_all(&serialize_frame(&error_resp)).await?;
                    // Skip one byte to try to recover
                    if !remaining_data.is_empty() {
                        remaining_data = &remaining_data[1..];
                    }
                }
            }
        }

        // Update buffer to keep only unprocessed data
        let remaining_len = remaining_data.len();
        let processed_len = buffer.len() - remaining_len;
        if processed_len > 0 {
            buffer.drain(..processed_len);
        }

        // Prevent buffer from growing too large
        if buffer.len() > 1024 * 1024 {
            tracing::warn!("Buffer too large, closing connection");
            return Err("Buffer overflow".into());
        }
    }
}

// Helper function to find the end of a complete message

const BYTES_PER_MB: u64 = 1024 * 1024;

#[derive(Debug)]
enum ShardVacuumStatus {
    Ok,
    ExecutionError,
    Cancelled,
    InvalidShard,
}

impl ShardVacuumStatus {
    fn as_str(&self) -> &'static str {
        match self {
            ShardVacuumStatus::Ok => "ok",
            ShardVacuumStatus::ExecutionError => "execution_error",
            ShardVacuumStatus::Cancelled => "cancelled",
            ShardVacuumStatus::InvalidShard => "invalid_shard",
        }
    }
}

#[derive(Debug)]
struct ShardVacuumDispatchResult {
    shard_id: usize,
    status: ShardVacuumStatus,
    error: Option<String>,
    vacuum_result: Option<VacuumResult>,
}

async fn handle_redis_command(
    stream: &mut TcpStream,
    state: &Arc<AppState>,
    command: RedisCommand,
) -> Result<(), Box<dyn std::error::Error>> {
    let timer = Timer::start();
    let cmd_name = command.name();

    let result = handle_redis_command_inner(stream, state, command).await;

    // Record command metrics
    state.metrics.record_command(&cmd_name, timer.start);

    if result.is_err() {
        state.metrics.record_error("protocol");
    }

    result
}

async fn handle_redis_command_inner(
    stream: &mut TcpStream,
    state: &Arc<AppState>,
    command: RedisCommand,
) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        RedisCommand::Get { key } => {
            if let Some(ref cluster_manager) = state.cluster_manager {
                if !cluster_manager.should_handle_locally(&key).await {
                    if let Some(redirect) = cluster_manager.get_redirect_response(&key).await {
                        let response = BytesFrame::Error(redirect.into());
                        stream.write_all(&serialize_frame(&response)).await?;
                        return Ok(());
                    }
                }
            }
            handle_get(stream, state, key).await?;
        }
        RedisCommand::Set {
            key,
            value,
            ttl_seconds,
        } => {
            if let Some(ref cluster_manager) = state.cluster_manager {
                if !cluster_manager.should_handle_locally(&key).await {
                    if let Some(redirect) = cluster_manager.get_redirect_response(&key).await {
                        let response = BytesFrame::Error(redirect.into());
                        stream.write_all(&serialize_frame(&response)).await?;
                        return Ok(());
                    }
                }
            }
            handle_set(stream, state, key, value, ttl_seconds).await?;
        }
        RedisCommand::Del { keys } => {
            handle_del_multiple(stream, state, keys).await?;
        }
        RedisCommand::Exists { key } => {
            if let Some(ref cluster_manager) = state.cluster_manager {
                if !cluster_manager.should_handle_locally(&key).await {
                    if let Some(redirect) = cluster_manager.get_redirect_response(&key).await {
                        let response = BytesFrame::Error(redirect.into());
                        stream.write_all(&serialize_frame(&response)).await?;
                        return Ok(());
                    }
                }
            }
            handle_exists(stream, state, key).await?;
        }
        RedisCommand::Ping { message } => {
            handle_ping(stream, message).await?;
        }
        RedisCommand::Info { section } => {
            handle_info(stream, state, section).await?;
        }
        RedisCommand::Command => {
            handle_command(stream).await?;
        }
        RedisCommand::HGet { namespace, key } => {
            handle_hget(stream, state, namespace, key).await?;
        }
        RedisCommand::HSet {
            namespace,
            key,
            value,
        } => {
            handle_hset(stream, state, namespace, key, value).await?;
        }
        RedisCommand::HSetEx {
            key,
            fnx,
            fxx,
            expire_option,
            fields,
        } => {
            handle_hsetex(stream, state, key, fnx, fxx, expire_option, fields).await?;
        }
        RedisCommand::HDel { namespace, key } => {
            handle_hdel(stream, state, namespace, key).await?;
        }
        RedisCommand::HExists { namespace, key } => {
            handle_hexists(stream, state, namespace, key).await?;
        }
        RedisCommand::HExpire {
            key,
            seconds,
            condition,
            fields,
        } => {
            handle_hexpire(stream, state, key, seconds, condition, fields).await?;
        }
        RedisCommand::HExpireAt {
            key,
            unix_time_seconds,
            condition,
            fields,
        } => {
            handle_hexpireat(stream, state, key, unix_time_seconds, condition, fields).await?;
        }
        RedisCommand::ClusterNodes => {
            handle_cluster_nodes(stream, state).await?;
        }
        RedisCommand::ClusterInfo => {
            handle_cluster_info(stream, state).await?;
        }
        RedisCommand::ClusterSlots => {
            handle_cluster_slots(stream, state).await?;
        }
        RedisCommand::ClusterAddSlots { slots } => {
            handle_cluster_addslots(stream, state, slots).await?;
        }
        RedisCommand::ClusterDelSlots { slots } => {
            handle_cluster_delslots(stream, state, slots).await?;
        }
        RedisCommand::ClusterKeySlot { key } => {
            handle_cluster_keyslot(stream, key).await?;
        }
        RedisCommand::Ttl { key } => {
            if let Some(ref cluster_manager) = state.cluster_manager {
                if !cluster_manager.should_handle_locally(&key).await {
                    if let Some(redirect) = cluster_manager.get_redirect_response(&key).await {
                        let response = BytesFrame::Error(redirect.into());
                        stream.write_all(&serialize_frame(&response)).await?;
                        return Ok(());
                    }
                }
            }
            handle_ttl(stream, state, key).await?;
        }
        RedisCommand::Expire { key, seconds } => {
            if let Some(ref cluster_manager) = state.cluster_manager {
                if !cluster_manager.should_handle_locally(&key).await {
                    if let Some(redirect) = cluster_manager.get_redirect_response(&key).await {
                        let response = BytesFrame::Error(redirect.into());
                        stream.write_all(&serialize_frame(&response)).await?;
                        return Ok(());
                    }
                }
            }
            handle_expire(stream, state, key, seconds).await?;
        }
        RedisCommand::BlobasaurVacuum {
            target,
            mode,
            budget_mb,
            dry_run,
        } => {
            handle_blobasaur_vacuum(stream, state, target, mode, budget_mb, dry_run).await?;
        }
        RedisCommand::Quit => {
            let response = BytesFrame::SimpleString("OK".into());
            stream.write_all(&serialize_frame(&response)).await?;
            return Err("Client quit".into());
        }
        RedisCommand::Unknown(cmd) => {
            tracing::warn!("Unknown command: {}", cmd);
            let response = BytesFrame::Error(format!("ERR unknown command '{}'", cmd).into());
            stream.write_all(&serialize_frame(&response)).await?;
        }
    }
    Ok(())
}

async fn compress_if_enabled(
    state: &Arc<AppState>,
    data: Bytes,
) -> Result<Bytes, Box<dyn std::error::Error>> {
    if state.cfg.is_compression() {
        if let Some(compressor) = &state.compressor {
            return Ok(compressor.compress(&data).await?.into());
        }
    }
    Ok(data)
}

async fn decompress_if_enabled(
    state: &Arc<AppState>,
    data: Bytes,
) -> Result<Bytes, Box<dyn std::error::Error>> {
    if state.cfg.is_compression() {
        if let Some(compressor) = &state.compressor {
            return Ok(compressor.decompress(&data).await?.into());
        }
    }
    Ok(data)
}

async fn handle_get(
    stream: &mut TcpStream,
    state: &Arc<AppState>,
    key: String,
) -> Result<(), Box<dyn std::error::Error>> {
    // First check inflight cache for pending writes
    if let Some(data) = state.inflight_cache.get(&key).await {
        // Decompress if needed
        let data = decompress_if_enabled(state, data).await?;

        let response = BytesFrame::BulkString(data.into());
        stream.write_all(&serialize_frame(&response)).await?;
        state.metrics.record_cache_hit();
        return Ok(());
    }

    let shard_index = state.get_shard(&key);
    let pool = &state.db_pools[shard_index];

    match sqlx::query_as::<_, (Vec<u8>,)>(
        "SELECT data FROM blobs WHERE key = ? AND (expires_at IS NULL OR expires_at > ?)",
    )
    .bind(&key)
    .bind(chrono::Utc::now().timestamp())
    .fetch_optional(pool)
    .await
    {
        Ok(Some(row)) => {
            // Decompress if needed
            let data = decompress_if_enabled(state, row.0.into()).await?;

            let response = BytesFrame::BulkString(data.into());
            stream.write_all(&serialize_frame(&response)).await?;
            state.metrics.record_cache_hit();
        }
        Ok(None) => {
            let response = BytesFrame::Null;
            stream.write_all(&serialize_frame(&response)).await?;
            state.metrics.record_cache_miss();
        }
        Err(e) => {
            tracing::error!("Failed to GET key {}: {}", key, e);
            let response = BytesFrame::Error("ERR database error ".into());
            stream.write_all(&serialize_frame(&response)).await?;
            state.metrics.record_error("storage");
        }
    }

    Ok(())
}

async fn handle_set(
    stream: &mut TcpStream,
    state: &Arc<AppState>,
    key: String,
    value: Bytes,
    ttl_seconds: Option<u64>,
) -> Result<(), Box<dyn std::error::Error>> {
    let shard_index = state.get_shard(&key);
    let sender = &state.shard_senders[shard_index];

    let value = compress_if_enabled(state, value).await?;

    // Check if async_write is enabled
    if state.cfg.async_write.unwrap_or(false) {
        // Store in inflight cache to prevent race conditions
        state
            .inflight_cache
            .insert(key.clone(), value.clone())
            .await;

        // Calculate expires_at timestamp if TTL is provided
        let expires_at = ttl_seconds.map(|ttl| chrono::Utc::now().timestamp() + ttl as i64);

        // Async mode: respond immediately after queueing
        let operation = ShardWriteOperation::SetAsync {
            key,
            data: value,
            expires_at,
        };

        if sender.send(operation).await.is_err() {
            tracing::error!(
                "Failed to send ASYNC SET operation to shard {}",
                shard_index
            );
            let response = BytesFrame::Error("ERR internal error ".into());
            stream.write_all(&serialize_frame(&response)).await?;
            state.metrics.record_error("storage");
        } else {
            let response = BytesFrame::SimpleString("OK".into());
            stream.write_all(&serialize_frame(&response)).await?;
            state.metrics.record_storage_operation();
        }
    } else {
        // Calculate expires_at timestamp if TTL is provided
        let expires_at = ttl_seconds.map(|ttl| chrono::Utc::now().timestamp() + ttl as i64);

        // Sync mode: wait for completion
        let (responder_tx, responder_rx) = oneshot::channel();

        let operation = ShardWriteOperation::Set {
            key,
            data: value,
            expires_at,
            responder: responder_tx,
        };

        if sender.send(operation).await.is_err() {
            tracing::error!("Failed to send SET operation to shard {}", shard_index);
            let response = BytesFrame::Error("ERR internal error ".into());
            stream.write_all(&serialize_frame(&response)).await?;
            state.metrics.record_error("storage");
        } else {
            match responder_rx.await {
                Ok(Ok(())) => {
                    let response = BytesFrame::SimpleString("OK".into());
                    stream.write_all(&serialize_frame(&response)).await?;
                    state.metrics.record_storage_operation();
                }
                Ok(Err(e)) => {
                    tracing::error!("Shard writer failed for SET: {}", e);
                    let response = BytesFrame::Error("ERR database error ".into());
                    stream.write_all(&serialize_frame(&response)).await?;
                    state.metrics.record_error("storage");
                }
                Err(_) => {
                    tracing::error!("Shard writer task cancelled or panicked for SET ");
                    let response = BytesFrame::Error("ERR internal error ".into());
                    stream.write_all(&serialize_frame(&response)).await?;
                    state.metrics.record_error("storage");
                }
            }
        }
    }

    Ok(())
}

async fn handle_del_multiple(
    stream: &mut TcpStream,
    state: &Arc<AppState>,
    keys: Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut deleted_count = 0;

    for key in keys {
        // Check cluster routing for each key
        if let Some(ref cluster_manager) = state.cluster_manager {
            if !cluster_manager.should_handle_locally(&key).await {
                if let Some(redirect) = cluster_manager.get_redirect_response(&key).await {
                    let response = BytesFrame::Error(redirect.into());
                    stream.write_all(&serialize_frame(&response)).await?;
                    return Ok(());
                }
            }
        }

        let shard_index = state.get_shard(&key);
        let pool = &state.db_pools[shard_index];

        // Check if key exists first
        let exists = sqlx::query("SELECT 1 FROM blobs WHERE key = ?")
            .bind(&key)
            .fetch_optional(pool)
            .await
            .map(|row| row.is_some())
            .unwrap_or(false);

        if !exists {
            continue; // Key doesn't exist, skip
        }

        let sender = &state.shard_senders[shard_index];

        // Check if async_write is enabled
        if state.cfg.async_write.unwrap_or(false) {
            // Remove from inflight cache immediately for delete operations
            state.inflight_cache.invalidate(&key).await;

            // Async mode: respond immediately after queueing
            let operation = ShardWriteOperation::DeleteAsync { key };

            if sender.send(operation).await.is_err() {
                tracing::error!(
                    "Failed to send ASYNC DELETE operation to shard {}",
                    shard_index
                );
                let response = BytesFrame::Error("ERR internal error".into());
                stream.write_all(&serialize_frame(&response)).await?;
                return Ok(());
            } else {
                // Assume success for async mode
                deleted_count += 1;
            }
        } else {
            // Sync mode: wait for completion
            let (responder_tx, responder_rx) = oneshot::channel();

            let operation = ShardWriteOperation::Delete {
                key,
                responder: responder_tx,
            };

            if sender.send(operation).await.is_err() {
                tracing::error!("Failed to send DELETE operation to shard {}", shard_index);
                let response = BytesFrame::Error("ERR internal error".into());
                stream.write_all(&serialize_frame(&response)).await?;
                return Ok(());
            } else {
                match responder_rx.await {
                    Ok(Ok(())) => {
                        deleted_count += 1;
                    }
                    Ok(Err(e)) => {
                        tracing::error!("Shard writer failed for DELETE: {}", e);
                        let response = BytesFrame::Error("ERR database error".into());
                        stream.write_all(&serialize_frame(&response)).await?;
                        return Ok(());
                    }
                    Err(_) => {
                        tracing::error!("Shard writer task cancelled or panicked for DELETE");
                        let response = BytesFrame::Error("ERR internal error".into());
                        stream.write_all(&serialize_frame(&response)).await?;
                        return Ok(());
                    }
                }
            }
        }
    }

    // Return the total number of keys deleted
    let response = BytesFrame::Integer(deleted_count);
    stream.write_all(&serialize_frame(&response)).await?;
    Ok(())
}

async fn handle_exists(
    stream: &mut TcpStream,
    state: &Arc<AppState>,
    key: String,
) -> Result<(), Box<dyn std::error::Error>> {
    let shard_index = state.get_shard(&key);
    let pool = &state.db_pools[shard_index];

    match sqlx::query(
        "SELECT 1 FROM blobs WHERE key = ? AND (expires_at IS NULL OR expires_at > ?)",
    )
    .bind(&key)
    .bind(chrono::Utc::now().timestamp())
    .fetch_optional(pool)
    .await
    {
        Ok(Some(_)) => {
            let response = BytesFrame::Integer(1);
            stream.write_all(&serialize_frame(&response)).await?;
        }
        Ok(None) => {
            let response = BytesFrame::Integer(0);
            stream.write_all(&serialize_frame(&response)).await?;
        }
        Err(e) => {
            tracing::error!("Failed to check EXISTS for key {}: {}", key, e);
            let response = BytesFrame::Error("ERR database error ".into());
            stream.write_all(&serialize_frame(&response)).await?;
        }
    }

    Ok(())
}

async fn handle_ping(
    stream: &mut TcpStream,
    message: Option<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let response = match message {
        Some(msg) => BytesFrame::BulkString(msg.into_bytes().into()),
        None => BytesFrame::SimpleString("PONG".into()),
    };
    stream.write_all(&serialize_frame(&response)).await?;
    Ok(())
}

async fn handle_info(
    stream: &mut TcpStream,
    state: &Arc<AppState>,
    section: Option<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let info = match section.as_deref() {
        Some("server") | None => {
            format!(
                "# Server\r\n\
                 redis_version:blobasaur-0.1.0\r\n\
                 redis_mode:standalone\r\n\
                 process_id:{}\r\n\
                 tcp_port:6379\r\n\
                 uptime_in_seconds:unknown\r\n\
                 \r\n\
                 # Keyspace\r\n\
                 db0:keys=unknown,expires=0,avg_ttl=0\r\n\
                 \r\n\
                 # Blobasaur\r\n\
                 shards:{}\r\n\
                 data_dir:{}\r\n\
                 async_write:{}\r\n\
                 batch_size:{}\r\n\
                 batch_timeout_ms:{}\r\n",
                std::process::id(),
                state.cfg.num_shards,
                state.cfg.data_dir,
                state.cfg.async_write.unwrap_or(false),
                state.cfg.batch_size.unwrap_or(1),
                state.cfg.batch_timeout_ms.unwrap_or(0)
            )
        }
        Some("stats") => "# Stats\r\n\
             total_connections_received:unknown\r\n\
             total_commands_processed:unknown\r\n\
             instantaneous_ops_per_sec:unknown\r\n\
             total_net_input_bytes:unknown\r\n\
             total_net_output_bytes:unknown\r\n\
             instantaneous_input_kbps:unknown\r\n\
             instantaneous_output_kbps:unknown\r\n\
             rejected_connections:0\r\n\
             sync_full:0\r\n\
             sync_partial_ok:0\r\n\
             sync_partial_err:0\r\n\
             expired_keys:0\r\n\
             evicted_keys:0\r\n\
             keyspace_hits:unknown\r\n\
             keyspace_misses:unknown\r\n\
             pubsub_channels:0\r\n\
             pubsub_patterns:0\r\n\
             latest_fork_usec:0\r\n\
             migrate_cached_sockets:0\r\n\
             slave_expires_tracked_keys:0\r\n\
             active_defrag_hits:0\r\n\
             active_defrag_misses:0\r\n\
             active_defrag_key_hits:0\r\n\
             active_defrag_key_misses:0\r\n"
            .to_string(),
        Some(s) => format!("# {}\r\n(section not implemented)\r\n", s),
    };

    let response = BytesFrame::BulkString(info.into_bytes().into());
    stream.write_all(&serialize_frame(&response)).await?;
    Ok(())
}

async fn handle_command(stream: &mut TcpStream) -> Result<(), Box<dyn std::error::Error>> {
    // Return a minimal COMMAND response - just an empty array for now
    // A full implementation would return detailed command information
    let response = BytesFrame::Array(vec![]);
    stream.write_all(&serialize_frame(&response)).await?;
    Ok(())
}

fn is_valid_hash_namespace(namespace: &str) -> bool {
    !namespace.is_empty()
        && namespace
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
}

async fn validate_hash_namespace(
    stream: &mut TcpStream,
    namespace: &str,
) -> Result<bool, Box<dyn std::error::Error>> {
    if is_valid_hash_namespace(namespace) {
        return Ok(true);
    }

    let response = BytesFrame::Error(
        "ERR invalid hash namespace: must be non-empty and contain only [A-Za-z0-9_]".into(),
    );
    stream.write_all(&serialize_frame(&response)).await?;
    Ok(false)
}

async fn hash_table_exists(pool: &sqlx::SqlitePool, table_name: &str) -> Result<bool, sqlx::Error> {
    sqlx::query("SELECT name FROM sqlite_master WHERE type='table' AND name = ?")
        .bind(table_name)
        .fetch_optional(pool)
        .await
        .map(|row| row.is_some())
}

async fn hash_field_exists(
    pool: &sqlx::SqlitePool,
    table_name: &str,
    key: &str,
    table_exists: bool,
) -> Result<bool, sqlx::Error> {
    if !table_exists {
        return Ok(false);
    }

    let query = format!("SELECT 1 FROM {} WHERE key = ?", table_name);
    sqlx::query(&query)
        .bind(key)
        .fetch_optional(pool)
        .await
        .map(|row| row.is_some())
}

async fn handle_hget(
    stream: &mut TcpStream,
    state: &Arc<AppState>,
    namespace: String,
    key: String,
) -> Result<(), Box<dyn std::error::Error>> {
    if !validate_hash_namespace(stream, &namespace).await? {
        return Ok(());
    }

    // Check if we should handle this hash operation locally in a cluster
    if let Some(ref cluster_manager) = state.cluster_manager {
        if !cluster_manager
            .should_handle_hash_locally(&namespace, &key)
            .await
        {
            if let Some(redirect) = cluster_manager
                .get_hash_redirect_response(&namespace, &key)
                .await
            {
                let response = BytesFrame::Error(redirect.into());
                stream.write_all(&serialize_frame(&response)).await?;
                return Ok(());
            }
        }
    }

    // First check inflight cache for pending writes
    let namespaced_key = state.namespaced_key(&namespace, &key);
    if let Some(data) = state.inflight_hcache.get(&namespaced_key).await {
        // Decompress if needed
        let data = decompress_if_enabled(state, data).await?;

        let response = BytesFrame::BulkString(data.into());
        stream.write_all(&serialize_frame(&response)).await?;
        return Ok(());
    }

    let shard_index = state.get_shard(&key);
    let pool = &state.db_pools[shard_index];
    let table_name = format!("blobs_{}", namespace);

    let query = format!(
        "SELECT data FROM {} WHERE key = ? AND (expires_at IS NULL OR expires_at > ?)",
        table_name
    );

    match sqlx::query_as::<_, (Vec<u8>,)>(&query)
        .bind(&key)
        .bind(chrono::Utc::now().timestamp())
        .fetch_optional(pool)
        .await
    {
        Ok(Some(row)) => {
            // Decompress if needed
            let data = decompress_if_enabled(state, row.0.into()).await?;

            let response = BytesFrame::BulkString(data.into());
            stream.write_all(&serialize_frame(&response)).await?;
        }
        Ok(None) => {
            let response = BytesFrame::Null;
            stream.write_all(&serialize_frame(&response)).await?;
        }
        Err(e) => {
            tracing::error!("Failed to HGET namespace {} key {}: {}", namespace, key, e);
            let response = BytesFrame::Error("ERR database error ".into());
            stream.write_all(&serialize_frame(&response)).await?;
        }
    }

    Ok(())
}

async fn handle_hset(
    stream: &mut TcpStream,
    state: &Arc<AppState>,
    namespace: String,
    key: String,
    value: Bytes,
) -> Result<(), Box<dyn std::error::Error>> {
    if !validate_hash_namespace(stream, &namespace).await? {
        return Ok(());
    }

    // Check if we should handle this hash operation locally in a cluster
    if let Some(ref cluster_manager) = state.cluster_manager {
        if !cluster_manager
            .should_handle_hash_locally(&namespace, &key)
            .await
        {
            if let Some(redirect) = cluster_manager
                .get_hash_redirect_response(&namespace, &key)
                .await
            {
                let response = BytesFrame::Error(redirect.into());
                stream.write_all(&serialize_frame(&response)).await?;
                return Ok(());
            }
        }
    }

    let shard_index = state.get_shard(&key);
    let pool = &state.db_pools[shard_index];
    let table_name = format!("blobs_{}", namespace);

    let table_exists = match hash_table_exists(pool, &table_name).await {
        Ok(exists) => exists,
        Err(e) => {
            tracing::error!(
                "Failed to check table existence for namespace {}: {}",
                table_name,
                e
            );
            let response = BytesFrame::Error("ERR database error ".into());
            stream.write_all(&serialize_frame(&response)).await?;
            return Ok(());
        }
    };

    let existed_before = match hash_field_exists(pool, &table_name, &key, table_exists).await {
        Ok(exists) => exists,
        Err(e) => {
            tracing::error!(
                "Failed to check HSET existence for namespace {} key {}: {}",
                namespace,
                key,
                e
            );
            let response = BytesFrame::Error("ERR database error ".into());
            stream.write_all(&serialize_frame(&response)).await?;
            return Ok(());
        }
    };

    let sender = &state.shard_senders[shard_index];

    // Compress data if storage compression is enabled
    let value = compress_if_enabled(state, value).await?;

    // Check if async_write is enabled
    if state.cfg.async_write.unwrap_or(false) {
        // Store in inflight cache to prevent race conditions
        let namespaced_key = state.namespaced_key(&namespace, &key);
        state
            .inflight_hcache
            .insert(namespaced_key, value.clone())
            .await;

        // Async mode: respond immediately after queueing
        let operation = ShardWriteOperation::HSetAsync {
            namespace,
            key,
            data: value,
        };

        if sender.send(operation).await.is_err() {
            tracing::error!(
                "Failed to send ASYNC HSET operation to shard {}",
                shard_index
            );
            let response = BytesFrame::Error("ERR internal error ".into());
            stream.write_all(&serialize_frame(&response)).await?;
        } else {
            let response = BytesFrame::Integer(if existed_before { 0 } else { 1 });
            stream.write_all(&serialize_frame(&response)).await?;
        }
    } else {
        // Sync mode: wait for completion
        let (responder_tx, responder_rx) = oneshot::channel();

        let operation = ShardWriteOperation::HSet {
            namespace,
            key,
            data: value,
            responder: responder_tx,
        };

        if sender.send(operation).await.is_err() {
            tracing::error!("Failed to send HSET operation to shard {}", shard_index);
            let response = BytesFrame::Error("ERR internal error ".into());
            stream.write_all(&serialize_frame(&response)).await?;
        } else {
            match responder_rx.await {
                Ok(Ok(())) => {
                    let response = BytesFrame::Integer(if existed_before { 0 } else { 1 });
                    stream.write_all(&serialize_frame(&response)).await?;
                }
                Ok(Err(e)) => {
                    tracing::error!("Shard writer failed for HSET: {}", e);
                    let response = BytesFrame::Error("ERR database error ".into());
                    stream.write_all(&serialize_frame(&response)).await?;
                }
                Err(_) => {
                    tracing::error!("Shard writer task cancelled or panicked for HSET ");
                    let response = BytesFrame::Error("ERR internal error ".into());
                    stream.write_all(&serialize_frame(&response)).await?;
                }
            }
        }
    }

    Ok(())
}

async fn handle_hsetex(
    stream: &mut TcpStream,
    state: &Arc<AppState>,
    key: String,
    fnx: bool,
    fxx: bool,
    expire_option: Option<crate::redis::protocol::ExpireOption>,
    fields: Vec<(String, Bytes)>,
) -> Result<(), Box<dyn std::error::Error>> {
    use crate::redis::protocol::ExpireOption;

    // In Blobasaur, the hash key is the namespace, and fields are the actual keys
    let namespace = key;

    if !validate_hash_namespace(stream, &namespace).await? {
        return Ok(());
    }

    // Calculate expires_at timestamp from expire option
    let expires_at = match expire_option {
        Some(ExpireOption::Ex(seconds)) => Some(chrono::Utc::now().timestamp() + seconds as i64),
        Some(ExpireOption::Px(millis)) => {
            Some(chrono::Utc::now().timestamp() + (millis as i64 / 1000))
        }
        Some(ExpireOption::ExAt(timestamp)) => Some(timestamp),
        Some(ExpireOption::PxAt(timestamp)) => Some(timestamp / 1000),
        Some(ExpireOption::KeepTtl) => {
            // For KEEPTTL, we'd need to fetch existing TTL - for now, skip TTL setting
            None
        }
        None => None,
    };

    // Process multiple fields
    let mut new_fields_added = 0;

    for (field_key, field_value) in fields {
        // Check if we should handle this hash operation locally in a cluster
        if let Some(ref cluster_manager) = state.cluster_manager {
            if !cluster_manager
                .should_handle_hash_locally(&namespace, &field_key)
                .await
            {
                if let Some(redirect) = cluster_manager
                    .get_hash_redirect_response(&namespace, &field_key)
                    .await
                {
                    let response = BytesFrame::Error(redirect.into());
                    stream.write_all(&serialize_frame(&response)).await?;
                    return Ok(());
                }
            }
        }

        let shard_index = state.get_shard(&field_key);
        let sender = &state.shard_senders[shard_index];
        let pool = &state.db_pools[shard_index];
        let table_name = format!("blobs_{}", namespace);

        let table_exists = match hash_table_exists(pool, &table_name).await {
            Ok(exists) => exists,
            Err(e) => {
                tracing::error!(
                    "Failed to check table existence for namespace {}: {}",
                    table_name,
                    e
                );
                let response = BytesFrame::Error("ERR database error".into());
                stream.write_all(&serialize_frame(&response)).await?;
                return Ok(());
            }
        };

        let exists = match hash_field_exists(pool, &table_name, &field_key, table_exists).await {
            Ok(exists) => exists,
            Err(e) => {
                tracing::error!(
                    "Failed to check field existence for namespace {} key {}: {}",
                    namespace,
                    field_key,
                    e
                );
                let response = BytesFrame::Error("ERR database error".into());
                stream.write_all(&serialize_frame(&response)).await?;
                return Ok(());
            }
        };

        // Check FNX/FXX conditions if specified
        if fnx || fxx {
            if fnx && exists {
                // FNX: Only set if field doesn't exist, return 0 for entire operation
                let response = BytesFrame::Integer(0);
                stream.write_all(&serialize_frame(&response)).await?;
                return Ok(());
            }
            if fxx && !exists {
                // FXX: Only set if field exists, return 0 for entire operation
                let response = BytesFrame::Integer(0);
                stream.write_all(&serialize_frame(&response)).await?;
                return Ok(());
            }
        }

        let is_new_field = !exists;

        // Compress data if storage compression is enabled
        let compressed_value = compress_if_enabled(state, field_value).await?;

        // Send the operation
        if state.cfg.async_write.unwrap_or(false) {
            // Store in inflight cache to prevent race conditions
            let namespaced_key = state.namespaced_key(&namespace, &field_key);
            state
                .inflight_hcache
                .insert(namespaced_key, compressed_value.clone())
                .await;

            let operation = if let Some(exp) = expires_at {
                ShardWriteOperation::HSetExAsync {
                    namespace: namespace.clone(),
                    key: field_key,
                    data: compressed_value,
                    expires_at: exp,
                }
            } else {
                ShardWriteOperation::HSetAsync {
                    namespace: namespace.clone(),
                    key: field_key,
                    data: compressed_value,
                }
            };

            if sender.send(operation).await.is_err() {
                tracing::error!(
                    "Failed to send ASYNC HSETEX operation to shard {}",
                    shard_index
                );
                let response = BytesFrame::Error("ERR internal error".into());
                stream.write_all(&serialize_frame(&response)).await?;
                return Ok(());
            }
            if is_new_field {
                new_fields_added += 1;
            }
        } else {
            // Sync mode: wait for completion
            let (responder_tx, responder_rx) = oneshot::channel();

            let operation = if let Some(exp) = expires_at {
                ShardWriteOperation::HSetEx {
                    namespace: namespace.clone(),
                    key: field_key,
                    data: compressed_value,
                    expires_at: exp,
                    responder: responder_tx,
                }
            } else {
                ShardWriteOperation::HSet {
                    namespace: namespace.clone(),
                    key: field_key,
                    data: compressed_value,
                    responder: responder_tx,
                }
            };

            if sender.send(operation).await.is_err() {
                tracing::error!("Failed to send HSETEX operation to shard {}", shard_index);
                let response = BytesFrame::Error("ERR internal error".into());
                stream.write_all(&serialize_frame(&response)).await?;
                return Ok(());
            }

            match responder_rx.await {
                Ok(Ok(())) => {
                    if is_new_field {
                        new_fields_added += 1;
                    }
                }
                Ok(Err(e)) => {
                    tracing::error!("Shard writer failed for HSETEX: {}", e);
                    let response = BytesFrame::Error("ERR database error".into());
                    stream.write_all(&serialize_frame(&response)).await?;
                    return Ok(());
                }
                Err(_) => {
                    tracing::error!("Shard writer task cancelled or panicked for HSETEX");
                    let response = BytesFrame::Error("ERR internal error".into());
                    stream.write_all(&serialize_frame(&response)).await?;
                    return Ok(());
                }
            }
        }
    }

    let response = BytesFrame::Integer(new_fields_added);
    stream.write_all(&serialize_frame(&response)).await?;

    Ok(())
}

async fn handle_hdel(
    stream: &mut TcpStream,
    state: &Arc<AppState>,
    namespace: String,
    key: String,
) -> Result<(), Box<dyn std::error::Error>> {
    if !validate_hash_namespace(stream, &namespace).await? {
        return Ok(());
    }

    // Check if we should handle this hash operation locally in a cluster
    if let Some(ref cluster_manager) = state.cluster_manager {
        if !cluster_manager
            .should_handle_hash_locally(&namespace, &key)
            .await
        {
            if let Some(redirect) = cluster_manager
                .get_hash_redirect_response(&namespace, &key)
                .await
            {
                let response = BytesFrame::Error(redirect.into());
                stream.write_all(&serialize_frame(&response)).await?;
                return Ok(());
            }
        }
    }

    let shard_index = state.get_shard(&key);
    let pool = &state.db_pools[shard_index];
    let table_name = format!("blobs_{}", namespace);

    // First check if key exists
    let query = format!("SELECT 1 FROM {} WHERE key = ?", table_name);
    let exists = sqlx::query(&query)
        .bind(&key)
        .fetch_optional(pool)
        .await
        .map(|row| row.is_some())
        .unwrap_or(false);

    if !exists {
        // Redis HDEL returns the number of keys deleted
        let response = BytesFrame::Integer(0);
        stream.write_all(&serialize_frame(&response)).await?;
        return Ok(());
    }

    let sender = &state.shard_senders[shard_index];

    // Check if async_write is enabled
    if state.cfg.async_write.unwrap_or(false) {
        // Remove from inflight cache immediately for delete operations
        let namespaced_key = state.namespaced_key(&namespace, &key);
        state.inflight_hcache.invalidate(&namespaced_key).await;

        // Async mode: respond immediately after queueing
        let operation = ShardWriteOperation::HDeleteAsync { namespace, key };

        if sender.send(operation).await.is_err() {
            tracing::error!(
                "Failed to send ASYNC HDEL operation to shard {}",
                shard_index
            );
            let response = BytesFrame::Error("ERR internal error ".into());
            stream.write_all(&serialize_frame(&response)).await?;
        } else {
            // Assume success for async mode
            let response = BytesFrame::Integer(1);
            stream.write_all(&serialize_frame(&response)).await?;
        }
    } else {
        // Sync mode: wait for completion
        let (responder_tx, responder_rx) = oneshot::channel();

        let operation = ShardWriteOperation::HDelete {
            namespace,
            key,
            responder: responder_tx,
        };

        if sender.send(operation).await.is_err() {
            tracing::error!("Failed to send HDEL operation to shard {}", shard_index);
            let response = BytesFrame::Error("ERR internal error ".into());
            stream.write_all(&serialize_frame(&response)).await?;
        } else {
            match responder_rx.await {
                Ok(Ok(())) => {
                    let response = BytesFrame::Integer(1);
                    stream.write_all(&serialize_frame(&response)).await?;
                }
                Ok(Err(e)) => {
                    tracing::error!("Shard writer failed for HDEL: {}", e);
                    let response = BytesFrame::Error("ERR database error ".into());
                    stream.write_all(&serialize_frame(&response)).await?;
                }
                Err(_) => {
                    tracing::error!("Shard writer task cancelled or panicked for HDEL ");
                    let response = BytesFrame::Error("ERR internal error ".into());
                    stream.write_all(&serialize_frame(&response)).await?;
                }
            }
        }
    }

    Ok(())
}

async fn handle_hexists(
    stream: &mut TcpStream,
    state: &Arc<AppState>,
    namespace: String,
    key: String,
) -> Result<(), Box<dyn std::error::Error>> {
    if !validate_hash_namespace(stream, &namespace).await? {
        return Ok(());
    }

    // Check if we should handle this hash operation locally in a cluster
    if let Some(ref cluster_manager) = state.cluster_manager {
        if !cluster_manager
            .should_handle_hash_locally(&namespace, &key)
            .await
        {
            if let Some(redirect) = cluster_manager
                .get_hash_redirect_response(&namespace, &key)
                .await
            {
                let response = BytesFrame::Error(redirect.into());
                stream.write_all(&serialize_frame(&response)).await?;
                return Ok(());
            }
        }
    }

    let shard_index = state.get_shard(&key);
    let pool = &state.db_pools[shard_index];
    let table_name = format!("blobs_{}", namespace);

    let query = format!(
        "SELECT 1 FROM {} WHERE key = ? AND (expires_at IS NULL OR expires_at > ?)",
        table_name
    );

    match sqlx::query(&query)
        .bind(&key)
        .bind(chrono::Utc::now().timestamp())
        .fetch_optional(pool)
        .await
    {
        Ok(Some(_)) => {
            let response = BytesFrame::Integer(1);
            stream.write_all(&serialize_frame(&response)).await?;
        }
        Ok(None) => {
            let response = BytesFrame::Integer(0);
            stream.write_all(&serialize_frame(&response)).await?;
        }
        Err(e) => {
            tracing::error!(
                "Failed to check HEXISTS for namespace {} key {}: {}",
                namespace,
                key,
                e
            );
            let response = BytesFrame::Error("ERR database error ".into());
            stream.write_all(&serialize_frame(&response)).await?;
        }
    }

    Ok(())
}

async fn handle_hexpire(
    stream: &mut TcpStream,
    state: &Arc<AppState>,
    key: String,
    seconds: i64,
    condition: Option<HExpireCondition>,
    fields: Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let expires_at = chrono::Utc::now().timestamp() + seconds;
    handle_hexpire_internal(stream, state, key, expires_at, condition, fields).await
}

async fn handle_hexpireat(
    stream: &mut TcpStream,
    state: &Arc<AppState>,
    key: String,
    unix_time_seconds: i64,
    condition: Option<HExpireCondition>,
    fields: Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    handle_hexpire_internal(stream, state, key, unix_time_seconds, condition, fields).await
}

/// Shared implementation for HEXPIRE and HEXPIREAT.
/// Both commands resolve to an absolute `expires_at` timestamp before calling this.
async fn handle_hexpire_internal(
    stream: &mut TcpStream,
    state: &Arc<AppState>,
    key: String,
    expires_at: i64,
    condition: Option<HExpireCondition>,
    fields: Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    // In Blobasaur, the hash key is the namespace, and fields are the actual keys
    let namespace = key;

    if !validate_hash_namespace(stream, &namespace).await? {
        return Ok(());
    }

    let mut field_results: Vec<i64> = Vec::with_capacity(fields.len());

    for field_key in fields {
        // Check cluster routing for each field
        if let Some(ref cluster_manager) = state.cluster_manager {
            if !cluster_manager
                .should_handle_hash_locally(&namespace, &field_key)
                .await
            {
                if let Some(redirect) = cluster_manager
                    .get_hash_redirect_response(&namespace, &field_key)
                    .await
                {
                    let response = BytesFrame::Error(redirect.into());
                    stream.write_all(&serialize_frame(&response)).await?;
                    return Ok(());
                }
            }
        }

        let shard_index = state.get_shard(&field_key);
        let sender = &state.shard_senders[shard_index];

        let (responder_tx, responder_rx) = oneshot::channel();

        let operation = ShardWriteOperation::HExpire {
            namespace: namespace.clone(),
            key: field_key.clone(),
            expires_at,
            condition,
            responder: responder_tx,
        };

        if sender.send(operation).await.is_err() {
            tracing::error!("Failed to send HEXPIRE operation to shard {}", shard_index);
            let response = BytesFrame::Error("ERR internal error".into());
            stream.write_all(&serialize_frame(&response)).await?;
            state.metrics.record_error("storage");
            return Ok(());
        }

        match responder_rx.await {
            Ok(Ok(result_code)) => {
                field_results.push(result_code);
            }
            Ok(Err(e)) => {
                tracing::error!(
                    "HEXPIRE operation failed for namespace {} key {}: {}",
                    namespace,
                    field_key,
                    e
                );
                let response = BytesFrame::Error("ERR internal error".into());
                stream.write_all(&serialize_frame(&response)).await?;
                state.metrics.record_error("storage");
                return Ok(());
            }
            Err(_) => {
                tracing::error!("Shard writer task cancelled or panicked for HEXPIRE");
                let response = BytesFrame::Error("ERR internal error".into());
                stream.write_all(&serialize_frame(&response)).await?;
                state.metrics.record_error("storage");
                return Ok(());
            }
        }
    }

    // Return array of integers, one per field
    let response = BytesFrame::Array(field_results.into_iter().map(BytesFrame::Integer).collect());
    stream.write_all(&serialize_frame(&response)).await?;
    state.metrics.record_storage_operation();

    Ok(())
}

// Cluster command handlers
async fn handle_cluster_nodes(
    stream: &mut TcpStream,
    state: &Arc<AppState>,
) -> Result<(), Box<dyn std::error::Error>> {
    tracing::info!("Handling CLUSTER NODES command ");
    if let Some(ref cluster_manager) = state.cluster_manager {
        let nodes_info = cluster_manager.get_cluster_nodes().await;
        let response = BytesFrame::BulkString(nodes_info.into());
        stream.write_all(&serialize_frame(&response)).await?;
    } else {
        let response = BytesFrame::Error("ERR This instance has cluster support disabled ".into());
        stream.write_all(&serialize_frame(&response)).await?;
    }
    Ok(())
}

async fn handle_cluster_info(
    stream: &mut TcpStream,
    state: &Arc<AppState>,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(ref cluster_manager) = state.cluster_manager {
        let cluster_info = cluster_manager.get_cluster_info().await;
        let response = BytesFrame::BulkString(cluster_info.into());
        stream.write_all(&serialize_frame(&response)).await?;
    } else {
        let response = BytesFrame::Error("ERR This instance has cluster support disabled ".into());
        stream.write_all(&serialize_frame(&response)).await?;
    }
    Ok(())
}

async fn handle_cluster_slots(
    stream: &mut TcpStream,
    state: &Arc<AppState>,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(ref cluster_manager) = state.cluster_manager {
        let local_slots = cluster_manager.get_local_slots().await;

        // Build Redis CLUSTER SLOTS response
        // Format: Array of [start_slot, end_slot, [master_ip, master_port]]
        let mut slot_ranges = Vec::new();

        if !local_slots.is_empty() {
            let mut sorted_slots: Vec<u16> = local_slots.iter().copied().collect();
            sorted_slots.sort();

            let mut start = sorted_slots[0];
            let mut end = sorted_slots[0];

            for &slot in &sorted_slots[1..] {
                if slot == end + 1 {
                    end = slot;
                } else {
                    // Add completed range
                    let advertise_addr = state
                        .cfg
                        .cluster
                        .as_ref()
                        .and_then(|c| c.advertise_addr.clone())
                        .unwrap_or("127.0.0.1:6379".to_string());
                    let parts: Vec<&str> = advertise_addr.split(':').collect();
                    let ip = parts.get(0).cloned().unwrap_or("127.0.0.1").to_string();
                    let port = parts
                        .get(1)
                        .and_then(|p| p.parse::<i64>().ok())
                        .unwrap_or(6379);

                    let range = BytesFrame::Array(vec![
                        BytesFrame::Integer(start as i64),
                        BytesFrame::Integer(end as i64),
                        BytesFrame::Array(vec![
                            BytesFrame::BulkString(ip.into()),
                            BytesFrame::Integer(port),
                        ]),
                    ]);
                    slot_ranges.push(range);
                    start = slot;
                    end = slot;
                }
            }

            let advertise_addr = state
                .cfg
                .cluster
                .as_ref()
                .and_then(|c| c.advertise_addr.clone())
                .unwrap_or("127.0.0.1:6379".to_string());
            let parts: Vec<&str> = advertise_addr.split(':').collect();
            let ip = parts.get(0).cloned().unwrap_or("127.0.0.1").to_string();
            let port = parts
                .get(1)
                .and_then(|p| p.parse::<i64>().ok())
                .unwrap_or(6379);

            // Add the last range
            let range = BytesFrame::Array(vec![
                BytesFrame::Integer(start as i64),
                BytesFrame::Integer(end as i64),
                BytesFrame::Array(vec![
                    BytesFrame::BulkString(ip.into()),
                    BytesFrame::Integer(port),
                ]),
            ]);
            slot_ranges.push(range);
        }

        let response = BytesFrame::Array(slot_ranges);
        stream.write_all(&serialize_frame(&response)).await?;
    } else {
        let response = BytesFrame::Error("ERR This instance has cluster support disabled ".into());
        stream.write_all(&serialize_frame(&response)).await?;
    }
    Ok(())
}

async fn handle_cluster_addslots(
    stream: &mut TcpStream,
    state: &Arc<AppState>,
    slots: Vec<u16>,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(ref cluster_manager) = state.cluster_manager {
        match cluster_manager.add_slots(slots).await {
            Ok(()) => {
                let response = BytesFrame::SimpleString("OK".into());
                stream.write_all(&serialize_frame(&response)).await?;
            }
            Err(e) => {
                tracing::error!("Failed to add slots: {}", e);
                let response = BytesFrame::Error("ERR failed to add slots ".into());
                stream.write_all(&serialize_frame(&response)).await?;
            }
        }
    } else {
        let response = BytesFrame::Error("ERR This instance has cluster support disabled ".into());
        stream.write_all(&serialize_frame(&response)).await?;
    }
    Ok(())
}

async fn handle_cluster_delslots(
    stream: &mut TcpStream,
    state: &Arc<AppState>,
    slots: Vec<u16>,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(ref cluster_manager) = state.cluster_manager {
        match cluster_manager.remove_slots(slots).await {
            Ok(()) => {
                let response = BytesFrame::SimpleString("OK".into());
                stream.write_all(&serialize_frame(&response)).await?;
            }
            Err(e) => {
                tracing::error!("Failed to remove slots: {}", e);
                let response = BytesFrame::Error("ERR failed to remove slots ".into());
                stream.write_all(&serialize_frame(&response)).await?;
            }
        }
    } else {
        let response = BytesFrame::Error("ERR This instance has cluster support disabled ".into());
        stream.write_all(&serialize_frame(&response)).await?;
    }
    Ok(())
}

async fn handle_cluster_keyslot(
    stream: &mut TcpStream,
    key: String,
) -> Result<(), Box<dyn std::error::Error>> {
    let slot = ClusterManager::calculate_slot(&key);
    let response = BytesFrame::Integer(slot as i64);
    stream.write_all(&serialize_frame(&response)).await?;
    Ok(())
}

async fn handle_ttl(
    stream: &mut TcpStream,
    state: &Arc<AppState>,
    key: String,
) -> Result<(), Box<dyn std::error::Error>> {
    let shard_index = state.get_shard(&key);
    let pool = &state.db_pools[shard_index];
    let now = chrono::Utc::now().timestamp();

    match sqlx::query_as::<_, (Option<i64>,)>(
        "SELECT expires_at FROM blobs WHERE key = ? AND (expires_at IS NULL OR expires_at > ?)",
    )
    .bind(&key)
    .bind(now)
    .fetch_optional(pool)
    .await
    {
        Ok(Some((expires_at,))) => {
            let ttl = match expires_at {
                Some(expires_at) => expires_at - now,
                None => -1, // Key exists but has no expiration
            };
            let response = BytesFrame::Integer(ttl);
            stream.write_all(&serialize_frame(&response)).await?;
        }
        Ok(None) => {
            // Key doesn't exist or is expired
            let response = BytesFrame::Integer(-2);
            stream.write_all(&serialize_frame(&response)).await?;
        }
        Err(e) => {
            tracing::error!("TTL query error for key {}: {}", key, e);
            let response = BytesFrame::Error("ERR internal error".into());
            stream.write_all(&serialize_frame(&response)).await?;
            state.metrics.record_error("storage");
        }
    }

    Ok(())
}

async fn handle_expire(
    stream: &mut TcpStream,
    state: &Arc<AppState>,
    key: String,
    seconds: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let shard_index = state.get_shard(&key);
    let sender = &state.shard_senders[shard_index];
    let expires_at = chrono::Utc::now().timestamp() + seconds as i64;

    let (responder_tx, responder_rx) = oneshot::channel();

    let operation = ShardWriteOperation::Expire {
        key,
        expires_at,
        responder: responder_tx,
    };

    if sender.send(operation).await.is_err() {
        tracing::error!("Failed to send EXPIRE operation to shard {}", shard_index);
        let response = BytesFrame::Error("ERR internal error".into());
        stream.write_all(&serialize_frame(&response)).await?;
        state.metrics.record_error("storage");
    } else {
        match responder_rx.await {
            Ok(Ok(true)) => {
                // Key was found and expiration was set
                let response = BytesFrame::Integer(1);
                stream.write_all(&serialize_frame(&response)).await?;
                state.metrics.record_storage_operation();
            }
            Ok(Ok(false)) => {
                // Key was not found
                let response = BytesFrame::Integer(0);
                stream.write_all(&serialize_frame(&response)).await?;
                state.metrics.record_storage_operation();
            }
            Ok(Err(e)) => {
                tracing::error!("EXPIRE operation failed: {}", e);
                let response = BytesFrame::Error("ERR internal error".into());
                stream.write_all(&serialize_frame(&response)).await?;
                state.metrics.record_error("storage");
            }
            Err(_) => {
                tracing::error!("Shard writer task cancelled or panicked for EXPIRE");
                let response = BytesFrame::Error("ERR internal error".into());
                stream.write_all(&serialize_frame(&response)).await?;
                state.metrics.record_error("storage");
            }
        }
    }

    Ok(())
}

fn vacuum_mode_from_command(mode: VacuumCommandMode) -> VacuumMode {
    match mode {
        VacuumCommandMode::Incremental => VacuumMode::Incremental,
        VacuumCommandMode::Full => VacuumMode::Full,
    }
}

fn vacuum_mode_name(mode: VacuumCommandMode) -> &'static str {
    match mode {
        VacuumCommandMode::Incremental => "incremental",
        VacuumCommandMode::Full => "full",
    }
}

fn vacuum_mode_name_internal(mode: VacuumMode) -> &'static str {
    match mode {
        VacuumMode::Incremental => "incremental",
        VacuumMode::Full => "full",
    }
}

fn maybe_integer(value: Option<u64>) -> BytesFrame {
    value
        .map(|v| BytesFrame::Integer(i64::try_from(v).unwrap_or(i64::MAX)))
        .unwrap_or(BytesFrame::Null)
}

fn vacuum_stats_to_frame(stats: VacuumStats) -> BytesFrame {
    BytesFrame::Array(vec![
        BytesFrame::BulkString("page_size".as_bytes().to_vec().into()),
        BytesFrame::Integer(i64::try_from(stats.page_size).unwrap_or(i64::MAX)),
        BytesFrame::BulkString("page_count".as_bytes().to_vec().into()),
        BytesFrame::Integer(i64::try_from(stats.page_count).unwrap_or(i64::MAX)),
        BytesFrame::BulkString("freelist_count".as_bytes().to_vec().into()),
        BytesFrame::Integer(i64::try_from(stats.freelist_count).unwrap_or(i64::MAX)),
    ])
}

fn shard_vacuum_result_to_frame(result: ShardVacuumDispatchResult) -> BytesFrame {
    let (
        duration_ms,
        before,
        after,
        incremental_pages_requested,
        estimated_reclaimed_pages,
        errors,
    ) = if let Some(vacuum_result) = &result.vacuum_result {
        (
            BytesFrame::Integer(
                i64::try_from(vacuum_result.duration.as_millis()).unwrap_or(i64::MAX),
            ),
            vacuum_result
                .before
                .map(vacuum_stats_to_frame)
                .unwrap_or(BytesFrame::Null),
            vacuum_result
                .after
                .map(vacuum_stats_to_frame)
                .unwrap_or(BytesFrame::Null),
            maybe_integer(vacuum_result.incremental_pages_requested),
            maybe_integer(vacuum_result.estimated_reclaimed_pages),
            BytesFrame::Array(
                vacuum_result
                    .errors
                    .iter()
                    .map(|err| BytesFrame::BulkString(err.as_bytes().to_vec().into()))
                    .collect(),
            ),
        )
    } else {
        (
            BytesFrame::Null,
            BytesFrame::Null,
            BytesFrame::Null,
            BytesFrame::Null,
            BytesFrame::Null,
            BytesFrame::Array(vec![]),
        )
    };

    BytesFrame::Array(vec![
        BytesFrame::BulkString("shard".as_bytes().to_vec().into()),
        BytesFrame::Integer(i64::try_from(result.shard_id).unwrap_or(i64::MAX)),
        BytesFrame::BulkString("status".as_bytes().to_vec().into()),
        BytesFrame::BulkString(result.status.as_str().as_bytes().to_vec().into()),
        BytesFrame::BulkString("error".as_bytes().to_vec().into()),
        result
            .error
            .map(|err| BytesFrame::BulkString(err.as_bytes().to_vec().into()))
            .unwrap_or(BytesFrame::Null),
        BytesFrame::BulkString("duration_ms".as_bytes().to_vec().into()),
        duration_ms,
        BytesFrame::BulkString("before".as_bytes().to_vec().into()),
        before,
        BytesFrame::BulkString("after".as_bytes().to_vec().into()),
        after,
        BytesFrame::BulkString("incremental_pages_requested".as_bytes().to_vec().into()),
        incremental_pages_requested,
        BytesFrame::BulkString("estimated_reclaimed_pages".as_bytes().to_vec().into()),
        estimated_reclaimed_pages,
        BytesFrame::BulkString("execution_errors".as_bytes().to_vec().into()),
        errors,
    ])
}

async fn run_shard_vacuum(
    state: &Arc<AppState>,
    shard_id: usize,
    mode: VacuumMode,
    budget_mb: u64,
    dry_run: bool,
) -> ShardVacuumDispatchResult {
    let mode_name = vacuum_mode_name_internal(mode);

    let sender = match state.shard_senders.get(shard_id) {
        Some(sender) => sender,
        None => {
            state
                .metrics
                .record_vacuum_run(mode_name, "invalid_shard", None);

            return ShardVacuumDispatchResult {
                shard_id,
                status: ShardVacuumStatus::InvalidShard,
                error: Some(format!("shard {} does not exist on this node", shard_id)),
                vacuum_result: None,
            };
        }
    };

    let budget_bytes = budget_mb.saturating_mul(BYTES_PER_MB);
    let (responder_tx, responder_rx) = oneshot::channel();

    if sender
        .send(ShardWriteOperation::Vacuum {
            mode,
            budget_bytes,
            dry_run,
            responder: responder_tx,
        })
        .await
        .is_err()
    {
        state
            .metrics
            .record_vacuum_run(mode_name, "dispatch_error", None);
        state
            .metrics
            .record_vacuum_shard_failure(shard_id, mode_name, "error");

        return ShardVacuumDispatchResult {
            shard_id,
            status: ShardVacuumStatus::ExecutionError,
            error: Some("failed to dispatch vacuum operation to shard writer".to_string()),
            vacuum_result: None,
        };
    }

    // No server-side timeout: the CLI's --timeout-sec controls the overall
    // deadline via the network-level timeout in send_redis_command_with_timeout.
    // A hardcoded server timeout causes false failures for large/full vacuums.
    match responder_rx.await {
        Ok(vacuum_result) => {
            if vacuum_result.errors.is_empty() {
                ShardVacuumDispatchResult {
                    shard_id,
                    status: ShardVacuumStatus::Ok,
                    error: None,
                    vacuum_result: Some(vacuum_result),
                }
            } else {
                let summary = vacuum_result.errors.join("; ");
                ShardVacuumDispatchResult {
                    shard_id,
                    status: ShardVacuumStatus::ExecutionError,
                    error: Some(summary),
                    vacuum_result: Some(vacuum_result),
                }
            }
        }
        Err(_) => {
            state
                .metrics
                .record_vacuum_run(mode_name, "cancelled", None);
            state
                .metrics
                .record_vacuum_shard_failure(shard_id, mode_name, "error");

            ShardVacuumDispatchResult {
                shard_id,
                status: ShardVacuumStatus::Cancelled,
                error: Some("shard writer cancelled vacuum response".to_string()),
                vacuum_result: None,
            }
        }
    }
}

async fn handle_blobasaur_vacuum(
    stream: &mut TcpStream,
    state: &Arc<AppState>,
    target: VacuumShardTarget,
    mode: VacuumCommandMode,
    budget_mb: u64,
    dry_run: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let mode_internal = vacuum_mode_from_command(mode);
    let shard_ids: Vec<usize> = match target {
        VacuumShardTarget::All => (0..state.shard_senders.len()).collect(),
        VacuumShardTarget::Shard(shard_id) => vec![shard_id],
    };

    let mut shard_results = Vec::with_capacity(shard_ids.len());
    for shard_id in shard_ids {
        let shard_result =
            run_shard_vacuum(state, shard_id, mode_internal, budget_mb, dry_run).await;

        if matches!(shard_result.status, ShardVacuumStatus::Ok) {
            tracing::info!(
                shard_id,
                status = shard_result.status.as_str(),
                "Shard vacuum finished"
            );
        } else {
            tracing::warn!(
                shard_id,
                status = shard_result.status.as_str(),
                error = ?shard_result.error,
                "Shard vacuum finished with error; continuing with remaining shards"
            );
        }

        shard_results.push(shard_vacuum_result_to_frame(shard_result));
    }

    let response = BytesFrame::Array(vec![
        BytesFrame::BulkString("mode".as_bytes().to_vec().into()),
        BytesFrame::BulkString(vacuum_mode_name(mode).as_bytes().to_vec().into()),
        BytesFrame::BulkString("budget_mb".as_bytes().to_vec().into()),
        BytesFrame::Integer(i64::try_from(budget_mb).unwrap_or(i64::MAX)),
        BytesFrame::BulkString("dry_run".as_bytes().to_vec().into()),
        BytesFrame::Integer(if dry_run { 1 } else { 0 }),
        BytesFrame::BulkString("results".as_bytes().to_vec().into()),
        BytesFrame::Array(shard_results),
    ]);

    stream.write_all(&serialize_frame(&response)).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::ClusterManager;
    use crate::{config::Cfg, shard_manager};
    use futures::future::BoxFuture;
    use tempfile::TempDir;
    use tokio::net::{TcpListener, TcpStream};

    struct TestContext {
        state: Arc<AppState>,
        writer_handles: Vec<tokio::task::JoinHandle<()>>,
        _temp_dir: TempDir,
    }

    impl TestContext {
        async fn new(async_write: bool) -> Self {
            Self::new_with_shards(async_write, 1).await
        }

        async fn new_with_shards(async_write: bool, num_shards: usize) -> Self {
            let temp_dir = TempDir::new().expect("temp dir");
            let cfg = Cfg {
                data_dir: temp_dir.path().to_string_lossy().into_owned(),
                num_shards,
                storage_compression: None,
                async_write: Some(async_write),
                batch_size: Some(1),
                batch_timeout_ms: Some(0),
                addr: None,
                cluster: None,
                metrics: None,
                sqlite: None,
            };

            let mut receivers = Vec::new();
            let state = Arc::new(AppState::new(cfg.clone(), &mut receivers).await.unwrap());

            let batch_size = cfg.batch_size.unwrap_or(1);
            let batch_timeout = cfg.batch_timeout_ms.unwrap_or(0);
            let mut writer_handles = Vec::new();
            for (i, receiver) in receivers.into_iter().enumerate() {
                let pool = state.db_pools[i].clone();
                let inflight_cache = state.inflight_cache.clone();
                let inflight_hcache = state.inflight_hcache.clone();
                let metrics = state.metrics.clone();
                writer_handles.push(tokio::spawn(shard_manager::shard_writer_task(
                    i,
                    pool,
                    receiver,
                    batch_size,
                    batch_timeout,
                    inflight_cache,
                    inflight_hcache,
                    metrics,
                )));
            }

            TestContext {
                state,
                writer_handles,
                _temp_dir: temp_dir,
            }
        }

        async fn shutdown(self) {
            let TestContext {
                state,
                writer_handles,
                ..
            } = self;
            drop(state);
            for handle in writer_handles {
                let _ = handle.await;
            }
        }
    }

    async fn tcp_pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local addr");
        let client = TcpStream::connect(addr).await.expect("connect client");
        let (server, _) = listener.accept().await.expect("accept server");
        (server, client)
    }

    async fn read_response(mut client: TcpStream) -> BytesFrame {
        let mut buf = Vec::new();
        client
            .read_to_end(&mut buf)
            .await
            .expect("read response bytes");
        let (frame, _) = parse_resp_with_remaining(&buf).expect("parse response");
        frame
    }

    async fn respond_with<F>(ctx: &TestContext, f: F) -> BytesFrame
    where
        F: FnOnce(
            Arc<AppState>,
            TcpStream,
        ) -> BoxFuture<'static, Result<(), Box<dyn std::error::Error>>>,
    {
        let (server_stream, client_stream) = tcp_pair().await;
        let state = ctx.state.clone();
        f(state, server_stream).await.unwrap();
        read_response(client_stream).await
    }

    fn assert_integer_response(frame: BytesFrame, expected: i64) {
        match frame {
            BytesFrame::Integer(v) => assert_eq!(v, expected),
            other => panic!("Expected integer {}, got {:?}", expected, other),
        }
    }

    fn assert_simple_string(frame: BytesFrame, expected: &str) {
        match frame {
            BytesFrame::SimpleString(s) => assert_eq!(s, expected),
            other => panic!("Expected simple string {}, got {:?}", expected, other),
        }
    }

    fn assert_bulk_string(frame: BytesFrame, expected: &[u8]) {
        match frame {
            BytesFrame::BulkString(data) => assert_eq!(data.as_ref(), expected),
            other => panic!(
                "Expected bulk string {:?}, got {:?}",
                String::from_utf8_lossy(expected),
                other
            ),
        }
    }

    fn assert_null(frame: BytesFrame) {
        match frame {
            BytesFrame::Null => {}
            other => panic!("Expected null bulk string, got {:?}", other),
        }
    }

    fn assert_array_len(frame: BytesFrame, expected_len: usize) {
        match frame {
            BytesFrame::Array(items) => assert_eq!(items.len(), expected_len),
            other => panic!("Expected array len {}, got {:?}", expected_len, other),
        }
    }

    fn assert_error_contains(frame: BytesFrame, expected: &str) {
        match frame {
            BytesFrame::Error(msg) => {
                assert!(
                    msg.to_string().contains(expected),
                    "expected error to contain '{}', got '{}'",
                    expected,
                    msg
                )
            }
            other => panic!("Expected error containing {}, got {:?}", expected, other),
        }
    }

    #[test]
    fn hash_namespace_validation_rules() {
        assert!(is_valid_hash_namespace("users_123"));
        assert!(!is_valid_hash_namespace(""));
        assert!(!is_valid_hash_namespace("users-prod"));
    }

    fn bulk_str(frame: &BytesFrame) -> Option<String> {
        match frame {
            BytesFrame::BulkString(bytes) => {
                Some(String::from_utf8_lossy(bytes.as_ref()).to_string())
            }
            _ => None,
        }
    }

    fn array_field<'a>(items: &'a [BytesFrame], field_name: &str) -> Option<&'a BytesFrame> {
        let mut idx = 0;
        while idx + 1 < items.len() {
            if let Some(name) = bulk_str(&items[idx]) {
                if name == field_name {
                    return Some(&items[idx + 1]);
                }
            }
            idx += 2;
        }
        None
    }

    #[tokio::test]
    async fn hset_returns_added_field_count() {
        let ctx = TestContext::new(false).await;

        let (mut server_stream, client_stream) = tcp_pair().await;
        handle_hset(
            &mut server_stream,
            &ctx.state,
            "users".to_string(),
            "alice".to_string(),
            Bytes::from_static(b"one"),
        )
        .await
        .unwrap();
        server_stream.shutdown().await.unwrap();
        assert_integer_response(read_response(client_stream).await, 1);

        let (mut server_stream, client_stream) = tcp_pair().await;
        handle_hset(
            &mut server_stream,
            &ctx.state,
            "users".to_string(),
            "alice".to_string(),
            Bytes::from_static(b"two"),
        )
        .await
        .unwrap();
        server_stream.shutdown().await.unwrap();
        assert_integer_response(read_response(client_stream).await, 0);

        ctx.shutdown().await;
    }

    #[tokio::test]
    async fn hsetex_returns_new_field_count() {
        let ctx = TestContext::new(false).await;

        let (mut server_stream, client_stream) = tcp_pair().await;
        handle_hsetex(
            &mut server_stream,
            &ctx.state,
            "sessions".to_string(),
            false,
            false,
            None,
            vec![
                ("user1".to_string(), Bytes::from_static(b"alpha")),
                ("user2".to_string(), Bytes::from_static(b"beta")),
            ],
        )
        .await
        .unwrap();
        server_stream.shutdown().await.unwrap();
        assert_integer_response(read_response(client_stream).await, 2);

        let (mut server_stream, client_stream) = tcp_pair().await;
        handle_hsetex(
            &mut server_stream,
            &ctx.state,
            "sessions".to_string(),
            false,
            false,
            None,
            vec![
                ("user1".to_string(), Bytes::from_static(b"updated")),
                ("user3".to_string(), Bytes::from_static(b"gamma")),
            ],
        )
        .await
        .unwrap();
        server_stream.shutdown().await.unwrap();
        assert_integer_response(read_response(client_stream).await, 1);

        ctx.shutdown().await;
    }

    #[tokio::test]
    async fn hset_async_returns_integer_count() {
        let ctx = TestContext::new(true).await;

        let (mut server_stream, client_stream) = tcp_pair().await;
        handle_hset(
            &mut server_stream,
            &ctx.state,
            "flags".to_string(),
            "feature".to_string(),
            Bytes::from_static(b"enabled"),
        )
        .await
        .unwrap();
        server_stream.shutdown().await.unwrap();
        assert_integer_response(read_response(client_stream).await, 1);

        ctx.shutdown().await;
    }

    #[tokio::test]
    async fn hset_rejects_invalid_namespace_sync_mode() {
        let ctx = TestContext::new(false).await;

        let frame = respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_hset(
                    &mut stream,
                    &state,
                    "has-dash".to_string(),
                    "field".to_string(),
                    Bytes::from_static(b"value"),
                )
                .await
            })
        })
        .await;
        assert_error_contains(frame, "invalid hash namespace");

        ctx.shutdown().await;
    }

    #[tokio::test]
    async fn hset_rejects_invalid_namespace_async_mode() {
        let ctx = TestContext::new(true).await;

        let frame = respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_hset(
                    &mut stream,
                    &state,
                    "has-dash".to_string(),
                    "field".to_string(),
                    Bytes::from_static(b"value"),
                )
                .await
            })
        })
        .await;
        assert_error_contains(frame, "invalid hash namespace");

        ctx.shutdown().await;
    }

    #[tokio::test]
    async fn other_hash_commands_reject_invalid_namespace() {
        let ctx = TestContext::new(false).await;

        let frame = respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_hget(
                    &mut stream,
                    &state,
                    "has-dash".to_string(),
                    "field".to_string(),
                )
                .await
            })
        })
        .await;
        assert_error_contains(frame, "invalid hash namespace");

        let frame = respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_hdel(
                    &mut stream,
                    &state,
                    "has-dash".to_string(),
                    "field".to_string(),
                )
                .await
            })
        })
        .await;
        assert_error_contains(frame, "invalid hash namespace");

        let frame = respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_hexists(
                    &mut stream,
                    &state,
                    "has-dash".to_string(),
                    "field".to_string(),
                )
                .await
            })
        })
        .await;
        assert_error_contains(frame, "invalid hash namespace");

        let frame = respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_hsetex(
                    &mut stream,
                    &state,
                    "has-dash".to_string(),
                    false,
                    false,
                    None,
                    vec![("field".to_string(), Bytes::from_static(b"value"))],
                )
                .await
            })
        })
        .await;
        assert_error_contains(frame, "invalid hash namespace");

        ctx.shutdown().await;
    }

    #[tokio::test]
    async fn set_and_get_return_expected_frames() {
        let ctx = TestContext::new(false).await;

        let frame = respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_set(
                    &mut stream,
                    &state,
                    "k1".to_string(),
                    Bytes::from_static(b"v1"),
                    None,
                )
                .await
            })
        })
        .await;
        assert_simple_string(frame, "OK");

        let frame = respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_get(&mut stream, &state, "k1".to_string()).await
            })
        })
        .await;
        assert_bulk_string(frame, b"v1");

        let frame = respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_get(&mut stream, &state, "missing".to_string()).await
            })
        })
        .await;
        assert_null(frame);

        ctx.shutdown().await;
    }

    #[tokio::test]
    async fn del_and_exists_return_counts() {
        let ctx = TestContext::new(false).await;

        respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_set(
                    &mut stream,
                    &state,
                    "to_delete".to_string(),
                    Bytes::from_static(b"v"),
                    None,
                )
                .await
            })
        })
        .await;

        let frame = respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_del_multiple(&mut stream, &state, vec!["to_delete".to_string()]).await
            })
        })
        .await;
        assert_integer_response(frame, 1);

        let frame = respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_del_multiple(&mut stream, &state, vec!["absent".to_string()]).await
            })
        })
        .await;
        assert_integer_response(frame, 0);

        let frame = respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_exists(&mut stream, &state, "absent".to_string()).await
            })
        })
        .await;
        assert_integer_response(frame, 0);

        ctx.shutdown().await;
    }

    #[tokio::test]
    async fn hash_read_delete_and_exists_returns() {
        let ctx = TestContext::new(false).await;

        // Seed field
        respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_hset(
                    &mut stream,
                    &state,
                    "ns".to_string(),
                    "field1".to_string(),
                    Bytes::from_static(b"hash val"),
                )
                .await
            })
        })
        .await;

        let frame = respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_hget(&mut stream, &state, "ns".to_string(), "field1".to_string()).await
            })
        })
        .await;
        assert_bulk_string(frame, b"hash val");

        let frame = respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_hexists(&mut stream, &state, "ns".to_string(), "field1".to_string()).await
            })
        })
        .await;
        assert_integer_response(frame, 1);

        let frame = respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_hdel(&mut stream, &state, "ns".to_string(), "field1".to_string()).await
            })
        })
        .await;
        assert_integer_response(frame, 1);

        let frame = respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_hdel(&mut stream, &state, "ns".to_string(), "field1".to_string()).await
            })
        })
        .await;
        assert_integer_response(frame, 0);

        let frame = respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_hexists(&mut stream, &state, "ns".to_string(), "field1".to_string()).await
            })
        })
        .await;
        assert_integer_response(frame, 0);

        let frame = respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_hget(&mut stream, &state, "ns".to_string(), "field1".to_string()).await
            })
        })
        .await;
        assert_null(frame);

        ctx.shutdown().await;
    }

    #[tokio::test]
    async fn ttl_and_expire_returns_match_redis() {
        let ctx = TestContext::new(false).await;

        let frame = respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_ttl(&mut stream, &state, "nope".to_string()).await
            })
        })
        .await;
        assert_integer_response(frame, -2);

        respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_set(
                    &mut stream,
                    &state,
                    "ttl_key".to_string(),
                    Bytes::from_static(b"val"),
                    None,
                )
                .await
            })
        })
        .await;

        let frame = respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_ttl(&mut stream, &state, "ttl_key".to_string()).await
            })
        })
        .await;
        assert_integer_response(frame, -1);

        let frame = respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_expire(&mut stream, &state, "ttl_key".to_string(), 30).await
            })
        })
        .await;
        assert_integer_response(frame, 1);

        let frame = respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_ttl(&mut stream, &state, "ttl_key".to_string()).await
            })
        })
        .await;
        if let BytesFrame::Integer(v) = frame {
            assert!(
                v >= 1 && v <= 30,
                "expected ttl between 1 and 30, got {}",
                v
            );
        } else {
            panic!("Expected TTL integer");
        }

        let frame = respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_expire(&mut stream, &state, "missing_key".to_string(), 10).await
            })
        })
        .await;
        assert_integer_response(frame, 0);

        ctx.shutdown().await;
    }

    #[tokio::test]
    async fn ping_info_command_quit_and_cluster_keyslot_returns() {
        let ctx = TestContext::new(false).await;

        let frame = respond_with(&ctx, |_, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_ping(&mut stream, None).await
            })
        });
        assert_simple_string(frame.await, "PONG");

        let frame = respond_with(&ctx, |_, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_ping(&mut stream, Some("hey".to_string())).await
            })
        });
        assert_bulk_string(frame.await, b"hey");

        let frame = respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_info(&mut stream, &state, None).await
            })
        });
        match frame.await {
            BytesFrame::BulkString(_) => {}
            other => panic!("Expected bulk string from INFO, got {:?}", other),
        }

        let frame = respond_with(&ctx, |_, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_command(&mut stream).await
            })
        });
        assert_array_len(frame.await, 0);

        let frame = respond_with(&ctx, |_, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_cluster_keyslot(&mut stream, "abc".into()).await
            })
        });
        assert_integer_response(frame.await, ClusterManager::calculate_slot("abc") as i64);

        let frame = respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                let _ = handle_redis_command_inner(&mut stream, &state, RedisCommand::Quit).await;
                Ok(())
            })
        });
        assert_simple_string(frame.await, "OK");

        ctx.shutdown().await;
    }

    #[tokio::test]
    async fn blobasaur_vacuum_shard_all_returns_per_shard_results() {
        let ctx = TestContext::new_with_shards(false, 2).await;

        let frame = respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_redis_command_inner(
                    &mut stream,
                    &state,
                    RedisCommand::BlobasaurVacuum {
                        target: VacuumShardTarget::All,
                        mode: VacuumCommandMode::Incremental,
                        budget_mb: 16,
                        dry_run: true,
                    },
                )
                .await
            })
        })
        .await;

        let top = match frame {
            BytesFrame::Array(items) => items,
            other => panic!("expected array response, got {:?}", other),
        };

        let results_frame = array_field(&top, "results").expect("results field must exist");
        let results = match results_frame {
            BytesFrame::Array(items) => items,
            other => panic!("expected results array, got {:?}", other),
        };
        assert_eq!(results.len(), 2, "expected one result per local shard");

        for item in results {
            let shard_fields = match item {
                BytesFrame::Array(fields) => fields,
                other => panic!("expected shard result array, got {:?}", other),
            };
            let status = array_field(shard_fields, "status")
                .and_then(bulk_str)
                .expect("status should be present");
            assert_eq!(status, "ok");
        }

        ctx.shutdown().await;
    }

    #[tokio::test]
    async fn blobasaur_vacuum_invalid_shard_returns_runtime_error_result() {
        let ctx = TestContext::new(false).await;

        let frame = respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_redis_command_inner(
                    &mut stream,
                    &state,
                    RedisCommand::BlobasaurVacuum {
                        target: VacuumShardTarget::Shard(99),
                        mode: VacuumCommandMode::Incremental,
                        budget_mb: 8,
                        dry_run: true,
                    },
                )
                .await
            })
        })
        .await;

        let top = match frame {
            BytesFrame::Array(items) => items,
            other => panic!("expected array response, got {:?}", other),
        };

        let results = match array_field(&top, "results") {
            Some(BytesFrame::Array(items)) => items,
            Some(other) => panic!("expected results array, got {:?}", other),
            None => panic!("results field missing"),
        };
        assert_eq!(results.len(), 1);

        let shard_fields = match &results[0] {
            BytesFrame::Array(fields) => fields,
            other => panic!("expected shard array, got {:?}", other),
        };

        let status = array_field(shard_fields, "status")
            .and_then(bulk_str)
            .expect("status should exist");
        assert_eq!(status, "invalid_shard");

        ctx.shutdown().await;
    }

    #[tokio::test]
    async fn hexpire_sets_expiration_on_existing_fields() {
        let ctx = TestContext::new(false).await;

        // Seed fields
        respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_hset(
                    &mut stream,
                    &state,
                    "ns".to_string(),
                    "field1".to_string(),
                    Bytes::from_static(b"val1"),
                )
                .await
            })
        })
        .await;

        respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_hset(
                    &mut stream,
                    &state,
                    "ns".to_string(),
                    "field2".to_string(),
                    Bytes::from_static(b"val2"),
                )
                .await
            })
        })
        .await;

        // Set expiration on both fields + one nonexistent field
        let frame = respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_hexpire(
                    &mut stream,
                    &state,
                    "ns".to_string(),
                    30,
                    None,
                    vec![
                        "field1".to_string(),
                        "field2".to_string(),
                        "field3".to_string(),
                    ],
                )
                .await
            })
        })
        .await;

        // Should return array [1, 1, -2]
        match frame {
            BytesFrame::Array(items) => {
                assert_eq!(items.len(), 3);
                assert_eq!(items[0], BytesFrame::Integer(1));
                assert_eq!(items[1], BytesFrame::Integer(1));
                assert_eq!(items[2], BytesFrame::Integer(-2));
            }
            other => panic!("Expected array, got {:?}", other),
        }

        ctx.shutdown().await;
    }

    #[tokio::test]
    async fn hexpire_rejects_invalid_namespace() {
        let ctx = TestContext::new(false).await;

        let frame = respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_hexpire(
                    &mut stream,
                    &state,
                    "has-dash".to_string(),
                    10,
                    None,
                    vec!["field".to_string()],
                )
                .await
            })
        })
        .await;
        assert_error_contains(frame, "invalid hash namespace");

        ctx.shutdown().await;
    }

    #[tokio::test]
    async fn hexpire_nx_only_sets_when_no_expiration() {
        let ctx = TestContext::new(false).await;

        // Seed field without expiration
        respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_hset(
                    &mut stream,
                    &state,
                    "ns".to_string(),
                    "field1".to_string(),
                    Bytes::from_static(b"val1"),
                )
                .await
            })
        })
        .await;

        // NX should succeed (no expiration set yet)
        let frame = respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_hexpire(
                    &mut stream,
                    &state,
                    "ns".to_string(),
                    60,
                    Some(HExpireCondition::Nx),
                    vec!["field1".to_string()],
                )
                .await
            })
        })
        .await;
        match frame {
            BytesFrame::Array(items) => {
                assert_eq!(items.len(), 1);
                assert_eq!(items[0], BytesFrame::Integer(1));
            }
            other => panic!("Expected array, got {:?}", other),
        }

        // NX should fail now (expiration already set)
        let frame = respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_hexpire(
                    &mut stream,
                    &state,
                    "ns".to_string(),
                    120,
                    Some(HExpireCondition::Nx),
                    vec!["field1".to_string()],
                )
                .await
            })
        })
        .await;
        match frame {
            BytesFrame::Array(items) => {
                assert_eq!(items.len(), 1);
                assert_eq!(items[0], BytesFrame::Integer(0));
            }
            other => panic!("Expected array, got {:?}", other),
        }

        ctx.shutdown().await;
    }

    #[tokio::test]
    async fn hexpire_immediate_expiry_still_respects_conditions() {
        let ctx = TestContext::new(false).await;

        // Seed field without expiration.
        respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_hset(
                    &mut stream,
                    &state,
                    "ns".to_string(),
                    "field1".to_string(),
                    Bytes::from_static(b"val1"),
                )
                .await
            })
        })
        .await;

        // XX should reject because field has no TTL, even with immediate expiration.
        let frame = respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_hexpire(
                    &mut stream,
                    &state,
                    "ns".to_string(),
                    0,
                    Some(HExpireCondition::Xx),
                    vec!["field1".to_string()],
                )
                .await
            })
        })
        .await;
        match frame {
            BytesFrame::Array(items) => {
                assert_eq!(items.len(), 1);
                assert_eq!(items[0], BytesFrame::Integer(0));
            }
            other => panic!("Expected array, got {:?}", other),
        }

        // Field should remain untouched.
        let frame = respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_hget(&mut stream, &state, "ns".to_string(), "field1".to_string()).await
            })
        })
        .await;
        assert_bulk_string(frame, b"val1");

        ctx.shutdown().await;
    }

    #[tokio::test]
    async fn hexpire_treats_logically_expired_fields_as_missing() {
        let ctx = TestContext::new(false).await;

        // Seed field.
        respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_hset(
                    &mut stream,
                    &state,
                    "ns".to_string(),
                    "field1".to_string(),
                    Bytes::from_static(b"val1"),
                )
                .await
            })
        })
        .await;

        // Mark row as expired in-place (without cleanup deleting it yet).
        let shard_index = ctx.state.get_shard("field1");
        let pool = &ctx.state.db_pools[shard_index];
        let past = chrono::Utc::now().timestamp() - 60;
        sqlx::query("UPDATE blobs_ns SET expires_at = ? WHERE key = ?")
            .bind(past)
            .bind("field1")
            .execute(pool)
            .await
            .expect("failed to force field expiration");

        // Expired field should be treated as missing and not revived.
        let frame = respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_hexpire(
                    &mut stream,
                    &state,
                    "ns".to_string(),
                    60,
                    None,
                    vec!["field1".to_string()],
                )
                .await
            })
        })
        .await;
        match frame {
            BytesFrame::Array(items) => {
                assert_eq!(items.len(), 1);
                assert_eq!(items[0], BytesFrame::Integer(-2));
            }
            other => panic!("Expected array, got {:?}", other),
        }

        let frame = respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_hget(&mut stream, &state, "ns".to_string(), "field1".to_string()).await
            })
        })
        .await;
        assert_null(frame);

        ctx.shutdown().await;
    }

    #[tokio::test]
    async fn hexpire_miss_does_not_create_namespace_table() {
        let ctx = TestContext::new(false).await;

        let namespace = "ghost_ns";
        let table_name = format!("blobs_{}", namespace);
        let shard_index = ctx.state.get_shard("field1");
        let pool = &ctx.state.db_pools[shard_index];

        let before = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(1) FROM sqlite_master WHERE type = 'table' AND name = ?",
        )
        .bind(&table_name)
        .fetch_one(pool)
        .await
        .expect("failed to check table existence before HEXPIRE");
        assert_eq!(before, 0);

        let frame = respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_hexpire(
                    &mut stream,
                    &state,
                    namespace.to_string(),
                    60,
                    None,
                    vec!["field1".to_string()],
                )
                .await
            })
        })
        .await;
        match frame {
            BytesFrame::Array(items) => {
                assert_eq!(items.len(), 1);
                assert_eq!(items[0], BytesFrame::Integer(-2));
            }
            other => panic!("Expected array, got {:?}", other),
        }

        let after = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(1) FROM sqlite_master WHERE type = 'table' AND name = ?",
        )
        .bind(&table_name)
        .fetch_one(pool)
        .await
        .expect("failed to check table existence after HEXPIRE");
        assert_eq!(after, 0);

        ctx.shutdown().await;
    }

    #[tokio::test]
    async fn hexpireat_sets_absolute_expiration() {
        let ctx = TestContext::new(false).await;

        // Seed a field
        respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_hset(
                    &mut stream,
                    &state,
                    "ns".to_string(),
                    "field1".to_string(),
                    Bytes::from_static(b"val1"),
                )
                .await
            })
        })
        .await;

        // Set absolute expiration far in the future
        let future_ts = chrono::Utc::now().timestamp() + 3600;
        let frame = respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_hexpireat(
                    &mut stream,
                    &state,
                    "ns".to_string(),
                    future_ts,
                    None,
                    vec!["field1".to_string(), "nonexistent".to_string()],
                )
                .await
            })
        })
        .await;

        match frame {
            BytesFrame::Array(items) => {
                assert_eq!(items.len(), 2);
                assert_eq!(items[0], BytesFrame::Integer(1)); // field1 set
                assert_eq!(items[1], BytesFrame::Integer(-2)); // nonexistent
            }
            other => panic!("Expected array, got {:?}", other),
        }

        ctx.shutdown().await;
    }

    #[tokio::test]
    async fn hexpireat_past_timestamp_deletes_field() {
        let ctx = TestContext::new(false).await;

        // Seed a field
        respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_hset(
                    &mut stream,
                    &state,
                    "ns".to_string(),
                    "field1".to_string(),
                    Bytes::from_static(b"val1"),
                )
                .await
            })
        })
        .await;

        // Set expiration to a past timestamp -> should delete
        let past_ts = chrono::Utc::now().timestamp() - 100;
        let frame = respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_hexpireat(
                    &mut stream,
                    &state,
                    "ns".to_string(),
                    past_ts,
                    None,
                    vec!["field1".to_string()],
                )
                .await
            })
        })
        .await;

        match frame {
            BytesFrame::Array(items) => {
                assert_eq!(items.len(), 1);
                assert_eq!(items[0], BytesFrame::Integer(2)); // deleted
            }
            other => panic!("Expected array, got {:?}", other),
        }

        // Confirm field is gone
        let frame = respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_hget(&mut stream, &state, "ns".to_string(), "field1".to_string()).await
            })
        })
        .await;
        assert_null(frame);

        ctx.shutdown().await;
    }

    #[tokio::test]
    async fn cluster_commands_error_when_disabled() {
        let ctx = TestContext::new(false).await;

        let frame = respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_cluster_nodes(&mut stream, &state).await
            })
        });
        assert_error_contains(frame.await, "cluster support disabled");

        let frame = respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_cluster_info(&mut stream, &state).await
            })
        });
        assert_error_contains(frame.await, "cluster support disabled");

        let frame = respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_cluster_addslots(&mut stream, &state, vec![1, 2, 3]).await
            })
        });
        assert_error_contains(frame.await, "cluster support disabled");

        let frame = respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_cluster_delslots(&mut stream, &state, vec![1, 2, 3]).await
            })
        });
        assert_error_contains(frame.await, "cluster support disabled");

        ctx.shutdown().await;
    }
}
