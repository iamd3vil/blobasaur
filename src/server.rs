use crate::AppState;
use crate::cluster::ClusterManager;
use crate::metrics::Timer;
use crate::redis::{
    ParseError, RedisCommand, parse_command, parse_resp_with_remaining, serialize_frame,
};
use crate::shard_manager::ShardWriteOperation;
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
        RedisCommand::MGet { keys } => {
            // For MGET in cluster mode, all keys must hash to the same slot
            // Otherwise return CROSSSLOT error (Redis behavior)
            if let Some(ref cluster_manager) = state.cluster_manager {
                if !keys.is_empty() {
                    let first_slot = ClusterManager::calculate_slot(&keys[0]);
                    for key in &keys[1..] {
                        let slot = ClusterManager::calculate_slot(key);
                        if slot != first_slot {
                            let response = BytesFrame::Error(
                                "CROSSSLOT Keys in request don't hash to the same slot".into(),
                            );
                            stream.write_all(&serialize_frame(&response)).await?;
                            return Ok(());
                        }
                    }
                    // All keys are in the same slot, check if we handle it locally
                    if !cluster_manager.should_handle_locally(&keys[0]).await {
                        if let Some(redirect) =
                            cluster_manager.get_redirect_response(&keys[0]).await
                        {
                            let response = BytesFrame::Error(redirect.into());
                            stream.write_all(&serialize_frame(&response)).await?;
                            return Ok(());
                        }
                    }
                }
            }
            handle_mget(stream, state, keys).await?;
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
        RedisCommand::MSet { key_values } => {
            // For MSET in cluster mode, all keys must hash to the same slot
            // Otherwise return CROSSSLOT error (Redis behavior)
            if let Some(ref cluster_manager) = state.cluster_manager {
                if !key_values.is_empty() {
                    let first_slot = ClusterManager::calculate_slot(&key_values[0].0);
                    for (key, _) in &key_values[1..] {
                        let slot = ClusterManager::calculate_slot(key);
                        if slot != first_slot {
                            let response = BytesFrame::Error(
                                "CROSSSLOT Keys in request don't hash to the same slot".into(),
                            );
                            stream.write_all(&serialize_frame(&response)).await?;
                            return Ok(());
                        }
                    }
                    // All keys are in the same slot, check if we handle it locally
                    if !cluster_manager.should_handle_locally(&key_values[0].0).await {
                        if let Some(redirect) =
                            cluster_manager.get_redirect_response(&key_values[0].0).await
                        {
                            let response = BytesFrame::Error(redirect.into());
                            stream.write_all(&serialize_frame(&response)).await?;
                            return Ok(());
                        }
                    }
                }
            }
            handle_mset(stream, state, key_values).await?;
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
        RedisCommand::HMGet { namespace, keys } => {
            // For HMGET in cluster mode, all fields must hash to the same slot
            // Otherwise return CROSSSLOT error (Redis behavior)
            if let Some(ref cluster_manager) = state.cluster_manager {
                if !keys.is_empty() {
                    let first_slot = cluster_manager.calculate_slot_for_hash(&namespace, &keys[0]);
                    for key in &keys[1..] {
                        let slot = cluster_manager.calculate_slot_for_hash(&namespace, key);
                        if slot != first_slot {
                            let response = BytesFrame::Error(
                                "CROSSSLOT Keys in request don't hash to the same slot".into(),
                            );
                            stream.write_all(&serialize_frame(&response)).await?;
                            return Ok(());
                        }
                    }
                    // All keys are in the same slot, check if we handle it locally
                    if !cluster_manager
                        .should_handle_hash_locally(&namespace, &keys[0])
                        .await
                    {
                        if let Some(redirect) = cluster_manager
                            .get_hash_redirect_response(&namespace, &keys[0])
                            .await
                        {
                            let response = BytesFrame::Error(redirect.into());
                            stream.write_all(&serialize_frame(&response)).await?;
                            return Ok(());
                        }
                    }
                }
            }
            handle_hmget(stream, state, namespace, keys).await?;
        }
        RedisCommand::HSet {
            namespace,
            key,
            value,
        } => {
            handle_hset(stream, state, namespace, key, value).await?;
        }
        RedisCommand::HMSet {
            namespace,
            field_values,
        } => {
            // For HMSET in cluster mode, all fields must hash to the same slot
            // Otherwise return CROSSSLOT error (Redis behavior)
            if let Some(ref cluster_manager) = state.cluster_manager {
                if !field_values.is_empty() {
                    let first_slot =
                        cluster_manager.calculate_slot_for_hash(&namespace, &field_values[0].0);
                    for (field, _) in &field_values[1..] {
                        let slot = cluster_manager.calculate_slot_for_hash(&namespace, field);
                        if slot != first_slot {
                            let response = BytesFrame::Error(
                                "CROSSSLOT Keys in request don't hash to the same slot".into(),
                            );
                            stream.write_all(&serialize_frame(&response)).await?;
                            return Ok(());
                        }
                    }
                    // All fields are in the same slot, check if we handle it locally
                    if !cluster_manager
                        .should_handle_hash_locally(&namespace, &field_values[0].0)
                        .await
                    {
                        if let Some(redirect) = cluster_manager
                            .get_hash_redirect_response(&namespace, &field_values[0].0)
                            .await
                        {
                            let response = BytesFrame::Error(redirect.into());
                            stream.write_all(&serialize_frame(&response)).await?;
                            return Ok(());
                        }
                    }
                }
            }
            handle_hmset(stream, state, namespace, field_values).await?;
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

async fn handle_mget(
    stream: &mut TcpStream,
    state: &Arc<AppState>,
    keys: Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let now = chrono::Utc::now().timestamp();
    let mut results: Vec<BytesFrame> = Vec::with_capacity(keys.len());

    for key in keys {
        // First check inflight cache for pending writes
        if let Some(data) = state.inflight_cache.get(&key).await {
            // Decompress if needed
            let data = decompress_if_enabled(state, data).await?;
            results.push(BytesFrame::BulkString(data.into()));
            state.metrics.record_cache_hit();
            continue;
        }

        let shard_index = state.get_shard(&key);
        let pool = &state.db_pools[shard_index];

        match sqlx::query_as::<_, (Vec<u8>,)>(
            "SELECT data FROM blobs WHERE key = ? AND (expires_at IS NULL OR expires_at > ?)",
        )
        .bind(&key)
        .bind(now)
        .fetch_optional(pool)
        .await
        {
            Ok(Some(row)) => {
                // Decompress if needed
                let data = decompress_if_enabled(state, row.0.into()).await?;
                results.push(BytesFrame::BulkString(data.into()));
                state.metrics.record_cache_hit();
            }
            Ok(None) => {
                results.push(BytesFrame::Null);
                state.metrics.record_cache_miss();
            }
            Err(e) => {
                tracing::error!("Failed to MGET key {}: {}", key, e);
                // For MGET, we return null for failed keys rather than erroring the whole operation
                results.push(BytesFrame::Null);
                state.metrics.record_error("storage");
            }
        }
    }

    let response = BytesFrame::Array(results);
    stream.write_all(&serialize_frame(&response)).await?;
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

async fn handle_mset(
    stream: &mut TcpStream,
    state: &Arc<AppState>,
    key_values: Vec<(String, Bytes)>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Note: Cluster routing checks (CROSSSLOT, MOVED) are handled at the command handler level
    // MSET is atomic in Redis - either all keys are set or none are
    // We'll process all keys and only return OK if all succeed

    for (key, value) in key_values {
        let shard_index = state.get_shard(&key);
        let sender = &state.shard_senders[shard_index];

        let value = compress_if_enabled(state, value).await?;

        if state.cfg.async_write.unwrap_or(false) {
            // Store in inflight cache to prevent race conditions
            state
                .inflight_cache
                .insert(key.clone(), value.clone())
                .await;

            let operation = ShardWriteOperation::SetAsync {
                key,
                data: value,
                expires_at: None,
            };

            if sender.send(operation).await.is_err() {
                tracing::error!(
                    "Failed to send ASYNC SET operation to shard {}",
                    shard_index
                );
                let response = BytesFrame::Error("ERR internal error".into());
                stream.write_all(&serialize_frame(&response)).await?;
                state.metrics.record_error("storage");
                return Ok(());
            }
            state.metrics.record_storage_operation();
        } else {
            // Sync mode: wait for completion
            let (responder_tx, responder_rx) = oneshot::channel();

            let operation = ShardWriteOperation::Set {
                key,
                data: value,
                expires_at: None,
                responder: responder_tx,
            };

            if sender.send(operation).await.is_err() {
                tracing::error!("Failed to send SET operation to shard {}", shard_index);
                let response = BytesFrame::Error("ERR internal error".into());
                stream.write_all(&serialize_frame(&response)).await?;
                state.metrics.record_error("storage");
                return Ok(());
            }

            match responder_rx.await {
                Ok(Ok(())) => {
                    state.metrics.record_storage_operation();
                }
                Ok(Err(e)) => {
                    tracing::error!("Shard writer failed for MSET: {}", e);
                    let response = BytesFrame::Error("ERR database error".into());
                    stream.write_all(&serialize_frame(&response)).await?;
                    state.metrics.record_error("storage");
                    return Ok(());
                }
                Err(_) => {
                    tracing::error!("Shard writer task cancelled or panicked for MSET");
                    let response = BytesFrame::Error("ERR internal error".into());
                    stream.write_all(&serialize_frame(&response)).await?;
                    state.metrics.record_error("storage");
                    return Ok(());
                }
            }
        }
    }

    // All keys set successfully
    let response = BytesFrame::SimpleString("OK".into());
    stream.write_all(&serialize_frame(&response)).await?;
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

async fn handle_hmget(
    stream: &mut TcpStream,
    state: &Arc<AppState>,
    namespace: String,
    keys: Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Note: Cluster routing checks (CROSSSLOT, MOVED) are handled at the command handler level
    let now = chrono::Utc::now().timestamp();
    let table_name = format!("blobs_{}", namespace);
    let mut results: Vec<BytesFrame> = Vec::with_capacity(keys.len());

    for key in keys {
        // First check inflight cache for pending writes
        let namespaced_key = state.namespaced_key(&namespace, &key);
        if let Some(data) = state.inflight_hcache.get(&namespaced_key).await {
            // Decompress if needed
            let data = decompress_if_enabled(state, data).await?;
            results.push(BytesFrame::BulkString(data.into()));
            continue;
        }

        let shard_index = state.get_shard(&key);
        let pool = &state.db_pools[shard_index];

        let query = format!(
            "SELECT data FROM {} WHERE key = ? AND (expires_at IS NULL OR expires_at > ?)",
            table_name
        );

        match sqlx::query_as::<_, (Vec<u8>,)>(&query)
            .bind(&key)
            .bind(now)
            .fetch_optional(pool)
            .await
        {
            Ok(Some(row)) => {
                // Decompress if needed
                let data = decompress_if_enabled(state, row.0.into()).await?;
                results.push(BytesFrame::BulkString(data.into()));
            }
            Ok(None) => {
                results.push(BytesFrame::Null);
            }
            Err(e) => {
                tracing::error!(
                    "Failed to HMGET namespace {} key {}: {}",
                    namespace,
                    key,
                    e
                );
                // For HMGET, we return null for failed keys rather than erroring the whole operation
                results.push(BytesFrame::Null);
            }
        }
    }

    let response = BytesFrame::Array(results);
    stream.write_all(&serialize_frame(&response)).await?;
    Ok(())
}

async fn handle_hset(
    stream: &mut TcpStream,
    state: &Arc<AppState>,
    namespace: String,
    key: String,
    value: Bytes,
) -> Result<(), Box<dyn std::error::Error>> {
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

async fn handle_hmset(
    stream: &mut TcpStream,
    state: &Arc<AppState>,
    namespace: String,
    field_values: Vec<(String, Bytes)>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Note: Cluster routing checks (CROSSSLOT, MOVED) are handled at the command handler level
    // HMSET always returns OK (unlike HSET which returns count of new fields)

    for (field, value) in field_values {
        let shard_index = state.get_shard(&field);
        let sender = &state.shard_senders[shard_index];

        // Compress data if storage compression is enabled
        let value = compress_if_enabled(state, value).await?;

        if state.cfg.async_write.unwrap_or(false) {
            // Store in inflight cache to prevent race conditions
            let namespaced_key = state.namespaced_key(&namespace, &field);
            state
                .inflight_hcache
                .insert(namespaced_key, value.clone())
                .await;

            let operation = ShardWriteOperation::HSetAsync {
                namespace: namespace.clone(),
                key: field,
                data: value,
            };

            if sender.send(operation).await.is_err() {
                tracing::error!(
                    "Failed to send ASYNC HSET operation to shard {}",
                    shard_index
                );
                let response = BytesFrame::Error("ERR internal error".into());
                stream.write_all(&serialize_frame(&response)).await?;
                state.metrics.record_error("storage");
                return Ok(());
            }
            state.metrics.record_storage_operation();
        } else {
            // Sync mode: wait for completion
            let (responder_tx, responder_rx) = oneshot::channel();

            let operation = ShardWriteOperation::HSet {
                namespace: namespace.clone(),
                key: field,
                data: value,
                responder: responder_tx,
            };

            if sender.send(operation).await.is_err() {
                tracing::error!("Failed to send HSET operation to shard {}", shard_index);
                let response = BytesFrame::Error("ERR internal error".into());
                stream.write_all(&serialize_frame(&response)).await?;
                state.metrics.record_error("storage");
                return Ok(());
            }

            match responder_rx.await {
                Ok(Ok(())) => {
                    state.metrics.record_storage_operation();
                }
                Ok(Err(e)) => {
                    tracing::error!("Shard writer failed for HMSET: {}", e);
                    let response = BytesFrame::Error("ERR database error".into());
                    stream.write_all(&serialize_frame(&response)).await?;
                    state.metrics.record_error("storage");
                    return Ok(());
                }
                Err(_) => {
                    tracing::error!("Shard writer task cancelled or panicked for HMSET");
                    let response = BytesFrame::Error("ERR internal error".into());
                    stream.write_all(&serialize_frame(&response)).await?;
                    state.metrics.record_error("storage");
                    return Ok(());
                }
            }
        }
    }

    // All fields set successfully
    let response = BytesFrame::SimpleString("OK".into());
    stream.write_all(&serialize_frame(&response)).await?;
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
            let temp_dir = TempDir::new().expect("temp dir");
            let cfg = Cfg {
                data_dir: temp_dir.path().to_string_lossy().into_owned(),
                num_shards: 1,
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

    #[tokio::test]
    async fn mget_returns_array_of_values() {
        let ctx = TestContext::new(false).await;

        // Set up some keys
        respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_set(
                    &mut stream,
                    &state,
                    "mget_key1".to_string(),
                    Bytes::from_static(b"value1"),
                    None,
                )
                .await
            })
        })
        .await;

        respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_set(
                    &mut stream,
                    &state,
                    "mget_key2".to_string(),
                    Bytes::from_static(b"value2"),
                    None,
                )
                .await
            })
        })
        .await;

        // MGET with existing keys and missing key
        let frame = respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_mget(
                    &mut stream,
                    &state,
                    vec![
                        "mget_key1".to_string(),
                        "mget_missing".to_string(),
                        "mget_key2".to_string(),
                    ],
                )
                .await
            })
        })
        .await;

        match frame {
            BytesFrame::Array(items) => {
                assert_eq!(items.len(), 3);
                assert_bulk_string(items[0].clone(), b"value1");
                assert_null(items[1].clone());
                assert_bulk_string(items[2].clone(), b"value2");
            }
            other => panic!("Expected array, got {:?}", other),
        }

        ctx.shutdown().await;
    }

    #[tokio::test]
    async fn hmget_returns_array_of_values() {
        let ctx = TestContext::new(false).await;

        // Set up some hash fields
        respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_hset(
                    &mut stream,
                    &state,
                    "hmget_hash".to_string(),
                    "field1".to_string(),
                    Bytes::from_static(b"hash_value1"),
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
                    "hmget_hash".to_string(),
                    "field2".to_string(),
                    Bytes::from_static(b"hash_value2"),
                )
                .await
            })
        })
        .await;

        // HMGET with existing fields and missing field
        let frame = respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_hmget(
                    &mut stream,
                    &state,
                    "hmget_hash".to_string(),
                    vec![
                        "field1".to_string(),
                        "missing_field".to_string(),
                        "field2".to_string(),
                    ],
                )
                .await
            })
        })
        .await;

        match frame {
            BytesFrame::Array(items) => {
                assert_eq!(items.len(), 3);
                assert_bulk_string(items[0].clone(), b"hash_value1");
                assert_null(items[1].clone());
                assert_bulk_string(items[2].clone(), b"hash_value2");
            }
            other => panic!("Expected array, got {:?}", other),
        }

        ctx.shutdown().await;
    }

    #[tokio::test]
    async fn mget_empty_keys_returns_empty_array() {
        let ctx = TestContext::new(false).await;

        let frame = respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_mget(&mut stream, &state, vec![]).await
            })
        })
        .await;

        assert_array_len(frame, 0);

        ctx.shutdown().await;
    }

    #[tokio::test]
    async fn hmget_empty_keys_returns_empty_array() {
        let ctx = TestContext::new(false).await;

        let frame = respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_hmget(&mut stream, &state, "some_hash".to_string(), vec![]).await
            })
        })
        .await;

        assert_array_len(frame, 0);

        ctx.shutdown().await;
    }

    #[tokio::test]
    async fn mset_sets_multiple_keys() {
        let ctx = TestContext::new(false).await;

        // MSET multiple keys
        let frame = respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_mset(
                    &mut stream,
                    &state,
                    vec![
                        ("mset_key1".to_string(), Bytes::from_static(b"mset_value1")),
                        ("mset_key2".to_string(), Bytes::from_static(b"mset_value2")),
                    ],
                )
                .await
            })
        })
        .await;
        assert_simple_string(frame, "OK");

        // Verify keys were set correctly
        let frame = respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_get(&mut stream, &state, "mset_key1".to_string()).await
            })
        })
        .await;
        assert_bulk_string(frame, b"mset_value1");

        let frame = respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_get(&mut stream, &state, "mset_key2".to_string()).await
            })
        })
        .await;
        assert_bulk_string(frame, b"mset_value2");

        ctx.shutdown().await;
    }

    #[tokio::test]
    async fn mset_empty_returns_ok() {
        let ctx = TestContext::new(false).await;

        let frame = respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_mset(&mut stream, &state, vec![]).await
            })
        })
        .await;
        assert_simple_string(frame, "OK");

        ctx.shutdown().await;
    }

    #[tokio::test]
    async fn hmset_sets_multiple_fields() {
        let ctx = TestContext::new(false).await;

        // HMSET multiple fields
        let frame = respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_hmset(
                    &mut stream,
                    &state,
                    "hmset_hash".to_string(),
                    vec![
                        (
                            "field1".to_string(),
                            Bytes::from_static(b"hmset_value1"),
                        ),
                        (
                            "field2".to_string(),
                            Bytes::from_static(b"hmset_value2"),
                        ),
                    ],
                )
                .await
            })
        })
        .await;
        assert_simple_string(frame, "OK");

        // Verify fields were set correctly
        let frame = respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_hget(&mut stream, &state, "hmset_hash".to_string(), "field1".to_string())
                    .await
            })
        })
        .await;
        assert_bulk_string(frame, b"hmset_value1");

        let frame = respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_hget(&mut stream, &state, "hmset_hash".to_string(), "field2".to_string())
                    .await
            })
        })
        .await;
        assert_bulk_string(frame, b"hmset_value2");

        ctx.shutdown().await;
    }

    #[tokio::test]
    async fn hmset_empty_returns_ok() {
        let ctx = TestContext::new(false).await;

        let frame = respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_hmset(&mut stream, &state, "some_hash".to_string(), vec![]).await
            })
        })
        .await;
        assert_simple_string(frame, "OK");

        ctx.shutdown().await;
    }
}
