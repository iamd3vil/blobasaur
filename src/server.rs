use crate::AppState;
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
    tracing::info!("Blobnom server listening on {}", addr);

    loop {
        let (stream, addr) = listener.accept().await?;
        tracing::debug!("New Blobnom connection from {}", addr);

        let state_clone = state.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, state_clone).await {
                tracing::error!("Error handling connection from {}: {}", addr, e);
            }
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
    match command {
        RedisCommand::Get { key } => {
            handle_get(stream, state, key).await?;
        }
        RedisCommand::Set { key, value } => {
            handle_set(stream, state, key, value).await?;
        }
        RedisCommand::Del { key } => {
            handle_del(stream, state, key).await?;
        }
        RedisCommand::Exists { key } => {
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

async fn handle_get(
    stream: &mut TcpStream,
    state: &Arc<AppState>,
    key: String,
) -> Result<(), Box<dyn std::error::Error>> {
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
            let response = BytesFrame::BulkString(row.0.into());
            stream.write_all(&serialize_frame(&response)).await?;
        }
        Ok(None) => {
            let response = BytesFrame::Null;
            stream.write_all(&serialize_frame(&response)).await?;
        }
        Err(e) => {
            tracing::error!("Failed to GET key {}: {}", key, e);
            let response = BytesFrame::Error("ERR database error".into());
            stream.write_all(&serialize_frame(&response)).await?;
        }
    }

    Ok(())
}

async fn handle_set(
    stream: &mut TcpStream,
    state: &Arc<AppState>,
    key: String,
    value: Bytes,
) -> Result<(), Box<dyn std::error::Error>> {
    let shard_index = state.get_shard(&key);
    let sender = &state.shard_senders[shard_index];

    // Check if async_write is enabled
    if state.cfg.async_write.unwrap_or(false) {
        // Async mode: respond immediately after queueing
        let operation = ShardWriteOperation::SetAsync {
            key,
            data: value.to_vec(),
        };

        if sender.send(operation).await.is_err() {
            tracing::error!(
                "Failed to send ASYNC SET operation to shard {}",
                shard_index
            );
            let response = BytesFrame::Error("ERR internal error".into());
            stream.write_all(&serialize_frame(&response)).await?;
        } else {
            let response = BytesFrame::SimpleString("OK".into());
            stream.write_all(&serialize_frame(&response)).await?;
        }
    } else {
        // Sync mode: wait for completion
        let (responder_tx, responder_rx) = oneshot::channel();

        let operation = ShardWriteOperation::Set {
            key,
            data: value.to_vec(),
            responder: responder_tx,
        };

        if sender.send(operation).await.is_err() {
            tracing::error!("Failed to send SET operation to shard {}", shard_index);
            let response = BytesFrame::Error("ERR internal error".into());
            stream.write_all(&serialize_frame(&response)).await?;
        } else {
            match responder_rx.await {
                Ok(Ok(())) => {
                    let response = BytesFrame::SimpleString("OK".into());
                    stream.write_all(&serialize_frame(&response)).await?;
                }
                Ok(Err(e)) => {
                    tracing::error!("Shard writer failed for SET: {}", e);
                    let response = BytesFrame::Error("ERR database error".into());
                    stream.write_all(&serialize_frame(&response)).await?;
                }
                Err(_) => {
                    tracing::error!("Shard writer task cancelled or panicked for SET");
                    let response = BytesFrame::Error("ERR internal error".into());
                    stream.write_all(&serialize_frame(&response)).await?;
                }
            }
        }
    }

    Ok(())
}

async fn handle_del(
    stream: &mut TcpStream,
    state: &Arc<AppState>,
    key: String,
) -> Result<(), Box<dyn std::error::Error>> {
    let shard_index = state.get_shard(&key);

    // First check if key exists
    let pool = &state.db_pools[shard_index];
    let exists = sqlx::query("SELECT 1 FROM blobs WHERE key = ?")
        .bind(&key)
        .fetch_optional(pool)
        .await
        .map(|row| row.is_some())
        .unwrap_or(false);

    if !exists {
        // Redis DEL returns the number of keys deleted
        let response = BytesFrame::Integer(0);
        stream.write_all(&serialize_frame(&response)).await?;
        return Ok(());
    }

    let sender = &state.shard_senders[shard_index];

    // Check if async_write is enabled
    if state.cfg.async_write.unwrap_or(false) {
        // Async mode: respond immediately after queueing
        let operation = ShardWriteOperation::DeleteAsync { key };

        if sender.send(operation).await.is_err() {
            tracing::error!(
                "Failed to send ASYNC DELETE operation to shard {}",
                shard_index
            );
            let response = BytesFrame::Error("ERR internal error".into());
            stream.write_all(&serialize_frame(&response)).await?;
        } else {
            // Assume success for async mode
            let response = BytesFrame::Integer(1);
            stream.write_all(&serialize_frame(&response)).await?;
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
        } else {
            match responder_rx.await {
                Ok(Ok(())) => {
                    let response = BytesFrame::Integer(1);
                    stream.write_all(&serialize_frame(&response)).await?;
                }
                Ok(Err(e)) => {
                    tracing::error!("Shard writer failed for DELETE: {}", e);
                    let response = BytesFrame::Error("ERR database error".into());
                    stream.write_all(&serialize_frame(&response)).await?;
                }
                Err(_) => {
                    tracing::error!("Shard writer task cancelled or panicked for DELETE");
                    let response = BytesFrame::Error("ERR internal error".into());
                    stream.write_all(&serialize_frame(&response)).await?;
                }
            }
        }
    }

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
            let response = BytesFrame::Error("ERR database error".into());
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
                 redis_version:blobnom-0.1.0\r\n\
                 redis_mode:standalone\r\n\
                 process_id:{}\r\n\
                 tcp_port:6379\r\n\
                 uptime_in_seconds:unknown\r\n\
                 \r\n\
                 # Keyspace\r\n\
                 db0:keys=unknown,expires=0,avg_ttl=0\r\n\
                 \r\n\
                 # Blobnom\r\n\
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
