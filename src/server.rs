use crate::AppState;
use crate::shard_manager::ShardWriteOperation;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

const CRLF: &[u8] = b"\r\n";

#[derive(Debug)]
enum RedisCommand {
    Get { key: String },
    Set { key: String, value: Vec<u8> },
    Del { key: String },
    Quit,
    Unknown,
}

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
    let mut buffer = vec![0; 4096];

    loop {
        let n = match stream.read(&mut buffer).await {
            Ok(0) => return Ok(()), // Connection closed
            Ok(n) => n,
            Err(e) => {
                tracing::error!("Failed to read from socket: {}", e);
                return Err(Box::new(e));
            }
        };

        let command = parse_redis_command(&buffer[..n])?;

        match command {
            RedisCommand::Get { key } => {
                handle_get(&mut stream, &state, key).await?;
            }
            RedisCommand::Set { key, value } => {
                handle_set(&mut stream, &state, key, value).await?;
            }
            RedisCommand::Del { key } => {
                handle_del(&mut stream, &state, key).await?;
            }
            RedisCommand::Quit => {
                stream.write_all(b"+OK\r\n").await?;
                return Ok(());
            }
            RedisCommand::Unknown => {
                stream.write_all(b"-ERR unknown command\r\n").await?;
            }
        }
    }
}

fn parse_redis_command(data: &[u8]) -> Result<RedisCommand, Box<dyn std::error::Error>> {
    // Redis uses RESP protocol
    // Commands come as arrays: *<count>\r\n$<len>\r\n<data>\r\n...

    let s = String::from_utf8_lossy(data);
    let lines: Vec<&str> = s.split("\r\n").collect();

    if lines.is_empty() || !lines[0].starts_with('*') {
        return Ok(RedisCommand::Unknown);
    }

    let count = lines[0][1..].parse::<usize>().unwrap_or(0);
    if count == 0 {
        return Ok(RedisCommand::Unknown);
    }

    // Extract command parts
    let mut parts = Vec::new();
    let mut i = 1;
    while i < lines.len() && parts.len() < count {
        if lines[i].starts_with('$') {
            i += 1; // Skip length line
            if i < lines.len() {
                parts.push(lines[i]);
                i += 1;
            }
        } else {
            i += 1;
        }
    }

    if parts.is_empty() {
        return Ok(RedisCommand::Unknown);
    }

    let cmd = parts[0].to_uppercase();

    match cmd.as_str() {
        "GET" => {
            if parts.len() >= 2 {
                Ok(RedisCommand::Get {
                    key: parts[1].to_string(),
                })
            } else {
                Ok(RedisCommand::Unknown)
            }
        }
        "SET" => {
            if parts.len() >= 3 {
                Ok(RedisCommand::Set {
                    key: parts[1].to_string(),
                    value: parts[2].as_bytes().to_vec(),
                })
            } else {
                Ok(RedisCommand::Unknown)
            }
        }
        "DEL" => {
            if parts.len() >= 2 {
                Ok(RedisCommand::Del {
                    key: parts[1].to_string(),
                })
            } else {
                Ok(RedisCommand::Unknown)
            }
        }
        "QUIT" => Ok(RedisCommand::Quit),
        _ => Ok(RedisCommand::Unknown),
    }
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
            // Return bulk string
            let data = &row.0;
            let response = format!("${}\r\n", data.len());
            stream.write_all(response.as_bytes()).await?;
            stream.write_all(data).await?;
            stream.write_all(CRLF).await?;
        }
        Ok(None) => {
            // Null bulk string for not found
            stream.write_all(b"$-1\r\n").await?;
        }
        Err(e) => {
            tracing::error!("Failed to GET key {}: {}", key, e);
            let error_msg = format!("-ERR database error\r\n");
            stream.write_all(error_msg.as_bytes()).await?;
        }
    }

    Ok(())
}

async fn handle_set(
    stream: &mut TcpStream,
    state: &Arc<AppState>,
    key: String,
    value: Vec<u8>,
) -> Result<(), Box<dyn std::error::Error>> {
    let shard_index = state.get_shard(&key);
    let sender = &state.shard_senders[shard_index];

    // Check if async_write is enabled
    if state.cfg.async_write.unwrap_or(false) {
        // Async mode: respond immediately after queueing
        let operation = ShardWriteOperation::SetAsync { key, data: value };

        if sender.send(operation).await.is_err() {
            tracing::error!(
                "Failed to send ASYNC SET operation to shard {}",
                shard_index
            );
            stream.write_all(b"-ERR internal error\r\n").await?;
        } else {
            stream.write_all(b"+OK\r\n").await?;
        }
    } else {
        // Sync mode: wait for completion
        let (responder_tx, responder_rx) = oneshot::channel();

        let operation = ShardWriteOperation::Set {
            key,
            data: value,
            responder: responder_tx,
        };

        if sender.send(operation).await.is_err() {
            tracing::error!("Failed to send SET operation to shard {}", shard_index);
            stream.write_all(b"-ERR internal error\r\n").await?;
        } else {
            match responder_rx.await {
                Ok(Ok(())) => {
                    stream.write_all(b"+OK\r\n").await?;
                }
                Ok(Err(e)) => {
                    tracing::error!("Shard writer failed for SET: {}", e);
                    stream.write_all(b"-ERR database error\r\n").await?;
                }
                Err(_) => {
                    tracing::error!("Shard writer task cancelled or panicked for SET");
                    stream.write_all(b"-ERR internal error\r\n").await?;
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
        stream.write_all(b":0\r\n").await?;
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
            stream.write_all(b"-ERR internal error\r\n").await?;
        } else {
            // Assume success for async mode
            stream.write_all(b":1\r\n").await?;
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
            stream.write_all(b"-ERR internal error\r\n").await?;
        } else {
            match responder_rx.await {
                Ok(Ok(())) => {
                    stream.write_all(b":1\r\n").await?;
                }
                Ok(Err(e)) => {
                    tracing::error!("Shard writer failed for DELETE: {}", e);
                    stream.write_all(b"-ERR database error\r\n").await?;
                }
                Err(_) => {
                    tracing::error!("Shard writer task cancelled or panicked for DELETE");
                    stream.write_all(b"-ERR internal error\r\n").await?;
                }
            }
        }
    }

    Ok(())
}
