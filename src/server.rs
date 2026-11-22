use crate::AppState;
use crate::cluster::ClusterManager;
use crate::config::Cfg;
use crate::metrics::Timer;
use crate::redis::{
    HExpireCondition, ParseError, RedisCommand, VacuumCommandMode, VacuumShardTarget,
    decode_request, parse_command, serialize_frame,
};
use crate::shard_manager::{
    self, Pending, ShardWriteOperation, ShardWriter, VacuumMode, VacuumResult, VacuumStats,
};
use bytes::{Buf, Bytes, BytesMut};
use futures::FutureExt;
use redis_protocol::resp2::types::BytesFrame;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

/// Runs blobasaur on `listener` until `shutdown` is cancelled, then shuts down
/// without losing acknowledged writes:
/// 1. stop accepting connections,
/// 2. stop reading new commands and let in-flight ones reply,
/// 3. drain every shard writer (commit all queued ops) and await it,
/// 4. stop background tasks and the cluster manager,
/// 5. checkpoint the WAL and close the pools.
///
/// Returns an error if shard writers could not drain within
/// `shutdown_timeout_secs`; acknowledged writes may have been lost then.
pub async fn run(
    cfg: Cfg,
    listener: TcpListener,
    shutdown: CancellationToken,
) -> miette::Result<()> {
    use miette::{Context, miette};

    // Initialize metrics if enabled
    let prometheus_handle = if cfg.metrics.as_ref().is_some_and(|m| m.enabled) {
        Some(crate::metrics::init_metrics_exporter().wrap_err("initializing metrics exporter")?)
    } else {
        None
    };

    // This vector will be populated by AppState::new
    let mut shard_receivers = Vec::with_capacity(cfg.num_shards);

    // Initialize AppState, AppState::new will populate shard_receivers
    let state = Arc::new(
        AppState::new(cfg.clone(), &mut shard_receivers)
            .await
            .wrap_err("initializing AppState")?,
    );

    // Initialize metrics
    state.metrics.record_server_startup();

    // Writers get their own token: they must keep running while in-flight
    // commands finish, and only drain once connections are done.
    let writers_shutdown = CancellationToken::new();
    let mut writers = Vec::with_capacity(cfg.num_shards);
    for (i, receiver) in shard_receivers.into_iter().enumerate() {
        writers.push(tokio::spawn(shard_manager::shard_writer_task(
            ShardWriter {
                shard_id: i,
                pool: state.write_db_pools[i].clone(),
                receiver,
                batch_size: cfg.batch_size.unwrap_or(1),
                batch_timeout_ms: cfg.batch_timeout_ms.unwrap_or(0),
                inflight_cache: state.inflight_cache.clone(),
                inflight_hcache: state.inflight_hcache.clone(),
                metrics: state.metrics.clone(),
                shutdown: writers_shutdown.clone(),
            },
        )));
    }

    let mut background = Vec::new();

    // Spawn cleanup tasks for each shard
    for (i, write_pool) in state.write_db_pools.iter().enumerate() {
        // Expiry cleanup deletes rows, so it shares the shard's single writer
        // connection: taking turns with the writer instead of holding the SQLite
        // write lock from another connection, which made writer batches fail
        // with SQLITE_BUSY.
        background.push(tokio::spawn(shard_manager::shard_cleanup_task(
            i,
            write_pool.clone(),
            cfg.cleanup_interval_secs(),
            cfg.cleanup_chunk_size(),
        )));
    }

    // Start HTTP metrics server if enabled
    if let Some(handle) = prometheus_handle {
        let metrics_addr = cfg
            .metrics
            .as_ref()
            .and_then(|m| m.addr.clone())
            .unwrap_or_else(|| "0.0.0.0:9090".to_string());

        let sampled = state.clone();
        let sample = move || sampled.record_gauges();
        background.push(tokio::spawn(async move {
            if let Err(e) =
                crate::http_server::run_metrics_server(handle, &metrics_addr, sample).await
            {
                tracing::error!("Failed to run metrics server: {}", e);
            }
        }));
    }

    let connections = TaskTracker::new();
    let server_result = run_redis_server(state.clone(), listener, &connections, &shutdown).await;
    if let Err(e) = &server_result {
        tracing::error!("Redis listener failed, shutting down: {}", e);
        shutdown.cancel();
    }

    let drain_timeout = cfg.shutdown_timeout();
    let deadline = tokio::time::Instant::now() + drain_timeout;
    tracing::info!(
        "Shutdown started: no longer accepting connections, draining {} active connections and {} shard writers (timeout {:?})",
        connections.len(),
        writers.len(),
        drain_timeout
    );

    connections.close();
    if tokio::time::timeout_at(deadline, connections.wait())
        .await
        .is_err()
    {
        tracing::warn!(
            "{} connections still running a command at the drain deadline; their writes will be rejected",
            connections.len()
        );
    }

    writers_shutdown.cancel();
    let mut drain_failed = false;
    for (shard_id, writer) in writers.iter_mut().enumerate() {
        match tokio::time::timeout_at(deadline, writer).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                tracing::error!(
                    "Shard {} writer task failed during shutdown: {}",
                    shard_id,
                    e
                );
                drain_failed = true;
            }
            Err(_) => {
                let sender = &state.shard_senders[shard_id];
                tracing::error!(
                    "SHUTDOWN TIMEOUT: shard {} writer did not drain within {:?}; {} queued ops (plus the batch being committed) were acknowledged but NOT persisted",
                    shard_id,
                    drain_timeout,
                    sender.queue_depth()
                );
                drain_failed = true;
            }
        }
    }

    for task in &background {
        task.abort();
    }
    if let Some(cluster_manager) = &state.cluster_manager {
        cluster_manager.shutdown().await;
    }

    if drain_failed {
        return Err(miette!(
            "shutdown drain incomplete: acknowledged writes may have been lost"
        ));
    }

    for (shard_id, pool) in state.write_db_pools.iter().enumerate() {
        if let Err(e) = shard_manager::wal_checkpoint_truncate(pool).await {
            tracing::warn!(
                "Shard {} WAL checkpoint on shutdown failed: {}",
                shard_id,
                e
            );
        }
    }
    for pool in state
        .db_pools
        .iter()
        .chain(&state.write_db_pools)
        .chain(&state.check_db_pools)
    {
        pool.close().await;
    }

    server_result.map_err(|e| miette!("Failed to run Redis server: {}", e))?;
    tracing::info!("Shutdown complete: all acknowledged writes persisted");
    Ok(())
}

/// Accepts connections until `shutdown` is cancelled. Connection tasks are
/// spawned on `connections` so the caller can wait for them to finish.
pub async fn run_redis_server(
    state: Arc<AppState>,
    listener: TcpListener,
    connections: &TaskTracker,
    shutdown: &CancellationToken,
) -> std::io::Result<()> {
    tracing::info!("Blobasaur server listening on {}", listener.local_addr()?);

    loop {
        let (stream, addr) = tokio::select! {
            accepted = listener.accept() => accepted?,
            _ = shutdown.cancelled() => return Ok(()),
        };
        tracing::debug!("New Blobasaur connection from {}", addr);

        let state_clone = state.clone();
        let shutdown = shutdown.clone();
        // Record new connection
        state.metrics.record_connection();

        connections.spawn(async move {
            if let Err(e) = handle_connection(stream, state_clone.clone(), shutdown).await {
                tracing::error!("Error handling connection from {}: {}", addr, e);
                state_clone.metrics.record_error("connection");
            }
            // Record dropped connection
            state_clone.metrics.record_connection_dropped();
        });
    }
}

/// Read size hint per socket read; the buffer still grows to fit large requests.
const READ_CHUNK: usize = 64 * 1024;

/// A read buffer that grew past this is replaced once everything in it has
/// been consumed; see [`release_large_buffer`].
const LARGE_READ_BUFFER: usize = 1024 * 1024;

/// A request split off the buffer still shares the buffer's allocation. Once
/// the buffer has grown large (`grown_to`, its biggest capacity since the last
/// reset) and nothing is left in it, start a fresh one. The last request's
/// value then owns that allocation alone, so the writer can take it without a
/// copy, and the allocation is freed with the request instead of being kept
/// for the connection's lifetime. `grown_to` is tracked separately because
/// `capacity()` shrinks as requests are split off the front.
fn release_large_buffer(buffer: &mut BytesMut, grown_to: &mut usize) {
    if buffer.is_empty() && *grown_to > LARGE_READ_BUFFER {
        *buffer = BytesMut::with_capacity(READ_CHUNK);
        *grown_to = READ_CHUNK;
    }
}

async fn handle_connection(
    mut stream: TcpStream,
    state: Arc<AppState>,
    shutdown: CancellationToken,
) -> Result<(), Box<dyn std::error::Error>> {
    let max_request_size = state.cfg.max_request_size();
    let mut buffer = BytesMut::with_capacity(READ_CHUNK);
    let mut grown_to = READ_CHUNK;

    loop {
        // Run every complete request already buffered before reading more.
        loop {
            // Shutting down: close instead of starting the next pipelined command.
            if shutdown.is_cancelled() {
                return Ok(());
            }
            let request = match decode_request(&mut buffer, max_request_size) {
                Ok(Some(request)) => request,
                Ok(None) => break,
                Err(e) => {
                    // Like Redis, reply once and close. Resyncing mid-stream could
                    // run bytes from inside a value as commands.
                    tracing::warn!("Protocol error, closing connection: {}", e);
                    state.metrics.record_error("protocol");
                    let error_resp = BytesFrame::Error(format!("ERR Protocol error: {}", e).into());
                    stream.write_all(&serialize_frame(&error_resp)).await?;
                    return Ok(());
                }
            };
            release_large_buffer(&mut buffer, &mut grown_to);

            match parse_command(request) {
                Ok(command) => {
                    let quit = matches!(command, RedisCommand::Quit);
                    if let Err(e) = handle_redis_command(&mut stream, &state, command).await {
                        tracing::error!("Error handling command: {}", e);
                        return Err(e);
                    }
                    if quit {
                        return Ok(());
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

        buffer.reserve(READ_CHUNK);
        // After growing, the data starts at the front, so this is the allocation.
        grown_to = grown_to.max(buffer.capacity());
        let read = tokio::select! {
            read = stream.read_buf(&mut buffer) => read,
            _ = shutdown.cancelled() => return Ok(()),
        };
        match read {
            Ok(0) => return Ok(()), // Connection closed
            Ok(n) => tracing::debug!(
                "Read {} bytes from socket, buffer size is now {}",
                n,
                buffer.len()
            ),
            Err(e) => {
                tracing::error!("Failed to read from socket: {}", e);
                return Err(Box::new(e));
            }
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

async fn check_key_route(state: &AppState, key: &str) -> Result<(), BytesFrame> {
    let Some(cluster_manager) = state.cluster_manager.as_ref() else {
        return Ok(());
    };
    match cluster_manager.route_key(key).await.into_error() {
        Some(err) => Err(BytesFrame::Error(err.into())),
        None => Ok(()),
    }
}

async fn check_hash_route(state: &AppState, namespace: &str, key: &str) -> Result<(), BytesFrame> {
    let Some(cluster_manager) = state.cluster_manager.as_ref() else {
        return Ok(());
    };
    match cluster_manager
        .route_hash(namespace, key)
        .await
        .into_error()
    {
        Some(err) => Err(BytesFrame::Error(err.into())),
        None => Ok(()),
    }
}

async fn handle_redis_command_inner(
    stream: &mut TcpStream,
    state: &Arc<AppState>,
    command: RedisCommand,
) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        RedisCommand::Get { key } => {
            if let Err(response) = check_key_route(state, &key).await {
                stream.write_all(&serialize_frame(&response)).await?;
                return Ok(());
            }
            handle_get(stream, state, key).await?;
        }
        RedisCommand::Set {
            key,
            value,
            ttl_seconds,
        } => {
            if let Err(response) = check_key_route(state, &key).await {
                stream.write_all(&serialize_frame(&response)).await?;
                return Ok(());
            }
            handle_set(stream, state, key, value, ttl_seconds).await?;
        }
        RedisCommand::Del { keys } => {
            handle_del_multiple(stream, state, keys).await?;
        }
        RedisCommand::Exists { key } => {
            if let Err(response) = check_key_route(state, &key).await {
                stream.write_all(&serialize_frame(&response)).await?;
                return Ok(());
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
            if let Err(response) = check_key_route(state, &key).await {
                stream.write_all(&serialize_frame(&response)).await?;
                return Ok(());
            }
            handle_ttl(stream, state, key).await?;
        }
        RedisCommand::Expire { key, seconds } => {
            if let Err(response) = check_key_route(state, &key).await {
                stream.write_all(&serialize_frame(&response)).await?;
                return Ok(());
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
        RedisCommand::Hello { protover } => {
            handle_hello(stream, protover).await?;
        }
        RedisCommand::Client { subcommand } => {
            handle_client(stream, &subcommand).await?;
        }
        RedisCommand::Quit => {
            // handle_connection closes the connection after this reply.
            let response = BytesFrame::SimpleString("OK".into());
            stream.write_all(&serialize_frame(&response)).await?;
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
    if state.cfg.is_compression()
        && let Some(compressor) = &state.compressor
    {
        return Ok(compressor.compress(&data).await?.into());
    }
    Ok(data)
}

async fn decompress_if_enabled(
    state: &Arc<AppState>,
    data: Bytes,
) -> Result<Bytes, Box<dyn std::error::Error>> {
    if state.cfg.is_compression()
        && let Some(compressor) = &state.compressor
    {
        return Ok(compressor.decompress(&data).await?.into());
    }
    Ok(data)
}

/// Writes `data` as a bulk string reply. Unlike `serialize_frame`, this doesn't
/// copy the value into a frame buffer first.
async fn write_bulk(stream: &mut TcpStream, data: &[u8]) -> std::io::Result<()> {
    let header = format!("${}\r\n", data.len());
    let mut reply = Buf::chain(header.as_bytes(), data).chain(&b"\r\n"[..]);
    stream.write_all_buf(&mut reply).await
}

async fn handle_get(
    stream: &mut TcpStream,
    state: &Arc<AppState>,
    key: String,
) -> Result<(), Box<dyn std::error::Error>> {
    // First check inflight cache for pending writes
    match state.inflight_cache.get(&key).await {
        Some(Pending::Value { data, .. }) => {
            state.metrics.record_read("get", "inflight", "hit");
            // Decompress if needed
            let data = decompress_if_enabled(state, data).await?;

            state.metrics.value_size_get_bytes.record(data.len() as f64);
            write_bulk(stream, &data).await?;
            return Ok(());
        }
        Some(Pending::Deleted(_)) => {
            state.metrics.record_read("get", "inflight", "miss");
            stream
                .write_all(&serialize_frame(&BytesFrame::Null))
                .await?;
            return Ok(());
        }
        None => {}
    }

    let shard_index = state.get_shard(&key);
    let pool = &state.db_pools[shard_index];

    let query = sqlx::query_as::<_, (Vec<u8>,)>(
        "SELECT data FROM blobs WHERE key = ? AND (expires_at IS NULL OR expires_at > ?)",
    )
    .bind(&key)
    .bind(chrono::Utc::now().timestamp());
    let shard_metrics = &state.shard_senders[shard_index].metrics;
    match async {
        query
            .fetch_optional(&mut *shard_metrics.acquire(pool, "read").await?)
            .await
    }
    .await
    {
        Ok(Some(row)) => {
            state.metrics.record_read("get", "sqlite", "hit");
            // Decompress if needed
            let data = decompress_if_enabled(state, row.0.into()).await?;

            state.metrics.value_size_get_bytes.record(data.len() as f64);
            write_bulk(stream, &data).await?;
        }
        Ok(None) => {
            state.metrics.record_read("get", "sqlite", "miss");
            let response = BytesFrame::Null;
            stream.write_all(&serialize_frame(&response)).await?;
        }
        Err(e) => {
            state.metrics.record_read("get", "sqlite", "error");
            tracing::error!("Failed to GET key {}: {}", key, e);
            let response = BytesFrame::Error("ERR database error".into());
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

    let value_len = value.len() as f64;
    let value = compress_if_enabled(state, value).await?;
    let stored_len = value.len() as f64;

    // Check if async_write is enabled
    if state.cfg.async_write.unwrap_or(false) {
        // Calculate expires_at timestamp if TTL is provided
        let expires_at = ttl_seconds.map(|ttl| chrono::Utc::now().timestamp() + ttl as i64);

        // Async mode: respond immediately after queueing. The inflight cache
        // serves the value until it commits.
        let token = shard_manager::next_pending_token();
        let operation = ShardWriteOperation::SetAsync {
            key: key.clone(),
            data: value.clone(),
            expires_at,
            token,
        };
        let pending = Pending::Value { data: value, token };

        if shard_manager::queue_pending(&state.inflight_cache, sender, key, pending, operation)
            .await
            .is_err()
        {
            tracing::error!(
                "Failed to send ASYNC SET operation to shard {}",
                shard_index
            );
            let response = BytesFrame::Error("ERR internal error".into());
            stream.write_all(&serialize_frame(&response)).await?;
            state.metrics.record_error("storage");
        } else {
            state.metrics.value_size_set_bytes.record(value_len);
            state.metrics.stored_value_size_set_bytes.record(stored_len);
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
            let response = BytesFrame::Error("ERR internal error".into());
            stream.write_all(&serialize_frame(&response)).await?;
            state.metrics.record_error("storage");
        } else {
            match responder_rx.await {
                Ok(Ok(())) => {
                    state.metrics.value_size_set_bytes.record(value_len);
                    state.metrics.stored_value_size_set_bytes.record(stored_len);
                    let response = BytesFrame::SimpleString("OK".into());
                    stream.write_all(&serialize_frame(&response)).await?;
                    state.metrics.record_storage_operation();
                }
                Ok(Err(e)) => {
                    tracing::error!("Shard writer failed for SET: {}", e);
                    let response = BytesFrame::Error("ERR database error".into());
                    stream.write_all(&serialize_frame(&response)).await?;
                    state.metrics.record_error("storage");
                }
                Err(_) => {
                    tracing::error!("Shard writer task cancelled or panicked for SET");
                    let response = BytesFrame::Error("ERR internal error".into());
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
    let response = del_reply(state, keys).await;
    stream.write_all(&serialize_frame(&response)).await?;
    Ok(())
}

/// DEL's reply. Like every command that takes key locks, it releases them
/// before the reply is written, so a client slow to read can't hold them.
async fn del_reply(state: &Arc<AppState>, mut keys: Vec<String>) -> BytesFrame {
    // Route every key before deleting any, so a redirect or unserved slot leaves them all.
    for key in &keys {
        if let Err(response) = check_key_route(state, key).await {
            return response;
        }
    }

    // A key named twice is deleted and counted once, as in Redis.
    keys.sort_unstable();
    keys.dedup();
    // Held until every delete is queued or committed, so concurrent DELs
    // can't all see a key and all count it as deleted.
    let key_refs: Vec<&str> = keys.iter().map(String::as_str).collect();
    let _guards = state.key_locks.lock_keys(&key_refs).await;

    let mut deleted_count = 0;
    for key in keys {
        let shard_index = state.get_shard(&key);
        let pool = &state.check_db_pools[shard_index];

        // A queued async SET is in the inflight cache but not the DB yet. It
        // counts as existing: skipping the delete would let that SET commit
        // afterwards and bring the key back. A queued DEL means it's gone.
        // Otherwise check the DB. A failed check (e.g. the shard file is
        // locked or unreadable) must not be reported as "key not found".
        let exists = match shard_manager::pending_exists(&state.inflight_cache, &key).await {
            Some(exists) => exists,
            None => match sqlx::query(
                "SELECT 1 FROM blobs WHERE key = ? AND (expires_at IS NULL OR expires_at > ?)",
            )
            .bind(&key)
            .bind(chrono::Utc::now().timestamp())
            .fetch_optional(pool)
            .await
            {
                Ok(row) => row.is_some(),
                Err(e) => {
                    tracing::error!("Failed to check DEL existence for key {}: {}", key, e);
                    return BytesFrame::Error("ERR database error".into());
                }
            },
        };

        if !exists {
            continue; // Key doesn't exist, skip
        }

        let sender = &state.shard_senders[shard_index];

        if state.cfg.async_write.unwrap_or(false) {
            // Async mode: respond immediately after queueing. Reads see the
            // key as gone until the delete commits.
            let token = shard_manager::next_pending_token();
            let operation = ShardWriteOperation::DeleteAsync {
                key: key.clone(),
                token,
            };
            let pending = Pending::Deleted(token);

            if shard_manager::queue_pending(&state.inflight_cache, sender, key, pending, operation)
                .await
                .is_err()
            {
                tracing::error!(
                    "Failed to send ASYNC DELETE operation to shard {}",
                    shard_index
                );
                return BytesFrame::Error("ERR internal error".into());
            }
            deleted_count += 1;
        } else {
            // Sync mode: wait for completion
            let (responder_tx, responder_rx) = oneshot::channel();
            let operation = ShardWriteOperation::Delete {
                key,
                responder: responder_tx,
            };

            if sender.send(operation).await.is_err() {
                tracing::error!("Failed to send DELETE operation to shard {}", shard_index);
                return BytesFrame::Error("ERR internal error".into());
            }
            match responder_rx.await {
                Ok(Ok(())) => deleted_count += 1,
                Ok(Err(e)) => {
                    tracing::error!("Shard writer failed for DELETE: {}", e);
                    return BytesFrame::Error("ERR database error".into());
                }
                Err(_) => {
                    tracing::error!("Shard writer task cancelled or panicked for DELETE");
                    return BytesFrame::Error("ERR internal error".into());
                }
            }
        }
    }

    BytesFrame::Integer(deleted_count)
}

async fn handle_exists(
    stream: &mut TcpStream,
    state: &Arc<AppState>,
    key: String,
) -> Result<(), Box<dyn std::error::Error>> {
    // A queued async SET or DEL decides it, as it does for GET.
    if let Some(exists) = shard_manager::pending_exists(&state.inflight_cache, &key).await {
        stream
            .write_all(&serialize_frame(&BytesFrame::Integer(exists as i64)))
            .await?;
        return Ok(());
    }

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
                 batch_timeout_ms:{}\r\n\
                 write_queue_capacity:{}\r\n",
                std::process::id(),
                state.cfg.num_shards,
                state.cfg.data_dir,
                state.cfg.async_write.unwrap_or(false),
                state.cfg.batch_size.unwrap_or(1),
                state.cfg.batch_timeout_ms.unwrap_or(0),
                state.cfg.write_queue_capacity()
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

async fn handle_hello(
    stream: &mut TcpStream,
    protover: Option<u64>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Only RESP2 is supported. If the client requests a different protocol
    // version, return NOPROTO so it can fall back to RESP2.
    if let Some(ver) = protover
        && ver != 2
    {
        let response = BytesFrame::Error(
            format!(
                "NOPROTO unsupported protocol version {}, only RESP2 is supported",
                ver
            )
            .into(),
        );
        stream.write_all(&serialize_frame(&response)).await?;
        return Ok(());
    }

    // Return server info as a RESP2 flat array of key-value pairs
    let response = BytesFrame::Array(vec![
        BytesFrame::BulkString("server".into()),
        BytesFrame::BulkString("blobasaur".into()),
        BytesFrame::BulkString("version".into()),
        BytesFrame::BulkString(env!("CARGO_PKG_VERSION").into()),
        BytesFrame::BulkString("proto".into()),
        BytesFrame::Integer(2),
        BytesFrame::BulkString("mode".into()),
        BytesFrame::BulkString("standalone".into()),
        BytesFrame::BulkString("role".into()),
        BytesFrame::BulkString("master".into()),
    ]);
    stream.write_all(&serialize_frame(&response)).await?;
    Ok(())
}

async fn handle_client(
    stream: &mut TcpStream,
    _subcommand: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // Handle CLIENT subcommands (SETNAME, GETNAME, etc.)
    // Redis clients send CLIENT commands on connect for connection management
    let response = BytesFrame::SimpleString("OK".into());
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

/// Whether a live hash field exists, for the commands that check before
/// writing. A queued async write or delete of the field decides it; otherwise
/// the DB does. The inflight cache must be checked first: an entry is cleared
/// only once its op has committed, so a miss means the DB already has it. A DB
/// check made first could run before a queued op commits, and the cache check
/// after it clears.
async fn hash_field_live(
    state: &AppState,
    pool: &sqlx::SqlitePool,
    namespace: &str,
    key: &str,
) -> Result<bool, sqlx::Error> {
    let namespaced_key = state.namespaced_key(namespace, key);
    if let Some(exists) =
        shard_manager::pending_exists(&state.inflight_hcache, &namespaced_key).await
    {
        return Ok(exists);
    }

    let table_name = shard_manager::namespace_table(namespace);
    let query = format!(
        "SELECT 1 FROM {} WHERE key = ? AND (expires_at IS NULL OR expires_at > ?)",
        table_name
    );
    match sqlx::query(&query)
        .bind(key)
        .bind(chrono::Utc::now().timestamp())
        .fetch_optional(pool)
        .await
    {
        Ok(row) => Ok(row.is_some()),
        Err(e) if shard_manager::is_missing_table(&e, &table_name) => Ok(false),
        Err(e) => Err(e),
    }
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
    if let Err(response) = check_hash_route(state, &namespace, &key).await {
        stream.write_all(&serialize_frame(&response)).await?;
        return Ok(());
    }

    // First check inflight cache for pending writes
    let namespaced_key = state.namespaced_key(&namespace, &key);
    match state.inflight_hcache.get(&namespaced_key).await {
        Some(Pending::Value { data, .. }) => {
            state.metrics.record_read("hget", "inflight", "hit");
            // Decompress if needed
            let data = decompress_if_enabled(state, data).await?;

            state
                .metrics
                .value_size_hget_bytes
                .record(data.len() as f64);
            write_bulk(stream, &data).await?;
            return Ok(());
        }
        Some(Pending::Deleted(_)) => {
            state.metrics.record_read("hget", "inflight", "miss");
            stream
                .write_all(&serialize_frame(&BytesFrame::Null))
                .await?;
            return Ok(());
        }
        None => {}
    }

    let shard_index = state.get_shard(&key);
    let pool = &state.db_pools[shard_index];
    let table_name = shard_manager::namespace_table(&namespace);

    let query = format!(
        "SELECT data FROM {} WHERE key = ? AND (expires_at IS NULL OR expires_at > ?)",
        table_name
    );

    let query = sqlx::query_as::<_, (Vec<u8>,)>(&query)
        .bind(&key)
        .bind(chrono::Utc::now().timestamp());
    let shard_metrics = &state.shard_senders[shard_index].metrics;
    match async {
        query
            .fetch_optional(&mut *shard_metrics.acquire(pool, "read").await?)
            .await
    }
    .await
    {
        Ok(Some(row)) => {
            state.metrics.record_read("hget", "sqlite", "hit");
            // Decompress if needed
            let data = decompress_if_enabled(state, row.0.into()).await?;

            state
                .metrics
                .value_size_hget_bytes
                .record(data.len() as f64);
            write_bulk(stream, &data).await?;
        }
        Ok(None) => {
            state.metrics.record_read("hget", "sqlite", "miss");
            let response = BytesFrame::Null;
            stream.write_all(&serialize_frame(&response)).await?;
        }
        Err(e) if shard_manager::is_missing_table(&e, &table_name) => {
            state.metrics.record_read("hget", "sqlite", "miss");
            stream
                .write_all(&serialize_frame(&BytesFrame::Null))
                .await?;
        }
        Err(e) => {
            state.metrics.record_read("hget", "sqlite", "error");
            tracing::error!("Failed to HGET namespace {} key {}: {}", namespace, key, e);
            let response = BytesFrame::Error("ERR database error".into());
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
    let response = hset_reply(state, namespace, key, value).await?;
    stream.write_all(&serialize_frame(&response)).await?;
    Ok(())
}

/// HSET's reply; it releases its key lock before the reply is written, as DEL.
async fn hset_reply(
    state: &Arc<AppState>,
    namespace: String,
    key: String,
    value: Bytes,
) -> Result<BytesFrame, Box<dyn std::error::Error>> {
    // Check if we should handle this hash operation locally in a cluster
    if let Err(response) = check_hash_route(state, &namespace, &key).await {
        return Ok(response);
    }

    let value_len = value.len() as f64;

    let shard_index = state.get_shard(&key);
    let pool = &state.check_db_pools[shard_index];

    // Held until the write is queued or committed, so concurrent HSETs of a
    // new field can't all report it as added.
    let _guard = state
        .key_locks
        .lock_fields(&namespace, &[key.as_str()])
        .await;

    let existed_before = match hash_field_live(state, pool, &namespace, &key).await {
        Ok(exists) => exists,
        Err(e) => {
            tracing::error!(
                "Failed to check HSET existence for namespace {} key {}: {}",
                namespace,
                key,
                e
            );
            return Ok(BytesFrame::Error("ERR database error".into()));
        }
    };
    let added = BytesFrame::Integer(if existed_before { 0 } else { 1 });

    let sender = &state.shard_senders[shard_index];

    // Compress data if storage compression is enabled
    let value = compress_if_enabled(state, value).await?;
    let stored_len = value.len() as f64;

    if state.cfg.async_write.unwrap_or(false) {
        // Async mode: respond immediately after queueing. The inflight cache
        // serves the value until it commits.
        let namespaced_key = state.namespaced_key(&namespace, &key);
        let token = shard_manager::next_pending_token();
        let operation = ShardWriteOperation::HSetAsync {
            namespace,
            key,
            data: value.clone(),
            token,
        };
        let pending = Pending::Value { data: value, token };

        if shard_manager::queue_pending(
            &state.inflight_hcache,
            sender,
            namespaced_key,
            pending,
            operation,
        )
        .await
        .is_err()
        {
            tracing::error!(
                "Failed to send ASYNC HSET operation to shard {}",
                shard_index
            );
            return Ok(BytesFrame::Error("ERR internal error".into()));
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
            return Ok(BytesFrame::Error("ERR internal error".into()));
        }
        match responder_rx.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                tracing::error!("Shard writer failed for HSET: {}", e);
                return Ok(BytesFrame::Error("ERR database error".into()));
            }
            Err(_) => {
                tracing::error!("Shard writer task cancelled or panicked for HSET");
                return Ok(BytesFrame::Error("ERR internal error".into()));
            }
        }
    }

    state.metrics.value_size_hset_bytes.record(value_len);
    state
        .metrics
        .stored_value_size_hset_bytes
        .record(stored_len);
    Ok(added)
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
    // In Blobasaur, the hash key is the namespace, and fields are the actual keys
    let namespace = key;

    if !validate_hash_namespace(stream, &namespace).await? {
        return Ok(());
    }

    let response = hsetex_reply(state, namespace, fnx, fxx, expire_option, fields).await?;
    stream.write_all(&serialize_frame(&response)).await?;
    Ok(())
}

/// HSETEX's reply; it releases its key locks before the reply is written, as
/// DEL.
async fn hsetex_reply(
    state: &Arc<AppState>,
    namespace: String,
    fnx: bool,
    fxx: bool,
    expire_option: Option<crate::redis::protocol::ExpireOption>,
    fields: Vec<(String, Bytes)>,
) -> Result<BytesFrame, Box<dyn std::error::Error>> {
    use crate::redis::protocol::ExpireOption;

    // Route every field before locking or checking FNX/FXX, so a redirect or
    // unserved slot leaves the hash untouched and isn't masked by another field.
    for (field_key, _) in &fields {
        if let Err(response) = check_hash_route(state, &namespace, field_key).await {
            return Ok(response);
        }
    }

    // Held until every write is queued or committed, so the FNX/FXX condition
    // and the new-field count still hold when the writes land.
    let field_refs: Vec<&str> = fields
        .iter()
        .map(|(field_key, _)| field_key.as_str())
        .collect();
    let _guards = state.key_locks.lock_fields(&namespace, &field_refs).await;

    // Resolved only now, so time spent waiting for the locks doesn't shorten
    // a relative TTL.
    let expires_at = match expire_option {
        Some(ExpireOption::Ex(seconds)) => Some(chrono::Utc::now().timestamp() + seconds as i64),
        // Round up like SET PX (the parser has range-checked it).
        Some(ExpireOption::Px(millis)) => {
            Some(chrono::Utc::now().timestamp() + millis.div_ceil(1000) as i64)
        }
        Some(ExpireOption::ExAt(timestamp)) => Some(timestamp),
        Some(ExpireOption::PxAt(timestamp)) => Some(timestamp / 1000),
        Some(ExpireOption::KeepTtl) => {
            // For KEEPTTL, we'd need to fetch existing TTL - for now, skip TTL setting
            None
        }
        None => None,
    };

    // Check every field before writing any, so a failed FNX/FXX condition
    // leaves the hash untouched.
    let mut checked = Vec::with_capacity(fields.len());
    let mut seen = HashSet::with_capacity(fields.len());
    for (field_key, field_value) in fields {
        let shard_index = state.get_shard(&field_key);
        let pool = &state.check_db_pools[shard_index];

        let exists = match hash_field_live(state, pool, &namespace, &field_key).await {
            Ok(exists) => exists,
            Err(e) => {
                tracing::error!(
                    "Failed to check field existence for namespace {} key {}: {}",
                    namespace,
                    field_key,
                    e
                );
                return Ok(BytesFrame::Error("ERR database error".into()));
            }
        };

        // FNX sets only if no field exists, FXX only if all of them do.
        if (fnx && exists) || (fxx && !exists) {
            return Ok(BytesFrame::Integer(0));
        }

        // A field repeated in one command is created once; the last value wins.
        let is_new_field = !exists && seen.insert(field_key.clone());
        checked.push((field_key, field_value, shard_index, is_new_field));
    }

    let mut new_fields_added = 0;
    for (field_key, field_value, shard_index, is_new_field) in checked {
        let value_len = field_value.len() as f64;
        let sender = &state.shard_senders[shard_index];

        // Compress data if storage compression is enabled
        let compressed_value = compress_if_enabled(state, field_value).await?;
        let stored_len = compressed_value.len() as f64;

        if state.cfg.async_write.unwrap_or(false) {
            // The inflight cache serves the value until it commits.
            let namespaced_key = state.namespaced_key(&namespace, &field_key);
            let token = shard_manager::next_pending_token();
            let pending = Pending::Value {
                data: compressed_value.clone(),
                token,
            };
            let operation = if let Some(exp) = expires_at {
                ShardWriteOperation::HSetExAsync {
                    namespace: namespace.clone(),
                    key: field_key,
                    data: compressed_value,
                    expires_at: exp,
                    token,
                }
            } else {
                ShardWriteOperation::HSetAsync {
                    namespace: namespace.clone(),
                    key: field_key,
                    data: compressed_value,
                    token,
                }
            };

            if shard_manager::queue_pending(
                &state.inflight_hcache,
                sender,
                namespaced_key,
                pending,
                operation,
            )
            .await
            .is_err()
            {
                tracing::error!(
                    "Failed to send ASYNC HSETEX operation to shard {}",
                    shard_index
                );
                return Ok(BytesFrame::Error("ERR internal error".into()));
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
                return Ok(BytesFrame::Error("ERR internal error".into()));
            }

            match responder_rx.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    tracing::error!("Shard writer failed for HSETEX: {}", e);
                    return Ok(BytesFrame::Error("ERR database error".into()));
                }
                Err(_) => {
                    tracing::error!("Shard writer task cancelled or panicked for HSETEX");
                    return Ok(BytesFrame::Error("ERR internal error".into()));
                }
            }
        }

        state.metrics.value_size_hset_bytes.record(value_len);
        state
            .metrics
            .stored_value_size_hset_bytes
            .record(stored_len);
        if is_new_field {
            new_fields_added += 1;
        }
    }

    Ok(BytesFrame::Integer(new_fields_added))
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
    let response = hdel_reply(state, namespace, key).await;
    stream.write_all(&serialize_frame(&response)).await?;
    Ok(())
}

/// HDEL's reply; it releases its key lock before the reply is written, as DEL.
async fn hdel_reply(state: &Arc<AppState>, namespace: String, key: String) -> BytesFrame {
    // Check if we should handle this hash operation locally in a cluster
    if let Err(response) = check_hash_route(state, &namespace, &key).await {
        return response;
    }

    let shard_index = state.get_shard(&key);
    let pool = &state.check_db_pools[shard_index];

    // Held until the delete is queued or committed, as in DEL.
    let _guard = state
        .key_locks
        .lock_fields(&namespace, &[key.as_str()])
        .await;

    // A queued async HSET counts as existing: skipping the delete would let
    // that HSET commit afterwards and bring the field back. A failed check
    // must surface instead of being reported as "not found".
    let exists = match hash_field_live(state, pool, &namespace, &key).await {
        Ok(exists) => exists,
        Err(e) => {
            tracing::error!(
                "Failed to check HDEL existence for namespace {} key {}: {}",
                namespace,
                key,
                e
            );
            return BytesFrame::Error("ERR database error".into());
        }
    };

    if !exists {
        // Redis HDEL returns the number of keys deleted
        return BytesFrame::Integer(0);
    }

    let sender = &state.shard_senders[shard_index];

    if state.cfg.async_write.unwrap_or(false) {
        // Async mode: respond immediately after queueing. Reads see the field
        // as gone until the delete commits.
        let namespaced_key = state.namespaced_key(&namespace, &key);
        let token = shard_manager::next_pending_token();
        let operation = ShardWriteOperation::HDeleteAsync {
            namespace,
            key,
            token,
        };
        let pending = Pending::Deleted(token);

        if shard_manager::queue_pending(
            &state.inflight_hcache,
            sender,
            namespaced_key,
            pending,
            operation,
        )
        .await
        .is_err()
        {
            tracing::error!(
                "Failed to send ASYNC HDEL operation to shard {}",
                shard_index
            );
            return BytesFrame::Error("ERR internal error".into());
        }
        BytesFrame::Integer(1)
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
            return BytesFrame::Error("ERR internal error".into());
        }
        match responder_rx.await {
            Ok(Ok(())) => BytesFrame::Integer(1),
            Ok(Err(e)) => {
                tracing::error!("Shard writer failed for HDEL: {}", e);
                BytesFrame::Error("ERR database error".into())
            }
            Err(_) => {
                tracing::error!("Shard writer task cancelled or panicked for HDEL");
                BytesFrame::Error("ERR internal error".into())
            }
        }
    }
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
    if let Err(response) = check_hash_route(state, &namespace, &key).await {
        stream.write_all(&serialize_frame(&response)).await?;
        return Ok(());
    }

    // A queued async HSET or HDEL decides it, as it does for HGET.
    if let Some(exists) = shard_manager::pending_exists(
        &state.inflight_hcache,
        &state.namespaced_key(&namespace, &key),
    )
    .await
    {
        stream
            .write_all(&serialize_frame(&BytesFrame::Integer(exists as i64)))
            .await?;
        return Ok(());
    }

    let shard_index = state.get_shard(&key);
    let pool = &state.db_pools[shard_index];
    let table_name = shard_manager::namespace_table(&namespace);

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
        Err(e) if shard_manager::is_missing_table(&e, &table_name) => {
            stream
                .write_all(&serialize_frame(&BytesFrame::Integer(0)))
                .await?;
        }
        Err(e) => {
            tracing::error!(
                "Failed to check HEXISTS for namespace {} key {}: {}",
                namespace,
                key,
                e
            );
            let response = BytesFrame::Error("ERR database error".into());
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
    let deadline = Deadline::In(seconds);
    handle_hexpire_internal(stream, state, key, deadline, condition, fields).await
}

async fn handle_hexpireat(
    stream: &mut TcpStream,
    state: &Arc<AppState>,
    key: String,
    unix_time_seconds: i64,
    condition: Option<HExpireCondition>,
    fields: Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Deadline::At(unix_time_seconds);
    handle_hexpire_internal(stream, state, key, deadline, condition, fields).await
}

/// When an expiry set by a command falls: `In` seconds from when the command
/// gets its key locks, or `At` a Unix time. A relative one is resolved only
/// once the locks are held, so time spent waiting for them doesn't shorten it.
#[derive(Clone, Copy)]
enum Deadline {
    In(i64),
    At(i64),
}

impl Deadline {
    fn resolve(self) -> i64 {
        match self {
            Deadline::In(seconds) => chrono::Utc::now().timestamp() + seconds,
            Deadline::At(timestamp) => timestamp,
        }
    }
}

/// Shared implementation for HEXPIRE and HEXPIREAT.
async fn handle_hexpire_internal(
    stream: &mut TcpStream,
    state: &Arc<AppState>,
    key: String,
    deadline: Deadline,
    condition: Option<HExpireCondition>,
    fields: Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    // In Blobasaur, the hash key is the namespace, and fields are the actual keys
    let namespace = key;

    if !validate_hash_namespace(stream, &namespace).await? {
        return Ok(());
    }

    let response = hexpire_reply(state, namespace, deadline, condition, fields).await;
    if matches!(response, BytesFrame::Array(_)) {
        state.metrics.record_storage_operation();
    }
    stream.write_all(&serialize_frame(&response)).await?;
    Ok(())
}

/// HEXPIRE's reply; it releases its key locks before the reply is written, as
/// DEL.
async fn hexpire_reply(
    state: &Arc<AppState>,
    namespace: String,
    deadline: Deadline,
    condition: Option<HExpireCondition>,
    fields: Vec<String>,
) -> BytesFrame {
    // Route every field before expiring any, so a redirect or unserved slot leaves them all.
    for field_key in &fields {
        if let Err(response) = check_hash_route(state, &namespace, field_key).await {
            return response;
        }
    }

    // An expiry in the past removes a field, so this orders against the
    // commands that check one, as in HDEL.
    let field_refs: Vec<&str> = fields.iter().map(String::as_str).collect();
    let _guards = state.key_locks.lock_fields(&namespace, &field_refs).await;
    let expires_at = deadline.resolve();

    let mut field_results: Vec<i64> = Vec::with_capacity(fields.len());
    for field_key in fields {
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
            state.metrics.record_error("storage");
            return BytesFrame::Error("ERR internal error".into());
        }

        match responder_rx.await {
            Ok(Ok(result_code)) => field_results.push(result_code),
            Ok(Err(e)) => {
                tracing::error!(
                    "HEXPIRE operation failed for namespace {} key {}: {}",
                    namespace,
                    field_key,
                    e
                );
                state.metrics.record_error("storage");
                return BytesFrame::Error("ERR internal error".into());
            }
            Err(_) => {
                tracing::error!("Shard writer task cancelled or panicked for HEXPIRE");
                state.metrics.record_error("storage");
                return BytesFrame::Error("ERR internal error".into());
            }
        }
    }

    // Return array of integers, one per field
    BytesFrame::Array(field_results.into_iter().map(BytesFrame::Integer).collect())
}

// Cluster command handlers
async fn handle_cluster_nodes(
    stream: &mut TcpStream,
    state: &Arc<AppState>,
) -> Result<(), Box<dyn std::error::Error>> {
    tracing::debug!("Handling CLUSTER NODES command");
    if let Some(ref cluster_manager) = state.cluster_manager {
        let nodes_info = cluster_manager.get_cluster_nodes().await;
        let response = BytesFrame::BulkString(nodes_info.into());
        stream.write_all(&serialize_frame(&response)).await?;
    } else {
        let response = BytesFrame::Error("ERR This instance has cluster support disabled".into());
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
        let response = BytesFrame::Error("ERR This instance has cluster support disabled".into());
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
                    let ip = parts.first().cloned().unwrap_or("127.0.0.1").to_string();
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
            let ip = parts.first().cloned().unwrap_or("127.0.0.1").to_string();
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
        let response = BytesFrame::Error("ERR This instance has cluster support disabled".into());
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
                let response = BytesFrame::Error("ERR failed to add slots".into());
                stream.write_all(&serialize_frame(&response)).await?;
            }
        }
    } else {
        let response = BytesFrame::Error("ERR This instance has cluster support disabled".into());
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
                let response = BytesFrame::Error("ERR failed to remove slots".into());
                stream.write_all(&serialize_frame(&response)).await?;
            }
        }
    } else {
        let response = BytesFrame::Error("ERR This instance has cluster support disabled".into());
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
    // A queued DEL means the key is gone. (A queued SET's expiry isn't in the
    // cache, so TTL still reads the DB for those.)
    if shard_manager::pending_exists(&state.inflight_cache, &key).await == Some(false) {
        stream
            .write_all(&serialize_frame(&BytesFrame::Integer(-2)))
            .await?;
        return Ok(());
    }

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
    let response = expire_reply(state, key, seconds).await;
    stream.write_all(&serialize_frame(&response)).await?;
    Ok(())
}

/// EXPIRE's reply; it releases its key lock before the reply is written, as
/// DEL.
async fn expire_reply(state: &Arc<AppState>, key: String, seconds: u64) -> BytesFrame {
    let shard_index = state.get_shard(&key);
    let sender = &state.shard_senders[shard_index];

    // EXPIRE 0 removes the key, so this orders against DEL's check.
    let _guard = state.key_locks.lock_keys(&[key.as_str()]).await;
    // Resolved only now, so time spent waiting for the lock doesn't shorten
    // the TTL.
    let expires_at = chrono::Utc::now().timestamp() + seconds as i64;

    let (responder_tx, responder_rx) = oneshot::channel();

    let operation = ShardWriteOperation::Expire {
        key,
        expires_at,
        responder: responder_tx,
    };

    if sender.send(operation).await.is_err() {
        tracing::error!("Failed to send EXPIRE operation to shard {}", shard_index);
        state.metrics.record_error("storage");
        return BytesFrame::Error("ERR internal error".into());
    }
    match responder_rx.await {
        // 1 if the key was found and its expiry set, 0 if it wasn't found.
        Ok(Ok(found)) => {
            state.metrics.record_storage_operation();
            BytesFrame::Integer(found as i64)
        }
        Ok(Err(e)) => {
            tracing::error!("EXPIRE operation failed: {}", e);
            state.metrics.record_error("storage");
            BytesFrame::Error("ERR internal error".into())
        }
        Err(_) => {
            tracing::error!("Shard writer task cancelled or panicked for EXPIRE");
            state.metrics.record_error("storage");
            BytesFrame::Error("ERR internal error".into())
        }
    }
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

/// Non-blocking check for a closed peer. Pipelined bytes count as still connected.
fn client_disconnected(stream: &TcpStream) -> bool {
    let mut buf = [0_u8; 1];
    match stream.peek(&mut buf).now_or_never() {
        Some(Ok(0)) | Some(Err(_)) => true,
        Some(Ok(_)) | None => false,
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
        // Nobody is waiting for the result (e.g. the CLI hit --timeout-sec), so don't
        // start more shards; otherwise a retried run queues a second pass behind this one.
        if client_disconnected(stream) {
            tracing::warn!(
                shard_id,
                completed_shards = shard_results.len(),
                "Client disconnected during vacuum; skipping remaining shards"
            );
            return Ok(());
        }

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
    use crate::config::{Cfg, SqliteConfig};
    use crate::redis::parse_resp_with_remaining;
    use crate::shard_manager::{self, ShardWriteOperation};
    use futures::future::{BoxFuture, join_all};
    use tempfile::TempDir;
    use tokio::net::{TcpListener, TcpStream};
    use tokio::time::{Duration, timeout};

    #[test]
    fn large_request_owns_its_buffer_after_release() {
        let value = vec![7u8; 2 * LARGE_READ_BUFFER];
        let request = format!("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n${}\r\n", value.len());
        let mut request = request.into_bytes();
        request.extend_from_slice(&value);
        request.extend_from_slice(b"\r\n");

        // The request fills its buffer exactly, so no capacity is left after it
        // is split off. grown_to is what handle_connection records on reserve.
        let mut buffer = BytesMut::with_capacity(request.len());
        buffer.extend_from_slice(&request);
        let mut grown_to = buffer.capacity();

        let frame = decode_request(&mut buffer, usize::MAX).unwrap().unwrap();
        release_large_buffer(&mut buffer, &mut grown_to);
        let BytesFrame::Array(mut args) = frame else {
            panic!("expected an array");
        };
        let Some(BytesFrame::BulkString(data)) = args.pop() else {
            panic!("expected a bulk string");
        };
        drop(args);
        assert_eq!(data, value);
        assert!(
            data.is_unique(),
            "buffer still shares the request's allocation"
        );
        assert_eq!(grown_to, READ_CHUNK);
    }

    #[test]
    fn small_requests_keep_their_buffer() {
        let mut buffer = BytesMut::with_capacity(READ_CHUNK);
        buffer.reserve(2 * READ_CHUNK);
        let mut grown_to = buffer.capacity();
        buffer.extend_from_slice(b"*1\r\n$4\r\nPING\r\n");
        decode_request(&mut buffer, usize::MAX).unwrap().unwrap();
        let before = buffer.as_ptr();
        release_large_buffer(&mut buffer, &mut grown_to);
        assert_eq!(buffer.as_ptr(), before, "small buffer was replaced");
    }

    struct TestContext {
        state: Arc<AppState>,
        writer_handles: Vec<tokio::task::JoinHandle<()>>,
        /// Receivers of shard writers not started yet; see [`Self::resume`].
        paused_receivers: Vec<tokio::sync::mpsc::Receiver<ShardWriteOperation>>,
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
                write_queue_capacity: None,
                addr: None,
                cluster: None,
                metrics: None,
                shutdown_timeout_secs: None,
                max_request_size_mb: None,
                cleanup_chunk_size: None,
                cleanup_interval_secs: None,
                sqlite: None,
            };

            Self::from_cfg(temp_dir, cfg).await
        }

        async fn from_cfg(temp_dir: TempDir, cfg: Cfg) -> Self {
            let mut ctx = Self::paused_from_cfg(temp_dir, cfg).await;
            ctx.resume();
            ctx
        }

        /// One shard whose writer doesn't run until [`Self::resume`], so async
        /// ops stay queued.
        async fn new_paused(async_write: bool) -> Self {
            let temp_dir = TempDir::new().expect("temp dir");
            let mut cfg = single_read_connection_cfg(&temp_dir, async_write);
            cfg.sqlite = None;
            cfg.batch_size = Some(16);
            Self::paused_from_cfg(temp_dir, cfg).await
        }

        async fn paused_from_cfg(temp_dir: TempDir, cfg: Cfg) -> Self {
            let mut paused_receivers = Vec::new();
            let state = Arc::new(AppState::new(cfg, &mut paused_receivers).await.unwrap());
            TestContext {
                state,
                writer_handles: Vec::new(),
                paused_receivers,
                _temp_dir: temp_dir,
            }
        }

        fn resume(&mut self) {
            let state = &self.state;
            for (i, receiver) in self.paused_receivers.drain(..).enumerate() {
                self.writer_handles
                    .push(tokio::spawn(shard_manager::shard_writer_task(
                        ShardWriter {
                            shard_id: i,
                            pool: state.write_db_pools[i].clone(),
                            receiver,
                            batch_size: state.cfg.batch_size.unwrap_or(1),
                            batch_timeout_ms: state.cfg.batch_timeout_ms.unwrap_or(0),
                            inflight_cache: state.inflight_cache.clone(),
                            inflight_hcache: state.inflight_hcache.clone(),
                            metrics: state.metrics.clone(),
                            shutdown: CancellationToken::new(),
                        },
                    )));
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

    fn single_read_connection_cfg(temp_dir: &TempDir, async_write: bool) -> Cfg {
        Cfg {
            data_dir: temp_dir.path().to_string_lossy().into_owned(),
            num_shards: 1,
            storage_compression: None,
            async_write: Some(async_write),
            batch_size: Some(1),
            batch_timeout_ms: Some(0),
            write_queue_capacity: None,
            addr: None,
            cluster: None,
            metrics: None,
            shutdown_timeout_secs: None,
            max_request_size_mb: None,
            cleanup_chunk_size: None,
            cleanup_interval_secs: None,
            sqlite: Some(SqliteConfig {
                cache_size_mb: None,
                busy_timeout_ms: Some(250),
                synchronous: None,
                mmap_size: None,
                max_connections: Some(1),
                auto_upgrade_legacy_auto_vacuum: None,
                auto_upgrade_legacy_auto_vacuum_concurrency: None,
            }),
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
            if let Some(name) = bulk_str(&items[idx])
                && name == field_name
            {
                return Some(&items[idx + 1]);
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

    type CommandFuture = BoxFuture<'static, Result<(), Box<dyn std::error::Error>>>;

    /// Runs `clients` copies of a command at once and returns their integer
    /// replies. They interleave at every await, so without the key lock each
    /// one's existence check runs before any of their writes land.
    async fn concurrent_integer_replies(
        ctx: &TestContext,
        clients: usize,
        command: impl Fn(usize, Arc<AppState>, TcpStream) -> CommandFuture,
    ) -> Vec<i64> {
        let command = &command;
        let replies = join_all(
            (0..clients).map(|i| respond_with(ctx, move |state, stream| command(i, state, stream))),
        )
        .await;
        replies
            .into_iter()
            .map(|frame| match frame {
                BytesFrame::Integer(n) => n,
                other => panic!("Expected integer, got {:?}", other),
            })
            .collect()
    }

    async fn hget_value(ctx: &TestContext, namespace: &str, field: &str) -> BytesFrame {
        let (namespace, field) = (namespace.to_string(), field.to_string());
        respond_with(ctx, |state, mut stream| {
            Box::pin(async move { handle_hget(&mut stream, &state, namespace, field).await })
        })
        .await
    }

    /// Waits for `key`'s queued async write to commit, so checks of it read
    /// the DB (and yield) instead of the inflight cache.
    async fn wait_for_commit(cache: &moka::future::Cache<String, Pending>, key: &str) {
        timeout(Duration::from_secs(5), async {
            while cache.contains_key(key) {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("async write didn't commit");
    }

    const CLIENTS: usize = 8;
    const ROUNDS: usize = 20;

    #[tokio::test]
    async fn concurrent_hsetex_fnx_has_one_winner() {
        for async_write in [false, true] {
            let ctx = TestContext::new(async_write).await;
            for round in 0..ROUNDS {
                // A new namespace each round, so each winner's write also
                // creates its namespace table. Half the clients spell it in
                // upper case: distinct cases get distinct tables and one winner each.
                let namespace = format!("ns{round}");
                let upper_namespace = namespace.to_ascii_uppercase();
                let field = "lock".to_string();
                let replies = concurrent_integer_replies(&ctx, CLIENTS, |i, state, mut stream| {
                    let namespace = if i % 2 == 0 {
                        namespace.clone()
                    } else {
                        upper_namespace.clone()
                    };
                    let field = field.clone();
                    Box::pin(async move {
                        let value = Bytes::from(format!("client{i}"));
                        handle_hsetex(
                            &mut stream,
                            &state,
                            namespace,
                            true,
                            false,
                            Some(crate::redis::protocol::ExpireOption::Ex(100_000)),
                            vec![(field, value)],
                        )
                        .await
                    })
                })
                .await;
                let lower_winners: Vec<usize> = (0..CLIENTS)
                    .filter(|&i| i % 2 == 0 && replies[i] == 1)
                    .collect();
                let upper_winners: Vec<usize> = (0..CLIENTS)
                    .filter(|&i| i % 2 == 1 && replies[i] == 1)
                    .collect();
                assert_eq!(
                    lower_winners.len(),
                    1,
                    "async_write={async_write}: lower winners {replies:?}"
                );
                assert_eq!(
                    upper_winners.len(),
                    1,
                    "async_write={async_write}: upper winners {replies:?}"
                );
                let expected_lower = format!("client{}", lower_winners[0]);
                let expected_upper = format!("client{}", upper_winners[0]);
                assert_bulk_string(
                    hget_value(&ctx, &namespace, &field).await,
                    expected_lower.as_bytes(),
                );
                assert_bulk_string(
                    hget_value(&ctx, &upper_namespace, &field).await,
                    expected_upper.as_bytes(),
                );
            }
            ctx.shutdown().await;
        }
    }

    #[tokio::test]
    async fn hsetex_fxx_never_revives_a_concurrently_deleted_field() {
        for async_write in [false, true] {
            let ctx = TestContext::new(async_write).await;
            for round in 0..ROUNDS {
                let field = format!("f{round}");
                let frame = respond_with(&ctx, |state, mut stream| {
                    let field = field.clone();
                    Box::pin(async move {
                        handle_hset(&mut stream, &state, "ns".into(), field, "v0".into()).await
                    })
                })
                .await;
                assert_integer_response(frame, 1);
                wait_for_commit(&ctx.state.inflight_hcache, &format!("ns:{field}")).await;

                // Client 0 deletes the field, the rest overwrite it only if it
                // still exists. Whatever order they run in, the field ends up
                // deleted: an FXX write after the HDEL must not bring it back.
                concurrent_integer_replies(&ctx, CLIENTS, |i, state, mut stream| {
                    let field = field.clone();
                    Box::pin(async move {
                        if i == 0 {
                            handle_hdel(&mut stream, &state, "ns".into(), field).await
                        } else {
                            handle_hsetex(
                                &mut stream,
                                &state,
                                "ns".to_string(),
                                false,
                                true,
                                None,
                                vec![(field, Bytes::from(format!("client{i}")))],
                            )
                            .await
                        }
                    })
                })
                .await;
                assert_null(hget_value(&ctx, "ns", &field).await);
            }

            // Control: with no HDEL racing them, FXX writes land. HSETEX
            // replies with the count of new fields, which FXX never adds.
            let frame = respond_with(&ctx, |state, mut stream| {
                Box::pin(async move {
                    handle_hset(&mut stream, &state, "ns".into(), "kept".into(), "v0".into()).await
                })
            })
            .await;
            assert_integer_response(frame, 1);
            let replies = concurrent_integer_replies(&ctx, CLIENTS, |i, state, mut stream| {
                Box::pin(async move {
                    handle_hsetex(
                        &mut stream,
                        &state,
                        "ns".to_string(),
                        false,
                        true,
                        None,
                        vec![("kept".to_string(), Bytes::from(format!("client{i}")))],
                    )
                    .await
                })
            })
            .await;
            assert_eq!(replies, [0; CLIENTS], "async_write={async_write}");
            let BytesFrame::BulkString(value) = hget_value(&ctx, "ns", "kept").await else {
                panic!("FXX control field is missing");
            };
            assert!(
                value.starts_with(b"client"),
                "async_write={async_write}: no FXX write landed"
            );
            ctx.shutdown().await;
        }
    }

    #[tokio::test]
    async fn concurrent_del_counts_the_key_once() {
        for async_write in [false, true] {
            let ctx = TestContext::new(async_write).await;
            for round in 0..ROUNDS {
                let key = format!("k{round}");
                let frame = respond_with(&ctx, |state, mut stream| {
                    let key = key.clone();
                    Box::pin(
                        async move { handle_set(&mut stream, &state, key, "v".into(), None).await },
                    )
                })
                .await;
                assert_simple_string(frame, "OK");
                wait_for_commit(&ctx.state.inflight_cache, &key).await;

                let replies = concurrent_integer_replies(&ctx, CLIENTS, |_, state, mut stream| {
                    let key = key.clone();
                    Box::pin(
                        async move { handle_del_multiple(&mut stream, &state, vec![key]).await },
                    )
                })
                .await;
                assert_eq!(
                    replies.iter().sum::<i64>(),
                    1,
                    "async_write={async_write}: {replies:?}"
                );
            }
            ctx.shutdown().await;
        }
    }

    #[tokio::test]
    async fn concurrent_multi_key_dels_each_count_a_key_once() {
        for async_write in [false, true] {
            let ctx = TestContext::new(async_write).await;
            for round in 0..ROUNDS {
                let (a, b) = (format!("a{round}"), format!("b{round}"));
                for key in [&a, &b] {
                    let key = key.clone();
                    let frame = respond_with(&ctx, |state, mut stream| {
                        Box::pin(async move {
                            handle_set(&mut stream, &state, key, "v".into(), None).await
                        })
                    })
                    .await;
                    assert_simple_string(frame, "OK");
                }
                wait_for_commit(&ctx.state.inflight_cache, &a).await;
                wait_for_commit(&ctx.state.inflight_cache, &b).await;

                // DEL a b, DEL b a and DEL a a b race: one of them deletes
                // both keys, and a key named twice counts once.
                let replies = concurrent_integer_replies(&ctx, 3, |i, state, mut stream| {
                    let keys = match i {
                        0 => vec![a.clone(), b.clone()],
                        1 => vec![b.clone(), a.clone()],
                        _ => vec![a.clone(), a.clone(), b.clone()],
                    };
                    Box::pin(async move { handle_del_multiple(&mut stream, &state, keys).await })
                })
                .await;
                let mut sorted = replies.clone();
                sorted.sort_unstable();
                assert_eq!(sorted, [0, 0, 2], "async_write={async_write}: {replies:?}");
            }
            ctx.shutdown().await;
        }
    }

    #[tokio::test]
    async fn hash_namespaces_are_case_sensitive() {
        for async_write in [false, true] {
            let ctx = TestContext::new(async_write).await;

            // Exact reproduction from #24: Users and users stay completely isolated.
            let frame = respond_with(&ctx, |state, mut stream| {
                Box::pin(async move {
                    handle_hset(
                        &mut stream,
                        &state,
                        "Users".into(),
                        "alice".into(),
                        "upper".into(),
                    )
                    .await
                })
            })
            .await;
            assert_integer_response(frame, 1);

            let frame = respond_with(&ctx, |state, mut stream| {
                Box::pin(async move {
                    handle_hset(
                        &mut stream,
                        &state,
                        "users".into(),
                        "alice".into(),
                        "lower".into(),
                    )
                    .await
                })
            })
            .await;
            assert_integer_response(frame, 1);

            // Both reads see their own value immediately (including inflight cache in async mode).
            assert_bulk_string(hget_value(&ctx, "Users", "alice").await, b"upper");
            assert_bulk_string(hget_value(&ctx, "users", "alice").await, b"lower");

            wait_for_commit(
                &ctx.state.inflight_hcache,
                &ctx.state.namespaced_key("Users", "alice"),
            )
            .await;
            wait_for_commit(
                &ctx.state.inflight_hcache,
                &ctx.state.namespaced_key("users", "alice"),
            )
            .await;

            // Both reads still see their own value from SQLite.
            assert_bulk_string(hget_value(&ctx, "Users", "alice").await, b"upper");
            assert_bulk_string(hget_value(&ctx, "users", "alice").await, b"lower");

            // HDEL on `users` removes only `users`, leaving `Users` untouched.
            let frame = respond_with(&ctx, |state, mut stream| {
                Box::pin(async move {
                    handle_hdel(&mut stream, &state, "users".into(), "alice".into()).await
                })
            })
            .await;
            assert_integer_response(frame, 1);
            assert_null(hget_value(&ctx, "users", "alice").await);
            assert_bulk_string(hget_value(&ctx, "Users", "alice").await, b"upper");

            // HEXISTS and HEXPIRE stay isolated too.
            let frame = respond_with(&ctx, |state, mut stream| {
                Box::pin(async move {
                    handle_hexists(&mut stream, &state, "users".into(), "alice".into()).await
                })
            })
            .await;
            assert_integer_response(frame, 0);
            let frame = respond_with(&ctx, |state, mut stream| {
                Box::pin(async move {
                    handle_hexists(&mut stream, &state, "Users".into(), "alice".into()).await
                })
            })
            .await;
            assert_integer_response(frame, 1);

            let frame = respond_with(&ctx, |state, mut stream| {
                Box::pin(async move {
                    handle_hexpire(
                        &mut stream,
                        &state,
                        "users".into(),
                        60,
                        None,
                        vec!["alice".into()],
                    )
                    .await
                })
            })
            .await;
            assert_eq!(frame, BytesFrame::Array(vec![BytesFrame::Integer(-2)]));

            // Both namespaces are recorded in blob_namespaces with distinct tables.
            let shard_idx = ctx.state.get_shard("alice");
            let meta: Vec<(String, String)> = sqlx::query_as(
                "SELECT namespace, table_name FROM blob_namespaces ORDER BY namespace",
            )
            .fetch_all(&ctx.state.db_pools[shard_idx])
            .await
            .unwrap();
            assert_eq!(
                meta,
                vec![
                    ("Users".to_string(), shard_manager::namespace_table("Users")),
                    ("users".to_string(), shard_manager::namespace_table("users")),
                ]
            );
            assert_ne!(meta[0].1, meta[1].1);

            ctx.shutdown().await;
        }
    }

    #[tokio::test]
    async fn startup_migrates_legacy_namespace_tables_in_place() {
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path().join("shard_0.db");
        let pool = sqlx::SqlitePool::connect(&format!("sqlite:{}?mode=rwc", db_path.display()))
            .await
            .unwrap();

        // Simulate a legacy (user_version = 0) shard with lowercase and mixed-case tables.
        sqlx::query(
            "CREATE TABLE blobs (
                key TEXT PRIMARY KEY,
                data BLOB,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                expires_at INTEGER,
                version INTEGER NOT NULL DEFAULT 0
            )",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "CREATE TABLE blobs_workspaces (
                key TEXT PRIMARY KEY,
                data BLOB,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                expires_at INTEGER,
                version INTEGER NOT NULL DEFAULT 0
            )",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "CREATE INDEX idx_workspaces_expires_at ON blobs_workspaces(expires_at) WHERE expires_at IS NOT NULL",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO blobs_workspaces (key, data, created_at, updated_at, expires_at, version)
             VALUES ('ws1', ?, 0, 0, NULL, 1)",
        )
        .bind(b"legacy-data".as_slice())
        .execute(&pool)
        .await
        .unwrap();
        pool.close().await;

        let mut cfg = single_read_connection_cfg(&temp_dir, false);
        cfg.sqlite = None;
        let ctx = TestContext::from_cfg(temp_dir, cfg).await;

        // Legacy data is readable under its original namespace and isolated from different casing.
        assert_bulk_string(hget_value(&ctx, "workspaces", "ws1").await, b"legacy-data");
        assert_null(hget_value(&ctx, "Workspaces", "ws1").await);

        let frame = respond_with(&ctx, |state, mut stream| {
            Box::pin(async move {
                handle_hset(
                    &mut stream,
                    &state,
                    "Workspaces".into(),
                    "ws1".into(),
                    "upper-data".into(),
                )
                .await
            })
        })
        .await;
        assert_integer_response(frame, 1);
        assert_bulk_string(hget_value(&ctx, "workspaces", "ws1").await, b"legacy-data");
        assert_bulk_string(hget_value(&ctx, "Workspaces", "ws1").await, b"upper-data");

        let version: i64 = sqlx::query_scalar("PRAGMA user_version")
            .fetch_one(&ctx.state.db_pools[0])
            .await
            .unwrap();
        assert_eq!(version, shard_manager::SHARD_SCHEMA_VERSION);

        ctx.shutdown().await;
    }

    #[tokio::test]
    async fn startup_rejects_future_schema_version_without_creating_blobs_table() {
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path().join("shard_0.db");
        let pool = sqlx::SqlitePool::connect(&format!("sqlite:{}?mode=rwc", db_path.display()))
            .await
            .unwrap();
        sqlx::query(&format!(
            "PRAGMA user_version = {}",
            shard_manager::SHARD_SCHEMA_VERSION + 1
        ))
        .execute(&pool)
        .await
        .unwrap();
        pool.close().await;

        let mut cfg = single_read_connection_cfg(&temp_dir, false);
        cfg.sqlite = None;
        let mut receivers = Vec::new();
        let err = match AppState::new(cfg, &mut receivers).await {
            Ok(_) => panic!("future schema version must be rejected"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("unsupported schema version"));

        // Verify startup did not create a `blobs` table in the future-version shard.
        let ro_pool = sqlx::SqlitePool::connect(&format!("sqlite:{}?mode=ro", db_path.display()))
            .await
            .unwrap();
        let blobs_exists: Option<(String,)> = sqlx::query_as(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'blobs'",
        )
        .fetch_optional(&ro_pool)
        .await
        .unwrap();
        assert_eq!(blobs_exists, None);
        ro_pool.close().await;
    }

    #[tokio::test]
    async fn waiting_for_a_key_lock_doesnt_shorten_a_relative_ttl() {
        let ctx = TestContext::new(false).await;
        let frame = respond_with(&ctx, |state, mut stream| {
            Box::pin(async move {
                handle_hset(&mut stream, &state, "ns".into(), "f".into(), "v".into()).await
            })
        })
        .await;
        assert_integer_response(frame, 1);

        // Hold the field's lock across a second boundary. HEXPIRE's TTL
        // counts from when it gets the lock, so its expiry can't be earlier
        // than the release plus the TTL.
        const TTL: i64 = 100;
        let guard = ctx.state.key_locks.lock_fields("ns", &["f"]).await;
        let hexpire = respond_with(&ctx, |state, mut stream| {
            Box::pin(async move {
                handle_hexpire(
                    &mut stream,
                    &state,
                    "ns".into(),
                    TTL,
                    None,
                    vec!["f".into()],
                )
                .await
            })
        });
        let release = async {
            tokio::time::sleep(Duration::from_millis(1200)).await;
            let released_at = chrono::Utc::now().timestamp();
            drop(guard);
            released_at
        };
        let (frame, released_at) = tokio::join!(hexpire, release);
        match frame {
            BytesFrame::Array(codes) => assert_eq!(codes, [BytesFrame::Integer(1)]),
            other => panic!("Expected HEXPIRE codes, got {:?}", other),
        }
        let ns_table = shard_manager::namespace_table("ns");
        let (expires_at,): (i64,) = sqlx::query_as(&format!(
            "SELECT expires_at FROM {ns_table} WHERE key = 'f'"
        ))
        .fetch_one(&ctx.state.db_pools[0])
        .await
        .unwrap();
        assert!(
            expires_at >= released_at + TTL,
            "expiry {expires_at} counts from before the lock was released at {released_at}"
        );
        ctx.shutdown().await;
    }

    #[tokio::test]
    async fn namespaces_get_their_own_expiry_index() {
        let ctx = TestContext::new(false).await;
        for namespace in ["foo", "FOO", "BLOBS_FOO"] {
            let frame = respond_with(&ctx, |state, mut stream| {
                Box::pin(async move {
                    handle_hset(
                        &mut stream,
                        &state,
                        namespace.into(),
                        "f".into(),
                        "v".into(),
                    )
                    .await
                })
            })
            .await;
            assert_integer_response(frame, 1);
        }
        let mut expected = vec![
            shard_manager::namespace_table("foo"),
            shard_manager::namespace_table("FOO"),
            shard_manager::namespace_table("BLOBS_FOO"),
        ];
        expected.sort();
        let indexes: Vec<(String,)> = sqlx::query_as(
            "SELECT tbl_name FROM sqlite_master WHERE type = 'index' AND name LIKE '%_expires_at'
             AND tbl_name IN (?, ?, ?) ORDER BY tbl_name",
        )
        .bind(&expected[0])
        .bind(&expected[1])
        .bind(&expected[2])
        .fetch_all(&ctx.state.db_pools[0])
        .await
        .unwrap();
        let tables: Vec<String> = indexes.into_iter().map(|(table,)| table).collect();
        assert_eq!(tables, expected);
        ctx.shutdown().await;
    }

    #[tokio::test]
    async fn concurrent_hset_and_hdel_count_the_field_once() {
        for async_write in [false, true] {
            let ctx = TestContext::new(async_write).await;
            for round in 0..ROUNDS {
                let field = format!("f{round}");
                let added = concurrent_integer_replies(&ctx, CLIENTS, |i, state, mut stream| {
                    let field = field.clone();
                    Box::pin(async move {
                        let value = Bytes::from(format!("client{i}"));
                        handle_hset(&mut stream, &state, "ns".into(), field, value).await
                    })
                })
                .await;
                assert_eq!(
                    added.iter().sum::<i64>(),
                    1,
                    "HSET async_write={async_write}: {added:?}"
                );

                let deleted = concurrent_integer_replies(&ctx, CLIENTS, |_, state, mut stream| {
                    let field = field.clone();
                    Box::pin(
                        async move { handle_hdel(&mut stream, &state, "ns".into(), field).await },
                    )
                })
                .await;
                assert_eq!(
                    deleted.iter().sum::<i64>(),
                    1,
                    "HDEL async_write={async_write}: {deleted:?}"
                );
            }
            ctx.shutdown().await;
        }
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
    async fn sync_write_completes_while_read_pool_connection_is_occupied() {
        let temp_dir = TempDir::new().expect("temp dir");
        let cfg = single_read_connection_cfg(&temp_dir, false);
        let ctx = TestContext::from_cfg(temp_dir, cfg).await;

        let read_conn = ctx.state.db_pools[0]
            .acquire()
            .await
            .expect("occupy read pool connection");

        let (responder, response_rx) = tokio::sync::oneshot::channel();
        ctx.state.shard_senders[0]
            .send(ShardWriteOperation::Set {
                key: "dedicated-writer".to_string(),
                data: Bytes::from_static(b"value"),
                expires_at: None,
                responder,
            })
            .await
            .expect("queue write");

        let write_result = timeout(Duration::from_secs(1), response_rx)
            .await
            .expect("write should not be blocked by a busy read pool")
            .expect("writer task dropped response channel");
        assert!(write_result.is_ok(), "writer task should persist the value");

        drop(read_conn);

        let persisted = sqlx::query_scalar::<_, Vec<u8>>("SELECT data FROM blobs WHERE key = ?")
            .bind("dedicated-writer")
            .fetch_one(&ctx.state.db_pools[0])
            .await
            .expect("read back persisted value");
        assert_eq!(persisted, b"value");

        ctx.shutdown().await;
    }

    #[tokio::test]
    async fn del_completes_while_read_pool_connection_is_occupied() {
        let temp_dir = TempDir::new().expect("temp dir");
        let cfg = single_read_connection_cfg(&temp_dir, false);
        let ctx = TestContext::from_cfg(temp_dir, cfg).await;

        let frame = respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_set(
                    &mut stream,
                    &state,
                    "delete-me".to_string(),
                    Bytes::from_static(b"value"),
                    None,
                )
                .await
            })
        })
        .await;
        assert_simple_string(frame, "OK");

        let read_conn = ctx.state.db_pools[0]
            .acquire()
            .await
            .expect("occupy read pool connection");

        let frame = timeout(
            Duration::from_secs(1),
            respond_with(&ctx, |state, stream| {
                Box::pin(async move {
                    let mut stream = stream;
                    handle_del_multiple(&mut stream, &state, vec!["delete-me".to_string()]).await
                })
            }),
        )
        .await
        .expect("DEL should not block on the read pool");
        assert_integer_response(frame, 1);

        drop(read_conn);

        let remaining = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM blobs WHERE key = ?")
            .bind("delete-me")
            .fetch_one(&ctx.state.db_pools[0])
            .await
            .expect("check deleted row");
        assert_eq!(remaining, 0);

        ctx.shutdown().await;
    }

    #[tokio::test]
    async fn async_writes_ack_while_writer_connection_is_occupied() {
        let temp_dir = TempDir::new().expect("temp dir");
        let cfg = single_read_connection_cfg(&temp_dir, true);
        let ctx = TestContext::from_cfg(temp_dir, cfg).await;

        for field in ["alice", "bob"] {
            let frame = respond_with(&ctx, |state, stream| {
                Box::pin(async move {
                    let mut stream = stream;
                    handle_hset(
                        &mut stream,
                        &state,
                        "users".to_string(),
                        field.to_string(),
                        Bytes::from_static(b"first"),
                    )
                    .await
                })
            })
            .await;
            assert_integer_response(frame, 1);
        }
        let frame = respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_set(
                    &mut stream,
                    &state,
                    "k".to_string(),
                    Bytes::from_static(b"v"),
                    None,
                )
                .await
            })
        })
        .await;
        assert_simple_string(frame, "OK");

        // Wait for the seeds to commit, so the checks below go to the DB.
        timeout(Duration::from_secs(5), async {
            while ctx.state.inflight_hcache.get("users:alice").await.is_some()
                || ctx.state.inflight_hcache.get("users:bob").await.is_some()
                || ctx.state.inflight_cache.get("k").await.is_some()
            {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("seed writes did not commit");

        // As if the writer were mid-commit: it holds its only connection.
        let writer_conn = ctx.state.write_db_pools[0]
            .acquire()
            .await
            .expect("occupy writer connection");

        let frame = timeout(
            Duration::from_secs(1),
            respond_with(&ctx, |state, stream| {
                Box::pin(async move {
                    let mut stream = stream;
                    handle_hset(
                        &mut stream,
                        &state,
                        "users".to_string(),
                        "alice".to_string(),
                        Bytes::from_static(b"second"),
                    )
                    .await
                })
            }),
        )
        .await
        .expect("async HSET should not wait for the writer connection");
        assert_integer_response(frame, 0);

        let frame = timeout(
            Duration::from_secs(1),
            respond_with(&ctx, |state, stream| {
                Box::pin(async move {
                    let mut stream = stream;
                    handle_hdel(&mut stream, &state, "users".to_string(), "bob".to_string()).await
                })
            }),
        )
        .await
        .expect("async HDEL should not wait for the writer connection");
        assert_integer_response(frame, 1);

        let frame = timeout(
            Duration::from_secs(1),
            respond_with(&ctx, |state, stream| {
                Box::pin(async move {
                    let mut stream = stream;
                    handle_del_multiple(&mut stream, &state, vec!["k".to_string()]).await
                })
            }),
        )
        .await
        .expect("async DEL should not wait for the writer connection");
        assert_integer_response(frame, 1);

        drop(writer_conn);
        ctx.shutdown().await;
    }

    #[tokio::test]
    async fn hdel_completes_while_read_pool_connection_is_occupied() {
        let temp_dir = TempDir::new().expect("temp dir");
        let cfg = single_read_connection_cfg(&temp_dir, false);
        let ctx = TestContext::from_cfg(temp_dir, cfg).await;

        let frame = respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_hset(
                    &mut stream,
                    &state,
                    "users".to_string(),
                    "alice".to_string(),
                    Bytes::from_static(b"value"),
                )
                .await
            })
        })
        .await;
        assert_integer_response(frame, 1);

        let read_conn = ctx.state.db_pools[0]
            .acquire()
            .await
            .expect("occupy read pool connection");

        let frame = timeout(
            Duration::from_secs(1),
            respond_with(&ctx, |state, stream| {
                Box::pin(async move {
                    let mut stream = stream;
                    handle_hdel(
                        &mut stream,
                        &state,
                        "users".to_string(),
                        "alice".to_string(),
                    )
                    .await
                })
            }),
        )
        .await
        .expect("HDEL should not block on the read pool");
        assert_integer_response(frame, 1);

        drop(read_conn);

        let users_table = shard_manager::namespace_table("users");
        let remaining = sqlx::query_scalar::<_, i64>(&format!(
            "SELECT COUNT(*) FROM {users_table} WHERE key = ?"
        ))
        .bind("alice")
        .fetch_one(&ctx.state.db_pools[0])
        .await
        .expect("check deleted hash field");
        assert_eq!(remaining, 0);

        ctx.shutdown().await;
    }

    #[tokio::test]
    async fn hset_and_hsetex_complete_while_read_pool_connection_is_occupied() {
        let temp_dir = TempDir::new().expect("temp dir");
        let cfg = single_read_connection_cfg(&temp_dir, false);
        let ctx = TestContext::from_cfg(temp_dir, cfg).await;

        let read_conn = ctx.state.db_pools[0]
            .acquire()
            .await
            .expect("occupy read pool connection");

        let frame = timeout(
            Duration::from_secs(1),
            respond_with(&ctx, |state, stream| {
                Box::pin(async move {
                    let mut stream = stream;
                    handle_hset(
                        &mut stream,
                        &state,
                        "users".to_string(),
                        "alice".to_string(),
                        Bytes::from_static(b"first"),
                    )
                    .await
                })
            }),
        )
        .await
        .expect("HSET should not block on the read pool");
        assert_integer_response(frame, 1);

        let frame = timeout(
            Duration::from_secs(1),
            respond_with(&ctx, |state, stream| {
                Box::pin(async move {
                    let mut stream = stream;
                    handle_hsetex(
                        &mut stream,
                        &state,
                        "users".to_string(),
                        true,
                        false,
                        None,
                        vec![("bob".to_string(), Bytes::from_static(b"second"))],
                    )
                    .await
                })
            }),
        )
        .await
        .expect("HSETEX should not block on the read pool");
        assert_integer_response(frame, 1);

        drop(read_conn);

        let users_table = shard_manager::namespace_table("users");
        let alice = sqlx::query_scalar::<_, Vec<u8>>(&format!(
            "SELECT data FROM {users_table} WHERE key = ?"
        ))
        .bind("alice")
        .fetch_one(&ctx.state.db_pools[0])
        .await
        .expect("read back alice");
        assert_eq!(alice, b"first");

        let bob = sqlx::query_scalar::<_, Vec<u8>>(&format!(
            "SELECT data FROM {users_table} WHERE key = ?"
        ))
        .bind("bob")
        .fetch_one(&ctx.state.db_pools[0])
        .await
        .expect("read back bob");
        assert_eq!(bob, b"second");

        ctx.shutdown().await;
    }

    #[tokio::test]
    async fn del_and_hdel_return_error_when_existence_check_fails() {
        let ctx = TestContext::new(false).await;
        // Simulates the existence check failing.
        ctx.state.check_db_pools[0].close().await;

        let frame = respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_del_multiple(&mut stream, &state, vec!["some-key".to_string()]).await
            })
        })
        .await;
        assert_error_contains(frame, "database error");

        let frame = respond_with(&ctx, |state, stream| {
            Box::pin(async move {
                let mut stream = stream;
                handle_hdel(&mut stream, &state, "ns".to_string(), "field".to_string()).await
            })
        })
        .await;
        assert_error_contains(frame, "database error");

        ctx.shutdown().await;
    }

    #[tokio::test]
    async fn client_disconnected_detects_closed_peer() {
        let (server, client) = tcp_pair().await;
        assert!(!client_disconnected(&server));

        drop(client);
        let detected = timeout(Duration::from_secs(1), async {
            while !client_disconnected(&server) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        assert!(detected.is_ok(), "closed peer should be detected");
    }

    #[tokio::test]
    async fn hget_and_hexists_miss_when_namespace_table_is_absent() {
        let ctx = TestContext::new(false).await;

        let frame = respond_with(&ctx, |state, mut stream| {
            Box::pin(async move {
                handle_hget(
                    &mut stream,
                    &state,
                    "never_written".to_string(),
                    "f".to_string(),
                )
                .await
            })
        })
        .await;
        assert_null(frame);

        let frame = respond_with(&ctx, |state, mut stream| {
            Box::pin(async move {
                handle_hexists(
                    &mut stream,
                    &state,
                    "never_written".to_string(),
                    "f".to_string(),
                )
                .await
            })
        })
        .await;
        assert_integer_response(frame, 0);

        ctx.shutdown().await;
    }

    #[tokio::test]
    async fn async_del_and_exists_count_writes_still_queued() {
        let ctx = TestContext::new(true).await;
        // An acked async SET/HSET whose op hasn't committed yet: in the inflight
        // cache, not in the DB.
        let value = Bytes::from_static(b"v");
        ctx.state
            .inflight_cache
            .insert(
                "queued".to_string(),
                Pending::Value {
                    data: value.clone(),
                    token: shard_manager::next_pending_token(),
                },
            )
            .await;
        ctx.state
            .inflight_hcache
            .insert(
                ctx.state.namespaced_key("ns", "field"),
                Pending::Value {
                    data: value,
                    token: shard_manager::next_pending_token(),
                },
            )
            .await;

        let frame = respond_with(&ctx, |state, mut stream| {
            Box::pin(async move { handle_exists(&mut stream, &state, "queued".to_string()).await })
        })
        .await;
        assert_integer_response(frame, 1);

        let frame = respond_with(&ctx, |state, mut stream| {
            Box::pin(async move {
                handle_hexists(&mut stream, &state, "ns".to_string(), "field".to_string()).await
            })
        })
        .await;
        assert_integer_response(frame, 1);

        // Overwriting a queued field is an update, not a new field.
        let frame = respond_with(&ctx, |state, mut stream| {
            Box::pin(async move {
                handle_hset(
                    &mut stream,
                    &state,
                    "ns".to_string(),
                    "field".to_string(),
                    Bytes::from_static(b"v2"),
                )
                .await
            })
        })
        .await;
        assert_integer_response(frame, 0);

        // Skipping these deletes would let the queued writes commit afterwards
        // and bring the keys back.
        let frame = respond_with(&ctx, |state, mut stream| {
            Box::pin(async move {
                handle_del_multiple(&mut stream, &state, vec!["queued".to_string()]).await
            })
        })
        .await;
        assert_integer_response(frame, 1);

        let frame = respond_with(&ctx, |state, mut stream| {
            Box::pin(async move {
                handle_hdel(&mut stream, &state, "ns".to_string(), "field".to_string()).await
            })
        })
        .await;
        assert_integer_response(frame, 1);

        let frame = respond_with(&ctx, |state, mut stream| {
            Box::pin(async move { handle_exists(&mut stream, &state, "queued".to_string()).await })
        })
        .await;
        assert_integer_response(frame, 0);

        ctx.shutdown().await;
    }

    #[tokio::test]
    async fn reads_see_async_deletes_still_queued() {
        let mut ctx = TestContext::new_paused(true).await;
        // A committed key and field.
        let pool = &ctx.state.write_db_pools[0];
        sqlx::query(
            "INSERT INTO blobs (key, data, created_at, updated_at) VALUES ('k', 'v', 0, 0)",
        )
        .execute(pool)
        .await
        .unwrap();
        let ns_table = shard_manager::namespace_table("ns");
        sqlx::query(&format!(
            "CREATE TABLE {ns_table} (key TEXT PRIMARY KEY, data BLOB, created_at INTEGER NOT NULL,
             updated_at INTEGER NOT NULL, expires_at INTEGER, version INTEGER NOT NULL DEFAULT 0)"
        ))
        .execute(pool)
        .await
        .unwrap();
        sqlx::query(&format!(
            "INSERT INTO {ns_table} (key, data, created_at, updated_at) VALUES ('f', 'v', 0, 0)"
        ))
        .execute(pool)
        .await
        .unwrap();

        // The writer is paused, so these deletes stay queued and the rows stay
        // in the DB. Reads must still see them as gone.
        let frame = respond_with(&ctx, |state, mut stream| {
            Box::pin(async move {
                handle_del_multiple(&mut stream, &state, vec!["k".to_string()]).await
            })
        })
        .await;
        assert_integer_response(frame, 1);
        let frame = respond_with(&ctx, |state, mut stream| {
            Box::pin(async move { handle_get(&mut stream, &state, "k".to_string()).await })
        })
        .await;
        assert_null(frame);
        let frame = respond_with(&ctx, |state, mut stream| {
            Box::pin(async move { handle_exists(&mut stream, &state, "k".to_string()).await })
        })
        .await;
        assert_integer_response(frame, 0);
        let frame = respond_with(&ctx, |state, mut stream| {
            Box::pin(async move { handle_ttl(&mut stream, &state, "k".to_string()).await })
        })
        .await;
        assert_integer_response(frame, -2);
        let frame = respond_with(&ctx, |state, mut stream| {
            Box::pin(async move {
                handle_del_multiple(&mut stream, &state, vec!["k".to_string()]).await
            })
        })
        .await;
        assert_integer_response(frame, 0);

        let frame = respond_with(&ctx, |state, mut stream| {
            Box::pin(async move {
                handle_hdel(&mut stream, &state, "ns".to_string(), "f".to_string()).await
            })
        })
        .await;
        assert_integer_response(frame, 1);
        let frame = respond_with(&ctx, |state, mut stream| {
            Box::pin(async move {
                handle_hget(&mut stream, &state, "ns".to_string(), "f".to_string()).await
            })
        })
        .await;
        assert_null(frame);
        let frame = respond_with(&ctx, |state, mut stream| {
            Box::pin(async move {
                handle_hexists(&mut stream, &state, "ns".to_string(), "f".to_string()).await
            })
        })
        .await;
        assert_integer_response(frame, 0);
        let frame = respond_with(&ctx, |state, mut stream| {
            Box::pin(async move {
                handle_hdel(&mut stream, &state, "ns".to_string(), "f".to_string()).await
            })
        })
        .await;
        assert_integer_response(frame, 0);
        // FXX needs the field to exist.
        let frame = respond_with(&ctx, |state, mut stream| {
            Box::pin(async move {
                let fields = vec![("f".to_string(), Bytes::from_static(b"fxx"))];
                handle_hsetex(
                    &mut stream,
                    &state,
                    "ns".to_string(),
                    false,
                    true,
                    None,
                    fields,
                )
                .await
            })
        })
        .await;
        assert_integer_response(frame, 0);
        // Re-adding a deleted field creates it.
        let frame = respond_with(&ctx, |state, mut stream| {
            Box::pin(async move {
                handle_hset(
                    &mut stream,
                    &state,
                    "ns".to_string(),
                    "f".to_string(),
                    Bytes::from_static(b"v2"),
                )
                .await
            })
        })
        .await;
        assert_integer_response(frame, 1);

        // Once the queue commits (EXPIRE is synchronous, so it waits for the
        // ops ahead of it), the DB agrees and the cache is empty.
        ctx.resume();
        respond_with(&ctx, |state, mut stream| {
            Box::pin(async move { handle_expire(&mut stream, &state, "k".to_string(), 10).await })
        })
        .await;
        assert!(!ctx.state.inflight_cache.contains_key("k"));
        assert!(!ctx.state.inflight_hcache.contains_key("ns:f"));
        let frame = respond_with(&ctx, |state, mut stream| {
            Box::pin(async move { handle_get(&mut stream, &state, "k".to_string()).await })
        })
        .await;
        assert_null(frame);
        let frame = respond_with(&ctx, |state, mut stream| {
            Box::pin(async move {
                handle_hget(&mut stream, &state, "ns".to_string(), "f".to_string()).await
            })
        })
        .await;
        assert_bulk_string(frame, b"v2");

        ctx.shutdown().await;
    }

    #[tokio::test]
    async fn pipelined_async_empty_writes_never_serve_stale_reads() {
        let ctx = TestContext::new(true).await;
        // Pre-create the namespace table (leaving f and f2 absent) so the
        // second pipelined HSET cannot see a stale table-absent check if the
        // first HSET commits before pending_exists runs.
        let ns_table = shard_manager::namespace_table("ns");
        sqlx::query(&format!(
            "CREATE TABLE {ns_table} (key TEXT PRIMARY KEY, data BLOB, created_at INTEGER NOT NULL,
             updated_at INTEGER NOT NULL, expires_at INTEGER, version INTEGER NOT NULL DEFAULT 0)"
        ))
        .execute(&ctx.state.write_db_pools[0])
        .await
        .unwrap();
        let (server_stream, mut client_stream) = tcp_pair().await;
        let shutdown = CancellationToken::new();
        let state_clone = ctx.state.clone();
        let shutdown_clone = shutdown.clone();
        let conn_task = tokio::spawn(async move {
            let _ = handle_connection(server_stream, state_clone, shutdown_clone).await;
        });

        let encode_cmd = |parts: &[&[u8]]| {
            let mut out = format!("*{}\r\n", parts.len()).into_bytes();
            for part in parts {
                out.extend_from_slice(format!("${}\r\n", part.len()).as_bytes());
                out.extend_from_slice(part);
                out.extend_from_slice(b"\r\n");
            }
            out
        };

        async fn read_replies(
            stream: &mut TcpStream,
            buf: &mut Vec<u8>,
            count: usize,
        ) -> Vec<BytesFrame> {
            let mut replies = Vec::with_capacity(count);
            let mut tmp = [0u8; 4096];
            while replies.len() < count {
                let mut consumed = 0;
                while replies.len() < count {
                    match parse_resp_with_remaining(&buf[consumed..]) {
                        Ok((frame, remaining)) => {
                            consumed = buf.len() - remaining.len();
                            replies.push(frame);
                        }
                        Err(crate::redis::ParseError::Incomplete) => break,
                        Err(e) => panic!("unexpected parse error: {e}"),
                    }
                }
                if consumed > 0 {
                    buf.drain(..consumed);
                }
                if replies.len() < count {
                    let n = timeout(Duration::from_secs(5), stream.read(&mut tmp))
                        .await
                        .expect("timed out waiting for reply")
                        .expect("read failed");
                    assert!(n > 0, "connection closed before all replies arrived");
                    buf.extend_from_slice(&tmp[..n]);
                }
            }
            replies
        }

        let mut read_buf = Vec::new();

        // Repro from #26: pipeline SET k "", SET k "x{i}", SET k "" 50 times,
        // then GET k immediately after the last ack.
        let mut payload = Vec::new();
        for i in 0..50 {
            let val = format!("x{i}");
            payload.extend_from_slice(&encode_cmd(&[b"SET", b"k", b""]));
            payload.extend_from_slice(&encode_cmd(&[b"SET", b"k", val.as_bytes()]));
            payload.extend_from_slice(&encode_cmd(&[b"SET", b"k", b""]));
        }
        client_stream.write_all(&payload).await.unwrap();
        let replies = read_replies(&mut client_stream, &mut read_buf, 150).await;
        for reply in replies {
            assert_simple_string(reply, "OK");
        }
        client_stream
            .write_all(&encode_cmd(&[b"GET", b"k"]))
            .await
            .unwrap();
        let reply = read_replies(&mut client_stream, &mut read_buf, 1)
            .await
            .pop()
            .unwrap();
        assert_bulk_string(reply, b"");

        // Same pattern with HSET/HGET on inflight_hcache.
        payload.clear();
        for i in 0..50 {
            let val = format!("x{i}");
            payload.extend_from_slice(&encode_cmd(&[b"HSET", b"ns", b"f", b""]));
            payload.extend_from_slice(&encode_cmd(&[b"HSET", b"ns", b"f", val.as_bytes()]));
            payload.extend_from_slice(&encode_cmd(&[b"HSET", b"ns", b"f", b""]));
        }
        client_stream.write_all(&payload).await.unwrap();
        let replies = read_replies(&mut client_stream, &mut read_buf, 150).await;
        for (idx, reply) in replies.into_iter().enumerate() {
            assert_integer_response(reply, if idx == 0 { 1 } else { 0 });
        }
        client_stream
            .write_all(&encode_cmd(&[b"HGET", b"ns", b"f"]))
            .await
            .unwrap();
        let reply = read_replies(&mut client_stream, &mut read_buf, 1)
            .await
            .pop()
            .unwrap();
        assert_bulk_string(reply, b"");

        // Same pattern with HSETEX/HGET on inflight_hcache.
        payload.clear();
        for i in 0..50 {
            let val = format!("x{i}");
            payload.extend_from_slice(&encode_cmd(&[
                b"HSETEX", b"ns", b"EX", b"60", b"FIELDS", b"1", b"f2", b"",
            ]));
            payload.extend_from_slice(&encode_cmd(&[
                b"HSETEX",
                b"ns",
                b"EX",
                b"60",
                b"FIELDS",
                b"1",
                b"f2",
                val.as_bytes(),
            ]));
            payload.extend_from_slice(&encode_cmd(&[
                b"HSETEX", b"ns", b"EX", b"60", b"FIELDS", b"1", b"f2", b"",
            ]));
        }
        client_stream.write_all(&payload).await.unwrap();
        let replies = read_replies(&mut client_stream, &mut read_buf, 150).await;
        for (idx, reply) in replies.into_iter().enumerate() {
            assert_integer_response(reply, if idx == 0 { 1 } else { 0 });
        }
        client_stream
            .write_all(&encode_cmd(&[b"HGET", b"ns", b"f2"]))
            .await
            .unwrap();
        let reply = read_replies(&mut client_stream, &mut read_buf, 1)
            .await
            .pop()
            .unwrap();
        assert_bulk_string(reply, b"");

        // Intervening DEL must not make the final empty SET read as missing.
        payload.clear();
        for _ in 0..50 {
            payload.extend_from_slice(&encode_cmd(&[b"SET", b"k2", b""]));
            payload.extend_from_slice(&encode_cmd(&[b"DEL", b"k2"]));
            payload.extend_from_slice(&encode_cmd(&[b"SET", b"k2", b""]));
        }
        client_stream.write_all(&payload).await.unwrap();
        let replies = read_replies(&mut client_stream, &mut read_buf, 150).await;
        for chunk in replies.chunks(3) {
            assert_simple_string(chunk[0].clone(), "OK");
            assert_integer_response(chunk[1].clone(), 1);
            assert_simple_string(chunk[2].clone(), "OK");
        }
        payload.clear();
        payload.extend_from_slice(&encode_cmd(&[b"GET", b"k2"]));
        payload.extend_from_slice(&encode_cmd(&[b"EXISTS", b"k2"]));
        client_stream.write_all(&payload).await.unwrap();
        let mut replies = read_replies(&mut client_stream, &mut read_buf, 2)
            .await
            .into_iter();
        assert_bulk_string(replies.next().unwrap(), b"");
        assert_integer_response(replies.next().unwrap(), 1);

        drop(client_stream);
        shutdown.cancel();
        let _ = conn_task.await;
        ctx.shutdown().await;
    }

    async fn hget_frame(ctx: &TestContext, field: &'static str) -> BytesFrame {
        respond_with(ctx, move |state, mut stream| {
            Box::pin(async move {
                handle_hget(&mut stream, &state, "ns".to_string(), field.to_string()).await
            })
        })
        .await
    }

    #[tokio::test]
    async fn application_metrics_count_hget_lookups_by_source() {
        use crate::metrics::rendered_value;
        // Every handle the context creates binds to this recorder.
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let _guard = metrics::set_default_local_recorder(&recorder);
        let mut ctx = TestContext::new_paused(true).await;
        let reads = |output: &str, source: &str, result: &str| {
            rendered_value(
                output,
                &format!(
                    r#"blobasaur_reads_total{{command="hget",source="{source}",result="{result}"}}"#
                ),
            )
        };

        // No namespace table yet: a SQLite miss, not an error.
        assert_null(hget_frame(&ctx, "f").await);
        respond_with(&ctx, |state, mut stream| {
            Box::pin(async move {
                handle_hset(
                    &mut stream,
                    &state,
                    "ns".to_string(),
                    "f".to_string(),
                    Bytes::from_static(b"v"),
                )
                .await
            })
        })
        .await;
        assert_bulk_string(hget_frame(&ctx, "f").await, b"v");

        ctx.state.record_gauges();
        let output = recorder.handle().render();
        assert_eq!(reads(&output, "sqlite", "miss"), 1.0);
        assert_eq!(reads(&output, "inflight", "hit"), 1.0);
        assert_eq!(
            rendered_value(&output, r#"blobasaur_write_queue_depth{shard="0"}"#),
            1.0
        );
        assert_eq!(
            rendered_value(&output, r#"blobasaur_async_writes_pending{shard="0"}"#),
            1.0
        );

        // EXPIRE is synchronous, so it waits for the HSET ahead of it.
        ctx.resume();
        respond_with(&ctx, |state, mut stream| {
            Box::pin(async move { handle_expire(&mut stream, &state, "k".to_string(), 10).await })
        })
        .await;
        assert_bulk_string(hget_frame(&ctx, "f").await, b"v");
        assert_null(hget_frame(&ctx, "gone").await);

        ctx.state.record_gauges();
        let output = recorder.handle().render();
        assert_eq!(reads(&output, "sqlite", "hit"), 1.0);
        assert_eq!(reads(&output, "sqlite", "miss"), 2.0);
        assert_eq!(
            rendered_value(&output, r#"blobasaur_write_queue_depth{shard="0"}"#),
            0.0
        );
        assert_eq!(
            rendered_value(&output, r#"blobasaur_async_writes_pending{shard="0"}"#),
            0.0
        );
        assert_eq!(
            rendered_value(
                &output,
                r#"blobasaur_sqlite_pool_acquire_seconds_count{shard="0",pool="read",result="ok"}"#
            ),
            3.0
        );
        for state in ["idle", "in_use"] {
            rendered_value(
                &output,
                &format!(
                    r#"blobasaur_sqlite_pool_connections{{shard="0",pool="read",state="{state}"}}"#
                ),
            );
        }

        // Sampling and rendering again doesn't move any counter.
        ctx.state.record_gauges();
        let again = recorder.handle().render();
        assert_eq!(reads(&again, "sqlite", "hit"), 1.0);
        assert_eq!(reads(&again, "sqlite", "miss"), 2.0);
        assert_eq!(reads(&again, "inflight", "hit"), 1.0);

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
                (1..=30).contains(&v),
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
        let ns_table = shard_manager::namespace_table("ns");
        sqlx::query(&format!(
            "UPDATE {ns_table} SET expires_at = ? WHERE key = ?"
        ))
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

    // Expired rows that cleanup has not removed yet must count as missing.

    /// Calls a command handler and returns its reply.
    macro_rules! run {
        ($ctx:expr, $handler:ident($($arg:expr),*)) => {
            respond_with(&$ctx, |state, stream| {
                Box::pin(async move {
                    let mut stream = stream;
                    $handler(&mut stream, &state, $($arg),*).await
                })
            })
            .await
        };
    }

    async fn set_expires_at(ctx: &TestContext, table: &str, key: &str, at: i64) {
        let pool = &ctx.state.db_pools[ctx.state.get_shard(key)];
        sqlx::query(&format!("UPDATE {table} SET expires_at = ? WHERE key = ?"))
            .bind(at)
            .bind(key)
            .execute(pool)
            .await
            .unwrap();
    }

    fn past() -> i64 {
        chrono::Utc::now().timestamp() - 60
    }

    fn v(bytes: &'static [u8]) -> Bytes {
        Bytes::from_static(bytes)
    }

    #[tokio::test]
    async fn expire_and_del_treat_expired_key_as_missing() {
        let ctx = TestContext::new(false).await;
        run!(ctx, handle_set("k".into(), v(b"old"), None));
        set_expires_at(&ctx, "blobs", "k", past()).await;

        assert_integer_response(run!(ctx, handle_expire("k".into(), 100)), 0);
        assert_null(run!(ctx, handle_get("k".into())));
        assert_integer_response(run!(ctx, handle_del_multiple(vec!["k".into()])), 0);
        ctx.shutdown().await;
    }

    #[tokio::test]
    async fn hash_writes_treat_expired_field_as_missing() {
        let ctx = TestContext::new(false).await;
        let ns_table = shard_manager::namespace_table("ns");
        for field in ["f", "d", "x", "n"] {
            run!(ctx, handle_hset("ns".into(), field.into(), v(b"v1")));
            set_expires_at(&ctx, &ns_table, field, past()).await;
        }

        // HSET over an expired field adds it and makes it visible.
        assert_integer_response(run!(ctx, handle_hset("ns".into(), "f".into(), v(b"v2"))), 1);
        assert_bulk_string(hget_frame(&ctx, "f").await, b"v2");

        assert_integer_response(run!(ctx, handle_hdel("ns".into(), "d".into())), 0);

        let fxx = vec![("x".to_string(), v(b"v2"))];
        assert_integer_response(
            run!(ctx, handle_hsetex("ns".into(), false, true, None, fxx)),
            0,
        );
        assert_null(hget_frame(&ctx, "x").await);

        let fnx = vec![("n".to_string(), v(b"v2"))];
        assert_integer_response(
            run!(ctx, handle_hsetex("ns".into(), true, false, None, fnx)),
            1,
        );
        assert_bulk_string(hget_frame(&ctx, "n").await, b"v2");
        ctx.shutdown().await;
    }

    #[tokio::test]
    async fn hset_keeps_ttl_of_live_field() {
        let ctx = TestContext::new(false).await;
        let ns_table = shard_manager::namespace_table("ns");
        run!(ctx, handle_hset("ns".into(), "f".into(), v(b"v1")));
        let future = chrono::Utc::now().timestamp() + 1000;
        set_expires_at(&ctx, &ns_table, "f", future).await;

        assert_integer_response(run!(ctx, handle_hset("ns".into(), "f".into(), v(b"v2"))), 0);
        let pool = &ctx.state.db_pools[ctx.state.get_shard("f")];
        let expires_at: Option<i64> = sqlx::query_scalar(&format!(
            "SELECT expires_at FROM {ns_table} WHERE key = 'f'"
        ))
        .fetch_one(pool)
        .await
        .unwrap();
        assert_eq!(expires_at, Some(future));
        ctx.shutdown().await;
    }

    #[tokio::test]
    async fn hexpire_miss_does_not_create_namespace_table() {
        let ctx = TestContext::new(false).await;

        let namespace = "ghost_ns";
        let table_name = shard_manager::namespace_table(namespace);
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
