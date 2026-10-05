//! Graceful shutdown: every write acknowledged with OK must be on disk once
//! `server::run` returns after its shutdown token is cancelled (the same path
//! the SIGINT/SIGTERM handler takes).

use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use blobasaur::config::{Cfg, SqliteConfig};
use blobasaur::server;
use sqlx::{Connection, SqliteConnection};
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

const NUM_SHARDS: usize = 2;

struct Server {
    addr: SocketAddr,
    shutdown: CancellationToken,
    handle: JoinHandle<miette::Result<()>>,
    dir: TempDir,
}

async fn start(async_write: bool, batch_size: usize, batch_timeout_ms: u64) -> Server {
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();
    let dir = TempDir::new().unwrap();
    let cfg = Cfg {
        data_dir: dir.path().to_string_lossy().into_owned(),
        num_shards: NUM_SHARDS,
        storage_compression: None,
        async_write: Some(async_write),
        batch_size: Some(batch_size),
        batch_timeout_ms: Some(batch_timeout_ms),
        addr: None,
        cluster: None,
        metrics: None,
        sqlite: Some(SqliteConfig {
            cache_size_mb: None,
            // Long enough for writers to wait out the lock the first test holds.
            busy_timeout_ms: Some(30_000),
            synchronous: None,
            mmap_size: None,
            max_connections: None,
            auto_upgrade_legacy_auto_vacuum: None,
            auto_upgrade_legacy_auto_vacuum_concurrency: None,
        }),
        shutdown_timeout_secs: Some(20),
        max_request_size_mb: None,
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let shutdown = CancellationToken::new();
    let handle = tokio::spawn(server::run(cfg, listener, shutdown.clone()));

    // Wait until AppState is initialised and the server answers.
    let mut client = Client::connect(addr).await;
    assert_eq!(client.call(&["PING"]).await.as_deref(), Some("+PONG"));

    Server {
        addr,
        shutdown,
        handle,
        dir,
    }
}

impl Server {
    async fn stop(self) -> TempDir {
        self.shutdown.cancel();
        timeout(Duration::from_secs(30), self.handle)
            .await
            .expect("shutdown hung")
            .unwrap()
            .expect("shutdown reported an error");
        self.dir
    }
}

fn shard_url(dir: &Path, shard: usize) -> String {
    format!("sqlite:{}/shard_{}.db", dir.display(), shard)
}

/// All keys in `table` across shards (missing tables count as empty).
async fn keys(dir: &Path, table: &str) -> HashSet<String> {
    let mut keys = HashSet::new();
    for shard in 0..NUM_SHARDS {
        let mut conn = SqliteConnection::connect(&shard_url(dir, shard))
            .await
            .unwrap();
        let rows: Vec<(String,)> = sqlx::query_as(&format!("SELECT key FROM {table}"))
            .fetch_all(&mut conn)
            .await
            .or_else(|e| match e {
                sqlx::Error::Database(db) if db.message().contains("no such table") => {
                    Ok(Vec::new())
                }
                e => Err(e),
            })
            .unwrap();
        keys.extend(rows.into_iter().map(|(k,)| k));
    }
    keys
}

struct Client(BufReader<TcpStream>);

impl Client {
    async fn connect(addr: SocketAddr) -> Self {
        Client(BufReader::new(TcpStream::connect(addr).await.unwrap()))
    }

    async fn send(&mut self, args: &[&str]) -> Option<()> {
        let mut req = format!("*{}\r\n", args.len());
        for arg in args {
            req.push_str(&format!("${}\r\n{}\r\n", arg.len(), arg));
        }
        self.0.get_mut().write_all(req.as_bytes()).await.ok()
    }

    /// Reads a single-line reply, or None if the connection was closed.
    async fn reply(&mut self) -> Option<String> {
        let mut line = String::new();
        match self.0.read_line(&mut line).await {
            Ok(n) if n > 0 => Some(line.trim_end().to_string()),
            _ => None,
        }
    }

    async fn call(&mut self, args: &[&str]) -> Option<String> {
        self.send(args).await?;
        self.reply().await
    }
}

/// Wedges every shard writer so acknowledged async writes pile up in the shard
/// queues: hold the SQLite write lock, then send each writer a full VACUUM,
/// which the writer runs inline and which waits on the lock (busy_timeout).
/// Returns the lock connections; roll them back to unblock the writers.
async fn wedge_writers(server: &Server) -> Vec<SqliteConnection> {
    // Let the startup expiry-cleanup pass finish first; otherwise it blocks on
    // the lock while holding the only warm read-pool connection.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let mut locks = Vec::new();
    for shard in 0..NUM_SHARDS {
        let mut conn = SqliteConnection::connect(&shard_url(server.dir.path(), shard))
            .await
            .unwrap();
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut conn)
            .await
            .unwrap();
        locks.push(conn);

        // Fire and forget: the reply only arrives once the lock is released.
        let mut client = Client::connect(server.addr).await;
        let shard = shard.to_string();
        tokio::spawn(async move {
            let args = [
                "BLOBASAUR.VACUUM",
                "SHARD",
                &shard,
                "MODE",
                "full",
                "BUDGET_MB",
                "1",
            ];
            client.call(&args).await
        });
    }
    // Let the vacuums reach the writers before queueing writes behind them.
    tokio::time::sleep(Duration::from_millis(200)).await;
    locks
}

#[tokio::test]
async fn shutdown_flushes_queued_async_sets() {
    const N: usize = 200;
    // Huge batch_timeout_ms: shutdown must not wait for it.
    let server = start(true, 1000, 600_000).await;
    let mut locks = wedge_writers(&server).await;

    let mut client = Client::connect(server.addr).await;
    for i in 0..N {
        let (key, value) = (format!("key-{i}"), format!("value-{i}"));
        assert_eq!(
            client.call(&["SET", &key, &value]).await.as_deref(),
            Some("+OK")
        );
    }
    assert!(
        keys(server.dir.path(), "blobs").await.is_empty(),
        "writes should still be queued"
    );

    server.shutdown.cancel();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !server.handle.is_finished(),
        "run returned before queued writes were committed"
    );
    for lock in &mut locks {
        sqlx::query("ROLLBACK").execute(&mut *lock).await.unwrap();
    }
    drop(locks);

    let dir = server.stop().await;
    let expected: HashSet<String> = (0..N).map(|i| format!("key-{i}")).collect();
    assert_eq!(keys(dir.path(), "blobs").await, expected);
}

#[tokio::test]
async fn shutdown_flushes_queued_async_hsets() {
    const N: usize = 200;
    // Async HSET checks field existence on the shard's single writer
    // connection, so it can't be acknowledged while the writer is wedged.
    // Pipeline a burst instead and shut down right after the last reply.
    let server = start(true, 1000, 600_000).await;
    let mut client = Client::connect(server.addr).await;
    for i in 0..N {
        let (key, value) = (format!("key-{i}"), format!("value-{i}"));
        client.send(&["HSET", "ns", &key, &value]).await.unwrap();
    }
    for _ in 0..N {
        assert_eq!(client.reply().await.as_deref(), Some(":1"));
    }

    let dir = server.stop().await;
    let expected: HashSet<String> = (0..N).map(|i| format!("key-{i}")).collect();
    assert_eq!(keys(dir.path(), "blobs_ns").await, expected);
}

/// Clients keep writing while shutdown starts. Each write must be either
/// rejected (error reply or closed connection) or persisted.
async fn assert_writes_during_shutdown_not_lost(async_write: bool) {
    let server = start(async_write, 16, 5).await;
    let acked = Arc::new(AtomicUsize::new(0));

    let mut clients = Vec::new();
    for c in 0..4 {
        let addr = server.addr;
        let acked = acked.clone();
        clients.push(tokio::spawn(async move {
            let mut client = Client::connect(addr).await;
            let mut ok_keys = Vec::new();
            for i in 0.. {
                let key = format!("c{c}-{i}");
                match client.call(&["SET", &key, "v"]).await.as_deref() {
                    Some("+OK") => {
                        ok_keys.push(key);
                        acked.fetch_add(1, Ordering::Relaxed);
                    }
                    _ => break,
                }
            }
            ok_keys
        }));
    }

    while acked.load(Ordering::Relaxed) < 200 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let addr = server.addr;
    let dir = server.stop().await;
    assert!(
        TcpStream::connect(addr).await.is_err(),
        "listener should be closed after shutdown"
    );

    let mut ok_keys = HashSet::new();
    for client in clients {
        ok_keys.extend(
            timeout(Duration::from_secs(5), client)
                .await
                .unwrap()
                .unwrap(),
        );
    }

    let persisted = keys(dir.path(), "blobs").await;
    let lost: Vec<_> = ok_keys.difference(&persisted).collect();
    assert!(lost.is_empty(), "acknowledged but lost: {lost:?}");
}

#[tokio::test]
async fn writes_during_async_shutdown_are_rejected_or_persisted() {
    assert_writes_during_shutdown_not_lost(true).await;
}

#[tokio::test]
async fn writes_during_sync_shutdown_are_rejected_or_persisted() {
    assert_writes_during_shutdown_not_lost(false).await;
}
