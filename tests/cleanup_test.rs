//! Background expiry cleanup: with a short `cleanup_interval_secs` and a small
//! `cleanup_chunk_size`, expired rows are deleted from disk (in several chunks)
//! in both `blobs` and namespaced tables, while live rows stay.

use std::collections::HashSet;
use std::path::Path;
use std::time::{Duration, Instant};

use blobasaur::config::Cfg;
use blobasaur::server;
use sqlx::{Connection, SqliteConnection};
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;

const NUM_SHARDS: usize = 2;
const EXPIRED: usize = 60;

/// Sends one command and returns its single-line reply.
async fn call(conn: &mut BufReader<TcpStream>, args: &[&str]) -> String {
    let mut req = format!("*{}\r\n", args.len());
    for arg in args {
        req.push_str(&format!("${}\r\n{}\r\n", arg.len(), arg));
    }
    conn.get_mut().write_all(req.as_bytes()).await.unwrap();
    let mut line = String::new();
    conn.read_line(&mut line).await.unwrap();
    line.trim_end().to_string()
}

async fn connect(dir: &Path, shard: usize) -> SqliteConnection {
    let url = format!("sqlite:{}/shard_{}.db", dir.display(), shard);
    SqliteConnection::connect(&url).await.unwrap()
}

/// A namespaced table may not exist on every shard; any other error is real.
fn missing_table_ok<T: Default>(result: Result<T, sqlx::Error>, table: &str) -> T {
    match result {
        Err(sqlx::Error::Database(e))
            if table != "blobs" && e.message().contains("no such table") =>
        {
            T::default()
        }
        r => r.unwrap_or_else(|e| panic!("query on {table} failed: {e}")),
    }
}

/// All keys stored in `table` across shards, expired or not.
async fn keys_on_disk(dir: &Path, table: &str) -> HashSet<String> {
    let mut keys = HashSet::new();
    for shard in 0..NUM_SHARDS {
        let mut conn = connect(dir, shard).await;
        let rows: Vec<(String,)> = missing_table_ok(
            sqlx::query_as(&format!("SELECT key FROM {table}"))
                .fetch_all(&mut conn)
                .await,
            table,
        );
        keys.extend(rows.into_iter().map(|(k,)| k));
    }
    keys
}

/// Marks every row in `table` whose key starts with `prefix` as long expired.
async fn expire_on_disk(dir: &Path, table: &str, prefix: &str) {
    for shard in 0..NUM_SHARDS {
        let mut conn = connect(dir, shard).await;
        missing_table_ok(
            sqlx::query(&format!(
                "UPDATE {table} SET expires_at = 1 WHERE key LIKE ?"
            ))
            .bind(format!("{prefix}%"))
            .execute(&mut conn)
            .await
            .map(|_| ()),
            table,
        );
    }
}

#[tokio::test]
async fn cleanup_deletes_expired_rows_in_chunks_and_keeps_live_ones() {
    let dir = TempDir::new().unwrap();
    let cfg = Cfg {
        data_dir: dir.path().to_string_lossy().into_owned(),
        num_shards: NUM_SHARDS,
        storage_compression: None,
        async_write: Some(false),
        batch_size: None,
        batch_timeout_ms: None,
        addr: None,
        cluster: None,
        metrics: None,
        sqlite: None,
        shutdown_timeout_secs: None,
        max_request_size_mb: None,
        // 60 expired keys per table, 2 per chunk: ~15 chunks per shard per sweep,
        // so cleanup within the deadline needs the chunk loop, not repeated sweeps
        cleanup_chunk_size: Some(2),
        cleanup_interval_secs: Some(1),
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let shutdown = CancellationToken::new();
    let handle = tokio::spawn(server::run(cfg, listener, shutdown.clone()));

    let mut conn = BufReader::new(TcpStream::connect(addr).await.unwrap());
    // Rows to expire are written with a long TTL, so none can expire (and be
    // cleaned up) before the on-disk check; expire_on_disk expires them after.
    for i in 0..EXPIRED {
        let key = format!("tmp{i}");
        assert_eq!(
            call(&mut conn, &["SET", &key, "v", "EX", "3600"]).await,
            "+OK"
        );
    }
    let count = EXPIRED.to_string();
    let fields: Vec<String> = (0..EXPIRED).map(|i| format!("f{i}")).collect();
    let mut expiring_fields = vec!["HSETEX", "sessions", "EX", "3600", "FIELDS", &count];
    for field in &fields {
        expiring_fields.extend([field.as_str(), "v"]);
    }
    assert_eq!(
        call(&mut conn, &expiring_fields).await,
        format!(":{EXPIRED}")
    );
    assert_eq!(call(&mut conn, &["SET", "live", "v"]).await, "+OK");
    assert_eq!(
        call(&mut conn, &["SET", "later", "v", "EX", "3600"]).await,
        "+OK"
    );
    assert_eq!(
        call(&mut conn, &["HSET", "sessions", "keep", "v"]).await,
        ":1"
    );

    // Sync writes are on disk once acknowledged.
    assert_eq!(keys_on_disk(dir.path(), "blobs").await.len(), EXPIRED + 2);
    assert_eq!(
        keys_on_disk(dir.path(), "blobs_sessions").await.len(),
        EXPIRED + 1
    );

    expire_on_disk(dir.path(), "blobs", "tmp").await;
    expire_on_disk(dir.path(), "blobs_sessions", "f").await;

    // The next 1s sweep must delete every expired row.
    let deadline = Instant::now() + Duration::from_secs(10);
    let live = HashSet::from(["live".to_string(), "later".to_string()]);
    let kept = HashSet::from(["keep".to_string()]);
    loop {
        let blobs = keys_on_disk(dir.path(), "blobs").await;
        let hash = keys_on_disk(dir.path(), "blobs_sessions").await;
        if blobs == live && hash == kept {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "expired rows not cleaned up: blobs={blobs:?} blobs_sessions={hash:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    shutdown.cancel();
    handle.await.unwrap().unwrap();
}
