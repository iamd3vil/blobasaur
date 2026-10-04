//! Runs the go-redis client suite in `tests/goredis` against a live server,
//! once with sync writes and once with async writes.
//!
//! Needs Go (see `tests/goredis/go.mod` for the version). The first run
//! downloads the pinned go-redis module. Set `BLOBASAUR_SKIP_GO_TESTS=1` to
//! skip on machines without Go.

use blobasaur::config::Cfg;
use blobasaur::server;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::process::Command;
use tokio_util::sync::CancellationToken;

async fn run_go_suite(async_write: bool) {
    if std::env::var_os("BLOBASAUR_SKIP_GO_TESTS").is_some() {
        eprintln!("BLOBASAUR_SKIP_GO_TESTS is set; skipping go-redis client tests");
        return;
    }

    let dir = TempDir::new().unwrap();
    let cfg = Cfg {
        data_dir: dir.path().to_string_lossy().into_owned(),
        num_shards: 4,
        storage_compression: None,
        async_write: Some(async_write),
        batch_size: Some(50),
        batch_timeout_ms: Some(5),
        addr: None,
        cluster: None,
        metrics: None,
        sqlite: None,
        shutdown_timeout_secs: None,
        max_request_size_mb: None,
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let shutdown = CancellationToken::new();
    let server = tokio::spawn(server::run(cfg, listener, shutdown.clone()));

    let output = Command::new("go")
        .args(["test", "-count=1", "./..."])
        .current_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/goredis"))
        .env("BLOBASAUR_ADDR", addr.to_string())
        .env("BLOBASAUR_ASYNC_WRITE", if async_write { "1" } else { "0" })
        .output()
        .await
        .unwrap_or_else(|e| {
            panic!(
                "failed to run `go test` ({e}). Install Go (version in tests/goredis/go.mod) \
                 or set BLOBASAUR_SKIP_GO_TESTS=1 to skip these tests"
            )
        });

    shutdown.cancel();
    server
        .await
        .unwrap()
        .expect("server shut down with an error");

    assert!(
        output.status.success(),
        "go-redis suite failed (async_write={async_write}):\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn go_redis_client_sync_writes() {
    run_go_suite(false).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn go_redis_client_async_writes() {
    run_go_suite(true).await;
}
