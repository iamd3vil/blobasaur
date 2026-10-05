//! End-to-end checks of client request framing against a running server.

use std::net::SocketAddr;

use blobasaur::config::Cfg;
use blobasaur::server;
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;

struct Server {
    addr: SocketAddr,
    shutdown: CancellationToken,
    _dir: TempDir,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

async fn start(max_request_size_mb: Option<u64>) -> Server {
    let dir = TempDir::new().unwrap();
    let cfg = Cfg {
        data_dir: dir.path().to_string_lossy().into_owned(),
        num_shards: 1,
        storage_compression: None,
        async_write: Some(false),
        batch_size: None,
        batch_timeout_ms: None,
        addr: None,
        cluster: None,
        metrics: None,
        sqlite: None,
        shutdown_timeout_secs: None,
        max_request_size_mb,
        cleanup_chunk_size: None,
        cleanup_interval_secs: None,
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let shutdown = CancellationToken::new();
    tokio::spawn(server::run(cfg, listener, shutdown.clone()));

    let server = Server {
        addr,
        shutdown,
        _dir: dir,
    };
    let mut client = server.connect().await;
    client.send(&request(&[b"PING"])).await;
    assert_eq!(client.line().await.as_deref(), Some("+PONG"));
    server
}

impl Server {
    async fn connect(&self) -> Client {
        Client(BufReader::new(TcpStream::connect(self.addr).await.unwrap()))
    }

    async fn call(&self, parts: &[&[u8]]) -> Option<String> {
        let mut client = self.connect().await;
        client.send(&request(parts)).await;
        client.line().await
    }
}

fn request(parts: &[&[u8]]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", parts.len()).into_bytes();
    for part in parts {
        out.extend_from_slice(format!("${}\r\n", part.len()).as_bytes());
        out.extend_from_slice(part);
        out.extend_from_slice(b"\r\n");
    }
    out
}

struct Client(BufReader<TcpStream>);

impl Client {
    async fn send(&mut self, bytes: &[u8]) {
        // The server may close mid-write once it has seen enough to reject.
        let _ = self.0.get_mut().write_all(bytes).await;
    }

    /// Next reply line, or None once the server has closed the connection.
    async fn line(&mut self) -> Option<String> {
        let mut line = String::new();
        match self.0.read_line(&mut line).await {
            Ok(n) if n > 0 => Some(line.trim_end().to_string()),
            _ => None,
        }
    }

    async fn bulk(&mut self) -> Vec<u8> {
        let header = self.line().await.expect("bulk header");
        let len: usize = header.strip_prefix('$').unwrap().parse().unwrap();
        let mut data = vec![0; len + 2];
        self.0.read_exact(&mut data).await.unwrap();
        data.truncate(len);
        data
    }

    async fn assert_protocol_error_then_closed(&mut self) {
        let reply = self.line().await.expect("error reply");
        assert!(reply.starts_with("-ERR Protocol error"), "{reply}");
        assert_eq!(self.line().await, None, "connection should be closed");
    }
}

#[tokio::test]
async fn deeply_nested_request_is_rejected_and_server_survives() {
    let server = start(None).await;
    let mut client = server.connect().await;
    let mut input = b"*1\r\n".repeat(100_000);
    input.extend_from_slice(&request(&[b"PING"]));
    client.send(&input).await;
    client.assert_protocol_error_then_closed().await;

    assert_eq!(server.call(&[b"PING"]).await.as_deref(), Some("+PONG"));
}

#[tokio::test]
async fn misframed_request_closes_connection_without_running_trailing_bytes() {
    let server = start(None).await;
    assert_eq!(
        server.call(&[b"SET", b"victim", b"v"]).await.as_deref(),
        Some("+OK")
    );

    let mut client = server.connect().await;
    let mut input = b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$2\r\nXYab".to_vec();
    input.extend_from_slice(&request(&[b"DEL", b"victim"]));
    client.send(&input).await;
    client.assert_protocol_error_then_closed().await;

    assert_eq!(
        server.call(&[b"EXISTS", b"victim"]).await.as_deref(),
        Some(":1")
    );
}

#[tokio::test]
async fn large_values_round_trip_under_the_default_limit() {
    let server = start(None).await;
    let value: Vec<u8> = (0..50 * 1024 * 1024).map(|i| (i % 251) as u8).collect();

    let mut client = server.connect().await;
    let mut pipeline = request(&[b"SET", b"big", &value]);
    pipeline.extend_from_slice(&request(&[b"GET", b"big"]));
    client.send(&pipeline).await;
    assert_eq!(client.line().await.as_deref(), Some("+OK"));
    assert!(client.bulk().await == value, "GET returned different bytes");
}

#[tokio::test]
async fn request_over_the_limit_is_rejected_and_server_survives() {
    let server = start(Some(1)).await;
    let mut client = server.connect().await;
    client
        .send(&request(&[b"SET", b"big", &vec![b'x'; 2 * 1024 * 1024]]))
        .await;
    let reply = client.line().await.expect("error reply");
    assert!(reply.contains("max_request_size_mb"), "{reply}");
    assert_eq!(client.line().await, None, "connection should be closed");

    assert_eq!(
        server.call(&[b"SET", b"small", b"v"]).await.as_deref(),
        Some("+OK")
    );
}

#[tokio::test]
async fn quit_replies_ok_and_closes_without_running_the_rest() {
    let server = start(None).await;
    let mut client = server.connect().await;
    let mut pipeline = request(&[b"QUIT"]);
    pipeline.extend_from_slice(&request(&[b"SET", b"after-quit", b"v"]));
    client.send(&pipeline).await;
    assert_eq!(client.line().await.as_deref(), Some("+OK"));
    assert_eq!(client.line().await, None, "connection should be closed");

    assert_eq!(
        server.call(&[b"EXISTS", b"after-quit"]).await.as_deref(),
        Some(":0")
    );
}
