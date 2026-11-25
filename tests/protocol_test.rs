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
        write_queue_capacity: None,
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

#[tokio::test]
async fn oversized_hash_field_count_is_rejected_and_server_survives() {
    let server = start(None).await;

    for args in [
        &[
            b"HEXPIRE".as_slice(),
            b"k",
            b"10",
            b"FIELDS",
            b"1000000000000000",
            b"f",
        ][..],
        &[b"HEXPIRE", b"k", b"10", b"FIELDS", b"2147483647", b"f"],
        &[
            b"HEXPIREAT",
            b"k",
            b"4102444800",
            b"FIELDS",
            b"1000000000000000",
            b"f",
        ],
        &[
            b"HEXPIREAT",
            b"k",
            b"4102444800",
            b"FIELDS",
            b"2147483647",
            b"f",
        ],
    ] {
        let reply = server.call(args).await.expect("error reply");
        assert!(reply.starts_with("-ERR "), "{reply}");
    }

    assert_eq!(server.call(&[b"PING"]).await.as_deref(), Some("+PONG"));
}

#[tokio::test]
async fn client_name_is_scoped_to_the_connection() {
    let server = start(None).await;
    let mut client = server.connect().await;
    client
        .send(&request(&[b"CLIENT", b"SETNAME", b"worker"]))
        .await;
    assert_eq!(client.line().await.as_deref(), Some("+OK"));
    client.send(&request(&[b"CLIENT", b"GETNAME"])).await;
    assert_eq!(client.bulk().await, b"worker");
    assert_eq!(
        server.call(&[b"CLIENT", b"GETNAME"]).await.as_deref(),
        Some("$-1")
    );
    client.send(&request(&[b"CLIENT", b"SETNAME", b""])).await;
    assert_eq!(client.line().await.as_deref(), Some("+OK"));
    client.send(&request(&[b"CLIENT", b"GETNAME"])).await;
    assert_eq!(client.line().await.as_deref(), Some("$-1"));
}

#[tokio::test]
async fn command_count_and_ping_check_their_arguments() {
    let server = start(None).await;
    assert_eq!(
        server.call(&[b"COMMAND", b"COUNT"]).await.as_deref(),
        Some(":21")
    );
    for args in [
        [b"COMMAND".as_slice(), b"COUNT", b"extra"],
        [b"PING", b"a", b"b"],
    ] {
        assert!(
            server
                .call(&args)
                .await
                .unwrap()
                .starts_with("-ERR wrong number of arguments")
        );
    }
}

#[tokio::test]
async fn hsetex_negative_absolute_expiry_preserves_field() {
    let server = start(None).await;
    let mut client = server.connect().await;
    client.send(&request(&[b"HSET", b"h", b"f", b"old"])).await;
    assert_eq!(client.line().await.as_deref(), Some(":1"));

    for option in [b"EXAT".as_slice(), b"PXAT"] {
        let commands: [&[&[u8]]; 2] = [
            &[
                b"HSETEX", b"h", option, b"-1", b"FIELDS", b"1", b"f", b"new",
            ],
            &[
                b"HSETEX", b"h", b"FIELDS", b"1", b"f", b"new", option, b"-1",
            ],
        ];
        for command in commands {
            client.send(&request(command)).await;
            assert_eq!(
                client.line().await.as_deref(),
                Some("-ERR invalid expire time, must be >= 0")
            );
            client.send(&request(&[b"HGET", b"h", b"f"])).await;
            assert_eq!(client.bulk().await, b"old");
        }
    }
}

#[tokio::test]
async fn hash_expiry_accepts_repeated_identical_conditions() {
    let server = start(None).await;
    let mut client = server.connect().await;
    client.send(&request(&[b"HSET", b"h", b"f", b"v"])).await;
    assert_eq!(client.line().await.as_deref(), Some(":1"));

    for (command, time, flag, expected) in [
        (b"HEXPIRE".as_slice(), b"60".as_slice(), b"NX", ":1"),
        (b"HEXPIREAT", b"0", b"XX", ":2"),
    ] {
        client
            .send(&request(&[
                command, b"h", time, flag, b"FIELDS", b"1", b"f", flag,
            ]))
            .await;
        assert_eq!(client.line().await.as_deref(), Some("*1"));
        assert_eq!(client.line().await.as_deref(), Some(expected));
    }
    assert_eq!(
        server.call(&[b"HEXISTS", b"h", b"f"]).await.as_deref(),
        Some(":0")
    );
}

#[tokio::test]
async fn hsetex_fnx_and_fxx_write_all_fields_or_none() {
    let server = start(None).await;
    assert_eq!(
        server
            .call(&[b"HSET", b"ns", b"old", b"v"])
            .await
            .as_deref(),
        Some(":1")
    );

    // FNX with one existing field must not write the new one listed before it.
    let fnx = [
        &b"HSETEX"[..],
        b"ns",
        b"FNX",
        b"FIELDS",
        b"2",
        b"new",
        b"v",
        b"old",
        b"v2",
    ];
    assert_eq!(server.call(&fnx).await.as_deref(), Some(":0"));
    assert_eq!(
        server.call(&[b"HEXISTS", b"ns", b"new"]).await.as_deref(),
        Some(":0")
    );

    // FXX with one missing field must not overwrite the existing one before it.
    let fxx = [
        &b"HSETEX"[..],
        b"ns",
        b"FXX",
        b"FIELDS",
        b"2",
        b"old",
        b"v2",
        b"absent",
        b"v",
    ];
    assert_eq!(server.call(&fxx).await.as_deref(), Some(":0"));
    let mut client = server.connect().await;
    client.send(&request(&[b"HGET", b"ns", b"old"])).await;
    assert_eq!(client.bulk().await, b"v");
}

// Redis replies 1 when HSETEX sets every field and 0 when it sets none; it
// doesn't count new fields like HSET does. FXX only ever updates existing
// fields, so a count would make a successful FXX write look like a failed one.
#[tokio::test]
async fn hsetex_replies_1_when_all_fields_are_set() {
    let server = start(None).await;
    assert_eq!(
        server
            .call(&[b"HSET", b"ns", b"old", b"v"])
            .await
            .as_deref(),
        Some(":1")
    );

    let fxx = [
        &b"HSETEX"[..],
        b"ns",
        b"FXX",
        b"EX",
        b"3600",
        b"FIELDS",
        b"1",
        b"old",
        b"v2",
    ];
    assert_eq!(server.call(&fxx).await.as_deref(), Some(":1"));
    let mut client = server.connect().await;
    client.send(&request(&[b"HGET", b"ns", b"old"])).await;
    assert_eq!(client.bulk().await, b"v2");

    // Several new fields still reply 1, not the number added.
    let new_fields = [
        &b"HSETEX"[..],
        b"ns",
        b"FIELDS",
        b"2",
        b"a",
        b"1",
        b"b",
        b"2",
    ];
    assert_eq!(server.call(&new_fields).await.as_deref(), Some(":1"));
}

#[tokio::test]
async fn hsetex_repeated_field_last_value_wins() {
    let server = start(None).await;
    let dup = [
        &b"HSETEX"[..],
        b"ns",
        b"FNX",
        b"FIELDS",
        b"2",
        b"f",
        b"a",
        b"f",
        b"b",
    ];
    assert_eq!(server.call(&dup).await.as_deref(), Some(":1"));
    let mut client = server.connect().await;
    client.send(&request(&[b"HGET", b"ns", b"f"])).await;
    assert_eq!(client.bulk().await, b"b");
}

// Redis treats a negative EXPIRE like EXPIRE 0: the key is deleted.
#[tokio::test]
async fn expire_with_a_negative_ttl_deletes_the_key() {
    let server = start(None).await;
    assert_eq!(
        server.call(&[b"SET", b"k", b"v"]).await.as_deref(),
        Some("+OK")
    );
    assert_eq!(
        server.call(&[b"EXPIRE", b"k", b"-5"]).await.as_deref(),
        Some(":1")
    );
    assert_eq!(server.call(&[b"EXISTS", b"k"]).await.as_deref(), Some(":0"));
    assert_eq!(
        server.call(&[b"EXPIRE", b"k", b"-5"]).await.as_deref(),
        Some(":0")
    );
}

fn unix_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

/// Sleeps until the wall clock's sub-second part is `millis`: later this
/// second, or in the next one if this second is already past it.
async fn sleep_until_subsec(millis: u64) {
    let subsec = unix_millis() % 1000;
    let wait = if subsec <= millis {
        millis - subsec
    } else {
        1000 + millis - subsec
    };
    tokio::time::sleep(std::time::Duration::from_millis(wait)).await;
}

// Expiry is stored in whole seconds. A key written late in a second must still
// live for its whole TTL, not just until the next whole-second boundary.
#[tokio::test]
async fn ttl_is_not_cut_short_by_whole_second_storage() {
    let server = start(None).await;
    // Create the namespace's table up front, so HSETEX below is a plain write.
    assert_eq!(
        server
            .call(&[b"HSET", b"ns", b"other", b"v"])
            .await
            .as_deref(),
        Some(":1")
    );

    // Both writes must land late in the same second; retry with fresh keys if a
    // slow run pushes them past the boundary.
    const MAX_ATTEMPTS: usize = 10;
    let mut landed = None;
    for attempt in 0..MAX_ATTEMPTS {
        sleep_until_subsec(900).await;
        let start = unix_millis();
        if start % 1000 < 900 {
            continue;
        }
        let second = start / 1000;
        let key = format!("k{attempt}");
        let field = format!("f{attempt}");
        assert_eq!(
            server
                .call(&[b"SET", key.as_bytes(), b"v", b"EX", b"1"])
                .await
                .as_deref(),
            Some("+OK")
        );
        assert_eq!(
            server
                .call(&[
                    b"HSETEX",
                    b"ns",
                    b"EX",
                    b"1",
                    b"FIELDS",
                    b"1",
                    field.as_bytes(),
                    b"v",
                ])
                .await
                .as_deref(),
            Some(":1")
        );
        if unix_millis() / 1000 == second {
            landed = Some((second, key, field));
            break;
        }
    }
    let (second, key, field) = landed.expect("writes never landed within the late-second window");

    // Just past the next whole second, well under the 1s asked for.
    let wait = (second * 1000 + 1150).saturating_sub(unix_millis());
    tokio::time::sleep(std::time::Duration::from_millis(wait)).await;
    assert_eq!(
        server.call(&[b"EXISTS", key.as_bytes()]).await.as_deref(),
        Some(":1")
    );
    assert_eq!(
        server
            .call(&[b"HEXISTS", b"ns", field.as_bytes()])
            .await
            .as_deref(),
        Some(":1")
    );
}

#[tokio::test]
async fn ttl_reports_the_ttl_that_was_set() {
    let server = start(None).await;
    for ex in [&b"100"[..], b"1"] {
        // Partway into a second, so the stored expiry has to round up, with
        // ~900ms left before TTL could cross into the next one.
        sleep_until_subsec(100).await;
        server.call(&[b"SET", b"k", b"v", b"EX", ex]).await;
        let expected = format!(":{}", std::str::from_utf8(ex).unwrap());
        assert_eq!(
            server.call(&[b"TTL", b"k"]).await.as_deref(),
            Some(expected.as_str())
        );
    }
}
