//! End-to-end tests for cluster slot routing (local, MOVED, and CLUSTERDOWN).

use std::net::{SocketAddr, UdpSocket};
use std::time::Duration;

use blobasaur::cluster::{CLUSTERDOWN_HASH_SLOT_NOT_SERVED, ClusterManager};
use blobasaur::config::{Cfg, ClusterConfig, SlotRange};
use blobasaur::server;
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;

struct ClusterServer {
    addr: SocketAddr,
    gossip_port: u16,
    shutdown: CancellationToken,
    _dir: TempDir,
}

impl Drop for ClusterServer {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

fn free_udp_port() -> u16 {
    UdpSocket::bind("127.0.0.1:0")
        .expect("bind ephemeral UDP socket")
        .local_addr()
        .expect("read UDP local addr")
        .port()
}

async fn start_cluster_node(
    node_id: &str,
    slot_range: SlotRange,
    gossip_port: u16,
    seeds: Vec<String>,
) -> ClusterServer {
    let dir = TempDir::new().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let cfg = Cfg {
        data_dir: dir.path().to_string_lossy().into_owned(),
        num_shards: 1,
        storage_compression: None,
        async_write: Some(false),
        batch_size: None,
        batch_timeout_ms: None,
        write_queue_capacity: None,
        addr: Some(addr.to_string()),
        cluster: Some(ClusterConfig {
            enabled: true,
            node_id: node_id.to_string(),
            seeds,
            port: gossip_port,
            slots: None,
            slot_ranges: Some(vec![slot_range]),
            gossip_interval_ms: Some(100),
            advertise_addr: Some(addr.to_string()),
        }),
        metrics: None,
        sqlite: None,
        shutdown_timeout_secs: None,
        max_request_size_mb: None,
        cleanup_chunk_size: None,
        cleanup_interval_secs: None,
        new_replid: false,
        replication: None,
    };

    let shutdown = CancellationToken::new();
    tokio::spawn(server::run(cfg, listener, shutdown.clone()));

    let server = ClusterServer {
        addr,
        gossip_port,
        shutdown,
        _dir: dir,
    };
    let mut client = server.connect().await;
    client.send(&request(&[b"PING"])).await;
    assert_eq!(client.line().await.as_deref(), Some("+PONG"));
    server
}

impl ClusterServer {
    async fn connect(&self) -> Client {
        Client(BufReader::new(TcpStream::connect(self.addr).await.unwrap()))
    }

    async fn call_line(&self, parts: &[&[u8]]) -> Option<String> {
        let mut client = self.connect().await;
        client.send(&request(parts)).await;
        client.line().await
    }

    async fn call_bulk(&self, parts: &[&[u8]]) -> Option<Vec<u8>> {
        let mut client = self.connect().await;
        client.send(&request(parts)).await;
        client.bulk_opt().await
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
        self.0.get_mut().write_all(bytes).await.unwrap();
    }

    async fn line(&mut self) -> Option<String> {
        let mut line = String::new();
        match self.0.read_line(&mut line).await {
            Ok(n) if n > 0 => Some(line.trim_end().to_string()),
            _ => None,
        }
    }

    async fn bulk_opt(&mut self) -> Option<Vec<u8>> {
        let header = self.line().await.expect("bulk header");
        if header == "$-1" {
            return None;
        }
        let len: usize = header.strip_prefix('$').unwrap().parse().unwrap();
        let mut data = vec![0; len + 2];
        self.0.read_exact(&mut data).await.unwrap();
        data.truncate(len);
        Some(data)
    }
}

#[tokio::test]
async fn unserved_slots_return_clusterdown_for_all_key_and_hash_commands() {
    let expected_err = format!("-{CLUSTERDOWN_HASH_SLOT_NOT_SERVED}");
    let node = start_cluster_node(
        "node-a",
        SlotRange {
            start: 0,
            end: 8191,
        },
        free_udp_port(),
        vec![],
    )
    .await;

    // "bar" -> slot 5061 (local); "layout:user42" -> slot 9706 (unserved)
    assert_eq!(ClusterManager::calculate_slot("bar"), 5061);
    assert_eq!(ClusterManager::calculate_slot("layout:user42"), 9706);

    // Local slot works normally.
    assert_eq!(
        node.call_line(&[b"SET", b"bar", b"v1"]).await.as_deref(),
        Some("+OK")
    );
    assert_eq!(
        node.call_bulk(&[b"GET", b"bar"]).await.as_deref(),
        Some(&b"v1"[..])
    );

    // Every keyed and hash command targeting an unserved slot must fail with CLUSTERDOWN.
    let commands: Vec<Vec<&[u8]>> = vec![
        vec![b"SET", b"layout:user42", b"v2"],
        vec![b"GET", b"layout:user42"],
        vec![b"EXISTS", b"layout:user42"],
        vec![b"DEL", b"layout:user42"],
        vec![b"TTL", b"layout:user42"],
        vec![b"EXPIRE", b"layout:user42", b"60"],
        vec![b"HSET", b"ns", b"layout:user42", b"v2"],
        vec![b"HGET", b"ns", b"layout:user42"],
        vec![b"HEXISTS", b"ns", b"layout:user42"],
        vec![b"HDEL", b"ns", b"layout:user42"],
        vec![b"HSETEX", b"ns", b"FIELDS", b"1", b"layout:user42", b"v2"],
        vec![b"HEXPIRE", b"ns", b"60", b"FIELDS", b"1", b"layout:user42"],
        vec![
            b"HEXPIREAT",
            b"ns",
            b"2000000000",
            b"FIELDS",
            b"1",
            b"layout:user42",
        ],
    ];

    for cmd in &commands {
        let reply = node.call_line(cmd).await;
        assert_eq!(
            reply.as_deref(),
            Some(expected_err.as_str()),
            "command {:?} should fail with CLUSTERDOWN",
            cmd.iter()
                .map(|p| String::from_utf8_lossy(p))
                .collect::<Vec<_>>()
        );
    }

    // Even if slot 9706 is later added locally, none of the rejected writes
    // should have been persisted.
    assert_eq!(
        node.call_line(&[b"CLUSTER", b"ADDSLOTS", b"9706"])
            .await
            .as_deref(),
        Some("+OK")
    );
    assert_eq!(node.call_bulk(&[b"GET", b"layout:user42"]).await, None);
    assert_eq!(
        node.call_bulk(&[b"HGET", b"ns", b"layout:user42"]).await,
        None
    );

    // Removing the slot again turns it back into CLUSTERDOWN.
    assert_eq!(
        node.call_line(&[b"CLUSTER", b"DELSLOTS", b"9706"])
            .await
            .as_deref(),
        Some("+OK")
    );
    assert_eq!(
        node.call_line(&[b"SET", b"layout:user42", b"v3"])
            .await
            .as_deref(),
        Some(expected_err.as_str())
    );
}

#[tokio::test]
async fn multi_key_and_multi_field_commands_abort_before_touching_local_data() {
    let expected_err = format!("-{CLUSTERDOWN_HASH_SLOT_NOT_SERVED}");
    let node = start_cluster_node(
        "node-a",
        SlotRange {
            start: 0,
            end: 8191,
        },
        free_udp_port(),
        vec![],
    )
    .await;

    assert_eq!(
        node.call_line(&[b"SET", b"bar", b"keep"]).await.as_deref(),
        Some("+OK")
    );
    assert_eq!(
        node.call_line(&[b"HSET", b"ns", b"bar", b"hkeep"])
            .await
            .as_deref(),
        Some(":1")
    );

    // DEL with a local key and an unserved key must not delete the local key.
    assert_eq!(
        node.call_line(&[b"DEL", b"bar", b"layout:user42"])
            .await
            .as_deref(),
        Some(expected_err.as_str())
    );
    assert_eq!(
        node.call_bulk(&[b"GET", b"bar"]).await.as_deref(),
        Some(&b"keep"[..])
    );

    // HSETEX FNX where the first field already exists locally and the second is
    // unserved must report CLUSTERDOWN (not :0) and leave the local field untouched.
    assert_eq!(
        node.call_line(&[
            b"HSETEX",
            b"ns",
            b"FNX",
            b"FIELDS",
            b"2",
            b"bar",
            b"overwrite",
            b"layout:user42",
            b"v2",
        ])
        .await
        .as_deref(),
        Some(expected_err.as_str())
    );
    assert_eq!(
        node.call_line(&[
            b"HSETEX",
            b"ns",
            b"FIELDS",
            b"2",
            b"bar",
            b"overwrite",
            b"layout:user42",
            b"v2",
        ])
        .await
        .as_deref(),
        Some(expected_err.as_str())
    );
    assert_eq!(
        node.call_bulk(&[b"HGET", b"ns", b"bar"]).await.as_deref(),
        Some(&b"hkeep"[..])
    );

    // An immediate expiry must route both fields before deleting the local one.
    assert_eq!(
        node.call_line(&[
            b"HEXPIRE",
            b"ns",
            b"0",
            b"FIELDS",
            b"2",
            b"bar",
            b"layout:user42",
        ])
        .await
        .as_deref(),
        Some(expected_err.as_str())
    );
    assert_eq!(
        node.call_bulk(&[b"HGET", b"ns", b"bar"]).await.as_deref(),
        Some(&b"hkeep"[..])
    );
}

#[tokio::test]
async fn known_peer_slot_still_returns_moved() {
    let port_a = free_udp_port();
    let port_b = free_udp_port();

    let node_a = start_cluster_node(
        "node-a",
        SlotRange {
            start: 0,
            end: 8191,
        },
        port_a,
        vec![format!("127.0.0.1:{port_b}")],
    )
    .await;
    let node_b = start_cluster_node(
        "node-b",
        SlotRange {
            start: 8192,
            end: 16383,
        },
        port_b,
        vec![format!("127.0.0.1:{port_a}")],
    )
    .await;

    // Wait for gossip convergence so node A discovers node B and its claimed slots.
    let mut discovered = false;
    for _ in 0..50 {
        if let Some(nodes) = node_a.call_bulk(&[b"CLUSTER", b"NODES"]).await {
            let text = String::from_utf8_lossy(&nodes);
            if text
                .lines()
                .any(|line| line.starts_with("node-b ") && line.contains("8192-16383"))
            {
                discovered = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        discovered,
        "node-a (gossip {}) did not discover node-b (gossip {})",
        node_a.gossip_port, node_b.gossip_port
    );

    let expected_moved = format!("-MOVED 9706 {}", node_b.addr);
    assert_eq!(
        node_a
            .call_line(&[b"SET", b"layout:user42", b"v1"])
            .await
            .as_deref(),
        Some(expected_moved.as_str())
    );
    assert_eq!(
        node_a
            .call_line(&[b"HSET", b"ns", b"layout:user42", b"v1"])
            .await
            .as_deref(),
        Some(expected_moved.as_str())
    );
}
