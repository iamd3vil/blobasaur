//! End-to-end tests for persisted cluster state, epochs, epoch collision
//! resolution, CLUSTER MYID/NODES/INFO/SHARDS, and restart state reloading.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::net::{SocketAddr, UdpSocket};
use std::path::Path;
use std::pin::Pin;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use blobasaur::cluster::{CLUSTER_STATE_FILE, ClusterState, PersistedRole};
use blobasaur::config::{Cfg, ClusterConfig, ReplicationConfig, SlotRange};
use blobasaur::server;
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;

fn free_udp_port() -> u16 {
    static ISSUED: Mutex<BTreeSet<u16>> = Mutex::new(BTreeSet::new());
    loop {
        let port = UdpSocket::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        if ISSUED.lock().unwrap().insert(port) {
            return port;
        }
    }
}

struct RunningNode {
    addr: SocketAddr,
    gossip_port: u16,
    gossip_addr: String,
    shutdown: CancellationToken,
    handle: JoinHandle<miette::Result<()>>,
}

impl RunningNode {
    async fn stop(self) {
        self.shutdown.cancel();
        timeout(Duration::from_secs(30), self.handle)
            .await
            .expect("shutdown hung")
            .unwrap()
            .expect("shutdown reported an error");
    }
}

async fn start_node(
    dir: &Path,
    node_id: &str,
    gossip_port: u16,
    seeds: Vec<String>,
    slot_ranges: Option<Vec<SlotRange>>,
    replica_of: Option<&str>,
) -> RunningNode {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let cfg = Cfg {
        data_dir: dir.to_string_lossy().into_owned(),
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
            slot_ranges,
            gossip_interval_ms: Some(100),
            advertise_addr: Some(addr.to_string()),
            node_timeout_ms: None,
            replica_validity_factor: None,
        }),
        replication: Some(ReplicationConfig {
            replica_of: replica_of.map(str::to_string),
            tombstone_retention_secs: Some(86400),
        }),
        metrics: None,
        sqlite: None,
        shutdown_timeout_secs: Some(20),
        max_request_size_mb: None,
        cleanup_chunk_size: None,
        cleanup_interval_secs: None,
        new_replid: false,
    };

    let shutdown = CancellationToken::new();
    let handle = tokio::spawn(server::run(cfg, listener, shutdown.clone()));

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(mut client) = Client::try_connect(addr).await
            && client.call_simple(&["PING"]).await == "+PONG"
        {
            break;
        }
        assert!(Instant::now() < deadline, "node {node_id} did not start");
        sleep(Duration::from_millis(50)).await;
    }

    RunningNode {
        addr,
        gossip_port,
        gossip_addr: format!("127.0.0.1:{gossip_port}"),
        shutdown,
        handle,
    }
}

#[derive(Debug, PartialEq)]
enum Reply {
    Int(i64),
    Text(String),
    Array(Vec<Reply>),
}

struct Client(BufReader<TcpStream>);

impl Client {
    async fn try_connect(addr: SocketAddr) -> std::io::Result<Self> {
        Ok(Self(BufReader::new(TcpStream::connect(addr).await?)))
    }

    async fn connect(addr: SocketAddr) -> Self {
        Self::try_connect(addr).await.unwrap()
    }

    async fn send(&mut self, args: &[&str]) {
        let mut req = format!("*{}\r\n", args.len());
        for arg in args {
            req.push_str(&format!("${}\r\n{}\r\n", arg.len(), arg));
        }
        self.0.get_mut().write_all(req.as_bytes()).await.unwrap();
    }

    async fn line(&mut self) -> String {
        let mut line = String::new();
        self.0.read_line(&mut line).await.unwrap();
        line.trim_end().to_string()
    }

    async fn call_simple(&mut self, args: &[&str]) -> String {
        self.send(args).await;
        self.line().await
    }

    async fn call_bulk(&mut self, args: &[&str]) -> String {
        self.send(args).await;
        let header = self.line().await;
        let len: usize = header.strip_prefix('$').unwrap().parse().unwrap();
        let mut buf = vec![0; len + 2];
        self.0.read_exact(&mut buf).await.unwrap();
        buf.truncate(len);
        String::from_utf8(buf).unwrap()
    }

    async fn tree(&mut self, args: &[&str]) -> Reply {
        self.send(args).await;
        self.reply().await
    }

    fn reply(&mut self) -> Pin<Box<dyn Future<Output = Reply> + '_>> {
        Box::pin(async move {
            let line = self.line().await;
            let (kind, rest) = line.split_at(1);
            match kind {
                ":" => Reply::Int(rest.parse().unwrap()),
                "*" => {
                    let mut items = Vec::new();
                    for _ in 0..rest.parse::<usize>().unwrap() {
                        items.push(self.reply().await);
                    }
                    Reply::Array(items)
                }
                "$" if rest != "-1" => {
                    let len: usize = rest.parse().unwrap();
                    let mut buf = vec![0; len + 2];
                    self.0.read_exact(&mut buf).await.unwrap();
                    buf.truncate(len);
                    Reply::Text(String::from_utf8(buf).unwrap())
                }
                _ => Reply::Text(line),
            }
        })
    }
}

async fn wait_for<F, Fut>(what: &str, mut cond: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = Instant::now() + Duration::from_secs(30);
    while !cond().await {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        sleep(Duration::from_millis(50)).await;
    }
}

fn parse_info(info: &str) -> BTreeMap<String, String> {
    info.lines()
        .filter_map(|line| {
            let (k, v) = line.split_once(':')?;
            Some((k.to_string(), v.to_string()))
        })
        .collect()
}

fn parse_shard_node(reply: &Reply) -> BTreeMap<String, String> {
    let Reply::Array(items) = reply else {
        panic!("node entry is not an array");
    };
    items
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| {
            let Reply::Text(k) = &pair[0] else {
                panic!("expected key string");
            };
            let v = match &pair[1] {
                Reply::Text(s) => s.clone(),
                Reply::Int(n) => n.to_string(),
                other => panic!("unexpected value {other:?}"),
            };
            (k.clone(), v)
        })
        .collect()
}

fn parse_nodes_epochs(nodes: &str) -> BTreeMap<String, u64> {
    nodes
        .lines()
        .filter(|line| !line.is_empty())
        .map(|line| {
            let parts: Vec<&str> = line.split_whitespace().collect();
            (parts[0].to_string(), parts[6].parse::<u64>().unwrap())
        })
        .collect()
}

#[tokio::test]
async fn three_node_cluster_resolves_epochs_serves_shards_and_reloads_state_on_restart() {
    let dir1 = TempDir::new().unwrap();
    let dir2 = TempDir::new().unwrap();
    let dir3 = TempDir::new().unwrap();
    let dir_r = TempDir::new().unwrap();

    let p1 = free_udp_port();
    let p2 = free_udp_port();
    let p3 = free_udp_port();
    let pr = free_udp_port();

    let g1 = format!("127.0.0.1:{p1}");
    let g2 = format!("127.0.0.1:{p2}");
    let g3 = format!("127.0.0.1:{p3}");

    let (n1, n2, n3) = tokio::join!(
        start_node(
            dir1.path(),
            "node-1",
            p1,
            vec![g2.clone(), g3.clone()],
            Some(vec![SlotRange {
                start: 0,
                end: 5460
            }]),
            None,
        ),
        start_node(
            dir2.path(),
            "node-2",
            p2,
            vec![g1.clone(), g3.clone()],
            Some(vec![SlotRange {
                start: 5461,
                end: 10922,
            }]),
            None,
        ),
        start_node(
            dir3.path(),
            "node-3",
            p3,
            vec![g1.clone(), g2.clone()],
            Some(vec![SlotRange {
                start: 10923,
                end: 16383,
            }]),
            None,
        ),
    );
    let replica = start_node(
        dir_r.path(),
        "node-1b",
        pr,
        vec![n1.gossip_addr.clone()],
        None,
        Some("node-1"),
    )
    .await;

    // CLUSTER MYID works on both primaries and replicas.
    for (node, expected) in [
        (&n1, "node-1"),
        (&n2, "node-2"),
        (&n3, "node-3"),
        (&replica, "node-1b"),
    ] {
        let id = Client::connect(node.addr)
            .await
            .call_bulk(&["CLUSTER", "MYID"])
            .await;
        assert_eq!(id, expected);
    }

    // All three primaries bootstrapped at epoch 0; collision resolution must
    // assign unique config_epochs and converge current_epoch everywhere.
    wait_for("unique primary epochs and synced replica", || async {
        let mut c1 = Client::connect(n1.addr).await;
        let nodes = c1.call_bulk(&["CLUSTER", "NODES"]).await;
        let info = parse_info(&c1.call_bulk(&["CLUSTER", "INFO"]).await);
        if info.get("cluster_state").map(String::as_str) != Some("ok")
            || info.get("cluster_size").map(String::as_str) != Some("3")
            || info.get("cluster_known_nodes").map(String::as_str) != Some("4")
        {
            return false;
        }
        let epochs = parse_nodes_epochs(&nodes);
        if epochs.len() != 4 {
            return false;
        }
        let primary_epochs: BTreeSet<u64> = ["node-1", "node-2", "node-3"]
            .into_iter()
            .filter_map(|id| epochs.get(id).copied())
            .collect();
        if primary_epochs.len() != 3 {
            return false;
        }
        let shards = c1.tree(&["CLUSTER", "SHARDS"]).await;
        // Wait until the replica is online in shard 0.
        let Reply::Array(ref shard_list) = shards else {
            return false;
        };
        if shard_list.len() != 3 {
            return false;
        }
        let Reply::Array(ref shard0) = shard_list[0] else {
            return false;
        };
        let Reply::Array(ref shard0_nodes) = shard0[3] else {
            return false;
        };
        shard0_nodes.iter().any(|n| {
            let map = parse_shard_node(n);
            map.get("id").map(String::as_str) == Some("node-1b")
                && map.get("health").map(String::as_str) == Some("online")
        })
    })
    .await;

    // Check CLUSTER INFO epochs match CLUSTER NODES on every primary.
    let mut primary_my_epochs = BTreeMap::new();
    let mut seen_current_epoch = 0u64;
    for (id, node) in [("node-1", &n1), ("node-2", &n2), ("node-3", &n3)] {
        let mut c = Client::connect(node.addr).await;
        let info = parse_info(&c.call_bulk(&["CLUSTER", "INFO"]).await);
        let cur: u64 = info["cluster_current_epoch"].parse().unwrap();
        let my: u64 = info["cluster_my_epoch"].parse().unwrap();
        assert!(cur >= 2, "expected current_epoch >= 2, got {cur}");
        seen_current_epoch = seen_current_epoch.max(cur);
        primary_my_epochs.insert(id.to_string(), my);

        let nodes = parse_nodes_epochs(&c.call_bulk(&["CLUSTER", "NODES"]).await);
        assert_eq!(nodes[id], my);
    }
    let unique_epochs: BTreeSet<u64> = primary_my_epochs.values().copied().collect();
    assert_eq!(unique_epochs.len(), 3, "{primary_my_epochs:?}");

    // Check CLUSTER SHARDS structure (Redis 7 flat map layout).
    let Reply::Array(shards) = Client::connect(n1.addr)
        .await
        .tree(&["CLUSTER", "SHARDS"])
        .await
    else {
        panic!("CLUSTER SHARDS must return an array");
    };
    assert_eq!(shards.len(), 3);
    // First shard covers 0..=5460 with node-1 (master) and node-1b (replica, online).
    let Reply::Array(shard0) = &shards[0] else {
        panic!("shard 0 is not an array");
    };
    assert_eq!(shard0[0], Reply::Text("slots".into()));
    assert_eq!(
        shard0[1],
        Reply::Array(vec![Reply::Int(0), Reply::Int(5460)])
    );
    assert_eq!(shard0[2], Reply::Text("nodes".into()));
    let Reply::Array(shard0_nodes) = &shard0[3] else {
        panic!("nodes is not an array");
    };
    assert_eq!(shard0_nodes.len(), 2);
    let master0 = parse_shard_node(&shard0_nodes[0]);
    assert_eq!(master0["id"], "node-1");
    assert_eq!(master0["role"], "master");
    assert_eq!(master0["health"], "online");
    assert_eq!(master0["port"], n1.addr.port().to_string());
    let replica0 = parse_shard_node(&shard0_nodes[1]);
    assert_eq!(replica0["id"], "node-1b");
    assert_eq!(replica0["role"], "replica");
    assert_eq!(replica0["health"], "online");
    assert_eq!(replica0["port"], replica.addr.port().to_string());

    // State file was persisted on disk with the resolved epochs and slot table.
    let saved_n1 = ClusterState::load(&dir1.path().join(CLUSTER_STATE_FILE), "node-1").unwrap();
    assert_eq!(saved_n1.role, PersistedRole::Primary);
    assert_eq!(saved_n1.config_epoch, primary_my_epochs["node-1"]);
    assert_eq!(saved_n1.slots.slot_count(), 16384);

    // Restart node-1 with a DISAGREEING config (wrong slots + replica_of set):
    // cluster-state.json must win over the config and keep its role, slots, and epoch.
    let n1_epoch_before = primary_my_epochs["node-1"];
    let n1_gossip_port = n1.gossip_port;
    n1.stop().await;

    let n1_restarted = start_node(
        dir1.path(),
        "node-1",
        n1_gossip_port,
        vec![n2.gossip_addr.clone(), n3.gossip_addr.clone()],
        Some(vec![SlotRange { start: 0, end: 10 }]),
        Some("node-2"),
    )
    .await;
    let mut c1 = Client::connect(n1_restarted.addr).await;
    let info = parse_info(&c1.call_bulk(&["CLUSTER", "INFO"]).await);
    assert_eq!(
        info["cluster_my_epoch"].parse::<u64>().unwrap(),
        n1_epoch_before
    );
    assert!(info["cluster_current_epoch"].parse::<u64>().unwrap() >= n1_epoch_before);
    let nodes = c1.call_bulk(&["CLUSTER", "NODES"]).await;
    assert!(
        nodes.contains(&format!(
            "node-1 {}:{} myself,master - 0 0 {} connected 0-5460\n",
            n1_restarted.addr.ip(),
            n1_restarted.addr.port(),
            n1_epoch_before
        )),
        "{nodes}"
    );

    // Restart the replica with a primary config (no replica_of): cluster-state.json
    // must keep it as a replica of node-1.
    let replica_gossip_port = replica.gossip_port;
    replica.stop().await;
    let replica_restarted = start_node(
        dir_r.path(),
        "node-1b",
        replica_gossip_port,
        vec![n1_restarted.gossip_addr.clone()],
        Some(vec![SlotRange { start: 0, end: 10 }]),
        None,
    )
    .await;
    let r_nodes = Client::connect(replica_restarted.addr)
        .await
        .call_bulk(&["CLUSTER", "NODES"])
        .await;
    assert!(
        r_nodes.contains(&format!(
            "node-1b {}:{} myself,slave node-1 0 0 0 connected\n",
            replica_restarted.addr.ip(),
            replica_restarted.addr.port()
        )),
        "{r_nodes}"
    );

    replica_restarted.stop().await;
    n1_restarted.stop().await;
    n2.stop().await;
    n3.stop().await;
}
