//! End-to-end tests for PFAIL/FAIL failure detection and minority write stop (failover step 3).

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::net::{SocketAddr, UdpSocket};
use std::path::Path;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use blobasaur::cluster::{CLUSTERDOWN_HASH_SLOT_NOT_SERVED, ClusterManager};
use blobasaur::config::{Cfg, ClusterConfig, ReplicationConfig, SlotRange};
use blobasaur::server;
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;

const NODE_TIMEOUT_MS: u64 = 1000;

/// Each test runs several nodes in-process with a 1 s `node_timeout` (the
/// default is 15 s). Run in parallel on a 4-vCPU CI runner, they starved each
/// other's gossip past that timeout, so the tests take turns.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

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

fn key_in_range(prefix: &str, start: u16, end: u16) -> String {
    (0..)
        .map(|i| format!("{prefix}_{i}"))
        .find(|k| {
            let slot = ClusterManager::calculate_slot(k);
            slot >= start && slot <= end
        })
        .unwrap()
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
            node_timeout_ms: Some(NODE_TIMEOUT_MS),
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
        let line = self.line().await;
        if let Some(rest) = line.strip_prefix('$')
            && rest != "-1"
            && let Ok(len) = rest.parse::<usize>()
        {
            let mut buf = vec![0; len + 2];
            self.0.read_exact(&mut buf).await.unwrap();
        }
        line
    }

    async fn call_bulk(&mut self, args: &[&str]) -> String {
        self.send(args).await;
        let header = self.line().await;
        let len: usize = header
            .strip_prefix('$')
            .unwrap_or_else(|| panic!("expected bulk string for {args:?}, got {header:?}"))
            .parse()
            .unwrap();
        let mut buf = vec![0; len + 2];
        self.0.read_exact(&mut buf).await.unwrap();
        buf.truncate(len);
        String::from_utf8(buf).unwrap()
    }
}

async fn wait_for<F, Fut>(what: &str, max_wait: Duration, mut cond: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = Instant::now() + max_wait;
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

/// Returns `(flags, link_state)` for `node_id` in a `CLUSTER NODES` reply.
fn node_status(nodes: &str, node_id: &str) -> Option<(String, String)> {
    nodes.lines().find_map(|line| {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.first() == Some(&node_id) && parts.len() >= 8 {
            Some((parts[2].to_string(), parts[7].to_string()))
        } else {
            None
        }
    })
}

#[tokio::test]
async fn three_primaries_detect_fail_and_clear_on_restart() {
    let _serial = SERIAL.lock().await;
    let dir1 = TempDir::new().unwrap();
    let dir2 = TempDir::new().unwrap();
    let dir3 = TempDir::new().unwrap();

    let p1 = free_udp_port();
    let p2 = free_udp_port();
    let p3 = free_udp_port();
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
                end: 5460,
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

    wait_for("3-primary cluster ok", Duration::from_secs(15), || async {
        for addr in [n1.addr, n2.addr, n3.addr] {
            let mut c = Client::connect(addr).await;
            let info = parse_info(&c.call_bulk(&["CLUSTER", "INFO"]).await);
            if info.get("cluster_state").map(String::as_str) != Some("ok") {
                return false;
            }
        }
        true
    })
    .await;

    let k1 = key_in_range("k1", 0, 5460);
    let k3 = key_in_range("k3", 10923, 16383);
    assert_eq!(
        Client::connect(n1.addr)
            .await
            .call_simple(&["SET", &k1, "v1"])
            .await,
        "+OK"
    );
    assert_eq!(
        Client::connect(n3.addr)
            .await
            .call_simple(&["SET", &k3, "v3"])
            .await,
        "+OK"
    );
    assert!(
        Client::connect(n1.addr)
            .await
            .call_simple(&["GET", &k3])
            .await
            .starts_with("-MOVED ")
    );

    // Kill node-3: node-1 and node-2 form a 2/3 majority and must mark node-3
    // `fail` within a few node_timeouts.
    let n3_gossip_port = n3.gossip_port;
    n3.stop().await;

    wait_for(
        "node-1 and node-2 mark node-3 fail",
        Duration::from_secs(8),
        || async {
            for addr in [n1.addr, n2.addr] {
                let mut c = Client::connect(addr).await;
                let nodes = c.call_bulk(&["CLUSTER", "NODES"]).await;
                if node_status(&nodes, "node-3")
                    != Some(("master,fail".to_string(), "disconnected".to_string()))
                {
                    return false;
                }
            }
            true
        },
    )
    .await;

    let clusterdown = format!("-{CLUSTERDOWN_HASH_SLOT_NOT_SERVED}");
    for addr in [n1.addr, n2.addr] {
        let mut c = Client::connect(addr).await;
        assert_eq!(c.call_simple(&["GET", &k3]).await, clusterdown);
        assert_eq!(c.call_simple(&["SET", &k3, "x"]).await, clusterdown);
        let info = parse_info(&c.call_bulk(&["CLUSTER", "INFO"]).await);
        assert_eq!(info.get("cluster_state").map(String::as_str), Some("fail"));
        assert_eq!(
            info.get("cluster_slots_fail").map(String::as_str),
            Some("5461")
        );
    }

    // Majority primaries still serve their own slots.
    assert_eq!(
        Client::connect(n1.addr)
            .await
            .call_simple(&["SET", &k1, "v1_updated"])
            .await,
        "+OK"
    );
    assert_eq!(
        Client::connect(n1.addr)
            .await
            .call_bulk(&["GET", &k1])
            .await,
        "v1_updated"
    );

    // Restart node-3: FAIL must clear after 2 * node_timeout_ms of reachability.
    let n3_restarted = start_node(
        dir3.path(),
        "node-3",
        n3_gossip_port,
        vec![n1.gossip_addr.clone(), n2.gossip_addr.clone()],
        Some(vec![SlotRange {
            start: 10923,
            end: 16383,
        }]),
        None,
    )
    .await;

    wait_for(
        "FAIL clears on node-1 and node-2 after node-3 restarts",
        Duration::from_secs(10),
        || async {
            for addr in [n1.addr, n2.addr] {
                let mut c = Client::connect(addr).await;
                let nodes = c.call_bulk(&["CLUSTER", "NODES"]).await;
                let info = parse_info(&c.call_bulk(&["CLUSTER", "INFO"]).await);
                if node_status(&nodes, "node-3")
                    != Some(("master".to_string(), "connected".to_string()))
                    || info.get("cluster_state").map(String::as_str) != Some("ok")
                {
                    return false;
                }
            }
            true
        },
    )
    .await;

    assert!(
        Client::connect(n1.addr)
            .await
            .call_simple(&["GET", &k3])
            .await
            .starts_with("-MOVED ")
    );

    n3_restarted.stop().await;
    n2.stop().await;
    n1.stop().await;
}

#[tokio::test]
async fn minority_primary_and_replica_fence_until_majority_returns() {
    let _serial = SERIAL.lock().await;
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
                end: 5460,
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
        vec![g1.clone(), g2.clone(), g3.clone()],
        None,
        Some("node-1"),
    )
    .await;

    let k1 = key_in_range("m1", 0, 5460);
    assert_eq!(
        Client::connect(n1.addr)
            .await
            .call_simple(&["SET", &k1, "before"])
            .await,
        "+OK"
    );

    wait_for(
        "cluster ok and replica serving READONLY reads",
        Duration::from_secs(15),
        || async {
            let mut c1 = Client::connect(n1.addr).await;
            let info1 = parse_info(&c1.call_bulk(&["CLUSTER", "INFO"]).await);
            if info1.get("cluster_state").map(String::as_str) != Some("ok") {
                return false;
            }
            let mut cr = Client::connect(replica.addr).await;
            let infor = parse_info(&cr.call_bulk(&["CLUSTER", "INFO"]).await);
            if infor.get("cluster_state").map(String::as_str) != Some("ok") {
                return false;
            }
            if cr.call_simple(&["READONLY"]).await != "+OK" {
                return false;
            }
            cr.send(&["GET", &k1]).await;
            let header = cr.line().await;
            header == "$6"
        },
    )
    .await;

    // Stop both node-2 and node-3: node-1 and its replica are now in a 1/3 minority.
    let n2_port = n2.gossip_port;
    let n3_port = n3.gossip_port;
    tokio::join!(n2.stop(), n3.stop());

    let clusterdown = format!("-{CLUSTERDOWN_HASH_SLOT_NOT_SERVED}");
    wait_for(
        "survivor primary and replica enter minority CLUSTERDOWN",
        Duration::from_secs(8),
        || async {
            let mut c1 = Client::connect(n1.addr).await;
            let set_reply = c1.call_simple(&["SET", &k1, "blocked"]).await;
            let get_reply = c1.call_simple(&["GET", &k1]).await;
            let info1 = parse_info(&c1.call_bulk(&["CLUSTER", "INFO"]).await);
            if set_reply != clusterdown
                || get_reply != clusterdown
                || info1.get("cluster_state").map(String::as_str) != Some("fail")
            {
                return false;
            }

            let mut cr = Client::connect(replica.addr).await;
            assert_eq!(cr.call_simple(&["READONLY"]).await, "+OK");
            let rep_get = cr.call_simple(&["GET", &k1]).await;
            let infor = parse_info(&cr.call_bulk(&["CLUSTER", "INFO"]).await);
            rep_get == clusterdown && infor.get("cluster_state").map(String::as_str) == Some("fail")
        },
    )
    .await;

    // On node-1 alone, node-2 and node-3 are PFAIL (`fail?`), not FAIL (no majority).
    let nodes1 = Client::connect(n1.addr)
        .await
        .call_bulk(&["CLUSTER", "NODES"])
        .await;
    assert_eq!(
        node_status(&nodes1, "node-2"),
        Some(("master,fail?".to_string(), "disconnected".to_string())),
        "{nodes1}"
    );
    assert_eq!(
        node_status(&nodes1, "node-3"),
        Some(("master,fail?".to_string(), "disconnected".to_string())),
        "{nodes1}"
    );

    // Bring node-2 back: node-1 and node-1b now reach 2/3 primaries (majority)
    // and must resume serving node-1's slots.
    let n2_back = start_node(
        dir2.path(),
        "node-2",
        n2_port,
        vec![n1.gossip_addr.clone(), g3.clone()],
        Some(vec![SlotRange {
            start: 5461,
            end: 10922,
        }]),
        None,
    )
    .await;

    wait_for(
        "primary and replica resume serving after majority returns",
        Duration::from_secs(10),
        || async {
            let mut c1 = Client::connect(n1.addr).await;
            if c1.call_simple(&["SET", &k1, "after"]).await != "+OK" {
                return false;
            }
            let mut cr = Client::connect(replica.addr).await;
            if cr.call_simple(&["READONLY"]).await != "+OK" {
                return false;
            }
            cr.send(&["GET", &k1]).await;
            let header = cr.line().await;
            header == "$5"
        },
    )
    .await;

    // Bring node-3 back too and verify full cluster_state:ok recovery.
    let n3_back = start_node(
        dir3.path(),
        "node-3",
        n3_port,
        vec![n1.gossip_addr.clone(), n2_back.gossip_addr.clone()],
        Some(vec![SlotRange {
            start: 10923,
            end: 16383,
        }]),
        None,
    )
    .await;

    wait_for(
        "cluster_state returns to ok with all three primaries up",
        Duration::from_secs(10),
        || async {
            let mut c1 = Client::connect(n1.addr).await;
            let info = parse_info(&c1.call_bulk(&["CLUSTER", "INFO"]).await);
            info.get("cluster_state").map(String::as_str) == Some("ok")
        },
    )
    .await;

    n3_back.stop().await;
    n2_back.stop().await;
    replica.stop().await;
    n1.stop().await;
}
