//! End-to-end integration tests for replica elections, automatic failover, and
//! `CLUSTER FAILOVER FORCE` (failover step 4).

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::net::{SocketAddr, UdpSocket};
use std::path::Path;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use blobasaur::cluster::ClusterManager;
use blobasaur::config::{Cfg, ClusterConfig, ReplicationConfig, SlotRange};
use blobasaur::server;
use sqlx::{Connection, SqliteConnection};
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::process::Command as TokioCommand;
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;

const SHARDS: usize = 2;
const NODE_TIMEOUT_MS: u64 = 1000;

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
        num_shards: SHARDS,
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

/// Returns `(flags, master_id, config_epoch, slots)` for `node_id` in `CLUSTER NODES`.
fn node_entry(nodes: &str, node_id: &str) -> Option<(String, String, u64, String)> {
    nodes.lines().find_map(|line| {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.first() == Some(&node_id) && parts.len() >= 8 {
            let slots = if parts.len() > 8 {
                parts[8..].join(" ")
            } else {
                String::new()
            };
            Some((
                parts[2].to_string(),
                parts[3].to_string(),
                parts[6].parse().unwrap_or(0),
                slots,
            ))
        } else {
            None
        }
    })
}

async fn all_shards_synced(dir: &Path) -> bool {
    for shard in 0..SHARDS {
        let url = format!("sqlite:{}/shard_{shard}.db", dir.display());
        let Ok(mut conn) = SqliteConnection::connect(&url).await else {
            return false;
        };
        let synced: Option<String> =
            sqlx::query_scalar("SELECT v FROM repl_state WHERE k = 'synced'")
                .fetch_optional(&mut conn)
                .await
                .ok()
                .flatten();
        if synced.as_deref() != Some("1") {
            return false;
        }
    }
    true
}

#[tokio::test]
async fn auto_failover_promotes_one_replica_repoints_sibling_and_demotes_returning_primary() {
    let d1 = TempDir::new().unwrap();
    let d2 = TempDir::new().unwrap();
    let d3 = TempDir::new().unwrap();
    let dr1 = TempDir::new().unwrap();
    let dr2 = TempDir::new().unwrap();

    let gp1 = free_udp_port();
    let gp2 = free_udp_port();
    let gp3 = free_udp_port();
    let gpr1 = free_udp_port();
    let gpr2 = free_udp_port();
    let g1 = format!("127.0.0.1:{gp1}");
    let g2 = format!("127.0.0.1:{gp2}");
    let g3 = format!("127.0.0.1:{gp3}");

    let (p1, p2, p3) = tokio::join!(
        start_node(
            d1.path(),
            "p1",
            gp1,
            vec![g2.clone(), g3.clone()],
            Some(vec![SlotRange {
                start: 0,
                end: 5460,
            }]),
            None,
        ),
        start_node(
            d2.path(),
            "p2",
            gp2,
            vec![g1.clone(), g3.clone()],
            Some(vec![SlotRange {
                start: 5461,
                end: 10922,
            }]),
            None,
        ),
        start_node(
            d3.path(),
            "p3",
            gp3,
            vec![g1.clone(), g2.clone()],
            Some(vec![SlotRange {
                start: 10923,
                end: 16383,
            }]),
            None,
        ),
    );
    let (r1, r2) = tokio::join!(
        start_node(
            dr1.path(),
            "r1",
            gpr1,
            vec![g1.clone(), g2.clone(), g3.clone()],
            None,
            Some("p1"),
        ),
        start_node(
            dr2.path(),
            "r2",
            gpr2,
            vec![g1.clone(), g2.clone(), g3.clone()],
            None,
            Some("p1"),
        ),
    );

    let k_pre = key_in_range("autofail_pre", 0, 5460);
    assert_eq!(
        Client::connect(p1.addr)
            .await
            .call_simple(&["SET", &k_pre, "before-fail"])
            .await,
        "+OK"
    );

    // Wait for cluster ok and both replicas synced and serving READONLY reads of k_pre.
    wait_for(
        "cluster ok and both replicas synced with pre-failover write",
        Duration::from_secs(20),
        || async {
            let mut c2 = Client::connect(p2.addr).await;
            let info = parse_info(&c2.call_bulk(&["CLUSTER", "INFO"]).await);
            if info.get("cluster_state").map(String::as_str) != Some("ok") {
                return false;
            }
            if !all_shards_synced(dr1.path()).await || !all_shards_synced(dr2.path()).await {
                return false;
            }
            for addr in [r1.addr, r2.addr] {
                let mut c = Client::connect(addr).await;
                if c.call_simple(&["READONLY"]).await != "+OK" {
                    return false;
                }
                c.send(&["GET", &k_pre]).await;
                if c.line().await != "$11" {
                    return false;
                }
            }
            true
        },
    )
    .await;

    // Kill p1: automatic failover must elect exactly one of r1/r2.
    let p1_gossip_port = p1.gossip_port;
    p1.stop().await;

    let winner_id = Mutex::new(String::new());
    wait_for(
        "exactly one replica automatically promoted and sibling re-pointed",
        Duration::from_secs(15),
        || async {
            let mut c2 = Client::connect(p2.addr).await;
            let nodes = c2.call_bulk(&["CLUSTER", "NODES"]).await;
            let r1_e = node_entry(&nodes, "r1");
            let r2_e = node_entry(&nodes, "r2");
            let (Some(e1), Some(e2)) = (r1_e, r2_e) else {
                return false;
            };
            let r1_won = e1.0.contains("master")
                && e1.3 == "0-5460"
                && e2.0.contains("slave")
                && e2.1 == "r1";
            let r2_won = e2.0.contains("master")
                && e2.3 == "0-5460"
                && e1.0.contains("slave")
                && e1.1 == "r2";
            if r1_won {
                *winner_id.lock().unwrap() = "r1".to_string();
                true
            } else if r2_won {
                *winner_id.lock().unwrap() = "r2".to_string();
                true
            } else {
                false
            }
        },
    )
    .await;
    let winner_id = winner_id.into_inner().unwrap();

    let (winner_addr, sibling_id, sibling_addr) = if winner_id == "r1" {
        (r1.addr, "r2", r2.addr)
    } else {
        (r2.addr, "r1", r1.addr)
    };

    // Check CLUSTER INFO counters on winner and voters.
    let winner_info = parse_info(
        &Client::connect(winner_addr)
            .await
            .call_bulk(&["CLUSTER", "INFO"])
            .await,
    );
    let wins: u64 = winner_info["cluster_elections_won"].parse().unwrap();
    assert!(
        wins >= 1,
        "winner should record election win: {winner_info:?}"
    );
    let p2_info = parse_info(
        &Client::connect(p2.addr)
            .await
            .call_bulk(&["CLUSTER", "INFO"])
            .await,
    );
    let p3_info = parse_info(
        &Client::connect(p3.addr)
            .await
            .call_bulk(&["CLUSTER", "INFO"])
            .await,
    );
    let total_votes: u64 = p2_info["cluster_votes_granted"].parse::<u64>().unwrap()
        + p3_info["cluster_votes_granted"].parse::<u64>().unwrap();
    assert!(total_votes >= 2, "p2+p3 should have granted >=2 votes");

    // Pre-failover key survives, post-failover writes succeed on winner, and p2 redirects MOVED.
    assert_eq!(
        Client::connect(winner_addr)
            .await
            .call_bulk(&["GET", &k_pre])
            .await,
        "before-fail"
    );
    let k_post = key_in_range("autofail_post", 0, 5460);
    assert_eq!(
        Client::connect(winner_addr)
            .await
            .call_simple(&["SET", &k_post, "after-fail"])
            .await,
        "+OK"
    );
    let moved = Client::connect(p2.addr)
        .await
        .call_simple(&["GET", &k_post])
        .await;
    assert!(
        moved.starts_with("-MOVED ") && moved.ends_with(&winner_addr.to_string()),
        "expected MOVED to {winner_addr}, got {moved}"
    );

    // Sibling replica resyncs from winner and serves READONLY reads of post-failover writes.
    wait_for(
        "sibling replica resynced from winner",
        Duration::from_secs(15),
        || async {
            let mut cs = Client::connect(sibling_addr).await;
            if cs.call_simple(&["READONLY"]).await != "+OK" {
                return false;
            }
            cs.send(&["GET", &k_post]).await;
            cs.line().await == "$10"
        },
    )
    .await;

    // Verify go-redis ClusterClient works against p1's slots after automatic failover.
    if std::env::var_os("BLOBASAUR_SKIP_GO_TESTS").is_none() {
        let addrs = format!("{},{},{}", p2.addr, p3.addr, winner_addr);
        let output = TokioCommand::new("go")
            .args([
                "test",
                "-count=1",
                "-run",
                "^TestClusterClientAfterAutoFailover$",
                "./...",
            ])
            .current_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/goredis"))
            .env("BLOBASAUR_CLUSTER_ADDRS", &addrs)
            .env("BLOBASAUR_SLOT_KEY", &k_pre)
            .env("BLOBASAUR_EXPECT_VAL", "before-fail")
            .output()
            .await
            .expect("failed to run go test");
        assert!(
            output.status.success(),
            "go-redis auto-failover test failed:\n{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    // Restart old primary p1: must automatically demote to a replica of the winner and resync.
    let p1_back = start_node(
        d1.path(),
        "p1",
        p1_gossip_port,
        vec![p2.gossip_addr.clone(), p3.gossip_addr.clone()],
        Some(vec![SlotRange {
            start: 0,
            end: 5460,
        }]),
        None,
    )
    .await;

    wait_for(
        "restarted p1 demotes to replica of winner and resyncs",
        Duration::from_secs(15),
        || async {
            let mut c1 = Client::connect(p1_back.addr).await;
            let nodes = c1.call_bulk(&["CLUSTER", "NODES"]).await;
            let Some((flags, master, _, slots)) = node_entry(&nodes, "p1") else {
                return false;
            };
            if !flags.contains("slave") || master != winner_id || !slots.is_empty() {
                return false;
            }
            if c1.call_simple(&["READONLY"]).await != "+OK" {
                return false;
            }
            c1.send(&["GET", &k_post]).await;
            c1.line().await == "$10"
        },
    )
    .await;

    // Sibling still follows winner too.
    let nodes = Client::connect(p2.addr)
        .await
        .call_bulk(&["CLUSTER", "NODES"])
        .await;
    let sib = node_entry(&nodes, sibling_id).unwrap();
    assert!(sib.0.contains("slave") && sib.1 == winner_id, "{nodes}");

    p1_back.stop().await;
    r2.stop().await;
    r1.stop().await;
    p3.stop().await;
    p2.stop().await;
}

#[tokio::test]
async fn unsynced_replica_never_stands_for_election() {
    let d1 = TempDir::new().unwrap();
    let d2 = TempDir::new().unwrap();
    let d3 = TempDir::new().unwrap();
    let dr1 = TempDir::new().unwrap();

    let gp1 = free_udp_port();
    let gp2 = free_udp_port();
    let gp3 = free_udp_port();
    let gpr1 = free_udp_port();
    let g1 = format!("127.0.0.1:{gp1}");
    let g2 = format!("127.0.0.1:{gp2}");
    let g3 = format!("127.0.0.1:{gp3}");

    let (p1, p2, p3) = tokio::join!(
        start_node(
            d1.path(),
            "p1",
            gp1,
            vec![g2.clone(), g3.clone()],
            Some(vec![SlotRange {
                start: 0,
                end: 5460,
            }]),
            None,
        ),
        start_node(
            d2.path(),
            "p2",
            gp2,
            vec![g1.clone(), g3.clone()],
            Some(vec![SlotRange {
                start: 5461,
                end: 10922,
            }]),
            None,
        ),
        start_node(
            d3.path(),
            "p3",
            gp3,
            vec![g1.clone(), g2.clone()],
            Some(vec![SlotRange {
                start: 10923,
                end: 16383,
            }]),
            None,
        ),
    );

    wait_for("3 primaries converged", Duration::from_secs(15), || async {
        let info = parse_info(
            &Client::connect(p2.addr)
                .await
                .call_bulk(&["CLUSTER", "INFO"])
                .await,
        );
        info.get("cluster_state").map(String::as_str) == Some("ok")
    })
    .await;

    // Kill p1 BEFORE starting r1 so r1 can never sync from p1.
    p1.stop().await;

    let r1 = start_node(
        dr1.path(),
        "r1",
        gpr1,
        vec![g2.clone(), g3.clone()],
        None,
        Some("p1"),
    )
    .await;

    // Wait for p2, p3, and r1 to mark p1 FAIL.
    wait_for(
        "p2 and p3 mark p1 FAIL",
        Duration::from_secs(10),
        || async {
            for addr in [p2.addr, p3.addr] {
                let nodes = Client::connect(addr)
                    .await
                    .call_bulk(&["CLUSTER", "NODES"])
                    .await;
                if !node_entry(&nodes, "p1").is_some_and(|e| e.0.contains("fail")) {
                    return false;
                }
            }
            true
        },
    )
    .await;

    // Wait well past election delay (500..1000ms) + election timeout: r1 must NOT promote.
    sleep(Duration::from_millis(2500)).await;
    let nodes = Client::connect(r1.addr)
        .await
        .call_bulk(&["CLUSTER", "NODES"])
        .await;
    let r1_entry = node_entry(&nodes, "r1").unwrap();
    assert!(
        r1_entry.0.contains("slave") && r1_entry.3.is_empty(),
        "unsynced replica must never promote: {nodes}"
    );
    let info = parse_info(
        &Client::connect(r1.addr)
            .await
            .call_bulk(&["CLUSTER", "INFO"])
            .await,
    );
    assert_eq!(
        info.get("cluster_elections_started").map(String::as_str),
        Some("0"),
        "unsynced replica must never start an election: {info:?}"
    );

    // Even FORCE on an unsynced replica must be refused.
    let force_err = Client::connect(r1.addr)
        .await
        .call_simple(&["CLUSTER", "FAILOVER", "FORCE"])
        .await;
    assert!(
        force_err.starts_with("-ERR") && force_err.contains("not synced"),
        "{force_err}"
    );

    r1.stop().await;
    p3.stop().await;
    p2.stop().await;
}

#[tokio::test]
async fn partition_leaving_primary_reachable_from_majority_does_not_failover() {
    let d1 = TempDir::new().unwrap();
    let d2 = TempDir::new().unwrap();
    let d3 = TempDir::new().unwrap();
    let dr1 = TempDir::new().unwrap();

    let gp1 = free_udp_port();
    let gp2 = free_udp_port();
    let gp3 = free_udp_port();
    let gpr1 = free_udp_port();
    let g1 = format!("127.0.0.1:{gp1}");
    let g2 = format!("127.0.0.1:{gp2}");
    let g3 = format!("127.0.0.1:{gp3}");

    let (p1, p2, p3) = tokio::join!(
        start_node(
            d1.path(),
            "p1",
            gp1,
            vec![g2.clone(), g3.clone()],
            Some(vec![SlotRange {
                start: 0,
                end: 5460,
            }]),
            None,
        ),
        start_node(
            d2.path(),
            "p2",
            gp2,
            vec![g1.clone(), g3.clone()],
            Some(vec![SlotRange {
                start: 5461,
                end: 10922,
            }]),
            None,
        ),
        start_node(
            d3.path(),
            "p3",
            gp3,
            vec![g1.clone(), g2.clone()],
            Some(vec![SlotRange {
                start: 10923,
                end: 16383,
            }]),
            None,
        ),
    );
    let r1 = start_node(
        dr1.path(),
        "r1",
        gpr1,
        vec![g1.clone(), g2.clone(), g3.clone()],
        None,
        Some("p1"),
    )
    .await;

    wait_for(
        "cluster ok and r1 synced",
        Duration::from_secs(15),
        || async {
            let info = parse_info(
                &Client::connect(p1.addr)
                    .await
                    .call_bulk(&["CLUSTER", "INFO"])
                    .await,
            );
            info.get("cluster_state").map(String::as_str) == Some("ok")
                && all_shards_synced(dr1.path()).await
        },
    )
    .await;

    // Partition p3 away from the {p1, p2} majority by restarting p3 on an isolated
    // gossip port with no seeds to p1/p2. p1 remains reachable from majority {p1, p2}.
    p3.stop().await;
    let isolated_gp3 = free_udp_port();
    let p3_isolated = start_node(
        d3.path(),
        "p3",
        isolated_gp3,
        vec![],
        Some(vec![SlotRange {
            start: 10923,
            end: 16383,
        }]),
        None,
    )
    .await;

    // Wait past node_timeout + election delay: p3 only sees p1 as PFAIL (fail?),
    // p1 + p2 stay healthy serving p1's slots, and r1 never triggers failover.
    sleep(Duration::from_millis(2500)).await;

    let p3_nodes = Client::connect(p3_isolated.addr)
        .await
        .call_bulk(&["CLUSTER", "NODES"])
        .await;
    let p1_on_p3 = node_entry(&p3_nodes, "p1").unwrap();
    assert!(
        p1_on_p3.0.contains("fail?") && !p1_on_p3.0.ends_with(",fail"),
        "isolated p3 must only mark p1 PFAIL, not FAIL: {p3_nodes}"
    );

    let k1 = key_in_range("maj_k", 0, 5460);
    assert_eq!(
        Client::connect(p1.addr)
            .await
            .call_simple(&["SET", &k1, "still-primary"])
            .await,
        "+OK"
    );
    let r1_nodes = Client::connect(r1.addr)
        .await
        .call_bulk(&["CLUSTER", "NODES"])
        .await;
    assert!(
        node_entry(&r1_nodes, "r1").unwrap().0.contains("slave"),
        "r1 must stay a replica while p1 is reachable from majority: {r1_nodes}"
    );

    // Even an explicit non-MANUAL BLOBASAUR.FAILOVER-AUTH sent to p2 must be refused.
    let mut c2 = Client::connect(p2.addr).await;
    let p1_epoch = node_entry(&r1_nodes, "p1").unwrap().2;
    let reply = c2
        .call_simple(&[
            "BLOBASAUR.FAILOVER-AUTH",
            "r1",
            "99",
            "p1",
            &p1_epoch.to_string(),
            "0-5460",
        ])
        .await;
    assert!(
        reply.starts_with("-ERR") && reply.contains("not marked FAIL"),
        "p2 must refuse vote while p1 is live: {reply}"
    );

    p3_isolated.stop().await;
    r1.stop().await;
    p2.stop().await;
    p1.stop().await;
}

#[tokio::test]
async fn cluster_failover_force_promotes_while_primary_alive_and_demotes_primary() {
    let d1 = TempDir::new().unwrap();
    let d2 = TempDir::new().unwrap();
    let d3 = TempDir::new().unwrap();
    let dr1 = TempDir::new().unwrap();

    let gp1 = free_udp_port();
    let gp2 = free_udp_port();
    let gp3 = free_udp_port();
    let gpr1 = free_udp_port();
    let g1 = format!("127.0.0.1:{gp1}");
    let g2 = format!("127.0.0.1:{gp2}");
    let g3 = format!("127.0.0.1:{gp3}");

    let (p1, p2, p3) = tokio::join!(
        start_node(
            d1.path(),
            "p1",
            gp1,
            vec![g2.clone(), g3.clone()],
            Some(vec![SlotRange {
                start: 0,
                end: 5460,
            }]),
            None,
        ),
        start_node(
            d2.path(),
            "p2",
            gp2,
            vec![g1.clone(), g3.clone()],
            Some(vec![SlotRange {
                start: 5461,
                end: 10922,
            }]),
            None,
        ),
        start_node(
            d3.path(),
            "p3",
            gp3,
            vec![g1.clone(), g2.clone()],
            Some(vec![SlotRange {
                start: 10923,
                end: 16383,
            }]),
            None,
        ),
    );
    let r1 = start_node(
        dr1.path(),
        "r1",
        gpr1,
        vec![g1.clone(), g2.clone(), g3.clone()],
        None,
        Some("p1"),
    )
    .await;

    let k_pre = key_in_range("force_pre", 0, 5460);
    assert_eq!(
        Client::connect(p1.addr)
            .await
            .call_simple(&["SET", &k_pre, "val-before-force"])
            .await,
        "+OK"
    );

    wait_for(
        "cluster ok and r1 synced with pre-force write",
        Duration::from_secs(15),
        || async {
            let info = parse_info(
                &Client::connect(p1.addr)
                    .await
                    .call_bulk(&["CLUSTER", "INFO"])
                    .await,
            );
            if info.get("cluster_state").map(String::as_str) != Some("ok")
                || !all_shards_synced(dr1.path()).await
            {
                return false;
            }
            let mut cr = Client::connect(r1.addr).await;
            if cr.call_simple(&["READONLY"]).await != "+OK" {
                return false;
            }
            cr.send(&["GET", &k_pre]).await;
            cr.line().await == "$16"
        },
    )
    .await;

    // p1 is still alive (NOT FAIL). Send CLUSTER FAILOVER FORCE to r1.
    let reply = Client::connect(r1.addr)
        .await
        .call_simple(&["CLUSTER", "FAILOVER", "FORCE"])
        .await;
    assert_eq!(reply, "+OK", "CLUSTER FAILOVER FORCE should succeed");

    // r1 immediately owns 0-5460 and serves writes.
    let k_post = key_in_range("force_post", 0, 5460);
    assert_eq!(
        Client::connect(r1.addr)
            .await
            .call_bulk(&["GET", &k_pre])
            .await,
        "val-before-force"
    );
    assert_eq!(
        Client::connect(r1.addr)
            .await
            .call_simple(&["SET", &k_post, "val-after-force"])
            .await,
        "+OK"
    );

    // Alive old primary p1 automatically demotes to a replica of r1 and resyncs.
    wait_for(
        "p1 demotes to replica of r1 and resyncs post-FORCE write",
        Duration::from_secs(15),
        || async {
            let mut c1 = Client::connect(p1.addr).await;
            let nodes = c1.call_bulk(&["CLUSTER", "NODES"]).await;
            let Some((flags, master, _, slots)) = node_entry(&nodes, "p1") else {
                return false;
            };
            if !flags.contains("slave") || master != "r1" || !slots.is_empty() {
                return false;
            }
            if c1.call_simple(&["READONLY"]).await != "+OK" {
                return false;
            }
            c1.send(&["GET", &k_post]).await;
            c1.line().await == "$15"
        },
    )
    .await;

    r1.stop().await;
    p3.stop().await;
    p2.stop().await;
    p1.stop().await;
}
