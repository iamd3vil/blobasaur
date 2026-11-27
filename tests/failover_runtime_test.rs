//! End-to-end runtime failover tests (step 2):
//! - 5-node cluster (3 primaries + 2 replicas): `CLUSTER FAILOVER TAKEOVER`,
//!   MOVED routing on other primaries, automatic replica re-pointing, automatic
//!   old-primary demotion (both alive throughout and restarted after stop) with
//!   unreplicated writes discarded, and restart reloading promoted state.
//! - `CLUSTER REPLICATE <node-id>` happy paths and refusals.
//! - `CLUSTER FAILOVER TAKEOVER` refused when not synced, plus default/FORCE errors.
//! - Crash-window recovery: persisted promotion without per-shard step completes on restart.
//! - go-redis `ClusterClient` surviving a runtime `TAKEOVER`.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::net::{SocketAddr, UdpSocket};
use std::path::Path;
use std::pin::Pin;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use blobasaur::cluster::{CLUSTER_STATE_FILE, ClusterManager, ClusterState, PersistedRole};
use blobasaur::config::{Cfg, ClusterConfig, ReplicationConfig, SlotRange};
use blobasaur::server;
use sqlx::{Connection, SqliteConnection};
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
    num_shards: usize,
) -> RunningNode {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let cfg = Cfg {
        data_dir: dir.to_string_lossy().into_owned(),
        num_shards,
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
        cleanup_interval_secs: Some(1),
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
        gossip_addr: format!("127.0.0.1:{gossip_port}"),
        shutdown,
        handle,
    }
}

#[derive(Debug, PartialEq)]
enum Reply {
    Int(i64),
    Text(String),
    Null,
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
                "$" if rest == "-1" => Reply::Null,
                "$" => {
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

async fn connect_db(dir: &Path, shard: usize) -> SqliteConnection {
    let url = format!("sqlite:{}/shard_{}.db", dir.display(), shard);
    SqliteConnection::connect(&url).await.unwrap()
}

async fn repl_state(dir: &Path, shard: usize, k: &str) -> Option<String> {
    sqlx::query_scalar("SELECT v FROM repl_state WHERE k = ?")
        .bind(k)
        .fetch_optional(&mut connect_db(dir, shard).await)
        .await
        .ok()
        .flatten()
}

async fn all_synced(dir: &Path, shards: usize) -> bool {
    for shard in 0..shards {
        if repl_state(dir, shard, "synced").await.as_deref() != Some("1") {
            return false;
        }
    }
    true
}

type Row = (String, Vec<u8>, i64, i64, Option<i64>, i64);

async fn snapshot(dir: &Path, shards: usize) -> BTreeSet<String> {
    let mut rows = BTreeSet::new();
    for shard in 0..shards {
        let mut conn = connect_db(dir, shard).await;
        let tables: Vec<String> = sqlx::query_scalar(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'blobs%'",
        )
        .fetch_all(&mut conn)
        .await
        .unwrap();
        for table in tables {
            let found: Vec<Row> = sqlx::query_as(&format!(
                "SELECT key, data, created_at, updated_at, expires_at, version FROM {table}"
            ))
            .fetch_all(&mut conn)
            .await
            .unwrap();
            for (key, data, created, updated, expires, version) in found {
                rows.insert(format!(
                    "{shard}/{table}/{key}/{data:?}/{created}/{updated}/{expires:?}/{version}"
                ));
            }
        }
    }
    rows
}

async fn shard_replids(dir: &Path, shards: usize) -> Vec<String> {
    let mut out = Vec::with_capacity(shards);
    for shard in 0..shards {
        out.push(repl_state(dir, shard, "replid").await.unwrap_or_default());
    }
    out
}

#[tokio::test]
async fn five_node_cluster_takeover_repoints_replica_and_demotes_live_primary() {
    const SHARDS: usize = 2;
    let d1 = TempDir::new().unwrap();
    let d2 = TempDir::new().unwrap();
    let d3 = TempDir::new().unwrap();
    let dr1 = TempDir::new().unwrap();
    let dr2 = TempDir::new().unwrap();

    let (gp1, gp2, gp3, gpr1, gpr2) = (
        free_udp_port(),
        free_udp_port(),
        free_udp_port(),
        free_udp_port(),
        free_udp_port(),
    );
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
                end: 5460
            }]),
            None,
            SHARDS,
        ),
        start_node(
            d2.path(),
            "p2",
            gp2,
            vec![g1.clone(), g3.clone()],
            Some(vec![SlotRange {
                start: 5461,
                end: 10922
            }]),
            None,
            SHARDS,
        ),
        start_node(
            d3.path(),
            "p3",
            gp3,
            vec![g1.clone(), g2.clone()],
            Some(vec![SlotRange {
                start: 10923,
                end: 16383
            }]),
            None,
            SHARDS,
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
            SHARDS,
        ),
        start_node(
            dr2.path(),
            "r2",
            gpr2,
            vec![g1.clone(), g2.clone(), g3.clone()],
            None,
            Some("p1"),
            SHARDS,
        ),
    );

    // Wait for all 5 nodes to converge on unique epochs and both replicas synced.
    wait_for("initial 5-node cluster convergence", || async {
        let mut c1 = Client::connect(p1.addr).await;
        let nodes = c1.call_bulk(&["CLUSTER", "NODES"]).await;
        let epochs = parse_nodes_epochs(&nodes);
        if epochs.len() != 5 {
            return false;
        }
        let primary_epochs: BTreeSet<u64> = ["p1", "p2", "p3"]
            .into_iter()
            .filter_map(|id| epochs.get(id).copied())
            .collect();
        primary_epochs.len() == 3
            && all_synced(dr1.path(), SHARDS).await
            && all_synced(dr2.path(), SHARDS).await
    })
    .await;

    // Default mode replies with a clear error; TAKEOVER and FORCE on a primary error.
    let mut cr1 = Client::connect(r1.addr).await;
    let err_default = cr1.call_simple(&["CLUSTER", "FAILOVER"]).await;
    assert!(
        err_default.starts_with("-ERR") && err_default.contains("not yet implemented"),
        "{err_default}"
    );
    let mut cp2 = Client::connect(p2.addr).await;
    let err_force_primary = cp2.call_simple(&["CLUSTER", "FAILOVER", "FORCE"]).await;
    assert!(
        err_force_primary.starts_with("-ERR") && err_force_primary.contains("replica"),
        "{err_force_primary}"
    );
    let err_primary = cp2.call_simple(&["CLUSTER", "FAILOVER", "TAKEOVER"]).await;
    assert!(
        err_primary.starts_with("-ERR") && err_primary.contains("replica"),
        "{err_primary}"
    );

    // Write keys to p1 and wait for both replicas to converge.
    let mut cp1 = Client::connect(p1.addr).await;
    let keys: Vec<String> = (0..10)
        .map(|i| key_in_range(&format!("pre{i}"), 0, 5460))
        .collect();
    for (i, key) in keys.iter().enumerate() {
        assert_eq!(
            cp1.call_simple(&["SET", key, &format!("val-{i}")]).await,
            "+OK"
        );
    }
    wait_for("r1 and r2 to copy initial writes", || async {
        let p1_snap = snapshot(d1.path(), SHARDS).await;
        snapshot(dr1.path(), SHARDS).await == p1_snap
            && snapshot(dr2.path(), SHARDS).await == p1_snap
    })
    .await;

    // Plant an unreplicated row directly in p1's shard 0 (simulating a write p1
    // held that never streamed to r1). Demotion + RESYNC must discard it.
    sqlx::query(
        "INSERT INTO blobs (key, data, created_at, updated_at) VALUES ('unreplicated_live', ?, 0, 0)",
    )
    .bind(&b"gone"[..])
    .execute(&mut connect_db(d1.path(), 0).await)
    .await
    .unwrap();

    let old_p1_replids = shard_replids(d1.path(), SHARDS).await;
    let r1_info_before = parse_info(&cr1.call_bulk(&["CLUSTER", "INFO"]).await);
    let epoch_before: u64 = r1_info_before["cluster_current_epoch"].parse().unwrap();

    // Promote r1 while p1 is still alive!
    assert_eq!(
        cr1.call_simple(&["CLUSTER", "FAILOVER", "TAKEOVER"]).await,
        "+OK"
    );

    // r1 now owns 0-5460 at a strictly higher epoch and generated new replids.
    let r1_info_after = parse_info(&cr1.call_bulk(&["CLUSTER", "INFO"]).await);
    let new_epoch: u64 = r1_info_after["cluster_my_epoch"].parse().unwrap();
    assert!(new_epoch > epoch_before, "{new_epoch} <= {epoch_before}");
    assert_eq!(
        r1_info_after["cluster_current_epoch"]
            .parse::<u64>()
            .unwrap(),
        new_epoch
    );
    let r1_replids = shard_replids(dr1.path(), SHARDS).await;
    for shard in 0..SHARDS {
        assert_ne!(r1_replids[shard], old_p1_replids[shard]);
        assert_eq!(
            repl_state(dr1.path(), shard, "role").await.as_deref(),
            Some("primary")
        );
    }

    // Reads and writes work directly on r1.
    for (i, key) in keys.iter().enumerate() {
        assert_eq!(cr1.call_bulk(&["GET", key]).await, format!("val-{i}"));
    }
    let post_key = key_in_range("post_takeover", 0, 5460);
    assert_eq!(
        cr1.call_simple(&["SET", &post_key, "after-win"]).await,
        "+OK"
    );
    assert_eq!(cr1.call_bulk(&["GET", &post_key]).await, "after-win");

    // p2 and p3 route slots 0-5460 to r1 (MOVED).
    let expected_moved = format!(
        "-MOVED {} {}",
        ClusterManager::calculate_slot(&post_key),
        r1.addr
    );
    wait_for("p2 and p3 to route slots 0-5460 to r1", || async {
        let m2 = Client::connect(p2.addr)
            .await
            .call_simple(&["GET", &post_key])
            .await;
        let m3 = Client::connect(p3.addr)
            .await
            .call_simple(&["GET", &post_key])
            .await;
        m2 == expected_moved && m3 == expected_moved
    })
    .await;

    // r2 re-points to r1, resyncs onto r1's new replids, and converges.
    // p1 (alive throughout) demotes to a replica of r1, resyncs, discards its
    // unreplicated row, and converges with r1.
    wait_for(
        "r2 to re-point and p1 to demote and resync to r1",
        || async {
            let r2_state = ClusterState::load(&dr2.path().join(CLUSTER_STATE_FILE), "r2").ok();
            let p1_state = ClusterState::load(&d1.path().join(CLUSTER_STATE_FILE), "p1").ok();
            let r2_ok = r2_state.is_some_and(|s| {
                s.role == PersistedRole::Replica && s.primary.as_deref() == Some("r1")
            }) && all_synced(dr2.path(), SHARDS).await
                && shard_replids(dr2.path(), SHARDS).await == r1_replids;
            let p1_ok = p1_state.is_some_and(|s| {
                s.role == PersistedRole::Replica && s.primary.as_deref() == Some("r1")
            }) && all_synced(d1.path(), SHARDS).await
                && shard_replids(d1.path(), SHARDS).await == r1_replids;
            r2_ok && p1_ok
        },
    )
    .await;

    let r1_snap = snapshot(dr1.path(), SHARDS).await;
    assert_eq!(snapshot(dr2.path(), SHARDS).await, r1_snap);
    assert_eq!(snapshot(d1.path(), SHARDS).await, r1_snap);
    assert!(
        !snapshot(d1.path(), SHARDS)
            .await
            .iter()
            .any(|row| row.contains("unreplicated_live")),
        "p1 kept unreplicated row after demotion resync"
    );

    // Demoted p1 redirects writes to r1, and serves READONLY reads from r1's data.
    assert_eq!(
        cp1.call_simple(&["SET", &post_key, "nope"]).await,
        expected_moved
    );
    assert_eq!(cp1.call_simple(&["READONLY"]).await, "+OK");
    assert_eq!(cp1.call_bulk(&["GET", &post_key]).await, "after-win");

    // Restart r1 with its original replica config: cluster-state.json wins and
    // reloads the promoted primary role, slots, and epoch.
    r1.stop().await;
    let r1_restarted = start_node(
        dr1.path(),
        "r1",
        gpr1,
        vec![g1.clone(), g2.clone(), g3.clone()],
        None,
        Some("p1"),
        SHARDS,
    )
    .await;
    let mut cr1 = Client::connect(r1_restarted.addr).await;
    let info_reloaded = parse_info(&cr1.call_bulk(&["CLUSTER", "INFO"]).await);
    assert_eq!(
        info_reloaded["cluster_my_epoch"].parse::<u64>().unwrap(),
        new_epoch
    );
    let nodes_reloaded = cr1.call_bulk(&["CLUSTER", "NODES"]).await;
    assert!(
        nodes_reloaded.contains(&format!(
            "r1 {}:{} myself,master - 0 0 {} connected 0-5460\n",
            r1_restarted.addr.ip(),
            r1_restarted.addr.port(),
            new_epoch
        )),
        "{nodes_reloaded}"
    );
    assert_eq!(cr1.call_bulk(&["GET", &post_key]).await, "after-win");
    let after_restart_key = key_in_range("after_restart", 0, 5460);
    assert_eq!(
        cr1.call_simple(&["SET", &after_restart_key, "works"]).await,
        "+OK"
    );

    r1_restarted.stop().await;
    r2.stop().await;
    p1.stop().await;
    p2.stop().await;
    p3.stop().await;
}

#[tokio::test]
async fn stopped_old_primary_with_unreplicated_writes_demotes_and_resyncs_on_restart() {
    const SHARDS: usize = 2;
    let d1 = TempDir::new().unwrap();
    let d2 = TempDir::new().unwrap();
    let d3 = TempDir::new().unwrap();
    let dr1 = TempDir::new().unwrap();
    let dr2 = TempDir::new().unwrap();

    let (gp1, gp2, gp3, gpr1, gpr2) = (
        free_udp_port(),
        free_udp_port(),
        free_udp_port(),
        free_udp_port(),
        free_udp_port(),
    );
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
                end: 5460
            }]),
            None,
            SHARDS,
        ),
        start_node(
            d2.path(),
            "p2",
            gp2,
            vec![g1.clone(), g3.clone()],
            Some(vec![SlotRange {
                start: 5461,
                end: 10922
            }]),
            None,
            SHARDS,
        ),
        start_node(
            d3.path(),
            "p3",
            gp3,
            vec![g1.clone(), g2.clone()],
            Some(vec![SlotRange {
                start: 10923,
                end: 16383
            }]),
            None,
            SHARDS,
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
            SHARDS,
        ),
        start_node(
            dr2.path(),
            "r2",
            gpr2,
            vec![g1.clone(), g2.clone(), g3.clone()],
            None,
            Some("p1"),
            SHARDS,
        ),
    );

    wait_for("replicas synced", || async {
        all_synced(dr1.path(), SHARDS).await
            && all_synced(dr2.path(), SHARDS).await
            && ClusterState::load(&dr1.path().join(CLUSTER_STATE_FILE), "r1")
                .is_ok_and(|s| s.slots.has_slots_of("p1"))
    })
    .await;

    let mut cp1 = Client::connect(p1.addr).await;
    let shared_key = key_in_range("shared", 0, 5460);
    assert_eq!(
        cp1.call_simple(&["SET", &shared_key, "before"]).await,
        "+OK"
    );
    wait_for("initial key replicated", || async {
        let snap = snapshot(d1.path(), SHARDS).await;
        snapshot(dr1.path(), SHARDS).await == snap && snapshot(dr2.path(), SHARDS).await == snap
    })
    .await;

    // Stop replicas, accept a real write on p1 that never streams, then stop p1.
    r1.stop().await;
    r2.stop().await;
    let lost_key = key_in_range("unreplicated_set", 0, 5460);
    assert_eq!(
        cp1.call_simple(&["SET", &lost_key, "never-replicated"])
            .await,
        "+OK"
    );
    p1.stop().await;

    // Restart r1 and r2 while p1 is down, then run TAKEOVER on r1.
    let (r1, r2) = tokio::join!(
        start_node(
            dr1.path(),
            "r1",
            gpr1,
            vec![g2.clone(), g3.clone()],
            None,
            Some("p1"),
            SHARDS,
        ),
        start_node(
            dr2.path(),
            "r2",
            gpr2,
            vec![g2.clone(), g3.clone()],
            None,
            Some("p1"),
            SHARDS,
        ),
    );
    let mut cr1 = Client::connect(r1.addr).await;
    assert_eq!(
        cr1.call_simple(&["CLUSTER", "FAILOVER", "TAKEOVER"]).await,
        "+OK"
    );
    let new_key = key_in_range("after_takeover", 0, 5460);
    assert_eq!(cr1.call_simple(&["SET", &new_key, "from-r1"]).await, "+OK");
    let r1_replids = shard_replids(dr1.path(), SHARDS).await;

    // Bring old primary p1 back up with its persisted state intact.
    let p1 = start_node(
        d1.path(),
        "p1",
        gp1,
        vec![g2.clone(), g3.clone()],
        Some(vec![SlotRange {
            start: 0,
            end: 5460,
        }]),
        None,
        SHARDS,
    )
    .await;

    // Both r2 and returning p1 converge as synced replicas of r1.
    wait_for("returning p1 and r2 to converge on r1", || async {
        let p1_state = ClusterState::load(&d1.path().join(CLUSTER_STATE_FILE), "p1").ok();
        let r2_state = ClusterState::load(&dr2.path().join(CLUSTER_STATE_FILE), "r2").ok();
        let p1_ok = p1_state.is_some_and(|s| {
            s.role == PersistedRole::Replica && s.primary.as_deref() == Some("r1")
        }) && all_synced(d1.path(), SHARDS).await
            && shard_replids(d1.path(), SHARDS).await == r1_replids;
        let r2_ok = r2_state.is_some_and(|s| {
            s.role == PersistedRole::Replica && s.primary.as_deref() == Some("r1")
        }) && all_synced(dr2.path(), SHARDS).await
            && shard_replids(dr2.path(), SHARDS).await == r1_replids;
        p1_ok && r2_ok
    })
    .await;

    // Unreplicated write on p1 is gone; p1 matches r1 verbatim.
    let r1_snap = snapshot(dr1.path(), SHARDS).await;
    assert_eq!(snapshot(d1.path(), SHARDS).await, r1_snap);
    assert_eq!(snapshot(dr2.path(), SHARDS).await, r1_snap);
    let mut cp1 = Client::connect(p1.addr).await;
    assert_eq!(cp1.call_simple(&["READONLY"]).await, "+OK");
    assert_eq!(cp1.tree(&["GET", &lost_key]).await, Reply::Null);
    assert_eq!(cp1.call_bulk(&["GET", &new_key]).await, "from-r1");
    assert_eq!(cp1.call_bulk(&["GET", &shared_key]).await, "before");

    p1.stop().await;
    r1.stop().await;
    r2.stop().await;
    p2.stop().await;
    p3.stop().await;
}

#[tokio::test]
async fn takeover_refused_when_replica_is_not_synced() {
    let dp = TempDir::new().unwrap();
    let dr = TempDir::new().unwrap();
    let (gp, gr) = (free_udp_port(), free_udp_port());

    let p = start_node(
        dp.path(),
        "p",
        gp,
        vec![],
        Some(vec![SlotRange {
            start: 0,
            end: 16383,
        }]),
        None,
        2,
    )
    .await;
    // Mismatched shard count (3 vs 2): primary refuses SYNC so r never syncs,
    // even though gossip delivers p's slots to r.
    let r = start_node(
        dr.path(),
        "r",
        gr,
        vec![p.gossip_addr.clone()],
        None,
        Some("p"),
        3,
    )
    .await;

    let mut cr = Client::connect(r.addr).await;
    wait_for("r to learn p's slots via gossip", || async {
        // Wait for both persisted state and in-memory slot table (the file
        // rename happens before `dir.sync_all()` returns to update memory).
        let persisted = ClusterState::load(&dr.path().join(CLUSTER_STATE_FILE), "r")
            .is_ok_and(|s| s.slots.has_slots_of("p"));
        if !persisted {
            return false;
        }
        let mut c = Client::connect(r.addr).await;
        c.call_bulk(&["CLUSTER", "NODES"]).await.contains("0-16383")
    })
    .await;

    let reply = cr.call_simple(&["CLUSTER", "FAILOVER", "TAKEOVER"]).await;
    assert!(
        reply.starts_with("-ERR") && reply.contains("not synced"),
        "expected not-synced refusal, got {reply}"
    );
    let saved = ClusterState::load(&dr.path().join(CLUSTER_STATE_FILE), "r").unwrap();
    assert_eq!(saved.role, PersistedRole::Replica);
    assert_eq!(saved.primary.as_deref(), Some("p"));

    r.stop().await;
    p.stop().await;
}

#[tokio::test]
async fn cluster_replicate_happy_path_and_refusals() {
    const SHARDS: usize = 2;
    let d1 = TempDir::new().unwrap();
    let d2 = TempDir::new().unwrap();
    let d_empty = TempDir::new().unwrap();
    let d_keys = TempDir::new().unwrap();
    let d_rep = TempDir::new().unwrap();

    let (gp1, gp2, gpe, gpk, gpr) = (
        free_udp_port(),
        free_udp_port(),
        free_udp_port(),
        free_udp_port(),
        free_udp_port(),
    );
    let g1 = format!("127.0.0.1:{gp1}");
    let g2 = format!("127.0.0.1:{gp2}");

    let (p1, p2, empty, has_keys) = tokio::join!(
        start_node(
            d1.path(),
            "p1",
            gp1,
            vec![g2.clone()],
            Some(vec![SlotRange {
                start: 0,
                end: 8000
            }]),
            None,
            SHARDS,
        ),
        start_node(
            d2.path(),
            "p2",
            gp2,
            vec![g1.clone()],
            Some(vec![SlotRange {
                start: 8001,
                end: 16382
            }]),
            None,
            SHARDS,
        ),
        start_node(
            d_empty.path(),
            "empty",
            gpe,
            vec![g1.clone(), g2.clone()],
            None,
            None,
            SHARDS,
        ),
        start_node(
            d_keys.path(),
            "has-keys",
            gpk,
            vec![g1.clone(), g2.clone()],
            Some(vec![SlotRange {
                start: 16383,
                end: 16383
            }]),
            None,
            SHARDS,
        ),
    );
    let rep = start_node(
        d_rep.path(),
        "rep",
        gpr,
        vec![g1.clone(), g2.clone()],
        None,
        Some("p1"),
        SHARDS,
    )
    .await;

    wait_for(
        "all nodes to discover each other and rep to sync",
        || async {
            let nodes = Client::connect(empty.addr)
                .await
                .call_bulk(&["CLUSTER", "NODES"])
                .await;
            nodes.contains("p1 ")
                && nodes.contains("p2 ")
                && nodes.contains("rep ")
                && nodes.contains("has-keys ")
                && all_synced(d_rep.path(), SHARDS).await
        },
    )
    .await;

    // Seed data on p1 and p2.
    let k1 = key_in_range("p1key", 0, 8000);
    let k2 = key_in_range("p2key", 8001, 16382);
    assert_eq!(
        Client::connect(p1.addr)
            .await
            .call_simple(&["SET", &k1, "from-p1"])
            .await,
        "+OK"
    );
    assert_eq!(
        Client::connect(p2.addr)
            .await
            .call_simple(&["SET", &k2, "from-p2"])
            .await,
        "+OK"
    );

    // Refusal: unknown node, self, target is replica.
    let mut c_empty = Client::connect(empty.addr).await;
    let err_unknown = c_empty
        .call_simple(&["CLUSTER", "REPLICATE", "no-such-node"])
        .await;
    assert!(err_unknown.contains("Unknown node"), "{err_unknown}");
    let err_self = c_empty
        .call_simple(&["CLUSTER", "REPLICATE", "empty"])
        .await;
    assert!(err_self.contains("myself"), "{err_self}");
    let err_replica = c_empty.call_simple(&["CLUSTER", "REPLICATE", "rep"]).await;
    assert!(err_replica.contains("not a replica"), "{err_replica}");

    // Refusal: primary owning slots.
    let err_slots = Client::connect(p1.addr)
        .await
        .call_simple(&["CLUSTER", "REPLICATE", "p2"])
        .await;
    assert_eq!(
        err_slots,
        "-ERR To set a master the node must be empty and without assigned slots."
    );

    // Refusal: primary with no slots, but holding a key.
    let mut c_keys = Client::connect(has_keys.addr).await;
    let k_16383 = key_in_range("slot16383", 16383, 16383);
    assert_eq!(c_keys.call_simple(&["SET", &k_16383, "data"]).await, "+OK");
    assert_eq!(
        c_keys.call_simple(&["CLUSTER", "DELSLOTS", "16383"]).await,
        "+OK"
    );
    let err_nonempty = c_keys.call_simple(&["CLUSTER", "REPLICATE", "p1"]).await;
    assert_eq!(
        err_nonempty,
        "-ERR To set a master the node must be empty and without assigned slots."
    );

    // Happy path 1: empty slotless primary becomes a replica of p1 and syncs.
    assert_eq!(
        c_empty.call_simple(&["CLUSTER", "REPLICATE", "p1"]).await,
        "+OK"
    );
    let saved_empty =
        ClusterState::load(&d_empty.path().join(CLUSTER_STATE_FILE), "empty").unwrap();
    assert_eq!(saved_empty.role, PersistedRole::Replica);
    assert_eq!(saved_empty.primary.as_deref(), Some("p1"));
    wait_for("empty to sync from p1", || async {
        all_synced(d_empty.path(), SHARDS).await
            && snapshot(d_empty.path(), SHARDS).await == snapshot(d1.path(), SHARDS).await
    })
    .await;
    assert_eq!(c_empty.call_simple(&["READONLY"]).await, "+OK");
    assert_eq!(c_empty.call_bulk(&["GET", &k1]).await, "from-p1");
    // Replicating the same primary again is idempotent.
    assert_eq!(
        c_empty.call_simple(&["CLUSTER", "REPLICATE", "p1"]).await,
        "+OK"
    );

    // Happy path 2: existing replica `rep` switches from p1 to p2.
    let mut c_rep = Client::connect(rep.addr).await;
    assert_eq!(
        c_rep.call_simple(&["CLUSTER", "REPLICATE", "p2"]).await,
        "+OK"
    );
    let saved_rep = ClusterState::load(&d_rep.path().join(CLUSTER_STATE_FILE), "rep").unwrap();
    assert_eq!(saved_rep.role, PersistedRole::Replica);
    assert_eq!(saved_rep.primary.as_deref(), Some("p2"));
    let p2_replids = shard_replids(d2.path(), SHARDS).await;
    wait_for("rep to resync from p2", || async {
        all_synced(d_rep.path(), SHARDS).await
            && shard_replids(d_rep.path(), SHARDS).await == p2_replids
            && snapshot(d_rep.path(), SHARDS).await == snapshot(d2.path(), SHARDS).await
    })
    .await;
    assert_eq!(c_rep.call_simple(&["READONLY"]).await, "+OK");
    assert_eq!(c_rep.call_bulk(&["GET", &k2]).await, "from-p2");

    rep.stop().await;
    has_keys.stop().await;
    empty.stop().await;
    p1.stop().await;
    p2.stop().await;
}

#[tokio::test]
async fn crash_window_after_promotion_persist_completes_on_restart() {
    const SHARDS: usize = 2;
    let dp = TempDir::new().unwrap();
    let dr = TempDir::new().unwrap();
    let (gp, gr) = (free_udp_port(), free_udp_port());

    let p = start_node(
        dp.path(),
        "p",
        gp,
        vec![],
        Some(vec![SlotRange {
            start: 0,
            end: 16383,
        }]),
        None,
        SHARDS,
    )
    .await;
    let r = start_node(
        dr.path(),
        "r",
        gr,
        vec![p.gossip_addr.clone()],
        None,
        Some("p"),
        SHARDS,
    )
    .await;

    let mut cp = Client::connect(p.addr).await;
    for i in 0..6 {
        assert_eq!(
            cp.call_simple(&["SET", &format!("k{i}"), &format!("v{i}")])
                .await,
            "+OK"
        );
    }
    wait_for("r synced with p", || async {
        all_synced(dr.path(), SHARDS).await
            && snapshot(dr.path(), SHARDS).await == snapshot(dp.path(), SHARDS).await
            && ClusterState::load(&dr.path().join(CLUSTER_STATE_FILE), "r")
                .is_ok_and(|s| s.slots.has_slots_of("p"))
    })
    .await;

    let old_replids = shard_replids(dr.path(), SHARDS).await;
    r.stop().await;
    p.stop().await;

    // Simulate a crash right after step 2 (cluster-state.json persisted with
    // role = primary and slots claimed at new epoch) and before step 3
    // (per-shard repl_state.role = primary and new replid).
    let state_path = dr.path().join(CLUSTER_STATE_FILE);
    let mut state = ClusterState::load(&state_path, "r").unwrap();
    let new_epoch = state.current_epoch + 1;
    state.current_epoch = new_epoch;
    state.config_epoch = new_epoch;
    state.role = PersistedRole::Primary;
    state.primary = None;
    for slot in state.slots.slots_of("p") {
        state.slots.set(slot, "r", new_epoch);
    }
    state.save_atomic(&state_path).unwrap();

    // Verify shards are still in replica state before restart.
    for shard in 0..SHARDS {
        assert_eq!(
            repl_state(dr.path(), shard, "role").await.as_deref(),
            Some("replica")
        );
        assert_eq!(
            repl_state(dr.path(), shard, "synced").await.as_deref(),
            Some("1")
        );
    }

    // Restart r (even with its original replica config): init_shard must finish
    // the promotion and generate new replids on every shard.
    let r = start_node(dr.path(), "r", gr, vec![], None, Some("p"), SHARDS).await;
    let new_replids = shard_replids(dr.path(), SHARDS).await;
    for shard in 0..SHARDS {
        assert_eq!(
            repl_state(dr.path(), shard, "role").await.as_deref(),
            Some("primary")
        );
        assert_ne!(new_replids[shard], old_replids[shard]);
    }
    let mut cr = Client::connect(r.addr).await;
    for i in 0..6 {
        assert_eq!(
            cr.call_bulk(&["GET", &format!("k{i}")]).await,
            format!("v{i}")
        );
    }
    assert_eq!(cr.call_simple(&["SET", "post-crash", "ok"]).await, "+OK");
    assert_eq!(cr.call_bulk(&["GET", "post-crash"]).await, "ok");

    r.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn go_redis_cluster_client_keeps_working_across_takeover() {
    if std::env::var_os("BLOBASAUR_SKIP_GO_TESTS").is_some() {
        eprintln!("BLOBASAUR_SKIP_GO_TESTS is set; skipping go-redis client tests");
        return;
    }
    const SHARDS: usize = 2;
    let d1 = TempDir::new().unwrap();
    let d2 = TempDir::new().unwrap();
    let d3 = TempDir::new().unwrap();
    let dr1 = TempDir::new().unwrap();

    let (gp1, gp2, gp3, gpr1) = (
        free_udp_port(),
        free_udp_port(),
        free_udp_port(),
        free_udp_port(),
    );
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
                end: 5460
            }]),
            None,
            SHARDS,
        ),
        start_node(
            d2.path(),
            "p2",
            gp2,
            vec![g1.clone(), g3.clone()],
            Some(vec![SlotRange {
                start: 5461,
                end: 10922
            }]),
            None,
            SHARDS,
        ),
        start_node(
            d3.path(),
            "p3",
            gp3,
            vec![g1.clone(), g2.clone()],
            Some(vec![SlotRange {
                start: 10923,
                end: 16383
            }]),
            None,
            SHARDS,
        ),
    );
    let r1 = start_node(
        dr1.path(),
        "r1",
        gpr1,
        vec![g1.clone(), g2.clone(), g3.clone()],
        None,
        Some("p1"),
        SHARDS,
    )
    .await;

    wait_for("cluster ready and r1 synced", || async {
        let info = parse_info(
            &Client::connect(p1.addr)
                .await
                .call_bulk(&["CLUSTER", "INFO"])
                .await,
        );
        info.get("cluster_state").map(String::as_str) == Some("ok")
            && all_synced(dr1.path(), SHARDS).await
            && ClusterState::load(&dr1.path().join(CLUSTER_STATE_FILE), "r1")
                .is_ok_and(|s| s.slots.has_slots_of("p1"))
    })
    .await;

    let slot_key = key_in_range("goredis_failover", 0, 5460);
    let addrs = format!("{},{},{}", p1.addr, p2.addr, p3.addr);

    let output = tokio::process::Command::new("go")
        .args([
            "test",
            "-count=1",
            "-run",
            "TestClusterClientSurvivesTakeover",
            "./...",
        ])
        .current_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/goredis"))
        .env("BLOBASAUR_CLUSTER_ADDRS", addrs)
        .env("BLOBASAUR_TAKEOVER_REPLICA", r1.addr.to_string())
        .env("BLOBASAUR_SLOT_KEY", &slot_key)
        .output()
        .await
        .unwrap_or_else(|e| {
            panic!("failed to run `go test` ({e}); set BLOBASAUR_SKIP_GO_TESTS=1 to skip")
        });
    assert!(
        output.status.success(),
        "go-redis failover test failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    r1.stop().await;
    p1.stop().await;
    p2.stop().await;
    p3.stop().await;
}

async fn exec_sql(dir: &Path, shard: usize, sql: &str) {
    let mut conn = connect_db(dir, shard).await;
    sqlx::query(sql).execute(&mut conn).await.unwrap();
}

#[tokio::test]
async fn demotion_retries_after_transient_failure_and_delslots_stays_primary() {
    const SHARDS: usize = 2;

    // Part 1: A primary that becomes slotless via CLUSTER DELSLOTS has no
    // demoting_to marker and stays a primary across gossip ticks.
    let d_solo = TempDir::new().unwrap();
    let gp_solo = free_udp_port();
    let solo = start_node(
        d_solo.path(),
        "solo",
        gp_solo,
        vec![],
        Some(vec![SlotRange { start: 0, end: 3 }]),
        None,
        SHARDS,
    )
    .await;
    let mut c_solo = Client::connect(solo.addr).await;
    assert_eq!(
        c_solo
            .call_simple(&["CLUSTER", "DELSLOTS", "0", "1", "2", "3"])
            .await,
        "+OK"
    );
    sleep(Duration::from_millis(350)).await;
    let solo_state = ClusterState::load(&d_solo.path().join(CLUSTER_STATE_FILE), "solo").unwrap();
    assert_eq!(solo_state.role, PersistedRole::Primary);
    assert_eq!(solo_state.demoting_to, None);
    assert!(!solo_state.slots.has_slots_of("solo"));
    solo.stop().await;

    // Part 2: Inject a per-shard failure on p1's shard 1 so the first demotion
    // attempt fails, then clear the failure and confirm a later gossip tick
    // completes demotion and resyncs p1 to r1.
    let dp = TempDir::new().unwrap();
    let dr = TempDir::new().unwrap();
    let (gp, gr) = (free_udp_port(), free_udp_port());

    let p1 = start_node(
        dp.path(),
        "p1",
        gp,
        vec![],
        Some(vec![SlotRange {
            start: 0,
            end: 16383,
        }]),
        None,
        SHARDS,
    )
    .await;
    let r1 = start_node(
        dr.path(),
        "r1",
        gr,
        vec![p1.gossip_addr.clone()],
        None,
        Some("p1"),
        SHARDS,
    )
    .await;

    let mut cp1 = Client::connect(p1.addr).await;
    assert_eq!(cp1.call_simple(&["SET", "before", "v1"]).await, "+OK");
    wait_for("r1 synced and learned p1 slots", || async {
        all_synced(dr.path(), SHARDS).await
            && snapshot(dr.path(), SHARDS).await == snapshot(dp.path(), SHARDS).await
            && ClusterState::load(&dr.path().join(CLUSTER_STATE_FILE), "r1")
                .is_ok_and(|s| s.slots.has_slots_of("p1"))
    })
    .await;

    // Fail `demote_shard` on p1's shard 1.
    exec_sql(
        dp.path(),
        1,
        "CREATE TRIGGER fail_demote BEFORE INSERT ON repl_state \
         WHEN NEW.k = 'role' AND NEW.v = 'replica' \
         BEGIN SELECT RAISE(ABORT, 'injected demote failure'); END",
    )
    .await;

    let mut cr1 = Client::connect(r1.addr).await;
    assert_eq!(
        cr1.call_simple(&["CLUSTER", "FAILOVER", "TAKEOVER"]).await,
        "+OK"
    );
    let r1_replids = shard_replids(dr.path(), SHARDS).await;

    // Wait until p1 persists the slot loss + demoting_to marker AND its first
    // demotion attempt has run (demoting shard 0 while failing on shard 1).
    wait_for(
        "p1 to persist demoting_to and fail shard 1 demotion",
        || async {
            let st = ClusterState::load(&dp.path().join(CLUSTER_STATE_FILE), "p1").ok();
            let state_ok = st.is_some_and(|s| {
                s.role == PersistedRole::Primary
                    && s.demoting_to.as_deref() == Some("r1")
                    && !s.slots.has_slots_of("p1")
            });
            state_ok
                && repl_state(dp.path(), 0, "role").await.as_deref() == Some("replica")
                && repl_state(dp.path(), 1, "role").await.as_deref() == Some("primary")
        },
    )
    .await;

    // While demotion is retrying, p1 must not serve writes for lost slots.
    let moved = cp1.call_simple(&["SET", "during_retry", "nope"]).await;
    assert!(
        moved.starts_with("-MOVED ") && moved.ends_with(&r1.addr.to_string()),
        "expected MOVED to r1 while demotion retries, got {moved}"
    );

    // Lift the injected failure; a subsequent gossip tick must finish demotion.
    exec_sql(dp.path(), 1, "DROP TRIGGER fail_demote").await;

    assert_eq!(cr1.call_simple(&["SET", "after", "v2"]).await, "+OK");
    wait_for("p1 demotion retry to succeed and resync to r1", || async {
        let st = ClusterState::load(&dp.path().join(CLUSTER_STATE_FILE), "p1").ok();
        st.is_some_and(|s| {
            s.role == PersistedRole::Replica
                && s.primary.as_deref() == Some("r1")
                && s.demoting_to.is_none()
        }) && all_synced(dp.path(), SHARDS).await
            && shard_replids(dp.path(), SHARDS).await == r1_replids
            && snapshot(dp.path(), SHARDS).await == snapshot(dr.path(), SHARDS).await
    })
    .await;

    assert_eq!(cp1.call_simple(&["READONLY"]).await, "+OK");
    assert_eq!(cp1.call_bulk(&["GET", "after"]).await, "v2");

    p1.stop().await;
    r1.stop().await;
}

#[tokio::test]
async fn demotion_crash_after_slot_loss_never_serves_lost_slots_and_finishes_on_restart() {
    const SHARDS: usize = 2;
    let dp = TempDir::new().unwrap();
    let dr = TempDir::new().unwrap();
    let (gp, gr) = (free_udp_port(), free_udp_port());

    let p1 = start_node(
        dp.path(),
        "p1",
        gp,
        vec![],
        Some(vec![SlotRange {
            start: 0,
            end: 16383,
        }]),
        None,
        SHARDS,
    )
    .await;
    let r1 = start_node(
        dr.path(),
        "r1",
        gr,
        vec![p1.gossip_addr.clone()],
        None,
        Some("p1"),
        SHARDS,
    )
    .await;

    wait_for("r1 synced and learned p1 slots", || async {
        all_synced(dr.path(), SHARDS).await
            && ClusterState::load(&dr.path().join(CLUSTER_STATE_FILE), "r1")
                .is_ok_and(|s| s.slots.has_slots_of("p1"))
    })
    .await;

    // Hold demotion mid-flight after slot loss + demoting_to persist and shard 0 demote.
    exec_sql(
        dp.path(),
        1,
        "CREATE TRIGGER fail_demote BEFORE INSERT ON repl_state \
         WHEN NEW.k = 'role' AND NEW.v = 'replica' \
         BEGIN SELECT RAISE(ABORT, 'injected demote failure'); END",
    )
    .await;

    let mut cr1 = Client::connect(r1.addr).await;
    assert_eq!(
        cr1.call_simple(&["CLUSTER", "FAILOVER", "TAKEOVER"]).await,
        "+OK"
    );
    assert_eq!(cr1.call_simple(&["SET", "winner_key", "ok"]).await, "+OK");
    let r1_replids = shard_replids(dr.path(), SHARDS).await;

    wait_for("p1 in crash window after slot loss", || async {
        let st = ClusterState::load(&dp.path().join(CLUSTER_STATE_FILE), "p1").ok();
        st.is_some_and(|s| {
            s.role == PersistedRole::Primary
                && s.demoting_to.as_deref() == Some("r1")
                && !s.slots.has_slots_of("p1")
        }) && repl_state(dp.path(), 0, "role").await.as_deref() == Some("replica")
            && repl_state(dp.path(), 1, "role").await.as_deref() == Some("primary")
    })
    .await;

    // Stop p1 before role change commits, then clear the injected failure.
    p1.stop().await;
    let pre_restart = ClusterState::load(&dp.path().join(CLUSTER_STATE_FILE), "p1").unwrap();
    assert_eq!(pre_restart.role, PersistedRole::Primary);
    assert_eq!(pre_restart.demoting_to.as_deref(), Some("r1"));
    assert!(!pre_restart.slots.has_slots_of("p1"));
    exec_sql(dp.path(), 1, "DROP TRIGGER fail_demote").await;

    // Restart p1 isolated (no seeds) with its original primary slot config:
    // it must never serve the lost slots and must finish demotion on startup.
    let p1_isolated = start_node(
        dp.path(),
        "p1",
        gp,
        vec![],
        Some(vec![SlotRange {
            start: 0,
            end: 16383,
        }]),
        None,
        SHARDS,
    )
    .await;
    let mut cp1 = Client::connect(p1_isolated.addr).await;
    let write_reply = cp1.call_simple(&["SET", "must_not_serve", "bad"]).await;
    assert!(
        write_reply.starts_with("-CLUSTERDOWN"),
        "restarted demoting node served lost slot locally: {write_reply}"
    );
    wait_for("isolated restarted p1 to finish demotion", || async {
        let st = ClusterState::load(&dp.path().join(CLUSTER_STATE_FILE), "p1").ok();
        st.is_some_and(|s| {
            s.role == PersistedRole::Replica
                && s.primary.as_deref() == Some("r1")
                && s.demoting_to.is_none()
        }) && repl_state(dp.path(), 0, "role").await.as_deref() == Some("replica")
            && repl_state(dp.path(), 1, "role").await.as_deref() == Some("replica")
    })
    .await;
    p1_isolated.stop().await;

    // Rejoin gossip and verify p1 resyncs cleanly from r1.
    let p1_rejoined = start_node(
        dp.path(),
        "p1",
        gp,
        vec![r1.gossip_addr.clone()],
        Some(vec![SlotRange {
            start: 0,
            end: 16383,
        }]),
        None,
        SHARDS,
    )
    .await;
    wait_for("rejoined p1 to sync from r1", || async {
        all_synced(dp.path(), SHARDS).await
            && shard_replids(dp.path(), SHARDS).await == r1_replids
            && snapshot(dp.path(), SHARDS).await == snapshot(dr.path(), SHARDS).await
    })
    .await;

    p1_rejoined.stop().await;
    r1.stop().await;
}

#[tokio::test]
async fn takeover_retries_transient_shard_promote_failure_without_serving_early() {
    const SHARDS: usize = 2;
    let dp = TempDir::new().unwrap();
    let dr = TempDir::new().unwrap();
    let (gp, gr) = (free_udp_port(), free_udp_port());

    let p = start_node(
        dp.path(),
        "p",
        gp,
        vec![],
        Some(vec![SlotRange {
            start: 0,
            end: 16383,
        }]),
        None,
        SHARDS,
    )
    .await;
    let r = start_node(
        dr.path(),
        "r",
        gr,
        vec![p.gossip_addr.clone()],
        None,
        Some("p"),
        SHARDS,
    )
    .await;

    let mut cp = Client::connect(p.addr).await;
    for i in 0..4 {
        assert_eq!(
            cp.call_simple(&["SET", &format!("k{i}"), &format!("v{i}")])
                .await,
            "+OK"
        );
    }
    wait_for("r synced with p", || async {
        all_synced(dr.path(), SHARDS).await
            && snapshot(dr.path(), SHARDS).await == snapshot(dp.path(), SHARDS).await
            && ClusterState::load(&dr.path().join(CLUSTER_STATE_FILE), "r")
                .is_ok_and(|s| s.slots.has_slots_of("p"))
    })
    .await;

    let old_replids = shard_replids(dr.path(), SHARDS).await;
    p.stop().await;

    // Fail `promote_shard` on r's shard 1 initially.
    exec_sql(
        dr.path(),
        1,
        "CREATE TRIGGER fail_promote BEFORE INSERT ON repl_state \
         WHEN NEW.k = 'role' AND NEW.v = 'primary' \
         BEGIN SELECT RAISE(ABORT, 'injected promote failure'); END",
    )
    .await;

    let r_addr = r.addr;
    let takeover_task = tokio::spawn(async move {
        let mut cr = Client::connect(r_addr).await;
        cr.call_simple(&["CLUSTER", "FAILOVER", "TAKEOVER"]).await
    });

    // Wait until cluster-state.json committed role=primary and shard 0 promoted
    // while shard 1 is still failing its retry loop.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let state_ok = ClusterState::load(&dr.path().join(CLUSTER_STATE_FILE), "r")
            .is_ok_and(|s| s.role == PersistedRole::Primary && s.slots.has_slots_of("r"));
        if state_ok
            && repl_state(dr.path(), 0, "role").await.as_deref() == Some("primary")
            && repl_state(dr.path(), 1, "role").await.as_deref() == Some("replica")
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for shard 0 promote during TAKEOVER retry"
        );
        sleep(Duration::from_millis(10)).await;
    }

    // Slots must NOT serve writes before shard 1 has its new replid.
    let mut probe = Client::connect(r.addr).await;
    let early = probe.call_simple(&["SET", "early_write", "nope"]).await;
    assert!(
        early.starts_with('-'),
        "r served write before every shard promoted: {early}"
    );

    // Clear the injected failure; a retry attempt must complete TAKEOVER.
    exec_sql(dr.path(), 1, "DROP TRIGGER fail_promote").await;

    let takeover_reply = timeout(Duration::from_secs(10), takeover_task)
        .await
        .expect("TAKEOVER timed out")
        .unwrap();
    assert_eq!(takeover_reply, "+OK");

    let new_replids = shard_replids(dr.path(), SHARDS).await;
    for shard in 0..SHARDS {
        assert_eq!(
            repl_state(dr.path(), shard, "role").await.as_deref(),
            Some("primary")
        );
        assert_ne!(new_replids[shard], old_replids[shard]);
    }
    assert_eq!(
        probe.call_simple(&["SET", "after_retry", "ok"]).await,
        "+OK"
    );
    assert_eq!(probe.call_bulk(&["GET", "after_retry"]).await, "ok");
    assert_eq!(probe.call_bulk(&["GET", "k0"]).await, "v0");

    r.stop().await;
}

#[tokio::test]
async fn takeover_persistent_shard_promote_failure_shuts_down_and_recovers_on_restart() {
    const SHARDS: usize = 2;
    let dp = TempDir::new().unwrap();
    let dr = TempDir::new().unwrap();
    let (gp, gr) = (free_udp_port(), free_udp_port());

    let p = start_node(
        dp.path(),
        "p",
        gp,
        vec![],
        Some(vec![SlotRange {
            start: 0,
            end: 16383,
        }]),
        None,
        SHARDS,
    )
    .await;
    let r = start_node(
        dr.path(),
        "r",
        gr,
        vec![p.gossip_addr.clone()],
        None,
        Some("p"),
        SHARDS,
    )
    .await;

    let mut cp = Client::connect(p.addr).await;
    for i in 0..4 {
        assert_eq!(
            cp.call_simple(&["SET", &format!("k{i}"), &format!("v{i}")])
                .await,
            "+OK"
        );
    }
    wait_for("r synced with p", || async {
        all_synced(dr.path(), SHARDS).await
            && snapshot(dr.path(), SHARDS).await == snapshot(dp.path(), SHARDS).await
            && ClusterState::load(&dr.path().join(CLUSTER_STATE_FILE), "r")
                .is_ok_and(|s| s.slots.has_slots_of("p"))
    })
    .await;

    let old_replids = shard_replids(dr.path(), SHARDS).await;

    // Keep p alive while failing shard 1 promotion persistently: r must never
    // roll back to replica or restart sync tasks against p; it must shut down
    // cleanly so restart finishes promotion via init_shard.
    exec_sql(
        dr.path(),
        1,
        "CREATE TRIGGER fail_promote BEFORE INSERT ON repl_state \
         WHEN NEW.k = 'role' AND NEW.v = 'primary' \
         BEGIN SELECT RAISE(ABORT, 'persistent promote failure'); END",
    )
    .await;

    let mut cr = Client::connect(r.addr).await;
    let reply = cr.call_simple(&["CLUSTER", "FAILOVER", "TAKEOVER"]).await;
    assert!(
        reply.starts_with("-ERR"),
        "expected error after retries exhausted, got {reply}"
    );
    drop(cr);

    // r must shut down cleanly on its own without external cancellation.
    let RunningNode { handle, .. } = r;
    timeout(Duration::from_secs(15), handle)
        .await
        .expect("r did not shut down after persistent post-commit TAKEOVER failure")
        .unwrap()
        .expect("r shutdown returned an error");

    // On disk: cluster-state.json stayed primary (never rolled back to replica),
    // shard 0 is promoted with a new replid, shard 1 is still replica/synced=1.
    let saved = ClusterState::load(&dr.path().join(CLUSTER_STATE_FILE), "r").unwrap();
    assert_eq!(saved.role, PersistedRole::Primary);
    assert_eq!(saved.primary, None);
    assert!(saved.slots.has_slots_of("r"));
    assert_eq!(
        repl_state(dr.path(), 0, "role").await.as_deref(),
        Some("primary")
    );
    assert_ne!(
        repl_state(dr.path(), 0, "replid").await.unwrap_or_default(),
        old_replids[0]
    );
    assert_eq!(
        repl_state(dr.path(), 1, "role").await.as_deref(),
        Some("replica")
    );
    assert_eq!(
        repl_state(dr.path(), 1, "synced").await.as_deref(),
        Some("1")
    );

    p.stop().await;
    exec_sql(dr.path(), 1, "DROP TRIGGER fail_promote").await;

    // Restart r (with its original replica config): init_shard completes promotion
    // of shard 1 and r serves both existing keys and new writes as primary.
    let r_restarted = start_node(dr.path(), "r", gr, vec![], None, Some("p"), SHARDS).await;
    let new_replids = shard_replids(dr.path(), SHARDS).await;
    for shard in 0..SHARDS {
        assert_eq!(
            repl_state(dr.path(), shard, "role").await.as_deref(),
            Some("primary")
        );
        assert_ne!(new_replids[shard], old_replids[shard]);
    }
    let mut cr = Client::connect(r_restarted.addr).await;
    for i in 0..4 {
        assert_eq!(
            cr.call_bulk(&["GET", &format!("k{i}")]).await,
            format!("v{i}")
        );
    }
    assert_eq!(cr.call_simple(&["SET", "recovered", "yes"]).await, "+OK");
    assert_eq!(cr.call_bulk(&["GET", "recovered"]).await, "yes");

    r_restarted.stop().await;
}
