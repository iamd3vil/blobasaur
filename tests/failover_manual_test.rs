//! End-to-end integration tests for coordinated zero-loss `CLUSTER FAILOVER`
//! (default mode, failover step 5):
//! - Continuous sync and `async_write` writes during `CLUSTER FAILOVER`:
//!   writes paused on the old primary complete via `MOVED` without failing,
//!   zero acknowledged writes are lost, and the old primary demotes and resyncs.
//! - Abort path: stalled sync prevents `caught_up_lsn` catch-up so the failover
//!   times out at ~5 s without starting an election, reads continue during the
//!   pause, and paused writes resume on the original primary once the pause expires.
//! - Refusals: on a primary, on an unsynced replica, and while another manual
//!   failover is already running.
//! - go-redis `ClusterClient` surviving a coordinated `CLUSTER FAILOVER`.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::net::{SocketAddr, UdpSocket};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use blobasaur::cluster::ClusterManager;
use blobasaur::config::{Cfg, ClusterConfig, ReplicationConfig, SlotRange};
use blobasaur::server;
use sqlx::{Connection, SqliteConnection};
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::process::Command as TokioCommand;
use tokio::sync::watch;
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

fn keys_in_range(prefix: &str, count: usize, start: u16, end: u16) -> Vec<String> {
    let mut keys = Vec::with_capacity(count);
    for i in 0u32.. {
        let k = format!("{prefix}_{i}");
        let slot = ClusterManager::calculate_slot(&k);
        if slot >= start && slot <= end {
            keys.push(k);
            if keys.len() == count {
                return keys;
            }
        }
    }
    unreachable!()
}

struct RunningNode {
    addr: SocketAddr,
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

#[allow(clippy::too_many_arguments)]
async fn start_node(
    dir: &Path,
    node_id: &str,
    gossip_port: u16,
    seeds: Vec<String>,
    slot_ranges: Option<Vec<SlotRange>>,
    replica_of: Option<&str>,
    async_write: bool,
    advertise_override: Option<SocketAddr>,
) -> RunningNode {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let advertise = advertise_override.unwrap_or(addr);

    let cfg = Cfg {
        data_dir: dir.to_string_lossy().into_owned(),
        num_shards: SHARDS,
        storage_compression: None,
        async_write: Some(async_write),
        batch_size: Some(if async_write { 100 } else { 1 }),
        batch_timeout_ms: Some(if async_write { 10 } else { 0 }),
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
            advertise_addr: Some(advertise.to_string()),
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

    async fn send(&mut self, args: &[&str]) -> std::io::Result<()> {
        let mut req = format!("*{}\r\n", args.len());
        for arg in args {
            req.push_str(&format!("${}\r\n{}\r\n", arg.len(), arg));
        }
        self.0.get_mut().write_all(req.as_bytes()).await
    }

    async fn line(&mut self) -> std::io::Result<String> {
        let mut line = String::new();
        if self.0.read_line(&mut line).await? == 0 {
            return Err(std::io::ErrorKind::UnexpectedEof.into());
        }
        Ok(line.trim_end().to_string())
    }

    async fn try_call_simple(&mut self, args: &[&str]) -> std::io::Result<String> {
        self.send(args).await?;
        let line = self.line().await?;
        if let Some(rest) = line.strip_prefix('$')
            && rest != "-1"
            && let Ok(len) = rest.parse::<usize>()
        {
            let mut buf = vec![0; len + 2];
            self.0.read_exact(&mut buf).await?;
        }
        Ok(line)
    }

    async fn call_simple(&mut self, args: &[&str]) -> String {
        self.try_call_simple(args).await.unwrap()
    }

    async fn call_bulk(&mut self, args: &[&str]) -> String {
        self.send(args).await.unwrap();
        let header = self.line().await.unwrap();
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

/// True once `p1` reports `cluster_state:ok`, every listed node sees `p1` as
/// master of `0-5460` and `r1` as a replica of `p1` in `CLUSTER NODES`, and
/// every shard on `r1` has completed initial sync.
async fn cluster_ready_for_manual_failover(
    p1_addr: SocketAddr,
    addrs: [SocketAddr; 4],
    r1_dir: &Path,
) -> bool {
    let info = parse_info(
        &Client::connect(p1_addr)
            .await
            .call_bulk(&["CLUSTER", "INFO"])
            .await,
    );
    if info.get("cluster_state").map(String::as_str) != Some("ok") {
        return false;
    }
    for addr in addrs {
        let nodes = Client::connect(addr)
            .await
            .call_bulk(&["CLUSTER", "NODES"])
            .await;
        let p1_ok = node_entry(&nodes, "p1")
            .is_some_and(|(flags, _, _, slots)| flags.contains("master") && slots == "0-5460");
        let r1_ok = node_entry(&nodes, "r1").is_some_and(|(flags, master, _, slots)| {
            flags.contains("slave") && master == "p1" && slots.is_empty()
        });
        if !p1_ok || !r1_ok {
            return false;
        }
    }
    all_shards_synced(r1_dir).await
}

/// Protocol-aware TCP proxy for the abort test: forwards `BLOBASAUR.MFSTART` and
/// other commands normally, while stalling `BLOBASAUR.SYNC` streams whenever
/// `stall_sync` is set to `true`.
struct SyncStallProxy {
    addr: SocketAddr,
    stall_sync: watch::Sender<bool>,
    shutdown: CancellationToken,
}

impl SyncStallProxy {
    async fn start(upstream: SocketAddr) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (stall_sync, rx) = watch::channel(false);
        let shutdown = CancellationToken::new();
        let cancel = shutdown.clone();
        tokio::spawn(async move {
            loop {
                let accepted = tokio::select! {
                    _ = cancel.cancelled() => return,
                    res = listener.accept() => res,
                };
                let Ok((client, _)) = accepted else {
                    return;
                };
                let rx = rx.clone();
                let cancel = cancel.clone();
                tokio::spawn(async move {
                    Self::handle(client, upstream, rx, cancel).await;
                });
            }
        });
        Self {
            addr,
            stall_sync,
            shutdown,
        }
    }

    fn set_stall_sync(&self, stall: bool) {
        self.stall_sync.send_replace(stall);
    }

    async fn handle(
        mut client: TcpStream,
        upstream: SocketAddr,
        rx: watch::Receiver<bool>,
        cancel: CancellationToken,
    ) {
        let mut first = vec![0u8; 4096];
        let n = tokio::select! {
            _ = cancel.cancelled() => return,
            res = client.read(&mut first) => match res {
                Ok(n) if n > 0 => n,
                _ => return,
            },
        };
        let is_sync = String::from_utf8_lossy(&first[..n])
            .to_ascii_uppercase()
            .contains("BLOBASAUR.SYNC");

        let Ok(mut up) = TcpStream::connect(upstream).await else {
            return;
        };
        if up.write_all(&first[..n]).await.is_err() {
            return;
        }
        let (cr, cw) = client.into_split();
        let (ur, uw) = up.into_split();
        tokio::select! {
            _ = cancel.cancelled() => {}
            _ = Self::pump(cr, uw, is_sync, rx.clone()) => {}
            _ = Self::pump(ur, cw, is_sync, rx) => {}
        }
    }

    async fn pump(
        mut from: tokio::net::tcp::OwnedReadHalf,
        mut to: tokio::net::tcp::OwnedWriteHalf,
        is_sync: bool,
        mut stall_rx: watch::Receiver<bool>,
    ) {
        let mut buf = vec![0u8; 16 * 1024];
        loop {
            loop {
                let stalled = is_sync && *stall_rx.borrow_and_update();
                if !stalled {
                    break;
                }
                if stall_rx.changed().await.is_err() {
                    return;
                }
            }
            tokio::select! {
                changed = stall_rx.changed(), if is_sync => {
                    if changed.is_err() {
                        return;
                    }
                }
                res = from.read(&mut buf) => match res {
                    Ok(n) if n > 0 => {
                        if to.write_all(&buf[..n]).await.is_err() {
                            return;
                        }
                    }
                    _ => return,
                },
            }
        }
    }
}

impl Drop for SyncStallProxy {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

/// Write a single command following `-MOVED <slot> <addr>` redirects until it
/// succeeds. Returns `(final_addr, saw_moved)`.
async fn write_following_moved(
    conn: &mut Client,
    current_addr: &mut SocketAddr,
    args: &[&str],
) -> Result<bool, String> {
    let mut saw_moved = false;
    for _ in 0..10 {
        let reply = conn
            .try_call_simple(args)
            .await
            .map_err(|e| format!("I/O error on {current_addr}: {e}"))?;
        if reply == "+OK" || reply.starts_with(':') {
            return Ok(saw_moved);
        }
        if let Some(rest) = reply.strip_prefix("-MOVED ") {
            let new_addr: SocketAddr = rest
                .split_whitespace()
                .nth(1)
                .ok_or_else(|| format!("malformed MOVED: {reply}"))?
                .parse()
                .map_err(|e| format!("bad MOVED addr in {reply}: {e}"))?;
            *current_addr = new_addr;
            *conn = Client::connect(new_addr).await;
            saw_moved = true;
            continue;
        }
        return Err(format!("unexpected reply for {args:?}: {reply}"));
    }
    Err(format!("too many MOVED redirects for {args:?}"))
}

async fn run_manual_failover_under_writes(async_write: bool) {
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
            async_write,
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
            async_write,
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
            async_write,
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
        async_write,
        None,
    )
    .await;

    wait_for(
        "cluster ok and r1 synced before manual failover",
        Duration::from_secs(15),
        || {
            cluster_ready_for_manual_failover(
                p1.addr,
                [p1.addr, p2.addr, p3.addr, r1.addr],
                dr1.path(),
            )
        },
    )
    .await;

    // Continuous writes from a client following MOVED, covering both SET and HSET.
    let stop = Arc::new(AtomicBool::new(false));
    let stop_w = stop.clone();
    let p1_addr = p1.addr;
    let write_keys = keys_in_range("mf_k", 300, 0, 5460);
    let writer = tokio::spawn(async move {
        let mut target = p1_addr;
        let mut conn = Client::connect(target).await;
        let mut acked: BTreeMap<String, String> = BTreeMap::new();
        let mut moved_count = 0usize;
        let mut i = 0usize;
        while !stop_w.load(Ordering::Relaxed) {
            let key = &write_keys[i % write_keys.len()];
            let val = format!("v-{i}");
            let saw_moved = write_following_moved(&mut conn, &mut target, &["SET", key, &val])
                .await
                .expect("write during manual failover should succeed (via MOVED), not fail");
            if saw_moved {
                moved_count += 1;
            }
            acked.insert(key.clone(), val);
            i += 1;
            if i.is_multiple_of(5) {
                sleep(Duration::from_millis(1)).await;
            }
        }
        (acked, moved_count, i, target)
    });

    // Let writes accumulate before triggering CLUSTER FAILOVER.
    sleep(Duration::from_millis(150)).await;

    // Default CLUSTER FAILOVER returns +OK immediately and runs in the background.
    let t_cmd = Instant::now();
    let reply = Client::connect(r1.addr)
        .await
        .call_simple(&["CLUSTER", "FAILOVER"])
        .await;
    assert_eq!(reply, "+OK", "CLUSTER FAILOVER must reply +OK immediately");
    assert!(
        t_cmd.elapsed() < Duration::from_secs(1),
        "CLUSTER FAILOVER should reply immediately, took {:?}",
        t_cmd.elapsed()
    );

    // Wait for r1 to own 0-5460 and p1 to demote to a synced replica of r1.
    wait_for(
        "r1 promoted and p1 demoted & resynced from r1",
        Duration::from_secs(15),
        || async {
            let nodes_r1 = Client::connect(r1.addr)
                .await
                .call_bulk(&["CLUSTER", "NODES"])
                .await;
            let Some((r1_flags, _, _, r1_slots)) = node_entry(&nodes_r1, "r1") else {
                return false;
            };
            if !r1_flags.contains("master") || r1_slots != "0-5460" {
                return false;
            }
            let nodes_p1 = Client::connect(p1.addr)
                .await
                .call_bulk(&["CLUSTER", "NODES"])
                .await;
            let Some((p1_flags, p1_master, _, p1_slots)) = node_entry(&nodes_p1, "p1") else {
                return false;
            };
            p1_flags.contains("slave")
                && p1_master == "r1"
                && p1_slots.is_empty()
                && all_shards_synced(d1.path()).await
        },
    )
    .await;

    // Keep writing briefly after failover so post-failover writes land on r1 too.
    sleep(Duration::from_millis(150)).await;
    stop.store(true, Ordering::Relaxed);
    let (acked, moved_count, total_ops, final_target) = writer.await.unwrap();
    assert!(
        total_ops > 20,
        "expected continuous writes, got {total_ops}"
    );
    assert!(
        moved_count >= 1,
        "client should have been redirected via MOVED to r1 at least once"
    );
    assert_eq!(
        final_target, r1.addr,
        "client should end up talking to promoted r1"
    );

    // Verify manual failover stats on r1.
    let r1_info = parse_info(
        &Client::connect(r1.addr)
            .await
            .call_bulk(&["CLUSTER", "INFO"])
            .await,
    );
    assert_eq!(
        r1_info
            .get("cluster_stats_manual_failover_started")
            .map(String::as_str),
        Some("1")
    );
    assert_eq!(
        r1_info
            .get("cluster_stats_manual_failover_succeeded")
            .map(String::as_str),
        Some("1")
    );
    assert_eq!(
        r1_info
            .get("cluster_stats_manual_failover_aborted")
            .map(String::as_str),
        Some("0")
    );

    // Zero loss: every single acknowledged write must be readable with its exact
    // final value directly from r1.
    let mut cr1 = Client::connect(r1.addr).await;
    for (k, expected) in &acked {
        let got = cr1.call_bulk(&["GET", k]).await;
        assert_eq!(
            &got, expected,
            "acknowledged write for {k} lost or stale on r1 (async_write={async_write})"
        );
    }

    r1.stop().await;
    p3.stop().await;
    p2.stop().await;
    p1.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn manual_failover_zero_loss_sync_writes() {
    run_manual_failover_under_writes(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn manual_failover_zero_loss_async_writes() {
    run_manual_failover_under_writes(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn manual_failover_abort_on_stalled_catchup_and_in_progress_refusal() {
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

    // Start p1 on a known local TCP address, fronted by SyncStallProxy for gossip's advertise_addr.
    let p1_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let p1_addr = p1_listener.local_addr().unwrap();
    let proxy = SyncStallProxy::start(p1_addr).await;

    let p1_cfg = Cfg {
        data_dir: d1.path().to_string_lossy().into_owned(),
        num_shards: SHARDS,
        storage_compression: None,
        async_write: Some(false),
        batch_size: Some(1),
        batch_timeout_ms: Some(0),
        write_queue_capacity: None,
        addr: Some(p1_addr.to_string()),
        cluster: Some(ClusterConfig {
            enabled: true,
            node_id: "p1".to_string(),
            seeds: vec![g2.clone(), g3.clone()],
            port: gp1,
            slots: None,
            slot_ranges: Some(vec![SlotRange {
                start: 0,
                end: 5460,
            }]),
            gossip_interval_ms: Some(100),
            advertise_addr: Some(proxy.addr.to_string()),
            node_timeout_ms: Some(NODE_TIMEOUT_MS),
            replica_validity_factor: None,
        }),
        replication: Some(ReplicationConfig {
            replica_of: None,
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
    let p1_shutdown = CancellationToken::new();
    let p1_handle = tokio::spawn(server::run(p1_cfg, p1_listener, p1_shutdown.clone()));
    let p1 = RunningNode {
        addr: p1_addr,
        shutdown: p1_shutdown,
        handle: p1_handle,
    };

    let (p2, p3) = tokio::join!(
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
            false,
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
            false,
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
        false,
        None,
    )
    .await;

    let k_read = key_in_range("abort_read", 0, 5460);
    assert_eq!(
        Client::connect(p1.addr)
            .await
            .call_simple(&["SET", &k_read, "readable"])
            .await,
        "+OK"
    );

    wait_for(
        "cluster ok and r1 synced through proxy",
        Duration::from_secs(15),
        || {
            cluster_ready_for_manual_failover(
                p1.addr,
                [p1.addr, p2.addr, p3.addr, r1.addr],
                dr1.path(),
            )
        },
    )
    .await;

    // Stall BLOBASAUR.SYNC streams (while still forwarding BLOBASAUR.MFSTART),
    // then advance p1's LSN so r1 cannot catch up.
    proxy.set_stall_sync(true);
    let k_behind = key_in_range("abort_behind", 0, 5460);
    assert_eq!(
        Client::connect(p1.addr)
            .await
            .call_simple(&["SET", &k_behind, "unreplicated"])
            .await,
        "+OK"
    );

    // Trigger CLUSTER FAILOVER on r1: accepted immediately.
    let started = Instant::now();
    assert_eq!(
        Client::connect(r1.addr)
            .await
            .call_simple(&["CLUSTER", "FAILOVER"])
            .await,
        "+OK"
    );

    // Second CLUSTER FAILOVER while the first is running must be refused.
    let second = Client::connect(r1.addr)
        .await
        .call_simple(&["CLUSTER", "FAILOVER"])
        .await;
    assert!(
        second.starts_with("-ERR") && second.contains("already in progress"),
        "expected already-in-progress refusal, got {second}"
    );

    // Give MFSTART a moment to pause writes on p1.
    sleep(Duration::from_millis(150)).await;

    // Reads on p1 must continue uninterrupted during the write pause.
    let read_val = timeout(
        Duration::from_secs(1),
        Client::connect(p1.addr).await.call_bulk(&["GET", &k_read]),
    )
    .await
    .expect("GET on paused primary should not block");
    assert_eq!(read_val, "readable");

    // A write on p1's slot must block until the ~5 s pause expires, then succeed on p1.
    let k_paused = key_in_range("abort_paused", 0, 5460);
    let p1_addr_copy = p1.addr;
    let k_paused_copy = k_paused.clone();
    let paused_write = tokio::spawn(async move {
        let t0 = Instant::now();
        let reply = Client::connect(p1_addr_copy)
            .await
            .call_simple(&["SET", &k_paused_copy, "after-pause"])
            .await;
        (reply, t0.elapsed())
    });

    // Must still be paused after 400 ms, and a stale MFABORT from another replica
    // must NOT release p1's pause.
    sleep(Duration::from_millis(400)).await;
    assert!(
        !paused_write.is_finished(),
        "write on paused primary should remain blocked while manual failover is active"
    );
    assert_eq!(
        Client::connect(p1.addr)
            .await
            .call_simple(&["BLOBASAUR.MFABORT", "other-replica"])
            .await,
        "+OK"
    );
    sleep(Duration::from_millis(300)).await;
    assert!(
        !paused_write.is_finished(),
        "stale MFABORT from another replica must not release p1's write pause"
    );

    // Wait for r1's 5 s timeout: r1 aborts and sends MFABORT to release p1's
    // 10 s pause promptly, so the paused write completes well before 10 s.
    let (write_reply, write_elapsed) = timeout(Duration::from_secs(8), paused_write)
        .await
        .expect("paused write did not resume promptly via MFABORT after r1's 5 s timeout")
        .unwrap();
    assert_eq!(
        write_reply, "+OK",
        "paused write should complete on p1 after MFABORT releases the pause"
    );
    assert!(
        write_elapsed >= Duration::from_secs(3),
        "write resumed too early ({write_elapsed:?}); pause may not have held"
    );
    assert!(
        started.elapsed() < Duration::from_secs(8),
        "MFABORT should release the 10 s pause near 5 s, took {:?}",
        started.elapsed()
    );

    // Verify r1 aborted without starting an election, and p1 remained primary.
    wait_for(
        "r1 records aborted manual failover",
        Duration::from_secs(5),
        || async {
            let info = parse_info(
                &Client::connect(r1.addr)
                    .await
                    .call_bulk(&["CLUSTER", "INFO"])
                    .await,
            );
            info.get("cluster_stats_manual_failover_aborted")
                .map(String::as_str)
                == Some("1")
        },
    )
    .await;

    let r1_info = parse_info(
        &Client::connect(r1.addr)
            .await
            .call_bulk(&["CLUSTER", "INFO"])
            .await,
    );
    assert_eq!(
        r1_info.get("cluster_elections_started").map(String::as_str),
        Some("0"),
        "aborted manual failover must not start an election after timeout: {r1_info:?}"
    );
    assert_eq!(
        r1_info
            .get("cluster_stats_manual_failover_succeeded")
            .map(String::as_str),
        Some("0")
    );

    let nodes = Client::connect(p1.addr)
        .await
        .call_bulk(&["CLUSTER", "NODES"])
        .await;
    let p1_entry = node_entry(&nodes, "p1").unwrap();
    let r1_entry = node_entry(&nodes, "r1").unwrap();
    assert!(
        p1_entry.0.contains("master") && p1_entry.3 == "0-5460",
        "p1 must remain primary: {nodes}"
    );
    assert!(
        r1_entry.0.contains("slave") && r1_entry.1 == "p1",
        "r1 must remain replica of p1: {nodes}"
    );
    assert_eq!(
        Client::connect(p1.addr)
            .await
            .call_bulk(&["GET", &k_paused])
            .await,
        "after-pause"
    );

    proxy.set_stall_sync(false);
    r1.stop().await;
    p3.stop().await;
    p2.stop().await;
    p1.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn manual_failover_refuses_primary_and_unsynced_replica() {
    let d1 = TempDir::new().unwrap();
    let d2 = TempDir::new().unwrap();
    let d3 = TempDir::new().unwrap();
    let dr_unsynced = TempDir::new().unwrap();

    let gp1 = free_udp_port();
    let gp2 = free_udp_port();
    let gp3 = free_udp_port();
    let gpr = free_udp_port();
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
            false,
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
            false,
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
            false,
            None,
        ),
    );

    wait_for("3-primary cluster ok", Duration::from_secs(15), || async {
        let info = parse_info(
            &Client::connect(p2.addr)
                .await
                .call_bulk(&["CLUSTER", "INFO"])
                .await,
        );
        info.get("cluster_state").map(String::as_str) == Some("ok")
    })
    .await;

    // 1) Refused on a primary.
    let on_primary = Client::connect(p1.addr)
        .await
        .call_simple(&["CLUSTER", "FAILOVER"])
        .await;
    assert!(
        on_primary.starts_with("-ERR") && on_primary.contains("replica"),
        "expected primary refusal, got {on_primary}"
    );

    // 2) Stop p1 before starting r_unsynced so r_unsynced knows p1's slots from p2/p3
    //    but has synced = 0 on every shard.
    p1.stop().await;
    let r_unsynced = start_node(
        dr_unsynced.path(),
        "r_unsynced",
        gpr,
        vec![g2.clone(), g3.clone()],
        None,
        Some("p1"),
        false,
        None,
    )
    .await;

    wait_for(
        "r_unsynced learns p1's slots from p2/p3",
        Duration::from_secs(10),
        || async {
            let nodes = Client::connect(r_unsynced.addr)
                .await
                .call_bulk(&["CLUSTER", "NODES"])
                .await;
            node_entry(&nodes, "p1").is_some_and(|(_, _, _, slots)| slots == "0-5460")
        },
    )
    .await;

    let on_unsynced = Client::connect(r_unsynced.addr)
        .await
        .call_simple(&["CLUSTER", "FAILOVER"])
        .await;
    assert!(
        on_unsynced.starts_with("-ERR") && on_unsynced.contains("not synced"),
        "expected unsynced refusal, got {on_unsynced}"
    );

    r_unsynced.stop().await;
    p3.stop().await;
    p2.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn goredis_cluster_client_survives_manual_failover() {
    if std::env::var_os("BLOBASAUR_SKIP_GO_TESTS").is_some() {
        eprintln!("BLOBASAUR_SKIP_GO_TESTS is set; skipping go-redis manual failover test");
        return;
    }

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
            true,
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
            true,
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
            true,
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
        true,
        None,
    )
    .await;

    wait_for(
        "cluster ok and r1 synced for go-redis manual failover",
        Duration::from_secs(15),
        || {
            cluster_ready_for_manual_failover(
                p1.addr,
                [p1.addr, p2.addr, p3.addr, r1.addr],
                dr1.path(),
            )
        },
    )
    .await;

    let slot_key = key_in_range("gomf", 0, 5460);
    let addrs = format!("{},{},{}", p1.addr, p2.addr, p3.addr);
    let output = TokioCommand::new("go")
        .args([
            "test",
            "-count=1",
            "-run",
            "^TestClusterClientSurvivesManualFailover$",
            "./...",
        ])
        .current_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/goredis"))
        .env("BLOBASAUR_CLUSTER_ADDRS", &addrs)
        .env("BLOBASAUR_MANUAL_REPLICA", r1.addr.to_string())
        .env("BLOBASAUR_SLOT_KEY", &slot_key)
        .output()
        .await
        .expect("failed to run go test");

    assert!(
        output.status.success(),
        "go-redis manual failover test failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    r1.stop().await;
    p3.stop().await;
    p2.stop().await;
    p1.stop().await;
}

/// Regression test for the gap between replica promotion and late gossip claim
/// propagation to the old primary:
/// - `p1` runs with a 6.5 s `gossip_interval_ms`, so after `r1` promotes it
///   takes > 5 s (past `CLUSTER_MF_TIMEOUT`, inside the 10 s primary pause)
///   for `p1` to observe `r1`'s higher-epoch claim and demote.
/// - A writer hammers `p1` continuously throughout the failover.
/// - Because `p1` stays paused for `2 * CLUSTER_MF_TIMEOUT` (10 s) until it
///   demotes, no write slips into `p1` after 5 s to be discarded by RESYNC:
///   every acknowledged write is readable from `r1` afterwards.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn manual_failover_zero_loss_when_claim_reaches_old_primary_late() {
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

    #[allow(clippy::too_many_arguments)]
    async fn start_with_timing(
        dir: &Path,
        node_id: &str,
        gossip_port: u16,
        seeds: Vec<String>,
        slot_ranges: Option<Vec<SlotRange>>,
        replica_of: Option<&str>,
        gossip_interval_ms: u64,
        node_timeout_ms: u64,
    ) -> RunningNode {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let cfg = Cfg {
            data_dir: dir.to_string_lossy().into_owned(),
            num_shards: SHARDS,
            storage_compression: None,
            async_write: Some(true),
            batch_size: Some(100),
            batch_timeout_ms: Some(10),
            write_queue_capacity: None,
            addr: Some(addr.to_string()),
            cluster: Some(ClusterConfig {
                enabled: true,
                node_id: node_id.to_string(),
                seeds,
                port: gossip_port,
                slots: None,
                slot_ranges,
                gossip_interval_ms: Some(gossip_interval_ms),
                advertise_addr: Some(addr.to_string()),
                node_timeout_ms: Some(node_timeout_ms),
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
            shutdown,
            handle,
        }
    }

    // Bootstrap quickly at 100 ms gossip so cluster-state.json is persisted on p1.
    let (p1, p2, p3) = tokio::join!(
        start_with_timing(
            d1.path(),
            "p1",
            gp1,
            vec![g2.clone(), g3.clone()],
            Some(vec![SlotRange {
                start: 0,
                end: 5460,
            }]),
            None,
            100,
            15_000,
        ),
        start_with_timing(
            d2.path(),
            "p2",
            gp2,
            vec![g1.clone(), g3.clone()],
            Some(vec![SlotRange {
                start: 5461,
                end: 10922,
            }]),
            None,
            100,
            15_000,
        ),
        start_with_timing(
            d3.path(),
            "p3",
            gp3,
            vec![g1.clone(), g2.clone()],
            Some(vec![SlotRange {
                start: 10923,
                end: 16383,
            }]),
            None,
            100,
            15_000,
        ),
    );
    let r1 = start_with_timing(
        dr1.path(),
        "r1",
        gpr1,
        vec![g1.clone(), g2.clone(), g3.clone()],
        None,
        Some("p1"),
        100,
        15_000,
    )
    .await;

    wait_for(
        "initial cluster ok before restarting p1 with slow gossip",
        Duration::from_secs(15),
        || {
            cluster_ready_for_manual_failover(
                p1.addr,
                [p1.addr, p2.addr, p3.addr, r1.addr],
                dr1.path(),
            )
        },
    )
    .await;

    // Restart p1 from persisted state with a 6.5 s gossip_interval_ms: its first
    // gossip tick runs immediately on start (before peers arrive) and its next
    // tick runs 6.5 s later, so right after p1 discovers r1 its following gossip
    // tick is ~6.5 s away (> 5 s CLUSTER_MF_TIMEOUT, < 10 s primary pause).
    p1.stop().await;
    let p1 = start_with_timing(
        d1.path(),
        "p1",
        gp1,
        vec![g2.clone(), g3.clone()],
        Some(vec![SlotRange {
            start: 0,
            end: 5460,
        }]),
        None,
        6500,
        15_000,
    )
    .await;

    wait_for(
        "slow-gossip p1 discovers r1 and r1 reconnects",
        Duration::from_secs(20),
        || async {
            let nodes = Client::connect(p1.addr)
                .await
                .call_bulk(&["CLUSTER", "NODES"])
                .await;
            let p1_knows_r1 = node_entry(&nodes, "r1")
                .is_some_and(|(flags, master, _, _)| flags.contains("slave") && master == "p1");
            if !p1_knows_r1 {
                return false;
            }
            let r1_nodes = Client::connect(r1.addr)
                .await
                .call_bulk(&["CLUSTER", "NODES"])
                .await;
            let r1_sees_p1 = r1_nodes
                .lines()
                .any(|l| l.starts_with("p1 ") && l.contains(&p1.addr.to_string()));
            r1_sees_p1 && all_shards_synced(dr1.path()).await
        },
    )
    .await;

    // Hammer p1 continuously while triggering CLUSTER FAILOVER on r1.
    let stop = Arc::new(AtomicBool::new(false));
    let stop_w = stop.clone();
    let p1_addr = p1.addr;
    let write_keys = keys_in_range("late_claim_k", 300, 0, 5460);
    let writer = tokio::spawn(async move {
        let mut target = p1_addr;
        let mut conn = Client::connect(target).await;
        let mut acked: BTreeMap<String, String> = BTreeMap::new();
        let mut moved_count = 0usize;
        let mut i = 0usize;
        while !stop_w.load(Ordering::Relaxed) {
            let key = &write_keys[i % write_keys.len()];
            let val = format!("late-v-{i}");
            let saw_moved = write_following_moved(&mut conn, &mut target, &["SET", key, &val])
                .await
                .expect("write during late-claim manual failover must complete via MOVED");
            if saw_moved {
                moved_count += 1;
            }
            acked.insert(key.clone(), val);
            i += 1;
            if i.is_multiple_of(5) {
                sleep(Duration::from_millis(1)).await;
            }
        }
        (acked, moved_count, target)
    });

    sleep(Duration::from_millis(100)).await;
    let mf_started = Instant::now();
    assert_eq!(
        Client::connect(r1.addr)
            .await
            .call_simple(&["CLUSTER", "FAILOVER"])
            .await,
        "+OK"
    );

    // r1 promotes quickly (well before its 5 s deadline).
    wait_for("r1 promotes quickly", Duration::from_secs(5), || async {
        let nodes = Client::connect(r1.addr)
            .await
            .call_bulk(&["CLUSTER", "NODES"])
            .await;
        node_entry(&nodes, "r1")
            .is_some_and(|(flags, _, _, slots)| flags.contains("master") && slots == "0-5460")
    })
    .await;
    let r1_promoted_at = mf_started.elapsed();
    assert!(
        r1_promoted_at < Duration::from_secs(4),
        "r1 should promote before its 5 s deadline, took {r1_promoted_at:?}"
    );

    // Wait for slow-gossip p1 to eventually observe r1's claim and demote.
    wait_for(
        "slow-gossip p1 demotes and resyncs from r1",
        Duration::from_secs(15),
        || async {
            let nodes = Client::connect(p1.addr)
                .await
                .call_bulk(&["CLUSTER", "NODES"])
                .await;
            node_entry(&nodes, "p1").is_some_and(|(flags, master, _, slots)| {
                flags.contains("slave") && master == "r1" && slots.is_empty()
            }) && all_shards_synced(d1.path()).await
        },
    )
    .await;
    let p1_demoted_at = mf_started.elapsed();
    assert!(
        p1_demoted_at > Duration::from_secs(5),
        "expected p1's slow gossip tick to land past 5 s to exercise the gap (demoted at {p1_demoted_at:?})"
    );

    sleep(Duration::from_millis(150)).await;
    stop.store(true, Ordering::Relaxed);
    let (acked, moved_count, final_target) = writer.await.unwrap();
    assert!(moved_count >= 1, "expected writer to follow MOVED to r1");
    assert_eq!(final_target, r1.addr);

    // Every single acknowledged write must be readable directly from r1 (zero loss).
    let mut cr1 = Client::connect(r1.addr).await;
    for (k, expected) in &acked {
        let got = cr1.call_bulk(&["GET", k]).await;
        assert_eq!(
            &got, expected,
            "acknowledged write for {k} lost during late-claim gap"
        );
    }

    r1.stop().await;
    p3.stop().await;
    p2.stop().await;
    p1.stop().await;
}

/// Direct test of `BLOBASAUR.MFABORT` owner matching:
/// - `BLOBASAUR.MFSTART r1` pauses writes on `p1` for 10 s.
/// - A stale `BLOBASAUR.MFABORT r2` from another replica leaves the pause active.
/// - `BLOBASAUR.MFABORT r1` from the owning replica ends the pause immediately.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn manual_failover_mfabort_ends_pause_promptly_and_ignores_stale_replica() {
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
            false,
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
            false,
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
            false,
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
            false,
            None,
        ),
        start_node(
            dr2.path(),
            "r2",
            gpr2,
            vec![g1.clone(), g2.clone(), g3.clone()],
            None,
            Some("p1"),
            false,
            None,
        ),
    );

    wait_for(
        "p1 sees both r1 and r2 as replicas",
        Duration::from_secs(15),
        || async {
            let nodes = Client::connect(p1.addr)
                .await
                .call_bulk(&["CLUSTER", "NODES"])
                .await;
            let has_r1 = node_entry(&nodes, "r1")
                .is_some_and(|(flags, master, _, _)| flags.contains("slave") && master == "p1");
            let has_r2 = node_entry(&nodes, "r2")
                .is_some_and(|(flags, master, _, _)| flags.contains("slave") && master == "p1");
            has_r1 && has_r2
        },
    )
    .await;

    // Start pause owned by r1.
    let mut c_mf = Client::connect(p1.addr).await;
    c_mf.send(&["BLOBASAUR.MFSTART", "r1"]).await.unwrap();
    let header = c_mf.line().await.unwrap();
    assert!(
        header.starts_with('*'),
        "expected watermark array reply from MFSTART, got {header}"
    );
    drop(c_mf);

    let key = key_in_range("mfabort_k", 0, 5460);
    let p1_addr = p1.addr;
    let key_copy = key.clone();
    let blocked_write = tokio::spawn(async move {
        Client::connect(p1_addr)
            .await
            .call_simple(&["SET", &key_copy, "after-abort"])
            .await
    });

    sleep(Duration::from_millis(250)).await;
    assert!(
        !blocked_write.is_finished(),
        "write should be paused after MFSTART r1"
    );

    // Stale MFABORT from r2 must be ignored and keep the write blocked.
    assert_eq!(
        Client::connect(p1.addr)
            .await
            .call_simple(&["BLOBASAUR.MFABORT", "r2"])
            .await,
        "+OK"
    );
    sleep(Duration::from_millis(350)).await;
    assert!(
        !blocked_write.is_finished(),
        "stale MFABORT from r2 must not end r1's pause"
    );

    // Owning replica's MFABORT must end the 10 s pause immediately.
    let t_abort = Instant::now();
    assert_eq!(
        Client::connect(p1.addr)
            .await
            .call_simple(&["BLOBASAUR.MFABORT", "r1"])
            .await,
        "+OK"
    );
    let reply = timeout(Duration::from_secs(2), blocked_write)
        .await
        .expect("MFABORT from owning replica r1 did not release pause promptly")
        .unwrap();
    assert_eq!(reply, "+OK");
    assert!(
        t_abort.elapsed() < Duration::from_secs(1),
        "pause should end immediately on owner MFABORT, took {:?}",
        t_abort.elapsed()
    );
    assert_eq!(
        Client::connect(p1.addr)
            .await
            .call_bulk(&["GET", &key])
            .await,
        "after-abort"
    );

    r2.stop().await;
    r1.stop().await;
    p3.stop().await;
    p2.stop().await;
    p1.stop().await;
}
