//! Replication end to end: a primary and a replica started in-process with
//! `server::run`, finding each other through real gossip on 127.0.0.1.

use std::collections::BTreeSet;
use std::future::Future;
use std::net::{SocketAddr, UdpSocket};
use std::path::Path;
use std::time::{Duration, Instant};

use blobasaur::cluster::ClusterManager;
use blobasaur::config::{
    Cfg, ClusterConfig, CompressionConfig, CompressionType, ReplicationConfig, SlotRange,
};
use blobasaur::server;
use sqlx::{Connection, SqliteConnection};
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;

/// The primary owns this many slots: gossip carries them as a JSON list in one
/// UDP datagram, which all 16384 would overflow.
const OWNED_SLOTS: u16 = 8000;

/// A key (or hash field) that name hashes into the primary's slots.
fn k(name: &str) -> String {
    (0..)
        .map(|i| {
            if i == 0 {
                name.to_string()
            } else {
                format!("{name}{i}")
            }
        })
        .find(|key| ClusterManager::calculate_slot(key) <= OWNED_SLOTS)
        .unwrap()
}

struct Opts<'a> {
    name: &'a str,
    gossip_port: u16,
    seeds: Vec<String>,
    replica_of: Option<&'a str>,
    shards: usize,
    retention_secs: u64,
    zstd: bool,
}

impl<'a> Opts<'a> {
    fn primary(name: &'a str, shards: usize) -> Self {
        Self {
            name,
            gossip_port: free_udp_port(),
            seeds: Vec::new(),
            replica_of: None,
            shards,
            retention_secs: 86400,
            zstd: false,
        }
    }

    fn replica(name: &'a str, of: &'a str, seeds: Vec<String>, shards: usize) -> Self {
        Self {
            replica_of: Some(of),
            seeds,
            ..Self::primary(name, shards)
        }
    }
}

struct Node {
    addr: SocketAddr,
    gossip: String,
    shutdown: CancellationToken,
    handle: JoinHandle<miette::Result<()>>,
}

fn free_udp_port() -> u16 {
    UdpSocket::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn cfg(dir: &Path, o: &Opts, addr: SocketAddr, gossip_port: u16) -> Cfg {
    Cfg {
        data_dir: dir.to_string_lossy().into_owned(),
        num_shards: o.shards,
        storage_compression: o.zstd.then_some(CompressionConfig {
            enabled: true,
            algorithm: CompressionType::Zstd,
            level: Some(3),
        }),
        async_write: Some(false),
        batch_size: None,
        batch_timeout_ms: None,
        addr: Some(addr.to_string()),
        cluster: Some(ClusterConfig {
            enabled: true,
            node_id: o.name.to_string(),
            seeds: o.seeds.clone(),
            port: gossip_port,
            slots: None,
            // A replica owns no slots.
            slot_ranges: o.replica_of.is_none().then(|| {
                vec![SlotRange {
                    start: 0,
                    end: OWNED_SLOTS,
                }]
            }),
            gossip_interval_ms: Some(100),
            // Peers gossip to the advertised host, so it can't be left at 0.0.0.0.
            advertise_addr: Some(addr.to_string()),
        }),
        replication: Some(ReplicationConfig {
            replica_of: o.replica_of.map(str::to_string),
            tombstone_retention_secs: Some(o.retention_secs),
        }),
        metrics: None,
        sqlite: None,
        shutdown_timeout_secs: Some(20),
        max_request_size_mb: None,
        cleanup_chunk_size: None,
        cleanup_interval_secs: Some(1),
        new_replid: false,
    }
}

async fn start(dir: &Path, o: &Opts<'_>) -> Node {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_test_writer()
        .try_init();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let gossip_port = o.gossip_port;
    let shutdown = CancellationToken::new();
    let handle = tokio::spawn(server::run(
        cfg(dir, o, addr, gossip_port),
        listener,
        shutdown.clone(),
    ));
    Node {
        addr,
        gossip: format!("127.0.0.1:{gossip_port}"),
        shutdown,
        handle,
    }
}

impl Node {
    async fn stop(self) {
        self.shutdown.cancel();
        timeout(Duration::from_secs(30), self.handle)
            .await
            .expect("shutdown hung")
            .unwrap()
            .expect("shutdown reported an error");
    }
}

async fn wait_for<F, Fut>(what: &str, mut condition: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = Instant::now() + Duration::from_secs(30);
    while !condition().await {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        sleep(Duration::from_millis(50)).await;
    }
}

struct Client(BufReader<TcpStream>);

impl Client {
    async fn connect(addr: SocketAddr) -> Self {
        // The server may still be starting.
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            match TcpStream::connect(addr).await {
                Ok(stream) => return Self(BufReader::new(stream)),
                Err(e) => assert!(Instant::now() < deadline, "cannot connect: {e}"),
            }
            sleep(Duration::from_millis(50)).await;
        }
    }

    /// Sends a command and returns its reply lines (an array reply's elements
    /// follow its header).
    async fn call(&mut self, args: &[&str]) -> Vec<String> {
        let mut req = format!("*{}\r\n", args.len());
        for arg in args {
            req.push_str(&format!("${}\r\n{}\r\n", arg.len(), arg));
        }
        self.0.get_mut().write_all(req.as_bytes()).await.unwrap();
        let mut lines = vec![self.line().await];
        if let Some(n) = lines[0].strip_prefix('*') {
            for _ in 0..n.parse::<usize>().unwrap() {
                lines.push(self.line().await);
            }
        }
        lines
    }

    async fn line(&mut self) -> String {
        let mut line = String::new();
        self.0.read_line(&mut line).await.unwrap();
        line.trim_end().to_string()
    }
}

async fn ok(client: &mut Client, args: &[&str]) {
    assert_eq!(client.call(args).await, ["+OK"], "{args:?}");
}

/// Opens a shard DB, waiting for a server that is still starting to create it.
async fn connect_db(dir: &Path, shard: usize) -> SqliteConnection {
    let url = format!("sqlite:{}/shard_{}.db", dir.display(), shard);
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match SqliteConnection::connect(&url).await {
            Ok(conn) => return conn,
            Err(e) => assert!(Instant::now() < deadline, "cannot open {url}: {e}"),
        }
        sleep(Duration::from_millis(50)).await;
    }
}

/// A `repl_state` value; unset also while the shard is still being set up.
async fn state(dir: &Path, shard: usize, k: &str) -> Option<String> {
    sqlx::query_scalar("SELECT v FROM repl_state WHERE k = ?")
        .bind(k)
        .fetch_optional(&mut connect_db(dir, shard).await)
        .await
        .ok()
        .flatten()
}

type Row = (String, Vec<u8>, i64, i64, Option<i64>, i64);

/// Every row of every table, formatted verbatim, across shards.
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

async fn all_synced(dir: &Path, shards: usize) -> bool {
    for shard in 0..shards {
        if state(dir, shard, "synced").await.as_deref() != Some("1") {
            return false;
        }
    }
    true
}

/// `rows` without the rows a test planted directly in the replica's DB.
fn without_markers(rows: BTreeSet<String>) -> BTreeSet<String> {
    rows.into_iter()
        .filter(|r| !r.contains("/marker/"))
        .collect()
}

async fn has_marker(dir: &Path, shards: usize) -> bool {
    snapshot(dir, shards)
        .await
        .iter()
        .any(|r| r.contains("/marker/"))
}

/// A row only the replica has: a reset wipes it, a resume leaves it.
async fn plant_marker(dir: &Path) {
    sqlx::query(
        "INSERT INTO blobs (key, data, created_at, updated_at) VALUES ('marker', x'00', 0, 0)",
    )
    .execute(&mut connect_db(dir, 0).await)
    .await
    .unwrap();
}

async fn converged(primary: &Path, replica: &Path, shards: usize) -> bool {
    without_markers(snapshot(replica, shards).await) == snapshot(primary, shards).await
}

#[tokio::test]
async fn replica_converges_with_primary() {
    const SHARDS: usize = 4;
    let (pdir, rdir) = (TempDir::new().unwrap(), TempDir::new().unwrap());
    let primary = start(pdir.path(), &Opts::primary("p", SHARDS)).await;
    let replica = start(
        rdir.path(),
        &Opts::replica("r", "p", vec![primary.gossip.clone()], SHARDS),
    )
    .await;

    // Nothing written yet: every shard is empty, and still reports synced
    // through the watermark.
    wait_for("empty shards to sync", || all_synced(rdir.path(), SHARDS)).await;

    let mut client = Client::connect(primary.addr).await;
    for i in 0..20 {
        ok(
            &mut client,
            &["SET", &k(&format!("k{i}")), &format!("value {i}")],
        )
        .await;
    }
    let (gone, k0, k1) = (k("gone"), k("k0"), k("k1"));
    let (f1, f2, f3) = (k("f1"), k("f2"), k("f3"));
    ok(&mut client, &["SET", &gone, "x"]).await;
    assert_eq!(client.call(&["DEL", &gone]).await, [":1"]);
    ok(&mut client, &["SET", &k0, "rewritten"]).await;
    ok(&mut client, &["SET", &k("ttl"), "v", "EX", "3600"]).await;
    assert_eq!(client.call(&["EXPIRE", &k1, "3600"]).await, [":1"]);
    for f in [&f1, &f2, &f3] {
        let reply = client.call(&["HSET", "ns", f, &format!("hash {f}")]).await;
        assert_eq!(reply, [":1"]);
    }
    assert_eq!(
        client.call(&["HSET", "other", &f1, "elsewhere"]).await,
        [":1"]
    );
    assert_eq!(
        client
            .call(&["HEXPIRE", "ns", "3600", "FIELDS", "1", &f1])
            .await,
        ["*1", ":1"]
    );
    // An immediate expiry deletes the field.
    assert_eq!(
        client
            .call(&["HEXPIRE", "ns", "0", "FIELDS", "1", &f3])
            .await,
        ["*1", ":2"]
    );

    wait_for("the replica to converge", || {
        converged(pdir.path(), rdir.path(), SHARDS)
    })
    .await;
    let rows = snapshot(rdir.path(), SHARDS).await;
    assert!(rows.iter().any(|r| r.contains(&format!("/blobs_ns/{f2}/"))));
    assert!(
        rows.iter()
            .any(|r| r.contains(&format!("/blobs_other/{f1}/")))
    );
    assert!(
        rows.iter()
            .any(|r| r.contains(&format!("/blobs/{k1}/")) && r.contains("Some("))
    );
    assert!(
        !rows
            .iter()
            .any(|r| r.contains(&format!("/{gone}/")) || r.contains(&format!("/blobs_ns/{f3}/")))
    );
    assert!(
        rows.iter()
            .any(|r| r.contains(&format!("/blobs/{k0}/")) && r.contains("[114, 101"))
    );
    assert!(all_synced(rdir.path(), SHARDS).await);

    // The replica's own change index covers what it copied.
    let mut logged = 0;
    for shard in 0..SHARDS {
        let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM repl_log WHERE deleted = 0")
            .fetch_one(&mut connect_db(rdir.path(), shard).await)
            .await
            .unwrap();
        logged += n;
    }
    assert_eq!(logged as usize, rows.len());

    replica.stop().await;
    primary.stop().await;
}

#[tokio::test]
async fn replica_restart_resumes_from_applied_lsn() {
    const SHARDS: usize = 2;
    let (pdir, rdir) = (TempDir::new().unwrap(), TempDir::new().unwrap());
    let primary = start(pdir.path(), &Opts::primary("p", SHARDS)).await;
    let ropts = |seeds| Opts::replica("r", "p", seeds, SHARDS);
    let replica = start(rdir.path(), &ropts(vec![primary.gossip.clone()])).await;

    let mut client = Client::connect(primary.addr).await;
    for i in 0..10 {
        ok(&mut client, &["SET", &k(&format!("k{i}")), "v"]).await;
    }
    wait_for("the first copy", || {
        converged(pdir.path(), rdir.path(), SHARDS)
    })
    .await;
    wait_for("synced", || all_synced(rdir.path(), SHARDS)).await;
    let replid = state(rdir.path(), 0, "replid").await.unwrap();
    replica.stop().await;

    // The replica is down: more writes, then a marker only a reset would remove.
    ok(&mut client, &["SET", &k("late"), "v"]).await;
    assert_eq!(client.call(&["DEL", &k("k0")]).await, [":1"]);
    plant_marker(rdir.path()).await;
    assert!(!converged(pdir.path(), rdir.path(), SHARDS).await);

    let replica = start(rdir.path(), &ropts(vec![primary.gossip.clone()])).await;
    wait_for("the replica to catch up", || {
        converged(pdir.path(), rdir.path(), SHARDS)
    })
    .await;
    assert!(has_marker(rdir.path(), SHARDS).await, "the shard was reset");
    assert_eq!(state(rdir.path(), 0, "replid").await.unwrap(), replid);

    replica.stop().await;
    primary.stop().await;
}

#[tokio::test]
async fn replica_behind_trimmed_history_resyncs_and_copies_everything() {
    let (pdir, rdir) = (TempDir::new().unwrap(), TempDir::new().unwrap());
    let primary = start(
        pdir.path(),
        &Opts {
            retention_secs: 1,
            ..Opts::primary("p", 1)
        },
    )
    .await;
    let ropts = |seeds| Opts::replica("r", "p", seeds, 1);
    let replica = start(rdir.path(), &ropts(vec![primary.gossip.clone()])).await;

    let mut client = Client::connect(primary.addr).await;
    for i in 0..5 {
        ok(&mut client, &["SET", &k(&format!("k{i}")), "v"]).await;
    }
    wait_for("the first copy", || converged(pdir.path(), rdir.path(), 1)).await;
    wait_for("synced", || all_synced(rdir.path(), 1)).await;
    let applied: u64 = state(rdir.path(), 0, "applied_lsn")
        .await
        .unwrap()
        .parse()
        .unwrap();
    replica.stop().await;

    // Deletes while it is away, which the cleanup trims once a second has passed.
    assert_eq!(client.call(&["DEL", &k("k0"), &k("k1")]).await, [":2"]);
    ok(&mut client, &["SET", &k("after"), "v"]).await;
    plant_marker(rdir.path()).await;
    wait_for("the tombstones to be trimmed", || async {
        let trimmed = state(pdir.path(), 0, "trimmed_through").await;
        trimmed.is_some_and(|t| t.parse::<u64>().unwrap() > applied)
    })
    .await;

    let replica = start(rdir.path(), &ropts(vec![primary.gossip.clone()])).await;
    wait_for("the full copy", || converged(pdir.path(), rdir.path(), 1)).await;
    wait_for("synced again", || all_synced(rdir.path(), 1)).await;
    assert!(!has_marker(rdir.path(), 1).await, "the shard was not reset");
    assert_eq!(
        state(rdir.path(), 0, "replid").await,
        state(pdir.path(), 0, "replid").await
    );

    replica.stop().await;
    primary.stop().await;
}

#[tokio::test]
async fn a_key_cleaned_up_while_the_replica_is_away_is_deleted_there() {
    let (pdir, rdir) = (TempDir::new().unwrap(), TempDir::new().unwrap());
    let primary = start(pdir.path(), &Opts::primary("p", 1)).await;
    let ropts = |seeds| Opts::replica("r", "p", seeds, 1);
    let replica = start(rdir.path(), &ropts(vec![primary.gossip.clone()])).await;

    let mut client = Client::connect(primary.addr).await;
    let doomed = k("doomed");
    ok(&mut client, &["SET", &doomed, "v"]).await;
    ok(&mut client, &["SET", &k("kept"), "v"]).await;
    wait_for("the first copy", || converged(pdir.path(), rdir.path(), 1)).await;
    replica.stop().await;

    // The replica holds `doomed` without a TTL; the primary expires and cleans
    // it up while the replica is away.
    assert_eq!(client.call(&["EXPIRE", &doomed, "1"]).await, [":1"]);
    wait_for("the primary to clean up", || async {
        !snapshot(pdir.path(), 1)
            .await
            .iter()
            .any(|r| r.contains(&format!("/{doomed}/")))
    })
    .await;
    plant_marker(rdir.path()).await;

    let replica = start(rdir.path(), &ropts(vec![primary.gossip.clone()])).await;
    wait_for("the replica to delete it", || {
        converged(pdir.path(), rdir.path(), 1)
    })
    .await;
    assert!(has_marker(rdir.path(), 1).await, "the shard was reset");

    replica.stop().await;
    primary.stop().await;
}

#[tokio::test]
async fn compressed_values_are_copied_as_stored() {
    let (pdir, rdir) = (TempDir::new().unwrap(), TempDir::new().unwrap());
    let primary = start(
        pdir.path(),
        &Opts {
            zstd: true,
            ..Opts::primary("p", 1)
        },
    )
    .await;
    let replica = start(
        rdir.path(),
        &Opts {
            zstd: true,
            ..Opts::replica("r", "p", vec![primary.gossip.clone()], 1)
        },
    )
    .await;
    let mut client = Client::connect(primary.addr).await;
    ok(&mut client, &["SET", "k", &"compress me ".repeat(100)]).await;
    wait_for("the copy", || converged(pdir.path(), rdir.path(), 1)).await;
    assert_eq!(snapshot(rdir.path(), 1).await.len(), 1);

    // The primary shuts down with a replica still streaming from it.
    primary.stop().await;
    replica.stop().await;
}

/// Sends a raw `BLOBASAUR.SYNC` and returns the handshake reply line.
async fn sync(addr: SocketAddr, args: [&str; 5]) -> (String, Client) {
    let mut client = Client::connect(addr).await;
    let mut command = vec!["BLOBASAUR.SYNC"];
    command.extend(args);
    let reply = client.call(&command).await.remove(0);
    (reply, client)
}

#[tokio::test]
async fn handshake_checks_the_replica_before_streaming() {
    let (pdir, rdir) = (TempDir::new().unwrap(), TempDir::new().unwrap());
    let primary = start(pdir.path(), &Opts::primary("p", 2)).await;
    // A replica of a node that never shows up.
    let replica = start(rdir.path(), &Opts::replica("r", "ghost", Vec::new(), 2)).await;
    let mut client = Client::connect(primary.addr).await;
    ok(&mut client, &["SET", "k", "v"]).await;
    let replid = state(pdir.path(), 0, "replid").await.unwrap();

    // The config has to match.
    for args in [
        ["0", replid.as_str(), "0", "3", "none"],
        ["0", replid.as_str(), "0", "2", "zstd"],
        ["2", replid.as_str(), "0", "2", "none"],
    ] {
        let (reply, _) = sync(primary.addr, args).await;
        assert!(reply.starts_with("-ERR"), "{args:?} got {reply}");
    }

    // A replica is no source to copy from.
    let (reply, _) = sync(replica.addr, ["0", &replid, "0", "2", "none"]).await;
    assert!(reply.starts_with("-ERR"), "{reply}");

    // Unknown history, or a cursor beyond the primary's commits: resync.
    let (reply, _) = sync(primary.addr, ["0", "", "0", "2", "none"]).await;
    assert!(reply.starts_with(&format!("+RESYNC {replid} ")), "{reply}");
    let (reply, _) = sync(primary.addr, ["0", "other", "0", "2", "none"]).await;
    assert!(reply.starts_with("+RESYNC "), "{reply}");
    let (reply, _) = sync(primary.addr, ["0", &replid, "999999", "2", "none"]).await;
    assert!(reply.starts_with("+RESYNC "), "{reply}");

    // Right history from the start: continue, and the stream ends in a watermark.
    let (reply, mut stream) = sync(primary.addr, ["0", &replid, "0", "2", "none"]).await;
    assert!(
        reply.starts_with(&format!("+CONTINUE {replid} ")),
        "{reply}"
    );
    let mut frames = Vec::new();
    loop {
        let header = stream.line().await;
        let elements: usize = header.strip_prefix('*').unwrap().parse().unwrap();
        let mut frame = Vec::new();
        for _ in 0..elements {
            let line = stream.line().await;
            if line.starts_with('$') && line != "$-1" {
                frame.push(stream.line().await);
            } else {
                frame.push(line);
            }
        }
        let done = elements == 2;
        frames.push(frame);
        if done {
            break;
        }
    }
    // Whatever shard 0 holds comes first, then `[watermark, unix_ms]`.
    let watermark = frames.pop().unwrap();
    assert_eq!(watermark[0], format!(":{}", frames.len()));
    for frame in &frames {
        assert_eq!(frame.len(), 8);
        assert_eq!(frame[1], "blobs");
    }

    replica.stop().await;
    primary.stop().await;
}

#[tokio::test]
async fn replication_needs_a_cluster() {
    let dir = TempDir::new().unwrap();
    let o = Opts::replica("r", "p", Vec::new(), 1);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mut config = cfg(dir.path(), &o, addr, free_udp_port());
    config.cluster = None;
    let result = server::run(config, listener, CancellationToken::new()).await;
    assert!(
        format!("{:?}", result.unwrap_err()).contains("requires [cluster]"),
        "wrong error"
    );
}

#[tokio::test]
async fn a_failed_cluster_start_stops_a_replicated_node() {
    let dir = TempDir::new().unwrap();
    let taken = UdpSocket::bind("0.0.0.0:0").unwrap();
    let port = taken.local_addr().unwrap().port();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let o = Opts::primary("p", 1);
    let result = server::run(
        cfg(dir.path(), &o, addr, port),
        listener,
        CancellationToken::new(),
    )
    .await;
    assert!(
        format!("{:?}", result.unwrap_err()).contains("cluster manager"),
        "wrong error"
    );
    drop(taken);
}
