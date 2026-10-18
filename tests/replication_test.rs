//! Replication end to end: a primary and a replica started in-process with
//! `server::run`, finding each other through real gossip on 127.0.0.1.

use std::collections::BTreeSet;
use std::future::Future;
use std::net::{SocketAddr, UdpSocket};
use std::path::Path;
use std::pin::Pin;
use std::time::{Duration, Instant};

use blobasaur::cluster::ClusterManager;
use blobasaur::config::{
    Cfg, ClusterConfig, CompressionConfig, CompressionType, ReplicationConfig, SlotRange,
};
use blobasaur::server;
use sqlx::{Connection, SqliteConnection};
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;

/// A primary owns slots up to this one, unless a test gives it a `range`.
const OWNED_SLOTS: u16 = 8000;

/// Every slot, whose old JSON list would overflow a gossip datagram.
const ALL_SLOTS: (u16, u16) = (0, 16383);

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
    async_write: bool,
    /// The slots a primary owns; 0 to `OWNED_SLOTS` if unset.
    range: Option<(u16, u16)>,
    new_replid: bool,
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
            async_write: false,
            range: None,
            new_replid: false,
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
        async_write: Some(o.async_write),
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
                let (start, end) = o.range.unwrap_or((0, OWNED_SLOTS));
                vec![SlotRange { start, end }]
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
        new_replid: o.new_replid,
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

#[derive(Debug, PartialEq)]
enum Reply {
    Int(i64),
    Text(String),
    Array(Vec<Reply>),
}

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
        self.send(args).await;
        let mut lines = vec![self.line().await];
        if let Some(n) = lines[0].strip_prefix('*') {
            for _ in 0..n.parse::<usize>().unwrap() {
                lines.push(self.line().await);
            }
        }
        lines
    }

    async fn send(&mut self, args: &[&str]) {
        let mut req = format!("*{}\r\n", args.len());
        for arg in args {
            req.push_str(&format!("${}\r\n{}\r\n", arg.len(), arg));
        }
        self.0.get_mut().write_all(req.as_bytes()).await.unwrap();
    }

    /// Sends a command and parses the reply, nested arrays included.
    async fn tree(&mut self, args: &[&str]) -> Reply {
        self.send(args).await;
        self.reply().await
    }

    async fn cluster_nodes(&mut self) -> String {
        self.send(&["CLUSTER", "NODES"]).await;
        let header = self.line().await;
        let mut text = vec![0; header[1..].parse::<usize>().unwrap() + 2];
        self.0.read_exact(&mut text).await.unwrap();
        String::from_utf8(text).unwrap()
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
                "$" if rest != "-1" => Reply::Text(self.line().await),
                _ => Reply::Text(line),
            }
        })
    }

    /// CLUSTER SLOTS as `(start, end, [(port, node id)])`, the primary first.
    async fn cluster_slots(&mut self) -> Vec<(i64, i64, Vec<(i64, String)>)> {
        let Reply::Array(ranges) = self.tree(&["CLUSTER", "SLOTS"]).await else {
            panic!("CLUSTER SLOTS is not an array");
        };
        ranges
            .into_iter()
            .map(|range| {
                let Reply::Array(items) = range else {
                    panic!("range is not an array")
                };
                let (Reply::Int(start), Reply::Int(end)) = (&items[0], &items[1]) else {
                    panic!("range has no bounds")
                };
                let nodes = items[2..]
                    .iter()
                    .map(|node| match node {
                        Reply::Array(n) => match (&n[0], &n[1], &n[2]) {
                            (Reply::Text(ip), Reply::Int(port), Reply::Text(id)) => {
                                assert_eq!(ip, "127.0.0.1");
                                (*port, id.clone())
                            }
                            _ => panic!("bad node {n:?}"),
                        },
                        _ => panic!("node is not an array"),
                    })
                    .collect();
                (*start, *end, nodes)
            })
            .collect()
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
    // All slots: the replica only finds this primary if gossip delivers them.
    let primary = start(
        pdir.path(),
        &Opts {
            range: Some(ALL_SLOTS),
            ..Opts::primary("p", SHARDS)
        },
    )
    .await;
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

/// `BLOBASAUR.WATERMARK`: per shard `(replid, lsn)`, the LSN as the reply's text.
async fn watermark(client: &mut Client) -> Result<Vec<(String, String)>, String> {
    client
        .0
        .get_mut()
        .write_all(b"*1\r\n$19\r\nBLOBASAUR.WATERMARK\r\n")
        .await
        .unwrap();
    let header = client.line().await;
    let Some(n) = header.strip_prefix('*') else {
        return Err(header);
    };
    let mut shards = Vec::new();
    for shard in 0..n.parse::<usize>().unwrap() {
        assert_eq!(client.line().await, "*3");
        assert_eq!(client.line().await, format!(":{shard}"));
        client.line().await;
        let replid = client.line().await;
        client.line().await;
        shards.push((replid, client.line().await));
    }
    Ok(shards)
}

/// A shard's highest assigned LSN, from the DB.
async fn latest_lsn(dir: &Path, shard: usize) -> u64 {
    sqlx::query_scalar::<_, i64>(
        "SELECT COALESCE(MAX(seq), 0) FROM sqlite_sequence WHERE name = 'repl_log'",
    )
    .fetch_one(&mut connect_db(dir, shard).await)
    .await
    .unwrap() as u64
}

/// Whether every shard of `marks` is under the primary's replid and at or past
/// the primary's LSN in `target`.
fn reached(marks: &[(String, String)], target: &[(String, String)]) -> bool {
    marks.iter().zip(target).all(|((rid, rl), (pid, pl))| {
        rid == pid && rl.parse::<u64>().is_ok_and(|l| l >= pl.parse().unwrap())
    })
}

async fn reached_by(addr: SocketAddr, target: &[(String, String)]) -> bool {
    reached(
        &watermark(&mut Client::connect(addr).await).await.unwrap(),
        target,
    )
}

#[tokio::test]
async fn watermark_on_a_primary_covers_acknowledged_async_writes() {
    const SHARDS: usize = 2;
    let pdir = TempDir::new().unwrap();
    let primary = start(
        pdir.path(),
        &Opts {
            async_write: true,
            ..Opts::primary("p", SHARDS)
        },
    )
    .await;
    let mut client = Client::connect(primary.addr).await;
    for i in 0..20 {
        ok(&mut client, &["SET", &k(&format!("k{i}_")), "v"]).await;
    }

    let marks = watermark(&mut client).await.unwrap();
    assert_eq!(marks.len(), SHARDS);
    // Each key is one log entry, so the LSNs add up to the writes, and they
    // are already committed rather than still queued.
    let mut total = 0;
    for (shard, (replid, lsn)) in marks.iter().enumerate() {
        assert_eq!(*replid, state(pdir.path(), shard, "replid").await.unwrap());
        let lsn: u64 = lsn.parse().unwrap();
        assert_eq!(lsn, latest_lsn(pdir.path(), shard).await);
        total += lsn;
    }
    assert_eq!(total, 20);
    assert_eq!(snapshot(pdir.path(), SHARDS).await.len(), 20);

    primary.stop().await;
}

#[tokio::test]
async fn watermark_on_a_replica_reports_caught_up_lsn_or_not_synced() {
    const SHARDS: usize = 2;
    let (pdir, rdir, odir) = (
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
    );

    // A replica of a node that never shows up has no copy.
    let orphan = start(
        odir.path(),
        &Opts::replica("o", "ghost", Vec::new(), SHARDS),
    )
    .await;
    let mut client = Client::connect(orphan.addr).await;
    let marks = watermark(&mut client).await.unwrap();
    assert_eq!(marks.len(), SHARDS);
    assert!(
        marks.iter().all(|(_, lsn)| lsn == "not-synced"),
        "{marks:?}"
    );
    orphan.stop().await;

    let primary = start(
        pdir.path(),
        &Opts {
            async_write: true,
            ..Opts::primary("p", SHARDS)
        },
    )
    .await;
    let replica = start(
        rdir.path(),
        &Opts::replica("r", "p", vec![primary.gossip.clone()], SHARDS),
    )
    .await;
    let mut pclient = Client::connect(primary.addr).await;
    for i in 0..20 {
        ok(&mut pclient, &["SET", &k(&format!("k{i}_")), "v"]).await;
    }
    let target = watermark(&mut pclient).await.unwrap();

    let raddr = replica.addr;
    wait_for("the replica's watermark to reach the primary's", || {
        reached_by(raddr, &target)
    })
    .await;
    assert!(converged(pdir.path(), rdir.path(), SHARDS).await);

    replica.stop().await;
    primary.stop().await;
}

fn text(s: impl Into<String>) -> Reply {
    Reply::Text(s.into())
}

type Slots = Vec<(i64, i64, Vec<(i64, String)>)>;

/// CLUSTER SLOTS for a primary `p` owning every slot, then its listed replicas.
fn listing(primary: &Node, replicas: &[(&Node, &str)]) -> Slots {
    let mut nodes = vec![(primary.addr.port() as i64, "p".to_string())];
    nodes.extend(
        replicas
            .iter()
            .map(|(n, id)| (n.addr.port() as i64, id.to_string())),
    );
    vec![(0, 16383, nodes)]
}

async fn slots_of(node: &Node) -> Slots {
    Client::connect(node.addr).await.cluster_slots().await
}

async fn readonly_client(node: &Node) -> Client {
    let mut client = Client::connect(node.addr).await;
    ok(&mut client, &["READONLY"]).await;
    client
}

/// A replica that never finishes syncing: the primary refuses its shard count.
fn stuck_replica<'a>(primary: &Node) -> Opts<'a> {
    Opts::replica("stuck", "p", vec![primary.gossip.clone()], 3)
}

fn all_slots(o: Opts<'_>) -> Opts<'_> {
    Opts {
        range: Some(ALL_SLOTS),
        ..o
    }
}

#[tokio::test]
async fn topology_lists_a_replica_once_it_is_synced() {
    const SHARDS: usize = 2;
    let (pdir, rdir, sdir) = (
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
    );
    let primary = start(pdir.path(), &all_slots(Opts::primary("p", SHARDS))).await;
    let stuck = start(sdir.path(), &stuck_replica(&primary)).await;
    let replica = start(
        rdir.path(),
        &Opts::replica("r", "p", vec![primary.gossip.clone()], SHARDS),
    )
    .await;

    // The replica appears after the primary, and only once it has synced. The
    // listing is read first and the database second: listed implies synced.
    let (mut before, deadline) = (0, Instant::now() + Duration::from_secs(30));
    loop {
        let slots = slots_of(&primary).await;
        if slots == listing(&primary, &[(&replica, "r")]) {
            assert!(
                all_synced(rdir.path(), SHARDS).await,
                "listed before synced"
            );
            break;
        }
        assert_eq!(slots, listing(&primary, &[]));
        before += 1;
        assert!(Instant::now() < deadline, "the replica was never listed");
        sleep(Duration::from_millis(20)).await;
    }
    eprintln!("replica absent from CLUSTER SLOTS for {before} polls");

    // The replica sees the same, and the primary's slots reached it as ranges.
    wait_for("the replica's own view", || async {
        slots_of(&replica).await == listing(&primary, &[(&replica, "r")])
    })
    .await;

    // Both are slaves in CLUSTER NODES, the stuck one too.
    let slave = |n: &Node, id: &str| {
        format!(
            "{id} {}:{} slave p 0 0 0 connected\n",
            n.addr.ip(),
            n.addr.port()
        )
    };
    wait_for("the stuck replica to be known", || async {
        let nodes = Client::connect(primary.addr).await.cluster_nodes().await;
        nodes.contains(&slave(&stuck, "stuck")) && nodes.contains(&slave(&replica, "r"))
    })
    .await;
    let nodes = Client::connect(replica.addr).await.cluster_nodes().await;
    let myself = format!(
        "r {}:{} myself,slave p 0 0 0 connected\n",
        replica.addr.ip(),
        replica.addr.port()
    );
    assert!(nodes.contains(&myself), "{nodes}");
    let master = format!(
        "p {}:{} master - 0 0 0 connected 0-16383\n",
        primary.addr.ip(),
        primary.addr.port()
    );
    assert!(nodes.contains(&master), "{nodes}");
    // Still syncing, so still not listed.
    assert_eq!(
        slots_of(&primary).await,
        listing(&primary, &[(&replica, "r")])
    );

    stuck.stop().await;
    replica.stop().await;
    primary.stop().await;
}

#[tokio::test]
async fn a_replica_serves_reads_only_on_readonly_connections() {
    const SHARDS: usize = 2;
    let (pdir, rdir, sdir) = (
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
    );
    let primary = start(pdir.path(), &all_slots(Opts::primary("p", SHARDS))).await;
    let stuck = start(sdir.path(), &stuck_replica(&primary)).await;
    let replica = start(
        rdir.path(),
        &Opts::replica("r", "p", vec![primary.gossip.clone()], SHARDS),
    )
    .await;
    wait_for("the replica to be listed", || async {
        slots_of(&primary).await == listing(&primary, &[(&replica, "r")])
    })
    .await;

    let mut writer = Client::connect(primary.addr).await;
    let (key, field) = ("key", "field");
    ok(&mut writer, &["SET", key, "value"]).await;
    ok(&mut writer, &["SET", "ttl", "v", "EX", "3600"]).await;
    assert_eq!(writer.call(&["HSET", "ns", field, "hv"]).await, [":1"]);
    let moved = |key: &str| {
        text(format!(
            "-MOVED {} {}",
            ClusterManager::calculate_slot(key),
            primary.addr
        ))
    };

    // Without READONLY every command goes to the primary, though this one is synced.
    let mut plain = Client::connect(replica.addr).await;
    assert_eq!(plain.tree(&["GET", key]).await, moved(key));
    assert_eq!(plain.tree(&["HGET", "ns", field]).await, moved(field));

    // With it, reads are served here once they have replicated.
    wait_for("the replica to hold the data", || async {
        let mut c = readonly_client(&replica).await;
        c.tree(&["GET", key]).await == text("value")
            && c.tree(&["HGET", "ns", field]).await == text("hv")
            && matches!(c.tree(&["TTL", "ttl"]).await, Reply::Int(n) if n > 3000)
    })
    .await;
    let mut ro = readonly_client(&replica).await;
    assert_eq!(ro.tree(&["GET", "nope"]).await, text("$-1"));
    assert_eq!(ro.tree(&["EXISTS", key]).await, Reply::Int(1));
    assert_eq!(ro.tree(&["HEXISTS", "ns", field]).await, Reply::Int(1));

    // Writes never are.
    for command in [
        vec!["SET", key, "other"],
        vec!["DEL", key],
        vec!["EXPIRE", key, "10"],
        vec!["HSET", "ns", field, "other"],
        vec!["HDEL", "ns", field],
    ] {
        let slot_key = if command[0].starts_with('H') {
            field
        } else {
            key
        };
        assert_eq!(ro.tree(&command).await, moved(slot_key), "{command:?}");
    }
    assert_eq!(ro.tree(&["GET", key]).await, text("value"));

    // READWRITE undoes READONLY.
    ok(&mut ro, &["READWRITE"]).await;
    assert_eq!(ro.tree(&["GET", key]).await, moved(key));

    // A replica owns no slots: ADDSLOTS/DELSLOTS are refused and a write still moves.
    let slot = ClusterManager::calculate_slot(key).to_string();
    for sub in ["ADDSLOTS", "DELSLOTS"] {
        assert_eq!(
            ro.tree(&["CLUSTER", sub, &slot]).await,
            text("-ERR this node is a replica")
        );
    }
    assert_eq!(ro.tree(&["SET", key, "other"]).await, moved(key));

    // A replica that is still copying serves nothing, READONLY or not.
    wait_for("the stuck replica to know its primary", || async {
        let mut c = readonly_client(&stuck).await;
        c.tree(&["GET", key]).await == moved(key)
    })
    .await;

    stuck.stop().await;
    replica.stop().await;
    primary.stop().await;
}

#[tokio::test]
async fn a_slot_nobody_serves_is_clusterdown() {
    let (pdir, rdir) = (TempDir::new().unwrap(), TempDir::new().unwrap());
    // The primary owns 0 to OWNED_SLOTS only.
    let primary = start(pdir.path(), &Opts::primary("p", 1)).await;
    let replica = start(
        rdir.path(),
        &Opts::replica("r", "p", vec![primary.gossip.clone()], 1),
    )
    .await;
    // A slot the primary does own is served here once the replica knows it.
    let known = k("known");
    wait_for("the replica to serve", || async {
        readonly_client(&replica).await.tree(&["GET", &known]).await == text("$-1")
    })
    .await;

    let key = (0..)
        .map(|i| format!("down{i}"))
        .find(|k| ClusterManager::calculate_slot(k) > OWNED_SLOTS)
        .unwrap();
    let down = text("-CLUSTERDOWN Hash slot not served");
    for (node, readonly) in [(&primary, false), (&replica, false), (&replica, true)] {
        let mut client = Client::connect(node.addr).await;
        if readonly {
            ok(&mut client, &["READONLY"]).await;
        }
        assert_eq!(client.tree(&["GET", &key]).await, down);
        assert_eq!(client.tree(&["SET", &key, "v"]).await, down);
    }

    replica.stop().await;
    primary.stop().await;
}

#[tokio::test]
async fn a_client_that_stops_reading_redirects_does_not_block_a_resync() {
    let (pdir, rdir) = (TempDir::new().unwrap(), TempDir::new().unwrap());
    let primary = start(pdir.path(), &Opts::primary("p", 1)).await;
    let replica = start(
        rdir.path(),
        &Opts::replica("r", "p", vec![primary.gossip.clone()], 1),
    )
    .await;
    wait_for("synced", || all_synced(rdir.path(), 1)).await;
    let old = state(rdir.path(), 0, "replid").await.unwrap();

    // READONLY reads on a replica take the shard's gate. Pipeline redirected
    // reads without reading the replies, until the server blocks writing them.
    let key = (0..)
        .map(|i| format!("down{i}"))
        .find(|k| ClusterManager::calculate_slot(k) > OWNED_SLOTS)
        .unwrap();
    // A small receive buffer, so the server's reply writes block quickly.
    let socket = tokio::net::TcpSocket::new_v4().unwrap();
    socket.set_recv_buffer_size(4096).unwrap();
    let mut blocked = socket.connect(replica.addr).await.unwrap();
    let mut pipeline = b"*1\r\n$8\r\nREADONLY\r\n".to_vec();
    let get = format!("*2\r\n$3\r\nGET\r\n${}\r\n{key}\r\n", key.len());
    for _ in 0..200_000 {
        pipeline.extend_from_slice(get.as_bytes());
    }
    let writer = tokio::spawn(async move {
        let _ = blocked.write_all(&pipeline).await;
        blocked
    });

    // Give the server time to fill the socket and block on a reply.
    sleep(Duration::from_secs(1)).await;
    assert!(
        !writer.is_finished(),
        "the server never blocked on the client"
    );
    // A new replid on the primary makes the replica reset, which needs the gate.
    // In-process, the stopped node's gossip keeps its port: restart on another.
    primary.stop().await;
    let primary = start(
        pdir.path(),
        &Opts {
            new_replid: true,
            seeds: vec![replica.gossip.clone()],
            ..Opts::primary("p", 1)
        },
    )
    .await;
    wait_for("the replica to resync onto the new replid", || async {
        let replid = state(rdir.path(), 0, "replid").await;
        replid.is_some_and(|r| r != old) && all_synced(rdir.path(), 1).await
    })
    .await;
    assert_eq!(
        state(rdir.path(), 0, "replid").await,
        state(pdir.path(), 0, "replid").await
    );

    writer.abort();
    replica.stop().await;
    primary.stop().await;
}

#[tokio::test]
async fn a_node_claiming_slots_a_live_node_owns_refuses_to_start() {
    let (pdir, qdir, odir) = (
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
    );
    let primary = start(pdir.path(), &Opts::primary("p", 1)).await;

    // 4000-9000 overlaps p's 0-8000. A slow gossip interval gives the guard a
    // long wait, so it hears about p even on a busy machine.
    let o = Opts {
        seeds: vec![primary.gossip.clone()],
        range: Some((4000, 9000)),
        ..Opts::primary("q", 1)
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mut config = cfg(qdir.path(), &o, addr, o.gossip_port);
    config.cluster.as_mut().unwrap().gossip_interval_ms = Some(300);
    let result = timeout(
        Duration::from_secs(30),
        server::run(config, listener, CancellationToken::new()),
    )
    .await
    .expect("startup hung");
    let error = format!("{:?}", result.unwrap_err());
    assert!(
        error.contains("slot conflict") && error.contains("node p ") && error.contains("4000-8000"),
        "{error}"
    );

    // Slots nobody claims start fine, and are advertised once claimed.
    let other = start(
        odir.path(),
        &Opts {
            seeds: vec![primary.gossip.clone()],
            range: Some((OWNED_SLOTS + 1, 16383)),
            ..Opts::primary("o", 1)
        },
    )
    .await;
    let owner = |node: &Node, id: &str| vec![(node.addr.port() as i64, id.to_string())];
    wait_for("both ranges to be listed", || async {
        slots_of(&primary).await
            == vec![
                (0, OWNED_SLOTS as i64, owner(&primary, "p")),
                (OWNED_SLOTS as i64 + 1, 16383, owner(&other, "o")),
            ]
    })
    .await;

    other.stop().await;
    primary.stop().await;
}

/// A go-redis `ReadOnly` cluster client reads from the replica listed in
/// CLUSTER SLOTS: the value it gets exists only in the replica's database.
#[tokio::test(flavor = "multi_thread")]
async fn go_redis_read_only_client_reads_from_the_replica() {
    if std::env::var_os("BLOBASAUR_SKIP_GO_TESTS").is_some() {
        eprintln!("BLOBASAUR_SKIP_GO_TESTS is set; skipping go-redis client tests");
        return;
    }
    let (pdir, rdir) = (TempDir::new().unwrap(), TempDir::new().unwrap());
    let primary = start(pdir.path(), &all_slots(Opts::primary("p", 1))).await;
    let replica = start(
        rdir.path(),
        &Opts::replica("r", "p", vec![primary.gossip.clone()], 1),
    )
    .await;
    wait_for("the replica to be listed", || async {
        slots_of(&primary).await == listing(&primary, &[(&replica, "r")])
    })
    .await;
    sqlx::query("INSERT INTO blobs (key, data, created_at, updated_at) VALUES ('go-key', ?, 0, 0)")
        .bind(&b"from-replica"[..])
        .execute(&mut connect_db(rdir.path(), 0).await)
        .await
        .unwrap();

    let output = tokio::process::Command::new("go")
        .args([
            "test",
            "-count=1",
            "-run",
            "TestClusterReadOnlyReadsFromReplica",
            "./...",
        ])
        .current_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/goredis"))
        .env("BLOBASAUR_ADDR", primary.addr.to_string())
        .env("BLOBASAUR_REPLICA_KEY", "go-key")
        .output()
        .await
        .unwrap_or_else(|e| {
            panic!("failed to run `go test` ({e}); set BLOBASAUR_SKIP_GO_TESTS=1 to skip")
        });
    assert!(
        output.status.success(),
        "go-redis failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    replica.stop().await;
    primary.stop().await;
}

#[tokio::test]
async fn planned_failover_promotes_the_replica_without_losing_writes() {
    const SHARDS: usize = 2;
    let (pdir, rdir) = (TempDir::new().unwrap(), TempDir::new().unwrap());
    let primary = start(
        pdir.path(),
        &Opts {
            async_write: true,
            ..Opts::primary("p", SHARDS)
        },
    )
    .await;
    let replica = start(
        rdir.path(),
        &Opts::replica("r", "p", vec![primary.gossip.clone()], SHARDS),
    )
    .await;
    let mut client = Client::connect(primary.addr).await;
    for i in 0..20 {
        let (key, value) = (k(&format!("k{i}_")), format!("v{i}"));
        ok(&mut client, &["SET", &key, &value]).await;
    }

    // Steps 2 and 3 of the planned procedure.
    let target = watermark(&mut client).await.unwrap();
    let raddr = replica.addr;
    wait_for("the replica to reach the primary's watermark", || {
        reached_by(raddr, &target)
    })
    .await;
    drop(client);

    // Step 4: stop the primary, restart the replica as primary with its slots.
    primary.stop().await;
    replica.stop().await;
    let promoted = start(rdir.path(), &Opts::primary("r", SHARDS)).await;
    let mut client = Client::connect(promoted.addr).await;
    for i in 0..20 {
        let (key, value) = (k(&format!("k{i}_")), format!("v{i}"));
        assert_eq!(
            client.call(&["GET", &key]).await,
            [format!("${}", value.len())]
        );
        assert_eq!(client.line().await, value);
    }
    let marks = watermark(&mut client).await.unwrap();
    assert!(marks.iter().all(|(replid, _)| *replid != target[0].0));
    ok(&mut client, &["SET", &k("after"), "promoted"]).await;

    promoted.stop().await;
}
