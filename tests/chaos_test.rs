//! Jepsen-style fault injection for replication, against real processes.
//!
//! Each scenario spawns the `blobasaur` binary as a primary and a replica with
//! generated configs, runs concurrent clients that record every operation and
//! its outcome, injects faults (a TCP proxy on the replication link, `kill -9`,
//! SIGTERM restarts, a simulated power loss), and checks the invariants from
//! REPLICATION.md on the history and on the shard DB files afterwards.
//!
//! Every scenario runs in two modes (`sync`, and `async_write` with
//! `batch_size = 100`, `batch_timeout_ms = 10`), as `<scenario>::sync` and
//! `<scenario>::async_write`.
//!
//! Durability in async mode: a write is acked once it is queued, before it
//! commits, so an ack alone does not make it durable. Each primary process
//! lifetime has durability points: a successful `BLOBASAUR.WATERMARK` (a writer
//! barrier: everything acked before the call was *sent* is committed when it
//! replies) and a SIGTERM shutdown that exited 0 (the drain commits everything
//! queued). An op is durable if, in the lifetime that acked it, it was acked
//! before a WATERMARK was sent, or the lifetime ended gracefully. Ops acked after
//! the last durability point of a lifetime that ended in `kill -9` are
//! indeterminate. See [`Journal`] and [`Durability`]. In sync mode an ack is
//! durable at once.
//!
//! * `BLOBASAUR_CHAOS_SEED`: u64 seed of the fault schedule and the workload
//!   (random if unset, always printed; output shows on failure or `--nocapture`).
//! * `BLOBASAUR_CHAOS_SECS`: seconds of faulted workload per scenario (default 6).
//!
//! The primary's `advertise_addr` is the proxy, so the replica reaches it only
//! through the proxy. Gossip (UDP, advertised host + `cluster.port`) and clients
//! go straight to the primary. On failure the temp dir is kept and the log
//! paths and tails are printed.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::future::Future;
use std::hash::{BuildHasher, RandomState};
use std::io;
use std::net::{SocketAddr, TcpListener as StdListener, UdpSocket};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use sqlx::{Connection, SqliteConnection};
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};

const SHARDS: usize = 2;
const CLIENTS: usize = 4;
/// Short, so a Cut of a few seconds is enough to trim tombstones.
const RETENTION_SECS: u64 = 2;

// ---------------------------------------------------------------- utilities

/// xorshift64*: enough randomness for a fault schedule, reproducible by seed.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }

    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }

    fn ms(&mut self, lo: u64, hi: u64) -> Duration {
        Duration::from_millis(lo + self.next() % (hi - lo + 1))
    }
}

fn seed() -> u64 {
    match std::env::var("BLOBASAUR_CHAOS_SEED") {
        Ok(s) => s.parse().expect("BLOBASAUR_CHAOS_SEED must be a u64"),
        Err(_) => RandomState::new().hash_one(std::time::SystemTime::now()),
    }
}

fn secs() -> u64 {
    std::env::var("BLOBASAUR_CHAOS_SECS")
        .map(|s| s.parse().expect("BLOBASAUR_CHAOS_SECS must be a u64"))
        .unwrap_or(6)
}

/// A port that is free now and never handed out twice in this process.
fn free_port(udp: bool) -> u16 {
    static ISSUED: Mutex<BTreeSet<(bool, u16)>> = Mutex::new(BTreeSet::new());
    loop {
        let port = if udp {
            UdpSocket::bind("127.0.0.1:0")
                .unwrap()
                .local_addr()
                .unwrap()
        } else {
            StdListener::bind("127.0.0.1:0")
                .unwrap()
                .local_addr()
                .unwrap()
        }
        .port();
        if ISSUED.lock().unwrap().insert((udp, port)) {
            return port;
        }
    }
}

async fn wait_for<F, Fut>(what: &str, secs: u64, mut condition: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = Instant::now() + Duration::from_secs(secs);
    while !condition().await {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        sleep(Duration::from_millis(20)).await;
    }
}

fn tail(path: &Path, lines: usize) -> String {
    let text = std::fs::read_to_string(path).unwrap_or_default();
    let all: Vec<&str> = text.lines().collect();
    all[all.len().saturating_sub(lines)..].join("\n")
}

// ------------------------------------------------------------- RESP client

#[derive(Debug)]
enum Resp {
    Simple(String),
    Err(String),
    Int(i64),
    Bulk(Option<Vec<u8>>),
    Array(Vec<Resp>),
}

struct Conn(BufReader<TcpStream>);

impl Conn {
    /// Connects and checks the server really answers: a connection to a
    /// process that is still starting would sit in the backlog, and a command
    /// sent then could be applied long after the client gave up on it.
    async fn open(addr: SocketAddr) -> io::Result<Self> {
        let stream = timeout(Duration::from_secs(2), TcpStream::connect(addr))
            .await
            .map_err(|_| io::ErrorKind::TimedOut)??;
        let mut conn = Self(BufReader::new(stream));
        match conn.call(&["PING"], Duration::from_secs(2)).await? {
            Resp::Simple(s) if s == "PONG" => Ok(conn),
            other => Err(io::Error::other(format!("bad PING reply {other:?}"))),
        }
    }

    async fn call(&mut self, args: &[&str], limit: Duration) -> io::Result<Resp> {
        let mut req = format!("*{}\r\n", args.len());
        for arg in args {
            req.push_str(&format!("${}\r\n{}\r\n", arg.len(), arg));
        }
        timeout(limit, async {
            self.0.get_mut().write_all(req.as_bytes()).await?;
            self.reply().await
        })
        .await
        .map_err(|_| io::ErrorKind::TimedOut)?
    }

    async fn cmd(&mut self, args: &[&str]) -> io::Result<Resp> {
        self.call(args, Duration::from_secs(10)).await
    }

    fn reply(&mut self) -> Pin<Box<dyn Future<Output = io::Result<Resp>> + Send + '_>> {
        Box::pin(async move {
            let mut line = String::new();
            if self.0.read_line(&mut line).await? == 0 {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
            let line = line.trim_end().to_string();
            let (kind, rest) = line.split_at(1);
            let num = || rest.parse::<i64>().map_err(io::Error::other);
            Ok(match kind {
                "+" => Resp::Simple(rest.to_string()),
                "-" => Resp::Err(rest.to_string()),
                ":" => Resp::Int(num()?),
                "$" if num()? < 0 => Resp::Bulk(None),
                "$" => {
                    let mut buf = vec![0; num()? as usize + 2];
                    self.0.read_exact(&mut buf).await?;
                    buf.truncate(buf.len() - 2);
                    Resp::Bulk(Some(buf))
                }
                "*" => {
                    let mut items = Vec::new();
                    for _ in 0..num()?.max(0) {
                        items.push(self.reply().await?);
                    }
                    Resp::Array(items)
                }
                _ => return Err(io::Error::other(format!("bad reply {line:?}"))),
            })
        })
    }
}

/// `BLOBASAUR.WATERMARK` as per-shard `(replid, lsn text)`.
async fn watermark(addr: SocketAddr) -> Result<Vec<(String, String)>, String> {
    let mut conn = Conn::open(addr).await.map_err(|e| e.to_string())?;
    let reply = conn
        .call(&["BLOBASAUR.WATERMARK"], Duration::from_secs(30))
        .await
        .map_err(|e| e.to_string())?;
    let Resp::Array(shards) = reply else {
        return Err(format!("{reply:?}"));
    };
    let text = |r: &Resp| match r {
        Resp::Bulk(Some(b)) => String::from_utf8_lossy(b).into_owned(),
        other => format!("{other:?}"),
    };
    Ok(shards
        .iter()
        .enumerate()
        .map(|(shard, s)| match s {
            Resp::Array(f) if f.len() == 3 && matches!(f[0], Resp::Int(i) if i == shard as i64) => {
                (text(&f[1]), text(&f[2]))
            }
            other => (format!("{other:?}"), String::new()),
        })
        .collect())
}

/// Every shard of `marks` is under the primary's replid and at or past its LSN.
fn reached(marks: &[(String, String)], target: &[(String, String)]) -> bool {
    marks.len() == target.len()
        && marks.iter().zip(target).all(|((rid, rl), (pid, pl))| {
            rid == pid && rl.parse::<u64>().is_ok_and(|l| l >= pl.parse().unwrap())
        })
}

// -------------------------------------------------------------------- proxy

#[derive(Clone, Copy, PartialEq, Debug)]
enum Mode {
    /// Forward bytes.
    Pass,
    /// Close every connection and refuse (close at once) new ones.
    Cut,
    /// Keep connections open, forward nothing in either direction.
    Stall,
}

struct Proxy {
    addr: SocketAddr,
    mode: watch::Sender<Mode>,
}

impl Proxy {
    async fn start(upstream: SocketAddr) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (mode, rx) = watch::channel(Mode::Pass);
        tokio::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                if *rx.borrow() != Mode::Cut {
                    tokio::spawn(Self::connection(client, upstream, rx.clone()));
                }
            }
        });
        Self { addr, mode }
    }

    fn set(&self, mode: Mode) {
        eprintln!("[chaos] proxy -> {mode:?}");
        self.mode.send_replace(mode);
    }

    async fn connection(client: TcpStream, upstream: SocketAddr, mode: watch::Receiver<Mode>) {
        let Ok(up) = TcpStream::connect(upstream).await else {
            return;
        };
        let (cr, cw) = client.into_split();
        let (ur, uw) = up.into_split();
        tokio::select! {
            _ = Self::pump(cr, uw, mode.clone()) => {}
            _ = Self::pump(ur, cw, mode) => {}
        }
    }

    async fn pump(
        mut from: OwnedReadHalf,
        mut to: OwnedWriteHalf,
        mut mode: watch::Receiver<Mode>,
    ) {
        let mut buf = vec![0; 16 * 1024];
        loop {
            let current = *mode.borrow_and_update();
            match current {
                Mode::Cut => return,
                Mode::Stall => {
                    if mode.changed().await.is_err() {
                        return;
                    }
                    continue;
                }
                Mode::Pass => {}
            }
            tokio::select! {
                changed = mode.changed() => {
                    if changed.is_err() {
                        return;
                    }
                }
                read = from.read(&mut buf) => match read {
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

// --------------------------------------------------------------------- node

/// How the nodes acknowledge writes.
#[derive(Clone, Copy, PartialEq, Debug)]
enum WriteMode {
    /// `async_write = false`: an ack means committed.
    Sync,
    /// `async_write = true`, `batch_size = 100`: an ack means queued.
    Async { batch_timeout_ms: u64 },
}

impl WriteMode {
    /// The config the docs recommend.
    const ASYNC: WriteMode = WriteMode::Async {
        batch_timeout_ms: 10,
    };

    fn is_async(self) -> bool {
        matches!(self, WriteMode::Async { .. })
    }

    fn label(self) -> &'static str {
        if self.is_async() { "async" } else { "sync" }
    }

    fn config(self) -> String {
        match self {
            WriteMode::Sync => "async_write = false\n".to_string(),
            WriteMode::Async { batch_timeout_ms } => format!(
                "async_write = true\nbatch_size = 100\nbatch_timeout_ms = {batch_timeout_ms}\n"
            ),
        }
    }
}

struct Node {
    name: &'static str,
    dir: PathBuf,
    addr: SocketAddr,
    gossip_port: u16,
    /// What the node tells its peers; for the primary this is the proxy.
    advertise: SocketAddr,
    seeds: Vec<String>,
    replica_of: Option<&'static str>,
    mode: WriteMode,
    /// Where the Prometheus endpoint listens, if enabled.
    metrics: Option<SocketAddr>,
    env: Vec<(String, String)>,
    child: Option<Child>,
}

impl Node {
    fn new(root: &Path, name: &'static str) -> Self {
        let dir = root.join(name);
        std::fs::create_dir_all(dir.join("data")).unwrap();
        let addr: SocketAddr = format!("127.0.0.1:{}", free_port(false)).parse().unwrap();
        Self {
            name,
            dir,
            addr,
            gossip_port: free_port(true),
            advertise: addr,
            seeds: Vec::new(),
            replica_of: None,
            mode: WriteMode::Sync,
            metrics: None,
            env: Vec::new(),
            child: None,
        }
    }

    fn data(&self) -> PathBuf {
        self.dir.join("data")
    }

    fn log(&self) -> PathBuf {
        self.dir.join("node.log")
    }

    fn write_config(&self) -> PathBuf {
        let seeds: Vec<String> = self.seeds.iter().map(|s| format!("\"{s}\"")).collect();
        let mut toml = format!(
            "data_dir = \"{}\"\nnum_shards = {SHARDS}\n{}\
             addr = \"{}\"\nshutdown_timeout_secs = 20\ncleanup_interval_secs = 1\n{}\
             [cluster]\nenabled = true\nnode_id = \"{}\"\nseeds = [{}]\nport = {}\n\
             gossip_interval_ms = 100\nadvertise_addr = \"{}\"\n\
             [replication]\ntombstone_retention_secs = {RETENTION_SECS}\n",
            self.data().display(),
            self.mode.config(),
            self.addr,
            self.metrics
                .map(|a| format!("[metrics]\nenabled = true\naddr = \"{a}\"\n"))
                .unwrap_or_default(),
            self.name,
            seeds.join(", "),
            self.gossip_port,
            self.advertise,
        );
        match self.replica_of {
            Some(primary) => toml.push_str(&format!("replica_of = \"{primary}\"\n")),
            // A primary owns every slot, so clients never see MOVED.
            None => toml.push_str("[[cluster.slot_ranges]]\nstart = 0\nend = 16383\n"),
        }
        let path = self.dir.join("config.toml");
        std::fs::write(&path, toml).unwrap();
        path
    }

    /// Spawns the process (same ports and data dir on every start) and waits
    /// until it answers PING. `Err` if it exits first, e.g. a refused promotion.
    async fn start(&mut self) -> Result<(), String> {
        assert!(self.child.is_none(), "{} is already running", self.name);
        let config = self.write_config();
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.log())
            .unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_blobasaur"))
            .arg("--config")
            .arg(&config)
            .arg("serve")
            .envs(self.env.iter().map(|(k, v)| (k, v)))
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log))
            .spawn()
            .unwrap();
        eprintln!("[chaos] start {} (pid {})", self.name, child.id());
        self.child = Some(child);
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            if let Some(status) = self.child.as_mut().unwrap().try_wait().unwrap() {
                self.child = None;
                return Err(format!("{} exited during startup: {status}", self.name));
            }
            if Conn::open(self.addr).await.is_ok() {
                return Ok(());
            }
            if Instant::now() > deadline {
                return Err(format!("{} never became ready", self.name));
            }
            sleep(Duration::from_millis(50)).await;
        }
    }

    async fn must_start(&mut self) {
        if let Err(e) = self.start().await {
            panic!("{e}");
        }
    }

    fn kill9(&mut self) {
        eprintln!("[chaos] kill -9 {}", self.name);
        let mut child = self.child.take().expect("not running");
        child.kill().unwrap();
        child.wait().unwrap();
    }

    /// SIGTERM and wait for a clean exit.
    async fn stop(&mut self) {
        eprintln!("[chaos] SIGTERM {}", self.name);
        let mut child = self.child.take().expect("not running");
        let pid = child.id().to_string();
        assert!(
            Command::new("kill")
                .args(["-TERM", &pid])
                .status()
                .unwrap()
                .success()
        );
        let deadline = Instant::now() + Duration::from_secs(40);
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                assert!(status.success(), "{} exited with {status}", self.name);
                return;
            }
            if Instant::now() > deadline {
                child.kill().unwrap();
                panic!("{} did not stop on SIGTERM", self.name);
            }
            sleep(Duration::from_millis(50)).await;
        }
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        if let Some(child) = self.child.as_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

// ----------------------------------------------------------------- workload

#[derive(Clone, Copy, PartialEq, Debug)]
enum Outcome {
    /// Acknowledged.
    Ok,
    /// An error reply: not applied.
    Failed,
    /// Connection error or timeout: may or may not have been applied.
    Indeterminate,
}

#[derive(Clone, Debug)]
enum Effect {
    Put(String),
    Del,
    /// EXPIRE far in the future: no change to the value.
    Touch,
}

#[derive(Clone, Debug)]
struct Rec {
    ident: String,
    effect: Effect,
    outcome: Outcome,
    /// [`Journal`] tick taken right after the reply (or the error) was read.
    seq: u64,
}

/// A logical clock shared by every client and fault, so that "acked before the
/// WATERMARK was sent" is a comparison of two numbers. Each `tick` is unique,
/// and a tick taken after an event in real time is higher than one taken before.
#[derive(Default)]
struct Journal {
    clock: AtomicU64,
    /// `(sent, replied)` ticks of every successful `BLOBASAUR.WATERMARK` on the primary.
    marks: Mutex<Vec<(u64, u64)>>,
}

impl Journal {
    fn tick(&self) -> u64 {
        self.clock.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// `BLOBASAUR.WATERMARK` on the primary, recorded as a durability point.
    /// The send tick is taken before anything is sent: every op acked at a lower
    /// tick was acked before the request left, so the barrier covers it.
    async fn durability_point(&self, primary: SocketAddr) -> Result<Vec<(String, String)>, String> {
        let sent = self.tick();
        let marks = watermark(primary).await?;
        let replied = self.tick();
        self.marks.lock().unwrap().push((sent, replied));
        Ok(marks)
    }
}

/// One run of the primary process, in ticks: from when it answered PING until
/// it was dead (`None` while it runs).
#[derive(Clone, Debug)]
struct Lifetime {
    start: u64,
    end: Option<u64>,
    /// SIGTERM, exit status 0.
    graceful: bool,
}

/// Which acked ops are durable, i.e. may be lost by no crash.
///
/// Sync: every acked op. Async: an op acked in a lifetime is durable if that
/// lifetime ended gracefully (the drain commits everything queued), or a
/// WATERMARK was sent after the ack and answered inside the same lifetime.
/// An op whose ack tick falls outside every lifetime (the few microseconds
/// around a start or an exit, where the process that sent the ack is unclear) is
/// never durable. That can only weaken the check, not fail it wrongly.
struct Durability {
    asynchronous: bool,
    lifetimes: Vec<Lifetime>,
    marks: Vec<(u64, u64)>,
}

impl Durability {
    fn durable(&self, r: &Rec) -> bool {
        if r.outcome != Outcome::Ok {
            return false;
        }
        if !self.asynchronous {
            return true;
        }
        let end = |l: &Lifetime| l.end.unwrap_or(u64::MAX);
        let Some(life) = self
            .lifetimes
            .iter()
            .find(|l| l.start < r.seq && r.seq < end(l))
        else {
            return false;
        };
        life.graceful
            || self
                .marks
                .iter()
                .any(|&(sent, replied)| r.seq < sent && replied < end(life))
    }
}

/// A key or a hash field, owned by one client. `ident` is `table/key`, as in the DB.
#[derive(Clone)]
struct Target {
    ns: Option<&'static str>,
    name: String,
}

impl Target {
    fn ident(&self) -> String {
        match self.ns {
            Some(ns) => format!("blobs_{ns}/{}", self.name),
            None => format!("blobs/{}", self.name),
        }
    }
}

fn targets(client: usize) -> Vec<Target> {
    let mut all: Vec<Target> = (0..3)
        .map(|i| Target {
            ns: None,
            name: format!("c{client}k{i}"),
        })
        .collect();
    for i in 0..3 {
        for ns in ["n0", "n1"] {
            all.push(Target {
                ns: Some(ns),
                name: format!("c{client}f{i}"),
            });
        }
    }
    all
}

/// Every value ever sent, with the ident it was written to. Filled before the
/// send, so anything a reader can see is already in it.
type Registry = Arc<Mutex<HashMap<String, String>>>;

struct Workload {
    stop: Arc<AtomicBool>,
    tasks: Vec<JoinHandle<Vec<Rec>>>,
}

impl Workload {
    /// `generation` keeps values unique across workloads of one scenario.
    fn start(
        addr: SocketAddr,
        seed: u64,
        generation: u32,
        registry: &Registry,
        journal: &Arc<Journal>,
        burst: bool,
    ) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let tasks = (0..CLIENTS)
            .map(|id| {
                tokio::spawn(client(
                    id,
                    addr,
                    Rng::new(seed ^ ((id as u64 + 1) << 32) ^ generation as u64),
                    generation,
                    stop.clone(),
                    registry.clone(),
                    journal.clone(),
                    burst,
                ))
            })
            .collect();
        Self { stop, tasks }
    }

    /// Stops the clients; the history in per-client order.
    async fn finish(self) -> Vec<Rec> {
        self.stop.store(true, Ordering::Relaxed);
        let mut recs = Vec::new();
        for task in self.tasks {
            recs.extend(task.await.expect("client panicked"));
        }
        recs
    }
}

#[allow(clippy::too_many_arguments)]
async fn client(
    id: usize,
    addr: SocketAddr,
    mut rng: Rng,
    generation: u32,
    stop: Arc<AtomicBool>,
    registry: Registry,
    journal: Arc<Journal>,
    burst: bool,
) -> Vec<Rec> {
    let all = targets(id);
    let mut conn: Option<Conn> = None;
    let mut recs: Vec<Rec> = Vec::new();
    let (mut n, mut errors) = (0u64, 0);
    while !stop.load(Ordering::Relaxed) {
        if conn.is_none() {
            match Conn::open(addr).await {
                Ok(c) => conn = Some(c),
                Err(_) => {
                    sleep(Duration::from_millis(50)).await;
                    continue;
                }
            }
        }
        let target = &all[rng.below(all.len())];
        let roll = rng.below(100);
        n += 1;
        let value = format!("v{generation}-{id}-{n}");
        let name = target.name.as_str();
        // A burst only SETs, each to a fresh key.
        let fresh = format!("b{generation}x{id}x{n}");
        let (ident, effect, args): (String, Effect, Vec<&str>) = if burst {
            (
                format!("blobs/{fresh}"),
                Effect::Put(value.clone()),
                vec!["SET", &fresh, &value],
            )
        } else {
            let (effect, args) = match (target.ns, roll) {
                (None, 0..65) => (Effect::Put(value.clone()), vec!["SET", name, &value]),
                (None, 65..90) => (Effect::Del, vec!["DEL", name]),
                // EXPIRE is not an async op: it waits for its batch to commit.
                (None, _) => (Effect::Touch, vec!["EXPIRE", name, "2592000"]),
                (Some(ns), 0..70) => (Effect::Put(value.clone()), vec!["HSET", ns, name, &value]),
                (Some(ns), _) => (Effect::Del, vec!["HDEL", ns, name]),
            };
            (target.ident(), effect, args)
        };
        if let Effect::Put(v) = &effect {
            registry.lock().unwrap().insert(v.clone(), ident.clone());
        }
        let outcome = match conn.as_mut().unwrap().cmd(&args).await {
            Ok(Resp::Err(e)) => {
                errors += 1;
                if errors <= 3 {
                    eprintln!("[chaos] client {id}: error reply to {args:?}: {e}");
                }
                Outcome::Failed
            }
            Ok(_) => Outcome::Ok,
            Err(_) => {
                conn = None;
                Outcome::Indeterminate
            }
        };
        recs.push(Rec {
            ident,
            effect,
            outcome,
            seq: journal.tick(),
        });
        // A burst runs flat out.
        if !burst {
            sleep(rng.ms(0, 3)).await;
        }
    }
    recs
}

#[derive(Default, Debug)]
struct Sampled {
    reads: u64,
    present: u64,
    phantoms: Vec<String>,
    /// Every distinct value a read returned, for the failed-op check.
    values: HashSet<String>,
}

/// READONLY GET/HGETs on the replica. A value that no op wrote to that key is a phantom.
fn sampler(
    addr: SocketAddr,
    seed: u64,
    registry: Registry,
    stop: Arc<AtomicBool>,
) -> JoinHandle<Sampled> {
    tokio::spawn(async move {
        let mut rng = Rng::new(seed ^ 0x5A17);
        let all: Vec<Target> = (0..CLIENTS).flat_map(targets).collect();
        let mut seen = Sampled::default();
        let mut conn: Option<Conn> = None;
        while !stop.load(Ordering::Relaxed) {
            if conn.is_none() {
                conn = match Conn::open(addr).await {
                    Ok(mut c) => match c.cmd(&["READONLY"]).await {
                        Ok(Resp::Simple(_)) => Some(c),
                        _ => None,
                    },
                    Err(_) => None,
                };
                if conn.is_none() {
                    sleep(Duration::from_millis(100)).await;
                    continue;
                }
            }
            let target = &all[rng.below(all.len())];
            let args: Vec<&str> = match target.ns {
                None => vec!["GET", &target.name],
                Some(ns) => vec!["HGET", ns, &target.name],
            };
            match conn.as_mut().unwrap().cmd(&args).await {
                Ok(Resp::Bulk(Some(bytes))) => {
                    seen.reads += 1;
                    seen.present += 1;
                    let value = String::from_utf8_lossy(&bytes).into_owned();
                    seen.values.insert(value.clone());
                    if registry.lock().unwrap().get(&value) != Some(&target.ident()) {
                        seen.phantoms
                            .push(format!("{} = {value:?}", target.ident()));
                    }
                }
                Ok(Resp::Bulk(None)) => seen.reads += 1,
                // MOVED while the replica is not synced, for one.
                Ok(_) => {}
                Err(_) => conn = None,
            }
            sleep(Duration::from_millis(3)).await;
        }
        seen
    })
}

/// Periodic `BLOBASAUR.WATERMARK` on the primary, so a lifetime that ends in
/// `kill -9` still has durability points to check against. Errors (the primary
/// is down) are ignored: only a reply is a durability point. Abort to stop.
fn watermarker(journal: Arc<Journal>, primary: SocketAddr, seed: u64) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut rng = Rng::new(seed ^ 0x3A7E);
        loop {
            sleep(rng.ms(200, 500)).await;
            let _ = journal.durability_point(primary).await;
        }
    })
}

// ------------------------------------------------------------------ checker

/// `table/key` to `(data, expires_at)`, across every shard DB of a stopped node.
type Data = BTreeMap<String, (Vec<u8>, Option<i64>)>;

fn shard_url(dir: &Path, shard: usize) -> String {
    format!("sqlite:{}/shard_{shard}.db", dir.display())
}

async fn read_data(dir: &Path) -> Data {
    let mut data = Data::new();
    for shard in 0..SHARDS {
        let mut conn = SqliteConnection::connect(&shard_url(dir, shard))
            .await
            .unwrap();
        let tables: Vec<String> = sqlx::query_scalar(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'blobs%'",
        )
        .fetch_all(&mut conn)
        .await
        .unwrap();
        for table in tables {
            let rows: Vec<(String, Vec<u8>, Option<i64>)> =
                sqlx::query_as(&format!("SELECT key, data, expires_at FROM {table}"))
                    .fetch_all(&mut conn)
                    .await
                    .unwrap();
            for (key, bytes, expires) in rows {
                let ident = format!("{table}/{key}");
                assert!(
                    data.insert(ident.clone(), (bytes, expires)).is_none(),
                    "{ident} twice"
                );
            }
        }
    }
    data
}

/// A `repl_state` value of a shard DB (a running node's too).
async fn repl_state(dir: &Path, shard: usize, k: &str) -> Option<String> {
    let mut conn = SqliteConnection::connect(&shard_url(dir, shard))
        .await
        .ok()?;
    sqlx::query_scalar("SELECT v FROM repl_state WHERE k = ?")
        .bind(k)
        .fetch_optional(&mut conn)
        .await
        .ok()?
}

fn value_of(data: &Data, ident: &str) -> Option<String> {
    data.get(ident)
        .map(|(bytes, _)| String::from_utf8_lossy(bytes).into_owned())
}

/// What a key may hold at the end: the effect of its last durable op, or of
/// any op after it that is acked but not durable, or indeterminate (`None` =
/// absent). In sync mode every acked op is durable, so this is the last acked
/// op or an indeterminate one after it.
fn allowed(ops: &[&Rec], durability: &Durability) -> BTreeSet<Option<String>> {
    let effect = |r: &Rec| match &r.effect {
        Effect::Put(v) => Some(Some(v.clone())),
        Effect::Del => Some(None),
        Effect::Touch => None,
    };
    let last_durable = ops
        .iter()
        .rposition(|r| durability.durable(r) && effect(r).is_some());
    let mut set = BTreeSet::new();
    let from = match last_durable {
        Some(i) => {
            set.insert(effect(ops[i]).unwrap());
            i + 1
        }
        None => {
            set.insert(None);
            0
        }
    };
    for r in &ops[from..] {
        if matches!(r.outcome, Outcome::Ok | Outcome::Indeterminate)
            && let Some(e) = effect(r)
        {
            set.insert(e);
        }
    }
    set
}

/// Keys that violate the durability rule, and keys in the DB nobody wrote.
fn durability_violations(recs: &[Rec], data: &Data, durability: &Durability) -> Vec<String> {
    let mut by_key: BTreeMap<&str, Vec<&Rec>> = BTreeMap::new();
    for r in recs {
        by_key.entry(&r.ident).or_default().push(r);
    }
    let mut bad: Vec<String> = by_key
        .iter()
        .filter_map(|(ident, ops)| {
            let allowed = allowed(ops, durability);
            let found = value_of(data, ident);
            (!allowed.contains(&found))
                .then(|| format!("{ident}: found {found:?}, allowed {allowed:?}"))
        })
        .collect();
    bad.extend(
        data.keys()
            .filter(|k| !by_key.contains_key(k.as_str()))
            .map(|k| format!("{k}: in the DB but never written")),
    );
    bad
}

/// A replica read never returns a value whose op got an error reply.
fn failed_read_violations(recs: &[Rec], seen: &Sampled) -> Vec<String> {
    recs.iter()
        .filter(|r| r.outcome == Outcome::Failed)
        .filter_map(|r| match &r.effect {
            Effect::Put(v) if seen.values.contains(v) => {
                Some(format!("{}: a replica read returned failed {v:?}", r.ident))
            }
            _ => None,
        })
        .collect()
}

/// Keys holding a value no op wrote to that key.
fn phantom_violations(data: &Data, registry: &Registry) -> Vec<String> {
    let registry = registry.lock().unwrap();
    data.keys()
        .filter(|ident| registry.get(&value_of(data, ident).unwrap()) != Some(*ident))
        .map(|ident| format!("{ident}: phantom value {:?}", value_of(data, ident)))
        .collect()
}

fn assert_converged(primary: &Data, replica: &Data) {
    let diff: Vec<String> = primary
        .keys()
        .chain(replica.keys())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .filter(|k| primary.get(*k) != replica.get(*k))
        .map(|k| {
            format!(
                "{k}: primary {:?}, replica {:?}",
                value_of(primary, k),
                value_of(replica, k)
            )
        })
        .collect();
    assert!(
        diff.is_empty(),
        "replica diverged from the primary on {} keys:\n{}",
        diff.len(),
        diff[..diff.len().min(15)].join("\n")
    );
}

/// The replica's cursor must never run ahead of its rows: every change the
/// primary logged at or below the replica's `applied_lsn` (same replid) is in
/// the replica's data, because rows and cursor commit together. Holds while the
/// primary takes writes (a changed key gets a new, higher LSN), so it runs
/// right after a `kill -9` of the replica, whose files are then static.
async fn assert_cursor_matches_rows(primary: &Path, replica: &Path) {
    type Row = (Vec<u8>, Option<i64>);
    let mut bad = Vec::new();
    for shard in 0..SHARDS {
        let applied = repl_state(replica, shard, "applied_lsn").await;
        let same_history = repl_state(primary, shard, "replid").await
            == repl_state(replica, shard, "replid").await;
        let (Some(applied), true) = (applied, same_history) else {
            continue;
        };
        let mut p = SqliteConnection::connect(&shard_url(primary, shard))
            .await
            .unwrap();
        let mut r = SqliteConnection::connect(&shard_url(replica, shard))
            .await
            .unwrap();
        // One read snapshot of the primary: the log and the rows agree.
        let mut snapshot = p.begin().await.unwrap();
        let logged: Vec<(String, String, i64)> =
            sqlx::query_as("SELECT tbl, key, lsn FROM repl_log WHERE lsn <= ?")
                .bind(applied.parse::<i64>().unwrap())
                .fetch_all(&mut *snapshot)
                .await
                .unwrap();
        for (tbl, key, lsn) in logged {
            let sql = format!("SELECT data, expires_at FROM {tbl} WHERE key = ?");
            let on_primary: Option<Row> = sqlx::query_as(&sql)
                .bind(&key)
                .fetch_optional(&mut *snapshot)
                .await
                .unwrap();
            // The replica may not have the namespace table at all.
            let on_replica: Option<Row> = sqlx::query_as(&sql)
                .bind(&key)
                .fetch_optional(&mut r)
                .await
                .ok()
                .flatten();
            if on_primary != on_replica {
                let text = |r: Option<Row>| r.map(|r| String::from_utf8_lossy(&r.0).into_owned());
                bad.push(format!(
                    "shard {shard} {tbl}/{key} (lsn {lsn} <= applied {applied}): primary {:?}, replica {:?}",
                    text(on_primary),
                    text(on_replica),
                ));
            }
        }
    }
    assert_none("the replica's cursor ran ahead of its rows", bad);
}

fn assert_none(what: &str, violations: Vec<String>) {
    assert!(
        violations.is_empty(),
        "{what}: {} violations:\n{}",
        violations.len(),
        violations[..violations.len().min(15)].join("\n")
    );
}

fn outcomes(recs: &[Rec]) -> String {
    let count = |o| recs.iter().filter(|r| r.outcome == o).count();
    format!(
        "{} ops: {} ok, {} failed, {} indeterminate",
        recs.len(),
        count(Outcome::Ok),
        count(Outcome::Failed),
        count(Outcome::Indeterminate)
    )
}

// ---------------------------------------------------------------------- rig

/// A primary behind a proxy, a replica, and the bookkeeping of one scenario.
struct Rig {
    name: String,
    seed: u64,
    mode: WriteMode,
    root: Option<TempDir>,
    p: Node,
    r: Node,
    proxy: Proxy,
    registry: Registry,
    journal: Arc<Journal>,
    /// Every run of the primary process, oldest first.
    lifetimes: Vec<Lifetime>,
    /// The periodic `BLOBASAUR.WATERMARK` on the primary (async only).
    marker: Option<JoinHandle<()>>,
    started: Instant,
}

impl Rig {
    async fn new(name: &str, seed: u64, mode: WriteMode) -> Self {
        let name = format!("{name}[{}]", mode.label());
        eprintln!("[chaos] scenario {name}: BLOBASAUR_CHAOS_SEED={seed}");
        let root = TempDir::new().unwrap();
        let mut p = Node::new(root.path(), "p");
        let proxy = Proxy::start(p.addr).await;
        p.advertise = proxy.addr;
        let mut r = Node::new(root.path(), "r");
        r.replica_of = Some("p");
        r.seeds = vec![format!("127.0.0.1:{}", p.gossip_port)];
        for node in [&mut p, &mut r] {
            node.mode = mode;
            node.metrics = Some(format!("127.0.0.1:{}", free_port(false)).parse().unwrap());
        }
        Self {
            name,
            seed,
            mode,
            root: Some(root),
            p,
            r,
            proxy,
            registry: Registry::default(),
            journal: Arc::default(),
            lifetimes: Vec::new(),
            marker: None,
            started: Instant::now(),
        }
    }

    /// Starts both nodes and waits for the replica's first full copy. In async
    /// mode the primary then gets its periodic durability points.
    async fn start(&mut self) {
        self.start_primary().await;
        self.r.must_start().await;
        self.wait_replica_synced().await;
        if self.mode.is_async() {
            self.marker = Some(watermarker(self.journal.clone(), self.p.addr, self.seed));
        }
    }

    /// Stops the periodic WATERMARKs. One in flight is dropped unrecorded.
    async fn stop_watermarker(&mut self) {
        if let Some(task) = self.marker.take() {
            task.abort();
            let _ = task.await;
        }
    }

    async fn start_primary(&mut self) {
        self.p.must_start().await;
        self.lifetimes.push(Lifetime {
            start: self.journal.tick(),
            end: None,
            graceful: false,
        });
    }

    fn kill_primary(&mut self) {
        self.p.kill9();
        self.lifetimes.last_mut().unwrap().end = Some(self.journal.tick());
    }

    /// SIGTERM; `stop` asserts the exit status is 0.
    async fn stop_primary(&mut self) {
        self.p.stop().await;
        let lifetime = self.lifetimes.last_mut().unwrap();
        lifetime.end = Some(self.journal.tick());
        lifetime.graceful = true;
    }

    /// What is durable, as of now.
    fn durability(&self) -> Durability {
        Durability {
            asynchronous: self.mode.is_async(),
            lifetimes: self.lifetimes.clone(),
            marks: self.journal.marks.lock().unwrap().clone(),
        }
    }

    async fn wait_replica_synced(&self) {
        let addr = self.r.addr;
        wait_for("the replica's first sync", 60, || async move {
            watermark(addr)
                .await
                .is_ok_and(|m| m.iter().all(|(_, lsn)| lsn != "not-synced"))
        })
        .await;
    }

    /// The replica is at `target`, same replid.
    async fn wait_reached(&self, target: &[(String, String)]) {
        let addr = self.r.addr;
        wait_for(
            "the replica to reach the primary's watermark",
            90,
            || async { watermark(addr).await.is_ok_and(|m| reached(&m, target)) },
        )
        .await;
    }

    /// Writes reach the replica: it is at the primary's watermark, same replid.
    async fn wait_caught_up(&self) {
        let target = self
            .journal
            .durability_point(self.p.addr)
            .await
            .expect("primary WATERMARK");
        self.wait_reached(&target).await;
    }

    fn workload(&self, generation: u32) -> Workload {
        self.workload_with(generation, false)
    }

    /// SETs of fresh keys, flat out. In async mode a SET is acked as soon as
    /// it is queued, so with the writers stalled the clients fill their queues
    /// with acked, uncommitted writes. (DEL, HSET and HDEL check for the
    /// key through the writer's own connection first, which holds them up
    /// behind the commit in progress, and EXPIRE waits for its commit.)
    fn burst(&self, generation: u32) -> Workload {
        self.workload_with(generation, true)
    }

    fn workload_with(&self, generation: u32, burst: bool) -> Workload {
        Workload::start(
            self.p.addr,
            self.seed,
            generation,
            &self.registry,
            &self.journal,
            burst,
        )
    }

    fn sampler(&self) -> (Arc<AtomicBool>, JoinHandle<Sampled>) {
        let stop = Arc::new(AtomicBool::new(false));
        let task = sampler(self.r.addr, self.seed, self.registry.clone(), stop.clone());
        (stop, task)
    }

    /// Random faults for `secs`; both nodes are up and the link passes at the end.
    async fn nemesis(&mut self, rng: &mut Rng, secs: u64, primary_faults: bool) {
        let end = Instant::now() + Duration::from_secs(secs);
        while Instant::now() < end {
            sleep(rng.ms(100, 900)).await;
            let roll = rng.below(100);
            match roll {
                0..25 => self.link(Mode::Cut, rng.ms(200, 3500)).await,
                25..40 => self.link(Mode::Stall, rng.ms(300, 2500)).await,
                40..55 => {
                    self.r.kill9();
                    sleep(rng.ms(100, 1500)).await;
                    self.r.must_start().await;
                }
                55..70 => {
                    self.r.stop().await;
                    sleep(rng.ms(0, 500)).await;
                    self.r.must_start().await;
                }
                70..80 if primary_faults => {
                    self.kill_primary();
                    sleep(rng.ms(100, 800)).await;
                    self.start_primary().await;
                }
                80..85 if primary_faults => {
                    self.stop_primary().await;
                    self.start_primary().await;
                }
                // Long enough for tombstones to age out.
                _ => self.link(Mode::Cut, rng.ms(3500, 5000)).await,
            }
        }
        self.proxy.set(Mode::Pass);
    }

    async fn link(&self, mode: Mode, duration: Duration) {
        self.proxy.set(mode);
        sleep(duration).await;
        self.proxy.set(Mode::Pass);
    }

    /// The runbook's promotion: restart the replica's data dir as a primary
    /// owning every slot, retrying while the startup guard refuses.
    async fn promote(&mut self) {
        self.r.replica_of = None;
        self.r.seeds = vec![format!("127.0.0.1:{}", self.p.gossip_port)];
        for attempt in 1..=30 {
            match self.r.start().await {
                Ok(()) => return,
                Err(e) => {
                    eprintln!("[chaos] promotion attempt {attempt} failed: {e}");
                    sleep(Duration::from_secs(1)).await;
                }
            }
        }
        panic!("the replica could not be promoted");
    }

    fn resyncs(&self) -> usize {
        std::fs::read_to_string(self.r.log())
            .unwrap_or_default()
            .matches("resyncing from")
            .count()
    }

    /// What the replica's READONLY reads showed, checked while the replica is up
    /// and the sampler has stopped: no phantom, no value of an op that got an
    /// error reply, and the replica's inflight caches were never used (it only
    /// serves committed data).
    async fn check_replica_reads(&self, recs: &[Rec], seen: Sampled) {
        let durability = self.durability();
        let acked = recs.iter().filter(|r| r.outcome == Outcome::Ok).count();
        let durable = recs.iter().filter(|r| durability.durable(r)).count();
        let text = scrape(self.r.metrics.unwrap()).await;
        let reads = |source: &str| metric_sum(&text, "blobasaur_reads_total", source);
        eprintln!(
            "[chaos] {}: {}; {durable} of {acked} acked durable; replica reads {} ({} present), \
             inflight {}, sqlite {}; replica resyncs {}; {:?}",
            self.name,
            outcomes(recs),
            seen.reads,
            seen.present,
            reads("source=\"inflight\""),
            reads("source=\"sqlite\""),
            self.resyncs(),
            self.started.elapsed()
        );
        assert_none("phantom reads on the replica", seen.phantoms.clone());
        assert_none(
            "reads of failed writes on the replica",
            failed_read_violations(recs, &seen),
        );
        assert!(text.contains("blobasaur_"), "no metrics from the replica");
        assert_eq!(
            reads("source=\"inflight\""),
            0.0,
            "the replica answered a read from an inflight cache"
        );
        assert_eq!(
            metric_sum(&text, "blobasaur_async_writes_pending", ""),
            0.0,
            "the replica has async writes pending"
        );
    }
}

impl Drop for Rig {
    fn drop(&mut self) {
        if let Some(task) = &self.marker {
            task.abort();
        }
        if !std::thread::panicking() {
            return;
        }
        eprintln!(
            "[chaos] FAILED scenario {} after {:?}: reproduce with BLOBASAUR_CHAOS_SEED={}",
            self.name,
            self.started.elapsed(),
            self.seed
        );
        if let Some(root) = self.root.take() {
            eprintln!("[chaos] kept {}", root.keep().display());
        }
        for node in [&self.p, &self.r] {
            eprintln!(
                "[chaos] ---- {} log {} (last 40 lines)\n{}",
                node.name,
                node.log().display(),
                tail(&node.log(), 40)
            );
        }
    }
}

/// `GET /metrics` of a node's Prometheus endpoint.
async fn scrape(addr: SocketAddr) -> String {
    let mut stream = TcpStream::connect(addr).await.expect("metrics endpoint");
    stream
        .write_all(b"GET /metrics HTTP/1.0\r\n\r\n")
        .await
        .unwrap();
    let mut text = String::new();
    stream.read_to_string(&mut text).await.unwrap();
    text
}

/// The sum of the samples of metric `name` whose labels contain `label`.
fn metric_sum(text: &str, name: &str, label: &str) -> f64 {
    text.lines()
        .filter(|l| {
            l.strip_prefix(name)
                .is_some_and(|rest| rest.starts_with('{') && l.contains(label))
        })
        .filter_map(|l| l.rsplit(' ').next()?.parse::<f64>().ok())
        .fold(0.0, |total, v| total + v)
}

// ---------------------------------------------------------------- scenarios

/// Link faults and node restarts under load. Then: durability on the primary,
/// convergence, and no phantoms on the replica.
async fn link_faults(mode: WriteMode) {
    let mut rig = Rig::new("link_faults", seed(), mode).await;
    let mut rng = Rng::new(rig.seed);
    rig.start().await;
    let (stop_sampler, sampler) = rig.sampler();
    let workload = rig.workload(0);
    rig.nemesis(&mut rng, secs(), true).await;
    let mut recs = workload.finish().await;

    // Crash cycles: kill -9 the replica while it applies a live stream and
    // restart it, over and over. The replica's cursor must never run ahead of
    // its rows (checked at each kill), and with the writes stopped it converges.
    let cycles = (secs() / 2).max(3);
    let workload = rig.workload(1);
    for _ in 0..cycles {
        sleep(rng.ms(200, 700)).await;
        rig.r.kill9();
        assert_cursor_matches_rows(&rig.p.data(), &rig.r.data()).await;
        rig.r.must_start().await;
    }
    recs.extend(workload.finish().await);
    rig.wait_caught_up().await;
    let (primary, replica) = (
        read_data(&rig.p.data()).await,
        read_data(&rig.r.data()).await,
    );
    assert_converged(&primary, &replica);

    // Trim episode: cut the link, delete some more, let the tombstones age out
    // past the retention and get trimmed, then heal. The replica's cursor is
    // now behind `trimmed_through` and it has to RESYNC.
    let workload = rig.workload(2);
    rig.proxy.set(Mode::Cut);
    sleep(Duration::from_secs(1)).await;
    recs.extend(workload.finish().await);
    sleep(Duration::from_secs(RETENTION_SECS + 2)).await;
    rig.proxy.set(Mode::Pass);

    rig.wait_caught_up().await;
    stop_sampler.store(true, Ordering::Relaxed);
    let seen = sampler.await.unwrap();
    rig.check_replica_reads(&recs, seen).await;

    rig.r.stop().await;
    rig.stop_primary().await;
    let (primary, replica) = (
        read_data(&rig.p.data()).await,
        read_data(&rig.r.data()).await,
    );
    assert_none(
        "durability on the primary",
        durability_violations(&recs, &primary, &rig.durability()),
    );
    assert_converged(&primary, &replica);
    assert_none(
        "phantoms on the replica",
        phantom_violations(&replica, &rig.registry),
    );
}

/// The primary restarts under load, over and over: SIGTERM (an exit status 0
/// is a durability point: the drain commits everything queued) or `kill -9`
/// (only what a WATERMARK covered survives for sure). With the primary dead and
/// its files still, every acked write must be durable in the sense of the
/// rule, checked after each restart so a later write can't hide a loss.
async fn primary_restarts(mode: WriteMode) {
    let mut rig = Rig::new("primary_restarts", seed(), mode).await;
    let mut rng = Rng::new(rig.seed);
    rig.start().await;
    let (stop_sampler, sampler) = rig.sampler();
    let mut recs = Vec::new();
    let cycles = (secs() / 2).max(4);
    for cycle in 0..cycles {
        let workload = rig.workload(cycle as u32);
        sleep(rng.ms(500, 1200)).await;
        // A burst right before the stop leaves a deep queue of acked writes.
        let burst = rig.burst(1000 + cycle as u32);
        sleep(Duration::from_millis(30)).await;
        // The first is always a SIGTERM, so every run has one.
        if cycle == 0 || rng.below(3) > 0 {
            rig.stop_primary().await;
        } else {
            rig.kill_primary();
        }
        recs.extend(workload.finish().await);
        recs.extend(burst.finish().await);
        let on_disk = read_data(&rig.p.data()).await;
        assert_none(
            &format!("durability on the primary after restart {cycle}"),
            durability_violations(&recs, &on_disk, &rig.durability()),
        );
        rig.start_primary().await;
    }
    rig.wait_caught_up().await;
    stop_sampler.store(true, Ordering::Relaxed);
    let seen = sampler.await.unwrap();
    rig.check_replica_reads(&recs, seen).await;

    rig.r.stop().await;
    rig.stop_primary().await;
    let (primary, replica) = (
        read_data(&rig.p.data()).await,
        read_data(&rig.r.data()).await,
    );
    assert_converged(&primary, &replica);
    assert_none(
        "phantoms on the replica",
        phantom_violations(&replica, &rig.registry),
    );
}

/// How many burst keys (see [`Rig::burst`]) the shard DBs of a node hold now.
async fn burst_rows(readers: &mut [SqliteConnection]) -> u64 {
    let mut rows = 0;
    for conn in readers {
        let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM blobs WHERE key LIKE 'b%'")
            .fetch_one(&mut *conn)
            .await
            .unwrap();
        rows += n as u64;
    }
    rows
}

/// The CLUSTERING.md planned failover: no acknowledged write is lost.
///
/// In async mode the writes are acked before they commit, so this is where the
/// WATERMARK barrier is proven. After the faulted workload and a catch-up, a
/// burst of SETs gets ahead of the shard writers, the clients are stopped and
/// WATERMARK is called at once: hundreds of acked writes are still queued, and
/// the barrier has to wait for them. Two checks follow:
///
/// * the instant the reply arrives, the primary's DBs hold every burst write
///   acked before the call (they are fresh keys, so a row count is enough);
/// * the replica catches up, the primary gets `kill -9`, the replica is
///   promoted, and every write acked before the call is on it.
///
/// The second can only fail if the pending writes outlive the replica's catch-up
/// (they commit within milliseconds), so the first is the deterministic one.
/// (`batch_timeout_ms` can't make a window: the writer commits whatever is
/// queued as soon as it is free, whatever the timeout.)
async fn planned_failover(mode: WriteMode) {
    let mut rig = Rig::new("planned_failover", seed(), mode).await;
    let mut rng = Rng::new(rig.seed);
    rig.start().await;
    let (stop_sampler, sampler) = rig.sampler();
    let workload = rig.workload(0);
    rig.nemesis(&mut rng, secs(), false).await;
    let mut recs = workload.finish().await;
    rig.stop_watermarker().await;

    let mut burst = None;
    if mode.is_async() {
        rig.wait_caught_up().await;
        burst = Some(rig.burst(1));
        sleep(Duration::from_millis(30)).await;
        // Not awaited: a client blocked on a full queue returns once it drains.
        burst.as_ref().unwrap().stop.store(true, Ordering::Relaxed);
    }
    let mut readers = Vec::new();
    for shard in 0..SHARDS {
        readers.push(
            SqliteConnection::connect(&shard_url(&rig.p.data(), shard))
                .await
                .unwrap(),
        );
    }
    // What is committed just before the call. Burst writes acked before this
    // tick and not among these rows were queued, uncommitted, at the call.
    let rows_before = burst_rows(&mut readers).await;
    let read_tick = rig.journal.tick();
    // Ops acked before this is sent are covered; ones racing with it may or may
    // not be.
    let target = rig
        .journal
        .durability_point(rig.p.addr)
        .await
        .expect("primary WATERMARK");
    let rows_at_reply = burst_rows(&mut readers).await;
    let sent = rig.journal.marks.lock().unwrap().last().unwrap().0;
    rig.wait_reached(&target).await;

    // Fence the primary, promote the replica.
    rig.kill_primary();
    let (mut acked_before_send, mut acked_before_read) = (0, 0);
    if let Some(burst) = burst {
        let burst = burst.finish().await;
        let acked_before = |tick| {
            burst
                .iter()
                .filter(|r| r.outcome == Outcome::Ok && r.seq < tick)
                .count() as u64
        };
        (acked_before_send, acked_before_read) = (acked_before(sent), acked_before(read_tick));
        recs.extend(burst);
    }
    let queued_at_call = acked_before_read.saturating_sub(rows_before);
    eprintln!(
        "[chaos] {}: {queued_at_call} of {acked_before_read} burst writes acked before the \
         WATERMARK were queued, uncommitted, when it was called; {acked_before_send} were acked \
         before it was sent, {rows_at_reply} rows were committed when it replied",
        rig.name
    );
    stop_sampler.store(true, Ordering::Relaxed);
    let seen = sampler.await.unwrap();
    rig.check_replica_reads(&recs, seen).await;
    rig.r.stop().await;
    rig.promote().await;
    let new = watermark(rig.r.addr).await.unwrap();
    assert!(
        new.iter().zip(&target).all(|((n, _), (o, _))| n != o),
        "promotion kept the replid: {target:?} -> {new:?}"
    );
    let mut conn = Conn::open(rig.r.addr).await.unwrap();
    assert!(matches!(
        conn.cmd(&["SET", "after-promotion", "x"]).await,
        Ok(Resp::Simple(_))
    ));
    drop(conn);

    rig.r.stop().await;
    let mut promoted = read_data(&rig.r.data()).await;
    assert!(promoted.remove("blobs/after-promotion").is_some());
    assert_none(
        "durability on the promoted node",
        durability_violations(&recs, &promoted, &rig.durability()),
    );
    assert!(
        rows_at_reply >= acked_before_send,
        "WATERMARK replied while {} writes acked before it were not committed",
        acked_before_send - rows_at_reply
    );
    // Last, so that a barrier that didn't wait fails on the lost writes above.
    assert!(
        !mode.is_async() || queued_at_call > 0,
        "nothing was queued at the WATERMARK: the barrier was not exercised"
    );
}

/// The primary dies mid-workload and the replica is promoted: acked writes
/// may be lost (reported, not asserted), but nothing may be invented.
async fn unplanned_failover(mode: WriteMode) {
    let mut rig = Rig::new("unplanned_failover", seed(), mode).await;
    let mut rng = Rng::new(rig.seed);
    rig.start().await;
    let (stop_sampler, sampler) = rig.sampler();
    let workload = rig.workload(0);

    sleep(rng.ms(1500, 1500 + secs() * 700)).await;
    if rng.below(2) == 0 {
        // The replica falls behind right before the primary dies.
        rig.proxy.set(Mode::Stall);
        sleep(rng.ms(200, 1500)).await;
    }
    let old = rig.journal.durability_point(rig.p.addr).await.unwrap();
    rig.kill_primary();
    let recs = workload.finish().await;
    stop_sampler.store(true, Ordering::Relaxed);
    let seen = sampler.await.unwrap();
    rig.check_replica_reads(&recs, seen).await;

    if rng.below(2) == 0 {
        rig.r.stop().await;
    } else {
        rig.r.kill9();
    }
    rig.promote().await;
    let new = watermark(rig.r.addr).await.unwrap();
    assert!(
        new.iter().zip(&old).all(|((n, _), (o, _))| n != o),
        "promotion kept the replid: {old:?} -> {new:?}"
    );
    rig.r.stop().await;
    let promoted = read_data(&rig.r.data()).await;
    assert_none(
        "phantoms on the promoted node",
        phantom_violations(&promoted, &rig.registry),
    );
    let lost = durability_violations(&recs, &promoted, &rig.durability());
    eprintln!(
        "[chaos] unplanned failover lost {} keys that were durable on the old primary (info only)",
        lost.len()
    );
}

/// Power loss on the primary: its shards go back to an earlier committed
/// prefix with `clean_shutdown = 0` and a different boot id, while the replica
/// already applied writes the primary no longer has. A running async primary's
/// snapshot holds committed rows only, which is what a power loss leaves too.
async fn power_loss_on_the_primary(mode: WriteMode) {
    let mut rig = Rig::new("power_loss", seed(), mode).await;
    let boot_file = rig.root.as_ref().unwrap().path().join("boot_id");
    std::fs::write(&boot_file, "boot-before\n").unwrap();
    rig.p.env = vec![(
        "BLOBASAUR_BOOT_ID_FILE".into(),
        boot_file.display().to_string(),
    )];
    rig.start().await;
    let (stop_sampler, sampler) = rig.sampler();
    let workload = rig.workload(0);
    sleep(Duration::from_millis(1500 + secs() * 300)).await;

    // A consistent snapshot of every primary shard, taken while it runs.
    let snaps = rig.root.as_ref().unwrap().path().join("snapshots");
    std::fs::create_dir_all(&snaps).unwrap();
    let mut taken = Vec::new(); // (replid, highest lsn) per shard
    for shard in 0..SHARDS {
        let mut conn = SqliteConnection::connect(&shard_url(&rig.p.data(), shard))
            .await
            .unwrap();
        sqlx::query(&format!(
            "VACUUM INTO '{}'",
            snaps.join(format!("shard_{shard}.db")).display()
        ))
        .execute(&mut conn)
        .await
        .unwrap();
        let mut snap = SqliteConnection::connect(&shard_url(&snaps, shard))
            .await
            .unwrap();
        let lsn: i64 = sqlx::query_scalar(
            "SELECT COALESCE(MAX(seq), 0) FROM sqlite_sequence WHERE name = 'repl_log'",
        )
        .fetch_one(&mut snap)
        .await
        .unwrap();
        taken.push((
            repl_state(&snaps, shard, "replid").await.unwrap(),
            lsn as u64,
        ));
        assert_eq!(
            repl_state(&snaps, shard, "clean_shutdown").await.as_deref(),
            Some("0")
        );
    }
    assert!(taken.iter().any(|(_, lsn)| *lsn > 0), "snapshots are empty");

    // Keep writing until the replica holds a write the snapshot lacks.
    let rdir = rig.r.data();
    wait_for(
        "the replica to apply a write made after the snapshot",
        60,
        || async {
            for (shard, (_, snap_lsn)) in taken.iter().enumerate() {
                let applied = repl_state(&rdir, shard, "applied_lsn").await;
                if applied.is_some_and(|a| a.parse::<u64>().unwrap() > *snap_lsn) {
                    return true;
                }
            }
            false
        },
    )
    .await;

    // Power loss: the unsynced WAL tail is gone, a committed prefix is left.
    // The link stays cut until the restored primary's LSNs have passed the
    // replica's cursor, so only the replid (not the "cursor above the
    // watermark" safety net) can tell the replica that its history is gone.
    rig.proxy.set(Mode::Cut);
    rig.stop_watermarker().await;
    rig.kill_primary();
    let before = workload.finish().await;
    sleep(Duration::from_millis(300)).await;
    let mut cursors = Vec::new();
    for shard in 0..SHARDS {
        let applied = repl_state(&rdir, shard, "applied_lsn").await.unwrap();
        cursors.push(applied.parse::<u64>().unwrap());
        let db = rig.p.data().join(format!("shard_{shard}.db"));
        let _ = std::fs::remove_file(rig.p.data().join(format!("shard_{shard}.db-wal")));
        let _ = std::fs::remove_file(rig.p.data().join(format!("shard_{shard}.db-shm")));
        std::fs::copy(snaps.join(format!("shard_{shard}.db")), db).unwrap();
    }
    std::fs::write(&boot_file, "boot-after\n").unwrap();
    rig.start_primary().await;

    let marks = watermark(rig.p.addr).await.unwrap();
    for (shard, (replid, _)) in marks.iter().enumerate() {
        assert_ne!(
            *replid, taken[shard].0,
            "shard {shard}: the primary kept its replid"
        );
    }

    // New writes on the restored primary, reusing LSNs the replica has seen.
    let mut conn = Conn::open(rig.p.addr).await.unwrap();
    let mut filler = 0;
    loop {
        for _ in 0..50 {
            filler += 1;
            let (key, value) = (format!("filler{filler}"), format!("vF-{filler}"));
            rig.registry
                .lock()
                .unwrap()
                .insert(value.clone(), format!("blobs/{key}"));
            assert!(matches!(
                conn.cmd(&["SET", &key, &value]).await,
                Ok(Resp::Simple(_))
            ));
        }
        let marks = watermark(rig.p.addr).await.unwrap();
        let past = marks
            .iter()
            .zip(&cursors)
            .all(|((_, lsn), cursor)| lsn.parse::<u64>().unwrap() > *cursor);
        assert!(
            filler < 20_000 || past,
            "primary LSNs never passed the replica's"
        );
        if past {
            break;
        }
    }
    rig.proxy.set(Mode::Pass);
    rig.wait_caught_up().await;
    for (shard, (replid, _)) in taken.iter().enumerate() {
        let now = repl_state(&rdir, shard, "replid").await.unwrap();
        assert_ne!(
            now, *replid,
            "shard {shard}: the replica kept the old replid"
        );
    }
    // Each shard's first copy was a resync too.
    assert!(rig.resyncs() >= 2 * SHARDS, "the replica never resynced");
    stop_sampler.store(true, Ordering::Relaxed);
    let seen = sampler.await.unwrap();
    rig.check_replica_reads(&before, seen).await;

    rig.r.stop().await;
    rig.stop_primary().await;
    let (primary, replica) = (
        read_data(&rig.p.data()).await,
        read_data(&rig.r.data()).await,
    );
    assert_converged(&primary, &replica);
    assert_none(
        "phantoms on the primary",
        phantom_violations(&primary, &rig.registry),
    );
    assert_none(
        "phantoms on the replica",
        phantom_violations(&replica, &rig.registry),
    );
}

/// Each scenario as `<scenario>::sync` and `<scenario>::async_write`.
macro_rules! both_modes {
    ($($scenario:ident),* $(,)?) => {$(
        mod $scenario {
            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn sync() {
                super::$scenario(super::WriteMode::Sync).await
            }

            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn async_write() {
                super::$scenario(super::WriteMode::ASYNC).await
            }
        }
    )*};
}

both_modes!(
    link_faults,
    primary_restarts,
    planned_failover,
    unplanned_failover,
    power_loss_on_the_primary,
);
