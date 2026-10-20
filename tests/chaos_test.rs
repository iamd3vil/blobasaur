//! Jepsen-style fault injection for replication, against real processes.
//!
//! Each scenario spawns the `blobasaur` binary as a primary and a replica with
//! generated configs, runs concurrent clients that record every operation and
//! its outcome, injects faults (a TCP proxy on the replication link, `kill -9`,
//! SIGTERM restarts, a simulated power loss), and checks the invariants from
//! REPLICATION.md on the history and on the shard DB files afterwards.
//!
//! * `BLOBASAUR_CHAOS_SEED`: u64 seed of the fault schedule and the workload
//!   (random if unset, always printed; output shows on failure or `--nocapture`).
//! * `BLOBASAUR_CHAOS_SECS`: seconds of faulted workload per scenario (default 6).
//!
//! The primary's `advertise_addr` is the proxy, so the replica reaches it only
//! through the proxy. Gossip (UDP, advertised host + `cluster.port`) and clients
//! go straight to the primary. On failure the temp dir is kept and the log
//! paths and tails are printed.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::future::Future;
use std::hash::{BuildHasher, RandomState};
use std::io;
use std::net::{SocketAddr, TcpListener as StdListener, UdpSocket};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
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
        sleep(Duration::from_millis(100)).await;
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

struct Node {
    name: &'static str,
    dir: PathBuf,
    addr: SocketAddr,
    gossip_port: u16,
    /// What the node tells its peers; for the primary this is the proxy.
    advertise: SocketAddr,
    seeds: Vec<String>,
    replica_of: Option<&'static str>,
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
            "data_dir = \"{}\"\nnum_shards = {SHARDS}\nasync_write = false\n\
             addr = \"{}\"\nshutdown_timeout_secs = 20\ncleanup_interval_secs = 1\n\
             [cluster]\nenabled = true\nnode_id = \"{}\"\nseeds = [{}]\nport = {}\n\
             gossip_interval_ms = 100\nadvertise_addr = \"{}\"\n\
             [replication]\ntombstone_retention_secs = {RETENTION_SECS}\n",
            self.data().display(),
            self.addr,
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
    fn start(addr: SocketAddr, seed: u64, generation: u32, registry: &Registry) -> Self {
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

async fn client(
    id: usize,
    addr: SocketAddr,
    mut rng: Rng,
    generation: u32,
    stop: Arc<AtomicBool>,
    registry: Registry,
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
        let (effect, args): (Effect, Vec<&str>) = match (target.ns, roll) {
            (None, 0..65) => (Effect::Put(value.clone()), vec!["SET", name, &value]),
            (None, 65..90) => (Effect::Del, vec!["DEL", name]),
            (None, _) => (Effect::Touch, vec!["EXPIRE", name, "2592000"]),
            (Some(ns), 0..70) => (Effect::Put(value.clone()), vec!["HSET", ns, name, &value]),
            (Some(ns), _) => (Effect::Del, vec!["HDEL", ns, name]),
        };
        if let Effect::Put(v) = &effect {
            registry.lock().unwrap().insert(v.clone(), target.ident());
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
            ident: target.ident(),
            effect,
            outcome,
        });
        sleep(rng.ms(0, 3)).await;
    }
    recs
}

#[derive(Default, Debug)]
struct Sampled {
    reads: u64,
    present: u64,
    phantoms: Vec<String>,
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

/// What a key may hold at the end: the effect of its last acked op, or of an
/// indeterminate op after it (`None` = absent).
fn allowed(ops: &[&Rec]) -> BTreeSet<Option<String>> {
    let effect = |r: &Rec| match &r.effect {
        Effect::Put(v) => Some(Some(v.clone())),
        Effect::Del => Some(None),
        Effect::Touch => None,
    };
    let last_acked = ops
        .iter()
        .rposition(|r| r.outcome == Outcome::Ok && effect(r).is_some());
    let mut set = BTreeSet::new();
    let from = match last_acked {
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
        if r.outcome == Outcome::Indeterminate
            && let Some(e) = effect(r)
        {
            set.insert(e);
        }
    }
    set
}

/// Keys that violate the durability rule, and keys in the DB nobody wrote.
fn durability_violations(recs: &[Rec], data: &Data) -> Vec<String> {
    let mut by_key: BTreeMap<&str, Vec<&Rec>> = BTreeMap::new();
    for r in recs {
        by_key.entry(&r.ident).or_default().push(r);
    }
    let mut bad: Vec<String> = by_key
        .iter()
        .filter_map(|(ident, ops)| {
            let allowed = allowed(ops);
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
    name: &'static str,
    seed: u64,
    root: Option<TempDir>,
    p: Node,
    r: Node,
    proxy: Proxy,
    registry: Registry,
    started: Instant,
}

impl Rig {
    async fn new(name: &'static str, seed: u64) -> Self {
        eprintln!("[chaos] scenario {name}: BLOBASAUR_CHAOS_SEED={seed}");
        let root = TempDir::new().unwrap();
        let mut p = Node::new(root.path(), "p");
        let proxy = Proxy::start(p.addr).await;
        p.advertise = proxy.addr;
        let mut r = Node::new(root.path(), "r");
        r.replica_of = Some("p");
        r.seeds = vec![format!("127.0.0.1:{}", p.gossip_port)];
        Self {
            name,
            seed,
            root: Some(root),
            p,
            r,
            proxy,
            registry: Registry::default(),
            started: Instant::now(),
        }
    }

    /// Starts both nodes and waits for the replica's first full copy.
    async fn start(&mut self) {
        self.p.must_start().await;
        self.r.must_start().await;
        self.wait_replica_synced().await;
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

    /// Writes reach the replica: it is at the primary's watermark, same replid.
    async fn wait_caught_up(&self) {
        let target = watermark(self.p.addr).await.expect("primary WATERMARK");
        let addr = self.r.addr;
        wait_for(
            "the replica to reach the primary's watermark",
            90,
            || async { watermark(addr).await.is_ok_and(|m| reached(&m, &target)) },
        )
        .await;
    }

    fn workload(&self, generation: u32) -> Workload {
        Workload::start(self.p.addr, self.seed, generation, &self.registry)
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
                    self.p.kill9();
                    sleep(rng.ms(100, 800)).await;
                    self.p.must_start().await;
                }
                80..85 if primary_faults => {
                    self.p.stop().await;
                    self.p.must_start().await;
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
}

impl Drop for Rig {
    fn drop(&mut self) {
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

fn report(rig: &Rig, recs: &[Rec], seen: &Sampled) {
    eprintln!(
        "[chaos] {}: {}; replica reads {} ({} present); replica resyncs {}; {:?}",
        rig.name,
        outcomes(recs),
        seen.reads,
        seen.present,
        rig.resyncs(),
        rig.started.elapsed()
    );
}

// ---------------------------------------------------------------- scenarios

/// Link faults and node restarts under load. Then: durability on the primary,
/// convergence, and no phantoms on the replica.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replication_link_faults() {
    let mut rig = Rig::new("link_faults", seed()).await;
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
    report(&rig, &recs, &seen);
    assert_none("phantom reads on the replica", seen.phantoms);

    rig.r.stop().await;
    rig.p.stop().await;
    let (primary, replica) = (
        read_data(&rig.p.data()).await,
        read_data(&rig.r.data()).await,
    );
    assert_none(
        "durability on the primary",
        durability_violations(&recs, &primary),
    );
    assert_converged(&primary, &replica);
    assert_none(
        "phantoms on the replica",
        phantom_violations(&replica, &rig.registry),
    );
}

/// The CLUSTERING.md planned failover: no acknowledged write is lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn planned_failover() {
    let mut rig = Rig::new("planned_failover", seed()).await;
    let mut rng = Rng::new(rig.seed);
    rig.start().await;
    let (stop_sampler, sampler) = rig.sampler();
    let workload = rig.workload(0);
    rig.nemesis(&mut rng, secs(), false).await;

    // Stop client writes, WATERMARK on the primary, wait for the replica.
    let recs = workload.finish().await;
    rig.wait_caught_up().await;
    let old = watermark(rig.p.addr).await.unwrap();
    stop_sampler.store(true, Ordering::Relaxed);
    let seen = sampler.await.unwrap();
    report(&rig, &recs, &seen);
    assert_none("phantom reads on the replica", seen.phantoms);

    // Fence the primary, promote the replica.
    rig.p.kill9();
    rig.r.stop().await;
    rig.promote().await;
    let new = watermark(rig.r.addr).await.unwrap();
    assert!(
        new.iter().zip(&old).all(|((n, _), (o, _))| n != o),
        "promotion kept the replid: {old:?} -> {new:?}"
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
        durability_violations(&recs, &promoted),
    );
}

/// The primary dies mid-workload and the replica is promoted: acked writes
/// may be lost (reported, not asserted), but nothing may be invented.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unplanned_failover() {
    let mut rig = Rig::new("unplanned_failover", seed()).await;
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
    let old = watermark(rig.p.addr).await.unwrap();
    rig.p.kill9();
    let recs = workload.finish().await;
    stop_sampler.store(true, Ordering::Relaxed);
    let seen = sampler.await.unwrap();
    report(&rig, &recs, &seen);
    assert_none("phantom reads on the replica", seen.phantoms);

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
    let lost = durability_violations(&recs, &promoted);
    eprintln!(
        "[chaos] unplanned failover lost {} acked keys (info only)",
        lost.len()
    );
}

/// Power loss on the primary: its shards go back to an earlier committed
/// prefix with `clean_shutdown = 0` and a different boot id, while the replica
/// already applied writes the primary no longer has.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn power_loss_on_the_primary() {
    let mut rig = Rig::new("power_loss", seed()).await;
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
    rig.p.kill9();
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
    rig.p.must_start().await;

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
    report(&rig, &before, &seen);
    assert_none("phantom reads on the replica", seen.phantoms);

    rig.r.stop().await;
    rig.p.stop().await;
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
