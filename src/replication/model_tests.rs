//! Model-based property test of the replication core.
//!
//! A proptest-generated sequence of [`Action`]s interleaves random primary
//! activity (client writes through `process_batch`, expiry cleanup, tombstone
//! trimming, a replid change) with the replication stream (handshake, pages,
//! disconnects) and replica events (apply, crash, local cleanup). It drives the
//! real code paths against real temp-file SQLite DBs, one primary shard and one
//! replica shard, and checks the invariants listed below after every action.
//! There is no network, gossip or `server::run`: what the socket carries is an
//! in-flight queue of decoded frames, which the test fills with the real
//! `read_page` and drains into the real `apply`.
//!
//! # Why one thread interleaving page-sized steps is faithful
//!
//! Every step the real system takes is a single SQLite transaction: a
//! `read_page` is one read transaction (a consistent snapshot, which in WAL
//! mode never blocks or sees a half-committed writer), a writer batch, a
//! cleanup chunk, a trim, an Apply and a Reset are each one write transaction,
//! and a shard has one writer connection, so writes are serialized. Concurrent
//! executions of these transactions are therefore equivalent to some sequential
//! order of them, with each read seeing exactly the commits before it. The
//! network only adds delay, loss and the buffering between a page being read
//! and being applied, which are the in-flight queue and `Disconnect`. Choosing
//! the sequence at random covers every such order.
//!
//! # Invariants
//!
//! 1. When an Apply commits a watermark `W`, the replica's live rows equal the
//!    primary's live rows as they were when the page carrying `W` was read, and
//!    `caught_up_lsn == W` and `synced == 1`.
//! 2. The primary's LSN (`sqlite_sequence`) only grows, and every committed write
//!    batch raises it. A replica's `applied_lsn` never exceeds the primary's LSN
//!    while their replids match.
//! 3. After a Reset, `caught_up_lsn` is absent and `synced = 0` until the next
//!    watermark commits.
//! 4. Convergence: after the sequence, a handshake and then pages and applies
//!    until a watermark is applied leave the replica with the primary's live rows
//!    and replid.
//!
//! # Clocks
//!
//! "Live" is `expires_at IS NULL OR expires_at > REPLICA_NOW`, a fixed instant
//! far from every generated expiry. TTLs are `Past` (expired for everybody),
//! `Soon` (live on the replica's clock, expired on the primary's) and `Far`.
//! The primary cleans up at `PRIMARY_NOW` and the replica at `REPLICA_NOW`: the
//! clocks of two nodes differ, and only a cleanup tombstone removes a `Soon` row
//! the replica already holds. (A single shared clock would hide a missing
//! tombstone: the replica's own cleanup would remove the row anyway.)

use super::*;
use crate::app_state::{CREATE_BLOBS_TABLE, CREATE_REPL_LOG, CREATE_REPL_STATE};
use crate::shard_manager::{Pending, delete_expired, load_existing_tables, process_batch};
use metrics::Counter;
use moka::future::Cache;
use proptest::prelude::*;
use sqlx::sqlite::{SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use std::collections::{BTreeMap, VecDeque};
use std::rc::Rc;
use tempfile::TempDir;

const PAST: i64 = 1_000_000_000;
const REPLICA_NOW: i64 = 2_000_000_000;
const SOON: i64 = 3_000_000_000;
const PRIMARY_NOW: i64 = 4_000_000_000;
const FAR: i64 = 5_000_000_000;
/// Rows per cleanup chunk, small so a cleanup spans several.
const CLEANUP_CHUNK: u32 = 2;
/// Seconds of retention a trim is asked for; aged tombstones are older.
const RETENTION: u64 = 3600;
const DEFAULT_CASES: u32 = 256;

#[derive(Clone, Copy, Debug)]
enum Ttl {
    Past,
    Soon,
    Far,
}

impl Ttl {
    fn at(self) -> i64 {
        match self {
            Ttl::Past => PAST,
            Ttl::Soon => SOON,
            Ttl::Far => FAR,
        }
    }
}

/// One client op. Keys are `k0..k5`, namespaces `n0..n1`, fields `f0..f3`.
#[derive(Clone, Debug)]
enum Write {
    Set(u8, Option<Ttl>),
    Del(u8),
    HSet(u8, u8),
    HDel(u8, u8),
    Expire(u8, Ttl),
    /// `Past` expires the field at once, which deletes it.
    HExpire(u8, u8, Ttl),
}

#[derive(Clone, Debug)]
enum Action {
    /// One writer batch.
    Write(Vec<Write>),
    /// The primary's expiry cleanup over every blob table.
    Cleanup,
    /// The primary's tombstone trim; `age` first makes every tombstone old.
    Trim {
        age: bool,
    },
    /// The primary restarts with a new replid, which also ends its streams.
    NewReplid,
    Connect,
    Page,
    /// Applies up to `max` of the oldest in-flight frames, stopping after a watermark.
    Apply {
        max: usize,
    },
    Disconnect,
    ReplicaCrash,
    ReplicaCleanup,
}

fn ttl() -> impl Strategy<Value = Ttl> {
    prop_oneof![Just(Ttl::Past), Just(Ttl::Soon), Just(Ttl::Far)]
}

fn write() -> impl Strategy<Value = Write> {
    let (key, ns, field) = (0..6u8, 0..2u8, 0..4u8);
    prop_oneof![
        4 => (key.clone(), prop::option::weighted(0.4, ttl())).prop_map(|(k, t)| Write::Set(k, t)),
        2 => key.clone().prop_map(Write::Del),
        3 => (ns.clone(), field.clone()).prop_map(|(n, f)| Write::HSet(n, f)),
        2 => (ns.clone(), field.clone()).prop_map(|(n, f)| Write::HDel(n, f)),
        2 => (key, ttl()).prop_map(|(k, t)| Write::Expire(k, t)),
        2 => (ns, field, ttl()).prop_map(|(n, f, t)| Write::HExpire(n, f, t)),
    ]
}

fn action() -> impl Strategy<Value = Action> {
    prop_oneof![
        8 => prop::collection::vec(write(), 1..=3).prop_map(Action::Write),
        2 => Just(Action::Cleanup),
        2 => any::<bool>().prop_map(|age| Action::Trim { age }),
        1 => Just(Action::NewReplid),
        3 => Just(Action::Connect),
        5 => Just(Action::Page),
        5 => (1..=4usize).prop_map(|max| Action::Apply { max }),
        1 => Just(Action::Disconnect),
        1 => Just(Action::ReplicaCrash),
        2 => Just(Action::ReplicaCleanup),
    ]
}

/// `key, data, created_at, updated_at, expires_at, version` as stored.
type StoredRow = (String, Option<Vec<u8>>, i64, i64, Option<i64>, i64);

/// `(table, key) -> (data, created_at, updated_at, expires_at, version)`.
type Live = BTreeMap<(String, String), (String, i64, i64, Option<i64>, i64)>;

macro_rules! ensure {
    ($cond:expr, $($arg:tt)+) => {
        if !$cond {
            return Err(format!($($arg)+));
        }
    };
}

/// The stream the primary is serving the replica, if connected.
struct Stream {
    pin: u64,
    cursor: u64,
}

struct Sim {
    _dir: TempDir,
    primary_opts: SqliteConnectOptions,
    replica_opts: SqliteConnectOptions,
    primary: SqlitePool,
    replica: SqlitePool,
    primary_known: HashSet<String>,
    replica_known: HashSet<String>,
    primary_commits: watch::Sender<u64>,
    replica_commits: watch::Sender<u64>,
    pins: ShardPins,
    stream: Option<Stream>,
    /// The network: decoded frames not applied yet. A watermark carries the
    /// primary's live rows as of the read that produced it.
    inflight: VecDeque<(Frame, Option<Rc<Live>>)>,
    /// The highest primary LSN seen so far.
    seq_seen: u64,
    /// A Reset happened and no watermark has committed since.
    reset_pending: bool,
    /// Makes every written value unique.
    writes: u64,
}

async fn open(dir: &TempDir, name: &str) -> (SqliteConnectOptions, SqlitePool) {
    let options = SqliteConnectOptions::new()
        .filename(dir.path().join(name))
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Off);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options.clone())
        .await
        .unwrap();
    for ddl in [CREATE_BLOBS_TABLE, CREATE_REPL_LOG, CREATE_REPL_STATE] {
        sqlx::query(ddl).execute(&pool).await.unwrap();
    }
    (options, pool)
}

async fn tables(pool: &SqlitePool) -> Vec<String> {
    let mut tables = vec!["blobs".to_string()];
    tables.extend(
        sqlx::query_scalar::<_, String>(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'blobs_%'",
        )
        .fetch_all(pool)
        .await
        .unwrap(),
    );
    tables
}

/// The rows of every blob table that are live at `REPLICA_NOW`.
async fn live(pool: &SqlitePool) -> Live {
    let mut rows = Live::new();
    for table in tables(pool).await {
        let found: Vec<StoredRow> = sqlx::query_as(&format!(
            "SELECT key, data, created_at, updated_at, expires_at, version FROM {table}
                 WHERE expires_at IS NULL OR expires_at > ?"
        ))
        .bind(REPLICA_NOW)
        .fetch_all(pool)
        .await
        .unwrap();
        for (key, data, created, updated, expires, version) in found {
            let data = String::from_utf8_lossy(&data.unwrap_or_default()).into_owned();
            rows.insert(
                (table.clone(), key),
                (data, created, updated, expires, version),
            );
        }
    }
    rows
}

async fn state(pool: &SqlitePool, k: &str) -> Option<String> {
    get_state(&mut pool.acquire().await.unwrap(), k)
        .await
        .unwrap()
}

async fn lsn(pool: &SqlitePool) -> u64 {
    watermark(&mut pool.acquire().await.unwrap()).await.unwrap() as u64
}

fn name(prefix: &str, n: u8) -> String {
    format!("{prefix}{n}")
}

impl Sim {
    async fn new() -> Self {
        let dir = TempDir::new().unwrap();
        let (primary_opts, primary) = open(&dir, "primary.db").await;
        let (replica_opts, replica) = open(&dir, "replica.db").await;
        init_shard(&primary_opts, true, false, false).await.unwrap();
        init_shard(&replica_opts, true, false, true).await.unwrap();
        Sim {
            _dir: dir,
            primary_opts,
            replica_opts,
            primary,
            replica,
            primary_known: HashSet::from(["blobs".to_string()]),
            replica_known: HashSet::from(["blobs".to_string()]),
            primary_commits: watch::channel(0).0,
            replica_commits: watch::channel(0).0,
            pins: ShardPins::default(),
            stream: None,
            inflight: VecDeque::new(),
            seq_seen: 0,
            reset_pending: false,
            writes: 0,
        }
    }

    async fn step(&mut self, action: &Action) -> Result<(), String> {
        match action {
            Action::Write(ops) => self.write(ops).await?,
            Action::Cleanup => {
                cleanup(&self.primary, PRIMARY_NOW, &self.primary_commits).await;
            }
            Action::Trim { age } => {
                if *age {
                    sqlx::query("UPDATE repl_log SET at = 0 WHERE deleted = 1")
                        .execute(&self.primary)
                        .await
                        .unwrap();
                }
                trim_tombstones(&self.primary, &self.pins, RETENTION)
                    .await
                    .unwrap();
            }
            Action::NewReplid => {
                // A restart: the replid changes and every stream ends.
                init_shard(&self.primary_opts, true, true, false)
                    .await
                    .unwrap();
                self.disconnect().await;
            }
            Action::Connect => self.connect().await,
            Action::Page => self.page().await?,
            Action::Apply { max } => {
                self.apply(*max).await?;
            }
            Action::Disconnect => self.disconnect().await,
            Action::ReplicaCrash => {
                // Committed Applies persist, what was in flight is lost, and the
                // writer reloads its table names on restart.
                self.disconnect().await;
                self.replica_known = load_existing_tables(&self.replica, 0).await;
                init_shard(&self.replica_opts, true, false, true)
                    .await
                    .unwrap();
            }
            Action::ReplicaCleanup => {
                cleanup(&self.replica, REPLICA_NOW, &self.replica_commits).await;
            }
        }
        self.check().await
    }

    async fn write(&mut self, writes: &[Write]) -> Result<(), String> {
        let mut batch = VecDeque::new();
        for write in writes {
            self.writes += 1;
            let data = Bytes::from(format!("v{}", self.writes));
            let (responder, _) = oneshot::channel();
            let (result, _) = oneshot::channel();
            let (ack, _) = oneshot::channel();
            batch.push_back(match write {
                Write::Set(k, ttl) => ShardWriteOperation::Set {
                    key: name("k", *k),
                    data,
                    expires_at: ttl.map(Ttl::at),
                    responder,
                },
                Write::Del(k) => ShardWriteOperation::Delete {
                    key: name("k", *k),
                    responder,
                },
                Write::HSet(n, f) => ShardWriteOperation::HSet {
                    namespace: name("n", *n),
                    key: name("f", *f),
                    data,
                    responder,
                },
                Write::HDel(n, f) => ShardWriteOperation::HDelete {
                    namespace: name("n", *n),
                    key: name("f", *f),
                    responder,
                },
                Write::Expire(k, ttl) => ShardWriteOperation::Expire {
                    key: name("k", *k),
                    expires_at: ttl.at(),
                    responder: result,
                },
                Write::HExpire(n, f, ttl) => ShardWriteOperation::HExpire {
                    namespace: name("n", *n),
                    key: name("f", *f),
                    expires_at: ttl.at(),
                    condition: None,
                    responder: ack,
                },
            });
        }
        let before = lsn(&self.primary).await;
        let committed = process_batch(
            0,
            &self.primary,
            &mut batch,
            &mut self.primary_known,
            &Cache::<String, Pending>::new(16),
            &Cache::<String, Pending>::new(16),
            &ShardMetrics::new(0),
            true,
        )
        .await;
        let after = lsn(&self.primary).await;
        if let Some(committed) = committed {
            ensure!(
                after > before && committed == after,
                "a committed batch must raise the LSN to its last entry: \
                 {before} -> {after}, reported {committed}"
            );
        }
        Ok(())
    }

    /// The handshake, as `serve_sync` decides it and the replica reacts to it.
    async fn connect(&mut self) {
        if self.stream.is_some() {
            return;
        }
        loop {
            let replid = state(&self.replica, "replid").await.unwrap_or_default();
            let applied = stored_lsn(&mut self.replica.acquire().await.unwrap(), "applied_lsn")
                .await
                .unwrap();
            // The check and the pin are one step as far as trimming is concerned.
            let mut pins = self.pins.lock().await;
            let (primary_replid, trimmed, top) = primary_state(&self.primary).await.unwrap();
            if can_continue(&primary_replid, trimmed, top, &replid, applied) {
                let pin = pins.add(applied);
                self.stream = Some(Stream {
                    pin,
                    cursor: applied,
                });
                return;
            }
            drop(pins);
            // RESYNC: reset to the primary's history and connect again from 0.
            reset(&self.replica, &primary_replid).await.unwrap();
            self.reset_pending = true;
        }
    }

    async fn disconnect(&mut self) {
        if let Some(stream) = self.stream.take() {
            self.pins.lock().await.cursors.remove(&stream.pin);
        }
        self.inflight.clear();
    }

    /// One page of the stream, as `stream_pages` reads it and moves its pin.
    async fn page(&mut self) -> Result<(), String> {
        let Some(stream) = &mut self.stream else {
            return Ok(());
        };
        let mut page = read_page(&self.primary, stream.cursor).await.unwrap();
        let mut frames = Vec::new();
        while let Some((frame, _, _)) = decode_bytes_mut(&mut page.bytes).unwrap() {
            frames.push(Frame::from_resp(frame)?);
        }
        if let Some(last) = page.last_lsn {
            stream.cursor = last;
            self.pins.lock().await.cursors.insert(stream.pin, last);
        }
        // Nothing wrote since the read, so this is the primary's state at the
        // page's watermark.
        let snapshot = if frames.iter().any(|f| matches!(f, Frame::Watermark { .. })) {
            Some(Rc::new(live(&self.primary).await))
        } else {
            None
        };
        for frame in frames {
            let snapshot = matches!(frame, Frame::Watermark { .. })
                .then(|| snapshot.clone())
                .flatten();
            self.inflight.push_back((frame, snapshot));
        }
        Ok(())
    }

    /// Applies the oldest frames, grouped like `replica_sync_task` does: up to
    /// a watermark, or `max` frames. Returns whether the group ended in a watermark.
    async fn apply(&mut self, max: usize) -> Result<bool, String> {
        let mut group = Vec::new();
        let mut snapshot = None;
        while let Some((frame, rows)) = self.inflight.pop_front() {
            let watermark = matches!(frame, Frame::Watermark { .. });
            group.push(frame);
            if watermark {
                snapshot = rows;
                break;
            }
            if group.len() >= max {
                break;
            }
        }
        if group.is_empty() {
            return Ok(false);
        }
        if let Err(e) = apply(&self.replica, &mut self.replica_known, &group).await {
            return Err(format!("apply failed: {e}"));
        }
        let Some(Frame::Watermark { watermark, .. }) = group.last() else {
            return Ok(false);
        };
        let expected = snapshot.expect("a watermark carries its snapshot");
        let got = live(&self.replica).await;
        ensure!(
            got == *expected,
            "replica differs from the primary at watermark {watermark}\n  primary: {expected:?}\n  replica: {got:?}"
        );
        let caught_up = state(&self.replica, "caught_up_lsn").await;
        ensure!(
            caught_up == Some(watermark.to_string()),
            "caught_up_lsn is {caught_up:?} after watermark {watermark}"
        );
        let synced = state(&self.replica, "synced").await;
        ensure!(
            synced.as_deref() == Some("1"),
            "synced is {synced:?} after watermark {watermark}"
        );
        self.reset_pending = false;
        Ok(true)
    }

    async fn check(&mut self) -> Result<(), String> {
        let top = lsn(&self.primary).await;
        ensure!(
            top >= self.seq_seen,
            "the primary's LSN went back from {} to {top}",
            self.seq_seen
        );
        self.seq_seen = top;

        let (primary_replid, _, _) = primary_state(&self.primary).await.unwrap();
        if state(&self.replica, "replid").await.as_deref() == Some(&primary_replid) {
            let applied = stored_lsn(&mut self.replica.acquire().await.unwrap(), "applied_lsn")
                .await
                .unwrap();
            ensure!(
                applied <= top,
                "applied_lsn {applied} is above the primary's LSN {top}"
            );
        }

        if self.reset_pending {
            let caught_up = state(&self.replica, "caught_up_lsn").await;
            let synced = state(&self.replica, "synced").await;
            ensure!(
                caught_up.is_none() && synced.as_deref() == Some("0"),
                "after a reset, before a watermark: caught_up_lsn {caught_up:?}, synced {synced:?}"
            );
        }
        Ok(())
    }

    /// Connects if needed, then pages and applies until a watermark commits.
    async fn converge(&mut self) -> Result<(), String> {
        self.connect().await;
        for _ in 0..1000 {
            self.page().await?;
            let mut caught_up = false;
            while !self.inflight.is_empty() {
                caught_up = self.apply(usize::MAX).await?;
                self.check().await?;
            }
            if caught_up {
                let (primary, replica) = (live(&self.primary).await, live(&self.replica).await);
                ensure!(
                    primary == replica,
                    "did not converge\n  primary: {primary:?}\n  replica: {replica:?}"
                );
                let (replid, _, _) = primary_state(&self.primary).await.unwrap();
                let stored = state(&self.replica, "replid").await;
                ensure!(
                    stored.as_deref() == Some(&replid),
                    "replica replid {stored:?} is not the primary's {replid}"
                );
                return Ok(());
            }
        }
        Err("never caught up to a watermark".to_string())
    }
}

/// The cleanup task's per-table loop, at the node's clock `now`.
async fn cleanup(pool: &SqlitePool, now: i64, commits: &watch::Sender<u64>) {
    for table in tables(pool).await {
        delete_expired(
            pool,
            &table,
            now,
            CLEANUP_CHUNK,
            &Counter::noop(),
            Some(commits),
            &CancellationToken::new(),
        )
        .await
        .unwrap();
    }
}

async fn run(actions: Vec<Action>) -> Result<(), String> {
    let mut sim = Sim::new().await;
    for (i, action) in actions.iter().enumerate() {
        sim.step(action)
            .await
            .map_err(|e| format!("after action {i} ({action:?}): {e}"))?;
    }
    sim.converge()
        .await
        .map_err(|e| format!("at final convergence: {e}"))?;
    sim.primary.close().await;
    sim.replica.close().await;
    Ok(())
}

fn cases() -> u32 {
    std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|n| n.parse().ok())
        .unwrap_or(DEFAULT_CASES)
}

proptest! {
    #![proptest_config(ProptestConfig { cases: cases(), ..ProptestConfig::default() })]

    #[test]
    fn replica_follows_primary(actions in prop::collection::vec(action(), 1..80)) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(run(actions)).map_err(TestCaseError::fail)?;
    }
}
