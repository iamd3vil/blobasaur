//! Replication: the change index (`repl_log`, `repl_state`) every shard keeps,
//! its startup initialization, the boot-id durability check and the commit
//! watermark, plus the stream a primary serves (`BLOBASAUR.SYNC`), what a
//! replica does with it (apply, reset, the sync task) and tombstone trimming.
//! See REPLICATION.md.

use crate::app_state::AppState;
use crate::metrics::ShardMetrics;
use crate::redis::serialize_frame;
use crate::shard_manager::{ShardWriteOperation, ensure_namespaced_table_exists, log_change};
use bytes::{Bytes, BytesMut};
use chrono::Utc;
use miette::{Result, miette};
use redis_protocol::resp2::{decode::decode_bytes_mut, encode::extend_encode, types::BytesFrame};
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{Connection, Sqlite, SqliteConnection, SqlitePool, Transaction};
use std::collections::{HashMap, HashSet};
use std::hash::{BuildHasher, RandomState};
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, oneshot, watch};
use tokio::time::{Instant, Interval, interval, sleep, timeout};
use tokio_util::sync::CancellationToken;

const BOOT_ID_PATH: &str = "/proc/sys/kernel/random/boot_id";

/// Raises the shard's published committed LSN to `lsn`; never lowers it.
pub fn publish(commit_watch: &watch::Sender<u64>, lsn: u64) {
    commit_watch.send_if_modified(|current| {
        let raise = lsn > *current;
        if raise {
            *current = lsn;
        }
        raise
    });
}

/// Whether the shard must start a new replication history (replid).
///
/// Commits can only be lost if the OS stopped without flushing, which
/// shows as a different boot id together with no clean shutdown marker. An
/// unreadable boot id counts as changed, so the marker alone decides.
fn needs_new_replid(
    forced: bool,
    replid: Option<&str>,
    stored_boot_id: Option<&str>,
    current_boot_id: Option<&str>,
    clean_shutdown: bool,
) -> bool {
    let same_boot = matches!((stored_boot_id, current_boot_id), (Some(a), Some(b)) if a == b);
    forced || replid.is_none() || (!clean_shutdown && !same_boot)
}

/// An opaque 40 hex digit id. std's `RandomState` is seeded by the OS, which
/// avoids a `rand` dependency for an id that only has to not collide.
fn new_replid() -> String {
    let id: String = (0..3)
        .map(|i| format!("{:016x}", RandomState::new().hash_one(i)))
        .collect();
    id[..40].to_string()
}

async fn get_state(conn: &mut SqliteConnection, k: &str) -> Result<Option<String>> {
    sqlx::query_scalar("SELECT v FROM repl_state WHERE k = ?")
        .bind(k)
        .fetch_optional(conn)
        .await
        .map_err(|e| miette!("failed to read repl_state {}: {}", k, e))
}

async fn set_state(conn: &mut SqliteConnection, k: &str, v: &str) -> Result<()> {
    sqlx::query("INSERT OR REPLACE INTO repl_state (k, v) VALUES (?, ?)")
        .bind(k)
        .bind(v)
        .execute(conn)
        .await
        .map(|_| ())
        .map_err(|e| miette!("failed to write repl_state {}: {}", k, e))
}

/// A short-lived connection with `synchronous = FULL`, so what it writes
/// survives power loss even though the shard's pools run NORMAL.
async fn connect_full(options: &SqliteConnectOptions) -> Result<SqliteConnection> {
    SqliteConnection::connect_with(&options.clone().pragma("synchronous", "FULL"))
        .await
        .map_err(|e| miette!("failed to open shard DB: {}", e))
}

/// Startup step for one shard, after its tables exist and before its writer
/// or cleanup task runs: indexes rows from before the change index (once),
/// settles the role and replid and records this boot. A replica never touches
/// its replid: it is the primary's identity there, stored by [`reset`]. Returns
/// the shard's highest committed LSN.
pub async fn init_shard(
    options: &SqliteConnectOptions,
    force_new_replid: bool,
    replica: bool,
) -> Result<u64> {
    let mut conn = connect_full(options).await?;
    let mut tx = conn
        .begin()
        .await
        .map_err(|e| miette!("failed to begin repl init: {}", e))?;

    if get_state(&mut tx, "indexed").await?.as_deref() != Some("1") {
        let mut tables = vec!["blobs".to_string()];
        tables.extend(
            sqlx::query_scalar::<_, String>(
                "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'blobs_%'",
            )
            .fetch_all(&mut *tx)
            .await
            .map_err(|e| miette!("failed to list tables for backfill: {}", e))?,
        );
        for table in tables {
            sqlx::query(&format!(
                "INSERT OR IGNORE INTO repl_log (tbl, key, deleted, at)
                 SELECT ?, key, 0, ? FROM {table}"
            ))
            .bind(&table)
            .bind(Utc::now().timestamp())
            .execute(&mut *tx)
            .await
            .map_err(|e| miette!("failed to backfill repl_log from {}: {}", table, e))?;
        }
        set_state(&mut tx, "indexed", "1").await?;
    }

    let stored_role = get_state(&mut tx, "role").await?;
    let promotion = !replica && stored_role.as_deref() == Some("replica");
    if promotion && get_state(&mut tx, "synced").await?.as_deref() != Some("1") {
        return Err(miette!(
            "cannot promote: the shard never finished syncing from its primary"
        ));
    }

    let replid = get_state(&mut tx, "replid").await?;
    let stored_boot_id = get_state(&mut tx, "boot_id").await?;
    let clean_shutdown = get_state(&mut tx, "clean_shutdown").await?.as_deref() == Some("1");
    let boot_id = std::fs::read_to_string(BOOT_ID_PATH)
        .ok()
        .map(|id| id.trim().to_string());
    if replica {
        // Data that isn't a copy of the configured primary doesn't count as synced.
        if stored_role.as_deref() != Some("replica") {
            set_state(&mut tx, "synced", "0").await?;
        }
    } else if needs_new_replid(
        force_new_replid || promotion,
        replid.as_deref(),
        stored_boot_id.as_deref(),
        boot_id.as_deref(),
        clean_shutdown,
    ) {
        if replid.is_some() {
            tracing::warn!(
                forced = force_new_replid,
                promotion,
                clean_shutdown,
                "Generating a new replid: replicas will resync"
            );
        }
        set_state(&mut tx, "replid", &new_replid()).await?;
    }
    set_state(&mut tx, "role", if replica { "replica" } else { "primary" }).await?;
    if let Some(boot_id) = &boot_id {
        set_state(&mut tx, "boot_id", boot_id).await?;
    }
    set_state(&mut tx, "clean_shutdown", "0").await?;

    let lsn: Option<i64> =
        sqlx::query_scalar("SELECT seq FROM sqlite_sequence WHERE name = 'repl_log'")
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| miette!("failed to read repl_log sequence: {}", e))?;
    tx.commit()
        .await
        .map_err(|e| miette!("failed to commit repl init: {}", e))?;
    Ok(lsn.unwrap_or(0) as u64)
}

/// Records that nothing can commit to the shard anymore. Only call it after
/// the writer and cleanup task are done and the shard's pools are closed.
pub async fn mark_clean_shutdown(options: &SqliteConnectOptions) -> Result<()> {
    let mut conn = connect_full(options).await?;
    set_state(&mut conn, "clean_shutdown", "1").await?;
    conn.close()
        .await
        .map_err(|e| miette!("failed to close shard DB: {}", e))
}

/// Refuses to start a primary on shards of a replica that never finished its
/// first full copy. Checked for every shard before any is changed, so a refusal
/// doesn't leave some shards promoted.
pub async fn ensure_promotable(options: &[SqliteConnectOptions]) -> Result<()> {
    for (shard, options) in options.iter().enumerate() {
        let mut conn = SqliteConnection::connect_with(options)
            .await
            .map_err(|e| miette!("failed to open shard {} DB: {}", shard, e))?;
        let replica = get_state(&mut conn, "role").await?.as_deref() == Some("replica");
        if replica && get_state(&mut conn, "synced").await?.as_deref() != Some("1") {
            return Err(miette!(
                "cannot promote: shard {} never finished syncing from its primary",
                shard
            ));
        }
    }
    Ok(())
}

/// Rows per stream page and frames per Apply.
const PAGE_ROWS: usize = 1000;
/// A page or Apply ends once it holds this many bytes.
const PAGE_BYTES: usize = 8 * 1024 * 1024;
const WRITE_TIMEOUT: Duration = Duration::from_secs(30);
/// How often an idle stream sends a watermark.
const HEARTBEAT: Duration = Duration::from_secs(10);
/// A replica that hears nothing for this long reconnects.
const SILENCE_LIMIT: Duration = Duration::from_secs(60);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const BACKOFF_MIN: Duration = Duration::from_millis(200);
const BACKOFF_MAX: Duration = Duration::from_secs(5);

/// One frame of the stream; see REPLICATION.md.
#[derive(Debug, Clone, PartialEq)]
pub enum Frame {
    Row {
        lsn: i64,
        tbl: String,
        key: String,
        created_at: i64,
        updated_at: i64,
        expires_at: Option<i64>,
        version: i64,
        data: Bytes,
    },
    Delete {
        lsn: i64,
        tbl: String,
        key: String,
    },
    /// Everything up to `watermark` was sent; `unix_ms` is the primary's clock.
    Watermark {
        watermark: i64,
        unix_ms: i64,
    },
}

impl Frame {
    fn to_resp(&self) -> BytesFrame {
        let text = |s: &String| BytesFrame::BulkString(Bytes::from(s.clone()));
        BytesFrame::Array(match self {
            Frame::Row {
                lsn,
                tbl,
                key,
                created_at,
                updated_at,
                expires_at,
                version,
                data,
            } => vec![
                BytesFrame::Integer(*lsn),
                text(tbl),
                text(key),
                BytesFrame::Integer(*created_at),
                BytesFrame::Integer(*updated_at),
                expires_at.map_or(BytesFrame::Null, BytesFrame::Integer),
                BytesFrame::Integer(*version),
                BytesFrame::BulkString(data.clone()),
            ],
            Frame::Delete { lsn, tbl, key } => {
                vec![BytesFrame::Integer(*lsn), text(tbl), text(key)]
            }
            Frame::Watermark { watermark, unix_ms } => {
                vec![
                    BytesFrame::Integer(*watermark),
                    BytesFrame::Integer(*unix_ms),
                ]
            }
        })
    }

    fn from_resp(frame: BytesFrame) -> Result<Self, String> {
        fn int(frame: &BytesFrame) -> Result<i64, String> {
            match frame {
                BytesFrame::Integer(i) => Ok(*i),
                other => Err(format!("expected an integer, got {other:?}")),
            }
        }
        fn text(frame: &BytesFrame) -> Result<String, String> {
            match frame {
                BytesFrame::BulkString(b) => String::from_utf8(b.to_vec())
                    .map_err(|_| "table or key is not valid UTF-8".to_string()),
                other => Err(format!("expected a string, got {other:?}")),
            }
        }
        let BytesFrame::Array(mut items) = frame else {
            return Err("expected an array frame".to_string());
        };
        match items.len() {
            2 => Ok(Frame::Watermark {
                watermark: int(&items[0])?,
                unix_ms: int(&items[1])?,
            }),
            3 => Ok(Frame::Delete {
                lsn: int(&items[0])?,
                tbl: text(&items[1])?,
                key: text(&items[2])?,
            }),
            8 => {
                let data = match items.pop() {
                    Some(BytesFrame::BulkString(data)) => data,
                    other => return Err(format!("expected a string, got {other:?}")),
                };
                Ok(Frame::Row {
                    lsn: int(&items[0])?,
                    tbl: text(&items[1])?,
                    key: text(&items[2])?,
                    created_at: int(&items[3])?,
                    updated_at: int(&items[4])?,
                    expires_at: match &items[5] {
                        BytesFrame::Null => None,
                        other => Some(int(other)?),
                    },
                    version: int(&items[6])?,
                    data,
                })
            }
            n => Err(format!("unexpected frame with {n} elements")),
        }
    }

    fn lsn(&self) -> Option<i64> {
        match self {
            Frame::Row { lsn, .. } | Frame::Delete { lsn, .. } => Some(*lsn),
            Frame::Watermark { .. } => None,
        }
    }

    /// Bytes of stored value it carries.
    fn data_len(&self) -> usize {
        match self {
            Frame::Row { data, .. } => data.len(),
            _ => 0,
        }
    }
}

/// The cursors of the streams a shard is serving. Handshakes and trimming take
/// this lock, so a tombstone above a stream's cursor can't be trimmed between
/// the handshake's check and the stream's pin.
#[derive(Default)]
pub struct Pins {
    next: u64,
    cursors: HashMap<u64, u64>,
}

pub type ShardPins = Mutex<Pins>;

impl Pins {
    fn add(&mut self, cursor: u64) -> u64 {
        self.next += 1;
        self.cursors.insert(self.next, cursor);
        self.next
    }

    fn min(&self) -> Option<u64> {
        self.cursors.values().copied().min()
    }
}

/// Deletes tombstones older than `retention_secs` that every active stream has
/// already passed, and records the highest one in `trimmed_through`: a replica
/// whose cursor is below that may have missed a delete and has to resync.
/// Returns how many it deleted.
pub async fn trim_tombstones(
    pool: &SqlitePool,
    pins: &ShardPins,
    retention_secs: u64,
) -> Result<u64, sqlx::Error> {
    let older_than = Utc::now()
        .timestamp()
        .saturating_sub(retention_secs.min(i64::MAX as u64) as i64);
    let pins = pins.lock().await;
    let below = pins.min().map_or(i64::MAX, |lsn| lsn as i64);
    let mut tx = pool.begin().await?;
    let top: Option<i64> = sqlx::query_scalar(
        "SELECT MAX(lsn) FROM repl_log WHERE deleted = 1 AND at < ? AND lsn < ?",
    )
    .bind(older_than)
    .bind(below)
    .fetch_one(&mut *tx)
    .await?;
    let Some(top) = top else {
        return Ok(0);
    };
    let trimmed = sqlx::query("DELETE FROM repl_log WHERE deleted = 1 AND at < ? AND lsn < ?")
        .bind(older_than)
        .bind(below)
        .execute(&mut *tx)
        .await?
        .rows_affected();
    // Tombstones can be trimmed out of order, so never lower it.
    let current = stored_lsn(&mut tx, "trimmed_through").await?;
    sqlx::query("INSERT OR REPLACE INTO repl_state (k, v) VALUES ('trimmed_through', ?)")
        .bind(top.max(current as i64).to_string())
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(trimmed)
}

/// A `repl_state` LSN, 0 if unset.
async fn stored_lsn(conn: &mut SqliteConnection, k: &str) -> Result<u64, sqlx::Error> {
    let stored: Option<String> = sqlx::query_scalar("SELECT v FROM repl_state WHERE k = ?")
        .bind(k)
        .fetch_optional(conn)
        .await?;
    Ok(stored.and_then(|v| v.parse().ok()).unwrap_or(0))
}

/// The shard's highest assigned LSN.
async fn watermark(conn: &mut SqliteConnection) -> Result<i64, sqlx::Error> {
    let seq: Option<i64> =
        sqlx::query_scalar("SELECT seq FROM sqlite_sequence WHERE name = 'repl_log'")
            .fetch_optional(conn)
            .await?;
    Ok(seq.unwrap_or(0))
}

/// `tbl` goes into SQL text, so accept only the names the writer itself creates.
fn check_table(tbl: &str) -> Result<(), String> {
    let valid = tbl == "blobs"
        || tbl.strip_prefix("blobs_").is_some_and(|ns| {
            !ns.is_empty() && ns.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        });
    valid
        .then_some(())
        .ok_or_else(|| format!("invalid table {tbl:?}"))
}

/// Replica: applies `frames` in one transaction, all or nothing. Returns the
/// replica's highest own LSN written, if any.
pub(crate) async fn apply(
    pool: &SqlitePool,
    known_tables: &mut HashSet<String>,
    frames: &[Frame],
) -> Result<Option<u64>, String> {
    let mut added = Vec::new();
    let result = apply_frames(pool, known_tables, &mut added, frames).await;
    if result.is_err() {
        for table in added {
            known_tables.remove(&table);
        }
    }
    result
}

async fn apply_frames(
    pool: &SqlitePool,
    known_tables: &mut HashSet<String>,
    added: &mut Vec<String>,
    frames: &[Frame],
) -> Result<Option<u64>, String> {
    let sql = |e: sqlx::Error| e.to_string();
    let state = |e: miette::Report| e.to_string();
    let mut tx = pool.begin().await.map_err(sql)?;
    let mut last_log = None;
    let mut applied = None;
    let mut caught_up = None;
    for frame in frames {
        match frame {
            Frame::Row {
                lsn,
                tbl,
                key,
                created_at,
                updated_at,
                expires_at,
                version,
                data,
            } => {
                check_table(tbl)?;
                ensure_namespaced_table_exists(&mut tx, tbl, known_tables, added).await?;
                sqlx::query(&format!(
                    "INSERT OR REPLACE INTO {tbl}
                     (key, data, created_at, updated_at, expires_at, version)
                     VALUES (?, ?, ?, ?, ?, ?)"
                ))
                .bind(key)
                .bind(&data[..])
                .bind(created_at)
                .bind(updated_at)
                .bind(expires_at)
                .bind(version)
                .execute(&mut *tx)
                .await
                .map_err(sql)?;
                last_log = Some(log_change(&mut tx, tbl, key, false).await.map_err(sql)?);
                applied = Some(*lsn);
            }
            Frame::Delete { lsn, tbl, key } => {
                check_table(tbl)?;
                ensure_namespaced_table_exists(&mut tx, tbl, known_tables, added).await?;
                sqlx::query(&format!("DELETE FROM {tbl} WHERE key = ?"))
                    .bind(key)
                    .execute(&mut *tx)
                    .await
                    .map_err(sql)?;
                last_log = Some(log_change(&mut tx, tbl, key, true).await.map_err(sql)?);
                applied = Some(*lsn);
            }
            Frame::Watermark { watermark, unix_ms } => caught_up = Some((*watermark, *unix_ms)),
        }
    }
    if let Some(lsn) = applied {
        set_state(&mut tx, "applied_lsn", &lsn.to_string())
            .await
            .map_err(state)?;
    }
    if let Some((watermark, unix_ms)) = caught_up {
        for (k, v) in [
            ("caught_up_lsn", watermark.to_string()),
            ("watermark_ms", unix_ms.to_string()),
            ("synced", "1".to_string()),
        ] {
            set_state(&mut tx, k, &v).await.map_err(state)?;
        }
    }
    tx.commit().await.map_err(sql)?;
    Ok(last_log.map(|lsn| lsn as u64))
}

/// Replica: empties the shard and its change index and adopts `replid`, the
/// replication history of the primary it is about to copy.
pub(crate) async fn reset(pool: &SqlitePool, replid: &str) -> Result<(), String> {
    let sql = |e: sqlx::Error| e.to_string();
    let state = |e: miette::Report| e.to_string();
    let mut tx = pool.begin().await.map_err(sql)?;
    let mut tables = vec!["blobs".to_string()];
    tables.extend(
        sqlx::query_scalar::<_, String>(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'blobs_%'",
        )
        .fetch_all(&mut *tx)
        .await
        .map_err(sql)?,
    );
    for table in tables {
        sqlx::query(&format!("DELETE FROM {table}"))
            .execute(&mut *tx)
            .await
            .map_err(sql)?;
    }
    sqlx::query("DELETE FROM repl_log")
        .execute(&mut *tx)
        .await
        .map_err(sql)?;
    sqlx::query("DELETE FROM repl_state WHERE k IN ('caught_up_lsn', 'watermark_ms')")
        .execute(&mut *tx)
        .await
        .map_err(sql)?;
    for (k, v) in [("applied_lsn", "0"), ("replid", replid), ("synced", "0")] {
        set_state(&mut tx, k, v).await.map_err(state)?;
    }
    tx.commit().await.map_err(sql)
}

/// What a replica asks for in `BLOBASAUR.SYNC`.
pub struct SyncRequest {
    pub shard: usize,
    pub replid: String,
    pub lsn: u64,
    pub num_shards: usize,
    pub compression: String,
}

async fn write_all(stream: &mut TcpStream, bytes: &[u8]) -> io::Result<()> {
    timeout(WRITE_TIMEOUT, stream.write_all(bytes))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "replication write timed out"))?
}

async fn reply(stream: &mut TcpStream, frame: BytesFrame) -> io::Result<()> {
    write_all(stream, &serialize_frame(&frame)).await
}

/// The shard's replid, `trimmed_through` and watermark, as of one read.
pub(crate) async fn primary_state(pool: &SqlitePool) -> Result<(String, u64, u64), sqlx::Error> {
    let mut tx = pool.begin().await?;
    let replid: Option<String> = sqlx::query_scalar("SELECT v FROM repl_state WHERE k = 'replid'")
        .fetch_optional(&mut *tx)
        .await?;
    let trimmed = stored_lsn(&mut tx, "trimmed_through").await?;
    let watermark = watermark(&mut tx).await?;
    Ok((replid.unwrap_or_default(), trimmed, watermark as u64))
}

/// One shard's `(replid, lsn)` for `BLOBASAUR.WATERMARK`. A primary first
/// waits for its writer to commit everything queued, so every acknowledged
/// write has an LSN, and reports the highest one. A replica reports its
/// `caught_up_lsn`, or `not-synced` for a shard that holds no complete copy.
pub async fn shard_watermark(state: &AppState, shard: usize) -> Result<(String, String)> {
    let replica = state.cfg.replica_of().is_some();
    if !replica {
        let (responder, done) = oneshot::channel();
        state.shard_senders[shard]
            .send(ShardWriteOperation::Barrier { responder })
            .await
            .map_err(|_| miette!("shard {} writer is gone", shard))?;
        done.await
            .map_err(|_| miette!("shard {} writer is gone", shard))?;
    }
    let mut tx = state.db_pools[shard]
        .begin()
        .await
        .map_err(|e| miette!("shard {} read failed: {}", shard, e))?;
    let replid = get_state(&mut tx, "replid").await?.unwrap_or_default();
    let lsn = if !replica {
        watermark(&mut tx)
            .await
            .map_err(|e| miette!("shard {} read failed: {}", shard, e))?
            .to_string()
    } else {
        let synced = get_state(&mut tx, "synced").await?.as_deref() == Some("1");
        match get_state(&mut tx, "caught_up_lsn").await? {
            Some(lsn) if synced => lsn,
            _ => "not-synced".to_string(),
        }
    };
    Ok((replid, lsn))
}

/// Serves one replica's `BLOBASAUR.SYNC`. Takes over the connection: after the
/// handshake reply it only streams, until the replica hangs up, a write fails
/// or times out, or `shutdown` is cancelled.
pub async fn serve_sync(
    mut stream: TcpStream,
    state: &AppState,
    shutdown: &CancellationToken,
    req: SyncRequest,
) -> io::Result<()> {
    let cfg = &state.cfg;
    let error = if cfg.replica_of().is_some() {
        Some("this node is a replica".to_string())
    } else if req.num_shards != cfg.num_shards {
        Some(format!(
            "num_shards mismatch: primary has {}, replica {}",
            cfg.num_shards, req.num_shards
        ))
    } else if req.compression != cfg.compression_id() {
        Some(format!(
            "compression mismatch: primary has {}, replica {}",
            cfg.compression_id(),
            req.compression
        ))
    } else if req.shard >= cfg.num_shards {
        Some(format!("no such shard {}", req.shard))
    } else {
        None
    };
    if let Some(error) = error {
        return reply(
            &mut stream,
            BytesFrame::Error(format!("ERR {error}").into()),
        )
        .await;
    }

    let shard = req.shard;
    // The check and the pin are one step as far as trimming is concerned.
    let mut pins = state.repl_pins[shard].lock().await;
    let (replid, trimmed_through, watermark) = match primary_state(&state.db_pools[shard]).await {
        Ok(read) => read,
        Err(e) => {
            drop(pins);
            let error = BytesFrame::Error(format!("ERR failed to read shard state: {e}").into());
            return reply(&mut stream, error).await;
        }
    };
    let can_continue = replid == req.replid
        && (req.lsn == 0 || (trimmed_through <= req.lsn && req.lsn <= watermark));
    let pin = can_continue.then(|| pins.add(req.lsn));
    drop(pins);

    let verb = if can_continue { "CONTINUE" } else { "RESYNC" };
    let sent = reply(
        &mut stream,
        BytesFrame::SimpleString(format!("{verb} {replid} {watermark}").into()),
    )
    .await;
    let Some(pin) = pin else {
        return sent;
    };

    let metrics = ShardMetrics::new(shard);
    metrics.record_replication_stream(true);
    let result = match sent {
        Ok(()) => tokio::select! {
            _ = shutdown.cancelled() => Ok(()),
            result = stream_pages(&mut stream, state, shard, pin, req.lsn) => result,
        },
        Err(e) => Err(e),
    };
    state.repl_pins[shard].lock().await.cursors.remove(&pin);
    metrics.record_replication_stream(false);
    result
}

/// Sends everything after `cursor`, then waits for more.
async fn stream_pages(
    stream: &mut TcpStream,
    state: &AppState,
    shard: usize,
    pin: u64,
    mut cursor: u64,
) -> io::Result<()> {
    // Subscribed before the first scan, so a commit during it isn't missed.
    let mut commits = state.commit_watch[shard].subscribe();
    loop {
        loop {
            let page = read_page(&state.db_pools[shard], cursor)
                .await
                .map_err(io::Error::other)?;
            write_all(stream, &page.bytes).await?;
            if let Some(last) = page.last_lsn {
                cursor = last;
                state.repl_pins[shard]
                    .lock()
                    .await
                    .cursors
                    .insert(pin, cursor);
            }
            if page.caught_up {
                break;
            }
        }
        // A replica sends nothing after the handshake, so a read means it hung up.
        let mut hangup = [0u8; 1];
        tokio::select! {
            _ = commits.changed() => {}
            _ = sleep(HEARTBEAT) => {}
            _ = stream.read(&mut hangup) => return Ok(()),
        }
    }
}

struct Page {
    /// Encoded frames: the entries after the cursor with their current rows,
    /// followed by a watermark if they reach the head.
    bytes: BytesMut,
    last_lsn: Option<u64>,
    /// Whether the page holds every entry after the cursor, so it ends with a
    /// watermark.
    caught_up: bool,
}

/// One page of the stream, read in one read transaction.
async fn read_page(pool: &SqlitePool, cursor: u64) -> Result<Page, sqlx::Error> {
    let mut tx = pool.begin().await?;
    let entries: Vec<(i64, String, String, bool)> = sqlx::query_as(
        "SELECT lsn, tbl, key, deleted FROM repl_log WHERE lsn > ? ORDER BY lsn LIMIT ?",
    )
    .bind(cursor as i64)
    .bind(PAGE_ROWS as i64)
    .fetch_all(&mut *tx)
    .await?;
    let mut bytes = BytesMut::new();
    let mut encode = |frame: Frame| {
        extend_encode(&mut bytes, &frame.to_resp(), false).expect("Failed to encode frame");
        bytes.len()
    };
    // Fewer rows than the limit, none cut off by the byte cap: this read saw
    // every entry after the cursor. A watermark only after an empty page
    // would never come while writes keep arriving between pages.
    let mut caught_up = entries.len() < PAGE_ROWS;
    let mut last_lsn = None;
    for (lsn, tbl, key, deleted) in entries {
        let row = if deleted {
            None
        } else {
            current_row(&mut tx, &tbl, &key).await?
        };
        // An entry whose row is gone is sent as a delete, which is what the
        // replica needs to end up with.
        let len = encode(match row {
            Some((created_at, updated_at, expires_at, version, data)) => Frame::Row {
                lsn,
                tbl,
                key,
                created_at,
                updated_at,
                expires_at,
                version,
                data: data.unwrap_or_default().into(),
            },
            None => Frame::Delete { lsn, tbl, key },
        });
        last_lsn = Some(lsn as u64);
        if len >= PAGE_BYTES {
            caught_up = false;
            break;
        }
    }
    if caught_up {
        // Read in the same transaction as the entries, so every entry up to
        // it is in this page or an earlier one.
        encode(Frame::Watermark {
            watermark: watermark(&mut tx).await?,
            unix_ms: Utc::now().timestamp_millis(),
        });
    }
    Ok(Page {
        bytes,
        last_lsn,
        caught_up,
    })
}

type RowImage = (i64, i64, Option<i64>, i64, Option<Vec<u8>>);

async fn current_row(
    tx: &mut Transaction<'_, Sqlite>,
    tbl: &str,
    key: &str,
) -> Result<Option<RowImage>, sqlx::Error> {
    let row = sqlx::query_as(&format!(
        "SELECT created_at, updated_at, expires_at, version, data FROM {tbl} WHERE key = ?"
    ))
    .bind(key)
    .fetch_optional(&mut **tx)
    .await;
    match row {
        Err(e) if crate::server::is_missing_table(&e) => Ok(None),
        row => row,
    }
}

/// Runs the replica side of one shard until `shutdown` is cancelled: keeps a
/// stream open to the primary's current address and applies it through the
/// shard's writer.
pub async fn replica_sync_task(state: Arc<AppState>, shard: usize, shutdown: CancellationToken) {
    let Some(primary) = state.cfg.replica_of().map(str::to_owned) else {
        return;
    };
    let mut replica = Replica {
        metrics: ShardMetrics::new(shard),
        tick: interval(Duration::from_secs(1)),
        last_watermark_ms: None,
        backoff: BACKOFF_MIN,
        state,
        shard,
        primary,
    };
    replica
        .tick
        .set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    tokio::select! {
        _ = shutdown.cancelled() => {}
        _ = replica.run() => {}
    }
}

struct Replica {
    state: Arc<AppState>,
    shard: usize,
    primary: String,
    metrics: ShardMetrics,
    /// Drives the staleness gauge and the address and silence checks.
    tick: Interval,
    /// The primary's clock at the last applied watermark. Unset until the first
    /// session reads it back (or starts counting from now), and restarted by a reset.
    last_watermark_ms: Option<i64>,
    backoff: Duration,
}

impl Replica {
    async fn run(&mut self) {
        loop {
            match self.session().await {
                Ok(()) => self.pause(BACKOFF_MIN).await,
                Err(e) => {
                    tracing::warn!(
                        "[Shard {}] replication from {} failed: {}",
                        self.shard,
                        self.primary,
                        e
                    );
                    self.pause(self.backoff).await;
                    self.backoff = (self.backoff * 2).min(BACKOFF_MAX);
                }
            }
        }
    }

    /// Waits `duration`, keeping the staleness gauge rising meanwhile.
    async fn pause(&mut self, duration: Duration) {
        let end = Instant::now() + duration;
        loop {
            tokio::select! {
                _ = tokio::time::sleep_until(end) => return,
                _ = self.tick.tick() => self.publish_staleness(),
            }
        }
    }

    fn publish_staleness(&self) {
        if let Some(ms) = self.last_watermark_ms {
            let seconds = (Utc::now().timestamp_millis() - ms).max(0) as f64 / 1000.0;
            self.metrics.set_replication_staleness(seconds);
        }
    }

    async fn primary_addr(&self) -> Option<SocketAddr> {
        self.state
            .cluster_manager
            .as_ref()?
            .node_addr(&self.primary)
            .await
    }

    /// The shard's stored replid and applied LSN, which the handshake sends.
    async fn stored(&mut self) -> Result<(String, u64), String> {
        let pool = &self.state.db_pools[self.shard];
        let mut conn = pool.acquire().await.map_err(|e| e.to_string())?;
        let replid = get_state(&mut conn, "replid")
            .await
            .map_err(|e| e.to_string())?;
        let applied = stored_lsn(&mut conn, "applied_lsn")
            .await
            .map_err(|e| e.to_string())?;
        let caught_up = stored_lsn(&mut conn, "caught_up_lsn")
            .await
            .map_err(|e| e.to_string())?;
        let watermark_ms = stored_lsn(&mut conn, "watermark_ms")
            .await
            .map_err(|e| e.to_string())?;
        self.metrics
            .set_replica_lsns(Some(applied), Some(caught_up));
        if self.last_watermark_ms.is_none() {
            let stored = (watermark_ms > 0).then_some(watermark_ms as i64);
            self.last_watermark_ms = Some(stored.unwrap_or_else(|| Utc::now().timestamp_millis()));
        }
        Ok((replid.unwrap_or_default(), applied))
    }

    /// One connection to the primary: handshake, then either a reset (`RESYNC`)
    /// or applying the stream. Returns `Ok` when it should reconnect at once.
    async fn session(&mut self) -> Result<(), String> {
        let (replid, applied) = self.stored().await?;
        let addr = self
            .primary_addr()
            .await
            .ok_or_else(|| format!("{} is not in the cluster gossip", self.primary))?;
        let mut stream = timeout(CONNECT_TIMEOUT, TcpStream::connect(addr))
            .await
            .map_err(|_| format!("connecting to {addr} timed out"))?
            .map_err(|e| format!("connecting to {addr}: {e}"))?;

        let cfg = &self.state.cfg;
        let bulk = |s: String| BytesFrame::BulkString(Bytes::from(s));
        let handshake = BytesFrame::Array(vec![
            bulk("BLOBASAUR.SYNC".to_string()),
            bulk(self.shard.to_string()),
            bulk(replid),
            bulk(applied.to_string()),
            bulk(cfg.num_shards.to_string()),
            bulk(cfg.compression_id()),
        ]);
        write_all(&mut stream, &serialize_frame(&handshake))
            .await
            .map_err(|e| e.to_string())?;

        let mut buf = BytesMut::new();
        let reply = timeout(CONNECT_TIMEOUT, async {
            loop {
                if let Some((frame, _, _)) =
                    decode_bytes_mut(&mut buf).map_err(|e| format!("bad reply: {e:?}"))?
                {
                    return Ok(frame);
                }
                if stream.read_buf(&mut buf).await.map_err(|e| e.to_string())? == 0 {
                    return Err("primary closed the connection".to_string());
                }
            }
        })
        .await
        .map_err(|_| "handshake timed out".to_string())??;

        let reply = match reply {
            BytesFrame::SimpleString(s) => String::from_utf8_lossy(&s).into_owned(),
            BytesFrame::Error(e) => return Err(format!("primary refused: {e}")),
            other => return Err(format!("unexpected handshake reply {other:?}")),
        };
        let mut words = reply.split_whitespace();
        match (words.next(), words.next()) {
            (Some("CONTINUE"), _) => {
                self.backoff = BACKOFF_MIN;
                self.stream(addr, stream, buf).await
            }
            (Some("RESYNC"), Some(replid)) => self.reset(replid).await,
            _ => Err(format!("unexpected handshake reply {reply:?}")),
        }
    }

    async fn stream(
        &mut self,
        addr: SocketAddr,
        mut stream: TcpStream,
        mut buf: BytesMut,
    ) -> Result<(), String> {
        let mut last_read = Instant::now();
        let mut frames = Vec::new();
        let mut bytes = 0;
        loop {
            // Apply what has arrived before reading more: at a watermark, when
            // enough has piled up, or when nothing is buffered anymore.
            while let Some((frame, _, _)) =
                decode_bytes_mut(&mut buf).map_err(|e| format!("bad frame: {e:?}"))?
            {
                let frame = Frame::from_resp(frame)?;
                bytes += frame.data_len();
                let watermark = matches!(frame, Frame::Watermark { .. });
                frames.push(frame);
                if watermark || frames.len() >= PAGE_ROWS || bytes >= PAGE_BYTES {
                    self.apply(std::mem::take(&mut frames)).await?;
                    bytes = 0;
                }
            }
            if !frames.is_empty() {
                self.apply(std::mem::take(&mut frames)).await?;
                bytes = 0;
            }

            tokio::select! {
                read = stream.read_buf(&mut buf) => {
                    if read.map_err(|e| e.to_string())? == 0 {
                        return Err("primary closed the stream".to_string());
                    }
                    last_read = Instant::now();
                }
                _ = self.tick.tick() => {
                    self.publish_staleness();
                    if last_read.elapsed() > SILENCE_LIMIT {
                        return Err("primary went silent".to_string());
                    }
                    if self.primary_addr().await.is_some_and(|a| a != addr) {
                        tracing::info!(
                            "[Shard {}] {} moved from {}, reconnecting",
                            self.shard, self.primary, addr
                        );
                        return Ok(());
                    }
                }
            }
        }
    }

    /// Tells the cluster whether the shard holds a full copy, for the gossiped role.
    async fn set_synced(&self, synced: bool) {
        if let Some(cluster) = &self.state.cluster_manager {
            cluster.set_shard_synced(self.shard, synced).await;
        }
    }

    /// Hands `frames` to the shard's writer and waits for the commit.
    async fn apply(&mut self, frames: Vec<Frame>) -> Result<(), String> {
        let rows = frames.iter().filter(|f| f.lsn().is_some()).count() as u64;
        let bytes = frames.iter().map(Frame::data_len).sum::<usize>() as u64;
        let applied = frames.iter().rev().find_map(Frame::lsn);
        let caught_up = frames.iter().find_map(|f| match f {
            Frame::Watermark { watermark, unix_ms } => Some((*watermark, *unix_ms)),
            _ => None,
        });
        let (responder, committed) = oneshot::channel();
        self.writer(ShardWriteOperation::Apply { frames, responder }, committed)
            .await?;
        self.metrics.record_replica_applied(
            rows,
            bytes,
            applied.map(|lsn| lsn as u64),
            caught_up.map(|(watermark, _)| watermark as u64),
        );
        if let Some((_, unix_ms)) = caught_up {
            self.last_watermark_ms = Some(unix_ms);
            self.publish_staleness();
            self.set_synced(true).await;
        }
        Ok(())
    }

    async fn reset(&mut self, replid: &str) -> Result<(), String> {
        tracing::warn!(
            "[Shard {}] resyncing from {}: discarding the local copy",
            self.shard,
            self.primary
        );
        // Reads admitted while synced finish first; later ones are redirected.
        let state = self.state.clone();
        let _gate = state.replica_reset_gate(self.shard).await;
        self.set_synced(false).await;
        let (responder, committed) = oneshot::channel();
        let op = ShardWriteOperation::Reset {
            replid: replid.to_string(),
            responder,
        };
        self.writer(op, committed).await?;
        self.state.set_replid(self.shard, replid);
        self.metrics.record_replica_resync();
        self.metrics.set_replica_lsns(Some(0), Some(0));
        self.last_watermark_ms = Some(Utc::now().timestamp_millis());
        Ok(())
    }

    /// Queues `op` and waits for the writer's answer, still ticking the
    /// staleness gauge meanwhile.
    async fn writer(
        &mut self,
        op: ShardWriteOperation,
        mut committed: oneshot::Receiver<Result<(), String>>,
    ) -> Result<(), String> {
        let stopped = || "shard writer stopped".to_string();
        self.state.shard_senders[self.shard]
            .send(op)
            .await
            .map_err(|_| stopped())?;
        loop {
            tokio::select! {
                result = &mut committed => return result.map_err(|_| stopped())?,
                _ = self.tick.tick() => self.publish_staleness(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app_state::{CREATE_BLOBS_TABLE, CREATE_REPL_LOG, CREATE_REPL_STATE};
    use tempfile::TempDir;

    #[test]
    fn replid_changes_only_for_a_changed_boot_with_an_unclean_stop() {
        let (same, other) = (Some("boot-a"), Some("boot-b"));
        let id = Some("id");
        // (clean, stored boot, current boot) -> new replid
        let table = [
            (true, same, same, false),
            (true, same, other, false),
            (true, same, None, false),
            (true, None, same, false),
            (false, same, same, false), // process crash
            (false, same, other, true), // OS crash or power loss
            (false, same, None, true),
            (false, None, same, true),
            (false, None, None, true), // no boot id at all: the marker alone
        ];
        for (clean, stored, current, expected) in table {
            assert_eq!(
                needs_new_replid(false, id, stored, current, clean),
                expected,
                "clean={clean} stored={stored:?} current={current:?}"
            );
        }
        // First start, and an explicit request, always make one.
        assert!(needs_new_replid(false, None, same, same, true));
        assert!(needs_new_replid(true, id, same, same, true));
    }

    #[test]
    fn new_replids_are_40_hex_digits_and_differ() {
        let (a, b) = (new_replid(), new_replid());
        assert_eq!(a.len(), 40);
        assert!(a.chars().all(|c| c.is_ascii_hexdigit()));
        assert_ne!(a, b);
    }

    #[test]
    fn publish_never_lowers_the_watermark() {
        let (tx, rx) = watch::channel(5);
        publish(&tx, 3);
        assert_eq!(*rx.borrow(), 5);
        publish(&tx, 9);
        assert_eq!(*rx.borrow(), 9);
    }

    async fn shard(dir: &TempDir) -> (SqliteConnectOptions, SqliteConnection) {
        let options = SqliteConnectOptions::new()
            .filename(dir.path().join("shard_0.db"))
            .create_if_missing(true);
        let mut conn = SqliteConnection::connect_with(&options).await.unwrap();
        for ddl in [CREATE_BLOBS_TABLE, CREATE_REPL_LOG, CREATE_REPL_STATE] {
            sqlx::query(ddl).execute(&mut conn).await.unwrap();
        }
        (options, conn)
    }

    async fn log(conn: &mut SqliteConnection) -> Vec<(String, String, bool)> {
        sqlx::query_as("SELECT tbl, key, deleted FROM repl_log ORDER BY lsn")
            .fetch_all(conn)
            .await
            .unwrap()
    }

    async fn state(conn: &mut SqliteConnection, k: &str) -> Option<String> {
        get_state(conn, k).await.unwrap()
    }

    #[tokio::test]
    async fn init_shard_applies_the_boot_id_check() {
        let dir = TempDir::new().unwrap();
        let (options, mut conn) = shard(&dir).await;

        // First start: a replid and this boot are stored, nothing is "unclean".
        init_shard(&options, false, false).await.unwrap();
        let replid = state(&mut conn, "replid").await.unwrap();
        assert_eq!(replid.len(), 40);
        assert_eq!(state(&mut conn, "clean_shutdown").await.unwrap(), "0");

        // Same boot, no clean shutdown (process crash): kept.
        init_shard(&options, false, false).await.unwrap();
        assert_eq!(state(&mut conn, "replid").await.unwrap(), replid);

        // Another boot after a clean shutdown: kept.
        mark_clean_shutdown(&options).await.unwrap();
        assert_eq!(state(&mut conn, "clean_shutdown").await.unwrap(), "1");
        set_state(&mut conn, "boot_id", "old-boot").await.unwrap();
        init_shard(&options, false, false).await.unwrap();
        assert_eq!(state(&mut conn, "replid").await.unwrap(), replid);
        assert_eq!(state(&mut conn, "clean_shutdown").await.unwrap(), "0");

        // Another boot, not shut down cleanly (power loss): new replid.
        set_state(&mut conn, "boot_id", "old-boot").await.unwrap();
        init_shard(&options, false, false).await.unwrap();
        let after_crash = state(&mut conn, "replid").await.unwrap();
        assert_ne!(after_crash, replid);
        assert_ne!(state(&mut conn, "boot_id").await.unwrap(), "old-boot");

        // `--new-replid`.
        init_shard(&options, true, false).await.unwrap();
        assert_ne!(state(&mut conn, "replid").await.unwrap(), after_crash);
    }

    #[tokio::test]
    async fn init_shard_backfills_existing_rows_once() {
        let dir = TempDir::new().unwrap();
        let (options, mut conn) = shard(&dir).await;
        let row = "INSERT INTO TABLE (key, data, created_at, updated_at) VALUES (?, x'00', 0, 0)";
        sqlx::query(&CREATE_BLOBS_TABLE.replace("blobs", "blobs_ns"))
            .execute(&mut conn)
            .await
            .unwrap();
        for (table, key) in [("blobs", "a"), ("blobs", "b"), ("blobs_ns", "a")] {
            sqlx::query(&row.replace("TABLE", table))
                .bind(key)
                .execute(&mut conn)
                .await
                .unwrap();
        }

        let lsn = init_shard(&options, false, false).await.unwrap();
        let expected = vec![
            ("blobs".to_string(), "a".to_string(), false),
            ("blobs".to_string(), "b".to_string(), false),
            ("blobs_ns".to_string(), "a".to_string(), false),
        ];
        assert_eq!(log(&mut conn).await, expected);
        assert_eq!(lsn, 3);
        assert_eq!(state(&mut conn, "indexed").await.unwrap(), "1");

        // Marked indexed: a row the index doesn't know about stays unindexed.
        sqlx::query(&row.replace("TABLE", "blobs"))
            .bind("late")
            .execute(&mut conn)
            .await
            .unwrap();
        assert_eq!(init_shard(&options, false, false).await.unwrap(), 3);
        assert_eq!(log(&mut conn).await, expected);
    }

    #[test]
    fn frames_survive_the_wire() {
        let frames = [
            Frame::Row {
                lsn: 7,
                tbl: "blobs_ns".to_string(),
                key: "k\r\n".to_string(),
                created_at: 1,
                updated_at: 2,
                expires_at: None,
                version: 3,
                data: Bytes::from_static(b"\x00\xffbinary\r\n"),
            },
            Frame::Row {
                lsn: 8,
                tbl: "blobs".to_string(),
                key: "k".to_string(),
                created_at: 1,
                updated_at: 2,
                expires_at: Some(99),
                version: 0,
                data: Bytes::new(),
            },
            Frame::Delete {
                lsn: 9,
                tbl: "blobs".to_string(),
                key: "gone".to_string(),
            },
            Frame::Watermark {
                watermark: 9,
                unix_ms: 1_700_000_000_000,
            },
        ];
        let mut buf = BytesMut::new();
        for frame in &frames {
            extend_encode(&mut buf, &frame.to_resp(), false).unwrap();
        }
        for frame in frames {
            let (decoded, _, _) = decode_bytes_mut(&mut buf).unwrap().unwrap();
            assert_eq!(Frame::from_resp(decoded).unwrap(), frame);
        }
        assert!(buf.is_empty());
    }

    fn decode_page(mut bytes: BytesMut) -> Vec<Frame> {
        let mut frames = Vec::new();
        while let Some((frame, _, _)) = decode_bytes_mut(&mut bytes).unwrap() {
            frames.push(Frame::from_resp(frame).unwrap());
        }
        frames
    }

    #[tokio::test]
    async fn a_page_that_reaches_the_head_ends_with_a_watermark() {
        let dir = TempDir::new().unwrap();
        let pool = replica_pool(&dir).await;
        let log = |n: usize| {
            let pool = pool.clone();
            async move {
                for i in 0..n {
                    sqlx::query(
                        "INSERT INTO repl_log (tbl, key, deleted, at) VALUES ('blobs', ?, 1, 0)",
                    )
                    .bind(format!(
                        "k{i}-{}",
                        Utc::now().timestamp_nanos_opt().unwrap()
                    ))
                    .execute(&pool)
                    .await
                    .unwrap();
                }
            }
        };

        // Not empty, but every entry after the cursor: rows then a watermark,
        // so a replica that keeps up gets one even while writes continue.
        log(3).await;
        let page = read_page(&pool, 1).await.unwrap();
        assert!(page.caught_up);
        assert_eq!(page.last_lsn, Some(3));
        let frames = decode_page(page.bytes);
        assert_eq!(frames.len(), 3);
        assert!(matches!(frames[2], Frame::Watermark { watermark: 3, .. }));

        // A full page may have more behind it: no watermark.
        log(PAGE_ROWS).await;
        let page = read_page(&pool, 0).await.unwrap();
        assert!(!page.caught_up);
        let frames = decode_page(page.bytes);
        assert_eq!(frames.len(), PAGE_ROWS);
        assert!(!frames.iter().any(|f| matches!(f, Frame::Watermark { .. })));
    }

    async fn replica_pool(dir: &TempDir) -> SqlitePool {
        let (options, _) = shard(dir).await;
        sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .unwrap()
    }

    fn row(lsn: i64, tbl: &str, key: &str) -> Frame {
        Frame::Row {
            lsn,
            tbl: tbl.to_string(),
            key: key.to_string(),
            created_at: 10,
            updated_at: 20,
            expires_at: Some(30),
            version: 4,
            data: Bytes::from_static(b"v"),
        }
    }

    async fn pool_state(pool: &SqlitePool, k: &str) -> Option<String> {
        get_state(&mut pool.acquire().await.unwrap(), k)
            .await
            .unwrap()
    }

    async fn count(pool: &SqlitePool, table: &str) -> i64 {
        sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
            .fetch_one(pool)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn apply_writes_rows_verbatim_and_a_failure_rolls_everything_back() {
        let dir = TempDir::new().unwrap();
        let pool = replica_pool(&dir).await;
        let mut known = HashSet::from(["blobs".to_string()]);

        // The last frame fails after earlier ones wrote and created a table.
        let frames = [
            row(5, "blobs_ns", "a"),
            row(6, "blobs", "b"),
            row(7, "blobs; DROP TABLE blobs", "c"),
        ];
        assert!(apply(&pool, &mut known, &frames).await.is_err());
        assert_eq!(known, HashSet::from(["blobs".to_string()]));
        assert_eq!(count(&pool, "blobs").await, 0);
        assert_eq!(count(&pool, "repl_log").await, 0);
        assert_eq!(
            count(&pool, "sqlite_master WHERE name = 'blobs_ns'").await,
            0
        );
        assert_eq!(pool_state(&pool, "applied_lsn").await, None);

        // Without the bad frame, and with a delete and a watermark, it commits.
        let frames = [
            row(5, "blobs_ns", "a"),
            row(6, "blobs", "b"),
            Frame::Delete {
                lsn: 8,
                tbl: "blobs".to_string(),
                key: "b".to_string(),
            },
            Frame::Watermark {
                watermark: 9,
                unix_ms: 1234,
            },
        ];
        // The replica's own LSNs: three entries, the last for the delete.
        assert_eq!(apply(&pool, &mut known, &frames).await.unwrap(), Some(3));
        assert!(known.contains("blobs_ns"));
        let stored: (i64, i64, Option<i64>, i64) = sqlx::query_as(
            "SELECT created_at, updated_at, expires_at, version FROM blobs_ns WHERE key = 'a'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(stored, (10, 20, Some(30), 4));
        assert_eq!(count(&pool, "blobs").await, 0);
        let log: Vec<(String, String, bool)> =
            sqlx::query_as("SELECT tbl, key, deleted FROM repl_log ORDER BY lsn")
                .fetch_all(&pool)
                .await
                .unwrap();
        assert_eq!(
            log,
            vec![
                ("blobs_ns".to_string(), "a".to_string(), false),
                ("blobs".to_string(), "b".to_string(), true),
            ]
        );
        for (k, v) in [
            ("applied_lsn", "8"),
            ("caught_up_lsn", "9"),
            ("watermark_ms", "1234"),
            ("synced", "1"),
        ] {
            assert_eq!(pool_state(&pool, k).await.as_deref(), Some(v), "{k}");
        }

        // A reset empties it all and adopts the new replid, not synced.
        reset(&pool, "new-history").await.unwrap();
        assert_eq!(count(&pool, "blobs_ns").await, 0);
        assert_eq!(count(&pool, "repl_log").await, 0);
        for (k, v) in [
            ("applied_lsn", Some("0")),
            ("caught_up_lsn", None),
            ("watermark_ms", None),
            ("synced", Some("0")),
            ("replid", Some("new-history")),
        ] {
            assert_eq!(pool_state(&pool, k).await.as_deref(), v, "{k}");
        }
    }

    #[tokio::test]
    async fn trimming_stops_at_the_oldest_pin_and_at_fresh_tombstones() {
        let dir = TempDir::new().unwrap();
        let pool = replica_pool(&dir).await;
        let now = Utc::now().timestamp();
        // lsn 1, 2, 3 are old tombstones, 4 is an old live entry, 5 a fresh tombstone.
        for (key, deleted, at) in [
            ("a", 1, now - 100),
            ("b", 1, now - 100),
            ("c", 1, now - 100),
            ("d", 0, now - 100),
            ("e", 1, now),
        ] {
            sqlx::query("INSERT INTO repl_log (tbl, key, deleted, at) VALUES ('blobs', ?, ?, ?)")
                .bind(key)
                .bind(deleted)
                .bind(at)
                .execute(&pool)
                .await
                .unwrap();
        }
        let keys = |pool: SqlitePool| async move {
            sqlx::query_scalar::<_, String>("SELECT key FROM repl_log ORDER BY lsn")
                .fetch_all(&pool)
                .await
                .unwrap()
        };
        let pins = ShardPins::default();
        let (low, high) = {
            let mut pins = pins.lock().await;
            (pins.add(2), pins.add(10))
        };

        // A stream at cursor 2 has sent lsn 1 and 2 but not 3: only lsn < 2 goes.
        assert_eq!(trim_tombstones(&pool, &pins, 50).await.unwrap(), 1);
        assert_eq!(keys(pool.clone()).await, ["b", "c", "d", "e"]);
        assert_eq!(
            pool_state(&pool, "trimmed_through").await.as_deref(),
            Some("1")
        );

        // The other pin holds back nothing below it; the fresh tombstone and
        // the live entry stay regardless.
        pins.lock().await.cursors.remove(&low);
        assert_eq!(trim_tombstones(&pool, &pins, 50).await.unwrap(), 2);
        assert_eq!(keys(pool.clone()).await, ["d", "e"]);
        assert_eq!(
            pool_state(&pool, "trimmed_through").await.as_deref(),
            Some("3")
        );

        // No pins: only the age bound applies.
        pins.lock().await.cursors.remove(&high);
        assert_eq!(trim_tombstones(&pool, &pins, 50).await.unwrap(), 0);
        assert_eq!(keys(pool.clone()).await, ["d", "e"]);
        assert_eq!(trim_tombstones(&pool, &pins, 0).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn init_shard_sets_roles_and_guards_promotion() {
        let dir = TempDir::new().unwrap();
        let (options, mut conn) = shard(&dir).await;

        // A new replica generates no replid, and isn't synced.
        init_shard(&options, true, true).await.unwrap();
        assert_eq!(state(&mut conn, "replid").await, None);
        assert_eq!(state(&mut conn, "role").await.unwrap(), "replica");
        assert_eq!(state(&mut conn, "synced").await.unwrap(), "0");

        // Its replid is the primary's, and a restart or --new-replid keeps it.
        set_state(&mut conn, "replid", "primary-history")
            .await
            .unwrap();
        set_state(&mut conn, "synced", "1").await.unwrap();
        init_shard(&options, true, true).await.unwrap();
        assert_eq!(state(&mut conn, "replid").await.unwrap(), "primary-history");
        assert_eq!(state(&mut conn, "synced").await.unwrap(), "1");

        // Promotion needs synced = 1 on every shard, checked before changing any.
        set_state(&mut conn, "synced", "0").await.unwrap();
        let options_list = std::slice::from_ref(&options);
        assert!(ensure_promotable(options_list).await.is_err());
        assert!(init_shard(&options, false, false).await.is_err());
        assert_eq!(state(&mut conn, "role").await.unwrap(), "replica");

        // Synced: promoted with a new replid, and a primary from then on.
        set_state(&mut conn, "synced", "1").await.unwrap();
        ensure_promotable(options_list).await.unwrap();
        init_shard(&options, false, false).await.unwrap();
        assert_eq!(state(&mut conn, "role").await.unwrap(), "primary");
        let promoted = state(&mut conn, "replid").await.unwrap();
        assert_ne!(promoted, "primary-history");
        init_shard(&options, false, false).await.unwrap();
        assert_eq!(state(&mut conn, "replid").await.unwrap(), promoted);

        // Demoting makes it a replica that has to sync again.
        init_shard(&options, false, true).await.unwrap();
        assert_eq!(state(&mut conn, "role").await.unwrap(), "replica");
        assert_eq!(state(&mut conn, "replid").await.unwrap(), promoted);
        assert_eq!(state(&mut conn, "synced").await.unwrap(), "0");
    }

    #[tokio::test]
    async fn a_resync_waits_for_replica_reads_admitted_before_it() {
        use crate::config::{Cfg, ReplicationConfig};
        use crate::metrics::Metrics;
        use crate::shard_manager::{ShardWriter, shard_writer_task};

        let dir = TempDir::new().unwrap();
        let cfg = Cfg {
            data_dir: dir.path().to_string_lossy().into_owned(),
            num_shards: 1,
            storage_compression: None,
            async_write: None,
            batch_size: Some(1),
            batch_timeout_ms: Some(0),
            addr: None,
            replication: None,
            cluster: None,
            metrics: None,
            shutdown_timeout_secs: None,
            max_request_size_mb: None,
            cleanup_chunk_size: None,
            cleanup_interval_secs: None,
            new_replid: false,
            sqlite: None,
        };
        let mut receivers = Vec::new();
        let mut state = AppState::new(cfg, &mut receivers).await.unwrap();
        // AppState::new wants a cluster for a replica; the gate only needs the role.
        state.cfg.replication = Some(ReplicationConfig {
            replica_of: Some("p".to_string()),
            tombstone_retention_secs: None,
        });
        let state = Arc::new(state);
        let shutdown = CancellationToken::new();
        let writer = tokio::spawn(shard_writer_task(ShardWriter {
            shard_id: 0,
            pool: state.write_db_pools[0].clone(),
            receiver: receivers.remove(0),
            batch_size: 1,
            batch_timeout_ms: 0,
            inflight_cache: state.inflight_cache.clone(),
            inflight_hcache: state.inflight_hcache.clone(),
            metrics: Metrics::new(),
            commit_watch: state.commit_watch[0].clone(),
            shutdown: shutdown.clone(),
        }));
        sqlx::query(
            "INSERT INTO blobs (key, data, created_at, updated_at) VALUES ('k', x'00', 0, 0)",
        )
        .execute(&state.write_db_pools[0])
        .await
        .unwrap();
        let rows = || async {
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM blobs")
                .fetch_one(&state.db_pools[0])
                .await
                .unwrap()
        };

        // Only a READONLY read on a replica takes the gate.
        assert!(state.replica_read_gate(0, false).await.is_none());
        let admitted = state.replica_read_gate(0, true).await.unwrap();

        let mut replica = Replica {
            state: state.clone(),
            shard: 0,
            primary: "p".to_string(),
            metrics: ShardMetrics::new(0),
            tick: interval(Duration::from_secs(1)),
            last_watermark_ms: None,
            backoff: BACKOFF_MIN,
        };
        let mut reset = tokio::spawn(async move { replica.reset("new-replid").await });
        // The reset can't start, so the admitted read still sees the copy. (A
        // reset that didn't wait would finish well within this.)
        assert!(
            timeout(Duration::from_millis(200), &mut reset)
                .await
                .is_err()
        );
        assert_eq!(rows().await, 1);

        drop(admitted);
        reset.await.unwrap().unwrap();
        assert_eq!(rows().await, 0);
        // And a read arriving afterwards is not blocked.
        assert!(state.replica_read_gate(0, true).await.is_some());

        shutdown.cancel();
        writer.await.unwrap();
    }
}
