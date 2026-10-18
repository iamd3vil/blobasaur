//! The change index (`repl_log`, `repl_state`) every shard keeps for
//! replication: startup initialization, the boot-id durability check and the
//! commit watermark. See REPLICATION.md.

use chrono::Utc;
use miette::{Result, miette};
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{Connection, SqliteConnection};
use std::hash::{BuildHasher, RandomState};
use tokio::sync::watch;

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
/// settles the replid and records this boot. Returns the shard's highest
/// committed LSN.
pub async fn init_shard(options: &SqliteConnectOptions, force_new_replid: bool) -> Result<u64> {
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

    let replid = get_state(&mut tx, "replid").await?;
    let stored_boot_id = get_state(&mut tx, "boot_id").await?;
    let clean_shutdown = get_state(&mut tx, "clean_shutdown").await?.as_deref() == Some("1");
    let boot_id = std::fs::read_to_string(BOOT_ID_PATH)
        .ok()
        .map(|id| id.trim().to_string());
    if needs_new_replid(
        force_new_replid,
        replid.as_deref(),
        stored_boot_id.as_deref(),
        boot_id.as_deref(),
        clean_shutdown,
    ) {
        if replid.is_some() {
            tracing::warn!(
                forced = force_new_replid,
                clean_shutdown,
                "Generating a new replid: replicas will resync"
            );
        }
        set_state(&mut tx, "replid", &new_replid()).await?;
    }
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
        init_shard(&options, false).await.unwrap();
        let replid = state(&mut conn, "replid").await.unwrap();
        assert_eq!(replid.len(), 40);
        assert_eq!(state(&mut conn, "clean_shutdown").await.unwrap(), "0");

        // Same boot, no clean shutdown (process crash): kept.
        init_shard(&options, false).await.unwrap();
        assert_eq!(state(&mut conn, "replid").await.unwrap(), replid);

        // Another boot after a clean shutdown: kept.
        mark_clean_shutdown(&options).await.unwrap();
        assert_eq!(state(&mut conn, "clean_shutdown").await.unwrap(), "1");
        set_state(&mut conn, "boot_id", "old-boot").await.unwrap();
        init_shard(&options, false).await.unwrap();
        assert_eq!(state(&mut conn, "replid").await.unwrap(), replid);
        assert_eq!(state(&mut conn, "clean_shutdown").await.unwrap(), "0");

        // Another boot, not shut down cleanly (power loss): new replid.
        set_state(&mut conn, "boot_id", "old-boot").await.unwrap();
        init_shard(&options, false).await.unwrap();
        let after_crash = state(&mut conn, "replid").await.unwrap();
        assert_ne!(after_crash, replid);
        assert_ne!(state(&mut conn, "boot_id").await.unwrap(), "old-boot");

        // `--new-replid`.
        init_shard(&options, true).await.unwrap();
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

        let lsn = init_shard(&options, false).await.unwrap();
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
        assert_eq!(init_shard(&options, false).await.unwrap(), 3);
        assert_eq!(log(&mut conn).await, expected);
    }
}
