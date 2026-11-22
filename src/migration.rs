use miette::{Context, Result};
use mpchash::HashRing;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode};
use sqlx::{Connection, Row, SqliteConnection, SqlitePool};
use std::collections::{HashMap, HashSet};
use std::fs::OpenOptions;
use std::path::Path;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};

const SQLITE_AUTO_VACUUM_NONE: i64 = 0;
const SQLITE_AUTO_VACUUM_FULL: i64 = 1;
const SQLITE_AUTO_VACUUM_INCREMENTAL: i64 = 2;

// Helper function to convert sqlx errors to miette errors
fn sqlx_to_miette(err: sqlx::Error, context: &str) -> miette::Error {
    miette::miette!("{}: {}", context, err)
}

fn sqlite_auto_vacuum_mode_name(mode: i64) -> &'static str {
    match mode {
        SQLITE_AUTO_VACUUM_NONE => "NONE",
        SQLITE_AUTO_VACUUM_FULL => "FULL",
        SQLITE_AUTO_VACUUM_INCREMENTAL => "INCREMENTAL",
        _ => "UNKNOWN",
    }
}

async fn warn_if_auto_vacuum_not_incremental(pool: &SqlitePool, shard_id: usize) {
    match sqlx::query_scalar::<_, i64>("PRAGMA auto_vacuum")
        .fetch_one(pool)
        .await
    {
        Ok(SQLITE_AUTO_VACUUM_INCREMENTAL) => {}
        Ok(mode) => {
            tracing::warn!(
                shard_id,
                auto_vacuum_mode = mode,
                auto_vacuum_mode_name = sqlite_auto_vacuum_mode_name(mode),
                "Shard DB auto_vacuum is not INCREMENTAL; existing DBs require VACUUM to switch mode"
            );
        }
        Err(error) => {
            tracing::warn!(
                shard_id,
                error = %error,
                "Failed to read PRAGMA auto_vacuum for shard DB"
            );
        }
    }
}

#[derive(Hash)]
struct ShardNode(u64);

pub struct MigrationManager {
    old_shard_count: usize,
    new_shard_count: usize,
    data_dir: String,
    new_ring: HashRing<ShardNode>,
}

impl MigrationManager {
    pub fn new(old_shard_count: usize, new_shard_count: usize, data_dir: String) -> Result<Self> {
        if old_shard_count == 0 || new_shard_count == 0 {
            return Err(miette::miette!("Shard count must be greater than 0"));
        }

        if old_shard_count == new_shard_count {
            return Err(miette::miette!(
                "Old and new shard counts are the same, no migration needed"
            ));
        }

        // Create hash rings for new configuration.
        let new_ring = HashRing::new();
        for i in 0..new_shard_count {
            new_ring.add(ShardNode(i as u64));
        }

        Ok(MigrationManager {
            old_shard_count,
            new_shard_count,
            data_dir,
            new_ring,
        })
    }

    fn get_new_shard(&self, key: &str) -> usize {
        let token = self.new_ring.node(&key).unwrap();
        token.node().0 as usize
    }

    async fn create_connection_pool(&self, shard_id: usize) -> Result<SqlitePool> {
        let db_path = format!("{}/shard_{}.db", self.data_dir, shard_id);

        // Use recommended SQLite settings for migration operations
        // See: https://kerkour.com/sqlite-for-servers
        let connect_options = SqliteConnectOptions::from_str(&format!("sqlite:{}", db_path))
            .map_err(|e| sqlx_to_miette(e, "Failed to parse connection string"))?
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .busy_timeout(std::time::Duration::from_millis(5000))
            .pragma("auto_vacuum", "INCREMENTAL")
            .pragma("synchronous", "NORMAL") // NORMAL is safer than OFF
            .pragma("cache_size", "-1024000") // 1GB cache for better performance during migration
            .pragma("temp_store", "MEMORY")
            .pragma("foreign_keys", "true");

        SqlitePool::connect_with(connect_options)
            .await
            .map_err(|e| sqlx_to_miette(e, "Failed to connect to database"))
    }

    async fn create_readonly_pool(&self, shard_id: usize) -> Result<SqlitePool> {
        let db_path = format!("{}/shard_{}.db", self.data_dir, shard_id);
        let connect_options = SqliteConnectOptions::from_str(&format!("sqlite:{}", db_path))
            .map_err(|e| sqlx_to_miette(e, "Failed to parse connection string"))?
            .read_only(true)
            .busy_timeout(std::time::Duration::from_millis(5000));

        SqlitePool::connect_with(connect_options)
            .await
            .map_err(|e| sqlx_to_miette(e, "Failed to connect to database read-only"))
    }

    /// Atomically creates and initializes a brand-new destination shard file
    /// via a process-private temporary file in the same directory and publishes
    /// it via `hard_link` (never overwriting an existing shard).
    async fn init_new_shard_atomically(&self, shard_id: usize, schema_version: i64) -> Result<()> {
        static TMP_SEQ: AtomicU64 = AtomicU64::new(0);
        let seq = TMP_SEQ.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        let db_path = format!("{}/shard_{}.db", self.data_dir, shard_id);
        let tmp_path = format!(
            "{}/shard_{}.db.init.{}.{}.tmp",
            self.data_dir, shard_id, pid, seq
        );
        let cleanup_tmp = |path: &str| {
            for sidecar in [
                path.to_string(),
                format!("{path}-journal"),
                format!("{path}-wal"),
                format!("{path}-shm"),
            ] {
                let _ = std::fs::remove_file(&sidecar);
            }
        };
        cleanup_tmp(&tmp_path);

        // Default rollback journal mode avoids creating -wal/-shm sidecars
        // before publishing; create_connection_pool switches to WAL once
        // the shard is published.
        let connect_options = SqliteConnectOptions::from_str(&format!("sqlite:{}", tmp_path))
            .map_err(|e| sqlx_to_miette(e, "Failed to parse temp shard connection string"))?
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Delete)
            .busy_timeout(std::time::Duration::from_millis(5000))
            .pragma("auto_vacuum", "INCREMENTAL")
            .pragma("foreign_keys", "true");

        let mut conn = SqliteConnection::connect_with(&connect_options)
            .await
            .map_err(|e| {
                cleanup_tmp(&tmp_path);
                sqlx_to_miette(
                    e,
                    &format!("Failed to create temporary shard {} DB", shard_id),
                )
            })?;

        let init_result: Result<()> = async {
            let mut tx = conn.begin_with("BEGIN IMMEDIATE").await.map_err(|e| {
                sqlx_to_miette(
                    e,
                    &format!("Failed to begin init transaction for shard {}", shard_id),
                )
            })?;
            sqlx::query(
                "CREATE TABLE IF NOT EXISTS blobs (
                    key TEXT PRIMARY KEY,
                    data BLOB,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    expires_at INTEGER,
                    version INTEGER NOT NULL DEFAULT 0
                )",
            )
            .execute(&mut *tx)
            .await
            .map_err(|e| {
                sqlx_to_miette(
                    e,
                    &format!("Failed to create blobs table in new shard {}", shard_id),
                )
            })?;

            sqlx::query(
                "CREATE INDEX IF NOT EXISTS idx_expires_at ON blobs(expires_at) WHERE expires_at IS NOT NULL",
            )
            .execute(&mut *tx)
            .await
            .map_err(|e| {
                sqlx_to_miette(
                    e,
                    &format!("Failed to create expires_at index in new shard {}", shard_id),
                )
            })?;

            if schema_version == crate::shard_manager::SHARD_SCHEMA_VERSION {
                sqlx::query(crate::shard_manager::CREATE_BLOB_NAMESPACES_TABLE)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| {
                        sqlx_to_miette(
                            e,
                            &format!(
                                "Failed to create blob_namespaces table in new shard {}",
                                shard_id
                            ),
                        )
                    })?;
                sqlx::query(&format!("PRAGMA user_version = {}", schema_version))
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| {
                        sqlx_to_miette(
                            e,
                            &format!("Failed to set user_version in new shard {}", shard_id),
                        )
                    })?;
            }

            tx.commit().await.map_err(|e| {
                sqlx_to_miette(
                    e,
                    &format!("Failed to commit init transaction for shard {}", shard_id),
                )
            })?;
            Ok(())
        }
        .await;

        let close_result = conn.close().await;
        if let Err(err) = init_result {
            cleanup_tmp(&tmp_path);
            return Err(err);
        }
        if let Err(e) = close_result {
            cleanup_tmp(&tmp_path);
            return Err(sqlx_to_miette(
                e,
                &format!("Failed to close temporary shard {} DB", shard_id),
            ));
        }

        // Publish via hard_link so we never replace an existing shard file.
        match std::fs::hard_link(&tmp_path, &db_path) {
            Ok(()) => {
                cleanup_tmp(&tmp_path);
                Ok(())
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                cleanup_tmp(&tmp_path);
                Err(miette::miette!(
                    "Destination shard {} already exists (concurrent resharding detected)",
                    db_path
                ))
            }
            Err(e) => {
                cleanup_tmp(&tmp_path);
                Err(miette::miette!(
                    "Failed to publish initialized shard {} ({} -> {}): {}",
                    shard_id,
                    tmp_path,
                    db_path,
                    e
                ))
            }
        }
    }

    fn acquire_migration_lock(&self) -> Result<std::fs::File> {
        let lock_path = format!("{}/.blobasaur_migration.lock", self.data_dir);
        let lock_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)
            .map_err(|e| {
                miette::miette!("Failed to open migration lock file {}: {}", lock_path, e)
            })?;
        lock_file.try_lock().map_err(|e| {
            miette::miette!(
                "Another shard migration is already running in {} ({})",
                self.data_dir,
                e
            )
        })?;
        Ok(lock_file)
    }

    #[allow(dead_code)]
    pub async fn run_migration(&self) -> Result<()> {
        self.run_migration_with_verify(false).await
    }

    pub async fn run_migration_with_verify(&self, verify: bool) -> Result<()> {
        tracing::info!(
            "Starting shard migration from {} to {} shards",
            self.old_shard_count,
            self.new_shard_count
        );

        // Hold the exclusive data_dir lock through both migration and optional
        // verification so a second reshard cannot interleave between them.
        let _migration_lock = self.acquire_migration_lock()?;

        // Preflight all source shards and any already-existing destination shards
        // before modifying any file. Uses probe_shard_schema_version so a hot
        // rollback journal can recover without switching journal_mode to WAL.
        let mut schema_version: Option<(usize, i64)> = None;
        let mut v0_namespace_spellings: HashMap<String, (usize, String)> = HashMap::new();
        let max_shards = self.old_shard_count.max(self.new_shard_count);
        for i in 0..max_shards {
            let db_path = format!("{}/shard_{}.db", self.data_dir, i);
            let exists = Path::new(&db_path).exists();
            if i < self.old_shard_count && !exists {
                return Err(miette::miette!("Old shard file {} does not exist", db_path));
            }
            if !exists {
                continue;
            }

            let version = crate::shard_manager::probe_shard_schema_version(&db_path, i, 5000)
                .await
                .wrap_err(format!("Failed to probe schema version for shard {}", i))?;

            if !(0..=crate::shard_manager::SHARD_SCHEMA_VERSION).contains(&version) {
                return Err(miette::miette!(
                    "Shard {} has unsupported schema version v{} (supported: 0..={})",
                    i,
                    version,
                    crate::shard_manager::SHARD_SCHEMA_VERSION
                ));
            }

            match schema_version {
                None => schema_version = Some((i, version)),
                Some((first_shard, expected)) if expected != version => {
                    return Err(miette::miette!(
                        "Shards have mismatched schema versions: shard {} is v{}, shard {} is v{}",
                        first_shard,
                        expected,
                        i,
                        version
                    ));
                }
                Some(_) => {}
            }

            let pool = self
                .create_readonly_pool(i)
                .await
                .wrap_err(format!("Failed to connect read-only to shard {}", i))?;

            let tables = self.get_all_tables(&pool).await.wrap_err(format!(
                "Failed to list tables during preflight for shard {}",
                i
            ))?;
            if version == 0 {
                for table_name in &tables {
                    if table_name == "blobs" {
                        continue;
                    }
                    // Compare the stripped namespace suffixes (preserving suffix case),
                    // so equivalent prefixes like `BLOBS_Users` and `blobs_Users` match.
                    let Some(ns_suffix) =
                        crate::shard_manager::strip_blobs_prefix_ignore_ascii_case(table_name)
                    else {
                        continue;
                    };
                    let lower = ns_suffix.to_ascii_lowercase();
                    if let Some((prev_shard, prev_suffix)) = v0_namespace_spellings.get(&lower) {
                        if prev_suffix != ns_suffix {
                            pool.close().await;
                            return Err(miette::miette!(
                                "Ambiguous legacy namespace tables across shards: shard {} has '{}' while shard {} has '{}'. Start Blobasaur to upgrade shards to schema v{} before resharding.",
                                prev_shard,
                                prev_suffix,
                                i,
                                ns_suffix,
                                crate::shard_manager::SHARD_SCHEMA_VERSION
                            ));
                        }
                    } else {
                        v0_namespace_spellings.insert(lower, (i, ns_suffix.to_string()));
                    }
                }
            } else if version == crate::shard_manager::SHARD_SCHEMA_VERSION {
                let check = self.validate_v1_namespace_metadata(&pool, i, &tables).await;
                pool.close().await;
                check?;
                continue;
            }

            pool.close().await;
        }
        let schema_version = schema_version.map(|(_, v)| v).unwrap_or(0);

        // Create new shard databases atomically if they don't exist yet; for
        // already-existing shards, ensure base schema objects are present.
        for i in 0..self.new_shard_count {
            let db_path = format!("{}/shard_{}.db", self.data_dir, i);
            if !Path::new(&db_path).exists() {
                self.init_new_shard_atomically(i, schema_version).await?;
            }

            let pool = self.create_connection_pool(i).await.wrap_err(format!(
                "Failed to create connection pool for new shard {}",
                i
            ))?;

            warn_if_auto_vacuum_not_incremental(&pool, i).await;

            // Create the main blobs table
            sqlx::query(
                "CREATE TABLE IF NOT EXISTS blobs (
                    key TEXT PRIMARY KEY,
                    data BLOB,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    expires_at INTEGER,
                    version INTEGER NOT NULL DEFAULT 0
                )",
            )
            .execute(&pool)
            .await
            .map_err(|e| {
                sqlx_to_miette(
                    e,
                    &format!("Failed to create blobs table in new shard {}", i),
                )
            })?;

            // Create index on expires_at
            sqlx::query(
                "CREATE INDEX IF NOT EXISTS idx_expires_at ON blobs(expires_at) WHERE expires_at IS NOT NULL",
            )
            .execute(&pool)
            .await
            .map_err(|e| sqlx_to_miette(e, &format!("Failed to create expires_at index in new shard {}", i)))?;

            pool.close().await;
        }

        // Migrate data from old shards to new shards
        for old_shard_id in 0..self.old_shard_count {
            tracing::info!("Processing old shard {}", old_shard_id);

            let old_pool = self
                .create_connection_pool(old_shard_id)
                .await
                .wrap_err(format!("Failed to connect to old shard {}", old_shard_id))?;

            // Get all table names (including namespaced tables)
            let tables = self.get_all_tables(&old_pool).await.wrap_err(format!(
                "Failed to get tables from old shard {}",
                old_shard_id
            ))?;

            tracing::info!(
                "Found {} tables in old shard {}: {:?}",
                tables.len(),
                old_shard_id,
                tables
            );

            for table_name in tables {
                tracing::info!(
                    "Migrating table {} from old shard {}",
                    table_name,
                    old_shard_id
                );

                // Count records before migration
                let count_query = format!("SELECT COUNT(*) as count FROM {}", table_name);
                if let Ok(row) = sqlx::query(&count_query).fetch_one(&old_pool).await {
                    let count: i64 = row.get("count");
                    tracing::info!(
                        "Table {} in old shard {} has {} records",
                        table_name,
                        old_shard_id,
                        count
                    );
                }

                self.migrate_table(&old_pool, &table_name, old_shard_id, schema_version)
                    .await
                    .wrap_err(format!(
                        "Failed to migrate table {} from old shard {}",
                        table_name, old_shard_id
                    ))?;
            }

            old_pool.close().await;
        }

        tracing::info!("Shard migration completed successfully");
        tracing::info!(
            "You can now update your configuration to use {} shards",
            self.new_shard_count
        );
        tracing::info!("Remember to backup the old shard files before deleting them");

        if verify {
            self.verify_migration_unlocked().await?;
        }

        Ok(())
    }

    async fn get_all_tables(&self, pool: &SqlitePool) -> Result<Vec<String>> {
        let rows = sqlx::query(
            "SELECT name FROM sqlite_master WHERE type='table' AND (name = 'blobs' OR name LIKE 'blobs\\_%' ESCAPE '\\') ORDER BY name",
        )
        .fetch_all(pool)
        .await
        .map_err(|e| sqlx_to_miette(e, "Failed to query table names"))?;

        let tables: Vec<String> = rows
            .iter()
            .map(|row| row.get::<String, _>("name"))
            .collect();

        Ok(tables)
    }

    async fn validate_v1_namespace_metadata(
        &self,
        pool: &SqlitePool,
        shard_id: usize,
        tables: &[String],
    ) -> Result<HashMap<String, String>> {
        let rows: Vec<(String, String)> =
            sqlx::query_as("SELECT namespace, table_name FROM blob_namespaces")
                .fetch_all(pool)
                .await
                .map_err(|e| {
                    sqlx_to_miette(
                        e,
                        &format!("Failed to read blob_namespaces from shard {}", shard_id),
                    )
                })?;

        let physical_set: HashSet<&str> = tables
            .iter()
            .map(String::as_str)
            .filter(|t| *t != "blobs")
            .collect();

        let mut table_to_ns = HashMap::with_capacity(rows.len());
        for (namespace, table_name) in rows {
            let expected_table = crate::shard_manager::namespace_table(&namespace);
            if expected_table != table_name {
                return Err(miette::miette!(
                    "Corrupted blob_namespaces entry in shard {}: namespace '{}' maps to '{}' but expected '{}'",
                    shard_id,
                    namespace,
                    table_name,
                    expected_table
                ));
            }
            if !physical_set.contains(table_name.as_str()) {
                return Err(miette::miette!(
                    "Orphan blob_namespaces entry in shard {}: namespace '{}' references missing table '{}'",
                    shard_id,
                    namespace,
                    table_name
                ));
            }
            table_to_ns.insert(table_name, namespace);
        }

        for table_name in physical_set {
            if !table_to_ns.contains_key(table_name) {
                return Err(miette::miette!(
                    "Missing blob_namespaces entry for table '{}' in v{} shard {}",
                    table_name,
                    crate::shard_manager::SHARD_SCHEMA_VERSION,
                    shard_id
                ));
            }
        }

        Ok(table_to_ns)
    }

    async fn migrate_table(
        &self,
        old_pool: &SqlitePool,
        table_name: &str,
        current_old_shard: usize,
        schema_version: i64,
    ) -> Result<()> {
        // First, read only keys to determine which ones need to be migrated
        let keys_query = format!("SELECT key FROM {}", table_name);
        let key_rows = sqlx::query(&keys_query)
            .fetch_all(old_pool)
            .await
            .map_err(|e| {
                sqlx_to_miette(e, &format!("Failed to read keys from table {}", table_name))
            })?;

        tracing::info!(
            "Found {} keys in table {} of old shard {}",
            key_rows.len(),
            table_name,
            current_old_shard
        );

        // Identify keys that need to be migrated (those moving to different shards)
        let mut keys_to_migrate: HashMap<usize, Vec<String>> = HashMap::new();
        let mut keys_to_delete: Vec<String> = Vec::new();

        for row in key_rows {
            let key: String = row.get("key");
            let new_shard_id = self.get_new_shard(&key);

            if new_shard_id != current_old_shard {
                // Key needs to be migrated to a different shard
                keys_to_migrate
                    .entry(new_shard_id)
                    .or_default()
                    .push(key.clone());
                keys_to_delete.push(key);
            }
        }

        if keys_to_migrate.is_empty() {
            tracing::info!(
                "No keys need migration from old shard {} table {} (all keys remain in same shard)",
                current_old_shard,
                table_name
            );
            return Ok(());
        }

        tracing::info!(
            "Need to migrate {} keys from old shard {} to {} different shards",
            keys_to_delete.len(),
            current_old_shard,
            keys_to_migrate.len()
        );

        // Process migration in batches for each destination shard
        const BATCH_SIZE: usize = 1000;

        for (new_shard_id, keys) in keys_to_migrate {
            if keys.is_empty() {
                continue;
            }

            tracing::info!(
                "Migrating {} keys from old shard {} to new shard {}",
                keys.len(),
                current_old_shard,
                new_shard_id
            );

            let new_pool = self
                .create_connection_pool(new_shard_id)
                .await
                .wrap_err(format!("Failed to connect to new shard {}", new_shard_id))?;

            // Ensure the table, expiry index, and v1 metadata exist atomically in the new shard
            if table_name != "blobs" {
                let source_namespace = if schema_version
                    == crate::shard_manager::SHARD_SCHEMA_VERSION
                {
                    let namespace: String = sqlx::query_scalar(
                        "SELECT namespace FROM blob_namespaces WHERE table_name = ?",
                    )
                    .bind(table_name)
                    .fetch_optional(old_pool)
                    .await
                    .map_err(|e| {
                        sqlx_to_miette(
                            e,
                            &format!(
                                "Failed to read blob_namespaces entry for {} from old shard {}",
                                table_name, current_old_shard
                            ),
                        )
                    })?
                    .ok_or_else(|| {
                        miette::miette!(
                            "Missing blob_namespaces entry for table '{}' in old shard {}",
                            table_name,
                            current_old_shard
                        )
                    })?;

                    let expected_table = crate::shard_manager::namespace_table(&namespace);
                    if expected_table != table_name {
                        return Err(miette::miette!(
                            "Corrupted blob_namespaces entry in old shard {}: namespace '{}' maps to '{}' but expected '{}'",
                            current_old_shard,
                            namespace,
                            table_name,
                            expected_table
                        ));
                    }
                    Some(namespace)
                } else {
                    None
                };

                let mut ddl_tx = new_pool.begin().await.map_err(|e| {
                    sqlx_to_miette(
                        e,
                        &format!(
                            "Failed to begin DDL transaction for table {} in new shard {}",
                            table_name, new_shard_id
                        ),
                    )
                })?;

                let create_query = format!(
                    "CREATE TABLE IF NOT EXISTS {} (
                        key TEXT PRIMARY KEY,
                        data BLOB,
                        created_at INTEGER NOT NULL,
                        updated_at INTEGER NOT NULL,
                        expires_at INTEGER,
                        version INTEGER NOT NULL DEFAULT 0
                    )",
                    table_name
                );

                sqlx::query(&create_query)
                    .execute(&mut *ddl_tx)
                    .await
                    .map_err(|e| {
                        sqlx_to_miette(
                            e,
                            &format!(
                                "Failed to create table {} in new shard {}",
                                table_name, new_shard_id
                            ),
                        )
                    })?;

                // Create index on expires_at for namespaced tables
                let index_name = crate::shard_manager::namespace_index(table_name);
                let index_query = format!(
                    "CREATE INDEX IF NOT EXISTS {} ON {}(expires_at) WHERE expires_at IS NOT NULL",
                    index_name, table_name
                );

                sqlx::query(&index_query)
                    .execute(&mut *ddl_tx)
                    .await
                    .map_err(|e| {
                        sqlx_to_miette(
                            e,
                            &format!(
                                "Failed to create expires_at index for table {} in new shard {}",
                                table_name, new_shard_id
                            ),
                        )
                    })?;

                if let Some(namespace) = source_namespace {
                    sqlx::query(crate::shard_manager::CREATE_BLOB_NAMESPACES_TABLE)
                        .execute(&mut *ddl_tx)
                        .await
                        .map_err(|e| {
                            sqlx_to_miette(
                                e,
                                &format!(
                                    "Failed to ensure blob_namespaces in new shard {}",
                                    new_shard_id
                                ),
                            )
                        })?;

                    let existing: Vec<(String, String)> = sqlx::query_as(
                        "SELECT namespace, table_name FROM blob_namespaces WHERE table_name = ? OR namespace = ?",
                    )
                    .bind(table_name)
                    .bind(&namespace)
                    .fetch_all(&mut *ddl_tx)
                    .await
                    .map_err(|e| {
                        sqlx_to_miette(
                            e,
                            &format!(
                                "Failed to inspect blob_namespaces in new shard {}",
                                new_shard_id
                            ),
                        )
                    })?;

                    if existing.is_empty() {
                        sqlx::query(
                            "INSERT INTO blob_namespaces (namespace, table_name) VALUES (?, ?)",
                        )
                        .bind(&namespace)
                        .bind(table_name)
                        .execute(&mut *ddl_tx)
                        .await
                        .map_err(|e| {
                            sqlx_to_miette(
                                e,
                                &format!(
                                    "Failed to insert blob_namespaces entry for {} into new shard {}",
                                    table_name, new_shard_id
                                ),
                            )
                        })?;
                    } else if existing.len() != 1
                        || existing[0].0 != namespace
                        || existing[0].1 != table_name
                    {
                        return Err(miette::miette!(
                            "Conflicting blob_namespaces entry in new shard {} for ('{}', '{}'): found {:?}",
                            new_shard_id,
                            namespace,
                            table_name,
                            existing
                        ));
                    }
                }

                ddl_tx.commit().await.map_err(|e| {
                    sqlx_to_miette(
                        e,
                        &format!(
                            "Failed to commit DDL transaction for table {} in new shard {}",
                            table_name, new_shard_id
                        ),
                    )
                })?;
            }

            // Process keys in batches
            for batch in keys.chunks(BATCH_SIZE) {
                // Read the full record data for this batch
                let placeholders = batch.iter().map(|_| "?").collect::<Vec<_>>().join(",");
                let batch_query = format!(
                    "SELECT key, data, created_at, updated_at, expires_at, version FROM {} WHERE key IN ({})",
                    table_name, placeholders
                );

                let mut query = sqlx::query(&batch_query);
                for key in batch {
                    query = query.bind(key);
                }

                let records = query.fetch_all(old_pool).await.map_err(|e| {
                    sqlx_to_miette(
                        e,
                        &format!("Failed to read batch of records from table {}", table_name),
                    )
                })?;

                let records_len = records.len();

                // Insert records in a transaction
                let mut tx = new_pool.begin().await.map_err(|e| {
                    sqlx_to_miette(
                        e,
                        &format!("Failed to start transaction for new shard {}", new_shard_id),
                    )
                })?;

                let insert_query = format!(
                    "INSERT OR REPLACE INTO {} (key, data, created_at, updated_at, expires_at, version) VALUES (?, ?, ?, ?, ?, ?)",
                    table_name
                );

                for record in records {
                    let key: String = record.get("key");
                    let data: Vec<u8> = record.get("data");
                    let created_at: i64 = record.get("created_at");
                    let updated_at: i64 = record.get("updated_at");
                    let expires_at: Option<i64> = record.get("expires_at");
                    let version: i64 = record.get("version");

                    sqlx::query(&insert_query)
                        .bind(&key)
                        .bind(&data)
                        .bind(created_at)
                        .bind(updated_at)
                        .bind(expires_at)
                        .bind(version)
                        .execute(&mut *tx)
                        .await
                        .map_err(|e| {
                            sqlx_to_miette(
                                e,
                                &format!(
                                    "Failed to insert record {} into new shard {}",
                                    key, new_shard_id
                                ),
                            )
                        })?;
                }

                tx.commit().await.map_err(|e| {
                    sqlx_to_miette(
                        e,
                        &format!(
                            "Failed to commit transaction for new shard {}",
                            new_shard_id
                        ),
                    )
                })?;

                tracing::info!(
                    "Migrated batch of {} records to new shard {}",
                    records_len,
                    new_shard_id
                );
            }

            new_pool.close().await;
        }

        // Delete migrated keys from the old shard in batches
        if !keys_to_delete.is_empty() {
            tracing::info!(
                "Deleting {} keys from old shard {} table {} (keys that moved to different shards)",
                keys_to_delete.len(),
                current_old_shard,
                table_name
            );

            for batch in keys_to_delete.chunks(BATCH_SIZE) {
                let mut tx = old_pool.begin().await.map_err(|e| {
                    sqlx_to_miette(
                        e,
                        &format!(
                            "Failed to start cleanup transaction for shard {}",
                            current_old_shard
                        ),
                    )
                })?;

                let delete_query = format!("DELETE FROM {} WHERE key = ?", table_name);

                for key in batch {
                    sqlx::query(&delete_query)
                        .bind(key)
                        .execute(&mut *tx)
                        .await
                        .map_err(|e| {
                            sqlx_to_miette(
                                e,
                                &format!(
                                    "Failed to delete key {} from shard {}",
                                    key, current_old_shard
                                ),
                            )
                        })?;
                }

                tx.commit().await.map_err(|e| {
                    sqlx_to_miette(
                        e,
                        &format!(
                            "Failed to commit cleanup transaction for shard {}",
                            current_old_shard
                        ),
                    )
                })?;

                tracing::info!(
                    "Deleted batch of {} keys from old shard {}",
                    batch.len(),
                    current_old_shard
                );
            }
        }

        Ok(())
    }

    #[allow(dead_code)]
    pub async fn verify_migration(&self) -> Result<()> {
        let _migration_lock = self.acquire_migration_lock()?;
        self.verify_migration_unlocked().await
    }

    async fn verify_migration_unlocked(&self) -> Result<()> {
        tracing::info!("Verifying migration integrity...");

        // For verification, we need to count the total records that exist in the new shard configuration
        // We cannot rely on old shards because some may overlap with new shards

        // Instead, we'll verify that each key from the original data is in the correct new shard
        // and that no data is lost or duplicated

        let mut new_total_count = 0;
        let mut new_table_counts: HashMap<String, usize> = HashMap::new();
        let mut schema_version: Option<(usize, i64)> = None;

        // Count all records and check schema/metadata consistency across the new shards
        for new_shard_id in 0..self.new_shard_count {
            let new_pool = self
                .create_readonly_pool(new_shard_id)
                .await
                .wrap_err(format!(
                    "Failed to connect read-only to new shard {}",
                    new_shard_id
                ))?;

            let version: i64 = sqlx::query_scalar("PRAGMA user_version")
                .fetch_one(&new_pool)
                .await
                .map_err(|e| {
                    sqlx_to_miette(
                        e,
                        &format!("Failed to read user_version for new shard {}", new_shard_id),
                    )
                })?;
            if !(0..=crate::shard_manager::SHARD_SCHEMA_VERSION).contains(&version) {
                new_pool.close().await;
                return Err(miette::miette!(
                    "Migration verification failed: shard {} has unsupported schema version v{}",
                    new_shard_id,
                    version
                ));
            }
            match schema_version {
                None => schema_version = Some((new_shard_id, version)),
                Some((first_shard, expected)) if expected != version => {
                    new_pool.close().await;
                    return Err(miette::miette!(
                        "Migration verification failed: mismatched schema versions (shard {} is v{}, shard {} is v{})",
                        first_shard,
                        expected,
                        new_shard_id,
                        version
                    ));
                }
                Some(_) => {}
            }

            let tables = self.get_all_tables(&new_pool).await.wrap_err(format!(
                "Failed to get tables from new shard {}",
                new_shard_id
            ))?;

            if version == crate::shard_manager::SHARD_SCHEMA_VERSION
                && let Err(err) = self
                    .validate_v1_namespace_metadata(&new_pool, new_shard_id, &tables)
                    .await
            {
                new_pool.close().await;
                return Err(err);
            }

            for table_name in tables {
                let query = format!("SELECT COUNT(*) as count FROM {}", table_name);
                let row = sqlx::query(&query)
                    .fetch_one(&new_pool)
                    .await
                    .map_err(|e| {
                        sqlx_to_miette(
                            e,
                            &format!(
                                "Failed to count records in table {} of new shard {}",
                                table_name, new_shard_id
                            ),
                        )
                    })?;

                let count: i64 = row.get("count");
                new_total_count += count as usize;
                *new_table_counts.entry(table_name).or_insert(0) += count as usize;
            }

            new_pool.close().await;
        }

        // Verify that each key in the new shards is in the correct location
        // by checking that it maps to the shard where it was found
        for new_shard_id in 0..self.new_shard_count {
            let new_pool = self
                .create_readonly_pool(new_shard_id)
                .await
                .wrap_err(format!(
                    "Failed to connect read-only to new shard {}",
                    new_shard_id
                ))?;

            let tables = self.get_all_tables(&new_pool).await.wrap_err(format!(
                "Failed to get tables from new shard {}",
                new_shard_id
            ))?;

            for table_name in tables {
                let query = format!("SELECT key FROM {}", table_name);
                let rows = sqlx::query(&query)
                    .fetch_all(&new_pool)
                    .await
                    .map_err(|e| {
                        sqlx_to_miette(
                            e,
                            &format!(
                                "Failed to fetch keys from table {} of new shard {}",
                                table_name, new_shard_id
                            ),
                        )
                    })?;

                for row in rows {
                    let key: String = row.get("key");
                    let expected_shard = self.get_new_shard(&key);

                    if expected_shard != new_shard_id {
                        return Err(miette::miette!(
                            "Migration verification failed: Key '{}' found in shard {} but should be in shard {}",
                            key,
                            new_shard_id,
                            expected_shard
                        ));
                    }
                }
            }

            new_pool.close().await;
        }

        tracing::info!("Migration verification passed!");
        tracing::info!("Total records after migration: {}", new_total_count);
        for (table_name, count) in new_table_counts.iter() {
            tracing::info!("  Table {}: {} records", table_name, count);
        }

        Ok(())
    }
}
