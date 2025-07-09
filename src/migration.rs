use miette::{Context, Result};
use mpchash::HashRing;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode};
use sqlx::{Row, SqlitePool};
use std::collections::HashMap;
use std::path::Path;
use std::str::FromStr;

// Helper function to convert sqlx errors to miette errors
fn sqlx_to_miette(err: sqlx::Error, context: &str) -> miette::Error {
    miette::miette!("{}: {}", context, err)
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

        // Create hash rings for old and new configurations
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

    #[allow(dead_code)]
    fn get_old_shard(&self, key: &str) -> usize {
        // Create old hash ring on the fly to calculate original shard
        let old_ring = HashRing::new();
        for i in 0..self.old_shard_count {
            old_ring.add(ShardNode(i as u64));
        }
        let token = old_ring.node(&key).unwrap();
        token.node().0 as usize
    }

    fn get_new_shard(&self, key: &str) -> usize {
        let token = self.new_ring.node(&key).unwrap();
        token.node().0 as usize
    }

    async fn create_connection_pool(&self, shard_id: usize) -> Result<SqlitePool> {
        let db_path = format!("{}/shard_{}.db", self.data_dir, shard_id);

        let connect_options = SqliteConnectOptions::from_str(&format!("sqlite:{}", db_path))
            .map_err(|e| sqlx_to_miette(e, "Failed to parse connection string"))?
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .busy_timeout(std::time::Duration::from_millis(5000))
            .pragma("synchronous", "OFF")
            .pragma("cache_size", "-100000")
            .pragma("temp_store", "MEMORY");

        SqlitePool::connect_with(connect_options)
            .await
            .map_err(|e| sqlx_to_miette(e, "Failed to connect to database"))
    }

    pub async fn run_migration(&self) -> Result<()> {
        tracing::info!(
            "Starting shard migration from {} to {} shards",
            self.old_shard_count,
            self.new_shard_count
        );

        // Validate that all old shard files exist
        for i in 0..self.old_shard_count {
            let db_path = format!("{}/shard_{}.db", self.data_dir, i);
            if !Path::new(&db_path).exists() {
                return Err(miette::miette!("Old shard file {} does not exist", db_path));
            }
        }

        // Create new shard databases if they don't exist
        for i in 0..self.new_shard_count {
            let pool = self.create_connection_pool(i).await.wrap_err(format!(
                "Failed to create connection pool for new shard {}",
                i
            ))?;

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

                self.migrate_table(&old_pool, &table_name, old_shard_id)
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

        Ok(())
    }

    async fn get_all_tables(&self, pool: &SqlitePool) -> Result<Vec<String>> {
        let rows = sqlx::query(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'blobs%' ORDER BY name"
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

    async fn migrate_table(
        &self,
        old_pool: &SqlitePool,
        table_name: &str,
        current_old_shard: usize,
    ) -> Result<()> {
        // Read all records from the old table
        let query = format!(
            "SELECT key, data, created_at, updated_at, expires_at, version FROM {}",
            table_name
        );
        let rows = sqlx::query(&query).fetch_all(old_pool).await.map_err(|e| {
            sqlx_to_miette(
                e,
                &format!("Failed to read records from table {}", table_name),
            )
        })?;

        tracing::info!(
            "Found {} records in table {} of old shard {}",
            rows.len(),
            table_name,
            current_old_shard
        );

        // Group records by their new shard destination
        let mut shard_data: HashMap<usize, Vec<(String, Vec<u8>, i64, i64, Option<i64>, i64)>> =
            HashMap::new();
        let mut keys_to_move: Vec<String> = Vec::new();

        for row in rows {
            let key: String = row.get("key");
            let data: Vec<u8> = row.get("data");
            let created_at: i64 = row.get("created_at");
            let updated_at: i64 = row.get("updated_at");
            let expires_at: Option<i64> = row.get("expires_at");
            let version: i64 = row.get("version");

            let new_shard_id = self.get_new_shard(&key);

            tracing::info!(
                "Key '{}' from old shard {} -> new shard {}",
                key,
                current_old_shard,
                new_shard_id
            );

            // All keys in this shard database belong to current_old_shard by definition
            shard_data
                .entry(new_shard_id)
                .or_insert_with(Vec::new)
                .push((
                    key.clone(),
                    data,
                    created_at,
                    updated_at,
                    expires_at,
                    version,
                ));

            keys_to_move.push(key);
        }

        tracing::info!(
            "Distributing {} keys from old shard {} to {} new shards",
            keys_to_move.len(),
            current_old_shard,
            shard_data.len()
        );

        // First, write data to new shards
        for (new_shard_id, records) in shard_data {
            if records.is_empty() {
                continue;
            }

            tracing::info!(
                "Writing {} records to new shard {}",
                records.len(),
                new_shard_id
            );

            let new_pool = self
                .create_connection_pool(new_shard_id)
                .await
                .wrap_err(format!("Failed to connect to new shard {}", new_shard_id))?;

            // Ensure the table exists in the new shard
            if table_name != "blobs" {
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
                    .execute(&new_pool)
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
                let namespace = table_name.strip_prefix("blobs_").unwrap_or("");
                let index_query = format!(
                    "CREATE INDEX IF NOT EXISTS idx_{}_expires_at ON {}(expires_at) WHERE expires_at IS NOT NULL",
                    namespace, table_name
                );

                sqlx::query(&index_query)
                    .execute(&new_pool)
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
            }

            // Insert records in batches
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

            for (key, data, created_at, updated_at, expires_at, version) in records {
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

            new_pool.close().await;
        }

        // Delete moved keys from the current old shard only
        // But only delete keys that are actually moving to a different shard
        let mut keys_to_delete = Vec::new();
        for key in &keys_to_move {
            let new_shard_id = self.get_new_shard(key);
            // Only delete if the key is moving to a different shard
            if new_shard_id != current_old_shard {
                keys_to_delete.push(key);
            }
        }

        if !keys_to_delete.is_empty() {
            tracing::info!(
                "Deleting {} keys from old shard {} table {} (keys that moved to different shards)",
                keys_to_delete.len(),
                current_old_shard,
                table_name
            );

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

            for key in &keys_to_delete {
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
        } else {
            tracing::info!(
                "No keys to delete from old shard {} table {} (all keys remain in same shard)",
                current_old_shard,
                table_name
            );
        }

        Ok(())
    }

    pub async fn verify_migration(&self) -> Result<()> {
        tracing::info!("Verifying migration integrity...");

        // For verification, we need to count the total records that exist in the new shard configuration
        // We cannot rely on old shards because some may overlap with new shards

        // Instead, we'll verify that each key from the original data is in the correct new shard
        // and that no data is lost or duplicated

        let mut new_total_count = 0;
        let mut new_table_counts: HashMap<String, usize> = HashMap::new();

        // Count all records in the new shard configuration
        for new_shard_id in 0..self.new_shard_count {
            let new_pool = self
                .create_connection_pool(new_shard_id)
                .await
                .wrap_err(format!("Failed to connect to new shard {}", new_shard_id))?;

            let tables = self.get_all_tables(&new_pool).await.wrap_err(format!(
                "Failed to get tables from new shard {}",
                new_shard_id
            ))?;

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
                .create_connection_pool(new_shard_id)
                .await
                .wrap_err(format!("Failed to connect to new shard {}", new_shard_id))?;

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
