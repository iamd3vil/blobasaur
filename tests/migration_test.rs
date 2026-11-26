//! Tests for shard migration functionality
//!
//! These tests verify that the migration process correctly:
//! 1. Moves data from old shards to new shards based on consistent hashing
//! 2. Maintains data integrity during the migration
//! 3. Handles both regular and namespaced tables
//! 4. Properly calculates old and new shard assignments

use miette::Result;
use mpchash::HashRing;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode};
use sqlx::{Row, SqlitePool};

use std::str::FromStr;
use tempfile::TempDir;

#[derive(Hash)]
struct ShardNode(u64);

// Helper function to create a test database pool
async fn create_test_pool(path: &str) -> Result<SqlitePool, sqlx::Error> {
    // Use recommended SQLite settings even in tests
    // See: https://kerkour.com/sqlite-for-servers
    let connect_options = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .busy_timeout(std::time::Duration::from_millis(5000))
        .pragma("synchronous", "NORMAL") // NORMAL is safer than OFF
        .pragma("cache_size", "-1024000") // 1GB cache
        .pragma("temp_store", "MEMORY")
        .pragma("foreign_keys", "true");

    SqlitePool::connect_with(connect_options).await
}

async fn read_auto_vacuum_mode(path: &str) -> Result<i64, Box<dyn std::error::Error>> {
    let pool = create_test_pool(path).await?;
    let mode = sqlx::query_scalar::<_, i64>("PRAGMA auto_vacuum")
        .fetch_one(&pool)
        .await?;
    pool.close().await;
    Ok(mode)
}

// Helper function to simulate consistent hashing for a given shard count
fn get_shard_for_key(key: &str, shard_count: usize) -> usize {
    let ring = HashRing::new();
    for i in 0..shard_count {
        ring.add(ShardNode(i as u64));
    }
    let token = ring.node(&key).unwrap();
    token.node().0 as usize
}

// Helper function to create test data in old shards
async fn create_test_data(
    temp_dir: &TempDir,
    old_shard_count: usize,
    test_keys: &[&str],
) -> Result<(), Box<dyn std::error::Error>> {
    for shard_id in 0..old_shard_count {
        let db_path = temp_dir.path().join(format!("shard_{}.db", shard_id));
        let pool = create_test_pool(db_path.to_str().unwrap()).await?;

        // Create main blobs table
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
        .await?;

        // Create a namespaced table
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS blobs_users (
                key TEXT PRIMARY KEY,
                data BLOB,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                expires_at INTEGER,
                version INTEGER NOT NULL DEFAULT 0
            )",
        )
        .execute(&pool)
        .await?;

        // Insert test data for keys that belong to this shard
        for key in test_keys {
            let expected_shard = get_shard_for_key(key, old_shard_count);
            if expected_shard == shard_id {
                let timestamp = 1234567890;
                let data = format!("data_for_{}", key);

                // Insert into main table
                sqlx::query(
                    "INSERT INTO blobs (key, data, created_at, updated_at, version) VALUES (?, ?, ?, ?, ?)"
                )
                .bind(key)
                .bind(data.as_bytes())
                .bind(timestamp)
                .bind(timestamp)
                .bind(1)
                .execute(&pool)
                .await?;

                // Insert into namespaced table
                sqlx::query(
                    "INSERT INTO blobs_users (key, data, created_at, updated_at, version) VALUES (?, ?, ?, ?, ?)"
                )
                .bind(key)
                .bind(format!("user_{}", data).as_bytes())
                .bind(timestamp)
                .bind(timestamp)
                .bind(1)
                .execute(&pool)
                .await?;
            }
        }

        pool.close().await;
    }

    Ok(())
}

// Helper function to count records in all shards
async fn count_records_in_shards(
    temp_dir: &TempDir,
    shard_count: usize,
    table_name: &str,
) -> Result<usize, Box<dyn std::error::Error>> {
    let mut total_count = 0;

    for shard_id in 0..shard_count {
        let db_path = temp_dir.path().join(format!("shard_{}.db", shard_id));

        if !db_path.exists() {
            continue;
        }

        let pool = create_test_pool(db_path.to_str().unwrap()).await?;

        // Check if table exists
        let table_exists =
            sqlx::query("SELECT name FROM sqlite_master WHERE type='table' AND name = ?")
                .bind(table_name)
                .fetch_optional(&pool)
                .await?
                .is_some();

        if table_exists {
            let query = format!("SELECT COUNT(*) as count FROM {}", table_name);
            let row = sqlx::query(&query).fetch_one(&pool).await?;
            let count: i64 = row.get("count");
            total_count += count as usize;
        }

        pool.close().await;
    }

    Ok(total_count)
}

// Helper function to verify data integrity after migration
async fn verify_data_integrity(
    temp_dir: &TempDir,
    new_shard_count: usize,
    test_keys: &[&str],
    table_name: &str,
    data_prefix: &str,
) -> Result<bool, Box<dyn std::error::Error>> {
    for key in test_keys {
        let expected_shard = get_shard_for_key(key, new_shard_count);
        let db_path = temp_dir.path().join(format!("shard_{}.db", expected_shard));
        let pool = create_test_pool(db_path.to_str().unwrap()).await?;

        let query = format!("SELECT data FROM {} WHERE key = ?", table_name);
        let row = sqlx::query(&query).bind(key).fetch_optional(&pool).await?;

        if let Some(row) = row {
            let data: Vec<u8> = row.get("data");
            let expected_data = format!("{}{}", data_prefix, key);
            if data != expected_data.as_bytes() {
                pool.close().await;
                return Ok(false);
            }
        } else {
            pool.close().await;
            return Ok(false);
        }

        pool.close().await;
    }

    Ok(true)
}

#[tokio::test]
async fn test_hash_ring_consistency() {
    // Test that our hash ring calculation is consistent
    let key = "test_key";
    let shard_count = 4;

    let shard1 = get_shard_for_key(key, shard_count);
    let shard2 = get_shard_for_key(key, shard_count);

    assert_eq!(shard1, shard2, "Hash ring should be consistent");
}

#[tokio::test]
async fn test_migration_basic() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let old_shard_count = 2;
    let new_shard_count = 3;

    let test_keys = vec![
        "key1", "key2", "key3", "key4", "key5", "key6", "key7", "key8", "key9", "key10",
    ];

    // Create test data in old shards
    create_test_data(&temp_dir, old_shard_count, &test_keys).await?;

    // Verify old data exists
    let old_blobs_count = count_records_in_shards(&temp_dir, old_shard_count, "blobs").await?;
    let old_users_count =
        count_records_in_shards(&temp_dir, old_shard_count, "blobs_users").await?;

    assert_eq!(old_blobs_count, test_keys.len());
    assert_eq!(old_users_count, test_keys.len());

    // Run migration
    let migration_manager = blobasaur::migration::MigrationManager::new(
        old_shard_count,
        new_shard_count,
        temp_dir.path().to_str().unwrap().to_string(),
    )?;

    migration_manager.run_migration().await?;

    // Verify new data exists
    let new_blobs_count = count_records_in_shards(&temp_dir, new_shard_count, "blobs").await?;
    let new_users_count =
        count_records_in_shards(&temp_dir, new_shard_count, "blobs_users").await?;

    assert_eq!(new_blobs_count, test_keys.len());
    assert_eq!(new_users_count, test_keys.len());

    // Verify data integrity
    let blobs_integrity =
        verify_data_integrity(&temp_dir, new_shard_count, &test_keys, "blobs", "data_for_").await?;
    let users_integrity = verify_data_integrity(
        &temp_dir,
        new_shard_count,
        &test_keys,
        "blobs_users",
        "user_data_for_",
    )
    .await?;

    assert!(blobs_integrity, "Blobs data integrity check failed");
    assert!(users_integrity, "Users data integrity check failed");

    // Verify migration verification works
    migration_manager.verify_migration().await?;

    Ok(())
}

#[tokio::test]
async fn test_migration_creates_new_shards_with_incremental_auto_vacuum()
-> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let old_shard_count = 1;
    let new_shard_count = 2;
    let test_keys = vec!["key1", "key2", "key3"];

    create_test_data(&temp_dir, old_shard_count, &test_keys).await?;

    let newly_created_shard = temp_dir.path().join("shard_1.db");
    assert!(
        !newly_created_shard.exists(),
        "shard_1 should not exist pre-migration"
    );

    let migration_manager = blobasaur::migration::MigrationManager::new(
        old_shard_count,
        new_shard_count,
        temp_dir.path().to_str().unwrap().to_string(),
    )?;

    migration_manager.run_migration().await?;

    let mode = read_auto_vacuum_mode(newly_created_shard.to_str().unwrap()).await?;
    assert_eq!(
        mode, 2,
        "newly created shard DB should persist auto_vacuum=INCREMENTAL"
    );

    Ok(())
}

#[tokio::test]
async fn test_migration_scale_up() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let old_shard_count = 2;
    let new_shard_count = 4;

    let test_keys = vec![
        "user1", "user2", "user3", "user4", "user5", "user6", "user7", "user8",
    ];

    // Create test data
    create_test_data(&temp_dir, old_shard_count, &test_keys).await?;

    // Run migration
    let migration_manager = blobasaur::migration::MigrationManager::new(
        old_shard_count,
        new_shard_count,
        temp_dir.path().to_str().unwrap().to_string(),
    )?;

    migration_manager.run_migration().await?;

    // Verify all data migrated correctly
    let new_blobs_count = count_records_in_shards(&temp_dir, new_shard_count, "blobs").await?;
    assert_eq!(new_blobs_count, test_keys.len());

    // Verify each key is in the correct new shard
    for key in &test_keys {
        let expected_shard = get_shard_for_key(key, new_shard_count);
        let db_path = temp_dir.path().join(format!("shard_{}.db", expected_shard));
        let pool = create_test_pool(db_path.to_str().unwrap()).await?;

        let count_query = "SELECT COUNT(*) as count FROM blobs WHERE key = ?";
        let row = sqlx::query(count_query).bind(key).fetch_one(&pool).await?;
        let count: i64 = row.get("count");

        assert_eq!(
            count, 1,
            "Key {} should exist exactly once in shard {}",
            key, expected_shard
        );
        pool.close().await;
    }

    Ok(())
}

#[tokio::test]
async fn test_migration_scale_down() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let old_shard_count = 4;
    let new_shard_count = 2;

    let test_keys = vec![
        "item1", "item2", "item3", "item4", "item5", "item6", "item7", "item8",
    ];

    // Create test data
    create_test_data(&temp_dir, old_shard_count, &test_keys).await?;

    // Run migration
    let migration_manager = blobasaur::migration::MigrationManager::new(
        old_shard_count,
        new_shard_count,
        temp_dir.path().to_str().unwrap().to_string(),
    )?;

    migration_manager.run_migration().await?;

    // Verify all data migrated correctly
    let new_blobs_count = count_records_in_shards(&temp_dir, new_shard_count, "blobs").await?;
    assert_eq!(new_blobs_count, test_keys.len());

    // Verify no data remains in old shards (beyond the new shard count)
    let remaining_old_count = count_records_in_shards(&temp_dir, old_shard_count, "blobs").await?;
    assert_eq!(
        remaining_old_count,
        test_keys.len(),
        "Data should only exist in new shard structure"
    );

    // The drained shards are removed, sidecars included, so a server with the
    // new count starts.
    for shard_id in new_shard_count..old_shard_count {
        for suffix in ["", "-wal", "-shm", "-journal"] {
            let file = temp_dir.path().join(format!("shard_{shard_id}.db{suffix}"));
            assert!(!file.exists(), "{} should be removed", file.display());
        }
    }
    let cfg = blobasaur::config::Cfg {
        data_dir: temp_dir.path().to_str().unwrap().to_string(),
        num_shards: new_shard_count,
        storage_compression: None,
        async_write: Some(false),
        batch_size: None,
        batch_timeout_ms: None,
        write_queue_capacity: None,
        addr: None,
        cluster: None,
        metrics: None,
        sqlite: None,
        shutdown_timeout_secs: None,
        max_request_size_mb: None,
        cleanup_chunk_size: None,
        cleanup_interval_secs: None,
        new_replid: false,
        replication: None,
    };
    let mut receivers = Vec::new();
    blobasaur::app_state::AppState::new(cfg, &mut receivers)
        .await
        .map_err(|e| format!("server should start on the migrated data dir: {e:?}"))?;

    Ok(())
}

#[tokio::test]
async fn test_migration_with_empty_shards() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let old_shard_count = 3;
    let new_shard_count = 2;

    // Create only some shards with data
    let test_keys = vec!["key1", "key2"];

    // Create test data (some shards may be empty)
    create_test_data(&temp_dir, old_shard_count, &test_keys).await?;

    // Run migration
    let migration_manager = blobasaur::migration::MigrationManager::new(
        old_shard_count,
        new_shard_count,
        temp_dir.path().to_str().unwrap().to_string(),
    )?;

    migration_manager.run_migration().await?;

    // Verify all data migrated correctly
    let new_blobs_count = count_records_in_shards(&temp_dir, new_shard_count, "blobs").await?;
    assert_eq!(new_blobs_count, test_keys.len());

    Ok(())
}

#[tokio::test]
async fn test_migration_verification_failure() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let old_shard_count = 2;
    let new_shard_count = 3;

    let test_keys = vec!["key1", "key2"];

    // Create test data
    create_test_data(&temp_dir, old_shard_count, &test_keys).await?;

    // Run migration
    let migration_manager = blobasaur::migration::MigrationManager::new(
        old_shard_count,
        new_shard_count,
        temp_dir.path().to_str().unwrap().to_string(),
    )?;

    migration_manager.run_migration().await?;

    // Manually corrupt data to test verification failure
    // Instead of deleting data, we'll move a key to the wrong shard
    // to test that verification catches keys in incorrect locations
    let key_to_corrupt = "key1";
    let expected_shard = get_shard_for_key(key_to_corrupt, new_shard_count);
    let wrong_shard = if expected_shard == 0 { 1 } else { 0 };

    // Move the key to the wrong shard
    let correct_shard_path = temp_dir.path().join(format!("shard_{}.db", expected_shard));
    let wrong_shard_path = temp_dir.path().join(format!("shard_{}.db", wrong_shard));

    if correct_shard_path.exists() && wrong_shard_path.exists() {
        let correct_pool = create_test_pool(correct_shard_path.to_str().unwrap()).await?;
        let wrong_pool = create_test_pool(wrong_shard_path.to_str().unwrap()).await?;

        // Get the record from the correct shard
        let row = sqlx::query("SELECT key, data, created_at, updated_at, expires_at, version FROM blobs WHERE key = ?")
            .bind(key_to_corrupt)
            .fetch_optional(&correct_pool)
            .await?;

        if let Some(row) = row {
            let key: String = row.get("key");
            let data: Vec<u8> = row.get("data");
            let created_at: i64 = row.get("created_at");
            let updated_at: i64 = row.get("updated_at");
            let expires_at: Option<i64> = row.get("expires_at");
            let version: i64 = row.get("version");

            // Insert it into the wrong shard
            sqlx::query("INSERT OR REPLACE INTO blobs (key, data, created_at, updated_at, expires_at, version) VALUES (?, ?, ?, ?, ?, ?)")
                .bind(&key)
                .bind(&data)
                .bind(created_at)
                .bind(updated_at)
                .bind(expires_at)
                .bind(version)
                .execute(&wrong_pool)
                .await?;

            // Delete it from the correct shard
            sqlx::query("DELETE FROM blobs WHERE key = ?")
                .bind(key_to_corrupt)
                .execute(&correct_pool)
                .await?;
        }

        correct_pool.close().await;
        wrong_pool.close().await;
    }

    // Verification should fail now
    let verification_result = migration_manager.verify_migration().await;
    assert!(
        verification_result.is_err(),
        "Verification should fail with corrupted data"
    );

    Ok(())
}

#[tokio::test]
async fn test_migration_consistent_hashing() -> Result<(), Box<dyn std::error::Error>> {
    // Test that keys distribute differently between different shard counts
    let keys = ["key1", "key2", "key3", "key4", "key5"];

    let old_distribution: Vec<usize> = keys.iter().map(|k| get_shard_for_key(k, 2)).collect();
    let new_distribution: Vec<usize> = keys.iter().map(|k| get_shard_for_key(k, 3)).collect();

    // Some keys should move to different shards
    let mut moved_keys = 0;
    for i in 0..keys.len() {
        if old_distribution[i] != new_distribution[i] {
            moved_keys += 1;
        }
    }

    // With consistent hashing, not all keys should move
    assert!(
        moved_keys > 0,
        "At least some keys should move between configurations"
    );
    assert!(
        moved_keys < keys.len(),
        "Not all keys should move (consistent hashing benefit)"
    );

    Ok(())
}

#[tokio::test]
async fn test_migration_manager_validation() {
    // Test invalid shard counts
    let result = blobasaur::migration::MigrationManager::new(0, 3, "/tmp".to_string());
    assert!(result.is_err(), "Should fail with zero old shard count");

    let result = blobasaur::migration::MigrationManager::new(3, 0, "/tmp".to_string());
    assert!(result.is_err(), "Should fail with zero new shard count");

    let result = blobasaur::migration::MigrationManager::new(3, 3, "/tmp".to_string());
    assert!(result.is_err(), "Should fail with same shard counts");

    // Test valid configuration
    let result = blobasaur::migration::MigrationManager::new(2, 3, "/tmp".to_string());
    assert!(result.is_ok(), "Should succeed with valid configuration");
}

#[tokio::test]
async fn test_migration_preserves_schema_version_and_blob_namespaces()
-> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let old_shard_count = 2;
    let new_shard_count = 3;
    let test_keys = ["k1", "k2", "k3", "k4", "k5", "k6", "k7", "k8"];

    let users_table = blobasaur::shard_manager::namespace_table("users");
    let upper_users_table = blobasaur::shard_manager::namespace_table("Users");

    for shard_id in 0..old_shard_count {
        let db_path = temp_dir.path().join(format!("shard_{}.db", shard_id));
        let pool = create_test_pool(db_path.to_str().unwrap()).await?;

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
        .await?;
        sqlx::query(blobasaur::shard_manager::CREATE_BLOB_NAMESPACES_TABLE)
            .execute(&pool)
            .await?;
        sqlx::query(&format!(
            "PRAGMA user_version = {}",
            blobasaur::shard_manager::SHARD_SCHEMA_VERSION
        ))
        .execute(&pool)
        .await?;

        for (ns, table) in [("users", &users_table), ("Users", &upper_users_table)] {
            sqlx::query(&format!(
                "CREATE TABLE IF NOT EXISTS {table} (
                    key TEXT PRIMARY KEY,
                    data BLOB,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    expires_at INTEGER,
                    version INTEGER NOT NULL DEFAULT 0
                )"
            ))
            .execute(&pool)
            .await?;
            sqlx::query(
                "INSERT OR IGNORE INTO blob_namespaces (namespace, table_name) VALUES (?, ?)",
            )
            .bind(ns)
            .bind(table)
            .execute(&pool)
            .await?;
        }

        for key in &test_keys {
            if get_shard_for_key(key, old_shard_count) == shard_id {
                sqlx::query(&format!(
                    "INSERT INTO {users_table} (key, data, created_at, updated_at, version) VALUES (?, ?, 0, 0, 1)"
                ))
                .bind(key)
                .bind(format!("lower_{key}").as_bytes())
                .execute(&pool)
                .await?;
                sqlx::query(&format!(
                    "INSERT INTO {upper_users_table} (key, data, created_at, updated_at, version) VALUES (?, ?, 0, 0, 1)"
                ))
                .bind(key)
                .bind(format!("upper_{key}").as_bytes())
                .execute(&pool)
                .await?;
            }
        }

        pool.close().await;
    }

    let migration_manager = blobasaur::migration::MigrationManager::new(
        old_shard_count,
        new_shard_count,
        temp_dir.path().to_str().unwrap().to_string(),
    )?;
    migration_manager.run_migration().await?;
    migration_manager.verify_migration().await?;

    // All shards (including the newly created shard_2) must keep user_version = 1.
    for shard_id in 0..new_shard_count {
        let db_path = temp_dir.path().join(format!("shard_{}.db", shard_id));
        let pool = create_test_pool(db_path.to_str().unwrap()).await?;
        let version: i64 = sqlx::query_scalar("PRAGMA user_version")
            .fetch_one(&pool)
            .await?;
        assert_eq!(version, blobasaur::shard_manager::SHARD_SCHEMA_VERSION);

        // Any namespaced table present on the shard must map to its exact expected namespace.
        let tables: Vec<(String,)> = sqlx::query_as(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'blobs\\_%' ESCAPE '\\'",
        )
        .fetch_all(&pool)
        .await?;
        for (table_name,) in tables {
            let mapped: Option<(String,)> =
                sqlx::query_as("SELECT namespace FROM blob_namespaces WHERE table_name = ?")
                    .bind(&table_name)
                    .fetch_optional(&pool)
                    .await?;
            let expected_ns = if table_name == users_table {
                "users"
            } else if table_name == upper_users_table {
                "Users"
            } else {
                panic!("unexpected table {table_name}");
            };
            assert_eq!(
                mapped.as_ref().map(|(ns,)| ns.as_str()),
                Some(expected_ns),
                "shard {shard_id} wrong blob_namespaces entry for {table_name}"
            );
        }
        pool.close().await;
    }

    assert!(
        verify_data_integrity(
            &temp_dir,
            new_shard_count,
            &test_keys,
            &users_table,
            "lower_"
        )
        .await?
    );
    assert!(
        verify_data_integrity(
            &temp_dir,
            new_shard_count,
            &test_keys,
            &upper_users_table,
            "upper_"
        )
        .await?
    );

    Ok(())
}

#[tokio::test]
async fn test_migration_rejects_mismatched_destination_future_version_and_ambiguous_v0()
-> Result<(), Box<dyn std::error::Error>> {
    // 1. Existing v1 destination shard when resharding v0 sources must be rejected before modifying files.
    {
        let temp_dir = TempDir::new()?;
        create_test_data(&temp_dir, 2, &["k1", "k2", "k3", "k4"]).await?;
        let dest_path = temp_dir.path().join("shard_2.db");
        let dest_pool = create_test_pool(dest_path.to_str().unwrap()).await?;
        sqlx::query(&format!(
            "PRAGMA user_version = {}",
            blobasaur::shard_manager::SHARD_SCHEMA_VERSION
        ))
        .execute(&dest_pool)
        .await?;
        dest_pool.close().await;

        let mgr = blobasaur::migration::MigrationManager::new(
            2,
            3,
            temp_dir.path().to_str().unwrap().to_string(),
        )?;
        let err = mgr
            .run_migration()
            .await
            .expect_err("v0 sources + existing v1 destination must fail");
        assert!(err.to_string().contains("mismatched schema versions"));
        // Source rows must be untouched.
        assert_eq!(
            count_records_in_shards(&temp_dir, 2, "blobs_users").await?,
            4
        );
    }

    // 2. Unsupported future schema version must be rejected.
    {
        let temp_dir = TempDir::new()?;
        create_test_data(&temp_dir, 2, &["k1", "k2"]).await?;
        for shard_id in 0..2 {
            let db_path = temp_dir.path().join(format!("shard_{shard_id}.db"));
            let pool = create_test_pool(db_path.to_str().unwrap()).await?;
            sqlx::query(&format!(
                "PRAGMA user_version = {}",
                blobasaur::shard_manager::SHARD_SCHEMA_VERSION + 1
            ))
            .execute(&pool)
            .await?;
            pool.close().await;
        }

        let mgr = blobasaur::migration::MigrationManager::new(
            2,
            3,
            temp_dir.path().to_str().unwrap().to_string(),
        )?;
        let err = mgr
            .run_migration()
            .await
            .expect_err("future schema version must fail");
        assert!(err.to_string().contains("unsupported schema version"));
    }

    // 3. Ambiguous v0 spellings across shards (`blobs_Users` vs `blobs_users`) must be rejected.
    {
        let temp_dir = TempDir::new()?;
        for (shard_id, table) in [(0, "blobs_Users"), (1, "blobs_users")] {
            let db_path = temp_dir.path().join(format!("shard_{shard_id}.db"));
            let pool = create_test_pool(db_path.to_str().unwrap()).await?;
            sqlx::query(
                "CREATE TABLE blobs (key TEXT PRIMARY KEY, data BLOB, created_at INTEGER NOT NULL,
                 updated_at INTEGER NOT NULL, expires_at INTEGER, version INTEGER NOT NULL DEFAULT 0)",
            )
            .execute(&pool)
            .await?;
            sqlx::query(&format!(
                "CREATE TABLE {table} (key TEXT PRIMARY KEY, data BLOB, created_at INTEGER NOT NULL,
                 updated_at INTEGER NOT NULL, expires_at INTEGER, version INTEGER NOT NULL DEFAULT 0)"
            ))
            .execute(&pool)
            .await?;
            pool.close().await;
        }

        let mgr = blobasaur::migration::MigrationManager::new(
            2,
            3,
            temp_dir.path().to_str().unwrap().to_string(),
        )?;
        let err = mgr
            .run_migration()
            .await
            .expect_err("ambiguous v0 namespace spellings across shards must fail");
        assert!(
            err.to_string()
                .contains("Ambiguous legacy namespace tables")
        );
    }

    // 4. v1 shard missing blob_namespaces entry for a namespaced table must fail preflight AND verify_migration independently.
    {
        let temp_dir = TempDir::new()?;
        let users_table = blobasaur::shard_manager::namespace_table("users");
        for shard_id in 0..2 {
            let db_path = temp_dir.path().join(format!("shard_{shard_id}.db"));
            let pool = create_test_pool(db_path.to_str().unwrap()).await?;
            sqlx::query(
                "CREATE TABLE blobs (key TEXT PRIMARY KEY, data BLOB, created_at INTEGER NOT NULL,
                 updated_at INTEGER NOT NULL, expires_at INTEGER, version INTEGER NOT NULL DEFAULT 0)",
            )
            .execute(&pool)
            .await?;
            sqlx::query(blobasaur::shard_manager::CREATE_BLOB_NAMESPACES_TABLE)
                .execute(&pool)
                .await?;
            sqlx::query(&format!(
                "CREATE TABLE {users_table} (key TEXT PRIMARY KEY, data BLOB, created_at INTEGER NOT NULL,
                 updated_at INTEGER NOT NULL, expires_at INTEGER, version INTEGER NOT NULL DEFAULT 0)"
            ))
            .execute(&pool)
            .await?;
            sqlx::query(&format!(
                "PRAGMA user_version = {}",
                blobasaur::shard_manager::SHARD_SCHEMA_VERSION
            ))
            .execute(&pool)
            .await?;
            pool.close().await;
        }

        let mgr = blobasaur::migration::MigrationManager::new(
            2,
            3,
            temp_dir.path().to_str().unwrap().to_string(),
        )?;
        let err = mgr
            .run_migration()
            .await
            .expect_err("v1 shard missing blob_namespaces entry must fail preflight");
        assert!(err.to_string().contains("Missing blob_namespaces entry"));

        let verify_mgr = blobasaur::migration::MigrationManager::new(
            1,
            2,
            temp_dir.path().to_str().unwrap().to_string(),
        )?;
        let verify_err = verify_mgr
            .verify_migration()
            .await
            .expect_err("verify_migration must independently reject missing blob_namespaces entry");
        assert!(
            verify_err
                .to_string()
                .contains("Missing blob_namespaces entry")
        );
    }

    // 5. Orphan blob_namespaces entry whose physical table was dropped must fail verify_migration.
    {
        let temp_dir = TempDir::new()?;
        let users_table = blobasaur::shard_manager::namespace_table("users");
        for shard_id in 0..2 {
            let db_path = temp_dir.path().join(format!("shard_{shard_id}.db"));
            let pool = create_test_pool(db_path.to_str().unwrap()).await?;
            sqlx::query(
                "CREATE TABLE blobs (key TEXT PRIMARY KEY, data BLOB, created_at INTEGER NOT NULL,
                 updated_at INTEGER NOT NULL, expires_at INTEGER, version INTEGER NOT NULL DEFAULT 0)",
            )
            .execute(&pool)
            .await?;
            sqlx::query(blobasaur::shard_manager::CREATE_BLOB_NAMESPACES_TABLE)
                .execute(&pool)
                .await?;
            sqlx::query("INSERT INTO blob_namespaces (namespace, table_name) VALUES ('users', ?)")
                .bind(&users_table)
                .execute(&pool)
                .await?;
            sqlx::query(&format!(
                "PRAGMA user_version = {}",
                blobasaur::shard_manager::SHARD_SCHEMA_VERSION
            ))
            .execute(&pool)
            .await?;
            pool.close().await;
        }

        let verify_mgr = blobasaur::migration::MigrationManager::new(
            1,
            2,
            temp_dir.path().to_str().unwrap().to_string(),
        )?;
        let verify_err = verify_mgr
            .verify_migration()
            .await
            .expect_err("verify_migration must reject orphan blob_namespaces entry");
        assert!(
            verify_err
                .to_string()
                .contains("Orphan blob_namespaces entry")
        );
    }

    // 6. Failed preflight must not switch a rollback-journal shard into WAL mode.
    {
        let temp_dir = TempDir::new()?;
        for (shard_id, version) in [
            (0, 0i64),
            (1, blobasaur::shard_manager::SHARD_SCHEMA_VERSION),
        ] {
            let db_path = temp_dir.path().join(format!("shard_{shard_id}.db"));
            let opts = SqliteConnectOptions::from_str(&format!("sqlite:{}", db_path.display()))?
                .create_if_missing(true)
                .journal_mode(SqliteJournalMode::Delete);
            let pool = SqlitePool::connect_with(opts).await?;
            sqlx::query(
                "CREATE TABLE blobs (key TEXT PRIMARY KEY, data BLOB, created_at INTEGER NOT NULL,
                 updated_at INTEGER NOT NULL, expires_at INTEGER, version INTEGER NOT NULL DEFAULT 0)",
            )
            .execute(&pool)
            .await?;
            sqlx::query(&format!("PRAGMA user_version = {version}"))
                .execute(&pool)
                .await?;
            pool.close().await;
        }

        let mgr = blobasaur::migration::MigrationManager::new(
            2,
            3,
            temp_dir.path().to_str().unwrap().to_string(),
        )?;
        assert!(mgr.run_migration().await.is_err());

        let db0 = temp_dir.path().join("shard_0.db");
        let ro_opts =
            SqliteConnectOptions::from_str(&format!("sqlite:{}", db0.display()))?.read_only(true);
        let ro_pool = SqlitePool::connect_with(ro_opts).await?;
        let journal_mode: String = sqlx::query_scalar("PRAGMA journal_mode")
            .fetch_one(&ro_pool)
            .await?;
        assert_eq!(journal_mode.to_ascii_lowercase(), "delete");
        ro_pool.close().await;
    }

    // 7. Equivalent v0 prefixes (`BLOBS_Users` and `blobs_Users`) have the same suffix `Users`
    // and must NOT be rejected as ambiguous.
    {
        let temp_dir = TempDir::new()?;
        for (shard_id, table) in [(0, "BLOBS_Users"), (1, "blobs_Users")] {
            let db_path = temp_dir.path().join(format!("shard_{shard_id}.db"));
            let pool = create_test_pool(db_path.to_str().unwrap()).await?;
            sqlx::query(
                "CREATE TABLE blobs (key TEXT PRIMARY KEY, data BLOB, created_at INTEGER NOT NULL,
                 updated_at INTEGER NOT NULL, expires_at INTEGER, version INTEGER NOT NULL DEFAULT 0)",
            )
            .execute(&pool)
            .await?;
            sqlx::query(&format!(
                "CREATE TABLE {table} (key TEXT PRIMARY KEY, data BLOB, created_at INTEGER NOT NULL,
                 updated_at INTEGER NOT NULL, expires_at INTEGER, version INTEGER NOT NULL DEFAULT 0)"
            ))
            .execute(&pool)
            .await?;
            pool.close().await;
        }

        let mgr = blobasaur::migration::MigrationManager::new(
            2,
            3,
            temp_dir.path().to_str().unwrap().to_string(),
        )?;
        mgr.run_migration().await?;
    }

    // 8. Concurrent resharding on the same data_dir is rejected by the migration lockfile.
    {
        let temp_dir = TempDir::new()?;
        create_test_data(&temp_dir, 2, &["k1", "k2"]).await?;
        let lock_path = temp_dir.path().join(".blobasaur_migration.lock");
        let lock_file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)?;
        lock_file.try_lock()?;

        let mgr = blobasaur::migration::MigrationManager::new(
            2,
            3,
            temp_dir.path().to_str().unwrap().to_string(),
        )?;
        let err = mgr
            .run_migration()
            .await
            .expect_err("locked data_dir must reject concurrent migration");
        assert!(
            err.to_string()
                .contains("Another shard migration is already running")
        );
        drop(lock_file);
    }

    Ok(())
}

#[tokio::test]
async fn shrink_uses_literal_directory() -> Result<(), Box<dyn std::error::Error>> {
    let root = TempDir::new()?;
    let literal = tempfile::Builder::new()
        .prefix("copy%20here")
        .rand_bytes(0)
        .tempdir_in(&root)?;
    let sibling = tempfile::Builder::new()
        .prefix("copy here")
        .rand_bytes(0)
        .tempdir_in(&root)?;
    create_test_data(&literal, 3, &["literal"]).await?;
    create_test_data(&sibling, 3, &["sibling"]).await?;

    let mgr = blobasaur::MigrationManager::new(3, 1, literal.path().display().to_string())?;
    mgr.run_migration_with_verify(true).await?;
    assert!(verify_data_integrity(&literal, 1, &["literal"], "blobs", "data_for_").await?);
    assert!(verify_data_integrity(&sibling, 3, &["sibling"], "blobs", "data_for_").await?);
    Ok(())
}

#[tokio::test]
async fn shrink_retries_after_cleanup_lock() -> Result<(), Box<dyn std::error::Error>> {
    let dir = TempDir::new()?;
    create_test_data(&dir, 3, &["a", "b", "c"]).await?;
    let path = dir.path().join("shard_2.db");
    let held = create_test_pool(path.to_str().unwrap()).await?;
    let mgr = blobasaur::MigrationManager::new(3, 1, dir.path().display().to_string())?;
    let err = mgr.run_migration().await.unwrap_err();
    assert!(err.to_string().contains("locked"), "{err}");
    assert!(!dir.path().join("shard_1.db").exists());
    held.close().await;
    let other = blobasaur::MigrationManager::new(3, 2, dir.path().display().to_string())?;
    let err = other.run_migration().await.unwrap_err();
    assert!(err.to_string().contains("shard migrate 3 1"), "{err}");

    mgr.run_migration_with_verify(true).await?;
    assert!(!path.exists());
    assert!(!dir.path().join("shard_1.db").exists());
    assert!(verify_data_integrity(&dir, 1, &["a", "b", "c"], "blobs", "data_for_").await?);
    Ok(())
}

#[tokio::test]
async fn shrink_requires_all_sources_before_cleanup() -> Result<(), Box<dyn std::error::Error>> {
    let dir = TempDir::new()?;
    create_test_data(&dir, 2, &["a"]).await?;
    let mgr = blobasaur::MigrationManager::new(3, 1, dir.path().display().to_string())?;
    let err = mgr.run_migration().await.unwrap_err();
    assert!(
        err.to_string().contains("shard_2.db does not exist"),
        "{err}"
    );
    assert!(verify_data_integrity(&dir, 2, &["a"], "blobs", "data_for_").await?);
    Ok(())
}
