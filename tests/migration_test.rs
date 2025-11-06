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
    let connect_options = SqliteConnectOptions::from_str(&format!("sqlite:{}", path))?
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .busy_timeout(std::time::Duration::from_millis(5000))
        .pragma("synchronous", "NORMAL") // NORMAL is safer than OFF
        .pragma("cache_size", "-1024000") // 1GB cache
        .pragma("temp_store", "MEMORY")
        .pragma("foreign_keys", "true");

    SqlitePool::connect_with(connect_options).await
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
    let keys = vec!["key1", "key2", "key3", "key4", "key5"];

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
