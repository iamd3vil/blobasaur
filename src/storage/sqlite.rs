use std::collections::HashSet;

use async_trait::async_trait;
use sqlx::{Sqlite, SqliteConnection, SqlitePool, pool::PoolConnection};

use super::{Storage, StorageError, StorageResult, StorageTransaction};

#[derive(Clone, Debug)]
pub struct SqliteStorage {
    pool: SqlitePool,
}

impl SqliteStorage {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

struct SqliteTxn {
    conn: Option<PoolConnection<Sqlite>>,
}

impl SqliteTxn {
    async fn new(pool: &SqlitePool) -> StorageResult<Box<dyn StorageTransaction>> {
        let mut conn = pool.acquire().await.map_err(StorageError::backend)?;

        sqlx::query("BEGIN IMMEDIATE")
            .execute(conn.as_mut())
            .await
            .map_err(StorageError::backend)?;

        Ok(Box::new(Self { conn: Some(conn) }))
    }

    fn table_name(namespace: &str) -> String {
        format!("blobs_{}", namespace)
    }

    fn conn(&mut self) -> StorageResult<&mut SqliteConnection> {
        self.conn
            .as_mut()
            .map(|conn| conn.as_mut())
            .ok_or_else(|| StorageError::backend("transaction already finished"))
    }
}

impl Drop for SqliteTxn {
    fn drop(&mut self) {
        if let Some(mut conn) = self.conn.take() {
            tokio::spawn(async move {
                if let Err(err) = sqlx::query("ROLLBACK").execute(conn.as_mut()).await {
                    tracing::error!("Failed to rollback sqlite transaction on drop: {}", err);
                }
            });
        }
    }
}

#[async_trait]
impl StorageTransaction for SqliteTxn {
    async fn upsert_blob(
        &mut self,
        key: &str,
        data: &[u8],
        expires_at: Option<i64>,
        now_ts: i64,
    ) -> StorageResult<()> {
        sqlx::query(
            "INSERT INTO blobs (key, data, created_at, updated_at, expires_at, version)
             VALUES (?, ?, ?, ?, ?, 0)
             ON CONFLICT(key) DO UPDATE SET
                 data = excluded.data,
                 updated_at = excluded.updated_at,
                 expires_at = excluded.expires_at,
                 version = blobs.version + 1",
        )
        .bind(key)
        .bind(data)
        .bind(now_ts)
        .bind(now_ts)
        .bind(expires_at)
        .execute(self.conn()?)
        .await
        .map_err(StorageError::backend)?;

        Ok(())
    }

    async fn delete_blob(&mut self, key: &str) -> StorageResult<()> {
        sqlx::query("DELETE FROM blobs WHERE key = ?")
            .bind(key)
            .execute(self.conn()?)
            .await
            .map_err(StorageError::backend)?;
        Ok(())
    }

    async fn set_blob_expiry(&mut self, key: &str, expires_at: i64) -> StorageResult<bool> {
        let result = sqlx::query("UPDATE blobs SET expires_at = ? WHERE key = ?")
            .bind(expires_at)
            .bind(key)
            .execute(self.conn()?)
            .await
            .map_err(StorageError::backend)?;
        Ok(result.rows_affected() > 0)
    }

    async fn ensure_namespace(&mut self, namespace: &str) -> StorageResult<()> {
        let table_name = Self::table_name(namespace);
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
            .execute(self.conn()?)
            .await
            .map_err(StorageError::backend)?;

        let index_query = format!(
            "CREATE INDEX IF NOT EXISTS idx_{}_expires_at ON {}(expires_at) WHERE expires_at IS NOT NULL",
            namespace, table_name
        );

        sqlx::query(&index_query)
            .execute(self.conn()?)
            .await
            .map_err(StorageError::backend)?;

        Ok(())
    }

    async fn upsert_namespace(
        &mut self,
        namespace: &str,
        key: &str,
        data: &[u8],
        expires_at: Option<i64>,
        now_ts: i64,
    ) -> StorageResult<()> {
        let table_name = Self::table_name(namespace);
        let query = format!(
            "INSERT INTO {} (key, data, created_at, updated_at, expires_at, version)
             VALUES (?, ?, ?, ?, ?, 0)
             ON CONFLICT(key) DO UPDATE SET
                data = excluded.data,
                updated_at = excluded.updated_at,
                expires_at = excluded.expires_at,
                version = {}.version + 1",
            table_name, table_name
        );

        sqlx::query(&query)
            .bind(key)
            .bind(data)
            .bind(now_ts)
            .bind(now_ts)
            .bind(expires_at)
            .execute(self.conn()?)
            .await
            .map_err(StorageError::backend)?;

        Ok(())
    }

    async fn delete_namespace(&mut self, namespace: &str, key: &str) -> StorageResult<()> {
        let table_name = Self::table_name(namespace);
        let query = format!("DELETE FROM {} WHERE key = ?", table_name);
        sqlx::query(&query)
            .bind(key)
            .execute(self.conn()?)
            .await
            .map_err(StorageError::backend)?;
        Ok(())
    }

    async fn commit(mut self: Box<Self>) -> StorageResult<()> {
        if let Some(mut conn) = self.conn.take() {
            sqlx::query("COMMIT")
                .execute(conn.as_mut())
                .await
                .map_err(StorageError::backend)?;
        }
        Ok(())
    }

    async fn rollback(mut self: Box<Self>) -> StorageResult<()> {
        if let Some(mut conn) = self.conn.take() {
            sqlx::query("ROLLBACK")
                .execute(conn.as_mut())
                .await
                .map_err(StorageError::backend)?;
        }
        Ok(())
    }
}

#[async_trait]
impl Storage for SqliteStorage {
    async fn ensure_base_schema(&self) -> StorageResult<()> {
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
        .execute(&self.pool)
        .await
        .map_err(StorageError::backend)?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_expires_at ON blobs(expires_at) WHERE expires_at IS NOT NULL",
        )
        .execute(&self.pool)
        .await
        .map_err(StorageError::backend)?;

        Ok(())
    }

    async fn begin_transaction(&self) -> StorageResult<Box<dyn StorageTransaction>> {
        SqliteTxn::new(&self.pool).await
    }

    async fn fetch_blob(&self, key: &str, now_ts: i64) -> StorageResult<Option<Vec<u8>>> {
        let row = sqlx::query_as::<_, (Vec<u8>,)>(
            "SELECT data FROM blobs WHERE key = ? AND (expires_at IS NULL OR expires_at > ?)",
        )
        .bind(key)
        .bind(now_ts)
        .fetch_optional(&self.pool)
        .await
        .map_err(StorageError::backend)?;
        Ok(row.map(|tuple| tuple.0))
    }

    async fn blob_exists(&self, key: &str) -> StorageResult<bool> {
        let exists = sqlx::query("SELECT 1 FROM blobs WHERE key = ?")
            .bind(key)
            .fetch_optional(&self.pool)
            .await
            .map_err(StorageError::backend)?
            .is_some();
        Ok(exists)
    }

    async fn blob_exists_with_expiry(&self, key: &str, now_ts: i64) -> StorageResult<bool> {
        let exists = sqlx::query(
            "SELECT 1 FROM blobs WHERE key = ? AND (expires_at IS NULL OR expires_at > ?)",
        )
        .bind(key)
        .bind(now_ts)
        .fetch_optional(&self.pool)
        .await
        .map_err(StorageError::backend)?
        .is_some();
        Ok(exists)
    }

    async fn fetch_ttl(&self, key: &str, now_ts: i64) -> StorageResult<Option<Option<i64>>> {
        let row = sqlx::query_as::<_, (Option<i64>,)>(
            "SELECT expires_at FROM blobs WHERE key = ? AND (expires_at IS NULL OR expires_at > ?)",
        )
        .bind(key)
        .bind(now_ts)
        .fetch_optional(&self.pool)
        .await
        .map_err(StorageError::backend)?;
        Ok(row.map(|tuple| tuple.0))
    }

    async fn fetch_namespace_blob(
        &self,
        namespace: &str,
        key: &str,
        now_ts: i64,
    ) -> StorageResult<Option<Vec<u8>>> {
        let table_name = format!("blobs_{}", namespace);
        let query = format!(
            "SELECT data FROM {} WHERE key = ? AND (expires_at IS NULL OR expires_at > ?)",
            table_name
        );

        let row = sqlx::query_as::<_, (Vec<u8>,)>(&query)
            .bind(key)
            .bind(now_ts)
            .fetch_optional(&self.pool)
            .await
            .map_err(StorageError::backend)?;
        Ok(row.map(|tuple| tuple.0))
    }

    async fn namespace_key_exists(&self, namespace: &str, key: &str) -> StorageResult<bool> {
        let table_name = format!("blobs_{}", namespace);
        let query = format!("SELECT 1 FROM {} WHERE key = ?", table_name);
        let exists = sqlx::query(&query)
            .bind(key)
            .fetch_optional(&self.pool)
            .await
            .map_err(StorageError::backend)?
            .is_some();
        Ok(exists)
    }

    async fn namespace_key_exists_with_expiry(
        &self,
        namespace: &str,
        key: &str,
        now_ts: i64,
    ) -> StorageResult<bool> {
        let table_name = format!("blobs_{}", namespace);
        let query = format!(
            "SELECT 1 FROM {} WHERE key = ? AND (expires_at IS NULL OR expires_at > ?)",
            table_name
        );
        let exists = sqlx::query(&query)
            .bind(key)
            .bind(now_ts)
            .fetch_optional(&self.pool)
            .await
            .map_err(StorageError::backend)?
            .is_some();
        Ok(exists)
    }

    async fn list_namespace_tables(&self) -> StorageResult<HashSet<String>> {
        let mut tables = HashSet::new();
        tables.insert("blobs".to_string());

        let rows = sqlx::query_as::<_, (String,)>(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'blobs_%'",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(StorageError::backend)?;

        for (name,) in rows {
            tables.insert(name);
        }

        Ok(tables)
    }

    async fn delete_expired_main(&self, now_ts: i64) -> StorageResult<u64> {
        let result =
            sqlx::query("DELETE FROM blobs WHERE expires_at IS NOT NULL AND expires_at <= ?")
                .bind(now_ts)
                .execute(&self.pool)
                .await
                .map_err(StorageError::backend)?;
        Ok(result.rows_affected())
    }

    async fn delete_expired_namespace(&self, namespace: &str, now_ts: i64) -> StorageResult<u64> {
        let table_name = format!("blobs_{}", namespace);
        let query = format!(
            "DELETE FROM {} WHERE expires_at IS NOT NULL AND expires_at <= ?",
            table_name
        );
        let result = sqlx::query(&query)
            .bind(now_ts)
            .execute(&self.pool)
            .await
            .map_err(StorageError::backend)?;
        Ok(result.rows_affected())
    }
}
