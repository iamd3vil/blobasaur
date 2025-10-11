use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use turso::params::{self, Params};
use turso::value::Value;
use turso::{Builder, Connection, Database, Row, Rows};

use super::{Storage, StorageError, StorageResult, StorageTransaction};

const DEFAULT_MAX_CONNECTIONS: usize = 32;

#[derive(Debug, Clone)]
pub struct TursoStorage {
    db: Arc<Database>,
    semaphore: Arc<Semaphore>,
}

impl TursoStorage {
    pub async fn new(path: &str) -> StorageResult<Self> {
        let db = Builder::new_local(path)
            .build()
            .await
            .map_err(StorageError::backend)?;

        Ok(Self {
            db: Arc::new(db),
            semaphore: Arc::new(Semaphore::new(DEFAULT_MAX_CONNECTIONS)),
        })
    }

    async fn connection(&self) -> StorageResult<(Connection, OwnedSemaphorePermit)> {
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| StorageError::backend("connection semaphore closed"))?;

        let conn = self.db.connect().map_err(StorageError::backend)?;
        Ok((conn, permit))
    }

    async fn execute_with_changes(
        &self,
        conn: &mut Connection,
        query: &str,
        params: Params,
    ) -> StorageResult<u64> {
        conn.execute(query, params)
            .await
            .map_err(StorageError::backend)?;

        let mut rows = conn
            .query("SELECT changes()", empty_params())
            .await
            .map_err(StorageError::backend)?;

        match next_value(&mut rows).await? {
            Some(Value::Integer(changes)) => Ok(changes as u64),
            Some(Value::Null) | None => Ok(0),
            Some(other) => Err(StorageError::backend(format!(
                "unexpected value from changes(): {:?}",
                other
            ))),
        }
    }
}

#[async_trait]
impl Storage for TursoStorage {
    async fn ensure_base_schema(&self) -> StorageResult<()> {
        let (conn, _permit) = self.connection().await?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS blobs (
                key TEXT PRIMARY KEY,
                data BLOB,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                expires_at INTEGER,
                version INTEGER NOT NULL DEFAULT 0
            )",
            empty_params(),
        )
        .await
        .map_err(StorageError::backend)?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_expires_at ON blobs(expires_at) WHERE expires_at IS NOT NULL",
            empty_params(),
        )
        .await
        .map_err(StorageError::backend)?;

        Ok(())
    }

    async fn begin_transaction(&self) -> StorageResult<Box<dyn StorageTransaction>> {
        let (conn, permit) = self.connection().await?;
        conn.execute("BEGIN IMMEDIATE", empty_params())
            .await
            .map_err(StorageError::backend)?;

        Ok(Box::new(TursoTransaction {
            conn: Some(conn),
            permit: Some(permit),
        }))
    }

    async fn fetch_blob(&self, key: &str, now_ts: i64) -> StorageResult<Option<Vec<u8>>> {
        let (conn, _permit) = self.connection().await?;
        let mut rows = conn
            .query(
                "SELECT data FROM blobs WHERE key = ? AND (expires_at IS NULL OR expires_at > ?)",
                params::Params::Positional(vec![
                    Value::Text(key.to_string()),
                    Value::Integer(now_ts),
                ]),
            )
            .await
            .map_err(StorageError::backend)?;

        match next_value(&mut rows).await? {
            Some(value) => value_to_bytes(value).map(Some),
            None => Ok(None),
        }
    }

    async fn blob_exists(&self, key: &str) -> StorageResult<bool> {
        let (conn, _permit) = self.connection().await?;
        let mut rows = conn
            .query(
                "SELECT 1 FROM blobs WHERE key = ?",
                params::Params::Positional(vec![Value::Text(key.to_string())]),
            )
            .await
            .map_err(StorageError::backend)?;

        Ok(next_row(&mut rows).await?.is_some())
    }

    async fn blob_exists_with_expiry(&self, key: &str, now_ts: i64) -> StorageResult<bool> {
        let (conn, _permit) = self.connection().await?;
        let mut rows = conn
            .query(
                "SELECT 1 FROM blobs WHERE key = ? AND (expires_at IS NULL OR expires_at > ?)",
                params::Params::Positional(vec![
                    Value::Text(key.to_string()),
                    Value::Integer(now_ts),
                ]),
            )
            .await
            .map_err(StorageError::backend)?;

        Ok(next_row(&mut rows).await?.is_some())
    }

    async fn fetch_ttl(&self, key: &str, now_ts: i64) -> StorageResult<Option<Option<i64>>> {
        let (conn, _permit) = self.connection().await?;
        let mut rows = conn
            .query(
                "SELECT expires_at FROM blobs WHERE key = ? AND (expires_at IS NULL OR expires_at > ?)",
                params::Params::Positional(vec![
                    Value::Text(key.to_string()),
                    Value::Integer(now_ts),
                ]),
            )
            .await
            .map_err(StorageError::backend)?;

        match next_value(&mut rows).await? {
            Some(value) => value_to_optional_i64(value).map(Some),
            None => Ok(None),
        }
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
        let (conn, _permit) = self.connection().await?;
        let mut rows = conn
            .query(
                &query,
                params::Params::Positional(vec![
                    Value::Text(key.to_string()),
                    Value::Integer(now_ts),
                ]),
            )
            .await
            .map_err(StorageError::backend)?;

        match next_value(&mut rows).await? {
            Some(value) => value_to_bytes(value).map(Some),
            None => Ok(None),
        }
    }

    async fn namespace_key_exists(&self, namespace: &str, key: &str) -> StorageResult<bool> {
        let table_name = format!("blobs_{}", namespace);
        let query = format!("SELECT 1 FROM {} WHERE key = ?", table_name);
        let (conn, _permit) = self.connection().await?;
        let mut rows = conn
            .query(
                &query,
                params::Params::Positional(vec![Value::Text(key.to_string())]),
            )
            .await
            .map_err(StorageError::backend)?;

        Ok(next_row(&mut rows).await?.is_some())
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
        let (conn, _permit) = self.connection().await?;
        let mut rows = conn
            .query(
                &query,
                params::Params::Positional(vec![
                    Value::Text(key.to_string()),
                    Value::Integer(now_ts),
                ]),
            )
            .await
            .map_err(StorageError::backend)?;

        Ok(next_row(&mut rows).await?.is_some())
    }

    async fn list_namespace_tables(&self) -> StorageResult<HashSet<String>> {
        let mut tables = HashSet::new();
        tables.insert("blobs".to_string());

        let (conn, _permit) = self.connection().await?;
        let mut rows = conn
            .query(
                "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'blobs_%'",
                empty_params(),
            )
            .await
            .map_err(StorageError::backend)?;

        while let Some(row) = next_row(&mut rows).await? {
            let value = row.get_value(0).map_err(StorageError::backend)?;
            if let Value::Text(name) = value {
                tables.insert(name);
            }
        }

        Ok(tables)
    }

    async fn delete_expired_main(&self, now_ts: i64) -> StorageResult<u64> {
        let (mut conn, _permit) = self.connection().await?;
        self.execute_with_changes(
            &mut conn,
            "DELETE FROM blobs WHERE expires_at IS NOT NULL AND expires_at <= ?",
            params::Params::Positional(vec![Value::Integer(now_ts)]),
        )
        .await
    }

    async fn delete_expired_namespace(&self, namespace: &str, now_ts: i64) -> StorageResult<u64> {
        let table_name = format!("blobs_{}", namespace);
        let query = format!(
            "DELETE FROM {} WHERE expires_at IS NOT NULL AND expires_at <= ?",
            table_name
        );
        let (mut conn, _permit) = self.connection().await?;

        self.execute_with_changes(
            &mut conn,
            &query,
            params::Params::Positional(vec![Value::Integer(now_ts)]),
        )
        .await
    }
}

struct TursoTransaction {
    conn: Option<Connection>,
    permit: Option<OwnedSemaphorePermit>,
}

impl TursoTransaction {
    fn conn(&mut self) -> StorageResult<&mut Connection> {
        self.conn
            .as_mut()
            .ok_or_else(|| StorageError::backend("transaction already finished"))
    }

    async fn execute_with_changes(&mut self, query: &str, params: Params) -> StorageResult<u64> {
        let conn = self.conn()?;
        conn.execute(query, params)
            .await
            .map_err(StorageError::backend)?;

        let mut rows = conn
            .query("SELECT changes()", empty_params())
            .await
            .map_err(StorageError::backend)?;

        match next_value(&mut rows).await? {
            Some(Value::Integer(changes)) => Ok(changes as u64),
            Some(Value::Null) | None => Ok(0),
            Some(other) => Err(StorageError::backend(format!(
                "unexpected value from changes(): {:?}",
                other
            ))),
        }
    }
}

#[async_trait]
impl StorageTransaction for TursoTransaction {
    async fn upsert_blob(
        &mut self,
        key: &str,
        data: &[u8],
        expires_at: Option<i64>,
        now_ts: i64,
    ) -> StorageResult<()> {
        self.conn()?
            .execute(
                "INSERT INTO blobs (key, data, created_at, updated_at, expires_at, version)
                 VALUES (?, ?, ?, ?, ?, 0)
                 ON CONFLICT(key) DO UPDATE SET
                     data = excluded.data,
                     updated_at = excluded.updated_at,
                     expires_at = excluded.expires_at,
                     version = blobs.version + 1",
                params::Params::Positional(vec![
                    Value::Text(key.to_string()),
                    Value::Blob(data.to_vec()),
                    Value::Integer(now_ts),
                    Value::Integer(now_ts),
                    value_from_option_i64(expires_at),
                ]),
            )
            .await
            .map_err(StorageError::backend)?;
        Ok(())
    }

    async fn delete_blob(&mut self, key: &str) -> StorageResult<()> {
        self.conn()?
            .execute(
                "DELETE FROM blobs WHERE key = ?",
                params::Params::Positional(vec![Value::Text(key.to_string())]),
            )
            .await
            .map_err(StorageError::backend)?;
        Ok(())
    }

    async fn set_blob_expiry(&mut self, key: &str, expires_at: i64) -> StorageResult<bool> {
        let changed = self
            .execute_with_changes(
                "UPDATE blobs SET expires_at = ? WHERE key = ?",
                params::Params::Positional(vec![
                    Value::Integer(expires_at),
                    Value::Text(key.to_string()),
                ]),
            )
            .await?;
        Ok(changed > 0)
    }

    async fn ensure_namespace(&mut self, namespace: &str) -> StorageResult<()> {
        let table_name = format!("blobs_{}", namespace);
        let index_name = format!("idx_{}_expires_at", namespace);

        self.conn()?
            .execute(
                &format!(
                    "CREATE TABLE IF NOT EXISTS {} (
                        key TEXT PRIMARY KEY,
                        data BLOB,
                        created_at INTEGER NOT NULL,
                        updated_at INTEGER NOT NULL,
                        expires_at INTEGER,
                        version INTEGER NOT NULL DEFAULT 0
                    )",
                    table_name
                ),
                empty_params(),
            )
            .await
            .map_err(StorageError::backend)?;

        self.conn()?
            .execute(
                &format!(
                    "CREATE INDEX IF NOT EXISTS {} ON {}(expires_at) WHERE expires_at IS NOT NULL",
                    index_name, table_name
                ),
                empty_params(),
            )
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
        let table_name = format!("blobs_{}", namespace);
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

        self.conn()?
            .execute(
                &query,
                params::Params::Positional(vec![
                    Value::Text(key.to_string()),
                    Value::Blob(data.to_vec()),
                    Value::Integer(now_ts),
                    Value::Integer(now_ts),
                    value_from_option_i64(expires_at),
                ]),
            )
            .await
            .map_err(StorageError::backend)?;
        Ok(())
    }

    async fn delete_namespace(&mut self, namespace: &str, key: &str) -> StorageResult<()> {
        let table_name = format!("blobs_{}", namespace);
        let query = format!("DELETE FROM {} WHERE key = ?", table_name);

        self.conn()?
            .execute(
                &query,
                params::Params::Positional(vec![Value::Text(key.to_string())]),
            )
            .await
            .map_err(StorageError::backend)?;
        Ok(())
    }

    async fn commit(mut self: Box<Self>) -> StorageResult<()> {
        if let Some(conn) = self.conn.take() {
            conn.execute("COMMIT", empty_params())
                .await
                .map_err(StorageError::backend)?;
        }
        self.permit.take();
        Ok(())
    }

    async fn rollback(mut self: Box<Self>) -> StorageResult<()> {
        if let Some(conn) = self.conn.take() {
            conn.execute("ROLLBACK", empty_params())
                .await
                .map_err(StorageError::backend)?;
        }
        self.permit.take();
        Ok(())
    }
}

impl Drop for TursoTransaction {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            tokio::spawn(async move {
                let _ = conn.execute("ROLLBACK", empty_params()).await;
            });
        }
        self.permit.take();
    }
}

async fn next_row(rows: &mut Rows) -> StorageResult<Option<Row>> {
    rows.next().await.map_err(StorageError::backend)
}

async fn next_value(rows: &mut Rows) -> StorageResult<Option<Value>> {
    if let Some(row) = next_row(rows).await? {
        let value = row.get_value(0).map_err(StorageError::backend)?;
        Ok(Some(value))
    } else {
        Ok(None)
    }
}

fn value_from_option_i64(value: Option<i64>) -> Value {
    value.map(Value::Integer).unwrap_or(Value::Null)
}

fn value_to_bytes(value: Value) -> StorageResult<Vec<u8>> {
    match value {
        Value::Blob(data) => Ok(data),
        Value::Text(text) => Ok(text.into_bytes()),
        Value::Null => Ok(Vec::new()),
        other => Err(StorageError::backend(format!(
            "unexpected value type for blob column: {:?}",
            other
        ))),
    }
}

fn value_to_optional_i64(value: Value) -> StorageResult<Option<i64>> {
    match value {
        Value::Integer(v) => Ok(Some(v)),
        Value::Null => Ok(None),
        other => Err(StorageError::backend(format!(
            "unexpected value type for integer column: {:?}",
            other
        ))),
    }
}

fn empty_params() -> Params {
    params::Params::Positional(Vec::new())
}
