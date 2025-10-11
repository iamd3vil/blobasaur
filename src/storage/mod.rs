use async_trait::async_trait;
use std::{collections::HashSet, fmt::Debug};

#[cfg(feature = "backend-sqlite")]
pub mod sqlite;
#[cfg(feature = "backend-turso")]
pub mod turso;

pub type StorageResult<T> = Result<T, StorageError>;

#[derive(thiserror::Error, Debug)]
pub enum StorageError {
    #[error("storage backend error: {0}")]
    Backend(String),
}

impl StorageError {
    pub fn backend<E: std::fmt::Display>(err: E) -> Self {
        StorageError::Backend(err.to_string())
    }
}

#[async_trait]
pub trait StorageTransaction: Send {
    async fn upsert_blob(
        &mut self,
        key: &str,
        data: &[u8],
        expires_at: Option<i64>,
        now_ts: i64,
    ) -> StorageResult<()>;

    async fn delete_blob(&mut self, key: &str) -> StorageResult<()>;

    async fn set_blob_expiry(&mut self, key: &str, expires_at: i64) -> StorageResult<bool>;

    async fn ensure_namespace(&mut self, namespace: &str) -> StorageResult<()>;

    async fn upsert_namespace(
        &mut self,
        namespace: &str,
        key: &str,
        data: &[u8],
        expires_at: Option<i64>,
        now_ts: i64,
    ) -> StorageResult<()>;

    async fn delete_namespace(&mut self, namespace: &str, key: &str) -> StorageResult<()>;

    async fn commit(self: Box<Self>) -> StorageResult<()>;

    async fn rollback(self: Box<Self>) -> StorageResult<()>;
}

#[async_trait]
pub trait Storage: Send + Sync + Debug {
    async fn ensure_base_schema(&self) -> StorageResult<()>;

    async fn begin_transaction(&self) -> StorageResult<Box<dyn StorageTransaction>>;

    async fn fetch_blob(&self, key: &str, now_ts: i64) -> StorageResult<Option<Vec<u8>>>;

    async fn blob_exists(&self, key: &str) -> StorageResult<bool>;

    async fn blob_exists_with_expiry(&self, key: &str, now_ts: i64) -> StorageResult<bool>;

    async fn fetch_ttl(&self, key: &str, now_ts: i64) -> StorageResult<Option<Option<i64>>>;

    async fn fetch_namespace_blob(
        &self,
        namespace: &str,
        key: &str,
        now_ts: i64,
    ) -> StorageResult<Option<Vec<u8>>>;

    async fn namespace_key_exists(&self, namespace: &str, key: &str) -> StorageResult<bool>;

    async fn namespace_key_exists_with_expiry(
        &self,
        namespace: &str,
        key: &str,
        now_ts: i64,
    ) -> StorageResult<bool>;

    async fn list_namespace_tables(&self) -> StorageResult<HashSet<String>>;

    async fn delete_expired_main(&self, now_ts: i64) -> StorageResult<u64>;

    async fn delete_expired_namespace(&self, namespace: &str, now_ts: i64) -> StorageResult<u64>;
}
