use config::Config;
use miette::{IntoDiagnostic, Result};

use crate::compression;

#[derive(Debug, Clone, serde::Deserialize)]
pub struct Cfg {
    pub data_dir: String,
    pub num_shards: usize,
    pub storage_compression: Option<CompressionConfig>,
    pub async_write: Option<bool>,
    pub batch_size: Option<usize>,
    pub batch_timeout_ms: Option<u64>,
    pub addr: Option<String>,
    pub cluster: Option<ClusterConfig>,
    pub metrics: Option<MetricsConfig>,
    pub sqlite: Option<SqliteConfig>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct SqliteConfig {
    /// SQLite cache size in MB for the whole process
    /// Default: 100 MB per shard (derived from shard count)
    /// Higher values improve read performance but consume more RAM
    pub cache_size_mb: Option<i32>,

    /// SQLite busy timeout in milliseconds
    /// Default: 5000 ms (5 seconds)
    /// Increase for high-contention workloads
    pub busy_timeout_ms: Option<u64>,

    /// SQLite synchronous mode: OFF, NORMAL, FULL
    /// Default: NORMAL (recommended for WAL mode)
    /// OFF = fastest but risk of corruption on power loss
    /// NORMAL = good performance with corruption safety in WAL mode
    /// FULL = safest but slower
    pub synchronous: Option<SqliteSynchronous>,

    /// Memory-mapped I/O size in MB for the whole process
    /// Default: 0 (disabled)
    /// Only useful for large databases that don't fit in cache
    /// Example: 3000 (≈3 GB total)
    pub mmap_size: Option<u64>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct SlotRange {
    pub start: u16,
    pub end: u16,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct ClusterConfig {
    pub enabled: bool,
    pub node_id: String,
    #[allow(dead_code)]
    pub seeds: Vec<String>,
    pub port: u16,
    pub slots: Option<Vec<u16>>,
    pub slot_ranges: Option<Vec<SlotRange>>,
    #[allow(dead_code)]
    pub gossip_interval_ms: Option<u64>,
    pub advertise_addr: Option<String>,
}

#[derive(Debug, Clone, Copy, serde::Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum CompressionType {
    None,
    Gzip,
    Zstd,
    Lz4,
    Brotli,
}

#[derive(Debug, Clone, Copy, serde::Deserialize, PartialEq)]
#[serde(rename_all = "UPPERCASE")]
pub enum SqliteSynchronous {
    OFF,
    NORMAL,
    FULL,
}

impl SqliteSynchronous {
    pub fn as_str(&self) -> &'static str {
        match self {
            SqliteSynchronous::OFF => "OFF",
            SqliteSynchronous::NORMAL => "NORMAL",
            SqliteSynchronous::FULL => "FULL",
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct CompressionConfig {
    pub enabled: bool,
    pub algorithm: CompressionType,
    pub level: Option<u32>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct MetricsConfig {
    pub enabled: bool,
    pub addr: Option<String>,
}

impl Cfg {
    pub fn load(cfg_path: &str) -> Result<Self> {
        let settings = Config::builder()
            .add_source(config::File::with_name(cfg_path))
            .build()
            .into_diagnostic()?;

        let cfg: Cfg = settings.try_deserialize().into_diagnostic()?;

        if cfg.num_shards == 0 {
            return Err(miette::miette!("num_shards must be greater than 0"));
        }

        if cfg.data_dir.is_empty() {
            return Err(miette::miette!("data_dir cannot be empty"));
        }

        if let Some(ref comp) = cfg.storage_compression {
            compression::validate_config(comp)?;
        }

        if cfg.async_write.is_some_and(|v| v) {
            println!("Async write is enabled");
        }

        let batch_size = cfg.batch_size.unwrap_or(1);
        let batch_timeout = cfg.batch_timeout_ms.unwrap_or(0);

        if batch_size > 1 {
            println!(
                "Batching enabled: size={}, timeout={}ms",
                batch_size, batch_timeout
            );
        }

        if batch_size == 0 {
            return Err(miette::miette!("batch_size must be greater than 0"));
        }

        println!("Data directory: {}", cfg.data_dir);
        println!("Number of shards: {}", cfg.num_shards);

        if let Some(ref metrics) = cfg.metrics {
            if metrics.enabled {
                println!(
                    "Metrics enabled on: {}",
                    metrics.addr.as_deref().unwrap_or("0.0.0.0:9090")
                );
            }
        }

        // Print SQLite configuration
        let cache_total_mb = cfg.sqlite_cache_total_mb();
        let cache_per_shard_mb = cfg.sqlite_cache_size_per_shard_mb();
        let busy_timeout = cfg.sqlite_busy_timeout_ms();
        let synchronous = cfg.sqlite_synchronous();
        let mmap_total_mb = cfg.sqlite_mmap_total_mb();
        let mmap_per_shard_mb = cfg.sqlite_mmap_per_shard_mb();

        println!("SQLite configuration:");
        println!(
            "  cache_size: {} MB total ({} MB per shard, floor)",
            cache_total_mb, cache_per_shard_mb
        );
        println!("  busy_timeout: {} ms", busy_timeout);
        println!("  synchronous: {}", synchronous.as_str());
        if mmap_total_mb > 0 {
            println!(
                "  mmap_size: {} MB total ({} MB per shard, floor)",
                mmap_total_mb, mmap_per_shard_mb
            );
        } else {
            println!("  mmap_size: disabled");
        }

        Ok(cfg)
    }

    pub fn is_compression(&self) -> bool {
        match &self.storage_compression {
            Some(comp) if comp.enabled => true,
            _ => false,
        }
    }

    /// Get configured SQLite cache size in MB for the whole process.
    /// Default scales with shard count to preserve the 100 MB per-shard baseline.
    pub fn sqlite_cache_total_mb(&self) -> i32 {
        let default_per_shard = 100usize;
        let default_total = self
            .num_shards
            .saturating_mul(default_per_shard)
            .min(i32::MAX as usize) as i32;

        self.sqlite
            .as_ref()
            .and_then(|s| s.cache_size_mb)
            .unwrap_or(default_total)
    }

    /// Derived SQLite cache size in MB per shard (floor division).
    pub fn sqlite_cache_size_per_shard_mb(&self) -> i32 {
        let total = self.sqlite_cache_total_mb();
        if total <= 0 {
            return 0;
        }

        let shards = self.num_shards as i32;
        if shards <= 0 {
            return 0;
        }

        let per_shard = total / shards;
        per_shard.max(0)
    }

    /// Get SQLite busy timeout in milliseconds (default: 5000 ms = 5 seconds)
    pub fn sqlite_busy_timeout_ms(&self) -> u64 {
        self.sqlite
            .as_ref()
            .and_then(|s| s.busy_timeout_ms)
            .unwrap_or(5000)
    }

    /// Get SQLite synchronous mode (default: NORMAL)
    pub fn sqlite_synchronous(&self) -> SqliteSynchronous {
        self.sqlite
            .as_ref()
            .and_then(|s| s.synchronous)
            .unwrap_or(SqliteSynchronous::NORMAL)
    }

    /// Get SQLite mmap size in MB (default: 0 = disabled)
    pub fn sqlite_mmap_total_mb(&self) -> u64 {
        self.sqlite.as_ref().and_then(|s| s.mmap_size).unwrap_or(0)
    }

    /// Derived SQLite mmap size in MB per shard (floor division).
    pub fn sqlite_mmap_per_shard_mb(&self) -> u64 {
        let total = self.sqlite_mmap_total_mb();
        if total == 0 {
            return 0;
        }

        let shards = self.num_shards as u64;
        if shards == 0 {
            return 0;
        }

        total / shards
    }

    /// Derived SQLite mmap size in bytes per shard (floor division).
    pub fn sqlite_mmap_per_shard_bytes(&self) -> u64 {
        let per_shard_mb = self.sqlite_mmap_per_shard_mb();
        if per_shard_mb == 0 {
            return 0;
        }

        per_shard_mb.saturating_mul(1024).saturating_mul(1024)
    }
}
