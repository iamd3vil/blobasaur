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

#[derive(Debug, Clone, serde::Deserialize)]
pub struct CompressionConfig {
    pub enabled: bool,
    pub algorithm: CompressionType,
    pub level: Option<u32>,
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

        Ok(cfg)
    }

    pub fn is_compression(&self) -> bool {
        match &self.storage_compression {
            Some(comp) if comp.enabled => true,
            _ => false,
        }
    }
}
