use config::Config;
use miette::{IntoDiagnostic, Result};

#[derive(Debug, Clone, serde::Deserialize)]
pub struct Cfg {
    pub data_dir: String,
    pub num_shards: usize,
    pub storage_compression: Option<bool>,
    pub output_compression: Option<bool>,
    pub async_write: Option<bool>,
    pub batch_size: Option<usize>,
    pub batch_timeout_ms: Option<u64>,
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

        if cfg.storage_compression.is_some_and(|v| v) {
            println!("Storage compression is enabled");
        }

        if cfg.output_compression.is_some_and(|v| v) {
            println!("Output compression is enabled");
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
}
