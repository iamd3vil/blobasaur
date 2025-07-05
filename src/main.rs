use std::sync::Arc;

use miette::{Context, Result};

mod app_state;
mod cluster;
mod compression;
mod config;
mod http_server;
mod metrics;
mod redis;
mod server;
mod shard_manager;

use app_state::AppState;
use gumdrop::Options;

#[derive(Options, Debug)]
struct Args {
    #[options(help = "Print help message")]
    help: bool,

    #[options(
        help = "Path to the configuration file",
        short = "c",
        long = "config",
        default = "config.toml"
    )]
    config: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    // initialize tracing
    tracing_subscriber::fmt::init();

    let args = Args::parse_args_default_or_exit();
    let cfg = config::Cfg::load(&args.config).wrap_err("loading config")?;

    // Initialize metrics if enabled
    let prometheus_handle = if cfg.metrics.as_ref().map_or(false, |m| m.enabled) {
        Some(metrics::init_metrics_exporter().wrap_err("initializing metrics exporter")?)
    } else {
        None
    };

    // This vector will be populated by AppState::new
    let mut shard_receivers = Vec::with_capacity(cfg.num_shards);

    // Initialize AppState, AppState::new will populate shard_receivers
    let shared_state = Arc::new(AppState::new(cfg.clone(), &mut shard_receivers).await);

    // Initialize metrics
    shared_state.metrics.record_server_startup();

    // Spawn shard writer tasks using the receivers populated by AppState::new
    for (i, receiver) in shard_receivers.into_iter().enumerate() {
        let pool = shared_state.db_pools[i].clone();
        let batch_size = cfg.batch_size.unwrap_or(1);
        let batch_timeout_ms = cfg.batch_timeout_ms.unwrap_or(0);
        let inflight_cache = shared_state.inflight_cache.clone();
        let inflight_hcache = shared_state.inflight_hcache.clone();
        let metrics = shared_state.metrics.clone();
        // Pass the receiver to the spawned task
        tokio::spawn(shard_manager::shard_writer_task(
            i,
            pool,
            receiver,
            batch_size,
            batch_timeout_ms,
            inflight_cache,
            inflight_hcache,
            metrics,
        ));
    }

    // Start HTTP metrics server if enabled
    if let Some(handle) = prometheus_handle {
        let metrics_addr = cfg
            .metrics
            .as_ref()
            .and_then(|m| m.addr.as_ref())
            .map(|s| s.as_str())
            .unwrap_or("0.0.0.0:9090");

        let metrics_addr = metrics_addr.to_string();
        let metrics_handle = handle.clone();

        tokio::spawn(async move {
            if let Err(e) = http_server::run_metrics_server(metrics_handle, &metrics_addr).await {
                tracing::error!("Failed to run metrics server: {}", e);
            }
        });
    }

    // Run Redis server
    if let Err(e) = server::run_redis_server(
        shared_state,
        &cfg.addr.unwrap_or("0.0.0.0:6379".to_string()),
    )
    .await
    {
        return Err(miette::miette!("Failed to run Redis server: {}", e));
    }

    Ok(())
}
