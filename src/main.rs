use std::sync::Arc;

use miette::{Context, Result};

mod app_state;
mod cluster;
mod config;
mod redis;
mod server;
mod shard_manager;

use app_state::AppState;
use clap::Parser;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value = "config.toml")]
    config: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    // initialize tracing
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    let cfg = config::Cfg::load(&args.config).wrap_err("loading config")?;

    // This vector will be populated by AppState::new
    let mut shard_receivers = Vec::with_capacity(cfg.num_shards);

    // Initialize AppState, AppState::new will populate shard_receivers
    let shared_state = Arc::new(AppState::new(cfg.clone(), &mut shard_receivers).await);

    // Spawn shard writer tasks using the receivers populated by AppState::new
    for (i, receiver) in shard_receivers.into_iter().enumerate() {
        let pool = shared_state.db_pools[i].clone();
        let batch_size = cfg.batch_size.unwrap_or(1);
        let batch_timeout_ms = cfg.batch_timeout_ms.unwrap_or(0);
        let inflight_cache = shared_state.inflight_cache.clone();
        let inflight_hcache = shared_state.inflight_hcache.clone();
        // Pass the receiver to the spawned task
        tokio::spawn(shard_manager::shard_writer_task(
            i,
            pool,
            receiver,
            batch_size,
            batch_timeout_ms,
            inflight_cache,
            inflight_hcache,
        ));
    }

    // Run Redis server instead of HTTP server
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
