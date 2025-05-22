use std::sync::Arc;

use axum::{Router, extract::DefaultBodyLimit, routing::get};
use http_server::AppState;
use miette::{Context, Result};

mod config;
mod http_server;
mod shard_manager;

#[tokio::main]
async fn main() -> Result<()> {
    // initialize tracing
    tracing_subscriber::fmt::init();

    let cfg = config::Cfg::load("config.toml").wrap_err("loading config")?;

    // This vector will be populated by AppState::new
    let mut shard_receivers = Vec::with_capacity(cfg.num_shards);

    // Initialize AppState, AppState::new will populate shard_receivers
    let shared_state =
        Arc::new(AppState::new(cfg.num_shards, &cfg.data_dir, &mut shard_receivers).await);

    // Spawn shard writer tasks using the receivers populated by AppState::new
    for (i, receiver) in shard_receivers.into_iter().enumerate() {
        let pool = shared_state.db_pools[i].clone();
        // Pass the receiver to the spawned task
        tokio::spawn(shard_manager::shard_writer_task(i, pool, receiver));
    }

    // build our application with a route
    let app = Router::new()
        // Use method chaining for routes, which is idiomatic Axum
        .route(
            "/blob/{key}",
            get(http_server::get_blob)
                .post(http_server::set_blob)
                .delete(http_server::delete_blob),
        )
        .layer(DefaultBodyLimit::max(1024 * 1024 * 30)) // 30 MB limit
        .with_state(shared_state);

    // run our app with hyper, listening globally on port 3000
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();

    Ok(())
}
