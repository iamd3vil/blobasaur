use axum::{
    Router,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use metrics_exporter_prometheus::PrometheusHandle;
use miette::Result;

/// HTTP server state containing the Prometheus handle
#[derive(Clone)]
pub struct HttpServerState {
    pub prometheus_handle: PrometheusHandle,
}

/// Run the HTTP server for metrics endpoint
pub async fn run_metrics_server(
    prometheus_handle: PrometheusHandle,
    bind_addr: &str,
) -> Result<()> {
    let state = HttpServerState { prometheus_handle };

    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/health", get(health_handler))
        .route("/", get(root_handler))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(bind_addr)
        .await
        .map_err(|e| miette::miette!("Failed to bind to {}: {}", bind_addr, e))?;

    tracing::info!("HTTP metrics server listening on {}", bind_addr);

    axum::serve(listener, app)
        .await
        .map_err(|e| miette::miette!("HTTP server failed: {}", e))?;

    Ok(())
}

/// Handler for /metrics endpoint - returns Prometheus metrics
async fn metrics_handler(State(state): State<HttpServerState>) -> Response {
    let metrics_output = state.prometheus_handle.render();

    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4")],
        metrics_output,
    )
        .into_response()
}

/// Handler for /health endpoint - basic health check
async fn health_handler() -> impl IntoResponse {
    (
        StatusCode::OK,
        [("content-type", "application/json")],
        r#"{"status": "ok", "service": "blobasaur"}"#,
    )
}

/// Handler for root endpoint - basic info
async fn root_handler() -> impl IntoResponse {
    (
        StatusCode::OK,
        [("content-type", "text/html")],
        r#"
<!DOCTYPE html>
<html>
<head>
    <title>Blobasaur Metrics Server</title>
</head>
<body>
    <h1>Blobasaur Metrics Server</h1>
    <p>This is the metrics server for the Blobasaur Redis-compatible server.</p>
    <ul>
        <li><a href="/metrics">Prometheus Metrics</a></li>
        <li><a href="/health">Health Check</a></li>
    </ul>
</body>
</html>
        "#,
    )
}
