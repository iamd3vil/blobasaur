use axum::{
    Router,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use metrics_exporter_prometheus::PrometheusHandle;
use miette::Result;
use std::sync::Arc;

/// HTTP server state containing the Prometheus handle
#[derive(Clone)]
pub struct HttpServerState {
    pub prometheus_handle: PrometheusHandle,
    pub runtime_metrics: tokio::runtime::RuntimeMetrics,
    /// Sets sampled gauges just before each render.
    pub sample: Arc<dyn Fn() + Send + Sync>,
}

/// Run the HTTP server for metrics endpoint
pub async fn run_metrics_server(
    prometheus_handle: PrometheusHandle,
    bind_addr: &str,
    sample: impl Fn() + Send + Sync + 'static,
) -> Result<()> {
    let state = HttpServerState {
        prometheus_handle,
        runtime_metrics: tokio::runtime::Handle::current().metrics(),
        sample: Arc::new(sample),
    };

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
    (state.sample)();
    let mut metrics_output = state.prometheus_handle.render();
    crate::metrics::append_tokio_metrics(&mut metrics_output, &state.runtime_metrics);

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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use metrics_exporter_prometheus::PrometheusBuilder;

    #[tokio::test]
    async fn tokio_metrics_endpoint_preserves_application_metrics() {
        // A local recorder avoids interference with other tests' global metrics.
        let recorder = PrometheusBuilder::new().build_recorder();
        metrics::with_local_recorder(&recorder, || {
            metrics::counter!("blobasaur_commands_total").increment(3);
        });
        let state = HttpServerState {
            prometheus_handle: recorder.handle(),
            runtime_metrics: tokio::runtime::Handle::current().metrics(),
            sample: Arc::new(|| {}),
        };
        let response = metrics_handler(State(state.clone())).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()["content-type"],
            "text/plain; version=0.0.4",
        );
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let output = std::str::from_utf8(&body).unwrap();
        assert!(output.contains("blobasaur_commands_total 3\n"));
        assert!(output.contains("# TYPE blobasaur_tokio_workers gauge\n"));
        assert!(output.contains("blobasaur_tokio_workers 1\n"));

        // A second scrape retains application counters and has one runtime sample.
        let response = metrics_handler(State(state)).await;
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let output = std::str::from_utf8(&body).unwrap();
        assert!(output.contains("blobasaur_commands_total 3\n"));
        assert_eq!(output.matches("blobasaur_tokio_workers 1\n").count(), 1);
    }
}
