use std::time::{Duration, Instant};

use metrics::{Counter, Gauge, Histogram};
use metrics_exporter_prometheus::PrometheusBuilder;
use miette::Result;

/// Metrics collector for the blobasaur Redis server
#[derive(Clone)]
pub struct Metrics {
    // Command counters
    pub commands_total: Counter,
    pub commands_get_total: Counter,
    pub commands_set_total: Counter,
    pub commands_del_total: Counter,
    pub commands_exists_total: Counter,
    pub commands_hget_total: Counter,
    pub commands_hset_total: Counter,
    pub commands_hdel_total: Counter,
    pub commands_hexists_total: Counter,
    pub commands_ping_total: Counter,
    pub commands_info_total: Counter,
    pub commands_cluster_total: Counter,
    pub commands_unknown_total: Counter,

    // Command latency histograms
    pub command_duration_seconds: Histogram,
    pub get_duration_seconds: Histogram,
    pub set_duration_seconds: Histogram,
    pub del_duration_seconds: Histogram,
    pub hget_duration_seconds: Histogram,
    pub hset_duration_seconds: Histogram,
    pub hdel_duration_seconds: Histogram,

    // Error counters
    pub errors_total: Counter,
    pub connection_errors_total: Counter,
    pub protocol_errors_total: Counter,
    pub storage_errors_total: Counter,

    // Connection metrics
    pub connections_active: Gauge,
    pub connections_total: Counter,
    pub connections_dropped_total: Counter,

    // Cache metrics
    pub cache_hits_total: Counter,
    pub cache_misses_total: Counter,

    // Storage metrics
    pub storage_operations_total: Counter,
    pub sqlite_auto_vacuum_misconfigured_total: Counter,

    // Batch metrics
    pub batch_operations_total: Counter,
    pub batch_size: Histogram,
    pub batch_duration_seconds: Histogram,
}

impl Metrics {
    /// Initialize metrics with the global recorder
    pub fn new() -> Self {
        let metrics = Self {
            // Command counters
            commands_total: metrics::counter!("blobasaur_commands_total"),
            commands_get_total: metrics::counter!("blobasaur_commands_get_total"),
            commands_set_total: metrics::counter!("blobasaur_commands_set_total"),
            commands_del_total: metrics::counter!("blobasaur_commands_del_total"),
            commands_exists_total: metrics::counter!("blobasaur_commands_exists_total"),
            commands_hget_total: metrics::counter!("blobasaur_commands_hget_total"),
            commands_hset_total: metrics::counter!("blobasaur_commands_hset_total"),
            commands_hdel_total: metrics::counter!("blobasaur_commands_hdel_total"),
            commands_hexists_total: metrics::counter!("blobasaur_commands_hexists_total"),
            commands_ping_total: metrics::counter!("blobasaur_commands_ping_total"),
            commands_info_total: metrics::counter!("blobasaur_commands_info_total"),
            commands_cluster_total: metrics::counter!("blobasaur_commands_cluster_total"),
            commands_unknown_total: metrics::counter!("blobasaur_commands_unknown_total"),

            // Command latency histograms
            command_duration_seconds: metrics::histogram!("blobasaur_command_duration_seconds"),
            get_duration_seconds: metrics::histogram!("blobasaur_get_duration_seconds"),
            set_duration_seconds: metrics::histogram!("blobasaur_set_duration_seconds"),
            del_duration_seconds: metrics::histogram!("blobasaur_del_duration_seconds"),
            hget_duration_seconds: metrics::histogram!("blobasaur_hget_duration_seconds"),
            hset_duration_seconds: metrics::histogram!("blobasaur_hset_duration_seconds"),
            hdel_duration_seconds: metrics::histogram!("blobasaur_hdel_duration_seconds"),

            // Error counters
            errors_total: metrics::counter!("blobasaur_errors_total"),
            connection_errors_total: metrics::counter!("blobasaur_connection_errors_total"),
            protocol_errors_total: metrics::counter!("blobasaur_protocol_errors_total"),
            storage_errors_total: metrics::counter!("blobasaur_storage_errors_total"),

            // Connection metrics
            connections_active: metrics::gauge!("blobasaur_connections_active"),
            connections_total: metrics::counter!("blobasaur_connections_total"),
            connections_dropped_total: metrics::counter!("blobasaur_connections_dropped_total"),

            // Cache metrics
            cache_hits_total: metrics::counter!("blobasaur_cache_hits_total"),
            cache_misses_total: metrics::counter!("blobasaur_cache_misses_total"),

            // Storage metrics
            storage_operations_total: metrics::counter!("blobasaur_storage_operations_total"),
            sqlite_auto_vacuum_misconfigured_total: metrics::counter!(
                "blobasaur_sqlite_auto_vacuum_misconfigured_total"
            ),

            // Batch metrics
            batch_operations_total: metrics::counter!("blobasaur_batch_operations_total"),
            batch_size: metrics::histogram!("blobasaur_batch_size"),
            batch_duration_seconds: metrics::histogram!("blobasaur_batch_duration_seconds"),
        };

        // Initialize baseline metrics to ensure we have data
        metrics.connections_active.set(0.0);

        metrics
    }

    /// Record server startup metrics
    pub fn record_server_startup(&self) {
        tracing::info!("Server startup metrics recorded");
    }

    /// Record a command execution
    pub fn record_command(&self, command: &str, start_time: Instant) {
        let duration = start_time.elapsed().as_secs_f64();

        self.commands_total.increment(1);
        self.command_duration_seconds.record(duration);

        match command.to_uppercase().as_str() {
            "GET" => {
                self.commands_get_total.increment(1);
                self.get_duration_seconds.record(duration);
            }
            "SET" => {
                self.commands_set_total.increment(1);
                self.set_duration_seconds.record(duration);
            }
            "DEL" => {
                self.commands_del_total.increment(1);
                self.del_duration_seconds.record(duration);
            }
            "EXISTS" => {
                self.commands_exists_total.increment(1);
            }
            "HGET" => {
                self.commands_hget_total.increment(1);
                self.hget_duration_seconds.record(duration);
            }
            "HSET" => {
                self.commands_hset_total.increment(1);
                self.hset_duration_seconds.record(duration);
            }
            "HDEL" => {
                self.commands_hdel_total.increment(1);
                self.hdel_duration_seconds.record(duration);
            }
            "HEXISTS" => {
                self.commands_hexists_total.increment(1);
            }
            "PING" => {
                self.commands_ping_total.increment(1);
            }
            "INFO" => {
                self.commands_info_total.increment(1);
            }
            cmd if cmd.starts_with("CLUSTER") => {
                self.commands_cluster_total.increment(1);
            }
            _ => {
                self.commands_unknown_total.increment(1);
            }
        }
    }

    /// Record a cache hit
    pub fn record_cache_hit(&self) {
        self.cache_hits_total.increment(1);
    }

    /// Record a cache miss
    pub fn record_cache_miss(&self) {
        self.cache_misses_total.increment(1);
    }

    /// Record an error
    pub fn record_error(&self, error_type: &str) {
        self.errors_total.increment(1);

        match error_type {
            "connection" => self.connection_errors_total.increment(1),
            "protocol" => self.protocol_errors_total.increment(1),
            "storage" => self.storage_errors_total.increment(1),
            _ => {}
        }
    }

    /// Record a new connection
    pub fn record_connection(&self) {
        self.connections_total.increment(1);
        self.connections_active.increment(1.0);
    }

    /// Record a dropped connection
    pub fn record_connection_dropped(&self) {
        self.connections_dropped_total.increment(1);
        self.connections_active.decrement(1.0);
    }

    /// Record storage operation
    pub fn record_storage_operation(&self) {
        self.storage_operations_total.increment(1);
    }

    /// Record a shard DB auto-vacuum mode mismatch.
    pub fn record_sqlite_auto_vacuum_misconfigured(&self) {
        self.sqlite_auto_vacuum_misconfigured_total.increment(1);
    }

    /// Record startup auto-upgrade attempts for legacy shard DB auto-vacuum mode.
    pub fn record_sqlite_auto_vacuum_upgrade_run(&self, result: &str, duration: Option<Duration>) {
        metrics::counter!(
            "blobasaur_sqlite_auto_vacuum_upgrade_runs_total",
            "result" => result.to_string()
        )
        .increment(1);

        if let Some(duration) = duration {
            metrics::histogram!(
                "blobasaur_sqlite_auto_vacuum_upgrade_duration_seconds",
                "result" => result.to_string()
            )
            .record(duration.as_secs_f64());
        }
    }

    /// Record a vacuum run outcome.
    pub fn record_vacuum_run(&self, mode: &str, result: &str, duration: Option<Duration>) {
        metrics::counter!(
            "blobasaur_vacuum_runs_total",
            "mode" => mode.to_string(),
            "result" => result.to_string()
        )
        .increment(1);

        if let Some(duration) = duration {
            metrics::histogram!(
                "blobasaur_vacuum_duration_seconds",
                "mode" => mode.to_string(),
                "result" => result.to_string()
            )
            .record(duration.as_secs_f64());
        }
    }

    /// Record estimated reclaimed pages/bytes for a vacuum run.
    pub fn record_vacuum_reclaimed_estimate(
        &self,
        mode: &str,
        shard_id: usize,
        pages: u64,
        bytes: Option<u64>,
    ) {
        let shard = shard_id.to_string();

        metrics::counter!(
            "blobasaur_vacuum_reclaimed_pages_estimate_total",
            "mode" => mode.to_string(),
            "shard" => shard.clone()
        )
        .increment(pages);

        if let Some(bytes) = bytes {
            metrics::counter!(
                "blobasaur_vacuum_reclaimed_bytes_estimate_total",
                "mode" => mode.to_string(),
                "shard" => shard
            )
            .increment(bytes);
        }
    }

    /// Record shard-level vacuum failures (busy vs generic error).
    pub fn record_vacuum_shard_failure(&self, shard_id: usize, mode: &str, result: &str) {
        metrics::counter!(
            "blobasaur_vacuum_shard_failures_total",
            "shard" => shard_id.to_string(),
            "mode" => mode.to_string(),
            "result" => result.to_string()
        )
        .increment(1);
    }

    /// Record batch operation
    pub fn record_batch_operation(&self, batch_size: usize, duration: std::time::Duration) {
        self.batch_operations_total.increment(1);
        self.batch_size.record(batch_size as f64);
        self.batch_duration_seconds.record(duration.as_secs_f64());
    }
}

/// Initialize the Prometheus metrics exporter
pub fn init_metrics_exporter() -> Result<metrics_exporter_prometheus::PrometheusHandle> {
    let handle = PrometheusBuilder::new()
        .install_recorder()
        .map_err(|e| miette::miette!("Failed to install Prometheus recorder: {}", e))?;

    tracing::info!("Prometheus metrics exporter initialized");
    Ok(handle)
}

/// Helper struct for timing operations
pub struct Timer {
    pub start: Instant,
}

impl Timer {
    pub fn start() -> Self {
        Self {
            start: Instant::now(),
        }
    }
}

impl Default for Timer {
    fn default() -> Self {
        Self::start()
    }
}
