use std::{
    fmt::Write,
    time::{Duration, Instant},
};

use tokio::runtime::RuntimeMetrics;

use metrics::{Counter, Gauge, Histogram};
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder};
use miette::Result;
use sqlx::{Sqlite, SqlitePool, pool::PoolConnection};

const VALUE_SIZE_METRIC: &str = "blobasaur_value_size_bytes";
const STORED_VALUE_SIZE_METRIC: &str = "blobasaur_stored_value_size_bytes";

/// Bucket bounds for value sizes: powers of 4 from 64 B to 64 MiB.
const VALUE_SIZE_BUCKETS: &[f64] = &[
    64.0, 256.0, 1024.0, 4096.0, 16384.0, 65536.0, 262144.0, 1048576.0, 4194304.0, 16777216.0,
    67108864.0,
];

const ENQUEUE_WAIT_METRIC: &str = "blobasaur_write_enqueue_wait_seconds";
const TRANSACTION_DURATION_METRIC: &str = "blobasaur_sqlite_transaction_duration_seconds";
const POOL_ACQUIRE_METRIC: &str = "blobasaur_sqlite_pool_acquire_seconds";
const CLEANUP_DURATION_METRIC: &str = "blobasaur_cleanup_duration_seconds";

/// Bucket bounds for write path and SQLite latencies: 100 µs to 30 s.
const LATENCY_BUCKETS: &[f64] = &[
    0.0001, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
    30.0,
];

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

    // Storage metrics
    pub storage_operations_total: Counter,
    pub sqlite_auto_vacuum_misconfigured_total: Counter,

    // Batch metrics
    pub batch_operations_total: Counter,
    pub batch_size: Histogram,
    pub batch_duration_seconds: Histogram,

    // Value size metrics (uncompressed bytes, by command)
    pub value_size_get_bytes: Histogram,
    pub value_size_set_bytes: Histogram,
    pub value_size_hget_bytes: Histogram,
    pub value_size_hset_bytes: Histogram,

    // Stored value size metrics (after compression, by write command)
    pub stored_value_size_set_bytes: Histogram,
    pub stored_value_size_hset_bytes: Histogram,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
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

            // Storage metrics
            storage_operations_total: metrics::counter!("blobasaur_storage_operations_total"),
            sqlite_auto_vacuum_misconfigured_total: metrics::counter!(
                "blobasaur_sqlite_auto_vacuum_misconfigured_total"
            ),

            // Batch metrics
            batch_operations_total: metrics::counter!("blobasaur_batch_operations_total"),
            batch_size: metrics::histogram!("blobasaur_batch_size"),
            batch_duration_seconds: metrics::histogram!("blobasaur_batch_duration_seconds"),

            // Value size metrics
            value_size_get_bytes: metrics::histogram!(VALUE_SIZE_METRIC, "command" => "get"),
            value_size_set_bytes: metrics::histogram!(VALUE_SIZE_METRIC, "command" => "set"),
            value_size_hget_bytes: metrics::histogram!(VALUE_SIZE_METRIC, "command" => "hget"),
            value_size_hset_bytes: metrics::histogram!(VALUE_SIZE_METRIC, "command" => "hset"),

            // Stored value size metrics
            stored_value_size_set_bytes: metrics::histogram!(
                STORED_VALUE_SIZE_METRIC,
                "command" => "set"
            ),
            stored_value_size_hset_bytes: metrics::histogram!(
                STORED_VALUE_SIZE_METRIC,
                "command" => "hset"
            ),
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

    /// Record one GET/HGET lookup outcome. `source` is `inflight` or
    /// `sqlite`, `result` is `hit`, `miss` or `error`.
    pub fn record_read(&self, command: &'static str, source: &'static str, result: &'static str) {
        metrics::counter!(
            "blobasaur_reads_total",
            "command" => command,
            "source" => source,
            "result" => result
        )
        .increment(1);
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

/// Per-shard write queue, SQLite, and expiry cleanup metrics. Constructing it
/// again for the same shard returns handles to the same series.
#[derive(Clone)]
pub struct ShardMetrics {
    shard: String,
    pub write_enqueue_wait_seconds: Histogram,
    /// Accepted async ops not yet committed or failed.
    pub async_writes_pending: Gauge,
    pub async_write_failures_total: Counter,
    pub cleanup_deleted_keys_total: Counter,
    read_acquire_ok: Histogram,
    read_acquire_error: Histogram,
    write_acquire_ok: Histogram,
    write_acquire_error: Histogram,
}

impl ShardMetrics {
    pub fn new(shard_id: usize) -> Self {
        let shard = shard_id.to_string();
        let acquire = |pool: &'static str, result: &'static str| {
            metrics::histogram!(
                POOL_ACQUIRE_METRIC,
                "shard" => shard.clone(),
                "pool" => pool,
                "result" => result
            )
        };
        Self {
            write_enqueue_wait_seconds: metrics::histogram!(
                ENQUEUE_WAIT_METRIC,
                "shard" => shard.clone()
            ),
            async_writes_pending: metrics::gauge!(
                "blobasaur_async_writes_pending",
                "shard" => shard.clone()
            ),
            async_write_failures_total: metrics::counter!(
                "blobasaur_async_write_failures_total",
                "shard" => shard.clone()
            ),
            cleanup_deleted_keys_total: metrics::counter!(
                "blobasaur_cleanup_deleted_keys_total",
                "shard" => shard.clone()
            ),
            read_acquire_ok: acquire("read", "ok"),
            read_acquire_error: acquire("read", "error"),
            write_acquire_ok: acquire("write", "ok"),
            write_acquire_error: acquire("write", "error"),
            shard,
        }
    }

    /// Acquire a connection from this shard's `pool` ("read" or "write"),
    /// timing only the wait for it.
    pub async fn acquire(
        &self,
        pool: &SqlitePool,
        name: &'static str,
    ) -> Result<PoolConnection<Sqlite>, sqlx::Error> {
        let start = Instant::now();
        let result = pool.acquire().await;
        let (ok, error) = match name {
            "write" => (&self.write_acquire_ok, &self.write_acquire_error),
            _ => (&self.read_acquire_ok, &self.read_acquire_error),
        };
        match &result {
            Ok(_) => ok.record(start.elapsed()),
            Err(e) => {
                error.record(start.elapsed());
                self.record_sqlite_error("acquire", &e.to_string());
            }
        }
        result
    }

    /// Record a SQLite failure at `stage`: acquire, begin, execute or commit.
    pub fn record_sqlite_error(&self, stage: &'static str, error: &str) {
        metrics::counter!(
            "blobasaur_sqlite_errors_total",
            "shard" => self.shard.clone(),
            "stage" => stage,
            "kind" => sqlite_error_kind(error)
        )
        .increment(1);
    }

    /// Record a writer transaction, from just before BEGIN to commit or failure.
    pub fn record_transaction(&self, ok: bool, duration: Duration) {
        metrics::histogram!(
            TRANSACTION_DURATION_METRIC,
            "shard" => self.shard.clone(),
            "result" => if ok { "ok" } else { "error" }
        )
        .record(duration);
    }

    /// Record one expiry cleanup pass.
    pub fn record_cleanup(&self, ok: bool, duration: Duration) {
        metrics::histogram!(
            CLEANUP_DURATION_METRIC,
            "shard" => self.shard.clone(),
            "result" => if ok { "ok" } else { "error" }
        )
        .record(duration);
    }

    /// Record a cleanup failure at `stage`: discover or delete.
    pub fn record_cleanup_error(&self, stage: &'static str) {
        metrics::counter!(
            "blobasaur_cleanup_errors_total",
            "shard" => self.shard.clone(),
            "stage" => stage
        )
        .increment(1);
    }
}

/// Bounded category for a SQLite or pool error message. SQLite reports
/// SQLITE_BUSY as "database is locked" and SQLITE_LOCKED as "database table is
/// locked".
fn sqlite_error_kind(error: &str) -> &'static str {
    let error = error.to_ascii_lowercase();
    if error.contains("pool timed out") {
        "pool_timeout"
    } else if error.contains("table is locked") {
        "locked"
    } else if error.contains("database is locked") || error.contains("database is busy") {
        "busy"
    } else if error.contains("constraint") {
        "constraint"
    } else if error.contains("i/o error") || error.contains("error communicating") {
        "io"
    } else {
        "other"
    }
}

/// Initialize the Prometheus metrics exporter
pub fn init_metrics_exporter() -> Result<metrics_exporter_prometheus::PrometheusHandle> {
    let mut builder = PrometheusBuilder::new();
    for (metric, buckets) in [
        (VALUE_SIZE_METRIC, VALUE_SIZE_BUCKETS),
        (STORED_VALUE_SIZE_METRIC, VALUE_SIZE_BUCKETS),
        (ENQUEUE_WAIT_METRIC, LATENCY_BUCKETS),
        (TRANSACTION_DURATION_METRIC, LATENCY_BUCKETS),
        (POOL_ACQUIRE_METRIC, LATENCY_BUCKETS),
        (CLEANUP_DURATION_METRIC, LATENCY_BUCKETS),
    ] {
        builder = builder
            .set_buckets_for_metric(Matcher::Full(metric.to_string()), buckets)
            .map_err(|e| miette::miette!("Failed to configure metric buckets: {}", e))?;
    }
    let handle = builder
        .install_recorder()
        .map_err(|e| miette::miette!("Failed to install Prometheus recorder: {}", e))?;

    tracing::info!("Prometheus metrics exporter initialized");
    Ok(handle)
}

/// Append a snapshot of the Tokio runtime using its stable metrics API.
///
/// Render directly so cumulative busy time can be a seconds-valued Prometheus
/// counter. The metrics crate's counters only accept integer values. Reading the
/// runtime totals also avoids double-counting when scrapes overlap.
pub(crate) fn append_tokio_metrics(output: &mut String, runtime: &RuntimeMetrics) {
    for (name, help, value) in [
        (
            "blobasaur_tokio_workers",
            "Number of Tokio runtime worker threads.",
            runtime.num_workers(),
        ),
        (
            "blobasaur_tokio_alive_tasks",
            "Number of alive async tasks in the Tokio runtime.",
            runtime.num_alive_tasks(),
        ),
        (
            "blobasaur_tokio_global_queue_depth",
            "Number of tasks waiting in the Tokio global scheduling queue.",
            runtime.global_queue_depth(),
        ),
    ] {
        writeln!(
            output,
            "# HELP {name} {help}\n# TYPE {name} gauge\n{name} {value}"
        )
        .expect("writing to a String cannot fail");
    }

    // Tokio exposes worker timing and park counts only with 64-bit atomics.
    #[cfg(target_has_atomic = "64")]
    {
        for (name, kind, help) in [
            (
                "blobasaur_tokio_worker_busy_duration_seconds_total",
                "counter",
                "Total time each Tokio worker has spent processing work in seconds, not CPU time.",
            ),
            (
                "blobasaur_tokio_worker_parks_total",
                "counter",
                "Total times each Tokio worker has parked waiting for work.",
            ),
            (
                "blobasaur_tokio_worker_park_unparks_total",
                "counter",
                "Total park and unpark transitions for each Tokio worker.",
            ),
            (
                "blobasaur_tokio_worker_parked",
                "gauge",
                "Whether each Tokio worker is parked waiting for work: 1 if parked, 0 otherwise.",
            ),
        ] {
            writeln!(output, "# HELP {name} {help}\n# TYPE {name} {kind}")
                .expect("writing to a String cannot fail");
            for worker in 0..runtime.num_workers() {
                let value = match name {
                    "blobasaur_tokio_worker_busy_duration_seconds_total" => {
                        runtime.worker_total_busy_duration(worker).as_secs_f64()
                    }
                    "blobasaur_tokio_worker_parks_total" => {
                        runtime.worker_park_count(worker) as f64
                    }
                    "blobasaur_tokio_worker_park_unparks_total" => {
                        runtime.worker_park_unpark_count(worker) as f64
                    }
                    _ => (runtime.worker_park_unpark_count(worker) % 2) as f64,
                };
                writeln!(output, "{name}{{worker=\"{worker}\"}} {value}")
                    .expect("writing to a String cannot fail");
            }
        }
    }
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

/// The value of `series` (name plus labels, as rendered) in Prometheus output.
#[cfg(test)]
pub(crate) fn rendered_value(output: &str, series: &str) -> f64 {
    output
        .lines()
        .find_map(|line| line.strip_prefix(series)?.strip_prefix(' '))
        .unwrap_or_else(|| panic!("missing {series} in:\n{output}"))
        .parse()
        .unwrap()
}

#[cfg(test)]
mod tests {
    use super::{append_tokio_metrics, sqlite_error_kind};
    use tokio::runtime::{Builder, RuntimeMetrics};

    fn snapshot(runtime: &RuntimeMetrics) -> String {
        let mut output = String::new();
        append_tokio_metrics(&mut output, runtime);
        output
    }

    fn sample(output: &str, name: &str) -> f64 {
        output
            .lines()
            .find_map(|line| line.strip_prefix(&format!("{name} ")))
            .unwrap_or_else(|| panic!("missing metric {name}"))
            .parse()
            .unwrap()
    }

    fn check_snapshot(runtime: &RuntimeMetrics, workers: usize) {
        let output = snapshot(runtime);
        assert_eq!(sample(&output, "blobasaur_tokio_workers"), workers as f64);
        assert_eq!(sample(&output, "blobasaur_tokio_alive_tasks"), 0.0);
        assert_eq!(sample(&output, "blobasaur_tokio_global_queue_depth"), 0.0);
        assert!(output.contains("# TYPE blobasaur_tokio_alive_tasks gauge\n"));

        #[cfg(target_has_atomic = "64")]
        for worker in 0..workers {
            for name in [
                "blobasaur_tokio_worker_busy_duration_seconds_total",
                "blobasaur_tokio_worker_parks_total",
                "blobasaur_tokio_worker_park_unparks_total",
            ] {
                assert!(output.contains(&format!("# TYPE {name} counter\n")));
                let value = sample(&output, &format!("{name}{{worker=\"{worker}\"}}"));
                assert!(value.is_finite() && value >= 0.0);
            }
            let parked = sample(
                &output,
                &format!("blobasaur_tokio_worker_parked{{worker=\"{worker}\"}}"),
            );
            assert!(parked == 0.0 || parked == 1.0);
        }
    }

    #[test]
    fn application_metrics_classify_sqlite_errors() {
        for (error, kind) in [
            (
                "pool timed out while waiting for an open connection",
                "pool_timeout",
            ),
            ("(code: 5) database is locked", "busy"),
            ("(code: 6) database table is locked", "locked"),
            ("UNIQUE constraint failed: blobs.key", "constraint"),
            ("disk I/O error", "io"),
            (
                "attempted to acquire a connection on a closed pool",
                "other",
            ),
        ] {
            assert_eq!(sqlite_error_kind(error), kind, "{error}");
        }
    }

    #[test]
    fn tokio_metrics_current_thread_snapshot() {
        let runtime = Builder::new_current_thread().build().unwrap();
        check_snapshot(&runtime.metrics(), 1);
    }

    #[test]
    fn tokio_metrics_multi_thread_snapshot() {
        let runtime = Builder::new_multi_thread()
            .worker_threads(2)
            .build()
            .unwrap();
        check_snapshot(&runtime.metrics(), 2);
    }

    #[test]
    fn tokio_metrics_track_tasks_and_preserve_runtime_totals() {
        let runtime = Builder::new_current_thread().build().unwrap();
        let metrics = runtime.metrics();
        runtime.block_on(async {
            let task = tokio::spawn(std::future::pending::<()>());
            tokio::task::yield_now().await;
            assert_eq!(
                sample(&snapshot(&metrics), "blobasaur_tokio_alive_tasks"),
                1.0
            );
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
            assert_eq!(
                sample(&snapshot(&metrics), "blobasaur_tokio_alive_tasks"),
                0.0
            );
        });

        // A stopped current-thread runtime cannot change between scrapes.
        // Rendering again must not add cumulative totals a second time.
        let first = snapshot(&metrics);
        assert_eq!(first, snapshot(&metrics));
        #[cfg(target_has_atomic = "64")]
        assert_eq!(
            sample(
                &first,
                "blobasaur_tokio_worker_busy_duration_seconds_total{worker=\"0\"}",
            ),
            metrics.worker_total_busy_duration(0).as_secs_f64(),
        );
    }
}
