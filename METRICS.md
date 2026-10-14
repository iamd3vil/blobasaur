# Blobasaur Metrics

Blobasaur provides comprehensive Prometheus-compatible metrics to monitor the performance and health of your Redis-compatible server.

## Configuration

Add the following to your `config.toml` to enable metrics:

```toml
[metrics]
enabled = true
addr = "0.0.0.0:9090"  # Address for metrics HTTP server
```

## Endpoints

When metrics are enabled, Blobasaur starts an HTTP server with the following endpoints:

- **`/metrics`** - Prometheus-compatible metrics endpoint
- **`/health`** - Health check endpoint (returns JSON status)
- **`/`** - Basic HTML page with links to other endpoints

## Available Metrics

### Command Metrics

- `blobasaur_commands_total` - Total number of commands processed
- `blobasaur_commands_get_total` - Total GET commands
- `blobasaur_commands_set_total` - Total SET commands
- `blobasaur_commands_del_total` - Total DEL commands
- `blobasaur_commands_exists_total` - Total EXISTS commands
- `blobasaur_commands_hget_total` - Total HGET commands
- `blobasaur_commands_hset_total` - Total HSET commands
- `blobasaur_commands_hdel_total` - Total HDEL commands
- `blobasaur_commands_hexists_total` - Total HEXISTS commands
- `blobasaur_commands_ping_total` - Total PING commands
- `blobasaur_commands_info_total` - Total INFO commands
- `blobasaur_commands_cluster_total` - Total CLUSTER commands
- `blobasaur_commands_unknown_total` - Total unknown commands

### Command Latency

- `blobasaur_command_duration_seconds` - Histogram of command execution times
- `blobasaur_get_duration_seconds` - Histogram of GET command latencies
- `blobasaur_set_duration_seconds` - Histogram of SET command latencies
- `blobasaur_del_duration_seconds` - Histogram of DEL command latencies
- `blobasaur_hget_duration_seconds` - Histogram of HGET command latencies
- `blobasaur_hset_duration_seconds` - Histogram of HSET command latencies
- `blobasaur_hdel_duration_seconds` - Histogram of HDEL command latencies

### Connection Metrics

- `blobasaur_connections_active` - Current number of active connections
- `blobasaur_connections_total` - Total number of connections accepted
- `blobasaur_connections_dropped_total` - Total number of connections dropped

### Tokio runtime metrics

These metrics are sampled from the server's Tokio runtime on every `/metrics` scrape. They use Tokio's stable API and need no additional configuration or `tokio_unstable` build flags.

| Metric | Type | Description |
| --- | --- | --- |
| `blobasaur_tokio_workers` | Gauge | Number of async runtime worker threads |
| `blobasaur_tokio_alive_tasks` | Gauge | Alive async tasks, including background and HTTP tasks, not just Redis connections |
| `blobasaur_tokio_global_queue_depth` | Gauge | Tasks waiting in the global scheduling queue, excluding worker-local queues |
| `blobasaur_tokio_worker_busy_duration_seconds_total{worker}` | Counter | Cumulative seconds each worker has spent processing work, not CPU time |
| `blobasaur_tokio_worker_parks_total{worker}` | Counter | Times each worker has parked waiting for work |
| `blobasaur_tokio_worker_park_unparks_total{worker}` | Counter | Combined park and unpark transitions for each worker |
| `blobasaur_tokio_worker_parked{worker}` | Gauge | `1` when a worker is parked waiting for work, otherwise `0` |

Worker labels are zero-based indices. Per-worker metrics require a target with 64-bit atomics. Runtime values are read independently, so a scrape is not an atomic snapshot of all workers. Tokio publishes worker statistics periodically, so busy time can lag current activity. Counters start at runtime creation and reset when the process restarts.

Blocking-pool size, worker-local queue depth, poll counts, and work-steal counts require Tokio's unstable API in the pinned Tokio version and are not exported.

### Read Metrics

- `blobasaur_reads_total{command,source,result}` - GET/HGET lookups. `command` is `get` or `hget`. `source` is `inflight` if the inflight cache answered (an async write or delete acknowledged but not yet committed; only used with `async_write`, and an entry lasts until its batch commits), otherwise `sqlite`. `result` is `hit` (value found), `miss` (nothing found, including a pending delete or a missing namespace table), or `error` (SQLite error)

### Error Metrics

- `blobasaur_errors_total` - Total number of errors
- `blobasaur_connection_errors_total` - Connection-related errors
- `blobasaur_protocol_errors_total` - Protocol parsing errors
- `blobasaur_storage_errors_total` - Storage/database errors

### Storage Metrics

- `blobasaur_storage_operations_total` - Total storage operations
- `blobasaur_sqlite_auto_vacuum_misconfigured_total` - Number of shard DBs observed at startup with `PRAGMA auto_vacuum` not set to `INCREMENTAL` (primarily useful when startup auto-upgrade is disabled)
- `blobasaur_sqlite_auto_vacuum_upgrade_runs_total{result}` - Startup legacy auto-vacuum conversion attempts by outcome:
  - `ok` = conversion succeeded
  - `error` = conversion failed (startup fails fast)
  - `skipped_disabled` = shard not converted because `[sqlite].auto_upgrade_legacy_auto_vacuum=false`
- `blobasaur_sqlite_auto_vacuum_upgrade_duration_seconds{result}` - Histogram of startup legacy auto-vacuum conversion duration by outcome

### Batch Processing Metrics

- `blobasaur_batch_operations_total` - Total batch operations processed
- `blobasaur_batch_size` - Histogram of batch sizes
- `blobasaur_batch_duration_seconds` - Histogram of batch processing times

### Write Queue Metrics

- `blobasaur_write_queue_depth{shard}` - Ops waiting in the shard's write queue, not counting the batch being written. Sampled on each scrape. The queue holds `batch_size` ops (default 100); a value at that limit means writers are blocked on it
- `blobasaur_write_enqueue_wait_seconds{shard}` - Histogram of time a SET/DEL/HSET/HSETEX/HDEL/EXPIRE/HEXPIRE/vacuum request waited for queue space, excluding SQLite execution
- `blobasaur_async_writes_pending{shard}` - `async_write` ops acknowledged to clients but not yet committed or failed, whether queued or in the batch being written
- `blobasaur_async_write_failures_total{shard}` - `async_write` ops that were acknowledged and then failed to persist, because their statement or their transaction failed. Each op counts once

### SQLite Metrics

- `blobasaur_sqlite_transaction_duration_seconds{shard,result}` - Histogram of writer batch transactions, from BEGIN through commit. `result` is `ok`, or `error` if BEGIN or COMMIT failed. A transaction with some failed statements still commits, so it counts as `ok`
- `blobasaur_sqlite_errors_total{shard,stage,kind}` - SQLite and pool failures. `stage` is `acquire` (getting a connection), `begin`, `execute` (one per failed writer statement), or `commit`. `kind` is `busy`, `locked`, `constraint`, `io`, `pool_timeout`, or `other`. Read query errors are not included; see `blobasaur_reads_total{result="error"}`
- `blobasaur_sqlite_pool_connections{shard,pool,state}` - Open connections in the shard's `read` or `write` pool, by `state` `idle` or `in_use`. Sampled on each scrape
- `blobasaur_sqlite_pool_acquire_seconds{shard,pool,result}` - Histogram of time waiting for a pool connection, separate from the query. Measured for GET/HGET reads (`read`) and writer batches (`write`); `result` is `ok` or `error`

### Expiry Cleanup Metrics

- `blobasaur_cleanup_deleted_keys_total{shard}` - Expired rows deleted by the cleanup task, across all tables
- `blobasaur_cleanup_duration_seconds{shard,result}` - Histogram of complete cleanup passes; `result` is `error` if any table lookup or delete failed
- `blobasaur_cleanup_errors_total{shard,stage}` - Cleanup failures; `stage` is `discover` (listing tables) or `delete`

The latency histograms in the three sections above use buckets from 100 µs to 30 s.

### Value Size Metrics

- `blobasaur_value_size_bytes{command}` - Histogram of uncompressed value sizes in bytes, where `command` is `set`, `hset` (includes HSETEX fields), `get`, or `hget`. Writes are recorded only when accepted (committed in sync mode, queued in `async_write` mode); rejected or failed writes are not counted. Reads are recorded only on hits. Buckets are powers of 4 from 64 B to 64 MiB.
- `blobasaur_stored_value_size_bytes{command}` - Histogram of value sizes as written to SQLite (after `storage_compression`), where `command` is `set` or `hset`. Recorded at the same points as the write side of `blobasaur_value_size_bytes`, so the two sums are directly comparable. Equal to the uncompressed size when compression is disabled. Same buckets.

### Vacuum Metrics

- `blobasaur_vacuum_runs_total{mode,result}` - Total shard vacuum attempts by vacuum mode and outcome (`ok`, `error`, `cancelled`, etc.)
- `blobasaur_vacuum_duration_seconds{mode,result}` - Histogram of shard vacuum duration (seconds) for completed/timed-out runs
- `blobasaur_vacuum_reclaimed_pages_estimate_total{mode,shard}` - Estimated pages reclaimed by vacuum runs
- `blobasaur_vacuum_reclaimed_bytes_estimate_total{mode,shard}` - Estimated bytes reclaimed by vacuum runs
- `blobasaur_vacuum_shard_failures_total{shard,mode,result}` - Per-shard vacuum failure counts, where `result` is `busy` or `error`

## Usage with Prometheus

### 1. Configure Prometheus

Add the following to your `prometheus.yml`:

```yaml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: 'blobasaur'
    static_configs:
      - targets: ['localhost:9090']
    scrape_interval: 5s
    metrics_path: /metrics
```

### 2. Start Prometheus

```bash
prometheus --config.file=prometheus.yml
```

### 3. Access Prometheus UI

Open `http://localhost:9090` in your browser to access the Prometheus UI.

## Example Queries

Here are some useful PromQL queries for monitoring Blobasaur:

### Request Rate
```promql
rate(blobasaur_commands_total[5m])
```

### Error Rate
```promql
rate(blobasaur_errors_total[5m])
```

### Read Hit Rate (lookups that found a value)
```promql
sum(rate(blobasaur_reads_total{result="hit"}[5m])) / sum(rate(blobasaur_reads_total{result=~"hit|miss"}[5m]))
```

### Write backpressure

Queue saturation per shard, as a fraction of the queue capacity (here the default 100):

```promql
max by (shard) (blobasaur_write_queue_depth) / 100
```

95th percentile wait for queue space:

```promql
histogram_quantile(0.95, sum by (le, shard) (rate(blobasaur_write_enqueue_wait_seconds_bucket[5m])))
```

### Async writes lost after acknowledgement

```promql
sum by (shard) (rate(blobasaur_async_write_failures_total[5m]))
```

### SQLite bottlenecks

Writer transaction p95, connection wait p95, and errors by kind:

```promql
histogram_quantile(0.95, sum by (le, shard) (rate(blobasaur_sqlite_transaction_duration_seconds_bucket[5m])))
```

```promql
histogram_quantile(0.95, sum by (le, pool) (rate(blobasaur_sqlite_pool_acquire_seconds_bucket[5m])))
```

```promql
sum by (stage, kind) (rate(blobasaur_sqlite_errors_total[5m]))
```

### Reads answered from the inflight cache

```promql
sum by (command) (rate(blobasaur_reads_total{source="inflight"}[5m])) / sum by (command) (rate(blobasaur_reads_total[5m]))
```

### Expired keys deleted per second

```promql
sum(rate(blobasaur_cleanup_deleted_keys_total[5m]))
```

### Command Latency (95th percentile)
```promql
histogram_quantile(0.95, rate(blobasaur_command_duration_seconds_bucket[5m]))
```

### Value Size (95th percentile, by command)
```promql
histogram_quantile(0.95, sum by (le, command) (rate(blobasaur_value_size_bytes_bucket[5m])))
```

### Bytes Written per Second
```promql
sum(rate(blobasaur_value_size_bytes_sum{command=~"set|hset"}[5m]))
```

### Compression Ratio (stored / uncompressed bytes)
```promql
sum(rate(blobasaur_stored_value_size_bytes_sum[5m])) / sum(rate(blobasaur_value_size_bytes_sum{command=~"set|hset"}[5m]))
```

### Active Connections
```promql
blobasaur_connections_active
```

### Tokio worker utilization

Fraction of time each worker spends processing work, averaged over five minutes:

```promql
rate(blobasaur_tokio_worker_busy_duration_seconds_total[5m])
```

Average utilization across workers for each instance:

```promql
avg by (instance) (rate(blobasaur_tokio_worker_busy_duration_seconds_total[5m]))
```

### Tokio task backlog

```promql
blobasaur_tokio_global_queue_depth
```

This only measures the global queue. A zero value does not rule out work waiting in worker-local queues.

### Top Commands by Volume
```promql
topk(5, rate(blobasaur_commands_get_total[5m]))
```

## Grafana Dashboard

### Sample Dashboard JSON

You can import this basic dashboard configuration into Grafana:

```json
{
  "dashboard": {
    "title": "Blobasaur Metrics",
    "panels": [
      {
        "title": "Commands per Second",
        "type": "graph",
        "targets": [
          {
            "expr": "rate(blobasaur_commands_total[5m])",
            "legendFormat": "Commands/sec"
          }
        ]
      },
      {
        "title": "Read Hit Rate",
        "type": "stat",
        "targets": [
          {
            "expr": "sum(rate(blobasaur_reads_total{result=\"hit\"}[5m])) / sum(rate(blobasaur_reads_total{result=~\"hit|miss\"}[5m]))",
            "legendFormat": "Hit Rate"
          }
        ]
      },
      {
        "title": "Active Connections",
        "type": "stat",
        "targets": [
          {
            "expr": "blobasaur_connections_active",
            "legendFormat": "Active"
          }
        ]
      },
      {
        "title": "Command Latency",
        "type": "graph",
        "targets": [
          {
            "expr": "histogram_quantile(0.95, rate(blobasaur_command_duration_seconds_bucket[5m]))",
            "legendFormat": "95th percentile"
          },
          {
            "expr": "histogram_quantile(0.50, rate(blobasaur_command_duration_seconds_bucket[5m]))",
            "legendFormat": "50th percentile"
          }
        ]
      }
    ]
  }
}
```
