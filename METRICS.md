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

### Cache Metrics

- `blobasaur_cache_hits_total` - Total cache hits (data found in memory/storage)
- `blobasaur_cache_misses_total` - Total cache misses (data not found)

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

### Vacuum Metrics

- `blobasaur_vacuum_runs_total{mode,result}` - Total shard vacuum attempts by vacuum mode and outcome (`ok`, `error`, `timeout`, `cancelled`, etc.)
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

### Cache Hit Rate
```promql
rate(blobasaur_cache_hits_total[5m]) / (rate(blobasaur_cache_hits_total[5m]) + rate(blobasaur_cache_misses_total[5m]))
```

### Command Latency (95th percentile)
```promql
histogram_quantile(0.95, rate(blobasaur_command_duration_seconds_bucket[5m]))
```

### Active Connections
```promql
blobasaur_connections_active
```

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
        "title": "Cache Hit Rate",
        "type": "stat",
        "targets": [
          {
            "expr": "rate(blobasaur_cache_hits_total[5m]) / (rate(blobasaur_cache_hits_total[5m]) + rate(blobasaur_cache_misses_total[5m]))",
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
