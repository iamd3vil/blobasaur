# Vacuum Maintenance

## Why Vacuum?

Blobasaur stores data in SQLite databases (one per shard). When keys are deleted — either explicitly with `DEL`/`HDEL` or automatically via TTL expiry — SQLite does **not** return disk space to the OS. Instead, freed pages are added to an internal **freelist** for reuse by future writes.

Over time, especially in workloads with heavy churn (frequent writes and deletes), the freelist grows and the database files remain larger than necessary. Vacuum reclaims these freelist pages, shrinking the on-disk footprint.

## Vacuum Modes

### Incremental (`--mode incremental`)

Reclaims freelist pages **up to a budget**, without rewriting the entire database. This is the default and recommended mode for routine maintenance.

- Runs `PRAGMA incremental_vacuum(N)` where `N` is derived from `--budget-mb` and the shard's page size.
- Only reclaims pages already on the freelist — fast and bounded.
- Does **not** defragment or compact the database.
- Safe to run during production traffic (though a brief WAL checkpoint is performed first).

### Full (`--mode full`)

Rewrites the **entire** database file, reclaiming all freelist pages and defragmenting storage.

- Runs `VACUUM`, which rebuilds the database from scratch into a new file.
- Reclaims **all** unused space, not just up to a budget.
- Significantly more expensive: requires temporary disk space equal to the database size and holds an exclusive lock.
- Best used during maintenance windows or after bulk deletions.

### Which mode to use?

| Scenario | Recommended mode |
|---|---|
| Routine daily/weekly maintenance | `incremental` |
| After a large bulk delete | `full` |
| Disk pressure, need to reclaim space quickly | `incremental` with a large `--budget-mb` |
| One-time compaction during maintenance window | `full` |

## Budget (`--budget-mb`)

The `--budget-mb` flag (default: `128`) controls how many freelist pages are reclaimed per shard during **incremental** vacuum. The budget is converted to a page count based on the shard's SQLite page size (typically 4096 bytes):

```
pages_to_vacuum = min(freelist_count, budget_mb * 1048576 / page_size)
```

For example, with the default 128 MB budget and 4 KB pages, up to ~32,768 freelist pages are reclaimed per shard.

The budget is **ignored** in `full` mode — a full vacuum always reclaims everything.

## Dry Run (`--dry-run`)

Adding `--dry-run` makes the operation **non-mutating**. It collects per-shard vacuum stats (page size, page count, freelist count) and estimates how many pages/bytes would be reclaimed, without actually running the vacuum. Use this to assess how much space can be recovered before committing to a real run.

## CLI Usage

The CLI orchestrator dispatches vacuum commands to one or more Blobasaur nodes over the Redis protocol.

### Synopsis

```
blobasaur shard vacuum --all-shards [OPTIONS]
```

### Options

| Flag | Default | Description |
|---|---|---|
| `--all-shards` | *(required)* | Target all local shards on each node |
| `--mode` | `incremental` | Vacuum mode: `incremental` or `full` |
| `--budget-mb` | `128` | Per-shard budget in MB (incremental mode) |
| `--nodes` | *(from config)* | Comma-separated node addresses (`host:port`) |
| `--dry-run` | `false` | Only calculate and report vacuum effects |
| `--node-concurrency` | `1` | Max parallel node calls |
| `--shard-concurrency` | `1` | Reserved for future use (currently serial) |
| `--timeout-sec` | `30` | Per-node network timeout in seconds |

### Node Resolution

When `--nodes` is omitted, the CLI resolves target nodes from your `config.toml`:

1. If `[cluster].advertise_addr` is set, use that (the publicly reachable address).
2. Otherwise, use `addr` from the top-level config.
3. If neither is set, fall back to `127.0.0.1:6379`.

Bind addresses like `0.0.0.0:PORT` are automatically normalized to `127.0.0.1:PORT`.

### Examples

**Dry run to assess reclaimable space:**

```bash
blobasaur shard vacuum --all-shards --dry-run
```

**Routine incremental vacuum with defaults (128 MB budget):**

```bash
blobasaur shard vacuum --all-shards
```

**Incremental vacuum with a larger budget:**

```bash
blobasaur shard vacuum --all-shards --budget-mb 512
```

**Full vacuum during a maintenance window:**

```bash
blobasaur shard vacuum --all-shards --mode full --timeout-sec 300
```

**Multi-node orchestration:**

```bash
blobasaur shard vacuum --all-shards \
  --nodes 10.0.0.1:6379,10.0.0.2:6379,10.0.0.3:6379 \
  --node-concurrency 2
```

### Example Output

A successful incremental dry run on a 4-shard single node:

```
Vacuum orchestration started
  mode: incremental
  budget_mb: 128
  dry_run: true
  node_concurrency: 1
  shard_concurrency: 1
  timeout_sec: 30
  nodes: 127.0.0.1:6379

Vacuum orchestration report:
- node 127.0.0.1:6379: connected (12 ms), mode=incremental, budget_mb=128, dry_run=true
    shard 0 -> status=ok, duration_ms=2, incremental_pages_requested=1024, estimated_reclaimed_pages=1024
    shard 1 -> status=ok, duration_ms=1, incremental_pages_requested=0, estimated_reclaimed_pages=0
    shard 2 -> status=ok, duration_ms=1, incremental_pages_requested=512, estimated_reclaimed_pages=512
    shard 3 -> status=ok, duration_ms=1, incremental_pages_requested=0, estimated_reclaimed_pages=0
Summary: node_failures=0, shard_successes=4, shard_failures=0
```

Key fields in the per-shard output:

| Field | Meaning |
|---|---|
| `status` | `ok`, `execution_error`, `cancelled`, or `invalid_shard` |
| `duration_ms` | Wall-clock time for that shard's vacuum |
| `incremental_pages_requested` | Pages targeted for reclamation (incremental mode) |
| `estimated_reclaimed_pages` | Actual (or estimated, in dry-run) pages reclaimed |
| `error` | Error message if status is not `ok` |
| `execution_errors` | Individual SQLite errors encountered during the operation |

### Exit Code

The CLI exits **non-zero** if any node call fails or any shard result has a status other than `ok`.

## Server Admin Command

The vacuum can also be invoked directly against a running Blobasaur node using any Redis client:

```
BLOBASAUR.VACUUM SHARD <id|ALL> MODE <incremental|full> BUDGET_MB <n> [DRYRUN]
```

Examples:

```bash
# Vacuum all shards incrementally
redis-cli BLOBASAUR.VACUUM SHARD ALL MODE incremental BUDGET_MB 128

# Vacuum a single shard
redis-cli BLOBASAUR.VACUUM SHARD 3 MODE incremental BUDGET_MB 256

# Full vacuum with dry run
redis-cli BLOBASAUR.VACUUM SHARD ALL MODE full BUDGET_MB 0 DRYRUN
```

The response is a RESP array containing per-shard results in the same format as the CLI output.

**Note:** `BUDGET_MB` must be greater than 0 (even for full mode where the value is ignored — pass any positive integer).

## Execution Details

For each shard, the vacuum operation performs the following steps:

1. **Collect pre-vacuum stats** — reads `PRAGMA page_size`, `PRAGMA page_count`, and `PRAGMA freelist_count`.
2. **WAL checkpoint** — runs `PRAGMA wal_checkpoint(TRUNCATE)` to flush the WAL into the main database file.
3. **Vacuum** (skipped in dry-run):
   - *Incremental:* runs `PRAGMA incremental_vacuum(N)` where `N` = `min(freelist_count, budget_bytes / page_size)`.
   - *Full:* runs `VACUUM` to rebuild the entire database.
4. **Collect post-vacuum stats** — re-reads page/freelist counts.
5. **Report** — calculates estimated reclaimed pages and bytes.

Shards on a given node are processed **serially** (one at a time) to avoid overwhelming disk I/O. Multiple nodes can be vacuumed in parallel using `--node-concurrency`.

## Startup Auto-Vacuum Upgrade

Newly created shard databases are initialized with `PRAGMA auto_vacuum = INCREMENTAL`, which enables SQLite's built-in incremental auto-vacuum support. However, shards created before this feature was added may have `auto_vacuum = NONE` (the SQLite default).

On startup, Blobasaur automatically detects and upgrades legacy shards:

### Upgrade Flow

For each shard database (`shard_0.db` through `shard_{N-1}.db`):

1. Read `PRAGMA auto_vacuum`.
2. If already `INCREMENTAL` (value `2`), skip — no work needed.
3. If not `INCREMENTAL` and auto-upgrade is **disabled**, log a warning and continue.
4. If not `INCREMENTAL` and auto-upgrade is **enabled** (default), run:
   - `PRAGMA wal_checkpoint(TRUNCATE);`
   - `PRAGMA auto_vacuum = INCREMENTAL;`
   - `VACUUM;`
5. Verify the mode is now `INCREMENTAL`.
6. **Fail fast** if any shard's upgrade fails, to avoid mixed maintenance state.

### Configuration

```toml
[sqlite]
# Enable/disable startup auto-vacuum upgrade (default: true)
auto_upgrade_legacy_auto_vacuum = true

# Max concurrent shard upgrades at startup (default: 2)
# Set to 1 for HDD or conservative environments
auto_upgrade_legacy_auto_vacuum_concurrency = 2
```

### Notes

- This is a **one-time** conversion per shard database under normal operation.
- The `VACUUM` step rewrites the database, so ensure sufficient free disk space (roughly equal to the largest shard).
- Upgrade concurrency defaults to `2`, tuned for NVMe hosts. Use `1` for spinning disks.

## Operational Recommendations

### Routine Maintenance

Run incremental vacuum on a regular schedule (daily or weekly, depending on churn):

```bash
# Cron: daily at 3 AM, incremental vacuum with 256 MB budget
0 3 * * * /usr/local/bin/blobasaur shard vacuum --all-shards --budget-mb 256
```

### Before Running Vacuum

1. **Dry run first** to understand how much space is reclaimable:
   ```bash
   blobasaur shard vacuum --all-shards --dry-run
   ```
2. **Check disk space** — full vacuum needs temporary space equal to the largest shard DB.
3. **Increase timeout for full vacuum** — large databases can take minutes:
   ```bash
   blobasaur shard vacuum --all-shards --mode full --timeout-sec 600
   ```

### Monitoring

Blobasaur exposes Prometheus metrics for vacuum operations. See [METRICS.md](METRICS.md) for the full list. Key metrics:

- `blobasaur_vacuum_runs_total{mode,result}` — vacuum attempts by mode and outcome
- `blobasaur_vacuum_duration_seconds{mode,result}` — vacuum duration histogram
- `blobasaur_vacuum_reclaimed_pages_estimate_total{mode,shard}` — estimated pages reclaimed
- `blobasaur_vacuum_reclaimed_bytes_estimate_total{mode,shard}` — estimated bytes reclaimed
- `blobasaur_vacuum_shard_failures_total{shard,mode,result}` — per-shard failure counts (`busy` or `error`)

### Alerting Suggestions

- Alert on `blobasaur_vacuum_shard_failures_total` increasing — indicates shards failing to vacuum (possibly due to lock contention or disk issues).
- Track `blobasaur_vacuum_reclaimed_bytes_estimate_total` over time to gauge churn and right-size your vacuum schedule.
