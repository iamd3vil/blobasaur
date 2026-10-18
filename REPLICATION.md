# Replication Design

Status: implemented.

## Goals

- **Disaster recovery**: a replica holds a live copy of its primary's data.
- **Read scaling**: cluster clients can read from replicas (`READONLY`).
- **Hot standby**: a replica can be promoted to take over its primary's slots.

## Non-goals (for now)

- Synchronous or quorum replication. Replication is asynchronous: on failover,
  writes acknowledged by the primary but not yet replicated are lost. This is
  accepted, and matches `async_write = true`, which already acknowledges before
  commit.
- Automatic failover. Promotion is manual (config edit and restart), and fencing
  the old primary is the operator's job.
- Partial resync across a change of primary. Every promotion makes all other
  nodes of that slot range resync from scratch.
- Standalone (non-cluster) replication. Blobasaur replicas are cluster members.
- AUTH/TLS on the replication stream. The network is assumed trusted.

## Constraints from the current code

- **One writer per shard.** The batch writer and expiry cleanup share the
  shard's single write connection (`src/server.rs`), so commits on a shard are
  serialized. A per-shard sequence number assigned inside the write
  transaction therefore commits in order, with no gaps or reordering.
- **Writes are not deterministic.** The writer stamps `created_at` /
  `updated_at` with `Utc::now()` and bumps `version`. Replaying commands on a
  replica would produce different rows, so replicas receive row images, not
  commands.
- **Shard mapping depends on `num_shards`** (an mpchash ring over the key), and
  stored values are compressed with no self-describing header. A replica must
  have the same `num_shards` and storage compression settings as its primary.
- **No multi-key atomicity.** Batches are transactions for throughput, not a
  client guarantee. Replicas need per-key ordering only, not a consistent
  snapshot of the whole shard.
- **Per-op failures don't abort a batch.** `process_batch` records a failed
  statement against its op and still commits the rest. Anything that must be
  atomic with an op's data change needs its own savepoint.

## Overview

```
client SET ─► shard writer txn ─┬─ upsert blobs_* row          ┐ one savepoint
                                └─ INSERT OR REPLACE repl_log   ┘ per op
              commit ─► watch<u64> ─► streamer (one per replica per shard)
                                       scan repl_log WHERE lsn > cursor ORDER BY lsn
                                       + current rows, same read txn
                                       ──RESP──► replica: Apply op → its writer task
                                                 (rows verbatim + repl_log + applied_lsn,
                                                  one txn, all-or-nothing)
```

Replication is state-based: each shard DB keeps a change index with one row per
key, recording the LSN of that key's last change. The primary streams the
*current* state of each changed key in LSN order. A replica pulls from its
primary, applies rows verbatim through its own shard writer, and records its
progress in the same transaction.

Because entries for rewritten keys move forward, the stream is coalesced: a
replica part-way through the stream does not hold a prefix of the primary's
history. Its state only converges once its cursor reaches the primary's
watermark. This is why there is no partial resync across a change of primary.

## Change index

Created in every shard DB, primary and replica alike:

```sql
CREATE TABLE IF NOT EXISTS repl_log (
  lsn     INTEGER PRIMARY KEY AUTOINCREMENT,
  tbl     TEXT NOT NULL,          -- 'blobs' or 'blobs_<namespace>'
  key     TEXT NOT NULL,
  deleted INTEGER NOT NULL,       -- 1 = tombstone
  at      INTEGER NOT NULL,       -- unix seconds, for tombstone trimming
  UNIQUE (tbl, key)
);

CREATE TABLE IF NOT EXISTS repl_state (
  k TEXT PRIMARY KEY,             -- role, replid, applied_lsn, caught_up_lsn,
  v                               -- synced, trimmed_through, indexed,
                                  -- boot_id, clean_shutdown
);
```

Properties:

- A key written 1000 times has one `repl_log` row. Blob values are never stored
  twice; only keys are duplicated (keys are small in our workloads).
- The log lives in the same DB as the data and each op writes both under one
  savepoint, so the two cannot drift apart, including across crashes.
- A replica starting from `lsn = 0` receives every live row. Initial sync and
  catch-up are the same code path; there is no separate snapshot mechanism.

Required details:

- **`AUTOINCREMENT` is mandatory.** Without it SQLite reuses the max rowid after
  that row is deleted, and a replica whose cursor sits at that value would skip
  the new entry.
- **Use `INSERT OR REPLACE`, not `ON CONFLICT DO UPDATE`.** REPLACE deletes and
  re-inserts, assigning a new LSN. An upsert keeps the old rowid, so the change
  would never be streamed.
- `lsn` is an explicit `INTEGER PRIMARY KEY`, so `VACUUM` preserves it. Each
  node vacuums independently.

### Backfill of existing data

Existing shard DBs already hold rows the index has never seen. On startup,
before the writer, cleanup, or replication tasks start, each shard runs, per
table (`blobs` and every `blobs_*`):

```sql
INSERT OR IGNORE INTO repl_log (tbl, key, deleted, at)
SELECT '<tbl>', key, 0, <now> FROM <tbl>;
```

then sets `repl_state.indexed = 1`. `INSERT OR IGNORE` makes a crash part-way
safe to rerun; the marker skips it on later starts.

### Durability

Primaries stream committed transactions immediately. With
`synchronous = NORMAL` (the current default), an OS crash or power loss can roll
back transactions that were already streamed, and the primary would then reuse
those LSNs for different writes under the same replid. Replicas would skip the
new entries or keep the discarded values.

Rather than fsync every commit (`synchronous = FULL`), a primary detects when
it may have lost commits and changes its `replid`, which makes its replicas
resync. Commits can only be lost if the OS stopped without flushing the page
cache. A process crash (OOM, `kill -9`, panic) loses nothing: WAL writes are
already in the kernel and reach disk anyway.

Each shard's `repl_state` holds `boot_id` and `clean_shutdown`:

- **Startup**, before the writer starts: read the kernel boot id
  (`/proc/sys/kernel/random/boot_id`, also visible inside containers). Generate
  a new `replid` if the stored `boot_id` differs **and** `clean_shutdown` is 0.
  Then store the current `boot_id` and `clean_shutdown = 0`.
- **Graceful shutdown**: set `clean_shutdown = 1` only once nothing else can
  commit to the shard. The writer has drained, the cleanup task has been
  stopped *and awaited* (today it is aborted without waiting, after the
  writers), and the shard's pools are closed. No commit may follow the
  marker. On a drain timeout or any shutdown error, the marker stays 0.

Both marker writes go through a short-lived connection opened with
`synchronous = FULL`, so the markers themselves survive power loss. Without
that, a lost `clean_shutdown = 0` would leave the previous run's `1` in place
and the check would pass wrongly.

| Event | Boot id | Clean | Result |
|---|---|---|---|
| Graceful restart or planned reboot | either | 1 | keep `replid` |
| Process crash | same | 0 | keep `replid` |
| OS crash or power loss | changed | 0 | new `replid`; replicas resync |

If the boot id can't be read (non-Linux), the check falls back to the marker
alone: any unclean stop changes the `replid`.

The check doesn't catch a primary whose data directory was restored from an
older backup or disk snapshot, since that looks like a clean stop. Start a
restored primary once with `--new-replid`. As a second safety net, the primary
answers `RESYNC` to any cursor above its committed watermark.

Only primaries need this. Apply writes rows, `applied_lsn` and `caught_up_lsn`
in one transaction, so a replica that loses commits rolls all three back
together and simply re-fetches from the older cursor. Promotion already
generates a new `replid`.

## Primary write path

In `process_batch` (`src/shard_manager.rs`), each op runs inside
`SAVEPOINT op` / `RELEASE op`, and on any statement error rolls back to the
savepoint. The data change and its `repl_log` row therefore commit together or
not at all, while other ops in the batch keep today's per-op failure handling.

| Op | `repl_log` entry |
|---|---|
| SET, HSET, HSETEX | live (`deleted = 0`) |
| DEL, HDEL (row existed) | tombstone (`deleted = 1`) |
| EXPIRE (row updated) | live |
| HEXPIRE / HEXPIREAT result 1 (TTL set) | live |
| HEXPIRE / HEXPIREAT result 2 (immediate expiry, field deleted) | tombstone |
| HEXPIRE / HEXPIREAT result 0 / -2 (condition not met, no field) | none |

After every committed transaction that wrote `repl_log` entries (writer
batches and cleanup chunks alike), the committing task publishes the shard's
highest LSN on a per-shard `tokio::sync::watch` channel with `send_if_modified`
keeping the value monotonic.

Namespace tables are created lazily inside the batch transaction, and
`ensure_namespaced_table_exists` adds the name to `known_tables` straight away,
which later ops in the same batch (HDEL, HEXPIRE) rely on. The writer also
records which names it added during the current batch. If the savepoint of the
op that created a table rolls back, that name is removed from `known_tables`;
if the commit fails, every name added in the batch is removed. The cache then
never claims a table that doesn't exist, without re-querying `sqlite_master`
(the pool-based `load_existing_tables` can't run while the batch holds the
shard's only writer connection).

## Expiry cleanup

`shard_cleanup_task` changes each chunk to one transaction that runs
`DELETE … RETURNING key` and writes a tombstone for every deleted key.

Cleanup deletes must be replicated. A replica that is behind may hold a key
without its later `EXPIRE`; if the primary then cleaned it up without a
tombstone, the replica would keep the key forever.

Replicas run the same cleanup locally on the `expires_at` values they received,
so expired keys disappear on time even under lag. Committed-row reads already
filter on `expires_at`. (The inflight cache in front of GET/HGET does not, but
Apply never goes through the inflight caches.)

Consequence: primary and replica clocks differ, so a key may expire a few
seconds earlier or later on a replica.

## Replication protocol

Runs on the existing Redis port with RESP framing. The replica opens one
connection per shard.

Handshake, replica → primary:

```
BLOBASAUR.SYNC <shard> <replid> <lsn> <num_shards> <compression>
```

Primary → replica:

- `-ERR ...` if `num_shards` or compression settings differ. The replica logs
  and retries with backoff; it does not wipe anything.
- `-ERR ...` if this node is itself a replica. There is no chained
  replication: a replica's LSNs are its own, not its primary's.
- `+CONTINUE <replid> <watermark>` if `replid` matches and either
  `lsn == 0` (a full copy into a freshly reset shard; it holds nothing an old
  tombstone could apply to) or `trimmed_through <= lsn <= watermark`.
- `+RESYNC <replid> <watermark>` otherwise. The replica resets the shard (see
  [Reset](#reset)), which stores the new `replid`, and reconnects with
  `lsn = 0`.

The replid and cursor check and the stream's pin registration (see
[Tombstone trimming](#tombstone-trimming)) happen under the same per-shard lock
that trimming takes, so no tombstone above the cursor can be trimmed between
the check and the start of streaming.

Stream frames, primary → replica:

```
[lsn, tbl, key, created_at, updated_at, expires_at, version, data]   # row
[lsn, tbl, key]                                                      # delete
[watermark, primary_unix_ms]                                         # caught up
```

- `data` is sent as stored (already compressed).
- The streamer scans immediately on connect, then rescans on each `watch`
  wake-up, and at least every 10 s when idle. A scan reads pages of `repl_log`
  entries and their current rows, each page in one read transaction, capped by
  bytes as well as row count.
- When a page reaches the head of the log (including an empty page), the
  streamer sends `[watermark]`: the shard's highest assigned LSN
  (`sqlite_sequence`), read in the same read transaction as that page. Every
  entry up to it has therefore been sent, so it is
  never below the cursor. `primary_unix_ms` is the primary's clock at that
  read. This is how an empty or idle shard reports that it is caught up, and
  the idle rescan makes it a heartbeat.
- Every socket write has a timeout (30 s). A replica that stops reading has its
  stream closed and its pin released; it reconnects through the normal
  handshake.

There are no ACKs. A watermark frame tells the replica it was fully caught up
as of that moment; see [Observability](#observability).

## Replica apply

A new `ShardWriteOperation::Apply { frames }` goes through the replica's
existing shard writer task. In one transaction it:

1. creates any missing namespace table (same helper as HSET);
2. upserts each row verbatim (timestamps and `version` from the primary), or
   deletes it;
3. writes the matching `repl_log` entries with the replica's own LSNs;
4. sets `applied_lsn` to the page's last LSN; for a `[watermark]` frame, also
   sets `caught_up_lsn` to the watermark and `synced = 1`.

`applied_lsn` is only a resume cursor. Because the stream is coalesced, a
replica whose `applied_lsn` has passed some LSN may still miss changes that
moved past it (for example a cleanup tombstone that replaced an earlier entry).
Only `caught_up_lsn` means "state matches the primary as of this LSN".

Any failure rolls back the whole Apply, and the replica reconnects from its
last committed `applied_lsn`. A replica restart resumes exactly from there.

The replica's own `repl_log` is what lets it serve as a primary immediately
after promotion: it already indexes every live key.

### Reset

`RESYNC` is applied as a `ShardWriteOperation::Reset` through the writer queue,
so it is ordered after any queued Apply. In one transaction it deletes all rows
from `blobs` and every `blobs_*` table, clears `repl_log`, sets `applied_lsn = 0`,
deletes `caught_up_lsn` and the last watermark time, stores the new `replid`,
and sets `synced = 0`. The shard stops serving replica
reads before the reset is queued. A per-shard gate orders this against reads:
a replica's READONLY read holds it shared from before its route check until its
DB read is done, and the sync task holds it exclusive from clearing the synced
flag until the reset has committed. A read admitted while synced finishes
before the wipe; a later one is redirected.

## Roles and promotion state

`repl_state.role` is persisted per shard (`primary` or `replica`). On startup:

| Config | Stored role | Action |
|---|---|---|
| primary | none | new node: generate `replid` |
| primary | primary | normal restart: keep `replid`, unless the [boot-id check](#durability) says commits may be lost |
| primary | replica | promotion: refuse unless every shard has `synced = 1`; then generate a new `replid` and set `role = primary` |
| replica | any | sync from the configured primary |

Promotion always generates a new `replid`, so every other node of that slot
range, including a returning old primary, gets `RESYNC`.

## Tombstone trimming

Tombstones older than `tombstone_retention_secs` are trimmed by the cleanup
task, but never at or above the cursor of any active stream on that shard. Each
active stream pins its own history at its current cursor; the pin moves forward
as the stream sends. Tombstones below a stream's cursor have already been sent,
so trimming can never invalidate a running stream. Trimming and the
`trimmed_through` update happen in one transaction, under the per-shard lock
shared with the handshake.

A replica that was disconnected for longer than the retention period gets
`RESYNC` when it reconnects. A connected replica that stops reading is cut off
by the socket write timeout, so it can't pin history indefinitely. No replica
membership tracking is needed.

## Cluster integration

### Configuration

```toml
# Replica of node-1
[cluster]
enabled = true
node_id = "node-1b"
# no slot_ranges / slots

[replication]
replica_of = "node-1"            # node id; address resolved via gossip
tombstone_retention_secs = 86400
```

If cluster initialization fails on a node with replication configured, startup
fails. Today it logs the error and runs without a cluster manager.

### Gossip

`NodeGossipData` gains, both `#[serde(default)]` for rolling upgrades:

- `role`: `primary` | `replica-syncing` | `replica`
- `primary`: `Option<String>`, the node id being replicated
- `slot_ranges`: the node's slots as `[start, end]` ranges. One UDP datagram
  can't carry a list of all 16384 slots, so ranges are the primary form; the
  legacy `slots` list is still emitted when the node has at most 8192 slots.

Replicas gossip no slots, so slot ownership logic is unaffected. The replica's
sync task resolves the primary's address from gossip and reconnects when it
changes. A replica reports `replica-syncing` until every shard has
`synced = 1`, then `replica`.

### Topology commands

- `CLUSTER NODES`: replicas appear as `slave <primary-id>`.
- `CLUSTER SLOTS`: lists every live node's ranges, each with the primary
  followed by its replicas in `replica` role, as `[ip, port, node_id]`.
  Replicas in `replica-syncing` are not listed.

### Command metadata

go-redis sends a command to a replica only when its `COMMAND` metadata marks it
read-only, and it computes the slot from the metadata's first-key position.
`COMMAND` currently returns an empty array.

`COMMAND` (and `COMMAND INFO`) must return name, arity, flags (`readonly` or
`write`), and first/last key positions and step for every supported command.

- HGET, HEXISTS, HSET, HDEL: key position **2** (the field), because Blobasaur
  routes hash commands by field, not namespace. This also removes today's
  `MOVED` round trip for these commands, where go-redis hashes the namespace.
- HSETEX, HEXPIRE, HEXPIREAT: the field position varies with options, so they
  keep key position 1. They are writes, go to a primary, and may take one
  `MOVED` round trip, as today.

Limitation: Blobasaur hashes the whole field (`KeyBased`), while go-redis
applies hash-tag extraction. For a field containing `{…}` the two can pick
different slots, so a replica read of such a field is redirected to the
primary. It still succeeds. Aligning field hashing with hash tags changes slots
for existing tagged fields and needs a data move, so it is out of scope here.

### Request routing

The repeated `should_handle_locally` / `get_redirect_response` checks in
`src/server.rs` are replaced by one method:

```rust
enum Route { Local, Moved(String), ClusterDown }
cluster_manager.route(slot, is_read, conn_readonly) -> Route
```

- **Primary**: `Local` for its own slots, `Moved` to the live owner otherwise.
- **Replica**: `Local` if the connection sent `READONLY`, the command is a read
  (GET, EXISTS, TTL, HGET, HEXISTS), the slot belongs to its primary, and the
  replica is in `replica` role. Otherwise `Moved` to the owner.
- **No known live owner**: `ClusterDown` (`-CLUSTERDOWN`). Today the request
  falls through and runs locally, because `get_node_for_slot` returns `None`
  both for "mine" and for "nobody's".

`READONLY` / `READWRITE` set a per-connection flag. Replica reads are eventually
consistent; there is no read-your-writes guarantee.

## Failover (manual)

### Unplanned (primary lost)

1. **Fence the old primary**: make sure it is stopped and cannot restart with
   its old config (stop the unit, block its port, or take the host down). This
   is mandatory. Nothing in the cluster prevents two nodes claiming the same
   slots.
2. Edit `node-1b`'s config: add `node-1`'s `slot_ranges`, remove `replica_of`.
   Restart it. It promotes (see [Roles and promotion state](#roles-and-promotion-state))
   and generates a new `replid`.
3. Point every other replica of `node-1` at `node-1b` (`replica_of = "node-1b"`,
   restart). Each gets `RESYNC` and re-copies.
4. To bring `node-1` back, give it `replica_of = "node-1b"` and no slots. It
   gets `RESYNC` on every shard and re-copies from `node-1b`. Writes it had not
   replicated are discarded.

### Planned (maintenance)

1. Stop client writes to the primary's slots at the application level.
2. Run `BLOBASAUR.WATERMARK` on the primary. It queues a barrier op through
   every shard's writer and, once each barrier returns, reports each shard's
   highest LSN. Every write acknowledged before the call (including
   `async_write` ones still queued) is committed and has an LSN by then.
3. Run `BLOBASAUR.WATERMARK` on the replica until each shard's
   `caught_up_lsn` is at least the primary's value, under the same `replid`.
   The primary keeps running meanwhile; idle heartbeats deliver the watermark.
4. Stop and fence the primary, then continue from step 2 of the unplanned
   procedure. No acknowledged writes are lost.

`BLOBASAUR.WATERMARK` returns `[shard, replid, lsn]` per shard: the committed
LSN after a writer barrier on a primary, `caught_up_lsn` on a replica. A
replica shard with `synced = 0` reports `not-synced` instead of an LSN, so a
reset shard can never look caught up, even to an empty primary at LSN 0. The barrier is a new
`ShardWriteOperation::Barrier { responder }` that the writer answers after
committing everything queued ahead of it.

### Startup guard

A node starting with slots waits a few gossip intervals before advertising
them. If a live node already claims any of those slots, it refuses to start.
This is **best-effort conflict detection**, not fencing: a partitioned node or
two nodes starting at once can both pass.

## Observability

Per shard:

- replica: **staleness seconds**: replica clock minus `primary_unix_ms` of the
  last applied watermark. Idle streams heartbeat a watermark every 10 s, so
  this keeps rising if the stream stalls or the replica's writer falls behind
  (a queued watermark doesn't reset it to zero; it carries the primary's time).
  It assumes NTP-synced clocks. This is the DR alert signal. Also
  `applied_lsn`, `caught_up_lsn`, resyncs total, rows and bytes applied.
- primary: active streams, latest LSN.
- role (gauge).

LSN lag is the primary's latest LSN minus the replica's `caught_up_lsn` for
the same shard and `replid`, compared across nodes in Prometheus.

## Prerequisite fixes in existing cluster code

1. **`CLUSTER SLOTS` returns only local slots.** It must return the full
   topology for clients to route reads to replicas.
2. **Gossip keeps dead nodes' slots.** `gossip_task` iterates `node_states()`,
   which includes dead nodes, for up to the deletion grace period. After
   failover other nodes could keep redirecting to the dead primary. Only
   `live_nodes()` should count as slot owners.
3. **Gossip generation is always 0.** `ClusterManager::new` sets
   `generation_id: 0`, and chitchat ignores already-seen versions from the same
   generation, so peers can keep a restarted node's old role and slots. Use the
   startup time as the generation, and when several generations of one node id
   are present, use the newest.
4. **Gossip advertises the Redis address as its UDP endpoint.**
   `ClusterManager::new` parses `advertise_addr` (the Redis address) as the
   chitchat advertise address. It should be the advertised host with
   `cluster.port`; the Redis address stays in `NodeGossipData.addr`.
5. **Unknown slot ownership runs locally.** See
   [Request routing](#request-routing).

## Known issues to fix separately

- `EXPIRE` runs `UPDATE … WHERE key = ?` with no expiry filter, so it can
  revive a key that expired but has not been cleaned up yet. Replication would
  copy that faithfully.
- Inflight reads (async writes not yet committed) ignore `expires_at`.

## Not a backup

Replication propagates accidental deletes within milliseconds. Keep separate
backups (periodic `VACUUM INTO`, or Litestream to object storage).

## Work plan

Each step is one PR.

1. **Cluster fixes**: the five prerequisites above, plus `COMMAND` metadata.
2. **Change index**: `repl_log` / `repl_state`, backfill, per-op savepoints,
   HEXPIRE mapping, cleanup tombstones, `known_tables` undo on rollback,
   commit `watch` from writer and cleanup, boot-id / clean-shutdown check and
   `--new-replid`. No client-visible change. Tests: LSNs strictly increase, one
   row per key, failed ops leave no log entry, cleanup writes tombstones,
   backfill indexes pre-existing rows, the boot-id check changes `replid` only
   for a changed boot id with an unclean stop.
3. **Sync stream**: `BLOBASAUR.SYNC` streamer, replica sync task, Apply and
   Reset ops, watermark and heartbeat frames, trimming with stream pins, socket
   write timeouts, handshake checks, metrics, and the roles and promotion
   state (persisted role, promotion on startup). The streamer sends a
   watermark whenever a page reaches the head of the log, not only on an empty
   page. Phase 3 also found the gossip slot-list size limit (a node with all
   16384 slots overflows one datagram); step 4 fixes it with slot ranges in
   gossip. Integration tests: primary plus
   replica converge, including namespaced tables and an empty shard; replica
   restart resumes from `applied_lsn`; a replica behind `trimmed_through`
   resyncs and then completes a full copy; a key expired and cleaned up while
   the replica is behind is deleted on the replica.
4. **Cluster roles**: gossip fields, `CLUSTER NODES` / `SLOTS` with replicas,
   `READONLY` / `READWRITE`, `route()`, read gating during `replica-syncing`.
   Test: go-redis with `ReadOnly: true` reads from the replica.
5. **Failover**: `BLOBASAUR.WATERMARK` with the writer barrier op, startup
   slot-conflict guard, failover runbook in `CLUSTERING.md`. (Persisted role and
   promotion on startup landed in step 3.)

## Alternatives considered

| Approach | Why not |
|---|---|
| Litestream (WAL to object storage) | Backup and restore only, not a serving replica. Still recommended as the backup. |
| WAL frame shipping (LiteFS-style) | sqlx owns connections; checkpoints must be coordinated; a primary VACUUM re-ships the whole DB; replicas can't vacuum independently. |
| SQLite session extension changesets | Fits batch transactions well, but sqlx doesn't expose it; the writer would move to rusqlite. |
| Op-log replay (`ShardWriteOperation`) | Not deterministic (`now()`, existence checks, HEXPIRE conditions); stores values twice; needs a separate snapshot path. |
| `lsn` column on each `blobs*` table plus a tombstone table | Avoids duplicating keys, but the stream must merge N dynamic tables in LSN order. Keys are small, so not worth it. |
| Raft per slot range | Strong consistency and automatic failover, but a quorum round trip per batch and far more code. Revisit if auto-failover becomes a requirement. |
