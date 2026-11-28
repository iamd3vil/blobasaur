# Blobasaur TLA+ Specifications

Formal models of Blobasaur's concurrency and distributed protocols, checked with [TLC](https://github.com/tlaplus/tlaplus).

## 1. `AsyncWrite.tla`

Models the `async_write = true` read/write/expiry path across `src/server.rs` and `src/shard_manager.rs`:

- **Inflight cache & tokens**: `Pending::Value { data, expires_at, token }`, `Pending::Deleted(token)`, `queue_pending`, and token-checked `clear_pending_entries`.
- **Key locks**: `state.key_locks` acquired by `DEL` and `EXPIRE` (not by `SET`, `GET`, or `TTL`).
- **Sync `EXPIRE` coordination**:
  - `queue_expire` atomic enqueue-time cache updates (`AllowExpireInflightUpdate`).
  - `conflicts_with_async_in_batch` per-key batch barrier (`AllowExpireBatchBarrier`), which also represents the `HEXPIRE` path.
- **Two-phase batch commit**: separates SQLite `tx.commit()` (`WriterCommitBatch`) from `clear_pending_entries` + sync responder replies (`WriterClearAndReply`), exposing the visibility window behind #65.
- **Logical clock (`now`) & expiry**: checks `GET`, `TTL`, `DEL`, `SET ... EX`, and `EXPIRE` across clock ticks.
- **Background expiry cleanup & cached key count**: `CleanupExpired` (`delete_expired` skipping inflight keys) and `keyCount` (`shard_key_counts`, updated from per-batch `rows_affected` deltas only after commit and decremented per cleanup chunk).
- **Graceful shutdown drain**: `TriggerShutdown` + draining `queue` and `deferred` before `WriterStop`.

### Properties verified
- **Safety**: `TypeOK`, `Linearizable` (refinement proof via ghost `absDb` + per-read LP window), `KeyCountAccurate` (`keyCount >= 0` and equals SQLite stored-row count), `QuiescentConsistency`, `LockSafety`, deadlock freedom.
- **Liveness**: `PendingOpEventuallyCommits`, `ExpireEventuallyReplies`, `ShutdownEventuallyDrains`.

## 2. `ClusterGossip.tla`

Models Blobasaur's cluster routing and Chitchat gossip protocol (`src/cluster.rs`):

- **Slot ownership & handoff**: `local_slots` mutated via `DELSLOTS` / `ADDSLOTS` and published via `update_gossip_data()`.
- **Chitchat anti-entropy & eviction**: versioned `node_info` propagation between reachable live peers, dead/partitioned peer eviction, and periodic `gossip_task` updates to `self.nodes`.
- **Routing (`route_slot`)**: `Local` vs `MOVED` vs `Unserved` (`CLUSTERDOWN`).
- **Faults & recovery**: node crashes/restarts and network partitions/heals.

### Properties verified
- **Safety**: `TypeOK`, `SingleSlotOwner`, `NoOrphanedWrites` (#35), `NoSelfRedirect`, deadlock freedom.
- **Liveness**: `GossipEventuallyConverges` (`<>[]TopologyConverged`), `RoutingEventuallyAccurate` (`<>[]RoutingAccurate`).

## 3. `Replication.tla`

Models primary -> replica replication for one shard (`src/replication.rs`, the `repl_log` writes in `src/shard_manager.rs`, `REPLICATION.md`):

- **Change index**: one `repl_log` entry per key, `INSERT OR REPLACE` with an `AUTOINCREMENT` LSN, tombstones for `DEL` and expiry cleanup, tombstone trimming bounded by stream pins, and `trimmed_through`.
- **Handshake** (`can_continue`): `CONTINUE` vs `RESYNC` on replid, `trimmed_through` and the committed watermark.
- **Stream** (`read_page`): pages of entries after the cursor with their current rows in one read transaction, ending with a watermark at the head of the log.
- **Replica**: `apply_frames` and `reset` as single transactions, and the replica's own expiry cleanup.
- **Faults**: dropped connections, primary process crash, primary power loss (commits since the last fsync are lost), replica power loss, graceful stop with the `clean_shutdown` marker, and the boot-id replid rotation in `init_shard`.

Abstractions: one shard, one table, one replica; each write is its own commit; a TTL is expired or not, the same on both clocks.

### Properties verified
- **Safety**: `TypeOK`, `CaughtUpMeansEqual` (when a replica commits watermark `W`, its live rows equal the primary's live rows as of LSN `W` under the same replid; planned failover relies on this), `ReplicaNeverAhead`, `LsnsUnique`, `SyncedMeansCaughtUp`, `PagesMatchReplid`, `StreamImpliesConnection`, deadlock freedom.
- **Liveness**: `ReplicaEventuallyConverges` (`<>[]Converged`: once faults and writes stop, the replica is caught up to the primary's watermark with its live rows).

## 4. `Failover.tla`

Models the automatic failover in `FAILOVER.md`, which follows Redis Cluster: `configEpoch`-versioned slot claims in per-node slot tables merged by gossip, FAIL agreement, replica elections voted by a majority of slot-owning primaries (`last_vote_epoch` and a persisted vote backoff), promotion, demotion of a returning primary, and re-pointing of the other replicas. Three primaries, two replicas of one of them, crashes, restarts, partitions and aborted elections. FAIL can be marked only while a majority of primaries can't reach the node; data is not modeled.

### Properties verified
- **Safety**: `TypeOK`, `OneWinnerPerEpoch`, `WinnerReplacesCurrentOwner` (a winner never takes slots from a superseded claim, which would drop the current owner's writes), `UniqueClaims`, `TableEpochsNeverDecrease` (action property: a returning old primary never takes its slots back).
- **Liveness**: `EventuallyConverges` (once faults stop, all live nodes agree on the table, every slot has a live primary and every replica is synced; a replica replaces a primary that never returns).

## Configs

| Spec | Config | Purpose | Expected |
| --- | --- | --- | --- |
| `AsyncWrite.tla` | `AsyncWrite.cfg` | Full async path (`queue_expire` + batch barrier) — safety + liveness | Pass |
| `AsyncWrite.tla` | `AsyncWrite_BarrierOnly.cfg` | Batch-barrier-only path (`HExpire` / finite-TTL `EXPIRE`) — safety + liveness | Pass |
| `AsyncWrite.tla` | `AsyncWrite_Bug65.cfg` | Disables `EXPIRE` inflight update & batch barrier | Fails `Linearizable` (#65) |
| `AsyncWrite.tla` | `AsyncWrite_Bug64.cfg` | Enables `BEGIN`/`COMMIT` failure dropping async ops | Fails `Linearizable` (#64) |
| `AsyncWrite.tla` | `AsyncWrite_BugKeyCountRollback.cfg` | Applies `batch_delta` on a failed/rolled-back batch | Fails `KeyCountAccurate` |
| `AsyncWrite.tla` | `AsyncWrite_BugCleanupInflight.cfg` | Disables inflight-cache skip in `delete_expired` | Fails `Linearizable` |
| `ClusterGossip.tla` | `ClusterGossip.cfg` | 3-node cluster with crashes, partitions, slot handoffs — safety + liveness | Pass |
| `ClusterGossip.tla` | `ClusterGossip_Bug35.cfg` | Enables pre-#35 local fallback on unserved slots | Fails `NoOrphanedWrites` (#35) |
| `ClusterGossip.tla` | `ClusterGossip_BugPublishRace.cfg` | Enables unlocked `update_gossip_data()` race (`src/cluster.rs:573-628`) | Fails `GossipEventuallyConverges` |
| `Replication.tla` | `Replication.cfg` | Writes, cleanup, trims, disconnects, crashes and power loss on both sides — safety + liveness | Pass |
| `Replication.tla` | `Replication_Large.cfg` | Same with 3 writes; about 25 minutes on 16 cores, not in CI (`./specs/check.sh replication-large`) | Pass |
| `Failover.tla` | `Failover.cfg` | Two replicas competing, one fault, two election epochs — safety | Pass |
| `Failover.tla` | `Failover_Liveness.cfg` | One replica; `p1` may crash for good — safety + liveness | Pass |
| `Failover.tla` | `Failover_Large.cfg` | Two faults; about 30 minutes on 16 cores, not in CI (`./specs/check.sh failover-large`) | Pass |
| `Failover.tla` | `Failover_BugNoVoteBackoff.cfg` | Votes again for the same failed primary before the first winner's claim arrives | Fails `WinnerReplacesCurrentOwner` |
| `Failover.tla` | `Failover_BugNoSlotEpochCheck.cfg` | Votes without comparing the request's slot epochs | Fails `WinnerReplacesCurrentOwner` |
| `Failover.tla` | `Failover_BugTableIgnoresEpoch.cfg` | Gossip overwrites slot bindings regardless of epoch | Fails `TableEpochsNeverDecrease` |
| `Failover.tla` | `Failover_BugVolatileVotes.cfg` | `last_vote_epoch` and the vote backoff lost on restart; about 10 minutes, not in CI | Fails `OneWinnerPerEpoch` |
| `Replication.tla` | `Replication_BugUpsertKeepsLsn.cfg` | `ON CONFLICT DO UPDATE` instead of `INSERT OR REPLACE`: a rewrite keeps its LSN | Fails `CaughtUpMeansEqual` |
| `Replication.tla` | `Replication_BugNoAutoincrement.cfg` | `repl_log` without `AUTOINCREMENT` reuses a trimmed tombstone's LSN | Fails `CaughtUpMeansEqual` |
| `Replication.tla` | `Replication_BugTrimIgnoresPins.cfg` | Trims tombstones above an active stream's cursor | Fails `CaughtUpMeansEqual` |
| `Replication.tla` | `Replication_BugSkipTrimmedThroughCheck.cfg` | Handshake answers `CONTINUE` to a cursor below `trimmed_through` | Fails `CaughtUpMeansEqual` |
| `Replication.tla` | `Replication_BugNoReplidRotation.cfg` | Keeps the replid after a power loss | Fails `CaughtUpMeansEqual` |
| `Replication.tla` | `Replication_BugCommitAfterCleanMarker.cfg` | Writes `clean_shutdown = 1` while the writer can still commit | Fails `CaughtUpMeansEqual` |
| `Replication.tla` | `Replication_BugVolatileStartupMarker.cfg` | Writes `clean_shutdown = 0` at startup without `synchronous = FULL` | Fails `CaughtUpMeansEqual` |
| `Replication.tla` | `Replication_BugReplicaCursorNotAtomic.cfg` | Makes `applied_lsn` durable apart from the replica's rows | Fails `CaughtUpMeansEqual` |

## Running locally

Requires Java 11+ (`java` on `PATH`). The script caches `tla2tools.jar` in `~/.cache/tla2tools/tla2tools.jar` (or uses `$TLA2TOOLS_JAR` if set):

```bash
./specs/check.sh
```
