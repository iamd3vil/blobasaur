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
- **Graceful shutdown drain**: `TriggerShutdown` + draining `queue` and `deferred` before `WriterStop`.

### Properties verified
- **Safety**: `TypeOK`, `Linearizable` (refinement proof via ghost `absDb` + per-read LP window), `QuiescentConsistency`, `LockSafety`, deadlock freedom.
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

## Configs

| Spec | Config | Purpose | Expected |
| --- | --- | --- | --- |
| `AsyncWrite.tla` | `AsyncWrite.cfg` | Full async path (`queue_expire` + batch barrier) — safety + liveness | Pass |
| `AsyncWrite.tla` | `AsyncWrite_BarrierOnly.cfg` | Batch-barrier-only path (`HExpire` / finite-TTL `EXPIRE`) — safety + liveness | Pass |
| `AsyncWrite.tla` | `AsyncWrite_Bug65.cfg` | Disables `EXPIRE` inflight update & batch barrier | Fails `Linearizable` (#65) |
| `AsyncWrite.tla` | `AsyncWrite_Bug64.cfg` | Enables `BEGIN`/`COMMIT` failure dropping async ops | Fails `Linearizable` (#64) |
| `ClusterGossip.tla` | `ClusterGossip.cfg` | 3-node cluster with crashes, partitions, slot handoffs — safety + liveness | Pass |
| `ClusterGossip.tla` | `ClusterGossip_Bug35.cfg` | Enables pre-#35 local fallback on unserved slots | Fails `NoOrphanedWrites` (#35) |
| `ClusterGossip.tla` | `ClusterGossip_BugPublishRace.cfg` | Enables unlocked `update_gossip_data()` race (`src/cluster.rs:573-628`) | Fails `GossipEventuallyConverges` |

## Running locally

Requires Java 11+ (`java` on `PATH`). The script caches `tla2tools.jar` in `~/.cache/tla2tools/tla2tools.jar` (or uses `$TLA2TOOLS_JAR` if set):

```bash
./specs/check.sh
```
