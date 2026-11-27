# Automatic Failover Design

Status: proposal. Builds on [REPLICATION.md](REPLICATION.md).

## Goal

When a primary dies, one of its replicas takes over its slots without an
operator, and clients follow. Behave like Redis Cluster, so Redis Cluster
clients (go-redis `ClusterClient`, Lettuce, Jedis, redis-py) work unchanged.

Redis Cluster clients don't configure replicas. They learn the topology from
`CLUSTER SLOTS` / `CLUSTER SHARDS`, follow `MOVED`, and reload the slot map when
a node errors or redirects. Failover is decided entirely by the servers: the
cluster detects the failure, a replica wins an election among the primaries,
takes the slots with a higher epoch, and every node starts redirecting to it.
(Configuring replicas on the client is Redis Sentinel, which this is not.)

## What changes compared to today

Today a node's role and slots come from its config file, are fixed for the
life of the process, and failover is "edit the config and restart". In Redis
Cluster they are cluster state:

- **Versioned**: each primary's slot claim carries a `configEpoch`; the higher
  epoch wins a conflict.
- **Changed at runtime**: by elections, `CLUSTER FAILOVER` and
  `CLUSTER REPLICATE`, with no restart.
- **Persisted per node** (`nodes.conf`), not in the config file.

The config's `slot_ranges` and `replica_of` become the bootstrap for a node
with no saved state.

## Requirements and limits

- **At least three primaries.** An election needs votes from a majority of the
  primaries that own slots, counting the dead one. With one or two primaries no
  replica can ever win. This is the Redis rule.
- **Asynchronous, as before.** Writes acknowledged by the dead primary but not
  yet streamed are lost. The window is bounded by the minority write stop
  (see [Fencing](#fencing-a-primary-in-a-minority)) plus replication lag.
- **Every failover still forces a full resync** of the other replicas and of the
  returning old primary: the stream is coalesced, so a replica's state isn't a
  prefix of the new primary's history (REPLICATION.md). Redis avoids this with
  PSYNC2; we can't without an op log.

## Node state

Persisted in `<data_dir>/cluster-state.json`, written to a temp file, fsynced,
renamed, and the directory fsynced, before any action that depends on it (as
Redis does with `nodes.conf`):

| Field | Meaning |
|---|---|
| `current_epoch` | Highest epoch this node has seen or started an election in |
| `last_vote_epoch` | Epoch of the last vote this node cast; at most one per epoch |
| `last_vote_for` | Per failed primary, the epoch this node last voted to replace it in (the vote backoff) |
| `role` | `primary` or `replica` |
| `primary` | Replica: the node id it follows |
| `config_epoch` | Primary: the epoch of its slot claim |
| `slots` | Every known slot binding: `slot -> (node_id, config_epoch)` |

On first start (no file), the node writes one from its config: a primary
claims its configured slots with `config_epoch = 0`, a replica records
`replica_of`. If the file exists and the config disagrees, the file wins and a
warning is logged.

The per-shard `repl_state.role` stays, and follows the node role (see
[Promotion](#promotion)).

## Gossip

`NodeGossipData` gains `current_epoch`, `config_epoch`, `pfail` (node ids this
node can't reach) and `fail` (node ids it has marked failed), and, on replicas,
the per-shard `caught_up_lsn` and replid used for ranking. A node's
`current_epoch` becomes the maximum it sees.

## Slot table

Each node keeps the table of slot bindings, persisted with its state. It
changes only by these rules (Redis' "last failover wins"):

- A primary claims slot `s` with epoch `e`. If `s` is unbound, or bound with a
  lower epoch, rebind `s` to the claimant. Equal epochs can't happen once
  collisions are resolved (below).
- Bindings are kept for nodes that are down. A slot bound to a failed node gets
  `-CLUSTERDOWN`, as today. A returning node's old, lower-epoch claim can't take
  the slot back. This replaces today's "only live nodes own slots".

Routing reads the table instead of scanning live peers' claims.

**Epoch collisions.** Two primaries with the same `config_epoch` (for example
both bootstrapped at 0): the one with the lexicographically smaller node id
bumps `current_epoch` and takes it as its new `config_epoch`. This is Redis'
rule, and it makes every primary's epoch unique eventually.

**Losing slots.** A primary that sees a higher-epoch claim on one of its slots
drops it at once (requests for it get `MOVED`). If it loses its last slot to
node X, it becomes a replica of X ([Demotion](#demotion)).

## Failure detection

`[cluster] node_timeout_ms`, default 15000 as in Redis.

- **PFAIL** (local suspicion): a peer's chitchat heartbeat hasn't advanced for
  `node_timeout_ms`. Gossiped in `pfail`.
- **FAIL** (agreement): a majority of slot-owning primaries reported the node
  in `pfail` or `fail`, each report seen within `2 * node_timeout_ms`. The node
  that reaches the majority marks it failed and gossips it in `fail`; any node
  seeing `fail` from a live peer marks it failed too.
- **Clearing FAIL**: when the node is reachable again and is a replica or owns
  no slots; or when it owns slots but no failover took them for
  `2 * node_timeout_ms` (Redis' rule for a primary that came back before
  anyone replaced it).

## Election

A replica starts an election when all of these hold:

- its primary is FAIL and owned slots;
- every shard is synced (`synced = 1`) under its primary's replids;
- every shard received a frame from the primary within
  `node_timeout_ms * replica_validity_factor` (default 10; 0 disables the
  check), so a replica that was cut off long before the failure doesn't take
  over with stale data.

**Delay**: `500 ms + random(0..500 ms) + rank * 1000 ms`. Rank 0 is the replica
of that primary with the most data: the highest sum of per-shard
`caught_up_lsn`, ties broken by node id. This lets the FAIL state spread and
the most up-to-date replica go first.

**Request**: increment `current_epoch` (persist), then send to every
slot-owning primary over its Redis port:

```
BLOBASAUR.FAILOVER-AUTH <candidate> <epoch> <failed-primary> <primary-config-epoch> <slot-ranges> [MANUAL]
```

`slot-ranges` are the failed primary's slots in the candidate's table.

A primary votes (replies `+VOTE <epoch>`) only if all of these hold, and
persists `last_vote_epoch = epoch` before replying:

- `epoch >= current_epoch` (it adopts it), and `epoch > last_vote_epoch`;
- the candidate is a replica of `failed-primary`, and `failed-primary` is FAIL
  in its view (or `MANUAL` is set, see [Manual failover](#manual-failover));
- it hasn't voted for a replica of the same primary in the last
  `2 * max(2 * node_timeout_ms, 2 s)` (the vote backoff: twice the election
  window, at least 4 s);
- for every slot in `slot-ranges`, its table's epoch for that slot is at
  most `primary-config-epoch` (the candidate isn't working from an old
  config).

Votes are counted only for the epoch the candidate asked in. With votes from a
majority of slot-owning primaries within `max(2 * node_timeout_ms, 2 s)`
(measured from before sending the first vote request), the candidate wins and
promotes only if the promotion commit point (`cluster-state.json` persist) is
still reached before that election deadline; otherwise it abandons the attempt
and can retry after `4 * max(2 * node_timeout_ms, 2 s)` (twice the vote backoff,
at least 8 s) from the start of the previous attempt, plus a fresh rank delay
and jitter, with a new epoch.

Because a primary votes once per epoch and a win needs a majority, at most one
candidate wins any epoch.

**The vote backoff is persisted** (`last_vote_for`), unlike Redis, which keeps
it in memory. The model shows why it matters: with both it and
`last_vote_epoch` lost on a restart, a primary votes twice in one epoch and two
replicas win it. With the backoff persisted, losing `last_vote_epoch` alone
can't cause that, since the backoff and the slot-epoch check refuse the second
vote.

**Timing assumption.** The backoff and the slot-epoch check together stop a
second replica from taking slots that the first winner already took (whose
writes it doesn't have). That holds only if an election ends, and the winner's
claim reaches the voters, within the backoff: the candidate sets its election
deadline to `T = max(2 * node_timeout_ms, 2 s)` before sending its first vote
request (earlier than any voter records its vote) and re-checks that deadline
right before committing `cluster-state.json` (so stopping sync tasks on a slow
in-flight apply cannot commit late), while voters hold the backoff for `2T`
(`2 * max(2 * node_timeout_ms, 2 s)`, at least 4 s), leaving a full election
window `T` for the winner's claim to spread through gossip. Redis relies on the
same assumption.

For liveness, the retry interval must strictly exceed the vote backoff plus an
election window (`election_timeout <= vote_backoff < retry_backoff - election_timeout`,
so `retry_backoff = 2 * vote_backoff = 4T`, at least 8 s, plus rank delay and
jitter on re-arm). If `retry_backoff <= vote_backoff`, two competing candidates
whose staggered first attempts each won one voter can retry in lockstep while
the other candidate's voter is still in backoff and split votes indefinitely.
`specs/Failover.tla`'s liveness abstracts this timing relation through
`ForgetVote`.

## Promotion

The winner, with `e` the election epoch:

1. Stops its replica sync tasks for every shard and waits for them, so no
   Apply or Reset is queued behind the promotion.
2. Persists its state: `role = primary`, `config_epoch = e`, and the failed
   primary's slots bound to itself at `e`. This is the commit point.
3. Per shard, in one `synchronous = FULL` transaction: `role = primary` and a
   new replid (the same rule as promotion at startup today).
4. Gossips the new claim and starts serving its slots.

A crash between 2 and 3 is finished at startup: the state file says primary, so
`init_shard` takes the existing promotion path (new replid, `synced` already
1).

## Reconfiguration of the other nodes

- **Other replicas of the failed primary** see its slots bound to the winner,
  set `primary` to the winner (persist), restart their sync tasks, get
  `RESYNC` (new replid) and copy from scratch.
- **The old primary**, when it returns or the partition heals, sees its slots
  bound at a higher epoch, loses them all and demotes to a replica of the
  winner.

### Demotion

1. Drop the lost slots from routing at once (writes get `MOVED`).
2. Queue a writer barrier on every shard, so nothing that passed the route check
   earlier is still uncommitted.
3. Persist the state: `role = replica`, `primary = winner`.
4. Per shard, through the writer: `role = replica`, `synced = 0`.
5. Start the replica sync tasks. The replid differs, so every shard gets `RESYNC`
   and resets. Writes it took but never streamed are discarded, including
   `async_write` ops acknowledged but queued.

## Fencing a primary in a minority

A primary that can reach fewer than a majority of slot-owning primaries
(itself included) for `node_timeout_ms` stops serving: every command that
routes to its slots gets `-CLUSTERDOWN`, until it reaches a majority again.
Redis does the same (`cluster_state:fail` in the minority). This bounds how
long a partitioned old primary keeps accepting writes that the majority side
will discard after electing a replica. It is not a guarantee: clock and
scheduling delays can stretch it.

Replicas in the minority stop serving `READONLY` reads the same way.

## Manual failover

`CLUSTER FAILOVER [FORCE|TAKEOVER]`, sent to a replica, as in Redis:

- **Default** (planned, no data loss): the replica asks its primary to pause
  writes and run a writer barrier (the existing `BLOBASAUR.WATERMARK` barrier);
  the primary replies with each shard's LSN. The replica waits until every
  shard's `caught_up_lsn` reaches it, then runs an election with `MANUAL`,
  which primaries accept although the primary isn't FAIL. The old primary
  demotes when it sees the new claim. If any step takes longer than 5 s
  (Redis' `CLUSTER_MF_TIMEOUT`), the failover is abandoned and the primary
  resumes writes.
- **FORCE**: skip the primary handshake (it's unreachable), still hold an
  election with `MANUAL`.
- **TAKEOVER**: no election: bump `current_epoch`, take it as `config_epoch`,
  promote. For when a majority of primaries is gone; the operator is
  responsible for the old primary.

## Commands

| Command | Change |
|---|---|
| `CLUSTER FAILOVER [FORCE\|TAKEOVER]` | New |
| `CLUSTER REPLICATE <node-id>` | New: make this node a replica of `node-id` at runtime (only if it owns no slots) |
| `CLUSTER MYID` | New |
| `CLUSTER SHARDS` | New (Redis 7 clients prefer it to `CLUSTER SLOTS`) |
| `CLUSTER NODES` | Real `config_epoch` per node; `fail?` and `fail` flags; a failed primary's slots stay listed against it |
| `CLUSTER INFO` | `cluster_current_epoch`, `cluster_my_epoch`, `cluster_state:fail` in a minority |
| `CLUSTER SLOTS` | From the slot table; replicas listed only once synced, as today |

## Configuration

```toml
[cluster]
node_timeout_ms = 15000          # PFAIL after this, also the fencing timeout
replica_validity_factor = 10     # 0: any synced replica may stand

[replication]
replica_of = "node-1"            # bootstrap only, ignored once state is saved
```

## Model first

`specs/Failover.tla` models the epochs, votes, slot tables, promotion,
demotion and replica re-pointing, with three primaries, two replicas of one of
them, crashes, restarts, partitions and aborted elections. FAIL can be marked
only while a majority of primaries can't reach the node. It checks:

- **Safety**: at most one winner per election epoch; a winner only replaces
  the current owner of the slots (not a superseded one); no two tables bind a
  slot to different owners at the same epoch; a slot's binding epoch never
  decreases on any node, so a returning old primary never takes a slot back.
- **Liveness**: if a primary stays down and an eligible replica and a majority
  of primaries are up, eventually the replica owns its slots and every live
  node routes them to it.
- **Negative configs**, each breaking one of those: vote state not persisted
  (two winners in an epoch), no vote backoff and no slot-epoch check (each
  lets a stale replica take over from the first winner), and gossip ignoring
  epochs (the old primary takes its slots back).

Not modeled: data (Replication.tla covers it), `TAKEOVER` and epoch
collisions, manual failover, and the minority write stop.

## Work plan

Each step is one PR.

0. This document and `specs/Failover.tla`.
1. **Cluster state**: state file, epochs, slot table and its rules, epoch
   collision resolution, runtime node role (today many call sites read
   `cfg.replica_of()`), `CLUSTER MYID` / `NODES` / `INFO` / `SHARDS`,
   `CLUSTER REPLICATE`.
2. **Promotion and demotion at runtime**, triggered only by
   `CLUSTER FAILOVER TAKEOVER`: replica promotion, old primary demotion, other
   replicas re-pointing.
3. **Failure detection**: PFAIL / FAIL and the minority write stop.
4. **Elections**: votes, ranking, automatic failover, `CLUSTER FAILOVER FORCE`.
5. **Manual failover** (default mode): pause, barrier, catch up, `MANUAL`
   election.

Chaos tests (`tests/chaos_test.rs`) grow a scenario per step: kill a primary,
partition it, restart it during and after the election, and check that no
acknowledged write is lost except within the documented window.

## Not planned

- Replica migration (moving a spare replica to an orphaned primary).
- Partial resync after failover (PSYNC2).
- `cluster-require-full-coverage`: slots without a live owner keep failing on
  their own, as today.
