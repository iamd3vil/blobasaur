---------------------------- MODULE AsyncWrite ----------------------------
(*
 * TLA+ specification of Blobasaur's async_write path:
 *   - inflight_cache with per-write tokens (src/shard_manager.rs: Pending, queue_pending, clear_pending_entries)
 *   - key_locks for DEL and EXPIRE (src/server.rs: del_reply, expire_reply)
 *   - queue_expire atomic enqueue-time cache updates (src/shard_manager.rs: queue_expire)
 *   - shard_writer_task batching, conflicts_with_async_in_batch barrier, two-phase
 *     commit -> clear_pending_entries window, batch failure mode, and shutdown drain
 *   - GET and TTL two-step read paths (inflight_cache first, then SQLite WAL)
 *   - Logical clock ticks for expiry consistency (#46, #55, #65)
 *   - Background expiry cleanup (src/shard_manager.rs: delete_expired) and
 *     cached per-shard stored-row count (src/app_state.rs: shard_key_counts)
 *
 * Properties checked:
 *   Safety:
 *     - TypeOK
 *     - Linearizable (Herlihy & Wing refinement proof via ghost absDb + per-read LP window)
 *     - QuiescentConsistency (when idle & empty, cache is empty and db matches absDb)
 *     - LockSafety (keyLock only held by clients actively in DEL/EXPIRE)
 *     - KeyCountAccurate (cached keyCount always equals SQLite stored-row count, >= 0)
 *   Liveness (under weak fairness on writer and in-progress client steps):
 *     - ExpireEventuallyReplies
 *     - PendingOpEventuallyCommits
 *     - ShutdownEventuallyDrains
 *
 * Negative configs prove the model catches real bugs:
 *   - AsyncWrite_Bug65.cfg: AllowExpireInflightUpdate = FALSE, AllowExpireBatchBarrier = FALSE -> #65
 *   - AsyncWrite_Bug64.cfg: AllowBatchFailure = TRUE -> #64
 *   - AsyncWrite_BugKeyCountRollback.cfg: AllowBatchFailure = TRUE, ApplyDeltaOnFailure = TRUE -> key_count drift
 *   - AsyncWrite_BugCleanupInflight.cfg: EnableCleanup = TRUE, CleanupSkipsInflight = FALSE -> stale NIL read
 *)

EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS
    Keys,                       \* Set of keys, e.g. {"k1"}
    Values,                     \* Set of client-written values, e.g. {"v1", "v2"}
    InitVal,                    \* Initial committed value for each key, e.g. "v0"
    Clients,                    \* Set of clients, e.g. {"c1", "c2"}
    MaxTime,                    \* Logical clock upper bound, e.g. 1
    BatchSize,                  \* Max ops per SQLite transaction, e.g. 2
    QueueCap,                   \* Shard mpsc channel capacity, e.g. 2
    MaxOps,                     \* Bound on total write ops issued per trace
    AllowExpireInflightUpdate,  \* TRUE = queue_expire updates inflight_cache at enqueue time
    AllowExpireBatchBarrier,    \* TRUE = conflicts_with_async_in_batch splits batch before uncached Expire
    AllowBatchFailure,          \* TRUE = writer can fail BEGIN/COMMIT and drop async ops (#64)
    ExpireUpdatesFiniteTtl,     \* TRUE = queue_expire replaces finite-TTL SET on EXPIRE 0 (Bug A on 303b939)
    ReadTsAfterCacheMiss,       \* TRUE = GET/TTL/DEL read Utc::now() after cache miss (Bug B on 303b939)
    EnableCleanup,              \* TRUE = background shard_cleanup_task deletes expired rows
    CleanupSkipsInflight,       \* TRUE = delete_expired skips keys present in inflight_cache
    ApplyDeltaOnFailure         \* TRUE = buggy writer applies batch_delta even on failed/rolled-back batch

NIL  == "NIL"
NONE == "NONE"
INF  == MaxTime + 10            \* Sentinel for expires_at = NULL (persistent)

EmptyEntry == [val |-> NIL, exp |-> 0]
NoCache    == [kind |-> "None", val |-> NIL, exp |-> 0, tok |-> 0]
NoOp       == [kind |-> "None", key |-> NONE, val |-> NIL, exp |-> 0, tok |-> 0, client |-> NONE, lin |-> TRUE]
IdleClient == [pc |-> "idle", key |-> NONE, allowed |-> {}, expected |-> -1, readTs |-> 0]

VARIABLES
    db,             \* [Keys -> [val: AllValues, exp: ExpValues]]  Committed SQLite WAL state
    keyCount,       \* Int                                         Cached shard_key_counts[shard]
    cache,          \* [Keys -> CacheEntry]                        inflight_cache
    queue,          \* Seq(Op)                                     mpsc channel to shard writer
    batch,          \* Seq(Op)                                     Current batch in process_batch
    deferred,       \* Op (NoOp when empty)                        deferred_operation in shard_writer_task
    writerPc,       \* "idle" | "committed" | "stopped"
    keyLock,        \* [Keys -> Clients \cup {NONE}]               state.key_locks
    client,         \* [Clients -> ClientRec]
    nextTok,        \* Next monotonically increasing pending token
    opsIssued,      \* Total write ops issued (bounded by MaxOps)
    now,            \* Logical wall clock in 0..MaxTime
    shutdown,       \* CancellationToken for graceful shutdown
    absDb,          \* Ghost: atomic sequential KV state for linearizability refinement
    linViolated     \* Ghost: set TRUE if any client reply violates linearizability

vars == <<db, keyCount, cache, queue, batch, deferred, writerPc, keyLock, client,
          nextTok, opsIssued, now, shutdown, absDb, linViolated>>

\* ---------------------------------------------------------------------------
\* Helpers matching SQLite queries & shard_manager::is_expired
\* ---------------------------------------------------------------------------

HasDeferred == deferred.kind # "None"
HasCache(cEntry) == cEntry.kind # "None"

IsLive(entry, t) ==
    entry.val # NIL /\ (entry.exp = INF \/ entry.exp > t)

ReadVal(entry, t) ==
    IF IsLive(entry, t) THEN entry.val ELSE NIL

ReadTtl(entry, t) ==
    IF ~IsLive(entry, t) THEN -2
    ELSE IF entry.exp = INF THEN -1
    ELSE entry.exp - t

CacheIsLive(cEntry, t) ==
    cEntry.kind = "Val" /\ (cEntry.exp = INF \/ cEntry.exp > t)

CacheReadVal(cEntry, t) ==
    IF CacheIsLive(cEntry, t) THEN cEntry.val ELSE NIL

CacheReadTtl(cEntry, t) ==
    IF ~CacheIsLive(cEntry, t) THEN -2
    ELSE IF cEntry.exp = INF THEN -1
    ELSE cEntry.exp - t

\* Stored SQLite row count (includes expired rows until cleanup deletes them)
StoredRowCount(dState) ==
    Cardinality({k \in Keys : dState[k].val # NIL})

\* Row delta produced by a single op inside process_batch (mirrors rows_affected())
\* Note: SetAsync over an expired-but-uncleaned row executes UPDATE (delta 0).
OpDelta(dState, op) ==
    CASE op.kind = "SetAsync" -> IF dState[op.key].val = NIL THEN 1 ELSE 0
      [] op.kind = "DelAsync" -> IF dState[op.key].val # NIL THEN -1 ELSE 0
      [] op.kind = "Expire"   -> 0

\* Apply a single op to a DB state at time t
ApplyOp(state, op, t) ==
    CASE op.kind = "SetAsync" ->
            [state EXCEPT ![op.key] = [val |-> op.val, exp |-> op.exp]]
      [] op.kind = "DelAsync" ->
            [state EXCEPT ![op.key] = EmptyEntry]
      [] op.kind = "Expire" ->
            IF IsLive(state[op.key], t)
            THEN [state EXCEPT ![op.key].exp = op.exp]
            ELSE state

\* Whether op conflicts with an earlier async op for the same key in b
\* (mirrors shard_manager::conflicts_with_async_in_batch)
ConflictsWithBatch(b, op) ==
    /\ AllowExpireBatchBarrier
    /\ op.kind = "Expire"
    /\ op.tok = 0
    /\ \E i \in 1..Len(b) :
        b[i].kind \in {"SetAsync", "DelAsync"} /\ b[i].key = op.key

\* Clear matching tokens in cache for every op in b
\* (mirrors shard_manager::clear_pending_entries)
ClearCacheForBatch(cMap, b) ==
    [k \in Keys |->
        IF HasCache(cMap[k]) /\ \E i \in 1..Len(b) :
            b[i].key = k /\ b[i].tok > 0 /\ b[i].tok = cMap[k].tok
        THEN NoCache
        ELSE cMap[k]]

\* Update in-flight readers' allowed return sets when absDb or now changes.
\* A read spanning [readTs, t] may linearize at readTs (e.g., against an EXPIRE
\* invoked at readTs that commits at t) or at the current wall-clock time t.
UpdateReaders(cMap, newAbs, t) ==
    [c \in Clients |->
        LET rt == cMap[c].readTs IN
        CASE cMap[c].pc = "get_db" ->
                [cMap[c] EXCEPT !.allowed = @ \cup {ReadVal(newAbs[cMap[c].key], rt), ReadVal(newAbs[cMap[c].key], t)}]
          [] cMap[c].pc = "ttl_db" ->
                [cMap[c] EXCEPT !.allowed = @ \cup {ReadTtl(newAbs[cMap[c].key], rt), ReadTtl(newAbs[cMap[c].key], t)}]
          [] cMap[c].pc = "del_db" ->
                [cMap[c] EXCEPT !.allowed = @ \cup {IsLive(newAbs[cMap[c].key], rt), IsLive(newAbs[cMap[c].key], t)}]
          [] OTHER -> cMap[c]]

\* Linearize any unlinearized Expire ops for key k already queued ahead of a new
\* async write for k (since FIFO execution orders them before the new write).
RECURSIVE LinearizeQueuedExpires(_, _, _, _)
LinearizeQueuedExpires(seq, k, aDb, t) ==
    IF Len(seq) = 0 THEN << <<>>, aDb >>
    ELSE LET h == Head(seq)
             res == IF h.kind = "Expire" /\ h.key = k /\ ~h.lin
                    THEN << [h EXCEPT !.lin = TRUE], ApplyOp(aDb, h, t) >>
                    ELSE << h, aDb >>
             rest == LinearizeQueuedExpires(Tail(seq), k, res[2], t)
         IN << <<res[1]>> \o rest[1], rest[2] >>

UpdateReadersAfterSeq(cMap, oldAbs, newAbs, t) ==
    UpdateReaders(UpdateReaders(cMap, oldAbs, t), newAbs, t)

\* ---------------------------------------------------------------------------
\* Initial State
\* ---------------------------------------------------------------------------

Init ==
    /\ db          = [k \in Keys |-> [val |-> InitVal, exp |-> INF]]
    /\ keyCount    = Cardinality(Keys)
    /\ cache       = [k \in Keys |-> NoCache]
    /\ queue       = <<>>
    /\ batch       = <<>>
    /\ deferred    = NoOp
    /\ writerPc    = "idle"
    /\ keyLock     = [k \in Keys |-> NONE]
    /\ client      = [c \in Clients |-> IdleClient]
    /\ nextTok     = 1
    /\ opsIssued   = 0
    /\ now         = 0
    /\ shutdown    = FALSE
    /\ absDb       = [k \in Keys |-> [val |-> InitVal, exp |-> INF]]
    /\ linViolated = FALSE

\* ---------------------------------------------------------------------------
\* Client Actions
\* ---------------------------------------------------------------------------

\* SET k v [EX] (async_write = true, src/server.rs:handle_set)
\* Note: SET does not acquire key_locks; queue_pending updates cache & channel atomically.
ClientSet(c, k, v, exp) ==
    /\ ~shutdown
    /\ client[c].pc = "idle"
    /\ opsIssued < MaxOps
    /\ Len(queue) < QueueCap
    /\ LET tok == nextTok
           bLin == LinearizeQueuedExpires(batch, k, absDb, now)
           dLin == IF HasDeferred /\ deferred.kind = "Expire" /\ deferred.key = k /\ ~deferred.lin
                   THEN << [deferred EXCEPT !.lin = TRUE], ApplyOp(bLin[2], deferred, now) >>
                   ELSE << deferred, bLin[2] >>
           qLin == LinearizeQueuedExpires(queue, k, dLin[2], now)
           preAbs == qLin[2]
           newOp  == [kind |-> "SetAsync", key |-> k, val |-> v, exp |-> exp, tok |-> tok, client |-> c, lin |-> TRUE]
           newAbs == [preAbs EXCEPT ![k] = [val |-> v, exp |-> exp]]
       IN
       /\ batch'     = bLin[1]
       /\ deferred'  = dLin[1]
       /\ queue'     = Append(qLin[1], newOp)
       /\ cache'     = [cache EXCEPT ![k] = [kind |-> "Val", val |-> v, exp |-> exp, tok |-> tok]]
       /\ nextTok'   = nextTok + 1
       /\ opsIssued' = opsIssued + 1
       /\ absDb'     = newAbs
       /\ client'    = UpdateReadersAfterSeq(client, preAbs, newAbs, now)
       /\ UNCHANGED <<db, keyCount, writerPc, keyLock, now, shutdown, linViolated>>

\* DEL k step 1: acquire key_lock and check inflight_cache (src/server.rs:del_reply)
ClientDelStart(c, k) ==
    /\ ~shutdown
    /\ client[c].pc = "idle"
    /\ keyLock[k] = NONE
    /\ opsIssued < MaxOps
    /\ IF HasCache(cache[k]) THEN
           IF CacheIsLive(cache[k], now) THEN
               /\ keyLock'   = [keyLock EXCEPT ![k] = c]
               /\ opsIssued' = opsIssued + 1
               /\ client'    = [client EXCEPT ![c] = [pc |-> "del_queue", key |-> k,
                                                      allowed |-> {IsLive(absDb[k], now)}, expected |-> -1, readTs |-> now]]
               /\ UNCHANGED <<db, keyCount, cache, queue, batch, deferred, writerPc, nextTok,
                              now, shutdown, absDb, linViolated>>
           ELSE
               /\ opsIssued'   = opsIssued + 1
               /\ linViolated' = (linViolated \/ IsLive(absDb[k], now))
               /\ UNCHANGED <<db, keyCount, cache, queue, batch, deferred, writerPc, keyLock,
                              client, nextTok, now, shutdown, absDb>>
       ELSE
           /\ keyLock'   = [keyLock EXCEPT ![k] = c]
           /\ opsIssued' = opsIssued + 1
           /\ client'    = [client EXCEPT ![c] = [pc |-> "del_db", key |-> k,
                                                  allowed |-> {IsLive(absDb[k], now)}, expected |-> -1, readTs |-> now]]
           /\ UNCHANGED <<db, keyCount, cache, queue, batch, deferred, writerPc, nextTok,
                          now, shutdown, absDb, linViolated>>

\* DEL k step 2a: check SQLite while holding key_lock
ClientDelDb(c) ==
    /\ client[c].pc = "del_db"
    /\ LET k == client[c].key
           rt == IF ReadTsAfterCacheMiss THEN now ELSE client[c].readTs
           exists == IsLive(db[k], rt)
       IN
       IF exists THEN
           /\ client' = [client EXCEPT ![c].pc = "del_queue",
                                       ![c].allowed = @ \cup {IsLive(absDb[k], rt)}]
           /\ UNCHANGED <<db, keyCount, cache, queue, batch, deferred, writerPc, keyLock,
                          nextTok, opsIssued, now, shutdown, absDb, linViolated>>
       ELSE
           /\ keyLock' = [keyLock EXCEPT ![k] = NONE]
           /\ client'  = [client EXCEPT ![c] = IdleClient]
           /\ linViolated' = (linViolated \/ (FALSE \notin client[c].allowed))
           /\ UNCHANGED <<db, keyCount, cache, queue, batch, deferred, writerPc, nextTok,
                          opsIssued, now, shutdown, absDb>>

\* DEL k step 2b: queue DeleteAsync and release key_lock
ClientDelQueue(c) ==
    /\ client[c].pc = "del_queue"
    /\ LET k == client[c].key IN
       IF shutdown THEN
           /\ keyLock' = [keyLock EXCEPT ![k] = NONE]
           /\ client'  = [client EXCEPT ![c] = IdleClient]
           /\ UNCHANGED <<db, keyCount, cache, queue, batch, deferred, writerPc, nextTok,
                          opsIssued, now, shutdown, absDb, linViolated>>
       ELSE
           /\ Len(queue) < QueueCap
           /\ LET tok == nextTok
                  bLin == LinearizeQueuedExpires(batch, k, absDb, now)
                  dLin == IF HasDeferred /\ deferred.kind = "Expire" /\ deferred.key = k /\ ~deferred.lin
                          THEN << [deferred EXCEPT !.lin = TRUE], ApplyOp(bLin[2], deferred, now) >>
                          ELSE << deferred, bLin[2] >>
                  qLin == LinearizeQueuedExpires(queue, k, dLin[2], now)
                  preAbs == qLin[2]
                  newOp  == [kind |-> "DelAsync", key |-> k, val |-> NIL, exp |-> 0, tok |-> tok, client |-> c, lin |-> TRUE]
                  newAbs == [preAbs EXCEPT ![k] = EmptyEntry]
                  updatedClients == UpdateReadersAfterSeq(client, preAbs, newAbs, now)
              IN
              /\ batch'     = bLin[1]
              /\ deferred'  = dLin[1]
              /\ queue'     = Append(qLin[1], newOp)
              /\ cache'     = [cache EXCEPT ![k] = [kind |-> "Del", val |-> NIL, exp |-> 0, tok |-> tok]]
              /\ keyLock'   = [keyLock EXCEPT ![k] = NONE]
              /\ nextTok'   = nextTok + 1
              /\ absDb'     = newAbs
              /\ client'    = [updatedClients EXCEPT ![c] = IdleClient]
              /\ linViolated' = (linViolated \/ (TRUE \notin (client[c].allowed \cup {IsLive(preAbs[k], now)})))
              /\ UNCHANGED <<db, keyCount, writerPc, opsIssued, now, shutdown>>

\* EXPIRE k newExp (sync, holds key_lock until responder replies; src/server.rs:expire_reply)
ClientExpireStart(c, k, newExp) ==
    /\ ~shutdown
    /\ client[c].pc = "idle"
    /\ keyLock[k] = NONE
    /\ opsIssued < MaxOps
    /\ Len(queue) < QueueCap
    /\ LET canUpdateCache ==
            /\ AllowExpireInflightUpdate
            /\ CacheIsLive(cache[k], now)
            /\ (cache[k].exp = INF \/ (ExpireUpdatesFiniteTtl /\ newExp <= now))
           tok == IF canUpdateCache THEN nextTok ELSE 0
           newCache ==
            IF ~canUpdateCache THEN cache
            ELSE IF newExp <= now
                 THEN [cache EXCEPT ![k] = [kind |-> "Del", val |-> NIL, exp |-> 0, tok |-> tok]]
                 ELSE [cache EXCEPT ![k] = [kind |-> "Val", val |-> cache[k].val, exp |-> newExp, tok |-> tok]]
           newAbs ==
            IF canUpdateCache THEN [absDb EXCEPT ![k].exp = newExp] ELSE absDb
           newOp == [kind |-> "Expire", key |-> k, val |-> NIL, exp |-> newExp,
                     tok |-> tok, client |-> c, lin |-> canUpdateCache]
           updatedClients ==
            IF canUpdateCache THEN UpdateReaders(client, newAbs, now) ELSE client
       IN
       /\ keyLock'   = [keyLock EXCEPT ![k] = c]
       /\ cache'     = newCache
       /\ queue'     = Append(queue, newOp)
       /\ nextTok'   = IF canUpdateCache THEN nextTok + 1 ELSE nextTok
       /\ opsIssued' = opsIssued + 1
       /\ absDb'     = newAbs
       /\ client'    = [updatedClients EXCEPT ![c] =
                            [pc |-> "expire_wait", key |-> k, allowed |-> {},
                             expected |-> IF canUpdateCache THEN 1 ELSE -1, readTs |-> now]]
       /\ UNCHANGED <<db, keyCount, batch, deferred, writerPc, now, shutdown, linViolated>>

\* GET k step 1: check inflight_cache (src/server.rs:handle_get)
ClientGetStart(c, k) ==
    /\ client[c].pc = "idle"
    /\ IF HasCache(cache[k]) THEN
           LET res == CacheReadVal(cache[k], now) IN
           /\ linViolated' = (linViolated \/ (res # ReadVal(absDb[k], now)))
           /\ UNCHANGED <<db, keyCount, cache, queue, batch, deferred, writerPc, keyLock,
                          client, nextTok, opsIssued, now, shutdown, absDb>>
       ELSE
           /\ client' = [client EXCEPT ![c] = [pc |-> "get_db", key |-> k,
                                               allowed |-> {ReadVal(absDb[k], now)}, expected |-> -1, readTs |-> now]]
           /\ UNCHANGED <<db, keyCount, cache, queue, batch, deferred, writerPc, keyLock,
                          nextTok, opsIssued, now, shutdown, absDb, linViolated>>

\* GET k step 2: read SQLite after cache miss
ClientGetDb(c) ==
    /\ client[c].pc = "get_db"
    /\ LET rt  == IF ReadTsAfterCacheMiss THEN now ELSE client[c].readTs
           res == ReadVal(db[client[c].key], rt)
       IN
       /\ linViolated' = (linViolated \/ (res \notin client[c].allowed))
       /\ client'      = [client EXCEPT ![c] = IdleClient]
       /\ UNCHANGED <<db, keyCount, cache, queue, batch, deferred, writerPc, keyLock,
                      nextTok, opsIssued, now, shutdown, absDb>>

\* TTL k step 1: check inflight_cache (src/server.rs:handle_ttl)
ClientTtlStart(c, k) ==
    /\ client[c].pc = "idle"
    /\ IF HasCache(cache[k]) THEN
           LET res == CacheReadTtl(cache[k], now) IN
           /\ linViolated' = (linViolated \/ (res # ReadTtl(absDb[k], now)))
           /\ UNCHANGED <<db, keyCount, cache, queue, batch, deferred, writerPc, keyLock,
                          client, nextTok, opsIssued, now, shutdown, absDb>>
       ELSE
           /\ client' = [client EXCEPT ![c] = [pc |-> "ttl_db", key |-> k,
                                               allowed |-> {ReadTtl(absDb[k], now)}, expected |-> -1, readTs |-> now]]
           /\ UNCHANGED <<db, keyCount, cache, queue, batch, deferred, writerPc, keyLock,
                          nextTok, opsIssued, now, shutdown, absDb, linViolated>>

\* TTL k step 2: read SQLite after cache miss
ClientTtlDb(c) ==
    /\ client[c].pc = "ttl_db"
    /\ LET rt  == IF ReadTsAfterCacheMiss THEN now ELSE client[c].readTs
           res == ReadTtl(db[client[c].key], rt)
       IN
       /\ linViolated' = (linViolated \/ (res \notin client[c].allowed))
       /\ client'      = [client EXCEPT ![c] = IdleClient]
       /\ UNCHANGED <<db, keyCount, cache, queue, batch, deferred, writerPc, keyLock,
                      nextTok, opsIssued, now, shutdown, absDb>>

\* ---------------------------------------------------------------------------
\* Shard Writer Actions (src/shard_manager.rs: shard_writer_task + process_batch)
\* ---------------------------------------------------------------------------

RECURSIVE TakeBatch(_, _, _, _)
TakeBatch(b0, q, rem, def) ==
    IF rem = 0 \/ Len(q) = 0 THEN << b0, q, def >>
    ELSE LET h == Head(q) IN
         IF ConflictsWithBatch(b0, h)
         THEN << b0, Tail(q), h >>
         ELSE TakeBatch(Append(b0, h), Tail(q), rem - 1, def)

RECURSIVE CommitBatchGhost(_, _, _, _, _)
CommitBatchGhost(dState, aState, seq, t, cMap) ==
    IF Len(seq) = 0 THEN << dState, aState, FALSE, 0 >>
    ELSE LET op == Head(seq)
             delta  == OpDelta(dState, op)
             nextDb == ApplyOp(dState, op, t)
             found  == IF IsLive(dState[op.key], t) THEN 1 ELSE 0
             badReply ==
                /\ op.kind = "Expire"
                /\ op.tok > 0
                /\ cMap[op.client].expected # found
             nextAbs ==
                IF op.kind = "Expire" /\ ~op.lin
                THEN ApplyOp(aState, op, t)
                ELSE aState
             rest == CommitBatchGhost(nextDb, nextAbs, Tail(seq), t, cMap)
         IN << rest[1], rest[2], badReply \/ rest[3], delta + rest[4] >>

\* Phase 1 of process_batch: form batch, execute tx.commit(), and apply batch_delta to key_count
WriterCommitBatch ==
    /\ writerPc = "idle"
    /\ (HasDeferred \/ Len(queue) > 0)
    /\ LET start == IF HasDeferred
                    THEN << <<deferred>>, queue, BatchSize - 1 >>
                    ELSE << <<Head(queue)>>, Tail(queue), BatchSize - 1 >>
       IN
       \E takeLimit \in 0..start[3] :
           LET formed == TakeBatch(start[1], start[2], takeLimit, NoOp)
               b      == formed[1]
               remQ   == formed[2]
               newDef == formed[3]
               res    == CommitBatchGhost(db, absDb, b, now, client)
               newDb  == res[1]
               newAbs == res[2]
               badExp == res[3]
               delta  == res[4]
           IN
           /\ batch'       = b
           /\ queue'       = remQ
           /\ deferred'    = newDef
           /\ db'          = newDb
           /\ keyCount'    = keyCount + delta
           /\ absDb'       = newAbs
           /\ writerPc'    = "committed"
           /\ client'      = UpdateReaders(client, newAbs, now)
           /\ linViolated' = (linViolated \/ badExp)
           /\ UNCHANGED <<cache, keyLock, nextTok, opsIssued, now, shutdown>>

\* Phase 2 of process_batch: clear_pending_entries + send sync responders
WriterClearAndReply ==
    /\ writerPc = "committed"
    /\ LET expireClients == {batch[i].client : i \in {j \in 1..Len(batch) : batch[j].kind = "Expire"}}
           expireKeys    == {batch[i].key    : i \in {j \in 1..Len(batch) : batch[j].kind = "Expire"}}
       IN
       /\ cache'    = ClearCacheForBatch(cache, batch)
       /\ client'   = [c \in Clients |-> IF c \in expireClients THEN IdleClient ELSE client[c]]
       /\ keyLock'  = [k \in Keys |-> IF k \in expireKeys THEN NONE ELSE keyLock[k]]
       /\ batch'    = <<>>
       /\ writerPc' = "idle"
       /\ UNCHANGED <<db, keyCount, queue, deferred, nextTok, opsIssued, now, shutdown, absDb, linViolated>>

\* Failed BEGIN, implicit rollback, or failed COMMIT: drops async ops, clears cache, replies ERR.
\* Real code keeps keyCount unchanged; ApplyDeltaOnFailure = TRUE models a broken counter update.
WriterFailBatch ==
    /\ AllowBatchFailure
    /\ writerPc = "idle"
    /\ (HasDeferred \/ Len(queue) > 0)
    /\ LET start == IF HasDeferred
                    THEN << <<deferred>>, queue, BatchSize - 1 >>
                    ELSE << <<Head(queue)>>, Tail(queue), BatchSize - 1 >>
           formed == TakeBatch(start[1], start[2], start[3], NoOp)
           b      == formed[1]
           delta  == CommitBatchGhost(db, absDb, b, now, client)[4]
           expireClients == {b[i].client : i \in {j \in 1..Len(b) : b[j].kind = "Expire"}}
           expireKeys    == {b[i].key    : i \in {j \in 1..Len(b) : b[j].kind = "Expire"}}
       IN
       /\ queue'    = formed[2]
       /\ deferred' = formed[3]
       /\ keyCount' = IF ApplyDeltaOnFailure THEN keyCount + delta ELSE keyCount
       /\ cache'    = ClearCacheForBatch(cache, b)
       /\ client'   = [c \in Clients |-> IF c \in expireClients THEN IdleClient ELSE client[c]]
       /\ keyLock'  = [k \in Keys |-> IF k \in expireKeys THEN NONE ELSE keyLock[k]]
       /\ UNCHANGED <<db, batch, writerPc, nextTok, opsIssued, now, shutdown, absDb, linViolated>>

\* Background shard_cleanup_task -> delete_expired (shares the shard's single writer connection)
CleanupExpired(k) ==
    /\ EnableCleanup
    /\ writerPc = "idle"
    /\ db[k].val # NIL
    /\ db[k].exp <= now
    /\ (CleanupSkipsInflight => ~HasCache(cache[k]))
    /\ db'       = [db EXCEPT ![k] = EmptyEntry]
    /\ keyCount' = keyCount - 1
    /\ UNCHANGED <<cache, queue, batch, deferred, writerPc, keyLock, client,
                   nextTok, opsIssued, now, shutdown, absDb, linViolated>>

\* ---------------------------------------------------------------------------
\* Clock Tick & Graceful Shutdown
\* ---------------------------------------------------------------------------

Tick ==
    /\ now < MaxTime
    /\ LET nextNow == now + 1 IN
       /\ now'    = nextNow
       /\ client' = UpdateReaders(client, absDb, nextNow)
       /\ UNCHANGED <<db, keyCount, cache, queue, batch, deferred, writerPc, keyLock,
                      nextTok, opsIssued, shutdown, absDb, linViolated>>

TriggerShutdown ==
    /\ ~shutdown
    /\ shutdown' = TRUE
    /\ UNCHANGED <<db, keyCount, cache, queue, batch, deferred, writerPc, keyLock, client,
                   nextTok, opsIssued, now, absDb, linViolated>>

WriterStop ==
    /\ shutdown
    /\ writerPc = "idle"
    /\ Len(queue) = 0
    /\ ~HasDeferred
    /\ writerPc' = "stopped"
    /\ UNCHANGED <<db, keyCount, cache, queue, batch, deferred, keyLock, client,
                   nextTok, opsIssued, now, shutdown, absDb, linViolated>>

\* ---------------------------------------------------------------------------
\* Next-State Relation & Fairness
\* ---------------------------------------------------------------------------

ClientStep ==
    \E c \in Clients :
        \/ \E k \in Keys, v \in Values, exp \in {now + 1, INF} : ClientSet(c, k, v, exp)
        \/ \E k \in Keys : ClientDelStart(c, k)
        \/ ClientDelDb(c)
        \/ ClientDelQueue(c)
        \/ \E k \in Keys, newExp \in {now, now + 1} : ClientExpireStart(c, k, newExp)
        \/ \E k \in Keys : ClientGetStart(c, k)
        \/ ClientGetDb(c)
        \/ \E k \in Keys : ClientTtlStart(c, k)
        \/ ClientTtlDb(c)

WriterStep ==
    \/ WriterCommitBatch
    \/ WriterClearAndReply
    \/ WriterFailBatch
    \/ \E k \in Keys : CleanupExpired(k)
    \/ WriterStop

Next ==
    \/ ClientStep
    \/ WriterStep
    \/ Tick
    \/ TriggerShutdown

Fairness ==
    /\ WF_vars(WriterCommitBatch)
    /\ WF_vars(WriterClearAndReply)
    /\ WF_vars(WriterStop)
    /\ \A c \in Clients : WF_vars(ClientDelDb(c) \/ ClientDelQueue(c))

Spec == Init /\ [][Next]_vars /\ Fairness

\* ---------------------------------------------------------------------------
\* Invariants (Safety)
\* ---------------------------------------------------------------------------

TypeOK ==
    /\ writerPc \in {"idle", "committed", "stopped"}
    /\ keyCount \in Int
    /\ now \in 0..MaxTime
    /\ opsIssued \in 0..MaxOps
    /\ Len(queue) <= QueueCap
    /\ Len(batch) <= BatchSize
    /\ shutdown \in BOOLEAN
    /\ linViolated \in BOOLEAN

Linearizable == ~linViolated

KeyCountAccurate ==
    /\ keyCount >= 0
    /\ keyCount = StoredRowCount(db)

QuiescentConsistency ==
    (writerPc \in {"idle", "stopped"} /\ Len(queue) = 0 /\ Len(batch) = 0 /\ ~HasDeferred)
    => /\ \A k \in Keys : ~HasCache(cache[k])
       /\ \A k \in Keys : ReadVal(db[k], now) = ReadVal(absDb[k], now)
       /\ \A k \in Keys : ReadTtl(db[k], now) = ReadTtl(absDb[k], now)

LockSafety ==
    \A k \in Keys :
        keyLock[k] # NONE =>
            client[keyLock[k]].pc \in {"del_db", "del_queue", "expire_wait"}

\* ---------------------------------------------------------------------------
\* Temporal Properties (Liveness)
\* ---------------------------------------------------------------------------

TokenInFlight(tok) ==
    \/ \E i \in 1..Len(queue) : queue[i].tok = tok
    \/ \E i \in 1..Len(batch) : batch[i].tok = tok
    \/ (HasDeferred /\ deferred.tok = tok)
    \/ \E k \in Keys : HasCache(cache[k]) /\ cache[k].tok = tok

PendingOpEventuallyCommits ==
    \A tok \in 1..(MaxOps + 1) : TokenInFlight(tok) ~> ~TokenInFlight(tok)

ExpireEventuallyReplies ==
    \A c \in Clients : (client[c].pc = "expire_wait") ~> (client[c].pc = "idle")

ShutdownEventuallyDrains ==
    shutdown ~> ( /\ writerPc = "stopped"
                  /\ Len(queue) = 0
                  /\ Len(batch) = 0
                  /\ ~HasDeferred
                  /\ \A k \in Keys : ~HasCache(cache[k])
                  /\ \A k \in Keys : ReadVal(db[k], now) = ReadVal(absDb[k], now) )

=============================================================================
