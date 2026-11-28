---------------------------- MODULE Replication ----------------------------
(*
 * TLA+ specification of Blobasaur's primary -> replica replication for one
 * shard (src/replication.rs, the repl_log writes in src/shard_manager.rs,
 * REPLICATION.md).
 *
 * Models:
 *   - The change index: one repl_log entry per key, INSERT OR REPLACE giving
 *     every change a new AUTOINCREMENT LSN, tombstones for DEL and expiry
 *     cleanup, and tombstone trimming bounded by active stream pins
 *     (trim_tombstones) with trimmed_through.
 *   - The BLOBASAUR.SYNC handshake (can_continue): CONTINUE vs RESYNC on
 *     replid, trimmed_through and the committed watermark.
 *   - The streamer (read_page): pages of entries after the cursor with their
 *     current rows, read in one transaction, ending with a watermark when the
 *     page reaches the head of the log.
 *   - Replica apply (apply_frames) and reset as single transactions, and the
 *     replica's own expiry cleanup.
 *   - Faults: dropped connections, primary process crash, primary power loss
 *     (commits since the last fsync are lost), replica power loss, graceful
 *     stop with the clean_shutdown marker, and the boot-id / clean-shutdown
 *     replid rotation on restart (init_shard, needs_new_replid).
 *
 * Abstractions:
 *   - One shard, one table, one replica. Each write is its own commit; a
 *     batch commits several LSNs at once, which only removes interleavings.
 *   - A TTL is "expired" or not, the same on both clocks. Clock skew changes
 *     when a row expires, not which rows the stream sends.
 *   - The network is one FIFO channel of pages; a disconnect drops it.
 *
 * Properties checked:
 *   Safety:
 *     - TypeOK
 *     - CaughtUpMeansEqual: when a replica commits a watermark W, its live
 *       rows equal the primary's live rows as of LSN W under the same replid.
 *       This is the claim planned failover (BLOBASAUR.WATERMARK) relies on.
 *     - ReplicaNeverAhead: while the primary is up and the replids match, the
 *       replica's applied_lsn is at most the primary's watermark.
 *     - LsnsUnique: repl_log LSNs are distinct and at most sqlite_sequence.
 *     - SyncedMeansCaughtUp, PagesMatchReplid, StreamImpliesConnection.
 *   Liveness (weak fairness on connect, stream, apply, EOF and restart):
 *     - ReplicaEventuallyConverges: once faults and writes stop, the replica
 *       ends up caught up to the primary's watermark with its live rows.
 *
 * Negative configs flip one flag each and must fail CaughtUpMeansEqual:
 *   - UpsertKeepsLsn: ON CONFLICT DO UPDATE instead of INSERT OR REPLACE.
 *   - NoAutoincrement: repl_log without AUTOINCREMENT reuses trimmed LSNs.
 *   - TrimIgnoresPins: trimming tombstones above an active stream's cursor.
 *   - SkipTrimmedThroughCheck: CONTINUE for a cursor below trimmed_through.
 *   - NoReplidRotation: keeping the replid after a power loss.
 *   - CommitAfterCleanMarker: a commit after clean_shutdown = 1.
 *   - VolatileStartupMarker: writing clean_shutdown = 0 without FULL sync.
 *   - ReplicaCursorNotAtomic: applied_lsn made durable apart from the rows.
 *)

EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS
    Keys,                    \* e.g. {"k1", "k2"}
    Values,                  \* e.g. {"v1", "v2"}
    MaxWrites,               \* Bound on client writes (SET, DEL, EXPIRE)
    MaxFaults,               \* Bound on disconnects, crashes, power losses, stops
    PageRows,                \* PAGE_ROWS in src/replication.rs
    ChanCap,                 \* Pages in flight on the connection
    \* Bug switches, all FALSE in Replication.cfg
    UpsertKeepsLsn,
    NoAutoincrement,
    TrimIgnoresPins,
    SkipTrimmedThroughCheck,
    NoReplidRotation,
    CommitAfterCleanMarker,
    VolatileStartupMarker,
    ReplicaCursorNotAtomic

NONE == "NONE"
NoWm == -1

Max(a, b) == IF a > b THEN a ELSE b
Min(a, b) == IF a < b THEN a ELSE b
MaxOf(S) == CHOOSE m \in S : \A x \in S : x <= m

\* TLC can't compare a record with a string, so "no row" and "no entry" are
\* sentinel records. LSNs start at 1, so lsn 0 never names a real entry.
NoRow == [v |-> NONE, exp |-> FALSE]
NoEntry == [lsn |-> 0, del |-> FALSE]
Row == [v : Values, exp : BOOLEAN]
EmptyDb == [k \in Keys |-> NoRow]
EmptyLog == [k \in Keys |-> NoEntry]

VARIABLES
    \* Primary shard
    pState,         \* "up" | "stopping" (CommitAfterCleanMarker only) | "down"
    pDb,            \* [Keys -> Row \cup {NoRow}]                blobs
    pLog,           \* [Keys -> [lsn, del]], NoEntry if none     repl_log
    pSeq,           \* sqlite_sequence for repl_log
    pTrimmed,       \* repl_state.trimmed_through
    pReplid,        \* repl_state.replid
    pClean,         \* repl_state.clean_shutdown
    pBootChanged,   \* The kernel boot id differs from the stored one
    pDurable,       \* What survives a power loss (last fsync of the WAL)
    replidCtr,      \* Source of fresh replids
    \* Primary side of the stream (serve_sync / stream_pages)
    sActive,        \* A stream is running and pinned
    sCursor,        \* Its cursor, which is also its trim pin
    sLastWm,        \* Last watermark it sent, so idle heartbeats add no states
    \* The connection
    chan,           \* Seq of pages: [replid, frames, wm]
    rConn,          \* The replica holds an open connection
    \* Replica shard
    rDb,            \* [Keys -> Row \cup {NoRow}]
    rReplid,        \* 0 = none
    rApplied,       \* applied_lsn
    rCaught,        \* caught_up_lsn, NoWm if unset
    rSynced,        \* synced
    rDurable,       \* What survives a replica power loss
    \* Bounds and ghosts
    writes,
    faults,
    hist,           \* <<replid, lsn>> -> primary live rows as of that LSN
    mismatch        \* A replica committed a watermark its rows don't match

pVars == <<pState, pDb, pLog, pSeq, pTrimmed, pReplid, pClean, pBootChanged,
           pDurable, replidCtr>>
sVars == <<sActive, sCursor, sLastWm>>
rVars == <<rDb, rReplid, rApplied, rCaught, rSynced, rDurable>>
vars == <<pVars, sVars, chan, rConn, rVars, writes, faults, hist, mismatch>>

\* ---------------------------------------------------------------------------
\* Helpers
\* ---------------------------------------------------------------------------

\* Rows a read sees: expired rows are filtered on expires_at.
Live(db) == [k \in Keys |-> IF db[k] = NoRow THEN NONE
                            ELSE IF db[k].exp THEN NONE ELSE db[k].v]

Logged == {k \in Keys : pLog[k] # NoEntry}
LogLsns == {pLog[k].lsn : k \in Logged}
MaxLogLsn == IF LogLsns = {} THEN 0 ELSE MaxOf(LogLsns)

\* The watermark read_page reports (SELECT seq FROM sqlite_sequence). Without
\* AUTOINCREMENT there is no sequence, only the highest rowid present.
WatermarkNow == IF NoAutoincrement THEN MaxLogLsn ELSE pSeq
NewLsn == IF NoAutoincrement THEN MaxLogLsn + 1 ELSE pSeq + 1

CurP == [db |-> pDb, log |-> pLog, seq |-> pSeq, trimmed |-> pTrimmed,
         replid |-> pReplid, clean |-> pClean]
CurR == [db |-> rDb, replid |-> rReplid, applied |-> rApplied,
         caught |-> rCaught, synced |-> rSynced]

RECURSIVE SortByLsn(_)
SortByLsn(S) ==
    IF S = {} THEN <<>>
    ELSE LET m == CHOOSE k \in S : \A j \in S : pLog[k].lsn <= pLog[j].lsn
         IN <<m>> \o SortByLsn(S \ {m})

Converged ==
    /\ pState = "up"
    /\ rConn /\ sActive /\ chan = <<>>
    /\ rReplid = pReplid
    /\ rCaught = WatermarkNow
    /\ Live(rDb) = Live(pDb)

\* ---------------------------------------------------------------------------
\* Initial state: a fresh primary with replid 1, an empty replica
\* ---------------------------------------------------------------------------

Init ==
    /\ pState = "up"
    /\ pDb = EmptyDb
    /\ pLog = EmptyLog
    /\ pSeq = 0
    /\ pTrimmed = 0
    /\ pReplid = 1
    /\ pClean = FALSE
    /\ pBootChanged = FALSE
    /\ pDurable = [db |-> EmptyDb, log |-> EmptyLog, seq |-> 0, trimmed |-> 0,
                   replid |-> 1, clean |-> FALSE]
    /\ replidCtr = 1
    /\ sActive = FALSE
    /\ sCursor = 0
    /\ sLastWm = NoWm
    /\ chan = <<>>
    /\ rConn = FALSE
    /\ rDb = EmptyDb
    /\ rReplid = 0
    /\ rApplied = 0
    /\ rCaught = NoWm
    /\ rSynced = FALSE
    /\ rDurable = [db |-> EmptyDb, replid |-> 0, applied |-> 0, caught |-> NoWm,
                   synced |-> FALSE]
    /\ writes = 0
    /\ faults = 0
    /\ hist = <<1, 0>> :> Live(EmptyDb)
    /\ mismatch = FALSE

\* ---------------------------------------------------------------------------
\* Primary writes (process_batch, shard_cleanup_task)
\* ---------------------------------------------------------------------------

\* One op and its repl_log entry, committed together (SAVEPOINT op).
\* INSERT OR REPLACE deletes and re-inserts the entry, so it gets a new LSN;
\* ON CONFLICT DO UPDATE (UpsertKeepsLsn) keeps the old one.
Commit(newDb, k, del) ==
    LET keep == UpsertKeepsLsn /\ pLog[k] # NoEntry
        lsn  == IF keep THEN pLog[k].lsn ELSE NewLsn
        seq  == IF keep THEN pSeq ELSE Max(pSeq, lsn)
        wm   == IF NoAutoincrement THEN Max(MaxLogLsn, lsn) ELSE seq
    IN /\ pDb' = newDb
       /\ pLog' = [pLog EXCEPT ![k] = [lsn |-> lsn, del |-> del]]
       /\ pSeq' = seq
       /\ hist' = (<<pReplid, wm>> :> Live(newDb)) @@ hist

CanCommit == pState \in {"up", "stopping"}

ClientSet(k, v) ==
    /\ CanCommit /\ writes < MaxWrites
    /\ Commit([pDb EXCEPT ![k] = [v |-> v, exp |-> FALSE]], k, FALSE)
    /\ writes' = writes + 1
    /\ UNCHANGED <<pState, pTrimmed, pReplid, pClean, pBootChanged, pDurable,
                   replidCtr, sVars, chan, rConn, rVars, faults, mismatch>>

\* DEL of an existing row: a tombstone.
ClientDel(k) ==
    /\ CanCommit /\ writes < MaxWrites
    /\ pDb[k] # NoRow
    /\ Commit([pDb EXCEPT ![k] = NoRow], k, TRUE)
    /\ writes' = writes + 1
    /\ UNCHANGED <<pState, pTrimmed, pReplid, pClean, pBootChanged, pDurable,
                   replidCtr, sVars, chan, rConn, rVars, faults, mismatch>>

\* EXPIRE whose deadline has passed: the row stays until cleanup, but reads
\* no longer see it. A live entry, like any EXPIRE that updated a row.
ClientExpire(k) ==
    /\ CanCommit /\ writes < MaxWrites
    /\ pDb[k] # NoRow /\ ~pDb[k].exp
    /\ Commit([pDb EXCEPT ![k].exp = TRUE], k, FALSE)
    /\ writes' = writes + 1
    /\ UNCHANGED <<pState, pTrimmed, pReplid, pClean, pBootChanged, pDurable,
                   replidCtr, sVars, chan, rConn, rVars, faults, mismatch>>

\* Expiry cleanup: DELETE ... RETURNING key plus a tombstone per key.
PrimaryCleanup(k) ==
    /\ CanCommit
    /\ pDb[k] # NoRow /\ pDb[k].exp
    /\ Commit([pDb EXCEPT ![k] = NoRow], k, TRUE)
    /\ UNCHANGED <<pState, pTrimmed, pReplid, pClean, pBootChanged, pDurable,
                   replidCtr, sVars, chan, rConn, rVars, writes, faults, mismatch>>

\* trim_tombstones: tombstones up to some age (lsn <= t, since `at` grows with
\* the LSN) and below every active stream's pin; raises trimmed_through.
Trim(t) ==
    LET Trimmable(k) == /\ pLog[k] # NoEntry /\ pLog[k].del /\ pLog[k].lsn <= t
                        /\ (TrimIgnoresPins \/ ~sActive \/ pLog[k].lsn < sCursor)
        D == {k \in Keys : Trimmable(k)}
    IN /\ CanCommit
       /\ D # {}
       /\ pLog' = [k \in Keys |-> IF k \in D THEN NoEntry ELSE pLog[k]]
       /\ pTrimmed' = Max(pTrimmed, MaxOf({pLog[k].lsn : k \in D}))
       /\ UNCHANGED <<pState, pDb, pSeq, pReplid, pClean, pBootChanged, pDurable,
                      replidCtr, sVars, chan, rConn, rVars, writes, faults, hist,
                      mismatch>>

\* The OS writes the WAL back to disk (checkpoint, fsync, page cache flush).
PrimaryFlush ==
    /\ pDurable # CurP
    /\ pDurable' = CurP
    /\ UNCHANGED <<pState, pDb, pLog, pSeq, pTrimmed, pReplid, pClean, pBootChanged,
                   replidCtr, sVars, chan, rConn, rVars, writes, faults, hist, mismatch>>

\* ---------------------------------------------------------------------------
\* Primary lifecycle (init_shard, mark_clean_shutdown)
\* ---------------------------------------------------------------------------

\* OOM, kill -9, panic: WAL writes are already in the kernel, nothing is lost.
PrimaryProcessCrash ==
    /\ pState # "down" /\ faults < MaxFaults
    /\ pState' = "down"
    /\ sActive' = FALSE
    /\ faults' = faults + 1
    /\ UNCHANGED <<pDb, pLog, pSeq, pTrimmed, pReplid, pClean, pBootChanged, pDurable,
                   replidCtr, sCursor, sLastWm, chan, rConn, rVars, writes, hist, mismatch>>

\* OS crash or power loss: everything since the last flush is gone, and the
\* next boot has a new boot id.
PrimaryPowerLoss ==
    /\ faults < MaxFaults
    /\ pState' = "down"
    /\ pDb' = pDurable.db
    /\ pLog' = pDurable.log
    /\ pSeq' = pDurable.seq
    /\ pTrimmed' = pDurable.trimmed
    /\ pReplid' = pDurable.replid
    /\ pClean' = pDurable.clean
    /\ pBootChanged' = TRUE
    /\ sActive' = FALSE
    /\ faults' = faults + 1
    /\ UNCHANGED <<pDurable, replidCtr, sCursor, sLastWm, chan, rConn, rVars, writes,
                   hist, mismatch>>

\* Graceful shutdown: the writer and cleanup are drained, then
\* clean_shutdown = 1 is written over a synchronous = FULL connection, which
\* also makes every earlier WAL frame durable.
GracefulStop ==
    /\ ~CommitAfterCleanMarker
    /\ pState = "up" /\ faults < MaxFaults
    /\ pState' = "down"
    /\ pClean' = TRUE
    /\ pDurable' = [CurP EXCEPT !.clean = TRUE]
    /\ sActive' = FALSE
    /\ faults' = faults + 1
    /\ UNCHANGED <<pDb, pLog, pSeq, pTrimmed, pReplid, pBootChanged, replidCtr,
                   sCursor, sLastWm, chan, rConn, rVars, writes, hist, mismatch>>

\* CommitAfterCleanMarker: the marker is written while the writer can still
\* commit (pState = "stopping"); FinishStop ends the process later.
MarkCleanEarly ==
    /\ CommitAfterCleanMarker
    /\ pState = "up" /\ faults < MaxFaults
    /\ pState' = "stopping"
    /\ pClean' = TRUE
    /\ pDurable' = [CurP EXCEPT !.clean = TRUE]
    /\ faults' = faults + 1
    /\ UNCHANGED <<pDb, pLog, pSeq, pTrimmed, pReplid, pBootChanged, replidCtr,
                   sVars, chan, rConn, rVars, writes, hist, mismatch>>

FinishStop ==
    /\ pState = "stopping"
    /\ pState' = "down"
    /\ sActive' = FALSE
    /\ UNCHANGED <<pDb, pLog, pSeq, pTrimmed, pReplid, pClean, pBootChanged, pDurable,
                   replidCtr, sCursor, sLastWm, chan, rConn, rVars, writes, faults,
                   hist, mismatch>>

\* init_shard: a new replid if the boot id changed and the last stop was not
\* clean; then clean_shutdown = 0, in one synchronous = FULL transaction.
PrimaryRestart ==
    LET rotate == ~NoReplidRotation /\ pBootChanged /\ ~pClean
        newR   == replidCtr + 1
        replid == IF rotate THEN newR ELSE pReplid
    IN /\ pState = "down"
       /\ pState' = "up"
       /\ pReplid' = replid
       /\ replidCtr' = IF rotate THEN newR ELSE replidCtr
       /\ pClean' = FALSE
       /\ pBootChanged' = FALSE
       /\ pDurable' = IF VolatileStartupMarker THEN pDurable
                      ELSE [CurP EXCEPT !.replid = replid, !.clean = FALSE]
       /\ hist' = IF rotate THEN (<<newR, WatermarkNow>> :> Live(pDb)) @@ hist ELSE hist
       /\ UNCHANGED <<pDb, pLog, pSeq, pTrimmed, sVars, chan, rConn, rVars, writes,
                      faults, mismatch>>

\* ---------------------------------------------------------------------------
\* Replication stream
\* ---------------------------------------------------------------------------

\* BLOBASAUR.SYNC handshake (serve_sync, can_continue). CONTINUE pins the
\* stream at the replica's cursor; RESYNC makes the replica reset first.
Connect ==
    LET ok == /\ rReplid = pReplid
              /\ \/ rApplied = 0
                 \/ /\ SkipTrimmedThroughCheck \/ pTrimmed <= rApplied
                    /\ rApplied <= WatermarkNow
    IN /\ ~rConn /\ pState = "up"
       /\ IF ok
          THEN /\ rConn' = TRUE
               /\ sActive' = TRUE
               /\ sCursor' = rApplied
               /\ sLastWm' = NoWm
               /\ UNCHANGED rVars
          ELSE \* Reset: wipe the shard and its index, adopt the primary's replid.
               /\ rDb' = EmptyDb
               /\ rReplid' = pReplid
               /\ rApplied' = 0
               /\ rCaught' = NoWm
               /\ rSynced' = FALSE
               /\ UNCHANGED <<rConn, sVars, rDurable>>
       /\ UNCHANGED <<pVars, chan, writes, faults, hist, mismatch>>

\* read_page: the entries after the cursor and their current rows in one read
\* transaction, with a watermark if fewer than PageRows were found. An entry
\* whose row is gone is sent as a delete.
SendPage ==
    LET above    == {k \in Keys : pLog[k] # NoEntry /\ pLog[k].lsn > sCursor}
        sorted   == SortByLsn(above)
        n        == Min(PageRows, Len(sorted))
        frames   == [i \in 1..n |->
                        [lsn |-> pLog[sorted[i]].lsn,
                         key |-> sorted[i],
                         row |-> IF pLog[sorted[i]].del THEN NoRow ELSE pDb[sorted[i]]]]
        caughtUp == Cardinality(above) < PageRows
        wm       == IF caughtUp THEN WatermarkNow ELSE NoWm
    IN /\ sActive
       /\ Len(chan) < ChanCap
       /\ above # {} \/ sLastWm # WatermarkNow
       /\ chan' = Append(chan, [replid |-> pReplid, frames |-> frames, wm |-> wm])
       /\ sCursor' = IF n > 0 THEN frames[n].lsn ELSE sCursor
       /\ sLastWm' = IF caughtUp THEN WatermarkNow ELSE sLastWm
       /\ UNCHANGED <<sActive, pVars, rConn, rVars, writes, faults, hist, mismatch>>

\* apply_frames: rows verbatim, applied_lsn, and on a watermark caught_up_lsn
\* and synced, all in one transaction.
Apply ==
    LET p      == Head(chan)
        fr     == p.frames
        n      == Len(fr)
        keys   == {fr[i].key : i \in 1..n}
        newDb  == [k \in Keys |->
                     IF k \in keys THEN fr[CHOOSE i \in 1..n : fr[i].key = k].row
                     ELSE rDb[k]]
        at     == <<p.replid, p.wm>>
    IN /\ rConn /\ chan # <<>>
       /\ rDb' = newDb
       /\ rApplied' = IF n > 0 THEN fr[n].lsn ELSE rApplied
       /\ chan' = Tail(chan)
       /\ IF p.wm # NoWm
          THEN /\ rCaught' = p.wm
               /\ rSynced' = TRUE
               /\ mismatch' = (mismatch \/ at \notin DOMAIN hist
                                        \/ hist[at] # Live(newDb))
          ELSE UNCHANGED <<rCaught, rSynced, mismatch>>
       /\ UNCHANGED <<rReplid, rDurable, pVars, sVars, rConn, writes, faults, hist>>

\* The replica's own expiry cleanup, on rows it received.
ReplicaCleanup(k) ==
    /\ rDb[k] # NoRow /\ rDb[k].exp
    /\ rDb' = [rDb EXCEPT ![k] = NoRow]
    /\ UNCHANGED <<rReplid, rApplied, rCaught, rSynced, rDurable, pVars, sVars, chan,
                   rConn, writes, faults, hist, mismatch>>

\* The primary's side ended (crash, restart): the replica drains what it
\* already received, then sees EOF.
ReplicaEOF ==
    /\ rConn /\ ~sActive /\ chan = <<>>
    /\ rConn' = FALSE
    /\ UNCHANGED <<pVars, sVars, chan, rVars, writes, faults, hist, mismatch>>

\* Network failure, write timeout, silence limit, replica process restart.
Disconnect ==
    /\ rConn /\ faults < MaxFaults
    /\ rConn' = FALSE
    /\ sActive' = FALSE
    /\ chan' = <<>>
    /\ faults' = faults + 1
    /\ UNCHANGED <<pVars, sCursor, sLastWm, rVars, writes, hist, mismatch>>

ReplicaFlush ==
    /\ rDurable # CurR
    /\ rDurable' = CurR
    /\ UNCHANGED <<rDb, rReplid, rApplied, rCaught, rSynced, pVars, sVars, chan, rConn,
                   writes, faults, hist, mismatch>>

\* Rows, applied_lsn, caught_up_lsn and synced roll back together, unless
\* ReplicaCursorNotAtomic keeps the cursor apart from the rows.
ReplicaPowerLoss ==
    /\ faults < MaxFaults
    /\ rDb' = rDurable.db
    /\ rReplid' = rDurable.replid
    /\ rApplied' = IF ReplicaCursorNotAtomic THEN rApplied ELSE rDurable.applied
    /\ rCaught' = rDurable.caught
    /\ rSynced' = rDurable.synced
    /\ rConn' = FALSE
    /\ sActive' = FALSE
    /\ chan' = <<>>
    /\ faults' = faults + 1
    /\ UNCHANGED <<rDurable, pVars, sCursor, sLastWm, writes, hist, mismatch>>

\* ---------------------------------------------------------------------------
\* Next-state relation & fairness
\* ---------------------------------------------------------------------------

\* Terminal stutter once the bounds are used up and the replica has converged.
\* Deadlock checking still reports any other stuck state.
QuiescentStutter ==
    /\ writes = MaxWrites
    /\ faults = MaxFaults
    /\ Converged
    /\ UNCHANGED vars

Next ==
    \/ \E k \in Keys, v \in Values : ClientSet(k, v)
    \/ \E k \in Keys : ClientDel(k)
    \/ \E k \in Keys : ClientExpire(k)
    \/ \E k \in Keys : PrimaryCleanup(k)
    \/ \E t \in LogLsns : Trim(t)
    \/ PrimaryFlush
    \/ PrimaryProcessCrash
    \/ PrimaryPowerLoss
    \/ GracefulStop
    \/ MarkCleanEarly
    \/ FinishStop
    \/ PrimaryRestart
    \/ Connect
    \/ SendPage
    \/ Apply
    \/ \E k \in Keys : ReplicaCleanup(k)
    \/ ReplicaEOF
    \/ Disconnect
    \/ ReplicaFlush
    \/ ReplicaPowerLoss
    \/ QuiescentStutter

Fairness ==
    /\ WF_vars(PrimaryRestart)
    /\ WF_vars(FinishStop)
    /\ WF_vars(Connect)
    /\ WF_vars(SendPage)
    /\ WF_vars(Apply)
    /\ WF_vars(ReplicaEOF)

Spec == Init /\ [][Next]_vars /\ Fairness

\* ---------------------------------------------------------------------------
\* Safety invariants
\* ---------------------------------------------------------------------------

TypeOK ==
    /\ pState \in {"up", "stopping", "down"}
    /\ pDb \in [Keys -> Row \cup {NoRow}]
    /\ pLog \in [Keys -> [lsn : Nat, del : BOOLEAN]]
    /\ pSeq \in Nat /\ pTrimmed \in Nat
    /\ pReplid \in 1..replidCtr
    /\ pClean \in BOOLEAN /\ pBootChanged \in BOOLEAN
    /\ sActive \in BOOLEAN /\ sCursor \in Nat
    /\ chan \in Seq([replid : Nat, frames : Seq([lsn : Nat, key : Keys,
                                                 row : Row \cup {NoRow}]),
                     wm : Nat \cup {NoWm}])
    /\ Len(chan) <= ChanCap
    /\ rConn \in BOOLEAN
    /\ rDb \in [Keys -> Row \cup {NoRow}]
    /\ rReplid \in 0..replidCtr
    /\ rApplied \in Nat
    /\ rCaught \in Nat \cup {NoWm}
    /\ rSynced \in BOOLEAN
    /\ writes \in 0..MaxWrites
    /\ faults \in 0..MaxFaults
    /\ mismatch \in BOOLEAN

CaughtUpMeansEqual == ~mismatch

ReplicaNeverAhead ==
    (pState = "up" /\ rReplid = pReplid) => rApplied <= WatermarkNow

LsnsUnique ==
    /\ \A a, b \in Logged : a # b => pLog[a].lsn # pLog[b].lsn
    /\ \A k \in Logged : pLog[k].lsn <= pSeq

SyncedMeansCaughtUp == rSynced <=> rCaught # NoWm

\* Every page in flight belongs to the replica's current history, so the
\* replid the watermark is checked against is the replica's.
PagesMatchReplid == \A i \in 1..Len(chan) : chan[i].replid = rReplid

StreamImpliesConnection == sActive => (rConn /\ pState # "down")

\* ---------------------------------------------------------------------------
\* Liveness
\* ---------------------------------------------------------------------------

ReplicaEventuallyConverges == <>[]Converged

=============================================================================
