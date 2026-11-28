---------------------------- MODULE Failover ----------------------------
(*
 * TLA+ specification of Blobasaur's automatic failover (FAILOVER.md), which
 * follows Redis Cluster: configEpoch-versioned slot claims, FAIL agreement,
 * replica elections voted by a majority of slot-owning primaries, promotion,
 * demotion of a returning primary and re-pointing of the other replicas.
 *
 * Abstractions:
 *   - Failure detection: a node may mark a peer FAIL only while a majority of
 *     the slot-owning primaries it knows can't reach the peer; it may clear the
 *     mark once it reaches the peer again. Timers are not modeled.
 *   - Gossip: Learn(n, m) merges m's slot table and current epoch into n's.
 *   - Vote requests and replies are messages that may be delayed forever or
 *     delivered more than once (msgs only grows).
 *   - Data is not modeled: `synced` says whether a replica holds a full copy of
 *     its primary (Replication.tla covers the stream itself).
 *   - The vote backoff (Redis: no vote for another replica of the same primary
 *     for 2 * node_timeout) is ForgetVote, enabled once that election is over
 *     and its winner, if any, is in the voter's table. That encodes the timing
 *     assumption that elections end and claims spread within the backoff.
 *   - Initial claims have distinct owners per slot, so epoch collisions only
 *     arise from TAKEOVER, which is not modeled.
 *
 * Properties checked:
 *   Safety:
 *     - TypeOK
 *     - OneWinnerPerEpoch: at most one candidate wins an election epoch.
 *     - WinnerReplacesCurrentOwner: a winner only takes slots its primary
 *       held under the highest claim made so far (no failover from stale
 *       config, which would discard the current owner's writes).
 *     - UniqueClaims: two tables never bind a slot to different owners under
 *       the same epoch.
 *     - TableEpochsNeverDecrease (action property): no node rebinds a slot to
 *       a lower epoch, so a returning old primary can't take its slots back.
 *   Liveness (Failover_Liveness.cfg):
 *     - EventuallyConverges: once faults stop, every live node has the same
 *       table, every slot is owned by a live primary, and every replica is
 *       synced to a live primary; in particular a replica replaces a primary
 *       that never comes back.
 *
 * Negative configs flip one flag each:
 *   - VolatileVotes: last_vote_epoch and the vote backoff are not persisted
 *     (two winners). Losing only last_vote_epoch is harmless here: the
 *     persisted backoff and the slot-epoch check refuse the second vote.
 *   - NoVoteBackoff: votes again for the same primary at once (stale winner).
 *   - NoSlotEpochCheck: votes without comparing slot epochs (stale winner).
 *   - TableIgnoresEpoch: gossip overwrites bindings regardless of epoch.
 *)

EXTENDS Integers, FiniteSets, TLC

CONSTANTS
    Primaries,          \* Initial primaries, e.g. {"p1", "p2", "p3"}
    Replicas,           \* Initial replicas, all of FailedPrimary
    FailedPrimary,      \* The primary the replicas follow, e.g. "p1"
    Slots,              \* e.g. {"s1", "s2", "s3"}
    InitOwner,          \* [Slots -> Primaries], a bijection
    Permanent,          \* Nodes that never restart after a crash
    MaxEpoch,           \* Bound on election epochs
    MaxFaults,          \* Bound on crashes, isolations and aborted elections
    VolatileVotes,
    NoVoteBackoff,
    NoSlotEpochCheck,
    TableIgnoresEpoch

Nodes == Primaries \cup Replicas
NONE == "NONE"
Epochs == 0..MaxEpoch

InitOwnerDef == [s \in Slots |->
                    CASE s = "s1" -> "p1" [] s = "s2" -> "p2" [] OTHER -> "p3"]

VARIABLES
    alive,          \* [Nodes -> BOOLEAN]
    isolated,       \* A node cut off from every other node, or NONE
    role,           \* [Nodes -> {"primary", "replica"}]          persisted
    primaryOf,      \* [Nodes -> Nodes \cup {NONE}]               persisted
    curEpoch,       \* [Nodes -> Epochs]                          persisted
    lastVote,       \* [Nodes -> Epochs]                          persisted
    lastVoteFor,    \* [Nodes -> [Nodes -> Epochs]] vote backoff, persisted
    table,          \* [Nodes -> [Slots -> [owner, epoch]]]       persisted
    synced,         \* [Nodes -> BOOLEAN]                         persisted
    fail,           \* [Nodes -> SUBSET Nodes] FAIL marks         volatile
    electing,       \* [Nodes -> Epochs] epoch of the running election, 0 none
    votes,          \* [Nodes -> SUBSET Nodes] votes for that election
    msgs,           \* Vote requests and replies
    faults,
    winners,        \* Ghost: [1..MaxEpoch -> SUBSET Nodes]
    latest,         \* Ghost: [Slots -> [owner, epoch]] highest claim made
    staleWin        \* Ghost: a winner took slots from a superseded claim

vars == <<alive, isolated, role, primaryOf, curEpoch, lastVote, lastVoteFor, table,
          synced, fail, electing, votes, msgs, faults, winners, latest, staleWin>>

\* ---------------------------------------------------------------------------
\* Helpers
\* ---------------------------------------------------------------------------

Max(a, b) == IF a > b THEN a ELSE b

Reach(a, b) ==
    /\ alive[a] /\ alive[b]
    /\ (a = b \/ isolated = NONE \/ (a # isolated /\ b # isolated))

Owners(t) == {t[s].owner : s \in Slots}
Quorum(t) == Cardinality(Owners(t)) \div 2 + 1
SlotsOf(t, p) == {s \in Slots : t[s].owner = p}
EpochOf(t, p) == LET ss == SlotsOf(t, p) IN
                 IF ss = {} THEN 0
                 ELSE CHOOSE e \in {t[s].epoch : s \in ss} :
                        \A s \in ss : t[s].epoch <= e

Merge(tn, tm) ==
    [s \in Slots |->
        IF TableIgnoresEpoch THEN (IF tm[s] # tn[s] THEN tm[s] ELSE tn[s])
        ELSE IF tm[s].epoch > tn[s].epoch THEN tm[s] ELSE tn[s]]

\* Both message kinds share one record shape, which TLC needs to compare them.
Req(c, e, p, ss, pe) == [type |-> "req", cand |-> c, epoch |-> e, primary |-> p,
                         slots |-> ss, pEpoch |-> pe, voter |-> NONE]
Ack(v, c, e) == [type |-> "ack", cand |-> c, epoch |-> e, primary |-> NONE,
                 slots |-> {}, pEpoch |-> 0, voter |-> v]

AliveNodes == {n \in Nodes : alive[n]}

Converged ==
    /\ isolated = NONE
    /\ \A n \in Nodes : n \notin Permanent => alive[n]
    /\ \A n, m \in AliveNodes : table[n] = table[m]
    /\ \A n \in AliveNodes :
         /\ \A s \in Slots : alive[table[n][s].owner]
                             /\ role[table[n][s].owner] = "primary"
         /\ role[n] = "primary" => SlotsOf(table[n], n) # {}
         /\ role[n] = "replica" => /\ synced[n]
                                   /\ alive[primaryOf[n]]
                                   /\ SlotsOf(table[n], primaryOf[n]) # {}

\* ---------------------------------------------------------------------------
\* Initial state: every primary owns its slots at epoch 0; the replicas are
\* synced copies of FailedPrimary.
\* ---------------------------------------------------------------------------

InitTable == [s \in Slots |-> [owner |-> InitOwner[s], epoch |-> 0]]

Init ==
    /\ alive = [n \in Nodes |-> TRUE]
    /\ isolated = NONE
    /\ role = [n \in Nodes |-> IF n \in Primaries THEN "primary" ELSE "replica"]
    /\ primaryOf = [n \in Nodes |-> IF n \in Replicas THEN FailedPrimary ELSE NONE]
    /\ curEpoch = [n \in Nodes |-> 0]
    /\ lastVote = [n \in Nodes |-> 0]
    /\ lastVoteFor = [n \in Nodes |-> [p \in Nodes |-> 0]]
    /\ table = [n \in Nodes |-> InitTable]
    /\ synced = [n \in Nodes |-> n \in Replicas]
    /\ fail = [n \in Nodes |-> {}]
    /\ electing = [n \in Nodes |-> 0]
    /\ votes = [n \in Nodes |-> {}]
    /\ msgs = {}
    /\ faults = 0
    /\ winners = [e \in 1..MaxEpoch |-> {}]
    /\ latest = InitTable
    /\ staleWin = FALSE

\* ---------------------------------------------------------------------------
\* Gossip: merge a peer's slot table and epoch. A primary that lost its last
\* slot becomes a replica of the node that took it; a replica whose primary's
\* slot moved follows the new owner. Either one must copy from scratch.
\* ---------------------------------------------------------------------------

Learn(n, m) ==
    LET newT     == Merge(table[n], table[m])
        mine     == SlotsOf(table[n], n)
        lostAll  == role[n] = "primary" /\ mine # {} /\ SlotsOf(newT, n) = {}
        moved    == {s \in Slots : table[n][s].owner = primaryOf[n]
                                   /\ newT[s].owner # primaryOf[n]}
        repoint  == role[n] = "replica" /\ moved # {}
        follow   == IF lostAll THEN newT[CHOOSE s \in mine : TRUE].owner
                    ELSE newT[CHOOSE s \in moved : TRUE].owner
    IN /\ n # m /\ Reach(n, m)
       /\ (newT # table[n] \/ curEpoch[m] > curEpoch[n])
       /\ table' = [table EXCEPT ![n] = newT]
       /\ curEpoch' = [curEpoch EXCEPT ![n] = Max(@, curEpoch[m])]
       /\ IF lostAll \/ repoint
          THEN /\ role' = [role EXCEPT ![n] = "replica"]
               /\ primaryOf' = [primaryOf EXCEPT ![n] = follow]
               /\ synced' = [synced EXCEPT ![n] = FALSE]
               /\ electing' = [electing EXCEPT ![n] = 0]
          ELSE UNCHANGED <<role, primaryOf, synced, electing>>
       /\ UNCHANGED <<alive, isolated, lastVote, lastVoteFor, fail, votes, msgs,
                      faults, winners, latest, staleWin>>

\* A replica finishes copying from its primary.
Sync(r) ==
    /\ role[r] = "replica" /\ ~synced[r]
    /\ Reach(r, primaryOf[r]) /\ role[primaryOf[r]] = "primary"
    /\ synced' = [synced EXCEPT ![r] = TRUE]
    /\ UNCHANGED <<alive, isolated, role, primaryOf, curEpoch, lastVote, lastVoteFor,
                   table, fail, electing, votes, msgs, faults, winners, latest,
                   staleWin>>

\* ---------------------------------------------------------------------------
\* Failure detection (PFAIL reports reaching a majority -> FAIL)
\* ---------------------------------------------------------------------------

\* Only slot owners are marked: no election or vote depends on a replica's mark.
MarkFail(n, p) ==
    /\ alive[n] /\ p # n /\ p \notin fail[n]
    /\ p \in Owners(table[n])
    /\ Cardinality({q \in Owners(table[n]) : Reach(q, p)}) < Quorum(table[n])
    /\ fail' = [fail EXCEPT ![n] = @ \cup {p}]
    /\ UNCHANGED <<alive, isolated, role, primaryOf, curEpoch, lastVote, lastVoteFor,
                   table, synced, electing, votes, msgs, faults, winners, latest,
                   staleWin>>

ClearFail(n, p) ==
    /\ p \in fail[n] /\ Reach(n, p)
    /\ fail' = [fail EXCEPT ![n] = @ \ {p}]
    /\ UNCHANGED <<alive, isolated, role, primaryOf, curEpoch, lastVote, lastVoteFor,
                   table, synced, electing, votes, msgs, faults, winners, latest,
                   staleWin>>

\* ---------------------------------------------------------------------------
\* Election
\* ---------------------------------------------------------------------------

\* A synced replica whose primary is FAIL and owns slots asks every primary
\* for a vote in a new epoch, listing the slots and epoch of its primary's claim.
StartElection(r) ==
    LET p == primaryOf[r]
        e == curEpoch[r] + 1
    IN /\ alive[r] /\ role[r] = "replica" /\ synced[r] /\ electing[r] = 0
       /\ p \in fail[r]
       /\ SlotsOf(table[r], p) # {}
       /\ e <= MaxEpoch
       /\ curEpoch' = [curEpoch EXCEPT ![r] = e]
       /\ electing' = [electing EXCEPT ![r] = e]
       /\ votes' = [votes EXCEPT ![r] = {}]
       /\ msgs' = msgs \cup {Req(r, e, p, SlotsOf(table[r], p), EpochOf(table[r], p))}
       /\ UNCHANGED <<alive, isolated, role, primaryOf, lastVote, lastVoteFor, table,
                      synced, fail, faults, winners, latest, staleWin>>

\* BLOBASAUR.FAILOVER-AUTH on a slot-owning primary.
Vote(v, m) ==
    /\ m.type = "req"
    /\ Reach(v, m.cand)
    /\ role[v] = "primary" /\ SlotsOf(table[v], v) # {}
    /\ m.epoch >= curEpoch[v]
    /\ m.epoch > lastVote[v]
    /\ m.primary \in fail[v]
    /\ NoVoteBackoff \/ lastVoteFor[v][m.primary] = 0
    /\ NoSlotEpochCheck \/ \A s \in m.slots : table[v][s].epoch <= m.pEpoch
    /\ curEpoch' = [curEpoch EXCEPT ![v] = m.epoch]
    /\ lastVote' = [lastVote EXCEPT ![v] = m.epoch]
    /\ lastVoteFor' = [lastVoteFor EXCEPT ![v][m.primary] = m.epoch]
    /\ msgs' = msgs \cup {Ack(v, m.cand, m.epoch)}
    /\ UNCHANGED <<alive, isolated, role, primaryOf, table, synced, fail, electing,
                   votes, faults, winners, latest, staleWin>>

\* The backoff ends once that election is over and its winner, if any, is
\* in the voter's table (the timing assumption above).
ForgetVote(v, p) ==
    LET e == lastVoteFor[v][p] IN
    /\ e > 0
    /\ \A c \in Nodes : electing[c] # e
    /\ \A w \in winners[e] : \E s \in Slots : table[v][s].epoch >= e
    /\ lastVoteFor' = [lastVoteFor EXCEPT ![v][p] = 0]
    /\ UNCHANGED <<alive, isolated, role, primaryOf, curEpoch, lastVote, table, synced,
                   fail, electing, votes, msgs, faults, winners, latest, staleWin>>

Collect(r, m) ==
    /\ m.type = "ack" /\ m.cand = r
    /\ electing[r] = m.epoch /\ m.voter \notin votes[r]
    /\ Reach(r, m.voter)
    /\ votes' = [votes EXCEPT ![r] = @ \cup {m.voter}]
    /\ UNCHANGED <<alive, isolated, role, primaryOf, curEpoch, lastVote, lastVoteFor,
                   table, synced, fail, electing, msgs, faults, winners, latest,
                   staleWin>>

\* A majority of the slot-owning primaries voted: claim the primary's slots
\* at the election epoch and become a primary.
Win(r) ==
    LET e     == electing[r]
        p     == primaryOf[r]
        taken == SlotsOf(table[r], p)
    IN /\ alive[r] /\ e > 0
       /\ taken # {}
       /\ Cardinality(votes[r]) >= Quorum(table[r])
       /\ role' = [role EXCEPT ![r] = "primary"]
       /\ primaryOf' = [primaryOf EXCEPT ![r] = NONE]
       /\ table' = [table EXCEPT ![r] =
                      [s \in Slots |-> IF s \in taken THEN [owner |-> r, epoch |-> e]
                                       ELSE table[r][s]]]
       /\ electing' = [electing EXCEPT ![r] = 0]
       /\ votes' = [votes EXCEPT ![r] = {}]
       /\ winners' = [winners EXCEPT ![e] = @ \cup {r}]
       /\ staleWin' = (staleWin \/ \E s \in taken : latest[s].owner # p)
       /\ latest' = [s \in Slots |->
                       IF s \in taken /\ e > latest[s].epoch
                       THEN [owner |-> r, epoch |-> e] ELSE latest[s]]
       /\ UNCHANGED <<alive, isolated, curEpoch, lastVote, lastVoteFor, synced, fail,
                      msgs, faults>>

\* ---------------------------------------------------------------------------
\* Faults
\* ---------------------------------------------------------------------------

AbortElection(r) ==
    /\ electing[r] > 0 /\ faults < MaxFaults
    /\ electing' = [electing EXCEPT ![r] = 0]
    /\ votes' = [votes EXCEPT ![r] = {}]
    /\ faults' = faults + 1
    /\ UNCHANGED <<alive, isolated, role, primaryOf, curEpoch, lastVote, lastVoteFor,
                   table, synced, fail, msgs, winners, latest, staleWin>>

Crash(n) ==
    /\ alive[n] /\ faults < MaxFaults
    /\ alive' = [alive EXCEPT ![n] = FALSE]
    /\ fail' = [fail EXCEPT ![n] = {}]
    /\ electing' = [electing EXCEPT ![n] = 0]
    /\ votes' = [votes EXCEPT ![n] = {}]
    /\ faults' = faults + 1
    /\ UNCHANGED <<isolated, role, primaryOf, curEpoch, lastVote, lastVoteFor, table,
                   synced, msgs, winners, latest, staleWin>>

Restart(n) ==
    /\ ~alive[n] /\ n \notin Permanent
    /\ alive' = [alive EXCEPT ![n] = TRUE]
    /\ lastVote' = IF VolatileVotes THEN [lastVote EXCEPT ![n] = 0] ELSE lastVote
    /\ lastVoteFor' = IF VolatileVotes THEN [lastVoteFor EXCEPT ![n] = [p \in Nodes |-> 0]]
                      ELSE lastVoteFor
    /\ UNCHANGED <<isolated, role, primaryOf, curEpoch, table, synced,
                   fail, electing, votes, msgs, faults, winners, latest, staleWin>>

Isolate(n) ==
    /\ isolated = NONE /\ faults < MaxFaults
    /\ isolated' = n
    /\ faults' = faults + 1
    /\ UNCHANGED <<alive, role, primaryOf, curEpoch, lastVote, lastVoteFor, table,
                   synced, fail, electing, votes, msgs, winners, latest, staleWin>>

Heal ==
    /\ isolated # NONE
    /\ isolated' = NONE
    /\ UNCHANGED <<alive, role, primaryOf, curEpoch, lastVote, lastVoteFor, table,
                   synced, fail, electing, votes, msgs, faults, winners, latest,
                   staleWin>>

\* ---------------------------------------------------------------------------
\* Next-state relation & fairness
\* ---------------------------------------------------------------------------

Next ==
    \/ \E n, m \in Nodes : Learn(n, m)
    \/ \E r \in Nodes : Sync(r)
    \/ \E n, p \in Nodes : MarkFail(n, p)
    \/ \E n, p \in Nodes : ClearFail(n, p)
    \/ \E r \in Nodes : StartElection(r)
    \/ \E v \in Nodes, m \in msgs : Vote(v, m)
    \/ \E v, p \in Nodes : ForgetVote(v, p)
    \/ \E r \in Nodes, m \in msgs : Collect(r, m)
    \/ \E r \in Nodes : Win(r)
    \/ \E r \in Nodes : AbortElection(r)
    \/ \E n \in Nodes : Crash(n)
    \/ \E n \in Nodes : Restart(n)
    \/ \E n \in Nodes : Isolate(n)
    \/ Heal

Fairness ==
    /\ \A n, m \in Nodes : WF_vars(Learn(n, m))
    /\ \A r \in Nodes : WF_vars(Sync(r))
    /\ \A n, p \in Nodes : WF_vars(MarkFail(n, p))
    /\ \A r \in Nodes : WF_vars(StartElection(r))
    /\ \A v \in Nodes : WF_vars(\E m \in msgs : Vote(v, m))
    /\ \A v, p \in Nodes : WF_vars(ForgetVote(v, p))
    /\ \A r \in Nodes : WF_vars(\E m \in msgs : Collect(r, m))
    /\ \A r \in Nodes : WF_vars(Win(r))
    /\ \A n \in Nodes : WF_vars(Restart(n))
    /\ WF_vars(Heal)

Spec == Init /\ [][Next]_vars /\ Fairness

\* ---------------------------------------------------------------------------
\* Properties
\* ---------------------------------------------------------------------------

Binding == [owner : Nodes, epoch : Epochs]

TypeOK ==
    /\ alive \in [Nodes -> BOOLEAN]
    /\ isolated \in Nodes \cup {NONE}
    /\ role \in [Nodes -> {"primary", "replica"}]
    /\ primaryOf \in [Nodes -> Nodes \cup {NONE}]
    /\ curEpoch \in [Nodes -> Epochs]
    /\ lastVote \in [Nodes -> Epochs]
    /\ table \in [Nodes -> [Slots -> Binding]]
    /\ synced \in [Nodes -> BOOLEAN]
    /\ electing \in [Nodes -> Epochs]
    /\ faults \in 0..MaxFaults

OneWinnerPerEpoch == \A e \in 1..MaxEpoch : Cardinality(winners[e]) <= 1

WinnerReplacesCurrentOwner == ~staleWin

UniqueClaims ==
    \A n, m \in Nodes, s \in Slots :
        table[n][s].epoch = table[m][s].epoch => table[n][s].owner = table[m][s].owner

TableEpochsNeverDecrease ==
    [][\A n \in Nodes, s \in Slots : table'[n][s].epoch >= table[n][s].epoch]_vars

EventuallyConverges == <>[]Converged

=============================================================================
