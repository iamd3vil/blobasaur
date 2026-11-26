---------------------------- MODULE ClusterGossip ----------------------------
(*
 * TLA+ specification of Blobasaur's cluster routing and Chitchat gossip protocol
 * (src/cluster.rs: ClusterManager, gossip_task, route_slot, add_slots, remove_slots,
 * update_gossip_data).
 *
 * Models:
 *   - Nodes owning disjoint hash slots (local_slots) and publishing node_info
 *     into Chitchat gossip (gossip_published)
 *   - Chitchat anti-entropy gossip propagation between reachable live nodes,
 *     plus dead-node eviction after grace period
 *   - Periodic gossip_task refreshing each node's routing table (self.nodes)
 *   - Slot handoff (DELSLOTS -> ADDSLOTS), node crash/restart, and network
 *     partition/heal
 *   - Client write routing via route_slot (Local / Moved / Unserved -> CLUSTERDOWN)
 *
 * Properties checked:
 *   Safety:
 *     - TypeOK
 *     - SingleSlotOwner (at most one node owns a slot at any time)
 *     - NoOrphanedWrites (a write is never accepted locally by a non-owner; #35)
 *     - NoSelfRedirect (route_slot never returns MOVED to self)
 *   Liveness (under weak fairness on healing, restarts, gossip, and gossip_task):
 *     - GossipEventuallyConverges (once stable & connected, every node's routing
 *       table matches every peer's true local_slots)
 *     - RoutingEventuallyAccurate (every node routes every owned slot to its true
 *       owner — no permanent stale MOVED or CLUSTERDOWN)
 *
 * Negative configs prove the model catches real bugs:
 *   - ClusterGossip_Bug35.cfg: AllowUnservedLocalFallback = TRUE -> #35 (safety)
 *   - ClusterGossip_BugPublishRace.cfg: AllowGossipPublishRace = TRUE ->
 *     concurrent add_slots/remove_slots stale gossip publish in src/cluster.rs:573-628 (liveness)
 *)

EXTENDS Integers, FiniteSets, TLC

CONSTANTS
    Nodes,                      \* Set of cluster nodes, e.g. {"n1", "n2", "n3"}
    Slots,                      \* Set of hash slots, e.g. {1, 2}
    InitOwner,                  \* Initial owner mapping [Slots -> Nodes]
    MaxSlotOps,                 \* Bound on slot handoff / mutation steps
    MaxFaults,                  \* Bound on crash + partition events
    AllowUnservedLocalFallback, \* TRUE = pre-#35 bug: handle unserved slots locally
    AllowGossipPublishRace      \* TRUE = src/cluster.rs:573-628 unlocked update_gossip_data race

NONE == "NONE"

\* Default initial slot-owner mappings used by the .cfg files
InitOwnerDef  == [s \in Slots |-> IF s = 1 THEN "n1" ELSE "n2"]
InitOwnerDef1 == [s \in Slots |-> "n1"]

VARIABLES
    alive,              \* [Nodes -> BOOLEAN]
    partitioned,        \* BOOLEAN: TRUE = an arbitrary node is isolated from peers
    isolatedNode,       \* Nodes \cup {NONE}
    localSlots,         \* [Nodes -> SUBSET Slots]                self.local_slots
    gossipVer,          \* [Nodes -> Nat]                         Chitchat self KV version
    gossipPublished,    \* [Nodes -> [ver: Nat, slots: SUBSET Slots]]
    pendingPublish,     \* [Nodes -> SUBSET (SUBSET Slots)]       In-flight update_gossip_data snapshots
    chitchatView,       \* [Nodes -> [Nodes -> [ver: Nat, slots: SUBSET Slots, present: BOOLEAN]]]
    routingTable,       \* [Nodes -> [Nodes -> SUBSET Slots]]     self.nodes (peer -> slots)
    slotOps,            \* Number of slot mutations performed (<= MaxSlotOps)
    faults,             \* Number of crash/partition events (<= MaxFaults)
    orphanWrite         \* Ghost: TRUE if any node accepted a write for a slot it does not own

vars == <<alive, partitioned, isolatedNode, localSlots, gossipVer, gossipPublished,
          pendingPublish, chitchatView, routingTable, slotOps, faults, orphanWrite>>

\* ---------------------------------------------------------------------------
\* Helpers
\* ---------------------------------------------------------------------------

CanCommunicate(a, b) ==
    /\ a # b
    /\ alive[a]
    /\ alive[b]
    /\ (~partitioned \/ (a # isolatedNode /\ b # isolatedNode))

\* Mirrors ClusterManager::route_slot (src/cluster.rs:375-388)
\* Returns "Local", a peer node in Nodes (Moved), or "Unserved".
Route(n, s) ==
    IF s \in localSlots[n] THEN "Local"
    ELSE LET peers == {p \in Nodes \ {n} : s \in routingTable[n][p]} IN
         IF peers # {} THEN CHOOSE p \in peers : TRUE
         ELSE IF AllowUnservedLocalFallback THEN "Local"
         ELSE "Unserved"

SlotOwners(s) == {n \in Nodes : s \in localSlots[n]}

AllConnectedAndAlive ==
    /\ \A n \in Nodes : alive[n]
    /\ ~partitioned

TopologyConverged ==
    /\ AllConnectedAndAlive
    /\ \A n \in Nodes : pendingPublish[n] = {}
    /\ \A n, p \in Nodes : p # n => routingTable[n][p] = localSlots[p]

\* ---------------------------------------------------------------------------
\* Initial State
\* ---------------------------------------------------------------------------

InitLocalSlots == [n \in Nodes |-> {s \in Slots : InitOwner[s] = n}]

Init ==
    /\ alive           = [n \in Nodes |-> TRUE]
    /\ partitioned     = FALSE
    /\ isolatedNode    = NONE
    /\ localSlots      = InitLocalSlots
    /\ gossipVer       = [n \in Nodes |-> 1]
    /\ gossipPublished = [n \in Nodes |-> [ver |-> 1, slots |-> InitLocalSlots[n]]]
    /\ pendingPublish  = [n \in Nodes |-> {}]
    /\ chitchatView    = [n \in Nodes |->
                            [p \in Nodes |-> [ver |-> 1, slots |-> InitLocalSlots[p], present |-> TRUE]]]
    /\ routingTable    = [n \in Nodes |->
                            [p \in Nodes |-> IF p = n THEN {} ELSE InitLocalSlots[p]]]
    /\ slotOps         = 0
    /\ faults          = 0
    /\ orphanWrite     = FALSE

\* ---------------------------------------------------------------------------
\* Slot Management (CLUSTER DELSLOTS / ADDSLOTS + update_gossip_data)
\* ---------------------------------------------------------------------------

\* Safe operator slot removal (CLUSTER DELSLOTS)
DelSlot(n, s) ==
    /\ alive[n]
    /\ slotOps < MaxSlotOps
    /\ s \in localSlots[n]
    /\ LET newSlots == localSlots[n] \ {s} IN
       /\ localSlots' = [localSlots EXCEPT ![n] = newSlots]
       /\ slotOps'    = slotOps + 1
       /\ IF AllowGossipPublishRace THEN
              \* Models src/cluster.rs:591-628: drops local_slots write lock,
              \* snapshots slots in update_gossip_data(), then later locks chitchat.
              /\ pendingPublish' = [pendingPublish EXCEPT ![n] = @ \cup {newSlots}]
              /\ UNCHANGED <<gossipVer, gossipPublished, chitchatView>>
          ELSE
              \* Atomic publish serialized with local_slots mutation.
              LET v == gossipVer[n] + 1 IN
              /\ gossipVer'       = [gossipVer EXCEPT ![n] = v]
              /\ gossipPublished' = [gossipPublished EXCEPT ![n] = [ver |-> v, slots |-> newSlots]]
              /\ chitchatView'    = [chitchatView EXCEPT ![n][n] = [ver |-> v, slots |-> newSlots, present |-> TRUE]]
              /\ UNCHANGED pendingPublish
       /\ UNCHANGED <<alive, partitioned, isolatedNode, routingTable, faults, orphanWrite>>

\* Safe operator slot addition (CLUSTER ADDSLOTS for an unowned slot)
AddSlot(n, s) ==
    /\ alive[n]
    /\ slotOps < MaxSlotOps
    /\ SlotOwners(s) = {}
    /\ LET newSlots == localSlots[n] \cup {s} IN
       /\ localSlots' = [localSlots EXCEPT ![n] = newSlots]
       /\ slotOps'    = slotOps + 1
       /\ IF AllowGossipPublishRace THEN
              /\ pendingPublish' = [pendingPublish EXCEPT ![n] = @ \cup {newSlots}]
              /\ UNCHANGED <<gossipVer, gossipPublished, chitchatView>>
          ELSE
              LET v == gossipVer[n] + 1 IN
              /\ gossipVer'       = [gossipVer EXCEPT ![n] = v]
              /\ gossipPublished' = [gossipPublished EXCEPT ![n] = [ver |-> v, slots |-> newSlots]]
              /\ chitchatView'    = [chitchatView EXCEPT ![n][n] = [ver |-> v, slots |-> newSlots, present |-> TRUE]]
              /\ UNCHANGED pendingPublish
       /\ UNCHANGED <<alive, partitioned, isolatedNode, routingTable, faults, orphanWrite>>

\* Finish an in-flight update_gossip_data() call (only active when AllowGossipPublishRace = TRUE)
FinishGossipPublish(n, snap) ==
    /\ alive[n]
    /\ snap \in pendingPublish[n]
    /\ LET v == gossipVer[n] + 1 IN
       /\ gossipVer'       = [gossipVer EXCEPT ![n] = v]
       /\ gossipPublished' = [gossipPublished EXCEPT ![n] = [ver |-> v, slots |-> snap]]
       /\ chitchatView'    = [chitchatView EXCEPT ![n][n] = [ver |-> v, slots |-> snap, present |-> TRUE]]
       /\ pendingPublish'  = [pendingPublish EXCEPT ![n] = @ \ {snap}]
       /\ UNCHANGED <<alive, partitioned, isolatedNode, localSlots, routingTable,
                      slotOps, faults, orphanWrite>>

\* ---------------------------------------------------------------------------
\* Chitchat Gossip Propagation, Dead-Node Eviction & gossip_task Refresh
\* ---------------------------------------------------------------------------

\* Node dst receives a gossip digest from reachable peer src and merges any
\* newer version (or re-discovers an evicted live node).
GossipSync(src, dst) ==
    /\ CanCommunicate(src, dst)
    /\ \E p \in Nodes :
        /\ chitchatView[src][p].present
        /\ ( \/ ~chitchatView[dst][p].present
             \/ chitchatView[src][p].ver > chitchatView[dst][p].ver )
        /\ chitchatView' = [chitchatView EXCEPT ![dst][p] = chitchatView[src][p]]
    /\ UNCHANGED <<alive, partitioned, isolatedNode, localSlots, gossipVer, gossipPublished,
                   pendingPublish, routingTable, slotOps, faults, orphanWrite>>

\* Chitchat evicts a crashed or partitioned-away peer after dead_node_grace_period
EvictUnreachablePeer(n, p) ==
    /\ alive[n]
    /\ p # n
    /\ ~CanCommunicate(n, p)
    /\ chitchatView[n][p].present
    /\ chitchatView' = [chitchatView EXCEPT ![n][p].present = FALSE]
    /\ UNCHANGED <<alive, partitioned, isolatedNode, localSlots, gossipVer, gossipPublished,
                   pendingPublish, routingTable, slotOps, faults, orphanWrite>>

\* Periodic ClusterManager::gossip_task tick (src/cluster.rs:231-342)
\* Copies all non-self present entries from chitchat node_states() into self.nodes.
GossipTaskTick(n) ==
    /\ alive[n]
    /\ LET newMap == [p \in Nodes |->
                        IF p # n /\ chitchatView[n][p].present
                        THEN chitchatView[n][p].slots
                        ELSE {}]
       IN
       /\ newMap # routingTable[n]
       /\ routingTable' = [routingTable EXCEPT ![n] = newMap]
       /\ UNCHANGED <<alive, partitioned, isolatedNode, localSlots, gossipVer, gossipPublished,
                      pendingPublish, chitchatView, slotOps, faults, orphanWrite>>

\* ---------------------------------------------------------------------------
\* Client Write Routing (check_key_route / route_slot)
\* ---------------------------------------------------------------------------

\* A client sends a write for slot s to live node n.
\* If route_slot returns Local when n does NOT own s, an orphaned write occurs (#35).
ClientWrite(n, s) ==
    /\ alive[n]
    /\ Route(n, s) = "Local"
    /\ orphanWrite' = (orphanWrite \/ (s \notin localSlots[n]))
    /\ UNCHANGED <<alive, partitioned, isolatedNode, localSlots, gossipVer, gossipPublished,
                   pendingPublish, chitchatView, routingTable, slotOps, faults>>

\* ---------------------------------------------------------------------------
\* Faults & Recovery (Node Crash/Restart, Network Partition/Heal)
\* ---------------------------------------------------------------------------

CrashNode(n) ==
    /\ alive[n]
    /\ faults < MaxFaults
    /\ alive'          = [alive EXCEPT ![n] = FALSE]
    /\ pendingPublish' = [pendingPublish EXCEPT ![n] = {}]
    /\ faults'         = faults + 1
    /\ UNCHANGED <<partitioned, isolatedNode, localSlots, gossipVer, gossipPublished,
                   chitchatView, routingTable, slotOps, orphanWrite>>

RestartNode(n) ==
    /\ ~alive[n]
    /\ LET v == gossipVer[n] + 1 IN
       /\ alive'           = [alive EXCEPT ![n] = TRUE]
       /\ gossipVer'       = [gossipVer EXCEPT ![n] = v]
       /\ gossipPublished' = [gossipPublished EXCEPT ![n] = [ver |-> v, slots |-> localSlots[n]]]
       /\ chitchatView'    = [chitchatView EXCEPT ![n][n] = [ver |-> v, slots |-> localSlots[n], present |-> TRUE]]
       /\ UNCHANGED <<partitioned, isolatedNode, localSlots, pendingPublish,
                      routingTable, slotOps, faults, orphanWrite>>

StartPartition(n) ==
    /\ ~partitioned
    /\ faults < MaxFaults
    /\ partitioned'  = TRUE
    /\ isolatedNode' = n
    /\ faults'       = faults + 1
    /\ UNCHANGED <<alive, localSlots, gossipVer, gossipPublished, pendingPublish,
                   chitchatView, routingTable, slotOps, orphanWrite>>

HealPartition ==
    /\ partitioned
    /\ partitioned'  = FALSE
    /\ isolatedNode' = NONE
    /\ UNCHANGED <<alive, localSlots, gossipVer, gossipPublished, pendingPublish,
                   chitchatView, routingTable, slotOps, faults, orphanWrite>>

\* ---------------------------------------------------------------------------
\* Next-State Relation & Fairness
\* ---------------------------------------------------------------------------

\* Clean terminal stutter once bounded mutations/faults are done and cluster converged.
\* Keeps TLC deadlock checking enabled for any non-converged stuck state.
QuiescentStutter ==
    /\ TopologyConverged
    /\ slotOps = MaxSlotOps
    /\ faults = MaxFaults
    /\ UNCHANGED vars

Next ==
    \/ \E n \in Nodes, s \in Slots : DelSlot(n, s)
    \/ \E n \in Nodes, s \in Slots : AddSlot(n, s)
    \/ \E n \in Nodes : \E snap \in pendingPublish[n] : FinishGossipPublish(n, snap)
    \/ \E a, b \in Nodes : GossipSync(a, b)
    \/ \E n, p \in Nodes : EvictUnreachablePeer(n, p)
    \/ \E n \in Nodes : GossipTaskTick(n)
    \/ \E n \in Nodes, s \in Slots : ClientWrite(n, s)
    \/ \E n \in Nodes : CrashNode(n)
    \/ \E n \in Nodes : RestartNode(n)
    \/ \E n \in Nodes : StartPartition(n)
    \/ HealPartition
    \/ QuiescentStutter

\* Weak fairness on recovery, pending gossip publishes, gossip propagation, and gossip_task.
Fairness ==
    /\ WF_vars(HealPartition)
    /\ \A n \in Nodes : WF_vars(RestartNode(n))
    /\ \A n \in Nodes : WF_vars(\E snap \in pendingPublish[n] : FinishGossipPublish(n, snap))
    /\ \A a, b \in Nodes : WF_vars(GossipSync(a, b))
    /\ \A n \in Nodes : WF_vars(GossipTaskTick(n))

Spec == Init /\ [][Next]_vars /\ Fairness

\* ---------------------------------------------------------------------------
\* Safety Invariants
\* ---------------------------------------------------------------------------

TypeOK ==
    /\ alive \in [Nodes -> BOOLEAN]
    /\ partitioned \in BOOLEAN
    /\ isolatedNode \in Nodes \cup {NONE}
    /\ slotOps \in 0..MaxSlotOps
    /\ faults \in 0..MaxFaults
    /\ orphanWrite \in BOOLEAN

SingleSlotOwner ==
    \A s \in Slots : Cardinality(SlotOwners(s)) <= 1

NoOrphanedWrites == ~orphanWrite

NoSelfRedirect ==
    \A n \in Nodes, s \in Slots : Route(n, s) # n

\* ---------------------------------------------------------------------------
\* Temporal Properties (Liveness)
\* ---------------------------------------------------------------------------

RoutingAccurate ==
    \A n \in Nodes, s \in Slots :
        IF SlotOwners(s) = {} THEN Route(n, s) = "Unserved"
        ELSE LET owner == CHOOSE o \in SlotOwners(s) : TRUE IN
             Route(n, s) = IF n = owner THEN "Local" ELSE owner

\* Once faults and slot mutations stop, all nodes eventually recover, heal, and
\* converge their routing tables to the true localSlots of every peer.
GossipEventuallyConverges ==
    <>[]TopologyConverged

\* Once converged, every node routes every slot accurately (Local / Moved(owner) / Unserved).
RoutingEventuallyAccurate ==
    <>[]RoutingAccurate

=============================================================================
