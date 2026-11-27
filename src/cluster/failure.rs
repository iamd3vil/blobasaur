//! PFAIL/FAIL failure detection and minority fencing state machine.
//!
//! Follow `FAILOVER.md` and `specs/Failover.tla`:
//! - **PFAIL** (local): a peer's chitchat `(generation_id, heartbeat)` has not
//!   advanced for `node_timeout`, or the peer vanished from chitchat (evicted).
//! - **FAIL** (agreement): a majority of the slot-owning primaries in this
//!   node's slot table (`owners.len() / 2 + 1`, counting any dead owner in
//!   `owners`) reported the node in `pfail` or `fail` within `2 * node_timeout`,
//!   or a live peer gossiped it in `fail`.
//! - **Clearing FAIL**: immediately once reachable again if the node is a
//!   replica or owns no slots; otherwise after `2 * node_timeout` of continuous
//!   reachability with no failover taking its slots.
//! - **Minority**: fewer than a majority of slot-owning primaries (self
//!   included when this node owns slots) have been reachable within
//!   `node_timeout`. A single-primary cluster (quorum 1) is never in a minority.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::time::{Duration, Instant};

/// Snapshot of one peer observed in chitchat `node_states()` (newest generation).
#[derive(Debug, Clone, Default)]
pub(crate) struct PeerHeartbeatView {
    pub generation_id: u64,
    pub heartbeat: u64,
    pub pfail: Vec<String>,
    pub fail: Vec<String>,
}

/// State transitions produced by one failure-detection tick.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct FailureTickOutcome {
    /// True when `pfail` or `fail` changed and should be re-gossiped.
    pub gossip_changed: bool,
    pub newly_pfail: Vec<String>,
    pub cleared_pfail: Vec<String>,
    pub newly_failed: Vec<String>,
    pub cleared_failed: Vec<String>,
    pub entered_minority: bool,
    pub exited_minority: bool,
}

#[derive(Debug, Clone)]
struct HeartbeatState {
    generation_id: u64,
    heartbeat: u64,
    /// False while the peer is known only from persisted slot bindings and has
    /// never appeared in chitchat yet.
    seen_in_gossip: bool,
    /// Monotonic time when `(generation_id, heartbeat)` last advanced (or
    /// tracker start time for an unseen slot owner).
    last_progress: Instant,
}

#[derive(Debug, Clone)]
struct FailRecord {
    failed_at: Instant,
    /// When the node became reachable again after `failed_at`, for the
    /// `2 * node_timeout` clear window on slot-owning primaries.
    reachable_since: Option<Instant>,
}

/// Clock-injectable failure detector and minority tracker.
#[derive(Debug, Clone)]
pub(crate) struct FailureTracker {
    self_id: String,
    node_timeout: Duration,
    started_at: Instant,
    heartbeats: HashMap<String, HeartbeatState>,
    pfail: BTreeSet<String>,
    failed: BTreeMap<String, FailRecord>,
    /// Failure reports: `target -> (reporter -> last_seen_at)`.
    reports: HashMap<String, HashMap<String, Instant>>,
    /// Per-peer `pfail` from its previous live observation, to detect new reports
    /// arriving between heartbeat ticks.
    prev_peer_pfail: HashMap<String, HashSet<String>>,
    /// Per-peer `fail` from its previous live observation, to detect rising
    /// edges and prevent lingering recovery-window `fail` marks from re-triggering
    /// FAIL after this node clears it.
    prev_peer_fail: HashMap<String, HashSet<String>>,
    in_minority: bool,
}

impl FailureTracker {
    pub(crate) fn new(self_id: String, node_timeout: Duration, now: Instant) -> Self {
        Self {
            self_id,
            node_timeout,
            started_at: now,
            heartbeats: HashMap::new(),
            pfail: BTreeSet::new(),
            failed: BTreeMap::new(),
            reports: HashMap::new(),
            prev_peer_pfail: HashMap::new(),
            prev_peer_fail: HashMap::new(),
            in_minority: false,
        }
    }

    /// Seed heartbeat tracking for a peer assumed live at `now` (used by unit
    /// test helpers that construct a `ClusterManager` with pre-populated peers).
    #[cfg(test)]
    pub(crate) fn seed_live_peer(&mut self, node_id: &str, now: Instant) {
        if node_id == self.self_id {
            return;
        }
        self.heartbeats.insert(
            node_id.to_string(),
            HeartbeatState {
                generation_id: 0,
                heartbeat: 0,
                seen_in_gossip: true,
                last_progress: now,
            },
        );
    }

    pub(crate) fn is_pfail(&self, node_id: &str) -> bool {
        self.pfail.contains(node_id)
    }

    pub(crate) fn is_failed(&self, node_id: &str) -> bool {
        self.failed.contains_key(node_id)
    }

    pub(crate) fn is_in_minority(&self) -> bool {
        self.in_minority
    }

    pub(crate) fn pfail_list(&self) -> Vec<String> {
        self.pfail.iter().cloned().collect()
    }

    pub(crate) fn fail_list(&self) -> Vec<String> {
        self.failed.keys().cloned().collect()
    }

    /// Phase 1: update heartbeat progress and local PFAIL from chitchat views.
    /// Returns the set of peer ids whose heartbeat advanced (or were first seen)
    /// on this tick, plus any PFAIL transitions.
    pub(crate) fn observe_heartbeats(
        &mut self,
        now: Instant,
        observations: &HashMap<String, PeerHeartbeatView>,
        slot_owners: &BTreeSet<String>,
    ) -> (HashSet<String>, FailureTickOutcome) {
        let mut outcome = FailureTickOutcome::default();
        let mut progressed: HashSet<String> = HashSet::new();

        for (id, obs) in observations {
            if id == &self.self_id {
                continue;
            }
            match self.heartbeats.get_mut(id) {
                None => {
                    self.heartbeats.insert(
                        id.clone(),
                        HeartbeatState {
                            generation_id: obs.generation_id,
                            heartbeat: obs.heartbeat,
                            seen_in_gossip: true,
                            last_progress: now,
                        },
                    );
                    progressed.insert(id.clone());
                }
                Some(entry) => {
                    if !entry.seen_in_gossip
                        || (obs.generation_id, obs.heartbeat)
                            > (entry.generation_id, entry.heartbeat)
                    {
                        entry.seen_in_gossip = true;
                        entry.generation_id = obs.generation_id;
                        entry.heartbeat = obs.heartbeat;
                        entry.last_progress = now;
                        progressed.insert(id.clone());
                    }
                }
            }
        }

        // Ensure every slot owner from persisted state is tracked even if it
        // has never appeared in gossip since startup.
        for owner in slot_owners {
            if owner == &self.self_id {
                continue;
            }
            self.heartbeats
                .entry(owner.clone())
                .or_insert(HeartbeatState {
                    generation_id: 0,
                    heartbeat: 0,
                    seen_in_gossip: false,
                    last_progress: self.started_at,
                });
        }

        let mut new_pfail: BTreeSet<String> = BTreeSet::new();
        for (id, hb) in &self.heartbeats {
            if now.saturating_duration_since(hb.last_progress) >= self.node_timeout {
                new_pfail.insert(id.clone());
            }
        }
        for id in new_pfail.difference(&self.pfail) {
            outcome.newly_pfail.push(id.clone());
        }
        for id in self.pfail.difference(&new_pfail) {
            outcome.cleared_pfail.push(id.clone());
        }
        if new_pfail != self.pfail {
            outcome.gossip_changed = true;
            self.pfail = new_pfail;
        }

        (progressed, outcome)
    }

    /// Phase 2: ingest reports from live peers, clear/mark FAIL, and update
    /// minority state against the post-merge `slot_owners`.
    pub(crate) fn evaluate_failures(
        &mut self,
        now: Instant,
        observations: &HashMap<String, PeerHeartbeatView>,
        progressed: &HashSet<String>,
        slot_owners: &BTreeSet<String>,
        outcome: &mut FailureTickOutcome,
    ) {
        // Ensure any newly merged slot owners are also present in `heartbeats`.
        for owner in slot_owners {
            if owner == &self.self_id {
                continue;
            }
            self.heartbeats
                .entry(owner.clone())
                .or_insert(HeartbeatState {
                    generation_id: 0,
                    heartbeat: 0,
                    seen_in_gossip: false,
                    last_progress: self.started_at,
                });
        }

        // Expire stale reports and ingest reports from live (reachable) peers.
        let report_ttl = self.node_timeout.saturating_mul(2);
        self.reports.retain(|_, reporters| {
            reporters.retain(|_, seen_at| now.saturating_duration_since(*seen_at) <= report_ttl);
            !reporters.is_empty()
        });

        let mut newly_propagated_fail: BTreeSet<String> = BTreeSet::new();
        for (peer_id, obs) in observations {
            if peer_id == &self.self_id || self.pfail.contains(peer_id) {
                continue;
            }
            let peer_progressed = progressed.contains(peer_id);
            let prev_pfail = self.prev_peer_pfail.get(peer_id);
            let prev_fail = self.prev_peer_fail.get(peer_id);

            for target in &obs.pfail {
                if target == &self.self_id {
                    continue;
                }
                let rising = prev_pfail.is_none_or(|s| !s.contains(target));
                if peer_progressed || rising {
                    self.reports
                        .entry(target.clone())
                        .or_default()
                        .insert(peer_id.clone(), now);
                }
            }
            for target in &obs.fail {
                if target == &self.self_id {
                    continue;
                }
                let rising = prev_fail.is_none_or(|s| !s.contains(target));
                if rising {
                    self.reports
                        .entry(target.clone())
                        .or_default()
                        .insert(peer_id.clone(), now);
                    newly_propagated_fail.insert(target.clone());
                }
            }

            self.prev_peer_pfail
                .insert(peer_id.clone(), obs.pfail.iter().cloned().collect());
            self.prev_peer_fail
                .insert(peer_id.clone(), obs.fail.iter().cloned().collect());
        }

        // Clear FAIL on nodes that are reachable again.
        let clear_timeout = self.node_timeout.saturating_mul(2);
        let mut to_clear: Vec<String> = Vec::new();
        for (target, rec) in &mut self.failed {
            let reachable_again = observations.contains_key(target)
                && !self.pfail.contains(target)
                && self
                    .heartbeats
                    .get(target)
                    .is_some_and(|hb| hb.seen_in_gossip && hb.last_progress > rec.failed_at);
            if !reachable_again {
                rec.reachable_since = None;
            } else if !slot_owners.contains(target) {
                to_clear.push(target.clone());
            } else {
                let since = *rec.reachable_since.get_or_insert(now);
                if now.saturating_duration_since(since) >= clear_timeout {
                    to_clear.push(target.clone());
                }
            }
        }
        let mut just_cleared: HashSet<String> = HashSet::new();
        for target in to_clear {
            self.failed.remove(&target);
            self.reports.remove(&target);
            just_cleared.insert(target.clone());
            outcome.cleared_failed.push(target);
            outcome.gossip_changed = true;
        }

        // Mark FAIL when a majority of slot-owning primaries report the node
        // or a live peer newly gossips it in `fail`.
        let quorum = slot_owners.len() / 2 + 1;
        let mut candidates: BTreeSet<String> = BTreeSet::new();
        candidates.extend(self.pfail.iter().cloned());
        candidates.extend(self.reports.keys().cloned());
        candidates.extend(newly_propagated_fail.iter().cloned());

        for target in candidates {
            if target == self.self_id
                || self.failed.contains_key(&target)
                || just_cleared.contains(&target)
            {
                continue;
            }
            let mut should_fail = newly_propagated_fail.contains(&target);
            if !should_fail && !slot_owners.is_empty() {
                let target_reports = self.reports.get(&target);
                let count = slot_owners
                    .iter()
                    .filter(|owner| {
                        if owner.as_str() == self.self_id {
                            self.pfail.contains(&target)
                        } else {
                            target_reports
                                .and_then(|m| m.get(owner.as_str()))
                                .is_some_and(|&seen_at| {
                                    now.saturating_duration_since(seen_at) <= report_ttl
                                })
                        }
                    })
                    .count();
                if count >= quorum {
                    should_fail = true;
                }
            }
            if should_fail {
                self.failed.insert(
                    target.clone(),
                    FailRecord {
                        failed_at: now,
                        reachable_since: None,
                    },
                );
                outcome.newly_failed.push(target);
                outcome.gossip_changed = true;
            }
        }

        // Evaluate minority state: fewer than a majority of slot-owning
        // primaries reachable within `node_timeout`.
        let new_in_minority = if slot_owners.is_empty() {
            false
        } else {
            let reachable_owners = slot_owners
                .iter()
                .filter(|owner| {
                    owner.as_str() == self.self_id || !self.pfail.contains(owner.as_str())
                })
                .count();
            reachable_owners < quorum
        };
        if new_in_minority && !self.in_minority {
            outcome.entered_minority = true;
        } else if !new_in_minority && self.in_minority {
            outcome.exited_minority = true;
        }
        self.in_minority = new_in_minority;
    }

    /// Run both phases in one call (used by unit tests).
    #[cfg(test)]
    pub(crate) fn tick(
        &mut self,
        now: Instant,
        observations: &HashMap<String, PeerHeartbeatView>,
        slot_owners: &BTreeSet<String>,
    ) -> FailureTickOutcome {
        let (progressed, mut outcome) = self.observe_heartbeats(now, observations, slot_owners);
        self.evaluate_failures(now, observations, &progressed, slot_owners, &mut outcome);
        outcome
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn owners(ids: &[&str]) -> BTreeSet<String> {
        ids.iter().map(|s| s.to_string()).collect()
    }

    fn peer(generation: u64, hb: u64, pfail: &[&str], fail: &[&str]) -> PeerHeartbeatView {
        PeerHeartbeatView {
            generation_id: generation,
            heartbeat: hb,
            pfail: pfail.iter().map(|s| s.to_string()).collect(),
            fail: fail.iter().map(|s| s.to_string()).collect(),
        }
    }

    fn obs(entries: &[(&str, PeerHeartbeatView)]) -> HashMap<String, PeerHeartbeatView> {
        entries
            .iter()
            .map(|(id, view)| (id.to_string(), view.clone()))
            .collect()
    }

    #[test]
    fn pfail_triggers_at_node_timeout_and_on_eviction() {
        let t0 = Instant::now();
        let timeout = Duration::from_millis(1000);
        let mut tracker = FailureTracker::new("p1".into(), timeout, t0);
        let o = owners(&["p1", "p2", "p3"]);

        // Initial observation of p2 and p3.
        tracker.tick(
            t0,
            &obs(&[("p2", peer(1, 1, &[], &[])), ("p3", peer(1, 1, &[], &[]))]),
            &o,
        );
        assert!(!tracker.is_pfail("p2"));
        assert!(!tracker.is_pfail("p3"));

        // At 999ms with p3 stalled and p2 advancing: p3 is not PFAIL yet.
        tracker.tick(
            t0 + Duration::from_millis(999),
            &obs(&[("p2", peer(1, 2, &[], &[])), ("p3", peer(1, 1, &[], &[]))]),
            &o,
        );
        assert!(!tracker.is_pfail("p3"));

        // At 1000ms with p3 still at hb=1: p3 becomes PFAIL.
        let out = tracker.tick(
            t0 + Duration::from_millis(1000),
            &obs(&[("p2", peer(1, 3, &[], &[])), ("p3", peer(1, 1, &[], &[]))]),
            &o,
        );
        assert!(tracker.is_pfail("p3"));
        assert_eq!(out.newly_pfail, vec!["p3"]);
        assert!(out.gossip_changed);

        // Even after chitchat evicts p3 from node_states, p3 stays PFAIL.
        tracker.tick(
            t0 + Duration::from_millis(70_000),
            &obs(&[("p2", peer(1, 70, &[], &[]))]),
            &o,
        );
        assert!(tracker.is_pfail("p3"));

        // If p3 restarts with a new generation, PFAIL clears immediately.
        let out = tracker.tick(
            t0 + Duration::from_millis(70_100),
            &obs(&[("p2", peer(1, 71, &[], &[])), ("p3", peer(2, 1, &[], &[]))]),
            &o,
        );
        assert!(!tracker.is_pfail("p3"));
        assert_eq!(out.cleared_pfail, vec!["p3"]);
    }

    #[test]
    fn fail_requires_majority_of_fresh_reports_and_counts_dead_owner() {
        let t0 = Instant::now();
        let timeout = Duration::from_millis(1000);
        let mut tracker = FailureTracker::new("p1".into(), timeout, t0);
        // 4 primaries -> quorum = 4 / 2 + 1 = 3 (including the dead owner p4).
        let o = owners(&["p1", "p2", "p3", "p4"]);

        tracker.tick(
            t0,
            &obs(&[
                ("p2", peer(1, 1, &[], &[])),
                ("p3", peer(1, 1, &[], &[])),
                ("p4", peer(1, 1, &[], &[])),
            ]),
            &o,
        );

        // p4 stalls for node_timeout: p1 marks p4 PFAIL, p2 also reports p4 PFAIL.
        // Reports = {p1, p2} = 2 < quorum 3 -> NOT FAIL yet.
        tracker.tick(
            t0 + Duration::from_millis(1000),
            &obs(&[
                ("p2", peer(1, 2, &["p4"], &[])),
                ("p3", peer(1, 2, &[], &[])),
                ("p4", peer(1, 1, &[], &[])),
            ]),
            &o,
        );
        assert!(tracker.is_pfail("p4"));
        assert!(!tracker.is_failed("p4"));

        // A replica's PFAIL report must not count toward primary quorum.
        tracker.tick(
            t0 + Duration::from_millis(1100),
            &obs(&[
                ("p2", peer(1, 3, &["p4"], &[])),
                ("p3", peer(1, 3, &[], &[])),
                ("p4", peer(1, 1, &[], &[])),
                ("r1", peer(1, 3, &["p4"], &[])),
            ]),
            &o,
        );
        assert!(!tracker.is_failed("p4"));

        // Once p3 also reports p4 PFAIL: reports = {p1, p2, p3} = 3 >= 3 -> FAIL!
        let out = tracker.tick(
            t0 + Duration::from_millis(1200),
            &obs(&[
                ("p2", peer(1, 4, &["p4"], &[])),
                ("p3", peer(1, 4, &["p4"], &[])),
                ("p4", peer(1, 1, &[], &[])),
            ]),
            &o,
        );
        assert!(tracker.is_failed("p4"));
        assert_eq!(out.newly_failed, vec!["p4"]);
        assert_eq!(tracker.fail_list(), vec!["p4"]);
    }

    #[test]
    fn two_primary_cluster_cannot_mark_dead_owner_fail() {
        let t0 = Instant::now();
        let timeout = Duration::from_millis(1000);
        let mut tracker = FailureTracker::new("p1".into(), timeout, t0);
        // 2 primaries -> quorum = 2 / 2 + 1 = 2 (dead owner p2 included).
        let o = owners(&["p1", "p2"]);

        tracker.tick(t0, &obs(&[("p2", peer(1, 1, &[], &[]))]), &o);
        tracker.tick(
            t0 + Duration::from_millis(1000),
            &obs(&[("p2", peer(1, 1, &[], &[]))]),
            &o,
        );
        assert!(tracker.is_pfail("p2"));
        assert!(!tracker.is_failed("p2"));
        assert!(tracker.is_in_minority());
    }

    #[test]
    fn stale_reports_older_than_two_node_timeouts_are_ignored() {
        let t0 = Instant::now();
        let timeout = Duration::from_millis(1000);
        let mut tracker = FailureTracker::new("p1".into(), timeout, t0);
        let o = owners(&["p1", "p2", "p3"]);

        // At t0: p2 reports p3 PFAIL, while p3 is still advancing on p1.
        tracker.tick(
            t0,
            &obs(&[
                ("p2", peer(1, 1, &["p3"], &[])),
                ("p3", peer(1, 1, &[], &[])),
            ]),
            &o,
        );
        assert!(!tracker.is_failed("p3"));

        // p2 stops reporting p3 (still live). p3 keeps advancing until t0 + 1001ms.
        tracker.tick(
            t0 + Duration::from_millis(1001),
            &obs(&[("p2", peer(1, 2, &[], &[])), ("p3", peer(1, 2, &[], &[]))]),
            &o,
        );

        // At t0 + 2001ms: p3 becomes PFAIL on p1 (stalled since 1001ms),
        // and p2's report from t0 is now 2001ms > 2 * 1000ms old -> stale!
        tracker.tick(
            t0 + Duration::from_millis(2001),
            &obs(&[("p2", peer(1, 3, &[], &[])), ("p3", peer(1, 2, &[], &[]))]),
            &o,
        );
        assert!(tracker.is_pfail("p3"));
        assert!(
            !tracker.is_failed("p3"),
            "stale report from p2 should have expired"
        );
    }

    #[test]
    fn fail_learned_from_live_peer_fail_gossip() {
        let t0 = Instant::now();
        let timeout = Duration::from_millis(1000);
        // Replica r1 (not a slot owner) learns FAIL from live peer p2's `fail`.
        let mut tracker = FailureTracker::new("r1".into(), timeout, t0);
        let o = owners(&["p1", "p2", "p3"]);

        tracker.tick(
            t0,
            &obs(&[
                ("p1", peer(1, 1, &[], &[])),
                ("p2", peer(1, 1, &[], &[])),
                ("p3", peer(1, 1, &[], &[])),
            ]),
            &o,
        );

        // Even before r1's own node_timeout expires on p3, p2 gossips p3 in `fail`.
        let out = tracker.tick(
            t0 + Duration::from_millis(500),
            &obs(&[
                ("p1", peer(1, 2, &[], &[])),
                ("p2", peer(1, 2, &["p3"], &["p3"])),
                ("p3", peer(1, 1, &[], &[])),
            ]),
            &o,
        );
        assert!(tracker.is_failed("p3"));
        assert_eq!(out.newly_failed, vec!["p3"]);

        // An unreachable (PFAIL) peer's `fail` gossip is ignored.
        let mut tracker2 = FailureTracker::new("r1".into(), timeout, t0);
        tracker2.tick(
            t0,
            &obs(&[
                ("p1", peer(1, 1, &[], &[])),
                ("p2", peer(1, 1, &[], &[])),
                ("p3", peer(1, 1, &[], &[])),
            ]),
            &o,
        );
        tracker2.tick(
            t0 + Duration::from_millis(1000),
            &obs(&[
                ("p1", peer(1, 2, &[], &[])),
                ("p2", peer(1, 1, &[], &["p3"])), // p2 stalled -> PFAIL
                ("p3", peer(1, 2, &[], &[])),
            ]),
            &o,
        );
        assert!(tracker2.is_pfail("p2"));
        assert!(!tracker2.is_failed("p3"));
    }

    #[test]
    fn clear_fail_immediately_for_replica_or_slotless_primary() {
        let t0 = Instant::now();
        let timeout = Duration::from_millis(1000);
        let mut tracker = FailureTracker::new("p1".into(), timeout, t0);
        let o = owners(&["p1", "p2", "p3"]);

        // Replica r1 starts live, stalls, is marked FAIL by majority {p1, p2}.
        tracker.tick(
            t0,
            &obs(&[
                ("p2", peer(1, 1, &[], &[])),
                ("p3", peer(1, 1, &[], &[])),
                ("r1", peer(1, 1, &[], &[])),
            ]),
            &o,
        );
        tracker.tick(
            t0 + Duration::from_millis(1000),
            &obs(&[
                ("p2", peer(1, 2, &["r1"], &[])),
                ("p3", peer(1, 2, &[], &[])),
                ("r1", peer(1, 1, &[], &[])),
            ]),
            &o,
        );
        assert!(tracker.is_failed("r1"));

        // r1 becomes reachable again (new generation): clears FAIL immediately.
        let out = tracker.tick(
            t0 + Duration::from_millis(1200),
            &obs(&[
                ("p2", peer(1, 3, &[], &["r1"])), // p2 hasn't cleared its fail yet
                ("p3", peer(1, 3, &[], &[])),
                ("r1", peer(2, 1, &[], &[])),
            ]),
            &o,
        );
        assert!(!tracker.is_failed("r1"));
        assert_eq!(out.cleared_failed, vec!["r1"]);

        // Next tick with p2 still holding lingering `fail: ["r1"]` must NOT re-mark r1 FAIL.
        tracker.tick(
            t0 + Duration::from_millis(1300),
            &obs(&[
                ("p2", peer(1, 4, &[], &["r1"])),
                ("p3", peer(1, 4, &[], &[])),
                ("r1", peer(2, 2, &[], &[])),
            ]),
            &o,
        );
        assert!(!tracker.is_failed("r1"));
    }

    #[test]
    fn clear_fail_for_slot_owner_waits_two_node_timeouts_or_slot_loss() {
        let t0 = Instant::now();
        let timeout = Duration::from_millis(1000);
        let mut tracker = FailureTracker::new("p1".into(), timeout, t0);
        let o = owners(&["p1", "p2", "p3"]);

        tracker.tick(
            t0,
            &obs(&[("p2", peer(1, 1, &[], &[])), ("p3", peer(1, 1, &[], &[]))]),
            &o,
        );
        // p3 marked FAIL at t0 + 1000ms.
        tracker.tick(
            t0 + Duration::from_millis(1000),
            &obs(&[
                ("p2", peer(1, 2, &["p3"], &["p3"])),
                ("p3", peer(1, 1, &[], &[])),
            ]),
            &o,
        );
        assert!(tracker.is_failed("p3"));

        // p3 restarts at t0 + 1500ms: reachable, still owns slots -> stays FAIL.
        tracker.tick(
            t0 + Duration::from_millis(1500),
            &obs(&[
                ("p2", peer(1, 3, &[], &["p3"])),
                ("p3", peer(2, 1, &[], &[])),
            ]),
            &o,
        );
        assert!(!tracker.is_pfail("p3"));
        assert!(tracker.is_failed("p3"));

        // At t0 + 3499ms (1999ms reachable < 2 * 1000ms): still FAIL.
        tracker.tick(
            t0 + Duration::from_millis(2500),
            &obs(&[
                ("p2", peer(1, 4, &[], &["p3"])),
                ("p3", peer(2, 2, &[], &[])),
            ]),
            &o,
        );
        tracker.tick(
            t0 + Duration::from_millis(3499),
            &obs(&[
                ("p2", peer(1, 5, &[], &["p3"])),
                ("p3", peer(2, 3, &[], &[])),
            ]),
            &o,
        );
        assert!(tracker.is_failed("p3"));

        // At t0 + 3500ms (2000ms reachable == 2 * 1000ms): FAIL clears!
        let out = tracker.tick(
            t0 + Duration::from_millis(3500),
            &obs(&[
                ("p2", peer(1, 6, &[], &["p3"])),
                ("p3", peer(2, 4, &[], &[])),
            ]),
            &o,
        );
        assert!(!tracker.is_failed("p3"));
        assert_eq!(out.cleared_failed, vec!["p3"]);

        // Now test immediate clear when failover takes the owner's slots while reachable.
        let mut tracker2 = FailureTracker::new("p1".into(), timeout, t0);
        tracker2.tick(
            t0,
            &obs(&[("p2", peer(1, 1, &[], &[])), ("p3", peer(1, 1, &[], &[]))]),
            &o,
        );
        tracker2.tick(
            t0 + Duration::from_millis(1000),
            &obs(&[
                ("p2", peer(1, 2, &["p3"], &["p3"])),
                ("p3", peer(1, 1, &[], &[])),
            ]),
            &o,
        );
        assert!(tracker2.is_failed("p3"));
        // p3 recovers, and r3 took p3's slots in the slot table.
        let o_after_failover = owners(&["p1", "p2", "r3"]);
        tracker2.tick(
            t0 + Duration::from_millis(1500),
            &obs(&[
                ("p2", peer(1, 3, &[], &["p3"])),
                ("p3", peer(2, 1, &[], &[])),
                ("r3", peer(1, 3, &[], &[])),
            ]),
            &o_after_failover,
        );
        assert!(
            !tracker2.is_failed("p3"),
            "FAIL should clear immediately once p3 is reachable and owns no slots"
        );

        // If a slot-owning primary flaps back to unreachable before 2 * node_timeout,
        // the clear timer resets and requires a full 2 * node_timeout again.
        let mut tracker3 = FailureTracker::new("p1".into(), timeout, t0);
        tracker3.tick(
            t0,
            &obs(&[("p2", peer(1, 1, &[], &[])), ("p3", peer(1, 1, &[], &[]))]),
            &o,
        );
        tracker3.tick(
            t0 + Duration::from_millis(1000),
            &obs(&[
                ("p2", peer(1, 2, &["p3"], &["p3"])),
                ("p3", peer(1, 1, &[], &[])),
            ]),
            &o,
        );
        assert!(tracker3.is_failed("p3"));
        // Reachable at 1500ms, then stalls again so it becomes PFAIL at 2500ms.
        tracker3.tick(
            t0 + Duration::from_millis(1500),
            &obs(&[
                ("p2", peer(1, 3, &[], &["p3"])),
                ("p3", peer(2, 1, &[], &[])),
            ]),
            &o,
        );
        tracker3.tick(
            t0 + Duration::from_millis(2500),
            &obs(&[
                ("p2", peer(1, 4, &["p3"], &["p3"])),
                ("p3", peer(2, 1, &[], &[])),
            ]),
            &o,
        );
        assert!(tracker3.is_pfail("p3"));
        assert!(tracker3.is_failed("p3"));
        // Reachable again at 3000ms: must still be FAIL at 4999ms and clear at 5000ms.
        tracker3.tick(
            t0 + Duration::from_millis(3000),
            &obs(&[
                ("p2", peer(1, 5, &[], &["p3"])),
                ("p3", peer(2, 2, &[], &[])),
            ]),
            &o,
        );
        tracker3.tick(
            t0 + Duration::from_millis(4000),
            &obs(&[
                ("p2", peer(1, 6, &[], &["p3"])),
                ("p3", peer(2, 3, &[], &[])),
            ]),
            &o,
        );
        tracker3.tick(
            t0 + Duration::from_millis(4999),
            &obs(&[
                ("p2", peer(1, 7, &[], &["p3"])),
                ("p3", peer(2, 4, &[], &[])),
            ]),
            &o,
        );
        assert!(tracker3.is_failed("p3"));
        tracker3.tick(
            t0 + Duration::from_millis(5000),
            &obs(&[
                ("p2", peer(1, 8, &[], &["p3"])),
                ("p3", peer(2, 5, &[], &[])),
            ]),
            &o,
        );
        assert!(!tracker3.is_failed("p3"));
    }

    #[test]
    fn clear_fail_five_primaries_does_not_livelock_on_staggered_recovery() {
        let t0 = Instant::now();
        let timeout = Duration::from_millis(1000);
        let mut tracker = FailureTracker::new("p1".into(), timeout, t0);
        let o = owners(&["p1", "p2", "p3", "p4", "p5"]);

        tracker.tick(
            t0,
            &obs(&[
                ("p2", peer(1, 1, &[], &[])),
                ("p3", peer(1, 1, &[], &[])),
                ("p4", peer(1, 1, &[], &[])),
                ("p5", peer(1, 1, &[], &[])),
            ]),
            &o,
        );
        // p5 fails at t0 + 1000ms.
        tracker.tick(
            t0 + Duration::from_millis(1000),
            &obs(&[
                ("p2", peer(1, 2, &["p5"], &["p5"])),
                ("p3", peer(1, 2, &["p5"], &["p5"])),
                ("p4", peer(1, 2, &["p5"], &["p5"])),
                ("p5", peer(1, 1, &[], &[])),
            ]),
            &o,
        );
        assert!(tracker.is_failed("p5"));

        // p5 comes back at t0 + 1500ms; p2/p3/p4 see it 100ms later so they
        // drop p5 from `pfail` at t0 + 1600ms but keep p5 in `fail` until t0 + 3600ms.
        tracker.tick(
            t0 + Duration::from_millis(1500),
            &obs(&[
                ("p2", peer(1, 3, &["p5"], &["p5"])),
                ("p3", peer(1, 3, &["p5"], &["p5"])),
                ("p4", peer(1, 3, &["p5"], &["p5"])),
                ("p5", peer(2, 1, &[], &[])),
            ]),
            &o,
        );
        tracker.tick(
            t0 + Duration::from_millis(2500),
            &obs(&[
                ("p2", peer(1, 4, &[], &["p5"])),
                ("p3", peer(1, 4, &[], &["p5"])),
                ("p4", peer(1, 4, &[], &["p5"])),
                ("p5", peer(2, 2, &[], &[])),
            ]),
            &o,
        );

        // At t0 + 3500ms: p1 clears FAIL even though p2/p3/p4 still have p5 in `fail`.
        tracker.tick(
            t0 + Duration::from_millis(3500),
            &obs(&[
                ("p2", peer(1, 5, &[], &["p5"])),
                ("p3", peer(1, 5, &[], &["p5"])),
                ("p4", peer(1, 5, &[], &["p5"])),
                ("p5", peer(2, 3, &[], &[])),
            ]),
            &o,
        );
        assert!(!tracker.is_failed("p5"));

        // At t0 + 3550ms: p2/p3/p4 STILL have p5 in `fail` (not `pfail`) -> p1 stays clear!
        tracker.tick(
            t0 + Duration::from_millis(3550),
            &obs(&[
                ("p2", peer(1, 6, &[], &["p5"])),
                ("p3", peer(1, 6, &[], &["p5"])),
                ("p4", peer(1, 6, &[], &["p5"])),
                ("p5", peer(2, 4, &[], &[])),
            ]),
            &o,
        );
        assert!(!tracker.is_failed("p5"));
    }

    #[test]
    fn minority_detection_with_1_3_and_5_primaries() {
        let t0 = Instant::now();
        let timeout = Duration::from_millis(1000);

        // 1 primary (quorum 1): never in a minority.
        let mut t1 = FailureTracker::new("p1".into(), timeout, t0);
        let o1 = owners(&["p1"]);
        t1.tick(t0, &obs(&[]), &o1);
        t1.tick(t0 + Duration::from_millis(10_000), &obs(&[]), &o1);
        assert!(!t1.is_in_minority());

        // 3 primaries (quorum 2): 1 down is fine; 2 down enters minority after node_timeout.
        let mut t3 = FailureTracker::new("p1".into(), timeout, t0);
        let o3 = owners(&["p1", "p2", "p3"]);
        t3.tick(
            t0,
            &obs(&[("p2", peer(1, 1, &[], &[])), ("p3", peer(1, 1, &[], &[]))]),
            &o3,
        );
        // Only p3 stalls.
        t3.tick(
            t0 + Duration::from_millis(1000),
            &obs(&[("p2", peer(1, 2, &[], &[])), ("p3", peer(1, 1, &[], &[]))]),
            &o3,
        );
        assert!(!t3.is_in_minority());
        // Now p2 stalls too; at 1999ms still not in minority, at 2000ms enters minority.
        t3.tick(
            t0 + Duration::from_millis(1999),
            &obs(&[("p2", peer(1, 2, &[], &[])), ("p3", peer(1, 1, &[], &[]))]),
            &o3,
        );
        assert!(!t3.is_in_minority());
        let out = t3.tick(
            t0 + Duration::from_millis(2000),
            &obs(&[("p2", peer(1, 2, &[], &[])), ("p3", peer(1, 1, &[], &[]))]),
            &o3,
        );
        assert!(t3.is_in_minority());
        assert!(out.entered_minority);
        // p2 comes back -> exits minority immediately.
        let out = t3.tick(
            t0 + Duration::from_millis(2100),
            &obs(&[("p2", peer(1, 3, &[], &[])), ("p3", peer(1, 1, &[], &[]))]),
            &o3,
        );
        assert!(!t3.is_in_minority());
        assert!(out.exited_minority);

        // 5 primaries (quorum 3): 2 down is fine; 3 down enters minority.
        let mut t5 = FailureTracker::new("p1".into(), timeout, t0);
        let o5 = owners(&["p1", "p2", "p3", "p4", "p5"]);
        t5.tick(
            t0,
            &obs(&[
                ("p2", peer(1, 1, &[], &[])),
                ("p3", peer(1, 1, &[], &[])),
                ("p4", peer(1, 1, &[], &[])),
                ("p5", peer(1, 1, &[], &[])),
            ]),
            &o5,
        );
        // p4 and p5 stall -> 3 reachable >= 3.
        t5.tick(
            t0 + Duration::from_millis(1000),
            &obs(&[
                ("p2", peer(1, 2, &[], &[])),
                ("p3", peer(1, 2, &[], &[])),
                ("p4", peer(1, 1, &[], &[])),
                ("p5", peer(1, 1, &[], &[])),
            ]),
            &o5,
        );
        assert!(!t5.is_in_minority());
        // p3 stalls too -> at t0 + 2000ms only 2 reachable < 3 -> minority.
        t5.tick(
            t0 + Duration::from_millis(2000),
            &obs(&[
                ("p2", peer(1, 3, &[], &[])),
                ("p3", peer(1, 2, &[], &[])),
                ("p4", peer(1, 1, &[], &[])),
                ("p5", peer(1, 1, &[], &[])),
            ]),
            &o5,
        );
        assert!(t5.is_in_minority());

        // Replica in a 3-primary cluster (not in owners): needs 2 reachable owners.
        let mut tr = FailureTracker::new("r1".into(), timeout, t0);
        tr.tick(
            t0,
            &obs(&[
                ("p1", peer(1, 1, &[], &[])),
                ("p2", peer(1, 1, &[], &[])),
                ("p3", peer(1, 1, &[], &[])),
            ]),
            &o3,
        );
        tr.tick(
            t0 + Duration::from_millis(1000),
            &obs(&[
                ("p1", peer(1, 2, &[], &[])),
                ("p2", peer(1, 1, &[], &[])),
                ("p3", peer(1, 1, &[], &[])),
            ]),
            &o3,
        );
        assert!(
            tr.is_in_minority(),
            "replica reaching only 1 of 3 primaries must be in minority"
        );
    }
}
