//! Replica elections, voting (`BLOBASAUR.FAILOVER-AUTH`), ranking, and vote collection.
//!
//! Follows `FAILOVER.md` and `specs/Failover.tla`.

use std::collections::{BTreeSet, HashSet};
use std::hash::{BuildHasher, RandomState};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

use bytes::{Bytes, BytesMut};
use redis_protocol::resp2::decode::decode_bytes_mut;
use redis_protocol::resp2::types::BytesFrame;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::task::JoinSet;
use tokio::time::{Instant, sleep_until, timeout_at};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::cluster::{ClusterManager, REDIS_CLUSTER_SLOTS};
use crate::redis::serialize_frame;

/// Per-shard replication identity and progress gossiped for election ranking.
///
/// On replicas, carries `(replid, caught_up_lsn)` for each shard. On primaries,
/// carries `replid` (with `caught_up_lsn = 0`) so replicas can verify they share
/// their primary's replication history before standing or comparing LSNs.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardReplState {
    #[serde(default)]
    pub replid: String,
    #[serde(default)]
    pub caught_up_lsn: u64,
}

/// Wire payload of `BLOBASAUR.FAILOVER-AUTH`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailoverAuthRequest {
    pub candidate: String,
    pub epoch: u64,
    pub failed_primary: String,
    pub primary_config_epoch: u64,
    pub slots: HashSet<u16>,
    pub manual: bool,
}

/// How a replica promotion determines its target `config_epoch` and preconditions.
#[derive(Debug, Clone)]
pub enum PromotionKind {
    /// `CLUSTER FAILOVER TAKEOVER`: bumps `current_epoch` and promotes at it.
    Takeover,
    /// Won an election: re-checks that this replica still follows `failed_primary`,
    /// `expected_slots` are still bound to `failed_primary` at `primary_config_epoch`,
    /// and `deadline` (measured from before the first vote request was sent) has
    /// not passed, then promotes with `config_epoch = election_epoch`.
    Elected {
        election_epoch: u64,
        failed_primary: String,
        primary_config_epoch: u64,
        expected_slots: HashSet<u16>,
        deadline: std::time::Instant,
    },
}

/// Prepared election attempt, created only after `current_epoch` is persisted.
#[derive(Debug, Clone)]
pub struct ElectionPlan {
    pub candidate: String,
    pub epoch: u64,
    pub failed_primary: String,
    pub primary_config_epoch: u64,
    pub slots: HashSet<u16>,
    /// All slot-owning primaries in the candidate's table (including `failed_primary`).
    pub owners: BTreeSet<String>,
    /// Distinct slot-owning primaries with a known Redis address to contact.
    pub targets: Vec<(String, SocketAddr)>,
    /// Majority of `owners` (`owners.len() / 2 + 1`).
    pub quorum: usize,
    pub manual: bool,
}

impl ElectionPlan {
    pub fn to_request(&self) -> FailoverAuthRequest {
        FailoverAuthRequest {
            candidate: self.candidate.clone(),
            epoch: self.epoch,
            failed_primary: self.failed_primary.clone(),
            primary_config_epoch: self.primary_config_epoch,
            slots: self.slots.clone(),
            manual: self.manual,
        }
    }
}

/// Lightweight counters exposed in `CLUSTER INFO`.
#[derive(Debug, Default)]
pub struct ElectionStats {
    pub elections_started: AtomicU64,
    pub elections_won: AtomicU64,
    pub elections_failed: AtomicU64,
    pub votes_granted: AtomicU64,
    pub votes_refused: AtomicU64,
}

/// Outcome of collecting votes for one election attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VoteCollectionOutcome {
    Won {
        votes: BTreeSet<String>,
    },
    NoMajority {
        votes: BTreeSet<String>,
        quorum: usize,
    },
    Aborted,
}

/// How long an election waits for a majority of votes: `max(2 * node_timeout, 2 s)`.
pub fn election_timeout(node_timeout: Duration) -> Duration {
    node_timeout.saturating_mul(2).max(Duration::from_secs(2))
}

/// Minimum time between the start of a failed election attempt and the next:
/// `max(4 * node_timeout, 4 s)`.
pub fn retry_backoff(node_timeout: Duration) -> Duration {
    node_timeout.saturating_mul(4).max(Duration::from_secs(4))
}

/// Per-failed-primary vote backoff on voters: `2 * election_timeout(node_timeout)`
/// (at least 4 s).
///
/// Matches the timing assumption in `FAILOVER.md` and `specs/Failover.tla`
/// (`ForgetVote` / `Failover_BugNoVoteBackoff`): the backoff must strictly cover
/// the full election window (`election_timeout`) plus gossip propagation of the
/// winner's claim, even when `node_timeout_ms` is small (e.g. `< 1000 ms`).
pub fn vote_backoff(node_timeout: Duration) -> Duration {
    election_timeout(node_timeout).saturating_mul(2)
}

/// Election start delay: `500 ms + random(0..500 ms) + rank * 1000 ms`.
pub fn election_delay(rank: usize, jitter_ms: u64) -> Duration {
    let base_ms = 500u64
        .saturating_add(jitter_ms % 500)
        .saturating_add((rank as u64).saturating_mul(1000));
    Duration::from_millis(base_ms)
}

/// Uniform-ish jitter in `0..500` ms using std's OS-seeded `RandomState`.
pub fn random_jitter_ms() -> u64 {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let h = RandomState::new().hash_one((SystemTime::now(), std::process::id(), n));
    h % 500
}

/// Sum of per-shard `caught_up_lsn` values.
pub fn total_caught_up_lsn(shards: &[ShardReplState]) -> u128 {
    shards.iter().map(|s| u128::from(s.caught_up_lsn)).sum()
}

/// True when `a` and `b` have the same non-empty length and identical non-empty
/// replids on every shard.
pub fn replids_match(a: &[ShardReplState], b: &[ShardReplState]) -> bool {
    !a.is_empty()
        && a.len() == b.len()
        && a.iter()
            .zip(b.iter())
            .all(|(x, y)| !x.replid.is_empty() && x.replid == y.replid)
}

/// Rank of `self_id` among replicas sharing the primary's replids.
/// Rank 0 is the highest sum of `caught_up_lsn`; ties break by smaller `node_id`.
pub fn compute_rank(self_id: &str, self_lsn: u128, peers: &[(String, u128)]) -> usize {
    peers
        .iter()
        .filter(|(peer_id, peer_lsn)| {
            peer_id != self_id
                && (*peer_lsn > self_lsn || (*peer_lsn == self_lsn && peer_id.as_str() < self_id))
        })
        .count()
}

/// Format slots as comma-separated inclusive ranges (e.g. `"0-5460"` or `"1,3-5"`).
pub fn format_slot_ranges(slots: &HashSet<u16>) -> String {
    ClusterManager::slot_runs(slots)
        .into_iter()
        .map(|(start, end)| {
            if start == end {
                start.to_string()
            } else {
                format!("{start}-{end}")
            }
        })
        .collect::<Vec<_>>()
        .join(",")
}

/// Parse comma- or whitespace-separated slot ranges (`"0-5460"` or `"1,3-5"`).
/// Rejects empty sets, inverted ranges, and slots >= `REDIS_CLUSTER_SLOTS`.
pub fn parse_slot_ranges(text: &str) -> Result<HashSet<u16>, String> {
    let mut slots = HashSet::new();
    for token in text.split(|c: char| c == ',' || c.is_ascii_whitespace()) {
        if token.is_empty() {
            continue;
        }
        if let Some((start_s, end_s)) = token.split_once('-') {
            let start: u16 = start_s
                .parse()
                .map_err(|_| format!("invalid slot range '{token}'"))?;
            let end: u16 = end_s
                .parse()
                .map_err(|_| format!("invalid slot range '{token}'"))?;
            if start > end || end >= REDIS_CLUSTER_SLOTS {
                return Err(format!("invalid slot range '{token}'"));
            }
            for slot in start..=end {
                slots.insert(slot);
            }
        } else {
            let slot: u16 = token
                .parse()
                .map_err(|_| format!("invalid slot '{token}'"))?;
            if slot >= REDIS_CLUSTER_SLOTS {
                return Err(format!("slot {slot} out of range"));
            }
            slots.insert(slot);
        }
    }
    if slots.is_empty() {
        return Err("slot-ranges cannot be empty".to_string());
    }
    Ok(slots)
}

/// Encode `BLOBASAUR.FAILOVER-AUTH` as a RESP2 array frame.
pub fn encode_failover_auth(req: &FailoverAuthRequest) -> Bytes {
    let bulk = |s: String| BytesFrame::BulkString(Bytes::from(s));
    let mut parts = vec![
        bulk("BLOBASAUR.FAILOVER-AUTH".to_string()),
        bulk(req.candidate.clone()),
        bulk(req.epoch.to_string()),
        bulk(req.failed_primary.clone()),
        bulk(req.primary_config_epoch.to_string()),
        bulk(format_slot_ranges(&req.slots)),
    ];
    if req.manual {
        parts.push(bulk("MANUAL".to_string()));
    }
    serialize_frame(&BytesFrame::Array(parts))
}

/// Send one `BLOBASAUR.FAILOVER-AUTH` RPC over TCP before `deadline`.
/// Returns `Ok(voted_epoch)` on `+VOTE <epoch>`, or `Err(reason)` otherwise.
pub async fn send_failover_auth(
    addr: SocketAddr,
    encoded: &[u8],
    deadline: Instant,
) -> Result<u64, String> {
    let mut stream = timeout_at(deadline, TcpStream::connect(addr))
        .await
        .map_err(|_| format!("connect to {addr} timed out"))?
        .map_err(|e| format!("connect to {addr}: {e}"))?;

    timeout_at(deadline, stream.write_all(encoded))
        .await
        .map_err(|_| format!("write to {addr} timed out"))?
        .map_err(|e| format!("write to {addr}: {e}"))?;

    let mut buf = BytesMut::new();
    let frame = timeout_at(deadline, async {
        loop {
            if let Some((frame, _, _)) =
                decode_bytes_mut(&mut buf).map_err(|e| format!("bad reply from {addr}: {e:?}"))?
            {
                return Ok(frame);
            }
            if stream
                .read_buf(&mut buf)
                .await
                .map_err(|e| format!("read from {addr}: {e}"))?
                == 0
            {
                return Err(format!("{addr} closed connection before replying"));
            }
        }
    })
    .await
    .map_err(|_| format!("waiting for vote from {addr} timed out"))??;

    match frame {
        BytesFrame::SimpleString(s) => {
            let text = String::from_utf8_lossy(&s);
            let mut parts = text.split_whitespace();
            match (parts.next(), parts.next(), parts.next()) {
                (Some("VOTE"), Some(ep), None) => ep
                    .parse::<u64>()
                    .map_err(|_| format!("invalid VOTE epoch '{ep}' from {addr}")),
                _ => Err(format!("unexpected reply '{text}' from {addr}")),
            }
        }
        BytesFrame::Error(e) => Err(e.to_string()),
        other => Err(format!("unexpected reply {other:?} from {addr}")),
    }
}

/// Concurrently send `BLOBASAUR.FAILOVER-AUTH` to every target in `plan` and
/// tally distinct votes matching `plan.epoch` until majority, `deadline`, abort,
/// or shutdown.
pub async fn collect_votes<F>(
    plan: &ElectionPlan,
    deadline: std::time::Instant,
    shutdown: &CancellationToken,
    mut should_abort: F,
) -> VoteCollectionOutcome
where
    F: FnMut() -> bool,
{
    let req = plan.to_request();
    let encoded = encode_failover_auth(&req);
    let deadline = Instant::from_std(deadline);

    let mut set: JoinSet<(String, Result<u64, String>)> = JoinSet::new();
    for (owner_id, addr) in &plan.targets {
        let owner_id = owner_id.clone();
        let addr = *addr;
        let encoded = encoded.clone();
        set.spawn(async move {
            let res = send_failover_auth(addr, &encoded, deadline).await;
            (owner_id, res)
        });
    }

    let mut votes: BTreeSet<String> = BTreeSet::new();
    while !set.is_empty() {
        if should_abort() {
            set.abort_all();
            return VoteCollectionOutcome::Aborted;
        }
        tokio::select! {
            _ = shutdown.cancelled() => {
                set.abort_all();
                return VoteCollectionOutcome::Aborted;
            }
            _ = sleep_until(deadline) => {
                set.abort_all();
                break;
            }
            res = set.join_next() => {
                let Some(joined) = res else {
                    break;
                };
                match joined {
                    Ok((voter, Ok(voted_epoch))) => {
                        if voted_epoch != plan.epoch {
                            warn!(
                                candidate = %plan.candidate,
                                expected_epoch = plan.epoch,
                                voted_epoch,
                                voter = %voter,
                                "Ignoring vote for mismatched epoch"
                            );
                            continue;
                        }
                        if !plan.owners.contains(&voter) {
                            warn!(
                                candidate = %plan.candidate,
                                voter = %voter,
                                "Ignoring vote from node not in slot owners table"
                            );
                            continue;
                        }
                        if votes.insert(voter.clone()) {
                            info!(
                                candidate = %plan.candidate,
                                epoch = plan.epoch,
                                voter = %voter,
                                votes = votes.len(),
                                quorum = plan.quorum,
                                "Received failover vote"
                            );
                        }
                        if votes.len() >= plan.quorum {
                            set.abort_all();
                            return VoteCollectionOutcome::Won { votes };
                        }
                    }
                    Ok((voter, Err(reason))) => {
                        debug!(
                            candidate = %plan.candidate,
                            epoch = plan.epoch,
                            voter = %voter,
                            reason = %reason,
                            "Failover vote request did not succeed"
                        );
                    }
                    Err(e) => {
                        debug!(
                            candidate = %plan.candidate,
                            epoch = plan.epoch,
                            error = %e,
                            "Failover vote task failed"
                        );
                    }
                }
            }
        }
    }

    if votes.len() >= plan.quorum {
        VoteCollectionOutcome::Won { votes }
    } else {
        VoteCollectionOutcome::NoMajority {
            votes,
            quorum: plan.quorum,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slot_ranges_round_trip_and_validation() {
        let mut slots = HashSet::new();
        for s in 0..=10 {
            slots.insert(s);
        }
        slots.insert(42);
        for s in 100..=105 {
            slots.insert(s);
        }
        let encoded = format_slot_ranges(&slots);
        assert_eq!(encoded, "0-10,42,100-105");
        assert_eq!(parse_slot_ranges(&encoded).unwrap(), slots);
        // Also accepts space-separated ranges.
        assert_eq!(parse_slot_ranges("0-10 42 100-105").unwrap(), slots);

        assert!(parse_slot_ranges("").is_err());
        assert!(parse_slot_ranges("   ").is_err());
        assert!(parse_slot_ranges("10-5").is_err());
        assert!(parse_slot_ranges("16384").is_err());
        assert!(parse_slot_ranges("0-16384").is_err());
        assert!(parse_slot_ranges("abc").is_err());
    }

    #[test]
    fn rank_orders_by_total_lsn_desc_then_node_id_asc() {
        let peers = vec![
            ("r-a".to_string(), 100),
            ("r-b".to_string(), 200),
            ("r-c".to_string(), 100),
            ("r-d".to_string(), 50),
        ];
        // r-b has highest LSN -> rank 0.
        assert_eq!(compute_rank("r-b", 200, &peers), 0);
        // r-a ties with r-c at 100; "r-a" < "r-c" -> r-a is rank 1, r-c is rank 2.
        assert_eq!(compute_rank("r-a", 100, &peers), 1);
        assert_eq!(compute_rank("r-c", 100, &peers), 2);
        // r-d has lowest LSN -> rank 3.
        assert_eq!(compute_rank("r-d", 50, &peers), 3);
    }

    #[test]
    fn timing_formulas_match_spec() {
        assert_eq!(
            election_timeout(Duration::from_millis(500)),
            Duration::from_secs(2)
        );
        assert_eq!(
            election_timeout(Duration::from_millis(1000)),
            Duration::from_secs(2)
        );
        assert_eq!(
            election_timeout(Duration::from_millis(15_000)),
            Duration::from_secs(30)
        );

        assert_eq!(
            retry_backoff(Duration::from_millis(500)),
            Duration::from_secs(4)
        );
        assert_eq!(
            retry_backoff(Duration::from_millis(1000)),
            Duration::from_secs(4)
        );
        assert_eq!(
            retry_backoff(Duration::from_millis(15_000)),
            Duration::from_secs(60)
        );

        for ms in [10, 100, 500, 999, 1000, 5000, 15_000, 60_000] {
            let nt = Duration::from_millis(ms);
            let et = election_timeout(nt);
            let vb = vote_backoff(nt);
            assert_eq!(vb, et.saturating_mul(2), "node_timeout_ms={ms}");
            assert!(
                vb >= et && vb >= Duration::from_secs(4),
                "node_timeout_ms={ms}: vote_backoff={vb:?}, election_timeout={et:?}"
            );
        }
        assert_eq!(
            vote_backoff(Duration::from_millis(200)),
            Duration::from_secs(4)
        );
        assert_eq!(
            vote_backoff(Duration::from_millis(1000)),
            Duration::from_secs(4)
        );
        assert_eq!(
            vote_backoff(Duration::from_millis(15_000)),
            Duration::from_secs(60)
        );

        assert_eq!(election_delay(0, 0), Duration::from_millis(500));
        assert_eq!(election_delay(0, 499), Duration::from_millis(999));
        assert_eq!(election_delay(1, 250), Duration::from_millis(1750));
        assert_eq!(election_delay(2, 100), Duration::from_millis(2600));
    }

    #[test]
    fn replids_match_requires_equal_non_empty_per_shard_ids() {
        let a = vec![
            ShardReplState {
                replid: "abc".into(),
                caught_up_lsn: 10,
            },
            ShardReplState {
                replid: "def".into(),
                caught_up_lsn: 20,
            },
        ];
        let b = vec![
            ShardReplState {
                replid: "abc".into(),
                caught_up_lsn: 5,
            },
            ShardReplState {
                replid: "def".into(),
                caught_up_lsn: 30,
            },
        ];
        let mismatched = vec![
            ShardReplState {
                replid: "abc".into(),
                caught_up_lsn: 5,
            },
            ShardReplState {
                replid: "xyz".into(),
                caught_up_lsn: 30,
            },
        ];
        let empty_id = vec![
            ShardReplState {
                replid: "".into(),
                caught_up_lsn: 0,
            },
            ShardReplState {
                replid: "def".into(),
                caught_up_lsn: 0,
            },
        ];
        assert!(replids_match(&a, &b));
        assert!(!replids_match(&a, &mismatched));
        assert!(!replids_match(&a, &empty_id));
        assert!(!replids_match(&[], &[]));
        assert_eq!(total_caught_up_lsn(&a), 30);
    }
}
