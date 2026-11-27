#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SPECS_DIR="$ROOT_DIR/specs"
TLA_VERSION="v1.7.4"
TLA_SHA256="936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88"
TLA_JAR="${TLA2TOOLS_JAR:-$HOME/.cache/tla2tools/tla2tools.jar}"

if [ ! -f "$TLA_JAR" ]; then
  mkdir -p "$(dirname "$TLA_JAR")"
  echo "Downloading tla2tools.jar ($TLA_VERSION) to $TLA_JAR..."
  curl -fsSL -o "$TLA_JAR" \
    "https://github.com/tlaplus/tlaplus/releases/download/$TLA_VERSION/tla2tools.jar"
  echo "$TLA_SHA256  $TLA_JAR" | sha256sum -c -
fi

run_tlc() {
  local spec="$1"
  local cfg="$2"
  java -XX:+UseParallelGC -cp "$TLA_JAR" tlc2.TLC \
    -workers auto -cleanup \
    -config "$SPECS_DIR/$cfg" \
    "$SPECS_DIR/$spec"
}

expect_pass() {
  local spec="$1"
  local cfg="$2"
  echo "=== [PASS EXPECTED] $spec ($cfg) ==="
  run_tlc "$spec" "$cfg"
}

expect_violation() {
  local spec="$1"
  local cfg="$2"
  local pattern="$3"
  echo "=== [VIOLATION EXPECTED: $pattern] $spec ($cfg) ==="
  local out
  set +e
  out="$(run_tlc "$spec" "$cfg" 2>&1)"
  local rc=$?
  set -e
  if [ "$rc" -eq 0 ]; then
    echo "ERROR: expected $cfg to fail matching '$pattern', but TLC passed!" >&2
    echo "$out" >&2
    exit 1
  fi
  if ! grep -q "$pattern" <<< "$out"; then
    echo "ERROR: $cfg failed (exit $rc), but output did not match '$pattern':" >&2
    echo "$out" >&2
    exit 1
  fi
  echo "OK: reproduced '$pattern' (exit $rc)"
}

SUITE="${1:-all}"

run_async_write() {
  expect_pass "AsyncWrite.tla" "AsyncWrite.cfg"
}

run_async_barrier() {
  expect_pass "AsyncWrite.tla" "AsyncWrite_BarrierOnly.cfg"
}

run_cluster_and_negatives() {
  # AsyncWrite negative checks
  expect_violation "AsyncWrite.tla" "AsyncWrite_Bug65.cfg" "Invariant Linearizable is violated"
  expect_violation "AsyncWrite.tla" "AsyncWrite_Bug64.cfg" "Invariant Linearizable is violated"
  expect_violation "AsyncWrite.tla" "AsyncWrite_BugKeyCountRollback.cfg" "Invariant KeyCountAccurate is violated"
  expect_violation "AsyncWrite.tla" "AsyncWrite_BugCleanupInflight.cfg" "Invariant Linearizable is violated"

  # Cluster routing & Chitchat gossip (slot handoff, crashes, partitions, convergence)
  expect_pass      "ClusterGossip.tla" "ClusterGossip.cfg"
  expect_violation "ClusterGossip.tla" "ClusterGossip_Bug35.cfg" "Invariant NoOrphanedWrites is violated"
  expect_violation "ClusterGossip.tla" "ClusterGossip_BugPublishRace.cfg" "Temporal properties were violated"
}

run_replication() {
  expect_pass "Replication.tla" "Replication.cfg"
  local bug
  for bug in UpsertKeepsLsn NoAutoincrement TrimIgnoresPins SkipTrimmedThroughCheck \
             NoReplidRotation CommitAfterCleanMarker VolatileStartupMarker ReplicaCursorNotAtomic; do
    expect_violation "Replication.tla" "Replication_Bug$bug.cfg" "Invariant CaughtUpMeansEqual is violated"
  done
}

run_failover() {
  expect_pass "Failover.tla" "Failover.cfg"
  expect_pass "Failover.tla" "Failover_Liveness.cfg"
  expect_violation "Failover.tla" "Failover_BugNoVoteBackoff.cfg" "Invariant WinnerReplacesCurrentOwner is violated"
  expect_violation "Failover.tla" "Failover_BugNoSlotEpochCheck.cfg" "Invariant WinnerReplacesCurrentOwner is violated"
  expect_violation "Failover.tla" "Failover_BugTableIgnoresEpoch.cfg" "Action property TableEpochsNeverDecrease is violated"
}

# Not in CI: about 40 minutes on 16 cores.
run_failover_large() {
  expect_pass "Failover.tla" "Failover_Large.cfg"
  expect_violation "Failover.tla" "Failover_BugVolatileVotes.cfg" "Invariant OneWinnerPerEpoch is violated"
}

case "$SUITE" in
  all)
    run_async_write
    run_async_barrier
    run_cluster_and_negatives
    run_replication
    run_failover
    ;;
  async-write)
    run_async_write
    ;;
  async-barrier)
    run_async_barrier
    ;;
  cluster-and-negatives)
    run_cluster_and_negatives
    ;;
  replication)
    run_replication
    ;;
  failover)
    run_failover
    ;;
  failover-large)
    run_failover_large
    ;;
  replication-large)
    expect_pass "Replication.tla" "Replication_Large.cfg"
    ;;
  *)
    echo "Usage: $0 [all|async-write|async-barrier|cluster-and-negatives|replication|replication-large|failover|failover-large]" >&2
    exit 2
    ;;
esac

echo "TLA+ checks ($SUITE) succeeded."
