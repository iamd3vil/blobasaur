//! Persisted cluster state (`<data_dir>/cluster-state.json`) and epoch-versioned slot table.
//!
//! Follow `FAILOVER.md` and `specs/Failover.tla`: a slot is rebound only to a
//! strictly higher `config_epoch` claim, and bindings for down nodes are kept so
//! a returning primary at an older epoch cannot reclaim its slots.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::cluster::{ClusterManager, REDIS_CLUSTER_SLOTS};
use crate::config::ClusterConfig;

/// File inside `<data_dir>` holding persisted cluster state.
pub const CLUSTER_STATE_FILE: &str = "cluster-state.json";

/// Prefix of temp files written by [`ClusterState::save_atomic`].
const CLUSTER_STATE_TMP_PREFIX: &str = ".cluster-state.json.tmp.";

/// Returns true if `name` is `cluster-state.json` or a temp file created while writing it.
pub fn is_cluster_state_file(name: &OsStr) -> bool {
    name == CLUSTER_STATE_FILE
        || name
            .to_str()
            .is_some_and(|s| s.starts_with(CLUSTER_STATE_TMP_PREFIX))
}

/// Node role stored in `cluster-state.json`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PersistedRole {
    Primary,
    Replica,
}

/// Ownership of one hash slot in the slot table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotBinding {
    pub node_id: String,
    pub epoch: u64,
}

/// Compact serialized run of contiguous slots sharing `(node_id, config_epoch)`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlotRangeBinding {
    pub start: u16,
    pub end: u16,
    pub node_id: String,
    pub config_epoch: u64,
}

/// Result of merging a peer's slot claims into the local slot table.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MergeOutcome {
    pub changed: bool,
    pub lost_own_slots: bool,
}

/// Epoch-versioned mapping of all 16384 hash slots.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotTable {
    slots: Vec<Option<SlotBinding>>,
}

impl Default for SlotTable {
    fn default() -> Self {
        Self::new()
    }
}

impl SlotTable {
    pub fn new() -> Self {
        Self {
            slots: vec![None; REDIS_CLUSTER_SLOTS as usize],
        }
    }

    pub fn get(&self, slot: u16) -> Option<&SlotBinding> {
        self.slots.get(slot as usize)?.as_ref()
    }

    pub fn set(&mut self, slot: u16, node_id: impl Into<String>, epoch: u64) {
        if let Some(entry) = self.slots.get_mut(slot as usize) {
            *entry = Some(SlotBinding {
                node_id: node_id.into(),
                epoch,
            });
        }
    }

    pub fn clear(&mut self, slot: u16) {
        if let Some(entry) = self.slots.get_mut(slot as usize) {
            *entry = None;
        }
    }

    pub fn slots_of(&self, node_id: &str) -> HashSet<u16> {
        self.slots
            .iter()
            .enumerate()
            .filter_map(|(slot, binding)| {
                binding
                    .as_ref()
                    .filter(|b| b.node_id == node_id)
                    .map(|_| slot as u16)
            })
            .collect()
    }

    pub fn has_slots_of(&self, node_id: &str) -> bool {
        self.slots
            .iter()
            .any(|binding| binding.as_ref().is_some_and(|b| b.node_id == node_id))
    }

    pub fn slot_count(&self) -> usize {
        self.slots.iter().filter(|b| b.is_some()).count()
    }

    pub fn owners(&self) -> BTreeSet<String> {
        self.slots
            .iter()
            .filter_map(|binding| binding.as_ref().map(|b| b.node_id.clone()))
            .collect()
    }

    pub fn max_epoch_for(&self, node_id: &str) -> u64 {
        self.slots
            .iter()
            .filter_map(|binding| {
                binding
                    .as_ref()
                    .filter(|b| b.node_id == node_id)
                    .map(|b| b.epoch)
            })
            .max()
            .unwrap_or(0)
    }

    /// Collapse contiguous slots with the same `(node_id, epoch)` into runs.
    pub fn runs(&self) -> Vec<SlotRangeBinding> {
        let mut out: Vec<SlotRangeBinding> = Vec::new();
        for (idx, binding) in self.slots.iter().enumerate() {
            let Some(binding) = binding else {
                continue;
            };
            let slot = idx as u16;
            match out.last_mut() {
                Some(last)
                    if last.end + 1 == slot
                        && last.node_id == binding.node_id
                        && last.config_epoch == binding.epoch =>
                {
                    last.end = slot;
                }
                _ => out.push(SlotRangeBinding {
                    start: slot,
                    end: slot,
                    node_id: binding.node_id.clone(),
                    config_epoch: binding.epoch,
                }),
            }
        }
        out
    }

    /// Collapse contiguous slots with the same owner into `(start, end, node_id)` runs.
    pub fn runs_by_owner(&self) -> Vec<(u16, u16, String)> {
        let mut out: Vec<(u16, u16, String)> = Vec::new();
        for (idx, binding) in self.slots.iter().enumerate() {
            let Some(binding) = binding else {
                continue;
            };
            let slot = idx as u16;
            match out.last_mut() {
                Some((_, end, owner)) if *end + 1 == slot && *owner == binding.node_id => {
                    *end = slot;
                }
                _ => out.push((slot, slot, binding.node_id.clone())),
            }
        }
        out
    }

    /// Reconstruct a slot table from serialized runs, rejecting invalid or overlapping ranges.
    pub fn from_runs(runs: Vec<SlotRangeBinding>) -> Result<Self, String> {
        let mut table = Self::new();
        for run in runs {
            if run.node_id.is_empty() {
                return Err("slot binding has empty node_id".to_string());
            }
            if run.start > run.end || run.end >= REDIS_CLUSTER_SLOTS {
                return Err(format!(
                    "invalid slot range {}-{} for node {}",
                    run.start, run.end, run.node_id
                ));
            }
            for slot in run.start..=run.end {
                if table.slots[slot as usize].is_some() {
                    return Err(format!(
                        "overlapping slot binding for slot {} in cluster state",
                        slot
                    ));
                }
                table.slots[slot as usize] = Some(SlotBinding {
                    node_id: run.node_id.clone(),
                    epoch: run.config_epoch,
                });
            }
        }
        Ok(table)
    }

    /// Apply a primary's slot claim by the epoch rule (`Failover.tla` `Merge`):
    /// bind unbound slots and rebind only on a strictly higher epoch.
    pub fn merge_claim(
        &mut self,
        claimant: &str,
        epoch: u64,
        claimed_slots: &HashSet<u16>,
        self_id: &str,
    ) -> MergeOutcome {
        let mut outcome = MergeOutcome::default();
        for &slot in claimed_slots {
            let Some(entry) = self.slots.get_mut(slot as usize) else {
                continue;
            };
            match entry {
                None => {
                    *entry = Some(SlotBinding {
                        node_id: claimant.to_string(),
                        epoch,
                    });
                    outcome.changed = true;
                }
                Some(existing) if epoch > existing.epoch => {
                    if existing.node_id == self_id && claimant != self_id {
                        outcome.lost_own_slots = true;
                    }
                    *entry = Some(SlotBinding {
                        node_id: claimant.to_string(),
                        epoch,
                    });
                    outcome.changed = true;
                }
                Some(_) => {}
            }
        }
        outcome
    }

    /// Update the epoch on every slot currently bound to `node_id`.
    pub fn rebind_owner_epoch(&mut self, node_id: &str, new_epoch: u64) {
        for binding in self.slots.iter_mut().flatten() {
            if binding.node_id == node_id {
                binding.epoch = new_epoch;
            }
        }
    }
}

/// On-disk JSON representation of [`ClusterState`].
#[derive(Debug, Serialize, Deserialize)]
struct ClusterStateWire {
    node_id: String,
    current_epoch: u64,
    last_vote_epoch: u64,
    #[serde(default)]
    last_vote_for: BTreeMap<String, u64>,
    role: PersistedRole,
    primary: Option<String>,
    config_epoch: u64,
    #[serde(default)]
    slots: Vec<SlotRangeBinding>,
    /// Durable marker set when a primary lost its last slot and must finish
    /// demoting to a replica of this node; cleared when `role = replica` persists.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    demoting_to: Option<String>,
    /// Durable marker set when a replica saw its primary's slots move to this
    /// node; cleared when `primary` is updated.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    repointing_to: Option<String>,
}

/// Persisted per-node cluster state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterState {
    pub node_id: String,
    pub current_epoch: u64,
    pub last_vote_epoch: u64,
    pub last_vote_for: BTreeMap<String, u64>,
    pub role: PersistedRole,
    pub primary: Option<String>,
    pub config_epoch: u64,
    pub slots: SlotTable,
    pub demoting_to: Option<String>,
    pub repointing_to: Option<String>,
}

impl ClusterState {
    pub fn bootstrap_primary(node_id: String) -> Self {
        Self {
            node_id,
            current_epoch: 0,
            last_vote_epoch: 0,
            last_vote_for: BTreeMap::new(),
            role: PersistedRole::Primary,
            primary: None,
            config_epoch: 0,
            slots: SlotTable::new(),
            demoting_to: None,
            repointing_to: None,
        }
    }

    pub fn bootstrap_replica(node_id: String, primary: String) -> Self {
        Self {
            node_id,
            current_epoch: 0,
            last_vote_epoch: 0,
            last_vote_for: BTreeMap::new(),
            role: PersistedRole::Replica,
            primary: Some(primary),
            config_epoch: 0,
            slots: SlotTable::new(),
            demoting_to: None,
            repointing_to: None,
        }
    }

    pub fn is_replica(&self) -> bool {
        self.role == PersistedRole::Replica
    }

    fn to_wire(&self) -> ClusterStateWire {
        ClusterStateWire {
            node_id: self.node_id.clone(),
            current_epoch: self.current_epoch,
            last_vote_epoch: self.last_vote_epoch,
            last_vote_for: self.last_vote_for.clone(),
            role: self.role,
            primary: self.primary.clone(),
            config_epoch: self.config_epoch,
            slots: self.slots.runs(),
            demoting_to: self.demoting_to.clone(),
            repointing_to: self.repointing_to.clone(),
        }
    }

    fn from_wire(
        wire: ClusterStateWire,
        expected_node_id: &str,
        path: &Path,
    ) -> Result<Self, String> {
        if wire.node_id.is_empty() {
            return Err(format!(
                "cluster state {} has an empty node_id",
                path.display()
            ));
        }
        if wire.node_id != expected_node_id {
            return Err(format!(
                "cluster state {} belongs to node '{}', but config has node_id '{}'",
                path.display(),
                wire.node_id,
                expected_node_id
            ));
        }
        let slots = SlotTable::from_runs(wire.slots)?;
        match wire.role {
            PersistedRole::Replica => {
                let Some(ref primary) = wire.primary else {
                    return Err("cluster state has role=replica without a primary".to_string());
                };
                if primary.is_empty() {
                    return Err("cluster state has role=replica with empty primary".to_string());
                }
                if primary == expected_node_id {
                    return Err(format!(
                        "cluster state has role=replica pointing to self ({expected_node_id})"
                    ));
                }
                if slots.has_slots_of(expected_node_id) {
                    return Err(format!(
                        "cluster state has role=replica while node {expected_node_id} owns slots"
                    ));
                }
                if wire.demoting_to.is_some() {
                    return Err("cluster state has role=replica with demoting_to set".to_string());
                }
                if let Some(ref target) = wire.repointing_to {
                    if target.is_empty() {
                        return Err("cluster state has empty repointing_to".to_string());
                    }
                    if target == expected_node_id {
                        return Err(format!(
                            "cluster state has repointing_to pointing to self ({expected_node_id})"
                        ));
                    }
                }
            }
            PersistedRole::Primary => {
                if wire.primary.is_some() {
                    return Err("cluster state has role=primary with a primary field".to_string());
                }
                if wire.repointing_to.is_some() {
                    return Err("cluster state has role=primary with repointing_to set".to_string());
                }
                if let Some(ref winner) = wire.demoting_to {
                    if winner.is_empty() {
                        return Err("cluster state has empty demoting_to".to_string());
                    }
                    if winner == expected_node_id {
                        return Err(format!(
                            "cluster state has demoting_to pointing to self ({expected_node_id})"
                        ));
                    }
                    if slots.has_slots_of(expected_node_id) {
                        return Err(format!(
                            "cluster state has demoting_to set while node {expected_node_id} owns slots"
                        ));
                    }
                }
            }
        }
        Ok(Self {
            node_id: wire.node_id,
            current_epoch: wire.current_epoch,
            last_vote_epoch: wire.last_vote_epoch,
            last_vote_for: wire.last_vote_for,
            role: wire.role,
            primary: wire.primary,
            config_epoch: wire.config_epoch,
            slots,
            demoting_to: wire.demoting_to,
            repointing_to: wire.repointing_to,
        })
    }

    /// Write `self` atomically: temp file in the same directory, fsync file,
    /// rename into place, fsync the parent directory.
    pub fn save_atomic(&self, path: &Path) -> Result<(), String> {
        static NEXT_TMP: AtomicU64 = AtomicU64::new(0);

        let bytes = serde_json::to_vec_pretty(&self.to_wire())
            .map_err(|e| format!("failed to serialize cluster state: {e}"))?;
        let parent = path.parent().unwrap_or_else(|| Path::new("."));
        let seq = NEXT_TMP.fetch_add(1, Ordering::Relaxed);
        let tmp_path = parent.join(format!(
            "{CLUSTER_STATE_TMP_PREFIX}{}.{seq}",
            std::process::id()
        ));

        let write_result = (|| -> std::io::Result<()> {
            let mut file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp_path)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            drop(file);
            fs::rename(&tmp_path, path)?;
            let dir = File::open(parent)?;
            dir.sync_all()?;
            Ok(())
        })();

        if let Err(e) = write_result {
            let _ = fs::remove_file(&tmp_path);
            return Err(format!(
                "failed to persist cluster state to {}: {e}",
                path.display()
            ));
        }
        Ok(())
    }

    /// Load and validate `cluster-state.json` from `path`.
    pub fn load(path: &Path, node_id: &str) -> Result<Self, String> {
        let raw = fs::read(path)
            .map_err(|e| format!("failed to read cluster state {}: {e}", path.display()))?;
        let wire: ClusterStateWire = serde_json::from_slice(&raw)
            .map_err(|e| format!("failed to parse cluster state {}: {e}", path.display()))?;
        Self::from_wire(wire, node_id, path)
    }
}

/// Remove stale `.cluster-state.json.tmp.*` files left behind by an interrupted write.
fn cleanup_stale_tmp_files(data_dir: &Path) {
    let Ok(entries) = fs::read_dir(data_dir) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        if name
            .to_str()
            .is_some_and(|s| s.starts_with(CLUSTER_STATE_TMP_PREFIX))
        {
            let path = entry.path();
            match fs::remove_file(&path) {
                Ok(()) => warn!("Removed stale cluster state temp file {}", path.display()),
                Err(e) => warn!(
                    "Failed to remove stale cluster state temp file {}: {}",
                    path.display(),
                    e
                ),
            }
        }
    }
}

/// Configured slots from `[cluster]` `slots` and `slot_ranges`.
pub fn configured_slots(config: &ClusterConfig) -> HashSet<u16> {
    let mut slots = HashSet::new();
    slots.extend(config.slots.iter().flatten().copied());
    for range in config.slot_ranges.iter().flatten() {
        slots.extend(range.start..=range.end);
    }
    slots
}

/// Startup result of loading `<data_dir>/cluster-state.json` or preparing a bootstrap.
#[derive(Debug, Clone)]
pub struct LoadedClusterState {
    pub state: ClusterState,
    /// True when loaded from an existing state file (wins over config).
    pub from_file: bool,
    pub path: PathBuf,
}

/// Load `<data_dir>/cluster-state.json` if it exists, otherwise build the
/// initial state from config (without writing the file yet; bootstrap writes it
/// once `claim_slots` completes). Removes stale temp files left by crashes.
pub fn load_or_bootstrap(
    data_dir: &Path,
    config: &ClusterConfig,
    cfg_replica_of: Option<&str>,
) -> Result<LoadedClusterState, String> {
    cleanup_stale_tmp_files(data_dir);

    let path = data_dir.join(CLUSTER_STATE_FILE);
    if path.exists() {
        let state = ClusterState::load(&path, &config.node_id)?;
        warn_on_config_disagreement(&state, config, cfg_replica_of);
        return Ok(LoadedClusterState {
            state,
            from_file: true,
            path,
        });
    }

    let state = match cfg_replica_of {
        Some(primary) => {
            if primary == config.node_id {
                return Err(format!("replica_of must be another node, not {primary}"));
            }
            ClusterState::bootstrap_replica(config.node_id.clone(), primary.to_string())
        }
        None => ClusterState::bootstrap_primary(config.node_id.clone()),
    };
    Ok(LoadedClusterState {
        state,
        from_file: false,
        path,
    })
}

fn warn_on_config_disagreement(
    state: &ClusterState,
    config: &ClusterConfig,
    cfg_replica_of: Option<&str>,
) {
    let cfg_slots = configured_slots(config);
    match state.role {
        PersistedRole::Primary => {
            if let Some(cfg_primary) = cfg_replica_of {
                warn!(
                    "Config sets replica_of = \"{}\", but {} has role = primary; using persisted state",
                    cfg_primary, CLUSTER_STATE_FILE
                );
            }
            let persisted_slots = state.slots.slots_of(&config.node_id);
            if cfg_slots != persisted_slots {
                warn!(
                    "Config slots ({}) disagree with {} ({}); using persisted state",
                    ClusterManager::slots_to_ranges(&cfg_slots),
                    CLUSTER_STATE_FILE,
                    ClusterManager::slots_to_ranges(&persisted_slots)
                );
            }
        }
        PersistedRole::Replica => {
            let persisted_primary = state.primary.as_deref().unwrap_or("");
            match cfg_replica_of {
                None => warn!(
                    "Config has no replica_of, but {} has role = replica of \"{}\"; using persisted state",
                    CLUSTER_STATE_FILE, persisted_primary
                ),
                Some(cfg_primary) if cfg_primary != persisted_primary => warn!(
                    "Config sets replica_of = \"{}\", but {} follows \"{}\"; using persisted state",
                    cfg_primary, CLUSTER_STATE_FILE, persisted_primary
                ),
                Some(_) => {}
            }
            if !cfg_slots.is_empty() {
                warn!(
                    "Config specifies slots ({}), but {} has role = replica; using persisted state",
                    ClusterManager::slots_to_ranges(&cfg_slots),
                    CLUSTER_STATE_FILE
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SlotRange;
    use tempfile::TempDir;

    fn test_cluster_cfg(node_id: &str, slot_ranges: Option<Vec<SlotRange>>) -> ClusterConfig {
        ClusterConfig {
            enabled: true,
            node_id: node_id.to_string(),
            seeds: vec![],
            port: 7000,
            slots: None,
            slot_ranges,
            gossip_interval_ms: Some(100),
            advertise_addr: None,
            node_timeout_ms: None,
            replica_validity_factor: None,
        }
    }

    #[test]
    fn state_file_round_trip_and_compact_slot_runs() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join(CLUSTER_STATE_FILE);

        let mut state = ClusterState::bootstrap_primary("node-1".to_string());
        state.current_epoch = 7;
        state.last_vote_epoch = 5;
        state.last_vote_for.insert("node-dead".to_string(), 5);
        state.config_epoch = 4;
        for s in 0..=5460 {
            state.slots.set(s, "node-1", 4);
        }
        for s in 5461..=10922 {
            state.slots.set(s, "node-2", 2);
        }
        state.slots.set(16383, "node-3", 7);

        state.save_atomic(&path).unwrap();

        // Compact runs: 3 runs instead of 10924 individual entries.
        let raw = fs::read_to_string(&path).unwrap();
        assert!(raw.len() < 1000, "state JSON should be compact:\n{raw}");
        // No temp file left behind after atomic rename.
        let entries: Vec<_> = fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(entries, vec![CLUSTER_STATE_FILE]);

        let loaded = ClusterState::load(&path, "node-1").unwrap();
        assert_eq!(loaded, state);
    }

    #[test]
    fn atomic_write_overwrites_existing_file_cleanly() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join(CLUSTER_STATE_FILE);

        let mut first = ClusterState::bootstrap_primary("node-2".to_string());
        first.current_epoch = 1;
        first.save_atomic(&path).unwrap();

        let mut second =
            ClusterState::bootstrap_replica("node-2".to_string(), "node-1".to_string());
        second.current_epoch = 9;
        second.last_vote_epoch = 8;
        second.save_atomic(&path).unwrap();

        let loaded = ClusterState::load(&path, "node-2").unwrap();
        assert_eq!(loaded, second);
    }

    #[test]
    fn bootstrap_vs_existing_state_file() {
        let dir = TempDir::new().unwrap();
        let cfg = test_cluster_cfg("node-1", Some(vec![SlotRange { start: 0, end: 10 }]));

        // Bootstrap without state file follows config.
        let boot = load_or_bootstrap(dir.path(), &cfg, None).unwrap();
        assert!(!boot.from_file);
        assert_eq!(boot.state.node_id, "node-1");
        assert_eq!(boot.state.role, PersistedRole::Primary);
        assert_eq!(boot.state.primary, None);
        assert!(!boot.path.exists(), "bootstrap writes only after claim");

        // Persist a replica state file, then restart with primary config: file wins.
        let mut persisted =
            ClusterState::bootstrap_replica("node-1".to_string(), "node-2".to_string());
        persisted.current_epoch = 3;
        persisted.save_atomic(&boot.path).unwrap();

        let reloaded = load_or_bootstrap(dir.path(), &cfg, None).unwrap();
        assert!(reloaded.from_file);
        assert_eq!(reloaded.state.node_id, "node-1");
        assert_eq!(reloaded.state.role, PersistedRole::Replica);
        assert_eq!(reloaded.state.primary.as_deref(), Some("node-2"));
        assert_eq!(reloaded.state.current_epoch, 3);
    }

    #[test]
    fn load_rejects_mismatched_node_id() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join(CLUSTER_STATE_FILE);

        let state = ClusterState::bootstrap_primary("node-1".to_string());
        state.save_atomic(&path).unwrap();

        let err = ClusterState::load(&path, "node-2").unwrap_err();
        assert!(err.contains("node-1"), "error should name state id: {err}");
        assert!(err.contains("node-2"), "error should name config id: {err}");
        assert!(
            err.contains(&path.display().to_string()),
            "error should name file path: {err}"
        );

        let cfg = test_cluster_cfg("node-2", None);
        let boot_err = load_or_bootstrap(dir.path(), &cfg, None).unwrap_err();
        assert!(boot_err.contains("node-1") && boot_err.contains("node-2"));
    }

    #[test]
    fn load_or_bootstrap_removes_stale_tmp_files() {
        let dir = TempDir::new().unwrap();
        let stale1 = dir.path().join(".cluster-state.json.tmp.1234.0");
        let stale2 = dir.path().join(".cluster-state.json.tmp.5678.9");
        let keep = dir.path().join("other.txt");
        fs::write(&stale1, b"partial").unwrap();
        fs::write(&stale2, b"partial").unwrap();
        fs::write(&keep, b"keep").unwrap();

        let cfg = test_cluster_cfg("node-1", None);
        let loaded = load_or_bootstrap(dir.path(), &cfg, None).unwrap();
        assert!(!loaded.from_file);
        assert!(!stale1.exists());
        assert!(!stale2.exists());
        assert!(keep.exists());

        // Also cleans up when an existing cluster-state.json is loaded.
        loaded.state.save_atomic(&loaded.path).unwrap();
        fs::write(&stale1, b"partial").unwrap();
        let reloaded = load_or_bootstrap(dir.path(), &cfg, None).unwrap();
        assert!(reloaded.from_file);
        assert!(!stale1.exists());
        assert!(loaded.path.exists());
    }

    #[test]
    fn slot_table_merge_respects_epoch_rule() {
        let mut table = SlotTable::new();
        let slots_a: HashSet<u16> = [0, 1, 2].into_iter().collect();

        // Unbound slots accept epoch 0.
        let out = table.merge_claim("node-a", 0, &slots_a, "node-a");
        assert!(out.changed);
        assert!(!out.lost_own_slots);
        assert_eq!(table.get(0).unwrap().node_id, "node-a");
        assert_eq!(table.get(0).unwrap().epoch, 0);

        // Equal epoch from another node is ignored.
        let out = table.merge_claim("node-b", 0, &slots_a, "node-a");
        assert!(!out.changed);
        assert!(!out.lost_own_slots);
        assert_eq!(table.get(0).unwrap().node_id, "node-a");

        // Strictly higher epoch wins and flags lost own slots.
        let take: HashSet<u16> = [1, 2].into_iter().collect();
        let out = table.merge_claim("node-b", 2, &take, "node-a");
        assert!(out.changed);
        assert!(out.lost_own_slots);
        assert_eq!(table.get(0).unwrap().node_id, "node-a");
        assert_eq!(table.get(1).unwrap().node_id, "node-b");
        assert_eq!(table.get(1).unwrap().epoch, 2);

        // Lower epoch from returning old owner is ignored.
        let out = table.merge_claim("node-a", 1, &take, "node-b");
        assert!(!out.changed);
        assert!(!out.lost_own_slots);
        assert_eq!(table.get(1).unwrap().node_id, "node-b");
        assert_eq!(table.get(1).unwrap().epoch, 2);
    }

    #[test]
    fn load_rejects_invalid_state_files() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join(CLUSTER_STATE_FILE);

        // Replica without primary.
        fs::write(
            &path,
            r#"{"node_id":"n1","current_epoch":0,"last_vote_epoch":0,"last_vote_for":{},"role":"replica","primary":null,"config_epoch":0,"slots":[]}"#,
        )
        .unwrap();
        assert!(ClusterState::load(&path, "n1").is_err());

        // Overlapping slot ranges.
        fs::write(
            &path,
            r#"{"node_id":"n1","current_epoch":1,"last_vote_epoch":0,"last_vote_for":{},"role":"primary","primary":null,"config_epoch":1,"slots":[{"start":0,"end":5,"node_id":"n1","config_epoch":1},{"start":5,"end":10,"node_id":"n2","config_epoch":1}]}"#,
        )
        .unwrap();
        assert!(ClusterState::load(&path, "n1").is_err());

        // Replica with demoting_to set.
        fs::write(
            &path,
            r#"{"node_id":"n1","current_epoch":1,"last_vote_epoch":0,"last_vote_for":{},"role":"replica","primary":"n2","config_epoch":0,"slots":[],"demoting_to":"n2"}"#,
        )
        .unwrap();
        assert!(ClusterState::load(&path, "n1").is_err());

        // Primary with repointing_to set.
        fs::write(
            &path,
            r#"{"node_id":"n1","current_epoch":1,"last_vote_epoch":0,"last_vote_for":{},"role":"primary","primary":null,"config_epoch":1,"slots":[],"repointing_to":"n2"}"#,
        )
        .unwrap();
        assert!(ClusterState::load(&path, "n1").is_err());

        // Primary with demoting_to set while still owning slots.
        fs::write(
            &path,
            r#"{"node_id":"n1","current_epoch":1,"last_vote_epoch":0,"last_vote_for":{},"role":"primary","primary":null,"config_epoch":1,"slots":[{"start":0,"end":5,"node_id":"n1","config_epoch":1}],"demoting_to":"n2"}"#,
        )
        .unwrap();
        assert!(ClusterState::load(&path, "n1").is_err());

        // demoting_to pointing to self.
        fs::write(
            &path,
            r#"{"node_id":"n1","current_epoch":1,"last_vote_epoch":0,"last_vote_for":{},"role":"primary","primary":null,"config_epoch":1,"slots":[],"demoting_to":"n1"}"#,
        )
        .unwrap();
        assert!(ClusterState::load(&path, "n1").is_err());
    }

    #[test]
    fn demoting_and_repointing_markers_round_trip() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join(CLUSTER_STATE_FILE);

        let mut primary = ClusterState::bootstrap_primary("n1".to_string());
        primary.current_epoch = 2;
        primary.slots.set(0, "n2", 2);
        primary.demoting_to = Some("n2".to_string());
        primary.save_atomic(&path).unwrap();
        assert_eq!(ClusterState::load(&path, "n1").unwrap(), primary);

        let mut replica = ClusterState::bootstrap_replica("n3".to_string(), "n1".to_string());
        replica.current_epoch = 2;
        replica.slots.set(0, "n2", 2);
        replica.repointing_to = Some("n2".to_string());
        replica.save_atomic(&path).unwrap();
        assert_eq!(ClusterState::load(&path, "n3").unwrap(), replica);
    }
}
