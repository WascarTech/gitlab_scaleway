//! Server state management.
//!
//! Manages the state of the current runner, especially
//! the creation time for the deletion logic.
//!
//! The state is persisted to `config/state.json` to survive
//! program restarts.

use std::path::Path;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{debug, info, warn};

/// Errors during state management.
#[derive(Error, Debug)]
pub enum StateError {
    #[error("Failed to read state file: {0}")]
    Read(#[from] std::io::Error),

    #[error("Failed to serialize state: {0}")]
    Serialize(#[from] serde_json::Error),
}

/// A resource ID paired with the zone it belongs to.
///
/// `zone: None` marks a pre-upgrade state entry whose zone is unknown; cleanup
/// tries every configured zone for those.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ZonedId {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub zone: Option<String>,
    pub id: String,
}

impl<'de> Deserialize<'de> for ZonedId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Repr {
            Id(String),
            Zoned { zone: Option<String>, id: String },
        }

        Ok(match Repr::deserialize(deserializer)? {
            Repr::Id(id) => Self { zone: None, id },
            Repr::Zoned { zone, id } => Self { zone, id },
        })
    }
}

/// State of an active runner.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunnerState {
    /// Scaleway server UUID
    pub server_id: String,
    /// Server name
    pub server_name: String,
    /// Availability Zone the server was created in. Empty for legacy state.
    #[serde(default)]
    pub zone: String,
    /// Creation timestamp
    pub created_at: DateTime<Utc>,
    /// IDs of volumes attached at creation, deleted after termination
    #[serde(default)]
    pub volume_ids: Vec<String>,
    /// Reserved flexible IPv4 IDs owned by this runner
    #[serde(default)]
    pub ip_ids: Vec<String>,
}

impl RunnerState {
    /// Creates a new runner state with current timestamp.
    pub fn new(
        server_id: String,
        server_name: String,
        zone: String,
        volume_ids: Vec<String>,
        ip_ids: Vec<String>,
    ) -> Self {
        let created_at = Utc::now();
        info!(
            "Runner state created: Server {} (ID: {}, zone: {})",
            server_name, server_id, zone
        );

        Self {
            server_id,
            server_name,
            zone,
            created_at,
            volume_ids,
            ip_ids,
        }
    }

    /// Calculates how long the server has been running (in minutes).
    pub fn uptime_minutes(&self) -> u64 {
        let duration = Utc::now().signed_duration_since(self.created_at);
        duration.num_minutes().max(0) as u64
    }
}

/// Persisted state (saved as JSON).
#[derive(Debug, Serialize, Deserialize)]
struct PersistedState {
    runner: Option<RunnerState>,
    /// Volumes awaiting deletion. The legacy `pending_volume_ids` key held bare
    /// strings, which `ZonedId` still deserializes.
    #[serde(default, alias = "pending_volume_ids")]
    pending_volumes: Vec<ZonedId>,
    #[serde(default)]
    pending_ips: Vec<ZonedId>,
}

/// Removes duplicate resources, keeping the first occurrence.
fn dedupe_resources(items: Vec<ZonedId>) -> Vec<ZonedId> {
    let mut deduped = Vec::new();
    for item in items {
        if !deduped
            .iter()
            .any(|existing: &ZonedId| existing.zone == item.zone && existing.id == item.id)
        {
            deduped.push(item);
        }
    }
    deduped
}

/// Orchestrator state - manages the entire application state.
#[derive(Debug, Default)]
pub struct OrchestratorState {
    /// Current runner (if present)
    pub runner: Option<RunnerState>,
    /// Path to the state file
    state_file_path: Option<std::path::PathBuf>,
    /// Volumes whose deletion failed and that must be retried
    pending_volumes: Vec<ZonedId>,
    /// Reserved IPs whose release failed and that must be retried
    pending_ips: Vec<ZonedId>,
}

impl OrchestratorState {
    /// Creates a new orchestrator state without persistence (for tests only).
    #[cfg(test)]
    pub fn new() -> Self {
        Self {
            runner: None,
            state_file_path: None,
            pending_volumes: Vec::new(),
            pending_ips: Vec::new(),
        }
    }

    /// Creates a new orchestrator state with persistence.
    ///
    /// Automatically loads the saved state if present.
    pub fn with_persistence<P: AsRef<Path>>(state_file: P) -> Result<Self, StateError> {
        let path = state_file.as_ref().to_path_buf();
        let mut state = Self {
            runner: None,
            state_file_path: Some(path.clone()),
            pending_volumes: Vec::new(),
            pending_ips: Vec::new(),
        };

        // Try to load state
        if path.exists() {
            match state.load_from_file(&path) {
                Ok(()) => {
                    if state.runner.is_some() {
                        info!("State loaded from file: {}", path.display());
                    }
                }
                Err(e) => {
                    warn!("Could not load state (ignoring): {}", e);
                }
            }
        }

        Ok(state)
    }

    /// Loads the state from a file.
    fn load_from_file(&mut self, path: &Path) -> Result<(), StateError> {
        let content = std::fs::read_to_string(path)?;
        let persisted: PersistedState = serde_json::from_str(&content)?;
        self.runner = persisted.runner;
        self.pending_volumes = persisted.pending_volumes;
        self.pending_ips = persisted.pending_ips;
        Ok(())
    }

    /// Saves the state to file.
    fn save_to_file(&self) -> Result<(), StateError> {
        if let Some(ref path) = self.state_file_path {
            let persisted = PersistedState {
                runner: self.runner.clone(),
                pending_volumes: self.pending_volumes.clone(),
                pending_ips: self.pending_ips.clone(),
            };
            let content = serde_json::to_string_pretty(&persisted)?;
            std::fs::write(path, content)?;
            debug!("State saved: {}", path.display());
        }
        Ok(())
    }

    /// Sets the active runner and saves the state.
    pub fn set_runner(&mut self, state: RunnerState) {
        info!(
            "Active runner set: {} (ID: {})",
            state.server_name, state.server_id
        );
        self.runner = Some(state);

        if let Err(e) = self.save_to_file() {
            warn!("Error saving state: {}", e);
        }
    }

    /// Removes the active runner and saves the state.
    pub fn clear_runner(&mut self) {
        if let Some(ref runner) = self.runner {
            info!(
                "Runner removed: {} (runtime: {} minutes)",
                runner.server_name,
                runner.uptime_minutes()
            );
        }
        self.runner = None;

        if let Err(e) = self.save_to_file() {
            warn!("Error saving state: {}", e);
        }
    }

    /// Returns a copy of the volumes still awaiting deletion.
    pub fn pending_volumes(&self) -> Vec<ZonedId> {
        self.pending_volumes.clone()
    }

    /// Replaces the pending volume list, de-duplicating and persisting it.
    pub fn set_pending_volumes(&mut self, volumes: Vec<ZonedId>) {
        self.pending_volumes = dedupe_resources(volumes);
        if let Err(e) = self.save_to_file() {
            warn!("Error saving state: {}", e);
        }
    }

    /// Adds volumes to the pending cleanup list.
    pub fn add_pending_volumes(&mut self, volumes: Vec<ZonedId>) {
        if volumes.is_empty() {
            return;
        }
        let mut all = self.pending_volumes.clone();
        all.extend(volumes);
        self.set_pending_volumes(all);
    }

    /// Returns a copy of the reserved IPs still awaiting release.
    pub fn pending_ips(&self) -> Vec<ZonedId> {
        self.pending_ips.clone()
    }

    /// Replaces the pending IP list, de-duplicating and persisting it.
    pub fn set_pending_ips(&mut self, ips: Vec<ZonedId>) {
        self.pending_ips = dedupe_resources(ips);
        if let Err(e) = self.save_to_file() {
            warn!("Error saving state: {}", e);
        }
    }

    /// Adds reserved IPs to the pending cleanup list.
    pub fn add_pending_ips(&mut self, ips: Vec<ZonedId>) {
        if ips.is_empty() {
            return;
        }
        let mut all = self.pending_ips.clone();
        all.extend(ips);
        self.set_pending_ips(all);
    }

    /// Checks if a runner is active.
    pub fn has_runner(&self) -> bool {
        self.runner.is_some()
    }

    /// Returns the uptime of the current runner (if present).
    pub fn runner_uptime(&self) -> Option<u64> {
        self.runner.as_ref().map(|r| r.uptime_minutes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn zoned(zone: &str, id: &str) -> ZonedId {
        ZonedId {
            zone: Some(zone.to_string()),
            id: id.to_string(),
        }
    }

    #[test]
    fn test_runner_state_creation() {
        let state = RunnerState::new(
            "server-uuid".to_string(),
            "test-runner".to_string(),
            "fr-par-1".to_string(),
            vec!["vol-1".to_string()],
            vec!["ip-1".to_string()],
        );

        assert_eq!(state.server_id, "server-uuid");
        assert_eq!(state.server_name, "test-runner");
        assert_eq!(state.zone, "fr-par-1");
        assert_eq!(state.volume_ids, vec!["vol-1".to_string()]);
        assert_eq!(state.ip_ids, vec!["ip-1".to_string()]);
        assert!(state.uptime_minutes() < 1);
    }

    #[test]
    fn test_orchestrator_state() {
        let mut state = OrchestratorState::new();
        assert!(!state.has_runner());

        let runner = RunnerState::new(
            "server-uuid".to_string(),
            "test-runner".to_string(),
            "fr-par-1".to_string(),
            vec![],
            vec![],
        );
        state.set_runner(runner);

        assert!(state.has_runner());

        state.clear_runner();
        assert!(!state.has_runner());
    }

    #[test]
    fn test_state_serialization() {
        let runner = RunnerState::new(
            "server-uuid".to_string(),
            "test-runner".to_string(),
            "pl-waw-1".to_string(),
            vec!["vol-1".to_string()],
            vec!["ip-1".to_string()],
        );
        let json = serde_json::to_string(&runner).unwrap();
        let restored: RunnerState = serde_json::from_str(&json).unwrap();

        assert_eq!(restored.server_id, "server-uuid");
        assert_eq!(restored.server_name, "test-runner");
        assert_eq!(restored.zone, "pl-waw-1");
        assert_eq!(restored.volume_ids, vec!["vol-1".to_string()]);
        assert_eq!(restored.ip_ids, vec!["ip-1".to_string()]);
    }

    #[test]
    fn test_state_without_zone_and_ip_ids_still_loads() {
        let json = r#"{
            "server_id": "server-uuid",
            "server_name": "test-runner",
            "created_at": "2026-01-14T10:30:00Z"
        }"#;
        let restored: RunnerState = serde_json::from_str(json).unwrap();
        assert_eq!(restored.server_id, "server-uuid");
        assert!(restored.zone.is_empty());
        assert!(restored.volume_ids.is_empty());
        assert!(restored.ip_ids.is_empty());
    }

    #[test]
    fn test_zoned_id_accepts_legacy_string() {
        let zoned: ZonedId = serde_json::from_str(r#""vol-1""#).unwrap();
        assert_eq!(
            zoned,
            ZonedId {
                zone: None,
                id: "vol-1".to_string()
            }
        );
    }

    #[test]
    fn test_zoned_id_round_trip() {
        let id = zoned("fr-par-1", "vol-1");
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, r#"{"zone":"fr-par-1","id":"vol-1"}"#);
        assert_eq!(serde_json::from_str::<ZonedId>(&json).unwrap(), id);
    }

    #[test]
    fn test_zoned_id_omits_unknown_zone() {
        let id = ZonedId {
            zone: None,
            id: "vol-1".to_string(),
        };
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, r#"{"id":"vol-1"}"#);
        assert_eq!(
            serde_json::from_str::<ZonedId>(&json).unwrap(),
            ZonedId {
                zone: None,
                id: "vol-1".to_string()
            }
        );
    }

    #[test]
    fn test_legacy_pending_volume_ids_migrate() {
        let json = r#"{"runner": null, "pending_volume_ids": ["vol-a", "vol-b"]}"#;
        let persisted: PersistedState = serde_json::from_str(json).unwrap();
        assert_eq!(
            persisted.pending_volumes,
            vec![
                ZonedId {
                    zone: None,
                    id: "vol-a".to_string()
                },
                ZonedId {
                    zone: None,
                    id: "vol-b".to_string()
                }
            ]
        );
        assert!(persisted.pending_ips.is_empty());
    }

    #[test]
    fn test_pending_volumes_round_trip_and_dedupe() {
        let mut state = OrchestratorState::new();
        let a = zoned("fr-par-1", "vol-a");
        state.set_pending_volumes(vec![a.clone(), a.clone(), zoned("fr-par-2", "vol-b")]);
        assert_eq!(state.pending_volumes(), vec![a, zoned("fr-par-2", "vol-b")]);
    }

    #[test]
    fn test_pending_ips_round_trip_and_dedupe() {
        let mut state = OrchestratorState::new();
        let a = zoned("fr-par-1", "ip-a");
        state.set_pending_ips(vec![a.clone(), a.clone(), zoned("pl-waw-1", "ip-b")]);
        assert_eq!(state.pending_ips(), vec![a, zoned("pl-waw-1", "ip-b")]);
    }

    #[test]
    fn test_pending_resources_saved_to_file() {
        let json = r#"{"runner": null}"#;
        let persisted: PersistedState = serde_json::from_str(json).unwrap();
        assert!(persisted.pending_volumes.is_empty());
        assert!(persisted.pending_ips.is_empty());
    }
}
