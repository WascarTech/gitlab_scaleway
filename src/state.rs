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

/// State of an active runner.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunnerState {
    /// Scaleway server UUID
    pub server_id: String,
    /// Server name
    pub server_name: String,
    /// Creation timestamp
    pub created_at: DateTime<Utc>,
    /// IDs of volumes attached at creation, deleted after termination
    #[serde(default)]
    pub volume_ids: Vec<String>,
}

impl RunnerState {
    /// Creates a new runner state with current timestamp.
    pub fn new(server_id: String, server_name: String, volume_ids: Vec<String>) -> Self {
        let created_at = Utc::now();
        info!(
            "Runner state created: Server {} (ID: {})",
            server_name, server_id
        );

        Self {
            server_id,
            server_name,
            created_at,
            volume_ids,
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
}

/// Orchestrator state - manages the entire application state.
#[derive(Debug, Default)]
pub struct OrchestratorState {
    /// Current runner (if present)
    pub runner: Option<RunnerState>,
    /// Path to the state file
    state_file_path: Option<std::path::PathBuf>,
}

impl OrchestratorState {
    /// Creates a new orchestrator state without persistence (for tests only).
    #[cfg(test)]
    pub fn new() -> Self {
        Self {
            runner: None,
            state_file_path: None,
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
        Ok(())
    }

    /// Saves the state to file.
    fn save_to_file(&self) -> Result<(), StateError> {
        if let Some(ref path) = self.state_file_path {
            let persisted = PersistedState {
                runner: self.runner.clone(),
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

    #[test]
    fn test_runner_state_creation() {
        let state = RunnerState::new(
            "server-uuid".to_string(),
            "test-runner".to_string(),
            vec!["vol-1".to_string()],
        );

        assert_eq!(state.server_id, "server-uuid");
        assert_eq!(state.server_name, "test-runner");
        assert_eq!(state.volume_ids, vec!["vol-1".to_string()]);
        assert!(state.uptime_minutes() < 1);
    }

    #[test]
    fn test_orchestrator_state() {
        let mut state = OrchestratorState::new();
        assert!(!state.has_runner());

        let runner = RunnerState::new("server-uuid".to_string(), "test-runner".to_string(), vec![]);
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
            vec!["vol-1".to_string()],
        );
        let json = serde_json::to_string(&runner).unwrap();
        let restored: RunnerState = serde_json::from_str(&json).unwrap();

        assert_eq!(restored.server_id, "server-uuid");
        assert_eq!(restored.server_name, "test-runner");
        assert_eq!(restored.volume_ids, vec!["vol-1".to_string()]);
    }

    #[test]
    fn test_state_without_volume_ids_still_loads() {
        let json = r#"{
            "server_id": "server-uuid",
            "server_name": "test-runner",
            "created_at": "2026-01-14T10:30:00Z"
        }"#;
        let restored: RunnerState = serde_json::from_str(json).unwrap();
        assert_eq!(restored.server_id, "server-uuid");
        assert!(restored.volume_ids.is_empty());
    }
}
