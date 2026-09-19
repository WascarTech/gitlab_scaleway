//! Configuration module for the GitLab Runner Orchestrator.
//!
//! Loads configuration from `config/config.toml` and provides
//! typed structs for all settings.

use serde::Deserialize;
use std::path::Path;
use thiserror::Error;
use tracing::info;

/// Errors that can occur when loading configuration.
#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("Failed to read configuration file: {0}")]
    Read(#[from] std::io::Error),

    #[error("Failed to parse configuration: {0}")]
    Parse(#[from] toml::de::Error),

    #[error("Failed to read runner configuration (runner.toml): {0}")]
    RunnerConfig(std::io::Error),
}

/// Main configuration - contains all sub-configurations.
#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub gitlab: GitLabConfig,
    pub scaleway: ScalewayConfig,
    pub runner: RunnerConfig,
}

/// GitLab-specific configuration.
#[derive(Debug, Deserialize, Clone)]
pub struct GitLabConfig {
    /// URL of the GitLab instance (e.g., "https://gitlab.example.com")
    pub url: String,
    /// Personal Access Token for API authentication
    pub token: String,
    /// Optional runner tag filter - only spin up a runner when a pending job has one of these tags.
    /// If absent, all pending jobs trigger a runner (original behavior).
    pub tag_filter: Option<Vec<String>>,
}

/// Scaleway Instances configuration.
#[derive(Debug, Deserialize, Clone)]
pub struct ScalewayConfig {
    /// Scaleway IAM API secret key
    pub token: String,
    /// Scaleway Project ID that owns the runner
    pub project_id: String,
    /// Availability Zone (e.g. "fr-par-1")
    pub zone: String,
    /// Instance type (e.g. "PRO2-XS", "PLAY2-PICO")
    pub server_type: String,
    /// Marketplace image label or local image UUID (e.g. "ubuntu_noble")
    pub image: String,
    /// Optional SSH public key injected via an AUTHORIZED_KEY tag
    pub ssh_public_key: Option<String>,
    /// Root volume size in GB (minimum 10)
    #[serde(default = "default_volume_size")]
    pub volume_size_gb: u32,
    /// Root volume type: "sbs_volume" (default) or "l_ssd"
    #[serde(default = "default_volume_type")]
    pub volume_type: String,
}

/// Default root volume size: 50 GB.
pub fn default_volume_size() -> u32 {
    50
}

/// Default root volume type: Block Storage.
pub fn default_volume_type() -> String {
    "sbs_volume".to_string()
}

/// Runner-specific configuration.
#[derive(Debug, Deserialize, Clone)]
pub struct RunnerConfig {
    /// Name of the server in Scaleway
    pub name: String,
    /// Minimum runtime in minutes before the server can be deleted
    #[serde(default = "default_min_lifetime")]
    pub min_lifetime_minutes: u32,
    /// Polling interval in seconds
    #[serde(default = "default_poll_interval")]
    pub poll_interval_seconds: u64,
    /// Whether the runner should accept untagged jobs
    #[serde(default = "default_run_untagged")]
    pub run_untagged: bool,
    /// Whether the runner should only run jobs on protected branches
    #[serde(default = "default_protected")]
    pub protected: bool,
}

/// Default value for minimum lifetime: 20 minutes
fn default_min_lifetime() -> u32 {
    20
}

/// Default value for polling interval: 30 seconds
fn default_poll_interval() -> u64 {
    30
}

/// Default: accept untagged jobs (GitLab default behavior)
fn default_run_untagged() -> bool {
    true
}

/// Default: do not restrict to protected branches only
fn default_protected() -> bool {
    false
}

impl Config {
    /// Loads configuration from the specified file.
    ///
    /// # Arguments
    /// * `path` - Path to the config.toml file
    ///
    /// # Returns
    /// The loaded configuration or an error
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        info!("Loading configuration from: {}", path.display());

        let content = std::fs::read_to_string(path)?;
        let config: Config = toml::from_str(&content)?;

        info!("Configuration loaded successfully");
        info!("  GitLab URL: {}", config.gitlab.url);
        info!(
            "  Scaleway zone: {} (type: {}, volume: {} GB {})",
            config.scaleway.zone,
            config.scaleway.server_type,
            config.scaleway.volume_size_gb,
            config.scaleway.volume_type
        );
        info!("  Runner name: {}", config.runner.name);

        Ok(config)
    }

    // NOTE: `load_default()` was removed - not needed in this context,
    // as the config path is explicitly defined in main.rs.
}

/// Loads the contents of runner.toml for cloud-init.
///
/// # Arguments
/// * `path` - Path to the runner.toml file
///
/// # Returns
/// The file contents as a string or an error
pub fn load_runner_config<P: AsRef<Path>>(path: P) -> Result<String, ConfigError> {
    let path = path.as_ref();
    info!("Loading runner configuration from: {}", path.display());

    std::fs::read_to_string(path).map_err(ConfigError::RunnerConfig)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_values() {
        assert_eq!(default_min_lifetime(), 20);
        assert_eq!(default_poll_interval(), 30);
    }

    #[test]
    fn test_scaleway_defaults() {
        let toml = r#"
token = "scw-secret"
project_id = "11111111-1111-1111-1111-111111111111"
zone = "fr-par-1"
server_type = "PRO2-XS"
image = "ubuntu_noble"
"#;
        let config: super::ScalewayConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.volume_size_gb, 50);
        assert_eq!(config.volume_type, "sbs_volume");
        assert!(config.ssh_public_key.is_none());
    }

    #[test]
    fn test_scaleway_overrides() {
        let toml = r#"
token = "scw-secret"
project_id = "11111111-1111-1111-1111-111111111111"
zone = "nl-ams-1"
server_type = "DEV1-M"
image = "ubuntu_noble"
ssh_public_key = "ssh-ed25519 AAAA user@host"
volume_size_gb = 120
volume_type = "l_ssd"
"#;
        let config: super::ScalewayConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.volume_size_gb, 120);
        assert_eq!(config.volume_type, "l_ssd");
        assert_eq!(
            config.ssh_public_key.as_deref(),
            Some("ssh-ed25519 AAAA user@host")
        );
    }
}
