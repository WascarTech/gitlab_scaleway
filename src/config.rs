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

    #[error("Invalid configuration: {0}")]
    Validation(String),
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
    /// Availability Zones in priority order (e.g. ["fr-par-1", "fr-par-2"]).
    ///
    /// The legacy single-value `zone` key is accepted as an alias. The custom
    /// deserializer accepts either a string or an array.
    #[serde(default, alias = "zone", deserialize_with = "deserialize_zones")]
    pub zones: Vec<String>,
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
    /// Block Storage IOPS class: 5000 (default) or 15000. Only applies to
    /// `sbs_volume`; ignored for `l_ssd`.
    pub volume_iops: Option<u32>,
}

/// Normalizes a configured zone list: trims entries, drops empty entries, and
/// removes duplicates while preserving priority order.
pub fn normalize_zones(raw: Vec<String>) -> Vec<String> {
    let mut zones = Vec::new();
    for zone in raw {
        let zone = zone.trim();
        if zone.is_empty() {
            continue;
        }
        if !zones.iter().any(|z| z == zone) {
            zones.push(zone.to_string());
        }
    }
    zones
}

/// Validates that at least one zone is configured.
pub fn validate_zones(zones: &[String]) -> Result<(), String> {
    if zones.is_empty() {
        return Err(
            "scaleway.zones must contain at least one zone (or set scaleway.zone)".to_string(),
        );
    }
    Ok(())
}

/// Accepts either a single zone string or an array of zones.
#[derive(Deserialize)]
#[serde(untagged)]
enum ZoneList {
    One(String),
    Many(Vec<String>),
}

fn deserialize_zones<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let raw = match ZoneList::deserialize(deserializer)? {
        ZoneList::One(zone) => vec![zone],
        ZoneList::Many(zones) => zones,
    };
    Ok(normalize_zones(raw))
}

impl ScalewayConfig {
    /// Returns the highest-priority zone, or an empty string when unset.
    ///
    /// Configuration loading rejects an empty list, so runtime callers can
    /// rely on this being a real zone.
    pub fn primary_zone(&self) -> &str {
        self.zones.first().map(String::as_str).unwrap_or_default()
    }
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
        validate_zones(&config.scaleway.zones).map_err(ConfigError::Validation)?;

        info!("Configuration loaded successfully");
        info!("  GitLab URL: {}", config.gitlab.url);
        info!(
            "  Scaleway zones (priority): {} (type: {}, volume: {} GB {}, iops: {})",
            config.scaleway.zones.join(", "),
            config.scaleway.server_type,
            config.scaleway.volume_size_gb,
            config.scaleway.volume_type,
            config
                .scaleway
                .volume_iops
                .map(|i| i.to_string())
                .unwrap_or_else(|| "default".to_string())
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
    fn test_normalize_zones_trims_dedupes_and_drops_empty() {
        let raw = vec![
            " fr-par-1 ".to_string(),
            "fr-par-2".to_string(),
            "fr-par-1".to_string(),
            "".to_string(),
        ];
        assert_eq!(
            normalize_zones(raw),
            vec!["fr-par-1".to_string(), "fr-par-2".to_string()]
        );
    }

    #[test]
    fn test_validate_zones_rejects_empty() {
        assert!(validate_zones(&[]).is_err());
    }

    #[test]
    fn test_validate_zones_accepts_zone() {
        assert!(validate_zones(&["fr-par-1".to_string()]).is_ok());
    }

    fn scaleway_toml(zone_line: &str) -> String {
        format!(
            r#"
token = "scw-secret"
project_id = "11111111-1111-1111-1111-111111111111"
{zone_line}
server_type = "PRO2-XS"
image = "ubuntu_noble"
"#
        )
    }

    #[test]
    fn test_zones_array_parses_in_priority_order() {
        let config: super::ScalewayConfig = toml::from_str(&scaleway_toml(
            r#"zones = ["fr-par-1", "fr-par-2", "pl-waw-1"]"#,
        ))
        .unwrap();
        assert_eq!(
            config.zones,
            vec![
                "fr-par-1".to_string(),
                "fr-par-2".to_string(),
                "pl-waw-1".to_string()
            ]
        );
        assert_eq!(config.primary_zone(), "fr-par-1");
    }

    #[test]
    fn test_legacy_zone_string_parses_as_single_zone() {
        let config: super::ScalewayConfig =
            toml::from_str(&scaleway_toml(r#"zone = "nl-ams-1""#)).unwrap();
        assert_eq!(config.zones, vec!["nl-ams-1".to_string()]);
    }

    #[test]
    fn test_zones_normalized_on_parse() {
        let config: super::ScalewayConfig = toml::from_str(&scaleway_toml(
            r#"zones = [" fr-par-1 ", "fr-par-2", "fr-par-1", ""]"#,
        ))
        .unwrap();
        assert_eq!(
            config.zones,
            vec!["fr-par-1".to_string(), "fr-par-2".to_string()]
        );
    }

    #[test]
    fn test_missing_zone_list_parses_empty_and_fails_validation() {
        let config: super::ScalewayConfig = toml::from_str(&scaleway_toml("")).unwrap();
        assert!(config.zones.is_empty());
        assert!(validate_zones(&config.zones).is_err());
    }

    #[test]
    fn test_zone_and_zones_together_are_rejected() {
        let toml = scaleway_toml(
            r#"zone = "fr-par-1"
zones = ["fr-par-2"]"#,
        );
        assert!(toml::from_str::<super::ScalewayConfig>(&toml).is_err());
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
        assert!(config.volume_iops.is_none());
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
volume_iops = 15000
"#;
        let config: super::ScalewayConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.volume_size_gb, 120);
        assert_eq!(config.volume_type, "l_ssd");
        assert_eq!(config.volume_iops, Some(15000));
        assert_eq!(
            config.ssh_public_key.as_deref(),
            Some("ssh-ed25519 AAAA user@host")
        );
    }
}
