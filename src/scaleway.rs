//! Scaleway Instances API client.
//!
//! Uses the zonal Instances v1 REST API for server lifecycle and the Block
//! Storage v1 API to remove detached volumes after termination.

use std::collections::HashMap;
use std::time::Duration;

use reqwest::Client;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::time::sleep;
use tracing::{debug, info, warn};

use crate::config::ScalewayConfig;

/// Scaleway API base URL.
const SCALEWAY_API_URL: &str = "https://api.scaleway.com";
/// Max time to wait for a server to reach the `stopped` state.
const STOPPED_TIMEOUT_SECS: u64 = 120;
/// Max time to wait for a server to reach the `running` state.
const RUNNING_TIMEOUT_SECS: u64 = 180;
/// Max time to wait for a server to be fully deleted after terminate.
const TERMINATED_TIMEOUT_SECS: u64 = 180;
/// Interval between state polls.
const STATE_POLL_INTERVAL_SECS: u64 = 3;
/// Scaleway's minimum root volume size in GB.
const MIN_VOLUME_SIZE_GB: u32 = 10;
/// Bytes in one gigabyte, as expected by the Scaleway API.
const BYTES_PER_GB: u64 = 1_000_000_000;
/// Block Storage IOPS classes accepted by the Scaleway API.
const ALLOWED_IOPS: [u32; 2] = [5_000, 15_000];
/// Attempts to apply the IOPS class before giving up.
const IOPS_UPDATE_ATTEMPTS: u32 = 3;
/// Delay between IOPS update attempts, in seconds.
const IOPS_UPDATE_DELAY_SECS: u64 = 3;

/// Errors that can occur during Scaleway API calls.
#[derive(Error, Debug)]
pub enum ScalewayError {
    #[error("HTTP request failed: {0}")]
    Request(#[from] reqwest::Error),

    #[error("Scaleway API error (status {status}): {message}")]
    Api { status: u16, message: String },

    #[error("Invalid API response: {0}")]
    Parse(String),

    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    #[error("Timed out waiting for server {server_id} to reach state '{desired}'")]
    Timeout { server_id: String, desired: String },
}

/// A Scaleway Instance.
#[derive(Debug, Deserialize, Clone)]
pub struct Server {
    /// Instance UUID
    pub id: String,
    /// Instance name
    pub name: String,
    /// State: running, stopped, starting, stopping, locked, "stopped in place"
    pub state: String,
    /// Public IPs (IPv4 and IPv6)
    #[serde(default)]
    pub public_ips: Vec<PublicIp>,
    /// Attached volumes, keyed by volume index ("0" is the root volume)
    #[serde(default)]
    pub volumes: HashMap<String, Volume>,
}

/// A public IP attached to an Instance.
#[derive(Debug, Deserialize, Clone)]
pub struct PublicIp {
    pub address: String,
    pub family: String,
}

/// A volume attached to an Instance.
#[derive(Debug, Deserialize, Clone)]
pub struct Volume {
    pub id: String,
}

#[derive(Debug, Deserialize)]
struct CreateServerResponse {
    server: Server,
}

#[derive(Debug, Deserialize)]
struct ServersResponse {
    servers: Vec<Server>,
}

#[derive(Debug, Deserialize)]
struct ServerResponse {
    server: Server,
}

#[derive(Debug, Serialize)]
struct CreateServerRequest {
    name: String,
    project: String,
    commercial_type: String,
    image: String,
    dynamic_ip_required: bool,
    tags: Vec<String>,
    volumes: HashMap<String, VolumeTemplate>,
}

#[derive(Debug, Serialize)]
struct VolumeTemplate {
    size: u64,
    volume_type: String,
    boot: bool,
}

#[derive(Debug, Serialize)]
struct UpdateVolumeIopsRequest {
    perf_iops: u32,
}

#[derive(Debug, Serialize)]
struct ActionRequest<'a> {
    action: &'a str,
}

/// Returns only the server whose name matches exactly.
///
/// The Scaleway `name` query filter is a prefix match, so this re-filter is
/// required to avoid adopting a differently named server.
fn exact_name_match(servers: Vec<Server>, name: &str) -> Option<Server> {
    servers.into_iter().find(|s| s.name == name)
}

/// Builds the `AUTHORIZED_KEY` tag Scaleway expects for per-instance SSH keys.
/// Scaleway requires spaces to be replaced with underscores.
fn authorized_key_tag(ssh_public_key: &str) -> String {
    format!("AUTHORIZED_KEY={}", ssh_public_key.replace(' ', "_"))
}

/// Converts a size in GB to bytes.
fn volume_size_bytes(size_gb: u32) -> u64 {
    size_gb as u64 * BYTES_PER_GB
}

/// Validates volume settings before calling the API.
fn validate_volume_config(size_gb: u32, volume_type: &str) -> Result<(), ScalewayError> {
    if size_gb < MIN_VOLUME_SIZE_GB {
        return Err(ScalewayError::InvalidConfig(format!(
            "volume_size_gb must be at least {}, got {}",
            MIN_VOLUME_SIZE_GB, size_gb
        )));
    }
    if volume_type != "sbs_volume" && volume_type != "l_ssd" {
        return Err(ScalewayError::InvalidConfig(format!(
            "volume_type must be 'sbs_volume' or 'l_ssd', got '{}'",
            volume_type
        )));
    }
    Ok(())
}

/// Validates the configured IOPS class, if any.
fn validate_volume_iops(iops: Option<u32>) -> Result<(), ScalewayError> {
    if let Some(iops) = iops {
        if !ALLOWED_IOPS.contains(&iops) {
            return Err(ScalewayError::InvalidConfig(format!(
                "volume_iops must be 5000 or 15000, got {}",
                iops
            )));
        }
    }
    Ok(())
}

/// Decides which IOPS class to apply to the root volume.
///
/// The IOPS class only exists for Block Storage (`sbs_volume`); it is ignored
/// for local SSD (`l_ssd`).
fn iops_to_apply(volume_type: &str, volume_iops: Option<u32>) -> Option<u32> {
    match (volume_type, volume_iops) {
        (_, None) => None,
        ("sbs_volume", Some(iops)) => Some(iops),
        _ => None,
    }
}

/// Extracts volume IDs from a server, sorted by volume key for determinism.
pub fn server_volume_ids(server: &Server) -> Vec<String> {
    let mut entries: Vec<(String, String)> = server
        .volumes
        .iter()
        .map(|(key, volume)| (key.clone(), volume.id.clone()))
        .collect();
    entries.sort();
    entries.into_iter().map(|(_, id)| id).collect()
}

/// Builds the create-server request body.
fn build_create_request(
    config: &ScalewayConfig,
    name: &str,
) -> Result<CreateServerRequest, ScalewayError> {
    validate_volume_config(config.volume_size_gb, &config.volume_type)?;
    validate_volume_iops(config.volume_iops)?;

    let mut tags = vec!["gitlab-runner".to_string()];
    if let Some(ref key) = config.ssh_public_key {
        tags.push(authorized_key_tag(key));
    }

    let mut volumes = HashMap::new();
    volumes.insert(
        "0".to_string(),
        VolumeTemplate {
            size: volume_size_bytes(config.volume_size_gb),
            volume_type: config.volume_type.clone(),
            boot: true,
        },
    );

    Ok(CreateServerRequest {
        name: name.to_string(),
        project: config.project_id.clone(),
        commercial_type: config.server_type.clone(),
        image: config.image.clone(),
        dynamic_ip_required: true,
        tags,
        volumes,
    })
}

/// Scaleway API client.
pub struct ScalewayClient {
    client: Client,
    token: String,
    config: ScalewayConfig,
}

impl ScalewayClient {
    /// Creates a new Scaleway client.
    pub fn new(config: &ScalewayConfig) -> Self {
        info!("Scaleway client initialized");
        info!("  Zone: {}", config.zone);
        info!("  Server type: {}", config.server_type);
        info!("  Image: {}", config.image);
        info!(
            "  Volume: {} GB ({})",
            config.volume_size_gb, config.volume_type
        );
        if let Some(iops) = iops_to_apply(&config.volume_type, config.volume_iops) {
            info!("  Volume IOPS: {}", iops);
        }

        Self {
            client: Client::new(),
            token: config.token.clone(),
            config: config.clone(),
        }
    }

    fn zone_url(&self, path: &str) -> String {
        format!(
            "{}/instance/v1/zones/{}{}",
            SCALEWAY_API_URL, self.config.zone, path
        )
    }

    async fn get<T: for<'de> Deserialize<'de>>(&self, path: &str) -> Result<T, ScalewayError> {
        let url = self.zone_url(path);
        debug!("Scaleway API GET: {}", url);
        let response = self
            .client
            .get(&url)
            .header("X-Auth-Token", &self.token)
            .send()
            .await?;
        Self::handle_json(response).await
    }

    async fn post<T: for<'de> Deserialize<'de>, B: Serialize>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<T, ScalewayError> {
        let url = self.zone_url(path);
        debug!("Scaleway API POST: {}", url);
        let response = self
            .client
            .post(&url)
            .header("X-Auth-Token", &self.token)
            .json(body)
            .send()
            .await?;
        Self::handle_json(response).await
    }

    async fn patch_text(&self, path: &str, body: &str) -> Result<(), ScalewayError> {
        let url = self.zone_url(path);
        debug!("Scaleway API PATCH: {}", url);
        let response = self
            .client
            .patch(&url)
            .header("X-Auth-Token", &self.token)
            .header("Content-Type", "text/plain")
            .body(body.to_string())
            .send()
            .await?;
        Self::handle_empty(response).await
    }

    async fn patch_json<B: Serialize>(&self, url: &str, body: &B) -> Result<(), ScalewayError> {
        debug!("Scaleway API PATCH: {}", url);
        let response = self
            .client
            .patch(url)
            .header("X-Auth-Token", &self.token)
            .json(body)
            .send()
            .await?;
        Self::handle_empty(response).await
    }

    async fn delete_url(&self, url: &str) -> Result<(), ScalewayError> {
        debug!("Scaleway API DELETE: {}", url);
        let response = self
            .client
            .delete(url)
            .header("X-Auth-Token", &self.token)
            .send()
            .await?;
        Self::handle_empty(response).await
    }

    async fn handle_json<T: for<'de> Deserialize<'de>>(
        response: reqwest::Response,
    ) -> Result<T, ScalewayError> {
        let status = response.status();
        if !status.is_success() {
            let message = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            return Err(ScalewayError::Api {
                status: status.as_u16(),
                message,
            });
        }
        response
            .json::<T>()
            .await
            .map_err(|e| ScalewayError::Parse(format!("JSON parsing failed: {}", e)))
    }

    async fn handle_empty(response: reqwest::Response) -> Result<(), ScalewayError> {
        let status = response.status();
        if !status.is_success() {
            let message = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            return Err(ScalewayError::Api {
                status: status.as_u16(),
                message,
            });
        }
        Ok(())
    }

    /// Finds a server by exact name.
    pub async fn find_server_by_name(&self, name: &str) -> Result<Option<Server>, ScalewayError> {
        let response: ServersResponse = self.get(&format!("/servers?name={}", name)).await?;
        Ok(exact_name_match(response.servers, name))
    }

    /// Fetches a server by ID; returns `None` on 404.
    pub async fn get_server(&self, server_id: &str) -> Result<Option<Server>, ScalewayError> {
        let result: Result<ServerResponse, _> = self.get(&format!("/servers/{}", server_id)).await;
        match result {
            Ok(response) => Ok(Some(response.server)),
            Err(ScalewayError::Api { status: 404, .. }) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Creates a server, applies cloud-init, and powers it on.
    ///
    /// Scaleway creates the Instance in the `stopped` state, so the requested
    /// volume can be booted with the user-data in place.
    pub async fn create_server(
        &self,
        name: &str,
        cloud_init: &str,
    ) -> Result<Server, ScalewayError> {
        info!("Creating server: {}", name);

        let request = build_create_request(&self.config, name)?;
        let response: CreateServerResponse = self.post("/servers", &request).await?;
        let server = response.server;
        info!(
            "Server created: {} (ID: {}, state: {})",
            server.name, server.id, server.state
        );

        self.wait_for_state(&server.id, "stopped", STOPPED_TIMEOUT_SECS)
            .await?;
        self.apply_root_volume_iops(&server).await;
        self.patch_text(
            &format!("/servers/{}/user_data/cloud-init", server.id),
            cloud_init,
        )
        .await?;
        self.action(&server.id, "poweron").await?;
        let server = self
            .wait_for_state(&server.id, "running", RUNNING_TIMEOUT_SECS)
            .await?;

        if let Some(ip) = server.public_ips.iter().find(|ip| ip.family == "inet") {
            info!("  IPv4: {}", ip.address);
        }

        Ok(server)
    }

    /// Applies the configured IOPS class to the server's root Block volume.
    ///
    /// The Instance API cannot set IOPS at creation time, so the class is set
    /// afterwards via the Block Storage API. The volume is always created with
    /// the `sbs` storage class, so switching between 5000 and 15000 IOPS is
    /// permitted. Failures are logged but do not abort provisioning: the runner
    /// remains usable at the default 5000 IOPS.
    async fn apply_root_volume_iops(&self, server: &Server) {
        let iops = match iops_to_apply(&self.config.volume_type, self.config.volume_iops) {
            Some(iops) => iops,
            None => {
                if self.config.volume_iops.is_some() && self.config.volume_type == "l_ssd" {
                    warn!(
                        "volume_iops is ignored for volume_type 'l_ssd' (local SSD has no IOPS class)"
                    );
                }
                return;
            }
        };

        let volume_id = match server.volumes.get("0") {
            Some(volume) => &volume.id,
            None => {
                warn!(
                    "Could not find root volume of server {} to set IOPS {}",
                    server.id, iops
                );
                return;
            }
        };

        info!("Setting root volume {} IOPS to {}", volume_id, iops);
        for attempt in 1..=IOPS_UPDATE_ATTEMPTS {
            match self.update_volume_iops(volume_id, iops).await {
                Ok(()) => {
                    info!("Root volume {} now at {} IOPS", volume_id, iops);
                    return;
                }
                Err(e) => {
                    warn!(
                        "Failed to set IOPS {} on volume {} (attempt {}/{}): {}",
                        iops, volume_id, attempt, IOPS_UPDATE_ATTEMPTS, e
                    );
                    if attempt < IOPS_UPDATE_ATTEMPTS {
                        sleep(Duration::from_secs(IOPS_UPDATE_DELAY_SECS)).await;
                    }
                }
            }
        }
        warn!(
            "Continuing without custom IOPS - root volume {} remains at the default class",
            volume_id
        );
    }

    /// Updates a Block volume's IOPS class via the Block Storage API.
    async fn update_volume_iops(&self, volume_id: &str, iops: u32) -> Result<(), ScalewayError> {
        let url = format!(
            "{}/block/v1/zones/{}/volumes/{}",
            SCALEWAY_API_URL, self.config.zone, volume_id
        );
        self.patch_json(&url, &UpdateVolumeIopsRequest { perf_iops: iops })
            .await
    }

    async fn action(&self, server_id: &str, action: &str) -> Result<(), ScalewayError> {
        info!("Server {} action: {}", server_id, action);
        let response = self
            .client
            .post(self.zone_url(&format!("/servers/{}/action", server_id)))
            .header("X-Auth-Token", &self.token)
            .json(&ActionRequest { action })
            .send()
            .await?;
        Self::handle_empty(response).await
    }

    /// Terminates a server, falling back to poweroff+delete if needed.
    ///
    /// Waits until the server no longer exists so that its Block Storage
    /// volumes are detached and can be deleted by the caller.
    pub async fn terminate_server(&self, server_id: &str) -> Result<(), ScalewayError> {
        info!("Terminating server: {}", server_id);
        match self.action(server_id, "terminate").await {
            Ok(()) => {}
            Err(e) => {
                warn!("Terminate failed for {} ({}); falling back", server_id, e);
                self.terminate_fallback(server_id).await?;
            }
        }
        self.wait_for_server_gone(server_id).await?;
        info!("Server {} terminated", server_id);
        Ok(())
    }

    async fn terminate_fallback(&self, server_id: &str) -> Result<(), ScalewayError> {
        match self.get_server(server_id).await? {
            None => Ok(()),
            Some(server) if server.state == "stopped" => self.delete_server(server_id).await,
            Some(_) => {
                self.action(server_id, "poweroff").await?;
                self.wait_for_state(server_id, "stopped", STOPPED_TIMEOUT_SECS)
                    .await?;
                self.delete_server(server_id).await
            }
        }
    }

    async fn delete_server(&self, server_id: &str) -> Result<(), ScalewayError> {
        let url = self.zone_url(&format!("/servers/{}", server_id));
        match self.delete_url(&url).await {
            Ok(()) => Ok(()),
            Err(ScalewayError::Api { status: 404, .. }) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Deletes volumes via the Block Storage API. 404s are ignored because
    /// `terminate` already removes local (`l_ssd`/`scratch`) volumes.
    pub async fn delete_volumes(&self, volume_ids: &[String]) {
        for volume_id in volume_ids {
            let url = format!(
                "{}/block/v1/zones/{}/volumes/{}",
                SCALEWAY_API_URL, self.config.zone, volume_id
            );
            match self.delete_url(&url).await {
                Ok(()) => info!("Deleted volume {}", volume_id),
                Err(ScalewayError::Api { status: 404, .. }) => {
                    debug!("Volume {} already deleted", volume_id);
                }
                Err(e) => warn!("Failed to delete volume {}: {}", volume_id, e),
            }
        }
    }

    /// Polls until the server reaches the desired state or the timeout elapses.
    pub async fn wait_for_state(
        &self,
        server_id: &str,
        desired: &str,
        timeout_secs: u64,
    ) -> Result<Server, ScalewayError> {
        let attempts = (timeout_secs / STATE_POLL_INTERVAL_SECS).max(1);
        for _ in 0..attempts {
            match self.get_server(server_id).await? {
                Some(server) if server.state == desired => return Ok(server),
                Some(_) => sleep(Duration::from_secs(STATE_POLL_INTERVAL_SECS)).await,
                None => {
                    return Err(ScalewayError::Parse(format!(
                        "Server {} disappeared while waiting for '{}'",
                        server_id, desired
                    )));
                }
            }
        }
        Err(ScalewayError::Timeout {
            server_id: server_id.to_string(),
            desired: desired.to_string(),
        })
    }

    /// Polls until the server no longer exists (termination complete).
    pub async fn wait_for_server_gone(&self, server_id: &str) -> Result<(), ScalewayError> {
        let attempts = (TERMINATED_TIMEOUT_SECS / STATE_POLL_INTERVAL_SECS).max(1);
        for _ in 0..attempts {
            if self.get_server(server_id).await?.is_none() {
                return Ok(());
            }
            sleep(Duration::from_secs(STATE_POLL_INTERVAL_SECS)).await;
        }
        Err(ScalewayError::Timeout {
            server_id: server_id.to_string(),
            desired: "deleted".to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn server(id: &str, name: &str) -> Server {
        Server {
            id: id.to_string(),
            name: name.to_string(),
            state: "stopped".to_string(),
            public_ips: vec![],
            volumes: HashMap::new(),
        }
    }

    #[test]
    fn test_exact_name_match_rejects_prefix() {
        let servers = vec![server("id-1", "runner-2"), server("id-2", "runner")];
        let matched = exact_name_match(servers, "runner").unwrap();
        assert_eq!(matched.id, "id-2");
    }

    #[test]
    fn test_exact_name_match_none() {
        let servers = vec![server("id-1", "runner-2")];
        assert!(exact_name_match(servers, "runner").is_none());
    }

    #[test]
    fn test_authorized_key_tag_replaces_spaces() {
        assert_eq!(
            authorized_key_tag("ssh-ed25519 AAAA key user@host"),
            "AUTHORIZED_KEY=ssh-ed25519_AAAA_key_user@host"
        );
    }

    #[test]
    fn test_volume_size_bytes() {
        assert_eq!(volume_size_bytes(50), 50_000_000_000);
        assert_eq!(volume_size_bytes(50) % 512, 0);
    }

    #[test]
    fn test_validate_volume_config_ok() {
        assert!(validate_volume_config(10, "sbs_volume").is_ok());
        assert!(validate_volume_config(50, "l_ssd").is_ok());
    }

    #[test]
    fn test_validate_volume_config_rejects_small_size() {
        assert!(matches!(
            validate_volume_config(9, "sbs_volume"),
            Err(ScalewayError::InvalidConfig(_))
        ));
    }

    #[test]
    fn test_validate_volume_config_rejects_bad_type() {
        assert!(matches!(
            validate_volume_config(50, "b_ssd"),
            Err(ScalewayError::InvalidConfig(_))
        ));
    }

    #[test]
    fn test_build_create_request_includes_key_tag_and_volume() {
        let config = ScalewayConfig {
            token: "t".to_string(),
            project_id: "p".to_string(),
            zone: "fr-par-1".to_string(),
            server_type: "PRO2-XS".to_string(),
            image: "ubuntu_noble".to_string(),
            ssh_public_key: Some("ssh-ed25519 AAAA key".to_string()),
            volume_size_gb: 50,
            volume_type: "sbs_volume".to_string(),
            volume_iops: None,
        };
        let request = build_create_request(&config, "flexi-runner").unwrap();
        assert_eq!(request.name, "flexi-runner");
        assert_eq!(request.project, "p");
        assert!(request
            .tags
            .contains(&"AUTHORIZED_KEY=ssh-ed25519_AAAA_key".to_string()));
        let volume = request.volumes.get("0").unwrap();
        assert_eq!(volume.size, 50_000_000_000);
        assert_eq!(volume.volume_type, "sbs_volume");
        assert!(volume.boot);
    }

    #[test]
    fn test_build_create_request_without_key() {
        let config = ScalewayConfig {
            token: "t".to_string(),
            project_id: "p".to_string(),
            zone: "fr-par-1".to_string(),
            server_type: "PRO2-XS".to_string(),
            image: "ubuntu_noble".to_string(),
            ssh_public_key: None,
            volume_size_gb: 50,
            volume_type: "sbs_volume".to_string(),
            volume_iops: None,
        };
        let request = build_create_request(&config, "flexi-runner").unwrap();
        assert_eq!(request.tags, vec!["gitlab-runner".to_string()]);
    }

    #[test]
    fn test_server_volume_ids_sorted() {
        let mut volumes = HashMap::new();
        volumes.insert(
            "1".to_string(),
            Volume {
                id: "vol-b".to_string(),
            },
        );
        volumes.insert(
            "0".to_string(),
            Volume {
                id: "vol-a".to_string(),
            },
        );
        let mut s = server("id-1", "runner");
        s.volumes = volumes;
        assert_eq!(server_volume_ids(&s), vec!["vol-a", "vol-b"]);
    }

    #[test]
    fn test_validate_volume_iops() {
        assert!(validate_volume_iops(None).is_ok());
        assert!(validate_volume_iops(Some(5_000)).is_ok());
        assert!(validate_volume_iops(Some(15_000)).is_ok());
        assert!(matches!(
            validate_volume_iops(Some(10_000)),
            Err(ScalewayError::InvalidConfig(_))
        ));
    }

    #[test]
    fn test_iops_to_apply() {
        assert_eq!(iops_to_apply("sbs_volume", None), None);
        assert_eq!(iops_to_apply("sbs_volume", Some(15_000)), Some(15_000));
        assert_eq!(iops_to_apply("l_ssd", Some(15_000)), None);
        assert_eq!(iops_to_apply("l_ssd", None), None);
    }
}
