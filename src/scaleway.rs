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
use crate::state::ZonedId;

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
/// Max time to wait for a volume to be detached after server termination.
const VOLUME_DETACH_TIMEOUT_SECS: u64 = 60;
/// Attempts to delete a volume before giving up.
const VOLUME_DELETE_ATTEMPTS: u32 = 3;
/// Delay between volume deletion attempts, in seconds.
const VOLUME_DELETE_DELAY_SECS: u64 = 3;

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

    #[error("Timed out waiting for volume {volume_id} to detach")]
    VolumeTimeout { volume_id: String },
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

/// How a failed creation attempt should be treated.
#[derive(Debug, PartialEq, Eq)]
pub enum CreationFailure {
    /// This zone cannot provide the runner; try the next one.
    NextZone,
    /// The failure is not zone-specific (auth, rate limit, bad config, network).
    Abort,
}

/// A public IP attached to an Instance.
#[derive(Debug, Deserialize, Clone)]
pub struct PublicIp {
    #[serde(default)]
    pub id: String,
    pub address: String,
    pub family: String,
    /// Reserved flexible IPs are owned by us; dynamic ones die with the server.
    #[serde(default = "default_dynamic")]
    pub dynamic: bool,
    #[serde(default)]
    pub tags: Vec<String>,
}

fn default_dynamic() -> bool {
    true
}

/// A reserved flexible IPv4.
#[derive(Debug, Deserialize, Clone)]
pub struct Ip {
    pub id: String,
    pub address: String,
}

#[derive(Debug, Serialize)]
struct CreateIpRequest {
    project: String,
    tags: Vec<String>,
    #[serde(rename = "type")]
    ip_type: String,
}

#[derive(Debug, Deserialize)]
struct IpResponse {
    ip: Ip,
}

/// A volume attached to an Instance.
#[derive(Debug, Deserialize, Clone)]
pub struct Volume {
    pub id: String,
}

/// Block Storage volume status, as returned by the Block API.
#[derive(Debug, Deserialize)]
struct BlockVolume {
    #[serde(default, alias = "state")]
    status: String,
}

/// `GET /block/v1/.../volumes/{id}` returns the object directly.
/// The wrapper variant is accepted defensively and is tried first.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum BlockVolumeGetResponse {
    Wrapped { volume: BlockVolume },
    Direct(BlockVolume),
}

impl BlockVolumeGetResponse {
    fn status(&self) -> &str {
        match self {
            Self::Wrapped { volume } => &volume.status,
            Self::Direct(volume) => &volume.status,
        }
    }
}

/// Returns true when a Block volume is detached and safe to delete.
fn block_volume_is_deletable(status: &str) -> bool {
    status == "available"
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
    public_ips: Vec<String>,
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

/// Builds a zonal Instance API URL.
fn zone_url(zone: &str, path: &str) -> String {
    format!("{}/instance/v1/zones/{}{}", SCALEWAY_API_URL, zone, path)
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

/// Returns true when the server has a non-empty IPv4 address attached.
pub fn server_has_public_ipv4(server: &Server) -> bool {
    server
        .public_ips
        .iter()
        .any(|ip| ip.family == "inet" && !ip.address.is_empty())
}

/// Returns reserved IP IDs owned by the orchestrator.
pub fn server_owned_ip_ids(server: &Server) -> Vec<String> {
    server
        .public_ips
        .iter()
        .filter(|ip| !ip.dynamic && ip.tags.iter().any(|tag| tag == "gitlab-runner"))
        .map(|ip| ip.id.clone())
        .collect()
}

/// Decides whether the next provisioning attempt should use another zone.
pub fn classify_creation_failure(error: &ScalewayError) -> CreationFailure {
    match error {
        ScalewayError::Api {
            status: 400 | 404 | 408 | 409 | 500 | 502 | 503 | 504,
            ..
        } => CreationFailure::NextZone,
        _ => CreationFailure::Abort,
    }
}

/// Builds the request that reserves a flexible IPv4 in a zone.
fn build_create_ip_request(config: &ScalewayConfig) -> CreateIpRequest {
    CreateIpRequest {
        project: config.project_id.clone(),
        tags: vec!["gitlab-runner".to_string()],
        ip_type: "routed_ipv4".to_string(),
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

/// Validates the provisioning settings that are common to every zone.
pub fn validate_config(config: &ScalewayConfig) -> Result<(), ScalewayError> {
    validate_volume_config(config.volume_size_gb, &config.volume_type)?;
    validate_volume_iops(config.volume_iops)
}

/// Builds the create-server request body.
fn build_create_request(
    config: &ScalewayConfig,
    name: &str,
    ip_id: &str,
) -> Result<CreateServerRequest, ScalewayError> {
    validate_config(config)?;

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
        dynamic_ip_required: false,
        public_ips: vec![ip_id.to_string()],
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
        info!("  Zones (priority): {}", config.zones.join(", "));
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

    async fn get<T: for<'de> Deserialize<'de>>(
        &self,
        zone: &str,
        path: &str,
    ) -> Result<T, ScalewayError> {
        let url = zone_url(zone, path);
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
        zone: &str,
        path: &str,
        body: &B,
    ) -> Result<T, ScalewayError> {
        let url = zone_url(zone, path);
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

    async fn get_url<T: for<'de> Deserialize<'de>>(&self, url: &str) -> Result<T, ScalewayError> {
        debug!("Scaleway API GET: {}", url);
        let response = self
            .client
            .get(url)
            .header("X-Auth-Token", &self.token)
            .send()
            .await?;
        Self::handle_json(response).await
    }

    async fn patch_text(&self, zone: &str, path: &str, body: &str) -> Result<(), ScalewayError> {
        let url = zone_url(zone, path);
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

    /// Finds a server by exact name in a zone.
    pub async fn find_server_by_name(
        &self,
        zone: &str,
        name: &str,
    ) -> Result<Option<Server>, ScalewayError> {
        let response: ServersResponse = self.get(zone, &format!("/servers?name={}", name)).await?;
        Ok(exact_name_match(response.servers, name))
    }

    /// Fetches a server by ID; returns `None` on 404.
    pub async fn get_server(
        &self,
        zone: &str,
        server_id: &str,
    ) -> Result<Option<Server>, ScalewayError> {
        let result: Result<ServerResponse, _> =
            self.get(zone, &format!("/servers/{}", server_id)).await;
        match result {
            Ok(response) => Ok(Some(response.server)),
            Err(ScalewayError::Api { status: 404, .. }) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Reserves a flexible IPv4 in a zone.
    ///
    /// Doing this before creating the Instance means a zone without addresses
    /// is skipped without creating (and billing) an unusable server.
    pub async fn create_ip(&self, zone: &str) -> Result<Ip, ScalewayError> {
        let request = build_create_ip_request(&self.config);
        let response: IpResponse = self.post(zone, "/ips", &request).await?;
        info!(
            "Reserved IPv4 {} in zone {} (ID: {})",
            response.ip.address, zone, response.ip.id
        );
        Ok(response.ip)
    }

    /// Creates a server in the `stopped` state with a reserved IP attached.
    ///
    /// Scaleway creates the Instance in the `stopped` state, so the requested
    /// volume can be booted with the user-data in place. The caller owns the
    /// returned server and is responsible for cleanup if a later step fails.
    pub async fn create_server_stopped(
        &self,
        zone: &str,
        name: &str,
        ip_id: &str,
    ) -> Result<Server, ScalewayError> {
        info!("Creating server in zone {}: {}", zone, name);

        let request = build_create_request(&self.config, name, ip_id)?;
        let response: CreateServerResponse = self.post(zone, "/servers", &request).await?;
        let server = response.server;
        info!(
            "Server created: {} (ID: {}, zone: {}, state: {})",
            server.name, server.id, zone, server.state
        );

        Ok(server)
    }

    /// Waits for the stopped server, applies cloud-init, and powers it on.
    pub async fn prepare_server(
        &self,
        zone: &str,
        server: &Server,
        cloud_init: &str,
    ) -> Result<Server, ScalewayError> {
        self.wait_for_state(zone, &server.id, "stopped", STOPPED_TIMEOUT_SECS)
            .await?;
        self.apply_root_volume_iops(zone, server).await;
        self.patch_text(
            zone,
            &format!("/servers/{}/user_data/cloud-init", server.id),
            cloud_init,
        )
        .await?;
        self.action(zone, &server.id, "poweron").await?;
        let server = self
            .wait_for_state(zone, &server.id, "running", RUNNING_TIMEOUT_SECS)
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
    async fn apply_root_volume_iops(&self, zone: &str, server: &Server) {
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
            match self.update_volume_iops(zone, volume_id, iops).await {
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
    async fn update_volume_iops(
        &self,
        zone: &str,
        volume_id: &str,
        iops: u32,
    ) -> Result<(), ScalewayError> {
        let url = format!(
            "{}/block/v1/zones/{}/volumes/{}",
            SCALEWAY_API_URL, zone, volume_id
        );
        self.patch_json(&url, &UpdateVolumeIopsRequest { perf_iops: iops })
            .await
    }

    async fn action(&self, zone: &str, server_id: &str, action: &str) -> Result<(), ScalewayError> {
        info!("Server {} action: {}", server_id, action);
        let response = self
            .client
            .post(zone_url(zone, &format!("/servers/{}/action", server_id)))
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
    pub async fn terminate_server(&self, zone: &str, server_id: &str) -> Result<(), ScalewayError> {
        info!("Terminating server {} in zone {}", server_id, zone);
        match self.action(zone, server_id, "terminate").await {
            Ok(()) => {}
            Err(e) => {
                warn!("Terminate failed for {} ({}); falling back", server_id, e);
                self.terminate_fallback(zone, server_id).await?;
            }
        }
        self.wait_for_server_gone(zone, server_id).await?;
        info!("Server {} terminated", server_id);
        Ok(())
    }

    async fn terminate_fallback(&self, zone: &str, server_id: &str) -> Result<(), ScalewayError> {
        match self.get_server(zone, server_id).await? {
            None => Ok(()),
            Some(server) if server.state == "stopped" => self.delete_server(zone, server_id).await,
            Some(_) => {
                self.action(zone, server_id, "poweroff").await?;
                self.wait_for_state(zone, server_id, "stopped", STOPPED_TIMEOUT_SECS)
                    .await?;
                self.delete_server(zone, server_id).await
            }
        }
    }

    async fn delete_server(&self, zone: &str, server_id: &str) -> Result<(), ScalewayError> {
        let url = zone_url(zone, &format!("/servers/{}", server_id));
        match self.delete_url(&url).await {
            Ok(()) => Ok(()),
            Err(ScalewayError::Api { status: 404, .. }) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Deletes volumes via the Block Storage API.
    ///
    /// Waits for each volume to be detached before deleting. 404s count as
    /// success (`terminate` already removed local volumes). Returns the entries
    /// that could not be deleted so callers can persist them for retry.
    pub async fn delete_volumes(&self, volumes: &[ZonedId]) -> Vec<ZonedId> {
        let mut failed = Vec::new();
        for volume in volumes {
            let result = match &volume.zone {
                Some(zone) => self.delete_volume(zone, &volume.id).await,
                None => self.delete_volume_unknown_zone(&volume.id).await,
            };
            if let Err(e) = result {
                warn!("Failed to delete volume {}: {}", volume.id, e);
                failed.push(volume.clone());
            }
        }
        failed
    }

    /// Deletes a volume whose zone is unknown by finding it across zones.
    async fn delete_volume_unknown_zone(&self, volume_id: &str) -> Result<(), ScalewayError> {
        for zone in &self.config.zones {
            if self.get_block_volume(zone, volume_id).await?.is_some() {
                return self.delete_volume(zone, volume_id).await;
            }
        }
        Ok(())
    }

    async fn get_block_volume(
        &self,
        zone: &str,
        volume_id: &str,
    ) -> Result<Option<BlockVolumeGetResponse>, ScalewayError> {
        let url = format!(
            "{}/block/v1/zones/{}/volumes/{}",
            SCALEWAY_API_URL, zone, volume_id
        );
        match self.get_url::<BlockVolumeGetResponse>(&url).await {
            Ok(volume) => Ok(Some(volume)),
            Err(ScalewayError::Api { status: 404, .. }) => Ok(None),
            Err(e) => Err(e),
        }
    }

    async fn wait_for_volume_detached(
        &self,
        zone: &str,
        volume_id: &str,
    ) -> Result<(), ScalewayError> {
        let attempts = (VOLUME_DETACH_TIMEOUT_SECS / STATE_POLL_INTERVAL_SECS).max(1);
        for _ in 0..attempts {
            match self.get_block_volume(zone, volume_id).await? {
                None => return Ok(()),
                Some(volume) if block_volume_is_deletable(volume.status()) => return Ok(()),
                Some(_) => sleep(Duration::from_secs(STATE_POLL_INTERVAL_SECS)).await,
            }
        }
        Err(ScalewayError::VolumeTimeout {
            volume_id: volume_id.to_string(),
        })
    }

    async fn delete_volume(&self, zone: &str, volume_id: &str) -> Result<(), ScalewayError> {
        self.wait_for_volume_detached(zone, volume_id).await?;
        for attempt in 1..=VOLUME_DELETE_ATTEMPTS {
            let url = format!(
                "{}/block/v1/zones/{}/volumes/{}",
                SCALEWAY_API_URL, zone, volume_id
            );
            match self.delete_url(&url).await {
                Ok(()) => {
                    info!("Deleted volume {}", volume_id);
                    return Ok(());
                }
                Err(ScalewayError::Api { status: 404, .. }) => {
                    debug!("Volume {} already deleted", volume_id);
                    return Ok(());
                }
                Err(e) if attempt == VOLUME_DELETE_ATTEMPTS => return Err(e),
                Err(e) => {
                    warn!(
                        "Delete volume {} failed (attempt {}/{}): {}",
                        volume_id, attempt, VOLUME_DELETE_ATTEMPTS, e
                    );
                    sleep(Duration::from_secs(VOLUME_DELETE_DELAY_SECS)).await;
                }
            }
        }
        unreachable!("loop always returns on the final attempt")
    }

    /// Releases a reserved IPv4. A 404 means it is already gone.
    async fn delete_ip(&self, zone: &str, ip_id: &str) -> Result<(), ScalewayError> {
        let url = zone_url(zone, &format!("/ips/{}", ip_id));
        match self.delete_url(&url).await {
            Ok(()) => {
                info!("Released IPv4 {}", ip_id);
                Ok(())
            }
            Err(ScalewayError::Api { status: 404, .. }) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Deletes reserved IPs and returns the entries that failed.
    pub async fn delete_ips(&self, ips: &[ZonedId]) -> Vec<ZonedId> {
        let mut failed = Vec::new();
        for ip in ips {
            let result = match &ip.zone {
                Some(zone) => self.delete_ip(zone, &ip.id).await,
                None => self.delete_ip_unknown_zone(&ip.id).await,
            };
            if let Err(e) = result {
                warn!("Failed to release IP {}: {}", ip.id, e);
                failed.push(ip.clone());
            }
        }
        failed
    }

    /// Deletes an IP whose zone is unknown by trying every configured zone.
    async fn delete_ip_unknown_zone(&self, ip_id: &str) -> Result<(), ScalewayError> {
        for zone in &self.config.zones {
            let url = zone_url(zone, &format!("/ips/{}", ip_id));
            match self.delete_url(&url).await {
                Ok(()) => {
                    info!("Released IPv4 {}", ip_id);
                    return Ok(());
                }
                Err(ScalewayError::Api { status: 404, .. }) => continue,
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    /// Polls until the server reaches the desired state or the timeout elapses.
    pub async fn wait_for_state(
        &self,
        zone: &str,
        server_id: &str,
        desired: &str,
        timeout_secs: u64,
    ) -> Result<Server, ScalewayError> {
        let attempts = (timeout_secs / STATE_POLL_INTERVAL_SECS).max(1);
        for _ in 0..attempts {
            match self.get_server(zone, server_id).await? {
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
    pub async fn wait_for_server_gone(
        &self,
        zone: &str,
        server_id: &str,
    ) -> Result<(), ScalewayError> {
        let attempts = (TERMINATED_TIMEOUT_SECS / STATE_POLL_INTERVAL_SECS).max(1);
        for _ in 0..attempts {
            if self.get_server(zone, server_id).await?.is_none() {
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

    fn test_config() -> ScalewayConfig {
        ScalewayConfig {
            token: "t".to_string(),
            project_id: "p".to_string(),
            zones: vec!["fr-par-1".to_string()],
            server_type: "PRO2-XS".to_string(),
            image: "ubuntu_noble".to_string(),
            ssh_public_key: None,
            volume_size_gb: 50,
            volume_type: "sbs_volume".to_string(),
            volume_iops: None,
        }
    }

    fn public_ip(id: &str, family: &str, address: &str, dynamic: bool, tags: &[&str]) -> PublicIp {
        PublicIp {
            id: id.to_string(),
            address: address.to_string(),
            family: family.to_string(),
            dynamic,
            tags: tags.iter().map(|tag| tag.to_string()).collect(),
        }
    }

    #[test]
    fn test_server_has_public_ipv4() {
        let mut s = server("id-1", "runner");
        s.public_ips = vec![public_ip("ip-6", "inet6", "2001:db8::1", false, &[])];
        assert!(!server_has_public_ipv4(&s));

        s.public_ips
            .push(public_ip("ip-4", "inet", "203.0.113.1", false, &[]));
        assert!(server_has_public_ipv4(&s));

        s.public_ips = vec![public_ip("ip-4", "inet", "", false, &[])];
        assert!(!server_has_public_ipv4(&s));
    }

    #[test]
    fn test_server_owned_ip_ids_filters_dynamic_and_untagged() {
        let mut s = server("id-1", "runner");
        s.public_ips = vec![
            public_ip("ip-owned", "inet", "203.0.113.1", false, &["gitlab-runner"]),
            public_ip(
                "ip-dynamic",
                "inet",
                "203.0.113.2",
                true,
                &["gitlab-runner"],
            ),
            public_ip("ip-manual", "inet", "203.0.113.3", false, &["my-static-ip"]),
        ];
        assert_eq!(server_owned_ip_ids(&s), vec!["ip-owned".to_string()]);
    }

    #[test]
    fn test_classify_creation_failure() {
        let next_zone = [400, 404, 408, 409, 500, 502, 503, 504];
        for status in next_zone {
            let error = ScalewayError::Api {
                status,
                message: "insufficient resources".to_string(),
            };
            assert_eq!(
                classify_creation_failure(&error),
                CreationFailure::NextZone,
                "status {} should try the next zone",
                status
            );
        }

        let abort = [401, 403, 418, 429];
        for status in abort {
            let error = ScalewayError::Api {
                status,
                message: "nope".to_string(),
            };
            assert_eq!(
                classify_creation_failure(&error),
                CreationFailure::Abort,
                "status {} should abort",
                status
            );
        }

        assert_eq!(
            classify_creation_failure(&ScalewayError::InvalidConfig("bad".to_string())),
            CreationFailure::Abort
        );
    }

    #[test]
    fn test_build_create_ip_request() {
        let request = build_create_ip_request(&test_config());
        assert_eq!(request.project, "p");
        assert_eq!(request.ip_type, "routed_ipv4");
        assert_eq!(request.tags, vec!["gitlab-runner".to_string()]);
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
    fn test_zone_url_uses_given_zone() {
        assert_eq!(
            zone_url("fr-par-2", "/servers"),
            "https://api.scaleway.com/instance/v1/zones/fr-par-2/servers"
        );
    }

    #[test]
    fn test_build_create_request_includes_key_tag_and_volume() {
        let mut config = test_config();
        config.ssh_public_key = Some("ssh-ed25519 AAAA key".to_string());
        let request = build_create_request(&config, "flexi-runner", "ip-1").unwrap();
        assert_eq!(request.name, "flexi-runner");
        assert_eq!(request.project, "p");
        assert_eq!(request.public_ips, vec!["ip-1".to_string()]);
        assert!(!request.dynamic_ip_required);
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
        let request = build_create_request(&test_config(), "flexi-runner", "ip-1").unwrap();
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
    fn test_block_volume_is_deletable() {
        assert!(block_volume_is_deletable("available"));
        assert!(!block_volume_is_deletable("in_use"));
        assert!(!block_volume_is_deletable("creating"));
        assert!(!block_volume_is_deletable(""));
    }

    #[test]
    fn test_block_volume_response_wrapped_and_direct() {
        let wrapped: BlockVolumeGetResponse =
            serde_json::from_str(r#"{"volume":{"id":"v1","status":"in_use"}}"#).unwrap();
        assert_eq!(wrapped.status(), "in_use");
        let direct: BlockVolumeGetResponse =
            serde_json::from_str(r#"{"id":"v1","status":"available"}"#).unwrap();
        assert_eq!(direct.status(), "available");
    }

    #[test]
    fn test_iops_to_apply() {
        assert_eq!(iops_to_apply("sbs_volume", None), None);
        assert_eq!(iops_to_apply("sbs_volume", Some(15_000)), Some(15_000));
        assert_eq!(iops_to_apply("l_ssd", Some(15_000)), None);
        assert_eq!(iops_to_apply("l_ssd", None), None);
    }
}
