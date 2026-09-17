# Scaleway Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the Hetzner Cloud provider in this GitLab runner orchestrator with Scaleway Instances, rename the project to `gitlab_scaleway`, and make the root volume configurable.

**Architecture:** A new `ScalewayClient` (Rust, `reqwest`) calls the Scaleway Instances v1 REST API: create a stopped server with an explicit root volume, write cloud-init via a raw-text PATCH, power it on, and terminate it (plus delete detached Block Storage volumes) when idle. The main polling loop, cloud-init generation, CSV logging, and state persistence stay; the provider-specific modules and names change.

**Tech Stack:** Rust 2021, Tokio, reqwest (json), serde/serde_json, toml, chrono, tracing, thiserror, anyhow. No new dependencies.

**Spec:** `docs/superpowers/specs/2026-09-17-scaleway-migration-design.md` (read it; this plan implements it).

## Global Constraints

- Crate name: `gitlab_scaleway`; version `0.3.0`; description "Automatic Scaleway server provisioning for GitLab CI runners".
- Scaleway API base: `https://api.scaleway.com`; auth header `X-Auth-Token`.
- Instance endpoints are zonal: `/instance/v1/zones/{zone}/servers...`; volume deletion is `/block/v1/zones/{zone}/volumes/{id}`.
- Server and volume IDs are UUID **strings**; never numbers.
- Cloud-init is written with `PATCH .../user_data/cloud-init`, raw text body, not JSON.
- Root volume defaults: `volume_size_gb = 50`, `volume_type = "sbs_volume"`; minimum size 10 GB; allowed types `sbs_volume` and `l_ssd`.
- Timeouts: wait-for-`stopped` 120 s, wait-for-`running` 180 s, poll every 3 s.
- Delete policy: debug build terminates immediately; release terminates when `uptime_minutes >= min_lifetime_minutes` and no jobs are active.
- Preserve: GitLab polling and tag filter, cloud-init generator, CSV format, daily log rotation, state persistence, `pull_policy = ["if-not-present"]` requirement.
- All existing behavior not named above is unchanged.

---

### Task 1: Add `ScalewayConfig` and a tested `scaleway` module (build stays green)

Adds the new config struct and a fully implemented, unit-tested `src/scaleway.rs` alongside the existing Hetzner code. `main.rs` declares `mod scaleway;` so its tests run, but still uses Hetzner until Task 2. `#[allow(dead_code)]` suppresses unused-code warnings until Task 2 removes it.

**Files:**
- Modify: `src/config.rs`
- Create: `src/scaleway.rs`
- Modify: `src/main.rs` (add `#[allow(dead_code)] mod scaleway;` only)

**Interfaces:**
- Consumes: nothing new.
- Produces:
  - `config::ScalewayConfig { token: String, project_id: String, zone: String, server_type: String, image: String, ssh_public_key: Option<String>, volume_size_gb: u32, volume_type: String }` (derives `Debug, Deserialize, Clone`)
  - `scaleway::ScalewayClient::new(&ScalewayConfig) -> ScalewayClient`
  - `scaleway::ScalewayClient::{find_server_by_name, create_server, terminate_server, delete_volumes, get_server, wait_for_state}` (async)
  - `scaleway::ScalewayError` variants `Request, Api, Parse, InvalidConfig, Timeout`
  - `scaleway::Server { id, name, state, public_ips, volumes }`, `scaleway::PublicIp { address, family }`, `scaleway::Volume { id }`
  - `scaleway::server_volume_ids(&Server) -> Vec<String>`

- [ ] **Step 1: Add `ScalewayConfig` to `src/config.rs`**

Add after the existing `HetznerConfig` (do not remove Hetzner yet):

```rust
/// Scaleway Instances configuration.
#[derive(Debug, Deserialize, Clone)]
#[allow(dead_code)] // wired into Config in Task 2
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
```

- [ ] **Step 2: Write failing tests for the config defaults in `src/config.rs`**

Add to the existing `mod tests`:

```rust
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
        assert_eq!(config.ssh_public_key.as_deref(), Some("ssh-ed25519 AAAA user@host"));
    }
```

- [ ] **Step 3: Run the config tests to verify they pass**

Run: `cargo test config::tests::test_scaleway`
Expected: PASS. (The struct exists from Step 1, so these pass immediately; they lock in the defaults.)

- [ ] **Step 4: Create `src/scaleway.rs` with full implementation**

```rust
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
/// Interval between state polls.
const STATE_POLL_INTERVAL_SECS: u64 = 3;
/// Scaleway's minimum root volume size in GB.
const MIN_VOLUME_SIZE_GB: u32 = 10;
/// Bytes in one gigabyte, as expected by the Scaleway API.
const BYTES_PER_GB: u64 = 1_000_000_000;

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
    pub async fn terminate_server(&self, server_id: &str) -> Result<(), ScalewayError> {
        info!("Terminating server: {}", server_id);
        match self.action(server_id, "terminate").await {
            Ok(()) => {
                info!("Server {} terminated", server_id);
                Ok(())
            }
            Err(e) => {
                warn!("Terminate failed for {} ({}); falling back", server_id, e);
                self.terminate_fallback(server_id).await
            }
        }
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
        };
        let request = build_create_request(&config, "flexi-runner").unwrap();
        assert_eq!(request.tags, vec!["gitlab-runner".to_string()]);
    }

    #[test]
    fn test_server_volume_ids_sorted() {
        let mut volumes = HashMap::new();
        volumes.insert("1".to_string(), Volume { id: "vol-b".to_string() });
        volumes.insert("0".to_string(), Volume { id: "vol-a".to_string() });
        let mut s = server("id-1", "runner");
        s.volumes = volumes;
        assert_eq!(server_volume_ids(&s), vec!["vol-a", "vol-b"]);
    }
}
```

- [ ] **Step 5: Declare the module in `src/main.rs`**

Add next to the other `mod` declarations (keep `mod hetzner;` for now):

```rust
#[allow(dead_code)] // wired in Task 2
mod scaleway;
```

- [ ] **Step 6: Build and run the new tests**

Run: `cargo test`
Expected: PASS — including all `config::tests::test_scaleway*` and `scaleway::tests::*` tests. No warnings that fail the build.

- [ ] **Step 7: Commit**

```bash
git add Cargo.toml src/config.rs src/scaleway.rs src/main.rs
git commit -m "feat: add Scaleway config and API client"
```

---

### Task 2: Swap the orchestrator to Scaleway and remove Hetzner

Replaces `Config.hetzner` with `Config.scaleway`, migrates state IDs to UUID strings with volume tracking, updates CSV logging, rewires `main.rs`, and deletes the Hetzner module.

**Files:**
- Modify: `src/config.rs`
- Modify: `src/state.rs`
- Modify: `src/csv_log.rs`
- Modify: `src/main.rs`
- Delete: `src/hetzner.rs`

**Interfaces:**
- Consumes: `ScalewayConfig`, `ScalewayClient`, `server_volume_ids`, `ScalewayError` from Task 1.
- Produces:
  - `config::Config { gitlab, scaleway, runner }`
  - `state::RunnerState::new(server_id: String, server_name: String, volume_ids: Vec<String>) -> RunnerState`; fields `server_id: String`, `volume_ids: Vec<String>`
  - `csv_log::CsvLogger::{log_start(&self, server_id: &str, project: &str, pipeline_id: u64, reason: &str), log_stop(&self, server_id: &str, reason: &str, duration_minutes: u64)}`
  - `main.rs` functions `verify_state_with_scaleway`, `create_runner`, `delete_runner` operating on `ScalewayClient`

- [ ] **Step 1: Update the state tests first in `src/state.rs`**

Replace the three tests in `mod tests` with the new signatures:

```rust
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

        let runner = RunnerState::new(
            "server-uuid".to_string(),
            "test-runner".to_string(),
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
```

- [ ] **Step 2: Run the state tests to verify they fail**

Run: `cargo test state::tests`
Expected: FAIL to compile — `RunnerState::new` expects `u64` and the struct lacks `volume_ids`.

- [ ] **Step 3: Update `src/state.rs`**

Replace the `RunnerState` struct and its `impl`:

```rust
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
```

Delete `has_min_uptime`, `minutes_until_next_billing_cycle`, `should_delete`, and `can_force_delete` entirely.

- [ ] **Step 4: Run the state tests**

Run: `cargo test state::tests`
Expected: PASS.

- [ ] **Step 5: Update `src/csv_log.rs` and its tests**

Change `LogEntry.server_id` to `Option<String>`:

```rust
    /// Scaleway server UUID
    pub server_id: Option<String>,
```

Change the two helper signatures and bodies:

```rust
    /// Helper method: Logs a server start.
    pub fn log_start(
        &self,
        server_id: &str,
        project: &str,
        pipeline_id: u64,
        reason: &str,
    ) -> Result<(), CsvLogError> {
        let entry = LogEntry {
            timestamp: Utc::now(),
            event: LogEvent::Start,
            server_id: Some(server_id.to_string()),
            project: Some(project.to_string()),
            pipeline_id: Some(pipeline_id),
            reason: reason.to_string(),
            duration_minutes: None,
        };
        self.log(&entry)
    }

    /// Helper method: Logs a server stop.
    pub fn log_stop(
        &self,
        server_id: &str,
        reason: &str,
        duration_minutes: u64,
    ) -> Result<(), CsvLogError> {
        let entry = LogEntry {
            timestamp: Utc::now(),
            event: LogEvent::Stop,
            server_id: Some(server_id.to_string()),
            project: None,
            pipeline_id: None,
            reason: reason.to_string(),
            duration_minutes: Some(duration_minutes),
        };
        self.log(&entry)
    }
```

The `log` method's formatting already calls `.map(|id| id.to_string())`, which works unchanged for `Option<String>`.

Add a test to `mod tests`:

```rust
    #[test]
    fn test_log_entry_serializes_string_server_id() {
        let entry = LogEntry {
            timestamp: Utc::now(),
            event: LogEvent::Start,
            server_id: Some("server-uuid".to_string()),
            project: Some("group/project".to_string()),
            pipeline_id: Some(42),
            reason: "pipeline_pending".to_string(),
            duration_minutes: None,
        };
        // Exercise the same formatting path used by `log`.
        let server_id = entry.server_id.clone().map(|id| id.to_string()).unwrap_or_default();
        assert_eq!(server_id, "server-uuid");
    }
```

- [ ] **Step 6: Run the CSV tests**

Run: `cargo test csv_log::tests`
Expected: PASS.

- [ ] **Step 7: Switch `Config` to Scaleway in `src/config.rs`**

Replace the `Config` struct, remove `HetznerConfig`, and update `load`:

```rust
/// Main configuration - contains all sub-configurations.
#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub gitlab: GitLabConfig,
    pub scaleway: ScalewayConfig,
    pub runner: RunnerConfig,
}
```

In `Config::load`, replace the Hetzner log line:

```rust
        info!(
            "  Scaleway zone: {} (type: {}, volume: {} GB {})",
            config.scaleway.zone,
            config.scaleway.server_type,
            config.scaleway.volume_size_gb,
            config.scaleway.volume_type
        );
```

Remove `ScalewayConfig`'s temporary `#[allow(dead_code)]` attribute.

Delete the `HetznerConfig` struct entirely.

- [ ] **Step 8: Delete `src/hetzner.rs`**

```bash
git rm src/hetzner.rs
```

- [ ] **Step 9: Rewire `src/main.rs`**

Replace the module declaration block and imports:

```rust
mod cloud_init;
mod config;
mod csv_log;
mod gitlab;
mod scaleway;
mod state;
```

```rust
use crate::cloud_init::generate_cloud_init;
use crate::config::{load_runner_config, Config};
use crate::csv_log::CsvLogger;
use crate::gitlab::GitLabClient;
use crate::scaleway::{server_volume_ids, ScalewayClient};
use crate::state::{OrchestratorState, RunnerState};
```

Remove the `BILLING_BUFFER_MINUTES` constant. Keep the other path constants and `is_debug_build`.

Replace `create_runner`:

```rust
/// Creates a new runner server.
async fn create_runner(
    scaleway_client: &ScalewayClient,
    csv_logger: &CsvLogger,
    cloud_init: &str,
    config: &Config,
    state: &mut OrchestratorState,
    project: &str,
    pipeline_id: u64,
) -> Result<()> {
    info!("Creating new runner server...");

    let server = scaleway_client
        .create_server(&config.runner.name, cloud_init)
        .await
        .context("Error creating server")?;

    let volume_ids = server_volume_ids(&server);
    let runner_state = RunnerState::new(server.id.clone(), server.name.clone(), volume_ids);
    state.set_runner(runner_state);

    if let Err(e) = csv_logger.log_start(&server.id, project, pipeline_id, "pipeline_pending") {
        warn!("Error in CSV logging: {}", e);
    }

    info!("Runner server created and ready");
    Ok(())
}
```

Replace `maybe_delete_runner` and `delete_runner`:

```rust
/// Checks if the server should be deleted and performs deletion if so.
async fn maybe_delete_runner(
    scaleway_client: &ScalewayClient,
    csv_logger: &CsvLogger,
    config: &Config,
    state: &mut OrchestratorState,
) -> Result<()> {
    let runner = match &state.runner {
        Some(r) => r,
        None => return Ok(()),
    };

    let uptime = runner.uptime_minutes();
    let min_lifetime = config.runner.min_lifetime_minutes;

    if is_debug_build() {
        info!(
            "[DEBUG] Server running for {}min - deleting immediately (no pipelines active)",
            uptime
        );
        delete_runner(scaleway_client, csv_logger, state, "debug_immediate_delete").await?;
        return Ok(());
    }

    if uptime >= min_lifetime as u64 {
        delete_runner(scaleway_client, csv_logger, state, "all_pipelines_done").await?;
    } else {
        info!(
            "Server running for {}min (minimum {}min), no pipelines active - waiting...",
            uptime, min_lifetime
        );
    }

    Ok(())
}

/// Terminates the runner server and removes its volumes.
async fn delete_runner(
    scaleway_client: &ScalewayClient,
    csv_logger: &CsvLogger,
    state: &mut OrchestratorState,
    reason: &str,
) -> Result<()> {
    let runner = match &state.runner {
        Some(r) => r,
        None => return Ok(()),
    };

    let server_id = runner.server_id.clone();
    let volume_ids = runner.volume_ids.clone();
    let uptime = runner.uptime_minutes();

    info!("Deleting runner server (reason: {})", reason);

    scaleway_client
        .terminate_server(&server_id)
        .await
        .context("Error terminating server")?;

    scaleway_client.delete_volumes(&volume_ids).await;

    if let Err(e) = csv_logger.log_stop(&server_id, reason, uptime) {
        warn!("Error in CSV logging: {}", e);
    }

    state.clear_runner();

    info!("Runner server deleted (runtime: {} minutes)", uptime);
    Ok(())
}
```

In `main`, replace the client construction and verification call:

```rust
    let gitlab_client = GitLabClient::new(&config.gitlab);
    let scaleway_client = ScalewayClient::new(&config.scaleway);
```

```rust
    verify_state_with_scaleway(&scaleway_client, &config.runner.name, &mut state).await?;
```

Pass `&scaleway_client` in the `orchestration_tick` call and rename the corresponding parameter and its uses.

Replace `verify_state_with_hetzner` with:

```rust
/// Verifies that the saved state matches Scaleway.
async fn verify_state_with_scaleway(
    scaleway_client: &ScalewayClient,
    server_name: &str,
    state: &mut OrchestratorState,
) -> Result<()> {
    info!("Verifying state with Scaleway API...");

    let scaleway_server = scaleway_client.find_server_by_name(server_name).await?;

    match (&state.runner, scaleway_server) {
        (Some(runner), Some(server)) if runner.server_id == server.id => {
            info!(
                "State verified: Server {} (ID: {}) exists, running for {} minutes",
                server.name,
                server.id,
                runner.uptime_minutes()
            );
        }
        (Some(runner), None) => {
            warn!(
                "State inconsistency: Server {} (ID: {}) no longer exists at Scaleway!",
                runner.server_name, runner.server_id
            );
            warn!("Clearing state...");
            state.clear_runner();
        }
        (Some(runner), Some(server)) => {
            warn!(
                "State inconsistency: State knows server ID {}, Scaleway has ID {}!",
                runner.server_id, server.id
            );
            warn!("Updating state with Scaleway data (creation time unknown)...");
            let volume_ids = server_volume_ids(&server);
            state.set_runner(RunnerState::new(server.id, server.name, volume_ids));
        }
        (None, Some(server)) => {
            warn!(
                "Orphaned server found: {} (ID: {}) - not in state!",
                server.name, server.id
            );
            warn!("Adding to state (creation time unknown)...");
            let volume_ids = server_volume_ids(&server);
            state.set_runner(RunnerState::new(server.id, server.name, volume_ids));
        }
        (None, None) => {
            info!("No server active - state is consistent");
        }
    }

    Ok(())
}
```

- [ ] **Step 10: Build and run the full test suite**

Run: `cargo build && cargo test`
Expected: PASS. No references to `hetzner`, `HetznerClient`, or `HetznerConfig` remain.

- [ ] **Step 11: Commit**

```bash
git add -A
git commit -m "refactor: replace Hetzner provider with Scaleway"
```

---

### Task 3: Extract and test the terminate decision

Makes the delete rule a pure, tested function so the threshold behavior is explicit and reviewable.

**Files:**
- Modify: `src/main.rs`

**Interfaces:**
- Consumes: `RunnerState::uptime_minutes`, `RunnerConfig::min_lifetime_minutes`, `is_debug_build`.
- Produces: `main::should_terminate(uptime_minutes: u64, min_lifetime_minutes: u32, is_debug: bool) -> bool`

- [ ] **Step 1: Write the failing tests**

Add at the bottom of `src/main.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_should_terminate_debug_always() {
        assert!(should_terminate(0, 20, true));
        assert!(should_terminate(1, 999, true));
    }

    #[test]
    fn test_should_terminate_before_min_lifetime() {
        assert!(!should_terminate(19, 20, false));
    }

    #[test]
    fn test_should_terminate_at_or_after_min_lifetime() {
        assert!(should_terminate(20, 20, false));
        assert!(should_terminate(65, 20, false));
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test main::tests`
Expected: FAIL to compile — `should_terminate` not found.

- [ ] **Step 3: Add `should_terminate` and use it in `maybe_delete_runner`**

Add near `is_debug_build`:

```rust
/// Returns true when an idle server may be terminated.
///
/// Debug builds always terminate immediately; release builds wait until the
/// minimum lifetime has elapsed.
fn should_terminate(uptime_minutes: u64, min_lifetime_minutes: u32, is_debug: bool) -> bool {
    is_debug || uptime_minutes >= min_lifetime_minutes as u64
}
```

Rewrite `maybe_delete_runner` to use it:

```rust
/// Checks if the server should be deleted and performs deletion if so.
async fn maybe_delete_runner(
    scaleway_client: &ScalewayClient,
    csv_logger: &CsvLogger,
    config: &Config,
    state: &mut OrchestratorState,
) -> Result<()> {
    let runner = match &state.runner {
        Some(r) => r,
        None => return Ok(()),
    };

    let uptime = runner.uptime_minutes();
    let min_lifetime = config.runner.min_lifetime_minutes;

    if !should_terminate(uptime, min_lifetime, is_debug_build()) {
        info!(
            "Server running for {}min (minimum {}min), no pipelines active - waiting...",
            uptime, min_lifetime
        );
        return Ok(());
    }

    let reason = if is_debug_build() {
        info!(
            "[DEBUG] Server running for {}min - deleting immediately (no pipelines active)",
            uptime
        );
        "debug_immediate_delete"
    } else {
        "all_pipelines_done"
    };

    delete_runner(scaleway_client, csv_logger, state, reason).await
}
```

- [ ] **Step 4: Run the tests and full suite**

Run: `cargo test`
Expected: PASS, including the three new `main::tests`.

- [ ] **Step 5: Commit**

```bash
git add src/main.rs
git commit -m "refactor: extract and test terminate decision"
```

---

### Task 4: Update packaging, example config, and documentation

Renames the crate/binary/user/service, updates the embedded example configuration, and rewrites the README for Scaleway.

**Files:**
- Modify: `Cargo.toml`
- Modify: `src/main.rs` (`CONFIG_EXAMPLE_CONTENT`)
- Modify: `Dockerfile`
- Modify: `docker-compose.yml`
- Modify: `README.md`

**Interfaces:**
- Consumes: nothing.
- Produces: packaging and docs matching the `[scaleway]` config schema.

- [ ] **Step 1: Update `Cargo.toml`**

```toml
[package]
name = "gitlab_scaleway"
version = "0.3.0"
edition = "2021"
description = "Automatic Scaleway server provisioning for GitLab CI runners"
license = "MIT"
authors = ["Maximilian Kutschka"]
```

- [ ] **Step 2: Update `CONFIG_EXAMPLE_CONTENT` in `src/main.rs`**

Replace the whole raw string with:

```rust
const CONFIG_EXAMPLE_CONTENT: &str = r#"# GitLab Runner Orchestrator - Example Configuration
# Copy this file to config.toml and customize the values.

[gitlab]
# URL of your GitLab instance
url = "https://gitlab.example.com"
# Personal Access Token with API access (read_api scope is sufficient)
token = "glpat-xxxxxxxxxxxxxxxxxxxx"
# Optional: only spin up a runner when a pending job has one of these tags.
# Remove or leave empty to react to all pending jobs.
# tag_filter = ["scaleway", "my-runner-tag"]

[scaleway]
# Scaleway IAM API secret key
token = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
# Scaleway Project ID that owns the runner
project_id = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
# Availability Zone (fr-par-1/2/3, nl-ams-1/2/3, pl-waw-1/2/3, it-mil-1)
zone = "fr-par-1"
# Instance type (e.g. PLAY2-PICO, DEV1-M, PRO2-XS, PRO2-S)
server_type = "PRO2-XS"
# Marketplace image label or local image UUID
image = "ubuntu_noble"
# Optional: SSH public key for debugging, injected via an AUTHORIZED_KEY tag
# ssh_public_key = "ssh-ed25519 AAAA... user@host"
# Root volume size in GB (minimum 10). 50+ recommended for CI caches.
volume_size_gb = 50
# Root volume type: "sbs_volume" (Block Storage, default) or "l_ssd" (local,
# only on DEV1/GP1 instance types)
volume_type = "sbs_volume"

[runner]
# Name of the server in Scaleway
name = "flexi-runner"
# Minimum runtime in minutes before the server can be deleted
min_lifetime_minutes = 20
# Polling interval in seconds
poll_interval_seconds = 30
"#;
```

- [ ] **Step 3: Update the `Dockerfile`**

Replace the user/UID environment and the copied binary:

```dockerfile
ENV USER=scw
ENV UID=42069

RUN adduser \
    --disabled-password \
    --gecos "" \
    --home "/nonexistent" \
    --shell "/sbin/nologin" \
    --no-create-home \
    --uid "${UID}" \
    "${USER}"
```

```dockerfile
COPY --from=builder /app/target/release/gitlab_scaleway /app/app
```

```dockerfile
# Use the unprivileged user
USER scw:scw
```

- [ ] **Step 4: Update `docker-compose.yml`**

```yaml
services:
  scaleway-starter:
    build: .
    volumes:
      - ./config:/app/config
```

- [ ] **Step 5: Rewrite `README.md`**

```markdown
# GitLab Runner Orchestrator for Scaleway

Automatic provisioning of Scaleway Instances as GitLab CI runners - **pay only when you need it**.

## Features

- **Automatic server creation** when pipelines are pending/running
- **Cost-aware deletion** - the instance is terminated once the minimum lifetime has elapsed and no jobs remain
- **Configurable root volume** - sized for Docker layer and build caches (default 50 GB)
- **Polls all projects** - one runner for the entire GitLab instance
- **State persistence** - survives restarts without data loss
- **CSV logging** - documents all server starts/stops with reason and duration
- **Rotating log files** - daily rotation

## Architecture

```mermaid
flowchart TB
    subgraph main [Main Loop]
        Poll[Polling Loop]
        Poll --> FetchProjects[GitLab: Fetch all projects]
        FetchProjects --> CheckPipelines[Per project: Pending/Running pipelines?]
        CheckPipelines -->|Yes + No server| LogStart[CSV: Log reason]
        LogStart --> CreateServer[Scaleway: Create stopped instance]
        CreateServer --> CloudInit[Scaleway: Apply cloud-init]
        CloudInit --> PowerOn[Scaleway: Power on]
        CheckPipelines -->|No everywhere + Server exists| CheckDelete[Check min lifetime]
        CheckDelete -->|Elapsed| LogStop[CSV: Log usage]
        LogStop --> Terminate[Scaleway: Terminate + delete volumes]
    end

    subgraph config [Config]
        ConfigToml[config/config.toml]
        RunnerToml[config/runner.toml]
    end

    subgraph logs [Logging]
        CSV[logs/runner_usage.csv]
        LogFile[logs/orchestrator.log]
    end

    ConfigToml --> main
    RunnerToml --> CloudInit
    main --> CSV
    main --> LogFile
```

## Quick Start

### 1. Build the binary

```bash
cargo build --release
```

### 2. Create configuration

On first start, `config/config.example.toml` is automatically created. Copy and customize it:

```bash
cp config/config.example.toml config/config.toml
# Edit config/config.toml with your Scaleway and GitLab credentials
```

You need a Scaleway IAM API secret key and the Project ID that should own the runners. See the [Scaleway IAM documentation](https://www.scaleway.com/en/docs/iam/how-to/create-api-keys/) for creating a key.

### 3. Create runner configuration

Create `config/runner.toml` with your GitLab Runner configuration.
You can register the GitLab Runner with the following command:

```bash
docker run -it -v /var/run/docker.sock:/var/run/docker.sock -v ./config:/etc/gitlab-runner gitlab/gitlab-runner:latest register --url https://gitlab.example.com --token YOUR_TOKEN
```

**IMPORTANT:**
Under `[runners.docker]` there is a `pull_policy` setting.
Set it to:

```toml
pull_policy = ["if-not-present"]
```

Otherwise the runner will do too many `docker image pull` requests and your IP will be banned!

### 4. Start

```bash
cargo run --release
# or
./target/release/gitlab_scaleway
```

## Configuration

### config/config.toml

```toml
[gitlab]
url = "https://gitlab.example.com"
token = "glpat-xxxxxxxxxxxxxxxxxxxx"  # read_api scope

[scaleway]
token = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"   # IAM API secret key
project_id = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
zone = "fr-par-1"                # fr-par-1/2/3, nl-ams-1/2/3, pl-waw-1/2/3, it-mil-1
server_type = "PRO2-XS"
image = "ubuntu_noble"
ssh_public_key = "ssh-ed25519 AAAA... user@host"  # optional
volume_size_gb = 50              # minimum 10
volume_type = "sbs_volume"       # or "l_ssd" on DEV1/GP1 types

[runner]
name = "flexi-runner"
min_lifetime_minutes = 20
poll_interval_seconds = 30
```

### Storage

Scaleway's OS image default root volume is only about 10 GB, which is too small for Docker layer caches and build caches. `volume_size_gb` controls the root volume size; **50 GB or more is recommended** for CI workloads.

`volume_type` selects the storage backend:

- `sbs_volume` (default) - network Block Storage. Works with all current instance ranges. When the instance is terminated, the volume is detached and the orchestrator explicitly deletes it.
- `l_ssd` - local SSD, only available on Development (DEV1) and first-generation General Purpose (GP1) instance types. It is deleted automatically when the instance is terminated.

### SSH access

If `ssh_public_key` is set, the orchestrator attaches it to the instance using an `AUTHORIZED_KEY` tag, matching how Scaleway injects per-instance keys. Leave it unset to rely solely on account-level keys.

## Debug vs Release

| Feature          | Debug     | Release                          |
| ---------------- | --------- | -------------------------------- |
| Polling interval | 5s        | 30s (from config)                |
| Server deletion  | Immediate | After min. lifetime              |

## Billing

Scaleway CPU Instances are billed **per hour while powered on**, with a minimum of 60 minutes per start/stop period. Storage volumes and flexible IPv4 addresses are billed separately and continue while the instance exists.

Because of the 60-minute minimum block, terminating an idle instance at `min_lifetime_minutes = 20` costs the same as waiting until 55 minutes. Setting `min_lifetime_minutes = 60` keeps the instance available for the full paid block to absorb follow-up jobs. The orchestrator terminates the instance and deletes its volumes once no jobs remain and the minimum lifetime has elapsed.

## Logs

- `logs/orchestrator.log` - Detailed logs (daily rotation)
- `logs/runner_usage.csv` - Server usage documentation

### CSV Format

```csv
timestamp,event,server_id,project,pipeline_id,reason,duration_minutes
2026-01-14T10:30:00Z,START,2e0394ea-120c-4a15-ad78-053f844d486c,mygroup/myproject,9876,pipeline_pending,
2026-01-14T11:15:00Z,STOP,2e0394ea-120c-4a15-ad78-053f844d486c,,,all_pipelines_done,45
```

## Migrating from the Hetzner version

- Rewrite `config/config.toml`: replace the `[hetzner]` section with `[scaleway]` (see above).
- Delete any existing `config/state.json`. The old numeric server ID is not compatible; the orchestrator ignores an unreadable state file and starts fresh.
- `config/runner.toml` is unchanged.
```

- [ ] **Step 6: Build the release binary and run tests**

Run: `cargo build --release && cargo test`
Expected: PASS. `target/release/gitlab_scaleway` exists.

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "docs: update packaging and documentation for Scaleway"
```

---

### Task 5: Final verification

Confirms the migration is complete and clean.

**Files:** none (verification only).

- [ ] **Step 1: Check formatting**

Run: `cargo fmt --check`
Expected: no output, exit 0. If it reports diffs, run `cargo fmt` and include the result in a commit.

- [ ] **Step 2: Run clippy with warnings as errors**

Run: `cargo clippy --all-targets -- -D warnings`
Expected: no warnings or errors.

- [ ] **Step 3: Run the full test suite**

Run: `cargo test`
Expected: all tests pass.

- [ ] **Step 4: Confirm no Hetzner references remain**

Run: `git grep -in hetzner`
Expected: only the "Migrating from the Hetzner version" heading and any historical commit messages; no code references. Investigate and remove any remaining `HetznerClient`, `HetznerConfig`, `hetzner.rs`, or `[hetzner]` code references.

- [ ] **Step 5: Build the optimized binary**

Run: `cargo build --release`
Expected: `target/release/gitlab_scaleway` builds successfully.

- [ ] **Step 6: Commit any formatting fixes**

```bash
git status --short
# If cargo fmt changed files:
git add -A && git commit -m "style: cargo fmt"
```

---

## Manual Smoke Test (requires a real Scaleway account)

Not automated; document the result in the PR description.

1. Create `config/config.toml` with a valid Scaleway secret key, project ID, and a zone, plus GitLab credentials.
2. Run `cargo run --release`.
3. Trigger a pending job matching the configured `tag_filter` (or any job if no filter).
4. Confirm the log shows: server created, cloud-init applied, powered on, and the job runs.
5. Wait past `min_lifetime_minutes` with no further jobs.
6. Confirm the log shows termination; verify in the Scaleway console that the instance and its volume are gone; confirm a STOP row was appended to `logs/runner_usage.csv`.
