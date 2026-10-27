//! GitLab Runner Orchestrator for Scaleway.
//!
//! Automatically creates Scaleway servers as GitLab runners when pipelines
//! are pending and terminates them once the minimum lifetime has elapsed.
//!
//! # How it works
//!
//! 1. Polls the GitLab API for active pipelines (pending/running)
//! 2. On active pipeline: Create Scaleway server (if not present)
//! 3. On no active pipelines: Terminate server (after minimum runtime)
//!
//! # Configuration
//!
//! Expects `config/config.toml` with GitLab and Scaleway credentials.
//! Expects `config/runner.toml` with GitLab Runner configuration.

mod cloud_init;
mod config;
mod csv_log;
mod gitlab;
mod provisioning;
mod scaleway;
mod state;

use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::time::sleep;
use tracing::{error, info, warn};
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, Layer};

use crate::cloud_init::generate_cloud_init;
use crate::config::{load_runner_config, Config};
use crate::csv_log::CsvLogger;
use crate::gitlab::GitLabClient;
use crate::scaleway::{
    server_has_public_ipv4, server_owned_ip_ids, server_volume_ids, ScalewayClient, Server,
};
use crate::state::{OrchestratorState, RunnerState, ZonedId};

/// Default path for configuration.
const CONFIG_PATH: &str = "config/config.toml";

/// Default path for runner configuration.
const RUNNER_CONFIG_PATH: &str = "config/runner.toml";

/// Default path for persisted state.
const STATE_PATH: &str = "config/state.json";

/// Default path for logs.
const LOG_DIR: &str = "logs";

/// Default path for example configuration.
const CONFIG_EXAMPLE_PATH: &str = "config/config.example.toml";

/// Content of the example configuration.
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
# Availability Zones in priority order. The runner is created in the first
# zone that can provide a reserved IPv4 address.
# Valid zones: fr-par-1/2/3, nl-ams-1/2/3, pl-waw-1/2/3, it-mil-1
zones = ["fr-par-1", "fr-par-2", "fr-par-3", "pl-waw-1"]
# The legacy single-zone form is still accepted:
# zone = "fr-par-1"
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
# Optional Block Storage IOPS class: 5000 (default) or 15000. Only applies to
# "sbs_volume"; ignored for "l_ssd". 15K requires the instance type to have at
# least 3 GiB/s of block bandwidth.
# volume_iops = 15000

[runner]
# Name of the server in Scaleway
name = "flexi-runner"
# Minimum runtime in minutes before the server can be deleted
min_lifetime_minutes = 20
# Polling interval in seconds
poll_interval_seconds = 30
"#;

/// Returns true if compiled in debug mode.
/// In debug mode, the server is immediately deleted when no pipelines are active.
const fn is_debug_build() -> bool {
    cfg!(debug_assertions)
}

/// Returns true when an idle server may be terminated.
///
/// Debug builds always terminate immediately; release builds wait until the
/// minimum lifetime has elapsed.
fn should_terminate(uptime_minutes: u64, min_lifetime_minutes: u32, is_debug: bool) -> bool {
    is_debug || uptime_minutes >= min_lifetime_minutes as u64
}

/// Creates the example configuration if it doesn't exist.
fn ensure_example_config() {
    if !Path::new(CONFIG_EXAMPLE_PATH).exists() {
        // Create config directory if needed
        std::fs::create_dir_all("config").ok();

        if let Err(e) = std::fs::write(CONFIG_EXAMPLE_PATH, CONFIG_EXAMPLE_CONTENT) {
            eprintln!("Warning: Could not create example configuration: {}", e);
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Create example configuration if not present
    ensure_example_config();

    // Create log directory if not present
    std::fs::create_dir_all(LOG_DIR).ok();

    // File appender for rotating logs (daily)
    let file_appender = RollingFileAppender::new(Rotation::DAILY, LOG_DIR, "orchestrator.log");

    // Initialize tracing with console + file output
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);

    tracing_subscriber::registry()
        // Console layer - compact format
        .with(
            fmt::layer()
                .with_target(false)
                .with_thread_ids(false)
                .with_file(false)
                .with_line_number(false)
                .with_filter(tracing_subscriber::filter::LevelFilter::INFO),
        )
        // File layer - detailed format with timestamps
        .with(
            fmt::layer()
                .with_writer(non_blocking)
                .with_ansi(false) // No ANSI colors in file
                .with_target(true)
                .with_thread_ids(false)
                .with_file(false)
                .with_line_number(false)
                .with_filter(tracing_subscriber::filter::LevelFilter::DEBUG),
        )
        .init();

    info!("=== GitLab Runner Orchestrator ===");
    info!("Logs are written to: {}/orchestrator.log", LOG_DIR);
    if is_debug_build() {
        warn!("DEBUG BUILD: Server will be deleted immediately when no pipelines are active!");
    }
    info!("Starting...");

    // Check if config directory exists
    if !Path::new("config").exists() {
        error!("Config directory 'config/' does not exist!");
        error!("Please create config/config.toml and config/runner.toml");
        std::process::exit(1);
    }

    // Load configuration
    let config = Config::load(CONFIG_PATH).context("Error loading configuration")?;

    // Load runner configuration
    let runner_config =
        load_runner_config(RUNNER_CONFIG_PATH).context("Error loading runner configuration")?;

    // Initialize CSV logger
    let csv_logger = CsvLogger::new(LOG_DIR).context("Error initializing CSV logger")?;

    // Create API clients
    let gitlab_client = GitLabClient::new(&config.gitlab);
    let scaleway_client = ScalewayClient::new(&config.scaleway);

    // Generate cloud-init template
    let cloud_init = generate_cloud_init(
        &runner_config,
        config.runner.run_untagged,
        config.runner.protected,
    );

    // Load orchestrator state with persistence
    let mut state =
        OrchestratorState::with_persistence(STATE_PATH).context("Error loading state")?;

    // Verify that saved state matches Scaleway
    verify_state_with_scaleway(&scaleway_client, &config, &config.runner.name, &mut state).await?;

    // Polling interval (Debug: 5s, Release: from config)
    let poll_interval_secs = if is_debug_build() {
        5
    } else {
        config.runner.poll_interval_seconds
    };
    let poll_interval = Duration::from_secs(poll_interval_secs);

    info!("Starting polling loop (interval: {}s)", poll_interval_secs);
    info!(
        "Minimum server runtime: {} minutes",
        config.runner.min_lifetime_minutes
    );

    // Main loop
    loop {
        if let Err(e) = orchestration_tick(
            &gitlab_client,
            &scaleway_client,
            &csv_logger,
            &cloud_init,
            &config,
            &mut state,
        )
        .await
        {
            error!("Error in orchestration tick: {}", e);
            // Continue on errors
        }

        sleep(poll_interval).await;
    }
}

/// Verifies that the saved state matches Scaleway.
///
/// Possible scenarios:
/// - State says server exists, Scaleway too → OK, keep state
/// - State says server exists, Scaleway doesn't → Clear state
/// - State says no server, Scaleway has one → Update state (emergency)
/// - Both say no server → OK
async fn verify_state_with_scaleway(
    scaleway_client: &ScalewayClient,
    config: &Config,
    server_name: &str,
    state: &mut OrchestratorState,
) -> Result<()> {
    info!("Verifying state with Scaleway API...");

    // A known zone is authoritative even if it is no longer configured. Legacy
    // state has no zone, so those runners are searched across the priority list.
    let located = match &state.runner {
        Some(runner) if !runner.zone.is_empty() => scaleway_client
            .find_server_by_name(&runner.zone, server_name)
            .await?
            .map(|server| (runner.zone.clone(), server)),
        _ => find_server_across_zones(scaleway_client, &config.scaleway.zones, server_name).await?,
    };

    match (&state.runner, located) {
        // State and Scaleway match - server exists
        (Some(runner), Some((zone, server))) if runner.server_id == server.id => {
            info!(
                "State verified: Server {} (ID: {}) exists in zone {}, running for {} minutes",
                server.name,
                server.id,
                zone,
                runner.uptime_minutes()
            );
            let mut updated = runner.clone();
            if updated.zone != zone {
                updated.zone = zone.clone();
            }
            if updated.ip_ids.is_empty() {
                updated.ip_ids = server_owned_ip_ids(&server);
            }
            if updated.zone != runner.zone || updated.ip_ids != runner.ip_ids {
                state.set_runner(updated);
            }
            ensure_runner_healthy(scaleway_client, state, &zone, &server).await?;
        }

        // State says server exists, but with a different ID (very unlikely)
        (Some(runner), Some((zone, server))) => {
            warn!(
                "State inconsistency: State knows server ID {}, Scaleway has ID {} in zone {}!",
                runner.server_id, server.id, zone
            );
            let old_runner = runner.clone();
            cleanup_runner_resources(scaleway_client, state, &old_runner).await;
            warn!("Updating state with Scaleway data (creation time unknown)...");
            adopt_server(state, &zone, &server);
            ensure_runner_healthy(scaleway_client, state, &zone, &server).await?;
        }

        // State says server exists, but Scaleway doesn't know it anymore
        (Some(runner), None) => {
            warn!(
                "State inconsistency: Server {} (ID: {}) no longer exists at Scaleway!",
                runner.server_name, runner.server_id
            );
            let old_runner = runner.clone();
            cleanup_runner_resources(scaleway_client, state, &old_runner).await;
            warn!("Clearing state...");
            state.clear_runner();
        }

        // State says no server, but Scaleway has one (emergency recovery)
        (None, Some((zone, server))) => {
            warn!(
                "Orphaned server found: {} (ID: {}) in zone {} - not in state!",
                server.name, server.id, zone
            );
            warn!("Adding to state (creation time unknown)...");
            adopt_server(state, &zone, &server);
            ensure_runner_healthy(scaleway_client, state, &zone, &server).await?;
        }

        // All OK - no server
        (None, None) => {
            info!("No server active - state is consistent");
        }
    }

    Ok(())
}

/// Finds a server with the given name in the first zone that has it.
async fn find_server_across_zones(
    scaleway_client: &ScalewayClient,
    zones: &[String],
    name: &str,
) -> Result<Option<(String, Server)>> {
    let mut found: Option<(String, Server)> = None;
    for zone in zones {
        if let Some(server) = scaleway_client.find_server_by_name(zone, name).await? {
            if found.is_some() {
                warn!(
                    "Multiple servers named '{}' found; using the highest-priority zone",
                    name
                );
            } else {
                found = Some((zone.clone(), server));
            }
        }
    }
    Ok(found)
}

/// Finds the zone containing a server ID, or `None` if it is gone.
async fn find_server_by_id_across_zones(
    scaleway_client: &ScalewayClient,
    zones: &[String],
    server_id: &str,
) -> Result<Option<String>> {
    for zone in zones {
        if scaleway_client.get_server(zone, server_id).await?.is_some() {
            return Ok(Some(zone.clone()));
        }
    }
    Ok(None)
}

/// Records a server found at Scaleway as the active runner.
fn adopt_server(state: &mut OrchestratorState, zone: &str, server: &Server) {
    state.set_runner(RunnerState::new(
        server.id.clone(),
        server.name.clone(),
        zone.to_string(),
        server_volume_ids(server),
        server_owned_ip_ids(server),
    ));
}

/// What to do with a runner found at startup.
#[derive(Debug, PartialEq, Eq)]
enum RunnerHealth {
    Healthy,
    Replace,
    LeaveTransient,
}

/// Classifies a runner's state for the startup health check.
fn runner_health(state: &str, has_ipv4: bool) -> RunnerHealth {
    match state {
        "running" if has_ipv4 => RunnerHealth::Healthy,
        "running" | "stopped" | "stopped in place" => RunnerHealth::Replace,
        "starting" | "stopping" | "locked" => RunnerHealth::LeaveTransient,
        _ => RunnerHealth::LeaveTransient,
    }
}

/// Terminates a runner that cannot serve traffic and clears its state.
async fn ensure_runner_healthy(
    scaleway_client: &ScalewayClient,
    state: &mut OrchestratorState,
    zone: &str,
    server: &Server,
) -> Result<()> {
    let has_ipv4 = server_has_public_ipv4(server);
    match runner_health(&server.state, has_ipv4) {
        RunnerHealth::Healthy => return Ok(()),
        RunnerHealth::LeaveTransient => {
            warn!(
                "Runner {} in zone {} is {} - leaving it alone",
                server.id, zone, server.state
            );
            return Ok(());
        }
        RunnerHealth::Replace => {}
    }

    warn!(
        "Runner {} in zone {} is unusable (state: {}, public IPv4: {}) - replacing it",
        server.id, zone, server.state, has_ipv4
    );

    if let Err(e) = scaleway_client.terminate_server(zone, &server.id).await {
        error!(
            "Could not terminate unhealthy runner {} in zone {}: {}",
            server.id, zone, e
        );
        // Keep the state so a later run can retry instead of leaking the server.
        return Ok(());
    }

    let volumes = cleanup_volumes(
        zone,
        &server_volume_ids(server),
        state.runner.as_ref(),
        state.pending_volumes(),
    );
    let failed = scaleway_client.delete_volumes(&volumes).await;
    state.set_pending_volumes(failed);

    let ips = cleanup_ips(
        zone,
        &server_owned_ip_ids(server),
        state.runner.as_ref(),
        state.pending_ips(),
    );
    let failed = scaleway_client.delete_ips(&ips).await;
    state.set_pending_ips(failed);

    state.clear_runner();
    Ok(())
}

/// Deletes a runner's recorded volumes and reserved IPs without terminating it.
async fn cleanup_runner_resources(
    scaleway_client: &ScalewayClient,
    state: &mut OrchestratorState,
    runner: &RunnerState,
) {
    let volumes = cleanup_volumes("", &[], Some(runner), state.pending_volumes());
    if !volumes.is_empty() {
        let failed = scaleway_client.delete_volumes(&volumes).await;
        state.set_pending_volumes(failed);
    }

    let ips = cleanup_ips("", &[], Some(runner), state.pending_ips());
    if !ips.is_empty() {
        let failed = scaleway_client.delete_ips(&ips).await;
        state.set_pending_ips(failed);
    }
}

/// Builds zoned IDs from a runner zone and resource IDs.
fn zoned_ids(zone: &str, ids: &[String]) -> Vec<ZonedId> {
    let zone = if zone.is_empty() {
        None
    } else {
        Some(zone.to_string())
    };
    ids.iter()
        .cloned()
        .map(|id| ZonedId {
            zone: zone.clone(),
            id,
        })
        .collect()
}

/// Appends resources not already present, de-duplicating by ID.
fn merge_zoned_ids(mut base: Vec<ZonedId>, extra: Vec<ZonedId>) -> Vec<ZonedId> {
    for item in extra {
        if !base.iter().any(|existing| existing.id == item.id) {
            base.push(item);
        }
    }
    base
}

/// Builds the volume deletion set for replacing or forgetting a runner.
///
/// Existing pending entries are preserved so a cleanup triggered by another
/// runner cannot erase resources that are still awaiting deletion.
fn cleanup_volumes(
    zone: &str,
    server_volumes: &[String],
    runner: Option<&RunnerState>,
    pending: Vec<ZonedId>,
) -> Vec<ZonedId> {
    let mut volumes = merge_zoned_ids(pending, zoned_ids(zone, server_volumes));
    if let Some(runner) = runner {
        volumes = merge_zoned_ids(volumes, zoned_ids(&runner.zone, &runner.volume_ids));
    }
    volumes
}

/// Builds the reserved-IP deletion set for replacing or forgetting a runner.
fn cleanup_ips(
    zone: &str,
    server_ips: &[String],
    runner: Option<&RunnerState>,
    pending: Vec<ZonedId>,
) -> Vec<ZonedId> {
    let mut ips = merge_zoned_ids(pending, zoned_ids(zone, server_ips));
    if let Some(runner) = runner {
        ips = merge_zoned_ids(ips, zoned_ids(&runner.zone, &runner.ip_ids));
    }
    ips
}

/// Picks the zone used to terminate a runner.
///
/// A concrete recorded zone wins when the server is actually there. If it is
/// not, the zone where the server was found is used; `None` means the server
/// could not be located and termination must be skipped.
fn choose_delete_zone(
    recorded_zone: &str,
    exists_in_recorded_zone: bool,
    found_elsewhere: Option<String>,
) -> Option<String> {
    if !recorded_zone.is_empty() && exists_in_recorded_zone {
        Some(recorded_zone.to_string())
    } else {
        found_elsewhere
    }
}

/// Retries deletion of resources from previous failed attempts.
async fn retry_pending_resources(scaleway_client: &ScalewayClient, state: &mut OrchestratorState) {
    let volumes = state.pending_volumes();
    if !volumes.is_empty() {
        info!("Retrying deletion of {} pending volume(s)", volumes.len());
        let failed = scaleway_client.delete_volumes(&volumes).await;
        state.set_pending_volumes(failed);
    }

    let ips = state.pending_ips();
    if !ips.is_empty() {
        info!("Retrying release of {} pending IP(s)", ips.len());
        let failed = scaleway_client.delete_ips(&ips).await;
        state.set_pending_ips(failed);
    }
}

/// One pass of the orchestration logic.
async fn orchestration_tick(
    gitlab_client: &GitLabClient,
    scaleway_client: &ScalewayClient,
    csv_logger: &CsvLogger,
    cloud_init: &str,
    config: &Config,
    state: &mut OrchestratorState,
) -> Result<()> {
    retry_pending_resources(scaleway_client, state).await;

    // Query GitLab for active jobs (filtered by tag if configured)
    let active_jobs = gitlab_client
        .find_active_jobs(config.gitlab.tag_filter.as_deref())
        .await?;
    let has_active = !active_jobs.is_empty();

    if has_active {
        // There are active jobs - ensure server exists
        if !state.has_runner() {
            // No server present - create one
            let first_job = &active_jobs[0];
            info!(
                "Active job found: {} (ID: {}) in {}",
                first_job.job.status, first_job.job.id, first_job.project.path_with_namespace
            );

            create_runner(
                scaleway_client,
                csv_logger,
                cloud_init,
                config,
                state,
                &first_job.project.path_with_namespace,
                first_job.job.id,
            )
            .await?;
        } else {
            // Server is already running
            if let Some(uptime) = state.runner_uptime() {
                info!(
                    "Server running ({}min), {} active job(s)",
                    uptime,
                    active_jobs.len()
                );
            }
        }
    } else {
        // No active jobs
        if state.has_runner() {
            // Server is running, but no jobs anymore - check if we should delete
            maybe_delete_runner(scaleway_client, csv_logger, config, state).await?;
        } else {
            info!("No active jobs, no server active - waiting...");
        }
    }

    Ok(())
}

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

    let provisioned = crate::provisioning::create_with_fallback(
        scaleway_client,
        &config.scaleway,
        &config.runner.name,
        cloud_init,
        state,
    )
    .await
    .context("Error creating server")?;

    let volume_ids = server_volume_ids(&provisioned.server);
    let runner_state = RunnerState::new(
        provisioned.server.id.clone(),
        provisioned.server.name.clone(),
        provisioned.zone.clone(),
        volume_ids,
        provisioned.ip_ids.clone(),
    );
    state.set_runner(runner_state);

    if let Err(e) = csv_logger.log_start(
        &provisioned.server.id,
        project,
        pipeline_id,
        "pipeline_pending",
    ) {
        warn!("Error in CSV logging: {}", e);
    }

    info!(
        "Runner server created and ready in zone {}",
        provisioned.zone
    );
    Ok(())
}

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

    delete_runner(scaleway_client, csv_logger, config, state, reason).await
}

/// Terminates the runner server and removes its volumes and reserved IPs.
async fn delete_runner(
    scaleway_client: &ScalewayClient,
    csv_logger: &CsvLogger,
    config: &Config,
    state: &mut OrchestratorState,
    reason: &str,
) -> Result<()> {
    let runner = match &state.runner {
        Some(r) => r,
        None => return Ok(()),
    };

    let server_id = runner.server_id.clone();
    let volume_ids = runner.volume_ids.clone();
    let ip_ids = runner.ip_ids.clone();
    let recorded_zone = runner.zone.clone();
    let uptime = runner.uptime_minutes();

    let exists_in_recorded_zone = if recorded_zone.is_empty() {
        false
    } else {
        scaleway_client
            .get_server(&recorded_zone, &server_id)
            .await?
            .is_some()
    };
    let found_elsewhere = if exists_in_recorded_zone {
        None
    } else {
        find_server_by_id_across_zones(scaleway_client, &config.scaleway.zones, &server_id).await?
    };
    let delete_zone = choose_delete_zone(&recorded_zone, exists_in_recorded_zone, found_elsewhere);

    if let Some(ref zone) = delete_zone {
        info!(
            "Deleting runner server in zone {} (reason: {})",
            zone, reason
        );
        scaleway_client
            .terminate_server(zone, &server_id)
            .await
            .context("Error terminating server")?;
    } else {
        warn!(
            "Server {} was not found in its recorded zone or any configured zone; \
             skipping termination and cleaning up recorded resources",
            server_id
        );
    }

    let resource_zone = delete_zone.as_deref().unwrap_or_default();
    let mut to_delete: Vec<ZonedId> = zoned_ids(resource_zone, &volume_ids);
    to_delete.extend(state.pending_volumes());
    let failed = scaleway_client.delete_volumes(&to_delete).await;
    state.set_pending_volumes(failed);

    let mut ips_to_delete: Vec<ZonedId> = zoned_ids(resource_zone, &ip_ids);
    ips_to_delete.extend(state.pending_ips());
    let failed = scaleway_client.delete_ips(&ips_to_delete).await;
    state.set_pending_ips(failed);

    if let Err(e) = csv_logger.log_stop(&server_id, reason, uptime) {
        warn!("Error in CSV logging: {}", e);
    }

    state.clear_runner();

    info!("Runner server deleted (runtime: {} minutes)", uptime);
    Ok(())
}

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

    #[test]
    fn test_runner_health() {
        assert_eq!(runner_health("running", true), RunnerHealth::Healthy);
        assert_eq!(runner_health("running", false), RunnerHealth::Replace);
        assert_eq!(runner_health("stopped", true), RunnerHealth::Replace);
        assert_eq!(
            runner_health("stopped in place", true),
            RunnerHealth::Replace
        );
        assert_eq!(
            runner_health("starting", false),
            RunnerHealth::LeaveTransient
        );
        assert_eq!(
            runner_health("stopping", false),
            RunnerHealth::LeaveTransient
        );
        assert_eq!(runner_health("locked", false), RunnerHealth::LeaveTransient);
        assert_eq!(
            runner_health("unexpected", true),
            RunnerHealth::LeaveTransient
        );
    }

    #[test]
    fn test_zoned_ids_keep_unknown_zone() {
        assert_eq!(
            zoned_ids("", &["vol-1".to_string()]),
            vec![ZonedId {
                zone: None,
                id: "vol-1".to_string()
            }]
        );
        assert_eq!(
            zoned_ids("fr-par-2", &["vol-1".to_string()]),
            vec![ZonedId {
                zone: Some("fr-par-2".to_string()),
                id: "vol-1".to_string()
            }]
        );
    }

    #[test]
    fn test_cleanup_volumes_keeps_preexisting_pending_entries() {
        let pending = vec![ZonedId {
            zone: Some("pl-waw-1".to_string()),
            id: "vol-old".to_string(),
        }];
        let runner = RunnerState::new(
            "srv-1".to_string(),
            "runner".to_string(),
            "fr-par-1".to_string(),
            vec!["vol-current".to_string()],
            vec![],
        );
        let volumes = cleanup_volumes(
            "fr-par-1",
            &["vol-current".to_string()],
            Some(&runner),
            pending,
        );

        assert_eq!(volumes.len(), 2);
        assert!(volumes.iter().any(|volume| volume.id == "vol-old"));
        assert!(volumes.iter().any(|volume| volume.id == "vol-current"));
    }

    #[test]
    fn test_cleanup_ips_keeps_preexisting_pending_entries() {
        let pending = vec![ZonedId {
            zone: Some("pl-waw-1".to_string()),
            id: "ip-old".to_string(),
        }];
        let runner = RunnerState::new(
            "srv-1".to_string(),
            "runner".to_string(),
            "fr-par-1".to_string(),
            vec![],
            vec!["ip-current".to_string()],
        );
        let ips = cleanup_ips(
            "fr-par-1",
            &["ip-current".to_string()],
            Some(&runner),
            pending,
        );

        assert_eq!(ips.len(), 2);
        assert!(ips.iter().any(|ip| ip.id == "ip-old"));
        assert!(ips.iter().any(|ip| ip.id == "ip-current"));
    }

    #[test]
    fn test_choose_delete_zone() {
        assert_eq!(
            choose_delete_zone("fr-par-1", true, None),
            Some("fr-par-1".to_string())
        );
        assert_eq!(
            choose_delete_zone("fr-par-1", false, Some("fr-par-2".to_string())),
            Some("fr-par-2".to_string())
        );
        assert_eq!(
            choose_delete_zone("", false, Some("fr-par-3".to_string())),
            Some("fr-par-3".to_string())
        );
        assert_eq!(choose_delete_zone("", false, None), None);
        assert_eq!(choose_delete_zone("fr-par-1", false, None), None);
    }

    #[test]
    fn test_merge_zoned_ids_dedupes_by_id() {
        let base = vec![ZonedId {
            zone: Some("fr-par-1".to_string()),
            id: "vol-1".to_string(),
        }];
        let extra = vec![
            ZonedId {
                zone: Some("fr-par-2".to_string()),
                id: "vol-1".to_string(),
            },
            ZonedId {
                zone: None,
                id: "vol-2".to_string(),
            },
        ];
        assert_eq!(
            merge_zoned_ids(base, extra),
            vec![
                ZonedId {
                    zone: Some("fr-par-1".to_string()),
                    id: "vol-1".to_string()
                },
                ZonedId {
                    zone: None,
                    id: "vol-2".to_string()
                }
            ]
        );
    }
}
