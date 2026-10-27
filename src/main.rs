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
use crate::scaleway::{server_owned_ip_ids, server_volume_ids, ScalewayClient};
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

    let lookup_zone = state
        .runner
        .as_ref()
        .map(|runner| runner.zone.clone())
        .filter(|zone| !zone.is_empty())
        .unwrap_or_else(|| config.scaleway.primary_zone().to_string());
    let scaleway_server = scaleway_client
        .find_server_by_name(&lookup_zone, server_name)
        .await?;

    match (&state.runner, scaleway_server) {
        // State and Scaleway match - server exists
        (Some(runner), Some(server)) if runner.server_id == server.id => {
            info!(
                "State verified: Server {} (ID: {}) exists in zone {}, running for {} minutes",
                server.name,
                server.id,
                lookup_zone,
                runner.uptime_minutes()
            );
            if runner.zone != lookup_zone {
                let mut updated = runner.clone();
                updated.zone = lookup_zone.to_string();
                state.set_runner(updated);
            }
        }

        // State says server exists, but Scaleway doesn't know it anymore
        (Some(runner), None) => {
            warn!(
                "State inconsistency: Server {} (ID: {}) no longer exists at Scaleway!",
                runner.server_name, runner.server_id
            );
            let volumes = zoned_volumes(runner);
            let failed = scaleway_client.delete_volumes(&volumes).await;
            state.set_pending_volumes(failed);
            warn!("Clearing state...");
            state.clear_runner();
        }

        // State says server exists, but with different ID (very unlikely)
        (Some(runner), Some(server)) => {
            warn!(
                "State inconsistency: State knows server ID {}, Scaleway has ID {}!",
                runner.server_id, server.id
            );
            let volumes = zoned_volumes(runner);
            let failed = scaleway_client.delete_volumes(&volumes).await;
            state.set_pending_volumes(failed);
            warn!("Updating state with Scaleway data (creation time unknown)...");
            let volume_ids = server_volume_ids(&server);
            let ip_ids = server_owned_ip_ids(&server);
            state.set_runner(RunnerState::new(
                server.id,
                server.name,
                lookup_zone.to_string(),
                volume_ids,
                ip_ids,
            ));
        }

        // State says no server, but Scaleway has one (emergency recovery)
        (None, Some(server)) => {
            warn!(
                "Orphaned server found: {} (ID: {}) in zone {} - not in state!",
                server.name, server.id, lookup_zone
            );
            warn!("Adding to state (creation time unknown)...");
            let volume_ids = server_volume_ids(&server);
            let ip_ids = server_owned_ip_ids(&server);
            state.set_runner(RunnerState::new(
                server.id,
                server.name,
                lookup_zone.to_string(),
                volume_ids,
                ip_ids,
            ));
        }

        // All OK - no server
        (None, None) => {
            info!("No server active - state is consistent");
        }
    }

    Ok(())
}

/// Builds zoned IDs from a runner's recorded volumes.
fn zoned_volumes(runner: &RunnerState) -> Vec<ZonedId> {
    let zone = if runner.zone.is_empty() {
        None
    } else {
        Some(runner.zone.clone())
    };
    runner
        .volume_ids
        .iter()
        .cloned()
        .map(|id| ZonedId {
            zone: zone.clone(),
            id,
        })
        .collect()
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
    let zone = if runner.zone.is_empty() {
        warn!(
            "Runner state has no zone; deleting in primary zone {}",
            config.scaleway.primary_zone()
        );
        config.scaleway.primary_zone().to_string()
    } else {
        runner.zone.clone()
    };
    let uptime = runner.uptime_minutes();

    info!(
        "Deleting runner server in zone {} (reason: {})",
        zone, reason
    );

    scaleway_client
        .terminate_server(&zone, &server_id)
        .await
        .context("Error terminating server")?;

    let mut to_delete: Vec<ZonedId> = volume_ids
        .into_iter()
        .map(|id| ZonedId {
            zone: Some(zone.clone()),
            id,
        })
        .collect();
    to_delete.extend(state.pending_volumes());
    let failed = scaleway_client.delete_volumes(&to_delete).await;
    state.set_pending_volumes(failed);

    let mut ips_to_delete: Vec<ZonedId> = ip_ids
        .into_iter()
        .map(|id| ZonedId {
            zone: Some(zone.clone()),
            id,
        })
        .collect();
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
}
