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
use crate::scaleway::{server_volume_ids, ScalewayClient};
use crate::state::{OrchestratorState, RunnerState};

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
# tag_filter = ["hetzner", "my-runner-tag"]

[hetzner]
# Hetzner Cloud API Token
token = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
# Server type (e.g., cx22, ccx23, cpx31)
server_type = "ccx23"
# Datacenter location (nbg1, fsn1, hel1, ash, hil)
location = "nbg1"
# OS Image
image = "ubuntu-24.04"
# Name of the SSH key in Hetzner Cloud
ssh_key_name = "my-ssh-key"

[runner]
# Name of the server in Hetzner Cloud
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
    let cloud_init = generate_cloud_init(&runner_config);

    // Load orchestrator state with persistence
    let mut state =
        OrchestratorState::with_persistence(STATE_PATH).context("Error loading state")?;

    // Verify that saved state matches Scaleway
    verify_state_with_scaleway(&scaleway_client, &config.runner.name, &mut state).await?;

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
    server_name: &str,
    state: &mut OrchestratorState,
) -> Result<()> {
    info!("Verifying state with Scaleway API...");

    let scaleway_server = scaleway_client.find_server_by_name(server_name).await?;

    match (&state.runner, scaleway_server) {
        // State and Scaleway match - server exists
        (Some(runner), Some(server)) if runner.server_id == server.id => {
            info!(
                "State verified: Server {} (ID: {}) exists, running for {} minutes",
                server.name,
                server.id,
                runner.uptime_minutes()
            );
        }

        // State says server exists, but Scaleway doesn't know it anymore
        (Some(runner), None) => {
            warn!(
                "State inconsistency: Server {} (ID: {}) no longer exists at Scaleway!",
                runner.server_name, runner.server_id
            );
            warn!("Clearing state...");
            state.clear_runner();
        }

        // State says server exists, but with different ID (very unlikely)
        (Some(runner), Some(server)) => {
            warn!(
                "State inconsistency: State knows server ID {}, Scaleway has ID {}!",
                runner.server_id, server.id
            );
            warn!("Updating state with Scaleway data (creation time unknown)...");
            let volume_ids = server_volume_ids(&server);
            state.set_runner(RunnerState::new(server.id, server.name, volume_ids));
        }

        // State says no server, but Scaleway has one (emergency recovery)
        (None, Some(server)) => {
            warn!(
                "Orphaned server found: {} (ID: {}) - not in state!",
                server.name, server.id
            );
            warn!("Adding to state (creation time unknown)...");
            let volume_ids = server_volume_ids(&server);
            state.set_runner(RunnerState::new(server.id, server.name, volume_ids));
        }

        // All OK - no server
        (None, None) => {
            info!("No server active - state is consistent");
        }
    }

    Ok(())
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
