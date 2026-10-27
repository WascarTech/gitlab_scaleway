//! Zone fallback provisioning.
//!
//! Reserves an IPv4 in each candidate zone before creating an Instance, so a
//! zone that cannot provide an address is skipped without creating (and
//! billing) an unreachable server.

use anyhow::{bail, Result};
use tracing::{error, info, warn};

use crate::config::ScalewayConfig;
use crate::scaleway::{
    classify_creation_failure, server_has_public_ipv4, server_volume_ids, CreationFailure,
    ScalewayClient, ScalewayError, Server,
};
use crate::state::{OrchestratorState, RunnerState, ZonedId};

/// A fully provisioned runner.
pub struct ProvisionedRunner {
    pub zone: String,
    pub server: Server,
    pub ip_ids: Vec<String>,
}

enum AttemptError {
    NextZone(String),
    Abort(anyhow::Error),
}

/// Provisions a runner in the first zone that can provide an address.
pub async fn create_with_fallback(
    client: &ScalewayClient,
    config: &ScalewayConfig,
    runner_name: &str,
    cloud_init: &str,
    state: &mut OrchestratorState,
) -> Result<ProvisionedRunner> {
    crate::config::validate_zones(&config.zones).map_err(|e| anyhow::anyhow!(e))?;
    crate::scaleway::validate_config(config).map_err(|e| anyhow::anyhow!(e))?;

    let mut failures: Vec<(String, String)> = Vec::new();

    for (index, zone) in config.zones.iter().enumerate() {
        info!(
            "Provisioning attempt {}/{} in zone {}",
            index + 1,
            config.zones.len(),
            zone
        );

        match try_zone(client, zone, runner_name, cloud_init, state).await {
            Ok(runner) => {
                info!("Runner will run in zone {}", runner.zone);
                return Ok(runner);
            }
            Err(AttemptError::NextZone(reason)) => {
                warn!("Zone {} unavailable: {}", zone, reason);
                failures.push((zone.clone(), reason));
            }
            Err(AttemptError::Abort(error)) => return Err(error),
        }
    }

    bail!("{}", format_attempt_failures(&failures))
}

/// Formats the per-zone reasons for an all-zones-failed error.
fn format_attempt_failures(failures: &[(String, String)]) -> String {
    let details = failures
        .iter()
        .map(|(zone, reason)| format!("{}: {}", zone, reason))
        .collect::<Vec<_>>()
        .join("; ");
    format!("Could not provision runner in any zone ({})", details)
}

async fn try_zone(
    client: &ScalewayClient,
    zone: &str,
    runner_name: &str,
    cloud_init: &str,
    state: &mut OrchestratorState,
) -> Result<ProvisionedRunner, AttemptError> {
    // Reserve the address first: this fails cheaply in a zone without IPv4.
    let ip = match client.create_ip(zone).await {
        Ok(ip) => ip,
        Err(e) => return Err(classify_failure(e)),
    };

    let server = match client
        .create_server_stopped(zone, runner_name, &ip.id)
        .await
    {
        Ok(server) => server,
        Err(e) => {
            release_ip(client, state, zone, &ip.id).await;
            return Err(classify_failure(e));
        }
    };

    match client.prepare_server(zone, &server, cloud_init).await {
        Ok(server) => {
            if !server_has_public_ipv4(&server) {
                warn!(
                    "Server {} in zone {} has no public IPv4 after power-on",
                    server.id, zone
                );
                return Err(handle_failed_attempt(
                    client,
                    state,
                    zone,
                    &server,
                    &ip.id,
                    "no public IPv4 assigned",
                )
                .await);
            }

            Ok(ProvisionedRunner {
                zone: zone.to_string(),
                server,
                ip_ids: vec![ip.id],
            })
        }
        Err(e) => {
            warn!(
                "Failed to prepare server {} in zone {}: {}",
                server.id, zone, e
            );
            Err(handle_failed_attempt(
                client,
                state,
                zone,
                &server,
                &ip.id,
                &format!("server preparation failed: {}", e),
            )
            .await)
        }
    }
}

/// Cleans up a failed attempt and decides whether another zone may be tried.
async fn handle_failed_attempt(
    client: &ScalewayClient,
    state: &mut OrchestratorState,
    zone: &str,
    server: &Server,
    ip_id: &str,
    reason: &str,
) -> AttemptError {
    if cleanup_attempt(client, state, zone, server, ip_id).await {
        AttemptError::NextZone(reason.to_string())
    } else {
        track_server(state, zone, server, ip_id);
        AttemptError::Abort(anyhow::anyhow!(
            "Server {} in zone {} could not be cleaned up after a provisioning failure; \
             tracking it for later cleanup: {}",
            server.id,
            zone,
            reason
        ))
    }
}

/// Terminates the server and releases its volumes and IP.
///
/// Returns true when the server is gone, which makes it safe to try another
/// zone. Volume/IP deletion failures are persisted for retry, so they do not
/// block the fallback.
async fn cleanup_attempt(
    client: &ScalewayClient,
    state: &mut OrchestratorState,
    zone: &str,
    server: &Server,
    ip_id: &str,
) -> bool {
    if let Err(e) = client.terminate_server(zone, &server.id).await {
        error!(
            "Could not terminate server {} in zone {}: {}",
            server.id, zone, e
        );
        return false;
    }

    let volumes: Vec<ZonedId> = server_volume_ids(server)
        .into_iter()
        .map(|id| ZonedId {
            zone: Some(zone.to_string()),
            id,
        })
        .collect();
    let failed = client.delete_volumes(&volumes).await;
    state.add_pending_volumes(failed);

    let ips = vec![ZonedId {
        zone: Some(zone.to_string()),
        id: ip_id.to_string(),
    }];
    let failed = client.delete_ips(&ips).await;
    state.add_pending_ips(failed);

    true
}

/// Releases the reserved IP of an attempt that never created a server.
async fn release_ip(
    client: &ScalewayClient,
    state: &mut OrchestratorState,
    zone: &str,
    ip_id: &str,
) {
    let ip = ZonedId {
        zone: Some(zone.to_string()),
        id: ip_id.to_string(),
    };
    let failed = client.delete_ips(std::slice::from_ref(&ip)).await;
    state.add_pending_ips(failed);
}

/// Persists an un-cleanable server so it is not leaked.
fn track_server(state: &mut OrchestratorState, zone: &str, server: &Server, ip_id: &str) {
    state.set_runner(RunnerState::new(
        server.id.clone(),
        server.name.clone(),
        zone.to_string(),
        server_volume_ids(server),
        vec![ip_id.to_string()],
    ));
}

fn classify_failure(error: ScalewayError) -> AttemptError {
    match classify_creation_failure(&error) {
        CreationFailure::NextZone => AttemptError::NextZone(error.to_string()),
        CreationFailure::Abort => AttemptError::Abort(anyhow::Error::new(error)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_attempt_failures_lists_each_zone() {
        let failures = vec![
            ("fr-par-1".to_string(), "no capacity".to_string()),
            ("fr-par-2".to_string(), "no public IPv4".to_string()),
        ];
        assert_eq!(
            format_attempt_failures(&failures),
            "Could not provision runner in any zone \
             (fr-par-1: no capacity; fr-par-2: no public IPv4)"
        );
    }
}
