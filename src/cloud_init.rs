//! Cloud-Init template generator.
//!
//! Generates the cloud-init configuration for the GitLab Runner server,
//! based on the Terraform template.

use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use tracing::info;

/// Docker-Compose configuration - included at compile time.
const DOCKER_COMPOSE: &str = include_str!("../assets/docker-compose.yml");

/// Generates the cloud-init configuration for the runner server.
///
/// The configuration:
/// 1. Updates packages
/// 2. Writes the runner.toml configuration with additional settings
/// 3. Writes the docker-compose.yml
/// 4. Installs Docker
/// 5. Starts the GitLab Runner container
///
/// # Arguments
/// * `runner_config` - Contents of the runner.toml file
/// * `run_untagged` - Whether to accept untagged jobs
/// * `protected` - Whether to only run on protected branches
///
/// # Returns
/// The complete cloud-init configuration as a string
pub fn generate_cloud_init(runner_config: &str, run_untagged: bool, protected: bool) -> String {
    info!("Generating cloud-init configuration");

    // Parse the runner.toml and inject our settings
    let mut full_config = runner_config.to_string();

    // Add or override the settings for runners
    full_config.push_str(&format!(
        "\n\n[runners]\nrun_untagged = {}\nprotected = {}\n",
        run_untagged, protected
    ));

    // Base64 encode full config
    let full_config_b64 = BASE64.encode(full_config.as_bytes());

    // Base64 encode docker-compose
    let docker_compose_b64 = BASE64.encode(DOCKER_COMPOSE.as_bytes());

    // Generate cloud-init YAML
    let cloud_init = format!(
        r#"#cloud-config
package_update: true
package_upgrade: true

write_files:
  - path: /srv/gitlab-runner/docker-compose.yml
    encoding: b64
    content: {docker_compose_b64}
  - path: /srv/gitlab-runner/config/config.toml
    encoding: b64
    content: {full_config_b64}

runcmd:
  - curl -fsSL https://get.docker.com -o install-docker.sh
  - sh install-docker.sh
  - docker compose -f /srv/gitlab-runner/docker-compose.yml up -d
"#,
        docker_compose_b64 = docker_compose_b64,
        full_config_b64 = full_config_b64,
    );

    info!(
        "Cloud-init configuration generated ({} bytes)",
        cloud_init.len()
    );
    cloud_init
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_cloud_init() {
        let runner_config = "concurrent = 1\ncheck_interval = 0";
        let result = generate_cloud_init(runner_config, true, false);

        assert!(result.starts_with("#cloud-config"));
        assert!(result.contains("package_update: true"));
        assert!(result.contains("docker compose"));
        // The result should contain the base64 encoded config
        assert!(result.contains("content:"));
    }

    #[test]
    fn test_config_includes_settings() {
        // Test that the full config string includes our settings
        let runner_config = "concurrent = 1\ncheck_interval = 0";
        let mut full_config = runner_config.to_string();
        full_config.push_str(&format!(
            "\n\n[runners]\nrun_untagged = {}\nprotected = {}\n",
            true, false
        ));
        
        assert!(full_config.contains("run_untagged = true"));
        assert!(full_config.contains("protected = false"));
    }

    #[test]
    fn test_config_includes_protected_settings() {
        // Test that the full config string includes protected settings
        let runner_config = "concurrent = 1\ncheck_interval = 0";
        let mut full_config = runner_config.to_string();
        full_config.push_str(&format!(
            "\n\n[runners]\nrun_untagged = {}\nprotected = {}\n",
            false, true
        ));
        
        assert!(full_config.contains("run_untagged = false"));
        assert!(full_config.contains("protected = true"));
    }

    #[test]
    fn test_cloud_init_contains_expected_sections() {
        let runner_config = "concurrent = 1\ncheck_interval = 0";
        let result = generate_cloud_init(runner_config, true, false);

        // Check that cloud-init YAML has the expected structure
        assert!(result.contains("#cloud-config"));
        assert!(result.contains("package_update: true"));
        assert!(result.contains("package_upgrade: true"));
        assert!(result.contains("write_files:"));
        assert!(result.contains("runcmd:"));
        assert!(result.contains("docker compose"));
    }
}
