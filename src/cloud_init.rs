//! Cloud-Init template generator.
//!
//! Generates the cloud-init configuration for the GitLab Runner server,
//! based on the Terraform template.

use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use tracing::{info, warn};

/// Docker-Compose configuration - included at compile time.
const DOCKER_COMPOSE: &str = include_str!("../assets/docker-compose.yml");

/// Parses runner.toml, sets `run_untagged`/`protected` on every
/// `[[runners]]` entry, and re-serializes it.
///
/// GitLab Runner reads `run_untagged` and `protected` per runner, inside each
/// `[[runners]]` table. The runner.toml rendered by the Ansible role already
/// declares that array of tables, so a standalone `[runners]` table is a TOML
/// error. Injecting the keys into the existing entries also lets a caller
/// override values already present there.
///
/// If the input has no `[[runners]]` entry (e.g. a minimal file), one is
/// created so the settings still take effect.
fn inject_runner_settings(runner_config: &str, run_untagged: bool, protected: bool) -> String {
    let mut doc: toml::Value = match toml::from_str(runner_config) {
        Ok(value) => value,
        Err(error) => {
            // Fall back to the raw config rather than dropping the runner: a
            // parse failure here would otherwise be silent. The previous
            // append-based behavior is not used because it produced invalid
            // TOML whenever the file already had `[[runners]]`.
            warn!(
                "Could not parse runner.toml to inject run_untagged/protected, \
                 passing it through unchanged: {error}"
            );
            return runner_config.to_string();
        }
    };

    let table = doc
        .as_table_mut()
        .expect("runner.toml top level is always a table");

    let runners = table
        .entry("runners")
        .or_insert_with(|| toml::Value::Array(Vec::new()));

    let runners = if let Some(array) = runners.as_array_mut() {
        array
    } else {
        // `runners` exists but is a scalar/table; replace it with the array
        // GitLab Runner expects.
        *runners = toml::Value::Array(Vec::new());
        runners.as_array_mut().expect("just assigned an array")
    };

    if runners.is_empty() {
        runners.push(toml::Value::Table(toml::map::Map::new()));
    }

    for runner in runners.iter_mut() {
        let entry = runner.as_table_mut().expect("runners entries are tables");
        entry.insert(
            "run_untagged".to_string(),
            toml::Value::Boolean(run_untagged),
        );
        entry.insert("protected".to_string(), toml::Value::Boolean(protected));
    }

    toml::to_string(&doc).expect("serializing a parsed toml::Value cannot fail")
}

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

    // Inject our settings into the existing `[[runners]]` entries. Appending a
    // `[runners]` table (the earlier behavior) collides with the array of
    // tables the runner.toml already declares and makes gitlab-runner reject
    // the file with "Key 'runners' has already been defined".
    let full_config = inject_runner_settings(runner_config, run_untagged, protected);

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

    /// A realistic runner.toml, as rendered by the Ansible role. It already
    /// contains a `[[runners]]` array-of-tables, which is why appending a
    /// second `[runners]` table produces invalid TOML.
    const REAL_RUNNER_TOML: &str = r#"concurrent = 3
check_interval = 0
shutdown_timeout = 0

[session_server]
  session_timeout = 1800

[[runners]]
  name = "gitlab-runner-scaleway"
  url = "https://git.wascar.tech"
  id = 27
  token = "glrt-xxx"
  executor = "docker"
  [runners.cache]
    MaxUploadedArchiveSize = 0
  [runners.docker]
    image = "alpine:latest"
    privileged = true
"#;

    /// Extracts and decodes the base64 runner config embedded in the
    /// cloud-init YAML written to /srv/gitlab-runner/config/config.toml.
    fn embedded_runner_config(cloud_init: &str) -> String {
        let mut lines = cloud_init.lines();
        while let Some(line) = lines.next() {
            if line.contains("path: /srv/gitlab-runner/config/config.toml") {
                for entry_line in lines.by_ref() {
                    let trimmed = entry_line.trim();
                    if let Some(content) = trimmed.strip_prefix("content: ") {
                        return String::from_utf8(BASE64.decode(content).expect("valid base64"))
                            .unwrap();
                    }
                }
            }
        }
        panic!("config.toml entry not found in cloud-init");
    }

    #[test]
    fn generated_config_is_valid_toml() {
        let result = generate_cloud_init(REAL_RUNNER_TOML, true, false);
        let config = embedded_runner_config(&result);

        let parsed: toml::Value = toml::from_str(&config).unwrap_or_else(|e| {
            panic!("generated runner config is not valid TOML: {e}\n---\n{config}")
        });

        // The injected settings must land inside the existing [[runners]] entry.
        let runners = parsed
            .get("runners")
            .and_then(|r| r.as_array())
            .expect("runners array present");
        assert_eq!(runners.len(), 1, "should not add a duplicate runners entry");
        assert_eq!(
            runners[0].get("run_untagged").and_then(|v| v.as_bool()),
            Some(true)
        );
        assert_eq!(
            runners[0].get("protected").and_then(|v| v.as_bool()),
            Some(false)
        );
    }

    #[test]
    fn generated_config_preserves_existing_runners_fields() {
        let result = generate_cloud_init(REAL_RUNNER_TOML, false, true);
        let config = embedded_runner_config(&result);
        let parsed: toml::Value = toml::from_str(&config).expect("valid TOML");

        let runner = &parsed.get("runners").unwrap().as_array().unwrap()[0];
        assert_eq!(runner.get("id").and_then(|v| v.as_integer()), Some(27));
        assert_eq!(
            runner.get("name").and_then(|v| v.as_str()),
            Some("gitlab-runner-scaleway")
        );
        assert_eq!(
            runner.get("run_untagged").and_then(|v| v.as_bool()),
            Some(false)
        );
        assert_eq!(
            runner.get("protected").and_then(|v| v.as_bool()),
            Some(true)
        );
    }

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
    fn config_without_runners_entry_still_gets_settings() {
        // A minimal runner.toml without `[[runners]]` should still produce a
        // valid config carrying the injected settings.
        let result = generate_cloud_init("concurrent = 1\ncheck_interval = 0", false, true);
        let config = embedded_runner_config(&result);
        let parsed: toml::Value = toml::from_str(&config).expect("valid TOML");

        let runners = parsed.get("runners").and_then(|r| r.as_array()).unwrap();
        assert_eq!(runners.len(), 1);
        assert_eq!(
            runners[0].get("run_untagged").and_then(|v| v.as_bool()),
            Some(false)
        );
        assert_eq!(
            runners[0].get("protected").and_then(|v| v.as_bool()),
            Some(true)
        );
    }

    #[test]
    fn invalid_runner_toml_passes_through_unchanged() {
        let broken = "this is not valid = toml =";
        let result = generate_cloud_init(broken, true, false);
        assert_eq!(embedded_runner_config(&result), broken);
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
