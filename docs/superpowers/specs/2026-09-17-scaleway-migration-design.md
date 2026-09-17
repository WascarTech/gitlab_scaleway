# Design: Migrate GitLab Runner Orchestrator from Hetzner Cloud to Scaleway

Date: 2026-09-17
Status: Approved for planning
Scope: Replace the Hetzner Cloud provider with Scaleway Instances (v1 API)

## 1. Summary

`gitlab_hcloud` provisions a Hetzner Cloud server as a GitLab CI runner when
pipelines are active and deletes it when they finish, with billing-cycle-aware
deletion. This migration replaces the Hetzner provider with Scaleway:

- A new Scaleway Instances v1 client replaces the Hetzner client.
- The crate, configuration section, binary, state schema, and documentation are
  renamed from Hetzner (`hcloud`) to Scaleway.
- The billing-optimized deletion logic is replaced by a simpler rule: terminate
  once the minimum lifetime has elapsed and no jobs are active.
- The root volume size and type become configurable (default 50 GB,
  `sbs_volume`).

The GitLab polling logic, cloud-init generation, CSV logging, log rotation, and
persistence behavior are unchanged.

## 2. Goals

- Provision Scaleway Instances as GitLab runners on demand.
- Preserve existing behavior: poll all projects, create on active jobs, persist
  state across restarts, CSV usage log, rotating log files, debug-build fast
  delete.
- Correctly clean up billable resources on deletion (including Block Storage
  volumes that `terminate` only detaches).
- Provide a configurable root volume large enough for GitLab CI workloads
  (default 50 GB).

## 3. Non-Goals

- Multi-provider support (no `CloudProvider` trait). Hetzner is removed.
- Migrating live state from a Hetzner deployment to Scaleway. Old state files
  are ignored (see §10).
- Scaleway-specific extras: Private Networks, Security Groups, placement
  groups, images/snapshots, IOPS tuning, standby/power-off lifecycle. Idle
  servers are always terminated.
- Changing the GitLab polling or tag-filter behavior.

## 4. Decisions (with rationale)

| Decision | Choice | Rationale |
|---|---|---|
| Provider model | Replace Hetzner entirely | Repo already named `gitlab_scaleway`; no requirement to keep both. |
| Idle handling | Terminate the instance | User choice; avoids paying volume + flexible IP while idle and avoids stale instances. |
| Delete policy | Terminate once `uptime >= min_lifetime_minutes` and no active jobs | Scaleway bills a minimum of 60 minutes per start/stop period, so deleting at 20 min costs the same as deleting at 55 min; waiting adds no value. |
| SSH access | Optional `ssh_public_key` in config, injected via `AUTHORIZED_KEY=<key-with-underscores>` tag | Mirrors Hetzner's explicit named-key selection; does not depend on account-level keys. |
| Naming | Clean rename | Avoids a confusing mix of `hcloud` names with Scaleway internals. |
| Root volume | Explicit volume template, `volume_size_gb` default 50, `volume_type` default `sbs_volume` | Scaleway's ~10 GB image default is too small for Docker layer caches and build caches. |
| Volume cleanup | Store volume IDs in state; delete detached volumes after terminate | `terminate` deletes `l_ssd`/`scratch` but only detaches `sbs_volume`, which would keep billing. |

## 5. Scaleway API Reference (v1)

All endpoints use `https://api.scaleway.com` with header
`X-Auth-Token: <secret key>`.

| Operation | Method and path |
|---|---|
| List servers | `GET /instance/v1/zones/{zone}/servers` |
| Create server | `POST /instance/v1/zones/{zone}/servers` |
| Set cloud-init | `PATCH /instance/v1/zones/{zone}/servers/{id}/user_data/cloud-init` (raw text body) |
| Power on | `POST /instance/v1/zones/{zone}/servers/{id}/action` `{"action":"poweron"}` |
| Terminate | `POST /instance/v1/zones/{zone}/servers/{id}/action` `{"action":"terminate"}` |
| Delete server | `DELETE /instance/v1/zones/{zone}/servers/{id}` (server must be stopped) |
| Delete volume | `DELETE /block/v1/zones/{zone}/volumes/{volume_id}` (volume must be detached) |

Notes and constraints:

- `GET /servers?name=` performs a **prefix/substring match**, not exact match.
  The client must re-filter results for exact equality.
- Server IDs and volume IDs are UUID strings, not integers.
- `terminate` deletes `l_ssd` and `scratch` volumes and detaches `sbs_volume`;
  detached volumes are not deleted automatically.
- `DELETE` on a server leaves all volumes in place; `terminate` is the primary
  deletion path.
- Server state enum: `running`, `stopped`, `stopped in place`, `starting`,
  `stopping`, `locked`.
- The cloud-init user-data key for the v1 API is `cloud-init`, set with a raw
  text `PATCH` (not the v2alpha1 JSON `PUT .../user-data/cloud-init`).
- The v1 API only supports plain-text user data (no gzip).
- The public IPv4 is read from `public_ips[]` where `family == "inet"`
  (the singular `public_ip` field is deprecated). A server created with a
  dynamic IP has an empty `public_ips` until it is powered on.

## 6. Configuration

`config/config.toml` replaces `[hetzner]` with `[scaleway]`. `[gitlab]` and
`[runner]` are unchanged.

```toml
[gitlab]
url = "https://gitlab.example.com"
token = "glpat-xxxxxxxxxxxxxxxxxxxx"   # read_api scope
# tag_filter = ["scaleway", "my-runner-tag"]

[scaleway]
token = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"  # IAM API secret key
project_id = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
zone = "fr-par-1"                # fr-par-1/2/3, nl-ams-1/2/3, pl-waw-1/2/3, it-mil-1
server_type = "PRO2-XS"          # e.g. PLAY2-PICO, DEV1-M, PRO2-XS, PRO2-S
image = "ubuntu_noble"           # marketplace label or local image UUID
ssh_public_key = "ssh-ed25519 AAAA... user@host"   # optional; enables SSH debugging
volume_size_gb = 50              # optional; default 50, minimum 10
volume_type = "sbs_volume"       # optional; default "sbs_volume", alternative "l_ssd"

[runner]
name = "flexi-runner"
min_lifetime_minutes = 20
poll_interval_seconds = 30
```

Config structs:

- `GitLabConfig` — unchanged (`url`, `token`, `tag_filter`).
- `ScalewayConfig` — `token: String`, `project_id: String`, `zone: String`,
  `server_type: String`, `image: String`, `ssh_public_key: Option<String>`,
  `volume_size_gb: u32` (default 50), `volume_type: String` (default
  `"sbs_volume"`).
- `RunnerConfig` — unchanged.

`min_lifetime_minutes` keeps its default of 20 and its meaning: minimum time
before an idle instance can be terminated. Scaleway bills CPU Instances in full
60-minute blocks per start/stop period, so a value of 60 maximizes useful time
per billed block; the README documents this.

## 7. Module Changes

### 7.1 `src/scaleway.rs` (replaces `src/hetzner.rs`)

Client responsibilities:

- `new(config)` — construct `reqwest::Client`, log zone, server type, image,
  and volume configuration.
- HTTP helpers `get`, `post`, `patch_text`, `delete` — send `X-Auth-Token`,
  map non-2xx responses to `ScalewayError::Api { status, message }`, and map
  JSON failures to `ScalewayError::Parse`.
- `find_server_by_name(name) -> Option<Server>` — query `?name=`, then return
  only an exact-name match.
- `create_server(name, cloud_init) -> (Server, Vec<String>)`:
  1. `POST /servers` with body:
     ```json
     {
       "name": "<runner name>",
       "project": "<project_id>",
       "commercial_type": "<server_type>",
       "image": "<image>",
       "dynamic_ip_required": true,
       "tags": ["gitlab-runner", "AUTHORIZED_KEY=<key with spaces replaced by _>"],
       "volumes": {
         "0": {
           "size": <volume_size_gb * 1_000_000_000>,
           "volume_type": "<volume_type>",
           "boot": true
         }
       }
     }
     ```
     The `AUTHORIZED_KEY` tag is included only when `ssh_public_key` is set.
  2. Wait for the server to reach `stopped` (it is created stopped).
  3. `PATCH /servers/{id}/user_data/cloud-init` with the raw cloud-init text.
  4. `POST /servers/{id}/action` `{"action":"poweron"}`.
  5. Wait for state `running`.
  6. Collect and return the volume IDs from the create response.
- `terminate_server(server_id)` — `POST .../action` `{"action":"terminate"}`;
  treat 404 as success. If terminate fails (e.g. the adopted server is stopped
  or in another non-terminable state), fall back: if the server is already
  `stopped`, call `DELETE /servers/{id}`; otherwise call `poweroff`, wait for
  `stopped`, then `DELETE /servers/{id}`.
- Timeout constants: `STOPPED_TIMEOUT_SECS = 120` and
  `RUNNING_TIMEOUT_SECS = 180`, polled every 3 seconds.
- `delete_volumes(volume_ids)` — for each recorded volume, `DELETE` via the
  Block Storage API; ignore 404 (already deleted by terminate, e.g. `l_ssd`).
- `wait_for_state(server_id, desired, timeout)` — poll `GET /servers/{id}`
  every 3 seconds until the desired state or timeout. Timeout returns an
  error; the server is still tracked in state.

Data types:

```rust
pub struct Server {
    pub id: String,              // UUID
    pub name: String,
    pub state: String,
    pub public_ips: Vec<PublicIp>,
    pub volumes: HashMap<String, Volume>,  // key "0" is the root volume
}
pub struct PublicIp { pub address: String, pub family: String }
pub struct Volume { pub id: String }
```

`create_server` validates config before calling the API: `volume_size_gb` must
be at least 10 (Scaleway's floor) and `volume_type` must be `sbs_volume` or
`l_ssd`; invalid values return an error that names the offending field.

### 7.2 `src/config.rs`

- `Config` uses `scaleway: ScalewayConfig` instead of `hetzner`.
- Add `default_volume_size() -> u32 { 50 }` and
  `default_volume_type() -> String { "sbs_volume".into() }` with `#[serde(default = ...)]`.
- Logging on load reports zone, server type, and volume configuration.

### 7.3 `src/state.rs`

- `RunnerState.server_id: String` (was `u64`).
- `RunnerState.volume_ids: Vec<String>` (new; `#[serde(default)]` so a state
  file that lacks the field still loads).
- Remove `minutes_until_next_billing_cycle`, `should_delete`, `can_force_delete`,
  and `has_min_uptime` (the latter is subsumed by a simple check in `main.rs`;
  keep `uptime_minutes`).

### 7.4 `src/main.rs`

- Rename `mod hetzner` to `mod scaleway`; use `ScalewayClient` and
  `verify_state_with_scaleway`.
- Remove `BILLING_BUFFER_MINUTES`.
- `maybe_delete_runner` becomes:
  ```
  if uptime < min_lifetime_minutes: log waiting; return
  else: terminate (reason "all_pipelines_done" or "debug_immediate_delete")
  ```
  Debug builds keep immediate deletion.
- `create_runner` stores the returned volume IDs in `RunnerState`.
- `delete_runner` terminates the server, deletes recorded volumes, logs CSV
  STOP with uptime and reason, clears state.
- `verify_state_with_scaleway` preserves current behavior:
  - state and Scaleway agree → keep;
  - state has server, Scaleway does not → clear state;
  - state has different server ID → adopt Scaleway server (creation time
    unknown, volumes read from the response);
  - state empty, Scaleway has server → adopt (emergency recovery).
- Example config content (`CONFIG_EXAMPLE_CONTENT`) is updated to the
  `[scaleway]` schema.

### 7.5 `src/csv_log.rs`

- `LogEntry.server_id: Option<String>`; `log_start`/`log_stop` take `&str`.
  The CSV format is otherwise unchanged.

### 7.6 Unchanged

`src/gitlab.rs`, `src/cloud_init.rs`, `assets/docker-compose.yml`.

### 7.7 Packaging and docs

- `Cargo.toml`: `name = "gitlab_scaleway"`, description
  "Automatic Scaleway server provisioning for GitLab CI runners". Version
  bump to `0.3.0`.
- `Dockerfile`: copy `target/release/gitlab_scaleway`; rename user
  `hcloud` → `scw`.
- `docker-compose.yml`: service `scaleway-starter`.
- `README.md`: retitle for Scaleway; update architecture diagram, config
  reference, billing/quota notes (60-minute minimum billing, detached SBS
  volumes, flexible IPv4 cost), storage guidance, and a note that an old
  `config/state.json` from a Hetzner deployment is ignored and can be deleted.
- `.github/workflows/docker-publish.yml`: unchanged.

## 8. Lifecycle

1. On startup: load config, runner config, csv logger, state; verify state
   against Scaleway.
2. Every `poll_interval_seconds`: query GitLab for pending/running jobs
   (optionally filtered by tag).
3. Active jobs and no server → create server (create → cloud-init → power on),
   record state, CSV START.
4. No active jobs and a server exists → if uptime ≥ `min_lifetime_minutes`,
   terminate + delete volumes + CSV STOP + clear state; else wait.
5. Active jobs and server exists → log and continue.
6. Errors in a tick are logged and do not stop the loop.

## 9. Error Handling

- API errors carry status and body (`ScalewayError::Api`).
- A failed state verification at startup is fatal: `main` logs the error and
  exits (current behavior).
- Terminate fallback: `terminate`, else `DELETE` if already stopped, else
  `poweroff` → wait `stopped` → `DELETE`; 404 is treated as already deleted.
- Volume deletion failures are logged as warnings and do not block state
  clearing; a leaked volume is visible via the log and the Scaleway console.
- Old/incompatible `state.json` fails to deserialize, logs a warning, and the
  orchestrator starts fresh (existing behavior).

## 10. Migration Notes for Operators

- Existing `config/config.toml` must be rewritten: `[hetzner]` → `[scaleway]`
  with `project_id`, `zone`, and volume settings.
- `config/state.json` from a Hetzner deployment is not migrated; delete it or
  let the orchestrator ignore it.
- Any existing `runner.toml` continues to work unchanged.
- The runner must still use `pull_policy = ["if-not-present"]` (unchanged).

## 11. Testing and Verification

Automated:

- `src/config.rs`: defaults parsing (`volume_size_gb` absent → 50,
  `volume_type` absent → `sbs_volume`), and explicit overrides.
- `src/scaleway.rs`:
  - exact-name filtering rejects prefix matches such as `runner-2` when
    searching for `runner`;
  - `AUTHORIZED_KEY` tag formatting replaces spaces with underscores and is
    omitted when no key is configured;
  - volume size conversion (GB → bytes, multiple of 512).
- `src/state.rs`: state with `String` server ID and `volume_ids` round-trips
  through JSON; a legacy state object without `volume_ids` still deserializes.
- `src/csv_log.rs`: `String` server ID is written correctly; existing escape
  tests remain.

Commands:

- `cargo test`
- `cargo fmt --check`
- `cargo clippy --all-targets -- -D warnings`
- `cargo build --release`

Manual smoke test (requires a real Scaleway account; documented in README):

1. Configure `[scaleway]` with a valid secret key and project.
2. Run the release binary; trigger a pending job with the configured tag.
3. Confirm: server created, cloud-init applied, job runs.
4. Wait past `min_lifetime_minutes`; confirm server terminated, no volumes
   remain (console or Block Storage list), CSV STOP row written.

## 12. Risks

- **`l_ssd` availability**: local volumes only exist on DEV1/GP1 types; if a
  user configures `volume_type = "l_ssd"` with `sbs_volume`-only types such as
  PRO2, creation fails with an API error. README documents the pairings; the
  default (`sbs_volume`) works across current types.
- **Dynamic IP timing**: the public IP appears only after power-on, so IP
  logging must happen after the server reaches `running`.
- **Volume drift**: if volume IDs are lost (e.g. state file deleted while a
  server exists), adoption reads volume IDs from the server response, so
  cleanup still works.
- **Billing expectations**: users expecting Hetzner-style per-started-hour
  optimization may over-wait; README explains the 60-minute minimum block.
