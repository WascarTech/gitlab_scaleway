# Plan: Scaleway zone fallback with reserved IPv4

Date: 2026-10-27
Spec: design agreed in conversation; no separate spec file
Status: approved for implementation

## Context

The orchestrator currently uses one `scaleway.zone` from `config/config.toml` and
creates instances with `dynamic_ip_required: true`. If that zone cannot provide
an IPv4 address, the instance comes up running but unreachable, and the
orchestrator never tries another zone.

This change makes zones a priority list and provisions a reserved (flexible)
IPv4 in each candidate zone before creating an instance. A zone that cannot
provide an address is skipped without creating an instance. The runner's zone
and reserved IP IDs are persisted so deletion targets the right resources.

## Global constraints

- No new crate dependencies.
- `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, and
  `cargo test` must pass at the end of every task.
- Existing `zone = "..."` configuration and old `config/state.json` files must
  keep working.
- Every zone-scoped API call must use the zone passed by the caller, never a
  field on `ScalewayClient`.
- Reserved IPs are deleted after every runner termination and after every
  failed provisioning attempt; failures go to the persisted pending list.

## Task 1: Zone list configuration

Files: `src/config.rs`, call sites in `src/scaleway.rs` and `src/main.rs` only
as needed to compile.

Steps:

1. Add tests:
   - `zones = ["fr-par-1", "fr-par-2"]` parses to two entries.
   - legacy `zone = "fr-par-1"` parses to `vec!["fr-par-1"]`.
   - `zones = [" fr-par-1 ", "fr-par-2", "fr-par-1", ""]` normalizes to
     `["fr-par-1", "fr-par-2"]`.
   - `normalize_zones(vec![])` and `validate_zones(&[])` return empty / error.
   - a config with neither key yields an empty list and fails validation.
2. Replace `ScalewayConfig.zone: String` with `zones: Vec<String>` using
   `#[serde(default, alias = "zone", deserialize_with = "deserialize_zones")]`.
3. Add `normalize_zones(Vec<String>) -> Vec<String>` (trim, drop empty,
   de-duplicate preserving order) and `validate_zones(&[String]) -> Result<(), String>`.
4. Add `ConfigError::Validation(String)` and call `validate_zones` from
   `Config::load`, returning a message that names both `scaleway.zones` and
   `scaleway.zone`.
5. Add `ScalewayConfig::primary_zone(&self) -> &str` returning the first entry
   or `""`.
6. Update logging in `Config::load` and `ScalewayClient::new` to print the
   priority list.
7. Update all struct literals in tests.

Expected: new tests pass; `cargo test` passes.

Commit: `feat: accept a priority list of Scaleway zones`

## Task 2: Zone-aware runner and pending-resource state

Files: `src/state.rs`, plus call sites in `src/main.rs`/tests to compile.

Steps:

1. Add tests:
   - `RunnerState` round-trips with `zone` and `ip_ids`.
   - legacy runner JSON without `zone`/`ip_ids` loads with empty values.
   - `ZonedId` deserializes from a bare string (`zone == None`) and from an
     object (`zone == Some("fr-par-1")`), and serializes back.
   - legacy `pending_volume_ids: ["vol-1"]` loads into `pending_volumes` with
     `zone == None`.
   - `pending_volumes`/`pending_ips` round-trip and de-duplicate.
2. Add:
   ```rust
   pub struct ZonedId { pub zone: Option<String>, pub id: String }
   ```
   with a custom `Deserialize` accepting a string or `{zone, id}`. Skip
   serializing `zone` when `None`.
3. Add `zone: String` and `ip_ids: Vec<String>` to `RunnerState`, both
   `#[serde(default)]`. Change `RunnerState::new` to take
   `(server_id, server_name, zone, volume_ids, ip_ids)`.
4. In `PersistedState`, rename the field to `pending_volumes: Vec<ZonedId>` with
   `alias = "pending_volume_ids"`, and add `pending_ips: Vec<ZonedId>`.
5. Add accessors and mutators:
   `pending_volumes`, `set_pending_volumes`, `add_pending_volumes`,
   `pending_ips`, `set_pending_ips`, `add_pending_ips`. De-duplicate by
   `(zone, id)`.

Expected: state tests pass; old JSON fixtures still load.

Commit: `feat: persist runner zone and zoned pending cleanup`

## Task 3: Zone-parameterized Scaleway client and IP lifecycle

Files: `src/scaleway.rs`.

Steps:

1. Add tests for pure helpers:
   - `build_create_request` includes `public_ips == ["ip-1"]` and
     `dynamic_ip_required == false`.
   - `zone_url("fr-par-2", "/servers")` contains `/zones/fr-par-2/`.
   - `server_has_public_ipv4` is true only for a non-empty `inet` entry.
   - `server_owned_ip_ids` returns only non-dynamic IPs tagged `gitlab-runner`.
   - error classification table: 400/404/409/408/500/502/503/504 → next zone;
     401/403/429/other 4xx → abort; `InvalidConfig`/`Request`/`Parse` → abort.
   - `build_create_ip_request` sets `project`, `type = "routed_ipv4"`, and the
     `gitlab-runner` tag.
2. Make every URL helper and API method take an explicit `zone: &str`:
   `find_server_by_name`, `get_server`, `wait_for_state`, `terminate_server`,
   `delete_server`, `get_block_volume`, `wait_for_volume_detached`,
   `delete_volume`, `update_volume_iops`, `wait_for_server_gone`.
3. Add `PublicIp` fields `id`, `dynamic` (default true), `tags` (default empty).
4. Add `Ip { id, address }`, `CreateIpRequest`, `IpResponse`,
   `create_ip(zone) -> Result<Ip, ScalewayError>` calling
   `POST /instance/v1/zones/{zone}/ips`.
5. Add `delete_ip(zone, id)` (404 = success) and
   `delete_ips(&[ZonedId]) -> Vec<ZonedId>`; for `zone == None`, try each
   configured zone and stop at the first successful delete.
6. Replace the monolithic `create_server` with:
   - `create_server_stopped(zone, name, ip_id) -> Result<Server>`
   - `prepare_server(zone, &server, cloud_init) -> Result<Server>` (wait stopped,
     apply IOPS, set cloud-init, power on, wait running)
   Keep `apply_root_volume_iops`, cloud-init patch and `action` private.
7. Change `CreateServerRequest` to `public_ips: Vec<String>`,
   `dynamic_ip_required: false`; update `build_create_request` to take the IP ID.
8. Make volume deletion zone-aware with `ZonedId`; for `zone == None`, find the
   volume by GET across configured zones before deleting.
9. Add `classify_creation_failure(&ScalewayError) -> CreationFailure`
   (`NextZone` / `Abort`) and `ScalewayClient::validate_config()`.

Expected: scaleway tests pass; no remaining `self.config.zone` references.

Commit: `feat: reserve IPs and make Scaleway operations zone-aware`

## Task 4: Zone fallback provisioning loop

Files: new `src/provisioning.rs`, `src/main.rs` (`mod provisioning;`).

Steps:

1. Add `ProvisionedRunner { zone: String, server: Server, ip_ids: Vec<String> }`.
2. Add `create_with_fallback(client, config, runner_name, cloud_init, state) -> Result<ProvisionedRunner>`:
   - validate config once before the loop;
   - for each zone in priority order, log the attempt;
   - call `try_zone`; `NextZone` logs and continues, `Abort` returns immediately;
   - if all zones fail, return an error listing each zone and reason.
3. `try_zone`:
   - `create_ip(zone)`; failure → classify (next zone or abort), no cleanup.
   - `create_server_stopped(zone, name, ip.id)`; failure → release the IP, then
     classify.
   - `prepare_server`; failure → clean up the attempt, then next zone.
   - verify `server_has_public_ipv4`; missing → clean up, next zone.
   - on success return the zone, prepared server and `[ip.id]`.
4. `cleanup_attempt` terminates the server and waits for it to disappear. If
   termination succeeds, delete volumes and the reserved IP, adding failures to
   the state pending lists, and report that it is safe to try the next zone. If
   termination fails, return false.
5. When cleanup is not safe, persist the server as the active runner (zone,
   volumes, IP) and abort, so no untracked instance is left behind.
6. Add unit tests for a pure `format_attempt_failures(&[(String, String)])`
   helper if the loop itself remains integration-tested.

Expected: module compiles; pure helper tests pass; `cargo test` passes.

Commit: `feat: fall back across zones when provisioning fails`

## Task 5: Main wiring, deletion and startup recovery

Files: `src/main.rs`.

Steps:

1. `create_runner` calls `provisioning::create_with_fallback`, persists
   `RunnerState::new(..., zone, volume_ids, ip_ids)`, and logs the zone in the
   START flow.
2. `retry_pending_resources` retries both pending volumes and pending IPs.
3. `delete_runner` takes `&Config`, uses `runner.zone` (falling back to the
   primary zone with a warning if empty), terminates there, then deletes runner
   and pending volumes and IPs, persists failures, clears state.
4. Rewrite `verify_state_with_scaleway`:
   - known state zone → look up the name there;
   - legacy state without zone → search configured zones in priority order;
   - no state runner → search all zones for an orphan;
   - same ID → keep and ensure the zone is recorded;
   - different ID → clean up the old runner's volumes/IPs, adopt the found one;
   - not found → clean up the old runner's volumes/IPs, clear state;
   - adopt sets zone, volumes and `server_owned_ip_ids`.
5. Health check after adoption/resolution: a `running` server without an `inet`
   address, or a `stopped`/`stopped in place` server, is terminated with its
   volumes and IPs; state is cleared so the next tick provisions a new runner.
   Leave `starting`/`stopping`/`locked` alone and log.
6. Update `main` and `maybe_delete_runner` call sites for the new signatures.

Expected: `cargo test`, `cargo clippy`, `cargo fmt --check` pass.

Commit: `feat: recover and delete runners using their recorded zone`

## Task 6: Documentation and version

Files: `README.md`, `CONFIG_EXAMPLE_CONTENT` in `src/main.rs`, `Cargo.toml`.

Steps:

1. Replace the `zone` example with a `zones` priority list and document that
   `zone` is still accepted.
2. Document fallback triggers, reserved-IP lifecycle, all-zones-failed retry,
   cross-zone image-label guidance, and the upgrade note to keep the previous
   zone in the list for the first run.
3. Bump `Cargo.toml` version to `0.4.0`.

Expected: docs mention `zones`, reserved IPs and the migration note.

Commit: `docs: document zone fallback and reserved IPs`

## Review focus

- No path leaves a created server untracked before another one is created.
- Every volume and IP delete uses the resource's own zone.
- Legacy state and legacy config still load.
- Auth/config/rate-limit failures abort instead of iterating zones.
- A failed attempt releases its reserved IP.

## Verification

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
cargo build --release
```

Manual smoke test (requires Scaleway credentials): configure a deliberately
unavailable first zone and a valid second zone, trigger a job, confirm the
runner lands in the second zone, and confirm server, volume and IP are gone
after termination.
