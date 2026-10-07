# Volume Cleanup Fix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ensure Scaleway Block Storage root volumes are deleted when the runner VM is terminated, even if detachment is still in progress, and never lose track of undeleted volumes.

**Architecture:** `terminate` is asynchronous and only detaches `sbs_volume`; the Block Storage `DELETE` requires the volume state to be `available`. We wait for server deletion, then wait for volume detachment, then retry the delete. Failed volume IDs are persisted in `state.json` and retried on every poll tick and at startup reconciliation.

**Tech Stack:** Rust 2021, tokio, reqwest, serde, tracing.

**Spec:** `docs/superpowers/specs/2026-09-17-scaleway-migration-design.md`

## Global Constraints

- No new dependencies; pure-logic unit tests only (no wiremock), plus a manual smoke test.
- Run `cargo test`, `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `cargo build --release`.
- No comments in code unless already idiomatic in the file.

---

### Task 1: Persist pending volume IDs (`src/state.rs`)

**Produces:** `OrchestratorState::pending_volume_ids() -> Vec<String>`, `set_pending_volumes(Vec<String>)` (dedups, saves).

- [ ] Step 1: Write failing tests (round-trip; legacy `{"runner": null}` loads).
- [ ] Step 2: `cargo test pending_volumes` → FAIL.
- [ ] Step 3: Add `pending_volume_ids` to `PersistedState` (`#[serde(default)]`) and `OrchestratorState`; wire load/save; add methods.
- [ ] Step 4: `cargo test` → PASS.
- [ ] Step 5: Commit `feat: persist pending volume deletions in state`.

### Task 2: Wait for server deletion (`src/scaleway.rs`)

**Produces:** `wait_for_server_gone(&self, server_id: &str) -> Result<(), ScalewayError>`; `terminate_server` waits on both paths.

- [ ] Step 1: Add `TERMINATED_TIMEOUT_SECS = 180` const and `wait_for_server_gone`; rewrite `terminate_server`.
- [ ] Step 2: `cargo build` → PASS.
- [ ] Step 3: Commit `fix: wait for instance termination to complete`.

### Task 3: Block volume detach wait + retried delete (`src/scaleway.rs`)

**Produces:** `delete_volumes(&self, &[String]) -> Vec<String>`; private `get_block_volume`, `delete_volume`, `wait_for_volume_detached`; `block_volume_is_deletable`; `BlockVolumeGetResponse`; `ScalewayError::VolumeTimeout`.

- [ ] Step 1: Write failing pure-logic tests for `block_volume_is_deletable` and response parsing.
- [ ] Step 2: `cargo test block_volume` → FAIL.
- [ ] Step 3: Implement consts, error variant, types, `get_url`, and the delete flow.
- [ ] Step 4: `cargo test` → PASS.
- [ ] Step 5: Commit `fix: wait for volume detach and retry block volume deletion`.

### Task 4: Integrate retries in orchestration (`src/main.rs`)

**Produces:** `retry_pending_volumes` helper; safer `delete_runner`.

- [ ] Step 1: Add `retry_pending_volumes` before `orchestration_tick`; call it first in the tick.
- [ ] Step 2: Update `delete_runner` to merge pending IDs and persist failures.
- [ ] Step 3: `cargo test` → PASS.
- [ ] Step 4: Commit `fix: retry unresolved volume deletions each tick`.

### Task 5: Reconcile volumes during startup verification (`src/main.rs`)

- [ ] Step 1: `(Some, None)` branch cleans up recorded volumes before clearing state.
- [ ] Step 2: `(Some, Some)` mismatch branch parks old volumes before adopting.
- [ ] Step 3: `cargo test` → PASS.
- [ ] Step 4: Commit `fix: reconcile orphaned volumes at startup`.

### Task 6: Documentation (`README.md`)

- [ ] Step 1: Document wait/retry/persist behavior.
- [ ] Step 2: Commit `docs: describe volume cleanup retry behavior`.

### Task 7: Verification

- [ ] Step 1: `cargo test && cargo fmt --check && cargo clippy --all-targets -- -D warnings && cargo build --release`.
- [ ] Step 2: Manual smoke test on Scaleway (server + volume created; after idle, no leftover volume).
- [ ] Step 3: Failure-path check (restart mid-cleanup retries pending volumes).
