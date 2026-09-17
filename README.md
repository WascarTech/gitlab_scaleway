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
