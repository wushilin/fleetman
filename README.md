# Fleetman

Fleetman is a lightweight **controller + agent** system for deploying “cells” (services) to nodes using **S3-compatible object storage** as the control plane.

- **`fleetman`**: controller (Axum web UI + API). Manages buckets, manifests, cells, versions, overrides, and publishes deployments by writing objects to storage.
- **`fleetagent`**: agent daemon running on nodes. Polls for triggers, downloads the desired version snapshot, deploys it via a selected profile (`systemd`, `supervisor`, or `deploy`), and reports health/status back to storage.

## What it does (high level)

- **Declarative “manifests”** define the files to deploy, permissions, and a runtime profile.
- **Cells** map a manifest to a specific node + cell id.
- **Publishing** creates an immutable per-cell snapshot under `cells/<node>/<cell>/versions/<version>/...` and updates `trigger.json` so agents deploy/redeploy that snapshot.
- **Agent heartbeat** writes a status JSON periodically to `agents/<node_id>.json` so the controller can show which agents are alive and what they’re running.

## Binaries

- **Controller**: `fleetman` (web portal + API)
- **Agent**: `fleetagent` (daemon)

Build:

```bash
cargo build --release
```

## Configuration files

- **Controller config**: `controller_config.yaml`
  - Example: `controller_config.example.yaml` (safe, credential placeholders)
- **Agent config**: `agent_config.yaml` (see `agent_config.example.yaml`)

### Credential safety

Do **not** commit real credentials.

- `controller_config.yaml` usually contains real `access_key` / `secret_key` values and should be treated as **local-only**.
- This repo’s `.gitignore` ignores `controller_config.yaml` by default.

## Object storage layout (key paths)

Fleetman standardizes S3 keys under a configured **bucket + prefix**:

- **Agent status / heartbeat**
  - `/<prefix>/agents/<node_id>.json`
- **Cell state**
  - `/<prefix>/cells/<node_id>/<cell_id>/trigger.json`
  - `/<prefix>/cells/<node_id>/<cell_id>/versions/<version>/manifest.json`
  - `/<prefix>/cells/<node_id>/<cell_id>/versions/<version>/...` (snapshotted deployment + overrides)
  - `/<prefix>/cells/<node_id>/<cell_id>/overrides/...` (editable overrides area; snapshots are copied into versions on publish)

## Deployment profiles

Fleetagent supports different ways to run services:

- **`systemd`**
  - Deploys files, installs/updates a `fleetman-<cell>.service`, and checks running state via `systemctl`.
- **`supervisor`**
  - Deploys files, installs/updates a `fleetman-<cell>` supervisor program, and checks running state via `supervisorctl`.
- **`deploy`**
  - Deploys files into a target folder only (no process manager); running state is reported as **N/A**.

On startup (and during periodic reporting), the agent reconciles its state with the host by scanning:

- systemd unit files in `systemd.service_folder`
- supervisor configs in `supervisor.conf_d_folder`
- deployed folders in `deploy.target_folder`

## Web UI

The controller UI includes:

- **Manifests / Deployments / Cells** management
- **Agents** page:
  - Active/Warning/Dead classification based on last heartbeat timestamp
  - Shows deployed cells, versions, and running status where applicable
  - Allows deleting stale agent status files

## Notes on version immutability

Publishing a version to a cell is intended to be **immutable**:

- If `cells/<node>/<cell>/versions/<version>/manifest.json` already exists, publishing that same version will **not rebuild** the snapshot; it will only **rewrite `trigger.json`** to force a redeploy/restart.
- If you truly need to rebuild an existing version snapshot, delete that version from the cell first (or publish a new version).

## Running locally (quick sketch)

Controller:

```bash
cp controller_config.example.yaml controller_config.yaml
# edit controller_config.yaml and fill in bucket + credentials
cargo run --bin fleetman
```

Agent (on a node):

```bash
cp agent_config.example.yaml agent_config.yaml
# edit agent_config.yaml (node id, bucket, folders, profile settings)
cargo run --bin fleetagent
```

## License

Licensed under the **Apache License, Version 2.0** (Apache-2.0). See `LICENSE` and `NOTICE`.


