mod agent_config;
mod agent_state;
// NOTE: systemd/supervisor templates are intentionally NOT embedded.
// Users are expected to provide templates via config `template_path` at runtime.

use agent_config::*;
use agent_state::*;
use anyhow::{Context, Result};
use aws_config::BehaviorVersion;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::primitives::ByteStream;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::{sleep, Duration, Instant, MissedTickBehavior};
use tracing::{debug, error, info, warn};

/// Check if an S3 error is retriable (transient error)
/// Returns true only for transient errors that should be retried
fn is_retriable_s3_error<E: std::fmt::Debug>(error: &E) -> bool {
    let error_str = format!("{:?}", error);
    
    // Retriable errors (transient):
    // - Network/timeout errors
    // - Server errors (5xx)
    // - Rate limiting (429)
    
    // Non-retriable errors (permanent):
    // - NotFound/NoSuchKey (404)
    // - AccessDenied (403)
    // - InvalidRequest (400)
    
    if error_str.contains("TimeoutError") || 
       error_str.contains("DispatchFailure") ||
       error_str.contains("ConnectorError") ||
       error_str.contains("ConnectionReset") {
        return true;  // Network issues - retry
    }
    
    if error_str.contains("NoSuchKey") ||
       error_str.contains("NotFound") ||
       error_str.contains("AccessDenied") ||
       error_str.contains("InvalidRequest") ||
       error_str.contains("NoSuchBucket") {
        return false;  // Permanent errors - don't retry
    }
    
    // Check for HTTP status codes in error
    if error_str.contains("429") || error_str.contains("TooManyRequests") {
        return true;  // Rate limit - retry
    }
    
    if error_str.contains("500") || error_str.contains("502") || 
       error_str.contains("503") || error_str.contains("504") ||
       error_str.contains("InternalError") ||
       error_str.contains("ServiceUnavailable") {
        return true;  // Server errors - retry
    }
    
    // Default: don't retry unknown errors
    false
}

/// Retry an S3 operation with configurable retry count and delay
/// Only retries on transient errors (network issues, timeouts, 5xx errors)
/// Does NOT retry on 404 (NotFound), 403 (Forbidden), or other permanent errors
async fn retry_s3_operation<F, Fut, T, E>(
    operation_name: &str,
    retry_count: u32,
    retry_delay_ms: u64,
    mut operation: F,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
    E: std::fmt::Debug,
{
    let mut attempts = 0;
    let max_attempts = retry_count.max(1); // At least 1 attempt
    
    loop {
        attempts += 1;
        
        match operation().await {
            Ok(result) => {
                if attempts > 1 {
                    info!("✓ S3 operation '{}' succeeded on attempt {}/{}", 
                          operation_name, attempts, max_attempts);
                }
                return Ok(result);
            }
            Err(e) => {
                // Check if error is retriable
                let retriable = is_retriable_s3_error(&e);
                
                if !retriable {
                    // Non-retriable error (404, 403, etc.) - fail immediately without retry
                    debug!("S3 operation '{}' failed with non-retriable error (no retry)", operation_name);
                    return Err(e);
                }
                
                if attempts >= max_attempts {
                    error!("✗ S3 operation '{}' failed after {} attempts: {:?}", 
                           operation_name, attempts, e);
                    return Err(e);
                }
                
                warn!("⚠ S3 operation '{}' failed on attempt {}/{}: {:?}. Retrying in {}ms...", 
                      operation_name, attempts, max_attempts, e, retry_delay_ms);
                sleep(Duration::from_millis(retry_delay_ms)).await;
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Load configuration
    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "agent_config.yaml".to_string());

    let config_contents = fs::read_to_string(&config_path)
        .with_context(|| format!("Failed to read config file: {}", config_path))?;

    let config: AgentConfig = serde_yaml_ng::from_str(&config_contents)
        .with_context(|| "Failed to parse config file")?;

    // Initialize tracing (guard must be kept alive)
    let _guard = init_tracing(&config.global);

    info!("🚀 Fleetagent starting...");
    info!("Node ID: {}", config.global.node_id);

    // Initialize S3 client
    let s3_client = create_s3_client(&config.object_storage).await;

    // Load or create state
    let state_file = PathBuf::from(format!(".agent_state_{}.json", config.global.node_id));
    let state = AgentState::load_from_file(&state_file).unwrap_or_else(|e| {
        warn!("Failed to load state file: {}, starting fresh", e);
        AgentState::new()
    });
    let shared_state = Arc::new(Mutex::new(state));

    // On startup, sync state from host reality (systemd units in service_folder, supervisor conf,
    // deploy folders) so we don't report stale deployments from the local state file.
    if let Err(e) = sync_state_from_running_services(&config, &shared_state) {
        warn!("Failed to sync state from host: {}", e);
    }

    info!("✅ Agent initialized, entering main loop");

    // Background: publish agent heartbeat every minute for the Fleetman controller UI.
    // S3 key: <prefix>/agents/<node_id>.json
    {
        let node_id = config.global.node_id.clone();
        let config_for_report = config.clone();
        let object_storage = config.object_storage.clone();
        let retry_count = config.global.s3_retry_count;
        let retry_delay_ms = config.global.s3_retry_delay_ms;
        let s3_client = s3_client.clone();

        tokio::spawn(async move {
            // Fixed-rate heartbeat (doesn't drift based on upload duration).
            // If the process is paused / overloaded, skip missed ticks rather than "catch up"
            // with a burst of back-to-back uploads.
            let period = Duration::from_secs(60);
            let mut interval = tokio::time::interval_at(Instant::now() + period, period);
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

            // Emit once immediately on startup (best-effort), then continue on the fixed cadence.
            if let Err(e) = write_agent_status_json(
                &config_for_report,
                &object_storage,
                &s3_client,
                &node_id,
                retry_count,
                retry_delay_ms,
            )
            .await
            {
                warn!("Failed to write initial agent heartbeat to S3: {}", e);
            }

            loop {
                interval.tick().await;
                if let Err(e) = write_agent_status_json(
                    &config_for_report,
                    &object_storage,
                    &s3_client,
                    &node_id,
                    retry_count,
                    retry_delay_ms,
                )
                .await
                {
                    warn!("Failed to write agent heartbeat to S3: {}", e);
                }
            }
        });
    }

    // Main loop
    loop {
        info!("🔍 Starting discovery cycle...");
        
        match run_agent_cycle(&config, &s3_client, &shared_state).await {
            Ok(_) => info!("✅ Agent cycle completed successfully"),
            Err(e) => error!("❌ Error in agent cycle: {}", e),
        }

        // Save state
        if let Err(e) = shared_state.lock().unwrap().save_to_file(&state_file) {
            error!("Failed to save state: {}", e);
        }

        info!("⏳ Sleeping for {}ms before next check", config.global.check_interval_ms);
        sleep(Duration::from_millis(config.global.check_interval_ms)).await;
    }
}

// -------------------- Deployed-folder manifest (for restart accuracy) --------------------

// Deployed-folder manifest used for restart reconciliation and UI reporting.
// Per requirement: this file is named `manifest.json` in the deployed folder.
const LOCAL_DEPLOYED_MANIFEST_FILE: &str = "manifest.json";

#[derive(Debug, Clone, Deserialize)]
struct DeployedManifestFile {
    // Controller snapshot fields (these exist in deployments/<manifest>/<version>/manifest.json)
    #[serde(default)]
    manifest_name: String,
    #[serde(default)]
    version: String,
    #[serde(default)]
    profile: String,
}

fn read_deployed_manifest(service_dir: &str) -> Option<DeployedManifestFile> {
    let path = PathBuf::from(service_dir).join(LOCAL_DEPLOYED_MANIFEST_FILE);
    let content = fs::read_to_string(path).ok()?;
    serde_json::from_str::<DeployedManifestFile>(&content).ok()
}

// -------------------- Host observation (startup sync + heartbeat) --------------------

#[derive(Debug, Clone)]
struct ObservedCell {
    cell_id: String,
    manifest_name: String,
    profile: String,
    version: Option<String>,
    running: Option<bool>, // None => N/A (deploy)
}

fn sync_state_from_running_services(config: &AgentConfig, shared_state: &SharedState) -> Result<()> {
    // Preserve trigger timestamps/previous version from state file to avoid redeploy storms.
    let previous_cells = {
        let state = shared_state.lock().unwrap();
        state.cells.clone()
    };

    let observed = observe_host_cells(config)?;
    let mut discovered: HashMap<String, CellState> = HashMap::new();
    for o in observed {
        let prev = previous_cells.get(&o.cell_id);
        discovered.insert(
            o.cell_id.clone(),
            CellState {
                cell_id: o.cell_id.clone(),
                manifest_name: o.manifest_name.clone(),
                profile: o.profile.clone(),
                current_version: o.version.clone(),
                previous_version: prev.and_then(|p| p.previous_version.clone()),
                trigger_timestamp: prev.and_then(|p| p.trigger_timestamp),
                last_deployment_status: match o.running {
                    Some(true) => DeploymentStatus::Success,
                    Some(false) => DeploymentStatus::Unknown,
                    None => DeploymentStatus::Success, // deploy: applied, runtime N/A
                },
                last_deployment_time: prev.and_then(|p| p.last_deployment_time),
                running: o.running,
            },
        );
    }

    {
        let mut state = shared_state.lock().unwrap();
        state.cells = discovered;
    }

    Ok(())
}

fn observe_host_cells(config: &AgentConfig) -> Result<Vec<ObservedCell>> {
    let mut out: HashMap<String, ObservedCell> = HashMap::new();

    // systemd: enumerate unit files in service_folder, then check running status
    if let Some(systemd_cfg) = &config.systemd {
        let units = discover_systemd_units_from_folder(systemd_cfg)?;
        for (unit, cell_id) in units {
            let running = is_systemd_unit_running(systemd_cfg, &unit).ok();
            let service_dir = format!("{}/{}", systemd_cfg.working_directory, cell_id);
            let manifest = read_deployed_manifest(&service_dir);
            let (manifest_name, profile, version) = match manifest {
                Some(m) => (
                    if m.manifest_name.is_empty() { "unknown".to_string() } else { m.manifest_name },
                    if m.profile.is_empty() { "systemd".to_string() } else { m.profile },
                    if m.version.is_empty() { None } else { Some(m.version) },
                ),
                None => ("unknown".to_string(), "systemd".to_string(), None),
            };
            out.insert(
                cell_id.clone(),
                ObservedCell {
                    cell_id,
                    manifest_name,
                    profile,
                    version,
                    running,
                },
            );
        }
    }

    // supervisor: enumerate programs from conf_d_folder, then check status
    if let Some(supervisor_cfg) = &config.supervisor {
        let programs = discover_supervisor_programs_from_confd(supervisor_cfg)?;
        for program in programs {
            if !program.starts_with("fleetman-") {
                continue;
            }
            let running = Some(is_supervisor_program_running(supervisor_cfg, &program));
            let cell_id = program.trim_start_matches("fleetman-").to_string();
            let service_dir = format!("{}/{}", supervisor_cfg.service_folder, cell_id);
            let manifest = read_deployed_manifest(&service_dir);
            let (manifest_name, profile, version) = match manifest {
                Some(m) => (
                    if m.manifest_name.is_empty() { "unknown".to_string() } else { m.manifest_name },
                    if m.profile.is_empty() { "supervisor".to_string() } else { m.profile },
                    if m.version.is_empty() { None } else { Some(m.version) },
                ),
                None => ("unknown".to_string(), "supervisor".to_string(), None),
            };
            out.insert(
                cell_id.clone(),
                ObservedCell {
                    cell_id,
                    manifest_name,
                    profile,
                    version,
                    running,
                },
            );
        }
    }

    // deploy: folders only; running is N/A
    if let Some(deploy_cfg) = &config.deploy {
        let base = PathBuf::from(&deploy_cfg.target_folder);
        if base.exists() {
            for entry in fs::read_dir(&base)
                .with_context(|| format!("Failed to read deploy target folder {:?}", base))?
            {
                let entry = entry?;
                let path = entry.path();
                if !path.is_dir() {
                    continue;
                }
                let cell_id = match path.file_name().and_then(|s| s.to_str()) {
                    Some(s) => s.to_string(),
                    None => continue,
                };
                let service_dir = path.to_string_lossy().to_string();
                let Some(m) = read_deployed_manifest(&service_dir) else { continue };
                if m.profile.to_lowercase() != "deploy" {
                    continue;
                }
                out.insert(
                    cell_id.clone(),
                    ObservedCell {
                        cell_id,
                        manifest_name: if m.manifest_name.is_empty() { "unknown".to_string() } else { m.manifest_name },
                        profile: if m.profile.is_empty() { "deploy".to_string() } else { m.profile },
                        version: if m.version.is_empty() { None } else { Some(m.version) },
                        running: None,
                    },
                );
            }
        }
    }

    Ok(out.into_values().collect())
}

fn discover_systemd_units_from_folder(systemd_cfg: &SystemdConfig) -> Result<Vec<(String, String)>> {
    let dir = PathBuf::from(&systemd_cfg.service_folder);
    if !dir.exists() {
        return Ok(Vec::new());
    }

    let mut units = Vec::new();
    for entry in fs::read_dir(&dir).with_context(|| format!("Failed to read systemd service folder {:?}", dir))? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        if path.extension().and_then(|s| s.to_str()) != Some("service") {
            continue;
        }
        let unit = match path.file_name().and_then(|s| s.to_str()) {
            Some(s) => s.to_string(),
            None => continue,
        };
        if !unit.starts_with("fleetman-") {
            continue;
        }
        let cell_id = unit
            .trim_end_matches(".service")
            .trim_start_matches("fleetman-")
            .to_string();
        if cell_id.is_empty() {
            continue;
        }
        units.push((unit, cell_id));
    }
    Ok(units)
}

fn is_systemd_unit_running(systemd_cfg: &SystemdConfig, unit: &str) -> Result<bool> {
    let output = Command::new(systemd_cfg.systemctl_path())
        .args(["is-active", "--no-pager", unit])
        .output()
        .with_context(|| "Failed to execute systemctl is-active")?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    Ok(stdout.trim() == "active")
}

fn discover_supervisor_programs_from_confd(supervisor_cfg: &SupervisorConfig) -> Result<Vec<String>> {
    let conf_dir = PathBuf::from(&supervisor_cfg.conf_d_folder);
    if !conf_dir.exists() {
        return Ok(Vec::new());
    }

    let mut programs = Vec::new();
    for entry in fs::read_dir(&conf_dir)
        .with_context(|| format!("Failed to read supervisor conf.d folder {:?}", conf_dir))?
    {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }

        let ext_ok = path.extension().and_then(|s| s.to_str()) == Some("conf");
        if !ext_ok {
            continue;
        }

        // Prefer filename convention: <conf_d_folder>/fleetman-<cell_id>.conf
        if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
            if stem.starts_with("fleetman-") {
                programs.push(stem.to_string());
                continue;
            }
        }

        // Otherwise, try to parse [program:<name>] stanzas from file content.
        if let Ok(content) = fs::read_to_string(&path) {
            for line in content.lines() {
                let line = line.trim();
                if let Some(rest) = line.strip_prefix("[program:") {
                    if let Some(name) = rest.strip_suffix(']') {
                        let name = name.trim();
                        if name.starts_with("fleetman-") && !name.is_empty() {
                            programs.push(name.to_string());
                        }
                    }
                }
            }
        }
    }

    programs.sort();
    programs.dedup();
    Ok(programs)
}

fn is_supervisor_program_running(supervisor_cfg: &SupervisorConfig, program: &str) -> bool {
    let output = Command::new(supervisor_cfg.supervisorctl_path())
        .args(["status", program])
        .output();

    let output = match output {
        Ok(o) => o,
        Err(_) => return false,
    };

    if !output.status.success() {
        return false;
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    stdout.contains("RUNNING")
}

#[derive(Debug, Clone, Serialize)]
struct AgentReportedCell {
    cell_id: String,
    manifest_name: String,
    profile: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    running: Option<bool>, // None => N/A (deploy)
}

#[derive(Debug, Clone, Serialize)]
struct AgentStatusReport {
    report_timestamp: u64,
    node_id: String,
    cells: Vec<AgentReportedCell>,
}

async fn write_agent_status_json(
    config: &AgentConfig,
    object_storage: &ObjectStorageConfig,
    s3_client: &S3Client,
    node_id: &str,
    retry_count: u32,
    retry_delay_ms: u64,
) -> Result<()> {
    let report_timestamp = current_timestamp();

    let observed = observe_host_cells(config).unwrap_or_else(|e| {
        warn!("Failed to observe host cells for heartbeat: {}", e);
        Vec::new()
    });

    let cells: Vec<AgentReportedCell> = observed
        .into_iter()
        .map(|c| AgentReportedCell {
            cell_id: c.cell_id,
            manifest_name: c.manifest_name,
            profile: c.profile,
            version: c.version,
            running: c.running,
        })
        .collect();

    let report = AgentStatusReport {
        report_timestamp,
        node_id: node_id.to_string(),
        cells,
    };

    let content = serde_json::to_string_pretty(&report)?;
    let key_base = format!("agents/{}.json", node_id);
    let key = object_storage.full_key(&key_base);

    retry_s3_operation(
        &format!("write_agent_status_{}", node_id),
        retry_count,
        retry_delay_ms,
        || async {
            s3_client
                .put_object()
                .bucket(&object_storage.bucket)
                .key(&key)
                .body(ByteStream::from(content.clone().into_bytes()))
                .send()
                .await
        },
    )
    .await
    .map_err(|e| anyhow::anyhow!("Failed to put agent status object: {:?}", e))?;

    Ok(())
}

async fn run_agent_cycle(
    config: &AgentConfig,
    s3_client: &S3Client,
    shared_state: &SharedState,
) -> Result<()> {
    // Discover cells for this node
    info!("🔎 Scanning S3 for cells under node: {}", config.global.node_id);
    let cells = discover_cells(config, s3_client).await?;

    if cells.is_empty() {
        info!("ℹ️  No cells found for node {} (bucket: {}, prefix: cells/{}/))", 
              config.global.node_id, 
              config.object_storage.bucket,
              config.global.node_id);
        return Ok(());
    }

    info!("📡 Discovered {} cell(s): {:?}", cells.len(), 
          cells.iter().map(|c| &c.cell_id).collect::<Vec<_>>());

    // Process each cell
    for cell_info in cells {
        match process_cell(config, s3_client, shared_state, &cell_info).await {
            Ok(_) => {}
            Err(e) => error!("Failed to process cell {}: {}", cell_info.cell_id, e),
        }
    }

    Ok(())
}

#[derive(Debug, Clone)]
struct CellInfo {
    cell_id: String,
    manifest_name: String,
}

async fn discover_cells(config: &AgentConfig, s3_client: &S3Client) -> Result<Vec<CellInfo>> {
    let prefix = config
        .object_storage
        .full_key(&format!("cells/{}/", config.global.node_id));

    info!("📂 Listing S3: bucket={}, prefix={}", config.object_storage.bucket, prefix);

    let mut cells = Vec::new();

    let mut continuation_token: Option<String> = None;

    loop {
        let mut request = s3_client
            .list_objects_v2()
            .bucket(&config.object_storage.bucket)
            .prefix(&prefix)
            .delimiter("/");

        if let Some(token) = continuation_token {
            request = request.continuation_token(token);
        }

        debug!("Sending S3 ListObjectsV2 request...");
        let response = retry_s3_operation(
            "list_cells",
            config.global.s3_retry_count,
            config.global.s3_retry_delay_ms,
            || async { request.clone().send().await },
        ).await.map_err(|e| {
            error!("❌ S3 ListObjectsV2 failed: {}", e);
            anyhow::anyhow!("Failed to list cells: {:?}", e)
        })?;

        // Process common prefixes (cell directories)
        let common_prefixes_count = response.common_prefixes().len();
        debug!("Found {} common prefixes in this batch", common_prefixes_count);
        
        for cp in response.common_prefixes() {
                if let Some(prefix_str) = cp.prefix() {
                    debug!("Processing prefix: {}", prefix_str);
                    
                    // Strip bucket prefix
                    let prefix_with_slash = format!("{}/", config.object_storage.prefix);
                    let stripped = prefix_str.strip_prefix(&prefix_with_slash).unwrap_or(prefix_str);
                    
                    debug!("Stripped prefix: {}", stripped);
                    
                    // Extract cell_id from prefix: cells/<node_id>/<cell_id>/
                    let parts: Vec<&str> = stripped.trim_end_matches('/').split('/').collect();
                    debug!("Path parts: {:?}", parts);
                    if parts.len() >= 3 {
                        let cell_id = parts[2].to_string();
                        info!("📦 Found cell directory: {}", cell_id);

                        // Read cell metadata to get manifest_name
                        match get_cell_manifest_name(config, s3_client, &cell_id).await {
                            Ok(manifest_name) => {
                                info!("   └─ manifest: {}", manifest_name);
                                cells.push(CellInfo {
                                    cell_id,
                                    manifest_name,
                                });
                            }
                            Err(e) => {
                                warn!("   └─ Failed to get metadata for cell {}: {}", cell_id, e);
                            }
                        }
                    }
                }
            }

        if response.is_truncated().unwrap_or(false) {
            continuation_token = response.next_continuation_token().map(|s| s.to_string());
        } else {
            break;
        }
    }

    Ok(cells)
}

#[derive(Debug, Deserialize)]
struct TriggerFile {
    version: String,
}

#[derive(Debug, Deserialize)]
struct ManifestSnapshot {
    profile: String,
    files: Vec<ManifestFile>,
    #[serde(default = "default_collect_logs")]
    collect_logs: bool,
    #[serde(default = "default_log_files")]
    log_files: Vec<String>,
    #[serde(default = "default_max_log_lines")]
    max_log_lines: usize,
}

fn default_collect_logs() -> bool {
    true
}

fn default_log_files() -> Vec<String> {
    vec![
        "logs/stdout.log".to_string(),
        "logs/stderr.log".to_string(),
    ]
}

fn default_max_log_lines() -> usize {
    30
}

#[derive(Debug, Deserialize, Clone)]
struct ManifestFile {
    path: String,
    file_type: String,  // "binary", "text", or "folder"
    owner: Option<String>,
    group: Option<String>,
    mode: Option<String>,
}

async fn get_cell_manifest_name(
    config: &AgentConfig,
    s3_client: &S3Client,
    cell_id: &str,
) -> Result<String> {
    let trigger_key = config
        .object_storage
        .full_key(&format!("cells/{}/{}/trigger.json", config.global.node_id, cell_id));

    let obj = retry_s3_operation(
        &format!("get_trigger_{}", cell_id),
        config.global.s3_retry_count,
        config.global.s3_retry_delay_ms,
        || async {
            s3_client
                .get_object()
                .bucket(&config.object_storage.bucket)
                .key(&trigger_key)
                .send()
                .await
        },
    ).await
    .with_context(|| format!("Failed to get trigger file for cell {}", cell_id))?;

    let body = obj.body.collect().await?.into_bytes();
    let _trigger: TriggerFile = serde_json::from_slice(&body)?;

    // Extract manifest_name from the path structure
    // Trigger points to a version, we need to find which manifest it belongs to
    // We'll need to list and find the manifest for this cell
    // For now, let's derive it from the cell's path structure

    // Actually, we should store manifest_name in the cell's metadata
    // Let's check for a cell_metadata.json file
    let metadata_key = config.object_storage.full_key(&format!(
        "cells/{}/{}/metadata.json",
        config.global.node_id, cell_id
    ));

    match s3_client
        .get_object()
        .bucket(&config.object_storage.bucket)
        .key(&metadata_key)
        .send()
        .await
    {
        Ok(obj) => {
            let body = obj.body.collect().await?.into_bytes();
            #[derive(Deserialize)]
            struct CellMetadata {
                manifest_name: String,
            }
            let metadata: CellMetadata = serde_json::from_slice(&body)?;
            Ok(metadata.manifest_name)
        }
        Err(_) => {
            // Fallback: try to infer from directory listing
            // This is a workaround if metadata doesn't exist
            Ok(cell_id.to_string())
        }
    }
}

async fn process_cell(
    config: &AgentConfig,
    s3_client: &S3Client,
    shared_state: &SharedState,
    cell_info: &CellInfo,
) -> Result<()> {
    info!("📋 Processing cell: {}", cell_info.cell_id);
    
    // Get trigger file
    let trigger_key = config.object_storage.full_key(&format!(
        "cells/{}/{}/trigger.json",
        config.global.node_id, cell_info.cell_id
    ));

    debug!("Reading trigger file: {}", trigger_key);
    let obj = retry_s3_operation(
        &format!("get_trigger_{}", cell_info.cell_id),
        config.global.s3_retry_count,
        config.global.s3_retry_delay_ms,
        || async {
            s3_client
                .get_object()
                .bucket(&config.object_storage.bucket)
                .key(&trigger_key)
                .send()
                .await
        },
    ).await
    .with_context(|| format!("Failed to get trigger.json for cell {} from S3", cell_info.cell_id))?;

    let last_modified = obj
        .last_modified()
        .map(|dt| dt.secs() as u64)
        .unwrap_or(0);

    let body = obj.body.collect().await
        .with_context(|| format!("Failed to read trigger.json body for cell {}", cell_info.cell_id))?
        .into_bytes();
    
    let trigger: TriggerFile = serde_json::from_slice(&body)
        .with_context(|| format!("Failed to parse trigger.json for cell {}: invalid JSON", cell_info.cell_id))?;
    
    info!("   Trigger version: {}, timestamp: {}", trigger.version, last_modified);

    // Check if this is a new deployment
    let should_deploy = {
        let state = shared_state.lock().unwrap();
        match state.get_cell(&cell_info.cell_id) {
            Some(cell_state) => {
                // Only check if trigger timestamp changed
                // Don't re-process same trigger even if current version differs (could be due to rollback)
                cell_state.trigger_timestamp != Some(last_modified)
            }
            None => true, // New cell, deploy
        }
    };

    if !should_deploy {
        info!(
            "✓ Cell {} already processed trigger (timestamp: {}), skipping",
            cell_info.cell_id, last_modified
        );
        return Ok(());
    }

    info!(
        "🔄 Deploying cell {} to version {}",
        cell_info.cell_id, trigger.version
    );

    // Read manifest.json *from the cell version folder* so deploy/rollback is fully determined
    // by the per-cell snapshot (base deployment + merged overrides).
    debug!("Reading manifest.json from cell version folder for version {}", trigger.version);
    let cell_manifest_key_base = format!(
        "cells/{}/{}/versions/{}/manifest.json",
        config.global.node_id,
        cell_info.cell_id,
        trigger.version
    );
    let cell_manifest_key = config.object_storage.full_key(&cell_manifest_key_base);

    let manifest_obj = retry_s3_operation(
        &format!("get_cell_manifest_{}_{}", cell_info.cell_id, trigger.version),
        config.global.s3_retry_count,
        config.global.s3_retry_delay_ms,
        || async {
            s3_client
                .get_object()
                .bucket(&config.object_storage.bucket)
                .key(&cell_manifest_key)
                .send()
                .await
        },
    )
    .await
    .with_context(|| {
        format!(
            "Failed to download cell manifest.json for cell {} version {}",
            cell_info.cell_id, trigger.version
        )
    })?;

    let manifest_body = manifest_obj.body.collect().await?.into_bytes();
    let manifest: ManifestSnapshot = serde_json::from_slice(&manifest_body)
        .with_context(|| format!("Failed to parse cell manifest.json for version {}", trigger.version))?;
    
    info!("   Deployment profile: {}", manifest.profile);
    info!("   Files in manifest: {}", manifest.files.len());

    // Update state to InProgress
    {
        let mut state = shared_state.lock().unwrap();
        let current_state = state.get_cell(&cell_info.cell_id).cloned();
        let new_state = CellState {
            cell_id: cell_info.cell_id.clone(),
            manifest_name: cell_info.manifest_name.clone(),
            profile: manifest.profile.clone(),
            current_version: current_state.as_ref().and_then(|s| s.current_version.clone()),
            previous_version: current_state.as_ref().and_then(|s| s.current_version.clone()),
            trigger_timestamp: Some(last_modified),
            last_deployment_status: DeploymentStatus::InProgress,
            last_deployment_time: Some(current_timestamp()),
            running: None,
        };
        state.update_cell(cell_info.cell_id.clone(), new_state);
    }

    // Execute deployment
    let deployment_result = match manifest.profile.as_str() {
        "supervisor" | "Supervisor" => {
            deploy_with_supervisor(config, s3_client, cell_info, &trigger.version, &manifest).await
                .with_context(|| format!("Supervisor deployment failed for cell {} version {}", 
                                        cell_info.cell_id, trigger.version))
        }
        "systemd" | "Systemd" => {
            deploy_with_systemd(config, s3_client, cell_info, &trigger.version, &manifest).await
                .with_context(|| format!("Systemd deployment failed for cell {} version {}", 
                                        cell_info.cell_id, trigger.version))
        }
        "deploy" | "Deploy" => {
            deploy_with_simple(config, s3_client, cell_info, &trigger.version, &manifest).await
                .with_context(|| format!("Deploy deployment failed for cell {} version {}", 
                                        cell_info.cell_id, trigger.version))
        }
        _ => Err(anyhow::anyhow!("Unknown profile '{}' for cell {}", manifest.profile, cell_info.cell_id)),
    };

    // Update state based on result and handle rollback if needed
    match deployment_result {
        Ok(_) => {
            let timestamp = current_timestamp();
            let mut state = shared_state.lock().unwrap();
            if let Some(mut cell_state) = state.get_cell(&cell_info.cell_id).cloned() {
                info!("✅ Successfully deployed {} to {}", cell_info.cell_id, trigger.version);
                cell_state.current_version = Some(trigger.version.clone());
                cell_state.last_deployment_status = DeploymentStatus::Success;
                cell_state.last_deployment_time = Some(timestamp);
                // Running status: known for service profiles, N/A for deploy.
                cell_state.running = match manifest.profile.to_lowercase().as_str() {
                    "deploy" => None,
                    _ => Some(true),
                };
                state.update_cell(cell_info.cell_id.clone(), cell_state);
            }
            drop(state); // Release lock before async operation
            
            // Write status.json to S3 (no logs on success)
            write_status_json(config, s3_client, cell_info, &trigger.version, timestamp, None, None).await
                .unwrap_or_else(|e| error!("Failed to write status.json for {}: {}", cell_info.cell_id, e));
            
            Ok(())
        }
        Err(e) => {
            let error_message = format!("{:#}", e);
            error!("❌ Failed to deploy {} to {}: {}", cell_info.cell_id, trigger.version, error_message);
            
            // Check if we should attempt rollback
            let previous_version = {
                let state = shared_state.lock().unwrap();
                state.get_cell(&cell_info.cell_id)
                    .and_then(|s| s.previous_version.clone())
            };
            
            let (final_version, final_error) = if let Some(prev_version) = previous_version {
                warn!("🔄 Attempting rollback to previous version: {}", prev_version);
                
                // Try to rollback to previous version
                let rollback_result = match manifest.profile.as_str() {
                    "supervisor" | "Supervisor" => {
                        rollback_supervisor(config, s3_client, cell_info, &prev_version).await
                    }
                    "systemd" | "Systemd" => {
                        rollback_systemd(config, s3_client, cell_info, &prev_version).await
                    }
                    _ => Err(anyhow::anyhow!("Unknown profile for rollback")),
                };
                
                // Update state based on rollback result
                let timestamp = current_timestamp();
                let mut state = shared_state.lock().unwrap();
                if let Some(mut cell_state) = state.get_cell(&cell_info.cell_id).cloned() {
                    match rollback_result {
                        Ok(_) => {
                            warn!("✅ Successfully rolled back {} to {}", cell_info.cell_id, prev_version);
                            cell_state.current_version = Some(prev_version.clone());
                            cell_state.last_deployment_status = DeploymentStatus::Failed; // Keep failed status for new version
                            cell_state.last_deployment_time = Some(timestamp);
                            state.update_cell(cell_info.cell_id.clone(), cell_state);
                            (prev_version, format!("Failed to deploy {}: {}. Rolled back successfully.", trigger.version, error_message))
                        }
                        Err(rollback_err) => {
                            let rollback_error = format!("{:#}", rollback_err);
                            error!("❌ Rollback also failed for {}: {}", cell_info.cell_id, rollback_error);
                            cell_state.last_deployment_status = DeploymentStatus::Failed;
                            cell_state.last_deployment_time = Some(timestamp);
                            state.update_cell(cell_info.cell_id.clone(), cell_state);
                            (prev_version.clone(), format!("Failed to deploy {}: {}. Rollback to {} also failed: {}", trigger.version, error_message, prev_version, rollback_error))
                        }
                    }
                } else {
                    (prev_version, error_message)
                }
            } else {
                warn!("⚠️  No previous version to rollback to for {}", cell_info.cell_id);
                
                // Update state to failed
                let timestamp = current_timestamp();
                let mut state = shared_state.lock().unwrap();
                if let Some(mut cell_state) = state.get_cell(&cell_info.cell_id).cloned() {
                    cell_state.last_deployment_status = DeploymentStatus::Failed;
                    cell_state.last_deployment_time = Some(timestamp);
                    state.update_cell(cell_info.cell_id.clone(), cell_state);
                }
                
                ("unknown".to_string(), format!("Failed to deploy {}: {}. No previous version to rollback to.", trigger.version, error_message))
            };
            
            // Collect logs if enabled in manifest
            let logs = if let Some(service_dir) = get_service_directory(config, &cell_info.cell_id, &manifest.profile) {
                collect_logs(&service_dir, &manifest)
            } else {
                None
            };
            
            // Write status.json with failure details and logs
            let timestamp = current_timestamp();
            write_status_json(config, s3_client, cell_info, &final_version, timestamp, Some(&final_error), logs).await
                .unwrap_or_else(|err| error!("Failed to write status.json for {}: {}", cell_info.cell_id, err));
            
            Err(e)
        }
    }
}

async fn deploy_with_supervisor(
    config: &AgentConfig,
    s3_client: &S3Client,
    cell_info: &CellInfo,
    version: &str,
    manifest: &ManifestSnapshot,
) -> Result<()> {
    let supervisor_config = config
        .supervisor
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("Supervisor config not found"))?;

    // Folder name: cell_id without prefix
    let service_dir = format!("{}/{}", supervisor_config.service_folder, &cell_info.cell_id);
    // Service name: fleetman-{cell_id} for easy identification
    let service_name = format!("fleetman-{}", &cell_info.cell_id);

    info!("0️⃣ Updating supervisor configuration");
    update_supervisor_config(supervisor_config)
        .with_context(|| "Failed to update supervisor configuration")?;

    info!("1️⃣ Stopping service: {}", service_name);
    stop_supervisor_service(supervisor_config, &service_name)
        .with_context(|| format!("Failed to stop supervisor service {}", service_name))?;

    info!("2️⃣ Verifying service stopped");
    verify_supervisor_stopped(supervisor_config, &service_name)
        .with_context(|| format!("Service {} failed to stop", service_name))?;

    info!("3️⃣ Downloading artifacts to {}", service_dir);
    download_version_files(config, s3_client, cell_info, version, &service_dir, manifest).await
        .with_context(|| format!("Failed to download version {} files to {}", version, service_dir))?;

    // Overrides are snapshotted & merged into versions/<ver>/ at publish time.
    // Apply override permissions from overrides_manifest.json if present in the deployed folder.
    apply_overrides_manifest_in_place(&service_dir)?;

    info!("5️⃣ Generating supervisor config");
    generate_supervisor_config(supervisor_config, &service_name, &cell_info.cell_id)
        .with_context(|| format!("Failed to generate supervisor config for {}", service_name))?;

    info!("6️⃣ Starting service: {}", service_name);
    start_supervisor_service(supervisor_config, &service_name)
        .with_context(|| format!("Failed to start supervisor service {}", service_name))?;

    info!("7️⃣ Verifying service running (waiting 10s)");
    tokio::time::sleep(Duration::from_secs(10)).await;
    verify_supervisor_running(supervisor_config, &service_name)
        .with_context(|| format!("Service {} is not running after 10s", service_name))?;

    Ok(())
}

async fn deploy_with_systemd(
    config: &AgentConfig,
    s3_client: &S3Client,
    cell_info: &CellInfo,
    version: &str,
    manifest: &ManifestSnapshot,
) -> Result<()> {
    let systemd_config = config
        .systemd
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("Systemd config not found"))?;

    // Folder name: cell_id without prefix
    let service_dir = format!("{}/{}", systemd_config.working_directory, &cell_info.cell_id);
    // Service name: fleetman-{cell_id} for easy identification
    let service_name = format!("fleetman-{}", &cell_info.cell_id);

    info!("1️⃣ Stopping service: {}", service_name);
    stop_systemd_service(systemd_config, &service_name)
        .with_context(|| format!("Failed to stop systemd service {}", service_name))?;

    info!("2️⃣ Verifying service stopped");
    verify_systemd_stopped(systemd_config, &service_name)
        .with_context(|| format!("Service {} failed to stop", service_name))?;

    info!("3️⃣ Downloading artifacts to {}", service_dir);
    download_version_files(config, s3_client, cell_info, version, &service_dir, manifest).await
        .with_context(|| format!("Failed to download version {} files to {}", version, service_dir))?;

    // Overrides are snapshotted & merged into versions/<ver>/ at publish time.
    // Apply override permissions from overrides_manifest.json if present in the deployed folder.
    apply_overrides_manifest_in_place(&service_dir)?;

    info!("5️⃣ Generating systemd service");
    generate_systemd_service(systemd_config, &service_name, &cell_info.cell_id)
        .with_context(|| format!("Failed to generate systemd service for {}", service_name))?;

    info!("6️⃣ Running systemctl daemon-reload");
    systemd_daemon_reload(systemd_config)
        .with_context(|| "Failed to reload systemd daemon")?;

    info!("7️⃣ Starting service: {}", service_name);
    start_systemd_service(systemd_config, &service_name)
        .with_context(|| format!("Failed to start systemd service {}", service_name))?;

    info!("8️⃣ Verifying service running (waiting 10s)");
    tokio::time::sleep(Duration::from_secs(10)).await;
    verify_systemd_running(systemd_config, &service_name)
        .with_context(|| format!("Service {} is not running after 10s", service_name))?;

    Ok(())
}

async fn download_version_files(
    config: &AgentConfig,
    s3_client: &S3Client,
    cell_info: &CellInfo,
    version: &str,
    target_dir: &str,
    manifest: &ManifestSnapshot,
) -> Result<()> {
    // Create target directory
    fs::create_dir_all(target_dir)?;
    
    // Set service folder permissions to 755 (rwxr-xr-x)
    let target_path = Path::new(target_dir);
    let mut perms = fs::metadata(target_path)?.permissions();
    perms.set_mode(0o755);
    fs::set_permissions(target_path, perms)?;
    info!("   Set service folder permissions to 0755: {}", target_dir);

    let version_prefix = config.object_storage.full_key(&format!(
        "cells/{}/{}/versions/{}/",
        config.global.node_id, cell_info.cell_id, version
    ));

    info!("   Processing {} items from manifest", manifest.files.len());

    // Create folders first
    for file in &manifest.files {
        if file.file_type == "folder" {
            let folder_path = PathBuf::from(target_dir).join(&file.path);
            info!("   Creating folder: {}", file.path);
            fs::create_dir_all(&folder_path)
                .with_context(|| format!("Failed to create folder {}", file.path))?;
            
            // Apply folder permissions
            apply_permissions(&folder_path, &file)?;
        }
    }

    // Download all files
    download_s3_directory(
        config,
        s3_client,
        &config.object_storage.bucket,
        &version_prefix,
        target_dir,
    )
    .await?;

    // Apply permissions to all files based on manifest
    for file in &manifest.files {
        if file.file_type != "folder" {
            let file_path = PathBuf::from(target_dir).join(&file.path);
            if file_path.exists() {
                debug!("   Setting permissions for: {}", file.path);
                apply_permissions(&file_path, &file)?;
            } else {
                warn!("   File not found, skipping permission setting: {}", file.path);
            }
        }
    }

    Ok(())
}

// NOTE: With override snapshot+merge at publish time, the agent no longer downloads live overrides.
// It only applies permissions from `overrides_manifest.json` if present in the version folder.
//
// The below defaults + legacy helpers are kept for backward compatibility and debugging, but are
// no longer used by the main deployment flow.
#[allow(dead_code)]
const DEFAULT_OVERRIDE_OWNER: &str = "root";
#[allow(dead_code)]
const DEFAULT_OVERRIDE_GROUP: &str = "root";
#[allow(dead_code)]
const DEFAULT_OVERRIDE_MODE_FILE: &str = "0644";
#[allow(dead_code)]
const DEFAULT_OVERRIDE_MODE_FOLDER: &str = "0755";

/// Apply override permissions in-place using a snapshotted `overrides_manifest.json`
/// that the controller copies into the deployed folder at publish time.
///
/// Expected location: <service_dir>/overrides_manifest.json
fn apply_overrides_manifest_in_place(service_dir: &str) -> Result<()> {
    let manifest_path = PathBuf::from(service_dir).join("overrides_manifest.json");
    if !manifest_path.exists() {
        return Ok(());
    }

    let content = fs::read_to_string(&manifest_path)
        .with_context(|| format!("Failed to read overrides manifest at {:?}", manifest_path))?;
    let manifest: CellOverridesManifest = serde_json::from_str(&content)
        .with_context(|| format!("Failed to parse overrides manifest at {:?}", manifest_path))?;

    // Ensure folders from manifest exist first
    for f in &manifest.files {
        if f.file_type == "folder" {
            let folder_path = PathBuf::from(service_dir).join(&f.path);
            if !folder_path.exists() {
                fs::create_dir_all(&folder_path)
                    .with_context(|| format!("Failed to create override folder {:?}", folder_path))?;
            }
        }
    }

    // Apply metadata for all files listed in manifest (only if they exist)
    for f in &manifest.files {
        let p = PathBuf::from(service_dir).join(&f.path);
        if !p.exists() {
            continue;
        }

        let mode_val = u32::from_str_radix(f.mode.trim_start_matches('0'), 8)
            .with_context(|| format!("Invalid mode '{}' for {:?}", f.mode, p))?;
        let perms = fs::Permissions::from_mode(mode_val);
        fs::set_permissions(&p, perms)
            .with_context(|| format!("Failed to set mode {} for {:?}", f.mode, p))?;

        let output = Command::new("chown")
            .arg(format!("{}:{}", f.owner, f.group))
            .arg(&p)
            .output()
            .with_context(|| format!("Failed to execute chown for {:?}", p))?;
        if !output.status.success() {
            return Err(anyhow::anyhow!(
                "chown failed for {:?}: {}",
                p,
                String::from_utf8_lossy(&output.stderr)
            ));
        }
    }

    // Best-effort defaults for override files not in manifest:
    // we *intentionally* do not walk the entire tree (could touch non-override files).
    Ok(())
}

#[derive(Debug, serde::Deserialize)]
struct CellOverridesManifest {
    files: Vec<CellOverrideFile>,
}

#[derive(Debug, serde::Deserialize)]
struct CellOverrideFile {
    path: String,
    file_type: String,
    owner: String,
    group: String,
    mode: String,
}

#[allow(dead_code)]
async fn download_cell_overrides(
    config: &AgentConfig,
    s3_client: &S3Client,
    cell_info: &CellInfo,
    target_dir: &str,
) -> Result<()> {
    // First, try to load the overrides manifest
    let manifest_key = config.object_storage.full_key(&format!(
        "cells/{}/{}/overrides_manifest.json",
        config.global.node_id, cell_info.cell_id
    ));
    
    let manifest: Option<CellOverridesManifest> = match retry_s3_operation(
        &format!("get_overrides_manifest_{}", cell_info.cell_id),
        config.global.s3_retry_count,
        config.global.s3_retry_delay_ms,
        || async {
            s3_client
                .get_object()
                .bucket(&config.object_storage.bucket)
                .key(&manifest_key)
                .send()
                .await
        },
    ).await
    {
        Ok(obj) => {
            match obj.body.collect().await {
                Ok(data) => {
                    match serde_json::from_slice::<CellOverridesManifest>(&data.to_vec()) {
                        Ok(m) => {
                            debug!("Loaded overrides manifest for {}", cell_info.cell_id);
                            Some(m)
                        }
                        Err(e) => {
                            warn!("Failed to parse overrides manifest: {}", e);
                            None
                        }
                    }
                }
                Err(e) => {
                    warn!("Failed to read overrides manifest body: {}", e);
                    None
                }
            }
        }
        Err(_) => {
            debug!("No overrides manifest found for {}", cell_info.cell_id);
            None
        }
    };
    
    let overrides_prefix = config.object_storage.full_key(&format!(
        "cells/{}/{}/overrides/",
        config.global.node_id, cell_info.cell_id
    ));

    // Check if overrides exist
    match retry_s3_operation(
        &format!("check_overrides_{}", cell_info.cell_id),
        config.global.s3_retry_count,
        config.global.s3_retry_delay_ms,
        || async {
            s3_client
                .list_objects_v2()
                .bucket(&config.object_storage.bucket)
                .prefix(&overrides_prefix)
                .max_keys(1)
                .send()
                .await
        },
    ).await
    {
        Ok(response) => {
            if !response.contents().is_empty() {
                let downloaded_files = download_s3_directory(
                    config,
                    s3_client,
                    &config.object_storage.bucket,
                    &overrides_prefix,
                    target_dir,
                )
                .await?;
                
                // Apply permissions from manifest (or defaults if no manifest)
                // ONLY to files that were actually downloaded as overrides
                apply_overrides_permissions(target_dir, manifest.as_ref(), &downloaded_files)?;
            } else {
                debug!("No overrides found for cell {}", cell_info.cell_id);
            }
        }
        Err(e) => {
            debug!("Failed to check for overrides: {}", e);
        }
    }

    Ok(())
}

#[allow(dead_code)]
fn apply_overrides_permissions(
    target_dir: &str,
    manifest: Option<&CellOverridesManifest>,
    downloaded_files: &[String],
) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    
    // Build a map of paths to metadata from manifest (if exists)
    let mut manifest_map = std::collections::HashMap::new();
    if let Some(m) = manifest {
        for file_info in &m.files {
            manifest_map.insert(file_info.path.clone(), file_info);
        }
        
        // First, create folders defined in manifest
        for file_info in &m.files {
            if file_info.file_type == "folder" {
                let folder_path = PathBuf::from(target_dir).join(&file_info.path);
                if !folder_path.exists() {
                    fs::create_dir_all(&folder_path)
                        .with_context(|| format!("Failed to create folder {:?}", folder_path))?;
                }
            }
        }
    }
    
    // Only apply permissions to files that were actually downloaded as overrides
    for relative_path in downloaded_files {
        let file_path = PathBuf::from(target_dir).join(relative_path);
        
        // Skip if file doesn't exist
        if !file_path.exists() {
            continue;
        }
        
        // Determine owner, group, mode
        let (owner, group, mode) = if let Some(file_info) = manifest_map.get(relative_path) {
            // Use manifest metadata
            (file_info.owner.as_str(), file_info.group.as_str(), file_info.mode.as_str())
        } else {
            // Use defaults for files not in manifest
            let is_dir = file_path.is_dir();
            let default_mode = if is_dir { DEFAULT_OVERRIDE_MODE_FOLDER } else { DEFAULT_OVERRIDE_MODE_FILE };
            debug!("Applying default permissions to {:?} (not in manifest)", file_path);
            (DEFAULT_OVERRIDE_OWNER, DEFAULT_OVERRIDE_GROUP, default_mode)
        };
        
        // Apply permissions
        let mode_val = u32::from_str_radix(mode.trim_start_matches('0'), 8)
            .with_context(|| format!("Invalid mode '{}' for {:?}", mode, file_path))?;
        let perms = fs::Permissions::from_mode(mode_val);
        fs::set_permissions(&file_path, perms)
            .with_context(|| format!("Failed to set mode {} for {:?}", mode, file_path))?;
        
        // Apply ownership (ERROR SENSITIVE - must succeed)
        let output = Command::new("chown")
            .arg(format!("{}:{}", owner, group))
            .arg(&file_path)
            .output()
            .with_context(|| format!("Failed to execute chown for {:?}", file_path))?;
        
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(anyhow::anyhow!(
                "chown failed for {:?}: {}",
                file_path,
                stderr
            ));
        }
        
        debug!(
            "Applied override permissions: {:?} -> {}:{} ({})",
            file_path, owner, group, mode
        );
    }
    
    Ok(())
}

async fn download_s3_directory(
    config: &AgentConfig,
    s3_client: &S3Client,
    bucket: &str,
    prefix: &str,
    target_dir: &str,
) -> Result<Vec<String>> {
    let mut continuation_token: Option<String> = None;
    let mut downloaded_files = Vec::new();

    loop {
        let mut request = s3_client
            .list_objects_v2()
            .bucket(bucket)
            .prefix(prefix);

        if let Some(token) = continuation_token.clone() {
            request = request.continuation_token(token);
        }

        let response = retry_s3_operation(
            &format!("list_objects_{}", prefix),
            config.global.s3_retry_count,
            config.global.s3_retry_delay_ms,
            || async { request.clone().send().await },
        ).await?;

        for object in response.contents() {
                if let Some(key) = object.key() {
                    // Skip directories
                    if key.ends_with('/') {
                        continue;
                    }

                    // Calculate relative path
                    let relative_path = key.strip_prefix(prefix).unwrap_or(key);
                    let target_path = PathBuf::from(target_dir).join(relative_path);

                    // Create parent directories
                    if let Some(parent) = target_path.parent() {
                        fs::create_dir_all(parent)?;
                    }

                    // Download file
                    let key_clone = key.to_string();
                    let obj = retry_s3_operation(
                        &format!("download_{}", key),
                        config.global.s3_retry_count,
                        config.global.s3_retry_delay_ms,
                        || async {
                            s3_client.get_object().bucket(bucket).key(&key_clone).send().await
                        },
                    ).await?;

                    let body = obj.body.collect().await?.into_bytes();
                    fs::write(&target_path, body)?;

                    debug!("Downloaded: {} -> {:?}", key, target_path);
                    downloaded_files.push(relative_path.to_string());
                }
            }

        if response.is_truncated().unwrap_or(false) {
            continuation_token = response.next_continuation_token().map(|s| s.to_string());
        } else {
            break;
        }
    }

    Ok(downloaded_files)
}

fn generate_supervisor_config(
    config: &SupervisorConfig,
    service_name: &str,
    cell_id: &str,
) -> Result<()> {
    let config_path = format!("{}/{}.conf", config.conf_d_folder, service_name);

    // Read template
    let template = fs::read_to_string(&config.template_path)
        .with_context(|| format!("Failed to read supervisor template: {}", config.template_path))?;

    // Replace placeholders
    // ${service_name} = fleetman-{cell_id} (for program name)
    // ${cell_id} = cell_id without prefix (for folder paths)
    let config_content = template
        .replace("${service_name}", service_name)
        .replace("${cell_id}", cell_id);

    // Always write config to ensure it's up-to-date with current template
    fs::write(&config_path, config_content)
        .with_context(|| format!("Failed to write supervisor config: {}", config_path))?;

    info!("Generated supervisor config: {}", config_path);

    // Reload supervisor
    let output = Command::new(&config.supervisorctl_path())
        .args(&["reread"])
        .output()?;

    if !output.status.success() {
        warn!(
            "supervisorctl reread warning: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let output = Command::new(&config.supervisorctl_path())
        .args(&["update"])
        .output()?;

    if !output.status.success() {
        warn!(
            "supervisorctl update warning: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    Ok(())
}

fn generate_systemd_service(
    config: &SystemdConfig,
    service_name: &str,
    cell_id: &str,
) -> Result<()> {
    let service_path = format!("{}/{}.service", config.service_folder, service_name);

    // Read template
    let template = fs::read_to_string(&config.template_path)
        .with_context(|| format!("Failed to read systemd template: {}", config.template_path))?;

    // Replace placeholders
    // ${service_name} = fleetman-{cell_id} (for service name)
    // ${cell_id} = cell_id without prefix (for folder paths)
    let service_content = template
        .replace("${service_name}", service_name)
        .replace("${cell_id}", cell_id);

    // Always write service file to ensure it's up-to-date with current template
    fs::write(&service_path, service_content)
        .with_context(|| format!("Failed to write systemd service: {}", service_path))?;

    info!("Generated systemd service: {}", service_path);

    // Enable service (daemon-reload is called separately in the deployment flow)
    let output = Command::new(&config.systemctl_path())
        .args(&["enable", service_name])
        .output()?;

    if !output.status.success() {
        warn!(
            "systemctl enable warning: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    Ok(())
}

async fn deploy_with_simple(
    config: &AgentConfig,
    s3_client: &S3Client,
    cell_info: &CellInfo,
    version: &str,
    manifest: &ManifestSnapshot,
) -> Result<()> {
    let deploy_config = config
        .deploy
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("Deploy config not found"))?;

    let target_dir = format!("{}/{}", deploy_config.target_folder, cell_info.cell_id);

    info!("1️⃣ Deploying files to {}", target_dir);
    download_version_files(config, s3_client, cell_info, version, &target_dir, manifest).await
        .with_context(|| format!("Failed to download version {} files to {}", version, target_dir))?;

    // Overrides are snapshotted & merged into versions/<ver>/ at publish time.
    // Apply override permissions from overrides_manifest.json if present in the deployed folder.
    apply_overrides_manifest_in_place(&target_dir)?;

    info!("✅ Deployment complete");
    Ok(())
}

// Supervisor service management
fn update_supervisor_config(config: &SupervisorConfig) -> Result<()> {
    let output = Command::new(&config.supervisorctl_path())
        .args(&["update"])
        .output()?;

    debug!(
        "Update output: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        warn!("supervisorctl update returned non-zero: {}", stderr);
        // Don't fail - supervisor might already be up-to-date
    }

    Ok(())
}

fn stop_supervisor_service(config: &SupervisorConfig, service_name: &str) -> Result<()> {
    let output = Command::new(&config.supervisorctl_path())
        .args(&["stop", service_name])
        .output()?;

    // Don't fail if service is already stopped
    debug!(
        "Stop output: {}",
        String::from_utf8_lossy(&output.stdout)
    );

    Ok(())
}

fn verify_supervisor_stopped(config: &SupervisorConfig, service_name: &str) -> Result<()> {
    let output = Command::new(&config.supervisorctl_path())
        .args(&["status", service_name])
        .output()?;

    let status = String::from_utf8_lossy(&output.stdout);

    if status.contains("STOPPED") || status.contains("FATAL") || !output.status.success() {
        Ok(())
    } else {
        Err(anyhow::anyhow!(
            "Service {} is not stopped: {}",
            service_name,
            status
        ))
    }
}

fn start_supervisor_service(config: &SupervisorConfig, service_name: &str) -> Result<()> {
    let output = Command::new(&config.supervisorctl_path())
        .args(&["start", service_name])
        .output()?;

    if !output.status.success() {
        return Err(anyhow::anyhow!(
            "Failed to start service {}: {}",
            service_name,
            String::from_utf8_lossy(&output.stderr)
        ));
    }

    Ok(())
}

fn verify_supervisor_running(config: &SupervisorConfig, service_name: &str) -> Result<()> {
    let output = Command::new(&config.supervisorctl_path())
        .args(&["status", service_name])
        .output()?;

    let status = String::from_utf8_lossy(&output.stdout);

    if status.contains("RUNNING") {
        Ok(())
    } else {
        Err(anyhow::anyhow!(
            "Service {} is not running: {}",
            service_name,
            status
        ))
    }
}

// Systemd service management
fn systemd_daemon_reload(config: &SystemdConfig) -> Result<()> {
    info!("   Running systemctl daemon-reload");
    let output = Command::new(&config.systemctl_path())
        .args(&["daemon-reload"])
        .output()?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(anyhow::anyhow!(
            "systemctl daemon-reload failed: {}",
            stderr
        ));
    }

    Ok(())
}

fn stop_systemd_service(config: &SystemdConfig, service_name: &str) -> Result<()> {
    let output = Command::new(&config.systemctl_path())
        .args(&["stop", service_name])
        .output()?;

    // Don't fail if service is already stopped
    debug!(
        "Stop output: {}",
        String::from_utf8_lossy(&output.stdout)
    );

    Ok(())
}

fn verify_systemd_stopped(config: &SystemdConfig, service_name: &str) -> Result<()> {
    let output = Command::new(&config.systemctl_path())
        .args(&["is-active", service_name])
        .output()?;

    let status = String::from_utf8_lossy(&output.stdout);

    if status.contains("inactive") || status.contains("failed") || !output.status.success() {
        Ok(())
    } else {
        Err(anyhow::anyhow!(
            "Service {} is not stopped: {}",
            service_name,
            status
        ))
    }
}

fn start_systemd_service(config: &SystemdConfig, service_name: &str) -> Result<()> {
    let output = Command::new(&config.systemctl_path())
        .args(&["start", service_name])
        .output()?;

    if !output.status.success() {
        return Err(anyhow::anyhow!(
            "Failed to start service {}: {}",
            service_name,
            String::from_utf8_lossy(&output.stderr)
        ));
    }

    Ok(())
}

fn verify_systemd_running(config: &SystemdConfig, service_name: &str) -> Result<()> {
    let output = Command::new(&config.systemctl_path())
        .args(&["status", service_name])
        .output()?;

    let status = String::from_utf8_lossy(&output.stdout);

    if status.contains("Active:") && status.contains("running") {
        Ok(())
    } else {
        Err(anyhow::anyhow!(
            "Service {} is not running: {}",
            service_name,
            status
        ))
    }
}

// Utility functions
async fn create_s3_client(config: &ObjectStorageConfig) -> S3Client {
    let credentials = aws_credential_types::Credentials::new(
        &config.access_key,
        &config.secret_key,
        None,
        None,
        "static",
    );

    let mut s3_config_builder = aws_sdk_s3::config::Builder::new()
        .behavior_version(BehaviorVersion::latest())
        .region(aws_sdk_s3::config::Region::new("us-east-1"))
        .endpoint_url(&config.endpoint)
        .credentials_provider(credentials);

    if config.path_style.unwrap_or(false) {
        s3_config_builder = s3_config_builder.force_path_style(true);
    }

    S3Client::from_conf(s3_config_builder.build())
}

fn init_tracing(global_config: &GlobalConfig) -> Option<tracing_appender::non_blocking::WorkerGuard> {
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    // Determine log level from RUST_LOG env var or default to "info"
    let log_level = std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string());

    if let Some(ref log_dir) = global_config.log_directory {
        let file_prefix = global_config
            .log_file_prefix
            .as_deref()
            .unwrap_or("agent");

        let file_appender = match global_config.log_rotation.as_str() {
            "daily" => tracing_appender::rolling::daily(log_dir, file_prefix),
            "hourly" => tracing_appender::rolling::hourly(log_dir, file_prefix),
            _ => tracing_appender::rolling::never(log_dir, file_prefix),
        };

        let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);

        tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .with_writer(non_blocking)
                    .with_ansi(false)
                    .with_target(false)
                    .with_thread_ids(false),
            )
            .with(tracing_subscriber::EnvFilter::new(&log_level))
            .init();
        
        eprintln!("✅ Logging initialized to file: {}/{}", log_dir, file_prefix);
        
        Some(guard)
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(&log_level)
            .with_target(false)
            .init();
        
        eprintln!("✅ Logging initialized to stdout (level: {})", log_level);
        
        None
    }
}

fn apply_permissions(path: &Path, file: &ManifestFile) -> Result<()> {
    // Set file mode (permissions)
    if let Some(ref mode_str) = file.mode {
        let mode = u32::from_str_radix(mode_str, 8)
            .with_context(|| format!("Invalid mode '{}' for {}", mode_str, file.path))?;
        
        let permissions = fs::Permissions::from_mode(mode);
        fs::set_permissions(path, permissions)
            .with_context(|| format!("Failed to set mode {} for {}", mode_str, file.path))?;
        
        debug!("      mode: {}", mode_str);
    }

    // Set owner and group using chown
    if file.owner.is_some() || file.group.is_some() {
        let owner = file.owner.as_deref().unwrap_or("root");
        let group = file.group.as_deref().unwrap_or("root");
        let chown_arg = format!("{}:{}", owner, group);
        
        let output = Command::new("chown")
            .arg(&chown_arg)
            .arg(path)
            .output()
            .with_context(|| format!("Failed to execute chown for {}", file.path))?;
        
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(anyhow::anyhow!(
                "chown failed for {}: {}",
                file.path,
                stderr
            ));
        }
        
        debug!("      owner: {}:{}", owner, group);
    }

    Ok(())
}

// Rollback functions
async fn rollback_supervisor(
    config: &AgentConfig,
    s3_client: &S3Client,
    cell_info: &CellInfo,
    rollback_version: &str,
) -> Result<()> {
    let supervisor_config = config
        .supervisor
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("Supervisor config not found"))?;

    // Folder name: cell_id without prefix
    let service_dir = format!("{}/{}", supervisor_config.service_folder, &cell_info.cell_id);
    // Service name: fleetman-{cell_id} for easy identification
    let service_name = format!("fleetman-{}", &cell_info.cell_id);

    info!("🔙 Rollback: Updating supervisor configuration");
    update_supervisor_config(supervisor_config)?;

    info!("🔙 Rollback: Stopping service");
    stop_supervisor_service(supervisor_config, &service_name)?;
    
    info!("🔙 Rollback: Downloading version {}", rollback_version);
    
    // Load manifest from the cell version folder (snapshot source of truth)
    let manifest_key_base = format!(
        "cells/{}/{}/versions/{}/manifest.json",
        config.global.node_id,
        cell_info.cell_id,
        rollback_version
    );
    let full_manifest_key = config.object_storage.full_key(&manifest_key_base);

    let manifest_obj = s3_client
        .get_object()
        .bucket(&config.object_storage.bucket)
        .key(&full_manifest_key)
        .send()
        .await
        .with_context(|| format!("Failed to download cell manifest for rollback version {}", rollback_version))?;

    let manifest_body = manifest_obj.body.collect().await?.into_bytes();
    let manifest: ManifestSnapshot = serde_json::from_slice(&manifest_body)
        .with_context(|| "Failed to parse rollback manifest")?;
    
    // Download files
    download_version_files(config, s3_client, cell_info, rollback_version, &service_dir, &manifest).await?;
    
    // Overrides are snapshotted & merged into versions/<ver>/ at publish time.
    apply_overrides_manifest_in_place(&service_dir)?;
    
    info!("🔙 Rollback: Starting service");
    start_supervisor_service(supervisor_config, &service_name)?;
    
    info!("🔙 Rollback: Verifying service");
    tokio::time::sleep(Duration::from_secs(10)).await;
    verify_supervisor_running(supervisor_config, &service_name)?;

    Ok(())
}

async fn rollback_systemd(
    config: &AgentConfig,
    s3_client: &S3Client,
    cell_info: &CellInfo,
    rollback_version: &str,
) -> Result<()> {
    let systemd_config = config
        .systemd
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("Systemd config not found"))?;

    // Folder name: cell_id without prefix
    let service_dir = format!("{}/{}", systemd_config.working_directory, &cell_info.cell_id);
    // Service name: fleetman-{cell_id} for easy identification
    let service_name = format!("fleetman-{}", &cell_info.cell_id);

    info!("🔙 Rollback: Stopping service");
    stop_systemd_service(systemd_config, &service_name)?;
    
    info!("🔙 Rollback: Downloading version {}", rollback_version);
    
    // Load manifest from the cell version folder (snapshot source of truth)
    let manifest_key_base = format!(
        "cells/{}/{}/versions/{}/manifest.json",
        config.global.node_id,
        cell_info.cell_id,
        rollback_version
    );
    let full_manifest_key = config.object_storage.full_key(&manifest_key_base);

    let manifest_obj = s3_client
        .get_object()
        .bucket(&config.object_storage.bucket)
        .key(&full_manifest_key)
        .send()
        .await
        .with_context(|| format!("Failed to download cell manifest for rollback version {}", rollback_version))?;

    let manifest_body = manifest_obj.body.collect().await?.into_bytes();
    let manifest: ManifestSnapshot = serde_json::from_slice(&manifest_body)
        .with_context(|| "Failed to parse rollback manifest")?;
    
    // Download files
    download_version_files(config, s3_client, cell_info, rollback_version, &service_dir, &manifest).await?;
    
    // Overrides are snapshotted & merged into versions/<ver>/ at publish time.
    apply_overrides_manifest_in_place(&service_dir)?;
    
    info!("🔙 Rollback: Running daemon-reload");
    systemd_daemon_reload(systemd_config)?;
    
    info!("🔙 Rollback: Starting service");
    start_systemd_service(systemd_config, &service_name)?;
    
    info!("🔙 Rollback: Verifying service (waiting 10s)");
    tokio::time::sleep(Duration::from_secs(10)).await;
    verify_systemd_running(systemd_config, &service_name)?;

    Ok(())
}

/// Get the service directory based on the profile and config
fn get_service_directory(config: &AgentConfig, cell_id: &str, profile: &str) -> Option<String> {
    match profile.to_lowercase().as_str() {
        "supervisor" => {
            config.supervisor.as_ref().map(|s| format!("{}/{}", s.service_folder, cell_id))
        }
        "systemd" => {
            // For systemd, we use the same pattern as supervisor (service_folder/cell_id)
            // This is based on the deployment logic in deploy_with_systemd
            config.systemd.as_ref().map(|s| format!("{}/{}", s.service_folder, cell_id))
        }
        "deploy" => {
            config.deploy.as_ref().map(|d| format!("{}/{}", d.target_folder, cell_id))
        }
        _ => None,
    }
}

/// Collect logs from specified files in the service directory
/// Returns a JSON object with log file paths as keys and arrays of log lines as values
fn collect_logs(
    service_dir: &str,
    manifest: &ManifestSnapshot,
) -> Option<serde_json::Value> {
    if !manifest.collect_logs || manifest.log_files.is_empty() {
        return None;
    }
    
    let mut logs = serde_json::Map::new();
    
    for log_file in &manifest.log_files {
        // Validate log file path (no absolute paths, no escapes)
        if log_file.starts_with('/') || log_file.starts_with("../") || log_file.starts_with("./")
            || log_file.contains("/../") || log_file.contains("/./") {
            warn!("Skipping invalid log file path: {}", log_file);
            continue;
        }
        
        let log_path = PathBuf::from(service_dir).join(log_file);
        
        // Read the file and collect last N lines
        match std::fs::read_to_string(&log_path) {
            Ok(content) => {
                let lines: Vec<String> = content
                    .lines()
                    .rev()
                    .take(manifest.max_log_lines)
                    .map(|s| s.to_string())
                    .collect::<Vec<_>>()
                    .into_iter()
                    .rev()
                    .collect();
                
                logs.insert(log_file.clone(), serde_json::json!(lines));
                debug!("Collected {} lines from log file: {}", lines.len(), log_file);
            }
            Err(e) => {
                warn!("Failed to read log file {}: {}", log_file, e);
                logs.insert(log_file.clone(), serde_json::Value::Null);
            }
        }
    }
    
    if logs.is_empty() {
        None
    } else {
        Some(serde_json::Value::Object(logs))
    }
}

async fn write_status_json(
    config: &AgentConfig,
    s3_client: &S3Client,
    cell_info: &CellInfo,
    current_version: &str,
    timestamp: u64,
    error: Option<&str>,
    logs: Option<serde_json::Value>,
) -> Result<()> {
    let mut status = serde_json::json!({
        "running_version": current_version,
        "applied_at": timestamp,
        "status": if error.is_none() { "success" } else { "failed" },
        "error": error,
    });
    
    // Add logs if provided
    if let Some(logs_data) = logs {
        status.as_object_mut().unwrap().insert("logs".to_string(), logs_data);
    }
    
    let status_content = serde_json::to_string_pretty(&status)?;
    
    let status_key_base = format!("cells/{}/{}/status.json", config.global.node_id, cell_info.cell_id);
    let status_key = config.object_storage.full_key(&status_key_base);
    
    let status_bytes = status_content.into_bytes();
    retry_s3_operation(
        &format!("write_status_{}", cell_info.cell_id),
        config.global.s3_retry_count,
        config.global.s3_retry_delay_ms,
        || async {
            s3_client
                .put_object()
                .bucket(&config.object_storage.bucket)
                .key(&status_key)
                .body(ByteStream::from(status_bytes.clone()))
                .send()
                .await
        },
    ).await?;
    
    debug!("Wrote status.json for cell {}: version={}, status={}", 
           cell_info.cell_id, current_version, if error.is_none() { "success" } else { "failed" });
    
    Ok(())
}

fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

