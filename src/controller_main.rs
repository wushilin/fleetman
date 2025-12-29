mod controller_config;
mod controller_s3;
mod s3_retry;

use askama::Template;
use axum::{
    body::Body,
    extract::{Path as AxumPath, Query, State as AxumState},
    http::StatusCode,
    response::{Html, IntoResponse, Redirect, Response, Json},
    routing::{get, post},
    Form, Router,
};
use axum_extra::extract::Multipart;
use aws_sdk_s3::{primitives::ByteStream, Client};
use chrono::Local;
use controller_config::{BucketConfig, CellConfig, ControllerConfig, DeploymentManifest, DeploymentProfile, FileType, ManifestFile};
use controller_config::ResourceProfile;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};
// (tokio::time::{sleep, Duration} were previously used by inline retry logic; now in `s3_retry`.)
use tower_http::trace::TraceLayer;
use futures::future::join_all;

type AppState = Arc<RwLock<ControllerConfig>>;

use s3_retry::retry_s3_operation;
use controller_s3::create_s3_client;

const CONFIG_FILE: &str = "controller_config.yaml";

#[derive(Template)]
#[template(path = "dashboard.html")]
struct DashboardTemplate {
    bucket_count: usize,
    manifest_count: usize,
}

#[derive(Template)]
#[template(path = "buckets.html")]
struct BucketsTemplate {
    buckets: Vec<(String, BucketConfig)>,
}

#[derive(Template)]
#[template(path = "manifests.html")]
struct ManifestsTemplate {
    manifests: Vec<(String, DeploymentManifest)>,
    buckets: Vec<(String, BucketConfig)>,
    resource_profiles: Vec<String>,
}

#[derive(Template)]
#[template(path = "edit_manifest.html")]
struct EditManifestTemplate {
    manifest_name: String,
    manifest: DeploymentManifest,
    buckets: Vec<(String, BucketConfig)>,
    selected_bucket: String,
    resource_profiles: Vec<String>,
}

#[derive(Template)]
#[template(path = "deployments.html")]
struct DeploymentsTemplate {
    existing_deployments: Vec<ExistingDeployment>,
}

#[derive(Template)]
#[template(path = "cleanup_deployment.html")]
struct CleanupDeploymentTemplate {
    orphan_versions: Vec<CleanupOrphanVersionRow>,
    orphan_manifests_count: usize,
    orphan_versions_count: usize,
    orphan_objects_total: usize,
}

#[derive(Debug, Clone)]
struct CleanupOrphanVersionRow {
    bucket_display: String,
    manifest_name: String,
    version: String,
    object_count: usize,
}

#[derive(Template)]
#[template(path = "recycle_bin.html")]
struct RecycleBinTemplate {
    entries: Vec<RecycleEntryDisplay>,
    total_objects: usize,
}

#[derive(Template)]
#[template(path = "recycle_clear.html")]
struct RecycleClearTemplate {
    entries: Vec<RecycleEntryDisplay>,
    total_objects: usize,
}

#[derive(Debug, Clone)]
struct RecycleEntryDisplay {
    bucket_display: String,
    recycle_prefix_base: String, // without bucket prefix
    manifest_name: String,
    version: String,
    deleted_at_raw: String,
    deleted_at_display: String,
    object_count: usize,
}

#[derive(Template)]
#[template(path = "view_deployment.html")]
struct ViewDeploymentTemplate {
    manifest_name: String,
    version: String,
    last_version: Option<String>,
    is_edit_mode: bool,
    is_finalized: bool,
    finalized_at: Option<String>,
}

#[derive(Template)]
#[template(path = "create_deployment.html")]
struct CreateDeploymentTemplate {
    manifests: Vec<String>,
}

#[derive(Clone, Serialize)]
struct VersionInfo {
    version: String,
    is_finalized: bool,
}

struct ExistingDeployment {
    manifest_name: String,
    versions: Vec<VersionInfo>,
}

#[derive(Template)]
#[template(path = "edit_file.html")]
struct EditFileTemplate {
    manifest: String,
    version: String,
    file_path: String,
    content: String,
    is_readonly: bool,
}

#[derive(Template)]
#[template(path = "agents.html")]
struct AgentsTemplate {
    agents: Vec<AgentDisplay>,
}

#[derive(Template)]
#[template(path = "resource_profiles.html")]
#[allow(dead_code)]
struct ResourceProfilesTemplate {
    profiles: Vec<ResourceProfileDisplay>,
    add_form: AddResourceProfileFormValues,
    error: Option<String>,
}

#[derive(Debug, Clone)]
struct ResourceProfileDisplay {
    name: String,
    cpu_display: String,
    memory_display: String,
    memory_swap_display: String,
    io_weight_display: String,
    editable: bool,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
struct AddResourceProfileFormValues {
    name: String,
    cpu: String,
    memory: String,
    memory_swap: String,
    io_weight: String,
}

const BUILTIN_PROFILE_UNLIMITED: &str = "unlimited";

fn builtin_unlimited_profile() -> ResourceProfile {
    ResourceProfile {
        cpu: None,
        memory: None,
        memory_swap: None,
        io_weight: Some(100),
    }
}

const MIN_RAM_BYTES: u64 = 5 * 1024 * 1024;

#[derive(Template)]
#[template(path = "systemd.service.template", escape = "none")]
struct SystemdServiceUnitTemplate {
    // Emit agent-owned placeholders verbatim; Fleetman must not assume it knows the agent's
    // cell folder mapping.
    fleetagent_cell_id: String,
    fleetagent_service_working_dir: String,
    // Fleetman-owned run-as identity.
    service_user: String,
    service_group: String,
    cpu_quota: Option<String>,     // e.g. "50%"
    memory_max: Option<String>,    // e.g. "2G"
    memory_swap_max: Option<String>, // e.g. "2G" or "0"
    io_weight: Option<u32>,
}

#[derive(Template)]
#[template(path = "pm.service.yml.template", escape = "none")]
#[allow(dead_code)]
struct ProcessMasterServiceTemplate {
    fleetagent_cell_id: String,
    fleetagent_service_working_dir: String,
    service_user: String,
    service_group: String,
    cpu: Option<String>,
    memory: Option<String>,
    memory_swap: Option<String>,
    io_weight: Option<u32>,
}

#[derive(Template)]
#[template(path = "run.sh.template", escape = "none")]
struct RunShTemplate {
    my_program: String,
}

fn resource_profile_names(config: &ControllerConfig) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    out.push(BUILTIN_PROFILE_UNLIMITED.to_string());
    let mut keys: Vec<String> = config.resource_profiles.keys().cloned().collect();
    keys.sort();
    out.extend(keys);
    out
}

fn resource_profile_exists(config: &ControllerConfig, name: &str) -> bool {
    let n = name.trim();
    if n.is_empty() {
        return false;
    }
    n == BUILTIN_PROFILE_UNLIMITED || config.resource_profiles.contains_key(n)
}

fn required_service_file_and_kind(profile: &DeploymentProfile) -> Option<(&'static str, &'static str)> {
    match profile {
        DeploymentProfile::Systemd => Some(("systemd", "app.service")),
        DeploymentProfile::ProcessMaster => Some(("processmaster", "service.yml")),
        _ => None,
    }
}

fn parse_mode_octal(mode: &str) -> Result<u32, String> {
    let t = mode.trim();
    if t.is_empty() {
        return Err("mode is empty".to_string());
    }
    u32::from_str_radix(t, 8).map_err(|_| {
        format!(
            "Invalid mode: {}. Mode must be an octal number (e.g., 0700, 0755).",
            mode
        )
    })
}

fn default_override_mode(path: &str, file_type: &FileType) -> &'static str {
    let is_folder = matches!(file_type, FileType::Folder) || path.ends_with('/');
    let is_run_sh = path == "run.sh" || path.ends_with("/run.sh");
    if is_folder || is_run_sh {
        "0700"
    } else {
        "0600"
    }
}

fn validate_run_sh(files: &[ManifestFile], profile: &DeploymentProfile) -> Result<(), String> {
    // For systemd/processmaster (and legacy supervisor), require run.sh as a text file with mode >= 0700 and owner execute bit.
    let require = matches!(
        profile,
        DeploymentProfile::Systemd | DeploymentProfile::ProcessMaster | DeploymentProfile::Supervisor
    );
    if !require {
        return Ok(());
    }

    let Some(f) = files.iter().find(|f| f.path == "run.sh") else {
        return Err(format!("run.sh is required for {} profile.", profile));
    };
    if f.file_type != FileType::Text {
        return Err("run.sh must be a text file.".to_string());
    }
    // Mode is optional. If not specified, the agent defaults run.sh to 0700.
    if let Some(mstr) = f.mode.as_deref().map(|s| s.trim()).filter(|s| !s.is_empty()) {
        let mode = parse_mode_octal(mstr)?;
        if (mode & 0o100) == 0 {
            return Err(format!(
                "run.sh must be executable (owner execute bit). Current mode: {}. Suggested: 0700, 0750, or 0755.",
                mstr
            ));
        }
        if mode < 0o700 {
            return Err(format!(
                "run.sh must have at least 0700 permission. Current mode: {}. Suggested: 0700, 0750, or 0755.",
                mstr
            ));
        }
    }
    Ok(())
}

fn validate_logs_folder(files: &[ManifestFile], profile: &DeploymentProfile) -> Result<(), String> {
    let require = matches!(profile, DeploymentProfile::Systemd | DeploymentProfile::ProcessMaster);
    if !require {
        return Ok(());
    }
    let Some(f) = files.iter().find(|f| f.path == "logs/") else {
        return Err("logs/ folder is required for systemd/processmaster profiles.".to_string());
    };
    if f.file_type != FileType::Folder {
        return Err("logs/ must be a folder entry (path should end with '/').".to_string());
    }
    Ok(())
}

fn validate_service_folder_meta(mode: &str) -> Result<(), String> {
    let m = parse_mode_octal(mode)?;
    if m < 0o700 {
        return Err(format!(
            "service_folder_mode must be at least 0700. Current: {}",
            mode
        ));
    }
    Ok(())
}

fn validate_service_identity(owner: &str, group: &str) -> Result<(), String> {
    let owner_t = owner.trim();
    if owner_t.is_empty() {
        return Err("service_owner cannot be empty".to_string());
    }
    if owner_t.contains(|c: char| c.is_whitespace()) {
        return Err("service_owner cannot contain whitespace".to_string());
    }
    let group_t = group.trim();
    if group_t.is_empty() {
        return Err("service_group cannot be empty".to_string());
    }
    if group_t.contains(|c: char| c.is_whitespace()) {
        return Err("service_group cannot contain whitespace".to_string());
    }
    Ok(())
}

fn validate_optional_owner_group(field: &str, v: &Option<String>) -> Result<(), String> {
    let Some(s) = v.as_deref().map(|x| x.trim()).filter(|x| !x.is_empty()) else {
        return Ok(());
    };
    if s.contains(|c: char| c.is_whitespace()) {
        return Err(format!("{} cannot contain whitespace", field));
    }
    Ok(())
}

fn parse_cpu_millis(s: &str) -> Result<Option<u32>, String> {
    let t = s.trim();
    if t.is_empty() {
        return Ok(None);
    }
    let lower = t.to_ascii_lowercase();
    if !lower.ends_with('m') {
        return Err("cpu must be in the form NNNm (e.g. 200m)".to_string());
    }
    let num_str = lower.trim_end_matches('m');
    let n: u32 = num_str
        .parse()
        .map_err(|_| "cpu must be in the form NNNm (e.g. 200m)".to_string())?;
    if n < 100 {
        return Err("cpu must be at least 100m".to_string());
    }
    Ok(Some(n))
}

fn parse_bytes(s: &str) -> Result<Option<u64>, String> {
    let t = s.trim();
    if t.is_empty() {
        return Ok(None);
    }
    // No spaces allowed between number and unit (e.g. "512MiB" ok, "512 MiB" rejected).
    let re = Regex::new(r"^([0-9]+)([A-Za-z]*)$").map_err(|e| e.to_string())?;
    let caps = re
        .captures(t)
        .ok_or_else(|| "invalid size (expected: <number><unit>, e.g. 512MiB, 2G, 200K, 0b)".to_string())?;
    let num: u64 = caps
        .get(1)
        .unwrap()
        .as_str()
        .parse()
        .map_err(|_| "invalid number in size".to_string())?;
    let mut unit = caps.get(2).unwrap().as_str().to_ascii_lowercase();
    if unit.ends_with('b') {
        unit.pop();
    }
    let mult: u64 = match unit.as_str() {
        "" => 1,
        "k" => 1_000,
        "m" => 1_000_000,
        "g" => 1_000_000_000,
        "ki" => 1024,
        "mi" => 1024 * 1024,
        "gi" => 1024 * 1024 * 1024,
        _ => {
            return Err(
                "invalid unit (allowed: k,m,g,ki,mi,gi with optional 'B' suffix; e.g. 512MiB, 2G, 200K, 0b)"
                    .to_string(),
            )
        }
    };
    Ok(Some(
        num.checked_mul(mult)
            .ok_or_else(|| "size is too large".to_string())?,
    ))
}

fn format_systemd_bytes(bytes: u64) -> String {
    // Prefer 1024-based K/M/G suffixes when divisible, else raw bytes.
    const K: u64 = 1024;
    const M: u64 = 1024 * 1024;
    const G: u64 = 1024 * 1024 * 1024;
    if bytes == 0 {
        return "0".to_string();
    }
    if bytes % G == 0 {
        return format!("{}G", bytes / G);
    }
    if bytes % M == 0 {
        return format!("{}M", bytes / M);
    }
    if bytes % K == 0 {
        return format!("{}K", bytes / K);
    }
    bytes.to_string()
}

fn format_cpu_quota_percent(millis: u32) -> String {
    // 1000m == 100% of one core.
    let pct10: u32 = millis;
    let int_part = pct10 / 10;
    let frac = pct10 % 10;
    if frac == 0 {
        format!("{}%", int_part)
    } else {
        format!("{}.{}%", int_part, frac)
    }
}

fn validate_and_normalize_resource_profile(
    cpu_in: &str,
    ram_in: &str,
    swap_in: &str,
    io_weight_in: &str,
) -> Result<(Option<String>, Option<String>, Option<String>, Option<u32>, Option<u32>, Option<u64>, Option<u64>), String> {
    // returns: (cpu_str, ram_str, swap_str, io_weight, cpu_millis, ram_bytes, swap_bytes)
    let cpu_millis = parse_cpu_millis(cpu_in)?;
    let cpu = cpu_millis.map(|m| format!("{}m", m));

    let ram_bytes = parse_bytes(ram_in)?;
    if let Some(b) = ram_bytes {
        if b < MIN_RAM_BYTES {
            return Err(format!(
                "ram must be at least 5MiB ({} bytes); got {} bytes",
                MIN_RAM_BYTES, b
            ));
        }
    }
    let ram = normalize_opt_string(ram_in);

    // swap defaults to 0b (unless user specifies), and has no minimum.
    let swap_bytes = if swap_in.trim().is_empty() {
        Some(0)
    } else {
        parse_bytes(swap_in)?
    };
    let swap = Some("0b".to_string()).filter(|_| swap_in.trim().is_empty()).or_else(|| normalize_opt_string(swap_in));

    let io_weight = parse_opt_u32(io_weight_in)?;
    if let Some(w) = io_weight {
        if !(1..=10000).contains(&w) {
            return Err("io_weight must be in 1..=10000".to_string());
        }
    }

    Ok((cpu, ram, swap, io_weight, cpu_millis, ram_bytes, swap_bytes))
}

#[allow(dead_code)] // fields are used by Askama templates via macro-generated code
#[derive(Debug, Clone, Deserialize)]
struct AgentStatusCell {
    cell_id: String,
    #[serde(default)]
    manifest_name: String,
    #[serde(default)]
    profile: String,
    #[serde(default)]
    version: Option<String>,
    #[serde(default)]
    running: Option<bool>, // None => N/A (deploy)
}

#[derive(Debug, Clone, Deserialize)]
struct AgentStatusReport {
    report_timestamp: u64,
    node_id: String,
    #[serde(default)]
    cells: Vec<AgentStatusCell>,
}

#[allow(dead_code)] // fields are used by Askama templates via macro-generated code
#[derive(Debug, Clone)]
struct AgentDisplayCell {
    cell_id: String,
    version: String,
    // Back-compat with older template compiles / tooling caches.
    // (The actual template uses `version`, but keeping this avoids spurious diagnostics.)
    version_display: String,
    manifest_name: String,
    profile: String,
    running_label: String,
    running_badge_class: String,
}

#[allow(dead_code)] // fields are used by Askama templates via macro-generated code
#[derive(Debug, Clone)]
struct AgentDisplay {
    node_id: String,
    bucket_display: String,
    status_label: String,
    status_badge_class: String,
    is_dead: bool,
    last_report_timestamp: u64,
    last_report_human: String,
    age_seconds: Option<u64>,
    age_human: String,
    cells_joined: String,
    cells: Vec<AgentDisplayCell>,
}

#[derive(Deserialize)]
struct BucketForm {
    display_name: String,
    bucket_name: String,
    endpoint: String,
    access_key: String,
    secret_key: String,
    #[serde(default)]
    path_style: bool,
    prefix: String,  // Required: must contain only alphanumerics, hyphens, and underscores
}

#[derive(Deserialize)]
struct DeleteForm {
    name: String,
}

#[derive(Deserialize)]
struct EditManifestQuery {
    name: String,
}

#[derive(Deserialize)]
struct UpdateManifestForm {
    original_name: String,
    name: String,
    profile: String,
    #[serde(default)]
    resource_profile: Option<String>,
    service_owner: String,
    service_group: String,
    #[serde(default)]
    service_folder_mode: Option<String>,
    #[serde(default)]
    service_folder_owner: Option<String>,
    #[serde(default)]
    service_folder_group: Option<String>,
    bucket: String,
    files: String,
    file_types: String,
    owners: String,
    groups: String,
    modes: String,
    #[serde(default)]
    collect_logs: Option<String>,
    #[serde(default)]
    log_files: Option<String>,
    #[serde(default)]
    max_log_lines: Option<String>,
}

#[derive(Deserialize)]
struct ManifestForm {
    name: String,
    profile: String,
    #[serde(default)]
    resource_profile: Option<String>,
    service_owner: String,
    service_group: String,
    #[serde(default)]
    service_folder_mode: Option<String>,
    #[serde(default)]
    service_folder_owner: Option<String>,
    #[serde(default)]
    service_folder_group: Option<String>,
    bucket: String,
    files: String,
    file_types: String,
    owners: String,
    groups: String,
    modes: String,
    #[serde(default)]
    collect_logs: Option<String>,
    #[serde(default)]
    log_files: Option<String>,
    #[serde(default)]
    max_log_lines: Option<String>,
}

#[derive(Deserialize)]
struct DeploymentQuery {
    manifest: String,
    version: Option<String>,
    clone_from: Option<String>,
}

#[derive(Deserialize)]
struct FileQuery {
    manifest: String,
    version: String,
}

#[derive(Deserialize)]
struct EditQuery {
    manifest: String,
    version: String,
    file_path: String,
}

#[derive(Deserialize)]
struct SaveForm {
    manifest: String,
    version: String,
    file_path: String,
    content: String,
}

#[derive(Deserialize)]
struct FileStatusQuery {
    manifest: String,
    version: String,
}

#[derive(Deserialize)]
struct DeleteFileForm {
    manifest: String,
    version: String,
    file_path: String,
}

#[derive(Deserialize)]
struct DeleteVersionForm {
    manifest: String,
    version: String,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let config = load_or_create_config();
    let state = Arc::new(RwLock::new(config));

    let app = Router::new()
        .route("/", get(dashboard))
        .route("/buckets", get(buckets_page))
        .route("/buckets/add", post(add_bucket))
        .route("/buckets/delete", post(delete_bucket))
        .route("/resource-profiles", get(resource_profiles_page))
        .route("/resource-profiles/add", post(add_resource_profile))
        .route("/resource-profiles/edit", get(edit_resource_profile_page))
        .route("/resource-profiles/update", post(update_resource_profile))
        .route("/resource-profiles/delete", post(delete_resource_profile))
        .route("/manifests", get(manifests_page))
        .route("/manifests/add", post(add_manifest))
        .route("/manifests/edit", get(edit_manifest_page))
        .route("/manifests/update", post(update_manifest))
        .route("/manifests/delete", post(delete_manifest))
        .route("/cells", get(cells_page))
        .route("/cells/add", get(add_cell_page).post(add_cell))
        .route("/cells/delete", post(delete_cell))
        .route("/cells/:node_id/:cell_id/status", get(view_cell_status))
        .route("/cells/:node_id/:cell_id/overrides", get(cell_overrides_page))
        .route("/cells/:node_id/:cell_id/overrides/upload", post(upload_cell_override))
        .route("/cells/:node_id/:cell_id/overrides/delete", post(delete_cell_override))
        .route("/cells/:node_id/:cell_id/overrides/edit", get(edit_cell_override))
        .route("/cells/:node_id/:cell_id/overrides/save", post(save_cell_override))
        .route("/cells/:node_id/:cell_id/overrides/update-meta", post(update_cell_override_meta))
        .route("/cells/:node_id/:cell_id/versions", get(cell_versions_page))
        .route("/cells/:node_id/:cell_id/versions/delete", post(delete_cell_version))
        .route("/cells/:node_id/:cell_id/publish", post(publish_deployment_to_cell))
        .route("/agents", get(agents_page))
        .route("/agents/delete", post(delete_agent))
        .route("/deployments", get(deployments_page))
        .route("/deployments/create", get(deployments_create))
        .route("/deployments/view", get(view_deployment))
        .route("/deployments/start", get(deployments_start))
        .route("/deployments/upload", post(upload_file))
        .route("/deployments/edit", get(edit_file))
        .route("/deployments/save", post(save_file))
        .route("/deployments/download", get(download_file))
        .route("/deployments/files", get(list_files))
        .route("/deployments/file-status", get(get_file_status))
        .route("/deployments/generate-service-file", post(generate_service_file))
        .route("/deployments/delete", post(delete_file))
        .route("/deployments/delete_version", post(delete_version))
        .route("/deployments/cleanup", get(deployments_cleanup_preview).post(deployments_cleanup_execute))
        .route("/deployments/recycle", get(recycle_bin_page))
        .route("/deployments/recycle/restore", post(recycle_restore))
        .route("/deployments/recycle/delete", post(recycle_delete_permanently))
        .route("/deployments/recycle/clear", get(recycle_clear_preview).post(recycle_clear_execute))
        .route("/deployments/finish", post(finish_deployment))
        .route("/deployments/bulk-publish", post(bulk_publish_deployment))
        .route("/deployments/staging/browse", get(browse_staging))
        .route("/deployments/staging/copy", post(copy_from_staging))
        .route("/api/manifest-versions", get(get_manifest_versions))
        .route("/api/app-versions", get(get_app_versions))
        .route("/api/app-cells", get(get_app_cells))
        .route("/api/service-template", get(api_service_template))
        .route("/api/cell-version-exists", get(api_cell_version_exists))
        .route("/api/cell-versions-exist", get(api_cell_versions_exist))
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000")
        .await
        .unwrap();

    println!("🚀 Fleetman Controller running at http://0.0.0.0:3000");
    axum::serve(listener, app).await.unwrap();
}

fn format_timestamp_local(ts: u64) -> String {
    // Render as local time string; keep it simple and consistent.
    let dt = chrono::DateTime::<chrono::Local>::from(UNIX_EPOCH + std::time::Duration::from_secs(ts));
    dt.format("%Y-%m-%d %H:%M:%S %Z").to_string()
}

fn format_age(age_seconds: u64) -> String {
    if age_seconds < 60 {
        format!("{}s ago", age_seconds)
    } else if age_seconds < 3600 {
        format!("{}m ago", age_seconds / 60)
    } else {
        format!("{}h ago", age_seconds / 3600)
    }
}

async fn dashboard(AxumState(state): AxumState<AppState>) -> impl IntoResponse {
    let config = state.read().unwrap();
    let template = DashboardTemplate {
        bucket_count: config.buckets.len(),
        manifest_count: config.manifests.len(),
    };
    HtmlTemplate(template)
}

#[derive(Deserialize)]
struct DeleteAgentForm {
    bucket: String, // bucket DISPLAY name (key in controller config)
    node_id: String,
}

#[derive(Deserialize)]
struct AddResourceProfileForm {
    name: String,
    #[serde(default)]
    cpu: String,
    #[serde(default)]
    memory: String,
    #[serde(default)]
    memory_swap: String,
    #[serde(default)]
    io_weight: String,
}

#[derive(Deserialize)]
struct EditResourceProfileQuery {
    name: String,
}

#[derive(Deserialize)]
struct UpdateResourceProfileForm {
    name: String,
    #[serde(default)]
    cpu: String,
    #[serde(default)]
    memory: String,
    #[serde(default)]
    memory_swap: String,
    #[serde(default)]
    io_weight: String,
}

#[derive(Deserialize)]
struct DeleteResourceProfileForm {
    name: String,
}

fn normalize_opt_string(s: &str) -> Option<String> {
    let t = s.trim();
    if t.is_empty() {
        None
    } else {
        Some(t.to_string())
    }
}

fn parse_opt_u32(s: &str) -> Result<Option<u32>, String> {
    let t = s.trim();
    if t.is_empty() {
        return Ok(None);
    }
    let v: u32 = t.parse().map_err(|_| format!("Invalid number: {}", t))?;
    Ok(Some(v))
}

fn profile_display_value(s: &Option<String>) -> String {
    s.clone().unwrap_or_else(|| "unlimited".to_string())
}

fn profile_display_u32(v: &Option<u32>, default_if_none: Option<u32>) -> String {
    match (v, default_if_none) {
        (Some(x), _) => x.to_string(),
        (None, Some(d)) => d.to_string(),
        (None, None) => "unlimited".to_string(),
    }
}

async fn resource_profiles_page(AxumState(state): AxumState<AppState>) -> impl IntoResponse {
    let mut profiles: Vec<ResourceProfileDisplay> = Vec::new();

    // Built-in unlimited (not editable).
    let unlimited = builtin_unlimited_profile();
    profiles.push(ResourceProfileDisplay {
        name: BUILTIN_PROFILE_UNLIMITED.to_string(),
        cpu_display: profile_display_value(&unlimited.cpu),
        memory_display: profile_display_value(&unlimited.memory),
        memory_swap_display: profile_display_value(&unlimited.memory_swap),
        io_weight_display: profile_display_u32(&unlimited.io_weight, Some(100)),
        editable: false,
    });

    // User-defined.
    {
        let cfg = state.read().unwrap();
        let mut keys: Vec<_> = cfg.resource_profiles.keys().cloned().collect();
        keys.sort();
        for k in keys {
            if let Some(p) = cfg.resource_profiles.get(&k) {
                profiles.push(ResourceProfileDisplay {
                    name: k,
                    cpu_display: profile_display_value(&p.cpu),
                    memory_display: profile_display_value(&p.memory),
                    memory_swap_display: profile_display_value(&p.memory_swap),
                    io_weight_display: profile_display_u32(&p.io_weight, None),
                    editable: true,
                });
            }
        }
    }

    HtmlTemplate(ResourceProfilesTemplate {
        profiles,
        add_form: AddResourceProfileFormValues {
            name: "".to_string(),
            cpu: "".to_string(),
            memory: "".to_string(),
            memory_swap: "".to_string(),
            io_weight: "".to_string(),
        },
        error: None,
    })
}

async fn add_resource_profile(
    AxumState(state): AxumState<AppState>,
    Form(form): Form<AddResourceProfileForm>,
) -> impl IntoResponse {
    let name = form.name.trim().to_string();
    if let Err(e) = validate_display_name(&name) {
        let mut profiles: Vec<ResourceProfileDisplay> = Vec::new();
        let unlimited = builtin_unlimited_profile();
        profiles.push(ResourceProfileDisplay {
            name: BUILTIN_PROFILE_UNLIMITED.to_string(),
            cpu_display: profile_display_value(&unlimited.cpu),
            memory_display: profile_display_value(&unlimited.memory),
            memory_swap_display: profile_display_value(&unlimited.memory_swap),
            io_weight_display: profile_display_u32(&unlimited.io_weight, Some(100)),
            editable: false,
        });
        {
            let cfg = state.read().unwrap();
            let mut keys: Vec<_> = cfg.resource_profiles.keys().cloned().collect();
            keys.sort();
            for k in keys {
                if let Some(p) = cfg.resource_profiles.get(&k) {
                    profiles.push(ResourceProfileDisplay {
                        name: k,
                        cpu_display: profile_display_value(&p.cpu),
                        memory_display: profile_display_value(&p.memory),
                        memory_swap_display: profile_display_value(&p.memory_swap),
                        io_weight_display: profile_display_u32(&p.io_weight, None),
                        editable: true,
                    });
                }
            }
        }
        return HtmlTemplate(ResourceProfilesTemplate {
            profiles,
            add_form: AddResourceProfileFormValues {
                name: form.name.clone(),
                cpu: form.cpu.clone(),
                memory: form.memory.clone(),
                memory_swap: form.memory_swap.clone(),
                io_weight: form.io_weight.clone(),
            },
            error: Some(format!("Invalid profile name: {}", e)),
        })
        .into_response();
    }
    if name == BUILTIN_PROFILE_UNLIMITED {
        let mut profiles: Vec<ResourceProfileDisplay> = Vec::new();
        let unlimited = builtin_unlimited_profile();
        profiles.push(ResourceProfileDisplay {
            name: BUILTIN_PROFILE_UNLIMITED.to_string(),
            cpu_display: profile_display_value(&unlimited.cpu),
            memory_display: profile_display_value(&unlimited.memory),
            memory_swap_display: profile_display_value(&unlimited.memory_swap),
            io_weight_display: profile_display_u32(&unlimited.io_weight, Some(100)),
            editable: false,
        });
        {
            let cfg = state.read().unwrap();
            let mut keys: Vec<_> = cfg.resource_profiles.keys().cloned().collect();
            keys.sort();
            for k in keys {
                if let Some(p) = cfg.resource_profiles.get(&k) {
                    profiles.push(ResourceProfileDisplay {
                        name: k,
                        cpu_display: profile_display_value(&p.cpu),
                        memory_display: profile_display_value(&p.memory),
                        memory_swap_display: profile_display_value(&p.memory_swap),
                        io_weight_display: profile_display_u32(&p.io_weight, None),
                        editable: true,
                    });
                }
            }
        }
        return HtmlTemplate(ResourceProfilesTemplate {
            profiles,
            add_form: AddResourceProfileFormValues {
                name: form.name.clone(),
                cpu: form.cpu.clone(),
                memory: form.memory.clone(),
                memory_swap: form.memory_swap.clone(),
                io_weight: form.io_weight.clone(),
            },
            error: Some("Profile name 'unlimited' is reserved".to_string()),
        })
        .into_response();
    }

    let (cpu, ram, swap, io_weight, _cpu_millis, _ram_bytes, _swap_bytes) = match validate_and_normalize_resource_profile(
        &form.cpu,
        &form.memory,
        &form.memory_swap,
        &form.io_weight,
    ) {
        Ok(v) => v,
        Err(e) => {
            let mut profiles: Vec<ResourceProfileDisplay> = Vec::new();
            let unlimited = builtin_unlimited_profile();
            profiles.push(ResourceProfileDisplay {
                name: BUILTIN_PROFILE_UNLIMITED.to_string(),
                cpu_display: profile_display_value(&unlimited.cpu),
                memory_display: profile_display_value(&unlimited.memory),
                memory_swap_display: profile_display_value(&unlimited.memory_swap),
                io_weight_display: profile_display_u32(&unlimited.io_weight, Some(100)),
                editable: false,
            });
            {
                let cfg = state.read().unwrap();
                let mut keys: Vec<_> = cfg.resource_profiles.keys().cloned().collect();
                keys.sort();
                for k in keys {
                    if let Some(p) = cfg.resource_profiles.get(&k) {
                        profiles.push(ResourceProfileDisplay {
                            name: k,
                            cpu_display: profile_display_value(&p.cpu),
                            memory_display: profile_display_value(&p.memory),
                            memory_swap_display: profile_display_value(&p.memory_swap),
                            io_weight_display: profile_display_u32(&p.io_weight, None),
                            editable: true,
                        });
                    }
                }
            }
            return HtmlTemplate(ResourceProfilesTemplate {
                profiles,
                add_form: AddResourceProfileFormValues {
                    name: form.name.clone(),
                    cpu: form.cpu.clone(),
                    memory: form.memory.clone(),
                    memory_swap: form.memory_swap.clone(),
                    io_weight: form.io_weight.clone(),
                },
                error: Some(format!("Invalid resource profile: {}", e)),
            })
            .into_response();
        }
    };

    let profile = ResourceProfile { cpu, memory: ram, memory_swap: swap, io_weight };

    {
        let mut cfg = state.write().unwrap();
        if cfg.resource_profiles.contains_key(&name) {
            let mut profiles: Vec<ResourceProfileDisplay> = Vec::new();
            let unlimited = builtin_unlimited_profile();
            profiles.push(ResourceProfileDisplay {
                name: BUILTIN_PROFILE_UNLIMITED.to_string(),
                cpu_display: profile_display_value(&unlimited.cpu),
                memory_display: profile_display_value(&unlimited.memory),
                memory_swap_display: profile_display_value(&unlimited.memory_swap),
                io_weight_display: profile_display_u32(&unlimited.io_weight, Some(100)),
                editable: false,
            });
            let mut keys: Vec<_> = cfg.resource_profiles.keys().cloned().collect();
            keys.sort();
            for k in keys {
                if let Some(p) = cfg.resource_profiles.get(&k) {
                    profiles.push(ResourceProfileDisplay {
                        name: k,
                        cpu_display: profile_display_value(&p.cpu),
                        memory_display: profile_display_value(&p.memory),
                        memory_swap_display: profile_display_value(&p.memory_swap),
                        io_weight_display: profile_display_u32(&p.io_weight, None),
                        editable: true,
                    });
                }
            }
            return HtmlTemplate(ResourceProfilesTemplate {
                profiles,
                add_form: AddResourceProfileFormValues {
                    name: form.name.clone(),
                    cpu: form.cpu.clone(),
                    memory: form.memory.clone(),
                    memory_swap: form.memory_swap.clone(),
                    io_weight: form.io_weight.clone(),
                },
                error: Some(format!("Duplicate resource profile name: {}", name)),
            })
            .into_response();
        }
        cfg.resource_profiles.insert(name.clone(), profile);
        save_config(&cfg).ok();
    }

    Redirect::to("/resource-profiles").into_response()
}

#[derive(Template)]
#[template(path = "edit_resource_profile.html")]
#[allow(dead_code)]
struct EditResourceProfileTemplate {
    name: String,
    cpu: String,
    memory: String,
    memory_swap: String,
    io_weight: String,
    error: Option<String>,
}

async fn edit_resource_profile_page(
    AxumState(state): AxumState<AppState>,
    Query(q): Query<EditResourceProfileQuery>,
) -> Response {
    let name = q.name.trim().to_string();
    if name == BUILTIN_PROFILE_UNLIMITED {
        return Redirect::to("/resource-profiles").into_response();
    }

    let cfg = state.read().unwrap();
    let Some(p) = cfg.resource_profiles.get(&name) else {
        return Redirect::to("/resource-profiles").into_response();
    };

    HtmlTemplate(EditResourceProfileTemplate {
        name,
        cpu: p.cpu.clone().unwrap_or_default(),
        memory: p.memory.clone().unwrap_or_default(),
        memory_swap: p.memory_swap.clone().unwrap_or_default(),
        io_weight: p.io_weight.map(|x| x.to_string()).unwrap_or_default(),
        error: None,
    })
    .into_response()
}

async fn update_resource_profile(
    AxumState(state): AxumState<AppState>,
    Form(form): Form<UpdateResourceProfileForm>,
) -> impl IntoResponse {
    let name = form.name.trim().to_string();
    if name == BUILTIN_PROFILE_UNLIMITED {
        return Redirect::to("/resource-profiles").into_response();
    }

    let (cpu, ram, swap, io_weight, _cpu_millis, _ram_bytes, _swap_bytes) = match validate_and_normalize_resource_profile(
        &form.cpu,
        &form.memory,
        &form.memory_swap,
        &form.io_weight,
    ) {
        Ok(v) => v,
        Err(e) => {
            return HtmlTemplate(EditResourceProfileTemplate {
                name: name.clone(),
                cpu: form.cpu.clone(),
                memory: form.memory.clone(),
                memory_swap: form.memory_swap.clone(),
                io_weight: form.io_weight.clone(),
                error: Some(e),
            })
            .into_response();
        }
    };

    let profile = ResourceProfile { cpu, memory: ram, memory_swap: swap, io_weight };

    {
        let mut cfg = state.write().unwrap();
        if !cfg.resource_profiles.contains_key(&name) {
            return Redirect::to("/resource-profiles").into_response();
        }
        cfg.resource_profiles.insert(name.clone(), profile);
        save_config(&cfg).ok();
    }

    Redirect::to("/resource-profiles").into_response()
}

async fn delete_resource_profile(
    AxumState(state): AxumState<AppState>,
    Form(form): Form<DeleteResourceProfileForm>,
) -> impl IntoResponse {
    let name = form.name.trim().to_string();
    if name == BUILTIN_PROFILE_UNLIMITED {
        return Redirect::to("/resource-profiles").into_response();
    }
    {
        let mut cfg = state.write().unwrap();
        cfg.resource_profiles.remove(&name);
        save_config(&cfg).ok();
    }
    Redirect::to("/resource-profiles").into_response()
}

async fn agents_page(AxumState(state): AxumState<AppState>) -> impl IntoResponse {
    let (buckets, retry_count, retry_delay_ms) = {
        let config = state.read().unwrap();
        (
            config
                .buckets
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect::<Vec<_>>(),
            config.s3_retry_count,
            config.s3_retry_delay_ms,
        )
    };

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    // Fetch agents from all buckets in parallel.
    let futures = buckets.into_iter().map(|(bucket_display, bucket_config)| {
        async move {
            let client = create_s3_client(&bucket_config).await;
            let prefix_base = "agents/".to_string();
            let prefix = bucket_config.full_key(&prefix_base);

            // List agent objects under agents/
            let mut keys: Vec<String> = Vec::new();
            let mut continuation: Option<String> = None;
            loop {
                let op_name = format!("list_agents: {}:{}", bucket_config.bucket_name, prefix);
                let resp = retry_s3_operation(
                    &op_name,
                    retry_count,
                    retry_delay_ms,
                    || {
                        let client = client.clone();
                        let bucket = bucket_config.bucket_name.clone();
                        let prefix = prefix.clone();
                        let token = continuation.clone();
                        async move {
                            let mut req = client.list_objects_v2().bucket(&bucket).prefix(&prefix);
                            if let Some(t) = token {
                                req = req.continuation_token(t);
                            }
                            req.send().await
                        }
                    },
                )
                .await;

                let output = match resp {
                    Ok(o) => o,
                    Err(_) => break,
                };

                if let Some(contents) = output.contents {
                    for obj in contents {
                        if let Some(k) = obj.key {
                            if k.ends_with(".json") {
                                keys.push(k);
                            }
                        }
                    }
                }

                if output.is_truncated.unwrap_or(false) {
                    continuation = output.next_continuation_token;
                } else {
                    break;
                }
            }

            // Load each agent JSON (parallel per bucket).
            let agent_futs: Vec<_> = keys.into_iter().map(|key| {
                let client = client.clone();
                let bucket_name = bucket_config.bucket_name.clone();
                let bucket_display = bucket_display.clone();
                async move {
                    let op_name = format!("get_agent: {}", key);
                    let res = retry_s3_operation(
                        &op_name,
                        retry_count,
                        retry_delay_ms,
                        || {
                            let client = client.clone();
                            let bucket = bucket_name.clone();
                            let key = key.clone();
                            async move { client.get_object().bucket(&bucket).key(&key).send().await }
                        },
                    )
                    .await;

                    let output = match res {
                        Ok(o) => o,
                        Err(_) => return None,
                    };

                    let data = output.body.collect().await.ok()?.into_bytes();
                    let content = String::from_utf8_lossy(&data).to_string();
                    let report: AgentStatusReport = serde_json::from_str(&content).ok()?;

                    let age = now.saturating_sub(report.report_timestamp);
                    let (label, badge, is_dead) = if age <= 180 {
                        ("Active".to_string(), "bg-success".to_string(), false)
                    } else if age <= 600 {
                        ("Warning".to_string(), "bg-warning text-dark".to_string(), false)
                    } else {
                        ("Dead".to_string(), "bg-danger".to_string(), true)
                    };

                    let mut report_cells = report.cells;
                    // Sort cells within an agent row by cell_id for stable UI.
                    report_cells.sort_by(|a, b| a.cell_id.cmp(&b.cell_id));
                    let cells = report_cells
                        .into_iter()
                        .map(|c| AgentDisplayCell {
                            cell_id: c.cell_id,
                            version: c.version.clone().unwrap_or_else(|| "-".to_string()),
                            version_display: c.version.unwrap_or_else(|| "-".to_string()),
                            manifest_name: if c.manifest_name.is_empty() { "unknown".to_string() } else { c.manifest_name },
                            profile: if c.profile.is_empty() { "unknown".to_string() } else { c.profile },
                            running_label: match c.running {
                                Some(true) => "Running".to_string(),
                                Some(false) => "Not running".to_string(),
                                None => "N/A".to_string(),
                            },
                            running_badge_class: match c.running {
                                Some(true) => "bg-success".to_string(),
                                Some(false) => "bg-danger".to_string(),
                                None => "bg-secondary".to_string(),
                            },
                        })
                        .collect::<Vec<_>>();

                    let cells_joined = cells
                        .iter()
                        .map(|c| c.cell_id.as_str())
                        .collect::<Vec<_>>()
                        .join(" ");

                    Some(AgentDisplay {
                        node_id: report.node_id,
                        bucket_display,
                        status_label: label,
                        status_badge_class: badge,
                        is_dead,
                        last_report_timestamp: report.report_timestamp,
                        last_report_human: format_timestamp_local(report.report_timestamp),
                        age_seconds: Some(age),
                        age_human: format_age(age),
                        cells_joined,
                        cells,
                    })
                }
            }).collect();

            let mut agents = join_all(agent_futs).await.into_iter().flatten().collect::<Vec<_>>();
            // Sort stable: status then node_id.
            agents.sort_by(|a, b| a.node_id.cmp(&b.node_id));
            Some(agents)
        }
    });

    let results = join_all(futures).await;
    let mut agents: Vec<AgentDisplay> = results.into_iter().flatten().flatten().collect();

    // Sort: Active first, then Warning, then Dead, then node_id.
    let rank = |s: &str| match s {
        "Active" => 0,
        "Warning" => 1,
        "Dead" => 2,
        _ => 9,
    };
    agents.sort_by(|a, b| {
        rank(&a.status_label)
            .cmp(&rank(&b.status_label))
            .then_with(|| a.node_id.cmp(&b.node_id))
            .then_with(|| a.bucket_display.cmp(&b.bucket_display))
    });

    HtmlTemplate(AgentsTemplate { agents })
}

async fn delete_agent(
    AxumState(state): AxumState<AppState>,
    Form(form): Form<DeleteAgentForm>,
) -> impl IntoResponse {
    let (bucket_config, retry_count, retry_delay_ms) = {
        let config = state.read().unwrap();
        match config.buckets.get(&form.bucket) {
            Some(bc) => (bc.clone(), config.s3_retry_count, config.s3_retry_delay_ms),
            None => return Redirect::to("/agents").into_response(),
        }
    };

    let client = create_s3_client(&bucket_config).await;
    let key_base = format!("agents/{}.json", form.node_id);
    let key = bucket_config.full_key(&key_base);

    let op_name = format!("delete_agent: {}", key);
    let _ = retry_s3_operation(
        &op_name,
        retry_count,
        retry_delay_ms,
        || {
            let client = client.clone();
            let bucket = bucket_config.bucket_name.clone();
            let key = key.clone();
            async move { client.delete_object().bucket(&bucket).key(&key).send().await }
        },
    )
    .await;

    Redirect::to("/agents").into_response()
}

async fn buckets_page(AxumState(state): AxumState<AppState>) -> impl IntoResponse {
    let config = state.read().unwrap();
    let buckets: Vec<_> = config.buckets.iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    
    let template = BucketsTemplate { buckets };
    HtmlTemplate(template)
}

async fn add_bucket(
    AxumState(state): AxumState<AppState>,
    Form(form): Form<BucketForm>,
) -> impl IntoResponse {
    // Validate display name format
    if let Err(e) = validate_display_name(&form.display_name) {
        eprintln!("✗ Invalid display name: {}", e);
        return Redirect::to("/buckets?error=Invalid+display+name+format").into_response();
    }
    
    // Check if display name already exists
    {
        let config = state.read().unwrap();
        if config.buckets.contains_key(&form.display_name) {
            eprintln!("✗ Display name already exists: {}", form.display_name);
            return Redirect::to("/buckets?error=Display+name+already+exists").into_response();
        }
    }
    
    // Validate prefix format
    if let Err(e) = BucketConfig::validate_prefix(&form.prefix) {
        eprintln!("✗ Invalid prefix: {}", e);
        return Redirect::to("/buckets?error=Invalid+prefix+format").into_response();
    }
    
    let bucket_config = BucketConfig {
        bucket_name: form.bucket_name.clone(),
        endpoint: form.endpoint.clone(),
        access_key: form.access_key.clone(),
        secret_key: form.secret_key.clone(),
        path_style: form.path_style,
        prefix: form.prefix,
    };
    
    // Test the bucket connection before saving
    let client = create_s3_client(&bucket_config).await;
    let test_result = client
        .list_objects_v2()
        .bucket(&form.bucket_name)
        .max_keys(1)  // Only need to check if we can list
        .send()
        .await;
    
    match test_result {
        Ok(_) => {
            // Connection successful, save the bucket
            let mut config = state.write().unwrap();
            config.buckets.insert(form.display_name.clone(), bucket_config);
            save_config(&config).ok();
            println!("✓ Bucket '{}' validated and added successfully", form.display_name);
            Redirect::to("/buckets").into_response()
        }
        Err(e) => {
            // Connection failed, show error
            eprintln!("✗ Failed to validate bucket '{}': {}", form.display_name, e);
            let error_msg = format!(
                "Failed to connect to bucket '{}'. Please check your credentials and endpoint. Error: {}",
                form.display_name, e
            );
            // Redirect back with error (for now just redirect, could enhance with flash messages)
            Html(format!(
                r#"<!DOCTYPE html>
<html>
<head>
    <meta http-equiv="refresh" content="5;url=/buckets">
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body>
    <div class="container mt-5">
        <div class="alert alert-danger">
            <h4><i class="bi bi-exclamation-triangle"></i> Bucket Validation Failed</h4>
            <p>{}</p>
            <p class="mb-0">Redirecting back to buckets page in 5 seconds... <a href="/buckets">Click here</a> if not redirected.</p>
        </div>
    </div>
</body>
</html>"#,
                error_msg
            )).into_response()
        }
    }
}

async fn delete_bucket(
    AxumState(state): AxumState<AppState>,
    Form(form): Form<DeleteForm>,
) -> impl IntoResponse {
    let mut config = state.write().unwrap();
    config.buckets.remove(&form.name);
    save_config(&config).ok();
    
    Redirect::to("/buckets")
}

async fn manifests_page(AxumState(state): AxumState<AppState>) -> impl IntoResponse {
    let config = state.read().unwrap();
    let manifests: Vec<_> = config.manifests.iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    let buckets: Vec<_> = config.buckets.iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    
    let resource_profiles = resource_profile_names(&config);
    let template = ManifestsTemplate {
        manifests,
        buckets,
        resource_profiles,
    };
    HtmlTemplate(template)
}

async fn add_manifest(
    AxumState(state): AxumState<AppState>,
    Form(form): Form<ManifestForm>,
) -> impl IntoResponse {
    let mut config = state.write().unwrap();
    
    let file_paths: Vec<String> = form.files
        .lines()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    
    let file_types: Vec<String> = form.file_types
        .lines()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    
    let service_owner = form.service_owner.trim().to_string();
    let service_group = form.service_group.trim().to_string();
    if let Err(e) = validate_service_identity(&service_owner, &service_group) {
        return Html(format!(
            r#"<!DOCTYPE html>
<html>
<head>
    <meta http-equiv="refresh" content="5;url=/manifests">
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body>
    <div class="container mt-5">
        <div class="alert alert-danger">
            <h4><i class="bi bi-exclamation-triangle"></i> Validation Failed</h4>
            <p>{}</p>
            <p class="mb-0">Redirecting back in 5 seconds... <a href="/manifests">Click here</a> if not redirected.</p>
        </div>
    </div>
</body>
</html>"#,
            e
        ))
        .into_response();
    }

    let service_folder_mode = form
        .service_folder_mode
        .as_deref()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .unwrap_or("0700")
        .to_string();
    if let Err(e) = validate_service_folder_meta(&service_folder_mode) {
        return Html(format!(
            r#"<!DOCTYPE html>
<html>
<head>
    <meta http-equiv="refresh" content="5;url=/manifests">
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body>
    <div class="container mt-5">
        <div class="alert alert-danger">
            <h4><i class="bi bi-exclamation-triangle"></i> Validation Failed</h4>
            <p>{}</p>
            <p class="mb-0">Redirecting back in 5 seconds... <a href="/manifests">Click here</a> if not redirected.</p>
        </div>
    </div>
</body>
</html>"#,
            e
        ))
        .into_response();
    }

    let service_folder_owner = form
        .service_folder_owner
        .as_deref()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string());
    let service_folder_group = form
        .service_folder_group
        .as_deref()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string());
    // Either set both, or set neither.
    if service_folder_owner.is_some() ^ service_folder_group.is_some() {
        return Html(
            r#"<!DOCTYPE html>
<html>
<head>
    <meta http-equiv="refresh" content="5;url=/manifests">
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body>
    <div class="container mt-5">
        <div class="alert alert-danger">
            <h4><i class="bi bi-exclamation-triangle"></i> Validation Failed</h4>
            <p>Service folder ownership override must include both owner and group (user:group).</p>
            <p class="mb-0">Redirecting back in 5 seconds... <a href="/manifests">Click here</a> if not redirected.</p>
        </div>
    </div>
</body>
</html>"#
            .to_string(),
        )
        .into_response();
    }
    // If override equals the default (service_owner/service_group), drop it so it doesn't "stick"
    // and block future changes to service_owner/service_group.
    let (service_folder_owner, service_folder_group) =
        match (&service_folder_owner, &service_folder_group) {
            (Some(o), Some(g)) if o == &service_owner && g == &service_group => (None, None),
            _ => (service_folder_owner, service_folder_group),
        };
    if let Err(e) = validate_optional_owner_group("service_folder_owner", &service_folder_owner) {
        return Html(format!(
            r#"<!DOCTYPE html>
<html>
<head>
    <meta http-equiv="refresh" content="5;url=/manifests">
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body>
    <div class="container mt-5">
        <div class="alert alert-danger">
            <h4><i class="bi bi-exclamation-triangle"></i> Validation Failed</h4>
            <p>{}</p>
            <p class="mb-0">Redirecting back in 5 seconds... <a href="/manifests">Click here</a> if not redirected.</p>
        </div>
    </div>
</body>
</html>"#,
            e
        ))
        .into_response();
    }
    if let Err(e) = validate_optional_owner_group("service_folder_group", &service_folder_group) {
        return Html(format!(
            r#"<!DOCTYPE html>
<html>
<head>
    <meta http-equiv="refresh" content="5;url=/manifests">
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body>
    <div class="container mt-5">
        <div class="alert alert-danger">
            <h4><i class="bi bi-exclamation-triangle"></i> Validation Failed</h4>
            <p>{}</p>
            <p class="mb-0">Redirecting back in 5 seconds... <a href="/manifests">Click here</a> if not redirected.</p>
        </div>
    </div>
</body>
</html>"#,
            e
        ))
        .into_response();
    }

    let owners: Vec<Option<String>> = form.owners
        .lines()
        .map(|s| s.trim().to_string())
        .map(|s| if s.is_empty() { None } else { Some(s) })
        .collect();
    
    let groups: Vec<Option<String>> = form.groups
        .lines()
        .map(|s| s.trim().to_string())
        .map(|s| if s.is_empty() { None } else { Some(s) })
        .collect();
    
    let modes: Vec<Option<String>> = form.modes
        .lines()
        .map(|s| s.trim().to_string())
        .map(|s| if s.is_empty() { None } else { Some(s) })
        .collect();
    
    let files: Vec<ManifestFile> = file_paths.iter()
        .zip(file_types.iter())
        .enumerate()
        .map(|(i, (path, ftype))| ManifestFile {
            path: path.clone(),
            file_type: match ftype.to_lowercase().as_str() {
                "text" => FileType::Text,
                "folder" => FileType::Folder,
                _ => FileType::Binary,
            },
            owner: owners.get(i).cloned().unwrap_or(None),
            group: groups.get(i).cloned().unwrap_or(None),
            mode: modes.get(i).cloned().unwrap_or(None),
        })
        .collect();
    
    // Validate file paths (applies to all profiles)
    for file in &files {
        let path = &file.path;
        if path.starts_with('/') || path.starts_with("../") || path.starts_with("./") || path.contains("/./") || path.contains("/../") {
            return Html(format!(
                r#"<!DOCTYPE html>
<html>
<head>
    <meta http-equiv="refresh" content="5;url=/manifests">
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body>
    <div class="container mt-5">
        <div class="alert alert-danger">
            <h4><i class="bi bi-exclamation-triangle"></i> Validation Failed</h4>
            <p><strong>Invalid file path:</strong> <code>{}</code></p>
            <p>File paths cannot:</p>
            <ul>
                <li>Start with <code>/</code> (absolute paths)</li>
                <li>Start with <code>../</code> (parent directory reference)</li>
                <li>Start with <code>./</code> (current directory reference)</li>
                <li>Contain <code>/./</code> (current directory escape)</li>
                <li>Contain <code>/../</code> (parent directory escape)</li>
            </ul>
            <p>Use simple relative paths like: <code>folder/file</code> or <code>folder/subfolder/</code></p>
            <p class="mb-0">Redirecting back in 5 seconds... <a href="/manifests">Click here</a> if not redirected.</p>
        </div>
    </div>
</body>
</html>"#,
                path
            )).into_response();
        }
    }
    
    let profile = match form.profile.to_lowercase().as_str() {
        "supervisor" => DeploymentProfile::Supervisor,
        "systemd" => DeploymentProfile::Systemd,
        "processmaster" => DeploymentProfile::ProcessMaster,
        "deploy" => DeploymentProfile::Deploy,
        _ => DeploymentProfile::Systemd,
    };

    if let Err(e) = validate_run_sh(&files, &profile) {
        return Html(format!(
            r#"<!DOCTYPE html>
<html>
<head>
    <meta http-equiv="refresh" content="5;url=/manifests">
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body>
    <div class="container mt-5">
        <div class="alert alert-danger">
            <h4><i class="bi bi-exclamation-triangle"></i> Validation Failed</h4>
            <p>{}</p>
            <p class="mb-0">Redirecting back in 5 seconds... <a href="/manifests">Click here</a> if not redirected.</p>
        </div>
    </div>
</body>
</html>"#,
            e
        ))
        .into_response();
    }
    if let Err(e) = validate_logs_folder(&files, &profile) {
        return Html(format!(
            r#"<!DOCTYPE html>
<html>
<head>
    <meta http-equiv="refresh" content="5;url=/manifests">
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body>
    <div class="container mt-5">
        <div class="alert alert-danger">
            <h4><i class="bi bi-exclamation-triangle"></i> Validation Failed</h4>
            <p>{}</p>
            <p class="mb-0">Redirecting back in 5 seconds... <a href="/manifests">Click here</a> if not redirected.</p>
        </div>
    </div>
</body>
</html>"#,
            e
        ))
        .into_response();
    }

    // Profile-specific required files.
    if let Some((_kind, required_path)) = required_service_file_and_kind(&profile) {
        let ok = files
            .iter()
            .any(|f| f.path == required_path && f.file_type == FileType::Text);
        if !ok {
            return Html(format!(
                r#"<!DOCTYPE html>
<html>
<head>
    <meta http-equiv="refresh" content="5;url=/manifests">
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body>
    <div class="container mt-5">
        <div class="alert alert-danger">
            <h4><i class="bi bi-exclamation-triangle"></i> Validation Failed</h4>
            <p><strong>Missing required file for {}</strong>: <code>{}</code> (must be a text file)</p>
            <p class="mb-0">Redirecting back in 5 seconds... <a href="/manifests">Click here</a> if not redirected.</p>
        </div>
    </div>
</body>
</html>"#,
                profile, required_path
            ))
            .into_response();
        }
    }

    let rp_in = form.resource_profile.clone().unwrap_or_default();
    let rp = rp_in.trim();
    let resource_profile = match profile {
        DeploymentProfile::Systemd | DeploymentProfile::ProcessMaster => {
            if rp.is_empty() {
                return Html(format!(
                    r#"<!DOCTYPE html>
<html>
<head>
    <meta http-equiv="refresh" content="5;url=/manifests">
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body>
    <div class="container mt-5">
        <div class="alert alert-danger">
            <h4><i class="bi bi-exclamation-triangle"></i> Validation Failed</h4>
            <p><strong>Resource profile is required</strong> for <code>{}</code> manifests.</p>
            <p class="mb-0">Redirecting back in 5 seconds... <a href="/manifests">Click here</a> if not redirected.</p>
        </div>
    </div>
</body>
</html>"#,
                    profile
                ))
                .into_response();
            }
            if !resource_profile_exists(&config, rp) {
                return Html(format!(
                    r#"<!DOCTYPE html>
<html>
<head>
    <meta http-equiv="refresh" content="5;url=/manifests">
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body>
    <div class="container mt-5">
        <div class="alert alert-danger">
            <h4><i class="bi bi-exclamation-triangle"></i> Validation Failed</h4>
            <p><strong>Unknown resource profile:</strong> <code>{}</code></p>
            <p>Please create it under <a href="/resource-profiles">Resource Profiles</a> (or select <code>unlimited</code>).</p>
            <p class="mb-0">Redirecting back in 5 seconds... <a href="/manifests">Click here</a> if not redirected.</p>
        </div>
    </div>
</body>
</html>"#,
                    rp
                ))
                .into_response();
            }
            rp.to_string()
        }
        _ => {
            // Deploy/Supervisor: optional; default to "unlimited" if not set.
            if rp.is_empty() {
                BUILTIN_PROFILE_UNLIMITED.to_string()
            } else if resource_profile_exists(&config, rp) {
                rp.to_string()
            } else {
                return Html(format!(
                    r#"<!DOCTYPE html>
<html>
<head>
    <meta http-equiv="refresh" content="5;url=/manifests">
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body>
    <div class="container mt-5">
        <div class="alert alert-danger">
            <h4><i class="bi bi-exclamation-triangle"></i> Validation Failed</h4>
            <p><strong>Unknown resource profile:</strong> <code>{}</code></p>
            <p>Please create it under <a href="/resource-profiles">Resource Profiles</a> (or select <code>unlimited</code>).</p>
            <p class="mb-0">Redirecting back in 5 seconds... <a href="/manifests">Click here</a> if not redirected.</p>
        </div>
    </div>
</body>
</html>"#,
                    rp
                ))
                .into_response();
            }
        }
    };
    
    // Parse log collection fields with new defaults
    let collect_logs = form.collect_logs.is_some();
    let log_files: Vec<String> = if let Some(log_files_str) = form.log_files {
        let parsed: Vec<String> = log_files_str
            .lines()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        if parsed.is_empty() {
            vec!["logs/stdout.log".to_string(), "logs/stderr.log".to_string()]
        } else {
            parsed
        }
    } else {
        vec!["logs/stdout.log".to_string(), "logs/stderr.log".to_string()]
    };
    let max_log_lines = form.max_log_lines
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(30);
    
    let manifest = DeploymentManifest {
        files,
        profile,
        resource_profile,
        service_owner,
        service_group,
        service_folder_mode,
        service_folder_owner,
        service_folder_group,
        bucket: form.bucket,
        collect_logs,
        log_files,
        max_log_lines,
    };
    
    config.manifests.insert(form.name, manifest);
    save_config(&config).ok();
    
    Redirect::to("/manifests").into_response()
}

async fn edit_manifest_page(
    AxumState(state): AxumState<AppState>,
    Query(query): Query<EditManifestQuery>,
) -> impl IntoResponse {
    let config = state.read().unwrap();
    
    let manifest = match config.manifests.get(&query.name) {
        Some(m) => m.clone(),
        None => return Redirect::to("/manifests").into_response(),
    };
    
    let buckets: Vec<_> = config.buckets.iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    let selected_bucket = manifest.bucket.clone();
    let resource_profiles = resource_profile_names(&config);
    
    let template = EditManifestTemplate {
        manifest_name: query.name,
        manifest,
        buckets,
        selected_bucket,
        resource_profiles,
    };
    
    HtmlTemplate(template).into_response()
}

async fn update_manifest(
    AxumState(state): AxumState<AppState>,
    Form(form): Form<UpdateManifestForm>,
) -> impl IntoResponse {
    let mut config = state.write().unwrap();
    
    let file_paths: Vec<String> = form.files
        .lines()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    
    let file_types: Vec<String> = form.file_types
        .lines()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    let service_owner = form.service_owner.trim().to_string();
    let service_group = form.service_group.trim().to_string();
    if let Err(e) = validate_service_identity(&service_owner, &service_group) {
        return Html(format!(
            r#"<!DOCTYPE html>
<html>
<head>
    <meta http-equiv="refresh" content="5;url=/manifests/edit?name={}">
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body>
    <div class="container mt-5">
        <div class="alert alert-danger">
            <h4><i class="bi bi-exclamation-triangle"></i> Validation Failed</h4>
            <p>{}</p>
            <p class="mb-0">Redirecting back in 5 seconds... <a href="/manifests/edit?name={}">Click here</a> if not redirected.</p>
        </div>
    </div>
</body>
</html>"#,
            form.original_name, e, form.original_name
        ))
        .into_response();
    }

    let service_folder_mode = form
        .service_folder_mode
        .as_deref()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .unwrap_or("0700")
        .to_string();
    if let Err(e) = validate_service_folder_meta(&service_folder_mode) {
        return Html(format!(
            r#"<!DOCTYPE html>
<html>
<head>
    <meta http-equiv="refresh" content="5;url=/manifests/edit?name={}">
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body>
    <div class="container mt-5">
        <div class="alert alert-danger">
            <h4><i class="bi bi-exclamation-triangle"></i> Validation Failed</h4>
            <p>{}</p>
            <p class="mb-0">Redirecting back in 5 seconds... <a href="/manifests/edit?name={}">Click here</a> if not redirected.</p>
        </div>
    </div>
</body>
</html>"#,
            form.original_name, e, form.original_name
        ))
        .into_response();
    }

    let service_folder_owner = form
        .service_folder_owner
        .as_deref()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string());
    let service_folder_group = form
        .service_folder_group
        .as_deref()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string());
    if service_folder_owner.is_some() ^ service_folder_group.is_some() {
        return Html(format!(
            r#"<!DOCTYPE html>
<html>
<head>
    <meta http-equiv="refresh" content="5;url=/manifests/edit?name={}">
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body>
    <div class="container mt-5">
        <div class="alert alert-danger">
            <h4><i class="bi bi-exclamation-triangle"></i> Validation Failed</h4>
            <p>Service folder ownership override must include both owner and group (user:group).</p>
            <p class="mb-0">Redirecting back in 5 seconds... <a href="/manifests/edit?name={}">Click here</a> if not redirected.</p>
        </div>
    </div>
</body>
</html>"#,
            form.original_name, form.original_name
        ))
        .into_response();
    }
    let (service_folder_owner, service_folder_group) =
        match (&service_folder_owner, &service_folder_group) {
            (Some(o), Some(g)) if o == &service_owner && g == &service_group => (None, None),
            _ => (service_folder_owner, service_folder_group),
        };
    if let Err(e) = validate_optional_owner_group("service_folder_owner", &service_folder_owner) {
        return Html(format!(
            r#"<!DOCTYPE html>
<html>
<head>
    <meta http-equiv="refresh" content="5;url=/manifests/edit?name={}">
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body>
    <div class="container mt-5">
        <div class="alert alert-danger">
            <h4><i class="bi bi-exclamation-triangle"></i> Validation Failed</h4>
            <p>{}</p>
            <p class="mb-0">Redirecting back in 5 seconds... <a href="/manifests/edit?name={}">Click here</a> if not redirected.</p>
        </div>
    </div>
</body>
</html>"#,
            form.original_name, e, form.original_name
        ))
        .into_response();
    }
    if let Err(e) = validate_optional_owner_group("service_folder_group", &service_folder_group) {
        return Html(format!(
            r#"<!DOCTYPE html>
<html>
<head>
    <meta http-equiv="refresh" content="5;url=/manifests/edit?name={}">
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body>
    <div class="container mt-5">
        <div class="alert alert-danger">
            <h4><i class="bi bi-exclamation-triangle"></i> Validation Failed</h4>
            <p>{}</p>
            <p class="mb-0">Redirecting back in 5 seconds... <a href="/manifests/edit?name={}">Click here</a> if not redirected.</p>
        </div>
    </div>
</body>
</html>"#,
            form.original_name, e, form.original_name
        ))
        .into_response();
    }
    
    let owners: Vec<Option<String>> = form.owners
        .lines()
        .map(|s| s.trim().to_string())
        .map(|s| if s.is_empty() { None } else { Some(s) })
        .collect();
    
    let groups: Vec<Option<String>> = form.groups
        .lines()
        .map(|s| s.trim().to_string())
        .map(|s| if s.is_empty() { None } else { Some(s) })
        .collect();
    
    let modes: Vec<Option<String>> = form.modes
        .lines()
        .map(|s| s.trim().to_string())
        .map(|s| if s.is_empty() { None } else { Some(s) })
        .collect();
    
    let files: Vec<ManifestFile> = file_paths.iter()
        .zip(file_types.iter())
        .enumerate()
        .map(|(i, (path, ftype))| ManifestFile {
            path: path.clone(),
            file_type: match ftype.to_lowercase().as_str() {
                "text" => FileType::Text,
                "folder" => FileType::Folder,
                _ => FileType::Binary,
            },
            owner: owners.get(i).cloned().unwrap_or(None),
            group: groups.get(i).cloned().unwrap_or(None),
            mode: modes.get(i).cloned().unwrap_or(None),
        })
        .collect();
    
    // Validate file paths (applies to all profiles)
    for file in &files {
        let path = &file.path;
        if path.starts_with('/') || path.starts_with("../") || path.starts_with("./") || path.contains("/./") || path.contains("/../") {
            return Html(format!(
                r#"<!DOCTYPE html>
<html>
<head>
    <meta http-equiv="refresh" content="5;url=/manifests/edit?name={}">
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body>
    <div class="container mt-5">
        <div class="alert alert-danger">
            <h4><i class="bi bi-exclamation-triangle"></i> Validation Failed</h4>
            <p><strong>Invalid file path:</strong> <code>{}</code></p>
            <p>File paths cannot:</p>
            <ul>
                <li>Start with <code>/</code> (absolute paths)</li>
                <li>Start with <code>../</code> (parent directory reference)</li>
                <li>Start with <code>./</code> (current directory reference)</li>
                <li>Contain <code>/./</code> (current directory escape)</li>
                <li>Contain <code>/../</code> (parent directory escape)</li>
            </ul>
            <p>Use simple relative paths like: <code>folder/file</code> or <code>folder/subfolder/</code></p>
            <p class="mb-0">Redirecting back in 5 seconds... <a href="/manifests/edit?name={}">Click here</a> if not redirected.</p>
        </div>
    </div>
</body>
</html>"#,
                form.original_name, path, form.original_name
            )).into_response();
        }
    }
    
    let profile = match form.profile.to_lowercase().as_str() {
        "supervisor" => DeploymentProfile::Supervisor,
        "systemd" => DeploymentProfile::Systemd,
        "processmaster" => DeploymentProfile::ProcessMaster,
        "deploy" => DeploymentProfile::Deploy,
        _ => DeploymentProfile::Systemd,
    };

    if let Err(e) = validate_run_sh(&files, &profile) {
        return Html(format!(
            r#"<!DOCTYPE html>
<html>
<head>
    <meta http-equiv="refresh" content="5;url=/manifests/edit?name={}">
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body>
    <div class="container mt-5">
        <div class="alert alert-danger">
            <h4><i class="bi bi-exclamation-triangle"></i> Validation Failed</h4>
            <p>{}</p>
            <p class="mb-0">Redirecting back in 5 seconds... <a href="/manifests/edit?name={}">Click here</a> if not redirected.</p>
        </div>
    </div>
</body>
</html>"#,
            form.original_name, e, form.original_name
        ))
        .into_response();
    }
    if let Err(e) = validate_logs_folder(&files, &profile) {
        return Html(format!(
            r#"<!DOCTYPE html>
<html>
<head>
    <meta http-equiv="refresh" content="5;url=/manifests/edit?name={}">
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body>
    <div class="container mt-5">
        <div class="alert alert-danger">
            <h4><i class="bi bi-exclamation-triangle"></i> Validation Failed</h4>
            <p>{}</p>
            <p class="mb-0">Redirecting back in 5 seconds... <a href="/manifests/edit?name={}">Click here</a> if not redirected.</p>
        </div>
    </div>
</body>
</html>"#,
            form.original_name, e, form.original_name
        ))
        .into_response();
    }

    // Profile-specific required files.
    if let Some((_kind, required_path)) = required_service_file_and_kind(&profile) {
        let ok = files
            .iter()
            .any(|f| f.path == required_path && f.file_type == FileType::Text);
        if !ok {
            return Html(format!(
                r#"<!DOCTYPE html>
<html>
<head>
    <meta http-equiv="refresh" content="5;url=/manifests/edit?name={}">
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body>
    <div class="container mt-5">
        <div class="alert alert-danger">
            <h4><i class="bi bi-exclamation-triangle"></i> Validation Failed</h4>
            <p><strong>Missing required file for {}</strong>: <code>{}</code> (must be a text file)</p>
            <p class="mb-0">Redirecting back in 5 seconds... <a href="/manifests/edit?name={}">Click here</a> if not redirected.</p>
        </div>
    </div>
</body>
</html>"#,
                form.original_name, profile, required_path, form.original_name
            ))
            .into_response();
        }
    }

    let rp_in = form.resource_profile.clone().unwrap_or_default();
    let rp = rp_in.trim();
    let resource_profile = match profile {
        DeploymentProfile::Systemd | DeploymentProfile::ProcessMaster => {
            if rp.is_empty() {
                return Html(format!(
                    r#"<!DOCTYPE html>
<html>
<head>
    <meta http-equiv="refresh" content="5;url=/manifests/edit?name={}">
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body>
    <div class="container mt-5">
        <div class="alert alert-danger">
            <h4><i class="bi bi-exclamation-triangle"></i> Validation Failed</h4>
            <p><strong>Resource profile is required</strong> for <code>{}</code> manifests.</p>
            <p class="mb-0">Redirecting back in 5 seconds... <a href="/manifests/edit?name={}">Click here</a> if not redirected.</p>
        </div>
    </div>
</body>
</html>"#,
                    form.original_name, profile, form.original_name
                ))
                .into_response();
            }
            if !resource_profile_exists(&config, rp) {
                return Html(format!(
                    r#"<!DOCTYPE html>
<html>
<head>
    <meta http-equiv="refresh" content="5;url=/manifests/edit?name={}">
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body>
    <div class="container mt-5">
        <div class="alert alert-danger">
            <h4><i class="bi bi-exclamation-triangle"></i> Validation Failed</h4>
            <p><strong>Unknown resource profile:</strong> <code>{}</code></p>
            <p>Please create it under <a href="/resource-profiles">Resource Profiles</a> (or select <code>unlimited</code>).</p>
            <p class="mb-0">Redirecting back in 5 seconds... <a href="/manifests/edit?name={}">Click here</a> if not redirected.</p>
        </div>
    </div>
</body>
</html>"#,
                    form.original_name, rp, form.original_name
                ))
                .into_response();
            }
            rp.to_string()
        }
        _ => {
            // Deploy/Supervisor: optional; default to "unlimited" if not set.
            if rp.is_empty() {
                BUILTIN_PROFILE_UNLIMITED.to_string()
            } else if resource_profile_exists(&config, rp) {
                rp.to_string()
            } else {
                return Html(format!(
                    r#"<!DOCTYPE html>
<html>
<head>
    <meta http-equiv="refresh" content="5;url=/manifests/edit?name={}">
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body>
    <div class="container mt-5">
        <div class="alert alert-danger">
            <h4><i class="bi bi-exclamation-triangle"></i> Validation Failed</h4>
            <p><strong>Unknown resource profile:</strong> <code>{}</code></p>
            <p>Please create it under <a href="/resource-profiles">Resource Profiles</a> (or select <code>unlimited</code>).</p>
            <p class="mb-0">Redirecting back in 5 seconds... <a href="/manifests/edit?name={}">Click here</a> if not redirected.</p>
        </div>
    </div>
</body>
</html>"#,
                    form.original_name, rp, form.original_name
                ))
                .into_response();
            }
        }
    };
    
    // Parse log collection fields with new defaults
    let collect_logs = form.collect_logs.is_some();
    let log_files: Vec<String> = if let Some(log_files_str) = form.log_files {
        let parsed: Vec<String> = log_files_str
            .lines()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        if parsed.is_empty() {
            vec!["logs/stdout.log".to_string(), "logs/stderr.log".to_string()]
        } else {
            parsed
        }
    } else {
        vec!["logs/stdout.log".to_string(), "logs/stderr.log".to_string()]
    };
    let max_log_lines = form.max_log_lines
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(30);
    
    let manifest = DeploymentManifest {
        files,
        profile,
        resource_profile,
        service_owner,
        service_group,
        service_folder_mode,
        service_folder_owner,
        service_folder_group,
        bucket: form.bucket,
        collect_logs,
        log_files,
        max_log_lines,
    };
    
    // Remove old manifest if name changed
    if form.original_name != form.name {
        config.manifests.remove(&form.original_name);
    }
    
    config.manifests.insert(form.name, manifest);
    save_config(&config).ok();
    
    Redirect::to("/manifests").into_response()
}

async fn delete_manifest(
    AxumState(state): AxumState<AppState>,
    Form(form): Form<DeleteForm>,
) -> impl IntoResponse {
    let mut config = state.write().unwrap();
    config.manifests.remove(&form.name);
    save_config(&config).ok();
    
    Redirect::to("/manifests")
}

async fn deployments_page(AxumState(state): AxumState<AppState>) -> impl IntoResponse {
    let manifest_list: Vec<(String, DeploymentManifest)> = {
        let config = state.read().unwrap();
        config.manifests.iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    };
    
    // Fetch existing deployments for all manifests
    let existing_deployments = fetch_all_deployments(&state, &manifest_list).await;
    
    let template = DeploymentsTemplate {
        existing_deployments,
    };
    HtmlTemplate(template)
}

async fn scan_orphan_deployments(
    state: &AppState,
) -> (Vec<(String, BucketConfig)>, std::collections::HashSet<String>, u32, u64) {
    let cfg = state.read().unwrap();
    let buckets = cfg
        .buckets
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect::<Vec<_>>();
    let active = cfg.manifests.keys().cloned().collect::<std::collections::HashSet<_>>();
    (buckets, active, cfg.s3_retry_count, cfg.s3_retry_delay_ms)
}

async fn deployments_cleanup_preview(AxumState(state): AxumState<AppState>) -> impl IntoResponse {
    // Snapshot config and then scan all buckets for orphan deployments, but DO NOT delete.
    let (buckets, active_manifests, retry_count, retry_delay_ms) = scan_orphan_deployments(&state).await;

    let mut rows: Vec<CleanupOrphanVersionRow> = Vec::new();
    let mut orphan_manifests_set: std::collections::HashSet<(String, String)> = std::collections::HashSet::new(); // (bucket_display, manifest)
    let mut total_objects: usize = 0;

    for (bucket_display, bucket_cfg) in buckets {
        let client = create_s3_client(&bucket_cfg).await;
        let bucket_name = bucket_cfg.bucket_name.clone();

        // List top-level manifest folders under deployments/
        let base_prefix = "deployments/".to_string();
        let prefix = bucket_cfg.full_key(&base_prefix);
        let mut continuation: Option<String> = None;
        let mut orphan_manifests: Vec<String> = Vec::new();

        loop {
            let op_name = format!("deployments_cleanup_preview:list_manifests:{}:{}", bucket_name, prefix);
            let resp = retry_s3_operation(
                &op_name,
                retry_count,
                retry_delay_ms,
                || {
                    let client = client.clone();
                    let bucket = bucket_name.clone();
                    let prefix = prefix.clone();
                    let token = continuation.clone();
                    async move {
                        let mut req = client
                            .list_objects_v2()
                            .bucket(&bucket)
                            .prefix(&prefix)
                            .delimiter("/");
                        if let Some(t) = token {
                            req = req.continuation_token(t);
                        }
                        req.send().await
                    }
                },
            )
            .await;

            let output = match resp {
                Ok(o) => o,
                Err(_) => break,
            };

            if let Some(common_prefixes) = output.common_prefixes {
                for p in common_prefixes {
                    if let Some(full_p) = p.prefix {
                        let rel = if let Some(stripped) = bucket_cfg.strip_prefix(&full_p) {
                            stripped.strip_prefix(&base_prefix).unwrap_or(stripped)
                        } else {
                            full_p.strip_prefix(&prefix).unwrap_or(&full_p)
                        };
                        let manifest_name = rel.trim_end_matches('/').split('/').next().unwrap_or("").to_string();
                        if manifest_name.is_empty() {
                            continue;
                        }
                        if !active_manifests.contains(&manifest_name) {
                            orphan_manifests.push(manifest_name);
                        }
                    }
                }
            }

            if output.is_truncated.unwrap_or(false) {
                continuation = output.next_continuation_token;
            } else {
                break;
            }
        }

        orphan_manifests.sort();
        orphan_manifests.dedup();

        // For each orphan manifest, list versions and count objects per version.
        for manifest_name in orphan_manifests {
            let versions_prefix_base = format!("deployments/{}/", manifest_name);
            let versions_prefix = bucket_cfg.full_key(&versions_prefix_base);

            let op_name = format!("deployments_cleanup_preview:list_versions:{}:{}", bucket_name, versions_prefix);
            let result = retry_s3_operation(
                &op_name,
                retry_count,
                retry_delay_ms,
                || {
                    let client = client.clone();
                    let bucket = bucket_name.clone();
                    let prefix = versions_prefix.clone();
                    async move {
                        client
                            .list_objects_v2()
                            .bucket(&bucket)
                            .prefix(&prefix)
                            .delimiter("/")
                            .send()
                            .await
                    }
                },
            )
            .await;

            let output = match result {
                Ok(o) => o,
                Err(_) => continue,
            };

            let mut versions: Vec<String> = Vec::new();
            if let Some(common_prefixes) = output.common_prefixes {
                for cp in common_prefixes {
                    if let Some(p) = cp.prefix {
                        let rel = p.strip_prefix(&versions_prefix).unwrap_or(&p);
                        let v = rel.trim_end_matches('/').split('/').next().unwrap_or("").to_string();
                        if !v.is_empty() {
                            versions.push(v);
                        }
                    }
                }
            }
            versions.sort();
            versions.dedup();

            for version in versions {
                let delete_prefix_base = format!("deployments/{}/{}/", manifest_name, version);
                let delete_prefix = bucket_cfg.full_key(&delete_prefix_base);

                let mut token: Option<String> = None;
                let mut count: usize = 0;
                loop {
                    let op_name = format!("deployments_cleanup_preview:count:{}:{}", bucket_name, delete_prefix);
                    let resp = retry_s3_operation(
                        &op_name,
                        retry_count,
                        retry_delay_ms,
                        || {
                            let client = client.clone();
                            let bucket = bucket_name.clone();
                            let prefix = delete_prefix.clone();
                            let token = token.clone();
                            async move {
                                let mut req = client.list_objects_v2().bucket(&bucket).prefix(&prefix);
                                if let Some(t) = token {
                                    req = req.continuation_token(t);
                                }
                                req.send().await
                            }
                        },
                    )
                    .await;

                    let out = match resp {
                        Ok(o) => o,
                        Err(_) => break,
                    };

                    count += out.contents.unwrap_or_default().len();
                    if out.is_truncated.unwrap_or(false) {
                        token = out.next_continuation_token;
                    } else {
                        break;
                    }
                }

                if count > 0 {
                    orphan_manifests_set.insert((bucket_display.clone(), manifest_name.clone()));
                    total_objects += count;
                    rows.push(CleanupOrphanVersionRow {
                        bucket_display: bucket_display.clone(),
                        manifest_name: manifest_name.clone(),
                        version,
                        object_count: count,
                    });
                }
            }
        }
    }

    rows.sort_by(|a, b| {
        a.bucket_display
            .cmp(&b.bucket_display)
            .then_with(|| a.manifest_name.cmp(&b.manifest_name))
            .then_with(|| a.version.cmp(&b.version))
    });

    let template = CleanupDeploymentTemplate {
        orphan_manifests_count: orphan_manifests_set.len(),
        orphan_versions_count: rows.len(),
        orphan_objects_total: total_objects,
        orphan_versions: rows,
    };
    HtmlTemplate(template).into_response()
}

async fn deployments_cleanup_execute(AxumState(state): AxumState<AppState>) -> impl IntoResponse {
    use aws_sdk_s3::types::{Delete, ObjectIdentifier};
    fn escape_html(s: &str) -> String {
        s.replace('&', "&amp;")
            .replace('<', "&lt;")
            .replace('>', "&gt;")
            .replace('"', "&quot;")
            .replace('\'', "&#39;")
    }

    // Snapshot config while holding the lock (then drop before awaiting).
    let (buckets, active_manifests, retry_count, retry_delay_ms) = scan_orphan_deployments(&state).await;

    // Scan all buckets for deployments/<manifest>/ prefixes, delete ones not in config.manifests.
    let mut deleted_total: usize = 0;
    let mut deleted_manifests: Vec<(String, String, usize, bool)> = Vec::new(); // (bucket_display, manifest, objects_deleted, empty_after)
    let mut issues: Vec<(String, String, String)> = Vec::new(); // (bucket_display, manifest, message)

    for (bucket_display, bucket_cfg) in buckets {
        let client = create_s3_client(&bucket_cfg).await;
        let bucket_name = bucket_cfg.bucket_name.clone();

        // List top-level manifest folders under deployments/
        let base_prefix = "deployments/".to_string();
        let prefix = bucket_cfg.full_key(&base_prefix);

        let mut continuation: Option<String> = None;
        let mut orphan_manifests: Vec<String> = Vec::new();

        loop {
            let op_name = format!("deployments_cleanup:list_manifests:{}:{}", bucket_name, prefix);
            let resp = retry_s3_operation(
                &op_name,
                retry_count,
                retry_delay_ms,
                || {
                    let client = client.clone();
                    let bucket = bucket_name.clone();
                    let prefix = prefix.clone();
                    let token = continuation.clone();
                    async move {
                        let mut req = client
                            .list_objects_v2()
                            .bucket(&bucket)
                            .prefix(&prefix)
                            .delimiter("/");
                        if let Some(t) = token {
                            req = req.continuation_token(t);
                        }
                        req.send().await
                    }
                },
            )
            .await;

            let output = match resp {
                Ok(o) => o,
                Err(_) => break,
            };

            if let Some(common_prefixes) = output.common_prefixes {
                for p in common_prefixes {
                    if let Some(full_p) = p.prefix {
                        // Strip bucket prefix and then "deployments/" to get manifest name.
                        let rel = if let Some(stripped) = bucket_cfg.strip_prefix(&full_p) {
                            stripped.strip_prefix(&base_prefix).unwrap_or(stripped)
                        } else {
                            full_p.strip_prefix(&prefix).unwrap_or(&full_p)
                        };
                        let manifest_name = rel.trim_end_matches('/').split('/').next().unwrap_or("").to_string();
                        if manifest_name.is_empty() {
                            continue;
                        }
                        if !active_manifests.contains(&manifest_name) {
                            orphan_manifests.push(manifest_name);
                        }
                    }
                }
            }

            if output.is_truncated.unwrap_or(false) {
                continuation = output.next_continuation_token;
            } else {
                break;
            }
        }

        orphan_manifests.sort();
        orphan_manifests.dedup();

        for manifest_name in orphan_manifests {
            let delete_prefix_base = format!("deployments/{}/", manifest_name);
            let delete_prefix = bucket_cfg.full_key(&delete_prefix_base);
            let mut token: Option<String> = None;
            let mut objects_deleted: usize = 0;
            let mut saw_any_delete_error = false;

            loop {
                let op_name = format!("deployments_cleanup:list_objects:{}:{}", bucket_name, delete_prefix);
                let resp = retry_s3_operation(
                    &op_name,
                    retry_count,
                    retry_delay_ms,
                    || {
                        let client = client.clone();
                        let bucket = bucket_name.clone();
                        let prefix = delete_prefix.clone();
                        let token = token.clone();
                        async move {
                            let mut req = client.list_objects_v2().bucket(&bucket).prefix(&prefix);
                            if let Some(t) = token {
                                req = req.continuation_token(t);
                            }
                            req.send().await
                        }
                    },
                )
                .await;

                let output = match resp {
                    Ok(o) => o,
                    Err(_) => break,
                };

                let keys: Vec<String> = output
                    .contents
                    .unwrap_or_default()
                    .into_iter()
                    .filter_map(|obj| obj.key)
                    .collect();

                if !keys.is_empty() {
                    // Delete in batches up to 1000.
                    for chunk in keys.chunks(1000) {
                        let objs = chunk
                            .iter()
                            .map(|k| {
                                ObjectIdentifier::builder()
                                    .key(k)
                                    .build()
                                    .expect("ObjectIdentifier build")
                            })
                            .collect::<Vec<_>>();
                        let del = Delete::builder()
                            .set_objects(Some(objs))
                            // We want a non-empty `deleted()` list for accurate counting.
                            .quiet(false)
                            .build()
                            .expect("Delete build");

                        let op_name = format!("deployments_cleanup:delete_objects:{}:{}", bucket_name, delete_prefix);
                        let resp = retry_s3_operation(
                            &op_name,
                            retry_count,
                            retry_delay_ms,
                            || {
                                let client = client.clone();
                                let bucket = bucket_name.clone();
                                let del = del.clone();
                                async move {
                                    client
                                        .delete_objects()
                                        .bucket(&bucket)
                                        .delete(del)
                                        .send()
                                        .await
                                }
                            },
                        )
                        .await;

                        match resp {
                            Ok(out) => {
                                // Count confirmed deletes; surface per-object failures.
                                objects_deleted += out.deleted().len();
                                for err in out.errors() {
                                    saw_any_delete_error = true;
                                    let key = err.key().unwrap_or("-");
                                    let code = err.code().unwrap_or("-");
                                    let msg = err.message().unwrap_or("-");
                                    issues.push((
                                        bucket_display.clone(),
                                        manifest_name.clone(),
                                        format!("delete failed: key={} code={} message={}", key, code, msg),
                                    ));
                                }
                            }
                            Err(e) => {
                                let err_dbg = format!("{:?}", e);
                                // Aliyun OSS (S3-compatible) can require Content-MD5 for multi-delete requests.
                                // If we hit that, fall back to per-object delete_object calls.
                                if err_dbg.contains("MissingArgument") && err_dbg.contains("Content-MD5") {
                                    for k in chunk {
                                        let op_name = format!("deployments_cleanup:delete_object:{}:{}", bucket_name, k);
                                        let one = retry_s3_operation(
                                            &op_name,
                                            retry_count,
                                            retry_delay_ms,
                                            || {
                                                let client = client.clone();
                                                let bucket = bucket_name.clone();
                                                let key = k.clone();
                                                async move {
                                                    client
                                                        .delete_object()
                                                        .bucket(&bucket)
                                                        .key(&key)
                                                        .send()
                                                        .await
                                                }
                                            },
                                        )
                                        .await;
                                        match one {
                                            Ok(_) => objects_deleted += 1,
                                            Err(e) => {
                                                saw_any_delete_error = true;
                                                issues.push((
                                                    bucket_display.clone(),
                                                    manifest_name.clone(),
                                                    format!("delete failed: key={} err={:?}", k, e),
                                                ));
                                            }
                                        }
                                    }
                                } else {
                                    saw_any_delete_error = true;
                                    issues.push((
                                        bucket_display.clone(),
                                        manifest_name.clone(),
                                        format!("delete request failed: {:?}", e),
                                    ));
                                }
                            }
                        }
                    }
                }

                if output.is_truncated.unwrap_or(false) {
                    token = output.next_continuation_token;
                } else {
                    break;
                }
            }

            // Verify prefix is empty after delete (best-effort).
            let empty_after = if saw_any_delete_error {
                false
            } else {
                let op_name = format!("deployments_cleanup:verify_empty:{}:{}", bucket_name, delete_prefix);
                match retry_s3_operation(
                    &op_name,
                    retry_count,
                    retry_delay_ms,
                    || {
                        let client = client.clone();
                        let bucket = bucket_name.clone();
                        let prefix = delete_prefix.clone();
                        async move {
                            client
                                .list_objects_v2()
                                .bucket(&bucket)
                                .prefix(&prefix)
                                .max_keys(1)
                                .send()
                                .await
                        }
                    },
                )
                .await
                {
                    Ok(o) => {
                        let first_key = o
                            .contents
                            .as_ref()
                            .and_then(|v| v.first())
                            .and_then(|x| x.key.as_deref());
                        if let Some(k) = first_key {
                            issues.push((
                                bucket_display.clone(),
                                manifest_name.clone(),
                                format!("prefix still has objects after delete (example key: {})", k),
                            ));
                            false
                        } else {
                            true
                        }
                    }
                    Err(e) => {
                        issues.push((
                            bucket_display.clone(),
                            manifest_name.clone(),
                            format!("failed to verify prefix empty: {:?}", e),
                        ));
                        false
                    }
                }
            };

            deleted_total += objects_deleted;
            deleted_manifests.push((bucket_display.clone(), manifest_name, objects_deleted, empty_after));
        }
    }

    deleted_manifests.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    issues.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));

    let mut rows = String::new();
    for (bucket_display, manifest, count, empty_after) in &deleted_manifests {
        rows.push_str(&format!(
            "<tr><td><code>{}</code></td><td><code>{}</code></td><td><code>{}</code></td><td>{}</td></tr>",
            escape_html(bucket_display),
            escape_html(manifest),
            count,
            if *empty_after { "<span class=\"badge bg-success\">Empty</span>" } else { "<span class=\"badge bg-warning text-dark\">Not empty</span>" }
        ));
    }

    let mut issue_rows = String::new();
    for (bucket_display, manifest, msg) in &issues {
        issue_rows.push_str(&format!(
            "<tr><td><code>{}</code></td><td><code>{}</code></td><td><code style=\"white-space: pre-wrap;\">{}</code></td></tr>",
            escape_html(bucket_display),
            escape_html(manifest),
            escape_html(msg),
        ));
    }

    let body = format!(
        r#"<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.10.5/font/bootstrap-icons.css">
  <title>Cleanup Deployment - Fleetman</title>
</head>
<body>
  <div class="container mt-4">
    <div class="d-flex justify-content-between align-items-center mb-3">
      <h2 class="mb-0">Cleanup Deployment</h2>
      <a class="btn btn-outline-secondary" href="/deployments"><i class="bi bi-arrow-left"></i> Back</a>
    </div>
    <div class="alert alert-info">
      Deleted <strong>{}</strong> object(s) across <strong>{}</strong> orphan manifest(s).
    </div>
    {}
    {}
  </div>
</body>
</html>"#,
        deleted_total,
        deleted_manifests.len(),
        if issues.is_empty() {
            "".to_string()
        } else {
            format!(
                "<div class=\"alert alert-warning\"><strong>Some objects were not deleted.</strong> See details below.</div>\
                 <div class=\"table-responsive mb-4\"><table class=\"table table-sm table-striped\"><thead><tr><th>Bucket</th><th>Manifest</th><th>Issue</th></tr></thead><tbody>{}</tbody></table></div>",
                issue_rows
            )
        },
        if deleted_manifests.is_empty() {
            "<p class=\"text-muted\">No orphan deployments found.</p>".to_string()
        } else {
            format!(
                "<div class=\"table-responsive\"><table class=\"table table-sm table-striped\"><thead><tr><th>Bucket</th><th>Manifest</th><th>Objects deleted</th><th>Post-check</th></tr></thead><tbody>{}</tbody></table></div>",
                rows
            )
        }
    );

    Html(body).into_response()
}

#[derive(Deserialize)]
struct RecycleActionForm {
    bucket: String,          // bucket display name (key in config.buckets)
    recycle_prefix: String,  // base prefix like "recycle/<...>/" (without bucket prefix)
}

fn parse_recycle_prefix(recycle_prefix_base: &str) -> (String, String, String) {
    // Expected: recycle/<manifest>-<version>-<timestamp>/
    // manifest may contain '-' so use regex that finds a version token.
    // Timestamp must be exactly: yyyyMMdd-HHmmss-SSS (time of deletion)
    let re = Regex::new(r"^recycle/(.+?)-(v[0-9]+\.[0-9]+\.[0-9]+)-([0-9]{8}-[0-9]{6}-[0-9]{3})/$").unwrap();
    if let Some(c) = re.captures(recycle_prefix_base) {
        let manifest = c.get(1).map(|m| m.as_str()).unwrap_or("unknown").to_string();
        let version = c.get(2).map(|m| m.as_str()).unwrap_or("-").to_string();
        let deleted_at = c
            .get(3)
            .map(|m| m.as_str())
            .unwrap_or("-")
            .to_string();
        (manifest, version, deleted_at)
    } else {
        ("unknown".to_string(), "-".to_string(), "-".to_string())
    }
}

fn format_recycle_deleted_at_human(raw: &str) -> String {
    // raw: yyyyMMdd-HHmmss-SSS
    let Ok(dt) = chrono::NaiveDateTime::parse_from_str(raw, "%Y%m%d-%H%M%S-%3f") else {
        return raw.to_string();
    };
    dt.format("%d %b %Y %H:%M:%S").to_string()
}

async fn list_recycle_entries(
    state: &AppState,
) -> Vec<RecycleEntryDisplay> {
    let (buckets, retry_count, retry_delay_ms) = {
        let cfg = state.read().unwrap();
        let buckets = cfg
            .buckets
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect::<Vec<_>>();
        (buckets, cfg.s3_retry_count, cfg.s3_retry_delay_ms)
    };

    let mut entries: Vec<RecycleEntryDisplay> = Vec::new();

    for (bucket_display, bucket_cfg) in buckets {
        let client = create_s3_client(&bucket_cfg).await;
        let bucket_name = bucket_cfg.bucket_name.clone();

        let base_prefix = "recycle/".to_string();
        let prefix = bucket_cfg.full_key(&base_prefix);

        let mut continuation: Option<String> = None;
        loop {
            let op_name = format!("recycle:list:{}:{}", bucket_name, prefix);
            let resp = retry_s3_operation(
                &op_name,
                retry_count,
                retry_delay_ms,
                || {
                    let client = client.clone();
                    let bucket = bucket_name.clone();
                    let prefix = prefix.clone();
                    let token = continuation.clone();
                    async move {
                        let mut req = client
                            .list_objects_v2()
                            .bucket(&bucket)
                            .prefix(&prefix)
                            .delimiter("/");
                        if let Some(t) = token {
                            req = req.continuation_token(t);
                        }
                        req.send().await
                    }
                },
            )
            .await;

            let output = match resp {
                Ok(o) => o,
                Err(_) => break,
            };

            if let Some(common_prefixes) = output.common_prefixes {
                for cp in common_prefixes {
                    let Some(full_p) = cp.prefix else { continue };
                    // Convert to base prefix without bucket prefix.
                    let rel = if let Some(stripped) = bucket_cfg.strip_prefix(&full_p) {
                        stripped.to_string()
                    } else {
                        full_p.strip_prefix(&prefix).unwrap_or(&full_p).to_string()
                    };
                    if !rel.starts_with("recycle/") {
                        continue;
                    }
                    let recycle_prefix_base = rel;

                    // Count objects under this recycle prefix.
                    let full_recycle_prefix = bucket_cfg.full_key(&recycle_prefix_base);
                    let mut token2: Option<String> = None;
                    let mut count: usize = 0;
                    loop {
                        let op_name = format!("recycle:count:{}:{}", bucket_name, full_recycle_prefix);
                        let resp = retry_s3_operation(
                            &op_name,
                            retry_count,
                            retry_delay_ms,
                            || {
                                let client = client.clone();
                                let bucket = bucket_name.clone();
                                let prefix = full_recycle_prefix.clone();
                                let token = token2.clone();
                                async move {
                                    let mut req = client.list_objects_v2().bucket(&bucket).prefix(&prefix);
                                    if let Some(t) = token {
                                        req = req.continuation_token(t);
                                    }
                                    req.send().await
                                }
                            },
                        )
                        .await;
                        let out = match resp {
                            Ok(o) => o,
                            Err(_) => break,
                        };
                        count += out.contents.unwrap_or_default().len();
                        if out.is_truncated.unwrap_or(false) {
                            token2 = out.next_continuation_token;
                        } else {
                            break;
                        }
                    }

                    let (manifest_name, version, deleted_at_raw) = parse_recycle_prefix(&recycle_prefix_base);
                    let deleted_at_display = if deleted_at_raw == "-" {
                        "-".to_string()
                    } else {
                        format_recycle_deleted_at_human(&deleted_at_raw)
                    };
                    entries.push(RecycleEntryDisplay {
                        bucket_display: bucket_display.clone(),
                        recycle_prefix_base,
                        manifest_name,
                        version,
                        deleted_at_raw,
                        deleted_at_display,
                        object_count: count,
                    });
                }
            }

            if output.is_truncated.unwrap_or(false) {
                continuation = output.next_continuation_token;
            } else {
                break;
            }
        }
    }

    entries.sort_by(|a, b| {
        a.bucket_display
            .cmp(&b.bucket_display)
            .then_with(|| a.manifest_name.cmp(&b.manifest_name))
            .then_with(|| a.version.cmp(&b.version))
            .then_with(|| b.deleted_at_raw.cmp(&a.deleted_at_raw))
    });
    entries
}

async fn recycle_bin_page(AxumState(state): AxumState<AppState>) -> impl IntoResponse {
    let entries = list_recycle_entries(&state).await;
    let total_objects = entries.iter().map(|e| e.object_count).sum();
    HtmlTemplate(RecycleBinTemplate { entries, total_objects }).into_response()
}

async fn recycle_clear_preview(AxumState(state): AxumState<AppState>) -> impl IntoResponse {
    let entries = list_recycle_entries(&state).await;
    let total_objects = entries.iter().map(|e| e.object_count).sum();
    HtmlTemplate(RecycleClearTemplate { entries, total_objects }).into_response()
}

async fn recycle_restore(
    AxumState(state): AxumState<AppState>,
    Form(form): Form<RecycleActionForm>,
) -> impl IntoResponse {
    let (bucket_cfg, retry_count, retry_delay_ms) = {
        let cfg = state.read().unwrap();
        let bc = match cfg.buckets.get(&form.bucket) {
            Some(bc) => bc.clone(),
            None => return Redirect::to("/deployments/recycle").into_response(),
        };
        (bc, cfg.s3_retry_count, cfg.s3_retry_delay_ms)
    };

    let client = create_s3_client(&bucket_cfg).await;
    let bucket_name = bucket_cfg.bucket_name.clone();

    let recycle_prefix_base = form.recycle_prefix;
    let (manifest_name, version, _deleted_at) = parse_recycle_prefix(&recycle_prefix_base);
    if manifest_name == "unknown" || version == "-" {
        return Html("<div class=\"container mt-4\"><div class=\"alert alert-danger\">Cannot restore: unrecognized recycle folder format.</div><a href=\"/deployments/recycle\">Back</a></div>".to_string()).into_response();
    }

    let dest_prefix_base = format!("deployments/{}/{}/", manifest_name, version);
    let dest_prefix = bucket_cfg.full_key(&dest_prefix_base);
    // Safety: refuse to restore if destination already has any objects.
    let op_name = format!("recycle_restore:dest_exists:{}:{}", bucket_name, dest_prefix);
    let exists = retry_s3_operation(
        &op_name,
        retry_count,
        retry_delay_ms,
        || {
            let client = client.clone();
            let bucket = bucket_name.clone();
            let prefix = dest_prefix.clone();
            async move { client.list_objects_v2().bucket(&bucket).prefix(&prefix).max_keys(1).send().await }
        },
    )
    .await
    .ok()
    .and_then(|o| o.contents)
    .and_then(|v| v.first().and_then(|x| x.key.clone()))
    .is_some();

    if exists {
        return Html(format!(
            r#"<div class="container mt-4"><div class="alert alert-danger">Cannot restore: deployment <code>{}-{}<\/code> already exists.</div><a href="/deployments/recycle">Back</a></div>"#,
            manifest_name, version
        ))
        .into_response();
    }

    let recycle_prefix = bucket_cfg.full_key(&recycle_prefix_base);
    // List keys under recycle prefix and copy back, then delete originals.
    let mut token: Option<String> = None;
    loop {
        let op_name = format!("recycle_restore:list:{}:{}", bucket_name, recycle_prefix);
        let resp = retry_s3_operation(
            &op_name,
            retry_count,
            retry_delay_ms,
            || {
                let client = client.clone();
                let bucket = bucket_name.clone();
                let prefix = recycle_prefix.clone();
                let token = token.clone();
                async move {
                    let mut req = client.list_objects_v2().bucket(&bucket).prefix(&prefix);
                    if let Some(t) = token {
                        req = req.continuation_token(t);
                    }
                    req.send().await
                }
            },
        )
        .await;

        let out = match resp {
            Ok(o) => o,
            Err(_) => break,
        };
        let keys: Vec<String> = out
            .contents
            .unwrap_or_default()
            .into_iter()
            .filter_map(|o| o.key)
            .collect();

        for key in keys {
            let rel = key.strip_prefix(&recycle_prefix).unwrap_or(&key);
            let new_key = format!("{}{}", dest_prefix, rel);
            let copy_source = format!("{}/{}", bucket_name, key);

            let op = format!("recycle_restore:copy:{}->{}", key, new_key);
            let _ = retry_s3_operation(
                &op,
                retry_count,
                retry_delay_ms,
                || {
                    let client = client.clone();
                    let bucket = bucket_name.clone();
                    let copy_source = copy_source.clone();
                    let new_key = new_key.clone();
                    async move {
                        client.copy_object().bucket(&bucket).copy_source(copy_source).key(new_key).send().await
                    }
                },
            )
            .await;

            let op = format!("recycle_restore:delete:{}", key);
            let _ = retry_s3_operation(
                &op,
                retry_count,
                retry_delay_ms,
                || {
                    let client = client.clone();
                    let bucket = bucket_name.clone();
                    let key = key.clone();
                    async move { client.delete_object().bucket(&bucket).key(key).send().await }
                },
            )
            .await;
        }

        if out.is_truncated.unwrap_or(false) {
            token = out.next_continuation_token;
        } else {
            break;
        }
    }

    Redirect::to("/deployments/recycle").into_response()
}

async fn recycle_delete_permanently(
    AxumState(state): AxumState<AppState>,
    Form(form): Form<RecycleActionForm>,
) -> impl IntoResponse {
    let (bucket_cfg, retry_count, retry_delay_ms) = {
        let cfg = state.read().unwrap();
        let bc = match cfg.buckets.get(&form.bucket) {
            Some(bc) => bc.clone(),
            None => return Redirect::to("/deployments/recycle").into_response(),
        };
        (bc, cfg.s3_retry_count, cfg.s3_retry_delay_ms)
    };
    let client = create_s3_client(&bucket_cfg).await;
    let bucket_name = bucket_cfg.bucket_name.clone();
    let recycle_prefix = bucket_cfg.full_key(&form.recycle_prefix);

    // List keys and delete (Aliyun-safe: fall back to per-object delete if multi-delete requires Content-MD5).
    let mut token: Option<String> = None;
    loop {
        let op_name = format!("recycle_delete:list:{}:{}", bucket_name, recycle_prefix);
        let resp = retry_s3_operation(
            &op_name,
            retry_count,
            retry_delay_ms,
            || {
                let client = client.clone();
                let bucket = bucket_name.clone();
                let prefix = recycle_prefix.clone();
                let token = token.clone();
                async move {
                    let mut req = client.list_objects_v2().bucket(&bucket).prefix(&prefix);
                    if let Some(t) = token {
                        req = req.continuation_token(t);
                    }
                    req.send().await
                }
            },
        )
        .await;
        let out = match resp {
            Ok(o) => o,
            Err(_) => break,
        };
        let keys: Vec<String> = out
            .contents
            .unwrap_or_default()
            .into_iter()
            .filter_map(|o| o.key)
            .collect();

        if !keys.is_empty() {
            // Try delete_objects, fallback to delete_object if OSS requires Content-MD5.
            let objs = keys
                .iter()
                .map(|k| aws_sdk_s3::types::ObjectIdentifier::builder().key(k).build().expect("objid"))
                .collect::<Vec<_>>();
            let del = aws_sdk_s3::types::Delete::builder()
                .set_objects(Some(objs))
                .quiet(false)
                .build()
                .expect("delete");
            let op = format!("recycle_delete:delete_objects:{}:{}", bucket_name, recycle_prefix);
            let resp = retry_s3_operation(
                &op,
                retry_count,
                retry_delay_ms,
                || {
                    let client = client.clone();
                    let bucket = bucket_name.clone();
                    let del = del.clone();
                    async move { client.delete_objects().bucket(&bucket).delete(del).send().await }
                },
            )
            .await;
            if let Err(e) = resp {
                let s = format!("{:?}", e);
                if s.contains("MissingArgument") && s.contains("Content-MD5") {
                    for k in keys {
                        let op = format!("recycle_delete:delete_object:{}:{}", bucket_name, k);
                        let _ = retry_s3_operation(
                            &op,
                            retry_count,
                            retry_delay_ms,
                            || {
                                let client = client.clone();
                                let bucket = bucket_name.clone();
                                let key = k.clone();
                                async move { client.delete_object().bucket(&bucket).key(key).send().await }
                            },
                        )
                        .await;
                    }
                }
            }
        }

        if out.is_truncated.unwrap_or(false) {
            token = out.next_continuation_token;
        } else {
            break;
        }
    }

    Redirect::to("/deployments/recycle").into_response()
}

async fn recycle_clear_execute(AxumState(state): AxumState<AppState>) -> impl IntoResponse {
    // Execute by reusing list + per-entry delete.
    let entries = list_recycle_entries(&state).await;
    for e in entries {
        let _ = recycle_delete_permanently(
            AxumState(state.clone()),
            Form(RecycleActionForm {
                bucket: e.bucket_display,
                recycle_prefix: e.recycle_prefix_base,
            }),
        )
        .await;
    }
    Redirect::to("/deployments/recycle").into_response()
}

async fn deployments_create(
    AxumState(state): AxumState<AppState>,
) -> impl IntoResponse {
    let manifests = {
        let config = state.read().unwrap();
        config.manifests.keys().cloned().collect()
    };
    
    let template = CreateDeploymentTemplate {
        manifests,
    };
    HtmlTemplate(template)
}

fn validate_version(version: &str) -> Result<(), String> {
    // Version must be in format: v<major>.<minor>.<patch>
    // Each number can be multiple digits (e.g., 11, 22)
    // Numbers cannot have leading zeros unless the number is "0" itself
    
    let re = Regex::new(r"^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$").unwrap();
    
    if !re.is_match(version) {
        return Err(format!(
            "Invalid version format: '{}'. Must be v<major>.<minor>.<patch> (e.g., v1.0.0, v2.10.3). \
             Numbers cannot have leading zeros (e.g., v1.02.0 is invalid).",
            version
        ));
    }
    
    Ok(())
}


async fn view_deployment(
    AxumState(state): AxumState<AppState>,
    Query(query): Query<DeploymentQuery>,
) -> impl IntoResponse {
    // Clone data while holding the lock, then drop it before await
    let (bucket_config, retry_count, retry_delay_ms) = {
        let config = state.read().unwrap();
        
        let manifest = match config.manifests.get(&query.manifest) {
            Some(m) => m.clone(),
            None => return Redirect::to("/deployments").into_response(),
        };
        
        let bucket_config = match config.buckets.get(&manifest.bucket) {
            Some(b) => b.clone(),
            None => return Redirect::to("/deployments").into_response(),
        };
        
        (bucket_config, config.s3_retry_count, config.s3_retry_delay_ms)
    }; // Lock guard is dropped here
    
    let version = query.version.unwrap_or_else(|| "v1.0.0".to_string());
    
    // Validate version format
    if let Err(e) = validate_version(&version) {
        return Html(format!(
            r#"<!DOCTYPE html>
<html>
<head>
    <meta http-equiv="refresh" content="5;url=/deployments">
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body>
    <div class="container mt-5">
        <div class="alert alert-danger">
            <h4><i class="bi bi-exclamation-triangle"></i> Version Validation Failed</h4>
            <p>{}</p>
            <p class="mb-0">Redirecting back in 5 seconds... <a href="/deployments">Click here</a> if not redirected.</p>
        </div>
    </div>
</body>
</html>"#,
            e
        )).into_response();
    }
    
    let client = create_s3_client(&bucket_config).await;
    
    // Check if deployment is finalized (manifest.json exists) and get finalization timestamp
    let is_finalized = is_deployment_finalized(&client, &bucket_config.bucket_name, &query.manifest, &version, &bucket_config, retry_count, retry_delay_ms).await;
    let finalized_at = if is_finalized {
        load_manifest_snapshot(&client, &bucket_config.bucket_name, &query.manifest, &version, &bucket_config, retry_count, retry_delay_ms)
            .await
            .map(|s| s.created_at)
    } else {
        None
    };
    
    let template = ViewDeploymentTemplate {
        manifest_name: query.manifest,
        version,
        last_version: None,
        is_edit_mode: true,
        is_finalized,
        finalized_at,
    };
    
    HtmlTemplate(template).into_response()
}

async fn deployments_start(
    AxumState(state): AxumState<AppState>,
    Query(query): Query<DeploymentQuery>,
) -> impl IntoResponse {
    // Clone data while holding the lock, then drop it before await
    let (bucket_config, retry_count, retry_delay_ms) = {
        let config = state.read().unwrap();
        
        let manifest = match config.manifests.get(&query.manifest) {
            Some(m) => m.clone(),
            None => return Redirect::to("/deployments").into_response(),
        };
        
        let bucket_config = match config.buckets.get(&manifest.bucket) {
            Some(b) => b.clone(),
            None => return Redirect::to("/deployments").into_response(),
        };
        
        (bucket_config, config.s3_retry_count, config.s3_retry_delay_ms)
    }; // Lock guard is dropped here
    
    let version = query.version.unwrap_or_else(|| "v1.0.0".to_string());
    
    // Validate version format
    if let Err(e) = validate_version(&version) {
        return Html(format!(
            r#"<!DOCTYPE html>
<html>
<head>
    <meta http-equiv="refresh" content="5;url=/deployments">
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body>
    <div class="container mt-5">
        <div class="alert alert-danger">
            <h4><i class="bi bi-exclamation-triangle"></i> Version Validation Failed</h4>
            <p>{}</p>
            <p class="mb-0">Redirecting back in 5 seconds... <a href="/deployments">Click here</a> if not redirected.</p>
        </div>
    </div>
</body>
</html>"#,
            e
        )).into_response();
    }
    
    // Check if version already exists
    let client = create_s3_client(&bucket_config).await;
    let all_versions = find_all_versions(&client, &bucket_config.bucket_name, &query.manifest, &bucket_config, retry_count, retry_delay_ms).await;
    
    if all_versions.contains(&version) {
        // Redirect to view instead of creating
        return Redirect::to(&format!("/deployments/view?manifest={}&version={}", query.manifest, version)).into_response();
    }
    
    // Only clone if clone_from parameter is provided (explicit cloning)
    if let Some(ref clone_from_version) = query.clone_from {
        // Verify the source version exists
        if all_versions.contains(clone_from_version) {
            let source_prefix = format!("deployments/{}/{}/", query.manifest, clone_from_version);
            let target_prefix = format!("deployments/{}/{}/", query.manifest, version);
            let _ = clone_version(&client, &bucket_config.bucket_name, &source_prefix, &target_prefix, &bucket_config, retry_count, retry_delay_ms).await;
        }
    }
    // Otherwise, create empty deployment (no files, no manifest.json yet)
    
    // Redirect to view the newly created deployment
    Redirect::to(&format!("/deployments/view?manifest={}&version={}", query.manifest, version)).into_response()
}

async fn upload_file(
    AxumState(state): AxumState<AppState>,
    mut multipart: Multipart,
) -> impl IntoResponse {
    let mut manifest_name = String::new();
    let mut version = String::new();
    let mut remote_path = String::new();
    let mut file_data: Vec<u8> = Vec::new();
    
    while let Some(field) = multipart.next_field().await.unwrap() {
        let name = field.name().unwrap_or("").to_string();
        
        match name.as_str() {
            "manifest" => manifest_name = field.text().await.unwrap_or_default(),
            "version" => version = field.text().await.unwrap_or_default(),
            "remote_path" => remote_path = field.text().await.unwrap_or_default(),
            "file" => file_data = field.bytes().await.unwrap_or_default().to_vec(),
            _ => {}
        }
    }
    
    // Clone config data before await and check file type
    let (bucket_name, bucket_config, retry_count, retry_delay_ms, file_type) = {
        let config = state.read().unwrap();
        match config.manifests.get(&manifest_name) {
            Some(manifest) => {
                // Find file type for this path
                let file_type = manifest.files.iter()
                    .find(|f| f.path == remote_path)
                    .map(|f| f.file_type.clone())
                    .unwrap_or(FileType::Binary);
                
                match config.buckets.get(&manifest.bucket) {
                    Some(bc) => (bc.bucket_name.clone(), bc.clone(), 
                                config.s3_retry_count, config.s3_retry_delay_ms, file_type),
                    None => return Redirect::to(&format!("/deployments/view?manifest={}&version={}", manifest_name, version)).into_response(),
                }
            }
            None => return Redirect::to(&format!("/deployments/view?manifest={}&version={}", manifest_name, version)).into_response(),
        }
    }; // Lock dropped here
    
    let client = create_s3_client(&bucket_config).await;
    
    // Check if deployment is finalized (manifest.json exists)
    if is_deployment_finalized(&client, &bucket_name, &manifest_name, &version, &bucket_config, retry_count, retry_delay_ms).await {
        return Html(format!(
            r#"<!DOCTYPE html>
<html>
<head>
    <meta http-equiv="refresh" content="5;url=/deployments/view?manifest={}&version={}">
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body>
    <div class="container mt-5">
        <div class="alert alert-danger">
            <h4><i class="bi bi-exclamation-triangle"></i> Deployment is Finalized</h4>
            <p>Version <strong>{}</strong> has been finalized and cannot be modified.</p>
            <p>Only unfinalized deployments can be edited. Create a new version if you need to make changes.</p>
            <p class="mb-0">Redirecting back in 5 seconds... <a href="/deployments">Click here</a> if not redirected.</p>
        </div>
    </div>
</body>
</html>"#,
            manifest_name, version, version
        )).into_response();
    }
    
    let base_key = format!("deployments/{}/{}/{}", manifest_name, version, remote_path);
    let key = bucket_config.full_key(&base_key);
    
    // For text files, normalize line endings to Unix (LF)
    let file_data_normalized = if matches!(file_type, FileType::Text) {
        // Convert to string, normalize, and back to bytes
        match String::from_utf8(file_data.clone()) {
            Ok(text) => {
                let normalized = text.replace("\r\n", "\n");
                normalized.into_bytes()
            }
            Err(_) => file_data, // Not valid UTF-8, use as-is
        }
    } else {
        file_data
    };
    
    let file_data_for_retry = file_data_normalized.clone();
    let operation_name = format!("upload_file: {}", key);
    let _ = retry_s3_operation(
        &operation_name,
        retry_count,
        retry_delay_ms,
        || {
            let client = client.clone();
            let bucket_name = bucket_name.clone();
            let key = key.clone();
            let file_data = file_data_for_retry.clone();
            async move {
                client
                    .put_object()
                    .bucket(&bucket_name)
                    .key(&key)
                    .body(ByteStream::from(file_data))
                    .send()
                    .await
            }
        }
    ).await;
    
    Redirect::to(&format!("/deployments/view?manifest={}&version={}", manifest_name, version)).into_response()
}

#[derive(Deserialize)]
struct BrowseStagingQuery {
    bucket: String,
    path: Option<String>,
}

#[derive(Serialize)]
struct StagingItem {
    name: String,
    path: String,
    is_folder: bool,
    size: Option<i64>,
    last_modified: Option<i64>,
}

#[derive(Deserialize)]
struct ServiceTemplateQuery {
    manifest: String,
}

async fn api_service_template(
    AxumState(state): AxumState<AppState>,
    Query(q): Query<ServiceTemplateQuery>,
) -> impl IntoResponse {
    let config = state.read().unwrap();
    let Some(manifest) = config.manifests.get(&q.manifest) else {
        return Json(serde_json::json!({ "ok": false, "error": "manifest not found" })).into_response();
    };
    let Some((kind, target_path)) = required_service_file_and_kind(&manifest.profile) else {
        return Json(serde_json::json!({ "ok": false, "error": "manifest profile has no service template" }))
            .into_response();
    };

    // Fixed var list (non-resource) for the popup form.
    // We no longer prompt for vars; generation emits placeholders and sealing blocks unresolved
    // placeholders (except fleetagent_*).
    let vars: Vec<&str> = match manifest.profile {
        DeploymentProfile::Systemd => vec![],
        DeploymentProfile::ProcessMaster => vec![],
        _ => vec![],
    };

    // Provide resource info + systemd translation preview.
    // For processmaster: no translation needed, keep raw cpu/memory/memory_swap strings.
    let (cpu_quota, memory_max, memory_swap_max, io_weight, cpu_millis, ram_bytes, swap_bytes, raw_cpu, raw_mem, raw_swap) = {
        // Built-in unlimited: cpu/ram/swap unlimited, io_weight=100.
        if manifest.resource_profile == BUILTIN_PROFILE_UNLIMITED {
            (
                None,
                None,
                None,
                Some(100u32),
                None,
                None,
                None,
                None,
                None,
                None,
            )
        } else if let Some(rp) = config.resource_profiles.get(&manifest.resource_profile) {
            let (_, _, _, io_w, cpu_m, ram_b, swap_b) =
                validate_and_normalize_resource_profile(
                    rp.cpu.as_deref().unwrap_or(""),
                    rp.memory.as_deref().unwrap_or(""),
                    rp.memory_swap.as_deref().unwrap_or(""),
                    &rp.io_weight.map(|x| x.to_string()).unwrap_or_default(),
                )
                .unwrap_or((None, None, None, Some(100), None, None, Some(0)));
            let cpu_q = cpu_m.map(format_cpu_quota_percent);
            let mem_max = ram_b.map(format_systemd_bytes);
            let mem_swap = swap_b.map(format_systemd_bytes);
            (
                cpu_q,
                mem_max,
                mem_swap,
                io_w.or(Some(100)),
                cpu_m,
                ram_b,
                swap_b,
                rp.cpu.clone(),
                rp.memory.clone(),
                rp.memory_swap.clone(),
            )
        } else {
            (None, None, None, Some(100), None, None, Some(0), None, None, None)
        }
    };

    Json(serde_json::json!({
        "ok": true,
        "kind": kind,
        "target_path": target_path,
        "resource_profile": manifest.resource_profile,
        "resource_preview": {
            "cpu_millis": cpu_millis,
            "cpu_quota": cpu_quota,
            "ram_bytes": ram_bytes,
            "memory_max": memory_max,
            "swap_bytes": swap_bytes,
            "memory_swap_max": memory_swap_max,
            "io_weight": io_weight,
            "cpu": raw_cpu,
            "memory": raw_mem,
            "memory_swap": raw_swap,
        },
        "vars": vars,
    }))
    .into_response()
}

#[derive(Deserialize)]
struct GenerateServiceFileReq {
    manifest: String,
    version: String,
    #[serde(default)]
    file_path: Option<String>,
    #[serde(default)]
    values: HashMap<String, String>,
}

fn generation_targets(profile: &DeploymentProfile) -> Vec<&'static str> {
    let mut out: Vec<&'static str> = Vec::new();
    if matches!(
        profile,
        DeploymentProfile::Systemd | DeploymentProfile::ProcessMaster | DeploymentProfile::Supervisor
    ) {
        out.push("run.sh");
    }
    if let Some((_kind, svc)) = required_service_file_and_kind(profile) {
        out.push(svc);
    }
    out.sort();
    out.dedup();
    out
}

fn scan_unresolved_tokens(content: &str) -> (Vec<String>, Vec<String>) {
    // Returns (blocking_tokens, allowed_tokens) where allowed are fleetagent_*.
    //
    // IMPORTANT: users are allowed to put comments in their service files. We therefore ignore
    // unresolved {{ token }} occurrences on comment-only lines:
    // - YAML:   "# ..."
    // - systemd: "# ..." or "; ..."
    // - shell:  "# ..." (including "#!/bin/sh")
    let re = Regex::new(r"\{\{\s*([A-Za-z0-9_]+)\s*\}\}").expect("token regex");
    let mut blocking: Vec<String> = Vec::new();
    let mut allowed: Vec<String> = Vec::new();

    for line in content.lines() {
        let t = line.trim_start();
        let is_comment_line = t.starts_with('#') || t.starts_with(';');
        if is_comment_line {
            continue;
        }
        for cap in re.captures_iter(line) {
            let tok = cap.get(1).unwrap().as_str().to_string();
            if tok.starts_with("fleetagent_") {
                if !allowed.contains(&tok) {
                    allowed.push(tok);
                }
            } else if !blocking.contains(&tok) {
                blocking.push(tok);
            }
        }
    }

    blocking.sort();
    allowed.sort();
    (blocking, allowed)
}

async fn generate_service_file(
    AxumState(state): AxumState<AppState>,
    Json(req): Json<GenerateServiceFileReq>,
) -> impl IntoResponse {
    // Validate version.
    if let Err(e) = validate_version(&req.version) {
        return Json(serde_json::json!({ "ok": false, "error": e })).into_response();
    }

    let (manifest, bucket_config, retry_count, retry_delay_ms) = {
        let config = state.read().unwrap();
        let Some(m) = config.manifests.get(&req.manifest) else {
            return Json(serde_json::json!({ "ok": false, "error": "manifest not found" })).into_response();
        };
        let Some(b) = config.buckets.get(&m.bucket) else {
            return Json(serde_json::json!({ "ok": false, "error": "bucket not found" })).into_response();
        };
        (m.clone(), b.clone(), config.s3_retry_count, config.s3_retry_delay_ms)
    };

    let targets = generation_targets(&manifest.profile);
    if targets.is_empty() {
        return Json(serde_json::json!({ "ok": false, "error": "manifest profile has no generated templates" }))
            .into_response();
    }
    let requested = req
        .file_path
        .as_deref()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty());
    let target_path: &str = match requested {
        Some(p) => p,
        None => {
            // Default to the profile's main service file if present, else run.sh.
            targets
                .iter()
                .find(|p| **p != "run.sh")
                .copied()
                .unwrap_or("run.sh")
        }
    };
    if !targets.iter().any(|p| *p == target_path) {
        return Json(serde_json::json!({ "ok": false, "error": format!("cannot generate file: {}", target_path) }))
            .into_response();
    }

    // Ensure the manifest declares the target file (text).
    let declared = manifest
        .files
        .iter()
        .any(|f| f.path == target_path && f.file_type == FileType::Text);
    if !declared {
        return Json(serde_json::json!({
            "ok": false,
            "error": format!("manifest does not declare {} as a text file", target_path)
        }))
        .into_response();
    }

    let client = create_s3_client(&bucket_config).await;
    let is_finalized = is_deployment_finalized(
        &client,
        &bucket_config.bucket_name,
        &req.manifest,
        &req.version,
        &bucket_config,
        retry_count,
        retry_delay_ms,
    )
    .await;
    if is_finalized {
        return Json(serde_json::json!({ "ok": false, "error": "deployment is sealed" })).into_response();
    }

    // Do not prompt for variables anymore.
    // If user didn't provide a value, render a placeholder like "{{ token }}".
    let placeholder = |k: &str| format!("{{{{ {} }}}}", k);
    let get_or_placeholder = |k: &str| req.values.get(k).cloned().filter(|v| !v.trim().is_empty()).unwrap_or_else(|| placeholder(k));

    // Resolve resource profile.
    // - Processmaster: no translation (keep raw cpu/memory/memory_swap strings)
    // - Systemd: translate to CPUQuota/MemoryMax/MemorySwapMax
    let (cpu_quota, memory_max, memory_swap_max, io_weight, _cpu_millis, _ram_bytes, _swap_bytes, raw_cpu, raw_mem, raw_swap) = {
        let config = state.read().unwrap();
        if manifest.resource_profile == BUILTIN_PROFILE_UNLIMITED {
            (None, None, None, Some(100u32), None, None, None, None, None, None)
        } else if let Some(rp) = config.resource_profiles.get(&manifest.resource_profile) {
            let (_cpu_s, _ram_s, _swap_s, io_w, cpu_m, ram_b, swap_b) =
                validate_and_normalize_resource_profile(
                    rp.cpu.as_deref().unwrap_or(""),
                    rp.memory.as_deref().unwrap_or(""),
                    rp.memory_swap.as_deref().unwrap_or(""),
                    &rp.io_weight.map(|x| x.to_string()).unwrap_or_default(),
                )
                .unwrap_or((None, None, Some("0b".to_string()), Some(100), None, None, Some(0)));
            (
                cpu_m.map(format_cpu_quota_percent),
                ram_b.map(format_systemd_bytes),
                swap_b.map(format_systemd_bytes),
                io_w.or(Some(100)),
                cpu_m,
                ram_b,
                swap_b,
                rp.cpu.clone(),
                rp.memory.clone(),
                rp.memory_swap.clone(),
            )
        } else {
            (None, None, None, Some(100), None, None, Some(0), None, None, None)
        }
    };

    let service_user = manifest.service_owner.clone();
    let service_group = manifest.service_group.clone();

    let content: String = match (manifest.profile.clone(), target_path) {
        (_, "run.sh") => {
            let tpl = RunShTemplate {
                my_program: get_or_placeholder("my_program"),
            };
            match tpl.render() {
                Ok(s) => s,
                Err(e) => {
                    return Json(serde_json::json!({ "ok": false, "error": e.to_string() })).into_response()
                }
            }
        }
        (DeploymentProfile::Systemd, _) => {
            let tpl = SystemdServiceUnitTemplate {
                fleetagent_cell_id: "{{ fleetagent_cell_id }}".to_string(),
                fleetagent_service_working_dir: "{{ fleetagent_service_working_dir }}".to_string(),
                service_user: service_user.clone(),
                service_group: service_group.clone(),
                cpu_quota,
                memory_max,
                memory_swap_max,
                io_weight,
            };
            match tpl.render() {
                Ok(s) => s,
                Err(e) => return Json(serde_json::json!({ "ok": false, "error": e.to_string() })).into_response(),
            }
        }
        (DeploymentProfile::ProcessMaster, _) => {
            let tpl = ProcessMasterServiceTemplate {
                fleetagent_cell_id: "{{ fleetagent_cell_id }}".to_string(),
                fleetagent_service_working_dir: "{{ fleetagent_service_working_dir }}".to_string(),
                service_user: service_user.clone(),
                service_group: service_group.clone(),
                cpu: raw_cpu,
                memory: raw_mem,
                memory_swap: raw_swap,
                io_weight: io_weight.or(Some(100)),
            };
            match tpl.render() {
                Ok(s) => s,
                Err(e) => return Json(serde_json::json!({ "ok": false, "error": e.to_string() })).into_response(),
            }
        }
        _ => {
            return Json(serde_json::json!({ "ok": false, "error": "manifest profile has no template for this file" }))
                .into_response()
        }
    };

    let base_key = format!("deployments/{}/{}/{}", req.manifest, req.version, target_path);
    let key = bucket_config.full_key(&base_key);
    let operation_name = format!("generate_service_file: {}", key);

    let put_res = retry_s3_operation(&operation_name, retry_count, retry_delay_ms, || {
        let client = client.clone();
        let bucket = bucket_config.bucket_name.clone();
        let key = key.clone();
        let body = content.clone();
        async move {
            client
                .put_object()
                .bucket(bucket)
                .key(key)
                .body(ByteStream::from(body.into_bytes()))
                .send()
                .await
        }
    })
    .await;

    match put_res {
        Ok(_) => Json(serde_json::json!({ "ok": true, "path": target_path })).into_response(),
        Err(e) => Json(serde_json::json!({ "ok": false, "error": format!("{}", e) })).into_response(),
    }
}

async fn browse_staging(
    AxumState(state): AxumState<AppState>,
    Query(query): Query<BrowseStagingQuery>,
) -> impl IntoResponse {
    let (bucket_config, retry_count, retry_delay_ms) = {
        let config = state.read().unwrap();
        match config.buckets.get(&query.bucket) {
            Some(bc) => (bc.clone(), config.s3_retry_count, config.s3_retry_delay_ms),
            None => return Json(Vec::<StagingItem>::new()).into_response(),
        }
    };
    
    let client = create_s3_client(&bucket_config).await;
    
    // Validate and sanitize path - prevent path traversal outside staging
    let sub_path = query.path.unwrap_or_default();
    if sub_path.contains("..") || sub_path.starts_with('/') {
        return Json(Vec::<StagingItem>::new()).into_response();
    }
    
    // Staging is under the bucket prefix
    let staging_path = if sub_path.is_empty() {
        "staging/".to_string()
    } else {
        format!("staging/{}/", sub_path.trim_end_matches('/'))
    };
    
    let prefix = bucket_config.full_key(&staging_path);
    
    let result = retry_s3_operation(
        "list_staging",
        retry_count,
        retry_delay_ms,
        || async {
            client
                .list_objects_v2()
                .bucket(&bucket_config.bucket_name)
                .prefix(&prefix)
                .delimiter("/")
                .send()
                .await
        },
    ).await;
    
    let mut items = Vec::new();
    
    if let Ok(output) = result {
        // Add folders
        if let Some(common_prefixes) = output.common_prefixes {
            for prefix_obj in common_prefixes {
                if let Some(prefix_str) = prefix_obj.prefix() {
                    // Remove bucket prefix and staging/ prefix
                    let prefix_to_strip = format!("{}/", bucket_config.prefix);
                    let relative_path = prefix_str
                        .strip_prefix(&prefix_to_strip)
                        .unwrap_or(prefix_str)
                        .strip_prefix("staging/")
                        .unwrap_or("")
                        .trim_end_matches('/');
                    
                    if let Some(name) = relative_path.split('/').last() {
                        items.push(StagingItem {
                            name: name.to_string(),
                            path: relative_path.to_string(),
                            is_folder: true,
                            size: None,
                            last_modified: None,
                        });
                    }
                }
            }
        }
        
        // Add files
        if let Some(contents) = output.contents {
            for object in contents {
                if let Some(key) = object.key() {
                    // Remove bucket prefix and staging/ prefix
                    let prefix_to_strip = format!("{}/", bucket_config.prefix);
                    let relative_path = key
                        .strip_prefix(&prefix_to_strip)
                        .unwrap_or(key)
                        .strip_prefix("staging/")
                        .unwrap_or("");
                    
                    // Skip if this is the folder itself
                    if relative_path.is_empty() || relative_path.ends_with('/') {
                        continue;
                    }
                    
                    if let Some(name) = relative_path.split('/').last() {
                        let last_modified = object.last_modified()
                            .map(|dt| dt.secs() as i64);
                        
                        items.push(StagingItem {
                            name: name.to_string(),
                            path: relative_path.to_string(),
                            is_folder: false,
                            size: object.size(),
                            last_modified,
                        });
                    }
                }
            }
        }
    }
    
    Json(items).into_response()
}

#[derive(Deserialize)]
struct CopyFromStagingForm {
    manifest: String,
    version: String,
    staging_path: String,
    remote_path: String,
}

async fn copy_from_staging(
    AxumState(state): AxumState<AppState>,
    Form(form): Form<CopyFromStagingForm>,
) -> impl IntoResponse {
    // Clone config data before await
    let (bucket_name, bucket_config, retry_count, retry_delay_ms) = {
        let config = state.read().unwrap();
        match config.manifests.get(&form.manifest) {
            Some(manifest) => {
                match config.buckets.get(&manifest.bucket) {
                    Some(bc) => (bc.bucket_name.clone(), bc.clone(), 
                                config.s3_retry_count, config.s3_retry_delay_ms),
                    None => return Redirect::to(&format!("/deployments/view?manifest={}&version={}", form.manifest, form.version)).into_response(),
                }
            }
            None => return Redirect::to(&format!("/deployments/view?manifest={}&version={}", form.manifest, form.version)).into_response(),
        }
    };
    
    let client = create_s3_client(&bucket_config).await;
    
    // Check if deployment is finalized
    if is_deployment_finalized(&client, &bucket_name, &form.manifest, &form.version, &bucket_config, retry_count, retry_delay_ms).await {
        return Html(format!(
            r#"<!DOCTYPE html>
<html>
<head>
    <meta http-equiv="refresh" content="5;url=/deployments/view?manifest={}&version={}">
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body>
    <div class="container mt-5">
        <div class="alert alert-danger">
            <h4><i class="bi bi-exclamation-triangle"></i> Deployment is Sealed</h4>
            <p>This deployment has been sealed and cannot be modified.</p>
            <p>Redirecting back in 5 seconds... <a href="/deployments/view?manifest={}&version={}">Click here</a> if not redirected.</p>
        </div>
    </div>
</body>
</html>"#,
            form.manifest, form.version, form.manifest, form.version
        )).into_response();
    }
    
    // Validate staging path - prevent path traversal
    if form.staging_path.contains("..") || form.staging_path.starts_with('/') {
        return Html("<html><body>Invalid staging path</body></html>").into_response();
    }
    
    // Staging is under the bucket prefix
    let source_key = bucket_config.full_key(&format!("staging/{}", form.staging_path));
    let dest_key = bucket_config.full_key(&format!("deployments/{}/{}/{}", 
                                                   form.manifest, form.version, form.remote_path));
    
    // Copy object within the same bucket
    let copy_source = format!("{}/{}", bucket_name, source_key);
    
    let _result = retry_s3_operation(
        "copy_from_staging",
        retry_count,
        retry_delay_ms,
        || async {
            client
                .copy_object()
                .bucket(&bucket_name)
                .copy_source(&copy_source)
                .key(&dest_key)
                .send()
                .await
        },
    ).await;
    
    Redirect::to(&format!("/deployments/view?manifest={}&version={}", form.manifest, form.version)).into_response()
}

async fn edit_file(
    AxumState(state): AxumState<AppState>,
    Query(query): Query<EditQuery>,
) -> impl IntoResponse {
    println!("=== edit_file called ===");
    println!("manifest: {}, version: {}, file_path: {}", 
             query.manifest, query.version, query.file_path);
    
    // Clone config data before await
    let (bucket_name, bucket_config, retry_count, retry_delay_ms) = {
        let config = state.read().unwrap();
        
        let manifest = match config.manifests.get(&query.manifest) {
            Some(m) => m,
            None => return Redirect::to("/deployments").into_response(),
        };
        
        let bucket_config = match config.buckets.get(&manifest.bucket) {
            Some(b) => b.clone(),
            None => return Redirect::to("/deployments").into_response(),
        };
        
        (bucket_config.bucket_name.clone(), bucket_config, 
         config.s3_retry_count, config.s3_retry_delay_ms)
    }; // Lock dropped here
    
    let client = create_s3_client(&bucket_config).await;
    
    // Check if deployment is finalized (manifest.json exists)
    let is_finalized = is_deployment_finalized(&client, &bucket_name, &query.manifest, &query.version, &bucket_config, retry_count, retry_delay_ms).await;
    
    // If finalized and not explicitly in view mode via the file list, just show read-only view
    // We allow viewing sealed deployments from the file list (they open in a new tab)
    
    let base_key = format!("deployments/{}/{}/{}", query.manifest, query.version, query.file_path);
    let key = bucket_config.full_key(&base_key);
    
    println!("Reading file from S3: bucket={}, key={}", bucket_name, key);
    
    let operation_name = format!("read_file: {}", key);
    let content = match retry_s3_operation(
        &operation_name,
        retry_count,
        retry_delay_ms,
        || {
            let client = client.clone();
            let bucket_name = bucket_name.clone();
            let key = key.clone();
            async move {
                client.get_object().bucket(&bucket_name).key(&key).send().await
            }
        }
    ).await {
        Ok(output) => {
            match output.body.collect().await {
                Ok(data) => {
                    let content = String::from_utf8_lossy(&data.into_bytes()).to_string();
                    println!("Successfully read file, content_length={}", content.len());
                    content
                }
                Err(e) => {
                    eprintln!("Error reading file body: {:?}", e);
                    format!("# New file: {}\n", query.file_path)
                }
            }
        }
        Err(e) => {
            eprintln!("Error getting file from S3: {:?}", e);
            format!("# New file: {}\n", query.file_path)
        }
    };
    
    let template = EditFileTemplate {
        manifest: query.manifest,
        version: query.version,
        file_path: query.file_path,
        content,
        is_readonly: is_finalized,
    };
    
    HtmlTemplate(template).into_response()
}

async fn download_file(
    AxumState(state): AxumState<AppState>,
    Query(query): Query<EditQuery>,
) -> impl IntoResponse {
    println!("=== download_file called ===");
    println!("manifest: {}, version: {}, file_path: {}", 
             query.manifest, query.version, query.file_path);
    
    // Clone config data before await
    let (bucket_name, bucket_config, retry_count, retry_delay_ms) = {
        let config = state.read().unwrap();
        
        let manifest = match config.manifests.get(&query.manifest) {
            Some(m) => m,
            None => return (StatusCode::NOT_FOUND, "Manifest not found").into_response(),
        };
        
        let bucket_config = match config.buckets.get(&manifest.bucket) {
            Some(b) => b.clone(),
            None => return (StatusCode::NOT_FOUND, "Bucket not found").into_response(),
        };
        
        (bucket_config.bucket_name.clone(), bucket_config, 
         config.s3_retry_count, config.s3_retry_delay_ms)
    }; // Lock dropped here
    
    let client = create_s3_client(&bucket_config).await;
    
    let base_key = format!("deployments/{}/{}/{}", query.manifest, query.version, query.file_path);
    let key = bucket_config.full_key(&base_key);
    
    println!("Downloading file from S3: bucket={}, key={}", bucket_name, key);
    
    let operation_name = format!("download_file: {}", key);
    match retry_s3_operation(
        &operation_name,
        retry_count,
        retry_delay_ms,
        || {
            let client = client.clone();
            let bucket_name = bucket_name.clone();
            let key = key.clone();
            async move {
                client.get_object().bucket(&bucket_name).key(&key).send().await
            }
        }
    ).await {
        Ok(output) => {
            // Get content_type before consuming the body
            let content_type = output.content_type().unwrap_or("application/octet-stream").to_string();
            
            match output.body.collect().await {
                Ok(data) => {
                    let bytes = data.into_bytes();
                    
                    // Extract filename from path
                    let filename = query.file_path.split('/').last().unwrap_or("download");
                    
                    println!("Successfully downloaded file, size={} bytes", bytes.len());
                    
                    // Create response with appropriate headers for download
                    Response::builder()
                        .status(StatusCode::OK)
                        .header("Content-Type", content_type)
                        .header("Content-Disposition", format!("attachment; filename=\"{}\"", filename))
                        .body(Body::from(bytes))
                        .unwrap()
                        .into_response()
                }
                Err(e) => {
                    eprintln!("Error reading file body: {:?}", e);
                    (StatusCode::INTERNAL_SERVER_ERROR, "Error reading file").into_response()
                }
            }
        }
        Err(e) => {
            eprintln!("Error getting file from S3: {:?}", e);
            (StatusCode::NOT_FOUND, "File not found").into_response()
        }
    }
}

async fn save_file(
    AxumState(state): AxumState<AppState>,
    Form(form): Form<SaveForm>,
) -> impl IntoResponse {
    println!("=== save_file called ===");
    println!("manifest: {}, version: {}, file_path: {}, content_length: {}", 
             form.manifest, form.version, form.file_path, form.content.len());
    
    // Clone config data before await
    let (bucket_name, bucket_config, retry_count, retry_delay_ms) = {
        let config = state.read().unwrap();
        match config.manifests.get(&form.manifest) {
            Some(manifest) => {
                match config.buckets.get(&manifest.bucket) {
                    Some(bc) => (bc.bucket_name.clone(), bc.clone(),
                                config.s3_retry_count, config.s3_retry_delay_ms),
                    None => return Redirect::to(&format!("/deployments/view?manifest={}&version={}", form.manifest, form.version)).into_response(),
                }
            }
            None => return Redirect::to(&format!("/deployments/view?manifest={}&version={}", form.manifest, form.version)).into_response(),
        }
    }; // Lock dropped here
    
    let client = create_s3_client(&bucket_config).await;
    
    // Check if deployment is finalized (manifest.json exists)
    if is_deployment_finalized(&client, &bucket_name, &form.manifest, &form.version, &bucket_config, retry_count, retry_delay_ms).await {
        return Html(format!(
            r#"<!DOCTYPE html>
<html>
<head>
    <meta http-equiv="refresh" content="5;url=/deployments/view?manifest={}&version={}">
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body>
    <div class="container mt-5">
        <div class="alert alert-danger">
            <h4><i class="bi bi-exclamation-triangle"></i> Deployment is Finalized</h4>
            <p>Version <strong>{}</strong> has been finalized and cannot be modified.</p>
            <p>Only unfinalized deployments can be edited. Create a new version if you need to make changes.</p>
            <p class="mb-0">Redirecting back in 5 seconds... <a href="/deployments">Click here</a> if not redirected.</p>
        </div>
    </div>
</body>
</html>"#,
            form.manifest, form.version, form.version
        )).into_response();
    }
    
    let base_key = format!("deployments/{}/{}/{}", form.manifest, form.version, form.file_path);
    let key = bucket_config.full_key(&base_key);
    
    // Convert Windows line endings (CRLF) to Unix (LF)
    let normalized_content = form.content.replace("\r\n", "\n");
    
    println!("Saving file to S3: bucket={}, key={}, content_length={}", 
             bucket_name, key, normalized_content.len());
    println!("Content preview (first 100 chars): {}", 
             &normalized_content.chars().take(100).collect::<String>());
    
    let content_for_retry = normalized_content.clone();
    let operation_name = format!("save_file: {}", key);
    match retry_s3_operation(
        &operation_name,
        retry_count,
        retry_delay_ms,
        || {
            let client = client.clone();
            let bucket_name = bucket_name.clone();
            let key = key.clone();
            let content = content_for_retry.clone();
            async move {
                client
                    .put_object()
                    .bucket(&bucket_name)
                    .key(&key)
                    .body(ByteStream::from(content.into_bytes()))
                    .send()
                    .await
            }
        }
    ).await
    {
        Ok(response) => {
            println!("Successfully saved file: {}", key);
            println!("S3 Response ETag: {:?}", response.e_tag());
        }
        Err(e) => {
            eprintln!("Error saving file {}: {:?}", key, e);
        }
    }
    
    Redirect::to(&format!("/deployments/view?manifest={}&version={}", form.manifest, form.version)).into_response()
}

async fn list_files(
    AxumState(state): AxumState<AppState>,
    Query(query): Query<FileQuery>,
) -> impl IntoResponse {
    // Clone config data before await
    let (bucket_name, bucket_config) = {
        let config = state.read().unwrap();
        
        let manifest = match config.manifests.get(&query.manifest) {
            Some(m) => m,
            None => return Html("<div class='alert alert-danger'>Manifest not found</div>".to_string()),
        };
        
        let bucket_config = match config.buckets.get(&manifest.bucket) {
            Some(b) => b.clone(),
            None => return Html("<div class='alert alert-danger'>Bucket not found</div>".to_string()),
        };
        
        (bucket_config.bucket_name.clone(), bucket_config)
    }; // Lock dropped here
    
    let client = create_s3_client(&bucket_config).await;
    let base_prefix = format!("deployments/{}/{}/", query.manifest, query.version);
    let prefix = bucket_config.full_key(&base_prefix);
    
    let result = match client
        .list_objects_v2()
        .bucket(&bucket_name)
        .prefix(&prefix)
        .send()
        .await
    {
        Ok(r) => r,
        Err(_) => return Html("<div class='alert alert-danger'>Failed to list files</div>".to_string()),
    };
    
    let mut html = String::from("<ul class='list-group'>");
    
    let contents = result.contents();
    if contents.is_empty() {
        html.push_str("<li class='list-group-item'><em>No files yet</em></li>");
    } else {
        for object in contents {
            if let Some(key) = object.key() {
                let size = object.size().unwrap_or(0);
                let relative_path = key.strip_prefix(&prefix).unwrap_or(key);
                html.push_str(&format!(
                    "<li class='list-group-item d-flex justify-content-between align-items-center'><code>{}</code><span class='badge bg-secondary'>{} bytes</span></li>",
                    relative_path, size
                ));
            }
        }
    }
    
    html.push_str("</ul>");
    Html(html)
}

async fn get_file_status(
    AxumState(state): AxumState<AppState>,
    Query(query): Query<FileStatusQuery>,
) -> Json<serde_json::Value> {
    use serde_json::json;
    
    // Clone config data before await
    let (bucket_name, bucket_config, retry_count, retry_delay_ms) = {
        let config = state.read().unwrap();
        
        let manifest = match config.manifests.get(&query.manifest) {
            Some(m) => m,
            None => return Json(json!({"error": "Manifest not found"})),
        };
        
        let bucket_config = match config.buckets.get(&manifest.bucket) {
            Some(b) => b.clone(),
            None => return Json(json!({"error": "Bucket not found"})),
        };
        
        (bucket_config.bucket_name.clone(), bucket_config,
         config.s3_retry_count, config.s3_retry_delay_ms)
    };
    
    let client = create_s3_client(&bucket_config).await;
    
    // Check if deployment is finalized and load snapshot if it is
    let manifest_files = if is_deployment_finalized(&client, &bucket_name, &query.manifest, &query.version, &bucket_config, retry_count, retry_delay_ms).await {
        // Load from manifest.json snapshot
        match load_manifest_snapshot(&client, &bucket_name, &query.manifest, &query.version, &bucket_config, retry_count, retry_delay_ms).await {
            Some(snapshot) => snapshot.files,
            None => {
                // Fallback to current manifest if snapshot can't be loaded
                let config = state.read().unwrap();
                config.manifests.get(&query.manifest)
                    .map(|m| m.files.clone())
                    .unwrap_or_default()
            }
        }
    } else {
        // Use current manifest for draft versions
        let config = state.read().unwrap();
        config.manifests.get(&query.manifest)
            .map(|m| m.files.clone())
            .unwrap_or_default()
    };
    let base_prefix = format!("deployments/{}/{}/", query.manifest, query.version);
    let prefix = bucket_config.full_key(&base_prefix);
    
    // Get all existing files with metadata - if this fails, treat it as empty (fresh deployment)
    let existing_files: std::collections::HashMap<String, (i64, String)> = match client
        .list_objects_v2()
        .bucket(&bucket_name)
        .prefix(&prefix)
        .send()
        .await
    {
        Ok(result) => result
            .contents()
            .iter()
            .filter_map(|obj| {
                let key = obj.key()?;
                let relative_key = key.strip_prefix(&prefix).unwrap_or(key).to_string();
                let size = obj.size().unwrap_or(0);
                let last_modified = obj.last_modified()
                    .map(|dt| dt.fmt(aws_sdk_s3::primitives::DateTimeFormat::DateTime).ok())
                    .flatten()
                    .unwrap_or_else(|| "Unknown".to_string());
                Some((relative_key, (size, last_modified)))
            })
            .collect(),
        Err(e) => {
            // Log the error but continue - this is likely a fresh deployment with no files yet
            eprintln!("Warning: Failed to list S3 objects (treating as empty): {}", e);
            std::collections::HashMap::new()
        }
    };
    
    let mut file_statuses = Vec::new();
    for file in manifest_files {
        let metadata = existing_files.get(&file.path);
        let exists = metadata.is_some();
        
        if let Some((size, last_modified)) = metadata {
            file_statuses.push(json!({
                "path": file.path,
                "file_type": file.file_type.to_string(),
                "exists": exists,
                "size": size,
                "last_modified": last_modified,
            }));
        } else {
            file_statuses.push(json!({
                "path": file.path,
                "file_type": file.file_type.to_string(),
                "exists": exists,
                "size": null,
                "last_modified": null,
            }));
        }
    }
    
    Json(json!({"files": file_statuses}))
}

async fn delete_file(
    AxumState(state): AxumState<AppState>,
    Form(form): Form<DeleteFileForm>,
) -> impl IntoResponse {
    // Clone config data before await
    let (bucket_name, bucket_config, retry_count, retry_delay_ms) = {
        let config = state.read().unwrap();
        match config.manifests.get(&form.manifest) {
            Some(manifest) => {
                match config.buckets.get(&manifest.bucket) {
                    Some(bc) => (bc.bucket_name.clone(), bc.clone(),
                                config.s3_retry_count, config.s3_retry_delay_ms),
                    None => return Redirect::to(&format!("/deployments/view?manifest={}&version={}", form.manifest, form.version)).into_response(),
                }
            }
            None => return Redirect::to(&format!("/deployments/view?manifest={}&version={}", form.manifest, form.version)).into_response(),
        }
    };
    
    let client = create_s3_client(&bucket_config).await;
    
    // Check if deployment is finalized (manifest.json exists)
    if is_deployment_finalized(&client, &bucket_name, &form.manifest, &form.version, &bucket_config, retry_count, retry_delay_ms).await {
        return Html(format!(
            r#"<!DOCTYPE html>
<html>
<head>
    <meta http-equiv="refresh" content="5;url=/deployments/view?manifest={}&version={}">
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body>
    <div class="container mt-5">
        <div class="alert alert-danger">
            <h4><i class="bi bi-exclamation-triangle"></i> Deployment is Sealed</h4>
            <p>Version <strong>{}</strong> has been sealed and cannot be modified.</p>
            <p>Only unsealed deployments can be edited. Create a new version if you need to make changes.</p>
            <p class="mb-0">Redirecting back in 5 seconds... <a href="/deployments">Click here</a> if not redirected.</p>
        </div>
    </div>
</body>
</html>"#,
            form.manifest, form.version, form.version
        )).into_response();
    }
    
    let base_key = format!("deployments/{}/{}/{}", form.manifest, form.version, form.file_path);
    let key = bucket_config.full_key(&base_key);
    
    let operation_name = format!("delete_file: {}", key);
    let _ = retry_s3_operation(
        &operation_name,
        retry_count,
        retry_delay_ms,
        || {
            let client = client.clone();
            let bucket_name = bucket_name.clone();
            let key = key.clone();
            async move {
                client
                    .delete_object()
                    .bucket(&bucket_name)
                    .key(&key)
                    .send()
                    .await
            }
        }
    ).await;
    
    Redirect::to(&format!("/deployments/view?manifest={}&version={}", form.manifest, form.version)).into_response()
}

#[derive(serde::Deserialize)]
struct FinishDeploymentForm {
    manifest: String,
    version: String,
}

async fn finish_deployment(
    AxumState(state): AxumState<AppState>,
    Form(form): Form<FinishDeploymentForm>,
) -> impl IntoResponse {
    // Clone config data before await
    let (bucket_name, bucket_config, retry_count, retry_delay_ms, manifest) = {
        let config = state.read().unwrap();
        match config.manifests.get(&form.manifest) {
            Some(m) => {
                match config.buckets.get(&m.bucket) {
                    Some(bc) => (bc.bucket_name.clone(), bc.clone(),
                                config.s3_retry_count, config.s3_retry_delay_ms, m.clone()),
                    None => return Redirect::to(&format!("/deployments/view?manifest={}&version={}", form.manifest, form.version)).into_response(),
                }
            }
            None => return Redirect::to(&format!("/deployments/view?manifest={}&version={}", form.manifest, form.version)).into_response(),
        }
    };
    
    let client = create_s3_client(&bucket_config).await;
    
    // Check if already finalized
    if is_deployment_finalized(&client, &bucket_name, &form.manifest, &form.version, &bucket_config, retry_count, retry_delay_ms).await {
        return Redirect::to(&format!("/deployments/start?manifest={}&version={}&edit=true", form.manifest, form.version)).into_response();
    }

    // Block sealing if required files contain unresolved tokens (except fleetagent_*).
    // This includes run.sh (for systemd/processmaster) and the main service file.
    for target_path in generation_targets(&manifest.profile) {
        let key = bucket_config.full_key(&format!(
            "deployments/{}/{}/{}",
            form.manifest, form.version, target_path
        ));
        let op = format!("finish_deployment_check_tokens: {}", key);
        let res = retry_s3_operation(&op, retry_count, retry_delay_ms, || {
            let client = client.clone();
            let bucket = bucket_name.clone();
            let key = key.clone();
            async move { client.get_object().bucket(bucket).key(key).send().await }
        })
        .await;

        let output = match res {
            Ok(o) => o,
            Err(e) => {
                return Html(format!(
                    r#"<!DOCTYPE html>
<html>
<head>
    <meta http-equiv="refresh" content="8;url=/deployments/view?manifest={}&version={}">
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body>
    <div class="container mt-5">
        <div class="alert alert-danger">
            <h4><i class="bi bi-exclamation-triangle"></i> Cannot Seal Deployment</h4>
            <p>Failed to read required file <code>{}</code>: {}</p>
            <p class="mb-0">Redirecting back in 8 seconds... <a href="/deployments/view?manifest={}&version={}">Click here</a> if not redirected.</p>
        </div>
    </div>
</body>
</html>"#,
                    form.manifest, form.version, target_path, e, form.manifest, form.version
                ))
                .into_response();
            }
        };

        let bytes = match output.body.collect().await {
            Ok(b) => b.into_bytes(),
            Err(e) => {
                return Html(format!(
                    r#"<!DOCTYPE html>
<html>
<head>
    <meta http-equiv="refresh" content="8;url=/deployments/view?manifest={}&version={}">
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body>
    <div class="container mt-5">
        <div class="alert alert-danger">
            <h4><i class="bi bi-exclamation-triangle"></i> Cannot Seal Deployment</h4>
            <p>Failed to read required file <code>{}</code>: {}</p>
            <p class="mb-0">Redirecting back in 8 seconds... <a href="/deployments/view?manifest={}&version={}">Click here</a> if not redirected.</p>
        </div>
    </div>
</body>
</html>"#,
                    form.manifest, form.version, target_path, e, form.manifest, form.version
                ))
                .into_response();
            }
        };

        let content = String::from_utf8_lossy(&bytes).to_string();
        let (blocking, allowed) = scan_unresolved_tokens(&content);
        if !blocking.is_empty() {
            let edit_url = format!(
                "/deployments/edit?manifest={}&version={}&file_path={}&edit=true",
                form.manifest, form.version, target_path
            );
            let tokens_html = blocking
                .iter()
                .map(|t| format!("<li><code>{{{{ {} }}}}</code></li>", t))
                .collect::<Vec<_>>()
                .join("");
            let allowed_html = if allowed.is_empty() {
                "".to_string()
            } else {
                format!(
                    "<p class=\"mb-2\">Allowed unresolved tokens (will be populated by agent): {}</p>",
                    allowed
                        .iter()
                        .map(|t| format!("<code>{{{{ {} }}}}</code>", t))
                        .collect::<Vec<_>>()
                        .join(" ")
                )
            };

            return Html(format!(
                r#"<!DOCTYPE html>
<html>
<head>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body>
    <div class="container mt-5">
        <div class="alert alert-danger">
            <h4><i class="bi bi-exclamation-triangle"></i> Cannot Seal Deployment</h4>
            <p>File <code>{}</code> still contains unresolved placeholders.</p>
            {}
            <p class="mb-2">Please edit the file and replace/remove these tokens:</p>
            <ul class="mb-3">{}</ul>
            <a class="btn btn-primary" href="{}"><i class="bi bi-pencil"></i> Edit {}</a>
            <a class="btn btn-secondary ms-2" href="/deployments/view?manifest={}&version={}">Back</a>
        </div>
    </div>
</body>
</html>"#,
                target_path, allowed_html, tokens_html, edit_url, target_path, form.manifest, form.version
            ))
            .into_response();
        }
    }
    
    // Create manifest snapshot
    let manifest_snapshot = manifest.to_snapshot(&form.manifest, &form.version);
    let manifest_json = serde_json::to_string_pretty(&manifest_snapshot)
        .unwrap_or_else(|_| "{}".to_string());
    
    // Save manifest.json to finalize the deployment
    let manifest_key_base = format!("deployments/{}/{}/manifest.json", form.manifest, form.version);
    let manifest_key = bucket_config.full_key(&manifest_key_base);
    
    println!("Finalizing deployment by saving manifest.json: {}", manifest_key);
    
    let put_manifest_op = format!("finalize_deployment: {}", manifest_key);
    let _ = retry_s3_operation(
        &put_manifest_op,
        retry_count,
        retry_delay_ms,
        || {
            let client = client.clone();
            let bucket = bucket_name.clone();
            let key = manifest_key.clone();
            let content = manifest_json.clone();
            async move {
                client
                    .put_object()
                    .bucket(&bucket)
                    .key(&key)
                    .body(ByteStream::from(content.into_bytes()))
                    .send()
                    .await
            }
        }
    ).await;
    
    Redirect::to("/deployments").into_response()
}

// API endpoint to get finalized versions for a manifest
#[derive(Deserialize)]
struct ManifestVersionsQuery {
    manifest: String,
}

async fn get_manifest_versions(
    AxumState(state): AxumState<AppState>,
    Query(query): Query<ManifestVersionsQuery>,
) -> impl IntoResponse {
    // Clone config data before await
    let (bucket_name, bucket_display_name, bucket_config, retry_count, retry_delay_ms) = {
        let config = state.read().unwrap();
        match config.manifests.get(&query.manifest) {
            Some(manifest) => {
                match config.buckets.get(&manifest.bucket) {
                    Some(bc) => (bc.bucket_name.clone(), manifest.bucket.clone(), bc.clone(),
                                config.s3_retry_count, config.s3_retry_delay_ms),
                    None => return Json(serde_json::json!({ "versions": [] })).into_response(),
                }
            }
            None => return Json(serde_json::json!({ "versions": [] })).into_response(),
        }
    };
    
    let client = create_s3_client(&bucket_config).await;
    
    // Get all versions
    let all_versions = find_all_versions(&client, &bucket_name, &query.manifest, &bucket_config, retry_count, retry_delay_ms).await;
    
    // Check which versions are finalized
    let mut version_infos = Vec::new();
    for version in all_versions {
        let is_finalized = is_deployment_finalized(&client, &bucket_name, &query.manifest, &version, &bucket_config, retry_count, retry_delay_ms).await;
        if is_finalized {
            version_infos.push(VersionInfo {
                version: version.clone(),
                is_finalized,
            });
        }
    }
    
    // Sort versions in descending order (newest first)
    version_infos.sort_by(|a, b| b.version.cmp(&a.version));
    
    Json(serde_json::json!({ 
        "versions": version_infos,
        "bucket": bucket_display_name 
    })).into_response()
}

// API endpoint to get finalized versions for a manifest directly
#[derive(Deserialize)]
struct AppVersionsQuery {
    manifest: String,
    bucket: String,
}

async fn get_app_versions(
    AxumState(state): AxumState<AppState>,
    Query(query): Query<AppVersionsQuery>,
) -> impl IntoResponse {
    // Clone config data before await
    let (bucket_name, bucket_config, retry_count, retry_delay_ms) = {
        let config = state.read().unwrap();
        match config.buckets.get(&query.bucket) {
            Some(bc) => (bc.bucket_name.clone(), bc.clone(),
                        config.s3_retry_count, config.s3_retry_delay_ms),
            None => return Json(serde_json::json!({ "versions": [] })).into_response(),
        }
    };
    
    let client = create_s3_client(&bucket_config).await;
    
    // Get all versions for this manifest
    let all_versions = find_all_versions(&client, &bucket_name, &query.manifest, &bucket_config, retry_count, retry_delay_ms).await;
    
    // Check which versions are finalized
    let mut version_infos = Vec::new();
    for version in all_versions {
        let is_finalized = is_deployment_finalized(&client, &bucket_name, &query.manifest, &version, &bucket_config, retry_count, retry_delay_ms).await;
        if is_finalized {
            version_infos.push(VersionInfo {
                version: version.clone(),
                is_finalized,
            });
        }
    }
    
    // Sort versions in descending order (newest first)
    version_infos.sort_by(|a, b| b.version.cmp(&a.version));
    
    Json(serde_json::json!({ "versions": version_infos })).into_response()
}

// API endpoint to get cells for a manifest
#[derive(Deserialize)]
struct AppCellsQuery {
    manifest: String,
}

#[derive(Deserialize)]
struct CellVersionExistsQuery {
    node_id: String,
    cell_id: String,
    version: String,
}

#[derive(Deserialize)]
struct CellVersionsExistQuery {
    version: String,
    cells: String, // JSON array of cell_keys (node_id/cell_id)
}

#[derive(Serialize)]
struct CellVersionExistResult {
    cell_key: String,
    exists: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Serialize)]
struct CellInfo {
    cell_key: String,
    node_id: String,
    cell_id: String,
    bucket: String,
}

async fn get_app_cells(
    AxumState(state): AxumState<AppState>,
    Query(query): Query<AppCellsQuery>,
) -> impl IntoResponse {
    let config = state.read().unwrap();
    
    // Find all cells that match the manifest name
    let cells: Vec<CellInfo> = config.cells.iter()
        .filter(|(_, cell)| cell.manifest == query.manifest)
        .map(|(cell_key, cell)| {
            let bucket_name = config.buckets.get(&cell.bucket)
                .map(|bc| bc.bucket_name.clone())
                .unwrap_or_else(|| cell.bucket.clone());
            CellInfo {
                cell_key: cell_key.clone(),
                node_id: cell.node_id.clone(),
                cell_id: cell.cell_id.clone(),
                bucket: bucket_name,
            }
        })
        .collect();
    
    Json(serde_json::json!({ "cells": cells })).into_response()
}

async fn api_cell_version_exists(
    AxumState(state): AxumState<AppState>,
    Query(query): Query<CellVersionExistsQuery>,
) -> impl IntoResponse {
    let (bucket_name, bucket_display, bucket_config, retry_count, retry_delay_ms) = {
        let config = state.read().unwrap();
        let cell_key = format!("{}/{}", query.node_id, query.cell_id);
        let cell = match config.cells.get(&cell_key) {
            Some(c) => c,
            None => return Json(serde_json::json!({ "exists": false, "error": "Cell not found" })).into_response(),
        };
        let bc = match config.buckets.get(&cell.bucket) {
            Some(bc) => bc.clone(),
            None => {
                return Json(serde_json::json!({ "exists": false, "error": "Bucket not configured" })).into_response()
            }
        };
        (bc.bucket_name.clone(), cell.bucket.clone(), bc, config.s3_retry_count, config.s3_retry_delay_ms)
    };

    let client = create_s3_client(&bucket_config).await;
    let key_base = format!(
        "cells/{}/{}/versions/{}/manifest.json",
        query.node_id, query.cell_id, query.version
    );
    let key = bucket_config.full_key(&key_base);

    let op = format!("head_cell_version_manifest: {}", key);
    let exists = retry_s3_operation(
        &op,
        retry_count,
        retry_delay_ms,
        || {
            let client = client.clone();
            let bucket = bucket_name.clone();
            let key = key.clone();
            async move { client.head_object().bucket(&bucket).key(&key).send().await }
        },
    )
    .await
    .is_ok();

    Json(serde_json::json!({
        "cell_key": format!("{}/{}", query.node_id, query.cell_id),
        "bucket": bucket_display,
        "exists": exists
    }))
    .into_response()
}

async fn api_cell_versions_exist(
    AxumState(state): AxumState<AppState>,
    Query(query): Query<CellVersionsExistQuery>,
) -> impl IntoResponse {
    let cells: Vec<String> = match serde_json::from_str(&query.cells) {
        Ok(c) => c,
        Err(e) => {
            return Json(serde_json::json!({
                "version": query.version,
                "results": [],
                "error": format!("Invalid cells JSON: {}", e)
            }))
            .into_response()
        }
    };

    let (retry_count, retry_delay_ms) = {
        let config = state.read().unwrap();
        (config.s3_retry_count, config.s3_retry_delay_ms)
    };

    let futs: Vec<_> = cells.into_iter().map(|cell_key| {
        let state = state.clone();
        let version = query.version.clone();
        async move {
            let (bucket_name, bucket_config, _bucket_display, node_id, cell_id) = {
                let config = state.read().unwrap();
                let parts: Vec<&str> = cell_key.split('/').collect();
                if parts.len() != 2 {
                    return CellVersionExistResult { cell_key, exists: false, error: Some("Invalid cell_key".to_string()) };
                }
                let node_id = parts[0].to_string();
                let cell_id = parts[1].to_string();
                let cell = match config.cells.get(&format!("{}/{}", node_id, cell_id)) {
                    Some(c) => c,
                    None => return CellVersionExistResult { cell_key, exists: false, error: Some("Cell not found".to_string()) },
                };
                let bc = match config.buckets.get(&cell.bucket) {
                    Some(bc) => bc.clone(),
                    None => return CellVersionExistResult { cell_key, exists: false, error: Some("Bucket not configured".to_string()) },
                };
                (bc.bucket_name.clone(), bc, cell.bucket.clone(), node_id, cell_id)
            };

            let client = create_s3_client(&bucket_config).await;
            let key_base = format!("cells/{}/{}/versions/{}/manifest.json", node_id, cell_id, version);
            let key = bucket_config.full_key(&key_base);
            let op = format!("head_cell_version_manifest: {}", key);
            let exists = retry_s3_operation(
                &op,
                retry_count,
                retry_delay_ms,
                || {
                    let client = client.clone();
                    let bucket = bucket_name.clone();
                    let key = key.clone();
                    async move { client.head_object().bucket(&bucket).key(&key).send().await }
                },
            )
            .await
            .is_ok();

            CellVersionExistResult { cell_key: format!("{}/{}", node_id, cell_id), exists, error: None }
        }
    }).collect();

    let mut results = join_all(futs).await;
    results.sort_by(|a, b| a.cell_key.cmp(&b.cell_key));

    Json(serde_json::json!({
        "version": query.version,
        "results": results
    }))
    .into_response()
}

// Helper to deserialize either a single string or a vec of strings
// Bulk publish deployment to multiple cells
#[derive(Deserialize)]
struct BulkPublishForm {
    manifest: String,
    version: String,
    cells: String,  // JSON array of cell_keys (node_id/cell_id)
}

async fn bulk_publish_deployment(
    AxumState(state): AxumState<AppState>,
    Form(form): Form<BulkPublishForm>,
) -> impl IntoResponse {
    // Parse the cells JSON array
    let cells: Vec<String> = match serde_json::from_str(&form.cells) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Failed to parse cells JSON: {}", e);
            return Redirect::to(&format!("/deployments/start?manifest={}&version={}&edit=true", 
                                         form.manifest, form.version)).into_response();
        }
    };
    
    // Get the cells list or return early if none selected
    if cells.is_empty() {
        return Redirect::to(&format!("/deployments/start?manifest={}&version={}&edit=true", 
                                     form.manifest, form.version)).into_response();
    }
    
    // Get manifest info for profile and source bucket
    let (profile, source_bucket_name, source_bucket_config, service_owner, service_group) = {
        let config = state.read().unwrap();
        match config.manifests.get(&form.manifest) {
            Some(manifest) => {
                let profile = manifest.profile.to_string();
                let service_owner = manifest.service_owner.clone();
                let service_group = manifest.service_group.clone();
                match config.buckets.get(&manifest.bucket) {
                    Some(bc) => (profile, bc.bucket_name.clone(), bc.clone(), service_owner, service_group),
                    None => return Redirect::to("/deployments").into_response(),
                }
            }
            None => return Redirect::to("/deployments").into_response(),
        }
    };
    
    // Publish to each selected cell
    for cell_key in &cells {
        let parts: Vec<&str> = cell_key.split('/').collect();
        if parts.len() != 2 {
            continue; // Skip invalid cell keys
        }
        let node_id = parts[0];
        let cell_id = parts[1];
        
        // Clone config data for each cell
        let (cell_bucket_name, cell_bucket_config, retry_count, retry_delay_ms) = {
            let config = state.read().unwrap();
            match config.cells.get(cell_key) {
                Some(cell) => {
                    match config.buckets.get(&cell.bucket) {
                        Some(bc) => (bc.bucket_name.clone(), bc.clone(),
                                    config.s3_retry_count, config.s3_retry_delay_ms),
                        None => continue, // Skip if bucket not found
                    }
                }
                None => continue, // Skip if cell not found
            }
        };
        
        // Create clients for both source and destination buckets
        let source_client = create_s3_client(&source_bucket_config).await;
        let dest_client = create_s3_client(&cell_bucket_config).await;
        
        // Source: deployments/manifest/version/ (in manifest's bucket)
        let source_prefix_base = format!("deployments/{}/{}/", form.manifest, form.version);
        let source_prefix = source_bucket_config.full_key(&source_prefix_base);
        
        // Destination: cells/node_id/cell_id/versions/version/ (in cell's bucket)
        let dest_prefix_base = format!("cells/{}/{}/versions/{}/", node_id, cell_id, form.version);

        // If this version already exists for this cell, treat it as immutable:
        // do NOT rebuild/copy/merge overrides again.
        let version_exists = {
            let manifest_key_base = format!("{}manifest.json", dest_prefix_base);
            let manifest_key = cell_bucket_config.full_key(&manifest_key_base);
            let op = format!("head_cell_version_manifest: {}", manifest_key);
            retry_s3_operation(
                &op,
                retry_count,
                retry_delay_ms,
                || {
                    let client = dest_client.clone();
                    let bucket = cell_bucket_name.clone();
                    let key = manifest_key.clone();
                    async move { client.head_object().bucket(&bucket).key(&key).send().await }
                },
            )
            .await
            .is_ok()
        };

        // If the version already exists, publishing means "issue a trigger" only.
        if version_exists {
            let trigger_key_base = format!("cells/{}/{}/trigger.json", node_id, cell_id);
            let trigger_key = cell_bucket_config.full_key(&trigger_key_base);

            let trigger_data = serde_json::json!({ "version": form.version });
            let trigger_content = serde_json::to_string_pretty(&trigger_data)
                .unwrap_or_else(|_| format!("{{\"version\":\"{}\"}}", form.version));

            let trigger_op = format!("create_trigger: {}", trigger_key);
            let _ = retry_s3_operation(
                &trigger_op,
                retry_count,
                retry_delay_ms,
                || {
                    let client = dest_client.clone();
                    let bucket = cell_bucket_name.clone();
                    let key = trigger_key.clone();
                    let content = trigger_content.clone();
                    async move {
                        client
                            .put_object()
                            .bucket(&bucket)
                            .key(&key)
                            .body(ByteStream::from(content.into_bytes()))
                            .send()
                            .await
                    }
                }
            ).await;
            continue;
        }
        
        if !version_exists {
            // List all files in source deployment (from manifest's bucket)
            let list_op = format!("list_deployment_files: {}", source_prefix);
            let list_result = retry_s3_operation(
                &list_op,
                retry_count,
                retry_delay_ms,
                || {
                    let client = source_client.clone();
                    let bucket = source_bucket_name.clone();
                    let prefix = source_prefix.clone();
                    async move {
                        client
                            .list_objects_v2()
                            .bucket(&bucket)
                            .prefix(&prefix)
                            .send()
                            .await
                    }
                }
            ).await;
            
            if let Ok(output) = list_result {
                if let Some(contents) = output.contents {
                    for obj in contents {
                        if let Some(source_key) = obj.key {
                            // Get relative path within deployment
                            let relative_path = if let Some(stripped) = source_bucket_config.strip_prefix(&source_key) {
                                stripped.strip_prefix(&source_prefix_base).unwrap_or(&stripped)
                            } else {
                                source_key.strip_prefix(&source_prefix).unwrap_or(&source_key)
                            };
                            
                            // Destination key (in cell's bucket)
                            let dest_key_base = format!("{}{}", dest_prefix_base, relative_path);
                            let dest_key = cell_bucket_config.full_key(&dest_key_base);
                            
                            // Copy object from source bucket to destination bucket
                            let copy_source = format!("{}/{}", source_bucket_name, source_key);
                            let copy_op = format!("copy_to_cell: {} -> {}/{}", copy_source, cell_bucket_name, dest_key);
                            let _ = retry_s3_operation(
                                &copy_op,
                                retry_count,
                                retry_delay_ms,
                                || {
                                    let client = dest_client.clone();
                                    let bucket = cell_bucket_name.clone();
                                    let dest = dest_key.clone();
                                    let src = copy_source.clone();
                                    async move {
                                        client
                                            .copy_object()
                                            .bucket(&bucket)
                                            .key(&dest)
                                            .copy_source(&src)
                                            .send()
                                            .await
                                    }
                                }
                            ).await;
                        }
                    }
                }
            }
            
            // === Snapshot + merge overrides into this version folder (cell bucket) ===
            // Copy overrides/ files to versions/<ver>/ (overriding base deployment files),
            // and snapshot overrides_manifest.json into versions/<ver>/overrides_manifest.json.
            let overrides_prefix_base = format!("cells/{}/{}/overrides/", node_id, cell_id);
            let overrides_prefix = cell_bucket_config.full_key(&overrides_prefix_base);
            let overrides_list_op = format!("list_cell_overrides: {}", overrides_prefix);
            let overrides_list_result = retry_s3_operation(
                &overrides_list_op,
                retry_count,
                retry_delay_ms,
                || {
                    let client = dest_client.clone();
                    let bucket = cell_bucket_name.clone();
                    let prefix = overrides_prefix.clone();
                    async move {
                        client
                            .list_objects_v2()
                            .bucket(&bucket)
                            .prefix(&prefix)
                            .send()
                            .await
                    }
                },
            )
            .await;
            
            if let Ok(output) = overrides_list_result {
                if let Some(contents) = output.contents {
                    for obj in contents {
                        if let Some(source_key) = obj.key {
                            if source_key.ends_with('/') {
                                continue;
                            }
                            
                            // relative path under overrides/
                            let relative_path = if let Some(stripped) = cell_bucket_config.strip_prefix(&source_key) {
                                stripped.strip_prefix(&overrides_prefix_base).unwrap_or(&stripped)
                            } else {
                                source_key.strip_prefix(&overrides_prefix).unwrap_or(&source_key)
                            };
                            
                            // merge into versions/<ver>/<relative_path>
                            let dest_key_base = format!("{}{}", dest_prefix_base, relative_path);
                            let dest_key = cell_bucket_config.full_key(&dest_key_base);
                            
                            let copy_source = format!("{}/{}", cell_bucket_name, source_key);
                            let copy_op = format!("copy_override_to_version: {} -> {}/{}", copy_source, cell_bucket_name, dest_key);
                            let _ = retry_s3_operation(
                                &copy_op,
                                retry_count,
                                retry_delay_ms,
                                || {
                                    let client = dest_client.clone();
                                    let bucket = cell_bucket_name.clone();
                                    let dest = dest_key.clone();
                                    let src = copy_source.clone();
                                    async move {
                                        client
                                            .copy_object()
                                            .bucket(&bucket)
                                            .key(&dest)
                                            .copy_source(&src)
                                            .send()
                                            .await
                                    }
                                },
                            )
                            .await;
                        }
                    }
                }
            }
            
            // Snapshot overrides manifest to versions/<ver>/overrides_manifest.json (repeatable):
            // store fully resolved owner/group/mode so the version doesn't depend on agent default rules.
            let overrides_manifest_src_base =
                format!("cells/{}/{}/overrides_manifest.json", node_id, cell_id);
            let overrides_manifest_src =
                cell_bucket_config.full_key(&overrides_manifest_src_base);
            let overrides_manifest_dest_base = format!("{}overrides_manifest.json", dest_prefix_base);
            let overrides_manifest_dest =
                cell_bucket_config.full_key(&overrides_manifest_dest_base);

            let get_op = format!("get_overrides_manifest_for_snapshot: {}", overrides_manifest_src);
            if let Ok(obj) = retry_s3_operation(
                &get_op,
                retry_count,
                retry_delay_ms,
                || {
                    let client = dest_client.clone();
                    let bucket = cell_bucket_name.clone();
                    let key = overrides_manifest_src.clone();
                    async move { client.get_object().bucket(&bucket).key(&key).send().await }
                },
            )
            .await
            {
                if let Ok(data) = obj.body.collect().await {
                    if let Ok(live) =
                        serde_json::from_slice::<CellOverridesManifest>(&data.to_vec())
                    {
                        let mut files: Vec<ResolvedCellOverrideFile> = Vec::new();
                        for f in live.files {
                            let owner = f
                                .owner
                                .as_deref()
                                .map(|s| s.trim())
                                .filter(|s| !s.is_empty())
                                .unwrap_or(service_owner.trim())
                                .to_string();
                            let group = f
                                .group
                                .as_deref()
                                .map(|s| s.trim())
                                .filter(|s| !s.is_empty())
                                .unwrap_or(service_group.trim())
                                .to_string();
                            let mode = f
                                .mode
                                .as_deref()
                                .map(|s| s.trim())
                                .filter(|s| !s.is_empty())
                                .unwrap_or(default_override_mode(&f.path, &f.file_type))
                                .to_string();
                            files.push(ResolvedCellOverrideFile {
                                path: f.path,
                                file_type: f.file_type,
                                owner,
                                group,
                                mode,
                            });
                        }
                        let resolved = ResolvedCellOverridesManifest { files };
                        if let Ok(content) = serde_json::to_string_pretty(&resolved) {
                            let put_op =
                                format!("put_overrides_manifest_snapshot: {}", overrides_manifest_dest);
                            let _ = retry_s3_operation(
                                &put_op,
                                retry_count,
                                retry_delay_ms,
                                || {
                                    let client = dest_client.clone();
                                    let bucket = cell_bucket_name.clone();
                                    let key = overrides_manifest_dest.clone();
                                    let body = content.clone();
                                    async move {
                                        client
                                            .put_object()
                                            .bucket(&bucket)
                                            .key(&key)
                                            .body(ByteStream::from(body.into_bytes()))
                                            .send()
                                            .await
                                    }
                                },
                            )
                            .await;
                        }
                    }
                }
            }
        }
        
        // Create metadata file (JSON format) in cell's bucket
        let metadata_data = serde_json::json!({
            "manifest_name": &form.manifest,
            "profile": &profile
        });
        let metadata_content = serde_json::to_string_pretty(&metadata_data)
            .unwrap_or_else(|_| format!("{{\"form.manifest\":\"{}\",\"profile\":\"{}\"}}", form.manifest, profile));
        
        let metadata_key_base = format!("cells/{}/{}/metadata.json", node_id, cell_id);
        let metadata_key = cell_bucket_config.full_key(&metadata_key_base);
        
        let metadata_op = format!("create_metadata: {}", metadata_key);
        let _ = retry_s3_operation(
            &metadata_op,
            retry_count,
            retry_delay_ms,
            || {
                let client = dest_client.clone();
                let bucket = cell_bucket_name.clone();
                let key = metadata_key.clone();
                let content = metadata_content.clone();
                async move {
                    client
                        .put_object()
                        .bucket(&bucket)
                        .key(&key)
                        .body(ByteStream::from(content.into_bytes()))
                        .send()
                        .await
                }
            }
        ).await;
        
        // Create trigger file (JSON format) in cell's bucket.
        // Publishing an existing version should "issue a trigger" (touch the trigger) as well.
        let trigger_key_base = format!("cells/{}/{}/trigger.json", node_id, cell_id);
        let trigger_key = cell_bucket_config.full_key(&trigger_key_base);
        let trigger_data = serde_json::json!({ "version": form.version });
        let trigger_content = serde_json::to_string_pretty(&trigger_data)
            .unwrap_or_else(|_| format!("{{\"version\":\"{}\"}}", form.version));

        let trigger_op = format!("create_trigger: {}", trigger_key);
        let _ = retry_s3_operation(
            &trigger_op,
            retry_count,
            retry_delay_ms,
            || {
                let client = dest_client.clone();
                let bucket = cell_bucket_name.clone();
                let key = trigger_key.clone();
                let content = trigger_content.clone();
                async move {
                    client
                        .put_object()
                        .bucket(&bucket)
                        .key(&key)
                        .body(ByteStream::from(content.into_bytes()))
                        .send()
                        .await
                }
            }
        ).await;
    }
    
    Redirect::to(&format!("/deployments/start?manifest={}&version={}&edit=true", 
                         form.manifest, form.version)).into_response()
}

async fn delete_version(
    AxumState(state): AxumState<AppState>,
    Form(form): Form<DeleteVersionForm>,
) -> impl IntoResponse {
    // Clone config data before await
    let (bucket_name, bucket_config, retry_count, retry_delay_ms) = {
        let config = state.read().unwrap();
        match config.manifests.get(&form.manifest) {
            Some(manifest) => {
                match config.buckets.get(&manifest.bucket) {
                    Some(bc) => (bc.bucket_name.clone(), bc.clone(),
                                config.s3_retry_count, config.s3_retry_delay_ms),
                    None => return Redirect::to("/deployments").into_response(),
                }
            }
            None => return Redirect::to("/deployments").into_response(),
        }
    };
    
    let client = create_s3_client(&bucket_config).await;
    
    // Generate timestamp for recycle folder
    let now = Local::now();
    let timestamp = now.format("%Y%m%d-%H%M%S-%3f").to_string();
    
    // Source and target prefixes (without bucket_config.full_key yet)
    let source_prefix_base = format!("deployments/{}/{}/", form.manifest, form.version);
    let recycle_prefix_base = format!("recycle/{}-{}-{}/", form.manifest, form.version, timestamp);
    
    // Apply bucket prefix
    let source_prefix = bucket_config.full_key(&source_prefix_base);
    let recycle_prefix = bucket_config.full_key(&recycle_prefix_base);
    
    println!("Moving version {} from {} to {}", form.version, source_prefix, recycle_prefix);
    
    // List all files in the version
    let list_op_name = format!("list_for_delete: {}", source_prefix);
    let result = match retry_s3_operation(
        &list_op_name,
        retry_count,
        retry_delay_ms,
        || {
            let client = client.clone();
            let bucket = bucket_name.clone();
            let prefix = source_prefix.clone();
            async move {
                client
                    .list_objects_v2()
                    .bucket(&bucket)
                    .prefix(&prefix)
                    .send()
                    .await
            }
        }
    ).await {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Failed to list files for deletion: {:?}", e);
            return Redirect::to("/deployments").into_response();
        }
    };
    
    let contents = result.contents();
    let file_count = contents.len();
    
    println!("Found {} files to move to recycle", file_count);
    
    // Move each file to recycle folder
    for object in contents {
        if let Some(key) = object.key() {
            // Calculate new key in recycle folder
            let relative_path = key.strip_prefix(&source_prefix).unwrap_or(key);
            let new_key = format!("{}{}", recycle_prefix, relative_path);
            let copy_source = format!("{}/{}", bucket_name, key);
            
            println!("Moving {} -> {}", key, new_key);
            
            // Copy to recycle folder
            let copy_op_name = format!("recycle_copy: {} -> {}", key, new_key);
            if let Err(e) = retry_s3_operation(
                &copy_op_name,
                retry_count,
                retry_delay_ms,
                || {
                    let client = client.clone();
                    let bucket = bucket_name.clone();
                    let copy_source = copy_source.clone();
                    let new_key = new_key.clone();
                    async move {
                        client
                            .copy_object()
                            .bucket(&bucket)
                            .copy_source(&copy_source)
                            .key(&new_key)
                            .send()
                            .await
                    }
                }
            ).await {
                eprintln!("Failed to copy {} to recycle: {:?}", key, e);
                continue;
            }
            
            // Delete original
            let delete_op_name = format!("recycle_delete: {}", key);
            if let Err(e) = retry_s3_operation(
                &delete_op_name,
                retry_count,
                retry_delay_ms,
                || {
                    let client = client.clone();
                    let bucket = bucket_name.clone();
                    let key = key.to_string();
                    async move {
                        client
                            .delete_object()
                            .bucket(&bucket)
                            .key(&key)
                            .send()
                            .await
                    }
                }
            ).await {
                eprintln!("Failed to delete original {}: {:?}", key, e);
            }
        }
    }
    
    println!("✓ Successfully moved {} files from {} to recycle folder", file_count, form.version);
    
    Redirect::to("/deployments").into_response()
}

async fn find_all_versions(client: &Client, bucket: &str, manifest_name: &str, bucket_config: &BucketConfig, retry_count: u32, retry_delay_ms: u64) -> Vec<String> {
    let base_prefix = format!("deployments/{}/", manifest_name);
    let prefix = bucket_config.full_key(&base_prefix);
    
    let operation_name = format!("list_versions: {}", prefix);
    let result = match retry_s3_operation(
        &operation_name,
        retry_count,
        retry_delay_ms,
        || {
            let client = client.clone();
            let bucket = bucket.to_string();
            let prefix = prefix.clone();
            async move {
                client
                    .list_objects_v2()
                    .bucket(&bucket)
                    .prefix(&prefix)
                    .delimiter("/")
                    .send()
                    .await
            }
        }
    ).await
    {
        Ok(r) => r,
        Err(_) => return Vec::new(),
    };
    
    let mut versions = Vec::new();
    let common_prefixes = result.common_prefixes();
    for common_prefix in common_prefixes {
        if let Some(p) = common_prefix.prefix() {
            // Strip the full prefix to get just the version part
            let relative = p.strip_prefix(&prefix).unwrap_or(p);
            let parts: Vec<&str> = relative.trim_end_matches('/').split('/').collect();
            if let Some(version) = parts.first() {
                if !version.is_empty() {
                    versions.push(version.to_string());
                }
            }
        }
    }
    
    // Sort versions semantically (v1.0.33 > v1.0.4)
    versions.sort_by(|a, b| {
        // Parse version strings like "v1.2.3" into (major, minor, patch)
        let parse_version = |v: &str| -> (u32, u32, u32) {
            let stripped = v.strip_prefix('v').unwrap_or(v);
            let parts: Vec<&str> = stripped.split('.').collect();
            let major = parts.get(0).and_then(|s| s.parse::<u32>().ok()).unwrap_or(0);
            let minor = parts.get(1).and_then(|s| s.parse::<u32>().ok()).unwrap_or(0);
            let patch = parts.get(2).and_then(|s| s.parse::<u32>().ok()).unwrap_or(0);
            (major, minor, patch)
        };
        
        let (a_major, a_minor, a_patch) = parse_version(a);
        let (b_major, b_minor, b_patch) = parse_version(b);
        
        // Compare major, then minor, then patch
        match b_major.cmp(&a_major) {
            std::cmp::Ordering::Equal => match b_minor.cmp(&a_minor) {
                std::cmp::Ordering::Equal => b_patch.cmp(&a_patch),
                other => other,
            },
            other => other,
        }
    });
    
    versions
}


async fn is_deployment_finalized(client: &Client, bucket: &str, manifest_name: &str, version: &str, bucket_config: &BucketConfig, retry_count: u32, retry_delay_ms: u64) -> bool {
    // Check if manifest.json exists for this deployment
    let manifest_key_base = format!("deployments/{}/{}/manifest.json", manifest_name, version);
    let manifest_key = bucket_config.full_key(&manifest_key_base);
    
    let operation_name = format!("check_manifest_exists: {}", manifest_key);
    let result = retry_s3_operation(
        &operation_name,
        retry_count,
        retry_delay_ms,
        || {
            let client = client.clone();
            let bucket = bucket.to_string();
            let key = manifest_key.clone();
            async move {
                client
                    .head_object()
                    .bucket(&bucket)
                    .key(&key)
                    .send()
                    .await
            }
        }
    ).await;
    
    result.is_ok()
}

async fn load_manifest_snapshot(
    client: &Client,
    bucket: &str,
    manifest_name: &str,
    version: &str,
    bucket_config: &BucketConfig,
    retry_count: u32,
    retry_delay_ms: u64,
) -> Option<controller_config::ManifestSnapshot> {
    let manifest_key_base = format!("deployments/{}/{}/manifest.json", manifest_name, version);
    let manifest_key = bucket_config.full_key(&manifest_key_base);
    
    let operation_name = format!("load_manifest_snapshot: {}", manifest_key);
    let result = retry_s3_operation(
        &operation_name,
        retry_count,
        retry_delay_ms,
        || {
            let client = client.clone();
            let bucket = bucket.to_string();
            let key = manifest_key.clone();
            async move {
                client
                    .get_object()
                    .bucket(&bucket)
                    .key(&key)
                    .send()
                    .await
            }
        }
    ).await;
    
    match result {
        Ok(output) => {
            match output.body.collect().await {
                Ok(data) => {
                    let content = String::from_utf8_lossy(&data.into_bytes()).to_string();
                    serde_json::from_str::<controller_config::ManifestSnapshot>(&content).ok()
                }
                Err(_) => None,
            }
        }
        Err(_) => None,
    }
}

async fn fetch_all_deployments(
    state: &AppState,
    manifest_list: &[(String, DeploymentManifest)],
) -> Vec<ExistingDeployment> {
    // Parallelize deployment fetching for all manifests
    let deployment_futures: Vec<_> = manifest_list.iter().map(|(manifest_name, manifest)| {
        let manifest_name = manifest_name.clone();
        let manifest = manifest.clone();
        let state_clone = state.clone();
        
        async move {
            let (bucket_config, retry_count, retry_delay_ms) = {
                let config = state_clone.read().unwrap();
                match config.buckets.get(&manifest.bucket) {
                    Some(bc) => (bc.clone(), config.s3_retry_count, config.s3_retry_delay_ms),
                    None => return None,
                }
            };
            
            let client = create_s3_client(&bucket_config).await;
            let version_strings = find_all_versions(&client, &bucket_config.bucket_name, &manifest_name, &bucket_config, retry_count, retry_delay_ms).await;
            
            // Check finalization status for each version IN PARALLEL
            let version_futures: Vec<_> = version_strings.into_iter().map(|version_str| {
                let client = client.clone();
                let bucket = bucket_config.bucket_name.clone();
                let manifest_name_clone = manifest_name.clone();
                let bucket_config = bucket_config.clone();
                
                async move {
                    let is_finalized = is_deployment_finalized(
                        &client,
                        &bucket,
                        &manifest_name_clone,
                        &version_str,
                        &bucket_config,
                        retry_count,
                        retry_delay_ms
                    ).await;
                    
                    VersionInfo {
                        version: version_str,
                        is_finalized,
                    }
                }
            }).collect();
            
            let versions = join_all(version_futures).await;
            
            Some(ExistingDeployment {
                manifest_name: manifest_name.clone(),
                versions,
            })
        }
    }).collect();
    
    let results = join_all(deployment_futures).await;
    results.into_iter().filter_map(|x| x).collect()
}

async fn clone_version(
    client: &Client, 
    bucket: &str, 
    source_prefix: &str, 
    target_prefix: &str, 
    bucket_config: &BucketConfig, 
    retry_count: u32, 
    retry_delay_ms: u64,
) -> Result<(), String> {
    let full_source_prefix = bucket_config.full_key(source_prefix);
    let full_target_prefix = bucket_config.full_key(target_prefix);
    
    // Clone all files from source (excluding manifest.json which will be created on finish)
    let list_op_name = format!("list_for_clone: {}", full_source_prefix);
    let result = retry_s3_operation(
        &list_op_name,
        retry_count,
        retry_delay_ms,
        || {
            let client = client.clone();
            let bucket = bucket.to_string();
            let prefix = full_source_prefix.clone();
            async move {
                client
                    .list_objects_v2()
                    .bucket(&bucket)
                    .prefix(&prefix)
                    .send()
                    .await
            }
        }
    ).await.map_err(|e| format!("{:?}", e))?;
    
    let contents = result.contents();
    for object in contents {
        if let Some(key) = object.key() {
            // Skip manifest.json from source (will be created on finish)
            if key.ends_with("/manifest.json") {
                println!("Skipping manifest.json from source: {}", key);
                continue;
            }
            
            let new_key = key.replace(&full_source_prefix, &full_target_prefix);
            let copy_source = format!("{}/{}", bucket, key);
            
            let copy_op_name = format!("copy_object: {} -> {}", key, new_key);
            retry_s3_operation(
                &copy_op_name,
                retry_count,
                retry_delay_ms,
                || {
                    let client = client.clone();
                    let bucket = bucket.to_string();
                    let copy_source = copy_source.clone();
                    let new_key = new_key.clone();
                    async move {
                        client
                            .copy_object()
                            .bucket(&bucket)
                            .copy_source(&copy_source)
                            .key(&new_key)
                            .send()
                            .await
                    }
                }
            ).await.map_err(|e| format!("{:?}", e))?;
        }
    }
    
    Ok(())
}

// create_initial_deployment removed - initial deployments start empty
// manifest.json is created only when user finishes the deployment

// ========== Cell Management Handlers ==========

#[derive(Template)]
#[template(path = "cells.html")]
struct CellsTemplate {
    cells: Vec<CellDisplayInfo>,
}

#[derive(Template)]
#[template(path = "add_cell.html")]
struct AddCellTemplate {
    buckets: Vec<String>,
    manifests: Vec<(String, DeploymentManifest)>,
}

#[derive(Clone, Debug)]
struct CellDisplayInfo {
    cell_key: String,
    cell: CellConfig,
    status: CellStatus,
    manifest_name: String,  // The manifest key, used for lookups
    manifest_bucket: Option<String>,
}

#[derive(Clone, Debug)]
struct CellStatus {
    desired_version: Option<String>,
    desired_timestamp: Option<u64>,
    running_version: Option<String>,
    applied_at: Option<u64>,
    status: String,  // "success", "failed", "unknown"
    error: Option<String>,
    agent_stale: bool,  // true if trigger is >2 min newer than status
}

#[derive(Deserialize)]
struct AddCellForm {
    node_id: String,
    cell_id: String,
    bucket: String,
    manifest: String,
}

#[derive(Deserialize)]
struct DeleteCellForm {
    cell_key: String,
}

async fn cells_page(AxumState(state): AxumState<AppState>) -> impl IntoResponse {
    let (cells_list, manifests, retry_count, retry_delay_ms) = {
        let config = state.read().unwrap();
        let cells: Vec<_> = config.cells.iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        
        // Pass full manifests HashMap for lookup
        let manifests = config.manifests.clone();
        
        (cells, manifests, config.s3_retry_count, config.s3_retry_delay_ms)
    };
    
    // Fetch status for each cell IN PARALLEL
    let status_futures: Vec<_> = cells_list.into_iter().map(|(key, cell)| {
        let state_clone = state.clone();
        async move {
            let status = fetch_cell_status(&state_clone, &cell, retry_count, retry_delay_ms).await;
            (key, cell, status)
        }
    }).collect();
    
    let cells_with_status = join_all(status_futures).await;
    
    // Enhance cells with manifest info and sort
    let mut cell_display_infos: Vec<CellDisplayInfo> = cells_with_status.into_iter().map(|(cell_key, cell, status)| {
        let manifest_bucket = manifests.get(&cell.manifest)
            .map(|m| m.bucket.clone());
        
        CellDisplayInfo {
            cell_key,
            manifest_name: cell.manifest.clone(),
            cell,
            status,
            manifest_bucket,
        }
    }).collect();
    
    // Sort cells by cell_key (ascending order)
    cell_display_infos.sort_by(|a, b| a.cell_key.cmp(&b.cell_key));
    
    let template = CellsTemplate { cells: cell_display_infos };
    HtmlTemplate(template)
}

async fn fetch_cell_status(state: &AppState, cell: &CellConfig, retry_count: u32, retry_delay_ms: u64) -> CellStatus {
    let bucket_config = {
        let config = state.read().unwrap();
        config.buckets.get(&cell.bucket).cloned()
    };
    
    let bucket_config = match bucket_config {
        Some(bc) => bc,
        None => return CellStatus {
            desired_version: None,
            desired_timestamp: None,
            running_version: None,
            applied_at: None,
            status: "unknown".to_string(),
            error: Some("Bucket not configured".to_string()),
            agent_stale: false,
        },
    };
    
    let client = create_s3_client(&bucket_config).await;
    let bucket_name = bucket_config.bucket_name.clone();
    
    // Fetch trigger.json
    let trigger_key_base = format!("cells/{}/{}/trigger.json", cell.node_id, cell.cell_id);
    let trigger_key = bucket_config.full_key(&trigger_key_base);
    
    let (desired_version, desired_timestamp) = {
        let operation_name = format!("read_trigger: {}", trigger_key);
        match retry_s3_operation(
            &operation_name,
            retry_count,
            retry_delay_ms,
            || {
                let client = client.clone();
                let bucket = bucket_name.clone();
                let key = trigger_key.clone();
                async move {
                    client.get_object().bucket(&bucket).key(&key).send().await
                }
            }
        ).await {
            Ok(output) => {
                let last_modified = output.last_modified()
                    .map(|dt| dt.secs() as u64)
                    .unwrap_or(0);
                
                match output.body.collect().await {
                    Ok(data) => {
                        let body = data.into_bytes();
                        match serde_json::from_slice::<serde_json::Value>(&body) {
                            Ok(json) => {
                                let version = json.get("version")
                                    .and_then(|v| v.as_str())
                                    .map(|s| s.to_string());
                                (version, Some(last_modified))
                            }
                            Err(_) => (None, Some(last_modified))
                        }
                    }
                    Err(_) => (None, Some(last_modified))
                }
            }
            Err(_) => (None, None)
        }
    };
    
    // Fetch status.json
    let status_key_base = format!("cells/{}/{}/status.json", cell.node_id, cell.cell_id);
    let status_key = bucket_config.full_key(&status_key_base);
    
    let (running_version, applied_at, status_str, error) = {
        let operation_name = format!("read_status: {}", status_key);
        match retry_s3_operation(
            &operation_name,
            retry_count,
            retry_delay_ms,
            || {
                let client = client.clone();
                let bucket = bucket_name.clone();
                let key = status_key.clone();
                async move {
                    client.get_object().bucket(&bucket).key(&key).send().await
                }
            }
        ).await {
            Ok(output) => {
                match output.body.collect().await {
                    Ok(data) => {
                        let body = data.into_bytes();
                        match serde_json::from_slice::<serde_json::Value>(&body) {
                            Ok(json) => {
                                let running_version = json.get("running_version")
                                    .and_then(|v| v.as_str())
                                    .map(|s| s.to_string());
                                let applied_at = json.get("applied_at")
                                    .and_then(|v| v.as_u64());
                                let status = json.get("status")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("unknown")
                                    .to_string();
                                let error = json.get("error")
                                    .and_then(|v| v.as_str())
                                    .map(|s| s.to_string());
                                (running_version, applied_at, status, error)
                            }
                            Err(_) => (None, None, "unknown".to_string(), Some("Invalid status.json format".to_string()))
                        }
                    }
                    Err(_) => (None, None, "unknown".to_string(), Some("Failed to read status.json".to_string()))
                }
            }
            Err(_) => (None, None, "unknown".to_string(), None)
        }
    };
    
    // Check if agent is stale:
    // 1. Status is older than trigger (or doesn't exist)
    // 2. Trigger has been there for >2 minutes (now - trigger_time > 120)
    let current_time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    
    let agent_stale = match (desired_timestamp, applied_at) {
        (Some(trigger_ts), Some(status_ts)) => {
            // Status is older than trigger AND trigger is >2 min old
            status_ts < trigger_ts && current_time > trigger_ts + 120
        }
        (Some(trigger_ts), None) => {
            // Trigger exists but no status, AND trigger is >2 min old
            current_time > trigger_ts + 120
        }
        _ => false,
    };
    
    // If status is outdated (or missing) and within 2-minute window, show "pending"
    let (final_status, final_error) = match (desired_timestamp, applied_at) {
        (Some(trigger_ts), Some(status_ts)) if status_ts < trigger_ts => {
            // Status is older than trigger
            if agent_stale {
                // Already >2 min, show actual status
                (status_str, error)
            } else {
                // Within 2 min window, show pending
                let age = current_time.saturating_sub(trigger_ts);
                ("pending".to_string(), Some(format!("Deployment in progress ({}s ago)", age)))
            }
        }
        (Some(trigger_ts), None) => {
            // No status exists yet
            if agent_stale {
                // Already >2 min, show unknown
                ("unknown".to_string(), Some("No status reported yet".to_string()))
            } else {
                // Within 2 min window, show pending
                let age = current_time.saturating_sub(trigger_ts);
                ("pending".to_string(), Some(format!("Deployment in progress ({}s ago)", age)))
            }
        }
        _ => {
            // Status is up to date, show actual status
            (status_str, error)
        }
    };
    
    CellStatus {
        desired_version,
        desired_timestamp,
        running_version,
        applied_at,
        status: final_status,
        error: final_error,
        agent_stale,
    }
}

async fn add_cell_page(AxumState(state): AxumState<AppState>) -> impl IntoResponse {
    let (buckets, manifests) = {
        let config = state.read().unwrap();
        let buckets: Vec<String> = config.buckets.keys().cloned().collect();
        let manifests: Vec<(String, DeploymentManifest)> = config.manifests.iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        (buckets, manifests)
    };
    
    let template = AddCellTemplate {
        buckets,
        manifests,
    };
    
    HtmlTemplate(template)
}

async fn add_cell(
    AxumState(state): AxumState<AppState>,
    Form(form): Form<AddCellForm>,
) -> impl IntoResponse {
    let mut config = state.write().unwrap();
    
    let cell = CellConfig {
        node_id: form.node_id.clone(),
        cell_id: form.cell_id.clone(),
        bucket: form.bucket,
        manifest: form.manifest,
    };
    
    let cell_key = cell.full_name();
    config.cells.insert(cell_key, cell);
    
    if let Err(e) = save_config(&config) {
        eprintln!("Failed to save config: {}", e);
    }
    
    Redirect::to("/cells").into_response()
}

async fn delete_cell(
    AxumState(state): AxumState<AppState>,
    Form(form): Form<DeleteCellForm>,
) -> impl IntoResponse {
    // Parse cell_key: "<node_id>/<cell_id>"
    let parts: Vec<&str> = form.cell_key.split('/').collect();
    if parts.len() != 2 {
        return Html("<div class=\"container mt-4\"><div class=\"alert alert-danger\">Invalid cell key</div><a href=\"/cells\">Back</a></div>".to_string()).into_response();
    }
    let node_id = parts[0].to_string();
    let cell_id = parts[1].to_string();

    // Snapshot cell + bucket config before awaiting.
    let (bucket_name, bucket_cfg, retry_count, retry_delay_ms) = {
        let cfg = state.read().unwrap();
        let cell = match cfg.cells.get(&form.cell_key) {
            Some(c) => c.clone(),
            None => return Redirect::to("/cells").into_response(),
        };
        let bc = match cfg.buckets.get(&cell.bucket) {
            Some(bc) => bc.clone(),
            None => {
                return Html("<div class=\"container mt-4\"><div class=\"alert alert-danger\">Bucket not configured for this cell</div><a href=\"/cells\">Back</a></div>".to_string()).into_response();
            }
        };
        (bc.bucket_name.clone(), bc, cfg.s3_retry_count, cfg.s3_retry_delay_ms)
    };

    // Move S3 folder: <root>/cells/<node>/<cell>/ -> <root>/cell_recycle/<node>/<cell>/<yyyyMMdd-HHmmss-SSS>/
    let client = create_s3_client(&bucket_cfg).await;
    let ts = chrono::Local::now().format("%Y%m%d-%H%M%S-%3f").to_string();

    let src_prefix_base = format!("cells/{}/{}/", node_id, cell_id);
    let dst_prefix_base = format!("cell_recycle/{}/{}/{}/", node_id, cell_id, ts);
    let src_prefix = bucket_cfg.full_key(&src_prefix_base);
    let dst_prefix = bucket_cfg.full_key(&dst_prefix_base);

    // Phase 1: paginate + collect all source keys
    let mut continuation: Option<String> = None;
    let mut src_keys: Vec<String> = Vec::new();
    loop {
        let op = format!("cell_delete:list:{}:{}", bucket_name, src_prefix);
        let res = retry_s3_operation(
            &op,
            retry_count,
            retry_delay_ms,
            || {
                let client = client.clone();
                let bucket = bucket_name.clone();
                let prefix = src_prefix.clone();
                let token = continuation.clone();
                async move {
                    let mut req = client.list_objects_v2().bucket(&bucket).prefix(&prefix);
                    if let Some(t) = token {
                        req = req.continuation_token(t);
                    }
                    req.send().await
                }
            },
        )
        .await;

        let output = match res {
            Ok(o) => o,
            Err(e) => {
                return Html(format!(
                    "<div class=\"container mt-4\"><div class=\"alert alert-danger\">Failed to list cell objects for deletion: {:?}</div><a href=\"/cells\">Back</a></div>",
                    e
                ))
                .into_response();
            }
        };

        for o in output.contents.unwrap_or_default() {
            if let Some(k) = o.key {
                src_keys.push(k);
            }
        }

        if output.is_truncated.unwrap_or(false) {
            continuation = output.next_continuation_token;
        } else {
            break;
        }
    }

    // Phase 2: copy everything (paginated already done), and verify each copy via head_object.
    for key in &src_keys {
        let rel = key.strip_prefix(&src_prefix).unwrap_or(key);
        let new_key = format!("{}{}", dst_prefix, rel);
        let copy_source = format!("{}/{}", bucket_name, key);

        let op = format!("cell_delete:copy:{}->{}", key, new_key);
        if let Err(e) = retry_s3_operation(
            &op,
            retry_count,
            retry_delay_ms,
            || {
                let client = client.clone();
                let bucket = bucket_name.clone();
                let copy_source = copy_source.clone();
                let new_key = new_key.clone();
                async move {
                    client
                        .copy_object()
                        .bucket(&bucket)
                        .copy_source(copy_source)
                        .key(new_key)
                        .send()
                        .await
                }
            },
        )
        .await
        {
            return Html(format!(
                "<div class=\"container mt-4\"><div class=\"alert alert-danger\">Failed to archive cell object: {:?}</div><a href=\"/cells\">Back</a></div>",
                e
            ))
            .into_response();
        }

        // Verify copy exists before deleting originals.
        let op = format!("cell_delete:verify_copy:{}", new_key);
        if let Err(e) = retry_s3_operation(
            &op,
            retry_count,
            retry_delay_ms,
            || {
                let client = client.clone();
                let bucket = bucket_name.clone();
                let k = new_key.clone();
                async move { client.head_object().bucket(&bucket).key(&k).send().await }
            },
        )
        .await
        {
            return Html(format!(
                "<div class=\"container mt-4\"><div class=\"alert alert-danger\">Archive verification failed (copied object missing): {:?}</div><a href=\"/cells\">Back</a></div>",
                e
            ))
            .into_response();
        }
    }

    // Phase 2b: verify destination keyset matches exactly (same relative paths).
    let mut dst_rel: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut dst_cont: Option<String> = None;
    loop {
        let op = format!("cell_delete:verify_list_dst:{}:{}", bucket_name, dst_prefix);
        let res = retry_s3_operation(
            &op,
            retry_count,
            retry_delay_ms,
            || {
                let client = client.clone();
                let bucket = bucket_name.clone();
                let prefix = dst_prefix.clone();
                let token = dst_cont.clone();
                async move {
                    let mut req = client.list_objects_v2().bucket(&bucket).prefix(&prefix);
                    if let Some(t) = token {
                        req = req.continuation_token(t);
                    }
                    req.send().await
                }
            },
        )
        .await;
        let out = match res {
            Ok(o) => o,
            Err(e) => {
                return Html(format!(
                    "<div class=\"container mt-4\"><div class=\"alert alert-danger\">Archive verification failed (list dst): {:?}</div><a href=\"/cells\">Back</a></div>",
                    e
                ))
                .into_response();
            }
        };
        for o in out.contents.unwrap_or_default() {
            if let Some(k) = o.key {
                let rel = k.strip_prefix(&dst_prefix).unwrap_or(&k).to_string();
                dst_rel.insert(rel);
            }
        }
        if out.is_truncated.unwrap_or(false) {
            dst_cont = out.next_continuation_token;
        } else {
            break;
        }
    }
    let mut src_rel: std::collections::HashSet<String> = std::collections::HashSet::new();
    for k in &src_keys {
        let rel = k.strip_prefix(&src_prefix).unwrap_or(k).to_string();
        src_rel.insert(rel);
    }
    if dst_rel != src_rel {
        return Html(
            "<div class=\"container mt-4\"><div class=\"alert alert-danger\">Archive verification failed (destination does not match source). No originals were deleted.</div><a href=\"/cells\">Back</a></div>"
                .to_string(),
        )
        .into_response();
    }

    // Phase 3: delete originals only after verification succeeds.
    for key in &src_keys {
        let op = format!("cell_delete:delete:{}", key);
        if let Err(e) = retry_s3_operation(
            &op,
            retry_count,
            retry_delay_ms,
            || {
                let client = client.clone();
                let bucket = bucket_name.clone();
                let key = key.clone();
                async move { client.delete_object().bucket(&bucket).key(&key).send().await }
            },
        )
        .await
        {
            return Html(format!(
                "<div class=\"container mt-4\"><div class=\"alert alert-danger\">Archive completed but failed to delete some original objects: {:?}</div><a href=\"/cells\">Back</a></div>",
                e
            ))
            .into_response();
        }
    }

    // Remove cell from controller config only after archive succeeded.
    {
        let mut cfg = state.write().unwrap();
        cfg.cells.remove(&form.cell_key);
        save_config(&cfg).ok();
    }

    // Best-effort: show success message via redirect.
    Redirect::to("/cells").into_response()
}

async fn view_cell_status(
    AxumState(state): AxumState<AppState>,
    AxumPath((node_id, cell_id)): AxumPath<(String, String)>,
) -> impl IntoResponse {
    let (bucket_name, bucket_config, retry_count, retry_delay_ms) = {
        let config = state.read().unwrap();
        let cell_key = format!("{}/{}", node_id, cell_id);
        match config.cells.get(&cell_key) {
            Some(cell) => {
                match config.buckets.get(&cell.bucket) {
                    Some(bc) => (bc.bucket_name.clone(), bc.clone(),
                                config.s3_retry_count, config.s3_retry_delay_ms),
                    None => return Html("<h1>Bucket not found</h1>".to_string()).into_response(),
                }
            }
            None => return Html("<h1>Cell not found</h1>".to_string()).into_response(),
        }
    };
    
    let client = create_s3_client(&bucket_config).await;
    
    let status_key_base = format!("cells/{}/{}/status.json", node_id, cell_id);
    let status_key = bucket_config.full_key(&status_key_base);
    
    let operation_name = format!("read_status: {}", status_key);
    match retry_s3_operation(
        &operation_name,
        retry_count,
        retry_delay_ms,
        || {
            let client = client.clone();
            let bucket = bucket_name.clone();
            let key = status_key.clone();
            async move {
                client.get_object().bucket(&bucket).key(&key).send().await
            }
        }
    ).await {
        Ok(output) => {
            match output.body.collect().await {
                Ok(data) => {
                    let body = data.into_bytes();
                    let content = String::from_utf8_lossy(&body).to_string();
                    
                    // Pretty print JSON
                    let json_content = match serde_json::from_str::<serde_json::Value>(&content) {
                        Ok(json) => serde_json::to_string_pretty(&json).unwrap_or(content),
                        Err(_) => content,
                    };
                    
                    Html(format!(
                        r#"<!DOCTYPE html>
<html>
<head>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <title>Status - {}/{}</title>
</head>
<body>
    <div class="container mt-4">
        <div class="d-flex justify-content-between align-items-center mb-4">
            <h2>Status: {}/{}</h2>
            <a href="/cells" class="btn btn-secondary">Back to Cells</a>
        </div>
        <pre class="bg-light p-3 rounded">{}</pre>
    </div>
</body>
</html>"#,
                        node_id, cell_id, node_id, cell_id, json_content
                    )).into_response()
                }
                Err(e) => Html(format!("<h1>Error reading status.json: {:?}</h1>", e)).into_response()
            }
        }
        Err(e) => Html(format!("<h1>Error fetching status.json: {:?}</h1>", e)).into_response()
    }
}

// ========== Cell Override Handlers ==========

#[derive(Debug, Serialize, Deserialize, Clone)]
struct CellOverridesManifest {
    files: Vec<CellOverrideFile>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct CellOverrideFile {
    path: String,
    file_type: FileType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    owner: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    group: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    mode: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct ResolvedCellOverridesManifest {
    files: Vec<ResolvedCellOverrideFile>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct ResolvedCellOverrideFile {
    path: String,
    file_type: FileType,
    owner: String,
    group: String,
    mode: String,
}

#[derive(Template)]
#[template(path = "cell_overrides.html")]
struct CellOverridesTemplate {
    node_id: String,
    cell_id: String,
    bucket: String,
    overrides: Vec<OverrideFileInfo>,
}

#[derive(Serialize)]
struct OverrideFileInfo {
    path: String,
    size: i64,
    last_modified: String,
    is_folder: bool,
    file_type: String,
    owner_group: Option<String>,
    mode: Option<String>,
    owner_group_str: String,
    mode_str: String,
}

// Load cell overrides manifest
async fn load_cell_overrides_manifest(
    client: &aws_sdk_s3::Client,
    bucket_name: &str,
    node_id: &str,
    cell_id: &str,
    bucket_config: &BucketConfig,
    retry_count: u32,
    retry_delay_ms: u64,
) -> Option<CellOverridesManifest> {
    let manifest_key = bucket_config.full_key(&format!("cells/{}/{}/overrides_manifest.json", node_id, cell_id));
    
    let get_op = format!("get_cell_overrides_manifest: {}", manifest_key);
    let result = retry_s3_operation(
        &get_op,
        retry_count,
        retry_delay_ms,
        || {
            let client = client.clone();
            let bucket = bucket_name.to_string();
            let key = manifest_key.clone();
            async move {
                client
                    .get_object()
                    .bucket(&bucket)
                    .key(&key)
                    .send()
                    .await
            }
        }
    ).await;
    
    match result {
        Ok(output) => {
            match output.body.collect().await {
                Ok(data) => {
                    match serde_json::from_slice::<CellOverridesManifest>(&data.to_vec()) {
                        Ok(manifest) => Some(manifest),
                        Err(_) => None,
                    }
                }
                Err(_) => None,
            }
        }
        Err(_) => None,
    }
}

// Save cell overrides manifest
async fn save_cell_overrides_manifest(
    client: &aws_sdk_s3::Client,
    bucket_name: &str,
    node_id: &str,
    cell_id: &str,
    manifest: &CellOverridesManifest,
    bucket_config: &BucketConfig,
    retry_count: u32,
    retry_delay_ms: u64,
) -> Result<(), String> {
    let manifest_key = bucket_config.full_key(&format!("cells/{}/{}/overrides_manifest.json", node_id, cell_id));
    let json_data = serde_json::to_string_pretty(manifest)
        .map_err(|e| format!("Failed to serialize manifest: {}", e))?;
    
    let put_op = format!("put_cell_overrides_manifest: {}", manifest_key);
    retry_s3_operation(
        &put_op,
        retry_count,
        retry_delay_ms,
        || {
            let client = client.clone();
            let bucket = bucket_name.to_string();
            let key = manifest_key.clone();
            let data = json_data.clone();
            async move {
                client
                    .put_object()
                    .bucket(&bucket)
                    .key(&key)
                    .body(ByteStream::from(data.into_bytes()))
                    .send()
                    .await
            }
        }
    ).await
    .map_err(|e| format!("Failed to save manifest: {}", e))?;
    
    Ok(())
}

async fn cell_overrides_page(
    AxumState(state): AxumState<AppState>,
    AxumPath((node_id, cell_id)): AxumPath<(String, String)>,
) -> impl IntoResponse {
    // Clone config data before await
    let (bucket_name, bucket_display_name, bucket_config, retry_count, retry_delay_ms) = {
        let config = state.read().unwrap();
        let cell_key = format!("{}/{}", node_id, cell_id);
        match config.cells.get(&cell_key) {
            Some(cell) => {
                let bucket_display = cell.bucket.clone();
                match config.buckets.get(&cell.bucket) {
                    Some(bc) => (bc.bucket_name.clone(), bucket_display, bc.clone(),
                                config.s3_retry_count, config.s3_retry_delay_ms),
                    None => return Html("<h1>Bucket not found</h1>".to_string()).into_response(),
                }
            }
            None => return Html("<h1>Cell not found</h1>".to_string()).into_response(),
        }
    };
    
    let client = create_s3_client(&bucket_config).await;
    
    // Load overrides manifest
    let manifest = load_cell_overrides_manifest(&client, &bucket_name, &node_id, &cell_id, &bucket_config, retry_count, retry_delay_ms).await
        .unwrap_or(CellOverridesManifest { files: Vec::new() });
    
    // List files in overrides/ folder
    let prefix_base = format!("cells/{}/{}/overrides/", node_id, cell_id);
    let prefix = bucket_config.full_key(&prefix_base);
    
    let list_op = format!("list_cell_overrides: {}", prefix);
    let list_result = retry_s3_operation(
        &list_op,
        retry_count,
        retry_delay_ms,
        || {
            let client = client.clone();
            let bucket = bucket_name.clone();
            let prefix = prefix.clone();
            async move {
                client
                    .list_objects_v2()
                    .bucket(&bucket)
                    .prefix(&prefix)
                    .send()
                    .await
            }
        }
    ).await;
    
    // Build a map of manifest metadata by path
    let mut metadata_map: std::collections::HashMap<String, &CellOverrideFile> = std::collections::HashMap::new();
    for file in &manifest.files {
        metadata_map.insert(file.path.clone(), file);
    }
    
    let mut overrides = Vec::new();
    if let Ok(output) = list_result {
        if let Some(contents) = output.contents {
            for obj in contents {
                if let Some(key) = obj.key {
                    // Remove the prefix to get relative path
                    let full_key = key.clone();
                    let relative_path = if let Some(stripped) = bucket_config.strip_prefix(&full_key) {
                        stripped.strip_prefix(&prefix_base).unwrap_or(&stripped).to_string()
                    } else {
                        key.strip_prefix(&prefix).unwrap_or(&key).to_string()
                    };
                    
                    if !relative_path.is_empty() {
                        let is_folder = relative_path.ends_with('/');
                        
                        // Get metadata from manifest (customization is optional)
                        let (file_type, owner_group, mode) = if let Some(meta) = metadata_map.get(&relative_path) {
                            let og = match (meta.owner.as_deref(), meta.group.as_deref()) {
                                (Some(o), Some(g)) if !o.trim().is_empty() && !g.trim().is_empty() => {
                                    Some(format!("{}:{}", o.trim(), g.trim()))
                                }
                                _ => None,
                            };
                            let m = meta.mode.as_deref().map(|s| s.trim()).filter(|s| !s.is_empty()).map(|s| s.to_string());
                            (meta.file_type.to_string(), og, m)
                        } else {
                            let ft = if is_folder { "folder" } else { "binary" };
                            (ft.to_string(), None, None)
                        };
                        
                        let owner_group_str = owner_group.clone().unwrap_or_default();
                        let mode_str = mode.clone().unwrap_or_default();
                        overrides.push(OverrideFileInfo {
                            path: relative_path.clone(),
                            size: obj.size.unwrap_or(0),
                            last_modified: obj.last_modified
                                .map(|dt| dt.fmt(aws_smithy_types::date_time::Format::DateTime).unwrap_or_default())
                                .unwrap_or_default(),
                            is_folder,
                            file_type,
                            owner_group,
                            mode,
                            owner_group_str,
                            mode_str,
                        });
                    }
                }
            }
        }
    }
    
    let template = CellOverridesTemplate {
        node_id,
        cell_id,
        bucket: bucket_display_name,
        overrides,
    };
    HtmlTemplate(template).into_response()
}

async fn upload_cell_override(
    AxumState(state): AxumState<AppState>,
    AxumPath((node_id, cell_id)): AxumPath<(String, String)>,
    mut multipart: Multipart,
) -> impl IntoResponse {
    // Clone config data before await
    let (bucket_name, bucket_config, retry_count, retry_delay_ms) = {
        let config = state.read().unwrap();
        let cell_key = format!("{}/{}", node_id, cell_id);
        match config.cells.get(&cell_key) {
            Some(cell) => {
                match config.buckets.get(&cell.bucket) {
                    Some(bc) => (bc.bucket_name.clone(), bc.clone(),
                                config.s3_retry_count, config.s3_retry_delay_ms),
                    None => return Redirect::to(&format!("/cells/{}/{}/overrides", node_id, cell_id)).into_response(),
                }
            }
            None => return Redirect::to("/cells").into_response(),
        }
    };
    
    let client = create_s3_client(&bucket_config).await;
    
    let mut file_path = String::new();
    let mut source_type = String::from("upload");
    let mut staging_path = String::new();
    let mut file_type = String::from("binary");
    let mut owner = String::new();
    let mut group = String::new();
    let mut mode = String::new();
    let mut file_data = Vec::new();
    
    while let Ok(Some(field)) = multipart.next_field().await {
        let name = field.name().unwrap_or("").to_string();
        
        if name == "file_path" {
            file_path = field.text().await.unwrap_or_default();
        } else if name == "source_type" {
            source_type = field.text().await.unwrap_or_default();
        } else if name == "staging_path" {
            staging_path = field.text().await.unwrap_or_default();
        } else if name == "file_type" {
            file_type = field.text().await.unwrap_or_default();
        } else if name == "owner" {
            owner = field.text().await.unwrap_or_default();
        } else if name == "group" {
            group = field.text().await.unwrap_or_default();
        } else if name == "mode" {
            mode = field.text().await.unwrap_or_default();
        } else if name == "file" {
            file_data = field.bytes().await.unwrap_or_default().to_vec();
        }
    }
    
    // Validate file path (no relative paths, no path escape)
    // Folders are allowed (ending with /) for file_type == "folder"
    if file_path.is_empty() || file_path.starts_with('/') || file_path.starts_with("../") || 
       file_path.starts_with("./") || file_path.contains("/./") || file_path.contains("/../") {
        return (
            StatusCode::BAD_REQUEST,
            Html("<html><head><meta http-equiv='refresh' content='3;url=/cells'></head><body>Invalid file path. Cannot use /, ../, ./, or path escape patterns.</body></html>")
        ).into_response();
    }
    
    // Validate file type
    if !["text", "binary", "folder"].contains(&file_type.as_str()) {
        return (
            StatusCode::BAD_REQUEST,
            Html("<html><head><meta http-equiv='refresh' content='3;url=/cells'></head><body>Invalid file type. Must be text, binary, or folder.</body></html>")
        ).into_response();
    }
    
    // For folders, ensure path ends with /
    let normalized_file_path = if file_type == "folder" && !file_path.ends_with('/') {
        format!("{}/", file_path)
    } else {
        file_path.clone()
    };
    
    // Get file data based on source type (skip for folders)
    let file_data_result = if file_type == "folder" {
        Ok(Vec::new()) // Folders don't have content
    } else if source_type == "staging" {
        // Copy from staging
        if staging_path.is_empty() {
            return (
                StatusCode::BAD_REQUEST,
                Html("<html><head><meta http-equiv='refresh' content='3;url=/cells'></head><body>No staging file selected</body></html>")
            ).into_response();
        }
        
        // Staging is under the bucket prefix
        let staging_key = bucket_config.full_key(&format!("staging/{}", staging_path));
        let get_op = format!("get_staging_file: {}", staging_key);
        
        match retry_s3_operation(
            &get_op,
            retry_count,
            retry_delay_ms,
            || {
                let client = client.clone();
                let bucket = bucket_name.clone();
                let key = staging_key.clone();
                async move {
                    client
                        .get_object()
                        .bucket(&bucket)
                        .key(&key)
                        .send()
                        .await
                }
            }
        ).await {
            Ok(resp) => {
                match resp.body.collect().await {
                    Ok(data) => Ok(data.to_vec()),
                    Err(e) => Err(format!("Failed to read staging file: {}", e)),
                }
            }
            Err(e) => Err(format!("Failed to get staging file: {}", e)),
        }
    } else {
        // Upload from computer
        if file_data.is_empty() {
            return (
                StatusCode::BAD_REQUEST,
                Html("<html><head><meta http-equiv='refresh' content='3;url=/cells'></head><body>No file uploaded</body></html>")
            ).into_response();
        }
        Ok(file_data)
    };
    
    let file_data = match file_data_result {
        Ok(data) => data,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Html(format!("<html><head><meta http-equiv='refresh' content='3;url=/cells'></head><body>Error: {}</body></html>", e))
            ).into_response();
        }
    };
    
    // Upload file to S3 (for non-folders)
    if !file_data.is_empty() {
        let key_base = format!("cells/{}/{}/overrides/{}", node_id, cell_id, normalized_file_path);
        let key = bucket_config.full_key(&key_base);
        
        // For text files (valid UTF-8), normalize line endings to Unix (LF)
        let file_data_normalized = match String::from_utf8(file_data.clone()) {
            Ok(text) => {
                let normalized = text.replace("\r\n", "\n");
                normalized.into_bytes()
            }
            Err(_) => file_data, // Binary file, use as-is
        };
        
        let upload_op = format!("upload_cell_override: {}", key);
        let _ = retry_s3_operation(
            &upload_op,
            retry_count,
            retry_delay_ms,
            || {
                let client = client.clone();
                let bucket = bucket_name.clone();
                let key = key.clone();
                let data = file_data_normalized.clone();
                async move {
                    client
                        .put_object()
                        .bucket(&bucket)
                        .key(&key)
                        .body(ByteStream::from(data))
                        .send()
                        .await
                }
            }
        ).await;
    } else if file_type == "folder" {
        // For folders, create a placeholder object
        let key_base = format!("cells/{}/{}/overrides/{}", node_id, cell_id, normalized_file_path);
        let key = bucket_config.full_key(&key_base);
        
        let upload_op = format!("upload_cell_override_folder: {}", key);
        let _ = retry_s3_operation(
            &upload_op,
            retry_count,
            retry_delay_ms,
            || {
                let client = client.clone();
                let bucket = bucket_name.clone();
                let key = key.clone();
                async move {
                    client
                        .put_object()
                        .bucket(&bucket)
                        .key(&key)
                        .body(ByteStream::from(vec![]))
                        .send()
                        .await
                }
            }
        ).await;
    }
    
    // Update manifest with metadata
    let mut manifest = load_cell_overrides_manifest(&client, &bucket_name, &node_id, &cell_id, &bucket_config, retry_count, retry_delay_ms).await
        .unwrap_or(CellOverridesManifest { files: Vec::new() });
    
    // Parse file type
    let ft = match file_type.as_str() {
        "text" => FileType::Text,
        "binary" => FileType::Binary,
        "folder" => FileType::Folder,
        _ => FileType::Binary,
    };

    // Normalize optional customization: either full (owner+group+mode) or empty.
    let owner_t = owner.trim().to_string();
    let group_t = group.trim().to_string();
    let mode_t = mode.trim().to_string();
    let has_any = !owner_t.is_empty() || !group_t.is_empty() || !mode_t.is_empty();
    if has_any && (owner_t.is_empty() || group_t.is_empty() || mode_t.is_empty()) {
        return (
            StatusCode::BAD_REQUEST,
            Html("<html><head><meta http-equiv='refresh' content='5;url=/cells'></head><body>Customization must include owner + group + mode, or be fully empty.</body></html>".to_string()),
        )
            .into_response();
    }
    if has_any {
        if let Err(e) = validate_service_identity(&owner_t, &group_t) {
            return (
                StatusCode::BAD_REQUEST,
                Html(format!(
                    "<html><head><meta http-equiv='refresh' content='5;url=/cells'></head><body>{}</body></html>",
                    e
                )),
            )
                .into_response();
        }
        if let Err(e) = parse_mode_octal(&mode_t) {
            return (
                StatusCode::BAD_REQUEST,
                Html(format!(
                    "<html><head><meta http-equiv='refresh' content='5;url=/cells'></head><body>{}</body></html>",
                    e
                )),
            )
                .into_response();
        }
    }
    
    // Remove existing entry if present
    manifest.files.retain(|f| f.path != normalized_file_path);
    
    // Add new entry
    manifest.files.push(CellOverrideFile {
        path: normalized_file_path.clone(),
        file_type: ft,
        owner: if has_any { Some(owner_t) } else { None },
        group: if has_any { Some(group_t) } else { None },
        mode: if has_any { Some(mode_t) } else { None },
    });
    
    // Save manifest
    let _ = save_cell_overrides_manifest(&client, &bucket_name, &node_id, &cell_id, &manifest, &bucket_config, retry_count, retry_delay_ms).await;
    
    Redirect::to(&format!("/cells/{}/{}/overrides", node_id, cell_id)).into_response()
}

#[derive(Deserialize)]
struct DeleteCellOverrideForm {
    file_path: String,
}

async fn delete_cell_override(
    AxumState(state): AxumState<AppState>,
    AxumPath((node_id, cell_id)): AxumPath<(String, String)>,
    Form(form): Form<DeleteCellOverrideForm>,
) -> impl IntoResponse {
    // Clone config data before await
    let (bucket_name, bucket_config, retry_count, retry_delay_ms) = {
        let config = state.read().unwrap();
        let cell_key = format!("{}/{}", node_id, cell_id);
        match config.cells.get(&cell_key) {
            Some(cell) => {
                match config.buckets.get(&cell.bucket) {
                    Some(bc) => (bc.bucket_name.clone(), bc.clone(),
                                config.s3_retry_count, config.s3_retry_delay_ms),
                    None => return Redirect::to(&format!("/cells/{}/{}/overrides", node_id, cell_id)).into_response(),
                }
            }
            None => return Redirect::to("/cells").into_response(),
        }
    };
    
    let client = create_s3_client(&bucket_config).await;
    
    let key_base = format!("cells/{}/{}/overrides/{}", node_id, cell_id, form.file_path);
    let key = bucket_config.full_key(&key_base);
    
    let delete_op = format!("delete_cell_override: {}", key);
    let _ = retry_s3_operation(
        &delete_op,
        retry_count,
        retry_delay_ms,
        || {
            let client = client.clone();
            let bucket = bucket_name.clone();
            let key = key.clone();
            async move {
                client
                    .delete_object()
                    .bucket(&bucket)
                    .key(&key)
                    .send()
                    .await
            }
        }
    ).await;
    
    // Update manifest to remove the file
    let mut manifest = load_cell_overrides_manifest(&client, &bucket_name, &node_id, &cell_id, &bucket_config, retry_count, retry_delay_ms).await
        .unwrap_or(CellOverridesManifest { files: Vec::new() });
    
    manifest.files.retain(|f| f.path != form.file_path);
    
    let _ = save_cell_overrides_manifest(&client, &bucket_name, &node_id, &cell_id, &manifest, &bucket_config, retry_count, retry_delay_ms).await;
    
    Redirect::to(&format!("/cells/{}/{}/overrides", node_id, cell_id)).into_response()
}

async fn edit_cell_override(
    AxumState(state): AxumState<AppState>,
    AxumPath((node_id, cell_id)): AxumPath<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let file_path = params.get("file_path").cloned().unwrap_or_default();
    
    // Clone config data before await
    let (bucket_name, bucket_config, retry_count, retry_delay_ms) = {
        let config = state.read().unwrap();
        let cell_key = format!("{}/{}", node_id, cell_id);
        match config.cells.get(&cell_key) {
            Some(cell) => {
                match config.buckets.get(&cell.bucket) {
                    Some(bc) => (bc.bucket_name.clone(), bc.clone(),
                                config.s3_retry_count, config.s3_retry_delay_ms),
                    None => return Html("<h1>Bucket not found</h1>".to_string()).into_response(),
                }
            }
            None => return Html("<h1>Cell not found</h1>".to_string()).into_response(),
        }
    };
    
    let client = create_s3_client(&bucket_config).await;
    
    let key_base = format!("cells/{}/{}/overrides/{}", node_id, cell_id, file_path);
    let key = bucket_config.full_key(&key_base);
    
    let get_op = format!("get_cell_override: {}", key);
    let get_result = retry_s3_operation(
        &get_op,
        retry_count,
        retry_delay_ms,
        || {
            let client = client.clone();
            let bucket = bucket_name.clone();
            let key = key.clone();
            async move {
                client
                    .get_object()
                    .bucket(&bucket)
                    .key(&key)
                    .send()
                    .await
            }
        }
    ).await;
    
    let content = match get_result {
        Ok(output) => {
            match output.body.collect().await {
                Ok(data) => String::from_utf8_lossy(&data.into_bytes()).to_string(),
                Err(_) => String::new(),
            }
        }
        Err(_) => String::new(),
    };
    
    let template = EditCellOverrideTemplate {
        node_id,
        cell_id,
        file_path,
        content,
    };
    HtmlTemplate(template).into_response()
}

#[derive(Template)]
#[template(path = "edit_cell_override.html")]
struct EditCellOverrideTemplate {
    node_id: String,
    cell_id: String,
    file_path: String,
    content: String,
}

#[derive(Deserialize)]
struct SaveCellOverrideForm {
    file_path: String,
    content: String,
}

async fn save_cell_override(
    AxumState(state): AxumState<AppState>,
    AxumPath((node_id, cell_id)): AxumPath<(String, String)>,
    Form(form): Form<SaveCellOverrideForm>,
) -> impl IntoResponse {
    // Clone config data before await
    let (bucket_name, bucket_config, retry_count, retry_delay_ms) = {
        let config = state.read().unwrap();
        let cell_key = format!("{}/{}", node_id, cell_id);
        match config.cells.get(&cell_key) {
            Some(cell) => {
                match config.buckets.get(&cell.bucket) {
                    Some(bc) => (bc.bucket_name.clone(), bc.clone(),
                                config.s3_retry_count, config.s3_retry_delay_ms),
                    None => return Redirect::to(&format!("/cells/{}/{}/overrides", node_id, cell_id)).into_response(),
                }
            }
            None => return Redirect::to("/cells").into_response(),
        }
    };
    
    let client = create_s3_client(&bucket_config).await;
    
    let key_base = format!("cells/{}/{}/overrides/{}", node_id, cell_id, form.file_path);
    let key = bucket_config.full_key(&key_base);
    
    // Convert Windows line endings (CRLF) to Unix (LF)
    let normalized_content = form.content.replace("\r\n", "\n");
    
    let save_op = format!("save_cell_override: {}", key);
    let _ = retry_s3_operation(
        &save_op,
        retry_count,
        retry_delay_ms,
        || {
            let client = client.clone();
            let bucket = bucket_name.clone();
            let key = key.clone();
            let content = normalized_content.clone();
            async move {
                client
                    .put_object()
                    .bucket(&bucket)
                    .key(&key)
                    .body(ByteStream::from(content.into_bytes()))
                    .send()
                    .await
            }
        }
    ).await;
    
    Redirect::to(&format!("/cells/{}/{}/overrides", node_id, cell_id)).into_response()
}

#[derive(Deserialize)]
struct UpdateCellOverrideMetaForm {
    file_path: String,
    file_type: String,
    #[serde(default)]
    owner: String,
    #[serde(default)]
    group: String,
    #[serde(default)]
    mode: String,
}

async fn update_cell_override_meta(
    AxumState(state): AxumState<AppState>,
    AxumPath((node_id, cell_id)): AxumPath<(String, String)>,
    Form(form): Form<UpdateCellOverrideMetaForm>,
) -> impl IntoResponse {
    // Clone config data before await
    let (bucket_name, bucket_config, retry_count, retry_delay_ms) = {
        let config = state.read().unwrap();
        let cell_key = format!("{}/{}", node_id, cell_id);
        match config.cells.get(&cell_key) {
            Some(cell) => {
                match config.buckets.get(&cell.bucket) {
                    Some(bc) => (bc.bucket_name.clone(), bc.clone(),
                                config.s3_retry_count, config.s3_retry_delay_ms),
                    None => return Redirect::to(&format!("/cells/{}/{}/overrides", node_id, cell_id)).into_response(),
                }
            }
            None => return Redirect::to("/cells").into_response(),
        }
    };
    
    let client = create_s3_client(&bucket_config).await;
    
    // Load existing manifest
    let mut manifest = load_cell_overrides_manifest(&client, &bucket_name, &node_id, &cell_id, &bucket_config, retry_count, retry_delay_ms).await
        .unwrap_or(CellOverridesManifest { files: Vec::new() });
    
    // Parse file type
    let ft = match form.file_type.as_str() {
        "text" => FileType::Text,
        "binary" => FileType::Binary,
        "folder" => FileType::Folder,
        _ => FileType::Binary,
    };

    // Normalize optional customization: either full (owner+group+mode) or empty.
    let owner_t = form.owner.trim().to_string();
    let group_t = form.group.trim().to_string();
    let mode_t = form.mode.trim().to_string();
    let has_any = !owner_t.is_empty() || !group_t.is_empty() || !mode_t.is_empty();
    if has_any && (owner_t.is_empty() || group_t.is_empty() || mode_t.is_empty()) {
        return (
            StatusCode::BAD_REQUEST,
            Html("<html><head><meta http-equiv='refresh' content='5;url=/cells'></head><body>Customization must include owner + group + mode, or be fully empty.</body></html>".to_string()),
        )
            .into_response();
    }
    if has_any {
        if let Err(e) = validate_service_identity(&owner_t, &group_t) {
            return (
                StatusCode::BAD_REQUEST,
                Html(format!(
                    "<html><head><meta http-equiv='refresh' content='5;url=/cells'></head><body>{}</body></html>",
                    e
                )),
            )
                .into_response();
        }
        if let Err(e) = parse_mode_octal(&mode_t) {
            return (
                StatusCode::BAD_REQUEST,
                Html(format!(
                    "<html><head><meta http-equiv='refresh' content='5;url=/cells'></head><body>{}</body></html>",
                    e
                )),
            )
                .into_response();
        }
    }
    
    // Find and update existing entry, or add new one
    if let Some(existing) = manifest.files.iter_mut().find(|f| f.path == form.file_path) {
        existing.file_type = ft;
        existing.owner = if has_any { Some(owner_t) } else { None };
        existing.group = if has_any { Some(group_t) } else { None };
        existing.mode = if has_any { Some(mode_t) } else { None };
    } else {
        // File not in manifest yet (legacy file), add it
        manifest.files.push(CellOverrideFile {
            path: form.file_path.clone(),
            file_type: ft,
            owner: if has_any { Some(owner_t) } else { None },
            group: if has_any { Some(group_t) } else { None },
            mode: if has_any { Some(mode_t) } else { None },
        });
    }
    
    // Save manifest
    let _ = save_cell_overrides_manifest(&client, &bucket_name, &node_id, &cell_id, &manifest, &bucket_config, retry_count, retry_delay_ms).await;
    
    Redirect::to(&format!("/cells/{}/{}/overrides", node_id, cell_id)).into_response()
}

// ========== Cell Version Management ==========

#[derive(Template)]
#[template(path = "cell_versions.html")]
struct CellVersionsTemplate {
    node_id: String,
    cell_id: String,
    bucket: String,
    versions: Vec<CellVersionInfo>,
}

#[derive(Serialize)]
struct CellVersionInfo {
    version: String,
    published_at: String,
}

async fn cell_versions_page(
    AxumState(state): AxumState<AppState>,
    AxumPath((node_id, cell_id)): AxumPath<(String, String)>,
) -> impl IntoResponse {
    // Clone config data before await
    let (bucket_name, bucket_config, retry_count, retry_delay_ms) = {
        let config = state.read().unwrap();
        let cell_key = format!("{}/{}", node_id, cell_id);
        match config.cells.get(&cell_key) {
            Some(cell) => {
                match config.buckets.get(&cell.bucket) {
                    Some(bc) => (bc.bucket_name.clone(), bc.clone(),
                                config.s3_retry_count, config.s3_retry_delay_ms),
                    None => return Html("<h1>Bucket not found</h1>".to_string()).into_response(),
                }
            }
            None => return Html("<h1>Cell not found</h1>".to_string()).into_response(),
        }
    };
    
    let client = create_s3_client(&bucket_config).await;
    
    // List versions in the cell's versions/ folder
    let prefix_base = format!("cells/{}/{}/versions/", node_id, cell_id);
    let prefix = bucket_config.full_key(&prefix_base);
    
    let list_op = format!("list_cell_versions: {}", prefix);
    let list_result = retry_s3_operation(
        &list_op,
        retry_count,
        retry_delay_ms,
        || {
            let client = client.clone();
            let bucket = bucket_name.clone();
            let prefix = prefix.clone();
            async move {
                client
                    .list_objects_v2()
                    .bucket(&bucket)
                    .prefix(&prefix)
                    .delimiter("/")
                    .send()
                    .await
            }
        }
    ).await;
    
    let mut versions = Vec::new();
    if let Ok(output) = list_result {
        if let Some(common_prefixes) = output.common_prefixes {
            for prefix_obj in common_prefixes {
                if let Some(prefix_str) = prefix_obj.prefix {
                    // Extract version from prefix
                    let full_prefix = prefix_str.clone();
                    let relative_path = if let Some(stripped) = bucket_config.strip_prefix(&full_prefix) {
                        stripped.strip_prefix(&prefix_base).unwrap_or(&stripped)
                    } else {
                        prefix_str.strip_prefix(&prefix).unwrap_or(&prefix_str)
                    };
                    
                    let version = relative_path.trim_end_matches('/').to_string();
                    
                    if !version.is_empty() {
                        // Get the trigger file to determine publish time (optional)
                        let published_at = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S UTC").to_string();
                        
                        versions.push(CellVersionInfo {
                            version,
                            published_at,
                        });
                    }
                }
            }
        }
    }
    
    // Sort versions in descending order (newest first)
    versions.sort_by(|a, b| b.version.cmp(&a.version));
    
    let template = CellVersionsTemplate {
        node_id,
        cell_id,
        bucket: bucket_name,
        versions,
    };
    HtmlTemplate(template).into_response()
}

#[derive(Deserialize)]
struct DeleteCellVersionForm {
    version: String,
}

async fn delete_cell_version(
    AxumState(state): AxumState<AppState>,
    AxumPath((node_id, cell_id)): AxumPath<(String, String)>,
    Form(form): Form<DeleteCellVersionForm>,
) -> impl IntoResponse {
    // Clone config data before await
    let (bucket_name, bucket_config, retry_count, retry_delay_ms) = {
        let config = state.read().unwrap();
        let cell_key = format!("{}/{}", node_id, cell_id);
        match config.cells.get(&cell_key) {
            Some(cell) => {
                match config.buckets.get(&cell.bucket) {
                    Some(bc) => (bc.bucket_name.clone(), bc.clone(),
                                config.s3_retry_count, config.s3_retry_delay_ms),
                    None => return Redirect::to(&format!("/cells/{}/{}/versions", node_id, cell_id)).into_response(),
                }
            }
            None => return Redirect::to("/cells").into_response(),
        }
    };
    
    let client = create_s3_client(&bucket_config).await;
    
    // List all files in the version folder
    let prefix_base = format!("cells/{}/{}/versions/{}/", node_id, cell_id, form.version);
    let prefix = bucket_config.full_key(&prefix_base);
    
    let list_op = format!("list_version_files: {}", prefix);
    let list_result = retry_s3_operation(
        &list_op,
        retry_count,
        retry_delay_ms,
        || {
            let client = client.clone();
            let bucket = bucket_name.clone();
            let prefix = prefix.clone();
            async move {
                client
                    .list_objects_v2()
                    .bucket(&bucket)
                    .prefix(&prefix)
                    .send()
                    .await
            }
        }
    ).await;
    
    // Delete all files in the version folder
    if let Ok(output) = list_result {
        if let Some(contents) = output.contents {
            for obj in contents {
                if let Some(key) = obj.key {
                    let delete_op = format!("delete_version_file: {}", key);
                    let _ = retry_s3_operation(
                        &delete_op,
                        retry_count,
                        retry_delay_ms,
                        || {
                            let client = client.clone();
                            let bucket = bucket_name.clone();
                            let key = key.clone();
                            async move {
                                client
                                    .delete_object()
                                    .bucket(&bucket)
                                    .key(&key)
                                    .send()
                                    .await
                            }
                        }
                    ).await;
                }
            }
        }
    }
    
    Redirect::to(&format!("/cells/{}/{}/versions", node_id, cell_id)).into_response()
}

// ========== Publish Deployment to Cell ==========

#[derive(Deserialize)]
struct PublishToCellForm {
    version: String,
}

async fn publish_deployment_to_cell(
    AxumState(state): AxumState<AppState>,
    AxumPath((node_id, cell_id)): AxumPath<(String, String)>,
    Form(form): Form<PublishToCellForm>,
) -> impl IntoResponse {
    // Clone config data before await - get both manifest and cell bucket configs
    let (
        source_manifest_name,
        source_bucket_name,
        source_bucket_config,
        cell_bucket_name,
        cell_bucket_config,
        retry_count,
        retry_delay_ms,
        profile,
        service_owner,
        service_group,
    ) = {
        let config = state.read().unwrap();
        let cell_key = format!("{}/{}", node_id, cell_id);
        match config.cells.get(&cell_key) {
            Some(cell) => {
                // Get manifest by direct lookup using manifest key
                let manifest = match config.manifests.get(&cell.manifest) {
                    Some(m) => m,
                    None => return Html(format!("<h1>Manifest not found: {}</h1>", cell.manifest)).into_response(),
                };
                let service_owner = manifest.service_owner.clone();
                let service_group = manifest.service_group.clone();
                    
                // Get manifest's bucket config (source)
                let (manifest_bucket_name, manifest_bucket_config, profile) = {
                    let profile = manifest.profile.to_string();
                    match config.buckets.get(&manifest.bucket) {
                        Some(bc) => (bc.bucket_name.clone(), bc.clone(), profile),
                        None => return Html("<h1>Manifest bucket not found</h1>".to_string()).into_response(),
                    }
                };
                
                // Get cell's bucket config (destination)
                match config.buckets.get(&cell.bucket) {
                    Some(bc) => (cell.manifest.clone(), manifest_bucket_name, manifest_bucket_config,
                                bc.bucket_name.clone(), bc.clone(),
                                config.s3_retry_count, config.s3_retry_delay_ms, profile, service_owner, service_group),
                    None => return Html("<h1>Cell bucket not found</h1>".to_string()).into_response(),
                }
            }
            None => return Html("<h1>Cell not found</h1>".to_string()).into_response(),
        }
    };
    
    // Create clients for both source and destination buckets
    let source_client = create_s3_client(&source_bucket_config).await;
    let dest_client = create_s3_client(&cell_bucket_config).await;
    
    // Source: deployments/manifest_name/version/ (in manifest's bucket)
    let source_prefix_base = format!("deployments/{}/{}/", source_manifest_name, form.version);
    let source_prefix = source_bucket_config.full_key(&source_prefix_base);
    
    // Destination: cells/node_id/cell_id/versions/version/ (in cell's bucket)
    let dest_prefix_base = format!("cells/{}/{}/versions/{}/", node_id, cell_id, form.version);

    // If this version already exists for this cell, treat it as immutable:
    // do NOT rebuild/copy/merge overrides again.
    let version_exists = {
        let manifest_key_base = format!("{}manifest.json", dest_prefix_base);
        let manifest_key = cell_bucket_config.full_key(&manifest_key_base);
        let op = format!("head_cell_version_manifest: {}", manifest_key);
        retry_s3_operation(
            &op,
            retry_count,
            retry_delay_ms,
            || {
                let client = dest_client.clone();
                let bucket = cell_bucket_name.clone();
                let key = manifest_key.clone();
                async move { client.head_object().bucket(&bucket).key(&key).send().await }
            },
        )
        .await
        .is_ok()
    };

    // If the version already exists, publishing means "issue a trigger" only.
    // (This can be used to re-apply/restart the same version deterministically.)
    if version_exists {
        let trigger_key_base = format!("cells/{}/{}/trigger.json", node_id, cell_id);
        let trigger_key = cell_bucket_config.full_key(&trigger_key_base);

        let trigger_data = serde_json::json!({ "version": form.version });
        let trigger_content = serde_json::to_string_pretty(&trigger_data)
            .unwrap_or_else(|_| format!("{{\"version\":\"{}\"}}", form.version));

        let trigger_op = format!("create_trigger: {}", trigger_key);
        let _ = retry_s3_operation(
            &trigger_op,
            retry_count,
            retry_delay_ms,
            || {
                let client = dest_client.clone();
                let bucket = cell_bucket_name.clone();
                let key = trigger_key.clone();
                let content = trigger_content.clone();
                async move {
                    client
                        .put_object()
                        .bucket(&bucket)
                        .key(&key)
                        .body(ByteStream::from(content.into_bytes()))
                        .send()
                        .await
                }
            },
        )
        .await;

        return Redirect::to("/cells").into_response();
    }
    
    if !version_exists {
        // List all files in source deployment (from manifest's bucket)
        let list_op = format!("list_deployment_files: {}", source_prefix);
        let list_result = retry_s3_operation(
            &list_op,
            retry_count,
            retry_delay_ms,
            || {
                let client = source_client.clone();
                let bucket = source_bucket_name.clone();
                let prefix = source_prefix.clone();
                async move {
                    client
                        .list_objects_v2()
                        .bucket(&bucket)
                        .prefix(&prefix)
                        .send()
                        .await
                }
            }
        ).await;
        
        if let Ok(output) = list_result {
            if let Some(contents) = output.contents {
                for obj in contents {
                    if let Some(source_key) = obj.key {
                        // Get relative path within deployment
                        let relative_path = if let Some(stripped) = source_bucket_config.strip_prefix(&source_key) {
                            stripped.strip_prefix(&source_prefix_base).unwrap_or(&stripped)
                        } else {
                            source_key.strip_prefix(&source_prefix).unwrap_or(&source_key)
                        };
                        
                        // Destination key (in cell's bucket)
                        let dest_key_base = format!("{}{}", dest_prefix_base, relative_path);
                        let dest_key = cell_bucket_config.full_key(&dest_key_base);
                        
                        // Copy object from source bucket to destination bucket
                        let copy_source = format!("{}/{}", source_bucket_name, source_key);
                        let copy_op = format!("copy_to_cell: {} -> {}/{}", copy_source, cell_bucket_name, dest_key);
                        let _ = retry_s3_operation(
                            &copy_op,
                            retry_count,
                            retry_delay_ms,
                            || {
                                let client = dest_client.clone();
                                let bucket = cell_bucket_name.clone();
                                let dest = dest_key.clone();
                                let src = copy_source.clone();
                                async move {
                                    client
                                        .copy_object()
                                        .bucket(&bucket)
                                        .key(&dest)
                                        .copy_source(&src)
                                        .send()
                                        .await
                                }
                            }
                        ).await;
                    }
                }
            }
        }

        // === Snapshot + merge overrides into this version folder (cell bucket) ===
        // Copy overrides/ files to versions/<ver>/ (overriding base deployment files),
        // and snapshot overrides_manifest.json into versions/<ver>/overrides_manifest.json.
        let overrides_prefix_base = format!("cells/{}/{}/overrides/", node_id, cell_id);
        let overrides_prefix = cell_bucket_config.full_key(&overrides_prefix_base);
        let overrides_list_op = format!("list_cell_overrides: {}", overrides_prefix);
        let overrides_list_result = retry_s3_operation(
            &overrides_list_op,
            retry_count,
            retry_delay_ms,
            || {
                let client = dest_client.clone();
                let bucket = cell_bucket_name.clone();
                let prefix = overrides_prefix.clone();
                async move {
                    client
                        .list_objects_v2()
                        .bucket(&bucket)
                        .prefix(&prefix)
                        .send()
                        .await
                }
            },
        )
        .await;

        if let Ok(output) = overrides_list_result {
            if let Some(contents) = output.contents {
                for obj in contents {
                    if let Some(source_key) = obj.key {
                        if source_key.ends_with('/') {
                            continue;
                        }

                        // relative path under overrides/
                        let relative_path = if let Some(stripped) = cell_bucket_config.strip_prefix(&source_key) {
                            stripped.strip_prefix(&overrides_prefix_base).unwrap_or(&stripped)
                        } else {
                            source_key.strip_prefix(&overrides_prefix).unwrap_or(&source_key)
                        };

                        // merge into versions/<ver>/<relative_path>
                        let dest_key_base = format!("{}{}", dest_prefix_base, relative_path);
                        let dest_key = cell_bucket_config.full_key(&dest_key_base);

                        let copy_source = format!("{}/{}", cell_bucket_name, source_key);
                        let copy_op = format!("copy_override_to_version: {} -> {}/{}", copy_source, cell_bucket_name, dest_key);
                        let _ = retry_s3_operation(
                            &copy_op,
                            retry_count,
                            retry_delay_ms,
                            || {
                                let client = dest_client.clone();
                                let bucket = cell_bucket_name.clone();
                                let dest = dest_key.clone();
                                let src = copy_source.clone();
                                async move {
                                    client
                                        .copy_object()
                                        .bucket(&bucket)
                                        .key(&dest)
                                        .copy_source(&src)
                                        .send()
                                        .await
                                }
                            },
                        )
                        .await;
                    }
                }
            }
        }

        // Snapshot overrides manifest to versions/<ver>/overrides_manifest.json (repeatable):
        // store fully resolved owner/group/mode so the version doesn't depend on agent default rules.
        let overrides_manifest_src_base =
            format!("cells/{}/{}/overrides_manifest.json", node_id, cell_id);
        let overrides_manifest_src = cell_bucket_config.full_key(&overrides_manifest_src_base);
        let overrides_manifest_dest_base = format!("{}overrides_manifest.json", dest_prefix_base);
        let overrides_manifest_dest = cell_bucket_config.full_key(&overrides_manifest_dest_base);

        let get_op = format!("get_overrides_manifest_for_snapshot: {}", overrides_manifest_src);
        if let Ok(obj) = retry_s3_operation(
            &get_op,
            retry_count,
            retry_delay_ms,
            || {
                let client = dest_client.clone();
                let bucket = cell_bucket_name.clone();
                let key = overrides_manifest_src.clone();
                async move { client.get_object().bucket(&bucket).key(&key).send().await }
            },
        )
        .await
        {
            if let Ok(data) = obj.body.collect().await {
                if let Ok(live) = serde_json::from_slice::<CellOverridesManifest>(&data.to_vec()) {
                    let mut files: Vec<ResolvedCellOverrideFile> = Vec::new();
                    for f in live.files {
                        let owner = f
                            .owner
                            .as_deref()
                            .map(|s| s.trim())
                            .filter(|s| !s.is_empty())
                            .unwrap_or(service_owner.trim())
                            .to_string();
                        let group = f
                            .group
                            .as_deref()
                            .map(|s| s.trim())
                            .filter(|s| !s.is_empty())
                            .unwrap_or(service_group.trim())
                            .to_string();
                        let mode = f
                            .mode
                            .as_deref()
                            .map(|s| s.trim())
                            .filter(|s| !s.is_empty())
                            .unwrap_or(default_override_mode(&f.path, &f.file_type))
                            .to_string();
                        files.push(ResolvedCellOverrideFile {
                            path: f.path,
                            file_type: f.file_type,
                            owner,
                            group,
                            mode,
                        });
                    }
                    let resolved = ResolvedCellOverridesManifest { files };
                    if let Ok(content) = serde_json::to_string_pretty(&resolved) {
                        let put_op =
                            format!("put_overrides_manifest_snapshot: {}", overrides_manifest_dest);
                        let _ = retry_s3_operation(
                            &put_op,
                            retry_count,
                            retry_delay_ms,
                            || {
                                let client = dest_client.clone();
                                let bucket = cell_bucket_name.clone();
                                let key = overrides_manifest_dest.clone();
                                let body = content.clone();
                                async move {
                                    client
                                        .put_object()
                                        .bucket(&bucket)
                                        .key(&key)
                                        .body(ByteStream::from(body.into_bytes()))
                                        .send()
                                        .await
                                }
                            },
                        )
                        .await;
                    }
                }
            }
        }
    }
    
    // Create metadata file (JSON format) - contains manifest_name and profile (in cell's bucket)
    let metadata_data = serde_json::json!({
        "manifest_name": source_manifest_name,
        "profile": profile
    });
    let metadata_content = serde_json::to_string_pretty(&metadata_data)
        .unwrap_or_else(|_| format!("{{\"manifest_name\":\"{}\",\"profile\":\"{}\"}}", source_manifest_name, profile));
    
    let metadata_key_base = format!("cells/{}/{}/metadata.json", node_id, cell_id);
    let metadata_key = cell_bucket_config.full_key(&metadata_key_base);
    
    let metadata_op = format!("create_metadata: {}", metadata_key);
    let _ = retry_s3_operation(
        &metadata_op,
        retry_count,
        retry_delay_ms,
        || {
            let client = dest_client.clone();
            let bucket = cell_bucket_name.clone();
            let key = metadata_key.clone();
            let content = metadata_content.clone();
            async move {
                client
                    .put_object()
                    .bucket(&bucket)
                    .key(&key)
                    .body(ByteStream::from(content.into_bytes()))
                    .send()
                    .await
            }
        }
    ).await;
    
    // Create trigger file (JSON format) (in cell's bucket)
    let trigger_key_base = format!("cells/{}/{}/trigger.json", node_id, cell_id);
    let trigger_key = cell_bucket_config.full_key(&trigger_key_base);
    let trigger_data = serde_json::json!({ "version": form.version });
    let trigger_content = serde_json::to_string_pretty(&trigger_data)
        .unwrap_or_else(|_| format!("{{\"version\":\"{}\"}}", form.version));

    let trigger_op = format!("create_trigger: {}", trigger_key);
    let _ = retry_s3_operation(
        &trigger_op,
        retry_count,
        retry_delay_ms,
        || {
            let client = dest_client.clone();
            let bucket = cell_bucket_name.clone();
            let key = trigger_key.clone();
            let content = trigger_content.clone();
            async move {
                client
                    .put_object()
                    .bucket(&bucket)
                    .key(&key)
                    .body(ByteStream::from(content.into_bytes()))
                    .send()
                    .await
            }
        }
    ).await;
    
    Redirect::to("/cells").into_response()
}

/// Validate display name format
/// - Required (cannot be empty)
/// - Must contain only alphanumerics (a-z, A-Z, 0-9), hyphens (-), and underscores (_)
/// - Maximum length of 40 characters
fn validate_display_name(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("Display name is required and cannot be empty".to_string());
    }
    
    if name.len() > 40 {
        return Err(format!("Display name must be 40 characters or less (current: {})", name.len()));
    }
    
    // Check that name contains only allowed characters
    if !name.chars().all(|c| c.is_alphanumeric() || c == '-' || c == '_') {
        return Err("Display name can only contain alphanumerics (a-z, A-Z, 0-9), hyphens (-), and underscores (_)".to_string());
    }
    
    Ok(())
}

fn load_or_create_config() -> ControllerConfig {
    if !Path::new(CONFIG_FILE).exists() {
        println!("Config file not found, creating new config");
        return ControllerConfig::new();
    }
    
    match fs::read_to_string(CONFIG_FILE) {
        Ok(contents) => {
            match serde_yaml_ng::from_str::<ControllerConfig>(&contents) {
                Ok(config) => {
                    println!("Successfully loaded config with {} buckets and {} manifests", 
                             config.buckets.len(), config.manifests.len());
                    config
                }
                Err(e) => {
                    eprintln!("Error parsing config file: {}", e);
                    eprintln!("Creating new empty config. Please check your controller_config.yaml format.");
                    ControllerConfig::new()
                }
            }
        }
        Err(e) => {
            eprintln!("Error reading config file: {}", e);
            ControllerConfig::new()
        }
    }
}

fn save_config(config: &ControllerConfig) -> Result<(), String> {
    let yaml = serde_yaml_ng::to_string(config).map_err(|e| e.to_string())?;
    fs::write(CONFIG_FILE, yaml).map_err(|e| e.to_string())?;
    Ok(())
}

struct HtmlTemplate<T>(T);

impl<T> IntoResponse for HtmlTemplate<T>
where
    T: Template,
{
    fn into_response(self) -> Response {
        match self.0.render() {
            Ok(html) => Html(html).into_response(),
            Err(err) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to render template. Error: {}", err),
            )
                .into_response(),
        }
    }
}
