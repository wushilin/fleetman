mod controller_config;

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
use aws_sdk_s3::{config::Builder, config::Credentials, config::Region, primitives::ByteStream, Client};
use chrono::Local;
use controller_config::{BucketConfig, CellConfig, ControllerConfig, DeploymentManifest, DeploymentProfile, FileType, ManifestFile};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::{sleep, Duration};
use tower_http::trace::TraceLayer;
use futures::future::join_all;

type AppState = Arc<RwLock<ControllerConfig>>;

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
                    println!("✓ S3 operation '{}' succeeded on attempt {}/{}", 
                             operation_name, attempts, max_attempts);
                }
                return Ok(result);
            }
            Err(e) => {
                // Check if error is retriable
                let retriable = is_retriable_s3_error(&e);
                
                if !retriable {
                    // Non-retriable error (404, 403, etc.) - fail immediately without retry
                    return Err(e);
                }
                
                if attempts >= max_attempts {
                    eprintln!("✗ S3 operation '{}' failed after {} attempts: {:?}", 
                             operation_name, attempts, e);
                    return Err(e);
                }
                
                eprintln!("⚠ S3 operation '{}' failed on attempt {}/{}: {:?}. Retrying in {}ms...", 
                         operation_name, attempts, max_attempts, e, retry_delay_ms);
                sleep(Duration::from_millis(retry_delay_ms)).await;
            }
        }
    }
}

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
}

#[derive(Template)]
#[template(path = "edit_manifest.html")]
struct EditManifestTemplate {
    manifest_name: String,
    manifest: DeploymentManifest,
    buckets: Vec<(String, BucketConfig)>,
    selected_bucket: String,
}

#[derive(Template)]
#[template(path = "deployments.html")]
struct DeploymentsTemplate {
    existing_deployments: Vec<ExistingDeployment>,
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
        .route("/deployments/delete", post(delete_file))
        .route("/deployments/delete_version", post(delete_version))
        .route("/deployments/finish", post(finish_deployment))
        .route("/deployments/bulk-publish", post(bulk_publish_deployment))
        .route("/deployments/staging/browse", get(browse_staging))
        .route("/deployments/staging/copy", post(copy_from_staging))
        .route("/api/manifest-versions", get(get_manifest_versions))
        .route("/api/app-versions", get(get_app_versions))
        .route("/api/app-cells", get(get_app_cells))
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
    
    let template = ManifestsTemplate { manifests, buckets };
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
    
    let owners: Vec<String> = form.owners
        .lines()
        .map(|s| s.trim().to_string())
        .map(|s| if s.is_empty() { "root".to_string() } else { s })
        .collect();
    
    let groups: Vec<String> = form.groups
        .lines()
        .map(|s| s.trim().to_string())
        .map(|s| if s.is_empty() { "root".to_string() } else { s })
        .collect();
    
    let modes: Vec<String> = form.modes
        .lines()
        .map(|s| s.trim().to_string())
        .map(|s| if s.is_empty() { "0600".to_string() } else { s })
        .collect();
    
    let mut files: Vec<ManifestFile> = file_paths.iter()
        .zip(file_types.iter())
        .enumerate()
        .map(|(i, (path, ftype))| ManifestFile {
            path: path.clone(),
            file_type: match ftype.to_lowercase().as_str() {
                "text" => FileType::Text,
                "folder" => FileType::Folder,
                _ => FileType::Binary,
            },
            owner: owners.get(i).cloned().unwrap_or_else(|| "root".to_string()),
            group: groups.get(i).cloned().unwrap_or_else(|| "root".to_string()),
            mode: modes.get(i).cloned().unwrap_or_else(|| "0600".to_string()),
        })
        .collect();
    
    // Auto-set mode to 0700 for shell scripts (.sh, .bash)
    for file in &mut files {
        if file.path.ends_with(".sh") || file.path.ends_with(".bash") {
            file.mode = "0700".to_string();
        }
    }
    
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
        "deploy" => DeploymentProfile::Deploy,
        _ => DeploymentProfile::Systemd,
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
    
    let template = EditManifestTemplate {
        manifest_name: query.name,
        manifest,
        buckets,
        selected_bucket,
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
    
    let owners: Vec<String> = form.owners
        .lines()
        .map(|s| s.trim().to_string())
        .map(|s| if s.is_empty() { "root".to_string() } else { s })
        .collect();
    
    let groups: Vec<String> = form.groups
        .lines()
        .map(|s| s.trim().to_string())
        .map(|s| if s.is_empty() { "root".to_string() } else { s })
        .collect();
    
    let modes: Vec<String> = form.modes
        .lines()
        .map(|s| s.trim().to_string())
        .map(|s| if s.is_empty() { "0600".to_string() } else { s })
        .collect();
    
    let mut files: Vec<ManifestFile> = file_paths.iter()
        .zip(file_types.iter())
        .enumerate()
        .map(|(i, (path, ftype))| ManifestFile {
            path: path.clone(),
            file_type: match ftype.to_lowercase().as_str() {
                "text" => FileType::Text,
                "folder" => FileType::Folder,
                _ => FileType::Binary,
            },
            owner: owners.get(i).cloned().unwrap_or_else(|| "root".to_string()),
            group: groups.get(i).cloned().unwrap_or_else(|| "root".to_string()),
            mode: modes.get(i).cloned().unwrap_or_else(|| "0600".to_string()),
        })
        .collect();
    
    // Auto-set mode to 0700 for shell scripts (.sh, .bash)
    for file in &mut files {
        if file.path.ends_with(".sh") || file.path.ends_with(".bash") {
            file.mode = "0700".to_string();
        }
    }
    
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
        "deploy" => DeploymentProfile::Deploy,
        _ => DeploymentProfile::Systemd,
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
    let (profile, source_bucket_name, source_bucket_config) = {
        let config = state.read().unwrap();
        match config.manifests.get(&form.manifest) {
            Some(manifest) => {
                let profile = manifest.profile.to_string();
                match config.buckets.get(&manifest.bucket) {
                    Some(bc) => (profile, bc.bucket_name.clone(), bc.clone()),
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
            
            // Snapshot overrides manifest to versions/<ver>/overrides_manifest.json (best-effort)
            let overrides_manifest_src_base = format!("cells/{}/{}/overrides_manifest.json", node_id, cell_id);
            let overrides_manifest_src = cell_bucket_config.full_key(&overrides_manifest_src_base);
            let overrides_manifest_dest_base = format!("{}overrides_manifest.json", dest_prefix_base);
            let overrides_manifest_dest = cell_bucket_config.full_key(&overrides_manifest_dest_base);
            let copy_source = format!("{}/{}", cell_bucket_name, overrides_manifest_src);
            let copy_op = format!("copy_overrides_manifest_to_version: {} -> {}/{}", copy_source, cell_bucket_name, overrides_manifest_dest);
            let _ = retry_s3_operation(
                &copy_op,
                retry_count,
                retry_delay_ms,
                || {
                    let client = dest_client.clone();
                    let bucket = cell_bucket_name.clone();
                    let dest = overrides_manifest_dest.clone();
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

async fn create_s3_client(config: &BucketConfig) -> Client {
    let credentials = Credentials::new(
        &config.access_key,
        &config.secret_key,
        None,
        None,
        "static",
    );
    
    let s3_config = Builder::new()
        .region(Region::new("us-east-1"))
        .endpoint_url(&config.endpoint)
        .credentials_provider(credentials)
        .force_path_style(config.path_style)
        .behavior_version_latest()
        .build();
    
    Client::from_conf(s3_config)
}

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
    let mut config = state.write().unwrap();
    config.cells.remove(&form.cell_key);
    
    if let Err(e) = save_config(&config) {
        eprintln!("Failed to save config: {}", e);
    }
    
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

// Default values for override files without manifest metadata
const DEFAULT_OVERRIDE_OWNER: &str = "root";
const DEFAULT_OVERRIDE_GROUP: &str = "root";
const DEFAULT_OVERRIDE_MODE_FILE: &str = "0644";
const DEFAULT_OVERRIDE_MODE_FOLDER: &str = "0755";

#[derive(Debug, Serialize, Deserialize, Clone)]
struct CellOverridesManifest {
    files: Vec<CellOverrideFile>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct CellOverrideFile {
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
    owner: String,
    group: String,
    mode: String,
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
                        
                        // Get metadata from manifest or use defaults
                        let (file_type, owner, group, mode) = if let Some(meta) = metadata_map.get(&relative_path) {
                            (meta.file_type.to_string(), meta.owner.clone(), meta.group.clone(), meta.mode.clone())
                        } else {
                            // Default values if not in manifest
                            let ft = if is_folder { "folder" } else { "binary" };
                            let mode = if is_folder { DEFAULT_OVERRIDE_MODE_FOLDER } else { DEFAULT_OVERRIDE_MODE_FILE };
                            (ft.to_string(), DEFAULT_OVERRIDE_OWNER.to_string(), DEFAULT_OVERRIDE_GROUP.to_string(), mode.to_string())
                        };
                        
                        overrides.push(OverrideFileInfo {
                            path: relative_path.clone(),
                            size: obj.size.unwrap_or(0),
                            last_modified: obj.last_modified
                                .map(|dt| dt.fmt(aws_smithy_types::date_time::Format::DateTime).unwrap_or_default())
                                .unwrap_or_default(),
                            is_folder,
                            file_type,
                            owner,
                            group,
                            mode,
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
    let mut owner = String::from("root");
    let mut group = String::from("root");
    let mut mode = String::from("0644");
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
    
    // Remove existing entry if present
    manifest.files.retain(|f| f.path != normalized_file_path);
    
    // Add new entry
    manifest.files.push(CellOverrideFile {
        path: normalized_file_path.clone(),
        file_type: ft,
        owner: owner.clone(),
        group: group.clone(),
        mode: mode.clone(),
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
    owner: String,
    group: String,
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
    
    // Find and update existing entry, or add new one
    if let Some(existing) = manifest.files.iter_mut().find(|f| f.path == form.file_path) {
        existing.file_type = ft;
        existing.owner = form.owner.clone();
        existing.group = form.group.clone();
        existing.mode = form.mode.clone();
    } else {
        // File not in manifest yet (legacy file), add it
        manifest.files.push(CellOverrideFile {
            path: form.file_path.clone(),
            file_type: ft,
            owner: form.owner.clone(),
            group: form.group.clone(),
            mode: form.mode.clone(),
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
    let (source_manifest_name, source_bucket_name, source_bucket_config, 
         cell_bucket_name, cell_bucket_config, retry_count, retry_delay_ms, profile) = {
        let config = state.read().unwrap();
        let cell_key = format!("{}/{}", node_id, cell_id);
        match config.cells.get(&cell_key) {
            Some(cell) => {
                // Get manifest by direct lookup using manifest key
                let manifest = match config.manifests.get(&cell.manifest) {
                    Some(m) => m,
                    None => return Html(format!("<h1>Manifest not found: {}</h1>", cell.manifest)).into_response(),
                };
                    
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
                                config.s3_retry_count, config.s3_retry_delay_ms, profile),
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

        // Snapshot overrides manifest to versions/<ver>/overrides_manifest.json (best-effort)
        let overrides_manifest_src_base = format!("cells/{}/{}/overrides_manifest.json", node_id, cell_id);
        let overrides_manifest_src = cell_bucket_config.full_key(&overrides_manifest_src_base);
        let overrides_manifest_dest_base = format!("{}overrides_manifest.json", dest_prefix_base);
        let overrides_manifest_dest = cell_bucket_config.full_key(&overrides_manifest_dest_base);
        let copy_source = format!("{}/{}", cell_bucket_name, overrides_manifest_src);
        let copy_op = format!("copy_overrides_manifest_to_version: {} -> {}/{}", copy_source, cell_bucket_name, overrides_manifest_dest);
        let _ = retry_s3_operation(
            &copy_op,
            retry_count,
            retry_delay_ms,
            || {
                let client = dest_client.clone();
                let bucket = cell_bucket_name.clone();
                let dest = overrides_manifest_dest.clone();
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
