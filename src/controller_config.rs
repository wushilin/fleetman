use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ControllerConfig {
    pub buckets: HashMap<String, BucketConfig>,
    pub manifests: HashMap<String, DeploymentManifest>,
    #[serde(default)]
    pub cells: HashMap<String, CellConfig>,
    #[serde(default)]
    pub resource_profiles: HashMap<String, ResourceProfile>,
    #[serde(default = "default_s3_retry_count")]
    pub s3_retry_count: u32,
    #[serde(default = "default_s3_retry_delay_ms")]
    pub s3_retry_delay_ms: u64,
}

fn default_s3_retry_count() -> u32 {
    3
}

fn default_s3_retry_delay_ms() -> u64 {
    1000
}

impl ControllerConfig {
    pub fn new() -> Self {
        Self {
            buckets: HashMap::new(),
            manifests: HashMap::new(),
            cells: HashMap::new(),
            resource_profiles: HashMap::new(),
            s3_retry_count: default_s3_retry_count(),
            s3_retry_delay_ms: default_s3_retry_delay_ms(),
        }
    }
}

/// Resource restrictions shared across profiles (systemd + processmaster).
/// Empty fields mean "unlimited" / "not set".
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct ResourceProfile {
    #[serde(default)]
    pub cpu: Option<String>,
    #[serde(default)]
    pub memory: Option<String>,
    #[serde(default)]
    pub memory_swap: Option<String>,
    #[serde(default)]
    pub io_weight: Option<u32>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BucketConfig {
    pub bucket_name: String,
    pub endpoint: String,
    pub access_key: String,
    pub secret_key: String,
    pub path_style: bool,
    pub prefix: String,  // Required: must contain only alphanumerics, hyphens, and underscores
}

impl BucketConfig {
    /// Validate prefix format
    /// - Required (cannot be empty)
    /// - Must contain only alphanumerics (a-z, A-Z, 0-9), hyphens (-), and underscores (_)
    pub fn validate_prefix(prefix: &str) -> Result<(), String> {
        if prefix.is_empty() {
            return Err("Prefix is required and cannot be empty".to_string());
        }
        
        // Check that prefix contains only allowed characters
        if !prefix.chars().all(|c| c.is_alphanumeric() || c == '-' || c == '_') {
            return Err("Prefix can only contain alphanumerics (a-z, A-Z, 0-9), hyphens (-), and underscores (_)".to_string());
        }
        
        Ok(())
    }
    
    /// Get the full S3 key with prefix prepended
    pub fn full_key(&self, key: &str) -> String {
        format!("{}/{}", self.prefix, key)
    }
    
    /// Strip the prefix from a full S3 key to get the relative key
    /// Returns the stripped key
    pub fn strip_prefix<'a>(&self, full_key: &'a str) -> Option<&'a str> {
        let prefix_with_slash = format!("{}/", self.prefix);
        full_key.strip_prefix(&prefix_with_slash)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DeploymentManifest {
    pub files: Vec<ManifestFile>,
    pub profile: DeploymentProfile,
    #[serde(default = "default_resource_profile")]
    pub resource_profile: String,
    #[serde(default = "default_service_owner")]
    pub service_owner: String,
    #[serde(default = "default_service_group")]
    pub service_group: String,
    #[serde(default = "default_service_folder_mode")]
    pub service_folder_mode: String,
    /// Optional per-service-folder ownership override. If not set, defaults to service_owner/service_group.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_folder_owner: Option<String>,
    /// Optional per-service-folder group override. If not set, defaults to service_owner/service_group.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_folder_group: Option<String>,
    pub bucket: String,  // Reference to bucket name in config
    #[serde(default = "default_collect_logs")]
    pub collect_logs: bool,
    #[serde(default = "default_log_files")]
    pub log_files: Vec<String>,
    #[serde(default = "default_max_log_lines")]
    pub max_log_lines: usize,
}

fn default_resource_profile() -> String {
    "unlimited".to_string()
}

fn default_service_folder_mode() -> String {
    "0700".to_string()
}

fn default_service_owner() -> String {
    "root".to_string()
}

fn default_service_group() -> String {
    "root".to_string()
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

impl DeploymentManifest {
    /// Create a snapshot of this manifest for a specific deployment
    pub fn to_snapshot(&self, manifest_name: &str, version: &str) -> ManifestSnapshot {
        let service_folder_owner = self
            .service_folder_owner
            .as_deref()
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string());
        let service_folder_group = self
            .service_folder_group
            .as_deref()
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string());
        ManifestSnapshot {
            manifest_name: manifest_name.to_string(),
            version: version.to_string(),
            files: self.files.clone(),
            profile: self.profile.clone(),
            resource_profile: self.resource_profile.clone(),
            service_owner: self.service_owner.clone(),
            service_group: self.service_group.clone(),
            service_folder_mode: self.service_folder_mode.clone(),
            service_folder_owner,
            service_folder_group,
            bucket: self.bucket.clone(),
            created_at: chrono::Utc::now().to_rfc3339(),
            collect_logs: self.collect_logs,
            log_files: self.log_files.clone(),
            max_log_lines: self.max_log_lines,
        }
    }
}

/// Snapshot of a manifest at deployment creation time
/// This is saved as manifest.json in each deployment version
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ManifestSnapshot {
    pub manifest_name: String,
    pub version: String,
    pub files: Vec<ManifestFile>,
    pub profile: DeploymentProfile,
    #[serde(default = "default_resource_profile")]
    pub resource_profile: String,
    #[serde(default = "default_service_folder_mode")]
    pub service_folder_mode: String,
    #[serde(default = "default_service_owner")]
    pub service_owner: String,
    #[serde(default = "default_service_group")]
    pub service_group: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_folder_owner: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_folder_group: Option<String>,
    pub bucket: String,
    pub created_at: String,  // ISO 8601 timestamp
    #[serde(default = "default_collect_logs")]
    pub collect_logs: bool,
    #[serde(default = "default_log_files")]
    pub log_files: Vec<String>,
    #[serde(default = "default_max_log_lines")]
    pub max_log_lines: usize,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ManifestFile {
    pub path: String,
    pub file_type: FileType,
    /// Optional owner override for this file. If not set, inherits from service folder owner.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
    /// Optional group override for this file. If not set, inherits from service folder group.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    /// Optional mode override for this file. If not set, defaults are applied by the agent
    /// (run.sh + folders: 0700; other files: 0600).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum FileType {
    Text,
    Binary,
    Folder,
}

impl std::fmt::Display for FileType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FileType::Text => write!(f, "text"),
            FileType::Binary => write!(f, "binary"),
            FileType::Folder => write!(f, "folder"),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum DeploymentProfile {
    Supervisor,
    Systemd,
    ProcessMaster,
    Deploy,
}

impl std::fmt::Display for DeploymentProfile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DeploymentProfile::Supervisor => write!(f, "supervisor"),
            DeploymentProfile::Systemd => write!(f, "systemd"),
            DeploymentProfile::ProcessMaster => write!(f, "processmaster"),
            DeploymentProfile::Deploy => write!(f, "deploy"),
        }
    }
}

impl std::str::FromStr for DeploymentProfile {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "supervisor" => Ok(DeploymentProfile::Supervisor),
            "systemd" => Ok(DeploymentProfile::Systemd),
            "processmaster" => Ok(DeploymentProfile::ProcessMaster),
            "deploy" => Ok(DeploymentProfile::Deploy),
            _ => Err(format!(
                "Unknown profile: {}. Must be 'supervisor', 'systemd', 'processmaster', or 'deploy'",
                s
            )),
        }
    }
}

// Cell Management
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CellConfig {
    pub node_id: String,
    pub cell_id: String,
    pub bucket: String,
    pub manifest: String,  // References manifest by KEY (HashMap key)
}

impl CellConfig {
    pub fn full_name(&self) -> String {
        format!("{}/{}", self.node_id, self.cell_id)
    }
}

