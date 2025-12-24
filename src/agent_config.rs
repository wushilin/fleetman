use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct AgentConfig {
    pub global: GlobalConfig,
    pub object_storage: ObjectStorageConfig,
    pub supervisor: Option<SupervisorConfig>,
    pub systemd: Option<SystemdConfig>,
    pub deploy: Option<DeployConfig>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DeployConfig {
    pub target_folder: String,  // e.g., /opt/deployed
}

#[derive(Debug, Deserialize, Clone)]
pub struct GlobalConfig {
    pub node_id: String,
    pub check_interval_ms: u64,
    pub log_directory: Option<String>,
    pub log_file_prefix: Option<String>,
    #[serde(default = "default_log_rotation")]
    pub log_rotation: String, // "daily", "hourly", "never"
    #[serde(default = "default_s3_retry_count")]
    pub s3_retry_count: u32,
    #[serde(default = "default_s3_retry_delay_ms")]
    pub s3_retry_delay_ms: u64,
}

fn default_log_rotation() -> String {
    "daily".to_string()
}

fn default_s3_retry_count() -> u32 {
    3
}

fn default_s3_retry_delay_ms() -> u64 {
    1000
}

#[derive(Debug, Deserialize, Clone)]
pub struct ObjectStorageConfig {
    pub endpoint: String,
    pub access_key: String,
    pub secret_key: String,
    pub bucket: String,
    pub path_style: Option<bool>,
    pub prefix: String,  // Required: must contain only alphanumerics, hyphens, and underscores
}

impl ObjectStorageConfig {
    pub fn full_key(&self, key: &str) -> String {
        format!("{}/{}", self.prefix, key)
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct SupervisorConfig {
    pub conf_d_folder: String,        // e.g., /etc/supervisor/conf.d
    pub service_folder: String,        // e.g., /opt/services
    pub template_path: String,         // Path to supervisor config template
    pub supervisorctl_path: Option<String>, // Default: /usr/bin/supervisorctl
}

impl SupervisorConfig {
    pub fn supervisorctl_path(&self) -> String {
        self.supervisorctl_path
            .clone()
            .unwrap_or_else(|| "/usr/bin/supervisorctl".to_string())
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct SystemdConfig {
    pub service_folder: String,        // e.g., /etc/systemd/system
    pub working_directory: String,     // e.g., /opt/services
    pub template_path: String,         // Path to systemd service template
    pub systemctl_path: Option<String>, // Default: /usr/bin/systemctl
}

impl SystemdConfig {
    pub fn systemctl_path(&self) -> String {
        self.systemctl_path
            .clone()
            .unwrap_or_else(|| "/usr/bin/systemctl".to_string())
    }
}


