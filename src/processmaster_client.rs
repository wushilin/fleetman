use serde::Deserialize;
use std::path::{Path, PathBuf};
use std::process::Command;

#[derive(Debug, Clone)]
pub struct ProcessMasterClient {
    pub pmctl_path: PathBuf,
    pub sock: Option<String>,
}

impl ProcessMasterClient {
    pub fn new(pmctl_path: PathBuf, sock: Option<String>) -> Self {
        Self { pmctl_path, sock }
    }

    fn cmd(&self) -> Command {
        let mut c = Command::new(&self.pmctl_path);
        if let Some(sock) = &self.sock {
            c.arg("-s").arg(sock);
        }
        c
    }

    pub fn update(&self) -> anyhow::Result<()> {
        let out = self.cmd().arg("update").output()?;
        if !out.status.success() {
            anyhow::bail!(
                "pmctl update failed: {}",
                String::from_utf8_lossy(&out.stderr)
            );
        }
        Ok(())
    }

    pub fn restart(&self, app: &str) -> anyhow::Result<()> {
        let out = self.cmd().arg("restart").arg(app).output()?;
        if !out.status.success() {
            anyhow::bail!(
                "pmctl restart {} failed: {}",
                app,
                String::from_utf8_lossy(&out.stderr)
            );
        }
        Ok(())
    }

    pub fn stop(&self, app: &str) -> anyhow::Result<()> {
        let out = self.cmd().arg("stop").arg(app).output()?;
        if !out.status.success() {
            anyhow::bail!(
                "pmctl stop {} failed: {}",
                app,
                String::from_utf8_lossy(&out.stderr)
            );
        }
        Ok(())
    }

    pub fn start(&self, app: &str) -> anyhow::Result<()> {
        let out = self.cmd().arg("start").arg(app).output()?;
        if !out.status.success() {
            anyhow::bail!(
                "pmctl start {} failed: {}",
                app,
                String::from_utf8_lossy(&out.stderr)
            );
        }
        Ok(())
    }

    pub fn status_json(&self, app: &str) -> anyhow::Result<PmctlStatusResponse> {
        let out = self
            .cmd()
            .arg("status")
            .arg(app)
            .arg("--format")
            .arg("json")
            .output()?;
        if !out.status.success() {
            anyhow::bail!(
                "pmctl status {} --format json failed: {}",
                app,
                String::from_utf8_lossy(&out.stderr)
            );
        }
        let v: PmctlStatusResponse = serde_json::from_slice(&out.stdout)?;
        Ok(v)
    }
}

#[derive(Debug, Deserialize)]
pub struct PmctlStatusResponse {
    pub ok: bool,
    #[serde(default)]
    pub message: String,
    #[serde(default)]
    pub statuses: Vec<PmctlAppStatus>,
}

#[derive(Debug, Deserialize)]
pub struct PmctlAppStatus {
    pub application: String,
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub running: bool,
    #[serde(default)]
    pub actual: Option<String>,
    #[serde(default)]
    pub phase: Option<String>,
    #[serde(default)]
    pub pids: Vec<i64>,
    #[serde(default)]
    pub last_exit_code: Option<i64>,
    #[serde(default)]
    pub working_directory: Option<String>,
    #[serde(default)]
    pub source_file: Option<String>,
    #[serde(default)]
    pub last_run_at_ms: Option<i64>,
}

pub fn ensure_dir_exists(path: &Path) -> anyhow::Result<()> {
    std::fs::create_dir_all(path)?;
    Ok(())
}


