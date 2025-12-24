use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentState {
    pub cells: HashMap<String, CellState>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CellState {
    pub cell_id: String,
    pub manifest_name: String,
    pub profile: String,              // "supervisor" or "systemd"
    pub current_version: Option<String>,
    pub previous_version: Option<String>,
    pub trigger_timestamp: Option<u64>, // Timestamp of last processed trigger
    pub last_deployment_status: DeploymentStatus,
    pub last_deployment_time: Option<u64>,
    /// Current runtime status as observed on the host.
    /// - Some(true): running
    /// - Some(false): not running
    /// - None: not applicable / unknown (e.g., deploy profile)
    #[serde(default)]
    pub running: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum DeploymentStatus {
    Success,
    Failed,
    InProgress,
    Unknown,
}

impl AgentState {
    pub fn new() -> Self {
        AgentState {
            cells: HashMap::new(),
        }
    }

    pub fn load_from_file(path: &PathBuf) -> anyhow::Result<Self> {
        if !path.exists() {
            return Ok(Self::new());
        }

        let contents = fs::read_to_string(path)?;
        let state: AgentState = serde_json::from_str(&contents)?;
        Ok(state)
    }

    pub fn save_to_file(&self, path: &PathBuf) -> anyhow::Result<()> {
        let contents = serde_json::to_string_pretty(self)?;
        fs::write(path, contents)?;
        Ok(())
    }

    pub fn update_cell(&mut self, cell_id: String, cell_state: CellState) {
        self.cells.insert(cell_id, cell_state);
    }

    pub fn get_cell(&self, cell_id: &str) -> Option<&CellState> {
        self.cells.get(cell_id)
    }
}

pub type SharedState = Arc<Mutex<AgentState>>;


