use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskConfig {
    pub urls: Vec<String>,
    #[serde(default = "default_threads")]
    pub max_threads: usize,
    #[serde(default = "default_split", deserialize_with = "deserialize_size")]
    pub max_split: u64,
    #[serde(default)]
    pub cache: bool,
    #[serde(default)]
    pub headers: HashMap<String, String>,
    #[serde(default)]
    pub name: Option<String>,
}

fn default_threads() -> usize {
    8
}

fn default_split() -> u64 {
    5 * 1024 * 1024
}

fn deserialize_size<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error;
    let v = serde_json::Value::deserialize(deserializer)?;
    match v {
        serde_json::Value::Number(n) => n.as_u64().ok_or_else(|| Error::custom("invalid number")),
        serde_json::Value::String(s) => parse_size(&s).map_err(Error::custom),
        _ => Err(Error::custom("expected number or string for size")),
    }
}

pub fn parse_size(s: &str) -> Result<u64, String> {
    let s = s.trim();
    if s.is_empty() {
        return Err("empty size string".to_string());
    }
    let (num_part, unit) = s
        .find(|c: char| c.is_ascii_alphabetic())
        .map(|i| (&s[..i], &s[i..]))
        .unwrap_or((s, ""));
    let num: f64 = num_part
        .trim()
        .parse()
        .map_err(|e: std::num::ParseFloatError| e.to_string())?;
    let mult: f64 = match unit.trim().to_ascii_uppercase().as_str() {
        "" | "B" => 1.0,
        "K" | "KB" => 1024.0,
        "M" | "MB" => 1024.0 * 1024.0,
        "G" | "GB" => 1024.0 * 1024.0 * 1024.0,
        other => return Err(format!("unknown size unit: {other}")),
    };
    Ok((num * mult) as u64)
}

#[derive(Debug, Clone, Serialize)]
pub struct TaskInfo {
    pub task_id: String,
    pub proxy_url: String,
    pub config: TaskConfig,
    pub created_at: u64,
    pub bytes_served: u64,
    pub active_connections: u32,
}

#[derive(Debug)]
pub struct TaskEntry {
    pub config: TaskConfig,
    pub created_at: u64,
    pub bytes_served: std::sync::atomic::AtomicU64,
    pub active_connections: std::sync::atomic::AtomicU32,
}

impl TaskEntry {
    pub fn new(config: TaskConfig) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        Self {
            config,
            created_at: now,
            bytes_served: std::sync::atomic::AtomicU64::new(0),
            active_connections: std::sync::atomic::AtomicU32::new(0),
        }
    }
}

#[derive(Clone)]
pub struct AppState {
    pub tasks: Arc<RwLock<HashMap<String, Arc<TaskEntry>>>>,
    pub bind_addr: String,
}

impl AppState {
    pub fn new(bind_addr: String) -> Self {
        Self {
            tasks: Arc::new(RwLock::new(HashMap::new())),
            bind_addr,
        }
    }

    pub fn insert(&self, id: String, entry: Arc<TaskEntry>) {
        self.tasks.write().insert(id, entry);
    }

    pub fn get(&self, id: &str) -> Option<Arc<TaskEntry>> {
        self.tasks.read().get(id).cloned()
    }

    pub fn remove(&self, id: &str) -> Option<Arc<TaskEntry>> {
        self.tasks.write().remove(id)
    }

    pub fn list(&self) -> Vec<TaskInfo> {
        let guard = self.tasks.read();
        guard
            .iter()
            .map(|(id, entry)| TaskInfo {
                task_id: id.clone(),
                proxy_url: format!("http://{}/stream/{}", self.bind_addr, id),
                config: entry.config.clone(),
                created_at: entry.created_at,
                bytes_served: entry.bytes_served.load(std::sync::atomic::Ordering::Relaxed),
                active_connections: entry
                    .active_connections
                    .load(std::sync::atomic::Ordering::Relaxed),
            })
            .collect()
    }
}

pub fn short_id() -> String {
    let uuid = uuid::Uuid::new_v4();
    let bytes = uuid.as_bytes();
    let mut out = String::with_capacity(8);
    let alphabet: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    for &b in &bytes[..6] {
        out.push(alphabet[(b as usize) % alphabet.len()] as char);
    }
    out
}
