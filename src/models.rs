use crate::cache::CacheStats;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
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

fn deserialize_opt_size<'de, D>(deserializer: D) -> Result<Option<u64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error;
    let v = Option::<serde_json::Value>::deserialize(deserializer)?;
    match v {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::Number(n)) => n
            .as_u64()
            .map(Some)
            .ok_or_else(|| Error::custom("invalid number")),
        Some(serde_json::Value::String(s)) => parse_size(&s).map(Some).map_err(Error::custom),
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

/// Partial update payload for PATCH /api/tasks/:id.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct TaskUpdate {
    pub urls: Option<Vec<String>>,
    pub max_threads: Option<usize>,
    #[serde(default, deserialize_with = "deserialize_opt_size")]
    pub max_split: Option<u64>,
    pub cache: Option<bool>,
    pub headers: Option<HashMap<String, String>>,
    pub name: Option<Option<String>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TaskInfo {
    pub task_id: String,
    pub proxy_url: String,
    pub config: TaskConfig,
    pub created_at: u64,
    pub bytes_served: u64,
    pub active_connections: u32,
    pub paused: bool,
    pub cache: Option<CacheStats>,
}

#[derive(Debug)]
pub struct TaskEntry {
    pub config: RwLock<TaskConfig>,
    pub created_at: u64,
    pub bytes_served: AtomicU64,
    pub active_connections: AtomicU32,
    pub paused: AtomicBool,
}

impl TaskEntry {
    pub fn new(config: TaskConfig) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        Self {
            config: RwLock::new(config),
            created_at: now,
            bytes_served: AtomicU64::new(0),
            active_connections: AtomicU32::new(0),
            paused: AtomicBool::new(false),
        }
    }

    pub fn config_snapshot(&self) -> TaskConfig {
        self.config.read().clone()
    }

    pub fn apply_update(&self, upd: TaskUpdate) -> std::result::Result<(), String> {
        let mut cfg = self.config.write();
        if let Some(urls) = upd.urls {
            if urls.is_empty() {
                return Err("urls must not be empty".into());
            }
            cfg.urls = urls;
        }
        if let Some(t) = upd.max_threads {
            if t == 0 {
                return Err("max_threads must be >= 1".into());
            }
            cfg.max_threads = t;
        }
        if let Some(s) = upd.max_split {
            if s < 64 * 1024 {
                return Err("max_split must be >= 64K".into());
            }
            cfg.max_split = s;
        }
        if let Some(c) = upd.cache {
            cfg.cache = c;
        }
        if let Some(h) = upd.headers {
            cfg.headers = h;
        }
        if let Some(n) = upd.name {
            cfg.name = n;
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct AppState {
    pub tasks: Arc<RwLock<HashMap<String, Arc<TaskEntry>>>>,
    pub bind_addr: String,
    pub cache: Arc<crate::cache::CacheStore>,
}

impl AppState {
    pub fn new(bind_addr: String, cache: Arc<crate::cache::CacheStore>) -> Self {
        Self {
            tasks: Arc::new(RwLock::new(HashMap::new())),
            bind_addr,
            cache,
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

    pub fn task_info(&self, id: &str, entry: &TaskEntry) -> TaskInfo {
        let cfg = entry.config_snapshot();
        let cache = if cfg.cache {
            let key = crate::cache::CacheStore::key_for_urls(&cfg.urls);
            self.cache.stats(&key)
        } else {
            None
        };
        TaskInfo {
            task_id: id.to_string(),
            proxy_url: format!("http://{}/stream/{}", self.bind_addr, id),
            config: cfg,
            created_at: entry.created_at,
            bytes_served: entry.bytes_served.load(Ordering::Relaxed),
            active_connections: entry.active_connections.load(Ordering::Relaxed),
            paused: entry.paused.load(Ordering::Relaxed),
            cache,
        }
    }

    pub fn list(&self) -> Vec<TaskInfo> {
        let guard = self.tasks.read();
        guard
            .iter()
            .map(|(id, entry)| self.task_info(id, entry))
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
