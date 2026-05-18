use crate::cache::CacheStats;
use crate::ratelimit::TokenBucket;
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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
    /// Per-task rate limit in bytes/sec. 0 = unlimited.
    #[serde(default, deserialize_with = "deserialize_opt_size_default_zero")]
    pub rate_limit_bps: u64,
    /// Persist this task across restarts.
    #[serde(default)]
    pub persist: bool,
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

fn deserialize_opt_size_default_zero<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(deserialize_opt_size(deserializer)?.unwrap_or(0))
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

#[derive(Debug, Clone, Deserialize, Default)]
pub struct TaskUpdate {
    pub urls: Option<Vec<String>>,
    pub max_threads: Option<usize>,
    #[serde(default, deserialize_with = "deserialize_opt_size")]
    pub max_split: Option<u64>,
    pub cache: Option<bool>,
    pub headers: Option<HashMap<String, String>>,
    pub name: Option<Option<String>>,
    #[serde(default, deserialize_with = "deserialize_opt_size")]
    pub rate_limit_bps: Option<u64>,
    pub persist: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct UrlHealth {
    pub url: String,
    pub last_status: Option<u16>,
    pub last_error: Option<String>,
    pub last_latency_ms: Option<u64>,
    pub bytes_contributed: u64,
    pub successful_requests: u64,
    pub failed_requests: u64,
    pub current_speed_bps: u64,
    pub last_used_at: Option<u64>,
}

#[derive(Debug)]
pub struct UrlHealthAcc {
    pub url: String,
    pub last_status: parking_lot::Mutex<Option<u16>>,
    pub last_error: parking_lot::Mutex<Option<String>>,
    pub last_latency_ms: AtomicU64,
    pub bytes_contributed: AtomicU64,
    pub successful_requests: AtomicU64,
    pub failed_requests: AtomicU64,
    pub last_used_at: AtomicU64,
    /// Recent-window bytes counter consumed by the throughput sampler.
    pub window_bytes: AtomicU64,
    pub current_speed_bps: AtomicU64,
}

impl UrlHealthAcc {
    pub fn new(url: String) -> Self {
        Self {
            url,
            last_status: parking_lot::Mutex::new(None),
            last_error: parking_lot::Mutex::new(None),
            last_latency_ms: AtomicU64::new(0),
            bytes_contributed: AtomicU64::new(0),
            successful_requests: AtomicU64::new(0),
            failed_requests: AtomicU64::new(0),
            last_used_at: AtomicU64::new(0),
            window_bytes: AtomicU64::new(0),
            current_speed_bps: AtomicU64::new(0),
        }
    }

    pub fn snapshot(&self) -> UrlHealth {
        let last_latency_ms = self.last_latency_ms.load(Ordering::Relaxed);
        let used_at = self.last_used_at.load(Ordering::Relaxed);
        UrlHealth {
            url: self.url.clone(),
            last_status: *self.last_status.lock(),
            last_error: self.last_error.lock().clone(),
            last_latency_ms: if last_latency_ms == 0 {
                None
            } else {
                Some(last_latency_ms)
            },
            bytes_contributed: self.bytes_contributed.load(Ordering::Relaxed),
            successful_requests: self.successful_requests.load(Ordering::Relaxed),
            failed_requests: self.failed_requests.load(Ordering::Relaxed),
            current_speed_bps: self.current_speed_bps.load(Ordering::Relaxed),
            last_used_at: if used_at == 0 { None } else { Some(used_at) },
        }
    }
}

/// Ring buffer of recent throughput samples for sparkline rendering.
/// Sample = bytes/sec averaged over `interval`.
#[derive(Debug)]
pub struct ThroughputSampler {
    samples: Mutex<Vec<u64>>,
    capacity: usize,
}

impl ThroughputSampler {
    pub fn new(capacity: usize) -> Self {
        Self {
            samples: Mutex::new(Vec::with_capacity(capacity)),
            capacity,
        }
    }

    pub fn push(&self, bps: u64) {
        let mut s = self.samples.lock();
        if s.len() >= self.capacity {
            s.remove(0);
        }
        s.push(bps);
    }

    pub fn snapshot(&self) -> Vec<u64> {
        self.samples.lock().clone()
    }

    pub fn current(&self) -> u64 {
        *self.samples.lock().last().unwrap_or(&0)
    }
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
    pub url_health: Vec<UrlHealth>,
    pub current_speed_bps: u64,
    pub speed_samples: Vec<u64>,
}

#[derive(Debug)]
pub struct TaskEntry {
    pub config: RwLock<TaskConfig>,
    pub created_at: u64,
    pub bytes_served: AtomicU64,
    pub active_connections: AtomicU32,
    pub paused: AtomicBool,
    /// Per-URL health (slot index matches `config.urls` at creation time;
    /// `apply_update` rebuilds this list whenever URLs change).
    pub url_health: RwLock<Vec<Arc<UrlHealthAcc>>>,
    pub limiter: Arc<TokenBucket>,
    pub throughput: Arc<ThroughputSampler>,
    /// Bytes counted toward the next sampler tick.
    pub window_bytes: AtomicU64,
    pub last_sample: Mutex<Instant>,
}

impl TaskEntry {
    pub fn new(config: TaskConfig) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let url_health = config
            .urls
            .iter()
            .map(|u| Arc::new(UrlHealthAcc::new(u.clone())))
            .collect();
        let limiter = Arc::new(TokenBucket::new(config.rate_limit_bps));
        Self {
            config: RwLock::new(config),
            created_at: now,
            bytes_served: AtomicU64::new(0),
            active_connections: AtomicU32::new(0),
            paused: AtomicBool::new(false),
            url_health: RwLock::new(url_health),
            limiter,
            throughput: Arc::new(ThroughputSampler::new(60)),
            window_bytes: AtomicU64::new(0),
            last_sample: Mutex::new(Instant::now()),
        }
    }

    pub fn config_snapshot(&self) -> TaskConfig {
        self.config.read().clone()
    }

    pub fn url_health_for(&self, url: &str) -> Option<Arc<UrlHealthAcc>> {
        self.url_health
            .read()
            .iter()
            .find(|h| h.url == url)
            .cloned()
    }

    pub fn apply_update(&self, upd: TaskUpdate) -> std::result::Result<(), String> {
        let mut cfg = self.config.write();
        if let Some(urls) = upd.urls {
            if urls.is_empty() {
                return Err("urls must not be empty".into());
            }
            // Preserve health stats for URLs that survived the edit.
            let mut prev: HashMap<String, Arc<UrlHealthAcc>> = self
                .url_health
                .read()
                .iter()
                .map(|h| (h.url.clone(), Arc::clone(h)))
                .collect();
            let new_health: Vec<Arc<UrlHealthAcc>> = urls
                .iter()
                .map(|u| {
                    prev.remove(u)
                        .unwrap_or_else(|| Arc::new(UrlHealthAcc::new(u.clone())))
                })
                .collect();
            *self.url_health.write() = new_health;
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
        if let Some(r) = upd.rate_limit_bps {
            cfg.rate_limit_bps = r;
            self.limiter.set_rate(r);
        }
        if let Some(p) = upd.persist {
            cfg.persist = p;
        }
        Ok(())
    }

    /// Sample bytes_served into the throughput ring. Called from a periodic
    /// background tick (~1 Hz).
    pub fn tick_throughput(&self) {
        let now = Instant::now();
        let mut last = self.last_sample.lock();
        let elapsed = now.duration_since(*last).as_secs_f64().max(0.001);
        *last = now;
        let bytes = self.window_bytes.swap(0, Ordering::Relaxed);
        let bps = (bytes as f64 / elapsed) as u64;
        self.throughput.push(bps);

        // Update each URL's current speed too.
        for h in self.url_health.read().iter() {
            let wb = h.window_bytes.swap(0, Ordering::Relaxed);
            let s = (wb as f64 / elapsed) as u64;
            h.current_speed_bps.store(s, Ordering::Relaxed);
        }
    }

    pub fn count_bytes(&self, n: u64) {
        self.bytes_served.fetch_add(n, Ordering::Relaxed);
        self.window_bytes.fetch_add(n, Ordering::Relaxed);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct GlobalSettings {
    /// 0 = unlimited.
    #[serde(default, deserialize_with = "deserialize_opt_size_default_zero")]
    pub global_rate_limit_bps: u64,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct GlobalSettingsUpdate {
    #[serde(default, deserialize_with = "deserialize_opt_size")]
    pub global_rate_limit_bps: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct GlobalState {
    pub settings: GlobalSettings,
    pub current_speed_bps: u64,
    pub speed_samples: Vec<u64>,
    pub cache_total_bytes: u64,
    pub task_count: usize,
    pub active_connections: u64,
    pub bytes_served_total: u64,
}

#[derive(Clone)]
pub struct AppState {
    pub tasks: Arc<RwLock<HashMap<String, Arc<TaskEntry>>>>,
    pub bind_addr: String,
    pub cache: Arc<crate::cache::CacheStore>,
    pub settings: Arc<RwLock<GlobalSettings>>,
    pub global_limiter: Arc<TokenBucket>,
    pub global_throughput: Arc<ThroughputSampler>,
    pub global_window_bytes: Arc<AtomicU64>,
    pub persist_path: Arc<std::path::PathBuf>,
}

impl AppState {
    pub fn new(
        bind_addr: String,
        cache: Arc<crate::cache::CacheStore>,
        persist_path: std::path::PathBuf,
        settings: GlobalSettings,
    ) -> Self {
        let limiter = Arc::new(TokenBucket::new(settings.global_rate_limit_bps));
        Self {
            tasks: Arc::new(RwLock::new(HashMap::new())),
            bind_addr,
            cache,
            settings: Arc::new(RwLock::new(settings)),
            global_limiter: limiter,
            global_throughput: Arc::new(ThroughputSampler::new(60)),
            global_window_bytes: Arc::new(AtomicU64::new(0)),
            persist_path: Arc::new(persist_path),
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
        let url_health = entry
            .url_health
            .read()
            .iter()
            .map(|h| h.snapshot())
            .collect();
        TaskInfo {
            task_id: id.to_string(),
            proxy_url: format!("http://{}/stream/{}", self.bind_addr, id),
            config: cfg,
            created_at: entry.created_at,
            bytes_served: entry.bytes_served.load(Ordering::Relaxed),
            active_connections: entry.active_connections.load(Ordering::Relaxed),
            paused: entry.paused.load(Ordering::Relaxed),
            cache,
            url_health,
            current_speed_bps: entry.throughput.current(),
            speed_samples: entry.throughput.snapshot(),
        }
    }

    pub fn list(&self) -> Vec<TaskInfo> {
        let guard = self.tasks.read();
        guard
            .iter()
            .map(|(id, entry)| self.task_info(id, entry))
            .collect()
    }

    pub fn global_state(&self) -> GlobalState {
        let settings = self.settings.read().clone();
        let task_count = self.tasks.read().len();
        let mut active_connections = 0u64;
        let mut bytes_served_total = 0u64;
        for t in self.tasks.read().values() {
            active_connections += t.active_connections.load(Ordering::Relaxed) as u64;
            bytes_served_total += t.bytes_served.load(Ordering::Relaxed);
        }
        GlobalState {
            settings,
            current_speed_bps: self.global_throughput.current(),
            speed_samples: self.global_throughput.snapshot(),
            cache_total_bytes: self.cache.total_bytes_on_disk(),
            task_count,
            active_connections,
            bytes_served_total,
        }
    }

    pub fn count_bytes_global(&self, n: u64) {
        self.global_window_bytes.fetch_add(n, Ordering::Relaxed);
    }

    pub fn tick_global_throughput(&self, elapsed: f64) {
        let bytes = self.global_window_bytes.swap(0, Ordering::Relaxed);
        let bps = (bytes as f64 / elapsed.max(0.001)) as u64;
        self.global_throughput.push(bps);
    }

    pub fn update_settings(
        &self,
        upd: GlobalSettingsUpdate,
    ) -> std::result::Result<GlobalSettings, String> {
        let mut s = self.settings.write();
        if let Some(r) = upd.global_rate_limit_bps {
            s.global_rate_limit_bps = r;
            self.global_limiter.set_rate(r);
        }
        Ok(s.clone())
    }

    /// Save persistable tasks + settings to disk atomically.
    pub fn persist(&self) -> std::io::Result<()> {
        #[derive(Serialize)]
        struct Persisted {
            settings: GlobalSettings,
            tasks: Vec<PersistedTask>,
        }
        #[derive(Serialize)]
        struct PersistedTask {
            id: String,
            config: TaskConfig,
            created_at: u64,
            paused: bool,
        }

        let tasks: Vec<PersistedTask> = self
            .tasks
            .read()
            .iter()
            .filter_map(|(id, entry)| {
                let cfg = entry.config_snapshot();
                if cfg.persist {
                    Some(PersistedTask {
                        id: id.clone(),
                        config: cfg,
                        created_at: entry.created_at,
                        paused: entry.paused.load(Ordering::Relaxed),
                    })
                } else {
                    None
                }
            })
            .collect();

        let p = Persisted {
            settings: self.settings.read().clone(),
            tasks,
        };
        let json = serde_json::to_string_pretty(&p)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        let path: &std::path::Path = &self.persist_path;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let tmp = path.with_extension("json.tmp");
        std::fs::write(&tmp, json)?;
        std::fs::rename(&tmp, path)?;
        Ok(())
    }

    /// Reload persisted tasks + settings from disk. Called once at startup.
    pub fn restore(&self) -> std::io::Result<usize> {
        #[derive(Deserialize)]
        struct Persisted {
            #[serde(default)]
            settings: GlobalSettings,
            #[serde(default)]
            tasks: Vec<PersistedTask>,
        }
        #[derive(Deserialize)]
        struct PersistedTask {
            id: String,
            config: TaskConfig,
            #[serde(default)]
            created_at: u64,
            #[serde(default)]
            paused: bool,
        }

        let path: &std::path::Path = &self.persist_path;
        if !path.exists() {
            return Ok(0);
        }
        let data = std::fs::read_to_string(path)?;
        let p: Persisted = serde_json::from_str(&data)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        // Apply settings.
        {
            let mut s = self.settings.write();
            *s = p.settings.clone();
        }
        self.global_limiter
            .set_rate(p.settings.global_rate_limit_bps);

        let mut count = 0;
        for pt in p.tasks {
            let entry = Arc::new(TaskEntry::new(pt.config));
            entry.paused.store(pt.paused, Ordering::Relaxed);
            // Force timestamp from disk so "created" age survives restart.
            // (TaskEntry::new sets it to "now" — there's no setter, but
            // created_at is pub so we set it via a small dance: rebuild
            // with the right ts.)
            let with_ts = TaskEntry {
                created_at: pt.created_at.max(1),
                ..Arc::try_unwrap(entry).unwrap_or_else(|_| unreachable!())
            };
            self.insert(pt.id, Arc::new(with_ts));
            count += 1;
        }
        Ok(count)
    }

    /// Spawn a background ticker that:
    /// 1. samples throughput per task and globally,
    /// 2. flushes persisted state if anything changed (cheap — checks dirty
    ///    via comparing serialized snapshot length, see usage in main).
    pub fn spawn_background(self: Arc<Self>) {
        let me = Arc::clone(&self);
        tokio::spawn(async move {
            let mut last_persist = Instant::now();
            let mut last_persist_hash = 0u64;
            let mut last_tick = Instant::now();
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                let now = Instant::now();
                let elapsed = now.duration_since(last_tick).as_secs_f64();
                last_tick = now;

                for entry in me.tasks.read().values() {
                    entry.tick_throughput();
                }
                me.tick_global_throughput(elapsed);

                if last_persist.elapsed() >= Duration::from_secs(5) {
                    last_persist = Instant::now();
                    let h = me.persist_hash();
                    if h != last_persist_hash {
                        last_persist_hash = h;
                        if let Err(e) = me.persist() {
                            tracing::warn!("persist failed: {}", e);
                        }
                    }
                }
            }
        });
    }

    fn persist_hash(&self) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.settings.read().global_rate_limit_bps.hash(&mut hasher);
        for (id, e) in self.tasks.read().iter() {
            let cfg = e.config.read();
            if !cfg.persist {
                continue;
            }
            id.hash(&mut hasher);
            cfg.urls.hash(&mut hasher);
            cfg.max_threads.hash(&mut hasher);
            cfg.max_split.hash(&mut hasher);
            cfg.cache.hash(&mut hasher);
            cfg.rate_limit_bps.hash(&mut hasher);
            cfg.name.hash(&mut hasher);
            for (k, v) in &cfg.headers {
                k.hash(&mut hasher);
                v.hash(&mut hasher);
            }
            e.paused.load(Ordering::Relaxed).hash(&mut hasher);
        }
        hasher.finish()
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
