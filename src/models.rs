use crate::cache::CacheStats;
use crate::plugins::{PluginRegistry, TaskPluginConfig};
use crate::ratelimit::{Algorithm, Limiter};
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskConfig {
    /// Structured volume layout — the **only** source of truth for the
    /// task's URL set. Each inner Vec is one volume's mirror URL list
    /// (interchangeable copies of that part). The outer Vec is in
    /// playback/concatenation order.
    ///
    /// * One volume with N mirrors → mirror-mode behavior (the single-file case).
    /// * N volumes with M mirrors each → ordered volume mode.
    ///
    /// Empty volumes are dropped by `normalize()` before validation. A task
    /// with zero non-empty volumes is rejected at create / update time with
    /// a user-facing error (rather than a serde "missing field" message,
    /// which the absent-field default below makes friendlier).
    #[serde(default)]
    pub volumes: Vec<Vec<String>>,
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
    /// Filename to emit on the proxied response's Content-Disposition. When
    /// `auto_filename` is true this is treated as a fallback / cached probe
    /// result; when false it's the authoritative value (None = no header).
    #[serde(default)]
    pub output_filename: Option<String>,
    /// If true, overwrite the served filename with whatever the upstream
    /// probe detects at stream time. If false, use `output_filename` verbatim.
    #[serde(default = "default_auto_filename")]
    pub auto_filename: bool,
    /// Per-task rate limit in bytes/sec. 0 = unlimited.
    #[serde(default, deserialize_with = "deserialize_opt_size_default_zero")]
    pub rate_limit_bps: u64,
    /// Rate-limit algorithm. Falls back to TokenBucket if absent.
    #[serde(default)]
    pub rate_limit_algorithm: Algorithm,
    /// Persist this task across restarts.
    #[serde(default)]
    pub persist: bool,
    /// Post-processing plugins applied to bytes on the proxy → client path.
    /// Stored in **forward order** (sender's pre-distribution application
    /// order); the engine applies them in reverse on the receive path so
    /// chained transforms like compress→encrypt undo correctly.
    #[serde(default)]
    pub plugins: Vec<TaskPluginConfig>,
    /// How the proxied response advertises itself via `Content-Disposition`.
    /// `Auto` reproduces the historic behavior (inline + upstream MIME, so
    /// the browser picks based on Content-Type — sometimes plays, sometimes
    /// downloads). `Inline` and `Attachment` are the explicit overrides.
    #[serde(default)]
    pub content_disposition: ContentDispositionMode,
}

/// User-facing knob for the served `Content-Disposition` (and a touch of
/// Content-Type fix-up). Defaults to `Auto` — matches the pre-existing
/// behavior so old persisted tasks round-trip identically.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ContentDispositionMode {
    /// Inline disposition + whatever Content-Type the upstream sent. The
    /// browser picks: plays for `video/*`, downloads for `application/octet-stream`.
    #[default]
    Auto,
    /// Inline disposition AND coerce a generic upstream Content-Type
    /// (`application/octet-stream`) into a more specific MIME guessed from
    /// the served filename — so e.g. a CDN that returns octet-stream for a
    /// `.mp4` still gets rendered by `<video>`. Use when you want preview.
    Inline,
    /// `attachment; filename="…"` — browsers always download, never preview.
    Attachment,
}

fn default_threads() -> usize {
    8
}

fn default_auto_filename() -> bool {
    true
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
    pub volumes: Option<Vec<Vec<String>>>,
    pub max_threads: Option<usize>,
    #[serde(default, deserialize_with = "deserialize_opt_size")]
    pub max_split: Option<u64>,
    pub cache: Option<bool>,
    pub headers: Option<HashMap<String, String>>,
    pub name: Option<Option<String>>,
    pub output_filename: Option<Option<String>>,
    pub auto_filename: Option<bool>,
    #[serde(default, deserialize_with = "deserialize_opt_size")]
    pub rate_limit_bps: Option<u64>,
    pub rate_limit_algorithm: Option<Algorithm>,
    pub persist: Option<bool>,
    pub plugins: Option<Vec<TaskPluginConfig>>,
    pub content_disposition: Option<ContentDispositionMode>,
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
    /// Per-URL health (one entry per unique URL across every volume's
    /// mirrors; `apply_update` rebuilds this list whenever the layout
    /// changes, carrying over stats for URLs that survived the edit).
    pub url_health: RwLock<Vec<Arc<UrlHealthAcc>>>,
    pub limiter: Arc<Limiter>,
    pub throughput: Arc<ThroughputSampler>,
    /// Bytes counted toward the next sampler tick.
    pub window_bytes: AtomicU64,
    pub last_sample: Mutex<Instant>,
}

impl TaskConfig {
    /// Volume layout with empty entries scrubbed. Identity transform plus
    /// hygiene — every non-empty mirror string in every non-empty volume,
    /// preserving order.
    pub fn effective_volumes(&self) -> Vec<Vec<String>> {
        self.volumes
            .iter()
            .map(|v| v.iter().filter(|u| !u.trim().is_empty()).cloned().collect())
            .filter(|v: &Vec<String>| !v.is_empty())
            .collect()
    }

    /// Flat de-duplicated URL list across every volume's mirrors, in
    /// first-seen order. Used for URL-health bookkeeping and the cache
    /// key's traceability hint.
    pub fn flat_unique_urls(volumes: &[Vec<String>]) -> Vec<String> {
        let mut seen = std::collections::HashSet::new();
        let mut out = Vec::new();
        for vol in volumes {
            for u in vol {
                if seen.insert(u.clone()) {
                    out.push(u.clone());
                }
            }
        }
        out
    }

    /// Instance-side projection of `flat_unique_urls(&self.volumes)`.
    /// Convenience for the many call sites that just want "what URLs does
    /// this task talk to".
    pub fn urls(&self) -> Vec<String> {
        Self::flat_unique_urls(&self.volumes)
    }

    /// Drop empty mirror strings and empty volumes so downstream code can
    /// assume every entry is non-empty.
    pub fn normalize(&mut self) {
        self.volumes = self.effective_volumes();
    }
}

impl TaskEntry {
    pub fn new(mut config: TaskConfig) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        config.normalize();
        let url_health = config
            .urls()
            .into_iter()
            .map(|u| Arc::new(UrlHealthAcc::new(u)))
            .collect();
        let limiter = Arc::new(Limiter::new(
            config.rate_limit_bps,
            config.rate_limit_algorithm,
        ));
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

        let volumes_changed = upd.volumes.is_some();
        if let Some(volumes) = upd.volumes {
            cfg.volumes = volumes;
        }
        if volumes_changed {
            cfg.normalize();
            if cfg.volumes.is_empty() {
                return Err("at least one URL is required across all volumes".into());
            }
            // Preserve health stats for URLs that survived the edit.
            let mut prev: HashMap<String, Arc<UrlHealthAcc>> = self
                .url_health
                .read()
                .iter()
                .map(|h| (h.url.clone(), Arc::clone(h)))
                .collect();
            let new_health: Vec<Arc<UrlHealthAcc>> = cfg
                .urls()
                .into_iter()
                .map(|u| {
                    prev.remove(&u)
                        .unwrap_or_else(|| Arc::new(UrlHealthAcc::new(u)))
                })
                .collect();
            *self.url_health.write() = new_health;
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
        if let Some(of) = upd.output_filename {
            cfg.output_filename = of.map(|s| s.trim().to_string()).filter(|s| !s.is_empty());
        }
        if let Some(a) = upd.auto_filename {
            cfg.auto_filename = a;
        }
        if let Some(r) = upd.rate_limit_bps {
            cfg.rate_limit_bps = r;
            self.limiter.set_rate(r);
        }
        if let Some(a) = upd.rate_limit_algorithm {
            cfg.rate_limit_algorithm = a;
            self.limiter.set_algorithm(a);
        }
        if let Some(p) = upd.persist {
            cfg.persist = p;
        }
        if let Some(pl) = upd.plugins {
            cfg.plugins = pl;
        }
        if let Some(cd) = upd.content_disposition {
            cfg.content_disposition = cd;
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
    #[serde(default)]
    pub global_rate_limit_algorithm: Algorithm,
    /// Per-plugin global config blob, keyed by plugin id. The plugin
    /// interprets its own value (e.g. ChaCha20 stores I/O buffer size here).
    #[serde(default)]
    pub plugin_globals: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct GlobalSettingsUpdate {
    #[serde(default, deserialize_with = "deserialize_opt_size")]
    pub global_rate_limit_bps: Option<u64>,
    pub global_rate_limit_algorithm: Option<Algorithm>,
    pub plugin_globals: Option<HashMap<String, serde_json::Value>>,
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
    pub global_limiter: Arc<Limiter>,
    pub global_throughput: Arc<ThroughputSampler>,
    pub global_window_bytes: Arc<AtomicU64>,
    pub persist_path: Arc<std::path::PathBuf>,
    pub plugins: Arc<PluginRegistry>,
}

impl AppState {
    pub fn new(
        bind_addr: String,
        cache: Arc<crate::cache::CacheStore>,
        persist_path: std::path::PathBuf,
        settings: GlobalSettings,
        plugins: Arc<PluginRegistry>,
    ) -> Self {
        let limiter = Arc::new(Limiter::new(
            settings.global_rate_limit_bps,
            settings.global_rate_limit_algorithm,
        ));
        Self {
            tasks: Arc::new(RwLock::new(HashMap::new())),
            bind_addr,
            cache,
            settings: Arc::new(RwLock::new(settings)),
            global_limiter: limiter,
            global_throughput: Arc::new(ThroughputSampler::new(60)),
            global_window_bytes: Arc::new(AtomicU64::new(0)),
            persist_path: Arc::new(persist_path),
            plugins,
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
            let key = crate::cache::CacheStore::key_for_task(&cfg);
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
        if let Some(a) = upd.global_rate_limit_algorithm {
            s.global_rate_limit_algorithm = a;
            self.global_limiter.set_algorithm(a);
        }
        if let Some(pg) = upd.plugin_globals {
            // Validate each plugin's new config against its own schema before
            // committing — a bad value here would otherwise only surface
            // when a task next streams (annoying to debug).
            for (id, value) in &pg {
                if let Some(plugin) = self.plugins.get(id) {
                    plugin
                        .validate_global_config(value)
                        .map_err(|e| format!("plugin '{}' global config: {}", id, e))?;
                }
                // Unknown plugin ids are accepted but logged — keeps
                // forward-compat with plugins added in a future build.
            }
            s.plugin_globals = pg;
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
        self.global_limiter
            .set_algorithm(p.settings.global_rate_limit_algorithm);

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
        let s = self.settings.read();
        s.global_rate_limit_bps.hash(&mut hasher);
        (s.global_rate_limit_algorithm as u8).hash(&mut hasher);
        // Plugin globals: hash the (id, serialized-config) pairs in stable
        // key order so reorderings don't appear as a change.
        let mut pg_keys: Vec<&String> = s.plugin_globals.keys().collect();
        pg_keys.sort();
        for k in pg_keys {
            k.hash(&mut hasher);
            if let Some(v) = s.plugin_globals.get(k) {
                serde_json::to_string(v).unwrap_or_default().hash(&mut hasher);
            }
        }
        drop(s);
        for (id, e) in self.tasks.read().iter() {
            let cfg = e.config.read();
            if !cfg.persist {
                continue;
            }
            id.hash(&mut hasher);
            for vol in &cfg.volumes {
                b"|".hash(&mut hasher);
                for u in vol {
                    u.hash(&mut hasher);
                }
            }
            cfg.max_threads.hash(&mut hasher);
            cfg.max_split.hash(&mut hasher);
            cfg.cache.hash(&mut hasher);
            cfg.rate_limit_bps.hash(&mut hasher);
            (cfg.rate_limit_algorithm as u8).hash(&mut hasher);
            cfg.name.hash(&mut hasher);
            cfg.output_filename.hash(&mut hasher);
            cfg.auto_filename.hash(&mut hasher);
            for (k, v) in &cfg.headers {
                k.hash(&mut hasher);
                v.hash(&mut hasher);
            }
            // Plugin slots: id + enabled + serialized config. Same plugin
            // listed twice (legal but unusual) hashes correctly because each
            // slot contributes independently.
            for pc in &cfg.plugins {
                pc.id.hash(&mut hasher);
                pc.enabled.hash(&mut hasher);
                serde_json::to_string(&pc.config)
                    .unwrap_or_default()
                    .hash(&mut hasher);
            }
            (cfg.content_disposition as u8).hash(&mut hasher);
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
