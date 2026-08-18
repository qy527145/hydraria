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
    /// Soft cap on concurrent fetchers allowed against a single volume's URL
    /// list. The scheduler enforces this in addition to `max_threads` so a
    /// long run of plan-adjacent chunks (which all live in the same volume)
    /// won't pile every fetcher onto one upstream connection — when this
    /// cap is hit the scheduler skips ahead to the first chunk in another
    /// volume that still has room. Pick this to match the upstream's
    /// per-IP / per-URL connection limit (4 is a common default for
    /// generic nginx / pan-CDN setups).
    ///
    /// Soft, not hard: when no other volume has work available (e.g. the
    /// client's Range only touches one volume), idle slots in the
    /// `max_threads` budget overflow back into already-capped volumes
    /// rather than sitting unused. This keeps total throughput at the
    /// task-wide limit even when work is unevenly distributed across
    /// volumes; trade-off is that a single upstream may briefly exceed
    /// its per-URL connection limit when there's no alternative.
    #[serde(default = "default_per_volume")]
    pub max_per_volume: usize,
    /// Upper bound on one upstream range request, or **0 for automatic**.
    ///
    /// Automatic lets the scheduler size claims per scenario: an even share of
    /// the work remaining for a download — which is what allows a single worker
    /// to pull hundreds of megabytes in one request and amortize a multi-second
    /// per-request latency the way a dedicated downloader does — and a ladder
    /// that grows with distance from the read head for playback. Neither has a
    /// ceiling until an upstream earns one.
    /// A non-zero value is a hard cap, kept for upstreams that dislike long
    /// ranges (and so that pre-existing tasks behave exactly as before).
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
    ///
    /// Defaults to **on**: a proxy short link that evaporates on restart is a
    /// broken link in whatever playlist / script / player it was pasted into,
    /// and "keep it" is what someone who bothered to create a task almost
    /// always meant. Opting out is one checkbox; recreating a lost task by
    /// hand is not.
    #[serde(default = "default_true")]
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
    /// 任务级域名映射，与全局设置里的那份取并集；`from` 撞车时以这里为准。
    /// 语义和全局的完全一样（只改 TCP 连到哪儿），见 [`crate::hostmap`]。
    #[serde(default)]
    pub host_mappings: Vec<crate::hostmap::HostMapping>,
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

/// 线程总数的硬上限。与校验里历史沿用的 128 一致。
pub const MAX_THREADS: usize = 128;

/// 手填 `max_split` 时的下限。再小的分片，每个请求的头部开销就开始盖过收益了；
/// `0` 仍然表示自动。
pub const MIN_SPLIT: u64 = 64 * 1024;

fn default_threads() -> usize {
    8
}

fn default_per_volume() -> usize {
    4
}

fn default_auto_filename() -> bool {
    true
}

fn default_true() -> bool {
    true
}

/// Automatic claim sizing. Pre-existing persisted tasks carry an explicit
/// value and keep it; only tasks created without one opt into auto.
fn default_split() -> u64 {
    0
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
    pub max_per_volume: Option<usize>,
    #[serde(default, deserialize_with = "deserialize_opt_size")]
    pub max_split: Option<u64>,
    pub cache: Option<bool>,
    pub headers: Option<HashMap<String, String>>,
    /// 外层 `Option` = 「这次 PATCH 提没提这个字段」，内层 = 字段的新值。
    /// 传 `null` 是清空任务名，字段缺席才是「别动它」—— 见
    /// [`deserialize_double_option`]。
    #[serde(default, deserialize_with = "deserialize_double_option")]
    pub name: Option<Option<String>>,
    #[serde(default, deserialize_with = "deserialize_double_option")]
    pub output_filename: Option<Option<String>>,
    pub auto_filename: Option<bool>,
    #[serde(default, deserialize_with = "deserialize_opt_size")]
    pub rate_limit_bps: Option<u64>,
    pub rate_limit_algorithm: Option<Algorithm>,
    pub persist: Option<bool>,
    pub plugins: Option<Vec<TaskPluginConfig>>,
    pub content_disposition: Option<ContentDispositionMode>,
    pub host_mappings: Option<Vec<crate::hostmap::HostMapping>>,
}

/// `Option<Option<T>>` 的老问题：serde 默认会让外层 `Option` 把 `null` 一并吃掉，
/// 于是「字段缺席」和「显式传 `null`」解出来是同一个 `None` —— 而这两件事在 PATCH
/// 里的含义正好相反（**别动它** vs **清空它**）。
///
/// 后果不是抽象的：面板保存任务时发的是完整配置，把任务名删空就是 `name: null`，
/// 而它曾经被读成「别动」，于是名字怎么也删不掉，脚本也没有任何办法清空它。
///
/// 配合 `#[serde(default)]`：字段缺席走 `Default`（`None`），出现了才调这里 ——
/// `null` → `Some(None)`（清空），有值 → `Some(Some(v))`。
fn deserialize_double_option<'de, D, T>(deserializer: D) -> Result<Option<Option<T>>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer).map(Some)
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
    /// Number of HTTP requests currently in flight against this URL. Updated
    /// by the engine via an RAII guard around the body-streaming loop so it
    /// reflects both the dial and the long-running download.
    pub in_flight_requests: u32,
    /// Size of the volume this URL belongs to, once the upstream probe has
    /// populated it. `None` until the first probe lands.
    pub volume_size: Option<u64>,
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
    pub in_flight_requests: AtomicU32,
    /// Normalized `ETag` this specific URL has been seen to serve.
    ///
    /// **Per-URL on purpose.** Mirrors of identical content routinely report
    /// different ETags (nginx derives them from mtime+size, so two servers
    /// holding the same bytes disagree), so comparing one mirror's ETag against
    /// another's — or against a single task-wide value — would false-positive on
    /// every healthy multi-mirror task. Comparing a URL only against itself
    /// detects the thing we actually care about: an origin that swapped its
    /// content out from under an in-flight download.
    pub observed_etag: parking_lot::Mutex<Option<String>>,
    /// How many times this URL has contradicted its own earlier `ETag`.
    ///
    /// Bounds the damage when a URL's validators are simply unstable — a CDN
    /// fronting several origins whose mtimes differ can hand out a different
    /// ETag per request for byte-identical content. Treating that as corruption
    /// forever would fail every claim and take the whole task down, which is
    /// strictly worse than not checking at all. After a couple of strikes the
    /// check stops trusting this URL's validators and stands down.
    pub etag_mismatches: AtomicU32,
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
            in_flight_requests: AtomicU32::new(0),
            observed_etag: parking_lot::Mutex::new(None),
            etag_mismatches: AtomicU32::new(0),
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
            in_flight_requests: self.in_flight_requests.load(Ordering::Relaxed),
            volume_size: None,
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

    /// Mean of the last `n` samples (all of them if fewer exist). Smooths the
    /// bursty per-second reading into something a human can read.
    pub fn recent_mean(&self, n: usize) -> u64 {
        let s = self.samples.lock();
        if s.is_empty() || n == 0 {
            return 0;
        }
        let tail = &s[s.len().saturating_sub(n)..];
        (tail.iter().sum::<u64>() / tail.len() as u64).max(0)
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
    /// Unix seconds of the last configuration edit, or `created_at` for a task
    /// nobody has edited. The dashboard orders by this, so "what was I just
    /// working on" is what the list opens on.
    pub updated_at: u64,
    pub bytes_served: u64,
    pub active_connections: u32,
    pub paused: bool,
    pub cache: Option<CacheStats>,
    pub url_health: Vec<UrlHealth>,
    pub current_speed_bps: u64,
    pub speed_samples: Vec<u64>,
    /// Active (or last) whole-file cache job for this task, if any.
    pub cache_job: Option<crate::download::CacheJobInfo>,
}

/// Holds a task's live-connection gauge up for exactly as long as the response
/// body exists.
///
/// `active_connections` used to be incremented per stream and never
/// decremented, so the dashboard's connection count only ever grew — a task
/// with nothing streaming still reported connections from every request it had
/// ever served. Tying the decrement to a guard's `Drop` makes it correct on
/// every exit path, including client disconnects and errors.
#[derive(Debug)]
pub struct ConnectionGuard(Arc<TaskEntry>);

impl ConnectionGuard {
    pub fn new(entry: Arc<TaskEntry>) -> Self {
        entry.active_connections.fetch_add(1, Ordering::Relaxed);
        Self(entry)
    }
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        // Saturating: an underflow would wrap the u32 to ~4 billion and make
        // the gauge worse than useless.
        let _ = self
            .0
            .active_connections
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_sub(1))
            });
    }
}

#[derive(Debug)]
pub struct TaskEntry {
    pub config: RwLock<TaskConfig>,
    pub created_at: u64,
    /// Unix seconds of the last successful [`TaskEntry::apply_update`]. Atomic
    /// rather than behind the config lock so `task_info` can read it without
    /// ordering itself against a writer.
    pub updated_at: AtomicU64,
    pub bytes_served: AtomicU64,
    /// Live client connections. Maintained exclusively through
    /// [`ConnectionGuard`] so it can't drift.
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
    /// Cached upstream probe result with insertion timestamp. Reused for
    /// `PROBE_CACHE_TTL` to avoid re-probing every volume on every Range
    /// request (a player like PotPlayer opens a fresh connection per seek,
    /// so without this every seek triggered N × (HEAD + Range-1) round-trips
    /// before any bytes flowed). Invalidated by `apply_update` whenever the
    /// volume list or request headers change.
    pub probe_cache: Mutex<Option<(Arc<crate::engine::UpstreamProbe>, Instant)>>,
    /// Serializes concurrent first-time probes for this task. Without this,
    /// PotPlayer's burst of N parallel reconnect attempts during a slow probe
    /// each kick off their own probe and starve each other on the upstream's
    /// connection pool — none ever finishes. The async mutex lets the second
    /// caller wait until the first completes (and populates `probe_cache`),
    /// then exit through the cache-hit path on the next check.
    pub probe_inflight: tokio::sync::Mutex<()>,
    /// URLs that returned non-success (or network error) on a HEAD request at
    /// least once during this process's lifetime. `probe_one` skips HEAD for
    /// these and goes straight to the 1-byte Range GET. Cleared whenever the
    /// task's volume list changes.
    pub head_unsupported: Arc<parking_lot::RwLock<std::collections::HashSet<String>>>,
    /// What this task's origins have taught us about how large a range they
    /// will swallow. Shared by every scheduler the task builds, because a
    /// scheduler is built per client request and the lesson costs a full read
    /// timeout to learn. Expires on its own (see `ClaimWall`) and is cleared
    /// whenever the volume list changes.
    pub claim_wall: Arc<crate::schedule::ClaimWall>,
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
        self.max_threads = Self::derive_threads(self.max_per_volume, self.volumes.len());
        // 表单里加了一行又没填完的，丢掉而不是报错。
        self.host_mappings
            .retain(|m| !m.from.trim().is_empty() || !m.to.trim().is_empty());
    }

    /// 任务级域名映射的校验，创建和编辑时各调一次。放在这里而不是 `normalize`
    /// 里，是因为它会失败，而 `normalize` 的契约是「只整形，不拒绝」。
    pub fn validate_host_mappings(&self) -> std::result::Result<(), String> {
        crate::hostmap::validate(&self.host_mappings).map_err(|e| format!("host mapping: {e}"))
    }

    /// 建任务时的取值校验（`normalize` 之后调）。
    ///
    /// 这几条必须和 [`TaskEntry::apply_update`] 里的完全一致：同一个值 PATCH 拒绝、
    /// POST 放过的话，「一次建好」和「先建再改」会得到两个不同的任务 —— 而脚本
    /// 通常两条路都走。
    ///
    /// 只在控制面调，不在恢复持久化状态时调：磁盘上那份是过去某个版本写的，宽容
    /// 地读进来比启动失败好。
    pub fn validate(&self) -> std::result::Result<(), String> {
        if self.max_per_volume == 0 {
            return Err("max_per_volume must be >= 1".into());
        }
        if self.max_split != 0 && self.max_split < MIN_SPLIT {
            return Err("max_split must be 0 (auto) or >= 64K".into());
        }
        self.validate_host_mappings()
    }

    /// 线程总数 = 单卷并发上限 × 卷数，不再单独配置。
    ///
    /// 两个数字各配一份时它们总在打架，而且哪一个赢完全取决于任务形状：单卷
    /// 文件配 16 线程实际只能跑 `max_per_volume` 条（调度器不会为了凑线程数
    /// 去超订同一个源），8 卷任务配 4 线程则让大半卷根本没人认领。既然真正
    /// 决定并发的永远是「一个源能同时开几条」，就只留这一个旋钮，总数由它
    /// 和卷数推出来。
    pub fn derive_threads(max_per_volume: usize, volumes: usize) -> usize {
        max_per_volume
            .max(1)
            .saturating_mul(volumes.max(1))
            .clamp(1, MAX_THREADS)
    }
}

impl TaskEntry {
    /// Unix seconds, or 0 if the clock is before the epoch.
    fn now() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    }

    pub fn new(mut config: TaskConfig) -> Self {
        let now = Self::now();
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
            updated_at: AtomicU64::new(now),
            bytes_served: AtomicU64::new(0),
            active_connections: AtomicU32::new(0),
            paused: AtomicBool::new(false),
            url_health: RwLock::new(url_health),
            limiter,
            throughput: Arc::new(ThroughputSampler::new(60)),
            window_bytes: AtomicU64::new(0),
            last_sample: Mutex::new(Instant::now()),
            probe_cache: Mutex::new(None),
            probe_inflight: tokio::sync::Mutex::new(()),
            head_unsupported: Arc::new(parking_lot::RwLock::new(std::collections::HashSet::new())),
            claim_wall: Arc::new(crate::schedule::ClaimWall::new()),
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
            // URL list changed → cached probe describes a stale layout, and
            // old HEAD-unsupported markers refer to URLs the user may have
            // just fixed/replaced. Drop both.
            *self.probe_cache.lock() = None;
            self.head_unsupported.write().clear();
            // The wall describes an origin that may no longer be in the list.
            self.claim_wall.clear();
        }
        // `max_threads` 是派生值，PATCH 里带上也只会被下面重新算出来 —— 保留
        // 这个字段只为兼容老客户端的请求体，收下即忽略。
        if let Some(p) = upd.max_per_volume {
            if p == 0 {
                return Err("max_per_volume must be >= 1".into());
            }
            cfg.max_per_volume = p;
        }
        cfg.max_threads = TaskConfig::derive_threads(cfg.max_per_volume, cfg.volumes.len());
        if let Some(s) = upd.max_split {
            // 0 = automatic: the scheduler sizes claims from the remaining work
            // and the thread count, with no ceiling.
            if s != 0 && s < MIN_SPLIT {
                return Err("max_split must be 0 (auto) or >= 64K".into());
            }
            cfg.max_split = s;
        }
        if let Some(c) = upd.cache {
            cfg.cache = c;
        }
        if let Some(h) = upd.headers {
            cfg.headers = h;
            // Probe requests use these headers, so a cached probe is no
            // longer guaranteed to reflect what the upstream would answer now.
            *self.probe_cache.lock() = None;
        }
        if let Some(n) = upd.name {
            // 空白名字和没有名字是一回事 —— 存下一个 `" "` 只会让列表里出现一个
            // 看不见的任务名，而搜索和显示都拿它没办法。
            cfg.name = n.map(|s| s.trim().to_string()).filter(|s| !s.is_empty());
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
        if let Some(mut maps) = upd.host_mappings {
            maps.retain(|m| !m.from.trim().is_empty() || !m.to.trim().is_empty());
            crate::hostmap::validate(&maps).map_err(|e| format!("host mapping: {e}"))?;
            cfg.host_mappings = maps;
            // 探测走的也是映射后的地址，换了映射，缓存的探测结果就未必还成立。
            *self.probe_cache.lock() = None;
        }
        // Only on the success path: a rejected edit changed nothing the user
        // asked for, so it should not reorder the dashboard either.
        self.updated_at.store(Self::now(), Ordering::Relaxed);
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
    /// Where the download button writes by default. `None` until the user
    /// picks one; a download started without an explicit directory then fails
    /// with a message rather than guessing at somewhere in the filesystem.
    #[serde(default)]
    pub download_dir: Option<String>,
    /// 自定义域名 / IP 映射，语义等同 `curl --resolve`：只改 TCP 连到哪儿，
    /// URL、Host 头、TLS SNI 一律保持原样，所以带签名的地址不会因此失效。
    /// 全局而非按任务，理由见 [`crate::hostmap`]：解析发生在进程唯一的那个
    /// 连接池里，拿不到「这条请求属于哪个任务」的上下文。
    #[serde(default)]
    pub host_mappings: Vec<crate::hostmap::HostMapping>,
    /// 解析**映射目标**用的 DNS。`None` / 空 = 系统解析器（默认）。
    /// 写 `tls://1.1.1.1` 则自己走 DoT 查 —— 开着 TUN 模式的代理时系统解析会
    /// 返回 fake-ip，域名映射会因此静默失效，详见 [`crate::dns`]。
    ///
    /// 与 `host_mappings` 一样放全局：解析发生在进程唯一那个连接池里，拿不到
    /// 「这条请求属于哪个任务」的上下文；而 TUN 本来也是整机状态。
    #[serde(default)]
    pub dns: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct GlobalSettingsUpdate {
    #[serde(default, deserialize_with = "deserialize_opt_size")]
    pub global_rate_limit_bps: Option<u64>,
    pub global_rate_limit_algorithm: Option<Algorithm>,
    pub plugin_globals: Option<HashMap<String, serde_json::Value>>,
    pub download_dir: Option<String>,
    pub host_mappings: Option<Vec<crate::hostmap::HostMapping>>,
    pub dns: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct GlobalState {
    pub settings: GlobalSettings,
    /// 所有缓存填充任务的合计拉取速率。与 `current_speed_bps`（发给客户端的
    /// 速率）是方向相反的两条流，分开报而不是相加 —— 见
    /// [`crate::download::DownloadManager::fill_speed_bps`]。
    pub cache_fill_speed_bps: u64,
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
    pub downloads: Arc<crate::download::DownloadManager>,
    /// The one upstream HTTP client, shared by every engine this process
    /// builds. A `Client` owns its connection pool, so this must not be
    /// rebuilt per request — see [`crate::engine::build_upstream_client`].
    pub upstream: reqwest::Client,
}

impl AppState {
    pub fn new(
        bind_addr: String,
        cache: Arc<crate::cache::CacheStore>,
        persist_path: std::path::PathBuf,
        settings: GlobalSettings,
        plugins: Arc<PluginRegistry>,
        downloads: Arc<crate::download::DownloadManager>,
        upstream: reqwest::Client,
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
            downloads,
            upstream,
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
        let key = crate::cache::CacheStore::key_for_task(&cfg);
        let cache = self.cache.stats(&key);
        let url_health: Vec<UrlHealth> = {
            // Look up each URL's volume size from the cached probe (if any),
            // so the dashboard can show "this URL is for a 1.2 GB volume"
            // alongside the live in-flight counter.
            let size_for: std::collections::HashMap<String, u64> = match &*entry.probe_cache.lock()
            {
                Some((probe, _)) => probe
                    .volumes
                    .as_ref()
                    .map(|vs| {
                        let mut m = std::collections::HashMap::new();
                        for v in vs.iter() {
                            for u in &v.urls {
                                m.insert(u.clone(), v.size);
                            }
                        }
                        m
                    })
                    .unwrap_or_default(),
                None => std::collections::HashMap::new(),
            };
            entry
                .url_health
                .read()
                .iter()
                .map(|h| {
                    let mut snap = h.snapshot();
                    snap.volume_size = size_for.get(&snap.url).copied();
                    snap
                })
                .collect()
        };
        TaskInfo {
            task_id: id.to_string(),
            proxy_url: format!("http://{}/stream/{}", self.bind_addr, id),
            config: cfg,
            created_at: entry.created_at,
            updated_at: entry.updated_at.load(Ordering::Relaxed),
            bytes_served: entry.bytes_served.load(Ordering::Relaxed),
            active_connections: entry.active_connections.load(Ordering::Relaxed),
            paused: entry.paused.load(Ordering::Relaxed),
            cache,
            url_health,
            current_speed_bps: entry.throughput.current(),
            speed_samples: entry.throughput.snapshot(),
            cache_job: self.downloads.info(id),
        }
    }

    /// Every task, most recently edited first.
    ///
    /// Ordering belongs here rather than in the dashboard because the map's
    /// iteration order is arbitrary: without it the list visibly reshuffles on
    /// every one-second poll. Newest-edit-first also means the task you just
    /// saved is the one you are looking at. `task_id` breaks ties so tasks
    /// created inside the same second (a restore, or a burst of API calls)
    /// still have a stable order.
    pub fn list(&self) -> Vec<TaskInfo> {
        let guard = self.tasks.read();
        let mut out: Vec<TaskInfo> = guard
            .iter()
            .map(|(id, entry)| self.task_info(id, entry))
            .collect();
        out.sort_unstable_by(|a, b| {
            b.updated_at
                .cmp(&a.updated_at)
                .then_with(|| a.task_id.cmp(&b.task_id))
        });
        out
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
            cache_fill_speed_bps: self.downloads.fill_speed_bps(),
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
        mut upd: GlobalSettingsUpdate,
    ) -> std::result::Result<GlobalSettings, String> {
        // 域名映射先于任何写入校验：一条写错的规则应该让整个保存失败，而不是
        // 「限速改了、映射没改」这种只改了一半的状态。
        if let Some(maps) = upd.host_mappings.as_mut() {
            // 空行是表单里删了一半的残留，直接丢掉而不是报错。
            maps.retain(|m| !m.from.trim().is_empty() || !m.to.trim().is_empty());
            crate::hostmap::validate(maps).map_err(|e| format!("host mapping: {e}"))?;
        }
        // DNS 同理：配错了整个保存失败，不留「限速改了、DNS 没改」的半吊子状态。
        let dns_mode = match upd.dns.as_deref() {
            Some(raw) => Some(crate::dns::parse_mode(Some(raw))?),
            None => None,
        };
        let mut s = self.settings.write();
        if let Some(r) = upd.global_rate_limit_bps {
            s.global_rate_limit_bps = r;
            self.global_limiter.set_rate(r);
        }
        if let Some(a) = upd.global_rate_limit_algorithm {
            s.global_rate_limit_algorithm = a;
            self.global_limiter.set_algorithm(a);
        }
        if let Some(d) = upd.download_dir {
            let trimmed = d.trim();
            s.download_dir = if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            };
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
        if let Some(maps) = upd.host_mappings {
            // 上面已经校验过，这里 install 不会失败；真失败了也只是这张表没换，
            // 报错回去让用户看见比 unwrap 掉进程好。
            crate::hostmap::install(&maps).map_err(|e| format!("host mapping: {e}"))?;
            s.host_mappings = maps;
        }
        if let Some(mode) = dns_mode {
            crate::hostmap::install_dns(&mode)?;
            s.dns = crate::dns::describe(&mode);
        }
        Ok(s.clone())
    }

    /// Save persistable tasks + settings to disk atomically.
    pub fn persist(&self) -> std::io::Result<()> {
        #[derive(Serialize)]
        struct Persisted {
            settings: GlobalSettings,
            tasks: Vec<PersistedTask>,
            /// Unfinished downloads. The bytes live in `.part` files next to
            /// their output, so this only needs enough to reopen them.
            downloads: Vec<crate::download::PersistedDownload>,
        }
        #[derive(Serialize)]
        struct PersistedTask {
            id: String,
            config: TaskConfig,
            created_at: u64,
            updated_at: u64,
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
                        updated_at: entry.updated_at.load(Ordering::Relaxed),
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
            downloads: self.downloads.persisted(),
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
    ///
    /// Returns the restored task count plus any unfinished downloads, which the
    /// caller reopens once the runtime is up (reopening needs an upstream probe,
    /// so it can't happen inside this synchronous function).
    pub fn restore(&self) -> std::io::Result<(usize, Vec<crate::download::PersistedDownload>)> {
        #[derive(Deserialize)]
        struct Persisted {
            #[serde(default)]
            settings: GlobalSettings,
            #[serde(default)]
            tasks: Vec<PersistedTask>,
            #[serde(default)]
            downloads: Vec<crate::download::PersistedDownload>,
        }
        #[derive(Deserialize)]
        struct PersistedTask {
            id: String,
            config: TaskConfig,
            #[serde(default)]
            created_at: u64,
            /// Absent in state files written before edit tracking existed;
            /// those tasks fall back to `created_at` below.
            #[serde(default)]
            updated_at: u64,
            #[serde(default)]
            paused: bool,
        }

        let path: &std::path::Path = &self.persist_path;
        if !path.exists() {
            return Ok((0, Vec::new()));
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
        // 装载域名映射。一条坏规则不该拦住整个启动 —— 记一条警告，其余配置照常
        // 恢复，用户在设置里改回来即可。
        if let Err(e) = crate::hostmap::install(&p.settings.host_mappings) {
            tracing::warn!("persisted host mappings are invalid, ignoring them: {e}");
        }
        // 解析映射目标用的 DNS，同样「坏配置不拦启动」：装不上就退回系统解析。
        match crate::dns::parse_mode(p.settings.dns.as_deref())
            .and_then(|mode| crate::hostmap::install_dns(&mode))
        {
            Ok(()) => {}
            Err(e) => {
                tracing::warn!("persisted dns setting is invalid, using the system resolver: {e}")
            }
        }

        let mut count = 0;
        for pt in p.tasks {
            let entry = Arc::new(TaskEntry::new(pt.config));
            entry.paused.store(pt.paused, Ordering::Relaxed);
            // Force timestamps from disk so "created" age and edit order both
            // survive a restart — `TaskEntry::new` stamps them with "now".
            let created_at = pt.created_at.max(1);
            entry
                .updated_at
                .store(pt.updated_at.max(created_at), Ordering::Relaxed);
            let with_ts = TaskEntry {
                created_at,
                ..Arc::try_unwrap(entry).unwrap_or_else(|_| unreachable!())
            };
            self.insert(pt.id, Arc::new(with_ts));
            count += 1;
        }
        Ok((count, p.downloads))
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
                me.downloads.tick_throughput();

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
                serde_json::to_string(v)
                    .unwrap_or_default()
                    .hash(&mut hasher);
            }
        }
        // 这两项也只在设置里改，不跟着任务变 —— 不进哈希的话，只改了它们的那次
        // 保存永远等不到落盘时机，重启就丢了。
        s.download_dir.hash(&mut hasher);
        for m in &s.host_mappings {
            m.from.hash(&mut hasher);
            m.to.hash(&mut hasher);
            m.enabled.hash(&mut hasher);
        }
        drop(s);
        for (id, e) in self.tasks.read().iter() {
            let cfg = e.config.read();
            if !cfg.persist {
                continue;
            }
            id.hash(&mut hasher);
            // Edit order is persisted state the dashboard sorts on, so a
            // re-save that happens to leave every config field identical still
            // has to reach disk.
            e.updated_at.load(Ordering::Relaxed).hash(&mut hasher);
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
            for m in &cfg.host_mappings {
                m.from.hash(&mut hasher);
                m.to.hash(&mut hasher);
                m.enabled.hash(&mut hasher);
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

#[cfg(test)]
mod tests {
    use super::*;

    fn config(name: &str) -> TaskConfig {
        serde_json::from_value(serde_json::json!({
            "volumes": [["https://example.invalid/a"]],
            "name": name,
        }))
        .expect("TaskConfig fills its own defaults")
    }

    #[test]
    fn an_edit_stamps_the_task_and_a_rejected_one_does_not() {
        // The dashboard orders by this timestamp, so it has to move on a real
        // edit — and hold still when the edit was refused, or a typo would
        // jump a task to the top of the list without changing anything.
        let entry = TaskEntry::new(config("before"));
        let created = entry.updated_at.load(Ordering::Relaxed);
        assert_eq!(
            created, entry.created_at,
            "an unedited task reads as created"
        );

        entry.updated_at.store(created - 60, Ordering::Relaxed);
        entry
            .apply_update(TaskUpdate {
                name: Some(Some("after".into())),
                ..Default::default()
            })
            .expect("renaming is valid");
        let edited = entry.updated_at.load(Ordering::Relaxed);
        assert!(edited > created - 60, "a successful edit must restamp");

        entry.updated_at.store(edited - 60, Ordering::Relaxed);
        assert!(
            entry
                .apply_update(TaskUpdate {
                    max_per_volume: Some(0),
                    ..Default::default()
                })
                .is_err()
        );
        assert_eq!(
            entry.updated_at.load(Ordering::Relaxed),
            edited - 60,
            "a rejected edit must not reorder the list",
        );
    }

    /// `max_threads` 是派生值：单卷并发上限 × 卷数。用户只配前者。    ///
    /// 这两个数字曾经各配一份，于是总有一个会输 —— 单卷文件配 16 线程实际只
    /// 跑 `max_per_volume` 条，多卷任务配小线程数则让大半卷没人认领。
    #[test]
    fn threads_are_derived_from_per_volume_and_volume_count() {
        let two_volumes = vec![
            vec!["http://a/1".to_string()],
            vec!["http://a/2".to_string()],
        ];
        let mut cfg = config("derive");
        cfg.volumes = two_volumes.clone();
        cfg.max_threads = 999;
        cfg.max_per_volume = 4;
        cfg.normalize();
        assert_eq!(cfg.max_threads, 8, "4 × 2 卷");

        // 单卷：总数就是单卷上限，不会虚报一个跑不出来的数字。
        cfg.volumes = vec![vec!["http://a/1".to_string()]];
        cfg.normalize();
        assert_eq!(cfg.max_threads, 4);

        // 编辑单卷上限后要重新派生，PATCH 里带的 max_threads 不作数。
        let mut seed = config("derive");
        seed.volumes = two_volumes;
        seed.max_per_volume = 4;
        let entry = TaskEntry::new(seed);
        entry
            .apply_update(TaskUpdate {
                max_per_volume: Some(8),
                max_threads: Some(3),
                ..Default::default()
            })
            .expect("valid");
        assert_eq!(
            entry.config.read().max_threads,
            16,
            "8 × 2 卷，忽略 PATCH 的 3"
        );

        // 硬上限守住 128。
        let mut huge = config("derive");
        huge.volumes = (0..100).map(|i| vec![format!("http://a/{i}")]).collect();
        huge.max_per_volume = 8;
        huge.normalize();
        assert_eq!(huge.max_threads, MAX_THREADS);
    }

    /// PATCH 的两种「没给值」必须分得开：字段缺席 = 别动，`null` = 清空。
    ///
    /// serde 默认会让 `Option<Option<T>>` 的外层把 `null` 吃掉，两者解出来一模一样
    /// —— 于是任务名怎么也删不掉（面板把空名字发成 `null`，被读成了「别动」）。
    #[test]
    fn a_null_clears_a_nullable_field_while_an_absent_one_leaves_it_alone() {
        let mut cfg = config("named");
        cfg.output_filename = Some("movie.mkv".into());
        let entry = TaskEntry::new(cfg);

        // 缺席：只改别的字段，名字和文件名都留着。
        entry
            .apply_update(serde_json::from_value(serde_json::json!({"cache": true})).unwrap())
            .unwrap();
        assert_eq!(entry.config.read().name.as_deref(), Some("named"));
        assert_eq!(
            entry.config.read().output_filename.as_deref(),
            Some("movie.mkv")
        );

        // 显式 null：清空。
        entry
            .apply_update(
                serde_json::from_value(serde_json::json!({"name": null, "output_filename": null}))
                    .unwrap(),
            )
            .unwrap();
        assert_eq!(entry.config.read().name, None);
        assert_eq!(entry.config.read().output_filename, None);

        // 全空白等同于清空 —— 存一个看不见的名字对谁都没用。
        entry
            .apply_update(serde_json::from_value(serde_json::json!({"name": "  "})).unwrap())
            .unwrap();
        assert_eq!(entry.config.read().name, None);
    }
}
