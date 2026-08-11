use crate::cache::{CacheEntry, CacheMeta, Staging};
use crate::download::{CacheJob, PersistedDownload};
use crate::engine::{Engine, UpstreamProbe, parse_range_header, suggest_volume_filename};
use crate::error::ProxyError;
use crate::fs_pick::{self, PickRequest, PickResponse};
use crate::models::{
    AppState, ConnectionGuard, ContentDispositionMode, GlobalSettingsUpdate, GlobalState,
    TaskConfig, TaskEntry, TaskInfo, TaskUpdate, short_id,
};
use crate::plugins::PluginInfo;
use crate::schedule::Strategy;
use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use futures_util::stream::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use tokio_stream::wrappers::ReceiverStream;

#[derive(Serialize)]
struct CreateResp {
    task_id: String,
    proxy_url: String,
}

#[derive(Serialize)]
struct ApiError {
    error: String,
}

#[derive(Deserialize)]
struct ProbeReq {
    /// Structured volume layout to probe. Each inner Vec is one volume's
    /// mirror URL list. The probe walks every volume, returning the merged
    /// size + a suggested stitched filename.
    #[serde(default)]
    volumes: Vec<Vec<String>>,
    #[serde(default)]
    headers: Option<HashMap<String, String>>,
}

#[derive(Serialize)]
struct ProbeResp {
    /// Filename detected from an upstream Content-Disposition (or URL path
    /// fallback). `None` if probing produced nothing usable.
    detected_filename: Option<String>,
    /// What the UI should put in the output-filename input by default. For
    /// volume mode this is the longest common prefix of the per-volume
    /// filenames; for mirror mode it's the detected filename.
    suggested_filename: Option<String>,
    total_size: Option<u64>,
    content_type: Option<String>,
    accepts_ranges: bool,
}

impl IntoResponse for ProxyError {
    fn into_response(self) -> Response {
        let status = match &self {
            ProxyError::TaskNotFound(_) => StatusCode::NOT_FOUND,
            ProxyError::NoUpstream => StatusCode::BAD_GATEWAY,
            ProxyError::InvalidRange(_) => StatusCode::RANGE_NOT_SATISFIABLE,
            ProxyError::BadStatus(s) => {
                StatusCode::from_u16(*s).unwrap_or(StatusCode::BAD_GATEWAY)
            }
            ProxyError::Upstream(_) => StatusCode::BAD_GATEWAY,
            ProxyError::Io(_) | ProxyError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let body = Json(ApiError {
            error: self.to_string(),
        });
        (status, body).into_response()
    }
}

pub fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/api/tasks", post(create_task).get(list_tasks))
        .route(
            "/api/tasks/{task_id}",
            get(get_task).patch(patch_task).delete(delete_task),
        )
        .route("/api/tasks/{task_id}/pause", post(pause_task))
        .route("/api/tasks/{task_id}/resume", post(resume_task))
        .route(
            "/api/tasks/{task_id}/cache",
            post(start_task_cache).delete(clear_task_cache),
        )
        .route("/api/tasks/{task_id}/cache/pause", post(pause_task_cache))
        .route("/api/cache", delete(clear_all_cache))
        .route("/api/tasks/{task_id}/export", get(export_task))
        .route("/api/probe", post(probe_urls))
        .route("/api/settings", get(get_settings).put(put_settings))
        .route("/api/global", get(get_global))
        .route("/api/plugins", get(list_plugins))
        .route(
            "/api/plugins/{plugin_id}/global",
            get(get_plugin_global).put(put_plugin_global),
        )
        .route("/api/plugins/{plugin_id}/forward", post(plugin_forward))
        .route("/api/fs/pick", post(fs_pick_handler))
        .route("/api/fs/info", get(fs_info))
        .route("/stream/{task_id}", get(stream_task).head(stream_task_head))
        .route("/healthz", get(|| async { "ok" }))
        // Everything else is the embedded dashboard, with an SPA fallback.
        .fallback(crate::assets::static_handler)
        .with_state(state)
}

async fn get_settings(State(state): State<AppState>) -> Json<crate::models::GlobalSettings> {
    Json(state.settings.read().clone())
}

async fn put_settings(
    State(state): State<AppState>,
    Json(upd): Json<GlobalSettingsUpdate>,
) -> Result<Json<crate::models::GlobalSettings>, ProxyError> {
    let s = state.update_settings(upd).map_err(ProxyError::Internal)?;
    Ok(Json(s))
}

async fn get_global(State(state): State<AppState>) -> Json<GlobalState> {
    Json(state.global_state())
}

async fn create_task(
    State(state): State<AppState>,
    Json(mut cfg): Json<TaskConfig>,
) -> Result<Json<CreateResp>, ProxyError> {
    cfg.normalize();
    if cfg.volumes.is_empty() {
        return Err(ProxyError::Internal(
            "at least one URL is required across all volumes".into(),
        ));
    }
    let id = {
        let mut tries = 0;
        loop {
            let candidate = short_id();
            if !state.tasks.read().contains_key(&candidate) {
                break candidate;
            }
            tries += 1;
            if tries > 5 {
                break short_id();
            }
        }
    };
    let entry = Arc::new(TaskEntry::new(cfg));
    state.insert(id.clone(), entry);
    Ok(Json(CreateResp {
        proxy_url: format!("http://{}/stream/{}", state.bind_addr, id),
        task_id: id,
    }))
}

async fn list_tasks(State(state): State<AppState>) -> Json<Vec<TaskInfo>> {
    Json(state.list())
}

/// Cheap one-shot probe used by the create/edit form to preview the detected
/// filename and metadata before the task exists. Builds a throwaway `Engine`
/// with the supplied URLs/volumes/headers, runs the same probe path the
/// streamer would, then derives a "suggested" filename (LCP across volumes).
async fn probe_urls(
    State(_state): State<AppState>,
    Json(req): Json<ProbeReq>,
) -> Result<Json<ProbeResp>, ProxyError> {
    if req.volumes.iter().all(|v| v.is_empty()) {
        return Err(ProxyError::Internal("volumes must not be empty".into()));
    }
    let mut cfg = TaskConfig {
        volumes: req.volumes.clone(),
        max_threads: 1,
        max_per_volume: 1,
        max_split: 5 * 1024 * 1024,
        cache: false,
        headers: req.headers.unwrap_or_default(),
        name: None,
        output_filename: None,
        auto_filename: true,
        rate_limit_bps: 0,
        rate_limit_algorithm: Default::default(),
        persist: false,
        plugins: Vec::new(),
        content_disposition: Default::default(),
    };
    cfg.normalize();
    let layout = cfg.effective_volumes();
    let engine = Engine::new(Arc::new(cfg))?;
    let probe = engine.probe().await?;

    // Suggested filename for the UI: when there are 2+ volumes, take the
    // longest common prefix of the per-volume filenames (the parts are
    // usually named "movie.part01", "movie.part02", … so the LCP is the
    // unsuffixed name the user wants). For one volume — i.e. the historic
    // mirror case — just use what the upstream advertised.
    let suggested = if layout.len() > 1 {
        // One representative URL per volume — pick the first mirror.
        let representatives: Vec<String> =
            layout.iter().filter_map(|v| v.first().cloned()).collect();
        suggest_volume_filename(&representatives).or_else(|| probe.filename.clone())
    } else {
        probe.filename.clone()
    };

    Ok(Json(ProbeResp {
        detected_filename: probe.filename,
        suggested_filename: suggested,
        total_size: probe.total_size,
        content_type: probe.content_type,
        accepts_ranges: probe.accepts_ranges,
    }))
}

async fn get_task(
    State(state): State<AppState>,
    Path(task_id): Path<String>,
) -> Result<Json<TaskInfo>, ProxyError> {
    let entry = state
        .get(&task_id)
        .ok_or_else(|| ProxyError::TaskNotFound(task_id.clone()))?;
    Ok(Json(state.task_info(&task_id, &entry)))
}

async fn patch_task(
    State(state): State<AppState>,
    Path(task_id): Path<String>,
    Json(update): Json<TaskUpdate>,
) -> Result<Json<TaskInfo>, ProxyError> {
    let entry = state
        .get(&task_id)
        .ok_or_else(|| ProxyError::TaskNotFound(task_id.clone()))?;
    // Snapshot the cache key before the URL list changes so we can migrate
    // the on-disk entry if the user just rotated a signed link.
    let old_cache_key = crate::cache::CacheStore::key_for_task(&entry.config_snapshot());
    entry
        .apply_update(update)
        .map_err(ProxyError::Internal)?;
    let new_cache_key = crate::cache::CacheStore::key_for_task(&entry.config_snapshot());
    if old_cache_key != new_cache_key {
        match state.cache.migrate_key(&old_cache_key, &new_cache_key) {
            Ok(true) => tracing::info!(
                "cache migrated for task {}: {} -> {}",
                task_id, old_cache_key, new_cache_key,
            ),
            Ok(false) => {}
            Err(e) => tracing::warn!(
                "cache migration failed for task {} ({} -> {}): {}",
                task_id, old_cache_key, new_cache_key, e,
            ),
        }
    }
    Ok(Json(state.task_info(&task_id, &entry)))
}

async fn delete_task(
    State(state): State<AppState>,
    Path(task_id): Path<String>,
) -> Result<StatusCode, ProxyError> {
    state
        .remove(&task_id)
        .ok_or_else(|| ProxyError::TaskNotFound(task_id.clone()))?;
    // The task is gone, so its coordinator must stop fetching rather than run on
    // as an orphan. Bytes already on disk stay: the cache is keyed by URL, not by
    // task id, so recreating the task resumes from them.
    if let Some(job) = state.downloads.remove(&task_id) {
        job.stop();
    }
    Ok(StatusCode::NO_CONTENT)
}

async fn pause_task(
    State(state): State<AppState>,
    Path(task_id): Path<String>,
) -> Result<Json<TaskInfo>, ProxyError> {
    let entry = state
        .get(&task_id)
        .ok_or_else(|| ProxyError::TaskNotFound(task_id.clone()))?;
    entry.paused.store(true, Ordering::Relaxed);
    Ok(Json(state.task_info(&task_id, &entry)))
}

async fn resume_task(
    State(state): State<AppState>,
    Path(task_id): Path<String>,
) -> Result<Json<TaskInfo>, ProxyError> {
    let entry = state
        .get(&task_id)
        .ok_or_else(|| ProxyError::TaskNotFound(task_id.clone()))?;
    entry.paused.store(false, Ordering::Relaxed);
    Ok(Json(state.task_info(&task_id, &entry)))
}

async fn clear_task_cache(
    State(state): State<AppState>,
    Path(task_id): Path<String>,
) -> Result<StatusCode, ProxyError> {
    let entry = state
        .get(&task_id)
        .ok_or_else(|| ProxyError::TaskNotFound(task_id.clone()))?;
    let cfg = entry.config_snapshot();
    let key = crate::cache::CacheStore::key_for_task(&cfg);
    // Tasks with the same URLs share one entry, so every coordinator on this key
    // has to let go before the directory disappears underneath it.
    state.downloads.stop_for_key(&key);
    state.cache.clear(&key)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn clear_all_cache(
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, ProxyError> {
    // Same reason as `clear_task_cache`: nothing may still be writing into a
    // directory that is about to be removed.
    state.downloads.stop_all();
    let freed = state.cache.clear_all()?;
    Ok(Json(serde_json::json!({ "bytes_freed": freed })))
}

/// Export a task's config as a downloadable JSON file. The body is a
/// `TaskConfig` (no runtime stats), so POSTing it back to `/api/tasks` on
/// any instance recreates the same task — round-trip portable.
async fn export_task(
    State(state): State<AppState>,
    Path(task_id): Path<String>,
) -> Result<Response, ProxyError> {
    let entry = state
        .get(&task_id)
        .ok_or_else(|| ProxyError::TaskNotFound(task_id.clone()))?;
    let cfg = entry.config_snapshot();
    let body = serde_json::to_vec_pretty(&cfg).map_err(|e| ProxyError::Internal(e.to_string()))?;
    let mut headers = HeaderMap::new();
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
    let disp = format!("attachment; filename=\"hydraria-task-{}.json\"", task_id);
    if let Ok(v) = HeaderValue::from_str(&disp) {
        headers.insert(header::CONTENT_DISPOSITION, v);
    }
    let mut resp = Response::new(Body::from(body));
    *resp.headers_mut() = headers;
    Ok(resp)
}

async fn stream_task_head(
    State(state): State<AppState>,
    Path(task_id): Path<String>,
) -> Result<Response, ProxyError> {
    handle_stream(state, task_id, None, true).await
}

async fn stream_task(
    State(state): State<AppState>,
    Path(task_id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ProxyError> {
    let range = headers
        .get(header::RANGE)
        .and_then(|v| v.to_str().ok())
        .map(String::from);
    handle_stream(state, task_id, range, false).await
}

/// Serve the proxy stream. **Playback only** — latency-first scheduling that
/// prioritizes the bytes just past the client's Range start, because that is
/// what a player is blocked on.
///
/// Bulk downloads deliberately do *not* come through here. An HTTP response body
/// is an ordered byte stream, so a download served this way is hostage to
/// whichever request sits at the read head; `POST /api/tasks/:id/download` runs
/// a server-side job instead, writing out of order straight to a file.
async fn handle_stream(
    state: AppState,
    task_id: String,
    range_header: Option<String>,
    head_only: bool,
) -> Result<Response, ProxyError> {
    let entry = state
        .get(&task_id)
        .ok_or_else(|| ProxyError::TaskNotFound(task_id.clone()))?;

    tracing::info!(
        "stream start task={} range={} head_only={}",
        task_id,
        range_header.as_deref().unwrap_or("none"),
        head_only,
    );

    if entry.paused.load(Ordering::Relaxed) {
        tracing::debug!("stream task={} paused, returning 503", task_id);
        let body = Json(ApiError {
            error: format!("task {} is paused", task_id),
        });
        return Ok((StatusCode::SERVICE_UNAVAILABLE, body).into_response());
    }

    let (cfg, probe, engine) = prepare_engine(&state, &task_id, &entry).await?;

    let shared_cache = if !head_only && (cfg.cache || state.downloads.get(&task_id).is_some()) {
        Some(ensure_cache_job(&state, &task_id, &entry).await?)
    } else {
        None
    };
    // Acquire the staging substrate for the legacy per-stream path only when a
    // task-level cache coordinator is not already serving this content.
    let staging: Option<Staging> = if head_only || shared_cache.is_some() {
        None
    } else {
        match resolve_staging(&state, &cfg, &probe) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(
                    "staging unavailable for task {} ({}); falling back to the \
                     in-memory ordered path",
                    task_id,
                    e,
                );
                None
            }
        }
    };
    // The in-memory fallback still benefits from a *persistent* cache entry;
    // an ephemeral scratch file is only meaningful to the staged path.
    let cache_entry: Option<Arc<CacheEntry>> = staging
        .as_ref()
        .filter(|s| !s.is_ephemeral())
        .map(|s| Arc::clone(s.entry()));
    tracing::debug!(
        "stream task={} staging={} mode={}",
        task_id,
        match &staging {
            Some(s) if s.is_ephemeral() => "ephemeral",
            Some(_) => "cache",
            None => "none",
        },
        if probe.accepts_ranges && probe.total_size.is_some() { "ranged" } else { "passthrough" },
    );

    let engine = Arc::new(engine.with_cache(cache_entry.clone()));

    build_stream_response(
        state,
        task_id,
        entry,
        engine,
        probe,
        range_header,
        head_only,
        cfg.content_disposition,
        staging,
        shared_cache,
        Strategy::latency_default(),
    )
    .await
}

/// Probe a task's upstream and assemble a fully-wired `Engine` for it: health
/// accumulators, volume layout, and the plugin transform pipeline.
///
/// Shared by the playback stream and the download API — both need the same
/// probe (which is cached per task, since players re-probe on every seek) and
/// the same engine, and neither should be duplicating this. The returned engine
/// deliberately has **no** cache/staging attached; that is the caller's choice.
async fn prepare_engine(
    state: &AppState,
    task_id: &str,
    entry: &Arc<TaskEntry>,
) -> Result<(Arc<TaskConfig>, UpstreamProbe, Engine), ProxyError> {
    let cfg = Arc::new(entry.config_snapshot());
    let engine = Engine::new(Arc::clone(&cfg))?
        .with_head_unsupported(Arc::clone(&entry.head_unsupported));

    // Probe is expensive on multi-volume tasks (N × HEAD + N × Range:0-0
    // round-trips) and players like PotPlayer open a fresh connection for
    // every seek. Cache the result on the TaskEntry for `PROBE_CACHE_TTL`
    // so subsequent seeks reuse the layout instead of re-probing every
    // volume. `apply_update` clears this cache whenever the volumes or
    // request headers change.
    const PROBE_CACHE_TTL: Duration = Duration::from_secs(300);
    let read_cached = || -> Option<Arc<UpstreamProbe>> {
        let guard = entry.probe_cache.lock();
        guard
            .as_ref()
            .filter(|(_, t)| t.elapsed() < PROBE_CACHE_TTL)
            .map(|(p, _)| Arc::clone(p))
    };
    let mut probe = if let Some(p) = read_cached() {
        tracing::debug!("probe cache HIT task={} (age<{}s)", task_id, PROBE_CACHE_TTL.as_secs());
        UpstreamProbe::clone(&p)
    } else {
        // Singleflight: serialize concurrent first-time probes so they don't
        // all hammer the upstream in parallel. The second caller will block
        // here briefly, then re-check the cache and find the freshly stored
        // result instead of starting its own probe.
        let _guard = entry.probe_inflight.lock().await;
        if let Some(p) = read_cached() {
            tracing::debug!("probe cache HIT task={} (after inflight wait)", task_id);
            UpstreamProbe::clone(&p)
        } else {
            let probe_t0 = Instant::now();
            let fresh = engine.probe().await?;
            tracing::info!(
                "probe ok task={} total={} vols={} accepts_ranges={} etag={:?} ({}ms)",
                task_id,
                fresh.total_size.map(|t| t.to_string()).unwrap_or_else(|| "unknown".into()),
                fresh.volumes.as_ref().map(|v| v.len()).unwrap_or(0),
                fresh.accepts_ranges,
                fresh.etag,
                probe_t0.elapsed().as_millis(),
            );
            *entry.probe_cache.lock() = Some((Arc::new(fresh.clone()), Instant::now()));
            fresh
        }
    };

    // Resolve which filename to advertise. Precedence:
    //   auto_filename=true  → probe result → output_filename → name → URL guess
    //   auto_filename=false → output_filename (None ⇒ no Content-Disposition)
    probe.filename = resolve_served_filename(&cfg, probe.filename.take());

    // Plugin pipeline active? Then the upstream's Content-Type describes
    // CIPHERTEXT (typically `application/octet-stream` from a CDN), not the
    // payload we're about to serve. Override using the served filename's
    // extension when that produces something more specific — otherwise
    // <video>/<audio> tags happily download but refuse to render. The
    // `Inline` content-disposition mode applies the same override even when
    // no plugin is active — user explicitly asked for preview, so we lift
    // the upstream's generic MIME the same way.
    let force_mime_from_filename = cfg.plugins.iter().any(|p| p.enabled)
        || cfg.content_disposition == ContentDispositionMode::Inline;
    if force_mime_from_filename {
        if let Some(name) = &probe.filename {
            let guessed = mime_guess::from_path(name).first_or_octet_stream();
            let g = guessed.essence_str();
            // Override only when the upstream's CT was missing or generic —
            // a real `video/mp4` from the origin should not be clobbered.
            let upstream_is_generic = probe
                .content_type
                .as_deref()
                .map(|ct| ct.eq_ignore_ascii_case("application/octet-stream"))
                .unwrap_or(true);
            if g != "application/octet-stream"
                && upstream_is_generic
                && probe.content_type.as_deref() != Some(g)
            {
                tracing::debug!(
                    "task={} forcing Content-Type from upstream {:?} → {} (from filename '{}')",
                    task_id, probe.content_type, g, name,
                );
                probe.content_type = Some(g.to_string());
            }
        }
    }


    let health = entry.url_health.read().iter().cloned().collect::<Vec<_>>();
    // Build the plugin transform pipeline from the task's enabled plugins
    // plus the global per-plugin config. A build failure here (e.g. wrong
    // key length) becomes a 5xx so the user notices immediately rather
    // than getting silently garbled bytes.
    let pipeline = {
        let globals = state.settings.read().plugin_globals.clone();
        state
            .plugins
            .build_pipeline(&cfg.plugins, &globals)
            .map_err(ProxyError::Internal)?
    };
    let pipeline = if pipeline.is_empty() {
        None
    } else {
        Some(Arc::new(pipeline))
    };
    let engine = engine
        .with_health(health)
        .with_volumes(probe.volumes.clone())
        .with_pipeline(pipeline);
    Ok((cfg, probe, engine))
}

/// Open the task's durable cache entry and install (or reuse) its shared
/// coordinator. Playback and explicit cache warming both call this helper, so
/// they always converge on the same bitmap and worker pool.
async fn ensure_cache_job(
    state: &AppState,
    task_id: &str,
    entry: &Arc<TaskEntry>,
) -> Result<Arc<CacheJob>, ProxyError> {
    if let Some(job) = state.downloads.get(task_id) {
        return Ok(job);
    }
    let (cfg, probe, engine) = prepare_engine(state, task_id, entry).await?;
    if !probe.accepts_ranges {
        return Err(ProxyError::Internal(
            "upstream does not support byte ranges; full caching needs ranges".into(),
       ));
    }
    let total = probe.total_size.filter(|value| *value > 0).ok_or_else(|| {
        ProxyError::Internal("upstream did not report a cacheable size".into())
    })?;
    let mut urls = cfg.urls();
    urls.sort_unstable();
    let meta = CacheMeta {
        etag: probe.etag,
        last_modified: probe.last_modified,
        total_size: total,
        content_type: probe.content_type,
        block_size: crate::cache::BLOCK_SIZE,
        urls,
    };
    let key = crate::cache::CacheStore::key_for_task(&cfg);
    let cache = state.cache.open(&key, meta)?;
    let job = CacheJob::new(task_id.to_string(), Arc::new(engine), cache)?;
    state.downloads.insert(Arc::clone(&job));
    Ok(job)
}
/// Start or continue filling the task's persistent cache. Idempotent while a
/// cache job is already running; already-cached blocks are skipped by bitmap.
async fn start_task_cache(
    State(state): State<AppState>,
    Path(task_id): Path<String>,
) -> Result<Json<crate::download::CacheJobInfo>, ProxyError> {
    let entry = state
        .get(&task_id)
        .ok_or_else(|| ProxyError::TaskNotFound(task_id.clone()))?;
    let job = ensure_cache_job(&state, &task_id, &entry).await?;
    job.start_cache();
    Ok(Json(job.info()))
}
/// Pause only the active whole-file fill. Playback leases remain live and keep
/// using/filling the same cache around their seek positions.
async fn pause_task_cache(
    State(state): State<AppState>,
    Path(task_id): Path<String>,
) -> Result<Json<crate::download::CacheJobInfo>, ProxyError> {
    let job = state
        .downloads
        .get(&task_id)
        .ok_or_else(|| ProxyError::Internal(format!("no cache job for task {task_id}")))?;
    job.pause_cache();
    Ok(Json(job.info()))
}
/// Recreate active cache fills restored from disk. The legacy persisted field
/// is still named `downloads`; old output-specific fields are ignored.
pub async fn restore_downloads(state: AppState, saved: Vec<PersistedDownload>) {
    for persisted in saved {
        if !persisted.was_running {
            continue;
        }
        let Some(entry) = state.get(&persisted.task_id) else {
            continue;
        };
        match ensure_cache_job(&state, &persisted.task_id, &entry).await {
            Ok(job) => job.start_cache(),
            Err(error) => tracing::warn!(
                "restoring cache job {} failed: {}",
                persisted.task_id,
                error,
            ),
        }
    }
}
/// Decide which filename (if any) to advertise on Content-Disposition.
///
/// * `auto_filename = true` → take whatever the upstream probe detected; if
///   probing returned nothing, fall back to the user's saved value, then to
///   the task's display name. This makes "auto" mean "always reflect the live
///   server" without losing a usable name when detection fails.
/// * `auto_filename = false` → use `output_filename` verbatim. Empty/None
///   means "no Content-Disposition header" — let the client pick its own.
fn resolve_served_filename(cfg: &TaskConfig, detected: Option<String>) -> Option<String> {
    let trim_opt = |s: Option<&str>| {
        s.map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
    };
    if cfg.auto_filename {
        detected
            .or_else(|| trim_opt(cfg.output_filename.as_deref()))
            .or_else(|| trim_opt(cfg.name.as_deref()))
    } else {
        trim_opt(cfg.output_filename.as_deref())
    }
}

/// Acquire the staging substrate for one stream.
///
/// Requires byte-range support and a known total size — without both there is
/// nothing to schedule and the caller falls back to a single passthrough
/// stream. `cache: true` yields the durable entry (bytes outlive the stream);
/// otherwise an ephemeral scratch file is used and removed when the last
/// concurrent stream on this content finishes.
fn resolve_staging(
    state: &AppState,
    cfg: &TaskConfig,
    probe: &UpstreamProbe,
) -> Result<Option<Staging>, ProxyError> {
    if !probe.accepts_ranges {
        tracing::trace!("staging skipped: upstream doesn't support ranges");
        return Ok(None);
    }
    let total = match probe.total_size {
        Some(t) if t > 0 => t,
        _ => {
            tracing::trace!("staging skipped: unknown total size");
            return Ok(None);
        }
    };
    let key = crate::cache::CacheStore::key_for_task(cfg);
    // Stored URL list is just a traceability hint — we record it sorted and
    // deduped (across all mirrors of all volumes) so it's stable regardless
    // of how the user ordered things.
    let mut urls = cfg.urls();
    urls.sort_unstable();
    let meta = CacheMeta {
        etag: probe.etag.clone(),
        last_modified: probe.last_modified.clone(),
        total_size: total,
        content_type: probe.content_type.clone(),
        block_size: crate::cache::BLOCK_SIZE,
        urls,
    };
    let staging =
        crate::cache::CacheStore::acquire_staging(&state.cache, &key, meta, cfg.cache)?;
    tracing::debug!(
        "staging ready key={} total={} persistent={} etag={:?}",
        key,
        total,
        !staging.is_ephemeral(),
        probe.etag,
    );
    Ok(Some(staging))
}

#[allow(clippy::too_many_arguments)]
async fn build_stream_response(
    state: AppState,
    task_id: String,
    entry: Arc<TaskEntry>,
    engine: Arc<Engine>,
    probe: UpstreamProbe,
    range_header: Option<String>,
    head_only: bool,
    disposition: ContentDispositionMode,
    staging: Option<Staging>,
    shared_cache: Option<Arc<CacheJob>>,
    strategy: Strategy,
) -> Result<Response, ProxyError> {
    let _ = state; // reserved for future per-state telemetry

    let mut resp_headers = HeaderMap::new();
    if let Some(ct) = &probe.content_type {
        if let Ok(v) = HeaderValue::from_str(ct) {
            resp_headers.insert(header::CONTENT_TYPE, v);
        }
    }
    if let Some(et) = &probe.etag {
        if let Ok(v) = HeaderValue::from_str(et) {
            resp_headers.insert(header::ETAG, v);
        }
    }
    if let Some(lm) = &probe.last_modified {
        if let Ok(v) = HeaderValue::from_str(lm) {
            resp_headers.insert(header::LAST_MODIFIED, v);
        }
    }
    if let Some(name) = &probe.filename {
        if let Some(cd) = build_content_disposition(name, disposition) {
            if let Ok(v) = HeaderValue::from_str(&cd) {
                resp_headers.insert(header::CONTENT_DISPOSITION, v);
            }
        }
    }
    resp_headers.insert(
        header::ACCEPT_RANGES,
        HeaderValue::from_static(if probe.accepts_ranges { "bytes" } else { "none" }),
    );
    resp_headers.insert(
        HeaderName::from_static("x-hydraria-task"),
        HeaderValue::from_str(&task_id).unwrap(),
    );

    // If upstream doesn't support ranges or size unknown, do passthrough.
    if !probe.accepts_ranges || probe.total_size.is_none() {
        if let Some(total) = probe.total_size {
            resp_headers.insert(
                header::CONTENT_LENGTH,
                HeaderValue::from_str(&total.to_string()).unwrap(),
            );
        }
        if head_only {
            let mut resp = Response::new(Body::empty());
            *resp.status_mut() = StatusCode::OK;
            *resp.headers_mut() = resp_headers;
            return Ok(resp);
        }
        let upstream = engine.open_passthrough(None).await?;
        let conn_guard = Arc::new(ConnectionGuard::new(Arc::clone(&entry)));
        let entry_for_count = Arc::clone(&entry);
        let state_for_count = state.clone();
        let task_limiter = Arc::clone(&entry.limiter);
        let global_limiter = Arc::clone(&state.global_limiter);
        let engine_for_transform = Arc::clone(&engine);
        // Passthrough = single linear stream from upstream byte 0. Track the
        // running merged offset across chunks so the plugin pipeline (if any)
        // sees each byte's true position in the file. `Mutex<u64>` rather
        // than atomic because we read+write atomically per chunk and the
        // mutex is uncontended (chunks process strictly in order here).
        let merged_cursor = Arc::new(parking_lot::Mutex::new(0u64));
        let stream = upstream.bytes_stream().then(move |item| {
            let entry_for_count = Arc::clone(&entry_for_count);
            let state_for_count = state_for_count.clone();
            let task_limiter = Arc::clone(&task_limiter);
            let global_limiter = Arc::clone(&global_limiter);
            let engine_for_transform = Arc::clone(&engine_for_transform);
            let merged_cursor = Arc::clone(&merged_cursor);
            // Keeps the connection gauge up until the stream is dropped.
            let conn_guard = Arc::clone(&conn_guard);
            async move {
                let _conn = conn_guard;
                let item = match item {
                    Ok(b) => {
                        let n = b.len() as u64;
                        let merged_offset = {
                            let mut c = merged_cursor.lock();
                            let off = *c;
                            *c += n;
                            off
                        };
                        let b = engine_for_transform.transform_outgoing_public(merged_offset, b);
                        global_limiter.acquire(n).await;
                        task_limiter.acquire(n).await;
                        entry_for_count.count_bytes(n);
                        state_for_count.count_bytes_global(n);
                        Ok(b)
                    }
                    Err(e) => Err(e),
                };
                item
            }
        });
        let body = Body::from_stream(stream);
        let mut resp = Response::new(body);
        *resp.status_mut() = StatusCode::OK;
        *resp.headers_mut() = resp_headers;
        return Ok(resp);
    }

    let total = probe.total_size.unwrap();
    let had_range = range_header.is_some();
    // Open-ended: client either sent no Range header at all or sent
    // `Range: X-` (no explicit upper bound). These are the requests where a
    // seek is most likely to come next, so the engine applies its head-zone
    // + abort-on-disconnect optimizations to keep seek latency low.
    let mut open_ended = !had_range;
    let (start, end) = if let Some(rh) = range_header {
        let (s, e) = parse_range_header(&rh, Some(total))?;
        if e.is_none() {
            open_ended = true;
        }
        let end = e.unwrap_or(total - 1).min(total - 1);
        if s > end {
            return Err(ProxyError::InvalidRange(rh));
        }
        (s, end)
    } else {
        (0, total.saturating_sub(1))
    };

    let length = end - start + 1;
    resp_headers.insert(
        header::CONTENT_LENGTH,
        HeaderValue::from_str(&length.to_string()).unwrap(),
    );

    // Per RFC 7233: respond 206 whenever the client sent a Range header that
    // we honor, even when it covers the entire file. Chrome's <video>
    // element uses 200-vs-206 to decide whether seeking is supported.
    let status = if had_range {
        resp_headers.insert(
            header::CONTENT_RANGE,
            HeaderValue::from_str(&format!("bytes {}-{}/{}", start, end, total)).unwrap(),
        );
        StatusCode::PARTIAL_CONTENT
    } else {
        StatusCode::OK
    };

    if head_only {
        let mut resp = Response::new(Body::empty());
        *resp.status_mut() = status;
        *resp.headers_mut() = resp_headers;
        return Ok(resp);
    }

    let conn_guard = Arc::new(ConnectionGuard::new(Arc::clone(&entry)));
    let entry_for_count = Arc::clone(&entry);
    let state_for_count = state.clone();
    let task_limiter = Arc::clone(&entry.limiter);
    let global_limiter = Arc::clone(&state.global_limiter);

    // Staged path when a staging file is available: workers fetch out of order
    // straight to disk and an ordered reader streams from it, so memory no
    // longer bounds how much reordering we can absorb. Falls back to the
    // in-memory ordered path (bounded lookahead, fixed splits) otherwise.
    let rx = match shared_cache {
        Some(job) => job.stream(start, end),
        None => match staging {
            Some(s) => engine.stream_staged(start, end, strategy, s),
            None => engine.stream_range(start, end, open_ended),
        },
    };
    let stream = ReceiverStream::new(rx).then(move |item| {
        let entry_for_count = Arc::clone(&entry_for_count);
        let state_for_count = state_for_count.clone();
        let task_limiter = Arc::clone(&task_limiter);
        let global_limiter = Arc::clone(&global_limiter);
        // Keeps the connection gauge up until the stream is dropped.
        let conn_guard = Arc::clone(&conn_guard);
        async move {
            let _conn = conn_guard;
            match item {
                Ok(b) => {
                    let n = b.len() as u64;
                    global_limiter.acquire(n).await;
                    task_limiter.acquire(n).await;
                    entry_for_count.count_bytes(n);
                    state_for_count.count_bytes_global(n);
                    Ok::<_, std::io::Error>(b)
                }
                Err(e) => Err(std::io::Error::other(e.to_string())),
            }
        }
    });

    let body = Body::from_stream(stream);
    let mut resp = Response::new(body);
    *resp.status_mut() = status;
    *resp.headers_mut() = resp_headers;
    Ok(resp)
}

/// Build an RFC 6266-compliant Content-Disposition header value for a filename.
/// Emits both an ASCII `filename=` fallback and an RFC 5987 `filename*=UTF-8''…`
/// form whenever the name contains anything non-ASCII or quote-unsafe, so old
/// clients see something readable while modern ones get the exact name. The
/// `disposition` knob picks `inline` (the historic default and the explicit
/// preview mode) vs `attachment` (force-download).
fn build_content_disposition(
    filename: &str,
    disposition: ContentDispositionMode,
) -> Option<String> {
    let name = filename.trim();
    if name.is_empty() {
        return None;
    }
    let disp_token = match disposition {
        ContentDispositionMode::Attachment => "attachment",
        ContentDispositionMode::Auto | ContentDispositionMode::Inline => "inline",
    };
    let needs_encoding = name
        .bytes()
        .any(|b| !b.is_ascii() || b < 0x20 || b == b'"' || b == b'\\');
    let ascii_fallback: String = name
        .chars()
        .map(|c| {
            if c.is_ascii() && !c.is_ascii_control() && c != '"' && c != '\\' {
                c
            } else {
                '_'
            }
        })
        .collect();
    if !needs_encoding {
        Some(format!("{}; filename=\"{}\"", disp_token, ascii_fallback))
    } else {
        let encoded = percent_encode_rfc5987(name);
        Some(format!(
            "{}; filename=\"{}\"; filename*=UTF-8''{}",
            disp_token, ascii_fallback, encoded
        ))
    }
}

fn percent_encode_rfc5987(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        // RFC 5987 attr-char set.
        let safe = b.is_ascii_alphanumeric()
            || matches!(
                b,
                b'!' | b'#' | b'$' | b'&' | b'+' | b'-' | b'.' | b'^' | b'_' | b'`' | b'|' | b'~'
            );
        if safe {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{:02X}", b));
        }
    }
    out
}

/// Aggregated metadata for one plugin: registry-defined fields + the
/// **current** global config in effect. `global` is always populated — when
/// the user hasn't customized anything, it's the plugin's defaults.
#[derive(Serialize)]
struct PluginEntry {
    #[serde(flatten)]
    info: PluginInfo,
    global: serde_json::Value,
}

/// `GET /api/plugins` — list every registered plugin with metadata + current
/// global config. The dashboard uses this to render config forms and decide
/// whether to expose a forward (tool) panel.
async fn list_plugins(State(state): State<AppState>) -> Json<Vec<PluginEntry>> {
    let globals = state.settings.read().plugin_globals.clone();
    let mut out: Vec<PluginEntry> = Vec::new();
    for info in state.plugins.info_list(&globals) {
        let global = globals
            .get(&info.id)
            .cloned()
            .unwrap_or_else(|| info.default_global.clone());
        out.push(PluginEntry { info, global });
    }
    Json(out)
}

/// `GET /api/plugins/:plugin_id/global` — single-plugin variant of the
/// listing endpoint, handy for refreshing one config block without
/// re-fetching the whole catalog.
async fn get_plugin_global(
    State(state): State<AppState>,
    Path(plugin_id): Path<String>,
) -> Result<Json<serde_json::Value>, ProxyError> {
    state
        .plugins
        .get(&plugin_id)
        .ok_or_else(|| ProxyError::Internal(format!("unknown plugin: {plugin_id}")))?;
    let g = state
        .settings
        .read()
        .plugin_globals
        .get(&plugin_id)
        .cloned()
        .unwrap_or_else(|| serde_json::Value::Object(Default::default()));
    Ok(Json(g))
}

/// `PUT /api/plugins/:plugin_id/global` — overwrite the global config for
/// one plugin. Validated against the plugin's own schema before being
/// committed; bad values surface as 400-ish errors back to the form.
async fn put_plugin_global(
    State(state): State<AppState>,
    Path(plugin_id): Path<String>,
    Json(value): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, ProxyError> {
    let plugin = state
        .plugins
        .get(&plugin_id)
        .ok_or_else(|| ProxyError::Internal(format!("unknown plugin: {plugin_id}")))?;
    plugin
        .validate_global_config(&value)
        .map_err(|e| ProxyError::Internal(format!("invalid global config: {e}")))?;
    {
        let mut s = state.settings.write();
        s.plugin_globals.insert(plugin_id.clone(), value.clone());
    }
    Ok(Json(value))
}

#[derive(Deserialize)]
struct ForwardReq {
    /// Per-task config — same shape that would go in `TaskConfig.plugins[i].config`.
    /// Optional: when omitted, the plugin's defaults are used (and if
    /// `generate_missing` is enabled, fresh secrets are generated).
    #[serde(default)]
    task: serde_json::Value,
    /// Forward-only parameters (input/output paths, etc.).
    #[serde(default)]
    params: serde_json::Value,
}

/// `POST /api/plugins/:plugin_id/forward` — run the plugin's sender-side
/// operation and stream **NDJSON** progress + final result back. Each line
/// is one JSON object:
///
///   * `{"type":"progress","bytes_done":..,"bytes_total":..,"phase":".."}`
///   * `{"type":"result", ...}`  — the final ForwardResult fields, flattened
///     onto the same object (`bytes_in`, `bytes_out`, `info`, `message`)
///   * `{"type":"error","error":".."}`
///
/// We chose NDJSON over SSE so the existing `fetch()` plumbing on the UI
/// (which already JSON-decodes responses) only needs a stream-reader added
/// around it — no `EventSource`, no second protocol.
///
/// File IO runs on a blocking pool so it doesn't stall the tokio runtime —
/// the operation can be multi-GB on a fast disk.
async fn plugin_forward(
    State(state): State<AppState>,
    Path(plugin_id): Path<String>,
    Json(req): Json<ForwardReq>,
) -> Result<Response, ProxyError> {
    let plugin = state
        .plugins
        .get(&plugin_id)
        .ok_or_else(|| ProxyError::Internal(format!("unknown plugin: {plugin_id}")))?;
    if !plugin.has_forward() {
        return Err(ProxyError::Internal(format!(
            "plugin '{plugin_id}' does not expose a forward tool"
        )));
    }
    let global = state
        .settings
        .read()
        .plugin_globals
        .get(&plugin_id)
        .cloned()
        .unwrap_or_else(|| plugin.default_global_config());

    // Channel from blocking encrypt loop → SSE-style streaming response.
    // Buffered so a slow client doesn't backpressure the encrypt loop into
    // stalling — we'd rather drop progress events than slow the work down.
    let (tx, rx) = tokio::sync::mpsc::channel::<bytes::Bytes>(32);

    let plugin_for_blocking = Arc::clone(&plugin);
    let task = req.task;
    let params = req.params;
    let progress_tx = tx.clone();
    tokio::task::spawn_blocking(move || {
        // The progress callback runs synchronously inside the encrypt loop,
        // so we use the channel's `try_send` which never blocks. When the
        // buffer is full we drop the event — the next tick will catch up.
        let progress: crate::plugins::ProgressSender = Arc::new(move |p| {
            let payload = serde_json::json!({
                "type": "progress",
                "bytes_done": p.bytes_done,
                "bytes_total": p.bytes_total,
                "phase": p.phase,
            });
            if let Ok(mut s) = serde_json::to_vec(&payload) {
                s.push(b'\n');
                let _ = progress_tx.try_send(bytes::Bytes::from(s));
            }
        });
        let outcome = plugin_for_blocking.forward(&global, &task, &params, progress);
        let final_payload = match outcome {
            Ok(r) => serde_json::json!({
                "type": "result",
                "bytes_in": r.bytes_in,
                "bytes_out": r.bytes_out,
                "info": r.info,
                "message": r.message,
            }),
            Err(e) => serde_json::json!({
                "type": "error",
                "error": e,
            }),
        };
        if let Ok(mut s) = serde_json::to_vec(&final_payload) {
            s.push(b'\n');
            // Final frame: blocking_send so it can't be dropped by a full
            // buffer — without this the UI may never see the result on a
            // backpressured channel.
            let _ = tx.blocking_send(bytes::Bytes::from(s));
        }
    });

    let stream = ReceiverStream::new(rx).map(Ok::<_, std::io::Error>);
    let body = Body::from_stream(stream);
    let mut resp = Response::new(body);
    *resp.status_mut() = StatusCode::OK;
    resp.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/x-ndjson"),
    );
    // Disable proxy buffering (nginx and friends will hold the whole body
    // otherwise, defeating the point of streaming).
    resp.headers_mut().insert(
        HeaderName::from_static("x-accel-buffering"),
        HeaderValue::from_static("no"),
    );
    Ok(resp)
}

/// `GET /api/fs/info` — small capability probe so the UI knows whether to
/// render "Browse..." buttons. Avoids having the UI guess from `navigator.userAgent`.
async fn fs_info() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "picker_supported": fs_pick::is_supported(),
        "platform": std::env::consts::OS,
    }))
}

/// `POST /api/fs/pick` — pop a native open-file / open-dir / save-as dialog
/// and return the user's selection. Runs on the blocking pool because the
/// OS dialog is synchronous (and may sit for minutes if the user takes
/// their time).
async fn fs_pick_handler(
    State(_state): State<AppState>,
    Json(req): Json<PickRequest>,
) -> Result<Json<PickResponse>, ProxyError> {
    let resp = tokio::task::spawn_blocking(move || fs_pick::pick(req))
        .await
        .map_err(|e| ProxyError::Internal(format!("fs/pick task join: {e}")))?
        .map_err(ProxyError::Internal)?;
    Ok(Json(resp))
}
