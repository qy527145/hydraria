use crate::cache::{CacheEntry, CacheMeta};
use crate::engine::{Engine, UpstreamProbe, parse_range_header};
use crate::error::ProxyError;
use crate::models::{
    AppState, GlobalSettingsUpdate, GlobalState, TaskConfig, TaskEntry, TaskInfo, TaskUpdate,
    short_id,
};
use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use futures_util::stream::StreamExt;
use serde::Serialize;
use std::sync::Arc;
use std::sync::atomic::Ordering;
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
        .route("/api/tasks/{task_id}/cache", delete(clear_task_cache))
        .route("/api/settings", get(get_settings).put(put_settings))
        .route("/api/global", get(get_global))
        .route("/stream/{task_id}", get(stream_task).head(stream_task_head))
        .route("/", get(crate::assets::serve_index))
        .route("/healthz", get(|| async { "ok" }))
        .route("/{*path}", get(crate::assets::serve_asset))
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
    Json(cfg): Json<TaskConfig>,
) -> Result<Json<CreateResp>, ProxyError> {
    if cfg.urls.is_empty() {
        return Err(ProxyError::Internal("urls must not be empty".into()));
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
    entry
        .apply_update(update)
        .map_err(ProxyError::Internal)?;
    Ok(Json(state.task_info(&task_id, &entry)))
}

async fn delete_task(
    State(state): State<AppState>,
    Path(task_id): Path<String>,
) -> Result<StatusCode, ProxyError> {
    state
        .remove(&task_id)
        .ok_or_else(|| ProxyError::TaskNotFound(task_id))?;
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
        .ok_or_else(|| ProxyError::TaskNotFound(task_id))?;
    let cfg = entry.config_snapshot();
    let key = crate::cache::CacheStore::key_for_urls(&cfg.urls);
    state.cache.clear(&key)?;
    Ok(StatusCode::NO_CONTENT)
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

async fn handle_stream(
    state: AppState,
    task_id: String,
    range_header: Option<String>,
    head_only: bool,
) -> Result<Response, ProxyError> {
    let entry = state
        .get(&task_id)
        .ok_or_else(|| ProxyError::TaskNotFound(task_id.clone()))?;

    if entry.paused.load(Ordering::Relaxed) {
        let body = Json(ApiError {
            error: format!("task {} is paused", task_id),
        });
        return Ok((StatusCode::SERVICE_UNAVAILABLE, body).into_response());
    }

    let cfg = Arc::new(entry.config_snapshot());
    let engine = Engine::new(Arc::clone(&cfg))?;
    let probe = engine.probe().await?;

    // Resolve cache entry up-front when (a) the task wants caching, (b) the
    // upstream supports ranges, and (c) we know a real total size. Any
    // mismatch with previously-stored meta wipes the on-disk cache and
    // starts over (auto-clear policy).
    let cache_entry: Option<Arc<CacheEntry>> =
        match resolve_cache(&state, &cfg, &probe) {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!("cache disabled for task {}: {}", task_id, e);
                None
            }
        };

    let health = entry.url_health.read().iter().cloned().collect::<Vec<_>>();
    let engine = Arc::new(engine.with_cache(cache_entry.clone()).with_health(health));

    build_stream_response(state, task_id, entry, engine, probe, range_header, head_only)
        .await
}

fn resolve_cache(
    state: &AppState,
    cfg: &TaskConfig,
    probe: &UpstreamProbe,
) -> Result<Option<Arc<CacheEntry>>, ProxyError> {
    if !cfg.cache || !probe.accepts_ranges {
        return Ok(None);
    }
    let total = match probe.total_size {
        Some(t) if t > 0 => t,
        _ => return Ok(None),
    };
    let mut urls = cfg.urls.clone();
    urls.sort_unstable();
    let key = crate::cache::CacheStore::key_for_urls(&urls);
    let meta = CacheMeta {
        etag: probe.etag.clone(),
        last_modified: probe.last_modified.clone(),
        total_size: total,
        content_type: probe.content_type.clone(),
        block_size: crate::cache::BLOCK_SIZE,
        urls,
    };
    state.cache.open(&key, meta).map(Some)
}

async fn build_stream_response(
    state: AppState,
    task_id: String,
    entry: Arc<TaskEntry>,
    engine: Arc<Engine>,
    probe: UpstreamProbe,
    range_header: Option<String>,
    head_only: bool,
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
        entry.active_connections.fetch_add(1, Ordering::Relaxed);
        let entry_for_count = Arc::clone(&entry);
        let state_for_count = state.clone();
        let task_limiter = Arc::clone(&entry.limiter);
        let global_limiter = Arc::clone(&state.global_limiter);
        let stream = upstream.bytes_stream().then(move |item| {
            let entry_for_count = Arc::clone(&entry_for_count);
            let state_for_count = state_for_count.clone();
            let task_limiter = Arc::clone(&task_limiter);
            let global_limiter = Arc::clone(&global_limiter);
            async move {
                if let Ok(ref b) = item {
                    let n = b.len() as u64;
                    global_limiter.acquire(n).await;
                    task_limiter.acquire(n).await;
                    entry_for_count.count_bytes(n);
                    state_for_count.count_bytes_global(n);
                }
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
    let (start, end) = if let Some(rh) = range_header {
        let (s, e) = parse_range_header(&rh, Some(total))?;
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

    entry.active_connections.fetch_add(1, Ordering::Relaxed);
    let entry_for_count = Arc::clone(&entry);
    let state_for_count = state.clone();
    let task_limiter = Arc::clone(&entry.limiter);
    let global_limiter = Arc::clone(&state.global_limiter);

    let rx = engine.stream_range(start, end);
    let stream = ReceiverStream::new(rx).then(move |item| {
        let entry_for_count = Arc::clone(&entry_for_count);
        let state_for_count = state_for_count.clone();
        let task_limiter = Arc::clone(&task_limiter);
        let global_limiter = Arc::clone(&global_limiter);
        async move {
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
