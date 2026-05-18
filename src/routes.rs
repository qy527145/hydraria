use crate::engine::{Engine, parse_range_header};
use crate::error::ProxyError;
use crate::models::{AppState, TaskConfig, TaskEntry, TaskInfo, short_id};
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
        .route("/api/tasks/{task_id}", delete(delete_task).get(get_task))
        .route("/stream/{task_id}", get(stream_task).head(stream_task_head))
        .route("/", get(crate::assets::serve_index))
        .route("/healthz", get(|| async { "ok" }))
        .route("/{*path}", get(crate::assets::serve_asset))
        .with_state(state)
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
    Ok(Json(TaskInfo {
        task_id: task_id.clone(),
        proxy_url: format!("http://{}/stream/{}", state.bind_addr, task_id),
        config: entry.config.clone(),
        created_at: entry.created_at,
        bytes_served: entry.bytes_served.load(Ordering::Relaxed),
        active_connections: entry.active_connections.load(Ordering::Relaxed),
    }))
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

    let cfg = Arc::new(entry.config.clone());
    let engine = Arc::new(Engine::new(Arc::clone(&cfg))?);

    let probe = engine.probe().await?;

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
        let entry_for_count = Arc::clone(&entry);
        entry_for_count.active_connections.fetch_add(1, Ordering::Relaxed);
        let stream = upstream.bytes_stream().map(move |item| {
            if let Ok(ref b) = item {
                entry_for_count
                    .bytes_served
                    .fetch_add(b.len() as u64, Ordering::Relaxed);
            }
            item
        });
        let body = Body::from_stream(stream);
        let mut resp = Response::new(body);
        *resp.status_mut() = StatusCode::OK;
        *resp.headers_mut() = resp_headers;
        // Decrement on drop is best-effort; here we just leave it incremented for the lifetime
        // of this connection — see note in README.
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

    // Per RFC 7233: if the client sent a Range header that we honor, the
    // response MUST be 206 (with Content-Range) — even when the range
    // happens to cover the entire resource. Chrome's <video> element uses
    // the 206-vs-200 distinction to decide whether the server supports
    // seeking; returning 200 here makes playback hang ("downloading at full
    // speed but the spinner keeps spinning").
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

    let entry_for_count = Arc::clone(&entry);
    entry_for_count.active_connections.fetch_add(1, Ordering::Relaxed);

    let rx = engine.stream_range(start, end);
    let stream = ReceiverStream::new(rx).map(move |item| {
        match item {
            Ok(b) => {
                entry_for_count
                    .bytes_served
                    .fetch_add(b.len() as u64, Ordering::Relaxed);
                Ok::<_, std::io::Error>(b)
            }
            Err(e) => Err(std::io::Error::other(e.to_string())),
        }
    });

    let body = Body::from_stream(stream);
    let mut resp = Response::new(body);
    *resp.status_mut() = status;
    *resp.headers_mut() = resp_headers;
    Ok(resp)
}
