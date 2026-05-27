use crate::cache::CacheEntry;
use crate::error::{ProxyError, Result};
use crate::models::{TaskConfig, UrlHealthAcc};
use crate::plugins::TransformPipeline;
use bytes::Bytes;
use futures_util::StreamExt;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, RANGE};
use reqwest::{Client, StatusCode};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// Adaptive first-chunk sizing for open-ended `Range: X-` requests. The
/// leading `HEAD_SMALL_COUNT` chunks are capped at `HEAD_SMALL_SPLIT` so a
/// player that issues `Range: 0-` purely to probe the file (and then seeks
/// elsewhere) only has a few hundred KB of in-flight work to abandon.
const HEAD_SMALL_SPLIT: u64 = 256 * 1024;
const HEAD_SMALL_COUNT: usize = 4;

/// One chunk of a multi-volume task: an ordered list of mirror URLs that all
/// serve this volume's bytes, plus its position in the merged stream. A
/// single-volume task has exactly one `VolumeMeta` covering the whole file.
#[derive(Debug, Clone)]
pub struct VolumeMeta {
    pub urls: Vec<String>,
    pub offset: u64,
    pub size: u64,
}

#[derive(Clone, Debug)]
pub struct UpstreamProbe {
    pub total_size: Option<u64>,
    pub accepts_ranges: bool,
    pub content_type: Option<String>,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub filename: Option<String>,
    /// Per-volume layout once the upstream probe has populated it. `None`
    /// before probing, and never `None` afterwards: even a single-volume
    /// task gets one `VolumeMeta` covering the whole file.
    pub volumes: Option<Vec<VolumeMeta>>,
}

pub struct Engine {
    client: Client,
    config: Arc<TaskConfig>,
    rr_counter: AtomicUsize,
    cache: Option<Arc<CacheEntry>>,
    health: Vec<Arc<UrlHealthAcc>>,
    volumes: Option<Arc<Vec<VolumeMeta>>>,
    /// Plugin post-processing pipeline applied to bytes on the way out to
    /// the client. `None` means "no transform" (the original zero-copy path);
    /// `Some(empty)` is equivalent and short-circuits to the same fast path.
    pipeline: Option<Arc<TransformPipeline>>,
    /// Shared (task-scoped) set of URLs that have failed a HEAD request at
    /// least once. `probe_one` consults this to skip the HEAD round-trip on
    /// known-bad URLs and go straight to the 1-byte Range GET.
    head_unsupported: Option<Arc<parking_lot::RwLock<std::collections::HashSet<String>>>>,
}

/// Where and what bytes a fetch attempt should request from a concrete URL.
struct FetchTarget {
    url: String,
    /// HTTP `Range` request bounds, in the upstream URL's own coordinate space.
    local_start: u64,
    local_end: u64,
    /// Same start expressed in the merged-stream coordinate space — used for
    /// cache offsets and chunk-window slicing as bytes stream in.
    merged_start: u64,
}

impl Engine {
    pub fn new(config: Arc<TaskConfig>) -> Result<Self> {
        let client = Client::builder()
            .pool_max_idle_per_host(64)
            .tcp_nodelay(true)
            .timeout(Duration::from_secs(60))
            .connect_timeout(Duration::from_secs(15))
            .http1_title_case_headers()
            .build()
            .map_err(ProxyError::Upstream)?;

        Ok(Self {
            client,
            config,
            rr_counter: AtomicUsize::new(0),
            cache: None,
            health: Vec::new(),
            volumes: None,
            pipeline: None,
            head_unsupported: None,
        })
    }

    pub fn with_cache(mut self, cache: Option<Arc<CacheEntry>>) -> Self {
        self.cache = cache;
        self
    }

    pub fn with_health(mut self, health: Vec<Arc<UrlHealthAcc>>) -> Self {
        self.health = health;
        self
    }

    pub fn with_volumes(mut self, volumes: Option<Vec<VolumeMeta>>) -> Self {
        self.volumes = volumes.map(Arc::new);
        self
    }

    /// Plug in the task-shared set of URLs known to reject HEAD requests.
    /// `probe_one` reads this on entry to skip the HEAD round-trip and writes
    /// to it when a HEAD fails for the first time. Without this set installed
    /// the engine treats every HEAD failure as a one-shot retry (the old
    /// behavior).
    pub fn with_head_unsupported(
        mut self,
        set: Arc<parking_lot::RwLock<std::collections::HashSet<String>>>,
    ) -> Self {
        self.head_unsupported = Some(set);
        self
    }

    /// Install the per-task plugin pipeline. Bytes flowing to the client are
    /// passed through every transform (in reverse-of-stored order) before
    /// being sent. Empty / `None` pipelines skip the work entirely.
    pub fn with_pipeline(mut self, pipeline: Option<Arc<TransformPipeline>>) -> Self {
        self.pipeline = pipeline.filter(|p| !p.is_empty());
        self
    }

    /// Apply the pipeline to `data` whose first byte sits at `merged_offset`
    /// in the merged-file coordinate space. When no pipeline is configured
    /// this is a zero-copy passthrough (returns the original `Bytes`); with
    /// a pipeline it materializes a writeable copy, transforms in place, and
    /// returns the frozen result. Net cost: one clone of the slice's bytes
    /// when transforms are active, zero otherwise.
    fn transform_outgoing(&self, merged_offset: u64, data: Bytes) -> Bytes {
        match &self.pipeline {
            Some(p) if !p.is_empty() => {
                if data.is_empty() {
                    return data;
                }
                let mut buf = data.to_vec();
                p.apply_reverse(merged_offset, &mut buf);
                Bytes::from(buf)
            }
            _ => data,
        }
    }

    /// Same as `transform_outgoing` but callable from outside the engine —
    /// used by the passthrough path in `routes::build_stream_response`,
    /// which lives outside `Engine::stream_range` and so doesn't have a
    /// direct hook into the chunk fetchers' send-sites.
    pub fn transform_outgoing_public(&self, merged_offset: u64, data: Bytes) -> Bytes {
        self.transform_outgoing(merged_offset, data)
    }

    /// True when a non-empty pipeline is installed. Callers can skip
    /// per-byte counters / offset bookkeeping when this is false.
    pub fn has_pipeline(&self) -> bool {
        matches!(&self.pipeline, Some(p) if !p.is_empty())
    }

    fn now_unix() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    }

    /// Increments the per-URL in-flight counter and returns a guard that
    /// decrements on drop. Held across the dial + body stream so the
    /// dashboard reports both connect and download phases as busy. Returns
    /// `None` for URLs not in the health table (e.g. one-shot probes).
    fn in_flight_guard(&self, url: &str) -> Option<InFlightGuard> {
        let h = self.health.iter().find(|h| h.url == url)?.clone();
        h.in_flight_requests.fetch_add(1, Ordering::Relaxed);
        Some(InFlightGuard(h))
    }

    fn record_success(&self, url: &str, status: u16, latency_ms: u64) {
        if let Some(h) = self.health.iter().find(|h| h.url == url) {
            *h.last_status.lock() = Some(status);
            *h.last_error.lock() = None;
            h.last_latency_ms.store(latency_ms, Ordering::Relaxed);
            h.successful_requests.fetch_add(1, Ordering::Relaxed);
            h.last_used_at.store(Self::now_unix(), Ordering::Relaxed);
        }
    }

    fn record_failure(&self, url: &str, status: Option<u16>, err: &str) {
        if let Some(h) = self.health.iter().find(|h| h.url == url) {
            *h.last_status.lock() = status;
            *h.last_error.lock() = Some(err.to_string());
            h.failed_requests.fetch_add(1, Ordering::Relaxed);
            h.last_used_at.store(Self::now_unix(), Ordering::Relaxed);
        }
    }

    fn record_bytes(&self, url: &str, n: u64) {
        if let Some(h) = self.health.iter().find(|h| h.url == url) {
            h.bytes_contributed.fetch_add(n, Ordering::Relaxed);
            h.window_bytes.fetch_add(n, Ordering::Relaxed);
        }
    }

    fn build_headers(&self, extra: Option<(u64, u64)>) -> Result<HeaderMap> {
        let mut headers = HeaderMap::new();
        for (k, v) in &self.config.headers {
            let name = HeaderName::from_bytes(k.as_bytes())
                .map_err(|e| ProxyError::Internal(format!("bad header name {k}: {e}")))?;
            let val = HeaderValue::from_str(v)
                .map_err(|e| ProxyError::Internal(format!("bad header value: {e}")))?;
            headers.insert(name, val);
        }
        if let Some((start, end)) = extra {
            let range = format!("bytes={start}-{end}");
            headers.insert(
                RANGE,
                HeaderValue::from_str(&range)
                    .map_err(|e| ProxyError::Internal(format!("bad range: {e}")))?,
            );
        }
        Ok(headers)
    }

    fn pick_url(&self) -> Result<String> {
        let urls = self.config.urls();
        if urls.is_empty() {
            return Err(ProxyError::NoUpstream);
        }
        let i = self.rr_counter.fetch_add(1, Ordering::Relaxed);
        Ok(urls[i % urls.len()].clone())
    }

    fn pick_url_for(&self, idx: usize) -> Result<String> {
        let urls = self.config.urls();
        if urls.is_empty() {
            return Err(ProxyError::NoUpstream);
        }
        Ok(urls[idx % urls.len()].clone())
    }

    /// True when the task spans more than one volume — the case where a 200
    /// OK to a ranged request is unsafe (we can't stitch volumes if the first
    /// one's mirror dumps an entire file). Single-volume tasks (the historic
    /// mirror case) tolerate 200 OK by slicing locally.
    fn is_multi_volume(&self) -> bool {
        self.volumes.as_ref().map(|v| v.len() > 1).unwrap_or(false)
    }

    /// Volume containing `merged_offset`. With the unified volume layout this
    /// always returns `Some(...)` once probing has populated `self.volumes`.
    fn volume_at(&self, merged_offset: u64) -> Option<&VolumeMeta> {
        self.volumes.as_ref().and_then(|vols| {
            vols.iter()
                .find(|v| merged_offset >= v.offset && merged_offset < v.offset + v.size)
        })
    }

    /// Retry budget for a single chunk fetch: rotate through the relevant
    /// volume's mirrors twice. `locator` is the chunk's start offset, used to
    /// pick the volume in multi-part tasks; in single-volume tasks every
    /// chunk lands in the one volume so `locator` doesn't matter.
    fn fetch_attempts(&self, locator: u64) -> usize {
        let n = self
            .volume_at(locator)
            .map(|v| v.urls.len())
            .unwrap_or_else(|| self.config.urls().len());
        n.max(1) * 2
    }

    /// Translate a merged-stream `[span_start, span_end]` plus an in-volume
    /// `locator` offset into the concrete URL and local byte range to fetch.
    /// Block-aligned cache spans may legitimately overshoot a volume boundary
    /// — clip to the containing volume so each request stays inside one URL.
    /// Returns `Ok(None)` when the clip results in an empty range (caller can
    /// treat as zero-byte success).
    ///
    /// Mirror selection: on first attempt (attempt == 0) we pick the
    /// healthiest mirror by weighted score (current throughput, error count);
    /// on retries (attempt > 0) we deterministically rotate so a bad pick on
    /// attempt 0 doesn't trap us in the same URL forever.
    fn resolve_target(
        &self,
        idx: usize,
        attempt: usize,
        locator: u64,
        span_start: u64,
        span_end: u64,
    ) -> Result<Option<FetchTarget>> {
        if let Some(vols) = self.volumes.as_ref() {
            let v = self.volume_at(locator).ok_or(ProxyError::NoUpstream)?;
            let v_end = v.offset + v.size - 1;
            let clipped_start = span_start.max(v.offset);
            let clipped_end = span_end.min(v_end);
            if clipped_end < clipped_start {
                return Ok(None);
            }
            if v.urls.is_empty() {
                return Err(ProxyError::NoUpstream);
            }
            // Per-mirror weighted picking on first attempt: prefer mirrors
            // with higher recent throughput and fewer failures. Retries
            // (attempt > 0) fall back to round-robin so we don't loop on the
            // same bad mirror.
            let pick = if attempt == 0 && v.urls.len() > 1 {
                self.pick_mirror_weighted(&v.urls, idx)
            } else {
                (idx + attempt) % v.urls.len()
            };
            let url = v.urls[pick].clone();
            let _ = vols; // borrow held only for `v`
            Ok(Some(FetchTarget {
                url,
                local_start: clipped_start - v.offset,
                local_end: clipped_end - v.offset,
                merged_start: clipped_start,
            }))
        } else {
            // No probe yet — fall back to the flat config URL list.
            let url = self.pick_url_for(idx + attempt)?;
            Ok(Some(FetchTarget {
                url,
                local_start: span_start,
                local_end: span_end,
                merged_start: span_start,
            }))
        }
    }

    /// Score-based mirror picker for the first attempt. Weight rises with
    /// recent throughput and falls with cumulative failures. Returns an
    /// index into `mirrors`.
    fn pick_mirror_weighted(&self, mirrors: &[String], idx: usize) -> usize {
        let weights: Vec<u64> = mirrors
            .iter()
            .map(|u| {
                let h = self.health.iter().find(|h| h.url.as_str() == u.as_str());
                match h {
                    Some(h) => mirror_weight(
                        h.current_speed_bps.load(Ordering::Relaxed),
                        h.failed_requests.load(Ordering::Relaxed),
                        h.successful_requests.load(Ordering::Relaxed),
                    ),
                    // Mirror has no health entry (shouldn't normally happen)
                    // — treat as neutral.
                    None => 100,
                }
            })
            .collect();
        let total: u64 = weights.iter().sum();
        if total == 0 {
            // No mirror has any signal yet — fall back to round-robin so the
            // initial probe spreads across all of them.
            return idx % mirrors.len();
        }
        // Deterministic pick: use `idx` as a pseudo-random offset modulo
        // total weight, then walk the cumulative distribution. Picking is
        // deterministic per-chunk so retries on a different attempt
        // counter visit a different mirror, but two parallel chunk
        // fetchers with neighboring `idx` will likely hit different
        // mirrors — naturally spreading load.
        let mut pick = (idx as u64) % total;
        for (i, w) in weights.iter().enumerate() {
            if pick < *w {
                return i;
            }
            pick -= *w;
        }
        mirrors.len() - 1
    }

    pub async fn probe(&self) -> Result<UpstreamProbe> {
        let layout = self.config.effective_volumes();
        if layout.is_empty() {
            return Err(ProxyError::NoUpstream);
        }
        // Single-volume tasks (the common "mirror copies of one file" case)
        // and multi-volume tasks share the same probing path: each volume
        // tries its mirrors until one answers, and we stitch the results.
        // `rr_counter` is bumped so downstream chunk fetchers don't always
        // start their rotation at the same mirror as the probe did.
        self.rr_counter.fetch_add(1, Ordering::Relaxed);
        self.probe_layout(&layout).await
    }

    /// Probe every volume in order. Each volume's mirrors are tried in turn
    /// until one returns a usable answer. For single-volume tasks this
    /// degenerates to "try each mirror until one works" — the historical
    /// mirror-mode behavior.
    async fn probe_layout(&self, layout: &[Vec<String>]) -> Result<UpstreamProbe> {
        let multi_volume = layout.len() > 1;
        let mut volumes: Vec<VolumeMeta> = Vec::with_capacity(layout.len());
        let mut content_type: Option<String> = None;
        let mut filename: Option<String> = None;
        let mut etag_first: Option<String> = None;
        let mut last_modified_first: Option<String> = None;
        let mut composite_etag_parts: Vec<String> = Vec::with_capacity(layout.len());
        let mut composite_lm_parts: Vec<String> = Vec::with_capacity(layout.len());
        // Per-volume detected filenames (from Content-Disposition or URL path),
        // collected so we can LCP-merge them at the end. Without this the
        // returned filename is just volume 0's name (e.g. "movie.part01" or
        // "t.mkv.01") — wrong for the stitched stream.
        let mut per_volume_filenames: Vec<Option<String>> = Vec::with_capacity(layout.len());
        let mut representative_urls: Vec<String> = Vec::with_capacity(layout.len());
        let mut accepts_ranges_all = true;
        let mut total_size: u64 = 0;
        let mut size_known_all = true;
        let mut offset: u64 = 0;

        for (vi, mirrors) in layout.iter().enumerate() {
            // Try mirrors in order until one yields a probe that the task's
            // mode can actually use. The ordering is the user-supplied one
            // — they decide which mirror is preferred.
            //
            // Multi-volume tasks need every chunk to come back with the
            // exact byte range we asked for, so a mirror that returns a
            // probe but doesn't support Range or doesn't report a size
            // counts as a per-mirror failure: we record it in url_health
            // (so the dashboard surfaces it as broken) and fall through to
            // the next mirror. Only when every mirror of the volume fails
            // these checks do we surface the fatal error.
            let n = mirrors.len();
            let start = self.rr_counter.load(Ordering::Relaxed);
            let mut last_err: Option<ProxyError> = None;
            let mut probe_result: Option<(String, UpstreamProbe)> = None;
            for off in 0..n {
                let url = &mirrors[(start + off) % n];
                match self.probe_one(url).await {
                    Ok(p) => {
                        if multi_volume {
                            if !p.accepts_ranges {
                                let msg = "no Range support; skipping for multi-volume task";
                                self.record_failure(url, None, msg);
                                tracing::debug!("probe vol {} mirror {} rejected: {}", vi + 1, url, msg);
                                last_err = Some(ProxyError::Internal(format!(
                                    "{} ({})", msg, url
                                )));
                                continue;
                            }
                            if p.total_size.is_none() {
                                let msg = "no Content-Length; can't stitch volumes without known sizes";
                                self.record_failure(url, None, msg);
                                tracing::debug!("probe vol {} mirror {} rejected: {}", vi + 1, url, msg);
                                last_err = Some(ProxyError::Internal(format!(
                                    "{} ({})", msg, url
                                )));
                                continue;
                            }
                        }
                        probe_result = Some((url.clone(), p));
                        break;
                    }
                    Err(e) => {
                        // Network / HTTP error reaching this mirror. probe_one
                        // already wrote to url_health for the cases it could
                        // (HEAD success / range_get response); the catch-all
                        // here covers the truly-unreachable mirror.
                        self.record_failure(url, None, &e.to_string());
                        last_err = Some(e);
                    }
                }
            }
            let (working_url, p) = match probe_result {
                Some(v) => v,
                None => {
                    let base = last_err.unwrap_or(ProxyError::NoUpstream);
                    if multi_volume {
                        // Volumes need to be stitched in known sizes; we
                        // can't recover without metadata.
                        return Err(ProxyError::Internal(format!(
                            "volume {} probe failed across all {} mirror(s): {}",
                            vi + 1,
                            n,
                            base
                        )));
                    }
                    // Single-volume / mirror task: probe failed completely
                    // (e.g. server temporarily blocked HEAD + 1-byte GET,
                    // but might still serve real GET requests). Return a
                    // bare-bones probe with no size and no ranges support,
                    // so `routes::build_stream_response` falls back to
                    // passthrough mode (one streaming GET to the first
                    // mirror). At worst the stream still 502s on the real
                    // GET, but a transient probe glitch no longer kills
                    // an otherwise-working task.
                    tracing::warn!(
                        "all mirrors of single-volume task failed probing ({}); falling back to passthrough",
                        base
                    );
                    (
                        mirrors.first().cloned().unwrap_or_default(),
                        UpstreamProbe {
                            total_size: None,
                            accepts_ranges: false,
                            content_type: None,
                            etag: None,
                            last_modified: None,
                            filename: None,
                            volumes: None,
                        },
                    )
                }
            };

            // In multi-volume mode the per-mirror loop above already rejected
            // any mirror missing Range / Content-Length, so the chosen
            // `working_url` is guaranteed acceptable. Single-volume tasks
            // tolerate a non-Range probe by falling back to passthrough; the
            // engine just records the lack so the caller can decide.
            if !p.accepts_ranges {
                accepts_ranges_all = false;
            }

            let size_opt = p.total_size;
            let size = size_opt.unwrap_or(0);
            if size_opt.is_none() {
                size_known_all = false;
            }

            if vi == 0 {
                content_type = p.content_type.clone();
                filename = p.filename.clone();
                etag_first = p.etag.clone();
                last_modified_first = p.last_modified.clone();
            }
            per_volume_filenames.push(p.filename.clone());
            representative_urls.push(working_url.clone());
            composite_etag_parts.push(p.etag.clone().unwrap_or_default());
            composite_lm_parts.push(p.last_modified.clone().unwrap_or_default());

            volumes.push(VolumeMeta {
                urls: mirrors.clone(),
                offset,
                size,
            });
            tracing::debug!(
                "probe volume {}/{}: url={} size={} accepts_ranges={} etag={:?}",
                vi + 1,
                layout.len(),
                working_url,
                size,
                p.accepts_ranges,
                p.etag,
            );
            total_size = total_size
                .checked_add(size)
                .ok_or_else(|| ProxyError::Internal("volume sizes overflow u64".into()))?;
            offset = total_size;
        }

        // Single-volume tasks keep the upstream's identity headers verbatim
        // (so ETag/If-None-Match flows continue to work). Multi-volume tasks
        // collapse per-part identity into a composite so any change in any
        // part flips the cache key.
        let etag = if multi_volume {
            let any = composite_etag_parts.iter().any(|s| !s.is_empty());
            any.then(|| format!("\"vols-{}\"", short_digest(&composite_etag_parts)))
        } else {
            etag_first
        };
        let last_modified = if multi_volume {
            let any = composite_lm_parts.iter().any(|s| !s.is_empty());
            any.then(|| last_modified_first.clone().unwrap_or_default())
                .filter(|s| !s.is_empty())
        } else {
            last_modified_first
        };

        // Multi-volume filename: merge per-volume names via longest-common-
        // prefix so we surface the stitched name (e.g. "movie") instead of
        // volume 0's (e.g. "movie.part01"). Single-volume tasks pass through.
        if multi_volume {
            filename = merge_volume_filenames(&per_volume_filenames, &representative_urls)
                .or(filename);
        }

        let total = if size_known_all { Some(total_size) } else { None };
        Ok(UpstreamProbe {
            total_size: total,
            accepts_ranges: accepts_ranges_all,
            content_type,
            etag,
            last_modified,
            filename,
            volumes: Some(volumes),
        })
    }

    async fn probe_one(&self, url: &str) -> Result<UpstreamProbe> {
        let base_headers = self.build_headers(None)?;

        // Step 1: HEAD for cheap metadata (content-type, content-length, etag, ...).
        // Some CDNs (nginx without `add_header Accept-Ranges`, Cloudflare in some
        // configs, GCS, etc.) omit `Accept-Ranges` from HEAD even when they
        // happily serve byte ranges, so HEAD alone cannot tell us whether ranges
        // are supported.
        //
        // Some upstreams reject HEAD outright (some pan/CDN endpoints only
        // serve GET). When that happens we record the URL in `head_unsupported`
        // and skip the HEAD on every subsequent probe for the lifetime of the
        // process — there's no point paying that round-trip again.
        let skip_head = self
            .head_unsupported
            .as_ref()
            .map(|s| s.read().contains(url))
            .unwrap_or(false);
        let head = if skip_head {
            tracing::trace!("probe_one skipping HEAD (known-unsupported) url={}", url);
            None
        } else {
            let head_start = Instant::now();
            let r = self
                .client
                .head(url)
                .headers(base_headers.clone())
                .send()
                .await
                .ok()
                .filter(|r| r.status().is_success());
            if let Some(resp) = &r {
                self.record_success(
                    url,
                    resp.status().as_u16(),
                    head_start.elapsed().as_millis() as u64,
                );
            } else if let Some(s) = &self.head_unsupported {
                s.write().insert(url.to_string());
                tracing::debug!(
                    "probe_one marking url as HEAD-unsupported (won't HEAD again this session): {}",
                    url
                );
            }
            r
        };

        let mut total_size: Option<u64> = None;
        let mut content_type: Option<String> = None;
        let mut etag: Option<String> = None;
        let mut last_modified: Option<String> = None;
        let mut filename: Option<String> = None;
        let mut head_accept_ranges = false;

        if let Some(resp) = &head {
            let h = resp.headers();
            head_accept_ranges = h
                .get(reqwest::header::ACCEPT_RANGES)
                .and_then(|v| v.to_str().ok())
                .map(|s| s.eq_ignore_ascii_case("bytes"))
                .unwrap_or(false);
            total_size = h
                .get(reqwest::header::CONTENT_LENGTH)
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok());
            content_type = h
                .get(reqwest::header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .map(String::from);
            etag = h
                .get(reqwest::header::ETAG)
                .and_then(|v| v.to_str().ok())
                .map(String::from);
            last_modified = h
                .get(reqwest::header::LAST_MODIFIED)
                .and_then(|v| v.to_str().ok())
                .map(String::from);
            filename = extract_filename(h);
        }

        // Step 2: actively probe range support with a 1-byte ranged GET.
        // This is the only reliable test: a server that returns 206 here truly
        // supports byte ranges (even if it didn't advertise `Accept-Ranges`).
        let mut probe_headers = base_headers.clone();
        probe_headers.insert(RANGE, HeaderValue::from_static("bytes=0-0"));
        let range_get = self.client.get(url).headers(probe_headers).send().await;

        let mut accepts_ranges = head_accept_ranges;
        if let Ok(resp) = &range_get {
            let status = resp.status();
            if status == StatusCode::PARTIAL_CONTENT {
                accepts_ranges = true;
            }
            let h = resp.headers();
            // Prefer Content-Range total when available (more authoritative than CL).
            if let Some(cr) = h.get(reqwest::header::CONTENT_RANGE) {
                if let Some(t) = cr
                    .to_str()
                    .ok()
                    .and_then(|s| s.rsplit('/').next())
                    .and_then(|s| s.trim().parse::<u64>().ok())
                {
                    total_size = Some(t);
                }
            }
            // Fill in metadata if HEAD was missing/failed.
            if content_type.is_none() {
                content_type = h
                    .get(reqwest::header::CONTENT_TYPE)
                    .and_then(|v| v.to_str().ok())
                    .map(String::from);
            }
            if etag.is_none() {
                etag = h
                    .get(reqwest::header::ETAG)
                    .and_then(|v| v.to_str().ok())
                    .map(String::from);
            }
            if last_modified.is_none() {
                last_modified = h
                    .get(reqwest::header::LAST_MODIFIED)
                    .and_then(|v| v.to_str().ok())
                    .map(String::from);
            }
            if filename.is_none() {
                filename = extract_filename(h);
            }
            if total_size.is_none() && status == StatusCode::OK {
                // Server ignored our Range and sent the whole file: CL is the total.
                total_size = h
                    .get(reqwest::header::CONTENT_LENGTH)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| s.parse::<u64>().ok());
            }
        }

        if head.is_none() && range_get.is_err() {
            return Err(ProxyError::NoUpstream);
        }

        if filename.is_none() {
            filename = filename_from_url(url);
        }

        Ok(UpstreamProbe {
            total_size,
            accepts_ranges,
            content_type,
            etag,
            last_modified,
            filename,
            volumes: None,
        })
    }

    /// Open a single full-stream proxy when ranges are not supported or unknown size.
    pub async fn open_passthrough(
        &self,
        client_range: Option<(u64, Option<u64>)>,
    ) -> Result<reqwest::Response> {
        let url = self.pick_url()?;
        let extra = client_range.map(|(s, e)| (s, e.unwrap_or(u64::MAX)));
        let headers = self.build_headers(extra)?;
        let resp = self
            .client
            .get(url)
            .headers(headers)
            .send()
            .await
            .map_err(ProxyError::Upstream)?;

        let status = resp.status();
        if !status.is_success() && status != StatusCode::PARTIAL_CONTENT {
            return Err(ProxyError::BadStatus(status.as_u16()));
        }
        Ok(resp)
    }

    /// Multi-threaded ordered streaming for a known byte range [start, end] inclusive.
    /// Returns a receiver that yields ordered Bytes chunks until the range is fully delivered.
    ///
    /// `open_ended` is true when the client's Range header had no explicit end
    /// (i.e. `Range: X-`), or when there was no Range header at all. We use it
    /// for two seek-related optimizations:
    ///
    /// 1. **Adaptive first chunks** — the leading few chunks are intentionally
    ///    small (`HEAD_SMALL_SPLIT`) so that a player which only fetches a
    ///    metadata-sized prefix before seeking has very little in-flight work
    ///    to tear down. After the head zone we ramp up to the full `max_split`.
    /// 2. **Hard cancellation** — when the client drops the connection the
    ///    serializer aborts every still-running fetcher task instead of
    ///    waiting for them to finish their current HTTP request. Without this
    ///    a seek would compete with up to `max_threads × max_split` bytes of
    ///    legacy traffic on the way down.
    pub fn stream_range(
        self: Arc<Self>,
        start: u64,
        end: u64,
        open_ended: bool,
    ) -> mpsc::Receiver<Result<Bytes>> {
        let split = self.config.max_split.max(64 * 1024);
        let max_threads = self.config.max_threads.max(1);
        let max_per_volume = self.config.max_per_volume.max(1);

        // Build chunk plan. In volume mode every chunk must live inside one
        // volume so the fetcher only ever talks to a single URL — split first
        // along volume boundaries, then along `max_split`. When the request
        // is open-ended (player will likely seek soon), peel a head zone of
        // smaller chunks off the plan.
        let head_split: Option<(u64, usize)> = if open_ended && split > HEAD_SMALL_SPLIT {
            Some((HEAD_SMALL_SPLIT, HEAD_SMALL_COUNT))
        } else {
            None
        };
        let plan = plan_chunks_with_head(
            start,
            end,
            split,
            self.volumes.as_deref().map(|v| v.as_slice()),
            head_split,
        );
        let total_chunks = plan.len();
        let vol_count = self
            .volumes
            .as_deref()
            .map(|v| v.len())
            .unwrap_or(1)
            .max(1);
        let vol_of: Vec<usize> = plan_volume_indices(
            &plan,
            self.volumes.as_deref().map(|v| v.as_slice()),
        );

        tracing::debug!(
            "stream_range [{}, {}] split={} max_threads={} max_per_volume={} vol_count={} chunks={} open_ended={} head_split={:?}",
            start,
            end,
            split,
            max_threads,
            max_per_volume,
            vol_count,
            total_chunks,
            open_ended,
            head_split,
        );

        // Per-chunk channels so fetchers can run concurrently while the
        // serializer stitches bytes back together in plan order. Buffer of 4
        // is enough to absorb network jitter without holding many MB of
        // already-fetched bytes when the client is slower than the upstream.
        let mut senders: Vec<Option<mpsc::Sender<Result<Bytes>>>> =
            Vec::with_capacity(total_chunks);
        let mut receivers: Vec<mpsc::Receiver<Result<Bytes>>> =
            Vec::with_capacity(total_chunks);
        for _ in 0..total_chunks {
            let (tx, rx) = mpsc::channel::<Result<Bytes>>(4);
            senders.push(Some(tx));
            receivers.push(rx);
        }

        // Output channel to caller
        let (out_tx, out_rx) = mpsc::channel::<Result<Bytes>>(8);

        // The scheduler and serializer cooperate via a release-permit channel.
        // Whenever a fetcher exits (success, error, abort) it sends its
        // volume index here so the scheduler can decrement that volume's
        // in-flight count and try to spawn the next eligible chunk.
        let (release_tx, mut release_rx) = mpsc::unbounded_channel::<usize>();

        let engine_arc = Arc::clone(&self);
        tokio::spawn(async move {
            // Live handles for every fetcher we've spawned. On client
            // disconnect we walk this list and `.abort()` each one so
            // tokio cancels their HTTP requests immediately — that's the
            // bandwidth claw-back that makes seeking responsive.
            let mut handles: Vec<JoinHandle<()>> = Vec::with_capacity(total_chunks);
            // Per-volume in-flight fetchers. Enforced together with
            // `total_in_flight <= max_threads` to honor both the task-wide
            // concurrency budget and the upstream's per-URL connection limit.
            let mut per_vol: Vec<usize> = vec![0; vol_count];
            let mut total_in_flight: usize = 0;
            // True when the chunk's fetcher has been spawned (or skipped as
            // a no-op). We can't rely on `senders[idx].is_none()` alone
            // because the warm-up path also takes senders.
            let mut spawned: Vec<bool> = vec![false; total_chunks];

            // Spawn one fetcher for chunk `idx`. Caller is responsible for
            // having taken the sender out of `senders`.
            let spawn_fetch = |idx: usize,
                               tx: mpsc::Sender<Result<Bytes>>,
                               release: mpsc::UnboundedSender<usize>,
                               vi: usize,
                               plan_arc: Arc<Vec<(u64, u64)>>|
             -> JoinHandle<()> {
                let engine = Arc::clone(&engine_arc);
                let (cs, ce) = plan_arc[idx];
                tokio::spawn(async move {
                    let result = engine.fetch_chunk_to(idx, cs, ce, &tx).await;
                    if let Err(e) = result {
                        let _ = tx.send(Err(e)).await;
                    }
                    // `tx` dropped here — receiver for chunk `idx` closes.
                    let _ = release.send(vi);
                })
            };

            let plan_arc = Arc::new(plan);

            // Helper: spawn fetchers until either every chunk is spawned or
            // `total_in_flight` hits `max_threads`. Two-pass policy:
            //
            //   Pass 1 (strict): honor `max_per_volume` — pick the
            //     lowest-indexed not-yet-spawned chunk whose volume still has
            //     room. This spreads work across volumes so no single upstream
            //     gets piled on.
            //
            //   Pass 2 (overflow): if pass 1 ran out of cap-respecting chunks
            //     but there's still budget, allow `per_vol` to exceed
            //     `max_per_volume`. This keeps idle threads from sitting on
            //     their hands when every remaining chunk lives in a volume
            //     that's already at its per-volume cap — typical when the
            //     client's Range only touches one volume, or when one
            //     volume's chunks finish faster than its siblings'.
            //
            // The per-volume cap thus behaves as a *soft* limit: we prefer to
            // spread across volumes when we can, but we won't waste task-wide
            // concurrency budget when we can't.
            let try_spawn_more = |handles: &mut Vec<JoinHandle<()>>,
                                  per_vol: &mut Vec<usize>,
                                  total_in_flight: &mut usize,
                                  spawned: &mut Vec<bool>,
                                  senders: &mut Vec<Option<mpsc::Sender<Result<Bytes>>>>|
             -> usize {
                let mut spawned_count = 0;
                while *total_in_flight < max_threads {
                    // Strict pass first.
                    let mut pick: Option<usize> = None;
                    for idx in 0..total_chunks {
                        if spawned[idx] {
                            continue;
                        }
                        let vi = vol_of.get(idx).copied().unwrap_or(0);
                        if per_vol[vi] >= max_per_volume {
                            continue;
                        }
                        pick = Some(idx);
                        break;
                    }
                    // Overflow pass: any not-yet-spawned chunk will do.
                    if pick.is_none() {
                        for idx in 0..total_chunks {
                            if spawned[idx] {
                                continue;
                            }
                            pick = Some(idx);
                            break;
                        }
                    }
                    let idx = match pick {
                        Some(i) => i,
                        None => break,
                    };
                    let vi = vol_of.get(idx).copied().unwrap_or(0);
                    let tx = match senders[idx].take() {
                        Some(t) => t,
                        None => {
                            spawned[idx] = true;
                            continue;
                        }
                    };
                    per_vol[vi] += 1;
                    *total_in_flight += 1;
                    spawned[idx] = true;
                    handles.push(spawn_fetch(
                        idx,
                        tx,
                        release_tx.clone(),
                        vi,
                        Arc::clone(&plan_arc),
                    ));
                    spawned_count += 1;
                }
                spawned_count
            };

            // Initial fill: respects both caps. Multi-volume tasks naturally
            // get cross-volume coverage because once a volume hits its cap
            // the loop falls through to the next volume's first chunk.
            let initial = try_spawn_more(
                &mut handles,
                &mut per_vol,
                &mut total_in_flight,
                &mut spawned,
                &mut senders,
            );

            // Cross-volume warm-up: ensure connections are open against the
            // first uncovered volumes too. With per-volume caps the initial
            // fill already spreads across volumes when chunks-per-volume is
            // dense, but for sparse plans (e.g. small Range over many
            // volumes) we still want to pre-open the next 1-2 volumes so
            // crossing a boundary doesn't pay TCP setup latency. For
            // open-ended requests (likely about to seek) we cap to 1 to
            // avoid wasting bandwidth that will be aborted shortly.
            let warmup_next_vols: usize = if open_ended { 1 } else { vol_count };
            if vol_count > 1 && warmup_next_vols > 0 {
                let mut covered: Vec<bool> = vec![false; vol_count];
                for (idx, &vi) in vol_of.iter().enumerate() {
                    if spawned[idx] && vi < covered.len() {
                        covered[vi] = true;
                    }
                }
                let mut warmed: Vec<usize> = Vec::new();
                let mut budget = warmup_next_vols;
                for vi in 0..vol_count {
                    if budget == 0 {
                        break;
                    }
                    if covered[vi] {
                        continue;
                    }
                    if let Some(idx) = vol_of.iter().position(|&v| v == vi) {
                        if spawned[idx] {
                            continue;
                        }
                        if let Some(tx) = senders[idx].take() {
                            spawned[idx] = true;
                            per_vol[vi] += 1;
                            total_in_flight += 1;
                            handles.push(spawn_fetch(
                                idx,
                                tx,
                                release_tx.clone(),
                                vi,
                                Arc::clone(&plan_arc),
                            ));
                            warmed.push(idx);
                            budget -= 1;
                        }
                    }
                }
                if !warmed.is_empty() {
                    tracing::debug!(
                        "warmup spawned {} extra fetcher(s) for uncovered volumes: idxs={:?}",
                        warmed.len(),
                        warmed,
                    );
                }
            }

            tracing::debug!(
                "initial spawn done: in_flight={}, per_vol_nonzero={:?}",
                total_in_flight,
                per_vol
                    .iter()
                    .enumerate()
                    .filter(|&(_, &c)| c > 0)
                    .map(|(i, &c)| (i, c))
                    .collect::<Vec<_>>(),
            );
            let _ = initial;

            // Main loop: serializer drains receivers in plan order while
            // simultaneously listening for fetcher completion events to
            // refill the in-flight window.
            for (i, mut rx) in receivers.into_iter().enumerate() {
                loop {
                    tokio::select! {
                        // Prefer draining the current chunk: bias = client
                        // throughput. The release-event branch will get its
                        // turn whenever there's no new data to forward.
                        biased;
                        item = rx.recv() => {
                            match item {
                                Some(item) => {
                                    let is_err = item.is_err();
                                    if out_tx.send(item).await.is_err() {
                                        tracing::debug!(
                                            "client gone at chunk {}, aborting {} in-flight fetcher(s)",
                                            i,
                                            handles.len(),
                                        );
                                        for h in &handles {
                                            h.abort();
                                        }
                                        return;
                                    }
                                    if is_err {
                                        tracing::debug!("chunk {} produced an error, ending stream", i);
                                        for h in &handles {
                                            h.abort();
                                        }
                                        return;
                                    }
                                }
                                None => {
                                    // Chunk i fully delivered. Move to next.
                                    break;
                                }
                            }
                        }
                        Some(vi) = release_rx.recv() => {
                            if vi < per_vol.len() && per_vol[vi] > 0 {
                                per_vol[vi] -= 1;
                            }
                            if total_in_flight > 0 {
                                total_in_flight -= 1;
                            }
                            try_spawn_more(
                                &mut handles,
                                &mut per_vol,
                                &mut total_in_flight,
                                &mut spawned,
                                &mut senders,
                            );
                        }
                    }
                }
                let _ = i;
            }
        });

        out_rx
    }

    async fn fetch_chunk_to(
        &self,
        idx: usize,
        start: u64,
        end: u64,
        tx: &mpsc::Sender<Result<Bytes>>,
    ) -> Result<()> {
        if let Some(cache) = self.cache.clone() {
            return self.fetch_chunk_cached(cache, idx, start, end, tx).await;
        }
        self.fetch_chunk_origin(idx, start, end, tx).await
    }

    async fn fetch_chunk_cached(
        &self,
        cache: Arc<CacheEntry>,
        idx: usize,
        start: u64,
        end: u64,
        tx: &mpsc::Sender<Result<Bytes>>,
    ) -> Result<()> {
        let bs = cache.meta.block_size;
        let total = cache.meta.total_size;
        if total == 0 || end >= total {
            // Outside the cached file's bounds — fall back to origin.
            return self.fetch_chunk_origin(idx, start, end, tx).await;
        }

        let first_block = start / bs;
        let last_block = end / bs;

        let mut i = first_block;
        while i <= last_block {
            let hit = cache.has_block(i);
            let mut j = i;
            while j + 1 <= last_block && cache.has_block(j + 1) == hit {
                j += 1;
            }
            let span_start = (i * bs).max(start);
            let span_end = (((j + 1) * bs - 1).min(total - 1)).min(end);

            if hit {
                tracing::trace!(
                    "cache HIT chunk={} blocks=[{}..={}]",
                    idx, i, j,
                );
                match cache.read_range(span_start, span_end) {
                    Ok(bytes) => {
                        cache.hits.fetch_add(j - i + 1, Ordering::Relaxed);
                        // Cache stores raw upstream bytes — for an encrypted
                        // task that's ciphertext, so the plugin pipeline
                        // still needs to run on the read-out.
                        let out = self.transform_outgoing(span_start, bytes);
                        if tx.send(Ok(out)).await.is_err() {
                            return Ok(());
                        }
                    }
                    Err(e) => {
                        // Disk read failed — fall back to origin for this span,
                        // but don't try to cache the result (the underlying
                        // file may be broken).
                        tracing::warn!(
                            "cache read failed chunk={} blocks=[{}..={}]: {}; falling back to origin",
                            idx, i, j, e,
                        );
                        let from = i * bs;
                        let to = ((j + 1) * bs - 1).min(total - 1);
                        self.fetch_span_to_tx(idx, from, to, start, end, None, tx)
                            .await?;
                    }
                }
            } else {
                tracing::trace!(
                    "cache MISS chunk={} blocks=[{}..={}]",
                    idx, i, j,
                );
                cache.misses.fetch_add(j - i + 1, Ordering::Relaxed);
                // Fetch the BLOCK-aligned span from origin so writes land at
                // the right offsets and complete whole blocks in the bitmap.
                let from = i * bs;
                let to = ((j + 1) * bs - 1).min(total - 1);
                self.fetch_span_to_tx(idx, from, to, start, end, Some(&cache), tx)
                    .await?;
            }
            i = j + 1;
        }
        Ok(())
    }

    /// Fetch [span_start, span_end] from any configured origin and:
    ///   * forward the slice within [chunk_start, chunk_end] to `tx`, and
    ///   * if `cache` is Some, persist the full span to the sparse file as it
    ///     streams in (so partial reads still warm the cache).
    ///
    /// In multi-volume mode the span may straddle one or more volume
    /// boundaries (block-aligned cache spans are independent of volume
    /// boundaries). This function splits the span by volumes and fetches each
    /// sub-span from its own volume — that's the only way the cache's
    /// per-block byte counter can ever reach the full block length for blocks
    /// that span a boundary.
    async fn fetch_span_to_tx(
        &self,
        idx: usize,
        span_start: u64,
        span_end: u64,
        chunk_start: u64,
        chunk_end: u64,
        cache: Option<&CacheEntry>,
        tx: &mpsc::Sender<Result<Bytes>>,
    ) -> Result<()> {
        let subs = slice_span_by_volumes(self.volumes.as_deref().map(|v| v.as_slice()), span_start, span_end);
        for (sub_start, sub_end) in subs {
            self.fetch_single_volume_subspan(
                idx, sub_start, sub_end, chunk_start, chunk_end, cache, tx,
            )
            .await?;
        }
        Ok(())
    }

    /// Fetch [sub_start, sub_end] from a single volume (the one containing
    /// `sub_start`), retrying on different mirrors as needed. Mid-stream
    /// disconnects are recovered by resuming the next attempt at the byte
    /// where the previous attempt failed — so a 90%-complete fetch that
    /// loses TCP doesn't restart from byte 0.
    async fn fetch_single_volume_subspan(
        &self,
        idx: usize,
        sub_start: u64,
        sub_end: u64,
        chunk_start: u64,
        chunk_end: u64,
        cache: Option<&CacheEntry>,
        tx: &mpsc::Sender<Result<Bytes>>,
    ) -> Result<()> {
        let attempts = self.fetch_attempts(sub_start);
        let mut last_err: Option<ProxyError> = None;
        // Bytes already delivered for this sub-span (relative to sub_start).
        // On mid-stream failure we resume the next attempt from this offset
        // instead of restarting at sub_start.
        let mut delivered: u64 = 0;
        for attempt in 0..attempts {
            let resume_at = sub_start + delivered;
            if resume_at > sub_end {
                return Ok(());
            }
            let target = match self.resolve_target(idx, attempt, sub_start, resume_at, sub_end)? {
                Some(t) => t,
                // Clipped to zero bytes (sub-span entirely outside containing
                // volume — shouldn't happen given slice_span_by_volumes).
                None => return Ok(()),
            };
            let headers = self.build_headers(Some((target.local_start, target.local_end)))?;
            tracing::trace!(
                "fetch span chunk={} attempt={} url={} range={}-{} merged_start={} resume={}",
                idx, attempt, target.url, target.local_start, target.local_end, target.merged_start, delivered,
            );
            let req_start = Instant::now();
            let _in_flight = self.in_flight_guard(&target.url);
            let resp = match self.client.get(&target.url).headers(headers).send().await {
                Ok(r) => r,
                Err(e) => {
                    let msg = e.to_string();
                    self.record_failure(&target.url, None, &msg);
                    tracing::debug!(
                        "fetch span chunk={} attempt={} url={} network error: {}",
                        idx, attempt, target.url, msg,
                    );
                    last_err = Some(ProxyError::Upstream(e));
                    continue;
                }
            };
            let latency_ms = req_start.elapsed().as_millis() as u64;
            let status_code = resp.status().as_u16();
            let status = resp.status();
            if status != StatusCode::PARTIAL_CONTENT {
                // We asked for a range; we expect 206. A 200 means the origin
                // sent the full file, which we can't safely write into a
                // block-aligned cache (offsets would be off). Retry on next
                // URL; if none oblige, surface the error.
                self.record_failure(
                    &target.url,
                    Some(status_code),
                    &format!("expected 206, got {status_code}"),
                );
                tracing::debug!(
                    "fetch span chunk={} attempt={} url={} expected 206, got {}",
                    idx, attempt, target.url, status_code,
                );
                last_err = Some(ProxyError::BadStatus(status.as_u16()));
                continue;
            }
            self.record_success(&target.url, status_code, latency_ms);

            // Cursor tracks merged-space offsets so cache writes and the
            // [chunk_start, chunk_end] slice are always correct, even when
            // the request was issued in a volume's local coordinates.
            let mut cursor = target.merged_start;
            let mut stream = resp.bytes_stream();
            let mut had_err = false;
            while let Some(item) = stream.next().await {
                match item {
                    Ok(b) => {
                        if b.is_empty() {
                            continue;
                        }
                        let piece_start = cursor;
                        let piece_end = piece_start + b.len() as u64 - 1;
                        cursor = piece_end + 1;
                        delivered += b.len() as u64;

                        if let Some(c) = cache {
                            if let Err(e) = c.write_range(piece_start, &b) {
                                tracing::warn!(
                                    "cache write at offset {} failed: {}",
                                    piece_start,
                                    e
                                );
                            }
                        }

                        self.record_bytes(&target.url, b.len() as u64);

                        if piece_end < chunk_start || piece_start > chunk_end {
                            continue;
                        }
                        let lo = chunk_start.saturating_sub(piece_start) as usize;
                        let hi = ((chunk_end + 1)
                            .saturating_sub(piece_start)
                            .min(b.len() as u64)) as usize;
                        if hi <= lo {
                            continue;
                        }
                        let to_send = b.slice(lo..hi);
                        // Merged offset of the first byte of `to_send`: the
                        // slice's start within `b` (lo) plus where `b`
                        // started in merged space (piece_start). Cache writes
                        // above stored the original ciphertext bytes, so
                        // running the transform here only affects what
                        // reaches the client.
                        let merged_offset = piece_start + lo as u64;
                        let to_send = self.transform_outgoing(merged_offset, to_send);
                        if tx.send(Ok(to_send)).await.is_err() {
                            return Ok(());
                        }
                    }
                    Err(e) => {
                        let msg = e.to_string();
                        self.record_failure(&target.url, Some(status_code), &msg);
                        tracing::debug!(
                            "fetch span chunk={} attempt={} url={} mid-stream error after {} bytes; will resume from offset {}",
                            idx, attempt, target.url, delivered, sub_start + delivered,
                        );
                        last_err = Some(ProxyError::Upstream(e));
                        had_err = true;
                        break;
                    }
                }
            }
            if !had_err {
                return Ok(());
            }
        }
        Err(last_err.unwrap_or(ProxyError::NoUpstream))
    }

    async fn fetch_chunk_origin(
        &self,
        idx: usize,
        start: u64,
        end: u64,
        tx: &mpsc::Sender<Result<Bytes>>,
    ) -> Result<()> {
        let mut last_err: Option<ProxyError> = None;
        let attempts = self.fetch_attempts(start);
        // Bytes already delivered (relative to `start`). Used to resume
        // mid-stream after a TCP break instead of restarting at byte 0.
        let mut delivered: u64 = 0;
        for attempt in 0..attempts {
            // Chunk planner guarantees [start, end] lives in one volume in
            // volume mode, so `start` pins the URL and `[start, end]` maps
            // cleanly to its local range. On retry we resume from where the
            // previous attempt left off.
            let resume_at = start + delivered;
            if resume_at > end {
                return Ok(());
            }
            let target = match self.resolve_target(idx, attempt, start, resume_at, end)? {
                Some(t) => t,
                None => return Ok(()),
            };
            let headers = self.build_headers(Some((target.local_start, target.local_end)))?;
            tracing::trace!(
                "fetch chunk={} attempt={} url={} range={}-{} resume={}",
                idx, attempt, target.url, target.local_start, target.local_end, delivered,
            );
            let req_start = Instant::now();
            let _in_flight = self.in_flight_guard(&target.url);
            let resp = match self.client.get(&target.url).headers(headers).send().await {
                Ok(r) => r,
                Err(e) => {
                    let msg = e.to_string();
                    self.record_failure(&target.url, None, &msg);
                    tracing::debug!(
                        "fetch chunk={} attempt={} url={} network error: {}",
                        idx, attempt, target.url, msg,
                    );
                    last_err = Some(ProxyError::Upstream(e));
                    continue;
                }
            };
            let latency_ms = req_start.elapsed().as_millis() as u64;
            let status_code = resp.status().as_u16();
            let status = resp.status();
            if !status.is_success() && status != StatusCode::PARTIAL_CONTENT {
                self.record_failure(
                    &target.url,
                    Some(status_code),
                    &format!("status {status_code}"),
                );
                tracing::debug!(
                    "fetch chunk={} attempt={} url={} bad status {}",
                    idx, attempt, target.url, status_code,
                );
                last_err = Some(ProxyError::BadStatus(status.as_u16()));
                continue;
            }
            // Multi-volume tasks require Range support per the probe
            // contract; a 200 here means the origin lied. Treat as failure
            // and retry. Single-volume tasks tolerate it (slice locally).
            if status == StatusCode::OK && self.is_multi_volume() {
                self.record_failure(
                    &target.url,
                    Some(status_code),
                    "volume returned 200 OK to a ranged request",
                );
                tracing::debug!(
                    "fetch chunk={} attempt={} url={} multi-volume task got 200 (expected 206), retrying",
                    idx, attempt, target.url,
                );
                last_err = Some(ProxyError::BadStatus(status.as_u16()));
                continue;
            }
            self.record_success(&target.url, status_code, latency_ms);

            // If server ignored Range and returned 200 OK, we got the whole
            // file. Slice the [local_start, local_end] window out as we read,
            // so the serializer still gets exactly the bytes for this chunk.
            // (Mirror mode only — guarded above for volumes.)
            //
            // Resume note: when the previous attempt delivered N bytes the
            // 206 path requested local_start += N (via resume_at above) and
            // emits everything; the 200 path however always streams the
            // whole file from byte 0, so we still slice [local_start..local_end]
            // out of it. `delivered` here only matters for the 206 path.
            let needs_slice = status == StatusCode::OK;
            let local_start = target.local_start;
            let local_end = target.local_end;
            let chunk_len = local_end - local_start + 1;
            let mut cursor: u64 = 0;
            let mut emitted: u64 = 0;

            let mut stream = resp.bytes_stream();
            let mut had_err = false;
            while let Some(item) = stream.next().await {
                match item {
                    Ok(b) => {
                        let to_send = if needs_slice {
                            let b_start = cursor;
                            let b_end = cursor + b.len() as u64;
                            cursor = b_end;
                            if b_end <= local_start || b_start > local_end {
                                if b_start > local_end {
                                    break;
                                }
                                continue;
                            }
                            let lo = local_start.saturating_sub(b_start) as usize;
                            let hi = (local_end + 1 - b_start).min(b.len() as u64) as usize;
                            b.slice(lo..hi)
                        } else {
                            b
                        };
                        if to_send.is_empty() {
                            continue;
                        }
                        let sent_len = to_send.len() as u64;
                        self.record_bytes(&target.url, sent_len);
                        // Both 206 and 200 paths emit bytes in plan order
                        // starting at `target.merged_start`. `emitted` is
                        // the cumulative byte count BEFORE this slice, so
                        // it doubles as the merged offset for the next emit.
                        let merged_offset = target.merged_start + emitted;
                        emitted += sent_len;
                        delivered += sent_len;
                        let to_send = self.transform_outgoing(merged_offset, to_send);
                        if tx.send(Ok(to_send)).await.is_err() {
                            return Ok(());
                        }
                        if needs_slice && emitted >= chunk_len {
                            break;
                        }
                    }
                    Err(e) => {
                        let msg = e.to_string();
                        self.record_failure(&target.url, Some(status_code), &msg);
                        tracing::debug!(
                            "fetch chunk={} attempt={} url={} mid-stream error after {} bytes; will resume from offset {}",
                            idx, attempt, target.url, delivered, start + delivered,
                        );
                        last_err = Some(ProxyError::Upstream(e));
                        had_err = true;
                        break;
                    }
                }
            }
            if !had_err {
                tracing::trace!(
                    "chunk done idx={} emitted={} attempt={}",
                    idx, emitted, attempt,
                );
                return Ok(());
            }
        }
        Err(last_err.unwrap_or(ProxyError::NoUpstream))
    }
}

/// RAII counter for per-URL in-flight HTTP requests. Held by the engine
/// across one fetch attempt; decrements on drop, so cancellation paths
/// (client disconnect → task abort) don't leak the counter.
struct InFlightGuard(Arc<crate::models::UrlHealthAcc>);

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.0.in_flight_requests.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Parse a single HTTP Range header (e.g. "bytes=100-200" or "bytes=100-")
/// Returns (start, optional end-inclusive).
pub fn parse_range_header(value: &str, total: Option<u64>) -> Result<(u64, Option<u64>)> {
    let value = value.trim();
    let rest = value
        .strip_prefix("bytes=")
        .ok_or_else(|| ProxyError::InvalidRange(value.into()))?;
    // Only the first range is honored.
    let first = rest.split(',').next().unwrap_or("").trim();
    if let Some(suffix) = first.strip_prefix('-') {
        let n: u64 = suffix
            .trim()
            .parse()
            .map_err(|_| ProxyError::InvalidRange(value.into()))?;
        let total = total.ok_or_else(|| ProxyError::InvalidRange("suffix range without total".into()))?;
        if n == 0 {
            return Err(ProxyError::InvalidRange(value.into()));
        }
        let start = total.saturating_sub(n);
        let end = total.saturating_sub(1);
        return Ok((start, Some(end)));
    }
    let mut parts = first.splitn(2, '-');
    let s = parts
        .next()
        .ok_or_else(|| ProxyError::InvalidRange(value.into()))?
        .trim();
    let e = parts.next().unwrap_or("").trim();
    let start: u64 = s
        .parse()
        .map_err(|_| ProxyError::InvalidRange(value.into()))?;
    if e.is_empty() {
        Ok((start, None))
    } else {
        let end: u64 = e
            .parse()
            .map_err(|_| ProxyError::InvalidRange(value.into()))?;
        if end < start {
            return Err(ProxyError::InvalidRange(value.into()));
        }
        Ok((start, Some(end)))
    }
}

/// Extract a clean filename from a response's Content-Disposition header.
///
/// Handles both RFC 6266 `filename="..."` and RFC 5987 `filename*=UTF-8''...`
/// forms. Falls back through a few common encoding mistakes (servers that
/// stuff UTF-8 bytes into a header technically scoped to ISO-8859-1, etc.).
fn extract_filename(headers: &reqwest::header::HeaderMap) -> Option<String> {
    let v = headers.get(reqwest::header::CONTENT_DISPOSITION)?;
    let bytes = v.as_bytes();
    // `to_str()` rejects non-ASCII even though many real servers send UTF-8
    // bytes in this header. Take the raw bytes and try UTF-8, fall back to
    // latin-1 (byte == codepoint) so we never lose the value.
    let raw = match std::str::from_utf8(bytes) {
        Ok(s) => s.to_string(),
        Err(_) => bytes.iter().map(|&b| b as char).collect(),
    };
    parse_content_disposition_filename(&raw)
}

fn parse_content_disposition_filename(value: &str) -> Option<String> {
    let mut filename_star: Option<String> = None;
    let mut filename_plain: Option<String> = None;

    for part in value.split(';') {
        let part = part.trim();
        if let Some(rest) = part.strip_prefix("filename*=") {
            let rest = rest.trim().trim_matches('"');
            let (charset, encoded) = match rest.split_once("''") {
                Some((cl, enc)) => (
                    cl.split('\'').next().unwrap_or("").to_ascii_uppercase(),
                    enc,
                ),
                None => (String::new(), rest),
            };
            let decoded = percent_decode(encoded);
            let s = if charset == "ISO-8859-1" {
                decoded.iter().map(|&b| b as char).collect::<String>()
            } else {
                String::from_utf8(decoded.clone())
                    .unwrap_or_else(|_| decoded.iter().map(|&b| b as char).collect())
            };
            if !s.is_empty() {
                filename_star = Some(s);
            }
        } else if let Some(rest) = part.strip_prefix("filename=") {
            let v = rest.trim().trim_matches('"');
            if v.is_empty() {
                continue;
            }
            // Some servers send UTF-8 bytes that arrived in this string via a
            // latin-1 round-trip; try to recover by treating chars as bytes.
            let latin1_bytes: Vec<u8> = v.chars().map(|c| c as u8).collect();
            let recovered = String::from_utf8(latin1_bytes).ok();
            filename_plain = recovered.or_else(|| Some(v.to_string()));
        }
    }

    filename_star
        .or(filename_plain)
        .map(sanitize_filename)
        .filter(|s| !s.is_empty())
}

/// Derive a filename from the URL's last path segment as a final fallback.
fn filename_from_url(url: &str) -> Option<String> {
    let path = url.split('?').next()?.split('#').next()?;
    let last = path.rsplit('/').next()?;
    if last.is_empty() {
        return None;
    }
    let decoded = percent_decode(last);
    let s = String::from_utf8(decoded).ok()?;
    let s = sanitize_filename(s);
    if s.is_empty() { None } else { Some(s) }
}

/// Merge per-volume detected filenames into a single name representing the
/// stitched file. Prefers names from Content-Disposition / URL probes when
/// available (more accurate than re-parsing URLs); falls back to deriving
/// from the representative URLs when too few volumes returned a name.
///
/// Used by `probe_layout` so multi-volume tasks advertise e.g. `movie.mkv`
/// instead of volume 0's `movie.mkv.01`.
fn merge_volume_filenames(
    per_vol: &[Option<String>],
    representative_urls: &[String],
) -> Option<String> {
    // Count how many volumes detected a name. With ≥2 we can do LCP on the
    // detected names directly; otherwise fall back to URL-derived names.
    let detected: Vec<String> = per_vol.iter().filter_map(|s| s.clone()).collect();
    if detected.len() >= 2 {
        if detected.len() == 1 {
            return Some(detected.into_iter().next().unwrap());
        }
        let prefix = longest_common_prefix(&detected);
        let trimmed = trim_filename_tail(&prefix);
        if !trimmed.is_empty() {
            return Some(trimmed);
        }
        // LCP trimmed to nothing — the detected names diverge too much; fall
        // through to URL-based suggestion.
    }
    suggest_volume_filename(representative_urls)
}

/// Public entry point: given a volume task's URL list, derive a sensible
/// merged filename. Strategy:
///   1. Extract each volume's filename (last path segment, percent-decoded).
///   2. Take the longest common *byte* prefix.
///   3. Trim trailing junk (dots, separators, partial " part" / " 卷" words).
///   4. If trimming wipes everything, fall back to the first volume's name.
pub fn suggest_volume_filename(urls: &[String]) -> Option<String> {
    let names: Vec<String> = urls.iter().filter_map(|u| filename_from_url(u)).collect();
    if names.is_empty() {
        return None;
    }
    if names.len() == 1 {
        return Some(names.into_iter().next().unwrap());
    }
    let prefix = longest_common_prefix(&names);
    let trimmed = trim_filename_tail(&prefix);
    if trimmed.is_empty() {
        // Common prefix was all junk — better to surface the first filename
        // than nothing.
        names.into_iter().next()
    } else {
        Some(trimmed)
    }
}

/// Longest shared leading substring of a slice of strings, sliced on UTF-8
/// boundaries so the result is always a valid string.
fn longest_common_prefix(strings: &[String]) -> String {
    if strings.is_empty() {
        return String::new();
    }
    let mut prefix = strings[0].as_str();
    for s in &strings[1..] {
        let mut end = 0;
        for (a, b) in prefix.char_indices().zip(s.chars()) {
            if a.1 != b {
                break;
            }
            // `a.0 + a.1.len_utf8()` is the byte index just past this char.
            end = a.0 + a.1.len_utf8();
        }
        prefix = &prefix[..end];
        if prefix.is_empty() {
            break;
        }
    }
    prefix.to_string()
}

/// Strip trailing characters that are almost certainly part of the per-volume
/// suffix that the LCP cut off mid-word: dots, dashes, spaces, partial digit
/// runs, and the trailing word "part" / "vol" / "卷" if it's clearly dangling.
fn trim_filename_tail(s: &str) -> String {
    let mut out = s.trim_end_matches(|c: char| {
        c.is_ascii_digit()
            || c == '.'
            || c == '-'
            || c == '_'
            || c == ' '
            || c == '('
            || c == '['
    });
    // Strip a trailing "part" / "vol" / "卷" / "第" word so the suggestion
    // doesn't end in "movie.part" or "movie 第".
    for token in ["part", "Part", "PART", "vol", "Vol", "VOL", "卷", "第"] {
        if let Some(stripped) = out.strip_suffix(token) {
            out = stripped;
            break;
        }
    }
    out.trim_end_matches(|c: char| {
        c == '.' || c == '-' || c == '_' || c == ' ' || c == '(' || c == '['
    })
    .to_string()
}

fn sanitize_filename(s: String) -> String {
    let cleaned: String = s
        .chars()
        .filter(|c| !c.is_control() && *c != '/' && *c != '\\')
        .collect();
    cleaned.trim().to_string()
}

fn percent_decode(s: &str) -> Vec<u8> {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 3 <= bytes.len() {
            if let Ok(hex) = std::str::from_utf8(&bytes[i + 1..i + 3]) {
                if let Ok(b) = u8::from_str_radix(hex, 16) {
                    out.push(b);
                    i += 3;
                    continue;
                }
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    out
}

/// Short hex digest used to fold a list of per-volume identity strings into
/// a single composite ETag-like value (so the cache invalidates if any part
/// changes underneath us).
fn short_digest(parts: &[String]) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    for p in parts {
        h.update(p.as_bytes());
        h.update(b"\n");
    }
    hex::encode(&h.finalize()[..8])
}

/// Map each chunk in `plan` to the volume that contains it. In mirror mode
/// (`volumes == None`) every chunk maps to bucket 0. In volume mode, since
/// `plan_chunks` clips each chunk to live entirely inside one volume, the
/// chunk's start offset uniquely identifies its volume.
fn plan_volume_indices(plan: &[(u64, u64)], volumes: Option<&[VolumeMeta]>) -> Vec<usize> {
    match volumes {
        None => vec![0; plan.len()],
        Some(vols) => plan
            .iter()
            .map(|(cs, _)| {
                vols.iter()
                    .position(|v| v.size > 0 && *cs >= v.offset && *cs < v.offset + v.size)
                    .unwrap_or(0)
            })
            .collect(),
    }
}

/// Score function for picking a mirror. Higher = more likely to be chosen.
///
/// * Mirrors with zero signal (never tried) get a neutral baseline so they
///   actually get sampled — otherwise a brand new mirror added to a task
///   would never receive traffic.
/// * Recent throughput (KiB/s) is the primary positive signal.
/// * Cumulative failures sharply discount the weight (mirrors that keep
///   erroring should drop out, but a single failure shouldn't black-hole
///   a permanently-good source).
/// * Successful requests with no recorded speed (e.g. completed instantly
///   from cache or before sampler ticked) still get a small bump.
///
/// Returns a u64 weight; callers normalize by the sum.
fn mirror_weight(current_speed_bps: u64, failed: u64, succeeded: u64) -> u64 {
    if current_speed_bps == 0 && failed == 0 && succeeded == 0 {
        return 100;
    }
    let speed_kib = (current_speed_bps / 1024).max(1);
    let success_bonus = succeeded.min(10);
    let raw = speed_kib + success_bonus;
    let denom = 1 + 4 * failed;
    (raw / denom).max(1)
}

/// Split `[span_start, span_end]` along volume boundaries so each returned
/// sub-span lives inside exactly one volume. Mirror mode (`volumes == None`)
/// returns the span unchanged. Sub-spans entirely outside the volume layout
/// are skipped. Used by `fetch_span_to_tx` so cache-aligned spans that
/// straddle a boundary still get every byte written.
fn slice_span_by_volumes(
    volumes: Option<&[VolumeMeta]>,
    span_start: u64,
    span_end: u64,
) -> Vec<(u64, u64)> {
    match volumes {
        None => vec![(span_start, span_end)],
        Some(vols) => {
            let mut out = Vec::new();
            for v in vols {
                if v.size == 0 {
                    continue;
                }
                let v_end = v.offset + v.size - 1;
                if v_end < span_start || v.offset > span_end {
                    continue;
                }
                let s = span_start.max(v.offset);
                let e = span_end.min(v_end);
                if s <= e {
                    out.push((s, e));
                }
            }
            if out.is_empty() {
                // Span entirely outside any volume — shouldn't happen but
                // returning the original span preserves the old single-call
                // behaviour (caller will then error cleanly via resolve_target).
                out.push((span_start, span_end));
            }
            out
        }
    }
}

/// Build the per-chunk fetch plan for a merged range. In volume mode, every
/// chunk is clipped to live in exactly one volume (so a single HTTP request
/// can serve it); within each volume, chunks are further capped to `split`
/// bytes. In mirror mode (volumes == None), the merged range is just split
/// by `split` directly.
fn plan_chunks(
    start: u64,
    end: u64,
    split: u64,
    volumes: Option<&[VolumeMeta]>,
) -> Vec<(u64, u64)> {
    let split = split.max(1);
    let mut plan: Vec<(u64, u64)> = Vec::new();
    match volumes {
        Some(vols) => {
            for v in vols {
                if v.size == 0 {
                    continue;
                }
                let v_end = v.offset + v.size - 1;
                if v_end < start || v.offset > end {
                    continue;
                }
                let clip_start = v.offset.max(start);
                let clip_end = v_end.min(end);
                let mut cur = clip_start;
                while cur <= clip_end {
                    let stop = (cur + split - 1).min(clip_end);
                    plan.push((cur, stop));
                    cur = stop + 1;
                }
            }
        }
        None => {
            let mut cur = start;
            while cur <= end {
                let stop = (cur + split - 1).min(end);
                plan.push((cur, stop));
                cur = stop + 1;
            }
        }
    }
    plan
}

/// `plan_chunks` plus an optional "head zone" of smaller chunks. The first
/// `head.1` chunks of the resulting plan are capped at `head.0` bytes; the
/// rest of the plan is unchanged. Used by `stream_range` to keep open-ended
/// `Range: X-` requests cheap to cancel.
fn plan_chunks_with_head(
    start: u64,
    end: u64,
    split: u64,
    volumes: Option<&[VolumeMeta]>,
    head: Option<(u64, usize)>,
) -> Vec<(u64, u64)> {
    let plan = plan_chunks(start, end, split, volumes);
    let (small_size, small_count) = match head {
        Some(h) if h.0 > 0 && h.1 > 0 => h,
        _ => return plan,
    };
    // Walk the plan prefix, splitting any oversized chunk into a small head
    // piece plus a remainder. We stop once we've emitted `small_count` head
    // pieces; everything beyond stays at the original split size.
    let mut out: Vec<(u64, u64)> = Vec::with_capacity(plan.len() + small_count);
    let mut head_emitted: usize = 0;
    let mut iter = plan.into_iter().peekable();
    while head_emitted < small_count {
        let (cs, ce) = match iter.next() {
            Some(p) => p,
            None => break,
        };
        let len = ce - cs + 1;
        if len <= small_size {
            out.push((cs, ce));
            head_emitted += 1;
            continue;
        }
        // Split this chunk into a small head piece and a remainder.
        let head_end = cs + small_size - 1;
        out.push((cs, head_end));
        head_emitted += 1;
        // Re-process the remainder: if we still have head budget AND the
        // remainder is itself > small_size, we'll split it again on the next
        // loop iteration. Push it back via a small detour through the iter.
        let remainder = (head_end + 1, ce);
        // Build a fresh iterator: remainder followed by everything still in `iter`.
        let mut tail: Vec<(u64, u64)> = Vec::with_capacity(1 + iter.len());
        tail.push(remainder);
        tail.extend(iter);
        iter = tail.into_iter().peekable();
    }
    out.extend(iter);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_plain_filename() {
        let s = parse_content_disposition_filename("attachment; filename=\"hello.bin\"");
        assert_eq!(s.as_deref(), Some("hello.bin"));
    }

    #[test]
    fn parses_rfc5987_utf8_filename() {
        let s = parse_content_disposition_filename(
            "attachment; filename=\"file.bin\"; filename*=UTF-8''%E4%B8%AD%E6%96%87.mp4",
        );
        assert_eq!(s.as_deref(), Some("中文.mp4"));
    }

    #[test]
    fn rfc5987_overrides_plain() {
        let s = parse_content_disposition_filename(
            "attachment; filename=\"ascii.bin\"; filename*=UTF-8''real%2Ename.bin",
        );
        assert_eq!(s.as_deref(), Some("real.name.bin"));
    }

    #[test]
    fn falls_back_to_url_path() {
        assert_eq!(
            filename_from_url("https://cdn.example.com/path/to/movie.mp4?token=abc"),
            Some("movie.mp4".to_string())
        );
        assert_eq!(
            filename_from_url("https://cdn.example.com/%E4%B8%AD%E6%96%87.mp4"),
            Some("中文.mp4".to_string())
        );
        assert_eq!(filename_from_url("https://cdn.example.com/"), None);
    }

    #[test]
    fn sanitize_strips_path_separators() {
        assert_eq!(
            sanitize_filename("../etc/passwd".to_string()),
            "..etcpasswd"
        );
    }

    fn vol(url: &str, offset: u64, size: u64) -> VolumeMeta {
        VolumeMeta {
            urls: vec![url.to_string()],
            offset,
            size,
        }
    }

    #[test]
    fn plan_mirror_mode_splits_by_size() {
        let plan = plan_chunks(0, 99, 30, None);
        assert_eq!(
            plan,
            vec![(0, 29), (30, 59), (60, 89), (90, 99)]
        );
    }

    #[test]
    fn head_split_is_noop_when_no_head_requested() {
        let p1 = plan_chunks(0, 99, 30, None);
        let p2 = plan_chunks_with_head(0, 99, 30, None, None);
        assert_eq!(p1, p2);
    }

    #[test]
    fn head_split_carves_small_prefix_from_oversize_first_chunk() {
        // One big virtual chunk (split=10MB, range=10MB). Head config asks
        // for 4 chunks of 256KB. Result: 4 × 256KB head + the remainder.
        let split = 10 * 1024 * 1024u64;
        let small = 256 * 1024u64;
        let plan = plan_chunks_with_head(0, split - 1, split, None, Some((small, 4)));
        assert_eq!(plan[0], (0, small - 1));
        assert_eq!(plan[1], (small, 2 * small - 1));
        assert_eq!(plan[2], (2 * small, 3 * small - 1));
        assert_eq!(plan[3], (3 * small, 4 * small - 1));
        // Remainder is one big chunk that goes back to the original split size.
        assert_eq!(plan[4], (4 * small, split - 1));
        assert_eq!(plan.len(), 5);
    }

    #[test]
    fn head_split_keeps_natural_small_chunks_intact() {
        // If the natural plan already produced chunks ≤ small_size, no further
        // splitting should happen — head_emitted just consumes them in place.
        let plan = plan_chunks_with_head(0, 99, 30, None, Some((50 * 1024, 4)));
        // Original plan: (0,29) (30,59) (60,89) (90,99) — all under 50K.
        assert_eq!(plan, vec![(0, 29), (30, 59), (60, 89), (90, 99)]);
    }

    #[test]
    fn head_split_respects_volume_boundaries() {
        // Two volumes [0..100), [100..250). split = 80. Head config = 2 chunks
        // of 30 bytes. The first volume's first chunk (0..79) gets split into
        // (0,29) + (30,59) + remainder (60,79); then we leave normal sizing.
        let vols = vec![vol("a", 0, 100), vol("b", 100, 150)];
        let plan = plan_chunks_with_head(0, 249, 80, Some(&vols), Some((30, 2)));
        // Expected: head chunks 0..29 and 30..59 (the two small head pieces),
        // then the remainder of vol a chunk (60..79), then vol a's second
        // chunk (80..99), then vol b's two chunks.
        assert_eq!(plan[0], (0, 29));
        assert_eq!(plan[1], (30, 59));
        assert_eq!(plan[2], (60, 79));
        assert_eq!(plan[3], (80, 99));
        assert_eq!(plan[4], (100, 179));
        assert_eq!(plan[5], (180, 249));
        // No chunk crosses a volume boundary.
        for (cs, ce) in &plan {
            for v in &vols {
                let v_end = v.offset + v.size - 1;
                if *cs >= v.offset && *cs < v.offset + v.size {
                    assert!(*ce <= v_end, "chunk ({}..={}) leaks across volume", cs, ce);
                }
            }
        }
    }

    #[test]
    fn plan_volume_mode_breaks_at_boundaries() {
        // Two volumes: [0..100), [100..250). split=80 forces a sub-chunk split
        // inside each volume *and* a boundary split between them, so the same
        // chunk never spans two URLs.
        let vols = vec![vol("a", 0, 100), vol("b", 100, 150)];
        let plan = plan_chunks(0, 249, 80, Some(&vols));
        assert_eq!(
            plan,
            vec![
                (0, 79),
                (80, 99),
                (100, 179),
                (180, 249),
            ]
        );
    }

    #[test]
    fn plan_volume_mode_partial_range_inside_one_volume() {
        // Range [120, 180] is entirely inside the second volume [100..250).
        let vols = vec![vol("a", 0, 100), vol("b", 100, 150)];
        let plan = plan_chunks(120, 180, 200, Some(&vols));
        assert_eq!(plan, vec![(120, 180)]);
    }

    #[test]
    fn plan_volume_mode_range_spans_boundary() {
        // Range [80, 160] crosses the boundary at 100.
        let vols = vec![vol("a", 0, 100), vol("b", 100, 150)];
        let plan = plan_chunks(80, 160, 200, Some(&vols));
        assert_eq!(plan, vec![(80, 99), (100, 160)]);
    }

    #[test]
    fn plan_volume_mode_skips_empty_volumes() {
        let vols = vec![vol("a", 0, 50), vol("empty", 50, 0), vol("c", 50, 50)];
        let plan = plan_chunks(0, 99, 200, Some(&vols));
        assert_eq!(plan, vec![(0, 49), (50, 99)]);
    }

    #[test]
    fn suggest_filename_lcp_trims_part_suffix() {
        let urls = vec![
            "https://cdn.example.com/movie.part01.mp4".to_string(),
            "https://cdn.example.com/movie.part02.mp4".to_string(),
            "https://cdn.example.com/movie.part03.mp4".to_string(),
        ];
        // LCP is "movie.part0" → trim digits → "movie.part" → strip "part" →
        // "movie." → trim dot → "movie".
        assert_eq!(suggest_volume_filename(&urls).as_deref(), Some("movie"));
    }

    #[test]
    fn suggest_filename_lcp_handles_zip_style() {
        let urls = vec![
            "https://cdn.example.com/big.zip.001".to_string(),
            "https://cdn.example.com/big.zip.002".to_string(),
        ];
        // LCP "big.zip.00" → trim digits + dot → "big.zip".
        assert_eq!(suggest_volume_filename(&urls).as_deref(), Some("big.zip"));
    }

    #[test]
    fn suggest_filename_single_url_returns_as_is() {
        let urls = vec!["https://cdn.example.com/movie.mp4".to_string()];
        assert_eq!(suggest_volume_filename(&urls).as_deref(), Some("movie.mp4"));
    }

    #[test]
    fn suggest_filename_no_common_prefix_falls_back() {
        let urls = vec![
            "https://cdn.example.com/alpha.bin".to_string(),
            "https://cdn.example.com/beta.bin".to_string(),
        ];
        // No useful common prefix → fall back to first.
        assert_eq!(suggest_volume_filename(&urls).as_deref(), Some("alpha.bin"));
    }

    #[test]
    fn suggest_filename_handles_utf8_safely() {
        let urls = vec![
            "https://cdn.example.com/%E7%94%B5%E5%BD%B1.part1.mp4".to_string(),
            "https://cdn.example.com/%E7%94%B5%E5%BD%B1.part2.mp4".to_string(),
        ];
        // Should not panic on multibyte boundary slicing.
        assert_eq!(suggest_volume_filename(&urls).as_deref(), Some("电影"));
    }

    #[test]
    fn merge_filenames_prefers_lcp_of_detected_names() {
        // Every volume reports a Content-Disposition name like "t.mkv.01" —
        // LCP + trim should give "t.mkv", not the first volume's raw name.
        let per_vol = vec![
            Some("t.mkv.01".to_string()),
            Some("t.mkv.02".to_string()),
            Some("t.mkv.03".to_string()),
        ];
        let urls = vec![
            "http://host/t.mkv.01".to_string(),
            "http://host/t.mkv.02".to_string(),
            "http://host/t.mkv.03".to_string(),
        ];
        assert_eq!(merge_volume_filenames(&per_vol, &urls).as_deref(), Some("t.mkv"));
    }

    #[test]
    fn merge_filenames_falls_back_to_urls_when_detection_sparse() {
        // Only volume 0 had a detected name — not enough to LCP. Fall back
        // to URL-derived suggestion.
        let per_vol = vec![Some("ignored.part01".to_string()), None, None];
        let urls = vec![
            "http://host/movie.part01.mp4".to_string(),
            "http://host/movie.part02.mp4".to_string(),
            "http://host/movie.part03.mp4".to_string(),
        ];
        // suggest_volume_filename on the URLs trims to "movie".
        assert_eq!(merge_volume_filenames(&per_vol, &urls).as_deref(), Some("movie"));
    }

    #[test]
    fn merge_filenames_falls_back_when_lcp_is_empty() {
        // Detected names diverge completely — LCP is empty, must not return
        // empty string; fall through to URL derivation.
        let per_vol = vec![Some("alpha.bin".into()), Some("beta.bin".into())];
        let urls = vec![
            "http://host/x.part01".to_string(),
            "http://host/x.part02".to_string(),
        ];
        // URL fallback gives "x".
        assert_eq!(merge_volume_filenames(&per_vol, &urls).as_deref(), Some("x"));
    }

    #[test]
    fn effective_volumes_filters_empties() {
        use crate::models::TaskConfig;
        let cfg = TaskConfig {
            volumes: vec![
                vec!["a1".into(), "  ".into(), "a2".into()],
                vec!["".into()],
                vec!["b1".into()],
            ],
            max_threads: 8,
            max_per_volume: 4,
            max_split: 5 * 1024 * 1024,
            cache: false,
            headers: Default::default(),
            name: None,
            output_filename: None,
            auto_filename: true,
            rate_limit_bps: 0,
            rate_limit_algorithm: Default::default(),
            persist: false,
            plugins: Vec::new(),
            content_disposition: Default::default(),
        };
        // Empty mirror strings and empty volumes are scrubbed; valid order
        // is preserved.
        assert_eq!(
            cfg.effective_volumes(),
            vec![vec!["a1".to_string(), "a2".into()], vec!["b1".into()]],
        );
    }

    #[test]
    fn flat_unique_urls_dedupes_preserving_order() {
        use crate::models::TaskConfig;
        let layout = vec![
            vec!["a".to_string(), "b".into()],
            vec!["b".to_string(), "c".into()],
        ];
        assert_eq!(
            TaskConfig::flat_unique_urls(&layout),
            vec!["a".to_string(), "b".into(), "c".into()],
        );
    }

    /// Pure-data simulation of the warm-up coverage logic in `stream_range`:
    /// given the volume index per plan chunk, `max_threads`, and `vol_count`,
    /// return the set of chunk indices that get spawned *before* the
    /// serializer starts draining (initial window + cross-volume warm-up).
    fn initial_spawn_set(vol_of: &[usize], max_threads: usize, vol_count: usize) -> Vec<usize> {
        let initial = max_threads.min(vol_of.len());
        let mut spawned: Vec<usize> = (0..initial).collect();
        if vol_count > 1 {
            let mut covered = vec![false; vol_count];
            for &idx in &spawned {
                if let Some(&vi) = vol_of.get(idx) {
                    if vi < covered.len() {
                        covered[vi] = true;
                    }
                }
            }
            for vi in 0..vol_count {
                if covered[vi] {
                    continue;
                }
                if let Some(idx) = vol_of.iter().position(|&v| v == vi) {
                    if !spawned.contains(&idx) {
                        spawned.push(idx);
                    }
                }
            }
        }
        spawned
    }

    #[test]
    fn warmup_covers_all_volumes_when_initial_window_only_hits_first() {
        // 3 volumes, max_threads=2. Initial window covers chunks 0,1 — both
        // in volume 0. Warm-up must spawn volume 1's first chunk and volume
        // 2's first chunk.
        // plan: v0 has 5 chunks, v1 has 5 chunks, v2 has 5 chunks.
        let vol_of: Vec<usize> = [0; 5]
            .into_iter()
            .chain([1; 5])
            .chain([2; 5])
            .collect();
        let spawned = initial_spawn_set(&vol_of, 2, 3);
        assert!(spawned.contains(&0), "initial chunk 0 must spawn");
        assert!(spawned.contains(&1), "initial chunk 1 must spawn");
        assert!(spawned.contains(&5), "v1's first chunk (idx=5) must be warmed up");
        assert!(spawned.contains(&10), "v2's first chunk (idx=10) must be warmed up");
        assert_eq!(spawned.len(), 4);
    }

    #[test]
    fn warmup_is_noop_when_initial_window_already_covers_all_volumes() {
        // 3 volumes, max_threads=8. Plan order means initial 8 may already
        // span volumes 0 and 1 but not 2 if v0 is large.
        let vol_of: Vec<usize> = [0; 20]
            .into_iter()
            .chain([1; 20])
            .chain([2; 20])
            .collect();
        let spawned = initial_spawn_set(&vol_of, 8, 3);
        // Initial window is all in v0; warm-up adds v1's first (idx=20) and
        // v2's first (idx=40).
        assert_eq!(spawned.len(), 10);
        assert!(spawned.contains(&20));
        assert!(spawned.contains(&40));
    }

    #[test]
    fn warmup_skips_volumes_already_covered_by_initial() {
        // Plan ordering puts v0 first then v1 (small v0).
        let vol_of: Vec<usize> = [0, 0, 1, 1, 1, 1].to_vec();
        // max_threads=4 covers chunks 0..4 which span v0 and v1.
        let spawned = initial_spawn_set(&vol_of, 4, 2);
        assert_eq!(spawned, vec![0, 1, 2, 3]);
    }

    #[test]
    fn warmup_noop_for_single_volume_tasks() {
        // Mirror mode (vol_count=1): no extra warm-up; behaves exactly like
        // the pre-multi-volume scheduler.
        let vol_of: Vec<usize> = vec![0; 10];
        let spawned = initial_spawn_set(&vol_of, 4, 1);
        assert_eq!(spawned, vec![0, 1, 2, 3]);
    }

    #[test]
    fn warmup_chunk_zero_always_first() {
        // No-deadlock invariant: chunk 0 must be in the spawn set.
        let vol_of: Vec<usize> = [0; 3]
            .into_iter()
            .chain([1; 3])
            .chain([2; 3])
            .collect();
        let spawned = initial_spawn_set(&vol_of, 1, 3);
        assert!(spawned.contains(&0));
        // Warm-up adds first chunk of v1 (idx=3) and v2 (idx=6).
        assert!(spawned.contains(&3));
        assert!(spawned.contains(&6));
    }

    /// Pure-data simulation of `try_spawn_more`'s two-pass policy: pick the
    /// first not-yet-spawned chunk whose volume has room (strict pass), and
    /// only if no such chunk exists, allow over-cap spawns (overflow pass).
    fn simulate_spawn_set(
        vol_of: &[usize],
        max_threads: usize,
        max_per_volume: usize,
        vol_count: usize,
    ) -> Vec<usize> {
        let total = vol_of.len();
        let mut spawned = vec![false; total];
        let mut per_vol = vec![0usize; vol_count];
        let mut total_in_flight = 0usize;
        let mut order: Vec<usize> = Vec::new();
        loop {
            if total_in_flight >= max_threads {
                break;
            }
            let mut pick: Option<usize> = None;
            for idx in 0..total {
                if spawned[idx] {
                    continue;
                }
                let vi = vol_of[idx];
                if per_vol[vi] >= max_per_volume {
                    continue;
                }
                pick = Some(idx);
                break;
            }
            if pick.is_none() {
                for idx in 0..total {
                    if !spawned[idx] {
                        pick = Some(idx);
                        break;
                    }
                }
            }
            let idx = match pick {
                Some(i) => i,
                None => break,
            };
            spawned[idx] = true;
            per_vol[vol_of[idx]] += 1;
            total_in_flight += 1;
            order.push(idx);
        }
        order
    }

    #[test]
    fn overflow_fills_idle_slots_when_only_one_volume_in_plan() {
        // Single-volume Range (or single-volume task): 10 chunks all in v0.
        // max_threads=8, max_per_volume=4. Old behavior: only 4 spawn. New
        // behavior: all 8 slots get filled by overflow.
        let vol_of: Vec<usize> = vec![0; 10];
        let spawned = simulate_spawn_set(&vol_of, 8, 4, 1);
        assert_eq!(spawned.len(), 8, "expected 8 fetchers, got {}", spawned.len());
        assert_eq!(spawned, vec![0, 1, 2, 3, 4, 5, 6, 7]);
    }

    #[test]
    fn strict_pass_spreads_across_volumes_before_overflow() {
        // Two volumes, plenty of chunks each. max_threads=8, cap=4.
        // Strict pass should give 4 to v0 and 4 to v1 — no overflow needed.
        let vol_of: Vec<usize> = [0; 10]
            .into_iter()
            .chain([1; 10])
            .collect();
        let spawned = simulate_spawn_set(&vol_of, 8, 4, 2);
        assert_eq!(spawned.len(), 8);
        let v0_count = spawned.iter().filter(|&&i| vol_of[i] == 0).count();
        let v1_count = spawned.iter().filter(|&&i| vol_of[i] == 1).count();
        assert_eq!(v0_count, 4, "v0 should hit its cap exactly");
        assert_eq!(v1_count, 4, "v1 should hit its cap exactly");
    }

    #[test]
    fn overflow_kicks_in_only_after_other_volumes_exhausted() {
        // v0 has 10 chunks, v1 has 2 chunks. max_threads=8, cap=4.
        // Strict pass: 4 from v0 + 2 from v1 = 6. Overflow needs to push 2
        // more — and they must come from v0 (v1 is empty).
        let vol_of: Vec<usize> = [0; 10]
            .into_iter()
            .chain([1; 2])
            .collect();
        let spawned = simulate_spawn_set(&vol_of, 8, 4, 2);
        assert_eq!(spawned.len(), 8);
        let v0_count = spawned.iter().filter(|&&i| vol_of[i] == 0).count();
        let v1_count = spawned.iter().filter(|&&i| vol_of[i] == 1).count();
        assert_eq!(v1_count, 2, "v1 contributes its only 2 chunks");
        assert_eq!(v0_count, 6, "remaining 6 slots overflow into v0");
    }

    #[test]
    fn no_overflow_when_total_chunks_below_max_threads() {
        // Edge case: only 3 chunks total, max_threads=8. Spawn all 3 then stop.
        let vol_of: Vec<usize> = vec![0, 0, 0];
        let spawned = simulate_spawn_set(&vol_of, 8, 4, 1);
        assert_eq!(spawned, vec![0, 1, 2]);
    }

    #[test]
    fn plan_volume_indices_mirror_mode_all_zero() {
        let plan = vec![(0, 99), (100, 199), (200, 299)];
        assert_eq!(plan_volume_indices(&plan, None), vec![0, 0, 0]);
    }

    #[test]
    fn plan_volume_indices_volume_mode_maps_by_offset() {
        // Volumes: [0..100), [100..250), [250..400)
        let vols = vec![vol("a", 0, 100), vol("b", 100, 150), vol("c", 250, 150)];
        // Plan derived from plan_chunks: each chunk lives in exactly one volume.
        let plan = plan_chunks(0, 399, 80, Some(&vols));
        let mapped = plan_volume_indices(&plan, Some(&vols));
        // Every chunk's volume index matches the volume containing its start.
        for ((cs, _ce), &vi) in plan.iter().zip(mapped.iter()) {
            let v = &vols[vi];
            assert!(*cs >= v.offset && *cs < v.offset + v.size,
                    "chunk start {} not in volume {} [{},{})", cs, vi, v.offset, v.offset + v.size);
        }
    }

    #[test]
    fn plan_volume_indices_skips_empty_volumes() {
        // Empty volume in the middle — plan_chunks skips it, so no chunk
        // should ever map to its index. (We still must not panic.)
        let vols = vec![vol("a", 0, 50), vol("empty", 50, 0), vol("c", 50, 50)];
        let plan = plan_chunks(0, 99, 200, Some(&vols));
        let mapped = plan_volume_indices(&plan, Some(&vols));
        assert!(!mapped.contains(&1), "empty volume must not appear in mapping");
    }

    #[test]
    fn slice_span_single_volume_returns_unchanged() {
        let vols = vec![vol("a", 0, 1000)];
        assert_eq!(slice_span_by_volumes(Some(&vols), 100, 500), vec![(100, 500)]);
    }

    #[test]
    fn slice_span_mirror_mode_returns_unchanged() {
        assert_eq!(slice_span_by_volumes(None, 100, 500), vec![(100, 500)]);
    }

    #[test]
    fn slice_span_splits_across_two_volumes() {
        // Volumes: [0..100), [100..250)
        let vols = vec![vol("a", 0, 100), vol("b", 100, 150)];
        // Span 50..199 crosses the boundary at 100.
        assert_eq!(
            slice_span_by_volumes(Some(&vols), 50, 199),
            vec![(50, 99), (100, 199)],
        );
    }

    #[test]
    fn slice_span_splits_across_three_volumes() {
        let vols = vec![vol("a", 0, 100), vol("b", 100, 100), vol("c", 200, 100)];
        // Span spans all three.
        assert_eq!(
            slice_span_by_volumes(Some(&vols), 50, 250),
            vec![(50, 99), (100, 199), (200, 250)],
        );
    }

    #[test]
    fn slice_span_skips_empty_volumes() {
        let vols = vec![vol("a", 0, 100), vol("empty", 100, 0), vol("c", 100, 100)];
        assert_eq!(
            slice_span_by_volumes(Some(&vols), 50, 150),
            vec![(50, 99), (100, 150)],
        );
    }

    #[test]
    fn slice_span_boundary_exactly_at_volume_edge() {
        let vols = vec![vol("a", 0, 100), vol("b", 100, 100)];
        // 99 is last byte of vol a, 100 is first of vol b.
        assert_eq!(
            slice_span_by_volumes(Some(&vols), 99, 100),
            vec![(99, 99), (100, 100)],
        );
    }

    #[test]
    fn mirror_weight_unknown_mirror_is_neutral() {
        // Brand-new mirror: should get a baseline so it's sampled.
        assert_eq!(mirror_weight(0, 0, 0), 100);
    }

    #[test]
    fn mirror_weight_fast_outweighs_slow() {
        let fast = mirror_weight(10 * 1024 * 1024, 0, 5);
        let slow = mirror_weight(100 * 1024, 0, 5);
        assert!(fast > slow * 10, "fast={} slow={}", fast, slow);
    }

    #[test]
    fn mirror_weight_failures_sharply_discount() {
        let healthy = mirror_weight(1024 * 1024, 0, 10);
        let flaky = mirror_weight(1024 * 1024, 10, 10);
        assert!(healthy > flaky * 5, "healthy={} flaky={}", healthy, flaky);
    }

    #[test]
    fn mirror_weight_never_returns_zero() {
        // Even a totally failed mirror gets weight 1 — picker handles total=0
        // separately by dropping to round-robin.
        assert!(mirror_weight(0, 1000, 0) >= 1);
    }
}
