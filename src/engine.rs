use crate::cache::CacheEntry;
use crate::error::{ProxyError, Result};
use crate::models::{TaskConfig, UrlHealthAcc};
use bytes::Bytes;
use futures_util::StreamExt;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, RANGE};
use reqwest::{Client, StatusCode};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;

pub struct UpstreamProbe {
    pub total_size: Option<u64>,
    pub accepts_ranges: bool,
    pub content_type: Option<String>,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
}

pub struct Engine {
    client: Client,
    config: Arc<TaskConfig>,
    rr_counter: AtomicUsize,
    cache: Option<Arc<CacheEntry>>,
    health: Vec<Arc<UrlHealthAcc>>,
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

    fn now_unix() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
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

    fn pick_url(&self) -> Result<&str> {
        if self.config.urls.is_empty() {
            return Err(ProxyError::NoUpstream);
        }
        let i = self.rr_counter.fetch_add(1, Ordering::Relaxed);
        Ok(&self.config.urls[i % self.config.urls.len()])
    }

    fn pick_url_for(&self, idx: usize) -> Result<&str> {
        if self.config.urls.is_empty() {
            return Err(ProxyError::NoUpstream);
        }
        Ok(&self.config.urls[idx % self.config.urls.len()])
    }

    pub async fn probe(&self) -> Result<UpstreamProbe> {
        let url = self.pick_url()?;
        let base_headers = self.build_headers(None)?;

        // Step 1: HEAD for cheap metadata (content-type, content-length, etag, ...).
        // Some CDNs (nginx without `add_header Accept-Ranges`, Cloudflare in some
        // configs, GCS, etc.) omit `Accept-Ranges` from HEAD even when they
        // happily serve byte ranges, so HEAD alone cannot tell us whether ranges
        // are supported.
        let head_start = Instant::now();
        let head = self
            .client
            .head(url)
            .headers(base_headers.clone())
            .send()
            .await
            .ok()
            .filter(|r| r.status().is_success());
        if let Some(r) = &head {
            self.record_success(url, r.status().as_u16(), head_start.elapsed().as_millis() as u64);
        }

        let mut total_size: Option<u64> = None;
        let mut content_type: Option<String> = None;
        let mut etag: Option<String> = None;
        let mut last_modified: Option<String> = None;
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

        Ok(UpstreamProbe {
            total_size,
            accepts_ranges,
            content_type,
            etag,
            last_modified,
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
    pub fn stream_range(
        self: Arc<Self>,
        start: u64,
        end: u64,
    ) -> mpsc::Receiver<Result<Bytes>> {
        let split = self.config.max_split.max(64 * 1024);
        let max_threads = self.config.max_threads.max(1);

        // Build chunk plan
        let mut plan: Vec<(u64, u64)> = Vec::new();
        let mut cur = start;
        while cur <= end {
            let stop = (cur + split - 1).min(end);
            plan.push((cur, stop));
            cur = stop + 1;
        }
        let total_chunks = plan.len();

        // Per-chunk channels so fetchers can run concurrently while the
        // serializer stitches bytes back together in plan order.
        let mut senders: Vec<Option<mpsc::Sender<Result<Bytes>>>> =
            Vec::with_capacity(total_chunks);
        let mut receivers: Vec<mpsc::Receiver<Result<Bytes>>> =
            Vec::with_capacity(total_chunks);
        for _ in 0..total_chunks {
            let (tx, rx) = mpsc::channel::<Result<Bytes>>(8);
            senders.push(Some(tx));
            receivers.push(rx);
        }

        // Output channel to caller
        let (out_tx, out_rx) = mpsc::channel::<Result<Bytes>>(16);

        // Driver: spawns chunk fetchers in a sliding window AND drains them
        // in order. This is the right way to bound concurrency for an
        // *ordered* stream: a plain Semaphore would deadlock because a
        // non-zero chunk could win the permit race, fill its bounded channel,
        // block on `tx.send().await`, and starve chunk 0 forever — the
        // serializer drains strictly in plan order, so chunks ahead can't
        // make progress until the one being drained finishes.
        let engine_arc = Arc::clone(&self);
        tokio::spawn(async move {
            let spawn_fetch =
                |idx: usize, tx: mpsc::Sender<Result<Bytes>>, plan: &[(u64, u64)]| {
                    let engine = Arc::clone(&engine_arc);
                    let (cs, ce) = plan[idx];
                    tokio::spawn(async move {
                        let result = engine.fetch_chunk_to(idx, cs, ce, &tx).await;
                        if let Err(e) = result {
                            let _ = tx.send(Err(e)).await;
                        }
                        // `tx` dropped here — receiver for chunk `idx` closes.
                    });
                };

            // Initial window: start the first `max_threads` chunks. Chunk 0
            // is always in this set, so the serializer can immediately make
            // progress.
            let initial = max_threads.min(total_chunks);
            for idx in 0..initial {
                let tx = senders[idx]
                    .take()
                    .expect("sender should be present on first spawn");
                spawn_fetch(idx, tx, &plan);
            }

            let mut next_to_spawn = initial;
            for (i, mut rx) in receivers.into_iter().enumerate() {
                // Drain chunk i in order, forwarding to the client.
                while let Some(item) = rx.recv().await {
                    let is_err = item.is_err();
                    if out_tx.send(item).await.is_err() {
                        // Client dropped the connection — abandon the rest.
                        return;
                    }
                    if is_err {
                        return;
                    }
                }
                // Chunk i is fully delivered; slide the window forward.
                if next_to_spawn < total_chunks {
                    let idx = next_to_spawn;
                    if let Some(tx) = senders[idx].take() {
                        spawn_fetch(idx, tx, &plan);
                    }
                    next_to_spawn += 1;
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
                match cache.read_range(span_start, span_end) {
                    Ok(bytes) => {
                        cache.hits.fetch_add(j - i + 1, Ordering::Relaxed);
                        if tx.send(Ok(bytes)).await.is_err() {
                            return Ok(());
                        }
                    }
                    Err(_) => {
                        // Disk read failed — fall back to origin for this span,
                        // but don't try to cache the result (the underlying
                        // file may be broken).
                        let from = i * bs;
                        let to = ((j + 1) * bs - 1).min(total - 1);
                        self.fetch_span_to_tx(idx, from, to, start, end, None, tx)
                            .await?;
                    }
                }
            } else {
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
        let attempts = self.config.urls.len().max(1) * 2;
        let mut last_err: Option<ProxyError> = None;
        for attempt in 0..attempts {
            let url = self.pick_url_for(idx + attempt)?.to_string();
            let headers = self.build_headers(Some((span_start, span_end)))?;
            let req_start = Instant::now();
            let resp = match self.client.get(&url).headers(headers).send().await {
                Ok(r) => r,
                Err(e) => {
                    let msg = e.to_string();
                    self.record_failure(&url, None, &msg);
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
                    &url,
                    Some(status_code),
                    &format!("expected 206, got {status_code}"),
                );
                last_err = Some(ProxyError::BadStatus(status.as_u16()));
                continue;
            }
            self.record_success(&url, status_code, latency_ms);

            let mut cursor = span_start;
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

                        if let Some(c) = cache {
                            if let Err(e) = c.write_range(piece_start, &b) {
                                tracing::warn!(
                                    "cache write at offset {} failed: {}",
                                    piece_start,
                                    e
                                );
                            }
                        }

                        self.record_bytes(&url, b.len() as u64);

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
                        if tx.send(Ok(to_send)).await.is_err() {
                            return Ok(());
                        }
                    }
                    Err(e) => {
                        let msg = e.to_string();
                        self.record_failure(&url, Some(status_code), &msg);
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
        let attempts = self.config.urls.len().max(1) * 2;
        for attempt in 0..attempts {
            let url = self.pick_url_for(idx + attempt)?.to_string();
            let headers = self.build_headers(Some((start, end)))?;
            let req_start = Instant::now();
            let resp = match self.client.get(&url).headers(headers).send().await {
                Ok(r) => r,
                Err(e) => {
                    let msg = e.to_string();
                    self.record_failure(&url, None, &msg);
                    last_err = Some(ProxyError::Upstream(e));
                    continue;
                }
            };
            let latency_ms = req_start.elapsed().as_millis() as u64;
            let status_code = resp.status().as_u16();
            let status = resp.status();
            if !status.is_success() && status != StatusCode::PARTIAL_CONTENT {
                self.record_failure(&url, Some(status_code), &format!("status {status_code}"));
                last_err = Some(ProxyError::BadStatus(status.as_u16()));
                continue;
            }
            self.record_success(&url, status_code, latency_ms);

            // If server ignored Range and returned 200 OK, we got the whole
            // file. Slice the [start, end] window out as we read, so the
            // serializer still gets exactly the bytes for this chunk.
            let needs_slice = status == StatusCode::OK;
            let chunk_len = end - start + 1;
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
                            if b_end <= start || b_start > end {
                                if b_start > end {
                                    break;
                                }
                                continue;
                            }
                            let lo = start.saturating_sub(b_start) as usize;
                            let hi = (end + 1 - b_start).min(b.len() as u64) as usize;
                            b.slice(lo..hi)
                        } else {
                            b
                        };
                        if to_send.is_empty() {
                            continue;
                        }
                        self.record_bytes(&url, to_send.len() as u64);
                        emitted += to_send.len() as u64;
                        if tx.send(Ok(to_send)).await.is_err() {
                            return Ok(());
                        }
                        if needs_slice && emitted >= chunk_len {
                            break;
                        }
                    }
                    Err(e) => {
                        let msg = e.to_string();
                        self.record_failure(&url, Some(status_code), &msg);
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
