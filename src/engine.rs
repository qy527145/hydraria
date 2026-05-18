use crate::error::{ProxyError, Result};
use crate::models::TaskConfig;
use bytes::Bytes;
use futures_util::StreamExt;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, RANGE};
use reqwest::{Client, StatusCode};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
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
        })
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
        let headers = self.build_headers(None)?;

        let resp = self
            .client
            .head(url)
            .headers(headers.clone())
            .send()
            .await;

        let resp = match resp {
            Ok(r) if r.status().is_success() => r,
            _ => {
                let probe_headers = {
                    let mut h = headers.clone();
                    h.insert(RANGE, HeaderValue::from_static("bytes=0-0"));
                    h
                };
                self.client
                    .get(url)
                    .headers(probe_headers)
                    .send()
                    .await
                    .map_err(ProxyError::Upstream)?
            }
        };

        let status = resp.status();
        if !status.is_success() && status != StatusCode::PARTIAL_CONTENT {
            return Err(ProxyError::BadStatus(status.as_u16()));
        }

        let h = resp.headers();
        let accepts_ranges = h
            .get(reqwest::header::ACCEPT_RANGES)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.eq_ignore_ascii_case("bytes"))
            .unwrap_or(false)
            || status == StatusCode::PARTIAL_CONTENT;

        let total_size = if let Some(cr) = h.get(reqwest::header::CONTENT_RANGE) {
            cr.to_str()
                .ok()
                .and_then(|s| s.rsplit('/').next())
                .and_then(|s| s.trim().parse::<u64>().ok())
        } else {
            h.get(reqwest::header::CONTENT_LENGTH)
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok())
        };

        let content_type = h
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(String::from);
        let etag = h
            .get(reqwest::header::ETAG)
            .and_then(|v| v.to_str().ok())
            .map(String::from);
        let last_modified = h
            .get(reqwest::header::LAST_MODIFIED)
            .and_then(|v| v.to_str().ok())
            .map(String::from);

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

        // Channels: one per chunk so we can stitch in order via a serializer task
        let mut chunk_senders: Vec<mpsc::Sender<Result<Bytes>>> = Vec::with_capacity(total_chunks);
        let mut chunk_receivers: Vec<mpsc::Receiver<Result<Bytes>>> =
            Vec::with_capacity(total_chunks);
        for _ in 0..total_chunks {
            let (tx, rx) = mpsc::channel::<Result<Bytes>>(8);
            chunk_senders.push(tx);
            chunk_receivers.push(rx);
        }

        // Output channel to caller
        let (out_tx, out_rx) = mpsc::channel::<Result<Bytes>>(16);

        // Concurrency limiter via a semaphore
        let sem = Arc::new(tokio::sync::Semaphore::new(max_threads));

        // Spawn per-chunk fetchers
        for (idx, (cs, ce)) in plan.into_iter().enumerate() {
            let engine = Arc::clone(&self);
            let sem = Arc::clone(&sem);
            let tx = chunk_senders[idx].clone();
            tokio::spawn(async move {
                let permit = match sem.acquire_owned().await {
                    Ok(p) => p,
                    Err(_) => {
                        let _ = tx
                            .send(Err(ProxyError::Internal("semaphore closed".into())))
                            .await;
                        return;
                    }
                };
                let result = engine.fetch_chunk_to(idx, cs, ce, &tx).await;
                if let Err(e) = result {
                    let _ = tx.send(Err(e)).await;
                }
                drop(permit);
            });
        }
        drop(chunk_senders);

        // Serializer task: drain receivers in order and forward to out_tx
        tokio::spawn(async move {
            for mut rx in chunk_receivers.into_iter() {
                while let Some(item) = rx.recv().await {
                    let is_err = item.is_err();
                    if out_tx.send(item).await.is_err() {
                        return;
                    }
                    if is_err {
                        return;
                    }
                }
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
        let mut last_err: Option<ProxyError> = None;
        let attempts = self.config.urls.len().max(1) * 2;
        for attempt in 0..attempts {
            let url = self.pick_url_for(idx + attempt)?;
            let headers = self.build_headers(Some((start, end)))?;
            let resp = match self.client.get(url).headers(headers).send().await {
                Ok(r) => r,
                Err(e) => {
                    last_err = Some(ProxyError::Upstream(e));
                    continue;
                }
            };
            let status = resp.status();
            if !status.is_success() && status != StatusCode::PARTIAL_CONTENT {
                last_err = Some(ProxyError::BadStatus(status.as_u16()));
                continue;
            }

            let mut stream = resp.bytes_stream();
            while let Some(item) = stream.next().await {
                match item {
                    Ok(b) => {
                        if tx.send(Ok(b)).await.is_err() {
                            return Ok(());
                        }
                    }
                    Err(e) => {
                        last_err = Some(ProxyError::Upstream(e));
                        // Bail out of this attempt; outer loop retries from next URL.
                        break;
                    }
                }
            }
            if last_err.is_none() {
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
