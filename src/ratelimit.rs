use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::time::{Duration, Instant};

/// Rate-limit algorithm selector.
///
/// * `TokenBucket` — accumulates tokens at `rate` B/s up to a half-second
///   burst. Allows short bursts above the long-run average but converges
///   to the cap. Good for streaming where players prefer a quick buffer
///   fill and then a steady rate.
/// * `SlidingWindow` — strictly never exceeds `rate` bytes within any
///   1-second window. No bursts. Smoother throughput curve, but a fresh
///   reader has to wait the same per-byte time as a long-running one.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum Algorithm {
    #[default]
    TokenBucket,
    SlidingWindow,
}

impl Algorithm {
    fn as_u8(self) -> u8 {
        match self {
            Algorithm::TokenBucket => 0,
            Algorithm::SlidingWindow => 1,
        }
    }

    fn from_u8(v: u8) -> Self {
        match v {
            1 => Algorithm::SlidingWindow,
            _ => Algorithm::TokenBucket,
        }
    }
}

const BURST_SECS: f64 = 0.5;
const MAX_SLEEP: Duration = Duration::from_millis(200);
const WINDOW: Duration = Duration::from_secs(1);

#[derive(Debug)]
struct TbState {
    tokens: f64,
    last_refill: Instant,
}

#[derive(Debug)]
struct SwState {
    window: VecDeque<(Instant, u64)>,
    window_bytes: u64,
}

/// Pluggable rate limiter. `rate_bps == 0` means unlimited and `acquire`
/// is a no-op regardless of algorithm. Both algorithm states are always
/// kept allocated — switching at runtime is just a flag flip + state
/// reset, no reallocation.
#[derive(Debug)]
pub struct Limiter {
    rate_bps: AtomicU64,
    algorithm: AtomicU8,
    tb: Mutex<TbState>,
    sw: Mutex<SwState>,
}

impl Limiter {
    pub fn new(rate_bps: u64, algorithm: Algorithm) -> Self {
        Self {
            rate_bps: AtomicU64::new(rate_bps),
            algorithm: AtomicU8::new(algorithm.as_u8()),
            tb: Mutex::new(TbState {
                tokens: 0.0,
                last_refill: Instant::now(),
            }),
            sw: Mutex::new(SwState {
                window: VecDeque::new(),
                window_bytes: 0,
            }),
        }
    }

    pub fn rate(&self) -> u64 {
        self.rate_bps.load(Ordering::Relaxed)
    }

    pub fn algorithm(&self) -> Algorithm {
        Algorithm::from_u8(self.algorithm.load(Ordering::Relaxed))
    }

    pub fn set_rate(&self, rate_bps: u64) {
        self.rate_bps.store(rate_bps, Ordering::Relaxed);
        self.reset_state();
    }

    pub fn set_algorithm(&self, algorithm: Algorithm) {
        let prev = self.algorithm.swap(algorithm.as_u8(), Ordering::Relaxed);
        if prev != algorithm.as_u8() {
            self.reset_state();
        }
    }

    fn reset_state(&self) {
        // Wipe accumulated tokens / sliding window so a tighter rate or a
        // different algorithm doesn't immediately burst on switchover.
        let mut tb = self.tb.lock();
        tb.tokens = 0.0;
        tb.last_refill = Instant::now();
        let mut sw = self.sw.lock();
        sw.window.clear();
        sw.window_bytes = 0;
    }

    pub async fn acquire(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        if self.rate_bps.load(Ordering::Relaxed) == 0 {
            return;
        }
        match Algorithm::from_u8(self.algorithm.load(Ordering::Relaxed)) {
            Algorithm::TokenBucket => self.acquire_token_bucket(bytes).await,
            Algorithm::SlidingWindow => self.acquire_sliding_window(bytes).await,
        }
    }

    async fn acquire_token_bucket(&self, bytes: u64) {
        let mut remaining = bytes;
        while remaining > 0 {
            let rate = self.rate_bps.load(Ordering::Relaxed);
            if rate == 0 {
                return;
            }
            let sleep = {
                let mut s = self.tb.lock();
                let now = Instant::now();
                let elapsed = now.duration_since(s.last_refill).as_secs_f64();
                s.last_refill = now;
                let cap = (rate as f64) * BURST_SECS;
                s.tokens = (s.tokens + elapsed * rate as f64).min(cap);

                let want = remaining as f64;
                if s.tokens >= want {
                    s.tokens -= want;
                    remaining = 0;
                    Duration::ZERO
                } else {
                    let take = s.tokens.max(0.0);
                    remaining -= take.floor() as u64;
                    s.tokens -= take;
                    let need = (remaining as f64) / rate as f64;
                    Duration::from_secs_f64(need).min(MAX_SLEEP)
                }
            };
            if sleep > Duration::ZERO {
                tokio::time::sleep(sleep).await;
            }
        }
    }

    async fn acquire_sliding_window(&self, bytes: u64) {
        let mut remaining = bytes;
        while remaining > 0 {
            let rate = self.rate_bps.load(Ordering::Relaxed);
            if rate == 0 {
                return;
            }
            // A single piece larger than the per-second cap can never fit;
            // chunk requests at `rate` so we converge in 1-second steps.
            let take = remaining.min(rate);
            let sleep = {
                let mut sw = self.sw.lock();
                let now = Instant::now();
                while let Some(&(t, b)) = sw.window.front() {
                    if now.duration_since(t) >= WINDOW {
                        sw.window.pop_front();
                        sw.window_bytes = sw.window_bytes.saturating_sub(b);
                    } else {
                        break;
                    }
                }
                if sw.window_bytes + take <= rate {
                    sw.window.push_back((now, take));
                    sw.window_bytes += take;
                    remaining -= take;
                    Duration::ZERO
                } else {
                    // Wait until the oldest entry exits the window. Clamp so
                    // a rate change is picked up promptly.
                    let oldest_age = sw
                        .window
                        .front()
                        .map(|(t, _)| now.duration_since(*t))
                        .unwrap_or(WINDOW);
                    WINDOW
                        .saturating_sub(oldest_age)
                        .min(MAX_SLEEP)
                        .max(Duration::from_millis(5))
                }
            };
            if sleep > Duration::ZERO {
                tokio::time::sleep(sleep).await;
            }
        }
    }
}
