use parking_lot::Mutex;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Token bucket rate limiter. A `rate_bps` of 0 means "unlimited" and
/// `acquire` is a no-op. Burst capacity is the bucket's max — sized to
/// half a second of rate so playback doesn't stutter when bytes come in
/// short bursts but the long-run average stays at the configured cap.
#[derive(Debug)]
pub struct TokenBucket {
    rate_bps: AtomicU64,
    state: Mutex<State>,
}

#[derive(Debug)]
struct State {
    tokens: f64,
    last_refill: Instant,
}

const BURST_SECS: f64 = 0.5;
const MAX_SLEEP: Duration = Duration::from_millis(250);

impl TokenBucket {
    pub fn new(rate_bps: u64) -> Self {
        Self {
            rate_bps: AtomicU64::new(rate_bps),
            state: Mutex::new(State {
                tokens: 0.0,
                last_refill: Instant::now(),
            }),
        }
    }

    pub fn unlimited() -> Arc<Self> {
        Arc::new(Self::new(0))
    }

    pub fn set_rate(&self, rate_bps: u64) {
        self.rate_bps.store(rate_bps, Ordering::Relaxed);
        // Reset accounting so an old over-budget deficit doesn't burst
        // immediately after a rate change.
        let mut s = self.state.lock();
        s.tokens = 0.0;
        s.last_refill = Instant::now();
    }

    pub fn rate(&self) -> u64 {
        self.rate_bps.load(Ordering::Relaxed)
    }

    /// Block until `bytes` worth of tokens are available. If the bucket is
    /// configured for unlimited rate, returns immediately.
    pub async fn acquire(&self, bytes: u64) {
        let mut remaining = bytes;
        while remaining > 0 {
            let rate = self.rate_bps.load(Ordering::Relaxed);
            if rate == 0 {
                return;
            }
            let sleep = {
                let mut s = self.state.lock();
                let now = Instant::now();
                let elapsed = now.duration_since(s.last_refill).as_secs_f64();
                s.last_refill = now;
                let cap = (rate as f64) * BURST_SECS;
                s.tokens = (s.tokens + elapsed * rate as f64).min(cap);

                let want = remaining as f64;
                if s.tokens >= want {
                    s.tokens -= want;
                    remaining = 0;
                    Duration::from_millis(0)
                } else {
                    // Spend what we have, then sleep just long enough to
                    // earn the rest (or MAX_SLEEP, whichever is shorter — we
                    // re-check rate after sleeping so a runtime rate change
                    // applies quickly).
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
}
