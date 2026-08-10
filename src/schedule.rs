//! Claim-based work scheduler for staged (out-of-order) fetching.
//!
//! The engine's historic scheduler pre-planned the whole request into
//! fixed-`max_split` chunks and handed them out in plan order, using per-chunk
//! memory channels as the reorder buffer. That coupled two unrelated things:
//! how big an upstream request is (which decides how well per-request latency
//! is amortized) and how much RAM one in-flight chunk may hold.
//!
//! Here the reorder buffer is the staging file, so claim size is free. A worker
//! asks for a [`Claim`] — a contiguous range inside one volume — fetches it in
//! a single HTTP request, and writes bytes at their absolute offsets. Order is
//! irrelevant to correctness; only the ordered reader cares, and it reads from
//! the staging file.
//!
//! Two policies share the machinery:
//!
//! * [`Strategy::Throughput`] — order-agnostic. Claims cover the request in
//!   roughly `max_threads` equal parts, so every worker finishes around the
//!   same time and the file is done as early as possible.
//! * [`Strategy::Latency`] — a small critical window ahead of the reader gets
//!   priority and deliberately *small* claims (short time-to-first-byte);
//!   spare workers prefetch further out with large ones.
//!
//! Work stealing is what makes either policy robust against upstreams with a
//! heavy latency tail: when a worker runs out of unclaimed work it takes the
//! back half of whichever live claim has the most left, so one unlucky slow
//! request can't leave the other workers idle.

use crate::engine::VolumeMeta;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Never hand out (or steal into) a claim smaller than this. Below roughly a
/// megabyte the per-request overhead starts to dominate the transfer on every
/// upstream we care about, and on staging-style relays it dominates by orders
/// of magnitude.
pub const MIN_CLAIM: u64 = 1024 * 1024;

/// Default critical window for [`Strategy::Latency`]: how far ahead of the
/// reader we treat bytes as "needed now" rather than "nice to have".
pub const DEFAULT_CRITICAL_WINDOW: u64 = 32 * 1024 * 1024;

/// Default claim size inside the critical window. Small on purpose — the
/// window's job is to get bytes to the player quickly, and a short range comes
/// back sooner than a long one on essentially every upstream.
pub const DEFAULT_HEAD_CLAIM: u64 = 2 * 1024 * 1024;

/// Where automatic claim sizing starts before anything is known about the
/// upstream. See [`Scheduler::note_claim_outcome`] for why it starts modest
/// instead of jumping straight to an even split of the request.
pub const INITIAL_AUTO_CLAIM: u64 = 8 * 1024 * 1024;

/// Ceiling on automatically-sized claims, in **both** strategies.
///
/// Two independent reasons, and it took a regression to establish that the
/// second one applies to downloads as much as to playback:
///
/// 1. A playback client receives in order, so one claim's latency is a lower
///    bound on how long the reader can stall at the head.
/// 2. Some upstreams — staging relays that materialize an entire requested
///    range before emitting a byte — have per-request latency that grows *with*
///    the range. An even split of a multi-gigabyte file is then a request that
///    never returns, and no amount of parallelism helps.
///
/// Reason 2 has nothing to do with delivery order, so lifting the ceiling for
/// `Strategy::Throughput` (on the theory that a download has no reader to
/// starve) went badly: against such a relay, unbounded claims stalled a download
/// at 176 MB in 180 s where the 64 MiB ceiling moved 768 MB in the same window.
///
/// Bounding growth also keeps halve-on-failure responsive — left unbounded,
/// `auto_target` saturates at astronomical values after a handful of successes
/// and then needs dozens of failures to come back down (observed as a run that
/// stalled 45 s and then dumped 256 MB at once).
///
/// 64 MiB still amortizes per-request overhead ~13× better than the 5 MiB that
/// used to be the fixed default, which is where most of the win lives.
pub const MAX_AUTO_CLAIM: u64 = 64 * 1024 * 1024;

/// How the scheduler prioritizes and sizes claims.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Strategy {
    /// Minimize total completion time; delivery order doesn't matter.
    /// Used for download-shaped requests (`?dl`).
    Throughput,
    /// Minimize time-to-playable around the reader's position, and only then
    /// use spare capacity to prefetch elsewhere. The default.
    Latency {
        critical_window: u64,
        head_claim: u64,
    },
}

impl Strategy {
    pub fn latency_default() -> Self {
        Strategy::Latency {
            critical_window: DEFAULT_CRITICAL_WINDOW,
            head_claim: DEFAULT_HEAD_CLAIM,
        }
    }

    pub fn label(&self) -> &'static str {
        match self {
            Strategy::Throughput => "throughput",
            Strategy::Latency { .. } => "latency",
        }
    }
}

/// A contiguous byte range assigned to one worker, in merged-stream
/// coordinates, guaranteed to lie inside a single volume.
///
/// `end` is shared and mutable: another worker may steal this claim's tail.
/// Workers must therefore consult [`Claim::end`] on every stream item rather
/// than caching it, and stop once the cursor passes it — the same contract
/// gopeed's fetcher has with its re-read of `conn.Chunk.remain()`.
#[derive(Debug, Clone)]
pub struct Claim {
    pub start: u64,
    pub volume: usize,
    end: Arc<AtomicU64>,
    cursor: Arc<AtomicU64>,
}

impl Claim {
    /// Current inclusive end. May shrink between calls.
    pub fn end(&self) -> u64 {
        self.end.load(Ordering::Relaxed)
    }

    /// Next byte this claim still owes.
    pub fn cursor(&self) -> u64 {
        self.cursor.load(Ordering::Relaxed)
    }

    /// Record that everything below `next` has landed in staging.
    pub fn advance_to(&self, next: u64) {
        self.cursor.store(next, Ordering::Relaxed);
    }

    /// True once the cursor has passed the (possibly stolen-from) end.
    pub fn is_complete(&self) -> bool {
        self.cursor() > self.end()
    }

    /// Bytes still owed, or 0 when complete.
    pub fn remaining(&self) -> u64 {
        let (end, cur) = (self.end(), self.cursor());
        if cur > end { 0 } else { end - cur + 1 }
    }
}

/// Scheduler-side record of a handed-out claim.
#[derive(Debug)]
struct Live {
    worker: usize,
    volume: usize,
    end: Arc<AtomicU64>,
    cursor: Arc<AtomicU64>,
}

impl Live {
    fn remaining(&self) -> u64 {
        let end = self.end.load(Ordering::Relaxed);
        let cur = self.cursor.load(Ordering::Relaxed);
        if cur > end { 0 } else { end - cur + 1 }
    }
}

pub struct Scheduler {
    /// Work nobody owns yet: sorted, non-overlapping, inclusive ends.
    unclaimed: Vec<(u64, u64)>,
    live: Vec<Live>,
    strategy: Strategy,
    volumes: Option<Arc<Vec<VolumeMeta>>>,
    max_threads: usize,
    max_per_volume: usize,
    /// Hard upper bound on one claim. `None` means automatic sizing with no
    /// ceiling — the "unlimited split" mode.
    split_cap: Option<u64>,
    req_start: u64,
    req_end: u64,
    /// Where the ordered reader is. Drives the [`Strategy::Latency`] window.
    read_head: u64,
    /// Current ceiling for automatically-sized claims, grown on success and
    /// halved on failure. See [`Scheduler::note_claim_outcome`].
    auto_target: u64,
}

impl Scheduler {
    /// `already_staged` lists ranges (inclusive) that the staging file already
    /// holds, so a reconnect or a warm cache doesn't refetch them.
    pub fn new(
        req_start: u64,
        req_end: u64,
        strategy: Strategy,
        volumes: Option<Arc<Vec<VolumeMeta>>>,
        max_threads: usize,
        max_per_volume: usize,
        split_cap: Option<u64>,
        already_staged: &[(u64, u64)],
    ) -> Self {
        let mut unclaimed = Vec::new();
        if req_start <= req_end {
            unclaimed.push((req_start, req_end));
        }
        let mut s = Self {
            unclaimed,
            live: Vec::new(),
            strategy,
            volumes,
            max_threads: max_threads.max(1),
            max_per_volume: max_per_volume.max(1),
            split_cap: split_cap.filter(|c| *c > 0),
            req_start,
            req_end,
            read_head: req_start,
            auto_target: INITIAL_AUTO_CLAIM,
        };
        for &(s0, e0) in already_staged {
            s.subtract(s0, e0);
        }
        s
    }

    /// Total bytes nobody is working on yet.
    pub fn unclaimed_bytes(&self) -> u64 {
        self.unclaimed.iter().map(|&(s, e)| e - s + 1).sum()
    }

    /// True when every byte of the request is either staged or being fetched.
    pub fn is_drained(&self) -> bool {
        self.unclaimed.is_empty() && self.live.iter().all(|l| l.remaining() == 0)
    }

    /// Move the reader position; re-derives the priority window.
    pub fn set_read_head(&mut self, offset: u64) {
        self.read_head = offset.clamp(self.req_start, self.req_end.saturating_add(1));
    }

    /// Feed a completed claim's outcome back into automatic sizing.
    ///
    /// Bigger claims amortize per-request overhead, which is the whole reason
    /// to allow unlimited ones. But on some upstreams — staging relays that
    /// materialize the entire requested range before emitting a byte — latency
    /// grows *with* the range, so an even split of a multi-gigabyte file is a
    /// request that never returns. Neither "always small" nor "always huge" is
    /// right, and which one applies isn't knowable up front.
    ///
    /// So this probes for it, TCP-slow-start style: double the ceiling after a
    /// success, halve it after a failure. An upstream that streams promptly
    /// converges within a few claims to an even split of the request (the
    /// dedicated-downloader shape); a staging relay settles just under whatever
    /// size still completes. `MIN_CLAIM` floors the descent so a flaky upstream
    /// can't collapse into a request storm.
    pub fn note_claim_outcome(&mut self, ok: bool) {
        self.auto_target = if ok {
            self.auto_target.saturating_mul(2).min(MAX_AUTO_CLAIM)
        } else {
            (self.auto_target / 2).max(MIN_CLAIM)
        };
    }

    /// Current automatic-sizing ceiling — exposed for logging and tests.
    pub fn auto_target(&self) -> u64 {
        self.auto_target
    }

    /// Hand `worker` its next claim, or `None` when there is nothing left to
    /// do — neither unclaimed work nor a live claim big enough to split.
    pub fn claim(&mut self, worker: usize) -> Option<Claim> {
        // Regions in priority order. Latency mode looks in the critical window
        // first; both modes then fall back to the whole request.
        let regions: Vec<(u64, u64)> = match self.strategy {
            Strategy::Throughput => vec![(self.req_start, self.req_end)],
            Strategy::Latency {
                critical_window, ..
            } => {
                // The window must be at least as deep as the whole worker pool
                // can usefully work ahead — `max_threads` claims' worth.
                // Otherwise a window smaller than that leaves most workers with
                // nothing prioritized to do, so they wander off to prefetch
                // regions the reader won't reach for a long time and *starve
                // the read head*. Measured: a fixed 32 MiB window against 16
                // workers held playback to 1.7 MB/s while the upstream as a
                // whole was doing ~12 MB/s on speculative prefetch.
                let depth = (self.max_threads as u64)
                    .saturating_mul(self.auto_target.max(MIN_CLAIM));
                let window = critical_window.max(depth).max(1);
                let hi = self
                    .read_head
                    .saturating_add(window - 1)
                    .min(self.req_end);
                if self.read_head <= hi {
                    vec![(self.read_head, hi), (self.req_start, self.req_end)]
                } else {
                    vec![(self.req_start, self.req_end)]
                }
            }
        };

        for (i, &(rs, re)) in regions.iter().enumerate() {
            let in_critical = i == 0 && matches!(self.strategy, Strategy::Latency { .. });
            // Two passes per region: honour `max_per_volume`, then ignore it.
            //
            // Scoping the overflow to the *region* is the important part. The
            // old scheduler only relaxed the per-volume cap once every volume
            // in the file was saturated, which on a 43-volume task never
            // happened — so idle threads ran off to prefetch volumes the
            // reader wouldn't reach for minutes while the volume it was
            // actually blocked on stayed pinned at `max_per_volume`. Effective
            // concurrency was `max_per_volume`, not `max_threads`.
            for respect_cap in [true, false] {
                if let Some(c) = self.take_unclaimed(worker, rs, re, respect_cap, in_critical) {
                    return Some(c);
                }
            }
        }

        // Nothing unclaimed anywhere: steal the back half of the biggest live
        // claim. Without this, the slowest single request in flight decides
        // when the whole transfer ends.
        self.steal(worker)
    }

    /// Release `worker`'s claim. `reached` is the first byte it did *not*
    /// deliver; `[reached, end]` goes back on the unclaimed list so another
    /// worker (or a retry) picks it up.
    pub fn finish(&mut self, worker: usize, reached: u64) {
        let Some(pos) = self.live.iter().position(|l| l.worker == worker) else {
            return;
        };
        let live = self.live.remove(pos);
        let end = live.end.load(Ordering::Relaxed);
        if reached <= end {
            self.insert_unclaimed(reached, end);
        }
    }

    /// Carve a claim out of `[rs, re]`.
    fn take_unclaimed(
        &mut self,
        worker: usize,
        rs: u64,
        re: u64,
        respect_cap: bool,
        in_critical: bool,
    ) -> Option<Claim> {
        // First unclaimed interval that intersects the region.
        let (idx, seg_start) = self.unclaimed.iter().enumerate().find_map(|(i, &(s, e))| {
            if e < rs || s > re {
                None
            } else {
                Some((i, s.max(rs)))
            }
        })?;
        let (_, seg_end_full) = self.unclaimed[idx];
        let seg_end = seg_end_full.min(re);

        let volume = self.volume_of(seg_start);
        if respect_cap && self.live_in_volume(volume) >= self.max_per_volume {
            return None;
        }

        // Clip to the volume that contains `seg_start` — one claim, one URL.
        let vol_end = self.volume_end(seg_start).unwrap_or(seg_end);
        let hard_end = seg_end.min(vol_end);

        let want = self.claim_len(in_critical && self.near_read_head(seg_start));
        let end = hard_end.min(seg_start.saturating_add(want.saturating_sub(1)));

        self.subtract(seg_start, end);
        let claim = Claim {
            start: seg_start,
            volume,
            end: Arc::new(AtomicU64::new(end)),
            cursor: Arc::new(AtomicU64::new(seg_start)),
        };
        self.live.push(Live {
            worker,
            volume,
            end: Arc::clone(&claim.end),
            cursor: Arc::clone(&claim.cursor),
        });
        Some(claim)
    }

    /// Take the back half of the live claim with the most work left.
    fn steal(&mut self, worker: usize) -> Option<Claim> {
        let victim = self
            .live
            .iter()
            .filter(|l| l.worker != worker && l.remaining() > MIN_CLAIM * 2)
            .max_by_key(|l| l.remaining())?;

        let v_end = victim.end.load(Ordering::Relaxed);
        let remaining = victim.remaining();
        // Same split point gopeed uses: halve what's *left*, not the original
        // claim, so a victim that is nearly done isn't cut at a stale offset.
        let split_at = v_end - remaining / 2;
        if split_at >= v_end {
            return None;
        }
        let new_start = split_at + 1;
        let volume = self.volume_of(new_start);
        // A claim never spans volumes, so the stolen tail is in the victim's
        // volume by construction; clip anyway to stay honest if that ever
        // changes.
        let new_end = self.volume_end(new_start).unwrap_or(v_end).min(v_end);
        if new_end < new_start {
            return None;
        }
        victim.end.store(split_at, Ordering::Relaxed);

        let claim = Claim {
            start: new_start,
            volume,
            end: Arc::new(AtomicU64::new(new_end)),
            cursor: Arc::new(AtomicU64::new(new_start)),
        };
        self.live.push(Live {
            worker,
            volume,
            end: Arc::clone(&claim.end),
            cursor: Arc::clone(&claim.cursor),
        });
        Some(claim)
    }

    /// True when `offset` sits in the stretch immediately ahead of the reader,
    /// where a short request measurably shortens time-to-first-byte.
    ///
    /// Only that stretch gets the small `head_claim`. Sizing the *whole*
    /// critical window small costs real throughput for no latency benefit: the
    /// reader can't consume byte 30 MiB-ahead any sooner than the bytes before
    /// it, so a short claim out there just buys another round of per-request
    /// overhead. Measured against a high-per-request-latency relay, uniformly
    /// small window claims held sustained throughput to ~1.7 MB/s where
    /// auto-sized ones reached ~12 MB/s.
    fn near_read_head(&self, offset: u64) -> bool {
        let head = match self.strategy {
            Strategy::Latency { head_claim, .. } => head_claim.max(1),
            Strategy::Throughput => return false,
        };
        offset < self.read_head.saturating_add(head)
    }

    /// How long the next claim should be.
    fn claim_len(&self, in_critical: bool) -> u64 {
        if in_critical {
            let head = match self.strategy {
                Strategy::Latency { head_claim, .. } => head_claim.max(1),
                Strategy::Throughput => MIN_CLAIM,
            };
            return match self.split_cap {
                Some(cap) => head.min(cap),
                None => head,
            };
        }
        // Automatic sizing: split what's left evenly among the workers that
        // still need something to do, so they finish together instead of the
        // early claimants hogging the file.
        //
        // Dividing by *free slots* rather than `max_threads` is what makes the
        // first round come out even: with 4 threads the claims are 100/4,
        // 75/3, 50/2, 25/1 — four equal quarters. Dividing by `max_threads`
        // throughout would shrink each successive claim geometrically and never
        // cover the request.
        //
        // Later, once every slot is busy, the divisor reaches 1 and a
        // returning worker sweeps up all remaining unclaimed work in one
        // claim. That is deliberate and self-correcting: the next worker to
        // free up steals half of it. Same dynamic as gopeed's
        // "one connection owns the tail, others split it".
        let free_slots = self.max_threads.saturating_sub(self.live.len()).max(1) as u64;
        let even = self
            .unclaimed_bytes()
            .div_ceil(free_slots)
            .max(MIN_CLAIM);
        // The probed ceiling caps the even split: `even` says how much work a
        // worker *should* take to finish alongside its peers, `auto_target`
        // says how much this upstream has actually proven it can deliver in one
        // request.
        let auto = even.min(self.auto_target.max(MIN_CLAIM));
        match self.split_cap {
            Some(cap) => auto.min(cap).max(1),
            None => auto,
        }
    }

    fn live_in_volume(&self, volume: usize) -> usize {
        self.live.iter().filter(|l| l.volume == volume).count()
    }

    fn volume_of(&self, offset: u64) -> usize {
        self.volumes
            .as_deref()
            .and_then(|vols| {
                vols.iter()
                    .position(|v| v.size > 0 && offset >= v.offset && offset < v.offset + v.size)
            })
            .unwrap_or(0)
    }

    /// Inclusive last byte of the volume containing `offset`.
    fn volume_end(&self, offset: u64) -> Option<u64> {
        self.volumes.as_deref().and_then(|vols| {
            vols.iter()
                .find(|v| v.size > 0 && offset >= v.offset && offset < v.offset + v.size)
                .map(|v| v.offset + v.size - 1)
        })
    }

    /// Remove `[s, e]` from the unclaimed set.
    fn subtract(&mut self, s: u64, e: u64) {
        if s > e {
            return;
        }
        let mut out: Vec<(u64, u64)> = Vec::with_capacity(self.unclaimed.len() + 1);
        for &(cs, ce) in &self.unclaimed {
            if ce < s || cs > e {
                out.push((cs, ce));
                continue;
            }
            if cs < s {
                out.push((cs, s - 1));
            }
            if ce > e {
                out.push((e + 1, ce));
            }
        }
        self.unclaimed = out;
    }

    /// Add `[s, e]` back, merging with neighbours so the list stays minimal.
    fn insert_unclaimed(&mut self, s: u64, e: u64) {
        if s > e {
            return;
        }
        self.unclaimed.push((s, e));
        self.unclaimed.sort_unstable_by_key(|&(a, _)| a);
        let mut merged: Vec<(u64, u64)> = Vec::with_capacity(self.unclaimed.len());
        for &(cs, ce) in &self.unclaimed {
            match merged.last_mut() {
                // `cs <= last.1 + 1` fuses both overlapping and merely
                // adjacent runs, so repeated give-backs can't fragment the
                // list into millions of one-byte entries.
                Some(last) if cs <= last.1.saturating_add(1) => {
                    last.1 = last.1.max(ce);
                }
                _ => merged.push((cs, ce)),
            }
        }
        self.unclaimed = merged;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vol(offset: u64, size: u64) -> VolumeMeta {
        VolumeMeta {
            urls: vec!["u".to_string()],
            offset,
            size,
        }
    }

    fn sched(strategy: Strategy, threads: usize, cap: Option<u64>) -> Scheduler {
        Scheduler::new(0, 100 * MIN_CLAIM - 1, strategy, None, threads, threads, cap, &[])
    }

    /// Lift the slow-start ceiling as far as `bytes` (or as far as
    /// [`MAX_AUTO_CLAIM`] allows) so a test can exercise the even-split /
    /// volume-clip / steal paths without automatic sizing being the binding
    /// constraint. Stops when growth saturates rather than spinning.
    fn warm_to(s: &mut Scheduler, bytes: u64) {
        while s.auto_target() < bytes {
            let before = s.auto_target();
            s.note_claim_outcome(true);
            if s.auto_target() == before {
                break;
            }
        }
    }

    #[test]
    fn auto_sizing_starts_modest_and_grows_on_success() {
        // The probe exists because "even split of the request" is a request
        // that never returns against a staging relay. Start small, earn the
        // right to go bigger.
        let mut s = sched(Strategy::Throughput, 4, None);
        let first = s.claim(0).unwrap();
        assert_eq!(
            first.end() - first.start + 1,
            INITIAL_AUTO_CLAIM,
            "first claim must be the probe size, not an even split",
        );
        s.finish(0, first.end() + 1);
        s.note_claim_outcome(true);
        let second = s.claim(0).unwrap();
        assert_eq!(second.end() - second.start + 1, 2 * INITIAL_AUTO_CLAIM);
    }

    #[test]
    fn auto_sizing_halves_on_failure_and_floors_at_min_claim() {
        let mut s = sched(Strategy::Throughput, 4, None);
        s.note_claim_outcome(false);
        assert_eq!(s.auto_target(), INITIAL_AUTO_CLAIM / 2);
        // Keep failing: it must bottom out rather than collapse to zero and
        // produce a request storm.
        for _ in 0..20 {
            s.note_claim_outcome(false);
        }
        assert_eq!(s.auto_target(), MIN_CLAIM);
        let c = s.claim(0).unwrap();
        assert_eq!(c.end() - c.start + 1, MIN_CLAIM);
    }

    #[test]
    fn auto_sizing_converges_to_an_even_split() {
        // Once the upstream has proven itself, claims stop growing at the point
        // where the workers would finish together — no reason to go past that.
        let mut s = sched(Strategy::Throughput, 4, None);
        warm_to(&mut s, 1000 * MIN_CLAIM);
        let a = s.claim(0).unwrap();
        assert_eq!(a.end() - a.start + 1, 25 * MIN_CLAIM, "capped by even split");
    }

    #[test]
    fn throughput_splits_request_across_threads() {
        // 100 MiB over 4 workers → ~25 MiB each, covering the whole file with
        // no leftovers once all four have claimed.
        let mut s = sched(Strategy::Throughput, 4, None);
        warm_to(&mut s, 100 * MIN_CLAIM);
        let a = s.claim(0).unwrap();
        assert_eq!(a.start, 0);
        assert_eq!(a.end() - a.start + 1, 25 * MIN_CLAIM);
        let b = s.claim(1).unwrap();
        assert_eq!(b.start, a.end() + 1);
        let c = s.claim(2).unwrap();
        let d = s.claim(3).unwrap();
        assert_eq!(d.end(), 100 * MIN_CLAIM - 1);
        assert_eq!(s.unclaimed_bytes(), 0);
        // Claims tile the request exactly.
        assert_eq!(b.end() + 1, c.start);
        assert_eq!(c.end() + 1, d.start);
    }

    #[test]
    fn explicit_split_cap_is_honoured() {
        let mut s = sched(Strategy::Throughput, 4, Some(4 * MIN_CLAIM));
        warm_to(&mut s, 100 * MIN_CLAIM);
        let a = s.claim(0).unwrap();
        assert_eq!(a.end() - a.start + 1, 4 * MIN_CLAIM);
    }

    #[test]
    fn auto_sizing_stops_growing_at_the_ceiling() {
        // Growth is bounded so a single claim can't stall the ordered reader for
        // minutes, and so halve-on-failure stays responsive.
        let mut s = Scheduler::new(
            0,
            10_000 * MIN_CLAIM,
            Strategy::latency_default(),
            None,
            1,
            1,
            None,
            &[],
        );
        for _ in 0..40 {
            s.note_claim_outcome(true);
        }
        assert_eq!(s.auto_target(), MAX_AUTO_CLAIM);
        // One failure must produce a real step down, not a rounding error off
        // some astronomical value.
        s.note_claim_outcome(false);
        assert_eq!(s.auto_target(), MAX_AUTO_CLAIM / 2);
    }

    #[test]
    fn the_claim_ceiling_applies_to_downloads_too() {
        // Regression guard. Lifting this for throughput mode — reasoning that a
        // download has no ordered reader to starve — stalled a real download at
        // 176 MB in 180 s where the ceiling moved 768 MB. The ceiling also
        // guards against upstreams whose per-request latency scales with range
        // size, which is order-agnostic.
        let mut s = Scheduler::new(
            0,
            10_000 * MIN_CLAIM - 1,
            Strategy::Throughput,
            None,
            4,
            4,
            None,
            &[],
        );
        for _ in 0..40 {
            s.note_claim_outcome(true);
        }
        assert_eq!(s.auto_target(), MAX_AUTO_CLAIM);
        let c = s.claim(0).unwrap();
        assert_eq!(c.end() - c.start + 1, MAX_AUTO_CLAIM);
    }

    #[test]
    fn unlimited_split_can_exceed_any_fixed_size() {
        // The point of "no max split" is that the *configuration* stops
        // dictating the size — a warmed-up worker claims far more than the
        // 5 MiB that used to be the default.
        let mut s = sched(Strategy::Throughput, 1, None);
        warm_to(&mut s, MAX_AUTO_CLAIM);
        let a = s.claim(0).unwrap();
        assert_eq!(a.start, 0);
        let len = a.end() - a.start + 1;
        assert_eq!(len, MAX_AUTO_CLAIM);
        assert!(len > 5 * 1024 * 1024 * 10, "must dwarf the old 5 MiB default");
    }

    #[test]
    fn latency_prioritizes_the_critical_window_with_small_claims() {
        let mut s = Scheduler::new(
            0,
            100 * MIN_CLAIM - 1,
            Strategy::Latency {
                critical_window: 8 * MIN_CLAIM,
                head_claim: 2 * MIN_CLAIM,
            },
            None,
            8,
            8,
            None,
            &[],
        );
        warm_to(&mut s, 100 * MIN_CLAIM);
        s.set_read_head(50 * MIN_CLAIM);
        // The claim at the read head is deliberately short: that's the one whose
        // latency the viewer actually feels.
        let a = s.claim(0).unwrap();
        assert_eq!(a.start, 50 * MIN_CLAIM);
        assert_eq!(a.end() - a.start + 1, 2 * MIN_CLAIM);
        // Beyond the head, claims are sized for throughput — a short request out
        // there buys latency nobody can observe.
        let b = s.claim(1).unwrap();
        assert_eq!(b.start, 52 * MIN_CLAIM);
        assert!(
            b.end() - b.start + 1 > 2 * MIN_CLAIM,
            "beyond the head, claims must be throughput-sized, got {}",
            b.end() - b.start + 1,
        );
        // Both claims are ahead of the reader. (How *many* workers the window
        // can keep ahead is covered by
        // `latency_window_is_at_least_as_deep_as_the_worker_pool`; here the
        // forward region is only 50 MiB, so it legitimately runs out.)
        assert!(a.start >= 50 * MIN_CLAIM && b.start >= 50 * MIN_CLAIM);
    }

    #[test]
    fn latency_window_is_at_least_as_deep_as_the_worker_pool() {
        // A window shallower than `max_threads × auto_target` leaves most
        // workers with nothing prioritized, so they prefetch far-away regions
        // and starve the read head. The window must scale with the pool.
        let threads = 8;
        let mut s = Scheduler::new(
            0,
            1000 * MIN_CLAIM - 1,
            Strategy::Latency {
                // Deliberately tiny: the pool depth must win.
                critical_window: MIN_CLAIM,
                head_claim: MIN_CLAIM,
            },
            None,
            threads,
            threads,
            None,
            &[],
        );
        warm_to(&mut s, MAX_AUTO_CLAIM);
        s.set_read_head(500 * MIN_CLAIM);
        for w in 0..threads {
            let c = s.claim(w).unwrap();
            assert!(
                c.start >= 500 * MIN_CLAIM,
                "worker {} was pushed out of the window to {}",
                w,
                c.start,
            );
        }
    }

    #[test]
    fn latency_prefetches_elsewhere_once_the_window_is_full() {
        // One worker, shallow window → it fills quickly, and the spare capacity
        // then goes to the earliest un-fetched gap rather than idling.
        let mut s = Scheduler::new(
            0,
            100 * MIN_CLAIM - 1,
            Strategy::Latency {
                critical_window: 4 * MIN_CLAIM,
                head_claim: MIN_CLAIM,
            },
            None,
            1,
            4,
            None,
            &[],
        );
        s.set_read_head(50 * MIN_CLAIM);
        // Window = max(4 MiB, 1 worker × 8 MiB probe) = 8 MiB.
        let a = s.claim(0).unwrap();
        assert_eq!(a.start, 50 * MIN_CLAIM);
        let b = s.claim(1).unwrap();
        assert_eq!(b.start, 51 * MIN_CLAIM);
        assert_eq!(b.end(), 58 * MIN_CLAIM - 1, "fills out the window");
        // Window exhausted → opportunistic prefetch from the earliest gap.
        let c = s.claim(2).unwrap();
        assert_eq!(c.start, 0, "prefetch starts at the earliest gap");
    }

    #[test]
    fn latency_window_follows_the_read_head() {
        let mut s = Scheduler::new(
            0,
            100 * MIN_CLAIM - 1,
            Strategy::Latency {
                critical_window: 4 * MIN_CLAIM,
                head_claim: MIN_CLAIM,
            },
            None,
            8,
            8,
            None,
            &[],
        );
        let a = s.claim(0).unwrap();
        assert_eq!(a.start, 0);
        // A seek moves the window; the next claim must chase it, not continue
        // sequentially from the abandoned position.
        s.set_read_head(70 * MIN_CLAIM);
        let b = s.claim(1).unwrap();
        assert_eq!(b.start, 70 * MIN_CLAIM);
    }

    #[test]
    fn claims_never_span_a_volume_boundary() {
        let vols = Arc::new(vec![vol(0, 10 * MIN_CLAIM), vol(10 * MIN_CLAIM, 10 * MIN_CLAIM)]);
        let mut s = Scheduler::new(
            0,
            20 * MIN_CLAIM - 1,
            Strategy::Throughput,
            Some(vols),
            1,
            4,
            None,
            &[],
        );
        warm_to(&mut s, 100 * MIN_CLAIM);
        // A single worker would otherwise claim all 20 MiB; the volume
        // boundary clips it at 10 MiB.
        let a = s.claim(0).unwrap();
        assert_eq!(a.start, 0);
        assert_eq!(a.end(), 10 * MIN_CLAIM - 1);
        assert_eq!(a.volume, 0);
        let b = s.claim(0).unwrap();
        assert_eq!(b.start, 10 * MIN_CLAIM);
        assert_eq!(b.volume, 1);
    }

    #[test]
    fn per_volume_cap_overflows_inside_the_critical_window() {
        // The regression this whole redesign exists for. One volume holds the
        // critical window, `max_per_volume` is 2, and there are 8 threads. The
        // old scheduler sent workers 3..8 off to prefetch distant volumes and
        // left the window at 2-way concurrency. Overflow must trigger here.
        let vols = Arc::new(vec![
            vol(0, 10 * MIN_CLAIM),
            vol(10 * MIN_CLAIM, 10 * MIN_CLAIM),
            vol(20 * MIN_CLAIM, 10 * MIN_CLAIM),
        ]);
        let mut s = Scheduler::new(
            0,
            30 * MIN_CLAIM - 1,
            Strategy::Latency {
                critical_window: 8 * MIN_CLAIM,
                head_claim: MIN_CLAIM,
            },
            Some(vols),
            8,
            2,
            None,
            &[],
        );
        let claims: Vec<Claim> = (0..4).filter_map(|w| s.claim(w)).collect();
        assert_eq!(claims.len(), 4);
        assert!(
            claims.iter().all(|c| c.volume == 0),
            "all four must stay in the window's volume, got {:?}",
            claims.iter().map(|c| c.volume).collect::<Vec<_>>(),
        );
    }

    #[test]
    fn steals_the_back_half_when_nothing_is_unclaimed() {
        // Request small enough that a warmed-up worker claims all of it, so the
        // only way a second worker can help is by stealing.
        let mut s = Scheduler::new(
            0,
            32 * MIN_CLAIM - 1,
            Strategy::Throughput,
            None,
            1,
            1,
            None,
            &[],
        );
        warm_to(&mut s, 32 * MIN_CLAIM);
        let victim = s.claim(0).unwrap();
        assert_eq!(s.unclaimed_bytes(), 0, "one worker took everything");
        let original_end = victim.end();

        let thief = s.claim(1).unwrap();
        assert_eq!(victim.end(), original_end / 2, "victim was cut in half");
        assert_eq!(thief.start, victim.end() + 1);
        assert_eq!(thief.end(), original_end);
        // The victim must notice its shrunken end rather than overrunning.
        victim.advance_to(victim.end() + 1);
        assert!(victim.is_complete());
    }

    #[test]
    fn stealing_accounts_for_delivered_bytes() {
        // A victim that is 90% done should surrender roughly half of what's
        // *left*, not half of its original span (which is already delivered).
        let mut s = Scheduler::new(
            0,
            32 * MIN_CLAIM - 1,
            Strategy::Throughput,
            None,
            1,
            1,
            None,
            &[],
        );
        warm_to(&mut s, 32 * MIN_CLAIM);
        let victim = s.claim(0).unwrap();
        let end = victim.end();
        victim.advance_to(end - 10 * MIN_CLAIM + 1); // 10 MiB left
        let thief = s.claim(1).unwrap();
        assert_eq!(thief.end(), end);
        assert_eq!(victim.end(), end - 5 * MIN_CLAIM);
        assert_eq!(thief.start, end - 5 * MIN_CLAIM + 1);
    }

    #[test]
    fn refuses_to_steal_slivers() {
        let mut s = Scheduler::new(0, MIN_CLAIM, Strategy::Throughput, None, 1, 1, None, &[]);
        let _only = s.claim(0).unwrap();
        // Remaining work is under the steal threshold — splitting it would
        // just add a request without adding parallelism.
        assert!(s.claim(1).is_none());
    }

    #[test]
    fn already_staged_ranges_are_not_refetched() {
        let mut s = Scheduler::new(
            0,
            10 * MIN_CLAIM - 1,
            Strategy::Throughput,
            None,
            4,
            4,
            None,
            &[(0, 4 * MIN_CLAIM - 1)],
        );
        assert_eq!(s.unclaimed_bytes(), 6 * MIN_CLAIM);
        let a = s.claim(0).unwrap();
        assert_eq!(a.start, 4 * MIN_CLAIM, "resumes past the staged prefix");
    }

    #[test]
    fn finish_returns_undelivered_tail_to_the_pool() {
        let mut s = sched(Strategy::Throughput, 4, None);
        warm_to(&mut s, 100 * MIN_CLAIM);
        let a = s.claim(0).unwrap();
        let end = a.end();
        // Worker died after delivering 1 MiB.
        s.finish(0, a.start + MIN_CLAIM);
        assert_eq!(s.unclaimed_bytes(), 100 * MIN_CLAIM - MIN_CLAIM);
        // The give-back is reusable and contiguous with what followed it.
        let b = s.claim(1).unwrap();
        assert_eq!(b.start, MIN_CLAIM);
        let _ = end;
    }

    #[test]
    fn finish_after_full_delivery_adds_nothing_back() {
        let mut s = sched(Strategy::Throughput, 4, None);
        warm_to(&mut s, 100 * MIN_CLAIM);
        let a = s.claim(0).unwrap();
        let before = s.unclaimed_bytes();
        s.finish(0, a.end() + 1);
        assert_eq!(s.unclaimed_bytes(), before);
    }

    #[test]
    fn drains_and_then_returns_none() {
        let mut s = Scheduler::new(0, 2 * MIN_CLAIM - 1, Strategy::Throughput, None, 2, 2, None, &[]);
        let a = s.claim(0).unwrap();
        let b = s.claim(1).unwrap();
        assert!(!s.is_drained());
        a.advance_to(a.end() + 1);
        s.finish(0, a.end() + 1);
        b.advance_to(b.end() + 1);
        s.finish(1, b.end() + 1);
        assert!(s.is_drained());
        assert!(s.claim(0).is_none());
    }
}
