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
//! Assignment follows the three-tier escalation from `design.md` §6.1, in cost
//! order — each tier is only reached when the one above it has nothing to give:
//!
//! * **T1 — take.** Claim out of the largest unclaimed gap (aria2's
//!   `getSparseMissingUnusedIndex`), which maximizes the contiguous work bought
//!   by one request. Latency mode overrides the score inside its critical
//!   window: there, nearest-the-reader wins, because the reader's stall is the
//!   thing being minimized.
//! * **T2 — steal.** Nothing unclaimed left: move a live claim's end inward and
//!   take the tail behind it. The victim notices at its next stream item and
//!   retires early, so the handoff copies no bytes twice.
//! * **T3 — hedge.** Nothing left to steal either, and the transfer is in its
//!   tail: deliberately race one in-flight claim, charged against a hard
//!   duplicate-byte budget. Bounded, throughput-mode-only, and the loser is cut
//!   short by the same end-watermark the steal path uses.
//!
//! Two more mechanisms keep a straggling upstream from setting the pace: steal
//! victims are chosen by slowness before size (`design.md` §6.4), and a claim
//! that stops moving entirely is declared dead and re-cut
//! ([`Scheduler::reclaim_stalled`]).

use crate::engine::VolumeMeta;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Absolute floor on a claim, used once the transfer reaches its tail. Below
/// roughly a megabyte the per-request overhead starts to dominate the transfer
/// on every upstream we care about, and on staging-style relays it dominates by
/// orders of magnitude.
pub const MIN_CLAIM: u64 = 1024 * 1024;

/// Working minimum fragment away from the tail (`design.md` §12
/// `min_fragment`).
///
/// The floor is 8 MiB rather than [`MIN_CLAIM`] because a fresh request pays a
/// round trip plus slow start plus whatever the origin spends seeking, and none
/// of that amortizes over a smaller range (research.md §1.3). It binds in two
/// places: how small a claim may be cut, and how much has to be left in a live
/// claim before splitting it buys more parallelism than it costs.
///
/// [`Scheduler::min_frag`] relaxes it back to [`MIN_CLAIM`] in the tail, where
/// the goal flips from "amortize overhead" to "get the last workers busy".
pub const MIN_FRAGMENT: u64 = 8 * 1024 * 1024;

/// Below this much work left, the transfer is in its tail: fragments relax to
/// [`MIN_CLAIM`] and hedging unlocks. `design.md` §12:
/// `max(4 × conns × min_fragment, 64 MiB)`.
pub const ENDGAME_FLOOR: u64 = 64 * 1024 * 1024;

/// A live claim moving slower than this fraction of the median live rate is a
/// straggler, and is preferred as a steal victim (`design.md` §12
/// `straggler_ratio`).
///
/// Stealing rather than killing is deliberate: the straggler keeps its warm
/// congestion window and everything it has already delivered, and simply ends
/// up responsible for less (`design.md` §6.4).
pub const STRAGGLER_RATIO: f64 = 0.35;

/// A claim that has not moved a byte for this long is not slow, it is dead:
/// [`Scheduler::reclaim_stalled`] takes the range back and the caller restarts
/// the worker (`design.md` §12 `dead_conn_window`).
pub const DEAD_CLAIM_WINDOW: Duration = Duration::from_secs(30);

/// Rate samples shorter than this are noise, so a claim younger than it is
/// never judged a straggler (`design.md` §11: 2000 ms or 512 KiB sampling
/// threshold).
const RATE_SAMPLE_WINDOW: Duration = Duration::from_secs(2);
const RATE_SAMPLE_BYTES: u64 = 512 * 1024;

/// Endgame duplicate-byte budget: `min(len / 200, 32 MiB)` — 0.5% of the
/// transfer, capped (`design.md` §12 `dup_budget`). Spent, never refilled.
const DUP_BUDGET_DIVISOR: u64 = 200;
const DUP_BUDGET_MAX: u64 = 32 * 1024 * 1024;

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
/// gopeed's fetcher has with its re-read of `conn.Chunk.remain()`, and the
/// `soft_end` handoff of `design.md` §3.1. It is the cheapest of the three
/// cancellation mechanisms in §4.2 (cost: zero — the victim simply decides it
/// is finished early), so it is the one used for stealing, for shortening a
/// prefetch after a seek, and for cutting an endgame loser short.
#[derive(Debug, Clone)]
pub struct Claim {
    pub start: u64,
    pub volume: usize,
    end: Arc<AtomicU64>,
    cursor: Arc<AtomicU64>,
    /// Bytes the worker has *committed* to writing — advanced before the write
    /// rather than after it, so a thief reading it can never pick a split point
    /// inside a write that is still in progress. `cursor` can't serve this
    /// purpose: it has to stay a lower bound on delivered bytes because retries
    /// resume from it.
    reserved: Arc<AtomicU64>,
}

impl Claim {
    /// Current inclusive end. May shrink between calls.
    pub fn end(&self) -> u64 {
        self.end.load(Ordering::Acquire)
    }

    /// Next byte this claim still owes.
    pub fn cursor(&self) -> u64 {
        self.cursor.load(Ordering::Acquire)
    }

    /// Announce that bytes below `next` are about to be written. Call this
    /// *before* the write; [`Claim::advance_to`] confirms it afterwards.
    pub fn reserve(&self, next: u64) {
        self.reserved.fetch_max(next, Ordering::AcqRel);
    }

    /// Record that everything below `next` has landed in staging.
    pub fn advance_to(&self, next: u64) {
        self.reserved.fetch_max(next, Ordering::AcqRel);
        self.cursor.store(next, Ordering::Release);
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
    /// Where the claim began, for rate accounting.
    start: u64,
    end: Arc<AtomicU64>,
    cursor: Arc<AtomicU64>,
    reserved: Arc<AtomicU64>,
    /// When the claim was handed out, for rate accounting.
    started: Instant,
    /// Cursor and wall-clock at the last [`Scheduler::reclaim_stalled`] sweep,
    /// so "has this moved at all" is answerable without a timer per claim.
    last_cursor: u64,
    last_progress: Instant,
    /// `Some(primary_start)` when this is a speculative endgame copy of another
    /// live claim. A hedge owns no range: it must never hand bytes back to the
    /// unclaimed list, because the claim it is racing still owns them.
    hedge_of: Option<u64>,
}

impl Live {
    fn remaining(&self) -> u64 {
        let end = self.end.load(Ordering::Acquire);
        let cur = self.cursor.load(Ordering::Acquire);
        if cur > end { 0 } else { end - cur + 1 }
    }

    /// Observed bytes per second, or `None` while the sample is too short to
    /// mean anything (`design.md` §11).
    fn rate(&self) -> Option<f64> {
        let elapsed = self.started.elapsed();
        let done = self.cursor.load(Ordering::Acquire).saturating_sub(self.start);
        if elapsed < RATE_SAMPLE_WINDOW && done < RATE_SAMPLE_BYTES {
            return None;
        }
        Some(done as f64 / elapsed.as_secs_f64().max(0.001))
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
    /// How far past `read_head` claims may reach, when set.
    ///
    /// Playback and whole-file caching share one scheduler, and the only thing
    /// that separates them is this bound. Without it, latency mode's fallback
    /// region is the entire request, so a reader that wants 4 MB in the middle
    /// of a file quietly pulls all of it — which makes an explicit "cache the
    /// whole file" action meaningless. `None` (caching) means the whole request
    /// is fair game.
    work_limit: Option<u64>,
    /// Current ceiling for automatically-sized claims, grown on success and
    /// halved on failure. See [`Scheduler::note_claim_outcome`].
    auto_target: u64,
    /// Duplicate bytes the endgame may still spend. Charged on every hedge and
    /// never refilled, so a pathological tail can't turn into a request storm
    /// against the origin (`design.md` §6.1).
    dup_budget: u64,
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
            work_limit: None,
            auto_target: INITIAL_AUTO_CLAIM,
            dup_budget: (req_end.saturating_sub(req_start).saturating_add(1)
                / DUP_BUDGET_DIVISOR)
                .min(DUP_BUDGET_MAX),
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

    /// Bytes of the request still outstanding: unclaimed plus whatever the live
    /// claims still owe. Hedges are copies of ranges a primary already owns, so
    /// they don't count.
    fn remaining_work(&self) -> u64 {
        let live: u64 = self
            .live
            .iter()
            .filter(|l| l.hedge_of.is_none())
            .map(|l| l.remaining())
            .sum();
        self.unclaimed_bytes().saturating_add(live)
    }

    /// Where the tail begins: `max(4 × conns × min_fragment, 64 MiB)`
    /// (`design.md` §12).
    ///
    /// Four rounds of full-width work is the point past which handing out
    /// 8 MiB fragments still keeps every worker busy to the end. Inside it,
    /// the aim changes from amortizing per-request overhead to converging, so
    /// fragments relax and hedging unlocks.
    fn endgame_threshold(&self) -> u64 {
        (self.max_threads as u64)
            .saturating_mul(4)
            .saturating_mul(MIN_FRAGMENT)
            .max(ENDGAME_FLOOR)
    }

    fn in_tail(&self) -> bool {
        self.remaining_work() < self.endgame_threshold()
    }

    /// Smallest fragment worth cutting right now.
    ///
    /// [`MIN_FRAGMENT`] is a Policy A constant (`design.md` §6.1) and applies
    /// only to downloads: it relaxes to [`MIN_CLAIM`] once the transfer reaches
    /// its tail (§6.1 T2'), where the goal flips from amortizing per-request
    /// overhead to getting the last workers busy.
    ///
    /// Playback is Policy B and sizes work from the reader's deadline instead
    /// (§7.3, where the chunk nearest the play head is *deliberately* as small
    /// as 64 KiB). Holding it to an 8 MiB floor would forbid exactly the splits
    /// that shorten time-to-first-byte, so it keeps the [`MIN_CLAIM`] floor.
    fn min_frag(&self) -> u64 {
        match self.strategy {
            Strategy::Latency { .. } => MIN_CLAIM,
            Strategy::Throughput if self.in_tail() => MIN_CLAIM,
            Strategy::Throughput => MIN_FRAGMENT,
        }
    }

    /// True when every byte of the request is either staged or being fetched.
    pub fn is_drained(&self) -> bool {
        self.unclaimed.is_empty() && self.live.iter().all(|l| l.remaining() == 0)
    }

    /// Move the reader position; re-derives the priority window.
    pub fn set_read_head(&mut self, offset: u64) {
        self.read_head = offset.clamp(self.req_start, self.req_end.saturating_add(1));
    }
    /// Switch scheduling policy without rebuilding live claims. A task-level
    /// cache coordinator uses throughput mode while only background caching is
    /// active, then flips to latency mode as soon as a playback reader seeks.
    /// Existing requests are allowed to finish; every newly freed worker is
    /// immediately assigned from the reader's critical window.
    pub fn set_strategy(&mut self, strategy: Strategy) {
        self.strategy = strategy;
    }

    /// Bound how far past the read head workers may claim, or lift the bound
    /// with `None`. See [`Scheduler::work_limit`].
    pub fn set_work_limit(&mut self, limit: Option<u64>) {
        self.work_limit = limit.filter(|value| *value > 0);
    }

    /// Last byte workers may currently claim.
    fn horizon(&self) -> u64 {
        match self.work_limit {
            Some(limit) => self
                .read_head
                .saturating_add(limit.saturating_sub(1))
                .min(self.req_end),
            None => self.req_end,
        }
    }

    /// First byte workers may currently claim. A bounded (playback-only) pool
    /// works strictly forward from the reader: bytes behind the read head were
    /// either already delivered or skipped by a seek, so fetching them is not
    /// what anybody is waiting for. Caching lifts the bound and sweeps the file
    /// from the start.
    fn floor(&self) -> u64 {
        match self.work_limit {
            Some(_) => self.read_head.max(self.req_start),
            None => self.req_start,
        }
    }

    /// Take back the live claims the reader won't reach soon, returning the
    /// workers whose requests the caller should abort.
    ///
    /// A seek makes every in-flight request outside the new critical window
    /// uninteresting: waiting for a 64 MiB throughput claim to come back before
    /// the pool can serve the reader is exactly the stall latency mode exists to
    /// avoid. Reclaiming at each claim's *cursor* keeps the bytes that already
    /// landed — the earlier approach of aborting the whole pool and rebuilding
    /// from the on-disk bitmap discarded everything still in flight, which
    /// measured as roughly a fifth of a transfer being fetched twice.
    pub fn reclaim_outside_window(&mut self, window: u64) -> Vec<usize> {
        let hi = self.read_head.saturating_add(window.max(1));
        let mut reclaimed = Vec::new();
        let mut i = 0;
        while i < self.live.len() {
            let cursor = self.live[i].cursor.load(Ordering::Acquire);
            let end = self.live[i].end.load(Ordering::Acquire);
            // Keep claims that are finished (the worker is about to report in)
            // or that overlap the window the reader cares about.
            let done = cursor > end;
            let overlaps = cursor < hi && end >= self.read_head;
            if done || overlaps {
                i += 1;
                continue;
            }
            let live = self.live.remove(i);
            reclaimed.push(live.worker);
            // A hedge owns nothing: the claim it was racing still holds this
            // range, so handing it back would queue it for a second fetch.
            if live.hedge_of.is_none() {
                self.insert_unclaimed(cursor, end);
            }
        }
        reclaimed
    }

    /// Declare stopped claims dead, hand their ranges back, and return the
    /// workers whose requests the caller should abort and restart.
    ///
    /// `design.md` §6.4 separates two failure modes that look alike from the
    /// outside. A *slow* claim is handled by stealing — it keeps its warm
    /// connection and the bytes it already delivered, and simply owes less. A
    /// *stopped* one (nothing at all for [`DEAD_CLAIM_WINDOW`], the "30 s under
    /// 1 B/s" rule, which also covers aria2's "owner is idle and has written
    /// nothing") has no bytes to protect and no evidence its connection will
    /// ever produce any, so the cheap fix is the wrong one: re-cut it.
    ///
    /// Intended to be driven from the same ~1 Hz tick that samples throughput;
    /// progress is tracked between calls rather than with a timer per claim.
    pub fn reclaim_stalled(&mut self) -> Vec<usize> {
        let now = Instant::now();
        let mut dead = Vec::new();
        let mut i = 0;
        while i < self.live.len() {
            let cursor = self.live[i].cursor.load(Ordering::Acquire);
            let end = self.live[i].end.load(Ordering::Acquire);
            if cursor > self.live[i].last_cursor || cursor > end {
                // Moving, or finished and about to report in.
                self.live[i].last_cursor = cursor;
                self.live[i].last_progress = now;
                i += 1;
                continue;
            }
            if now.duration_since(self.live[i].last_progress) < DEAD_CLAIM_WINDOW {
                i += 1;
                continue;
            }
            let live = self.live.remove(i);
            dead.push(live.worker);
            if live.hedge_of.is_none() {
                self.insert_unclaimed(cursor, end);
            }
        }
        dead
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

    #[cfg(test)]
    fn unclaimed_ranges(&self) -> &[(u64, u64)] {
        &self.unclaimed
    }

    /// Hand `worker` its next claim, or `None` when there is nothing left to
    /// do — no unclaimed work, no live claim big enough to split, and no
    /// endgame budget left.
    ///
    /// The three tiers of `design.md` §6.1, in cost order: take, then steal,
    /// then (in the tail only) hedge.
    pub fn claim(&mut self, worker: usize) -> Option<Claim> {
        // Regions in priority order. Latency mode looks in the critical window
        // first; both modes then fall back to everything up to the horizon,
        // which equals the whole request unless a playback-only coordinator has
        // bounded how far ahead the pool may run.
        let horizon = self.horizon();
        let floor = self.floor();
        let regions: Vec<(u64, u64)> = match self.strategy {
            Strategy::Throughput => vec![(floor, horizon)],
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
                let hi = self.read_head.saturating_add(window - 1).min(horizon);
                if self.read_head <= hi {
                    vec![(self.read_head, hi), (floor, horizon)]
                } else {
                    vec![(floor, horizon)]
                }
            }
        };

        // T1: something unclaimed. Cheapest tier and the only one that adds no
        // risk of duplicate work, so it is exhausted before anything else.
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

        // T2: nothing unclaimed anywhere — steal the back half of a live claim.
        // Without this, the slowest single request in flight decides when the
        // whole transfer ends.
        if let Some(c) = self.steal(worker) {
            return Some(c);
        }

        // T3: nothing left to split either. In the tail, and only for a
        // download (playback answers a starved reader with its critical window,
        // not with duplicate bytes), race an in-flight claim on a budget.
        if matches!(self.strategy, Strategy::Throughput) && self.in_tail() {
            return self.hedge(worker);
        }
        None
    }

    /// Release `worker`'s claim. `reached` is the first byte it did *not*
    /// deliver; `[reached, end]` goes back on the unclaimed list so another
    /// worker (or a retry) picks it up.
    pub fn finish(&mut self, worker: usize, reached: u64) {
        let Some(pos) = self.live.iter().position(|l| l.worker == worker) else {
            return;
        };
        let live = self.live.remove(pos);
        let end = live.end.load(Ordering::Acquire);
        let complete = reached > end;
        match live.hedge_of {
            // A hedge owns no range — the primary it raced does. Handing bytes
            // back here would schedule a third fetch of the same range.
            Some(primary_start) => {
                if complete {
                    self.cut_short(|l| l.hedge_of.is_none() && l.start == primary_start);
                }
            }
            None => {
                if complete {
                    // Won the race (or was never in one). Any hedge still
                    // fetching this range is pure waste now.
                    self.cut_short(|l| l.hedge_of == Some(live.start));
                } else {
                    self.insert_unclaimed(reached, end);
                }
            }
        }
    }

    /// Tell the first matching live claim it is already finished, by pulling
    /// its end below its cursor. The worker sees it at its next stream item and
    /// retires — the zero-cost cancellation of `design.md` §4.2, which beats
    /// resetting the stream because the connection stays warm and poolable.
    fn cut_short(&mut self, matches: impl Fn(&Live) -> bool) {
        let Some(loser) = self.live.iter().find(|l| matches(l)) else {
            return;
        };
        let cursor = loser.cursor.load(Ordering::Acquire);
        loser
            .end
            .fetch_min(cursor.saturating_sub(1), Ordering::AcqRel);
    }

    /// Carve a claim out of `[rs, re]`.
    ///
    /// Which gap it comes from is the scoring decision of `design.md` §1:
    ///
    /// * Inside latency mode's critical window, the first gap wins — that is
    ///   the one nearest the reader, and the reader's stall is what the window
    ///   exists to minimize. A capped volume there blocks the pass entirely so
    ///   the caller's second, cap-ignoring pass can overflow *into the volume
    ///   the reader is actually waiting on*.
    /// * Everywhere else the largest gap wins (aria2
    ///   `getSparseMissingUnusedIndex`): one request should buy as much
    ///   contiguous work as possible, and a capped volume is simply skipped in
    ///   favour of the next-largest gap, which by construction lives on a
    ///   different URL and so is real added parallelism.
    ///
    /// aria2 additionally starts at the *midpoint* of the winning gap, because
    /// its segments grow forward until they collide with someone else's. Here
    /// the range is subtracted at hand-out, so collisions are impossible and
    /// taking from the front is strictly better: it leaves one gap instead of
    /// two and keeps the completed prefix contiguous for the ordered reader.
    fn take_unclaimed(
        &mut self,
        worker: usize,
        rs: u64,
        re: u64,
        respect_cap: bool,
        in_critical: bool,
    ) -> Option<Claim> {
        let idx = self.pick_gap(rs, re, respect_cap, in_critical)?;
        let (gap_start, gap_end) = self.unclaimed[idx];
        let seg_start = gap_start.max(rs);
        let seg_end = gap_end.min(re);

        let volume = self.volume_of(seg_start);
        // Clip to the volume that contains `seg_start` — one claim, one URL.
        let vol_end = self.volume_end(seg_start).unwrap_or(seg_end);
        let hard_end = seg_end.min(vol_end);

        let want = self.claim_len(in_critical && self.near_read_head(seg_start));
        let end = hard_end.min(seg_start.saturating_add(want.saturating_sub(1)));

        self.subtract(seg_start, end);
        Some(self.register(worker, seg_start, end, volume, None))
    }

    /// Index of the unclaimed interval this claim should come out of.
    fn pick_gap(&self, rs: u64, re: u64, respect_cap: bool, nearest_wins: bool) -> Option<usize> {
        let mut best: Option<(usize, u64)> = None;
        for (i, &(s, e)) in self.unclaimed.iter().enumerate() {
            if e < rs || s > re {
                continue;
            }
            let seg_start = s.max(rs);
            if nearest_wins {
                // `unclaimed` is sorted, so the first hit is the nearest one.
                let volume = self.volume_of(seg_start);
                if respect_cap && self.live_in_volume(volume) >= self.max_per_volume {
                    return None;
                }
                return Some(i);
            }
            let volume = self.volume_of(seg_start);
            if respect_cap && self.live_in_volume(volume) >= self.max_per_volume {
                continue;
            }
            let score = e.min(re) - seg_start + 1;
            if best.is_none_or(|(_, b)| score > b) {
                best = Some((i, score));
            }
        }
        best.map(|(i, _)| i)
    }

    /// Record a handed-out range and build the worker's handle for it.
    fn register(
        &mut self,
        worker: usize,
        start: u64,
        end: u64,
        volume: usize,
        hedge_of: Option<u64>,
    ) -> Claim {
        let now = Instant::now();
        let claim = Claim {
            start,
            volume,
            end: Arc::new(AtomicU64::new(end)),
            cursor: Arc::new(AtomicU64::new(start)),
            reserved: Arc::new(AtomicU64::new(start)),
        };
        self.live.push(Live {
            worker,
            volume,
            start,
            end: Arc::clone(&claim.end),
            cursor: Arc::clone(&claim.cursor),
            reserved: Arc::clone(&claim.reserved),
            started: now,
            last_cursor: start,
            last_progress: now,
            hedge_of,
        });
        claim
    }

    /// T2 — take the back half of a live claim (`design.md` §3.1
    /// `steal_far_half`).
    ///
    /// The victim is not interrupted and loses nothing it has already fetched:
    /// its end moves inward, and at its next stream item it simply decides it
    /// is done. That is why stealing, not killing, is the answer to a slow
    /// connection — a kill throws away a warm congestion window and every byte
    /// in flight (`design.md` §6.4).
    fn steal(&mut self, worker: usize) -> Option<Claim> {
        let min_frag = self.min_frag();
        // Splitting has to leave both halves worth a request of their own.
        let pos = self.pick_victim(worker, min_frag.saturating_mul(2))?;
        let victim = &self.live[pos];

        let v_end = victim.end.load(Ordering::Acquire);
        let remaining = victim.remaining();
        // Same split point gopeed uses: halve what's *left*, not the original
        // claim, so a victim that is nearly done isn't cut at a stale offset.
        // (`design.md` writes this as `max(written_hwm, midpoint)`, which hands
        // the thief more than half of the remainder whenever the victim is less
        // than half done; halving the remainder balances the two.)
        let split_at = v_end - remaining / 2;
        if split_at >= v_end {
            return None;
        }
        victim.end.store(split_at, Ordering::Release);

        // The victim clamps each write to the end it read just before writing,
        // so it may already have committed to bytes past the split point. Start
        // after whatever it reserved rather than at the split, and the handoff
        // transfers zero bytes twice.
        let new_start = split_at
            .saturating_add(1)
            .max(victim.reserved.load(Ordering::Acquire));
        if new_start > v_end {
            return None;
        }
        let volume = self.volume_of(new_start);
        // A claim never spans volumes, so the stolen tail is in the victim's
        // volume by construction; clip anyway to stay honest if that ever
        // changes.
        let new_end = self.volume_end(new_start).unwrap_or(v_end).min(v_end);
        if new_end < new_start {
            return None;
        }
        Some(self.register(worker, new_start, new_end, volume, None))
    }

    /// Which live claim to rob. Slowness beats size (`design.md` §6.4): a
    /// straggler is where the transfer's completion time is actually being
    /// decided, whereas the biggest claim may simply be the one that started
    /// last. Size is the tie-break when no rate sample is trustworthy yet.
    fn pick_victim(&self, worker: usize, need: u64) -> Option<usize> {
        let eligible: Vec<usize> = (0..self.live.len())
            .filter(|&i| self.live[i].worker != worker && self.live[i].hedge_of.is_none())
            .filter(|&i| self.live[i].remaining() > need)
            .collect();
        if eligible.is_empty() {
            return None;
        }
        // Median over every live claim, not just the eligible ones — the point
        // of comparison is "slow relative to this upstream right now".
        let mut rates: Vec<f64> = self.live.iter().filter_map(|l| l.rate()).collect();
        if rates.len() >= 3 {
            rates.sort_unstable_by(f64::total_cmp);
            let cutoff = rates[rates.len() / 2] * STRAGGLER_RATIO;
            let slowest = eligible
                .iter()
                .filter_map(|&i| self.live[i].rate().map(|r| (i, r)))
                .filter(|&(_, r)| r < cutoff)
                .min_by(|a, b| a.1.total_cmp(&b.1));
            if let Some((i, _)) = slowest {
                return Some(i);
            }
        }
        eligible.into_iter().max_by_key(|&i| self.live[i].remaining())
    }

    /// T3 — the endgame (`design.md` §6.1). Every byte is claimed and nothing
    /// is big enough to split, so the only way left to help is to fetch a range
    /// somebody else is already fetching and let the two race.
    ///
    /// Three limits keep this from becoming self-inflicted DDoS, which is what
    /// an unconditional endgame turns into on a correlated slowdown:
    ///
    /// * a hard duplicate-byte budget (0.5% of the transfer, ≤32 MiB) that is
    ///   spent and never refilled;
    /// * a duplication factor of two — one hedge per claim, never a third;
    /// * a [`MIN_CLAIM`] floor, because a duplicate request too small to
    ///   amortize its own round trip cannot win a race it started late.
    ///
    /// The loser is cut short through [`Scheduler::cut_short`] as soon as the
    /// winner reports, so the wasted bytes are bounded by one stream item on
    /// top of whatever the loser had already fetched.
    fn hedge(&mut self, worker: usize) -> Option<Claim> {
        if self.dup_budget < MIN_CLAIM {
            return None;
        }
        let hedged: Vec<u64> = self.live.iter().filter_map(|l| l.hedge_of).collect();
        let candidates: Vec<usize> = (0..self.live.len())
            .filter(|&i| {
                let l = &self.live[i];
                l.worker != worker
                    && l.hedge_of.is_none()
                    && l.remaining() >= MIN_CLAIM
                    && !hedged.contains(&l.start)
            })
            .collect();
        // Race the slowest claim: it is the one holding up the transfer, and
        // the one a fresh request has a chance of beating.
        let by_rate = candidates
            .iter()
            .copied()
            .filter_map(|i| self.live[i].rate().map(|r| (i, r)))
            .min_by(|a, b| a.1.total_cmp(&b.1))
            .map(|(i, _)| i);
        let pos = match by_rate {
            Some(i) => i,
            None => candidates
                .into_iter()
                .max_by_key(|&i| self.live[i].remaining())?,
        };

        let target = &self.live[pos];
        let primary_start = target.start;
        let lo = target.cursor.load(Ordering::Acquire);
        let hi = target
            .end
            .load(Ordering::Acquire)
            .min(lo.saturating_add(self.dup_budget - 1));
        if hi < lo || hi - lo + 1 < MIN_CLAIM {
            return None;
        }
        self.dup_budget -= hi - lo + 1;
        let volume = self.volume_of(lo);
        Some(self.register(worker, lo, hi, volume, Some(primary_start)))
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
            .max(self.min_frag());
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

    /// Playback-only: the pool must stay inside the horizon ahead of the reader
    /// instead of falling back to the whole file. Without this bound, watching a
    /// few megabytes quietly downloads everything and the explicit "cache the
    /// whole file" action means nothing.
    #[test]
    fn a_work_limit_keeps_the_pool_ahead_of_the_reader() {
        let mut s = Scheduler::new(
            0,
            100 * MIN_CLAIM - 1,
            Strategy::Latency {
                critical_window: 2 * MIN_CLAIM,
                head_claim: MIN_CLAIM,
            },
            None,
            1,
            4,
            Some(MIN_CLAIM),
            &[],
        );
        s.set_read_head(50 * MIN_CLAIM);
        s.set_work_limit(Some(4 * MIN_CLAIM));
        // Four 1 MiB claims cover [50, 54); after that there is nothing left
        // inside the horizon, and nothing behind the reader may be taken.
        for i in 0..4 {
            let c = s.claim(i).unwrap();
            assert_eq!(c.start, (50 + i as u64) * MIN_CLAIM);
        }
        assert!(
            s.claim(9).is_none(),
            "a bounded pool must not wander outside the reader's horizon",
        );
    }

    #[test]
    fn lifting_the_work_limit_opens_up_the_whole_file() {
        let mut s = sched(Strategy::Throughput, 1, Some(MIN_CLAIM));
        s.set_read_head(50 * MIN_CLAIM);
        s.set_work_limit(Some(MIN_CLAIM));
        let inside = s.claim(0).unwrap();
        assert_eq!(inside.start, 50 * MIN_CLAIM);
        assert!(s.claim(1).is_none(), "horizon is one claim deep");
        // Pressing "cache" lifts the bound: the rest of the file, including
        // everything before the reader, becomes fair game again.
        s.set_work_limit(None);
        assert_eq!(s.claim(1).unwrap().start, 0);
    }

    /// A seek hands back the requests the reader won't reach, and keeps the
    /// bytes those requests already delivered — that difference is what stops a
    /// seek from re-fetching data the pool had already paid for.
    #[test]
    fn reclaiming_outside_the_window_keeps_delivered_bytes() {
        let mut s = sched(Strategy::Throughput, 4, Some(4 * MIN_CLAIM));
        warm_to(&mut s, 4 * MIN_CLAIM);
        let far = s.claim(0).unwrap();
        let near = s.claim(1).unwrap();
        assert_eq!(far.start, 0);
        // Worker 0 delivered the first megabyte of its claim before the seek.
        far.advance_to(MIN_CLAIM);

        s.set_read_head(near.start);
        let stale = s.reclaim_outside_window(MIN_CLAIM);
        assert_eq!(stale, vec![0], "only the far worker is reclaimed");

        // The delivered megabyte is not back on the unclaimed list; the rest of
        // the abandoned claim is. (Which gap the pool picks up *next* is the
        // largest-gap decision, not this test's business — so assert on the
        // give-back itself.)
        assert!(
            s.unclaimed_ranges().contains(&(MIN_CLAIM, 4 * MIN_CLAIM - 1)),
            "refetching must resume at the cursor, not at the claim's start; \
             unclaimed = {:?}",
            s.unclaimed_ranges(),
        );
    }

    #[test]
    fn reclaiming_leaves_the_reader_s_own_requests_alone() {
        let mut s = sched(Strategy::Throughput, 4, Some(4 * MIN_CLAIM));
        warm_to(&mut s, 4 * MIN_CLAIM);
        let first = s.claim(0).unwrap();
        s.set_read_head(first.start);
        assert!(
            s.reclaim_outside_window(8 * MIN_CLAIM).is_empty(),
            "a claim overlapping the window must not be cancelled",
        );
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

    /// Building block for the fragmented-ledger tests: a request whose ledger
    /// already has holes punched in it, the way reclaims, retries and a warm
    /// cache leave one. Two narrow gaps low in the file, one wide gap high up.
    fn fragmented(strategy: Strategy) -> Scheduler {
        Scheduler::new(
            0,
            600 * MIN_CLAIM - 1,
            strategy,
            None,
            4,
            4,
            Some(4 * MIN_CLAIM),
            &[
                (2 * MIN_CLAIM, 3 * MIN_CLAIM - 1),
                (5 * MIN_CLAIM, 100 * MIN_CLAIM - 1),
            ],
        )
    }

    #[test]
    fn claims_come_out_of_the_largest_gap() {
        // aria2's `getSparseMissingUnusedIndex`, and the reason for it: one
        // request should buy as much contiguous work as it can. First-fit
        // instead hands the next worker whatever sliver happens to sit lowest
        // in the file, which is how a ledger fragmented by reclaims and retries
        // degenerates into a request storm.
        let mut s = fragmented(Strategy::Throughput);
        assert_eq!(
            s.claim(0).unwrap().start,
            100 * MIN_CLAIM,
            "expected the 500 MiB gap, not one of the 2 MiB slivers below it",
        );
    }

    #[test]
    fn the_critical_window_still_takes_the_nearest_gap() {
        // Largest-gap is a throughput score. Inside the reader's window the
        // score is proximity — the viewer is blocked on the *next* byte, not on
        // whichever hole happens to be widest.
        let mut s = fragmented(Strategy::Latency {
            critical_window: 600 * MIN_CLAIM,
            head_claim: MIN_CLAIM,
        });
        assert_eq!(
            s.claim(0).unwrap().start,
            0,
            "the reader's window must be filled front-to-back, not widest-first",
        );
    }

    #[test]
    fn the_tail_relaxes_fragments_to_one_megabyte() {
        // design.md §6.1 T2': in the endgame the goal flips from amortizing
        // per-request overhead to getting everyone busy, so a remainder too
        // small to be worth splitting mid-transfer becomes worth splitting.
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
        victim.advance_to(victim.end() - 12 * MIN_CLAIM + 1);
        assert!(s.in_tail(), "32 MiB left is inside the endgame threshold");
        let thief = s.claim(1).expect("tail fragments relax to MIN_CLAIM");
        assert_eq!(thief.end() - thief.start + 1, 6 * MIN_CLAIM);
    }

    #[test]
    fn playback_is_not_held_to_the_download_fragment_floor() {
        // MIN_FRAGMENT is a Policy A constant. Applying it to playback would
        // forbid splitting the small claims near the read head — which is
        // backwards, since those are small precisely to shorten the stall the
        // viewer feels.
        let mut s = Scheduler::new(
            0,
            4000 * MIN_CLAIM - 1,
            Strategy::Latency {
                critical_window: 4 * MIN_CLAIM,
                head_claim: 4 * MIN_CLAIM,
            },
            None,
            1,
            1,
            Some(4 * MIN_CLAIM),
            &[],
        );
        let victim = s.claim(0).unwrap();
        assert_eq!(victim.end() - victim.start + 1, 4 * MIN_CLAIM);
        // Pin the pool to exactly that claim so the only way to help is to
        // split it.
        s.set_work_limit(Some(4 * MIN_CLAIM));
        assert!(!s.in_tail(), "a 4 GiB file is nowhere near its tail");
        let thief = s
            .claim(1)
            .expect("a 4 MiB head claim must still be splittable");
        assert_eq!(thief.end() - thief.start + 1, 2 * MIN_CLAIM);
    }

    #[test]
    fn a_steal_starts_after_whatever_the_victim_reserved() {
        // The victim clamps each write to the end it read a moment earlier, so
        // between the steal and the victim noticing it there may be a write
        // already committed past the split point. Starting at the reservation
        // rather than the split is what keeps the handoff at zero duplicated
        // bytes (design.md §3.1).
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
        let split = victim.end() - victim.remaining() / 2;
        // In-flight write that will land past where the thief would cut.
        victim.reserve(split + MIN_CLAIM);

        let thief = s.claim(1).unwrap();
        assert_eq!(
            thief.start,
            split + MIN_CLAIM,
            "the thief must start past the victim's committed write",
        );
    }

    #[test]
    fn a_dead_claim_is_re_cut_and_its_worker_restarted() {
        let mut s = sched(Strategy::Throughput, 4, None);
        let stuck = s.claim(0).unwrap();
        let live = s.claim(1).unwrap();
        stuck.advance_to(stuck.start + MIN_CLAIM); // delivered, then stopped
        s.reclaim_stalled(); // arms the progress baseline

        // Nothing has moved since; pretend the window elapsed.
        for l in s.live.iter_mut() {
            l.last_progress -= DEAD_CLAIM_WINDOW;
        }
        live.advance_to(live.start + MIN_CLAIM); // this one is still working

        assert_eq!(s.reclaim_stalled(), vec![0], "only the dead claim is re-cut");
        assert!(
            s.unclaimed_ranges()
                .contains(&(stuck.start + MIN_CLAIM, stuck.end())),
            "the undelivered part of a dead claim goes back on the pool",
        );
    }

    #[test]
    fn the_endgame_hedges_on_a_budget_and_cuts_the_loser_short() {
        // design.md §6.1 T3. Everything is claimed and nothing is big enough to
        // split, so the last idle worker races the straggler.
        let mut s = Scheduler::new(
            0,
            400 * MIN_CLAIM - 1,
            Strategy::Throughput,
            None,
            1,
            1,
            None,
            &[],
        );
        warm_to(&mut s, MAX_AUTO_CLAIM);
        let mut primary = s.claim(0).unwrap();
        while s.unclaimed_bytes() > 0 {
            s.finish(0, u64::MAX);
            primary = s.claim(0).unwrap();
        }
        // Too little left to steal, so T1 and T2 are both exhausted.
        primary.advance_to(primary.end() - MIN_CLAIM + 1);
        assert!(s.in_tail());

        let hedge = s.claim(1).expect("the endgame must find work");
        assert_eq!(hedge.start, primary.cursor(), "hedges race from the cursor");
        assert_eq!(hedge.end(), primary.end());

        // Duplication factor two: one copy of a claim, never a third.
        assert!(s.claim(2).is_none());

        // Primary wins: the hedge is told it is already done. No stream reset
        // needed — the same end-watermark the steal path uses.
        primary.advance_to(primary.end() + 1);
        s.finish(0, primary.end() + 1);
        assert!(hedge.is_complete(), "the loser must be cut short");
    }

    #[test]
    fn the_endgame_budget_is_spent_not_refilled() {
        // 0.5% of the transfer, capped at 32 MiB, and a hedge too small to
        // amortize its own round trip is not worth issuing at all — which is
        // what stops a small file from hedging on a few kilobytes.
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
        let only = s.claim(0).unwrap();
        only.advance_to(only.end() - MIN_CLAIM + 1);
        assert!(s.in_tail());
        assert!(
            s.claim(1).is_none(),
            "0.5% of 32 MiB is 168 KiB — below the floor where a request pays \
             for itself",
        );
    }

    #[test]
    fn playback_never_hedges() {
        // Duplicate bytes are a download's answer to a straggler. A starved
        // reader is answered with the critical window instead — spending its
        // bandwidth on a second copy of bytes already in flight is exactly
        // backwards.
        let mut s = Scheduler::new(
            0,
            400 * MIN_CLAIM - 1,
            Strategy::Latency {
                critical_window: 400 * MIN_CLAIM,
                head_claim: MIN_CLAIM,
            },
            None,
            1,
            1,
            None,
            &[],
        );
        warm_to(&mut s, MAX_AUTO_CLAIM);
        let mut last = s.claim(0).unwrap();
        while s.unclaimed_bytes() > 0 {
            s.finish(0, u64::MAX);
            last = s.claim(0).unwrap();
        }
        last.advance_to(last.end() - MIN_CLAIM + 1);
        assert!(s.in_tail());
        assert!(s.claim(1).is_none());
    }
}
