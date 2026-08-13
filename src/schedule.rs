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
//! * [`Strategy::Latency`] — the region ahead of the reader gets priority, and
//!   the pool packs into it in short, equal claims, so the ordered prefix
//!   advances at the pool's aggregate rate rather than one connection's.
//!   Claims lengthen only once the reader has buffer to spare.
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

/// First pause after the origin answers 429/503, doubled per further strike.
const OVERLOAD_BACKOFF_BASE: Duration = Duration::from_millis(250);

/// Ceiling on the 429/503 pause. Long enough to actually clear a rate-limit
/// window, short enough that a recovered origin is not left idle.
const OVERLOAD_BACKOFF_MAX: Duration = Duration::from_secs(8);

/// Strike ceiling. The ladder is `250ms × 2^(strikes-1)`, so six strikes is
/// exactly [`OVERLOAD_BACKOFF_MAX`]; counting past that could only slow how
/// fast a successful claim works the strikes back off.
const OVERLOAD_STRIKES_MAX: u32 = 6;

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

/// Default claim length for [`Strategy::Latency`] before the reader has built
/// up any buffer, and the floor it never goes below.
///
/// Short on purpose, and *uniform* on purpose: every worker takes one of these
/// packed consecutively behind the read head, so a round of the whole pool
/// advances the ordered prefix by `max_threads × head_claim` at once. See
/// [`Scheduler::claim_len`] for why sizing these by distance instead caps
/// delivery at a single connection's rate.
pub const DEFAULT_HEAD_CLAIM: u64 = 2 * 1024 * 1024;

/// Where automatic sizing lands once an upstream has shown it cannot take a
/// full-size range.
///
/// This is a *recovery* size, not a starting point. Sizing starts at whatever
/// the strategy asks for — an even split of the work for a download, a
/// buffer-sized claim for playback — because "how big a request is worth making"
/// is overwhelmingly "as big as the policy says", and probing up to it wastes
/// a round trip plus slow start plus origin seek on every step of the ramp.
///
/// The exception is real, though: some upstreams — staging relays that
/// materialize an entire requested range before emitting a byte — have
/// per-request latency that grows *with* the range, so an even split of a
/// multi-gigabyte file is a request that never returns. That shape is not
/// knowable up front, only observable, and it announces itself unmistakably:
/// a claim that times out having delivered *nothing*. The response is to drop
/// here in one step rather than halve repeatedly, because every wrong guess
/// costs a full read timeout. See [`Scheduler::note_claim_outcome`].
pub const RECOVERY_CLAIM: u64 = 8 * 1024 * 1024;

/// How long a learned claim-size wall stays valid.
///
/// The wall is learned from a single observation — one claim that timed out
/// having delivered nothing — so it can also be learned from a one-off network
/// blip. Inside a request that is self-correcting (the request ends); once the
/// wall outlives the request it needs its own expiry, or one bad moment would
/// hold a task at recovery-sized claims forever.
///
/// Ten minutes is the same order as the profile TTLs aria2 (`24h`) and
/// safedrive (`10min`) use, and it turns the worst case from "pay a read
/// timeout on every seek" into "pay one every ten minutes".
const CLAIM_WALL_TTL: Duration = Duration::from_secs(600);

/// Cross-request memory of the largest range an origin will actually swallow.
///
/// Without this, [`Scheduler::auto_wall`] resets to `u64::MAX` for every new
/// [`Scheduler`] — and a staged stream builds one per client request, so a
/// player that reconnects on every seek re-learns the wall each time, paying a
/// full read timeout to do it.
///
/// Only ever tightens while a learned value is live: the wall records "this
/// origin failed above N", and a second, larger failure says nothing new.
/// Expiry is what lets it widen again.
#[derive(Debug, Default)]
pub struct ClaimWall {
    inner: parking_lot::Mutex<Option<(u64, Instant)>>,
}

impl ClaimWall {
    pub fn new() -> Self {
        Self::default()
    }

    /// The remembered wall, or `None` if nothing was learned or it has expired.
    pub fn get(&self) -> Option<u64> {
        let mut slot = self.inner.lock();
        match *slot {
            Some((bytes, at)) if at.elapsed() < CLAIM_WALL_TTL => Some(bytes),
            Some(_) => {
                // Expired: drop it so a recovered origin gets full-size claims
                // again instead of being pinned by an old bad minute.
                *slot = None;
                None
            }
            None => None,
        }
    }

    /// Remember that this origin could not swallow ranges above `bytes`.
    pub fn record(&self, bytes: u64) {
        let mut slot = self.inner.lock();
        let tightened = match *slot {
            Some((prev, at)) if at.elapsed() < CLAIM_WALL_TTL => prev.min(bytes),
            _ => bytes,
        };
        *slot = Some((tightened, Instant::now()));
    }

    /// Forget everything learned — used when the task's URLs change, since the
    /// wall describes an origin that may no longer be in the list.
    pub fn clear(&self) {
        *self.inner.lock() = None;
    }
}

/// How the scheduler prioritizes and sizes claims.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Strategy {
    /// Minimize total completion time; delivery order doesn't matter.
    /// Used for download-shaped requests (`?dl`).
    Throughput,
    /// Minimize time-to-playable around the reader's position, and only then
    /// use spare capacity to prefetch elsewhere. The default.
    Latency {
        /// Floor on the priority region ahead of the reader. The region is
        /// normally wider — see [`Scheduler::critical_span`].
        critical_window: u64,
        /// Claim length before the reader has buffer to spare, and the floor
        /// below which claims never shrink.
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
    /// Length this claim was cut to when it was handed out. Fixed, unlike
    /// [`Claim::end`], which a thief may move inward — automatic sizing needs
    /// to know what was *asked* of the upstream, not what survived.
    issued: u64,
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

    /// How long this claim was when it was handed out.
    pub fn issued_len(&self) -> u64 {
        self.issued
    }

    /// Bytes this claim has actually delivered so far.
    pub fn delivered(&self) -> u64 {
        self.cursor().saturating_sub(self.start)
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
        let done = self
            .cursor
            .load(Ordering::Acquire)
            .saturating_sub(self.start);
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
    /// Bytes the ordered reader can already consume without blocking, as last
    /// reported by [`Scheduler::set_reader_buffer`]. Zero — the default, and
    /// what a fresh seek resets to — means claims stay at their shortest.
    /// See [`Scheduler::claim_len`] for why this, and not distance from the
    /// read head, is what latency-mode claim size grows out of.
    buffered: u64,
    /// How far past `read_head` claims may reach, when set.
    ///
    /// Playback and whole-file caching share one scheduler, and the only thing
    /// that separates them is this bound. Without it, latency mode's fallback
    /// region is the entire request, so a reader that wants 4 MB in the middle
    /// of a file quietly pulls all of it — which makes an explicit "cache the
    /// whole file" action meaningless. `None` (caching) means the whole request
    /// is fair game.
    work_limit: Option<u64>,
    /// Ceiling automatic sizing is currently holding itself to, or `None` while
    /// no upstream has given a reason for one — the normal case, in which the
    /// strategy's own size (an even split, or the distance ladder) governs.
    /// See [`Scheduler::note_claim_outcome`].
    auto_limit: Option<u64>,
    /// Half the smallest range this upstream has been seen to swallow whole
    /// without delivering a byte. Recovery never grows back past it, so a
    /// relay that dies above ~20 MiB is probed at most twice, not on every
    /// other claim forever. `u64::MAX` until something actually fails.
    auto_wall: u64,
    /// Duplicate bytes the endgame may still spend. Charged on every hedge and
    /// never refilled, so a pathological tail can't turn into a request storm
    /// against the origin (`design.md` §6.1).
    dup_budget: u64,
    /// Unworked-off "slow down" answers (HTTP 429/503) from the origin.
    ///
    /// This is the one signal that says the *number of connections* is wrong
    /// rather than their size — `auto_limit` covers the latter. Each strike
    /// stretches the pause a worker takes before its next claim, which thins
    /// concurrency without retiring workers: a staged reader infers "nobody is
    /// coming" from the live worker count, so parking a worker is safe where
    /// ending one is not. Successful claims work strikes back off, so a
    /// transient 429 costs a short stagger, not a permanently narrower pool.
    overload_strikes: u32,
    /// The longest `Retry-After` the origin has asked for and we have not yet
    /// served. Cleared once a claim succeeds, so a stale hint cannot keep
    /// inflating the pause after the origin has recovered.
    overload_hint: Option<Duration>,
    /// Cross-request memory of [`Scheduler::auto_wall`], when the caller has
    /// one to share. `None` keeps the scheduler self-contained (what the tests
    /// use): everything learned dies with this instance.
    wall_memory: Option<Arc<ClaimWall>>,
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
            buffered: 0,
            work_limit: None,
            auto_limit: None,
            auto_wall: u64::MAX,
            dup_budget: (req_end.saturating_sub(req_start).saturating_add(1) / DUP_BUDGET_DIVISOR)
                .min(DUP_BUDGET_MAX),
            overload_strikes: 0,
            overload_hint: None,
            wall_memory: None,
        };
        for &(s0, e0) in already_staged {
            s.subtract(s0, e0);
        }
        s
    }

    /// Share a cross-request [`ClaimWall`] with this scheduler.
    ///
    /// Seeds sizing from whatever the origin already taught us, so a fresh
    /// request does not have to re-discover an oversized-range wall by paying
    /// another read timeout. Recovery still climbs back towards the strategy's
    /// size on every successful claim — the wall is a ceiling, not a target.
    pub fn with_claim_wall(mut self, wall: Arc<ClaimWall>) -> Self {
        if let Some(remembered) = wall.get() {
            self.auto_wall = remembered;
            self.auto_limit = Some(RECOVERY_CLAIM.min(remembered));
        }
        self.wall_memory = Some(wall);
        self
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
    ///
    /// A move is treated as a seek unless the caller says otherwise: the
    /// buffer estimate resets to zero, so claims go back to their shortest
    /// until [`Scheduler::set_reader_buffer`] reports runway again. That is
    /// the safe direction — short claims cost overhead, long ones cost stalls.
    pub fn set_read_head(&mut self, offset: u64) {
        self.read_head = offset.clamp(self.req_start, self.req_end.saturating_add(1));
        self.buffered = 0;
    }

    /// Report how many bytes the reader can consume from `read_head` without
    /// blocking. Call it right after [`Scheduler::set_read_head`] from the
    /// reader loop, which has the number in hand already.
    ///
    /// This is what lets latency-mode claims grow (see
    /// [`Scheduler::claim_len`]). Leaving it unset is safe and merely
    /// conservative.
    pub fn set_reader_buffer(&mut self, bytes: u64) {
        self.buffered = bytes;
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

    /// How far ahead of the reader the priority region reaches, in
    /// [`Strategy::Latency`]. Zero in throughput mode, which has no reader.
    ///
    /// It has to be deep enough to hold the whole worker pool — one claim per
    /// worker, packed consecutively behind the read head. A window shallower
    /// than that leaves the surplus workers with nothing prioritized, so they
    /// wander off to prefetch regions the reader won't reach for a long time
    /// and *starve the read head*: measured, a fixed 32 MiB window against 16
    /// workers held playback to 1.7 MB/s while the upstream as a whole was
    /// doing ~12 MB/s on speculative prefetch. `critical_window` is the floor.
    fn critical_span(&self) -> u64 {
        let Strategy::Latency {
            critical_window, ..
        } = self.strategy
        else {
            return 0;
        };
        let depth = (self.max_threads as u64).saturating_mul(self.claim_len());
        critical_window.max(depth).max(1)
    }

    /// Re-aim the whole pool at a reader that just arrived or seeked, and
    /// return the workers whose in-flight requests the caller must abort and
    /// restart.
    ///
    /// Playback wins outright here. A pool that was serving a download is
    /// carrying claims sized for throughput, and three separate things about it
    /// are wrong the instant somebody presses play:
    ///
    /// * **Nowhere near the reader.** Work nobody is waiting for. Hand it back
    ///   and put the worker next to the reader instead.
    /// * **Straddling the read head.** The reader is blocked on a byte inside
    ///   a claim whose cursor is still *behind* it, so the only way it gets
    ///   that byte is to wait out every byte in between — up to a full claim's
    ///   length of data nobody wants. This is the one that turns "press play on
    ///   a running download" into a multi-second stall, and it gets worse the
    ///   larger claims are allowed to be. Hand it back too; the pool re-cuts
    ///   the range starting *at* the read head, in a short head claim.
    /// * **Ahead of the reader but over-committed.** A claim longer than the
    ///   distance ladder would allow at its position holds a stretch the pool
    ///   should be covering in several small claims. Shorten it in place — the
    ///   zero-cost cancellation of `design.md` §4.2, where the worker simply
    ///   retires early — and the tail goes back for re-cutting.
    ///
    /// Only the first two abort anything, and both hand their range back at the
    /// claim's *cursor*, so bytes already delivered are kept. The earlier
    /// approach of aborting the whole pool and rebuilding from the on-disk
    /// bitmap discarded everything still in flight, which measured as roughly a
    /// fifth of a transfer being fetched twice.
    ///
    /// `window` is how far ahead of the reader a claim may sit and still count
    /// as relevant. It is deliberately tighter than
    /// [`Scheduler::critical_span`]: the point of a seek is to pull workers
    /// *back* to the reader, not to bless wherever they already are.
    ///
    /// No-op in [`Strategy::Throughput`], which has no reader to focus on.
    pub fn refocus_on_reader(&mut self, window: u64) -> Vec<usize> {
        let Strategy::Latency { head_claim, .. } = self.strategy else {
            return Vec::new();
        };
        let head = self.read_head;
        let hi = head.saturating_add(window.max(1));
        // How far behind the read head a claim may be and still be worth
        // waiting for. Below this, catching up costs less than a fresh request
        // would, and killing the claim would throw away a warm connection.
        let grace = head_claim.max(MIN_CLAIM);
        let mut reclaimed = Vec::new();
        let mut i = 0;
        while i < self.live.len() {
            let cursor = self.live[i].cursor.load(Ordering::Acquire);
            let end = self.live[i].end.load(Ordering::Acquire);
            if cursor > end {
                // Finished; the worker is about to report in.
                i += 1;
                continue;
            }
            let irrelevant = cursor >= hi || end < head;
            let blocks_the_reader = end >= head && cursor.saturating_add(grace) < head;
            if irrelevant || blocks_the_reader {
                let live = self.live.remove(i);
                reclaimed.push(live.worker);
                // A hedge owns nothing: the claim it was racing still holds
                // this range, so handing it back would queue a second fetch.
                if live.hedge_of.is_none() {
                    self.insert_unclaimed(cursor, end);
                }
                continue;
            }
            if self.live[i].hedge_of.is_none() {
                let keep = cursor.saturating_add(self.claim_len().saturating_sub(1));
                if keep < end {
                    self.live[i].end.store(keep, Ordering::Release);
                    // The worker clamps each write to the end it read just
                    // before writing, so it may already have committed to bytes
                    // past the cut. Give back only what starts after whatever
                    // it reserved, exactly as the steal path does.
                    let give_back = keep
                        .saturating_add(1)
                        .max(self.live[i].reserved.load(Ordering::Acquire));
                    if give_back <= end {
                        self.insert_unclaimed(give_back, end);
                    }
                }
            }
            i += 1;
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

    /// Feed a completed claim's outcome back into automatic sizing, and say
    /// whether the caller should charge a failure against its failure budget.
    ///
    /// Sizing does **not** probe upwards. The strategy's own size — an even
    /// split of the work for a download, the distance ladder for playback — is
    /// the answer for every upstream that behaves like an HTTP server, and
    /// ramping up to it just buys extra round trips. Only evidence to the
    /// contrary moves the ceiling, and only downwards.
    ///
    /// The evidence is specific: a claim that timed out having delivered
    /// *nothing at all*. That is the signature of a staging relay materializing
    /// the whole requested range before emitting a byte, and it is the one
    /// shape for which a big request is worse than several small ones. A claim
    /// that moved real bytes and then broke is a transport failure — the size
    /// was fine, so sizing stays put and the failure is charged normally.
    ///
    /// Two rules keep the descent from costing more than it saves, both of
    /// which matter because every wrong guess costs a full read timeout:
    ///
    /// * The first such failure drops straight to [`RECOVERY_CLAIM`] instead of
    ///   halving, and records `issued / 2` as [`Scheduler::auto_wall`] — the
    ///   ceiling recovery may climb back to, never past.
    /// * Failures of claims that were already bigger than the current limit
    ///   were cut *before* that drop. They re-prove what the first one already
    ///   established, so they neither shrink the limit further nor count: with
    ///   `max_threads` workers in flight, one bad round would otherwise both
    ///   collapse the size to [`MIN_CLAIM`] and exhaust a failure budget of
    ///   `2 × max_threads` on its own.
    pub fn note_claim_outcome(&mut self, ok: bool, claim: &Claim) -> bool {
        if ok {
            // A claim that completed is evidence the current width is workable,
            // so work off one strike. Additive recovery against multiplicative
            // backoff — the same shape TCP uses, and for the same reason.
            self.overload_strikes = self.overload_strikes.saturating_sub(1);
            if self.overload_strikes == 0 {
                // Fully recovered: drop the origin's old wait request, or it
                // would keep padding every future pause.
                self.overload_hint = None;
            }
            // Recovery: climb back towards the strategy's size, but never past
            // what this upstream has been shown to swallow.
            if let Some(limit) = self.auto_limit {
                self.auto_limit = Some(limit.saturating_mul(2).min(self.auto_wall));
            }
            return false;
        }
        if claim.delivered() >= MIN_FRAGMENT {
            // It carried real bytes and then broke. Nothing about the size was
            // the problem, so leave it alone and let the budget see this.
            return true;
        }
        let issued = claim.issued_len();
        let counts = match self.auto_limit {
            Some(limit) if issued > limit => false,
            Some(limit) => {
                let cut = (limit / 2).max(MIN_CLAIM);
                self.auto_wall = self.auto_wall.min(cut);
                self.auto_limit = Some(cut);
                true
            }
            None => {
                self.auto_wall = (issued / 2).max(MIN_CLAIM);
                self.auto_limit = Some(RECOVERY_CLAIM.min(self.auto_wall));
                false
            }
        };
        // Hand what we just learned to the next request against this task, so
        // it does not have to buy the same lesson with another read timeout.
        if let Some(wall) = &self.wall_memory {
            wall.record(self.auto_wall);
        }
        counts
    }

    /// The ceiling automatic sizing is currently holding itself to, if any —
    /// exposed for logging and tests.
    pub fn auto_limit(&self) -> Option<u64> {
        self.auto_limit
    }

    /// Record that the origin answered 429/503 — it is refusing this much
    /// concurrency, whatever the claim size.
    ///
    /// Rotating to the next mirror (what a plain failure does) is the wrong
    /// answer here: on a single-origin task there is nowhere to rotate to, so
    /// the retry lands on the same rate limiter and burns the task's failure
    /// budget at full speed.
    ///
    /// `retry_after` is the origin's own `Retry-After`, already sanity-checked
    /// and clamped by the caller. It is treated as a *floor*, not a
    /// replacement: honouring a server that keeps answering "wait 1s" while it
    /// keeps refusing us would hammer it once a second forever, so the strike
    /// ladder still escalates underneath.
    pub fn note_overload(&mut self, retry_after: Option<Duration>) {
        self.overload_strikes = (self.overload_strikes + 1).min(OVERLOAD_STRIKES_MAX);
        if let Some(hint) = retry_after {
            // Keep the longest outstanding request; a shorter later hint must
            // not shrink a wait we already committed to.
            self.overload_hint = Some(self.overload_hint.map_or(hint, |cur| cur.max(hint)));
        }
    }

    /// How long `worker` should wait before taking its next claim.
    ///
    /// The jitter is what makes this thin concurrency rather than just slow
    /// everything down: an unjittered pause is served by every worker at once,
    /// so they resynchronise and hit the origin in the same bursts that earned
    /// the 429. Spreading them across the window staggers the requests instead.
    ///
    /// A `Retry-After` from the origin raises the floor but never lowers it, so
    /// a server asking for less time than our own escalation says gets our
    /// escalation. Jitter is applied on top either way — even a server-dictated
    /// wait must not release the whole pool at the same instant.
    pub fn overload_backoff(&self, worker: usize) -> Option<Duration> {
        if self.overload_strikes == 0 {
            return None;
        }
        let scaled = OVERLOAD_BACKOFF_BASE
            .saturating_mul(1u32 << (self.overload_strikes - 1))
            .min(OVERLOAD_BACKOFF_MAX);
        let scaled = match self.overload_hint {
            Some(hint) => scaled.max(hint),
            None => scaled,
        };
        // Deterministic per-worker offset over [0, scaled): no RNG dependency,
        // and reproducible in tests.
        let span = scaled.as_millis() as u64;
        let offset = if span == 0 {
            0
        } else {
            (worker as u64).wrapping_mul(2_654_435_761) % span
        };
        Some(scaled + Duration::from_millis(offset))
    }

    /// Unworked-off 429/503 strikes — exposed for logging and tests.
    pub fn overload_strikes(&self) -> u32 {
        self.overload_strikes
    }

    /// How long a claim starting at the read head would be right now — the
    /// single number that summarizes automatic sizing, for logging and tests.
    pub fn auto_claim_len(&self) -> u64 {
        self.claim_len()
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
            Strategy::Latency { .. } => {
                let hi = self
                    .read_head
                    .saturating_add(self.critical_span().saturating_sub(1))
                    .min(horizon);
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

        let want = self.claim_len();
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
            issued: end.saturating_sub(start).saturating_add(1),
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
        eligible
            .into_iter()
            .max_by_key(|&i| self.live[i].remaining())
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

    /// An even share of the work still to do: what one worker should take so
    /// that everybody finishes at the same time.
    ///
    /// Dividing by *free slots* rather than `max_threads` is what makes the
    /// first round come out even: with 4 threads the claims are 100/4, 75/3,
    /// 50/2, 25/1 — four equal quarters. Dividing by `max_threads` throughout
    /// would shrink each successive claim geometrically and never cover the
    /// request.
    ///
    /// Later, once every slot is busy, the divisor reaches 1 and a returning
    /// worker sweeps up all remaining unclaimed work in one claim. That is
    /// deliberate and self-correcting: the next worker to free up steals half
    /// of it. Same dynamic as gopeed's "one connection owns the tail, others
    /// split it".
    fn even_share(&self) -> u64 {
        let free_slots = self.max_threads.saturating_sub(self.live.len()).max(1) as u64;
        self.unclaimed_bytes()
            .div_ceil(free_slots)
            .max(self.min_frag())
    }

    /// How long a claim should be right now.
    ///
    /// Both strategies want claims as large as their policy allows, because
    /// every extra claim costs a round trip, another slow start, and whatever
    /// the origin spends seeking. They differ in what bounds "as large as
    /// allowed":
    ///
    /// * [`Strategy::Throughput`] — an even share of the remaining work. No
    ///   ordered reader exists, so the only thing worth optimizing is that all
    ///   workers finish together, and the largest claim that does that is
    ///   `remaining / free workers`.
    /// * [`Strategy::Latency`] — small and **uniform**, so the pool packs tight
    ///   behind the read head. This is the one that decides whether ordered
    ///   delivery runs at the pool's aggregate rate or at one connection's.
    ///   The ordered reader can only emit its contiguous prefix, so its speed
    ///   is the speed of whichever single claim covers the byte it wants next.
    ///   Give that claim a length proportional to its distance from the head
    ///   — the obvious "the reader has runway before it needs this" argument —
    ///   and the prefix advances at exactly one connection's rate while the
    ///   other fifteen workers pour bytes into regions the reader won't reach
    ///   for a minute. Measured: 25 MB/s pulled from upstream, 3.2 MB/s
    ///   delivered to the client. Equal claims packed consecutively make the
    ///   whole pool finish a round together, so the prefix jumps
    ///   `max_threads × claim` at once — aggregate rate, which is the point.
    ///
    /// The runway argument only becomes true once the runway actually exists,
    /// so that is exactly when latency claims are allowed to grow: with
    /// `buffered` bytes already readable ahead of the reader and `max_threads`
    /// workers, a claim of `buffered / max_threads` is one the whole pool can
    /// turn over in the time the reader drains what it already has. A player
    /// that has built up a buffer therefore gets big, cheap claims; a cold
    /// start or a fresh seek gets short ones until it has earned otherwise.
    ///
    /// [`Scheduler::auto_limit`] then caps whatever the policy asked for, but
    /// only once an upstream has proven it needs capping, and an explicit
    /// `max_split` caps everything unconditionally.
    fn claim_len(&self) -> u64 {
        let policy = match self.strategy {
            Strategy::Throughput => self.even_share(),
            Strategy::Latency { head_claim, .. } => (self.buffered / (self.max_threads as u64))
                .max(head_claim.max(1))
                .min(self.even_share()),
        };
        let bounded = policy.min(self.auto_limit.unwrap_or(u64::MAX));
        match self.split_cap {
            Some(cap) => bounded.min(cap).max(1),
            None => bounded.max(1),
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

    #[test]
    fn a_healthy_scheduler_never_makes_a_worker_wait() {
        let s = sched(Strategy::Throughput, 8, None);
        assert_eq!(s.overload_strikes(), 0);
        for worker in 0..8 {
            assert_eq!(s.overload_backoff(worker), None);
        }
    }

    #[test]
    fn overload_backoff_grows_multiplicatively_and_caps() {
        let mut s = sched(Strategy::Throughput, 8, None);
        let mut last = Duration::ZERO;
        for _ in 0..OVERLOAD_STRIKES_MAX {
            s.note_overload(None);
            // Worker 0's jitter offset is 0, so it reads the base pause exactly.
            let now = s.overload_backoff(0).unwrap();
            assert!(now >= last, "backoff must not shrink while strikes climb");
            last = now;
        }
        assert_eq!(s.overload_strikes(), OVERLOAD_STRIKES_MAX);
        // Further strikes neither overflow the shift nor exceed the cap.
        for _ in 0..4 {
            s.note_overload(None);
        }
        assert_eq!(s.overload_strikes(), OVERLOAD_STRIKES_MAX);
        assert_eq!(s.overload_backoff(0).unwrap(), OVERLOAD_BACKOFF_MAX);
    }

    #[test]
    fn overload_backoff_staggers_workers_instead_of_pausing_them_together() {
        let mut s = sched(Strategy::Throughput, 8, None);
        s.note_overload(None);
        s.note_overload(None);
        // The point of the jitter: an unjittered pause is served by the whole
        // pool at once, which resynchronises it into the same burst that earned
        // the 429 in the first place.
        let waits: std::collections::HashSet<_> =
            (0..8).map(|w| s.overload_backoff(w).unwrap()).collect();
        assert!(
            waits.len() > 1,
            "all workers got the same pause, so they will resynchronise: {waits:?}"
        );
    }

    #[test]
    fn a_successful_claim_works_off_one_strike() {
        let mut s = sched(Strategy::Throughput, 4, None);
        s.note_overload(None);
        s.note_overload(None);
        assert_eq!(s.overload_strikes(), 2);

        let c = s.claim(0).expect("a fresh scheduler has work");
        s.finish(0, c.end());
        s.note_claim_outcome(true, &c);
        assert_eq!(
            s.overload_strikes(),
            1,
            "success is evidence, so decay by one"
        );

        let c = s.claim(0).expect("still work left");
        s.finish(0, c.end());
        s.note_claim_outcome(true, &c);
        assert_eq!(s.overload_strikes(), 0);
        assert_eq!(s.overload_backoff(0), None, "recovered pools do not wait");
    }

    #[test]
    fn a_fresh_wall_remembers_nothing() {
        let w = ClaimWall::new();
        assert_eq!(w.get(), None);
        // Seeding from an empty wall must leave sizing exactly as it was.
        let plain = sched(Strategy::Throughput, 8, None);
        let seeded =
            sched(Strategy::Throughput, 8, None).with_claim_wall(Arc::new(ClaimWall::new()));
        assert_eq!(seeded.auto_limit(), plain.auto_limit());
        assert_eq!(seeded.auto_claim_len(), plain.auto_claim_len());
    }

    #[test]
    fn the_wall_only_tightens() {
        let w = ClaimWall::new();
        w.record(32 * MIN_CLAIM);
        w.record(8 * MIN_CLAIM);
        assert_eq!(w.get(), Some(8 * MIN_CLAIM));
        // A later, larger failure says nothing new — it must not widen.
        w.record(64 * MIN_CLAIM);
        assert_eq!(w.get(), Some(8 * MIN_CLAIM));
        w.clear();
        assert_eq!(w.get(), None);
    }

    #[test]
    fn a_learned_wall_seeds_the_next_scheduler() {
        let wall = Arc::new(ClaimWall::new());

        // First request: a big claim times out having delivered nothing.
        let mut first = sched(Strategy::Throughput, 4, None).with_claim_wall(Arc::clone(&wall));
        let c = first.claim(0).expect("fresh scheduler has work");
        first.finish(0, c.start);
        first.note_claim_outcome(false, &c);
        let learned = wall.get().expect("the failure must be remembered");

        // Second request against the same task starts already knowing, instead
        // of buying the same lesson with another read timeout.
        let second = sched(Strategy::Throughput, 4, None).with_claim_wall(Arc::clone(&wall));
        assert_eq!(second.auto_limit(), Some(RECOVERY_CLAIM.min(learned)));
        assert!(
            second.auto_claim_len() <= learned,
            "a seeded scheduler must not re-issue a claim above the wall"
        );
    }

    #[test]
    fn the_wall_expires_so_one_bad_minute_is_not_permanent() {
        let w = ClaimWall::new();
        w.record(MIN_CLAIM);
        assert_eq!(w.get(), Some(MIN_CLAIM));
        // Backdate past the TTL: a stale wall must stop constraining, or a
        // single blip would hold the task at recovery-sized claims forever.
        *w.inner.lock() = Some((
            MIN_CLAIM,
            Instant::now() - CLAIM_WALL_TTL - Duration::from_secs(1),
        ));
        assert_eq!(w.get(), None);
    }

    #[test]
    fn a_retry_after_hint_raises_the_floor_but_never_lowers_it() {
        // Server asks for much longer than our first-strike pause.
        let mut generous = sched(Strategy::Throughput, 4, None);
        generous.note_overload(Some(Duration::from_secs(30)));
        assert!(
            generous.overload_backoff(0).unwrap() >= Duration::from_secs(30),
            "an explicit Retry-After must be honoured when it exceeds our ladder"
        );

        // Server asks for less than our ladder already decided. Honouring that
        // would hammer an origin that keeps refusing us, so our value wins.
        let mut stingy = sched(Strategy::Throughput, 4, None);
        for _ in 0..OVERLOAD_STRIKES_MAX {
            stingy.note_overload(Some(Duration::from_millis(1)));
        }
        let ours = OVERLOAD_BACKOFF_MAX;
        assert!(
            stingy.overload_backoff(0).unwrap() >= ours,
            "a tiny Retry-After must not undercut the escalation we earned"
        );
    }

    #[test]
    fn a_retry_after_hint_is_still_jittered() {
        // Even a server-dictated wait must not release the whole pool at once.
        let mut s = sched(Strategy::Throughput, 8, None);
        s.note_overload(Some(Duration::from_secs(10)));
        let waits: std::collections::HashSet<_> =
            (0..8).map(|w| s.overload_backoff(w).unwrap()).collect();
        assert!(
            waits.len() > 1,
            "server-dictated waits resynchronised the pool"
        );
    }

    #[test]
    fn recovery_forgets_the_retry_after_hint() {
        let mut s = sched(Strategy::Throughput, 4, None);
        s.note_overload(Some(Duration::from_secs(45)));

        // Work the single strike off; the hint must go with it, or every future
        // pause would stay padded by a request the origin has long since
        // stopped making.
        let c = s.claim(0).expect("fresh scheduler has work");
        s.finish(0, c.end());
        s.note_claim_outcome(true, &c);
        assert_eq!(s.overload_strikes(), 0);
        assert_eq!(s.overload_backoff(0), None);

        // A later, hint-free strike must fall back to the base ladder.
        s.note_overload(None);
        assert!(s.overload_backoff(0).unwrap() < Duration::from_secs(45));
    }

    fn sched(strategy: Strategy, threads: usize, cap: Option<u64>) -> Scheduler {
        Scheduler::new(
            0,
            100 * MIN_CLAIM - 1,
            strategy,
            None,
            threads,
            threads,
            cap,
            &[],
        )
    }

    /// Hand out a claim, deliver nothing, and report the failure the way a
    /// worker would — the staging-relay signature. Returns the claim's issued
    /// length and whether the failure was charged to the caller's budget.
    fn fail_empty(s: &mut Scheduler, worker: usize) -> (u64, bool) {
        let c = s.claim(worker).expect("a claim to fail");
        s.finish(worker, c.cursor());
        (c.issued_len(), s.note_claim_outcome(false, &c))
    }

    /// Hand out a claim, deliver all of it, and report success.
    fn succeed(s: &mut Scheduler, worker: usize) -> u64 {
        let c = s.claim(worker).expect("a claim to run");
        c.advance_to(c.end() + 1);
        s.finish(worker, c.cursor());
        s.note_claim_outcome(true, &c);
        c.issued_len()
    }

    #[test]
    fn the_first_claim_is_already_a_full_even_split() {
        // No slow start. Sizing opens at what the policy asks for, because for
        // every upstream that behaves like an HTTP server that is the right
        // answer, and ramping up to it pays a round trip per step.
        let mut s = sched(Strategy::Throughput, 4, None);
        let first = s.claim(0).unwrap();
        assert_eq!(
            first.issued_len(),
            25 * MIN_CLAIM,
            "100 MiB over 4 workers is 25 MiB each, from the very first claim",
        );
    }

    #[test]
    fn throughput_splits_request_across_threads() {
        // 100 MiB over 4 workers → ~25 MiB each, covering the whole file with
        // no leftovers once all four have claimed.
        let mut s = sched(Strategy::Throughput, 4, None);
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
        let a = s.claim(0).unwrap();
        assert_eq!(a.end() - a.start + 1, 4 * MIN_CLAIM);
    }

    #[test]
    fn an_explicit_split_cap_bounds_playback_claims_too() {
        // A hand-set `max_split` is a hard ceiling on *every* claim, so the
        // distance ladder must flatten out at it rather than run past.
        let mut s = Scheduler::new(
            0,
            1000 * MIN_CLAIM - 1,
            Strategy::Latency {
                critical_window: 8 * MIN_CLAIM,
                head_claim: 2 * MIN_CLAIM,
            },
            None,
            8,
            8,
            Some(3 * MIN_CLAIM),
            &[],
        );
        s.set_read_head(500 * MIN_CLAIM);
        for w in 0..6 {
            let c = s.claim(w).unwrap();
            assert!(
                c.issued_len() <= 3 * MIN_CLAIM,
                "claim {} ran past the configured ceiling at {} B",
                w,
                c.issued_len(),
            );
        }
    }

    #[test]
    fn a_claim_that_delivers_nothing_drops_sizing_in_one_step() {
        // The one shape a big request gets wrong: a staging relay that
        // materializes the whole range before emitting a byte. It announces
        // itself as a timeout with zero bytes delivered, and the answer is to
        // land on a size that works everywhere immediately — halving toward it
        // would cost a full read timeout per step.
        let mut s = sched(Strategy::Throughput, 4, None);
        let (issued, charged) = fail_empty(&mut s, 0);
        assert_eq!(issued, 25 * MIN_CLAIM);
        assert_eq!(s.auto_limit(), Some(RECOVERY_CLAIM));
        assert_eq!(s.claim(0).unwrap().issued_len(), RECOVERY_CLAIM);
        assert!(
            !charged,
            "a failure the scheduler answers by resizing is its own to absorb",
        );
    }

    #[test]
    fn one_bad_round_neither_compounds_nor_exhausts_the_budget() {
        // Regression guard for the reason sizing may open at a full share at
        // all. Every worker cuts its claim before any of them reports back, so
        // a relay that swallows big ranges fails all of them at once. Those
        // later failures were sized before the drop: they re-prove what the
        // first one established, so they must neither shrink sizing further
        // (down to MIN_CLAIM, a request storm) nor spend a failure budget of
        // `2 × max_threads` in a single round (a task marked failed before it
        // ever got to try the smaller size).
        let mut s = sched(Strategy::Throughput, 4, None);
        let claims: Vec<Claim> = (0..4).map(|w| s.claim(w).unwrap()).collect();
        let mut charged = 0;
        for (worker, c) in claims.iter().enumerate() {
            s.finish(worker, c.cursor());
            if s.note_claim_outcome(false, c) {
                charged += 1;
            }
        }
        assert_eq!(charged, 0);
        assert_eq!(s.auto_limit(), Some(RECOVERY_CLAIM));
    }

    #[test]
    fn recovery_climbs_back_but_never_past_the_wall() {
        // Growth after a drop is bounded by what the upstream was seen to
        // swallow, so a relay is probed twice, not on every other claim
        // forever — the oscillation that showed up as a run stalling for 45 s
        // and then dumping 256 MB at once.
        let mut s = sched(Strategy::Throughput, 4, None);
        let (issued, _) = fail_empty(&mut s, 0);
        let wall = issued / 2;
        assert_eq!(s.auto_limit(), Some(RECOVERY_CLAIM));
        for _ in 0..3 {
            succeed(&mut s, 0);
        }
        assert_eq!(s.auto_limit(), Some(wall));
    }

    #[test]
    fn repeated_empty_failures_bottom_out_at_min_claim() {
        let mut s = sched(Strategy::Throughput, 4, None);
        for _ in 0..20 {
            fail_empty(&mut s, 0);
        }
        assert_eq!(s.auto_limit(), Some(MIN_CLAIM));
        assert_eq!(s.claim(0).unwrap().issued_len(), MIN_CLAIM);
    }

    #[test]
    fn a_failure_after_real_progress_is_charged_and_leaves_sizing_alone() {
        // A claim that carried bytes and then broke says nothing about how big
        // a request this upstream can take. Shrinking would be the wrong fix,
        // and swallowing the failure would let a flapping connection retry
        // forever.
        let mut s = sched(Strategy::Throughput, 4, None);
        let c = s.claim(0).unwrap();
        c.advance_to(c.start + MIN_FRAGMENT);
        s.finish(0, c.cursor());
        assert!(s.note_claim_outcome(false, &c));
        assert_eq!(s.auto_limit(), None);
    }

    #[test]
    fn unlimited_split_can_exceed_any_fixed_size() {
        // The point of "no max split" is that the *configuration* stops
        // dictating the size — one worker with the whole request takes the
        // whole request, not some fraction the code picked.
        let mut s = sched(Strategy::Throughput, 1, None);
        let a = s.claim(0).unwrap();
        assert_eq!(a.start, 0);
        let len = a.end() - a.start + 1;
        assert_eq!(len, 100 * MIN_CLAIM);
        assert!(
            len > 5 * 1024 * 1024 * 10,
            "must dwarf the old 5 MiB default"
        );
    }

    #[test]
    fn latency_packs_the_pool_into_equal_claims_behind_the_reader() {
        // The regression this sizing rule exists for. An ordered reader can
        // only emit its contiguous prefix, so the prefix advances at the speed
        // of whichever single claim covers the byte it wants next. Equal
        // claims packed consecutively make the whole pool finish a round
        // together and the prefix jump `max_threads × claim` at once — the
        // aggregate rate. Sizing them by distance from the head instead caps
        // delivery at one connection: measured, 25 MB/s pulled from upstream
        // and 3.2 MB/s handed to the client.
        let mut s = Scheduler::new(
            0,
            1000 * MIN_CLAIM - 1,
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
        s.set_read_head(500 * MIN_CLAIM);
        let mut at = 500 * MIN_CLAIM;
        for worker in 0..5 {
            let c = s.claim(worker).unwrap();
            assert_eq!(c.start, at, "claims must tile forward from the read head");
            assert_eq!(
                c.issued_len(),
                2 * MIN_CLAIM,
                "every worker takes the same short claim, not a longer one \
                 further out",
            );
            at = c.end() + 1;
        }
    }

    #[test]
    fn latency_claims_grow_with_the_readers_buffer() {
        // Once runway exists, the "the reader won't need this for a while"
        // argument becomes true and claims may lengthen to amortize per-request
        // overhead. The measure is the reader's buffer, not the claim's
        // distance: with `buffered` bytes readable and `n` workers, a claim of
        // `buffered / n` is one the whole pool turns over in the time the
        // reader drains what it has.
        let threads = 8;
        let mut s = Scheduler::new(
            0,
            1000 * MIN_CLAIM - 1,
            Strategy::Latency {
                critical_window: 8 * MIN_CLAIM,
                head_claim: 2 * MIN_CLAIM,
            },
            None,
            threads,
            threads,
            None,
            &[],
        );
        s.set_read_head(100 * MIN_CLAIM);
        assert_eq!(s.claim(0).unwrap().issued_len(), 2 * MIN_CLAIM, "no buffer");

        s.set_reader_buffer(80 * MIN_CLAIM);
        assert_eq!(
            s.claim(1).unwrap().issued_len(),
            10 * MIN_CLAIM,
            "80 MiB of runway over 8 workers is a 10 MiB claim each",
        );

        // A seek is a fresh start: the runway it was earned on is gone.
        s.set_read_head(300 * MIN_CLAIM);
        assert_eq!(
            s.claim(2).unwrap().issued_len(),
            2 * MIN_CLAIM,
            "moving the read head must reset sizing to the short claim",
        );
    }

    #[test]
    fn latency_window_is_at_least_as_deep_as_the_worker_pool() {
        // A window too shallow to hold the pool leaves most workers with
        // nothing prioritized, so they prefetch far-away regions and starve the
        // read head. The window must scale with the pool.
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
        // Window = max(4 MiB, one worker × a 1 MiB claim) = 4 MiB, filled by
        // four uniform 1 MiB claims.
        for (i, worker) in (0..4).enumerate() {
            let c = s.claim(worker).unwrap();
            assert_eq!(c.start, (50 + i as u64) * MIN_CLAIM);
            assert_eq!(c.issued_len(), MIN_CLAIM);
        }
        // Window exhausted → opportunistic prefetch from the earliest gap.
        let d = s.claim(4).unwrap();
        assert_eq!(d.start, 0, "prefetch starts at the earliest gap");
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

    /// A latency-mode scheduler over a 1000 MiB request with a 4 MiB head
    /// claim, for the seek-refocus tests.
    fn playback_sched(threads: usize, cap: Option<u64>) -> Scheduler {
        Scheduler::new(
            0,
            1000 * MIN_CLAIM - 1,
            Strategy::Latency {
                critical_window: 8 * MIN_CLAIM,
                head_claim: 4 * MIN_CLAIM,
            },
            None,
            threads,
            threads,
            cap,
            &[],
        )
    }

    /// A seek hands back the requests the reader won't reach, and keeps the
    /// bytes those requests already delivered — that difference is what stops a
    /// seek from re-fetching data the pool had already paid for.
    #[test]
    fn refocusing_keeps_delivered_bytes() {
        let mut s = playback_sched(4, Some(4 * MIN_CLAIM));
        let far = s.claim(0).unwrap();
        let near = s.claim(1).unwrap();
        assert_eq!(far.start, 0);
        // Worker 0 delivered the first megabyte of its claim before the seek.
        far.advance_to(MIN_CLAIM);

        s.set_read_head(near.start);
        let stale = s.refocus_on_reader(MIN_CLAIM);
        assert_eq!(stale, vec![0], "only the far worker is reclaimed");

        // The delivered megabyte is not back on the unclaimed list; the rest of
        // the abandoned claim is. (Which gap the pool picks up *next* is the
        // largest-gap decision, not this test's business — so assert on the
        // give-back itself.)
        assert!(
            s.unclaimed_ranges()
                .contains(&(MIN_CLAIM, 4 * MIN_CLAIM - 1)),
            "refetching must resume at the cursor, not at the claim's start; \
             unclaimed = {:?}",
            s.unclaimed_ranges(),
        );
    }

    #[test]
    fn refocusing_leaves_the_reader_s_own_request_alone() {
        let mut s = playback_sched(4, Some(4 * MIN_CLAIM));
        let first = s.claim(0).unwrap();
        s.set_read_head(first.start);
        assert!(
            s.refocus_on_reader(8 * MIN_CLAIM).is_empty(),
            "a claim the reader is sitting on must not be cancelled",
        );
    }

    /// A download already in flight: a throughput-sized claim handed out before
    /// any reader existed. Returns the scheduler with the strategy flipped to
    /// playback, exactly as a coordinator's `retune` does when somebody presses
    /// play. The claim is *not* re-cut by that flip — only
    /// [`Scheduler::refocus_on_reader`] does that, which is the point.
    fn downloading_then_playing(threads: usize) -> (Scheduler, Claim) {
        let mut s = Scheduler::new(
            0,
            1000 * MIN_CLAIM - 1,
            Strategy::Throughput,
            None,
            threads,
            threads,
            None,
            &[],
        );
        let big = s.claim(0).unwrap();
        s.set_strategy(Strategy::Latency {
            critical_window: 8 * MIN_CLAIM,
            head_claim: 4 * MIN_CLAIM,
        });
        (s, big)
    }

    #[test]
    fn a_reader_never_waits_out_the_claim_it_is_stuck_behind() {
        // Press play in the middle of a running download. The read head lands
        // inside a throughput-sized claim whose cursor is still far behind it,
        // so the byte the viewer is waiting for arrives only after the worker
        // has streamed everything in between. Handing that claim back is the
        // whole point: the pool re-cuts it *at* the read head.
        let (mut s, big) = downloading_then_playing(4);
        assert_eq!(big.issued_len(), 250 * MIN_CLAIM, "a full even share");
        big.advance_to(200 * MIN_CLAIM);

        s.set_read_head(220 * MIN_CLAIM);
        assert_eq!(s.refocus_on_reader(32 * MIN_CLAIM), vec![0]);

        // Everything the worker had not delivered is available again, and the
        // next claim starts exactly where the viewer is waiting — short.
        let next = s.claim(0).unwrap();
        assert_eq!(next.start, 220 * MIN_CLAIM);
        assert_eq!(next.issued_len(), 4 * MIN_CLAIM, "a head claim, not a slab");
    }

    #[test]
    fn a_claim_the_reader_is_about_to_reach_is_left_running() {
        // The mirror image: cutting a claim whose cursor is a hair behind the
        // read head would throw away a warm connection to save less than one
        // request's worth of bytes.
        let (mut s, big) = downloading_then_playing(4);
        big.advance_to(199 * MIN_CLAIM);
        s.set_read_head(200 * MIN_CLAIM);
        assert!(
            s.refocus_on_reader(32 * MIN_CLAIM).is_empty(),
            "a claim within a head claim's reach of the reader must keep going",
        );
    }

    #[test]
    fn over_long_claims_ahead_of_the_reader_are_shortened_not_killed() {
        // A claim that starts ahead of the reader is legitimate work, but a
        // throughput-sized one covers a stretch the pool should be splitting
        // into several short claims. Shorten it in place — no abort, no lost
        // bytes, and the tail comes back for re-cutting.
        let (mut s, big) = downloading_then_playing(4);
        let before = big.end();
        big.advance_to(4 * MIN_CLAIM);
        s.set_read_head(0);

        assert!(
            s.refocus_on_reader(32 * MIN_CLAIM).is_empty(),
            "shortening must not abort the worker",
        );
        // Shortened to one claim's worth from the cursor.
        assert_eq!(big.end(), 8 * MIN_CLAIM - 1);
        assert!(
            s.unclaimed_ranges()
                .iter()
                .any(|&(start, end)| start == 8 * MIN_CLAIM && end >= before),
            "the surrendered tail must be claimable again (merged with the gap \
             behind it); unclaimed = {:?}",
            s.unclaimed_ranges(),
        );
        // And the freed range is immediately re-cut into a short claim.
        let next = s.claim(1).unwrap();
        assert_eq!(next.start, 8 * MIN_CLAIM);
        assert_eq!(next.issued_len(), 4 * MIN_CLAIM);
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
        let vols = Arc::new(vec![
            vol(0, 10 * MIN_CLAIM),
            vol(10 * MIN_CLAIM, 10 * MIN_CLAIM),
        ]);
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
        let a = s.claim(0).unwrap();
        let before = s.unclaimed_bytes();
        s.finish(0, a.end() + 1);
        assert_eq!(s.unclaimed_bytes(), before);
    }

    #[test]
    fn drains_and_then_returns_none() {
        let mut s = Scheduler::new(
            0,
            2 * MIN_CLAIM - 1,
            Strategy::Throughput,
            None,
            2,
            2,
            None,
            &[],
        );
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

        assert_eq!(
            s.reclaim_stalled(),
            vec![0],
            "only the dead claim is re-cut"
        );
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
