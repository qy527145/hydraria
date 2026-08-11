//! Task-level persistent cache coordinator.
//!
//! Playback and explicit whole-file caching share one sparse [`CacheEntry`] and
//! one worker pool, so neither ever refetches what the other already wrote.
//! The two scenarios differ only in how the shared [`Scheduler`] is tuned:
//!
//! * A playback reader puts it in latency mode, pins the critical window to the
//!   reader's position, and bounds the pool to [`PLAYBACK_HORIZON`] bytes ahead
//!   — watching part of a file must not quietly download all of it.
//! * An explicit cache request lifts that bound (and drops back to throughput
//!   mode once no reader is left), so the whole file is fair game.
//!
//! Both can be on at once: the reader's window keeps priority while spare
//! workers fill the rest of the file.
use crate::cache::CacheEntry;
use crate::engine::Engine;
use crate::error::{ProxyError, Result};
use crate::models::ThroughputSampler;
use crate::schedule::{DEFAULT_CRITICAL_WINDOW, Scheduler, Strategy};
use bytes::Bytes;
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;
use tokio::sync::mpsc;
use tokio::task::AbortHandle;
const SPEED_SAMPLES: usize = 60;
const READ_CHUNK: u64 = crate::cache::BLOCK_SIZE;
/// How far ahead of a playback reader the pool may work while caching is *not*
/// requested. Large enough that a fast link stays saturated and the reader keeps
/// a comfortable buffer, small enough that opening a 20 GB file doesn't pull the
/// whole thing down behind the user's back.
pub const PLAYBACK_HORIZON: u64 = 256 * 1024 * 1024;
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobState {
    Idle,
    Running,
    Paused,
    Done,
    Failed(String),
}
impl JobState {
    fn label(&self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Running => "running",
            Self::Paused => "paused",
            Self::Done => "done",
            Self::Failed(_) => "failed",
        }
    }
}
#[derive(Debug, Clone, Serialize)]
pub struct CacheJobInfo {
    pub state: &'static str,
    pub error: Option<String>,
    pub total_bytes: u64,
    pub done_bytes: u64,
    pub current_speed_bps: u64,
    pub speed_samples: Vec<u64>,
    pub threads: usize,
    pub active_readers: usize,
    pub bitmap_summary: Vec<u8>,
}
/// Backward-compatible persisted shape. Old builds called these downloads and
/// stored output fields; serde defaults let us read both forms while only the
/// task id, worker count and running state matter to cache warming.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedDownload {
    pub task_id: String,
    #[serde(default)]
    pub out_dir: String,
    #[serde(default)]
    pub filename: String,
    #[serde(default)]
    pub threads: usize,
    #[serde(default)]
    pub was_running: bool,
}
/// The shared worker pool. One slot per configured thread; a slot stays
/// occupied until its task retires, so a slot is never staffed twice and the
/// scheduler can rely on one live claim per worker id.
struct Workers {
    /// Bumped to tell the current tasks to stop taking new claims.
    generation: u64,
    handles: Vec<Option<Slot>>,
}

struct Slot {
    /// Generation this task was started for; a task only clears its own slot.
    generation: u64,
    handle: AbortHandle,
}

impl Workers {
    fn new(threads: usize) -> Self {
        Self {
            generation: 1,
            handles: (0..threads).map(|_| None).collect(),
        }
    }
}

pub struct CacheJob {
    pub task_id: String,
    total_size: u64,
    threads: usize,
    engine: Arc<Engine>,
    entry: Arc<CacheEntry>,
    scheduler: Mutex<Scheduler>,
    workers: Mutex<Workers>,
    state: Mutex<JobState>,
    cache_requested: AtomicBool,
    /// Live playback readers by id, each holding its current read offset.
    readers: RwLock<HashMap<u64, u64>>,
    next_reader: AtomicU64,
    failures: AtomicUsize,
    failure_budget: usize,
    last_done_bytes: AtomicU64,
    current_speed_bps: AtomicU64,
    throughput: Arc<ThroughputSampler>,
    last_sample: Mutex<Instant>,
}
impl CacheJob {
    pub fn new(task_id: String, engine: Arc<Engine>, entry: Arc<CacheEntry>) -> Result<Arc<Self>> {
        let total_size = entry.meta.total_size;
        if total_size == 0 {
            return Err(ProxyError::Internal("cannot cache an unknown-size upstream".into()));
        }
        let threads = engine.config().max_threads.max(1);
        let scheduler = build_scheduler(&entry, &engine, total_size, threads, Strategy::Throughput);
        let complete = entry.contiguous_from(0, total_size) >= total_size;
        let initial_done = entry.bytes_cached.load(Ordering::Relaxed);
        Ok(Arc::new(Self {
            task_id,
            total_size,
            threads,
            engine,
            entry,
            scheduler: Mutex::new(scheduler),
            workers: Mutex::new(Workers::new(threads)),
            state: Mutex::new(if complete { JobState::Done } else { JobState::Idle }),
            cache_requested: AtomicBool::new(false),
            readers: RwLock::new(HashMap::new()),
            next_reader: AtomicU64::new(1),
            failures: AtomicUsize::new(0),
            failure_budget: threads.saturating_mul(2).max(8),
            last_done_bytes: AtomicU64::new(initial_done),
            current_speed_bps: AtomicU64::new(0),
            throughput: Arc::new(ThroughputSampler::new(SPEED_SAMPLES)),
            last_sample: Mutex::new(Instant::now()),
        }))
    }
    pub fn entry(&self) -> Arc<CacheEntry> {
        Arc::clone(&self.entry)
    }
    pub fn info(&self) -> CacheJobInfo {
        let state = self.state.lock().clone();
        let stats = self.entry.stats();
        CacheJobInfo {
            state: state.label(),
            error: match state { JobState::Failed(e) => Some(e), _ => None },
            total_bytes: self.total_size,
            done_bytes: stats.bytes_cached.min(self.total_size),
            current_speed_bps: self.current_speed_bps.load(Ordering::Relaxed),
            speed_samples: self.throughput.snapshot(),
            threads: self.threads,
            active_readers: self.readers.read().len(),
            bitmap_summary: stats.bitmap_summary,
        }
    }
    pub fn is_cache_active(&self) -> bool {
        self.cache_requested.load(Ordering::Relaxed)
    }
    pub fn start_cache(self: &Arc<Self>) {
        if self.entry.contiguous_from(0, self.total_size) >= self.total_size {
            *self.state.lock() = JobState::Done;
            self.cache_requested.store(false, Ordering::Relaxed);
            return;
        }
        self.cache_requested.store(true, Ordering::Relaxed);
        *self.state.lock() = JobState::Running;
        self.failures.store(0, Ordering::Relaxed);
        self.reset_speed_baseline();
        self.retune();
        self.ensure_workers();
    }
    /// Stop filling the rest of the file. Playback keeps running and keeps
    /// filling around its own position, so the pool is re-bounded rather than
    /// shut down whenever a reader is still attached.
    pub fn pause_cache(self: &Arc<Self>) {
        self.cache_requested.store(false, Ordering::Relaxed);
        {
            let mut state = self.state.lock();
            if !matches!(*state, JobState::Done) {
                *state = JobState::Paused;
            }
        }
        if self.readers.read().is_empty() {
            self.drain_workers();
        } else {
            // A reader is still attached: re-bound the pool to its horizon and
            // let the in-flight claims land (their bytes are already paid for).
            // The draining workers restaff themselves under the new bound.
            self.retune();
            self.drain_workers();
            self.ensure_workers();
        }
    }
    pub fn stop(&self) {
        self.cache_requested.store(false, Ordering::Relaxed);
        self.abort_workers();
    }
    pub fn to_persisted(&self) -> PersistedDownload {
        PersistedDownload {
            task_id: self.task_id.clone(),
            out_dir: String::new(),
            filename: String::new(),
            threads: self.threads,
            was_running: self.is_cache_active(),
        }
    }
    pub fn tick_throughput(&self) {
        let now = Instant::now();
        let mut last = self.last_sample.lock();
        let elapsed = now.duration_since(*last).as_secs_f64().max(0.001);
        *last = now;
        let done = self.entry.bytes_cached.load(Ordering::Relaxed).min(self.total_size);
        let previous = self.last_done_bytes.swap(done, Ordering::Relaxed);
        let bps = ((done.saturating_sub(previous)) as f64 / elapsed) as u64;
        self.throughput.push(bps);
        self.current_speed_bps.store(self.throughput.recent_mean(4), Ordering::Relaxed);
    }
    /// Add a playback reader. A fresh reader is a seek: point the scheduler at
    /// the new position and hand back the in-flight requests that no longer
    /// matter, so the pool converges on the reader within one request instead of
    /// waiting out whatever it was already fetching.
    pub fn stream(self: &Arc<Self>, start: u64, end: u64) -> mpsc::Receiver<Result<Bytes>> {
        let id = self.next_reader.fetch_add(1, Ordering::Relaxed);
        self.readers.write().insert(id, start);
        self.retune();
        let stale = self
            .scheduler
            .lock()
            .reclaim_outside_window(DEFAULT_CRITICAL_WINDOW);
        self.respawn(&stale);
        self.ensure_workers();
        let (tx, rx) = mpsc::channel(8);
        let me = Arc::clone(self);
        tokio::spawn(async move { me.run_reader(id, start, end, tx).await });
        rx
    }
    /// Point the scheduler at whatever is currently going on: a reader present
    /// means latency mode around the lowest read head, and the pool is bounded
    /// to [`PLAYBACK_HORIZON`] unless the whole file was explicitly requested.
    fn retune(&self) {
        let head = self.readers.read().values().copied().min();
        let mut scheduler = self.scheduler.lock();
        match head {
            Some(head) => {
                scheduler.set_read_head(head);
                scheduler.set_strategy(Strategy::latency_default());
                scheduler.set_work_limit(if self.is_cache_active() {
                    None
                } else {
                    Some(PLAYBACK_HORIZON)
                });
            }
            None => {
                scheduler.set_strategy(Strategy::Throughput);
                scheduler.set_work_limit(None);
            }
        }
    }
    /// Fill every idle worker slot. Cheap and idempotent, so anything that
    /// creates new work (a cache request, a reader that moved past its filled
    /// horizon) can just call it. Slots still occupied by a draining task are
    /// left alone — that task restaffs them when it retires.
    fn ensure_workers(self: &Arc<Self>) {
        if !self.is_cache_active() && self.readers.read().is_empty() {
            return;
        }
        let mut workers = self.workers.lock();
        let generation = workers.generation;
        for worker in 0..self.threads {
            if workers.handles[worker].is_some() {
                continue;
            }
            let job = Arc::clone(self);
            let handle = tokio::spawn(async move { job.run_worker(worker, generation).await });
            workers.handles[worker] = Some(Slot {
                generation,
                handle: handle.abort_handle(),
            });
        }
    }
    /// Abort the listed workers' in-flight requests and start replacements.
    /// Their claims must already have been handed back to the scheduler (see
    /// [`Scheduler::reclaim_outside_window`]) — letting a worker keep writing a
    /// range that is unclaimed again is exactly the duplicate work we're trying
    /// to avoid, so here cancelling is the point.
    fn respawn(self: &Arc<Self>, stale: &[usize]) {
        if stale.is_empty() {
            return;
        }
        let mut workers = self.workers.lock();
        let generation = workers.generation;
        for &worker in stale {
            if let Some(slot) = workers.handles[worker].take() {
                slot.handle.abort();
            }
            let job = Arc::clone(self);
            let handle = tokio::spawn(async move { job.run_worker(worker, generation).await });
            workers.handles[worker] = Some(Slot {
                generation,
                handle: handle.abort_handle(),
            });
        }
    }
    /// Stop handing out new claims, but let the requests already in flight land
    /// in the cache.
    ///
    /// Those bytes are already paid for; cancelling the sockets throws away
    /// everything that hadn't reached a block boundary and the next reader
    /// fetches the same ranges again — measured at ~18 MB discarded per
    /// disconnect with 8 workers. Bandwidth still stops within one claim, which
    /// is what pausing has to mean in practice.
    fn drain_workers(&self) {
        let mut workers = self.workers.lock();
        workers.generation = workers.generation.wrapping_add(1);
    }
    /// Hard cancel, for a job that is going away entirely (task deleted, cache
    /// cleared, coordinator replaced) where landing more bytes is pointless.
    fn abort_workers(&self) {
        let mut workers = self.workers.lock();
        workers.generation = workers.generation.wrapping_add(1);
        for slot in workers.handles.iter_mut() {
            if let Some(slot) = slot.take() {
                slot.handle.abort();
            }
        }
    }
    /// Release the worker's slot on exit so it can be staffed again. Only the
    /// task that owns the slot clears it, so a slot never holds two tasks.
    fn retire(&self, worker: usize, generation: u64) {
        let mut workers = self.workers.lock();
        if workers.handles[worker]
            .as_ref()
            .is_some_and(|slot| slot.generation == generation)
        {
            workers.handles[worker] = None;
        }
    }
    fn reset_speed_baseline(&self) {
        self.last_done_bytes.store(
            self.entry.bytes_cached.load(Ordering::Relaxed),
            Ordering::Relaxed,
        );
        *self.last_sample.lock() = Instant::now();
    }
    async fn run_worker(self: Arc<Self>, worker: usize, generation: u64) {
        // `superseded` separates "a newer generation took over" from "nothing
        // left to claim". Only the former restaffs the slot; doing it for the
        // latter would spin, because the replacement finds nothing either.
        let mut superseded = false;
        loop {
            if self.workers.lock().generation != generation {
                superseded = true;
                break;
            }
            let claim = self.scheduler.lock().claim(worker);
            let Some(claim) = claim else { break };
            let result = self.engine.fetch_claim(worker, &claim, &self.entry, false).await;
            self.scheduler.lock().finish(worker, claim.cursor());
            match result {
                Ok(()) => self.scheduler.lock().note_claim_outcome(true),
                Err(error) => {
                    self.scheduler.lock().note_claim_outcome(false);
                    let failures = self.failures.fetch_add(1, Ordering::Relaxed) + 1;
                    if failures >= self.failure_budget {
                        *self.state.lock() = JobState::Failed(error.to_string());
                        self.cache_requested.store(false, Ordering::Relaxed);
                        self.abort_workers();
                        return;
                    }
                }
            }
        }
        self.retire(worker, generation);
        if self.entry.contiguous_from(0, self.total_size) >= self.total_size {
            self.cache_requested.store(false, Ordering::Relaxed);
            let mut state = self.state.lock();
            if !matches!(*state, JobState::Failed(_)) {
                *state = JobState::Done;
            }
        } else if superseded {
            // Demand may have outlived the generation we drained for — e.g. a
            // reader arrived while this worker was landing its last claim.
            self.ensure_workers();
        }
    }
    async fn run_reader(
        self: Arc<Self>,
        id: u64,
        start: u64,
        end: u64,
        tx: mpsc::Sender<Result<Bytes>>,
    ) {
        let mut writes = self.entry.subscribe_progress();
        let mut cursor = start;
        while cursor <= end {
            let want = (end - cursor + 1).min(READ_CHUNK);
            let available = self.entry.contiguous_from(cursor, want);
            if available == 0 {
                // Nothing here yet. Aim the pool at this position and make sure
                // it is running: a bounded playback pool retires its workers
                // once the horizon is full, so the reader catching up is exactly
                // what has to wake them again.
                self.retune();
                self.ensure_workers();
                tokio::select! {
                    changed = writes.changed() => {
                        if changed.is_err() { break; }
                    }
                    _ = tx.closed() => break,
                }
                continue;
            }
            let entry = Arc::clone(&self.entry);
            let lo = cursor;
            let hi = cursor + available - 1;
            let read = tokio::task::spawn_blocking(move || entry.read_range(lo, hi)).await;
            let bytes = match read {
                Ok(Ok(bytes)) => bytes,
                Ok(Err(error)) => {
                    let _ = tx.send(Err(ProxyError::Io(error))).await;
                    break;
                }
                Err(error) => {
                    let _ = tx.send(Err(ProxyError::Internal(error.to_string()))).await;
                    break;
                }
            };
            let output = self.engine.transform_outgoing_public(cursor, bytes);
            if tx.send(Ok(output)).await.is_err() {
                break;
            }
            cursor += available;
            self.readers.write().insert(id, cursor);
            self.scheduler.lock().set_read_head(cursor);
        }
        self.readers.write().remove(&id);
        if self.readers.read().is_empty() {
            if self.is_cache_active() {
                // Last reader gone: back to sweeping the whole file at full width.
                self.retune();
                self.ensure_workers();
            } else {
                // Nobody is watching and nobody asked for the whole file. Stop
                // taking new claims, but keep whatever is already on the wire —
                // a player that reconnects (or a later cache request) reuses it.
                self.drain_workers();
            }
        } else {
            // Other readers remain; re-derive the head from the lowest of them.
            self.retune();
        }
    }
}
fn build_scheduler(
    entry: &CacheEntry,
    engine: &Engine,
    total_size: u64,
    threads: usize,
    strategy: Strategy,
) -> Scheduler {
    let already = entry.staged_ranges(0, total_size - 1);
    Scheduler::new(
       0,
        total_size - 1,
        strategy,
        engine.volumes(),
        threads,
        engine.config().max_per_volume.max(1),
        Some(engine.config().max_split).filter(|value| *value > 0),
        &already,
    )
}
pub struct DownloadManager {
    jobs: RwLock<HashMap<String, Arc<CacheJob>>>,
}
impl Default for DownloadManager {
    fn default() -> Self { Self::new() }
}
impl DownloadManager {
    pub fn new() -> Self { Self { jobs: RwLock::new(HashMap::new()) } }
    pub fn get(&self, task_id: &str) -> Option<Arc<CacheJob>> {
        self.jobs.read().get(task_id).cloned()
    }
    pub fn insert(&self, job: Arc<CacheJob>) {
        if let Some(previous) = self.jobs.write().insert(job.task_id.clone(), job) {
            previous.stop();
        }
    }
    pub fn remove(&self, task_id: &str) -> Option<Arc<CacheJob>> {
        self.jobs.write().remove(task_id)
    }
    /// Detach every coordinator holding `key`, returning how many were stopped.
    ///
    /// The cache key is derived from a task's URL list, so two tasks pointing at
    /// the same content share one entry — and clearing it has to stop *all* of
    /// them. Stopping only the requesting task left the others writing into a
    /// directory that no longer existed, which surfaced as the whole job failing
    /// with ENOENT.
    pub fn stop_for_key(&self, key: &str) -> usize {
        let mut jobs = self.jobs.write();
        let doomed: Vec<String> = jobs
            .iter()
            .filter(|(_, job)| job.entry().key == key)
            .map(|(id, _)| id.clone())
            .collect();
        for id in &doomed {
            if let Some(job) = jobs.remove(id) {
                job.stop();
            }
        }
        doomed.len()
    }
    /// Detach every coordinator, for a global cache wipe.
    pub fn stop_all(&self) {
        for (_, job) in self.jobs.write().drain() {
            job.stop();
        }
    }
    pub fn info(&self, task_id: &str) -> Option<CacheJobInfo> {
        self.jobs.read().get(task_id).map(|job| job.info())
    }
    pub fn tick_throughput(&self) {
        for job in self.jobs.read().values() { job.tick_throughput(); }
    }
    pub fn persisted(&self) -> Vec<PersistedDownload> {
        self.jobs.read().values()
            .filter(|job| job.is_cache_active() || matches!(*job.state.lock(), JobState::Paused))
            .map(|job| job.to_persisted())
            .collect()
    }
}
