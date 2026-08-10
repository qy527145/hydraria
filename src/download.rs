//! Server-side download jobs: multi-threaded, out-of-order, straight to a file.
//!
//! The proxy's `/stream/:id` endpoint has to deliver bytes in order — it *is* an
//! HTTP response body — so the byte the client is waiting for gates everything
//! behind it. That constraint is fundamental to streaming and fine for playback,
//! but it is exactly the wrong shape for "finish this file as fast as possible":
//! one slow request at the read head stalls visible progress even when every
//! other worker is delivering.
//!
//! A download job drops the constraint entirely. Workers claim ranges, fetch
//! them in any order, and `pwrite` at absolute offsets. Nothing reads the file
//! until it is complete, so there is no head, no reorder buffer, and no memory
//! that scales with concurrency — the same shape a dedicated downloader uses.
//!
//! Almost all of the machinery already existed:
//!
//! * [`crate::schedule::Scheduler`] with [`Strategy::Throughput`] — claims, work
//!   stealing, adaptive sizing, volume clipping.
//! * [`Engine::fetch_claim`] — one request per claim, resume-in-place on
//!   mid-stream failure, yields when its tail is stolen.
//! * [`CacheEntry`] — sparse file plus block bitmap, persisted on every
//!   completed block. Pointing one at a `.part` directory gives resume across
//!   restarts for free, and `staged_ranges` tells a resuming scheduler exactly
//!   what not to fetch again.

use crate::cache::{CacheEntry, CacheMeta};
use crate::engine::Engine;
use crate::error::{ProxyError, Result};
use crate::models::ThroughputSampler;
use crate::schedule::{Scheduler, Strategy};
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;
use tokio::task::AbortHandle;

/// Speed history kept per job, matching the dashboard's sparkline width.
const SPEED_SAMPLES: usize = 60;

/// Prefix of the working directory a job keeps beside its output file. Lives
/// *inside* the output directory on purpose: completing the download is then a
/// same-filesystem `rename`, not a multi-gigabyte copy.
const PART_PREFIX: &str = ".hydraria-part-";

/// Guard against a pathological loop when deduplicating output filenames.
const MAX_NAME_ATTEMPTS: u32 = 9999;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobState {
    Running,
    Paused,
    Done,
    Failed(String),
}

impl JobState {
    fn label(&self) -> &'static str {
        match self {
            JobState::Running => "running",
            JobState::Paused => "paused",
            JobState::Done => "done",
            JobState::Failed(_) => "failed",
        }
    }
}

/// What the dashboard needs to render a job, in one poll.
#[derive(Debug, Clone, Serialize)]
pub struct DownloadInfo {
    pub state: &'static str,
    pub error: Option<String>,
    pub out_dir: String,
    pub filename: String,
    /// Final path, set once the job completes (may differ from `filename` if a
    /// file of that name already existed).
    pub output_path: Option<String>,
    pub total_bytes: u64,
    pub done_bytes: u64,
    pub current_speed_bps: u64,
    pub speed_samples: Vec<u64>,
    pub threads: usize,
    /// Downsampled block bitmap — the UI renders it with the same heat strip it
    /// already uses for cache coverage, which for an out-of-order download is a
    /// genuinely useful picture of what's landed where.
    pub bitmap_summary: Vec<u8>,
}

/// Everything needed to recreate a job after a restart. The bytes themselves
/// are already on disk in the `.part` directory, so resuming costs nothing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedDownload {
    pub task_id: String,
    pub out_dir: String,
    pub filename: String,
    pub threads: usize,
    /// Whether the job was running when we last saved. Paused jobs stay paused.
    #[serde(default)]
    pub was_running: bool,
}

pub struct DownloadJob {
    pub task_id: String,
    pub out_dir: PathBuf,
    pub filename: String,
    pub total_size: u64,
    pub threads: usize,
    /// `.part` working file: sparse data + durable block bitmap.
    part: Arc<CacheEntry>,
    engine: Arc<Engine>,
    scheduler: Mutex<Scheduler>,
    /// Abort handles rather than `JoinHandle`s: pausing has to cancel in-flight
    /// requests (dropping the future is the only way to release the socket)
    /// while a separate supervisor awaits the same tasks to detect completion.
    aborts: Mutex<Vec<AbortHandle>>,
    state: Mutex<JobState>,
    output_path: Mutex<Option<PathBuf>>,
    /// Set by `pause`/`cancel` so a worker between claims exits promptly
    /// instead of picking up more work.
    stopping: AtomicBool,
    failures: AtomicUsize,
    failure_budget: usize,
    /// Bumped by every `start`. A supervisor only acts on the generation it was
    /// spawned for — otherwise a quick pause→resume could let the *previous*
    /// supervisor wake up, see the freshly-set `Running`, find the file
    /// incomplete, and wrongly fail a job that is running fine.
    generation: AtomicU64,
    /// `done_bytes()` as of the previous tick, so the speed gauge can be a
    /// delta of bytes actually on disk. Counting only at claim completion made
    /// the reading useless: mostly 0 B/s with a spike each time a large claim
    /// landed (observed 0.00 / 0.00 / 63.85 MB/s on a job averaging ~4 MB/s).
    last_done_bytes: AtomicU64,
    current_speed_bps: AtomicU64,
    throughput: Arc<ThroughputSampler>,
    last_sample: Mutex<Instant>,
}

impl DownloadJob {
    /// Working directory for `task_id` under `out_dir`.
    pub fn part_dir(out_dir: &Path, task_id: &str) -> PathBuf {
        out_dir.join(format!("{PART_PREFIX}{task_id}"))
    }

    /// Open (or reopen, after a restart) a job's `.part` file and build its
    /// scheduler seeded with whatever is already on disk.
    ///
    /// `meta` carries the upstream's identity — a changed ETag or size makes
    /// `CacheEntry::open_at` wipe the partial rather than stitch new bytes into
    /// stale ones.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        engine: Arc<Engine>,
        task_id: String,
        out_dir: PathBuf,
        filename: String,
        meta: CacheMeta,
        threads: usize,
    ) -> Result<Arc<Self>> {
        let total_size = meta.total_size;
        if total_size == 0 {
            return Err(ProxyError::Internal(
                "cannot download a zero-length or unknown-size upstream".into(),
            ));
        }
        std::fs::create_dir_all(&out_dir).map_err(ProxyError::Io)?;
        let part_dir = Self::part_dir(&out_dir, &task_id);
        let part = CacheEntry::open_at(part_dir, &task_id, meta, false)?;

        let threads = threads.max(1);
        let resumed: u64 = part
            .staged_ranges(0, total_size - 1)
            .iter()
            .map(|&(s, e)| e - s + 1)
            .sum();
        let scheduler = build_scheduler(&part, &engine, total_size, threads);
        if resumed > 0 {
            tracing::info!(
                "download task={} resuming with {} of {} bytes already on disk",
                task_id,
                resumed,
                total_size,
            );
        }

        Ok(Arc::new(Self {
            task_id,
            out_dir,
            filename,
            total_size,
            threads,
            part,
            engine,
            scheduler: Mutex::new(scheduler),
            aborts: Mutex::new(Vec::new()),
            state: Mutex::new(JobState::Paused),
            output_path: Mutex::new(None),
            stopping: AtomicBool::new(false),
            failures: AtomicUsize::new(0),
            failure_budget: threads.saturating_mul(2).max(8),
            generation: AtomicU64::new(0),
            last_done_bytes: AtomicU64::new(0),
            current_speed_bps: AtomicU64::new(0),
            throughput: Arc::new(ThroughputSampler::new(SPEED_SAMPLES)),
            last_sample: Mutex::new(Instant::now()),
        }))
    }

    pub fn state(&self) -> JobState {
        self.state.lock().clone()
    }

    pub fn done_bytes(&self) -> u64 {
        self.part.bytes_cached.load(Ordering::Relaxed).min(self.total_size)
    }

    pub fn is_active(&self) -> bool {
        matches!(self.state(), JobState::Running)
    }

    pub fn info(&self) -> DownloadInfo {
        let st = self.state();
        DownloadInfo {
            state: st.label(),
            error: match &st {
                JobState::Failed(e) => Some(e.clone()),
                _ => None,
            },
            out_dir: self.out_dir.to_string_lossy().into_owned(),
            filename: self.filename.clone(),
            output_path: self
                .output_path
                .lock()
                .as_ref()
                .map(|p| p.to_string_lossy().into_owned()),
            total_bytes: self.total_size,
            done_bytes: self.done_bytes(),
            current_speed_bps: self.current_speed_bps.load(Ordering::Relaxed),
            speed_samples: self.throughput.snapshot(),
            threads: self.threads,
            bitmap_summary: self.part.stats().bitmap_summary,
        }
    }

    pub fn to_persisted(&self) -> PersistedDownload {
        PersistedDownload {
            task_id: self.task_id.clone(),
            out_dir: self.out_dir.to_string_lossy().into_owned(),
            filename: self.filename.clone(),
            threads: self.threads,
            was_running: self.is_active(),
        }
    }

    /// Fold this second's bytes into the speed gauge. Driven by the app's
    /// existing one-second ticker, same as `TaskEntry::tick_throughput`.
    pub fn tick_throughput(&self) {
        let now = Instant::now();
        let mut last = self.last_sample.lock();
        let elapsed = now.duration_since(*last).as_secs_f64().max(0.001);
        *last = now;
        let done = self.done_bytes();
        let prev = self.last_done_bytes.swap(done, Ordering::Relaxed);
        // Block-granular (1 MiB) and monotonic, unlike a per-claim counter.
        let bps = ((done.saturating_sub(prev)) as f64 / elapsed) as u64;
        self.throughput.push(bps);
        // Report a short moving average, not the raw sample. Blocks complete in
        // bursts — 16 workers each writing inside a 64 MiB claim finish blocks
        // unevenly — so the instantaneous value alternates between 0 and large
        // spikes on a job that is in fact moving steadily. The sparkline keeps
        // the raw samples; the headline number should be readable.
        self.current_speed_bps
            .store(self.throughput.recent_mean(4), Ordering::Relaxed);
    }

    /// Start (or restart) the worker pool. No-op when already running or
    /// finished.
    pub fn start(self: &Arc<Self>) {
        {
            let mut st = self.state.lock();
            match &*st {
                JobState::Running => return,
                JobState::Done => return,
                _ => *st = JobState::Running,
            }
        }
        self.stopping.store(false, Ordering::Relaxed);
        self.failures.store(0, Ordering::Relaxed);
        // Seed the speed watermark with what's already present, so a resumed job
        // doesn't report its entire recovered prefix as one tick of throughput.
        self.last_done_bytes
            .store(self.done_bytes(), Ordering::Relaxed);
        *self.last_sample.lock() = Instant::now();
        // Rebuild the scheduler from the bitmap on every start.
        //
        // Pausing aborts workers mid-claim, and an aborted worker never gets to
        // hand its claim back — so its byte range would otherwise be stranded:
        // neither unclaimed nor being fetched. A resumed job then runs out of
        // work a few megabytes short and can never finish. The bitmap is the
        // only authoritative answer to "what is actually on disk", so deriving
        // the remaining work from it makes resume correct by construction,
        // whether we're recovering from a pause, a crash, or a restart.
        *self.scheduler.lock() = self.fresh_scheduler();

        let mut handles = Vec::with_capacity(self.threads);
        let mut aborts = Vec::with_capacity(self.threads);
        for worker in 0..self.threads {
            let me = Arc::clone(self);
            let h = tokio::spawn(async move { me.run_worker(worker).await });
            aborts.push(h.abort_handle());
            handles.push(h);
        }
        *self.aborts.lock() = aborts;

        // Supervisor: the workers all exit either because the file is complete,
        // because they were aborted (pause/cancel), or because the failure
        // budget blew. Joining them here is the only place that can tell those
        // apart without racing.
        let generation = self.generation.fetch_add(1, Ordering::Relaxed) + 1;
        let me = Arc::clone(self);
        tokio::spawn(async move {
            for h in handles {
                let _ = h.await;
            }
            me.on_workers_finished(generation);
        });
        tracing::info!(
            "download task={} started with {} workers ({} of {} bytes present)",
            self.task_id,
            self.threads,
            self.done_bytes(),
            self.total_size,
        );
    }

    /// A scheduler covering exactly the bytes still missing from the `.part`
    /// file, per its block bitmap.
    fn fresh_scheduler(&self) -> Scheduler {
        build_scheduler(&self.part, &self.engine, self.total_size, self.threads)
    }

    /// Stop fetching but keep the `.part` file so `start` can resume.
    pub fn pause(&self) {
        {
            let mut st = self.state.lock();
            if !matches!(&*st, JobState::Running) {
                return;
            }
            *st = JobState::Paused;
        }
        self.abort_workers();
        tracing::info!("download task={} paused", self.task_id);
    }

    /// Stop fetching and discard the partial data.
    pub fn cancel(&self) {
        *self.state.lock() = JobState::Paused;
        self.abort_workers();
        let dir = Self::part_dir(&self.out_dir, &self.task_id);
        if dir.exists() {
            if let Err(e) = std::fs::remove_dir_all(&dir) {
                tracing::warn!("failed to remove part dir {}: {}", dir.display(), e);
            }
        }
        tracing::info!("download task={} cancelled, partial discarded", self.task_id);
    }

    fn abort_workers(&self) {
        self.stopping.store(true, Ordering::Relaxed);
        for a in self.aborts.lock().drain(..) {
            a.abort();
        }
    }

    async fn run_worker(self: Arc<Self>, worker: usize) {
        loop {
            if self.stopping.load(Ordering::Relaxed) {
                break;
            }
            let claim = self.scheduler.lock().claim(worker);
            let Some(claim) = claim else { break };
            // `transform_on_write = true`: the file on disk is the deliverable,
            // so an encrypted task must land as plaintext.
            let res = self
                .engine
                .fetch_claim(worker, &claim, &self.part, true)
                .await;
            self.scheduler.lock().finish(worker, claim.cursor());
            match res {
                Ok(()) => self.scheduler.lock().note_claim_outcome(true),
                Err(e) => {
                    self.scheduler.lock().note_claim_outcome(false);
                    let n = self.failures.fetch_add(1, Ordering::Relaxed) + 1;
                    tracing::debug!(
                        "download task={} worker={} claim failed ({}/{}): {}",
                        self.task_id,
                        worker,
                        n,
                        self.failure_budget,
                        e,
                    );
                    if n >= self.failure_budget {
                        *self.state.lock() = JobState::Failed(e.to_string());
                        self.stopping.store(true, Ordering::Relaxed);
                        break;
                    }
                }
            }
        }
    }

    /// Called once, after every worker of generation `generation` has exited.
    fn on_workers_finished(&self, generation: u64) {
        // A newer `start` has already superseded us — its own supervisor owns
        // the outcome.
        if self.generation.load(Ordering::Relaxed) != generation {
            return;
        }
        // Pause / cancel / a blown failure budget already set the state; don't
        // second-guess them.
        if !matches!(&*self.state.lock(), JobState::Running) {
            return;
        }
        let have = self.part.contiguous_from(0, self.total_size);
        if have < self.total_size {
            let msg = format!(
                "workers exited with {have} of {} bytes fetched",
                self.total_size
            );
            tracing::warn!("download task={} incomplete: {}", self.task_id, msg);
            *self.state.lock() = JobState::Failed(msg);
            return;
        }
        match self.finalize() {
            Ok(path) => {
                *self.output_path.lock() = Some(path.clone());
                *self.state.lock() = JobState::Done;
                tracing::info!(
                    "download task={} complete → {}",
                    self.task_id,
                    path.display(),
                );
            }
            Err(e) => {
                tracing::error!("download task={} finalize failed: {}", self.task_id, e);
                *self.state.lock() = JobState::Failed(e.to_string());
            }
        }
    }

    /// Move the completed data file into place and tear down the `.part`
    /// directory. The rename is within one filesystem (the part directory is a
    /// child of the output directory), so it's atomic and instant regardless of
    /// file size.
    fn finalize(&self) -> Result<PathBuf> {
        let dest = unique_path(&self.out_dir, &self.filename);
        let src = self.part.data_path();
        std::fs::rename(&src, &dest).map_err(ProxyError::Io)?;
        let dir = Self::part_dir(&self.out_dir, &self.task_id);
        if let Err(e) = std::fs::remove_dir_all(&dir) {
            // The payload is already safely in place; a leftover bookkeeping
            // directory is worth a warning, not a failure.
            tracing::warn!(
                "download task={} left part dir behind ({}): {}",
                self.task_id,
                dir.display(),
                e,
            );
        }
        Ok(dest)
    }
}

/// Scheduler covering the bytes `part` is still missing. The bitmap is the
/// authority on what's on disk, which is what makes resume correct regardless
/// of how the previous attempt ended.
fn build_scheduler(
    part: &CacheEntry,
    engine: &Engine,
    total_size: u64,
    threads: usize,
) -> Scheduler {
    let already = part.staged_ranges(0, total_size - 1);
    Scheduler::new(
        0,
        total_size - 1,
        Strategy::Throughput,
        engine.volumes(),
        threads,
        threads,
        None,
        &already,
    )
}

pub struct DownloadManager {
    jobs: RwLock<HashMap<String, Arc<DownloadJob>>>,
}

impl Default for DownloadManager {
    fn default() -> Self {
        Self::new()
    }
}

impl DownloadManager {
    pub fn new() -> Self {
        Self {
            jobs: RwLock::new(HashMap::new()),
        }
    }

    pub fn get(&self, task_id: &str) -> Option<Arc<DownloadJob>> {
        self.jobs.read().get(task_id).cloned()
    }

    /// Register `job` for `task_id`, cancelling and replacing any predecessor.
    pub fn insert(&self, job: Arc<DownloadJob>) {
        let prev = self.jobs.write().insert(job.task_id.clone(), job);
        if let Some(p) = prev {
            p.pause();
        }
    }

    /// Forget the job, leaving its `.part` data alone. Callers that want the
    /// partial gone call `DownloadJob::cancel` first.
    pub fn remove(&self, task_id: &str) -> Option<Arc<DownloadJob>> {
        self.jobs.write().remove(task_id)
    }

    pub fn info(&self, task_id: &str) -> Option<DownloadInfo> {
        self.jobs.read().get(task_id).map(|j| j.info())
    }

    pub fn tick_throughput(&self) {
        for j in self.jobs.read().values() {
            j.tick_throughput();
        }
    }

    pub fn persisted(&self) -> Vec<PersistedDownload> {
        self.jobs
            .read()
            .values()
            .filter(|j| !matches!(j.state(), JobState::Done))
            .map(|j| j.to_persisted())
            .collect()
    }
}

/// Split `name` into (stem, extension) for deduplication, treating a leading
/// dot as part of the stem so `.bashrc` doesn't become `(1).bashrc`.
fn split_ext(name: &str) -> (&str, Option<&str>) {
    match name.rfind('.') {
        Some(i) if i > 0 && i + 1 < name.len() => (&name[..i], Some(&name[i + 1..])),
        _ => (name, None),
    }
}

/// First free path for `filename` in `dir`, appending ` (1)`, ` (2)`, … as
/// needed. Never returns a path that already exists — silently overwriting
/// somebody's file is not an acceptable outcome of pressing "download".
pub fn unique_path(dir: &Path, filename: &str) -> PathBuf {
    let base = dir.join(filename);
    if !base.exists() {
        return base;
    }
    let (stem, ext) = split_ext(filename);
    for n in 1..=MAX_NAME_ATTEMPTS {
        let candidate = match ext {
            Some(e) => dir.join(format!("{stem} ({n}).{e}")),
            None => dir.join(format!("{stem} ({n})")),
        };
        if !candidate.exists() {
            return candidate;
        }
    }
    // Pathological: ~10k collisions. Fall back to a name that can't collide
    // with the numbered series rather than looping forever.
    dir.join(format!("{stem}-{}", uuid::Uuid::new_v4().simple()))
}

/// Reduce a user- or upstream-supplied name to a single safe path component.
/// Returns `None` when nothing usable is left.
pub fn sanitize_filename(name: &str) -> Option<String> {
    let cleaned: String = name
        .chars()
        .filter(|c| !c.is_control() && *c != '/' && *c != '\\')
        .collect();
    let trimmed = cleaned.trim().trim_matches('.').trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch() -> PathBuf {
        // Per-process counter rather than a timestamp — see the same note in
        // `cache::tests::fresh_store`. Colliding scratch dirs made parallel
        // tests delete each other's files.
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let d = std::env::temp_dir().join(format!(
            "hydraria-dl-test-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed),
        ));
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    #[test]
    fn unique_path_returns_the_plain_name_when_free() {
        let d = scratch();
        assert_eq!(unique_path(&d, "a.mkv"), d.join("a.mkv"));
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn unique_path_never_overwrites_an_existing_file() {
        let d = scratch();
        std::fs::write(d.join("movie.mkv"), b"x").unwrap();
        assert_eq!(unique_path(&d, "movie.mkv"), d.join("movie (1).mkv"));
        std::fs::write(d.join("movie (1).mkv"), b"x").unwrap();
        assert_eq!(unique_path(&d, "movie.mkv"), d.join("movie (2).mkv"));
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn unique_path_handles_names_without_an_extension() {
        let d = scratch();
        std::fs::write(d.join("blob"), b"x").unwrap();
        assert_eq!(unique_path(&d, "blob"), d.join("blob (1)"));
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn split_ext_keeps_dotfiles_intact() {
        assert_eq!(split_ext("a.mkv"), ("a", Some("mkv")));
        assert_eq!(split_ext("archive.tar.gz"), ("archive.tar", Some("gz")));
        assert_eq!(split_ext("blob"), ("blob", None));
        // Leading dot is part of the stem, not an empty-stem extension.
        assert_eq!(split_ext(".bashrc"), (".bashrc", None));
        // Trailing dot has no extension after it.
        assert_eq!(split_ext("weird."), ("weird.", None));
    }

    #[test]
    fn sanitize_filename_strips_separators_and_traversal() {
        assert_eq!(sanitize_filename("../../etc/passwd").as_deref(), Some("etcpasswd"));
        assert_eq!(sanitize_filename("a/b\\c.mkv").as_deref(), Some("abc.mkv"));
        assert_eq!(sanitize_filename("  spaced.mkv  ").as_deref(), Some("spaced.mkv"));
        assert_eq!(sanitize_filename("..").as_deref(), None);
        assert_eq!(sanitize_filename("   ").as_deref(), None);
    }

    #[test]
    fn part_dir_is_a_child_of_the_output_dir() {
        // Same-filesystem rename on completion depends on this.
        let out = Path::new("/tmp/dls");
        let p = DownloadJob::part_dir(out, "abc123");
        assert_eq!(p.parent(), Some(out));
        assert!(
            p.file_name().unwrap().to_string_lossy().starts_with('.'),
            "part dir should be hidden",
        );
    }

    #[test]
    fn job_state_labels_are_stable_for_the_api() {
        assert_eq!(JobState::Running.label(), "running");
        assert_eq!(JobState::Paused.label(), "paused");
        assert_eq!(JobState::Done.label(), "done");
        assert_eq!(JobState::Failed("x".into()).label(), "failed");
    }
}
