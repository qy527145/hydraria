use crate::error::{ProxyError, Result};
use bytes::Bytes;
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Positional read at `offset`. Cross-platform wrapper:
/// - Unix uses `FileExt::read_exact_at` directly.
/// - Windows uses `seek_read` in a loop (it may return short reads), and we
///   treat 0 bytes as unexpected EOF to match Unix's "exact" semantics.
#[cfg(unix)]
fn pread_exact(f: &std::fs::File, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
    use std::os::unix::fs::FileExt;
    f.read_exact_at(buf, offset)
}

#[cfg(windows)]
fn pread_exact(f: &std::fs::File, mut buf: &mut [u8], mut offset: u64) -> std::io::Result<()> {
    use std::os::windows::fs::FileExt;
    while !buf.is_empty() {
        let n = f.seek_read(buf, offset)?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "unexpected eof in positional read",
            ));
        }
        let tmp = buf;
        buf = &mut tmp[n..];
        offset += n as u64;
    }
    Ok(())
}

/// Positional write at `offset`. Same shape as `pread_exact`.
#[cfg(unix)]
fn pwrite_all(f: &std::fs::File, buf: &[u8], offset: u64) -> std::io::Result<()> {
    use std::os::unix::fs::FileExt;
    f.write_all_at(buf, offset)
}

#[cfg(windows)]
fn pwrite_all(f: &std::fs::File, mut buf: &[u8], mut offset: u64) -> std::io::Result<()> {
    use std::os::windows::fs::FileExt;
    while !buf.is_empty() {
        let n = f.seek_write(buf, offset)?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "positional write returned 0",
            ));
        }
        buf = &buf[n..];
        offset += n as u64;
    }
    Ok(())
}

/// Block granularity used for the bitmap. Bytes are stored at their absolute
/// file offset in a sparse `file.bin`; the bitmap simply records which
/// `BLOCK_SIZE`-sized regions are *fully* present, so reads can decide
/// whether to hit disk or fall back to the origin.
pub const BLOCK_SIZE: u64 = 1024 * 1024;

/// Subdirectory of the cache root holding ephemeral staging scratch files.
/// Dot-prefixed so the entry-directory walks (`clear_all`,
/// `total_bytes_on_disk`) can tell it apart from real cache entries, whose
/// keys are always hex digests.
const STAGING_DIR: &str = ".staging";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheMeta {
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub total_size: u64,
    pub content_type: Option<String>,
    pub block_size: u64,
    /// Original URL list (sorted, for traceability — not used for lookup).
    pub urls: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CacheStats {
    pub key: String,
    pub total_size: u64,
    pub bytes_cached: u64,
    pub blocks_cached: u64,
    pub blocks_total: u64,
    pub hits: u64,
    pub misses: u64,
    pub etag: Option<String>,
    /// Downsampled view of the block bitmap: each entry is the percentage of
    /// blocks completed in that segment, 0-100. Segment count is capped at
    /// ~128 so the payload stays tiny even for multi-GB files. UI renders
    /// this as a heat-strip progress bar showing which parts are cached.
    pub bitmap_summary: Vec<u8>,
}

pub struct CacheEntry {
    pub key: String,
    pub root: PathBuf,
    pub meta: CacheMeta,
    /// Data file, accessed exclusively through positional read/write.
    ///
    /// Deliberately **not** behind a mutex: `pread`/`pwrite` (and the Windows
    /// `seek_read`/`seek_write` equivalents) take `&self` and are atomic with
    /// respect to the file offset, so no serialization is needed. A mutex here
    /// would be a process-wide choke point — the ordered reader and every
    /// concurrent fetcher would queue behind each other for no reason.
    file: std::fs::File,
    bitmap: Mutex<Vec<u8>>,
    /// In-memory only: **per-block set of covered byte intervals (relative to
    /// the block start)**. A block is marked complete only when the merged
    /// union of its intervals covers `[0, block_len)`.
    ///
    /// Why intervals and not a simple byte-counter? Two concurrent fetchers
    /// can each contribute bytes to the same block (e.g. a browser opens
    /// parallel HTTP connections, or the engine warms up multiple chunks
    /// that fall in the same block). A counter would happily mark a block
    /// "complete" the moment `sum(contributed) >= block_len`, even when
    /// those contributions overlap on disk — leaving holes that read back
    /// as zeros, which then XOR with the keystream to produce garbage in
    /// transformed (encrypted) tasks.
    ///
    /// Stored as `(u32, u32)` `[lo, hi]` exclusive-end pairs. Block size is
    /// capped at 1 MiB elsewhere so u32 is comfortable; intervals are kept
    /// sorted + merged on every write so the inner Vec stays tiny in the
    /// common (sequential-write) case.
    ///
    /// **Lock order:** whenever both are needed, `bitmap` is taken before
    /// `partial`. Never the other way round.
    partial: Mutex<Vec<Vec<(u32, u32)>>>,
    pub bytes_cached: AtomicU64,
    pub hits: AtomicU64,
    pub misses: AtomicU64,
    /// Monotonic write counter. Bumped after every `write_range` so an
    /// ordered reader can park on `changed()` instead of polling the bitmap.
    /// A watch channel (rather than `Notify`) because its per-receiver version
    /// tracking makes the check-then-wait race impossible: a write landing
    /// between "availability came back 0" and "await" leaves the receiver
    /// behind, so `changed()` returns immediately instead of sleeping.
    progress: tokio::sync::watch::Sender<u64>,
    /// True for ephemeral staging entries. Skips `persist_bitmap` (the
    /// directory is deleted once the last reader drops, so there is nothing
    /// to resume from) and marks the entry for removal by `release_staging`.
    pub ephemeral: bool,
}

impl CacheEntry {
    fn block_count(&self) -> u64 {
        self.meta.total_size.div_ceil(self.meta.block_size)
    }

    pub fn has_block(&self, idx: u64) -> bool {
        let bm = self.bitmap.lock();
        let byte_idx = (idx / 8) as usize;
        let bit = (idx % 8) as u8;
        bm.get(byte_idx).map(|&b| (b >> bit) & 1 != 0).unwrap_or(false)
    }

    /// Read [start, end] inclusive from the sparse file. Caller must have
    /// verified all covered blocks are present.
    pub fn read_range(&self, start: u64, end: u64) -> std::io::Result<Bytes> {
        let len = (end - start + 1) as usize;
        let mut buf = vec![0u8; len];
        pread_exact(&self.file, &mut buf, start)?;
        Ok(Bytes::from(buf))
    }

    /// How many bytes are contiguously readable starting at `offset`, capped
    /// at `limit`. Stops at the first hole.
    ///
    /// This is what lets an ordered reader run ahead of block granularity: a
    /// block that is only partially filled still yields its covered prefix,
    /// so a sequential fetcher's bytes reach the client as they land instead
    /// of waiting for the enclosing 1 MiB block to complete. Complete blocks
    /// are answered from `bitmap`; the block at the frontier is resolved from
    /// its `partial` interval set.
    pub fn contiguous_from(&self, offset: u64, limit: u64) -> u64 {
        let total = self.meta.total_size;
        if limit == 0 || total == 0 || offset >= total {
            return 0;
        }
        let limit = limit.min(total - offset);
        let bs = self.meta.block_size;

        // Lock order: bitmap before partial (see the `partial` field doc).
        let bm = self.bitmap.lock();
        let partial = self.partial.lock();

        let mut avail: u64 = 0;
        while avail < limit {
            let cur = offset + avail;
            let b = cur / bs;
            let block_start = b * bs;
            let block_end = ((b + 1) * bs - 1).min(total - 1);
            let block_len = block_end - block_start + 1;
            let in_block = cur - block_start;

            let byte_idx = (b / 8) as usize;
            let bit = (b % 8) as u8;
            let complete = bm
                .get(byte_idx)
                .map(|&x| (x >> bit) & 1 != 0)
                .unwrap_or(false);

            // Exclusive end (relative to the block) of the covered run that
            // contains `in_block`. Equal to `in_block` when nothing is there.
            let covered_to = if complete {
                block_len
            } else {
                partial
                    .get(b as usize)
                    .and_then(|set| {
                        // Intervals are sorted and non-overlapping, so the
                        // first one that spans `in_block` is the only one
                        // that can extend the contiguous run.
                        set.iter()
                            .take_while(|&&(lo, _)| lo as u64 <= in_block)
                            .find(|&&(lo, hi)| {
                                lo as u64 <= in_block && in_block < hi as u64
                            })
                            .map(|&(_, hi)| hi as u64)
                    })
                    .unwrap_or(in_block)
            };

            if covered_to <= in_block {
                break; // hole right at the cursor
            }
            avail += (covered_to - in_block).min(limit - avail);
            if covered_to < block_len {
                break; // covered run ends mid-block → hole follows
            }
        }
        avail
    }

    /// Receiver for this entry's write counter. Subscribe **before** checking
    /// `contiguous_from`, then `changed().await` when it returns 0 — the
    /// watch channel's version tracking makes that sequence race-free.
    pub fn subscribe_progress(&self) -> tokio::sync::watch::Receiver<u64> {
        self.progress.subscribe()
    }

    /// Materialize an entry rooted at `dir`, creating — or wiping and
    /// recreating, on an identity mismatch — the sparse data file, bitmap and
    /// meta.
    ///
    /// Free-standing rather than a `CacheStore` method because a download's
    /// `.part` file lives in the user's chosen output directory, nowhere near
    /// the cache root, yet wants exactly the same sparse-file + block-bitmap
    /// machinery (and therefore gets resume for free).
    ///
    /// `ephemeral` suppresses bitmap persistence — appropriate for scratch
    /// staging that is discarded when the stream ends, wrong for a download
    /// that must survive a restart.
    pub fn open_at(
        dir: PathBuf,
        key: &str,
        desired: CacheMeta,
        ephemeral: bool,
    ) -> Result<Arc<CacheEntry>> {

        let meta_path = dir.join("meta.json");
        let file_path = dir.join("file.bin");
        let bitmap_path = dir.join("bitmap.bin");

        let stored: Option<CacheMeta> = std::fs::read_to_string(&meta_path)
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok());

        let needs_rebuild = match &stored {
            None => true,
            Some(m) => !cache_meta_compatible(m, &desired),
        };

        if needs_rebuild {
            // Wipe and recreate.
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).map_err(ProxyError::Io)?;
            // Sparse file: open with truncate + set_len.
            let f = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&file_path)
                .map_err(ProxyError::Io)?;
            if desired.total_size > 0 {
                f.set_len(desired.total_size).map_err(ProxyError::Io)?;
            }
            // Empty bitmap.
            let block_count = desired.total_size.div_ceil(desired.block_size);
            let bitmap_bytes = block_count.div_ceil(8) as usize;
            let bm = vec![0u8; bitmap_bytes];
            std::fs::write(&bitmap_path, &bm).map_err(ProxyError::Io)?;
            // Persist meta.
            let json = serde_json::to_string_pretty(&desired)
                .map_err(|e| ProxyError::Internal(format!("meta encode: {e}")))?;
            std::fs::write(&meta_path, json).map_err(ProxyError::Io)?;
        }

        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&file_path)
            .map_err(ProxyError::Io)?;
        let bm = std::fs::read(&bitmap_path).map_err(ProxyError::Io)?;
        let block_count = desired.total_size.div_ceil(desired.block_size);
        let bitmap_bytes = block_count.div_ceil(8) as usize;
        let bm = if bm.len() >= bitmap_bytes {
            bm
        } else {
            let mut padded = bm;
            padded.resize(bitmap_bytes, 0);
            padded
        };

        let bytes_cached: u64 = bm.iter().map(|b| b.count_ones() as u64).sum::<u64>()
            * desired.block_size;
        let bytes_cached = bytes_cached.min(desired.total_size);

        Ok(Arc::new(CacheEntry {
            key: key.to_string(),
            root: dir,
            file: f,
            bitmap: Mutex::new(bm),
            partial: Mutex::new(vec![Vec::new(); block_count as usize]),
            bytes_cached: AtomicU64::new(bytes_cached),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            progress: tokio::sync::watch::Sender::new(0),
            ephemeral,
            meta: desired,
        }))
    }

    /// Path of the backing data file. A completed download renames this into
    /// place rather than copying it — the `.part` directory is a child of the
    /// output directory, so the rename stays within one filesystem.
    pub fn data_path(&self) -> PathBuf {
        self.root.join("file.bin")
    }

    /// Every already-present run inside `[start, end]`, as inclusive ranges.
    ///
    /// Used once when a staged stream starts, to tell the scheduler what it
    /// must *not* refetch — a warm cache, or bytes a previous connection on
    /// this task prefetched before the client seeked and reconnected.
    /// Advances a whole block per step across gaps, so this is O(blocks).
    pub fn staged_ranges(&self, start: u64, end: u64) -> Vec<(u64, u64)> {
        let mut out: Vec<(u64, u64)> = Vec::new();
        if start > end {
            return out;
        }
        let bs = self.meta.block_size.max(1);
        let mut cur = start;
        while cur <= end {
            let n = self.contiguous_from(cur, end - cur + 1);
            if n > 0 {
                out.push((cur, cur + n - 1));
                cur += n;
            } else {
                // Nothing at `cur`; the smallest useful step is to the next
                // block boundary (a fully empty block can't hold a run).
                cur = (cur / bs + 1) * bs;
            }
        }
        out
    }

    /// Write `data` at absolute offset `start`. Each call updates the
    /// per-block interval set with the slice it contributed; a block is
    /// marked complete only when the merged union covers `[0, block_len)`.
    /// See the field doc on `partial` for why this isn't a byte counter.
    pub fn write_range(&self, start: u64, data: &[u8]) -> std::io::Result<()> {
        if data.is_empty() || self.meta.total_size == 0 {
            return Ok(());
        }
        pwrite_all(&self.file, data, start)?;

        let end = start + data.len() as u64 - 1;
        let first_block = start / self.meta.block_size;
        let last_block = end / self.meta.block_size;

        // Single critical section, `bitmap` before `partial`. This used to be
        // two sections taken in *opposite* orders (partial→bitmap, then
        // bitmap→partial), which two concurrent writers could deadlock on —
        // increasingly likely now that a staged stream runs `max_threads`
        // writers against one entry.
        let mut total_bytes_marked: u64 = 0;
        {
            let mut bm = self.bitmap.lock();
            let mut partial = self.partial.lock();
            for b in first_block..=last_block {
                let byte_idx = (b / 8) as usize;
                let bit = (b % 8) as u8;
                // Out of range, or already complete → nothing to track.
                if byte_idx >= bm.len() || (bm[byte_idx] >> bit) & 1 != 0 {
                    continue;
                }
                let block_start = b * self.meta.block_size;
                let block_end =
                    ((b + 1) * self.meta.block_size - 1).min(self.meta.total_size - 1);
                let bl = block_end - block_start + 1;

                let in_start = start.max(block_start);
                let in_end = end.min(block_end);
                if in_end < in_start {
                    continue;
                }
                let lo = (in_start - block_start) as u32;
                let hi = (in_end - block_start + 1) as u32;

                let Some(slot) = partial.get_mut(b as usize) else {
                    continue;
                };
                merge_interval(slot, lo, hi);
                if interval_set_covers(slot, bl as u32) {
                    bm[byte_idx] |= 1 << bit;
                    total_bytes_marked += bl;
                    // Block is now permanently in the bitmap — we don't
                    // need the interval set any more. Reclaim the Vec
                    // capacity (a long-running task with many blocks
                    // would otherwise hold onto megabytes of stale
                    // interval buffers).
                    *slot = Vec::new();
                }
            }
        }

        if total_bytes_marked > 0 {
            self.bytes_cached
                .fetch_add(total_bytes_marked, Ordering::Relaxed);
            // Ephemeral staging is discarded when the stream ends, so there
            // is nothing to resume from — skip the whole-bitmap rewrite.
            if !self.ephemeral {
                self.persist_bitmap()?;
            }
        }

        // Wake parked readers. Bumped on every write, not just on block
        // completion, because `contiguous_from` can serve a partially
        // filled block up to its covered frontier.
        self.progress.send_modify(|v| *v = v.wrapping_add(1));
        Ok(())
    }

    fn persist_bitmap(&self) -> std::io::Result<()> {
        let bm = self.bitmap.lock().clone();
        let path = self.root.join("bitmap.bin");
        let tmp = self.root.join("bitmap.bin.tmp");
        std::fs::write(&tmp, &bm)?;
        std::fs::rename(&tmp, &path)?;
        Ok(())
    }

    pub fn stats(&self) -> CacheStats {
        let blocks_total = self.block_count();
        let (blocks_cached, bitmap_summary) = {
            let bm = self.bitmap.lock();
            let cached = bm.iter().map(|b| b.count_ones() as u64).sum::<u64>().min(blocks_total);
            let summary = downsample_bitmap(&bm, blocks_total, 128);
            (cached, summary)
        };
        CacheStats {
            key: self.key.clone(),
            total_size: self.meta.total_size,
            bytes_cached: self.bytes_cached.load(Ordering::Relaxed),
            blocks_cached,
            blocks_total,
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            etag: self.meta.etag.clone(),
            bitmap_summary,
        }
    }
}

/// Compress a bitmap into at most `max_buckets` segments, each holding the
/// percentage (0-100) of blocks completed in that segment. For files smaller
/// than `max_buckets` blocks each bucket maps to exactly one block.
fn downsample_bitmap(bm: &[u8], blocks_total: u64, max_buckets: usize) -> Vec<u8> {
    if blocks_total == 0 {
        return Vec::new();
    }
    let buckets = (blocks_total as usize).min(max_buckets).max(1);
    let mut out = Vec::with_capacity(buckets);
    let total = blocks_total as usize;
    for i in 0..buckets {
        let lo = (i * total) / buckets;
        let hi = (((i + 1) * total) / buckets).min(total);
        if hi <= lo {
            out.push(0);
            continue;
        }
        let span = (hi - lo) as u64;
        let mut filled = 0u64;
        for b in lo..hi {
            let byte_idx = b / 8;
            let bit = (b % 8) as u8;
            if byte_idx < bm.len() && (bm[byte_idx] >> bit) & 1 != 0 {
                filled += 1;
            }
        }
        out.push(((filled * 100) / span) as u8);
    }
    out
}

pub struct CacheStore {
    root: PathBuf,
    entries: RwLock<HashMap<String, Arc<CacheEntry>>>,
    /// Ephemeral staging entries, keyed the same way as `entries` but living
    /// under `.staging/`. The `usize` is a refcount: concurrent streams on one
    /// task share a single scratch file (so each one's fetches serve the
    /// others), and the directory is removed when the last of them drops.
    staging: Mutex<HashMap<String, (Arc<CacheEntry>, usize)>>,
}

/// A stream's staging target — the random-access substrate that decouples
/// fetch order from delivery order.
///
/// Two flavours, same interface: the task's **persistent** cache entry when
/// `cache: true`, or an **ephemeral** scratch entry otherwise. The ephemeral
/// variant releases its refcount on drop, tearing down the scratch directory
/// once no stream is using it.
pub struct Staging {
    entry: Arc<CacheEntry>,
    /// `Some` only for ephemeral entries.
    release: Option<(Arc<CacheStore>, String)>,
}

impl Staging {
    pub fn entry(&self) -> &Arc<CacheEntry> {
        &self.entry
    }

    pub fn is_ephemeral(&self) -> bool {
        self.release.is_some()
    }
}

impl Drop for Staging {
    fn drop(&mut self) {
        if let Some((store, key)) = self.release.take() {
            store.release_staging(&key);
        }
    }
}

impl CacheStore {
    pub fn new(root: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&root).map_err(ProxyError::Io)?;
        // Nothing under `.staging/` survives a restart: ephemeral entries
        // never persist their bitmap, so a leftover directory from a crashed
        // run is a fully-allocated file we can't trust a single block of.
        let staging_root = root.join(STAGING_DIR);
        if staging_root.exists() {
            if let Err(e) = std::fs::remove_dir_all(&staging_root) {
                tracing::warn!("failed to clear stale staging root: {}", e);
            }
        }
        Ok(Self {
            root,
            entries: RwLock::new(HashMap::new()),
            staging: Mutex::new(HashMap::new()),
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Stable cache key derived from the (sorted) URL list, so the same
    /// content cached under task A is reused by task B if the URL list
    /// matches.
    pub fn key_for_urls(urls: &[String]) -> String {
        let mut sorted: Vec<&str> = urls.iter().map(|s| s.as_str()).collect();
        sorted.sort_unstable();
        let mut hasher = Sha256::new();
        for u in &sorted {
            hasher.update(u.as_bytes());
            hasher.update(b"\n");
        }
        hex::encode(&hasher.finalize()[..12])
    }

    /// Cache key for a structured volume layout. Volumes are hashed in order
    /// (their sequence is part of the merged file's identity); mirrors within
    /// a volume are hashed sorted (they're interchangeable, so reordering or
    /// adding a synonym shouldn't trigger a re-fetch).
    pub fn key_for_volume_layout(volumes: &[Vec<String>]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(b"vols-v2:");
        for vol in volumes {
            hasher.update(b"|");
            let mut sorted: Vec<&str> = vol.iter().map(|s| s.as_str()).collect();
            sorted.sort_unstable();
            for u in &sorted {
                hasher.update(u.as_bytes());
                hasher.update(b"\n");
            }
        }
        hex::encode(&hasher.finalize()[..12])
    }

    /// Pick the right key derivation for a task. Single-volume tasks reuse
    /// the flat mirror-mode key (so existing caches keep working through the
    /// schema upgrade); multi-volume tasks get the layout-aware key.
    /// An empty layout is treated as an empty mirror list — the caller
    /// won't reach this path because task creation rejects zero-URL tasks.
    pub fn key_for_task(cfg: &crate::models::TaskConfig) -> String {
        let vols = cfg.effective_volumes();
        match vols.len() {
            0 => Self::key_for_urls(&[]),
            1 => Self::key_for_urls(&vols[0]),
            _ => Self::key_for_volume_layout(&vols),
        }
    }

    fn entry_dir(&self, key: &str) -> PathBuf {
        self.root.join(key)
    }

    fn staging_dir(&self, key: &str) -> PathBuf {
        self.root.join(STAGING_DIR).join(key)
    }

    /// Materialize a `CacheEntry` rooted at `dir` via [`CacheEntry::open_at`].
    fn build_entry(
        &self,
        dir: PathBuf,
        key: &str,
        desired: CacheMeta,
        ephemeral: bool,
    ) -> Result<Arc<CacheEntry>> {
        CacheEntry::open_at(dir, key, desired, ephemeral)
    }

    /// Open or create a cache entry for `key`. If an existing entry's stored
    /// meta disagrees with `desired` (different ETag or size), the on-disk
    /// state is wiped and re-initialized — per the project's "auto-clear on
    /// ETag mismatch" policy.
    pub fn open(&self, key: &str, desired: CacheMeta) -> Result<Arc<CacheEntry>> {
        if let Some(e) = self.entries.read().get(key) {
            if cache_meta_compatible(&e.meta, &desired) {
                return Ok(Arc::clone(e));
            }
        }

        let mut entries = self.entries.write();
        if let Some(e) = entries.get(key) {
            if cache_meta_compatible(&e.meta, &desired) {
                return Ok(Arc::clone(e));
            }
            // Stale in-memory entry — drop it before rebuilding.
            entries.remove(key);
        }

        let entry = self.build_entry(self.entry_dir(key), key, desired, false)?;
        entries.insert(key.to_string(), Arc::clone(&entry));
        Ok(entry)
    }

    /// Acquire the staging target for one stream.
    ///
    /// `persist = true` (the task set `cache: true`) hands back the durable
    /// cache entry, so the bytes outlive the stream and a later request is
    /// served from disk. Otherwise an ephemeral scratch entry under
    /// `.staging/` is used and torn down once the last concurrent stream on
    /// that key drops its handle.
    ///
    /// Concurrent streams on one task deliberately share a single entry: each
    /// one's fetches then satisfy the others, and a seek that reconnects can
    /// reuse whatever the previous connection prefetched.
    pub fn acquire_staging(
        store: &Arc<Self>,
        key: &str,
        desired: CacheMeta,
        persist: bool,
    ) -> Result<Staging> {
        if persist {
            return Ok(Staging {
                entry: store.open(key, desired)?,
                release: None,
            });
        }
        let mut map = store.staging.lock();
        if let Some((entry, refs)) = map.get_mut(key) {
            if cache_meta_compatible(&entry.meta, &desired) {
                *refs += 1;
                let entry = Arc::clone(entry);
                return Ok(Staging {
                    entry,
                    release: Some((Arc::clone(store), key.to_string())),
                });
            }
            // Upstream identity changed under us — the old scratch file is
            // unusable for this stream. Forget it here and rebuild; any stream
            // still holding the old Arc keeps reading its own open file until
            // it exits (the directory removal below only happens on refcount
            // zero, and this entry no longer has a refcount to reach).
            map.remove(key);
        }
        let entry = store.build_entry(store.staging_dir(key), key, desired, true)?;
        map.insert(key.to_string(), (Arc::clone(&entry), 1));
        Ok(Staging {
            entry,
            release: Some((Arc::clone(store), key.to_string())),
        })
    }

    /// Drop one reference to an ephemeral staging entry, removing its scratch
    /// directory once the count hits zero. Called from `Staging::drop`.
    fn release_staging(&self, key: &str) {
        let remove = {
            let mut map = self.staging.lock();
            match map.get_mut(key) {
                Some((_, refs)) => {
                    *refs = refs.saturating_sub(1);
                    if *refs == 0 {
                        map.remove(key);
                        true
                    } else {
                        false
                    }
                }
                None => false,
            }
        };
        if !remove {
            return;
        }
        let dir = self.staging_dir(key);
        if dir.exists() {
            match std::fs::remove_dir_all(&dir) {
                Ok(()) => tracing::debug!("staging scratch removed: {}", dir.display()),
                Err(e) => tracing::warn!(
                    "failed to remove staging dir {}: {}",
                    dir.display(),
                    e
                ),
            }
        }
    }

    pub fn get(&self, key: &str) -> Option<Arc<CacheEntry>> {
        self.entries.read().get(key).cloned()
    }

    /// Move an entry's on-disk state from `from_key` to `to_key`. Used when a
    /// task's URLs change (e.g. a pan-CDN signed link expires and the user
    /// pastes a new one for the same content) — the cache key is derived
    /// from the URL list, so without migration the old directory becomes an
    /// orphan and the user pays a full re-fetch.
    ///
    /// Optimistic: we don't verify the new URLs point to the same content.
    /// If they don't, `CacheStore::open`'s fresh-probe path will see
    /// incompatible etag/size and wipe the renamed directory on the next
    /// stream request — same outcome as the no-migration baseline.
    ///
    /// Safety: refuses to migrate while a live in-memory `CacheEntry` exists
    /// under `from_key`. A live entry's internal `root: PathBuf` would go
    /// stale across the rename, breaking `persist_bitmap` for in-flight
    /// writes. The expected real-world path here — user notices a download
    /// stalled, replaces a dead URL — has no active stream and so no live
    /// entry, so this restriction barely costs anything in practice.
    ///
    /// Returns `Ok(true)` when a rename actually happened, `Ok(false)` for
    /// every no-op condition (same key, no source dir, destination exists,
    /// source still in use). I/O errors propagate as `Err`.
    pub fn migrate_key(&self, from_key: &str, to_key: &str) -> Result<bool> {
        if from_key == to_key {
            return Ok(false);
        }
        let from = self.entry_dir(from_key);
        let to = self.entry_dir(to_key);
        if !from.is_dir() {
            return Ok(false);
        }
        if to.exists() {
            return Ok(false);
        }
        if self.entries.read().contains_key(from_key) {
            return Ok(false);
        }
        std::fs::rename(&from, &to).map_err(ProxyError::Io)?;
        Ok(true)
    }

    pub fn clear(&self, key: &str) -> Result<()> {
        self.entries.write().remove(key);
        let dir = self.entry_dir(key);
        if dir.exists() {
            std::fs::remove_dir_all(&dir).map_err(ProxyError::Io)?;
        }
        Ok(())
    }

    /// Wipe every cache entry — both in-memory handles and on-disk blocks.
    /// Returns the number of bytes that were on disk before clearing, for
    /// reporting in the UI toast. Active tasks that re-fetch on the next
    /// stream request will lazily recreate their entries (CacheStore::open).
    pub fn clear_all(&self) -> Result<u64> {
        let freed = self.total_bytes_on_disk();
        self.entries.write().clear();
        if self.root.exists() {
            // Walk one level deep — every direct child of `root` is an entry
            // directory. Skip files (state.json lives elsewhere) and tolerate
            // partial failures so one stuck entry doesn't block the rest.
            let read = std::fs::read_dir(&self.root).map_err(ProxyError::Io)?;
            for ent in read.flatten() {
                let path = ent.path();
                if path.is_dir() && !is_reserved_dir(&path) {
                    if let Err(e) = std::fs::remove_dir_all(&path) {
                        tracing::warn!(
                            "clear_all: failed to remove {}: {}",
                            path.display(), e,
                        );
                    }
                }
            }
        }
        Ok(freed)
    }

    pub fn stats(&self, key: &str) -> Option<CacheStats> {
        if let Some(e) = self.entries.read().get(key) {
            return Some(e.stats());
        }
        // In-memory miss — entry was never opened in this process (typical
        // right after restart, before any stream request has run). Read the
        // bitmap straight off disk so the dashboard reflects the durable
        // state instead of looking empty.
        self.stats_from_disk(key)
    }

    /// Read meta.json + bitmap.bin off disk and synthesize a CacheStats
    /// without opening the data file or inserting into `entries`. Returns
    /// None when there's no on-disk entry for `key` (or it's malformed).
    /// `hits`/`misses` reset to zero — they're per-process counters and
    /// haven't started counting yet for this entry.
    fn stats_from_disk(&self, key: &str) -> Option<CacheStats> {
        let dir = self.entry_dir(key);
        if !dir.is_dir() {
            return None;
        }
        let meta: CacheMeta = std::fs::read_to_string(dir.join("meta.json"))
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())?;
        let bm = std::fs::read(dir.join("bitmap.bin")).ok()?;
        let blocks_total = meta.total_size.div_ceil(meta.block_size);
        let blocks_cached = bm
            .iter()
            .map(|b| b.count_ones() as u64)
            .sum::<u64>()
            .min(blocks_total);
        let bytes_cached = (blocks_cached * meta.block_size).min(meta.total_size);
        let bitmap_summary = downsample_bitmap(&bm, blocks_total, 128);
        Some(CacheStats {
            key: key.to_string(),
            total_size: meta.total_size,
            bytes_cached,
            blocks_cached,
            blocks_total,
            hits: 0,
            misses: 0,
            etag: meta.etag,
            bitmap_summary,
        })
    }

    /// Sum `bytes_cached` across every entry currently on disk. Walks the
    /// cache root one level deep and reads each entry's bitmap to compute
    /// covered bytes — so the count is accurate after a restart, before any
    /// entry has been re-opened. Falls back to in-memory state when the
    /// directory walk fails for any reason.
    pub fn total_bytes_on_disk(&self) -> u64 {
        let mut total: u64 = 0;
        let read = match std::fs::read_dir(&self.root) {
            Ok(r) => r,
            Err(_) => {
                return self
                    .entries
                    .read()
                    .values()
                    .map(|e| e.bytes_cached.load(Ordering::Relaxed))
                    .sum();
            }
        };
        for ent in read.flatten() {
            let path = ent.path();
            if !path.is_dir() || is_reserved_dir(&path) {
                continue;
            }
            let key = match path.file_name().and_then(|s| s.to_str()) {
                Some(k) => k.to_string(),
                None => continue,
            };
            // Prefer in-memory live state when available — `bytes_cached` is
            // updated incrementally as fetches land, so it stays in lockstep
            // with the bitmap without re-reading from disk every tick.
            if let Some(e) = self.entries.read().get(&key) {
                total = total.saturating_add(e.bytes_cached.load(Ordering::Relaxed));
                continue;
            }
            if let Some(s) = self.stats_from_disk(&key) {
                total = total.saturating_add(s.bytes_cached);
            }
        }
        total
    }
}

/// True for cache-root children that are bookkeeping, not cache entries.
/// Entry keys are hex digests, so a dot prefix can never collide with one.
fn is_reserved_dir(path: &Path) -> bool {
    path.file_name()
        .and_then(|s| s.to_str())
        .map(|s| s.starts_with('.'))
        .unwrap_or(false)
}

fn cache_meta_compatible(stored: &CacheMeta, desired: &CacheMeta) -> bool {    if stored.total_size != desired.total_size {
        return false;
    }
    if stored.block_size != desired.block_size {
        return false;
    }
    match (&stored.etag, &desired.etag) {
        (Some(a), Some(b)) => a == b,
        // No ETag on either side: fall back to last_modified comparison.
        (None, None) => stored.last_modified == desired.last_modified,
        // Asymmetric: treat as a mismatch — safer to refetch than to serve
        // possibly-stale bytes.
        _ => false,
    }
}

/// Insert `[lo, hi)` into a sorted, non-overlapping interval list. Merges
/// any neighbours it touches so the list stays minimal — in steady state
/// (a single sequential writer per block) `set` collapses to one entry.
///
/// Pure data, no I/O. Called under the `partial` mutex.
fn merge_interval(set: &mut Vec<(u32, u32)>, lo: u32, hi: u32) {
    if hi <= lo {
        return;
    }
    // Find the first interval whose `end >= lo`. Everything before it ends
    // strictly to the left of `lo` and is unaffected.
    let i = match set.iter().position(|&(_, e)| e >= lo) {
        Some(i) => i,
        None => {
            // New interval extends past every existing one — append.
            set.push((lo, hi));
            return;
        }
    };
    // If interval i starts after hi, the new interval is fully to the left
    // of it; insert and return.
    if set[i].0 > hi {
        set.insert(i, (lo, hi));
        return;
    }
    // Overlap (or touches): swallow interval i into the merged window.
    let mut merged_lo = set[i].0.min(lo);
    let mut merged_hi = set[i].1.max(hi);
    // Keep swallowing subsequent intervals while they overlap with the
    // growing window.
    let mut j = i + 1;
    while j < set.len() && set[j].0 <= merged_hi {
        merged_hi = merged_hi.max(set[j].1);
        merged_lo = merged_lo.min(set[j].0);
        j += 1;
    }
    set.drain(i + 1..j);
    set[i] = (merged_lo, merged_hi);
}

/// True iff the merged interval list covers `[0, block_len)` exactly — i.e.
/// every byte of the block has been written. With `merge_interval` keeping
/// the set sorted + minimal, full coverage = exactly one entry `(0, block_len)`.
fn interval_set_covers(set: &[(u32, u32)], block_len: u32) -> bool {
    set.len() == 1 && set[0].0 == 0 && set[0].1 >= block_len
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_interval_appends_disjoint_right() {
        let mut s = vec![(0u32, 10u32)];
        merge_interval(&mut s, 20, 30);
        assert_eq!(s, vec![(0, 10), (20, 30)]);
    }

    #[test]
    fn merge_interval_inserts_disjoint_left() {
        let mut s = vec![(20u32, 30u32)];
        merge_interval(&mut s, 0, 10);
        assert_eq!(s, vec![(0, 10), (20, 30)]);
    }

    #[test]
    fn merge_interval_fuses_touching_intervals() {
        // Touch on the right edge: [0,10) + [10,20) → [0,20)
        let mut s = vec![(0u32, 10u32)];
        merge_interval(&mut s, 10, 20);
        assert_eq!(s, vec![(0, 20)]);
    }

    #[test]
    fn merge_interval_subsumes_overlapping_chain() {
        // Existing: [0,5), [10,15), [20,25)
        // Insert [3,22) → swallows all three into [0,25)
        let mut s = vec![(0u32, 5u32), (10, 15), (20, 25)];
        merge_interval(&mut s, 3, 22);
        assert_eq!(s, vec![(0, 25)]);
    }

    #[test]
    fn merge_interval_ignores_duplicate_writes() {
        let mut s = vec![(0u32, 100u32)];
        merge_interval(&mut s, 0, 100);
        assert_eq!(s, vec![(0, 100)]);
        merge_interval(&mut s, 40, 60);
        assert_eq!(s, vec![(0, 100)]);
    }

    #[test]
    fn interval_set_covers_requires_single_full_span() {
        assert!(interval_set_covers(&[(0, 100)], 100));
        // Holes — must not be considered complete.
        assert!(!interval_set_covers(&[(0, 50), (60, 100)], 100));
        // Coverage with a gap at the start.
        assert!(!interval_set_covers(&[(1, 100)], 100));
        // Coverage that exceeds is still OK (callers cap to block_len).
        assert!(interval_set_covers(&[(0, 150)], 100));
    }

    /// **The regression test for the playback-tearing bug.** Two concurrent
    /// writers contribute byte ranges that overlap inside one block. A
    /// byte-counter implementation would mark the block complete the moment
    /// `sum(contributed) >= block_len`, leaving a hole that reads back as
    /// zeros. The interval set must require true coverage.
    #[test]
    fn overlapping_writes_do_not_falsely_complete_block() {
        let mut s: Vec<(u32, u32)> = Vec::new();
        let block_len = 1024u32;
        // Writer A: contributes [0, 600) — 600 bytes.
        merge_interval(&mut s, 0, 600);
        // Writer B: contributes [400, 1024) — 624 bytes.
        merge_interval(&mut s, 400, 1024);
        // Total bytes "contributed" = 600 + 624 = 1224 > 1024,
        // but the union [0, 1024) does cover the block — so this is
        // legitimately complete. The interesting case is when they overlap
        // BUT leave a hole:
        assert!(interval_set_covers(&s, block_len));

        let mut s2: Vec<(u32, u32)> = Vec::new();
        // Writer A: [0, 700)
        merge_interval(&mut s2, 0, 700);
        // Writer B: [300, 1000) — overlaps A but leaves [1000, 1024) untouched.
        merge_interval(&mut s2, 300, 1000);
        // Counter-based logic: contributed = 700 + 700 = 1400, hits 1024 →
        // **falsely** marks complete. Interval logic correctly refuses.
        assert!(!interval_set_covers(&s2, block_len));

        // Filling the tail finally completes it.
        merge_interval(&mut s2, 1000, 1024);
        assert!(interval_set_covers(&s2, block_len));
    }

    fn fresh_store() -> (PathBuf, CacheStore) {
        // A per-process counter, not a timestamp: macOS clock granularity is
        // coarse enough that two tests starting in the same microsecond got the
        // same directory, and then one test's cleanup deleted the other's data
        // mid-run. Collision-free within the process is exactly the scope that
        // matters here. We don't bother with cleanup on drop — these are tiny
        // scratch dirs and CI tmpfs gets wiped between runs anyway.
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let id = format!(
            "hydraria-cache-test-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed),
        );
        let dir = std::env::temp_dir().join(id);
        let store = CacheStore::new(dir.clone()).expect("store");
        (dir, store)
    }

    fn seed_entry_dir(store: &CacheStore, key: &str) {
        let dir = store.entry_dir(key);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("meta.json"), b"{}").unwrap();
        std::fs::write(dir.join("bitmap.bin"), [0u8; 4]).unwrap();
        std::fs::write(dir.join("file.bin"), b"").unwrap();
    }

    #[test]
    fn migrate_key_renames_directory_when_dest_is_free() {
        let (root, store) = fresh_store();
        seed_entry_dir(&store, "old");
        let moved = store.migrate_key("old", "new").unwrap();
        assert!(moved);
        assert!(!store.entry_dir("old").exists());
        assert!(store.entry_dir("new").is_dir());
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn migrate_key_noop_for_same_key() {
        let (root, store) = fresh_store();
        seed_entry_dir(&store, "k");
        let moved = store.migrate_key("k", "k").unwrap();
        assert!(!moved);
        assert!(store.entry_dir("k").is_dir());
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn migrate_key_noop_when_source_missing() {
        let (root, store) = fresh_store();
        let moved = store.migrate_key("ghost", "new").unwrap();
        assert!(!moved);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn migrate_key_refuses_to_overwrite_existing_dest() {
        let (root, store) = fresh_store();
        seed_entry_dir(&store, "old");
        seed_entry_dir(&store, "new");
        let moved = store.migrate_key("old", "new").unwrap();
        assert!(!moved, "must not clobber existing dest");
        assert!(store.entry_dir("old").is_dir(), "source preserved");
        assert!(store.entry_dir("new").is_dir(), "dest preserved");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn migrate_key_refuses_when_source_has_live_entry() {
        // A live in-memory entry's `root` would go stale across the rename
        // and break `persist_bitmap`. Skip the migration in that case.
        let (root, store) = fresh_store();
        let meta = CacheMeta {
            etag: Some("e".into()),
            last_modified: None,
            total_size: 1024,
            content_type: None,
            block_size: BLOCK_SIZE,
            urls: vec![],
        };
        let _live = store.open("live", meta).unwrap();
        let moved = store.migrate_key("live", "new").unwrap();
        assert!(!moved);
        assert!(store.entry_dir("live").is_dir(), "source preserved");
        assert!(!store.entry_dir("new").exists(), "dest not created");
        let _ = std::fs::remove_dir_all(&root);
    }

    /// Small-block entry so the `contiguous_from` tests stay readable:
    /// block_size 64, total 200 → blocks 0..=2 full, block 3 is 8 bytes.
    fn tiny_entry(store: &CacheStore) -> Arc<CacheEntry> {
        store
            .open(
                "tiny",
                CacheMeta {
                    etag: Some("t".into()),
                    last_modified: None,
                    total_size: 200,
                    content_type: None,
                    block_size: 64,
                    urls: vec![],
                },
            )
            .unwrap()
    }

    #[test]
    fn contiguous_from_is_zero_on_empty_entry() {
        let (root, store) = fresh_store();
        let e = tiny_entry(&store);
        assert_eq!(e.contiguous_from(0, 200), 0);
        assert_eq!(e.contiguous_from(70, 200), 0);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn contiguous_from_serves_partial_block_prefix() {
        // The whole point of byte-granularity availability: a sequential
        // fetcher's bytes must reach the reader without waiting for the
        // enclosing block to complete.
        let (root, store) = fresh_store();
        let e = tiny_entry(&store);
        e.write_range(0, &[1u8; 10]).unwrap();
        assert_eq!(e.contiguous_from(0, 200), 10);
        // Mid-run offsets report the remainder of the run.
        assert_eq!(e.contiguous_from(4, 200), 6);
        // `limit` caps the answer.
        assert_eq!(e.contiguous_from(0, 3), 3);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn contiguous_from_spans_completed_blocks_into_partial_one() {
        let (root, store) = fresh_store();
        let e = tiny_entry(&store);
        // Blocks 0 and 1 complete, plus 5 bytes of block 2.
        e.write_range(0, &[7u8; 128 + 5]).unwrap();
        assert!(e.has_block(0));
        assert!(e.has_block(1));
        assert!(!e.has_block(2));
        assert_eq!(e.contiguous_from(0, 200), 133);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn contiguous_from_stops_at_a_hole() {
        let (root, store) = fresh_store();
        let e = tiny_entry(&store);
        e.write_range(0, &[1u8; 10]).unwrap();
        // Disjoint island further in — must not be counted.
        e.write_range(20, &[1u8; 10]).unwrap();
        assert_eq!(e.contiguous_from(0, 200), 10);
        // Reading from inside the island works and stops at its end.
        assert_eq!(e.contiguous_from(20, 200), 10);
        // The hole itself reports nothing.
        assert_eq!(e.contiguous_from(10, 200), 0);
        // Filling the gap fuses the runs.
        e.write_range(10, &[1u8; 10]).unwrap();
        assert_eq!(e.contiguous_from(0, 200), 30);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn contiguous_from_stops_at_a_block_boundary_hole() {
        // Block 0 complete but block 1 empty: the run must end at 64, not
        // spill into the untouched next block.
        let (root, store) = fresh_store();
        let e = tiny_entry(&store);
        e.write_range(0, &[3u8; 64]).unwrap();
        assert!(e.has_block(0));
        assert_eq!(e.contiguous_from(0, 200), 64);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn contiguous_from_clamps_to_total_size() {
        let (root, store) = fresh_store();
        let e = tiny_entry(&store);
        // Fill everything, including the short trailing block.
        e.write_range(0, &[9u8; 200]).unwrap();
        // Asking for more than the file holds returns only what exists.
        assert_eq!(e.contiguous_from(0, u64::MAX), 200);
        assert_eq!(e.contiguous_from(199, u64::MAX), 1);
        // At/after EOF there is nothing.
        assert_eq!(e.contiguous_from(200, 200), 0);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn write_range_bumps_progress_counter() {
        let (root, store) = fresh_store();
        let e = tiny_entry(&store);
        let rx = e.subscribe_progress();
        assert!(!rx.has_changed().unwrap());
        e.write_range(0, &[1u8; 4]).unwrap();
        assert!(rx.has_changed().unwrap(), "reader must be woken by a write");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn ephemeral_staging_is_refcounted_and_removed_on_last_drop() {
        let (root, store) = fresh_store();
        let store = Arc::new(store);
        let meta = CacheMeta {
            etag: Some("s".into()),
            last_modified: None,
            total_size: 200,
            content_type: None,
            block_size: 64,
            urls: vec![],
        };
        let a = CacheStore::acquire_staging(&store, "k", meta.clone(), false).unwrap();
        let dir = store.staging_dir("k");
        assert!(dir.is_dir(), "scratch dir created");
        assert!(a.is_ephemeral());

        // Second stream on the same key shares the entry, so one's fetches
        // satisfy the other.
        let b = CacheStore::acquire_staging(&store, "k", meta.clone(), false).unwrap();
        assert!(Arc::ptr_eq(a.entry(), b.entry()), "handles share one entry");

        drop(a);
        assert!(dir.is_dir(), "still in use by the second handle");
        drop(b);
        assert!(!dir.exists(), "removed once the last handle drops");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn persistent_staging_survives_handle_drop() {
        let (root, store) = fresh_store();
        let store = Arc::new(store);
        let meta = CacheMeta {
            etag: Some("p".into()),
            last_modified: None,
            total_size: 200,
            content_type: None,
            block_size: 64,
            urls: vec![],
        };
        let h = CacheStore::acquire_staging(&store, "k", meta, true).unwrap();
        assert!(!h.is_ephemeral());
        drop(h);
        assert!(store.entry_dir("k").is_dir(), "cache entry must persist");
        assert!(!store.staging_dir("k").exists(), "must not use scratch root");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn clear_all_leaves_the_staging_root_alone() {
        let (root, store) = fresh_store();
        let store = Arc::new(store);
        seed_entry_dir(&store, "abc123");
        let meta = CacheMeta {
            etag: Some("s".into()),
            last_modified: None,
            total_size: 200,
            content_type: None,
            block_size: 64,
            urls: vec![],
        };
        let _live = CacheStore::acquire_staging(&store, "k", meta, false).unwrap();
        store.clear_all().unwrap();
        assert!(!store.entry_dir("abc123").exists(), "cache entry wiped");
        assert!(
            store.staging_dir("k").is_dir(),
            "an in-use scratch file must not be pulled out from under its stream",
        );
        let _ = std::fs::remove_dir_all(&root);
    }
}
