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
    file: Mutex<std::fs::File>,
    bitmap: Mutex<Vec<u8>>,
    /// In-memory only: bytes written so far per block. Used to detect when a
    /// block becomes fully cached across many small writes from
    /// `reqwest::bytes_stream`. Not persisted — on restart, partial blocks
    /// just need to be refetched, which is correct fallback behaviour.
    partial: Mutex<Vec<u32>>,
    pub bytes_cached: AtomicU64,
    pub hits: AtomicU64,
    pub misses: AtomicU64,
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

    fn block_len(&self, idx: u64) -> u64 {
        let start = idx * self.meta.block_size;
        if start >= self.meta.total_size {
            0
        } else {
            (self.meta.total_size - start).min(self.meta.block_size)
        }
    }

    /// Read [start, end] inclusive from the sparse file. Caller must have
    /// verified all covered blocks are present.
    pub fn read_range(&self, start: u64, end: u64) -> std::io::Result<Bytes> {
        let len = (end - start + 1) as usize;
        let mut buf = vec![0u8; len];
        pread_exact(&self.file.lock(), &mut buf, start)?;
        Ok(Bytes::from(buf))
    }

    /// Write `data` at absolute offset `start`. Each call tracks how many
    /// bytes of each touched block are now on disk; when the cumulative
    /// total for a block reaches its full length, the bit is flipped and
    /// `bytes_cached` increments by the block's true length.
    pub fn write_range(&self, start: u64, data: &[u8]) -> std::io::Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        pwrite_all(&self.file.lock(), data, start)?;

        let end = start + data.len() as u64 - 1;
        let first_block = start / self.meta.block_size;
        let last_block = end / self.meta.block_size;

        let mut newly_complete: Vec<u64> = Vec::new();
        {
            let mut partial = self.partial.lock();
            let bm = self.bitmap.lock();
            for b in first_block..=last_block {
                let byte_idx = (b / 8) as usize;
                let bit = (b % 8) as u8;
                // Skip blocks that are already marked complete.
                if byte_idx < bm.len() && (bm[byte_idx] >> bit) & 1 != 0 {
                    continue;
                }
                let block_start = b * self.meta.block_size;
                let block_end =
                    ((b + 1) * self.meta.block_size - 1).min(self.meta.total_size - 1);
                let bl = block_end - block_start + 1;

                let in_start = start.max(block_start);
                let in_end = end.min(block_end);
                let contributed = in_end - in_start + 1;

                let slot = partial.get_mut(b as usize);
                let cur = slot.as_ref().map(|s| **s as u64).unwrap_or(0);
                let new_total = (cur + contributed).min(bl);
                if let Some(s) = slot {
                    *s = new_total as u32;
                }
                if new_total >= bl {
                    newly_complete.push(b);
                }
            }
        }

        if !newly_complete.is_empty() {
            let mut total_bytes_marked: u64 = 0;
            {
                let mut bm = self.bitmap.lock();
                for b in &newly_complete {
                    let byte_idx = (b / 8) as usize;
                    let bit = (b % 8) as u8;
                    if byte_idx < bm.len() && (bm[byte_idx] >> bit) & 1 == 0 {
                        bm[byte_idx] |= 1 << bit;
                        total_bytes_marked += self.block_len(*b);
                    }
                }
            }
            if total_bytes_marked > 0 {
                self.bytes_cached
                    .fetch_add(total_bytes_marked, Ordering::Relaxed);
                self.persist_bitmap()?;
            }
        }
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
}

impl CacheStore {
    pub fn new(root: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&root).map_err(ProxyError::Io)?;
        Ok(Self {
            root,
            entries: RwLock::new(HashMap::new()),
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

    /// Pick the right key derivation for a task. Single-volume tasks reuse the
    /// flat mirror-mode key (so existing caches keep working through the
    /// schema upgrade); multi-volume tasks get the layout-aware key.
    pub fn key_for_task(cfg: &crate::models::TaskConfig) -> String {
        let vols = cfg.effective_volumes();
        match vols.len() {
            0 => Self::key_for_urls(&cfg.urls),
            1 => Self::key_for_urls(&vols[0]),
            _ => Self::key_for_volume_layout(&vols),
        }
    }

    fn entry_dir(&self, key: &str) -> PathBuf {
        self.root.join(key)
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

        let dir = self.entry_dir(key);
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

        let entry = Arc::new(CacheEntry {
            key: key.to_string(),
            root: dir,
            file: Mutex::new(f),
            bitmap: Mutex::new(bm),
            partial: Mutex::new(vec![0u32; block_count as usize]),
            bytes_cached: AtomicU64::new(bytes_cached),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            meta: desired,
        });

        entries.insert(key.to_string(), Arc::clone(&entry));
        Ok(entry)
    }

    pub fn get(&self, key: &str) -> Option<Arc<CacheEntry>> {
        self.entries.read().get(key).cloned()
    }

    pub fn clear(&self, key: &str) -> Result<()> {
        self.entries.write().remove(key);
        let dir = self.entry_dir(key);
        if dir.exists() {
            std::fs::remove_dir_all(&dir).map_err(ProxyError::Io)?;
        }
        Ok(())
    }

    pub fn stats(&self, key: &str) -> Option<CacheStats> {
        self.entries.read().get(key).map(|e| e.stats())
    }

    /// Sum `bytes_cached` across all open entries. (Closed entries — wiped
    /// or never opened in this process — are not counted; that's fine for
    /// the UI's "current footprint" since we re-open lazily.)
    pub fn total_bytes_on_disk(&self) -> u64 {
        self.entries
            .read()
            .values()
            .map(|e| e.bytes_cached.load(Ordering::Relaxed))
            .sum()
    }
}

fn cache_meta_compatible(stored: &CacheMeta, desired: &CacheMeta) -> bool {
    if stored.total_size != desired.total_size {
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
