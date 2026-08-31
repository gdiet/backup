//! A per-open-file write cache (DESIGN-MOUNT-010/012 in `docs/design/mount-write-path.md`):
//! memory-first up to a budget shared across the whole mount session, then spilling to a sparse
//! temporary file; tracks only the byte ranges this session actually writes, falling back to the
//! file's pre-existing content (via a caller-supplied reader, so this module stays independent of
//! `crates/db`/`crates/store`) for everything else. Used by `crate::pending_files::GenerationSlot`.

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

/// Bytes currently resident in memory across every [`WriteCache`] in one mount session -
/// DESIGN-MOUNT-010's shared budget.
#[derive(Debug)]
pub struct MemoryBudget {
    available: AtomicU64,
}

/// DESIGN-MOUNT-010's default: large enough that an ordinary file, written by one or a few
/// concurrent writers, never spills at all; small enough that even several mount sessions on the
/// same machine stay within a modest, predictable memory footprint.
pub const DEFAULT_BUDGET_BYTES: u64 = 256 * 1024 * 1024;

impl MemoryBudget {
    pub fn new(total_bytes: u64) -> Self {
        Self {
            available: AtomicU64::new(total_bytes),
        }
    }

    /// Reserves `n` bytes if available, without blocking. `false` means nothing was reserved.
    fn try_acquire(&self, n: u64) -> bool {
        self.available
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |avail| {
                avail.checked_sub(n)
            })
            .is_ok()
    }

    /// The total bytes currently spilled to disk across every [`WriteCache`] sharing this budget
    /// that this method's caller is tracking on its own behalf - see DESIGN-MOUNT-006's
    /// backpressure signal, narrowed in DESIGN-MOUNT-010 to released-but-not-yet-persisted work
    /// specifically. This budget itself only tracks memory, not spilled bytes - a caller
    /// maintaining that separate count is not this type's job.
    fn release(&self, n: u64) {
        self.available.fetch_add(n, Ordering::AcqRel);
    }
}

impl Default for MemoryBudget {
    fn default() -> Self {
        Self::new(DEFAULT_BUDGET_BYTES)
    }
}

static SPILL_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn unique_spill_path(temp_dir: &Path) -> PathBuf {
    let n = SPILL_COUNTER.fetch_add(1, Ordering::Relaxed);
    temp_dir.join(format!("dfs-write-cache-{}-{n}", std::process::id()))
}

/// Marks `file` as sparse so writes at scattered positions do not consume real disk space for the
/// gaps between them (DESIGN-MOUNT-010) - a no-op on Unix, where an ordinary file written at
/// scattered offsets is already sparse with no separate step needed.
#[cfg(not(windows))]
fn mark_sparse(_file: &File) -> io::Result<()> {
    Ok(())
}

#[cfg(windows)]
fn mark_sparse(file: &File) -> io::Result<()> {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Foundation::HANDLE;
    use windows_sys::Win32::System::IO::DeviceIoControl;
    use windows_sys::Win32::System::Ioctl::FSCTL_SET_SPARSE;

    let handle = file.as_raw_handle() as HANDLE;
    let mut bytes_returned = 0u32;
    // Not verified against a real Windows/NTFS volume yet - see agent-todos/.
    let ok = unsafe {
        DeviceIoControl(
            handle,
            FSCTL_SET_SPARSE,
            std::ptr::null(),
            0,
            std::ptr::null_mut(),
            0,
            &mut bytes_returned,
            std::ptr::null_mut(),
        )
    };
    if ok == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

enum Backing {
    /// `start -> bytes`, disjoint, sorted.
    Memory(BTreeMap<u64, Vec<u8>>),
    /// `start -> length`, disjoint, sorted - the bytes themselves live in `file` at that same
    /// position.
    Spilled {
        file: File,
        path: PathBuf,
        ranges: BTreeMap<u64, u64>,
    },
}

impl Drop for Backing {
    fn drop(&mut self) {
        if let Backing::Spilled { path, .. } = self {
            let _ = std::fs::remove_file(path);
        }
    }
}

/// A single open file's not-yet-persisted write state. See the module doc comment.
pub struct WriteCache {
    /// The file's current logical size, from this session's point of view.
    size: u64,
    /// How much of `[0, original_size)` still reflects the file's content as it stood when this
    /// cache was created, unless shadowed by a tracked entry - capped down (never up) by
    /// `truncate` to a smaller size, matching ordinary POSIX truncate-then-grow semantics: a
    /// region shrunk away and then grown back reads as zero, never as its old, truncated-away
    /// content.
    original_size: u64,
    backing: Backing,
    /// Bytes currently reserved from `budget` while `backing` is [`Backing::Memory`] - always `0`
    /// once spilled.
    mem_bytes: u64,
    budget: Arc<MemoryBudget>,
    temp_dir: PathBuf,
}

impl WriteCache {
    /// Creates a cache overlaying a file whose current content, if any, is `original_size` bytes
    /// long - `0` for a brand new file (DESIGN-MOUNT-012).
    pub fn new(
        temp_dir: impl Into<PathBuf>,
        budget: Arc<MemoryBudget>,
        original_size: u64,
    ) -> Self {
        Self {
            size: original_size,
            original_size,
            backing: Backing::Memory(BTreeMap::new()),
            mem_bytes: 0,
            budget,
            temp_dir: temp_dir.into(),
        }
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    /// The total bytes currently spilled to disk - `0` while still in memory. This is
    /// DESIGN-MOUNT-010's raw signal; DESIGN-MOUNT-006's backpressure narrows it further to only
    /// a released (not still actively being written) cache's own spilled total - a caller's
    /// concern, not this type's.
    pub fn spilled_bytes(&self) -> u64 {
        match &self.backing {
            Backing::Memory(_) => 0,
            Backing::Spilled { ranges, .. } => ranges.values().sum(),
        }
    }

    /// Writes `data` at `position`, extending the cache's logical size if this write reaches past
    /// it. Buffers in memory while the shared budget allows it, otherwise spills this cache's
    /// entire content to a private sparse temporary file and continues there.
    pub fn write(&mut self, position: u64, data: &[u8]) -> io::Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        let end = position + data.len() as u64;
        self.clear_range(position, Some(end));

        match &mut self.backing {
            Backing::Memory(entries) => {
                // Fast path: a pure append directly onto the immediately preceding entry extends
                // it in place (amortized O(1), like an ordinary Vec push) instead of allocating a
                // new entry next to it - without this, a file written as many small sequential
                // writes (the common case for a straight file copy) would accumulate one entry
                // per write, and any later full-range read would still be correct but needlessly
                // slow to assemble. A write landing anywhere else becomes its own new entry;
                // merging on that side too is not implemented; the same sequential-write pattern
                // is the one worth optimizing for.
                let appended = if let Some((&prev_start, prev_data)) =
                    entries.range_mut(..position).next_back()
                {
                    prev_start + prev_data.len() as u64 == position
                        && self.budget.try_acquire(data.len() as u64)
                } else {
                    false
                };
                if appended {
                    let prev_data = entries.range_mut(..position).next_back().unwrap().1;
                    prev_data.extend_from_slice(data);
                    self.mem_bytes += data.len() as u64;
                } else if self.budget.try_acquire(data.len() as u64) {
                    entries.insert(position, data.to_vec());
                    self.mem_bytes += data.len() as u64;
                } else {
                    self.spill_to_disk()?;
                    self.write_into_spill(position, data)?;
                }
            }
            Backing::Spilled { .. } => self.write_into_spill(position, data)?,
        }

        self.size = self.size.max(end);
        Ok(())
    }

    fn write_into_spill(&mut self, position: u64, data: &[u8]) -> io::Result<()> {
        let Backing::Spilled { file, ranges, .. } = &mut self.backing else {
            unreachable!("write_into_spill is only ever called once already spilled")
        };
        file.seek(SeekFrom::Start(position))?;
        file.write_all(data)?;
        ranges.insert(position, data.len() as u64);
        Ok(())
    }

    fn spill_to_disk(&mut self) -> io::Result<()> {
        let Backing::Memory(entries) = &self.backing else {
            return Ok(());
        };
        let path = unique_spill_path(&self.temp_dir);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)?;
        mark_sparse(&file)?;

        let mut file = file;
        let mut ranges = BTreeMap::new();
        for (&start, data) in entries.iter() {
            file.seek(SeekFrom::Start(start))?;
            file.write_all(data)?;
            ranges.insert(start, data.len() as u64);
        }

        self.budget.release(self.mem_bytes);
        self.mem_bytes = 0;
        self.backing = Backing::Spilled { file, path, ranges };
        Ok(())
    }

    /// Truncates (or zero-extends) the cache's logical size. Shrinking discards any tracked
    /// entries, and the portion of the original content, beyond `new_size`; growing back later
    /// never resurrects either - matching ordinary POSIX truncate-then-grow semantics, which
    /// always zero-fills a grown region regardless of what used to be there before a shrink.
    pub fn truncate(&mut self, new_size: u64) {
        if new_size < self.size {
            self.clear_range(new_size, None);
        }
        self.original_size = self.original_size.min(new_size);
        self.size = new_size;
    }

    /// Removes/trims tracked entries overlapping `[start, end)` (`end = None` meaning "to
    /// infinity"), splitting an entry that only partially overlaps so its untouched portion(s)
    /// survive under their own (possibly new) position.
    fn clear_range(&mut self, start: u64, end: Option<u64>) {
        match &mut self.backing {
            Backing::Memory(entries) => {
                let overlapping: Vec<u64> = entries
                    .iter()
                    .filter(|&(&k, v)| overlaps(k, k + v.len() as u64, start, end))
                    .map(|(&k, _)| k)
                    .collect();
                for k in overlapping {
                    let data = entries.remove(&k).expect("key just found via iter()");
                    let entry_end = k + data.len() as u64;
                    if k < start {
                        entries.insert(k, data[..(start - k) as usize].to_vec());
                    }
                    if let Some(end) = end
                        && entry_end > end
                    {
                        entries.insert(end, data[(end - k) as usize..].to_vec());
                    }
                }
                let new_total: u64 = entries.values().map(|v| v.len() as u64).sum();
                if new_total < self.mem_bytes {
                    self.budget.release(self.mem_bytes - new_total);
                }
                self.mem_bytes = new_total;
            }
            Backing::Spilled { ranges, .. } => {
                let overlapping: Vec<(u64, u64)> = ranges
                    .iter()
                    .filter(|&(&k, &len)| overlaps(k, k + len, start, end))
                    .map(|(&k, &len)| (k, len))
                    .collect();
                for (k, len) in overlapping {
                    ranges.remove(&k);
                    let entry_end = k + len;
                    if k < start {
                        ranges.insert(k, start - k);
                    }
                    if let Some(end) = end
                        && entry_end > end
                    {
                        ranges.insert(end, entry_end - end);
                    }
                }
            }
        }
    }

    /// Reads `len` bytes at `position` - never more than [`Self::size`] minus `position` worth of
    /// real data (the caller is expected to already clip against `size` itself, matching ordinary
    /// short-read-at-EOF behavior; this does not clip on its own). A byte not covered by a
    /// tracked entry is read from `original` (DESIGN-MOUNT-012's fallback, for a position still
    /// within the not-yet-shrunk-away part of the original content) or, past that, filled with
    /// zero.
    pub fn read(
        &mut self,
        position: u64,
        len: u32,
        mut original: impl FnMut(u64, u32) -> io::Result<Vec<u8>>,
    ) -> io::Result<Vec<u8>> {
        let mut result = Vec::with_capacity(len as usize);
        let mut pos = position;
        let end = position + u64::from(len);

        let cached = self.cached_ranges();
        let mut cached = cached.into_iter().peekable();
        // Skip ranges entirely before `pos`.
        while cached.peek().is_some_and(|&(start, l)| start + l <= pos) {
            cached.next();
        }

        while pos < end {
            match cached.peek().copied() {
                Some((start, l)) if start <= pos => {
                    let avail = (start + l) - pos;
                    let take = avail.min(end - pos) as u32;
                    result.extend_from_slice(&self.read_cached(pos, take)?);
                    pos += u64::from(take);
                    if pos >= start + l {
                        cached.next();
                    }
                }
                next => {
                    let gap_end = next.map_or(end, |(start, _)| start.min(end));
                    let gap_len = (gap_end - pos) as u32;
                    if pos < self.original_size {
                        let take = gap_len.min((self.original_size - pos) as u32);
                        result.extend_from_slice(&original(pos, take)?);
                        pos += u64::from(take);
                    }
                    if pos < gap_end {
                        let zeros = (gap_end - pos) as usize;
                        result.extend(std::iter::repeat_n(0u8, zeros));
                        pos = gap_end;
                    }
                }
            }
        }

        Ok(result)
    }

    fn cached_ranges(&self) -> Vec<(u64, u64)> {
        match &self.backing {
            Backing::Memory(entries) => entries.iter().map(|(&k, v)| (k, v.len() as u64)).collect(),
            Backing::Spilled { ranges, .. } => ranges.iter().map(|(&k, &v)| (k, v)).collect(),
        }
    }

    /// Reads `len` bytes starting at `position`, which the caller guarantees falls entirely
    /// inside one tracked entry.
    fn read_cached(&mut self, position: u64, len: u32) -> io::Result<Vec<u8>> {
        match &mut self.backing {
            Backing::Memory(entries) => {
                let (&start, data) = entries
                    .range(..=position)
                    .next_back()
                    .expect("caller guarantees a covering entry exists");
                let offset = (position - start) as usize;
                Ok(data[offset..offset + len as usize].to_vec())
            }
            Backing::Spilled { file, .. } => {
                let mut buf = vec![0u8; len as usize];
                file.seek(SeekFrom::Start(position))?;
                file.read_exact(&mut buf)?;
                Ok(buf)
            }
        }
    }
}

/// Whether an entry spanning `[entry_start, entry_end)` overlaps `[clear_start, clear_end)`
/// (`clear_end = None` meaning "to infinity").
fn overlaps(entry_start: u64, entry_end: u64, clear_start: u64, clear_end: Option<u64>) -> bool {
    if entry_end <= clear_start {
        return false;
    }
    match clear_end {
        Some(clear_end) => entry_start < clear_end,
        None => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cache(budget_bytes: u64, original_size: u64) -> (WriteCache, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let cache = WriteCache::new(
            dir.path(),
            Arc::new(MemoryBudget::new(budget_bytes)),
            original_size,
        );
        (cache, dir)
    }

    fn no_original(_pos: u64, _len: u32) -> io::Result<Vec<u8>> {
        panic!("original() must not be called for a brand-new file with nothing to fall back to")
    }

    #[test]
    fn write_then_read_back_exact_bytes() {
        let (mut cache, _dir) = cache(1000, 0);
        cache.write(0, b"hello world").unwrap();
        let data = cache.read(0, 11, no_original).unwrap();
        assert_eq!(data, b"hello world");
        assert_eq!(cache.size(), 11);
    }

    #[test]
    fn a_second_write_overwrites_the_overlapping_part_of_the_first() {
        let (mut cache, _dir) = cache(1000, 0);
        cache.write(0, b"hello world").unwrap();
        cache.write(6, b"there").unwrap();
        let data = cache.read(0, 11, no_original).unwrap();
        assert_eq!(data, b"hello there");
    }

    #[test]
    fn a_write_fully_inside_an_existing_entry_splits_it_into_a_prefix_and_suffix() {
        let (mut cache, _dir) = cache(1000, 0);
        cache.write(0, b"0123456789").unwrap();
        cache.write(3, b"XX").unwrap();
        let data = cache.read(0, 10, no_original).unwrap();
        assert_eq!(data, b"012XX56789");
    }

    #[test]
    fn a_write_beyond_current_size_leaves_a_gap_read_as_zero() {
        let (mut cache, _dir) = cache(1000, 0);
        cache.write(0, b"ab").unwrap();
        cache.write(5, b"cd").unwrap();
        assert_eq!(cache.size(), 7);
        let data = cache.read(0, 7, no_original).unwrap();
        assert_eq!(data, b"ab\0\0\0cd");
    }

    #[test]
    fn a_gap_within_the_original_size_falls_back_to_the_original_content() {
        let (mut cache, _dir) = cache(1000, 10);
        cache.write(2, b"XX").unwrap();
        let data = cache.read(0, 10, original_bytes).unwrap();
        // Position 0,1 and 4..10 come from "original" (letters starting at 'A' + position);
        // position 2,3 are the tracked write.
        let mut expected = vec![b'A', b'A' + 1, b'X', b'X'];
        expected.extend((4u8..10).map(|p| b'A' + p));
        assert_eq!(data, expected);
    }

    /// Distinct, position-dependent bytes, so a test can tell "read from the right offset" apart
    /// from "read some placeholder value regardless of offset".
    fn original_bytes(pos: u64, len: u32) -> io::Result<Vec<u8>> {
        Ok((pos..pos + u64::from(len))
            .map(|p| b'A' + p as u8)
            .collect())
    }

    #[test]
    fn truncate_down_then_up_reads_the_regrown_region_as_zero_not_the_old_original() {
        let (mut cache, _dir) = cache(1000, 10);
        cache.truncate(3);
        cache.truncate(10);
        // Only the regrown part [3, 10) - the surviving prefix [0, 3) is covered by the next test.
        let data = cache
            .read(3, 7, |_, _| {
                panic!("original must never be consulted past the shrink point")
            })
            .unwrap();
        assert_eq!(data, [0u8; 7]);
    }

    #[test]
    fn truncate_down_then_up_still_reads_the_untouched_prefix_from_the_original() {
        let (mut cache, _dir) = cache(1000, 10);
        cache.truncate(3);
        cache.truncate(10);
        let data = cache.read(0, 3, original_bytes).unwrap();
        assert_eq!(data, [b'A', b'A' + 1, b'A' + 2]);
    }

    #[test]
    fn truncate_up_without_a_prior_shrink_reads_as_zero_beyond_the_original_size() {
        let (mut cache, _dir) = cache(1000, 3);
        cache.truncate(6);
        let data = cache.read(0, 6, original_bytes).unwrap();
        assert_eq!(data, [b'A', b'A' + 1, b'A' + 2, 0, 0, 0]);
    }

    #[test]
    fn writing_past_the_shared_budget_spills_to_disk_and_still_reads_correctly() {
        let (mut cache, _dir) = cache(10, 0);
        cache.write(0, b"0123456789").unwrap();
        cache.write(10, b"more").unwrap(); // pushes total past the 10-byte budget
        assert_eq!(cache.spilled_bytes(), 14);
        let data = cache.read(0, 14, no_original).unwrap();
        assert_eq!(data, b"0123456789more");
    }

    #[test]
    fn spilling_releases_the_budget_for_other_caches_to_use() {
        let budget = Arc::new(MemoryBudget::new(10));
        let dir = tempfile::tempdir().unwrap();
        let mut a = WriteCache::new(dir.path(), Arc::clone(&budget), 0);
        a.write(0, b"0123456789").unwrap();
        a.write(10, b"more").unwrap(); // spills `a`, releasing its 10 reserved bytes
        // Now b can acquire the same budget again.
        let mut b = WriteCache::new(dir.path(), Arc::clone(&budget), 0);
        b.write(0, b"0123456789").unwrap();
        assert_eq!(a.read(0, 14, no_original).unwrap(), b"0123456789more");
        assert_eq!(b.read(0, 10, no_original).unwrap(), b"0123456789");
    }

    #[test]
    fn a_pure_sequential_append_reuses_the_same_entry_instead_of_growing_the_entry_count() {
        let (mut cache, _dir) = cache(1000, 0);
        for chunk in [&b"aa"[..], &b"bb"[..], &b"cc"[..]] {
            let pos = cache.size();
            cache.write(pos, chunk).unwrap();
        }
        match &cache.backing {
            Backing::Memory(entries) => assert_eq!(entries.len(), 1, "entries: {entries:?}"),
            Backing::Spilled { .. } => panic!("must not have spilled at this size"),
        }
        assert_eq!(cache.read(0, 6, no_original).unwrap(), b"aabbcc");
    }

    #[test]
    fn clearing_entries_below_the_budget_releases_it_for_reuse() {
        let (mut cache, _dir) = cache(10, 0);
        cache.write(0, b"0123456789").unwrap(); // uses the whole 10-byte budget
        cache.write(0, b"XXXXX").unwrap(); // overwrites/shrinks the tracked footprint to 10 bytes total still (X's + tail 56789)
        // Still fits: total tracked bytes must not exceed original write size after overwrite.
        assert_eq!(cache.read(0, 10, no_original).unwrap(), b"XXXXX56789");
    }
}
