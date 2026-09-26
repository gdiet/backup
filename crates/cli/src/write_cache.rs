//! A per-open-file write cache (DESIGN-MOUNT-010/012/019 in `docs/design/mount-write-path.md`):
//! memory-first up to a budget shared across the whole mount session, then spilling any range that
//! does not fit to a sparse temporary file, without migrating what is already memory-resident;
//! tracks only the byte ranges this session actually writes, falling back to the file's
//! pre-existing content (via a caller-supplied reader, so this module stays independent of
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

    /// DESIGN-MOUNT-019's per-handle growth formula (`docs/design/mount-write-path.md`), computed
    /// and reserved atomically in one CAS loop: how much of `requested` (an open file handle's
    /// current write) that handle's own cache share may grow by right now, given `handle_used`
    /// bytes it has already claimed. Returns the granted amount (`0..=requested`, already reserved
    /// from the shared budget) - less than `requested` means the write does not fully fit within
    /// this handle's current fair share; a caller not using a partial grant must
    /// [`release`](Self::release) it back.
    pub fn try_acquire_share(&self, handle_used: u64, requested: u64) -> u64 {
        let mut granted = 0u64;
        let _ = self
            .available
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |avail| {
                let share = avail.saturating_sub(handle_used) / 2;
                granted = share.min(requested);
                Some(avail - granted)
            });
        granted
    }

    /// Returns `n` previously-[`try_acquire_share`](Self::try_acquire_share)d bytes to the shared
    /// budget, making them available to other reservations again.
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

/// The one subdirectory every spill file lives under, directly inside `--spill-directory` (or the
/// OS temp directory, its default) - grouped there instead of sitting loose among everything else
/// already in that directory, so an operator looking at either one sees a single, recognizable
/// entry rather than a scatter of individually-named files. Left behind once empty (removing it
/// would race a concurrent mount session's own spill files) - harmless, the same way the OS temp
/// directory itself accumulates other applications' own leftover directories.
const SPILL_SUBDIR: &str = "dfs-write-cache";

fn unique_spill_path(temp_dir: &Path) -> io::Result<PathBuf> {
    let subdir = temp_dir.join(SPILL_SUBDIR);
    std::fs::create_dir_all(&subdir)?;
    let n = SPILL_COUNTER.fetch_add(1, Ordering::Relaxed);
    Ok(subdir.join(format!("{}-{n}", std::process::id())))
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
    // Verified against a real Windows/NTFS volume: succeeds, and a scattered write pattern
    // afterward stays sparse (fsutil sparse queryflag/queryrange both confirm it - only the
    // written ranges show as allocated, not the full logical size).
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

/// A handle's private spillover file, created lazily on its first spill - DESIGN-MOUNT-019's
/// "once spilling starts, it never reconsiders memory": every write for a not-yet-cached range
/// lands here from then on, while whatever is already resident in [`WriteCache::mem_entries`]
/// stays there untouched.
struct SpillFile {
    file: File,
    path: PathBuf,
    /// `start -> length`, disjoint, sorted - the bytes themselves live in `file` at that same
    /// position.
    ranges: BTreeMap<u64, u64>,
}

impl SpillFile {
    fn create(temp_dir: &Path) -> io::Result<Self> {
        let path = unique_spill_path(temp_dir)?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)?;
        mark_sparse(&file)?;
        Ok(Self {
            file,
            path,
            ranges: BTreeMap::new(),
        })
    }

    fn write(&mut self, position: u64, data: &[u8]) -> io::Result<()> {
        self.file.seek(SeekFrom::Start(position))?;
        self.file.write_all(data)?;
        self.ranges.insert(position, data.len() as u64);
        Ok(())
    }

    fn read(&mut self, position: u64, len: u32) -> io::Result<Vec<u8>> {
        let mut buf = vec![0u8; len as usize];
        self.file.seek(SeekFrom::Start(position))?;
        self.file.read_exact(&mut buf)?;
        Ok(buf)
    }
}

impl Drop for SpillFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Which of [`WriteCache`]'s two backings a [`WriteCache::cached_ranges`] entry lives in.
#[derive(Clone, Copy)]
enum Source {
    Mem,
    Spill,
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
    /// `start -> bytes`, disjoint, sorted - byte ranges resident in memory. An entry here is never
    /// migrated to `spill` once written (DESIGN-MOUNT-019): only a range not yet covered by either
    /// backing ever spills.
    mem_entries: BTreeMap<u64, Vec<u8>>,
    /// Bytes currently reserved from `budget` for `mem_entries` - stays reserved for as long as
    /// this cache exists, even after it starts spilling; released as entries are cleared or
    /// overwritten, and in full once this cache is dropped.
    mem_bytes: u64,
    /// `Some` once this cache has spilled at least one write to disk - stays `Some` for the rest
    /// of this cache's lifetime (DESIGN-MOUNT-019): every later write for a not-yet-cached range
    /// goes straight here, without ever attempting to grow `mem_entries` again, regardless of
    /// whether the shared budget has since freed up.
    spill: Option<SpillFile>,
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
            mem_entries: BTreeMap::new(),
            mem_bytes: 0,
            spill: None,
            budget,
            temp_dir: temp_dir.into(),
        }
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    /// The total bytes currently spilled to disk - `0` while nothing has spilled yet. Test-only:
    /// no production code reads this directly (DESIGN-MOUNT-006's `bytesInPersistQueue`
    /// backpressure signal is tracked at the `JobPool`/`Settler` level instead,
    /// `crate::settle_pool`), but it stays a useful assertion for tests that need to confirm a
    /// write actually spilled.
    #[cfg(test)]
    pub fn spilled_bytes(&self) -> u64 {
        self.spill.as_ref().map_or(0, |s| s.ranges.values().sum())
    }

    /// Writes `data` at `position`, extending the cache's logical size if this write reaches past
    /// it. Buffers in memory while the shared budget allows it; once a write does not fit, that
    /// write (and every later write for a range not already cached) spills to a private sparse
    /// temporary file instead, without disturbing what is already memory-resident
    /// (DESIGN-MOUNT-019).
    pub fn write(&mut self, position: u64, data: &[u8]) -> io::Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        let end = position + data.len() as u64;
        self.clear_range(position, Some(end));

        if self.spill.is_some() {
            // Already spilling: this handle never reattempts memory (DESIGN-MOUNT-019), even if
            // the shared budget has since freed up.
            self.write_into_spill(position, data)?;
        } else {
            let want = data.len() as u64;
            let granted = self.budget.try_acquire_share(self.mem_bytes, want);
            if granted < want {
                // DESIGN-MOUNT-019: this write does not fully fit within this handle's current
                // fair share of the budget - a partial grant is not partially used (no split
                // representation for "half of this write is memory-resident, half is spilled"),
                // so it is released back and this write spills in full. Content already resident
                // in `mem_entries` from earlier writes is untouched.
                if granted > 0 {
                    self.budget.release(granted);
                }
                self.spill = Some(SpillFile::create(&self.temp_dir)?);
                self.write_into_spill(position, data)?;
            } else {
                // Fast path: a pure append directly onto the immediately preceding entry extends
                // it in place (amortized O(1), like an ordinary Vec push) instead of allocating a
                // new entry next to it - without this, a file written as many small sequential
                // writes (the common case for a straight file copy) would accumulate one entry per
                // write, and any later full-range read would still be correct but needlessly slow
                // to assemble. A write landing anywhere else becomes its own new entry; merging on
                // that side too is not implemented, the same sequential-write pattern is the one
                // worth optimizing for.
                let appended = self
                    .mem_entries
                    .range_mut(..position)
                    .next_back()
                    .is_some_and(|(&prev_start, prev_data)| {
                        prev_start + prev_data.len() as u64 == position
                    });
                if appended {
                    let prev_data = self
                        .mem_entries
                        .range_mut(..position)
                        .next_back()
                        .unwrap()
                        .1;
                    prev_data.extend_from_slice(data);
                } else {
                    self.mem_entries.insert(position, data.to_vec());
                }
                self.mem_bytes += want;
            }
        }

        self.size = self.size.max(end);
        Ok(())
    }

    fn write_into_spill(&mut self, position: u64, data: &[u8]) -> io::Result<()> {
        self.spill
            .as_mut()
            .expect("write_into_spill is only ever called once already spilling")
            .write(position, data)
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
    /// infinity") in both backings, splitting an entry that only partially overlaps so its
    /// untouched portion(s) survive under their own (possibly new) position.
    fn clear_range(&mut self, start: u64, end: Option<u64>) {
        let overlapping: Vec<u64> = self
            .mem_entries
            .iter()
            .filter(|&(&k, v)| overlaps(k, k + v.len() as u64, start, end))
            .map(|(&k, _)| k)
            .collect();
        for k in overlapping {
            let data = self
                .mem_entries
                .remove(&k)
                .expect("key just found via iter()");
            let entry_end = k + data.len() as u64;
            if k < start {
                self.mem_entries
                    .insert(k, data[..(start - k) as usize].to_vec());
            }
            if let Some(end) = end
                && entry_end > end
            {
                self.mem_entries
                    .insert(end, data[(end - k) as usize..].to_vec());
            }
        }
        let new_total: u64 = self.mem_entries.values().map(|v| v.len() as u64).sum();
        if new_total < self.mem_bytes {
            self.budget.release(self.mem_bytes - new_total);
        }
        self.mem_bytes = new_total;

        if let Some(spill) = &mut self.spill {
            let overlapping: Vec<(u64, u64)> = spill
                .ranges
                .iter()
                .filter(|&(&k, &len)| overlaps(k, k + len, start, end))
                .map(|(&k, &len)| (k, len))
                .collect();
            for (k, len) in overlapping {
                spill.ranges.remove(&k);
                let entry_end = k + len;
                if k < start {
                    spill.ranges.insert(k, start - k);
                }
                if let Some(end) = end
                    && entry_end > end
                {
                    spill.ranges.insert(end, entry_end - end);
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
        while cached.peek().is_some_and(|&(start, l, _)| start + l <= pos) {
            cached.next();
        }

        while pos < end {
            match cached.peek().copied() {
                Some((start, l, source)) if start <= pos => {
                    let avail = (start + l) - pos;
                    let take = avail.min(end - pos) as u32;
                    result.extend_from_slice(&self.read_cached(pos, take, source)?);
                    pos += u64::from(take);
                    if pos >= start + l {
                        cached.next();
                    }
                }
                next => {
                    let gap_end = next.map_or(end, |(start, _, _)| start.min(end));
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

    /// Every tracked entry across both backings, merged and sorted by position - disjoint, since
    /// `clear_range` already keeps `mem_entries` and a spill's `ranges` each individually disjoint,
    /// and a byte position is only ever tracked in one backing at a time.
    fn cached_ranges(&self) -> Vec<(u64, u64, Source)> {
        let mut ranges: Vec<(u64, u64, Source)> = self
            .mem_entries
            .iter()
            .map(|(&k, v)| (k, v.len() as u64, Source::Mem))
            .collect();
        if let Some(spill) = &self.spill {
            ranges.extend(spill.ranges.iter().map(|(&k, &v)| (k, v, Source::Spill)));
        }
        ranges.sort_by_key(|&(k, _, _)| k);
        ranges
    }

    /// Reads `len` bytes starting at `position`, which the caller guarantees falls entirely
    /// inside one tracked entry in the backing `source` names.
    fn read_cached(&mut self, position: u64, len: u32, source: Source) -> io::Result<Vec<u8>> {
        match source {
            Source::Mem => {
                let (&start, data) = self
                    .mem_entries
                    .range(..=position)
                    .next_back()
                    .expect("caller guarantees a covering entry exists");
                let offset = (position - start) as usize;
                Ok(data[offset..offset + len as usize].to_vec())
            }
            Source::Spill => self
                .spill
                .as_mut()
                .expect("source == Spill implies self.spill is Some")
                .read(position, len),
        }
    }
}

impl Drop for WriteCache {
    fn drop(&mut self) {
        self.budget.release(self.mem_bytes);
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
    fn try_acquire_share_grants_half_the_available_budget_to_a_single_growing_handle() {
        let budget = MemoryBudget::new(1000);
        // First write: this handle owns nothing yet, so it may grow by up to half of what is
        // available - converging directly to its 50% equilibrium, not gradually.
        let granted = budget.try_acquire_share(0, 1000);
        assert_eq!(granted, 500);
        // A second write finds nothing further available for this same handle: it already sits
        // exactly at its own equilibrium (DESIGN-MOUNT-019's `H = A` convergence point).
        let granted = budget.try_acquire_share(500, 1000);
        assert_eq!(granted, 0);
    }

    #[test]
    fn try_acquire_share_gives_a_second_handle_half_of_what_the_first_left_behind() {
        let budget = MemoryBudget::new(1000);
        let first = budget.try_acquire_share(0, 1000);
        assert_eq!(first, 500); // first handle converges to 50% of the total
        let second = budget.try_acquire_share(0, 1000);
        assert_eq!(second, 250); // second handle converges to 25% of the total
        let third = budget.try_acquire_share(0, 1000);
        assert_eq!(third, 125); // third handle converges to 12.5%
    }

    #[test]
    fn try_acquire_share_grants_at_most_the_requested_amount_even_with_budget_to_spare() {
        let budget = MemoryBudget::new(1000);
        assert_eq!(budget.try_acquire_share(0, 10), 10);
    }

    #[test]
    fn try_acquire_share_never_underflows_when_handle_used_exceeds_available() {
        // A second handle's own already-claimed share can exceed what a first, larger handle has
        // left as "available" system-wide - the max(0, ...) clamp (DESIGN-MOUNT-019) must return
        // 0 here rather than panicking or wrapping via unsigned subtraction underflow.
        let budget = MemoryBudget::new(1000);
        assert_eq!(budget.try_acquire_share(2000, 100), 0);
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
    fn writing_past_the_shared_budget_spills_only_the_write_that_does_not_fit() {
        // A single handle's own share caps at half the (remaining) budget (DESIGN-MOUNT-019): 20
        // grants exactly 10 for the first write, leaving no further share for this same handle
        // until it is released, so the second write spills - but only the second write's own 4
        // bytes, not the first write's already-memory-resident 10.
        let (mut cache, _dir) = cache(20, 0);
        cache.write(0, b"0123456789").unwrap();
        cache.write(10, b"more").unwrap();
        assert_eq!(cache.spilled_bytes(), 4);
        assert_eq!(cache.mem_bytes, 10);
        let data = cache.read(0, 14, no_original).unwrap();
        assert_eq!(data, b"0123456789more");
    }

    #[test]
    fn a_spilled_write_lands_under_a_dedicated_subdirectory_not_loose_in_temp_dir() {
        let (mut cache, dir) = cache(0, 0);
        cache
            .write(0, b"spills immediately, no budget at all")
            .unwrap();
        assert!(cache.spilled_bytes() > 0, "test setup must actually spill");

        let subdir = dir.path().join(SPILL_SUBDIR);
        assert!(
            subdir.is_dir(),
            "expected a {SPILL_SUBDIR} subdirectory under the given temp_dir"
        );
        let top_level_files: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect();
        assert_eq!(
            top_level_files,
            vec![subdir.clone()],
            "the only entry directly under temp_dir must be the {SPILL_SUBDIR} subdirectory - \
             no loose spill file beside it"
        );
        assert_eq!(
            std::fs::read_dir(&subdir).unwrap().count(),
            1,
            "expected exactly one spill file inside {SPILL_SUBDIR}"
        );
    }

    #[test]
    fn a_handle_that_has_spilled_keeps_spilling_new_ranges_even_once_budget_frees_up_again() {
        // A shared budget of 20: `a` claims its whole 10-byte share and then spills a further
        // write, leaving `a` in spilling mode. Dropping a competing handle that held the rest of
        // the budget frees it all up again - `a`'s own next write for a still-uncached range must
        // still spill, not silently start caching in memory again, per DESIGN-MOUNT-019's "once
        // spilling starts, it never reconsiders memory".
        let budget = Arc::new(MemoryBudget::new(20));
        let dir = tempfile::tempdir().unwrap();
        let mut a = WriteCache::new(dir.path(), Arc::clone(&budget), 0);
        a.write(0, b"0123456789").unwrap(); // claims a's whole 10-byte share
        a.write(10, b"more").unwrap(); // does not fit -> a starts spilling
        assert_eq!(a.spilled_bytes(), 4);

        // Nothing else is competing for the budget, so plenty is technically available now.
        drop(WriteCache::new(dir.path(), Arc::clone(&budget), 0));

        a.write(14, b"even-more").unwrap();
        assert_eq!(
            a.spilled_bytes(),
            13,
            "a new write on an already-spilling handle must keep spilling, not re-acquire memory"
        );
        assert_eq!(
            a.mem_bytes, 10,
            "the original in-memory entry must stay untouched"
        );
        assert_eq!(
            a.read(0, 23, no_original).unwrap(),
            b"0123456789moreeven-more"
        );
    }

    #[test]
    fn dropping_a_cache_that_never_spilled_releases_its_whole_reservation() {
        let budget = Arc::new(MemoryBudget::new(20));
        let dir = tempfile::tempdir().unwrap();
        let mut a = WriteCache::new(dir.path(), Arc::clone(&budget), 0);
        a.write(0, b"0123456789").unwrap(); // claims a's whole 10-byte share
        drop(a);

        // With a's reservation released, b can claim the same share again.
        let mut b = WriteCache::new(dir.path(), Arc::clone(&budget), 0);
        b.write(0, b"0123456789").unwrap();
        assert_eq!(b.spilled_bytes(), 0);
        assert_eq!(b.read(0, 10, no_original).unwrap(), b"0123456789");
    }

    #[test]
    fn a_pure_sequential_append_reuses_the_same_entry_instead_of_growing_the_entry_count() {
        let (mut cache, _dir) = cache(1000, 0);
        for chunk in [&b"aa"[..], &b"bb"[..], &b"cc"[..]] {
            let pos = cache.size();
            cache.write(pos, chunk).unwrap();
        }
        assert!(cache.spill.is_none(), "must not have spilled at this size");
        assert_eq!(
            cache.mem_entries.len(),
            1,
            "entries: {:?}",
            cache.mem_entries
        );
        assert_eq!(cache.read(0, 6, no_original).unwrap(), b"aabbcc");
    }

    #[test]
    fn clearing_entries_below_the_budget_releases_it_for_reuse() {
        let (mut cache, _dir) = cache(20, 0);
        cache.write(0, b"0123456789").unwrap(); // uses this handle's whole 10-byte share
        cache.write(0, b"XXXXX").unwrap(); // overwrites/shrinks the tracked footprint to 10 bytes total still (X's + tail 56789)
        // Still fits: total tracked bytes must not exceed original write size after overwrite.
        assert_eq!(cache.read(0, 10, no_original).unwrap(), b"XXXXX56789");
    }
}
