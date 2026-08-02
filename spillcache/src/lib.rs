//! A random-access byte buffer with a shared RAM budget and automatic disk
//! spillover: write bytes at any position, read them back (including
//! ranges spanning both RAM- and disk-resident data transparently), grow
//! or shrink it - all while a fixed byte budget, shared across every
//! buffer created against it, caps how much RAM the whole set can use at
//! once. Once that budget is exhausted, further bytes spill to a private
//! temp file per buffer instead of failing or blocking.
//!
//! A good fit for assembling data of unpredictable, potentially large size
//! from many small writes - e.g. buffering content written through a
//! virtual filesystem before it's persisted elsewhere, or accumulating a
//! variable-length unit of data (a chunk, a record, a message) piece by
//! piece - in any setting where several of these buffers might be active
//! at once and the total memory they use collectively needs a hard
//! ceiling, without requiring any individual buffer to know its own
//! eventual size upfront.
//!
//! # Building blocks
//!
//! - [`RamBudget`] is the shared ceiling: a fixed byte count, reserved
//!   from and released back to as buffers grow and shrink. Reservation
//!   never blocks - once exhausted, a [`WriteCache`] falls back to disk
//!   instead.
//! - [`WriteCache`] is the buffer itself: three internal tiers (in-RAM
//!   bytes, a disk-spillover file, and zero-filled holes from growing past
//!   the previously-written end) that [`WriteCache::read_filling_gaps`]
//!   reads through transparently, falling through to a caller-supplied
//!   callback for anything not written in this session at all.

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

/// A fixed byte budget shared (via `Arc`) by every [`WriteCache`] created
/// against it. Each reservation attempt either succeeds immediately or
/// fails immediately - there is no blocking/waiting variant, by design:
/// see [`WriteCache`]'s disk-spillover tier, which is what callers are
/// expected to fall back to on failure.
pub struct RamBudget(AtomicI64);

impl RamBudget {
    pub fn new(bytes: u64) -> Self {
        Self(AtomicI64::new(bytes.min(i64::MAX as u64) as i64))
    }

    /// `false` if not enough budget remains - the caller is expected to
    /// fall back to disk spillover, not block.
    fn try_acquire(&self, size: u64) -> bool {
        let size = size as i64;
        loop {
            let avail = self.0.load(Ordering::Relaxed);
            if avail < size {
                return false;
            }
            if self
                .0
                .compare_exchange_weak(avail, avail - size, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                return true;
            }
        }
    }

    fn release(&self, size: u64) {
        self.0.fetch_add(size as i64, Ordering::Relaxed);
    }
}

/// Tracks non-overlapping `position -> length` spans - shared by the disk
/// spillover tier (bytes live in the spill file at the same offset, only
/// the length needs tracking here) and the zero-hole tier (bytes are
/// implicitly zero, nothing stored at all).
#[derive(Default, Debug, PartialEq, Eq)]
struct LengthSpanMap(BTreeMap<u64, u64>);

/// One span of a [`LengthSpanMap`]/[`ByteSpanMap`] read, clipped to the
/// requested range: either present (with its data/length) or a gap - not
/// part of this cache at all, the caller must resolve it some other way.
enum Span<T> {
    Present(T),
    Gap(u64),
}

impl LengthSpanMap {
    /// Removes (or trims) any spans overlapping `[position, position+size)`.
    fn clear(&mut self, position: u64, size: u64) {
        if size == 0 {
            return;
        }
        let end = position + size;
        // A predecessor may still straddle `position`.
        if let Some((&start, &len)) = self.0.range(..position).next_back() {
            let span_end = start + len;
            if span_end > position {
                self.0.insert(start, position - start);
                if span_end > end {
                    self.0.insert(end, span_end - end);
                }
            }
        }
        // Spans starting within [position, end) are removed, trimming the
        // last one if it extends past `end`.
        let overlapping: Vec<u64> = self.0.range(position..end).map(|(&k, _)| k).collect();
        for start in overlapping {
            let len = self.0.remove(&start).expect("just found via range");
            let span_end = start + len;
            if span_end > end {
                self.0.insert(end, span_end - end);
            }
        }
    }

    /// Truncates to `new_size`, dropping/trimming anything at or beyond it.
    fn keep(&mut self, new_size: u64) {
        let overflow: Vec<u64> = self.0.range(new_size..).map(|(&k, _)| k).collect();
        for start in overflow {
            self.0.remove(&start);
        }
        if let Some((&start, &len)) = self.0.range(..new_size).next_back()
            && start + len > new_size
        {
            self.0.insert(start, new_size - start);
        }
    }

    /// Inserts `[position, position+length)`, clearing any overlap first
    /// and merging with an adjacent contiguous span on either side.
    fn put(&mut self, position: u64, length: u64) {
        if length == 0 {
            return;
        }
        self.clear(position, length);
        self.0.insert(position, length);
        self.merge_around(position);
    }

    fn merge_around(&mut self, position: u64) {
        let mut start = position;
        let mut len = self.0[&position];
        if let Some((&pstart, &plen)) = self.0.range(..position).next_back()
            && pstart + plen == position
        {
            self.0.remove(&position);
            len += plen;
            start = pstart;
            self.0.insert(start, len);
        }
        let after = start + len;
        if let Some(&nlen) = self.0.get(&after) {
            self.0.remove(&after);
            self.0.insert(start, len + nlen);
        }
    }

    /// Spans covering `[position, position+size)` in order: present spans
    /// carry their length, gaps carry theirs too (nothing stored for them).
    fn read(&self, position: u64, size: u64) -> Vec<(u64, Span<u64>)> {
        if size == 0 {
            return Vec::new();
        }
        let end = position + size;
        let mut clipped: Vec<(u64, u64)> = Vec::new();
        if let Some((&start, &len)) = self.0.range(..position).next_back() {
            let span_end = start + len;
            if span_end > position {
                clipped.push((position, span_end.min(end) - position));
            }
        }
        for (&start, &len) in self.0.range(position..end) {
            let span_end = (start + len).min(end);
            if span_end > start {
                clipped.push((start, span_end - start));
            }
        }
        let mut result = Vec::new();
        let mut cursor = position;
        for (start, len) in clipped {
            if start > cursor {
                result.push((cursor, Span::Gap(start - cursor)));
            }
            result.push((start, Span::Present(len)));
            cursor = start + len;
        }
        if cursor < end {
            result.push((cursor, Span::Gap(end - cursor)));
        }
        result
    }
}

/// RAM tier: non-overlapping `position -> bytes` spans, budget-limited via
/// [`RamBudget`].
struct ByteSpanMap {
    entries: BTreeMap<u64, Vec<u8>>,
    budget: Arc<RamBudget>,
}

impl ByteSpanMap {
    fn new(budget: Arc<RamBudget>) -> Self {
        Self {
            entries: BTreeMap::new(),
            budget,
        }
    }

    fn clear(&mut self, position: u64, size: u64) {
        if size == 0 {
            return;
        }
        let end = position + size;
        if let Some((&start, _)) = self.entries.range(..position).next_back() {
            let data = self.entries.remove(&start).expect("just found via range");
            let span_end = start + data.len() as u64;
            if span_end > position {
                let head_len = (position - start) as usize;
                let (head, rest) = data.split_at(head_len);
                self.entries.insert(start, head.to_vec());
                if span_end > end {
                    let middle_len = (end - position) as usize;
                    self.budget.release(middle_len as u64);
                    self.entries.insert(end, rest[middle_len..].to_vec());
                } else {
                    self.budget.release(rest.len() as u64);
                }
            } else {
                self.entries.insert(start, data);
            }
        }
        let overlapping: Vec<u64> = self.entries.range(position..end).map(|(&k, _)| k).collect();
        for start in overlapping {
            let data = self.entries.remove(&start).expect("just found via range");
            let span_end = start + data.len() as u64;
            if span_end > end {
                let keep_from = (end - start) as usize;
                self.budget.release(keep_from as u64);
                self.entries.insert(end, data[keep_from..].to_vec());
            } else {
                self.budget.release(data.len() as u64);
            }
        }
    }

    /// `false` if not enough RAM budget is available - caller falls back
    /// to disk spillover.
    fn write(&mut self, position: u64, data: &[u8]) -> bool {
        if !self.budget.try_acquire(data.len() as u64) {
            return false;
        }
        self.clear(position, data.len() as u64);
        self.entries.insert(position, data.to_vec());
        self.merge_around(position);
        true
    }

    fn merge_around(&mut self, position: u64) {
        if let Some((&pstart, pdata)) = self.entries.range(..position).next_back() {
            let plen = pdata.len() as u64;
            if pstart + plen == position {
                let mut merged = self.entries.remove(&pstart).expect("just found");
                let this = self.entries.remove(&position).expect("just inserted");
                merged.extend_from_slice(&this);
                self.entries.insert(pstart, merged);
            }
        }
        let (start, len) = {
            let (&s, d) = self
                .entries
                .range(..=position)
                .next_back()
                .expect("this span was just inserted (possibly merged left)");
            (s, d.len() as u64)
        };
        let after = start + len;
        if self.entries.contains_key(&after) {
            let mut merged = self.entries.remove(&start).expect("just found");
            let next = self.entries.remove(&after).expect("just found");
            merged.extend_from_slice(&next);
            self.entries.insert(start, merged);
        }
    }

    fn keep(&mut self, new_size: u64) {
        let overflow: Vec<u64> = self.entries.range(new_size..).map(|(&k, _)| k).collect();
        for start in overflow {
            let data = self.entries.remove(&start).expect("just found via range");
            self.budget.release(data.len() as u64);
        }
        if let Some((&start, _)) = self.entries.range(..new_size).next_back() {
            let data = self.entries.get(&start).expect("just found via range");
            let span_end = start + data.len() as u64;
            if span_end > new_size {
                let keep_len = (new_size - start) as usize;
                let data = self.entries.remove(&start).expect("just found");
                self.budget.release((data.len() - keep_len) as u64);
                self.entries.insert(start, data[..keep_len].to_vec());
            }
        }
    }

    fn read(&self, position: u64, size: u64) -> Vec<(u64, Span<Vec<u8>>)> {
        if size == 0 {
            return Vec::new();
        }
        let end = position + size;
        let mut clipped: Vec<(u64, Vec<u8>)> = Vec::new();
        if let Some((&start, data)) = self.entries.range(..position).next_back() {
            let span_end = start + data.len() as u64;
            if span_end > position {
                let from = (position - start) as usize;
                let to = (span_end.min(end) - start) as usize;
                clipped.push((position, data[from..to].to_vec()));
            }
        }
        for (&start, data) in self.entries.range(position..end) {
            let span_end = (start + data.len() as u64).min(end);
            if span_end > start {
                let to = (span_end - start) as usize;
                clipped.push((start, data[..to].to_vec()));
            }
        }
        let mut result = Vec::new();
        let mut cursor = position;
        for (start, data) in clipped {
            if start > cursor {
                result.push((cursor, Span::Gap(start - cursor)));
            }
            cursor = start + data.len() as u64;
            result.push((start, Span::Present(data)));
        }
        if cursor < end {
            result.push((cursor, Span::Gap(end - cursor)));
        }
        result
    }
}

impl Drop for ByteSpanMap {
    fn drop(&mut self) {
        let total: u64 = self.entries.values().map(|v| v.len() as u64).sum();
        self.budget.release(total);
    }
}

/// Disk-spillover tier: a lazily-created temp file (deleted on drop) plus a
/// [`LengthSpanMap`] recording which byte ranges have actually been
/// written - stale bytes physically left behind by an overwrite/clear are
/// simply never read again (the span map is the single source of truth for
/// what's valid), so `clear` doesn't need to touch the file at all.
struct FileCache {
    path: PathBuf,
    file: Option<File>,
    spans: LengthSpanMap,
}

impl FileCache {
    fn new(path: PathBuf) -> Self {
        Self {
            path,
            file: None,
            spans: LengthSpanMap::default(),
        }
    }

    fn file(&mut self) -> io::Result<&mut File> {
        if self.file.is_none() {
            self.file = Some(
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(&self.path)?,
            );
        }
        Ok(self.file.as_mut().expect("just set above"))
    }

    fn clear(&mut self, position: u64, size: u64) {
        self.spans.clear(position, size);
    }

    fn write(&mut self, position: u64, data: &[u8]) -> io::Result<()> {
        let file = self.file()?;
        file.seek(SeekFrom::Start(position))?;
        file.write_all(data)?;
        self.spans.put(position, data.len() as u64);
        Ok(())
    }

    fn keep(&mut self, new_size: u64) {
        self.spans.keep(new_size);
    }

    fn read(&mut self, position: u64, size: u64) -> io::Result<Vec<(u64, Span<Vec<u8>>)>> {
        let spans = self.spans.read(position, size);
        let mut result = Vec::with_capacity(spans.len());
        for (pos, span) in spans {
            match span {
                Span::Gap(len) => result.push((pos, Span::Gap(len))),
                Span::Present(len) => {
                    let mut buf = vec![0u8; len as usize];
                    let file = self.file()?;
                    file.seek(SeekFrom::Start(pos))?;
                    file.read_exact(&mut buf)?;
                    result.push((pos, Span::Present(buf)));
                }
            }
        }
        Ok(result)
    }
}

impl Drop for FileCache {
    fn drop(&mut self) {
        if self.file.take().is_some() {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

/// A growable, position-addressed byte buffer combining all three tiers -
/// see the module doc comment. Not thread-safe: a caller sharing one
/// across threads must provide its own external locking.
pub struct WriteCache {
    mem: ByteSpanMap,
    file: FileCache,
    zero: LengthSpanMap,
    size: u64,
}

impl WriteCache {
    /// `spill_path` is this cache's private temp file location (deleted on
    /// drop) - never actually created unless the RAM budget is exhausted.
    /// `initial_size` is the size this buffer should report before any
    /// `write`/`truncate` call (`0` for a freshly created buffer, or
    /// whatever size the caller's own prior state implies otherwise).
    pub fn new(budget: Arc<RamBudget>, spill_path: PathBuf, initial_size: u64) -> Self {
        Self {
            mem: ByteSpanMap::new(budget),
            file: FileCache::new(spill_path),
            zero: LengthSpanMap::default(),
            size: initial_size,
        }
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    /// Truncates to `new_size`, zero-padding if it grows.
    pub fn truncate(&mut self, new_size: u64) {
        if new_size == self.size {
            return;
        }
        if new_size > self.size {
            self.zero.put(self.size, new_size - self.size);
        } else {
            self.mem.keep(new_size);
            self.file.keep(new_size);
            self.zero.keep(new_size);
        }
        self.size = new_size;
    }

    /// Writes `data` at `position`, growing the cache (zero-padding any gap
    /// before `position`) if needed.
    pub fn write(&mut self, position: u64, data: &[u8]) -> io::Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        let len = data.len() as u64;
        if position < self.size {
            self.mem.clear(position, len);
            self.zero.clear(position, len);
            self.file.clear(position, len);
        } else if position > self.size {
            self.zero.put(self.size, position - self.size);
        }
        if !self.mem.write(position, data) {
            self.file.write(position, data)?;
        }
        self.size = self.size.max(position + len);
        Ok(())
    }

    /// Reads `[position, position+size)`, clipped to the cache's current
    /// size. Bytes not covered by any tier (i.e. untouched by this write
    /// session) are resolved via `fill_gap(position, length)`, which the
    /// caller wires up to read whatever this buffer's previously-persisted
    /// content is - this is what lets a reader see a consistent view of
    /// data that's only partially been (re)written in this session,
    /// without this cache needing to know anything about where that
    /// previously-persisted content actually lives.
    pub fn read_filling_gaps(
        &mut self,
        position: u64,
        size: u64,
        mut fill_gap: impl FnMut(u64, u64) -> Vec<u8>,
    ) -> io::Result<Vec<u8>> {
        let size = size.min(self.size.saturating_sub(position));
        if size == 0 {
            return Ok(Vec::new());
        }
        let mut result = vec![0u8; size as usize];
        let fill = |result: &mut Vec<u8>, at: u64, bytes: &[u8]| {
            let start = (at - position) as usize;
            result[start..start + bytes.len()].copy_from_slice(bytes);
        };

        for (pos1, span1) in self.mem.read(position, size) {
            match span1 {
                Span::Present(bytes) => fill(&mut result, pos1, &bytes),
                Span::Gap(len1) => {
                    for (pos2, span2) in self.file.read(pos1, len1)? {
                        match span2 {
                            Span::Present(bytes) => fill(&mut result, pos2, &bytes),
                            Span::Gap(len2) => {
                                for (pos3, span3) in self.zero.read(pos2, len2) {
                                    match span3 {
                                        Span::Present(len3) => {
                                            fill(&mut result, pos3, &vec![0u8; len3 as usize]);
                                        }
                                        Span::Gap(len3) => {
                                            let bytes = fill_gap(pos3, len3);
                                            fill(&mut result, pos3, &bytes);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn budget(bytes: u64) -> Arc<RamBudget> {
        Arc::new(RamBudget::new(bytes))
    }

    fn spill_path() -> PathBuf {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("spill");
        // Leak the TempDir so the path stays valid for the test's duration -
        // fine for a short-lived unit test, avoided in real use where
        // WriteCache's own Drop deletes just the file, not the directory.
        std::mem::forget(dir);
        path
    }

    fn read_all(cache: &mut WriteCache) -> Vec<u8> {
        cache
            .read_filling_gaps(0, cache.size(), |_, len| vec![b'?'; len as usize])
            .unwrap()
    }

    #[test]
    fn length_span_map_clear_trims_overlapping_spans() {
        let mut m = LengthSpanMap::default();
        m.put(0, 10);
        m.clear(4, 3); // clears [4,7): leaves [0,4) and [7,10)
        assert_eq!(m.0.get(&0), Some(&4));
        assert_eq!(m.0.get(&7), Some(&3));
    }

    #[test]
    fn length_span_map_put_merges_contiguous_neighbors() {
        let mut m = LengthSpanMap::default();
        m.put(0, 5);
        m.put(10, 5);
        m.put(5, 5); // fills the gap between them exactly
        assert_eq!(m.0.len(), 1);
        assert_eq!(m.0.get(&0), Some(&15));
    }

    #[test]
    fn length_span_map_keep_truncates_and_trims() {
        let mut m = LengthSpanMap::default();
        m.put(0, 10);
        m.put(20, 10);
        m.keep(25);
        assert_eq!(m.0.get(&0), Some(&10));
        assert_eq!(m.0.get(&20), Some(&5));
    }

    #[test]
    fn length_span_map_read_reports_gaps_and_present_spans_in_order() {
        let mut m = LengthSpanMap::default();
        m.put(10, 5); // [10,15)
        let spans = m.read(0, 20);
        let described: Vec<(u64, bool, u64)> = spans
            .into_iter()
            .map(|(pos, s)| match s {
                Span::Present(len) => (pos, true, len),
                Span::Gap(len) => (pos, false, len),
            })
            .collect();
        assert_eq!(
            described,
            vec![(0, false, 10), (10, true, 5), (15, false, 5)]
        );
    }

    #[test]
    fn write_cache_write_then_read_round_trips() {
        let mut cache = WriteCache::new(budget(1_000_000), spill_path(), 0);
        cache.write(0, b"hello world").unwrap();
        assert_eq!(cache.size(), 11);
        let bytes = cache
            .read_filling_gaps(0, 11, |_, _| panic!("no gaps expected"))
            .unwrap();
        assert_eq!(bytes, b"hello world");
    }

    #[test]
    fn write_cache_overwrite_in_the_middle_replaces_only_that_range() {
        let mut cache = WriteCache::new(budget(1_000_000), spill_path(), 0);
        cache.write(0, b"hello world").unwrap();
        cache.write(6, b"there").unwrap();
        assert_eq!(read_all(&mut cache), b"hello there");
    }

    #[test]
    fn write_cache_write_past_end_zero_fills_the_gap() {
        let mut cache = WriteCache::new(budget(1_000_000), spill_path(), 0);
        cache.write(0, b"ab").unwrap();
        cache.write(5, b"cd").unwrap();
        assert_eq!(cache.size(), 7);
        assert_eq!(read_all(&mut cache), b"ab\0\0\0cd");
    }

    #[test]
    fn write_cache_truncate_grow_zero_pads() {
        let mut cache = WriteCache::new(budget(1_000_000), spill_path(), 0);
        cache.write(0, b"ab").unwrap();
        cache.truncate(5);
        assert_eq!(cache.size(), 5);
        assert_eq!(read_all(&mut cache), b"ab\0\0\0");
    }

    #[test]
    fn write_cache_truncate_shrink_drops_bytes_beyond_new_size() {
        let mut cache = WriteCache::new(budget(1_000_000), spill_path(), 0);
        cache.write(0, b"hello world").unwrap();
        cache.truncate(5);
        assert_eq!(cache.size(), 5);
        assert_eq!(read_all(&mut cache), b"hello");
        // Growing back past the truncated point must read as zeros, not
        // stale bytes left behind in the (still-open) spill file.
        cache.truncate(11);
        assert_eq!(read_all(&mut cache), b"hello\0\0\0\0\0\0");
    }

    #[test]
    fn write_cache_falls_back_to_disk_when_ram_budget_is_exhausted() {
        let mut cache = WriteCache::new(budget(4), spill_path(), 0);
        cache.write(0, b"ab").unwrap(); // fits the 4-byte budget
        cache.write(2, b"cdef").unwrap(); // doesn't fit - must spill to disk
        assert_eq!(read_all(&mut cache), b"abcdef");
    }

    #[test]
    fn write_cache_read_before_any_write_falls_through_to_the_gap_filler() {
        let mut cache = WriteCache::new(budget(1_000_000), spill_path(), 100);
        let bytes = cache.read_filling_gaps(0, 5, |pos, len| {
            assert_eq!(pos, 0);
            assert_eq!(len, 5);
            b"XXXXX".to_vec()
        });
        assert_eq!(bytes.unwrap(), b"XXXXX");
    }

    #[test]
    fn write_cache_read_merges_written_bytes_with_gap_filled_old_content() {
        // A file that already had 10 bytes of persisted content; only
        // bytes [2,5) have been overwritten in this session.
        let mut cache = WriteCache::new(budget(1_000_000), spill_path(), 10);
        cache.write(2, b"XYZ").unwrap();
        let bytes = cache
            .read_filling_gaps(0, 10, |pos, len| {
                // Stand-in "old content": position as an ASCII digit.
                (pos..pos + len).map(|p| b'0' + p as u8).collect()
            })
            .unwrap();
        assert_eq!(bytes, b"01XYZ56789");
    }

    #[test]
    fn ram_budget_release_makes_room_for_later_writes() {
        let b = budget(4);
        let mut cache = WriteCache::new(Arc::clone(&b), spill_path(), 0);
        cache.write(0, b"abcd").unwrap(); // uses up the whole budget
        cache.truncate(0); // drops the mem span, releasing its budget
        cache.write(0, b"ef").unwrap(); // must fit again now
        assert_eq!(read_all(&mut cache), b"ef");
    }
}
