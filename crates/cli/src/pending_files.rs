//! Per-file registry of not-yet-persisted write state (DESIGN-MOUNT-007/013 in
//! `docs/design/mount-write-path.md`): tracks, per file identity, how many write-intent handles
//! are currently open and the chain of [`WriteCache`] generations a read still needs to see
//! through - at most one *writable* generation currently receiving writes, plus older generations
//! still settling in the background, each reachable from the next-newer one so a read composes
//! through however many are in flight without ever blocking a fresh open on them. Used by
//! `crate::dedup_fs::DedupFs`.

use std::collections::HashMap;
use std::io;
use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::write_cache::{MemoryBudget, WriteCache};

/// What a [`GenerationSlot`] falls back to for a byte range it does not itself track.
enum Base {
    /// The file's durably committed content at the moment its very first generation this session
    /// was created - `None` for a brand new file with nothing committed yet.
    Content(Option<i64>),
    /// The generation that came immediately before this one in the same file's chain, resolved
    /// live at read time rather than copied when this generation was created (DESIGN-MOUNT-013) -
    /// so once that generation settles and its own [`WriteCache`] is dropped, a read here starts
    /// resolving directly against the durably committed content it produced instead.
    Chain(Arc<GenerationSlot>),
}

/// Either still being written/settled in memory (or spilled to disk), or already durably
/// committed under a `content_id` - DESIGN-MOUNT-013's per-generation state, transitioning from
/// `Cache` to `Settled` exactly once, when the background job (DESIGN-MOUNT-006, not yet built)
/// finishes.
enum SlotState {
    Cache(WriteCache),
    Settled { content_id: i64, size: u64 },
}

/// One generation in a file's write-cache chain (DESIGN-MOUNT-013). Shared via [`Arc`]: a newer
/// generation's [`Base::Chain`] keeps an older, still-settling generation alive only for as long
/// as something still needs it; once settled, `state` drops the [`WriteCache`] and keeps only the
/// resulting `content_id`/`size`.
pub struct GenerationSlot {
    state: Mutex<SlotState>,
    base: Base,
}

impl GenerationSlot {
    fn new(cache: WriteCache, base: Base) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(SlotState::Cache(cache)),
            base,
        })
    }

    /// Marks this generation settled, dropping its [`WriteCache`] (and whatever memory/spill file
    /// it was still holding) and switching subsequent reads over to `content_id` instead - the
    /// hook DESIGN-MOUNT-006's background job pool calls once it has durably committed this
    /// generation's content. `size` is the settled content's logical size, needed so a newer
    /// generation still chained to this one can resolve its own base size without this module
    /// depending on `crates/db`.
    pub fn mark_settled(&self, content_id: i64, size: u64) {
        *self.state.lock().expect("not poisoned") = SlotState::Settled { content_id, size };
    }

    /// This generation's logical size - what a settle job (DESIGN-MOUNT-006, not yet built)
    /// needs to know how many bytes [`Self::read`] can cover.
    pub fn size(&self) -> u64 {
        match &*self.state.lock().expect("not poisoned") {
            SlotState::Cache(cache) => cache.size(),
            SlotState::Settled { size, .. } => *size,
        }
    }

    /// The bytes currently spilled to disk for this generation specifically - `0` once settled,
    /// since a settled generation no longer holds a [`WriteCache`] at all. DESIGN-MOUNT-010's
    /// backpressure signal, for a generation that has already been released and is queued or
    /// being processed by the settle job pool (DESIGN-MOUNT-006, not yet built).
    pub fn spilled_bytes(&self) -> u64 {
        match &*self.state.lock().expect("not poisoned") {
            SlotState::Cache(cache) => cache.spilled_bytes(),
            SlotState::Settled { .. } => 0,
        }
    }

    /// Reads `len` bytes at `position`, composing through however many older generations this
    /// one's chain still has - see [`read_chain`]. The settle job pool uses this, in windowed
    /// calls covering `[0, self.size())`, to read the complete byte stream a generation needs
    /// chunking and hashing.
    pub fn read(
        &self,
        position: u64,
        len: u32,
        resolve_content: &impl Fn(i64, u64, u32) -> io::Result<Vec<u8>>,
    ) -> io::Result<Vec<u8>> {
        read_chain(self, position, len, resolve_content)
    }
}

/// Reads `len` bytes at `position` from `slot`, composing through however many older generations
/// its chain still has, and finally through `resolve_content` for whichever `content_id` the
/// chain bottoms out at (or an implicit empty file, for a brand new one with no committed content
/// at all).
fn read_chain(
    slot: &GenerationSlot,
    position: u64,
    len: u32,
    resolve_content: &impl Fn(i64, u64, u32) -> io::Result<Vec<u8>>,
) -> io::Result<Vec<u8>> {
    let mut state = slot.state.lock().expect("not poisoned");
    match &mut *state {
        SlotState::Cache(cache) => cache.read(position, len, |p, l| {
            resolve_base(&slot.base, p, l, resolve_content)
        }),
        SlotState::Settled { content_id, .. } => resolve_content(*content_id, position, len),
    }
}

fn resolve_base(
    base: &Base,
    position: u64,
    len: u32,
    resolve_content: &impl Fn(i64, u64, u32) -> io::Result<Vec<u8>>,
) -> io::Result<Vec<u8>> {
    match base {
        Base::Content(Some(content_id)) => resolve_content(*content_id, position, len),
        Base::Content(None) => Ok(vec![0u8; len as usize]),
        Base::Chain(previous) => read_chain(previous, position, len, resolve_content),
    }
}

/// One file's open-handle count and write-cache chain state (DESIGN-MOUNT-013).
struct PendingFile {
    handle_count: u32,
    /// The generation currently allowed to receive new writes - `None` once every write-intent
    /// handle that could write to it has been released, even though `latest` still points at it
    /// while it settles.
    writable: Option<Arc<GenerationSlot>>,
    /// The most recently created generation for this file, whether still `writable` or already
    /// handed off and settling - what the *next* generation, if one is ever created, chains
    /// against. Cleared once it is known to have settled and no handle is open, so the registry
    /// does not keep an entry around forever for a file nothing references anymore.
    latest: Option<Arc<GenerationSlot>>,
}

/// Registry of every file with at least one write-intent handle open, or at least one background
/// settle job still in flight, this mount session - keyed by file identity (a `tree_entries.id`),
/// per DESIGN-MOUNT-007's "looked up by file identity, not by open handle".
#[derive(Default)]
pub struct PendingFiles {
    inner: Mutex<HashMap<i64, PendingFile>>,
}

/// What [`PendingFiles::write`]/[`PendingFiles::truncate`] need to create a file's very first
/// generation this session - grouped into one type since it is only ever consulted together, and
/// only that first time; every later generation reuses its predecessor's own size and needs none
/// of this.
pub struct NewGeneration<'a> {
    pub budget: &'a Arc<MemoryBudget>,
    pub temp_dir: &'a Path,
    /// The file's durably committed content at the moment this session's first generation for it
    /// is created - `None` for a brand new file with nothing committed yet.
    pub base_content_id: Option<i64>,
    pub base_size: u64,
}

fn is_settled(slot: &GenerationSlot) -> bool {
    matches!(
        &*slot.state.lock().expect("not poisoned"),
        SlotState::Settled { .. }
    )
}

impl PendingFiles {
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a fresh write-intent open on `file_id`, bumping its handle count. Does not by
    /// itself create a write cache - one is only created lazily, on the first `write`/`truncate`
    /// call, so a handle opened for writing but never actually written to costs nothing beyond
    /// this count.
    pub fn open(&self, file_id: i64) {
        let mut inner = self.inner.lock().expect("not poisoned");
        inner
            .entry(file_id)
            .or_insert(PendingFile {
                handle_count: 0,
                writable: None,
                latest: None,
            })
            .handle_count += 1;
    }

    /// Releases one write-intent handle on `file_id`. If this was the last one and it leaves
    /// behind a generation that was still receiving writes, that generation is handed to the
    /// caller (destined for DESIGN-MOUNT-006's background job pool) - a later write-intent open
    /// on the same file starts a brand new generation immediately (DESIGN-MOUNT-013), chained to
    /// this one rather than blocking on it.
    pub fn release(&self, file_id: i64) -> Option<Arc<GenerationSlot>> {
        let mut inner = self.inner.lock().expect("not poisoned");
        let entry = inner.get_mut(&file_id)?;
        entry.handle_count = entry
            .handle_count
            .checked_sub(1)
            .expect("release without a matching open");
        let handed_off = if entry.handle_count == 0 {
            entry.writable.take()
        } else {
            None
        };
        if entry.handle_count == 0 {
            if entry.latest.as_ref().is_some_and(|s| is_settled(s)) {
                entry.latest = None;
            }
            if entry.writable.is_none() && entry.latest.is_none() {
                inner.remove(&file_id);
            }
        }
        handed_off
    }

    /// The current logical size of `file_id`'s pending write-cache chain, if it has one - `None`
    /// when nothing is pending, meaning the caller should use the durably committed size instead
    /// (DESIGN-MOUNT-007/013's same-session visibility, extended to `getattr`).
    pub fn current_size(&self, file_id: i64) -> Option<u64> {
        let inner = self.inner.lock().expect("not poisoned");
        inner
            .get(&file_id)
            .and_then(|entry| entry.latest.as_ref())
            .map(|slot| slot.size())
    }

    /// Whether `file_id` currently has a generation still accepting writes. A caller about to
    /// call [`Self::write`]/[`Self::truncate`] can check this first to skip resolving the file's
    /// durably committed content when it will not actually be needed - `new_generation`'s
    /// `base_content_id`/`base_size` are consulted only the first time a generation is created.
    pub fn has_writable(&self, file_id: i64) -> bool {
        let inner = self.inner.lock().expect("not poisoned");
        inner
            .get(&file_id)
            .is_some_and(|entry| entry.writable.is_some())
    }

    /// Writes `data` at `position` for `file_id`, creating a new writable generation first if
    /// none exists yet - either the file's first write this session, or every previous
    /// write-intent handle already released. `new_generation` describes the file's durably
    /// committed content, consulted only the very first time this session creates a generation
    /// for this file; every later generation chains to the one before it instead.
    pub fn write(
        &self,
        file_id: i64,
        position: u64,
        data: &[u8],
        new_generation: NewGeneration<'_>,
    ) -> io::Result<()> {
        let slot = self.writable_generation(file_id, new_generation);
        let mut state = slot.state.lock().expect("not poisoned");
        match &mut *state {
            SlotState::Cache(cache) => cache.write(position, data),
            SlotState::Settled { .. } => {
                unreachable!("a writable generation is always Cache until it is released")
            }
        }
    }

    /// Truncates `file_id`'s writable generation to `new_size`, creating one first if none exists
    /// yet - see [`Self::write`].
    pub fn truncate(&self, file_id: i64, new_size: u64, new_generation: NewGeneration<'_>) {
        let slot = self.writable_generation(file_id, new_generation);
        let mut state = slot.state.lock().expect("not poisoned");
        match &mut *state {
            SlotState::Cache(cache) => cache.truncate(new_size),
            SlotState::Settled { .. } => {
                unreachable!("a writable generation is always Cache until it is released")
            }
        }
    }

    fn writable_generation(
        &self,
        file_id: i64,
        new_generation: NewGeneration<'_>,
    ) -> Arc<GenerationSlot> {
        let mut inner = self.inner.lock().expect("not poisoned");
        let entry = inner.entry(file_id).or_insert(PendingFile {
            handle_count: 0,
            writable: None,
            latest: None,
        });
        if let Some(writable) = &entry.writable {
            return Arc::clone(writable);
        }
        let (base, size) = match &entry.latest {
            Some(previous) => (Base::Chain(Arc::clone(previous)), previous.size()),
            None => (
                Base::Content(new_generation.base_content_id),
                new_generation.base_size,
            ),
        };
        let cache = WriteCache::new(
            new_generation.temp_dir,
            Arc::clone(new_generation.budget),
            size,
        );
        let slot = GenerationSlot::new(cache, base);
        entry.latest = Some(Arc::clone(&slot));
        entry.writable = Some(Arc::clone(&slot));
        slot
    }

    /// Reads `len` bytes at `position` for `file_id` through its write-cache chain, if it has
    /// one - `None` when nothing is pending for this file at all, meaning the caller should read
    /// the durably committed content directly instead. Sees every generation still in the chain,
    /// not only a currently-writable one, per DESIGN-MOUNT-007's whole-window same-session
    /// visibility.
    pub fn read(
        &self,
        file_id: i64,
        position: u64,
        len: u32,
        resolve_content: &impl Fn(i64, u64, u32) -> io::Result<Vec<u8>>,
    ) -> Option<io::Result<Vec<u8>>> {
        let slot = {
            let inner = self.inner.lock().expect("not poisoned");
            let latest = inner.get(&file_id)?.latest.as_ref()?;
            Arc::clone(latest)
        };
        Some(read_chain(&slot, position, len, resolve_content))
    }
}

#[cfg(test)]
impl PendingFiles {
    fn len(&self) -> usize {
        self.inner.lock().expect("not poisoned").len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn no_content(_content_id: i64, _pos: u64, _len: u32) -> io::Result<Vec<u8>> {
        panic!("resolve_content must not be called when nothing is durably committed yet")
    }

    fn budget_and_dir() -> (Arc<MemoryBudget>, tempfile::TempDir) {
        (
            Arc::new(MemoryBudget::new(1000)),
            tempfile::tempdir().unwrap(),
        )
    }

    /// None of these tests starts from a durably committed file, so `base_content_id` is always
    /// `None` here - only `base_size` (the size DESIGN-MOUNT-013's very first generation for a
    /// file needs, e.g. for a `truncate` up-front on an empty file) varies.
    fn params<'a>(
        budget: &'a Arc<MemoryBudget>,
        temp_dir: &'a std::path::Path,
        base_size: u64,
    ) -> NewGeneration<'a> {
        NewGeneration {
            budget,
            temp_dir,
            base_content_id: None,
            base_size,
        }
    }

    #[test]
    fn a_write_creates_a_writable_generation_lazily_and_reads_it_back() {
        let (budget, dir) = budget_and_dir();
        let registry = PendingFiles::new();
        registry.open(1);
        registry
            .write(1, 0, b"hello", params(&budget, dir.path(), 0))
            .unwrap();
        let data = registry.read(1, 0, 5, &no_content).unwrap().unwrap();
        assert_eq!(data, b"hello");
    }

    #[test]
    fn reading_a_file_with_nothing_pending_returns_none() {
        let registry = PendingFiles::new();
        assert!(registry.read(42, 0, 1, &no_content).is_none());
    }

    #[test]
    fn release_keeps_the_generation_readable_while_it_still_settles() {
        let (budget, dir) = budget_and_dir();
        let registry = PendingFiles::new();
        registry.open(1);
        registry
            .write(1, 0, b"hi", params(&budget, dir.path(), 0))
            .unwrap();
        let handed_off = registry.release(1);
        assert!(handed_off.is_some());
        // DESIGN-MOUNT-007: still visible this session even though every handle is closed and
        // the background job (not modeled in this test) has not run yet.
        let data = registry.read(1, 0, 2, &no_content).unwrap().unwrap();
        assert_eq!(data, b"hi");
    }

    #[test]
    fn a_settled_generation_is_forgotten_once_released_again() {
        let (budget, dir) = budget_and_dir();
        let registry = PendingFiles::new();
        registry.open(1);
        registry
            .write(1, 0, b"hi", params(&budget, dir.path(), 0))
            .unwrap();
        let settling = registry.release(1).unwrap();
        assert_eq!(
            registry.len(),
            1,
            "kept while the generation is still settling"
        );

        settling.mark_settled(7, 2);
        // Cleanup happens lazily, on the registry's next touch for this file - here, a read-only
        // open/release cycle that creates no new generation of its own - not the instant
        // `mark_settled` runs, which does not go through the registry at all.
        registry.open(1);
        registry.release(1);
        assert_eq!(
            registry.len(),
            0,
            "forgotten once nothing needs the settled generation anymore"
        );
    }

    #[test]
    fn release_with_more_than_one_handle_open_keeps_the_generation_writable() {
        let (budget, dir) = budget_and_dir();
        let registry = PendingFiles::new();
        registry.open(1);
        registry.open(1);
        registry
            .write(1, 0, b"hi", params(&budget, dir.path(), 0))
            .unwrap();
        assert!(registry.release(1).is_none());
        // Still pending - the second handle is still open, and a further write must still land
        // on the same generation, not create a new one chained to itself.
        registry
            .write(1, 2, b"!!", params(&budget, dir.path(), 0))
            .unwrap();
        let data = registry.read(1, 0, 4, &no_content).unwrap().unwrap();
        assert_eq!(data, b"hi!!");
    }

    #[test]
    fn current_size_and_has_writable_reflect_the_generation_lifecycle() {
        let (budget, dir) = budget_and_dir();
        let registry = PendingFiles::new();
        assert_eq!(registry.current_size(1), None);
        assert!(!registry.has_writable(1));

        registry.open(1);
        registry
            .write(1, 0, b"hello", params(&budget, dir.path(), 0))
            .unwrap();
        assert_eq!(registry.current_size(1), Some(5));
        assert!(registry.has_writable(1));

        let settling = registry.release(1).unwrap();
        // Released - still readable/sized (DESIGN-MOUNT-007), but no longer writable.
        assert_eq!(registry.current_size(1), Some(5));
        assert!(!registry.has_writable(1));
        settling.mark_settled(1, 5);
    }

    #[test]
    fn a_fresh_write_after_release_creates_a_new_generation_chained_to_the_settling_one() {
        let (budget, dir) = budget_and_dir();
        let registry = PendingFiles::new();

        registry.open(1);
        registry
            .write(1, 0, b"0123456789", params(&budget, dir.path(), 0))
            .unwrap();
        let settling = registry.release(1).unwrap();

        // A fresh write-intent open does not block on `settling` at all - it can write
        // immediately, even though the previous generation's background job has not run yet.
        registry.open(1);
        registry
            .write(1, 2, b"XX", params(&budget, dir.path(), 10))
            .unwrap();

        // Reading a byte the new generation never touched falls through live to the still-
        // settling previous generation.
        let data = registry.read(1, 0, 10, &no_content).unwrap().unwrap();
        assert_eq!(data, b"01XX456789");

        // Once the old generation settles, the same read now resolves via the durably committed
        // content instead - proving the fallback is live, not a snapshot taken at creation time.
        settling.mark_settled(99, 10);
        let resolve_from_content = |content_id: i64, pos: u64, len: u32| {
            assert_eq!(content_id, 99);
            Ok((pos..pos + u64::from(len))
                .map(|p| b'A' + p as u8)
                .collect())
        };
        let data = registry
            .read(1, 0, 10, &resolve_from_content)
            .unwrap()
            .unwrap();
        let mut expected = vec![b'A', b'A' + 1, b'X', b'X'];
        expected.extend((4u8..10).map(|p| b'A' + p));
        assert_eq!(data, expected);
    }

    #[test]
    fn a_chain_three_generations_deep_still_composes_correctly() {
        let (budget, dir) = budget_and_dir();
        let registry = PendingFiles::new();

        registry.open(1);
        registry
            .write(1, 0, b"0123456789", params(&budget, dir.path(), 0))
            .unwrap();
        let gen1 = registry.release(1).unwrap();

        registry.open(1);
        registry
            .write(1, 0, b"AA", params(&budget, dir.path(), 10))
            .unwrap();
        let gen2 = registry.release(1).unwrap();

        registry.open(1);
        registry
            .write(1, 4, b"BB", params(&budget, dir.path(), 10))
            .unwrap();

        // Nothing settled yet - reads compose all the way through gen2 and gen1.
        let data = registry.read(1, 0, 10, &no_content).unwrap().unwrap();
        assert_eq!(data, b"AA23BB6789");

        // Settling oldest-first, as a real background pool would - gen2 remains reachable for
        // the third (still open) generation's own fallback.
        gen1.mark_settled(1, 10);
        gen2.mark_settled(2, 10);
        let resolve_settled = |content_id: i64, _pos: u64, len: u32| match content_id {
            2 => Ok(vec![b'2'; len as usize]),
            other => panic!("only gen2 (content_id 2) should ever be asked here: got {other}"),
        };
        // gen2, once settled, fully represents [0,10) on its own (its settling already folded
        // gen1's content in) - so gen1 (content_id 1) is never consulted for this read at all.
        let data = registry.read(1, 0, 10, &resolve_settled).unwrap().unwrap();
        assert_eq!(data, b"2222BB2222");
    }
}
