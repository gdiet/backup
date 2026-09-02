//! An experimental, small LRU cache of a handful of recently-touched directories' live children -
//! meant to spare [`crate::tree`]'s Windows-only case-insensitive lookup fallback its unindexed
//! full scan on every miss when the same directory is visited repeatedly in a row (the common case
//! for a bulk `mkdir`/`create` run into one directory). See `developer-todos/windows-mkdir-degrades-in-large-directories.md`
//! for the problem this targets and the measurement this cache is being tried against.
//!
//! Lives behind its own [`Mutex`] rather than [`crate::Repository`]'s single connection lock, but
//! every access happens while a caller already holds that connection lock (every [`NameCache`]
//! method is only ever called from within [`crate::Repository::with_connection`]/`with_transaction`'s
//! closure) - so this lock is never contended in practice, just a cheap uncontended lock/unlock per
//! call, not a source of real cross-thread waiting.
//!
//! This correctness argument holds only as long as a [`crate::Repository`] instance still funnels
//! *all* of its own database access through that one connection/lock - which is exactly the
//! narrower step DESIGN-METADATA-003 describes itself as being (see its doc comment on
//! [`crate::Repository`]), not its eventual fuller design of splitting reads onto their own,
//! unlocked connection(s). A distinct [`crate::Repository`] instance (e.g. one opened via
//! [`crate::open_repository_read_only`]) already gets its own independent [`NameCache`], so nothing
//! here assumes there is only ever one connection to a repository at a time - but a *pool of read
//! connections shared by one [`crate::Repository`] instance* would put more than one connection
//! behind a single cache, and each would need to reach it without the single-lock argument above to
//! fall back on. Revisit this cache's locking (a real, contended lock; or a cache per connection in
//! the pool instead of per `Repository`) before or alongside implementing that split, not after.

use std::collections::VecDeque;
use std::sync::Mutex;

/// A directory's live children as cached `(id, folded_name)` pairs - `folded_name` is
/// [`crate::tree`]'s `fold_key` output, not the entry's raw stored name, so a lookup against an
/// already-warm entry never has to fold a candidate's name again.
type CachedChildren = Vec<(i64, String)>;

/// Bounded, most-recently-used-first cache of `(id, folded_name)` pairs for a directory's live
/// children, keyed by `parent_id`. Deliberately small and linear-scanned by `parent_id` -
/// `capacity` is meant to stay in the low tens at most, just enough to keep the few directories a
/// caller is actively working in warm, not to cache the whole tree.
#[derive(Debug)]
pub(crate) struct NameCache {
    capacity: usize,
    entries: Mutex<VecDeque<(i64, CachedChildren)>>,
}

impl NameCache {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            entries: Mutex::new(VecDeque::with_capacity(capacity)),
        }
    }

    /// Runs `use_candidates` against `parent_id`'s cached candidate list - calling `populate_miss`
    /// to fetch it from the database first if not yet cached, evicting the least-recently-used
    /// entry if already at capacity - and moves it to the front (most recently used) either way.
    ///
    /// The candidate list is only ever borrowed here, never cloned out to the caller - cloning the
    /// whole `Vec` on every lookup would silently reintroduce an O(n) cost per call on a warm,
    /// large directory, defeating the point of caching it at all.
    pub(crate) fn with_cached_or_populate<T>(
        &self,
        parent_id: i64,
        populate_miss: impl FnOnce() -> Result<CachedChildren, crate::Error>,
        use_candidates: impl FnOnce(&CachedChildren) -> T,
    ) -> Result<T, crate::Error> {
        let mut entries = self
            .entries
            .lock()
            .expect("name cache mutex poisoned by an earlier panic");
        let entry = match entries.iter().position(|(p, _)| *p == parent_id) {
            Some(pos) => entries
                .remove(pos)
                .expect("pos just came back from position() above"),
            None => {
                if entries.len() >= self.capacity {
                    entries.pop_back();
                }
                (parent_id, populate_miss()?)
            }
        };
        let result = use_candidates(&entry.1);
        entries.push_front(entry);
        Ok(result)
    }

    /// Appends a newly inserted child to `parent_id`'s cached list, if it is currently cached -
    /// keeps a directory being bulk-populated warm across repeated inserts without a full re-scan
    /// on every one of them, rather than invalidating the whole entry on every single insert.
    ///
    /// `folded_name` must already be [`crate::tree`]'s folded form (`fold_key`), not the entry's
    /// raw stored name - every candidate in the cache is kept pre-folded so a lookup against a warm
    /// entry never has to fold again (see `find_child_id_case_insensitive`).
    pub(crate) fn note_inserted(&self, parent_id: i64, id: i64, folded_name: &str) {
        let mut entries = self
            .entries
            .lock()
            .expect("name cache mutex poisoned by an earlier panic");
        if let Some((_, candidates)) = entries.iter_mut().find(|(p, _)| *p == parent_id) {
            candidates.push((id, folded_name.to_string()));
        }
    }

    /// Drops `parent_id`'s cached list entirely, if present - used wherever a child is removed or
    /// renamed away. Simpler and safer than patching the cached list in place for those less
    /// common, more intricate mutation shapes; the next miss just repopulates it from the database.
    pub(crate) fn invalidate(&self, parent_id: i64) {
        let mut entries = self
            .entries
            .lock()
            .expect("name cache mutex poisoned by an earlier panic");
        entries.retain(|(p, _)| *p != parent_id);
    }
}
