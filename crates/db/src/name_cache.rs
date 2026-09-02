//! DESIGN-MOUNT-017's (`tree-namespace-case-sensitivity.md`) small LRU cache of a handful of
//! recently-touched directories' live children - meant to spare
//! [`crate::tree`]'s Windows-only case-insensitive lookup fallback its unindexed full scan on
//! every miss when the same directory is visited repeatedly in a row (the common case for a bulk
//! `mkdir`/`create` run into one directory). See that design entry for the background this targets,
//! the memory/correctness tradeoffs it commits to, and the measurements behind it; see
//! `developer-todos/windows-mkdir-degrades-in-large-directories.md` for where the problem itself
//! was first found.
//!
//! A Windows-only feature end to end, on both sides:
//! [`crate::tree::case_insensitive::find_child_id_case_insensitive`] (the only reader, via
//! [`NameCache::with_cached_or_populate`]) is itself only ever reached through
//! [`crate::tree::find_child_id`]'s `cfg!(windows)` gate, so it already never runs on another
//! platform - but [`NameCache::note_inserted`]/[`NameCache::invalidate`] (the writers) are called
//! unconditionally from every `tree_entries` mutation, regardless of platform, so *they* each carry
//! their own `cfg!(windows)` no-op guard. Keeping a cache updated that nothing ever reads on that
//! platform would be silent, pointless work - and worse, would leave a reader wondering why a
//! Windows-only cache is being populated at all on a Linux build.
//!
//! Holds no lock of its own - [`NameCache`] lives as a plain field alongside the connection inside
//! [`crate::Repository`]'s single [`std::sync::Mutex`] (see that struct's own doc comment), so it
//! is unreachable at all except from within [`crate::Repository::with_connection`]/
//! `with_transaction`'s closure, which already holds that one lock. A second, nested lock (an
//! earlier version of this cache had one) would only ever have been uncontended by convention, not
//! by construction - a future caller could have reached it without going through
//! `with_connection`/`with_transaction` and no compiler error would have caught it. Merging it into
//! the same `Mutex` instead makes that mistake impossible to make, not just unlikely.
//!
//! This still depends on a [`crate::Repository`] instance funneling *all* of its own database
//! access through that one connection/lock - DESIGN-METADATA-003's current
//! one-connection-per-`Repository` model, not its eventual fuller design of splitting reads onto
//! their own, unlocked connection(s) (see `metadata-storage.md`). A distinct [`crate::Repository`]
//! instance (e.g. one opened via [`crate::open_repository_read_only`]) already gets its own
//! independent [`NameCache`], so nothing here assumes there is only ever one connection to a
//! repository at a time - but a *pool of read connections shared by one [`crate::Repository`]
//! instance* would put more than one connection behind a single cache, and each would need its own
//! way to reach it without a shared `&mut` to rely on. Revisit this cache's access pattern (a real
//! lock again; or a cache per connection in the pool instead of per `Repository`) before or
//! alongside implementing that split, not after.

use std::collections::HashMap;
use std::num::NonZeroUsize;

use lru::LruCache;

/// A directory's live children, keyed by [`crate::tree`]'s folded name (`fold_key` output, not the
/// entry's raw stored name) and mapped to the id currently holding that folded name - `O(1)`
/// average lookup instead of a linear scan, which is the actual point of caching a large, warm
/// directory rather than just avoiding the SQL round-trip.
type CachedChildren = HashMap<String, i64>;

/// Bounded, most-recently-used-first cache of the live children (see [`CachedChildren`]) of a
/// handful of directories, keyed by `parent_id` - a thin wrapper around the `lru` crate's
/// [`LruCache`] (MIT-licensed, one dependency of its own - `hashbrown`, already indirectly present
/// in this workspace - actively maintained: 74 published versions, latest within days of when this
/// was adopted) rather than a hand-rolled eviction list, for exactly the LRU-over-directories
/// bookkeeping this needs (move-to-front on access, evict-oldest on overflow) without reimplementing
/// it. The list of cached *directories* itself is deliberately small - capacity is meant to stay in
/// the low tens at most, just enough to keep the few directories a caller is actively working in
/// warm, not to cache the whole tree - independent of how large any one cached directory's own
/// children map is.
pub(crate) struct NameCache {
    entries: LruCache<i64, CachedChildren>,
    /// Whether [`Self::note_inserted`]/[`Self::invalidate`] actually do anything - `cfg!(windows)`
    /// via [`Self::new`] in production, forced explicitly via [`Self::with_active`] so a test can
    /// exercise the maintenance bookkeeping itself on any platform, the same way
    /// `find_child_id_case_insensitive` stays testable off Windows despite `find_child_id` gating
    /// it - see this module's doc comment for why the gate exists at all.
    active: bool,
}

/// A fill-level summary (directory count, total cached entries) rather than the derived dump of
/// every cached name - real file/directory names, from a real filesystem being backed up, have no
/// business appearing unredacted in a debug log line.
impl std::fmt::Debug for NameCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let total_entries: usize = self
            .entries
            .iter()
            .map(|(_, children)| children.len())
            .sum();
        f.debug_struct("NameCache")
            .field("capacity", &self.entries.cap())
            .field("directories_cached", &self.entries.len())
            .field("total_entries", &total_entries)
            .finish()
    }
}

impl NameCache {
    pub(crate) fn new(capacity: usize) -> Self {
        Self::with_active(capacity, cfg!(windows))
    }

    /// Like [`Self::new`], but takes the "should this actually cache" decision explicitly instead
    /// of hardcoding `cfg!(windows)` - see the `active` field's own doc comment above.
    pub(crate) fn with_active(capacity: usize, active: bool) -> Self {
        Self {
            entries: LruCache::new(
                NonZeroUsize::new(capacity)
                    .expect("NAME_CACHE_CAPACITY must be nonzero for an LruCache to exist at all"),
            ),
            active,
        }
    }

    /// Runs `use_candidates` against `parent_id`'s cached [`CachedChildren`] - calling
    /// `populate_miss` to fetch it from the database first if not yet cached, evicting the
    /// least-recently-used entry if already at capacity - and moves it to the front (most recently
    /// used) either way.
    ///
    /// The candidates are only ever borrowed here, never cloned out to the caller - cloning the
    /// whole map on every lookup would silently reintroduce an O(n) cost per call on a warm, large
    /// directory, defeating the point of caching it at all.
    pub(crate) fn with_cached_or_populate<T>(
        &mut self,
        parent_id: i64,
        populate_miss: impl FnOnce() -> Result<CachedChildren, crate::Error>,
        use_candidates: impl FnOnce(&CachedChildren) -> T,
    ) -> Result<T, crate::Error> {
        let candidates = self
            .entries
            .try_get_or_insert_mut(parent_id, populate_miss)?;
        Ok(use_candidates(candidates))
    }

    /// Records a newly inserted child under `parent_id`'s cached entry, if it is currently cached -
    /// keeps a directory being bulk-populated warm across repeated inserts without a full re-scan
    /// on every one of them, rather than invalidating the whole entry on every single insert.
    ///
    /// `folded_name` must already be [`crate::tree`]'s folded form (`fold_key`), not the entry's
    /// raw stored name - every candidate in the cache is kept pre-folded so a lookup against a warm
    /// entry never has to fold again (see `find_child_id_case_insensitive`).
    ///
    /// Overwrites unconditionally, unlike population's `insert_keeping_highest_id` - `id` here is
    /// always a genuinely new insert, and `tree_entries.id` is `AUTOINCREMENT` (never reused,
    /// always increasing - see "Why tree_entries.id is AUTOINCREMENT" in
    /// `metadata-schema-with-contents-table.md`), so it is always DESIGN-MOUNT-005's tiebreak
    /// winner for its folded key regardless of whatever was cached under that key before.
    ///
    /// Uses [`LruCache::peek_mut`], not `get_mut` - a mutation to an already-cached entry's
    /// *contents* is not itself a fresh "use" of that directory worth re-promoting to
    /// most-recently-used on its own; [`Self::with_cached_or_populate`] is what actually reflects
    /// use.
    ///
    /// A no-op outside a Windows build - see this module's doc comment for why keeping this cache
    /// updated on a platform that never reads it would be pure waste.
    pub(crate) fn note_inserted(&mut self, parent_id: i64, id: i64, folded_name: &str) {
        if !self.active {
            return;
        }
        if let Some(candidates) = self.entries.peek_mut(&parent_id) {
            candidates.insert(folded_name.to_string(), id);
        }
    }

    /// Drops `parent_id`'s cached list entirely, if present - used wherever a child is removed or
    /// renamed away. Simpler and safer than patching the cached list in place for those less
    /// common, more intricate mutation shapes; the next miss just repopulates it from the database.
    ///
    /// A no-op outside a Windows build - see this module's doc comment for why keeping this cache
    /// updated on a platform that never reads it would be pure waste.
    pub(crate) fn invalidate(&mut self, parent_id: i64) {
        if !self.active {
            return;
        }
        self.entries.pop(&parent_id);
    }
}

#[cfg(test)]
mod tests {
    use super::NameCache;

    #[test]
    fn note_inserted_and_invalidate_are_no_ops_off_windows() {
        // Meaningful only on a non-Windows build - `cfg!(windows)` is false here, matching this
        // crate's own CI/development platform. The Windows-side "these calls actually take effect"
        // behavior is covered by tree.rs's #[cfg(windows)]-gated tests instead, compiled only there.
        if cfg!(windows) {
            return;
        }
        let mut cache = NameCache::new(16);
        cache
            .with_cached_or_populate(
                0,
                || {
                    let mut candidates = std::collections::HashMap::new();
                    candidates.insert("FOO".to_string(), 1);
                    Ok(candidates)
                },
                |_| (),
            )
            .unwrap();

        cache.note_inserted(0, 2, "BAR");
        cache.invalidate(0);

        let (has_foo, has_bar) = cache
            .with_cached_or_populate(
                0,
                || panic!("invalidate() must have been a no-op - the entry must still be cached"),
                |c| (c.contains_key("FOO"), c.contains_key("BAR")),
            )
            .unwrap();
        assert!(
            has_foo,
            "the originally populated entry must still be there"
        );
        assert!(!has_bar, "note_inserted() must have been a no-op");
    }

    #[test]
    fn debug_output_is_a_fill_level_summary_not_the_cached_names() {
        let mut cache = NameCache::new(16);
        cache
            .with_cached_or_populate(
                0,
                || {
                    let mut candidates = std::collections::HashMap::new();
                    candidates.insert("SECRET-FILENAME".to_string(), 1);
                    candidates.insert("OTHER-FILENAME".to_string(), 2);
                    Ok(candidates)
                },
                |_| (),
            )
            .unwrap();
        cache
            .with_cached_or_populate(1, || Ok(std::collections::HashMap::new()), |_| ())
            .unwrap();

        let debug = format!("{cache:?}");
        assert!(!debug.contains("SECRET-FILENAME"));
        assert!(!debug.contains("OTHER-FILENAME"));
        assert!(debug.contains("directories_cached: 2"));
        assert!(debug.contains("total_entries: 2"));
    }
}
