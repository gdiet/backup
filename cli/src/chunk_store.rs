//! Encapsulates "a chunk's bytes may live in more than one physical extent
//! in the data store" (see `docs/plans/implemented/03-chunk-extents.md`) behind a small
//! space allocator and two read/write helpers - the only things in this
//! codebase that need to know that. Every other consumer of chunk bytes
//! (`store`'s writer, `check`, `restore`, `mount`) goes through
//! [`read_chunk_bytes`]/[`write_chunk_from_cache`] instead of touching
//! `store::LongTermStore::read`/`write` with a single range directly.

use std::sync::Mutex;

use rusqlite::Connection;
use store::{LongTermStore, ReadIntegrity};

use crate::io_limiter::IoLimiter;
use spillcache::WriteCache;

/// Size of the pieces [`write_chunk_from_cache`] drains `bytes` in - keeps
/// peak memory for writing out a chunk bounded regardless of the chunk's
/// total size, the same way `mount.rs`'s `PERSIST_CHUNK_SIZE` and
/// `store.rs`'s `READ_BUFFER_SIZE` already bound their own streaming
/// loops.
const DRAIN_PIECE_SIZE: u64 = 256 * 1024;

/// First-fit allocator over the gaps left in the data store by
/// `reclaim-space`-deleted chunks, plus an open-ended trailing region past
/// the highest byte ever written (mirrors Scala's `Long.MAX_VALUE`-sentinel
/// trailing gap in `FreeAreas`).
///
/// One coarse [`Mutex`], not a lock-free structure: `reserve` only ever does
/// small, fast, in-memory bookkeeping (no I/O happens while the lock is
/// held - the actual store write happens afterward, using the already-
/// reserved ranges), so contention cost is negligible even with many
/// parallel `store` workers calling it concurrently.
///
/// **Deliberately `reserve`-only, no `release`/`free` method** - even
/// though a caller can end up reserving (and physically writing) a range it
/// never ends up needing. This happens on the losing side of the
/// `apply_backup_batch`/`db::resolve_content` dedup-insert race (see that
/// function's own doc comment on `ON CONFLICT ... DO NOTHING`): two callers
/// discovering the same new chunk content at once each reserve and write
/// their own extent, but only the winner's extent ever gets a
/// `chunk_extents` row, so the loser's reservation is permanently gone from
/// *this process's* `gaps` for the rest of its run.
///
/// Raised and rejected as a "give it back immediately" idea (2026-08-15, no
/// code changed): it looks like a small addition (interval-merge the
/// released range back into `gaps`) but isn't. `reserve` only ever touches
/// `gaps[0]` today, so a real `release` would need proper sorted insertion
/// plus merging with adjacent gaps to avoid needlessly fragmenting the
/// list, logic that doesn't exist here at all yet. More fundamentally,
/// *knowing* a reservation turned out to be redundant only happens inside
/// `db::resolve_content`, in the `db` crate, which has no notion of
/// `SpaceAllocator` at all (this type lives in `cli`, one layer up, and
/// `db` mustn't depend back on it); surfacing "these specific extents are
/// now orphaned" up to whoever holds the allocator would mean changing
/// `resolve_content`'s return contract, affecting every caller
/// (`apply_backup_batch`, the Scala migration tool), not just this one.
/// And the payoff is small while the downside isn't: the waste is already
/// bounded (one chunk's worth per race, and races require genuinely
/// identical new content written at almost the same moment, rare in
/// practice) and self-healing, since the *next* `store`/`mount
/// --read-write` run rebuilds this allocator from
/// `db::chunk_extents_sorted` (the DB's real `chunk_extents` table, which
/// the loser's write was never added to), so its range shows up as an
/// ordinary gap again with no explicit reclaim needed. A buggy `release`
/// handing out a range that turns out to still be genuinely referenced
/// would instead be silent data corruption, not wasted space - and a
/// mid-run `release` would reintroduce non-monotonic allocation, exactly
/// the kind of layout churn `docs/plans/store-vs-mount-slow-drive-write-path.md`
/// had to rule out as a candidate cause of the slow-drive regression there,
/// so "fixing" this could plausibly make that problem worse for the
/// workloads most sensitive to it, not better. See
/// `docs/plans/persist-worker-thread-pool.md`'s "Still open" section for
/// where this is tracked.
pub struct SpaceAllocator {
    /// Sorted by `start`, non-overlapping. The final entry's `stop` is
    /// always `u64::MAX`: the open-ended region past the last known extent,
    /// which can satisfy however much of a reservation remains after real
    /// gaps run out.
    gaps: Mutex<Vec<(u64, u64)>>,
}

impl SpaceAllocator {
    /// Builds the allocator from every extent currently in the repository
    /// (`db::chunk_extents_sorted`, already sorted by `start`), deriving the
    /// gaps between them plus the open-ended trailing region.
    pub fn from_sorted_extents(extents: &[(i64, i64)]) -> Self {
        let mut gaps = Vec::new();
        let mut cursor = 0u64;
        for &(start, stop) in extents {
            let (start, stop) = (start as u64, stop as u64);
            if start > cursor {
                gaps.push((cursor, start));
            }
            cursor = cursor.max(stop);
        }
        gaps.push((cursor, u64::MAX));
        Self {
            gaps: Mutex::new(gaps),
        }
    }

    /// Reserves `size` bytes, returning one or more half-open ranges whose
    /// lengths sum to `size`, in the order they must be written/read to
    /// form one contiguous logical byte sequence. Always consumes gaps
    /// starting from the lowest address: takes a gap whole if it's not
    /// bigger than what's still needed, otherwise splits it, so a single
    /// reservation can span several old gaps before finally reaching (and,
    /// if needed, only partially consuming) the trailing open-ended region.
    pub fn reserve(&self, size: u64) -> Vec<(u64, u64)> {
        if size == 0 {
            return Vec::new();
        }
        let mut gaps = self.gaps.lock().expect("allocator mutex poisoned");
        let mut remaining = size;
        let mut result = Vec::new();
        loop {
            let (start, stop) = gaps[0];
            let available = stop - start;
            if available <= remaining {
                result.push((start, stop));
                remaining -= available;
                gaps.remove(0);
                if remaining == 0 {
                    return result;
                }
            } else {
                result.push((start, start + remaining));
                gaps[0] = (start + remaining, stop);
                return result;
            }
        }
    }
}

/// Reads bytes back from the store given already-resolved extents (see
/// `db::chunk_extents`), without touching the metadata database at all -
/// the I/O-only half of [`read_chunk_bytes`], split out so a caller that
/// resolved those extents itself under a database lock (currently only
/// `mount.rs`'s `Inner::read_persisted`, the one place chunk reads happen
/// from genuinely concurrent threads - `check`/`restore`/`compact-store`
/// are all single-threaded, so holding the lock for the whole read there
/// costs nothing) can release that lock *before* doing the actual disk
/// I/O, instead of blocking every other FUSE/WinFSP dispatch thread's own
/// database access behind however long this read's `store.read` calls
/// take. `length` is the chunk's total byte length (already known to the
/// caller from `chunks.length`/`ChunkInfo`), used to size the output
/// buffer up front.
pub fn read_chunk_bytes_from_extents(
    store: &LongTermStore,
    extents: &[(i64, i64)],
    length: u64,
) -> std::io::Result<(Vec<u8>, ReadIntegrity)> {
    let mut buf = vec![0u8; length as usize];
    let mut offset = 0usize;
    let mut missing_or_short = Vec::new();
    for &(start, stop) in extents {
        let extent_len = (stop - start) as usize;
        let integrity = store.read(start as u64, &mut buf[offset..offset + extent_len])?;
        if let ReadIntegrity::Incomplete {
            missing_or_short: mut part,
        } = integrity
        {
            missing_or_short.append(&mut part);
        }
        offset += extent_len;
    }
    let integrity = if missing_or_short.is_empty() {
        ReadIntegrity::Complete
    } else {
        ReadIntegrity::Incomplete { missing_or_short }
    };
    Ok((buf, integrity))
}

/// Reads `chunk_id`'s bytes back from the store, looking up and
/// concatenating its extents in order. `length` is the chunk's total byte
/// length (already known to the caller from `chunks.length`/`ChunkInfo`),
/// used to size the output buffer up front.
pub fn read_chunk_bytes(
    conn: &Connection,
    store: &LongTermStore,
    chunk_id: i64,
    length: u64,
) -> Result<(Vec<u8>, ReadIntegrity), db::Error> {
    let extents = db::chunk_extents(conn, chunk_id)?;
    Ok(read_chunk_bytes_from_extents(store, &extents, length)?)
}

/// Computes `buf`'s content hash the same way `store`'s chunking pipeline
/// does (blake3 extendable output, truncated to `crate::store::HASH_LENGTH`
/// bytes) and reports whether it matches `expected` (a chunk's recorded
/// `chunks.hash`). Shared by `check`'s integrity verification and
/// `restore`'s optional `--verify` - the only two places in this codebase
/// that need to re-derive a chunk's hash from its bytes rather than trust
/// what `store` already recorded.
pub(crate) fn chunk_hash_matches(buf: &[u8], expected: &[u8]) -> bool {
    let mut hash = [0u8; crate::store::HASH_LENGTH];
    blake3::Hasher::new()
        .update(buf)
        .finalize_xof()
        .fill(&mut hash);
    hash.as_slice() == expected
}

/// Reserves store space for a chunk of `total_len` bytes (its boundary/
/// hash is already known - see `spilling_chunker::SpillingHashingChunker`)
/// and writes it out by draining `bytes` (a [`WriteCache`], not a plain
/// `&[u8]`) in [`DRAIN_PIECE_SIZE`] pieces across as many extents as the
/// reservation needed, returning those extents for the caller to pass to
/// `db::ChunkRef::New`. Draining in bounded pieces rather than requiring
/// the caller to already hold `total_len` contiguously in memory is the
/// entire point of buffering a chunk in a `WriteCache` (RAM-budgeted,
/// spills to disk) instead of a plain `Vec<u8>` in the first place - a
/// single very large chunk (a large CDC chunk, or an entire file under
/// `chunking: none`) never needs its full size resident in RAM at once,
/// either while buffering it or while writing it out.
///
/// `io_limiter`, if given, is acquired around each individual `store.write`
/// call (not around the whole chunk) - see [`crate::io_limiter`] for why
/// this is a plain semaphore gating the calling thread rather than a
/// separate thread pool, and why it lives at this level rather than inside
/// `LongTermStore` itself.
pub fn write_chunk_from_cache(
    store: &LongTermStore,
    allocator: &SpaceAllocator,
    bytes: &mut WriteCache,
    total_len: u64,
    io_limiter: Option<&IoLimiter>,
) -> std::io::Result<Vec<(u64, u64)>> {
    let extents = allocator.reserve(total_len);
    let mut chunk_pos = 0u64;
    for &(start, stop) in &extents {
        let mut store_pos = start;
        while store_pos < stop {
            let piece_len = DRAIN_PIECE_SIZE.min(stop - store_pos);
            let piece = bytes.read_filling_gaps(chunk_pos, piece_len, |_, _| {
                unreachable!("a freshly-accumulated chunk buffer has no gaps to fill")
            })?;
            let _permit = io_limiter.map(IoLimiter::acquire);
            store.write(store_pos, &piece)?;
            chunk_pos += piece_len;
            store_pos += piece_len;
        }
    }
    Ok(extents)
}

/// Reserves store space for `bytes.len()` bytes and writes them out across
/// as many extents as the reservation needed, returning those extents.
/// The `compact-store` counterpart to [`write_chunk_from_cache`]: used
/// when the bytes to write are already fully materialized (from
/// [`read_chunk_bytes`], relocating an existing chunk) rather than being
/// accumulated into a [`WriteCache`] for the first time - no draining in
/// bounded pieces needed, since nothing here is buffering a chunk that
/// might still be growing.
pub fn write_chunk_bytes(
    store: &LongTermStore,
    allocator: &SpaceAllocator,
    bytes: &[u8],
) -> std::io::Result<Vec<(u64, u64)>> {
    let extents = allocator.reserve(bytes.len() as u64);
    let mut offset = 0usize;
    for &(start, stop) in &extents {
        let len = (stop - start) as usize;
        store.write(start, &bytes[offset..offset + len])?;
        offset += len;
    }
    Ok(extents)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn with_no_extents_reserve_appends_from_zero() {
        let allocator = SpaceAllocator::from_sorted_extents(&[]);
        assert_eq!(allocator.reserve(100), vec![(0, 100)]);
        assert_eq!(
            allocator.reserve(50),
            vec![(100, 150)],
            "the trailing region keeps growing past what was already reserved"
        );
    }

    #[test]
    fn reserve_fits_exactly_into_a_single_gap() {
        // 3x1000-byte chunks, the middle one deleted+reclaimed: a gap at [1000, 2000).
        let allocator = SpaceAllocator::from_sorted_extents(&[(0, 1000), (2000, 3000)]);

        assert_eq!(allocator.reserve(1000), vec![(1000, 2000)]);
        assert_eq!(
            allocator.reserve(1),
            vec![(3000, 3001)],
            "the gap is now used up; falls back to the trailing region"
        );
    }

    #[test]
    fn reserve_splits_a_larger_gap_and_leaves_the_remainder_reusable() {
        let allocator = SpaceAllocator::from_sorted_extents(&[(0, 1000), (2000, 3000)]);

        assert_eq!(allocator.reserve(300), vec![(1000, 1300)]);
        assert_eq!(
            allocator.reserve(700),
            vec![(1300, 2000)],
            "the rest of the same gap, still preferred over the trailing region"
        );
    }

    #[test]
    fn reserve_spans_a_gap_and_the_trailing_region_when_the_gap_alone_is_too_small() {
        // The plan's own worked example: 3x1000-byte chunks, the middle one
        // reclaimed (gap [1000, 2000)), then a 1200-byte request - too big
        // for that single gap alone.
        let allocator = SpaceAllocator::from_sorted_extents(&[(0, 1000), (2000, 3000)]);

        let extents = allocator.reserve(1200);

        assert_eq!(extents, vec![(1000, 2000), (3000, 3200)]);
    }

    #[test]
    fn reserve_spans_more_than_two_gaps_if_needed() {
        let allocator =
            SpaceAllocator::from_sorted_extents(&[(0, 100), (200, 300), (400, 500), (600, 700)]);

        // Gaps: [100,200) 100b, [300,400) 100b, [500,600) 100b, [700,MAX) trailing.
        let extents = allocator.reserve(250);

        assert_eq!(extents, vec![(100, 200), (300, 400), (500, 550)]);
    }

    #[test]
    fn read_chunk_bytes_concatenates_extents_in_order() {
        let temp_dir = tempfile::tempdir().unwrap();
        let store = LongTermStore::new(temp_dir.path(), false);
        store.write(100, b"hello").unwrap();
        store.write(200, b"world").unwrap();

        let db_temp = tempfile::tempdir().unwrap();
        let repo_root = db_temp.path().join("repo");
        db::init_repository(
            &repo_root,
            &db::RepositorySettings::new(20, db::Chunking::Cdc).unwrap(),
        )
        .unwrap();
        let conn = db::open_repository(&repo_root)
            .unwrap()
            .open_write_connection()
            .unwrap();
        conn.execute(
            "INSERT INTO chunks (id, length, hash) VALUES (1, 10, x'AA')",
            (),
        )
        .unwrap();
        conn.execute(
            "INSERT INTO chunk_extents (chunk_id, seq, start, stop) VALUES
             (1, 0, 100, 105), (1, 1, 200, 205)",
            (),
        )
        .unwrap();

        let (bytes, integrity) = read_chunk_bytes(&conn, &store, 1, 10).unwrap();

        assert_eq!(integrity, ReadIntegrity::Complete);
        assert_eq!(bytes, b"helloworld");
    }

    #[test]
    fn write_chunk_from_cache_drains_a_write_cache_across_returned_extents() {
        use spillcache::RamBudget;
        use std::sync::Arc;

        let temp_dir = tempfile::tempdir().unwrap();
        let store = LongTermStore::new(temp_dir.path(), false);
        let allocator = SpaceAllocator::from_sorted_extents(&[(0, 1000), (2000, 3000)]);

        // A tiny RAM budget forces the cache to spill to disk partway
        // through - `write_chunk_from_cache` must still drain it correctly
        // regardless of which tier(s) the bytes actually live in.
        let budget = Arc::new(RamBudget::new(4));
        let spill_dir = tempfile::tempdir().unwrap();
        let mut cache = WriteCache::new(budget, spill_dir.path().join("chunk"), 0);
        let payload = [7u8; 1200];
        cache.write(0, &payload).unwrap();

        let extents = write_chunk_from_cache(&store, &allocator, &mut cache, 1200, None).unwrap();

        assert_eq!(extents, vec![(1000, 2000), (3000, 3200)]);
        let mut buf = [0u8; 1200];
        let integrity = {
            store.read(1000, &mut buf[..1000]).unwrap();
            store.read(3000, &mut buf[1000..]).unwrap()
        };
        assert_eq!(integrity, ReadIntegrity::Complete);
        assert_eq!(buf, payload);
    }

    #[test]
    fn write_chunk_bytes_writes_a_plain_slice_across_returned_extents() {
        let temp_dir = tempfile::tempdir().unwrap();
        let store = LongTermStore::new(temp_dir.path(), false);
        let allocator = SpaceAllocator::from_sorted_extents(&[(0, 100), (200, 300)]);

        let extents = write_chunk_bytes(&store, &allocator, &[9u8; 150]).unwrap();

        assert_eq!(extents, vec![(100, 200), (300, 350)]);
        let mut buf = [0u8; 150];
        store.read(100, &mut buf[..100]).unwrap();
        store.read(300, &mut buf[100..]).unwrap();
        assert_eq!(buf, [9u8; 150]);
    }
}
