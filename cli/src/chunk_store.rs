//! Encapsulates "a chunk's bytes may live in more than one physical extent
//! in the data store" (see `docs/plans/chunk-extents.md`) behind a small
//! space allocator and two read/write helpers - the only things in this
//! codebase that need to know that. Every other consumer of chunk bytes
//! (`store`'s writer, `check`, `restore`, a future FUSE mount) goes through
//! [`read_chunk_bytes`]/[`write_chunk_bytes`] instead of touching
//! `store::LongTermStore::read`/`write` with a single range directly.

use std::sync::Mutex;

use rusqlite::Connection;
use store::{LongTermStore, ReadIntegrity};

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
    let mut buf = vec![0u8; length as usize];
    let mut offset = 0usize;
    let mut missing_or_short = Vec::new();
    for (start, stop) in extents {
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

/// Reserves store space for `bytes` via `allocator` and writes them across
/// as many extents as the reservation needed, returning those extents for
/// the caller to pass to `db::ChunkRef::New`.
pub fn write_chunk_bytes(
    store: &LongTermStore,
    allocator: &SpaceAllocator,
    bytes: &[u8],
) -> std::io::Result<Vec<(u64, u64)>> {
    let extents = allocator.reserve(bytes.len() as u64);
    let mut offset = 0usize;
    for &(start, stop) in &extents {
        let extent_len = (stop - start) as usize;
        store.write(start, &bytes[offset..offset + extent_len])?;
        offset += extent_len;
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
    fn write_chunk_bytes_reserves_and_writes_across_returned_extents() {
        let temp_dir = tempfile::tempdir().unwrap();
        let store = LongTermStore::new(temp_dir.path(), false);
        let allocator = SpaceAllocator::from_sorted_extents(&[(0, 1000), (2000, 3000)]);

        let extents = write_chunk_bytes(&store, &allocator, &[7u8; 1200]).unwrap();

        assert_eq!(extents, vec![(1000, 2000), (3000, 3200)]);
        let mut buf = [0u8; 1200];
        let integrity = {
            store.read(1000, &mut buf[..1000]).unwrap();
            store.read(3000, &mut buf[1000..]).unwrap()
        };
        assert_eq!(integrity, ReadIntegrity::Complete);
        assert!(buf.iter().all(|&b| b == 7));
    }
}
