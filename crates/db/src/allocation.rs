//! Free-byte-range allocation (DESIGN-STORE-003 in `docs/design/byte-store.md`) - answers "which
//! byte range is free to write new content into". The state is derived entirely from
//! `chunk_extents`, which already lives in this database. It is built into memory once per write
//! session ([`FreeSpace::build`]) and kept in step with the database from then on, so a
//! reservation never scans `chunk_extents` again.
//!
//! `pub(crate)` only, reached exclusively through [`crate::content`] (DESIGN-METADATA-006) and
//! `Repository`'s own write transactions.

use std::collections::BTreeMap;

use rusqlite::Connection;

use crate::Error;

/// The exclusive upper limit of the byte range that can be allocated. `chunk_extents` stores its
/// positions as `i64`.
const END_OF_ADDRESS_SPACE: u64 = i64::MAX as u64;

/// The free byte ranges of a repository's byte store: a sorted set of disjoint, non-adjacent
/// `start -> stop` gaps. The last gap always runs from the end of the used range to
/// [`END_OF_ADDRESS_SPACE`], so a reservation can never run out of space in practice.
#[derive(Debug)]
pub(crate) struct FreeSpace {
    gaps: BTreeMap<u64, u64>,
}

impl FreeSpace {
    /// Reads every `chunk_extents` row once and derives the gaps between them. `chunk_extents`
    /// positions are always disjoint (every range still recorded there was itself reserved through
    /// this module, and reclaiming removes a chunk's rows entirely rather than shrinking them), so
    /// a single ordered pass finds every gap.
    pub(crate) fn build(conn: &Connection) -> Result<Self, Error> {
        let mut gaps = BTreeMap::new();
        let mut cursor: u64 = 0;
        let mut stmt = conn.prepare("SELECT start, stop FROM chunk_extents ORDER BY start")?;
        let mut rows = stmt.query(())?;
        while let Some(row) = rows.next()? {
            let start = row.get::<_, i64>(0)? as u64;
            let stop = row.get::<_, i64>(1)? as u64;
            if start > cursor {
                gaps.insert(cursor, start);
            }
            cursor = stop;
        }
        gaps.insert(cursor, END_OF_ADDRESS_SPACE);
        Ok(Self { gaps })
    }

    /// Takes `length` bytes out of the free space and returns the `(start, stop)` ranges that
    /// together cover exactly `length` bytes, in order - more than one only if no single gap was
    /// large enough on its own. Lower gaps are used before higher ones, so space freed by reclaim
    /// is filled before the used range is extended.
    pub(crate) fn reserve(&mut self, length: u64) -> Vec<(u64, u64)> {
        let mut remaining = length;
        let mut ranges = Vec::new();
        while remaining > 0 {
            let (start, stop) = self
                .gaps
                .pop_first()
                .expect("the last gap always extends to the end of the address space");
            let take = (stop - start).min(remaining);
            ranges.push((start, start + take));
            remaining -= take;
            if start + take < stop {
                self.gaps.insert(start + take, stop);
            }
        }
        ranges
    }

    /// Returns `start..stop` to the free space, merging it with an adjacent gap on either side.
    pub(crate) fn release(&mut self, start: u64, stop: u64) {
        let mut start = start;
        let mut stop = stop;
        if let Some((&previous_start, &previous_stop)) = self.gaps.range(..=start).next_back()
            && previous_stop == start
        {
            self.gaps.remove(&previous_start);
            start = previous_start;
        }
        if let Some(next_stop) = self.gaps.remove(&stop) {
            stop = next_stop;
        }
        self.gaps.insert(start, stop);
    }
}

/// What a write transaction may use of the free space (see `Repository::with_transaction_alloc`).
/// A reservation is taken from the in-memory [`FreeSpace`] right away, which is safe even if the
/// transaction later rolls back (the caller then discards the whole [`FreeSpace`] and rebuilds it
/// from the database). A range freed by reclaim is only remembered here and handed back to the
/// [`FreeSpace`] once the transaction has committed. Doing that earlier could let a rolled-back
/// transaction leave a range marked free that a chunk still uses.
#[derive(Debug)]
pub(crate) struct Allocation<'a> {
    free_space: Option<&'a mut FreeSpace>,
    freed: Vec<(u64, u64)>,
}

impl<'a> Allocation<'a> {
    pub(crate) fn new(free_space: Option<&'a mut FreeSpace>) -> Self {
        Self {
            free_space,
            freed: Vec::new(),
        }
    }

    /// See [`FreeSpace::reserve`].
    pub(crate) fn reserve(&mut self, length: u64) -> Vec<(u64, u64)> {
        self.free_space
            .as_mut()
            .expect("only transactions that ask for the free space reserve from it")
            .reserve(length)
    }

    /// Notes that `start..stop` no longer holds any chunk. It becomes reusable once the
    /// transaction has committed.
    pub(crate) fn record_freed(&mut self, start: u64, stop: u64) {
        self.freed.push((start, stop));
    }

    pub(crate) fn into_freed(self) -> Vec<(u64, u64)> {
        self.freed
    }
}

#[cfg(test)]
mod tests {
    use super::FreeSpace;
    use crate::{Error, RepositorySettings, init_repository, open_repository};

    const HASH_A: &[u8] = &[0xAA; 20];
    const HASH_B: &[u8] = &[0xBB; 20];

    fn repo() -> (crate::Repository, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let repo_root = dir.path().join("repo");
        let settings = RepositorySettings::new(20, 1_700_000_000_000);
        init_repository(&repo_root, settings).expect("init must succeed");
        let repo = open_repository(&repo_root).expect("open must succeed");
        (repo, dir)
    }

    fn insert_extent(repo: &crate::Repository, chunk_id: i64, start: i64, stop: i64) {
        repo.with_connection(|conn, _cache| {
            conn.execute(
                "INSERT INTO chunks (id, length, hash) \
                 VALUES (?1, ?2, X'0102030405060708090A0B0C0D0E0F1011121314')",
                (chunk_id, stop - start),
            )?;
            conn.execute(
                "INSERT INTO chunk_extents (chunk_id, seq, start, stop) VALUES (?1, 0, ?2, ?3)",
                (chunk_id, start, stop),
            )?;
            Ok(())
        })
        .unwrap();
    }

    fn reserve_from_database(repo: &crate::Repository, length: u64) -> Vec<(u64, u64)> {
        repo.with_connection(|conn, _cache| Ok(FreeSpace::build(conn)?.reserve(length)))
            .unwrap()
    }

    #[test]
    fn reserve_starts_at_zero_in_an_empty_store() {
        let (repo, _dir) = repo();
        assert_eq!(reserve_from_database(&repo, 100), vec![(0, 100)]);
    }

    #[test]
    fn reserve_extends_past_the_high_water_mark() {
        let (repo, _dir) = repo();
        insert_extent(&repo, 1, 0, 100);
        assert_eq!(reserve_from_database(&repo, 50), vec![(100, 150)]);
    }

    #[test]
    fn reserve_fills_a_gap_between_two_extents_before_extending() {
        let (repo, _dir) = repo();
        insert_extent(&repo, 1, 0, 100);
        insert_extent(&repo, 2, 130, 200);
        // Gap is exactly 30 bytes (100..130) - a 30-byte request fits inside it alone.
        assert_eq!(reserve_from_database(&repo, 30), vec![(100, 130)]);
    }

    #[test]
    fn reserve_spans_a_gap_and_the_high_water_mark_when_the_gap_is_too_small() {
        let (repo, _dir) = repo();
        insert_extent(&repo, 1, 0, 100);
        insert_extent(&repo, 2, 110, 200);
        // Gap is only 10 bytes (100..110); the remaining 40 bytes extend past 200.
        assert_eq!(
            reserve_from_database(&repo, 50),
            vec![(100, 110), (200, 240)]
        );
    }

    #[test]
    fn reserve_of_zero_length_returns_no_ranges() {
        let (repo, _dir) = repo();
        assert_eq!(reserve_from_database(&repo, 0), Vec::<(u64, u64)>::new());
    }

    #[test]
    fn a_gap_before_the_first_extent_is_free() {
        let (repo, _dir) = repo();
        insert_extent(&repo, 1, 40, 100);
        assert_eq!(reserve_from_database(&repo, 50), vec![(0, 40), (100, 110)]);
    }

    #[test]
    fn consecutive_reservations_do_not_overlap() {
        let (repo, _dir) = repo();
        insert_extent(&repo, 1, 10, 20);
        let mut free_space = repo
            .with_connection(|conn, _cache| FreeSpace::build(conn))
            .unwrap();
        assert_eq!(free_space.reserve(15), vec![(0, 10), (20, 25)]);
        assert_eq!(free_space.reserve(10), vec![(25, 35)]);
    }

    #[test]
    fn a_released_range_is_reused_and_merged_with_its_neighbors() {
        let (repo, _dir) = repo();
        insert_extent(&repo, 1, 0, 100);
        let mut free_space = repo
            .with_connection(|conn, _cache| FreeSpace::build(conn))
            .unwrap();
        // Free 20..40 and 40..60 separately: they merge into one 20..60 gap.
        free_space.release(40, 60);
        free_space.release(20, 40);
        assert_eq!(free_space.reserve(40), vec![(20, 60)]);
        // Freeing the very end of the used range merges into the open-ended last gap.
        free_space.release(60, 100);
        assert_eq!(free_space.reserve(60), vec![(60, 120)]);
    }

    #[test]
    fn space_freed_by_a_purge_is_reused_in_the_same_session() {
        let (repo, _dir) = repo();
        repo.load_free_space().unwrap();
        let (first_chunk, _) = repo.reserve_and_insert_chunk(100, HASH_A).unwrap();
        repo.reserve_and_insert_chunk(50, HASH_B).unwrap();
        let content_id = repo
            .find_or_create_content(100, &[0x11; 20], &[first_chunk])
            .unwrap();
        let file_id = repo.settle_file(0, "a.txt", 100, content_id).unwrap();
        repo.unlink_file(file_id, 200).unwrap();
        let purged = repo.purge_deleted_entry(file_id, false).unwrap();
        assert_eq!(purged.reclaimed_bytes, 100);

        let (_, ranges) = repo.reserve_and_insert_chunk(100, &[0xCC; 20]).unwrap();
        assert_eq!(
            ranges,
            vec![(0, 100)],
            "the purged chunk's range must be handed out again without a restart"
        );
    }

    #[test]
    fn a_failed_reservation_does_not_leak_its_range() {
        let (repo, _dir) = repo();
        repo.reserve_and_insert_chunk(100, HASH_A).unwrap();
        // Same (length, hash): reserves 100..200 in memory, then violates the chunks table's
        // uniqueness constraint, so the transaction rolls back.
        assert!(repo.reserve_and_insert_chunk(100, HASH_A).is_err());
        let (_, ranges) = repo.reserve_and_insert_chunk(50, HASH_B).unwrap();
        assert_eq!(ranges, vec![(100, 150)]);
    }

    #[test]
    fn a_range_freed_by_a_rolled_back_transaction_is_not_reused() {
        let (repo, _dir) = repo();
        repo.reserve_and_insert_chunk(100, HASH_A).unwrap();
        let result: Result<(), Error> = repo.with_transaction_alloc(false, |_, _, allocation| {
            allocation.record_freed(0, 100);
            Err(Error::NoSuchEntry(1))
        });
        assert!(result.is_err());
        let (_, ranges) = repo.reserve_and_insert_chunk(10, HASH_B).unwrap();
        assert_eq!(
            ranges,
            vec![(100, 110)],
            "the chunk at 0..100 still exists, so its range is not free"
        );
    }

    #[test]
    fn registering_a_chunk_at_a_fixed_position_is_respected_by_later_reservations() {
        let (repo, _dir) = repo();
        repo.load_free_space().unwrap();
        repo.register_existing_chunk(100, HASH_A, &[(500, 600)])
            .unwrap();
        let (_, ranges) = repo.reserve_and_insert_chunk(600, HASH_B).unwrap();
        assert_eq!(ranges, vec![(0, 500), (600, 700)]);
    }

    #[test]
    fn load_free_space_is_accepted_by_a_read_only_repository() {
        let dir = tempfile::tempdir().unwrap();
        let repo_root = dir.path().join("repo");
        init_repository(&repo_root, RepositorySettings::new(20, 1_700_000_000_000)).unwrap();
        let repo = crate::open_repository_read_only(&repo_root).unwrap();
        repo.load_free_space().unwrap();
    }
}
