//! Free-byte-range allocation (DESIGN-STORE-003 in `docs/design/byte-store.md`) - answers "which
//! byte range is free to write new content into". The state is derived entirely from
//! `chunk_extents`, which already lives in this database. It is built into memory once per write
//! session ([`FreeSpace::build`]) and only ever shrinks from then on (DESIGN-STORE-006), so a
//! reservation never scans `chunk_extents` again.
//!
//! `pub(crate)` only, reached exclusively through [`crate::content`] (DESIGN-METADATA-006) and
//! `Repository`'s own write transactions.

use std::collections::VecDeque;

use rusqlite::Connection;

use crate::Error;

/// The exclusive upper limit of the byte range that can be allocated. `chunk_extents` stores its
/// positions as `i64`.
const END_OF_ADDRESS_SPACE: u64 = i64::MAX as u64;

/// The free byte ranges of a repository's byte store: disjoint gaps, sorted by position. The last
/// gap always runs from the end of the used range to [`END_OF_ADDRESS_SPACE`], so a reservation can
/// never run out of space in practice.
#[derive(Debug)]
pub(crate) struct FreeSpace {
    gaps: VecDeque<(u64, u64)>,
}

impl FreeSpace {
    /// Reads every `chunk_extents` row once and derives the gaps between them. `chunk_extents`
    /// positions are always disjoint (every range still recorded there was itself reserved through
    /// this module, and reclaiming removes a chunk's rows entirely rather than shrinking them), so
    /// a single ordered pass finds every gap.
    pub(crate) fn build(conn: &Connection) -> Result<Self, Error> {
        let mut gaps = VecDeque::new();
        let mut cursor: u64 = 0;
        let mut stmt = conn.prepare("SELECT start, stop FROM chunk_extents ORDER BY start")?;
        let mut rows = stmt.query(())?;
        while let Some(row) = rows.next()? {
            let start = row.get::<_, i64>(0)? as u64;
            let stop = row.get::<_, i64>(1)? as u64;
            if start > cursor {
                gaps.push_back((cursor, start));
            }
            cursor = stop;
        }
        gaps.push_back((cursor, END_OF_ADDRESS_SPACE));
        Ok(Self { gaps })
    }

    /// Takes `length` bytes out of the free space and returns the `(start, stop)` ranges that
    /// together cover exactly `length` bytes, in order - more than one only if no single gap was
    /// large enough on its own. Lower gaps are used before higher ones, so space freed by an
    /// earlier session is filled before the used range is extended.
    pub(crate) fn reserve(&mut self, length: u64) -> Vec<(u64, u64)> {
        let mut remaining = length;
        let mut ranges = Vec::new();
        while remaining > 0 {
            let (start, stop) = self
                .gaps
                .pop_front()
                .expect("the last gap always extends to the end of the address space");
            let take = (stop - start).min(remaining);
            ranges.push((start, start + take));
            remaining -= take;
            if start + take < stop {
                self.gaps.push_front((start + take, stop));
            }
        }
        ranges
    }
}

#[cfg(test)]
mod tests {
    use super::FreeSpace;
    use crate::{RepositorySettings, init_repository, open_repository};

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
    fn space_freed_by_a_purge_is_reused_by_the_next_session_not_the_running_one() {
        let dir = tempfile::tempdir().unwrap();
        let repo_root = dir.path().join("repo");
        init_repository(&repo_root, RepositorySettings::new(20, 1_700_000_000_000)).unwrap();
        let repo = open_repository(&repo_root).unwrap();
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
            vec![(150, 250)],
            "a running session appends past the end of the used range"
        );
        drop(repo);

        let next_session = open_repository(&repo_root).unwrap();
        let (_, ranges) = next_session
            .reserve_and_insert_chunk(100, &[0xDD; 20])
            .unwrap();
        assert_eq!(ranges, vec![(0, 100)], "the purged range is free again");
    }

    #[test]
    fn a_failed_reservation_leaves_a_gap_that_only_the_next_session_reuses() {
        let dir = tempfile::tempdir().unwrap();
        let repo_root = dir.path().join("repo");
        init_repository(&repo_root, RepositorySettings::new(20, 1_700_000_000_000)).unwrap();
        let repo = open_repository(&repo_root).unwrap();
        repo.reserve_and_insert_chunk(100, HASH_A).unwrap();
        // Same (length, hash): reserves 100..200 in memory, then violates the chunks table's
        // uniqueness constraint, so the transaction rolls back and nothing is recorded for it.
        assert!(repo.reserve_and_insert_chunk(100, HASH_A).is_err());
        let (_, ranges) = repo.reserve_and_insert_chunk(50, HASH_B).unwrap();
        assert_eq!(ranges, vec![(200, 250)], "the failed range is skipped");
        drop(repo);

        let next_session = open_repository(&repo_root).unwrap();
        let (_, ranges) = next_session
            .reserve_and_insert_chunk(60, &[0xCC; 20])
            .unwrap();
        assert_eq!(
            ranges,
            vec![(100, 160)],
            "and is free again after a restart"
        );
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
