//! Free-byte-range allocation (DESIGN-STORE-003 in `docs/design/byte-store.md`) - answers "which
//! byte range is free to write new content into" derived entirely from `chunk_extents`, which
//! already lives in this database, so this module carries no state of its own.
//!
//! `pub(crate)` only, reached exclusively through [`crate::content`] (DESIGN-METADATA-006).

use rusqlite::Connection;

use crate::Error;

/// Finds `length` bytes of free space and returns the `(start, stop)` ranges that together cover
/// exactly `length` bytes, in order - more than one only if no single gap between existing
/// `chunk_extents` rows was large enough on its own, in which case the remainder extends past the
/// current high-water mark. Existing gaps (from reclaimed content) are filled before extending
/// past that mark.
///
/// Scans every `chunk_extents` row on every call; free space is not cached or materialized
/// anywhere. `chunk_extents` positions are always disjoint (every range still recorded there was
/// itself reserved through this same function, and reclaiming removes a chunk's rows entirely
/// rather than shrinking them), so a single ordered pass is sufficient to find every gap.
pub(crate) fn reserve(conn: &Connection, length: u64) -> Result<Vec<(u64, u64)>, Error> {
    let mut remaining = length;
    let mut ranges = Vec::new();
    let mut cursor: u64 = 0;

    let mut stmt = conn.prepare("SELECT start, stop FROM chunk_extents ORDER BY start")?;
    let extents = stmt
        .query_map((), |row| {
            let start: i64 = row.get(0)?;
            let stop: i64 = row.get(1)?;
            Ok((start as u64, stop as u64))
        })?
        .collect::<Result<Vec<_>, _>>()?;

    for (start, stop) in extents {
        if remaining == 0 {
            break;
        }
        if start > cursor {
            let take = (start - cursor).min(remaining);
            ranges.push((cursor, cursor + take));
            remaining -= take;
        }
        cursor = stop;
    }
    if remaining > 0 {
        ranges.push((cursor, cursor + remaining));
    }
    Ok(ranges)
}

#[cfg(test)]
mod tests {
    use crate::{RepositorySettings, init_repository, open_repository};

    fn repo() -> (crate::Repository, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let repo_root = dir.path().join("repo");
        let settings = RepositorySettings::new(Some(20), 1_700_000_000_000);
        init_repository(&repo_root, settings).expect("init must succeed");
        let repo = open_repository(&repo_root).expect("open must succeed");
        (repo, dir)
    }

    fn insert_extent(repo: &crate::Repository, chunk_id: i64, start: i64, stop: i64) {
        repo.with_connection(|conn| {
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

    #[test]
    fn reserve_starts_at_zero_in_an_empty_store() {
        let (repo, _dir) = repo();
        let ranges = repo
            .with_connection(|conn| super::reserve(conn, 100))
            .unwrap();
        assert_eq!(ranges, vec![(0, 100)]);
    }

    #[test]
    fn reserve_extends_past_the_high_water_mark() {
        let (repo, _dir) = repo();
        insert_extent(&repo, 1, 0, 100);
        let ranges = repo
            .with_connection(|conn| super::reserve(conn, 50))
            .unwrap();
        assert_eq!(ranges, vec![(100, 150)]);
    }

    #[test]
    fn reserve_fills_a_gap_between_two_extents_before_extending() {
        let (repo, _dir) = repo();
        insert_extent(&repo, 1, 0, 100);
        insert_extent(&repo, 2, 130, 200);
        // Gap is exactly 30 bytes (100..130) - a 30-byte request fits inside it alone.
        let ranges = repo
            .with_connection(|conn| super::reserve(conn, 30))
            .unwrap();
        assert_eq!(ranges, vec![(100, 130)]);
    }

    #[test]
    fn reserve_spans_a_gap_and_the_high_water_mark_when_the_gap_is_too_small() {
        let (repo, _dir) = repo();
        insert_extent(&repo, 1, 0, 100);
        insert_extent(&repo, 2, 110, 200);
        // Gap is only 10 bytes (100..110); the remaining 40 bytes extend past 200.
        let ranges = repo
            .with_connection(|conn| super::reserve(conn, 50))
            .unwrap();
        assert_eq!(ranges, vec![(100, 110), (200, 240)]);
    }

    #[test]
    fn reserve_of_zero_length_returns_no_ranges() {
        let (repo, _dir) = repo();
        let ranges = repo
            .with_connection(|conn| super::reserve(conn, 0))
            .unwrap();
        assert_eq!(ranges, Vec::<(u64, u64)>::new());
    }
}
