//! Query/mutation support for the `compact-store` command - see
//! `docs/plans/implemented/compact-store.md`. `compact-store` itself (the
//! relocation loop, reading/writing physical bytes) lives in `cli`; this
//! module only covers the metadata-side pieces: what to move next,
//! recording where it ended up, and bumping [`crate::store_generation`]
//! once the run's done.

use rusqlite::{Connection, OptionalExtension, params};

use crate::Error;

/// Total bytes of live chunk data - the size the store would shrink to if
/// every chunk were packed into one contiguous block starting at 0.
/// Summed from each chunk's own `length`, not derived from `chunk_extents`
/// directly: a chunk's *positions* are exactly what `compact-store` is
/// about to change, but how many bytes it totals never does.
pub fn total_live_bytes(conn: &Connection) -> Result<i64, Error> {
    conn.query_row("SELECT COALESCE(SUM(length), 0) FROM chunks", (), |row| {
        row.get(0)
    })
    .map_err(Error::from)
}

/// The id and length of the chunk currently occupying the highest
/// position at or past `target_size` - the next chunk a `compact-store`
/// run should relocate, always working from the top down so each move
/// strictly shrinks the range still needing work. `None` once every live
/// chunk already fits below `target_size`, i.e. the store is fully
/// packed.
pub fn next_chunk_to_relocate(
    conn: &Connection,
    target_size: i64,
) -> Result<Option<(i64, i64)>, Error> {
    conn.query_row(
        "SELECT c.id, c.length FROM chunk_extents e
         JOIN chunks c ON c.id = e.chunk_id
         WHERE e.stop > ?1
         ORDER BY e.stop DESC LIMIT 1",
        [target_size],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )
    .optional()
    .map_err(Error::from)
}

/// Total bytes across every chunk with at least one extent at or past
/// `target_size` - exactly the chunks a full `compact-store` run will
/// process (see [`next_chunk_to_relocate`]), used to size its progress
/// bar up front.
pub fn bytes_to_relocate(conn: &Connection, target_size: i64) -> Result<i64, Error> {
    conn.query_row(
        "SELECT COALESCE(SUM(c.length), 0) FROM chunks c
         WHERE EXISTS (
             SELECT 1 FROM chunk_extents e WHERE e.chunk_id = c.id AND e.stop > ?1
         )",
        [target_size],
        |row| row.get(0),
    )
    .map_err(Error::from)
}

/// Replaces `chunk_id`'s `chunk_extents` rows with `new_extents` in one
/// transaction - the atomic pointer-switch half of a relocation, matching
/// the "write bytes to an unreferenced location first, commit once
/// second" pattern used throughout this crate (see
/// `docs/plans/implemented/compact-store.md`'s "Crash-safety today"): by
/// the time this runs, the caller has already durably written
/// `new_extents`' bytes to disk at a
/// position nothing yet references, so this commit is what makes the
/// move visible - a kill before it leaves the chunk exactly where it was,
/// a kill after leaves it exactly at its new position; either way a
/// resumed run's next [`next_chunk_to_relocate`] call sees a consistent
/// state to continue from.
pub fn relocate_chunk(
    conn: &mut Connection,
    chunk_id: i64,
    new_extents: &[(u64, u64)],
) -> Result<(), Error> {
    let tx = conn.transaction()?;
    tx.execute("DELETE FROM chunk_extents WHERE chunk_id = ?1", [chunk_id])?;
    for (seq, &(start, stop)) in new_extents.iter().enumerate() {
        tx.execute(
            "INSERT INTO chunk_extents (chunk_id, seq, start, stop) VALUES (?1, ?2, ?3, ?4)",
            params![chunk_id, seq as i64, start as i64, stop as i64],
        )?;
    }
    tx.commit()?;
    Ok(())
}

/// Bumps [`crate::store_generation`] by one - shared by `reclaim_space`
/// (guarded by having actually purged a chunk) and `compact-store`
/// (unconditional on a successful run) rather than duplicating the same
/// one-line `UPDATE` in both places.
pub fn bump_store_generation(conn: &Connection) -> Result<(), Error> {
    conn.execute(
        "UPDATE repository_settings SET store_generation = store_generation + 1",
        (),
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Chunking, RepositorySettings};

    fn test_connection() -> (tempfile::TempDir, Connection) {
        let temp_dir = tempfile::tempdir().unwrap();
        let repo_root = temp_dir.path().join("repo");
        crate::init_repository(
            &repo_root,
            &RepositorySettings::new(20, Chunking::Cdc).unwrap(),
        )
        .unwrap();
        let conn = crate::open_repository(&repo_root)
            .unwrap()
            .open_write_connection()
            .unwrap();
        (temp_dir, conn)
    }

    fn insert_chunk(conn: &Connection, id: i64, length: i64, start: i64, stop: i64) {
        conn.execute(
            "INSERT INTO chunks (id, length, hash) VALUES (?1, ?2, ?3)",
            params![id, length, format!("h{id}").into_bytes()],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO chunk_extents (chunk_id, seq, start, stop) VALUES (?1, 0, ?2, ?3)",
            params![id, start, stop],
        )
        .unwrap();
    }

    #[test]
    fn total_live_bytes_sums_chunk_lengths_not_extent_spans() {
        let (_temp_dir, conn) = test_connection();
        assert_eq!(total_live_bytes(&conn).unwrap(), 0);
        insert_chunk(&conn, 1, 100, 0, 100);
        insert_chunk(&conn, 2, 50, 1000, 1050);
        assert_eq!(total_live_bytes(&conn).unwrap(), 150);
    }

    #[test]
    fn next_chunk_to_relocate_picks_the_highest_stop_above_target_and_stops_once_packed() {
        let (_temp_dir, conn) = test_connection();
        insert_chunk(&conn, 1, 100, 0, 100);
        insert_chunk(&conn, 2, 50, 1000, 1050);
        insert_chunk(&conn, 3, 30, 500, 530);

        assert_eq!(
            next_chunk_to_relocate(&conn, 150).unwrap(),
            Some((2, 50)),
            "highest stop (1050) wins, even though chunk 3 has the higher start"
        );
        assert_eq!(
            next_chunk_to_relocate(&conn, 1050).unwrap(),
            None,
            "nothing left past a target already at the highest stop"
        );
    }

    #[test]
    fn bytes_to_relocate_only_counts_chunks_with_an_extent_past_the_target() {
        let (_temp_dir, conn) = test_connection();
        insert_chunk(&conn, 1, 100, 0, 100);
        insert_chunk(&conn, 2, 50, 1000, 1050);
        assert_eq!(bytes_to_relocate(&conn, 150).unwrap(), 50);
        assert_eq!(bytes_to_relocate(&conn, 2000).unwrap(), 0);
    }

    #[test]
    fn relocate_chunk_replaces_extents_and_can_split_across_several() {
        let (_temp_dir, mut conn) = test_connection();
        insert_chunk(&conn, 1, 100, 1000, 1100);

        relocate_chunk(&mut conn, 1, &[(0, 60), (200, 240)]).unwrap();

        let extents = crate::chunk_extents(&conn, 1).unwrap();
        assert_eq!(extents, vec![(0, 60), (200, 240)]);
    }
}
