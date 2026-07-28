//! Mutating maintenance operations: soft-deletion (`del`) and hard-deletion of
//! old soft-deleted entries plus orphan cleanup (`reclaim-space`).

use rusqlite::{Connection, params};

use crate::Error;

/// Soft-deletes `id` and its entire active subtree (if it's a directory) in
/// one statement, all with the same `deleted_at` timestamp.
///
/// This atomicity is what makes `reclaim-space`'s later hard-delete safe as a
/// single multi-row `DELETE ... WHERE deleted_at <= cutoff` (see its doc
/// comment): a directory and every one of its descendants are always
/// soft-deleted together, so they always share the same `deleted_at` and are
/// never left in a partially-deleted state - unlike a naive per-row,
/// bottom-up recursive delete (no transaction spanning the whole subtree),
/// which a killed process could leave half-done.
///
/// Returns the number of entries marked deleted (`0` if `id` was already
/// deleted, doesn't exist, or - the `t.id != t.parent_id` guard in the
/// recursive step - `id` is the repository root, which is its own parent and
/// would otherwise make the walk recurse into itself forever; callers should
/// reject deleting the root before calling this, this guard is a second line
/// of defense, not the primary check).
pub fn soft_delete(conn: &Connection, id: i64, deleted_at: i64) -> Result<usize, Error> {
    let count = conn.execute(
        "UPDATE tree_entries SET deleted_at = ?1
         WHERE deleted_at IS NULL AND id IN (
             WITH RECURSIVE subtree(id) AS (
                 SELECT ?2
                 UNION ALL
                 SELECT t.id FROM tree_entries t JOIN subtree s ON t.parent_id = s.id
                 WHERE t.id != t.parent_id
             )
             SELECT id FROM subtree
         )",
        params![deleted_at, id],
    )?;
    Ok(count)
}

/// Counts of rows actually removed by [`reclaim_space`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ReclaimStats {
    pub tree_entries_purged: usize,
    pub contents_purged: usize,
    pub chunks_purged: usize,
}

/// Hard-deletes soft-deleted `tree_entries` rows with `deleted_at <=
/// cutoff_millis`, then sweeps `contents`/`chunks` rows that are now (or
/// already were) unreferenced. All in one transaction, so this is all-or-
/// nothing - either the whole reclaim succeeds or the database is left
/// exactly as it was.
///
/// The first statement is a single, plain multi-row `DELETE` - not a
/// leaf-first loop, and no Scala-style iterative "unrooting" repair pass for
/// partially-deleted subtrees. That's safe for two reasons specific to this
/// schema: [`soft_delete`] always marks an entire subtree with one shared
/// timestamp (so any directory selected by the cutoff has every one of its
/// descendants selected too, never a mix), and SQLite checks non-deferred
/// foreign keys once at the end of the whole statement rather than per
/// intermediate row - so a `DELETE` removing a parent and its children
/// together in the same statement never trips the `parent_id` foreign key
/// against a row also being removed by that statement.
///
/// The second and third statements are exactly the two-line cleanup already
/// described in `migrations.rs`'s doc comment, now finally exercised: a
/// content or chunk only reaches `ref_count = 0` once nothing live (or
/// soft-deleted-but-within-the-grace-period) references it any more, which
/// this first statement is what actually brings about for anything that was
/// only kept alive by an old soft-deleted entry.
///
/// Does not reclaim physical byte-store space (`store::LongTermStore` has no
/// delete/truncate operation) - only database rows.
pub fn reclaim_space(conn: &mut Connection, cutoff_millis: i64) -> Result<ReclaimStats, Error> {
    let tx = conn.transaction()?;
    let tree_entries_purged = tx.execute(
        "DELETE FROM tree_entries WHERE deleted_at IS NOT NULL AND deleted_at <= ?1",
        [cutoff_millis],
    )?;
    let contents_purged = tx.execute("DELETE FROM contents WHERE ref_count = 0", ())?;
    let chunks_purged = tx.execute("DELETE FROM chunks WHERE ref_count = 0", ())?;
    tx.commit()?;
    Ok(ReclaimStats {
        tree_entries_purged,
        contents_purged,
        chunks_purged,
    })
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

    fn is_deleted(conn: &Connection, id: i64) -> bool {
        conn.query_row(
            "SELECT deleted_at IS NOT NULL FROM tree_entries WHERE id = ?1",
            [id],
            |row| row.get(0),
        )
        .unwrap()
    }

    #[test]
    fn soft_deletes_a_single_file() {
        let (_temp_dir, conn) = test_connection();
        conn.execute(
            "INSERT INTO tree_entries (id, parent_id, name, time, kind) VALUES (1, 0, 'a.txt', 0, 'file')",
            (),
        )
        .unwrap();

        let count = soft_delete(&conn, 1, 1000).unwrap();

        assert_eq!(count, 1);
        assert!(is_deleted(&conn, 1));
    }

    #[test]
    fn soft_deletes_an_entire_subtree_atomically() {
        let (_temp_dir, conn) = test_connection();
        let sub_id = crate::insert_directory(&conn, 0, "sub", 0).unwrap();
        let nested_id = crate::insert_directory(&conn, sub_id, "nested", 0).unwrap();
        conn.execute(
            "INSERT INTO tree_entries (parent_id, name, time, kind) VALUES (?1, 'a.txt', 0, 'file')",
            [nested_id],
        )
        .unwrap();
        let file_id = conn.last_insert_rowid();

        let count = soft_delete(&conn, sub_id, 1000).unwrap();

        assert_eq!(count, 3, "sub, nested, and a.txt");
        assert!(is_deleted(&conn, sub_id));
        assert!(is_deleted(&conn, nested_id));
        assert!(is_deleted(&conn, file_id));
    }

    #[test]
    fn does_not_touch_already_deleted_entries_or_a_missing_id() {
        let (_temp_dir, conn) = test_connection();
        conn.execute(
            "INSERT INTO tree_entries (id, parent_id, name, time, kind) VALUES (1, 0, 'a.txt', 0, 'file')",
            (),
        )
        .unwrap();
        assert_eq!(soft_delete(&conn, 1, 1000).unwrap(), 1);

        assert_eq!(soft_delete(&conn, 1, 2000).unwrap(), 0, "already deleted");
        assert_eq!(soft_delete(&conn, 999, 2000).unwrap(), 0, "no such id");
    }

    #[test]
    fn does_not_decrement_content_ref_count() {
        // Soft-deletion must not release content: it's still needed to keep
        // the entry recoverable, and reclaim-space is what actually frees it
        // later via a real DELETE (which the ref_count triggers do react to).
        let (_temp_dir, conn) = test_connection();
        conn.execute(
            "INSERT INTO contents (id, length, hash) VALUES (1, 5, x'AA')",
            (),
        )
        .unwrap();
        conn.execute(
            "INSERT INTO tree_entries (id, parent_id, name, time, kind, content_id) VALUES (1, 0, 'a.txt', 0, 'file', 1)",
            (),
        )
        .unwrap();

        soft_delete(&conn, 1, 1000).unwrap();

        let ref_count: i64 = conn
            .query_row("SELECT ref_count FROM contents WHERE id = 1", (), |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(ref_count, 1);
    }

    #[test]
    fn reclaim_space_purges_old_soft_deleted_entries_and_the_orphaned_content_and_chunk() {
        let (_temp_dir, mut conn) = test_connection();
        conn.execute(
            "INSERT INTO chunks (id, length, hash) VALUES (1, 5, x'AA')",
            (),
        )
        .unwrap();
        conn.execute(
            "INSERT INTO contents (id, length, hash) VALUES (1, 5, x'BB')",
            (),
        )
        .unwrap();
        conn.execute(
            "INSERT INTO content_chunks (content_id, seq, chunk_id) VALUES (1, 0, 1)",
            (),
        )
        .unwrap();
        conn.execute(
            "INSERT INTO tree_entries (id, parent_id, name, time, kind, content_id, deleted_at)
             VALUES (1, 0, 'a.txt', 0, 'file', 1, 1000)",
            (),
        )
        .unwrap();

        let stats = reclaim_space(&mut conn, 1000).unwrap();

        assert_eq!(
            stats,
            ReclaimStats {
                tree_entries_purged: 1,
                contents_purged: 1,
                chunks_purged: 1,
            }
        );
        let remaining: i64 = conn
            .query_row("SELECT COUNT(*) FROM tree_entries", (), |row| row.get(0))
            .unwrap();
        assert_eq!(remaining, 1, "only the root remains");
    }

    #[test]
    fn reclaim_space_preserves_entries_within_the_keep_window() {
        let (_temp_dir, mut conn) = test_connection();
        conn.execute(
            "INSERT INTO tree_entries (id, parent_id, name, time, kind, deleted_at)
             VALUES (1, 0, 'a.txt', 0, 'file', 5000)",
            (),
        )
        .unwrap();

        let stats = reclaim_space(&mut conn, 1000).unwrap();

        assert_eq!(
            stats.tree_entries_purged, 0,
            "deleted_at is after the cutoff"
        );
        assert!(is_deleted(&conn, 1));
    }

    #[test]
    fn reclaim_space_removes_a_whole_soft_deleted_subtree_in_one_statement() {
        let (_temp_dir, mut conn) = test_connection();
        let sub_id = crate::insert_directory(&conn, 0, "sub", 0).unwrap();
        let nested_id = crate::insert_directory(&conn, sub_id, "nested", 0).unwrap();
        conn.execute(
            "INSERT INTO tree_entries (parent_id, name, time, kind) VALUES (?1, 'a.txt', 0, 'file')",
            [nested_id],
        )
        .unwrap();
        let file_id = conn.last_insert_rowid();
        // soft_delete marks the whole subtree with one shared timestamp - the
        // exact invariant that makes the single multi-row DELETE below safe
        // against the parent_id foreign key (see reclaim_space's doc comment).
        soft_delete(&conn, sub_id, 1000).unwrap();

        let stats = reclaim_space(&mut conn, 1000).unwrap();

        assert_eq!(stats.tree_entries_purged, 3, "sub, nested, and a.txt");
        for id in [sub_id, nested_id, file_id] {
            assert_eq!(
                conn.query_row(
                    "SELECT COUNT(*) FROM tree_entries WHERE id = ?1",
                    [id],
                    |row| row.get::<_, i64>(0)
                )
                .unwrap(),
                0
            );
        }
    }
}
