//! Mutating maintenance operations: soft-deletion (`del`) and hard-deletion of
//! old soft-deleted entries plus orphan cleanup (`reclaim-space`).

use rusqlite::{Connection, OptionalExtension, params};

use crate::Error;
use crate::tree::find_tree_entry;

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

/// Soft-deletes `id` (via [`soft_delete`]) and, in the same transaction,
/// inserts a fresh zero-byte file at `(parent_id, name)` with `content_id
/// NULL` - the atomic form of `fix-problems --replace-empty`'s two-step
/// "soft-delete, then re-insert" behavior.
///
/// Wrapping both statements in one transaction closes a crash-safety gap: a
/// process killed between two separate top-level calls would leave `id`
/// soft-deleted with no replacement ever inserted. Because a soft-deleted
/// entry is no longer active, a later re-run of `fix-problems` would never
/// find it again to retry - the placeholder would simply be gone for good,
/// not just delayed. Committing them together means either both happen or
/// neither does, so a re-run always sees a consistent, retryable state.
///
/// `id` is expected to be a plain file (this exists for `fix-problems`,
/// which only ever calls it for entries `problems` already found to be
/// files) - unlike [`soft_delete`], this does not need to consider `id`
/// being a directory with descendants.
pub fn soft_delete_and_replace_with_empty(
    conn: &mut Connection,
    id: i64,
    deleted_at_millis: i64,
    parent_id: i64,
    name: &str,
    time_millis: i64,
) -> Result<(), Error> {
    let tx = conn.transaction()?;
    soft_delete(&tx, id, deleted_at_millis)?;
    tx.execute(
        "INSERT INTO tree_entries (parent_id, name, time, kind) VALUES (?1, ?2, ?3, 'file')",
        params![parent_id, name, time_millis],
    )?;
    tx.commit()?;
    Ok(())
}

/// Reactivates `id` (clearing `deleted_at`), and - if `recursive` - every
/// descendant that still carries the exact same `deleted_at` timestamp `id`
/// itself had, i.e. exactly the set [`soft_delete`] originally marked
/// together in one operation, mirrored in reverse (see its own doc
/// comment). A descendant deleted independently, at a different time, is
/// left alone: its `deleted_at` won't match, so the query below simply
/// never selects it.
///
/// `relocate_to`, if given, reactivates `id` at a *different*
/// `(parent_id, name)` instead of its original one - only `id` itself
/// moves; any reactivated descendants already reference its `id` as their
/// own `parent_id`, unaffected by this. Either way (original location or
/// `relocate_to`), fails with [`Error::AlreadyExists`] if an active entry
/// already occupies the target - no silent auto-rename, the same
/// uniqueness conflict [`crate::rename_entry`] already guards against.
///
/// Returns the number of rows reactivated - `0` if `id` doesn't exist or
/// isn't currently deleted (callers that want a more specific error for
/// that case, e.g. `backup undelete`'s CLI layer, should check first via
/// [`crate::get_tree_entry`], the same way `del`'s CLI layer already
/// pre-checks before calling [`soft_delete`] rather than pushing CLI-
/// friendly messages into this crate).
///
/// Content/`ref_count` need no special handling here: a soft-deleted entry
/// already keeps holding its content's `ref_count` contribution (see
/// `migrations.rs`'s schema doc comment - only an actual `DELETE` releases
/// it), and the `tree_entries_ref_count_*` triggers only fire on
/// `INSERT`/`DELETE` of `tree_entries` rows, never `UPDATE` - so flipping
/// `deleted_at` back to `NULL` is already ref-count-neutral.
///
/// Runs the relocate and reactivate steps (when both apply) in one
/// transaction: a process killed between two separate top-level statements
/// would leave the entry relocated but still deleted - recoverable via a
/// second `undelete` call, but not atomic, and briefly visible in that
/// half-done state to any concurrent reader.
pub fn undelete(
    conn: &mut Connection,
    id: i64,
    recursive: bool,
    relocate_to: Option<(i64, &str)>,
) -> Result<usize, Error> {
    let tx = conn.transaction()?;
    let current: Option<(i64, String, Option<i64>)> = tx
        .query_row(
            "SELECT parent_id, name, deleted_at FROM tree_entries WHERE id = ?1",
            [id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    let Some((current_parent_id, current_name, deleted_at)) = current else {
        return Ok(0);
    };
    if deleted_at.is_none() {
        return Ok(0);
    }

    let (target_parent_id, target_name) = match relocate_to {
        Some((parent_id, name)) => (parent_id, name.to_string()),
        None => (current_parent_id, current_name),
    };
    if find_tree_entry(&tx, target_parent_id, &target_name)?.is_some() {
        return Err(Error::AlreadyExists {
            parent_id: target_parent_id,
            name: target_name,
        });
    }

    // Relocate *before* clearing deleted_at, not after: while a row is still
    // deleted it's exempt from the partial unique index (`WHERE deleted_at
    // IS NULL`), so changing its parent_id/name here can never conflict -
    // clearing deleted_at afterwards is what actually re-checks uniqueness,
    // now against the already-relocated (and already conflict-checked
    // above) target. Doing this in the other order would briefly reactivate
    // the row at its *original* location, which can easily still be taken
    // (as in this test's own setup: something else may well have reused the
    // name since this entry was deleted) and fail there instead.
    if relocate_to.is_some() {
        tx.execute(
            "UPDATE tree_entries SET parent_id = ?1, name = ?2 WHERE id = ?3",
            params![target_parent_id, target_name, id],
        )?;
    }

    let count = if recursive {
        tx.execute(
            "UPDATE tree_entries SET deleted_at = NULL
             WHERE deleted_at = (SELECT deleted_at FROM tree_entries WHERE id = ?1)
             AND id IN (
                 WITH RECURSIVE subtree(id) AS (
                     SELECT ?1
                     UNION ALL
                     SELECT t.id FROM tree_entries t JOIN subtree s ON t.parent_id = s.id
                     WHERE t.id != t.parent_id
                 )
                 SELECT id FROM subtree
             )",
            [id],
        )?
    } else {
        tx.execute(
            "UPDATE tree_entries SET deleted_at = NULL WHERE id = ?1 AND deleted_at IS NOT NULL",
            [id],
        )?
    };

    tx.commit()?;
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

    fn insert_dir(conn: &Connection, parent_id: i64, name: &str) -> i64 {
        crate::insert_directory(conn, parent_id, name, 0).unwrap()
    }

    fn insert_file(conn: &Connection, parent_id: i64, name: &str) -> i64 {
        conn.execute(
            "INSERT INTO tree_entries (parent_id, name, time, kind) VALUES (?1, ?2, 0, 'file')",
            params![parent_id, name],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    #[test]
    fn undelete_reactivates_a_single_entry() {
        let (_temp_dir, mut conn) = test_connection();
        let id = insert_file(&conn, 0, "a.txt");
        soft_delete(&conn, id, 1000).unwrap();

        let count = undelete(&mut conn, id, false, None).unwrap();

        assert_eq!(count, 1);
        assert!(!is_deleted(&conn, id));
    }

    #[test]
    fn undelete_returns_zero_for_a_missing_or_already_active_id() {
        let (_temp_dir, mut conn) = test_connection();
        let active_id = insert_file(&conn, 0, "a.txt");

        assert_eq!(
            undelete(&mut conn, 999, false, None).unwrap(),
            0,
            "no such id"
        );
        assert_eq!(
            undelete(&mut conn, active_id, false, None).unwrap(),
            0,
            "not currently deleted"
        );
    }

    #[test]
    fn undelete_without_recursive_leaves_descendants_deleted() {
        let (_temp_dir, mut conn) = test_connection();
        let dir_id = insert_dir(&conn, 0, "sub");
        let file_id = insert_file(&conn, dir_id, "a.txt");
        soft_delete(&conn, dir_id, 1000).unwrap();

        let count = undelete(&mut conn, dir_id, false, None).unwrap();

        assert_eq!(count, 1);
        assert!(!is_deleted(&conn, dir_id));
        assert!(
            is_deleted(&conn, file_id),
            "not reactivated without --recursive"
        );
    }

    #[test]
    fn undelete_recursive_reactivates_descendants_sharing_the_same_deleted_at() {
        let (_temp_dir, mut conn) = test_connection();
        let dir_id = insert_dir(&conn, 0, "sub");
        let nested_id = insert_dir(&conn, dir_id, "nested");
        let file_id = insert_file(&conn, nested_id, "a.txt");
        soft_delete(&conn, dir_id, 1000).unwrap();

        let count = undelete(&mut conn, dir_id, true, None).unwrap();

        assert_eq!(count, 3, "sub, nested, and a.txt");
        assert!(!is_deleted(&conn, dir_id));
        assert!(!is_deleted(&conn, nested_id));
        assert!(!is_deleted(&conn, file_id));
    }

    #[test]
    fn undelete_recursive_leaves_an_independently_deleted_descendant_alone() {
        let (_temp_dir, mut conn) = test_connection();
        let dir_id = insert_dir(&conn, 0, "sub");
        let file_id = insert_file(&conn, dir_id, "a.txt");
        // Deleted separately, at a different time, from the directory itself.
        soft_delete(&conn, file_id, 500).unwrap();
        soft_delete(&conn, dir_id, 1000).unwrap();

        let count = undelete(&mut conn, dir_id, true, None).unwrap();

        assert_eq!(count, 1, "only 'sub' shares deleted_at 1000");
        assert!(!is_deleted(&conn, dir_id));
        assert!(
            is_deleted(&conn, file_id),
            "deleted_at 500 != 1000, left alone"
        );
    }

    #[test]
    fn undelete_refuses_a_conflict_with_an_active_entry() {
        let (_temp_dir, mut conn) = test_connection();
        let id = insert_file(&conn, 0, "a.txt");
        soft_delete(&conn, id, 1000).unwrap();
        insert_file(&conn, 0, "a.txt"); // a new active entry re-occupies the name

        let result = undelete(&mut conn, id, false, None);

        assert!(matches!(
            result,
            Err(Error::AlreadyExists { parent_id: 0, name }) if name == "a.txt"
        ));
        assert!(is_deleted(&conn, id), "left untouched on conflict");
    }

    #[test]
    fn undelete_can_relocate_to_a_different_name_to_avoid_a_conflict() {
        let (_temp_dir, mut conn) = test_connection();
        let id = insert_file(&conn, 0, "a.txt");
        soft_delete(&conn, id, 1000).unwrap();
        insert_file(&conn, 0, "a.txt");

        let count = undelete(&mut conn, id, false, Some((0, "a-recovered.txt"))).unwrap();

        assert_eq!(count, 1);
        assert!(!is_deleted(&conn, id));
        let (parent_id, name): (i64, String) = conn
            .query_row(
                "SELECT parent_id, name FROM tree_entries WHERE id = ?1",
                [id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!((parent_id, name.as_str()), (0, "a-recovered.txt"));
    }

    #[test]
    fn undelete_does_not_change_content_ref_count() {
        let (_temp_dir, mut conn) = test_connection();
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

        undelete(&mut conn, 1, false, None).unwrap();

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
