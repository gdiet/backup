//! Mutating maintenance operations: soft-deletion (`del`) and, eventually,
//! hard-deletion of old soft-deleted entries plus orphan cleanup
//! (`reclaim-space`).

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
}
