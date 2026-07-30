use rusqlite::{Connection, OptionalExtension, params};

use crate::Error;

/// The kind of a `tree_entries` row - see the schema doc comment in `migrations.rs`
/// for why this is needed (an empty file and a directory are otherwise
/// indistinguishable: both have `content_id IS NULL`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryKind {
    Dir,
    File,
}

impl EntryKind {
    fn from_db_str(s: &str) -> Self {
        match s {
            "dir" => EntryKind::Dir,
            "file" => EntryKind::File,
            other => unreachable!("tree_entries.kind CHECK constraint violated: {other:?}"),
        }
    }
}

/// A row from `tree_entries`, as returned by [`find_tree_entry`]/[`get_tree_entry`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeEntryRow {
    pub id: i64,
    pub name: String,
    pub time_millis: i64,
    pub kind: EntryKind,
    pub content_id: Option<i64>,
}

/// Reads a `TreeEntryRow` from a query result whose `(id, name, time, kind,
/// content_id)` columns start at `offset` - `0` for a plain
/// `SELECT id, name, time, kind, content_id FROM tree_entries ...`, or a higher
/// offset when those columns follow others selected earlier (as `query.rs`'s
/// subtree walk does, with a `path` column first).
pub(crate) fn row_to_tree_entry_at(
    row: &rusqlite::Row,
    offset: usize,
) -> rusqlite::Result<TreeEntryRow> {
    let kind: String = row.get(offset + 3)?;
    Ok(TreeEntryRow {
        id: row.get(offset)?,
        name: row.get(offset + 1)?,
        time_millis: row.get(offset + 2)?,
        kind: EntryKind::from_db_str(&kind),
        content_id: row.get(offset + 4)?,
    })
}

pub(crate) fn row_to_tree_entry(row: &rusqlite::Row) -> rusqlite::Result<TreeEntryRow> {
    row_to_tree_entry_at(row, 0)
}

const SELECT_TREE_ENTRY: &str = "SELECT id, name, time, kind, content_id FROM tree_entries";

/// Looks up a tree entry by id, regardless of whether it's soft-deleted -
/// unlike [`find_tree_entry`], which only ever finds active entries by name.
pub fn get_tree_entry(conn: &Connection, id: i64) -> Result<Option<TreeEntryRow>, Error> {
    conn.query_row(
        &format!("{SELECT_TREE_ENTRY} WHERE id = ?1"),
        [id],
        row_to_tree_entry,
    )
    .optional()
    .map_err(Error::from)
}

/// Looks up `id`'s `parent_id`, regardless of whether it's soft-deleted -
/// `TreeEntryRow` itself doesn't carry this (see its own doc comment), so
/// this is the standalone way to get it (used by the mount's phase 2b
/// persist pipeline, which needs it to build a `FileBackupRecord`).
pub fn parent_id(conn: &Connection, id: i64) -> Result<Option<i64>, Error> {
    conn.query_row(
        "SELECT parent_id FROM tree_entries WHERE id = ?1",
        [id],
        |row| row.get(0),
    )
    .optional()
    .map_err(Error::from)
}

/// Looks up the active (non-soft-deleted) child of `parent_id` named `name`.
pub fn find_tree_entry(
    conn: &Connection,
    parent_id: i64,
    name: &str,
) -> Result<Option<TreeEntryRow>, Error> {
    conn.query_row(
        &format!("{SELECT_TREE_ENTRY} WHERE parent_id = ?1 AND name = ?2 AND deleted_at IS NULL"),
        params![parent_id, name],
        row_to_tree_entry,
    )
    .optional()
    .map_err(Error::from)
}

/// Resolves the id of the directory named `name` under `parent_id`, creating it
/// (with `time` set to `time_millis`) if it doesn't already exist.
///
/// Idempotent: calling this again for an already-existing directory just returns
/// its id, `time` is left unchanged. Fails with [`Error::NotADirectory`] if an
/// entry of that name already exists but is a file, not a directory.
pub fn insert_directory(
    conn: &Connection,
    parent_id: i64,
    name: &str,
    time_millis: i64,
) -> Result<i64, Error> {
    conn.execute(
        "INSERT INTO tree_entries (parent_id, name, time, kind) VALUES (?1, ?2, ?3, 'dir')
         ON CONFLICT (parent_id, name) WHERE deleted_at IS NULL DO NOTHING",
        params![parent_id, name, time_millis],
    )?;

    let entry = find_tree_entry(conn, parent_id, name)?
        .expect("just inserted, or already present: the row must exist now");
    if entry.kind != EntryKind::Dir {
        return Err(Error::NotADirectory {
            parent_id,
            name: name.to_string(),
        });
    }
    Ok(entry.id)
}

/// Updates `id`'s `time` in place. Used by the mount's `utimens` - not
/// needed by `store`, which always inserts a fresh row or goes through
/// [`crate::apply_backup_batch`]'s own unchanged-content branch instead.
pub fn touch_mtime(conn: &Connection, id: i64, time_millis: i64) -> Result<(), Error> {
    conn.execute(
        "UPDATE tree_entries SET time = ?1 WHERE id = ?2 AND deleted_at IS NULL",
        params![time_millis, id],
    )?;
    Ok(())
}

/// Moves `id` to a new `(parent_id, name)`. Fails with [`Error::AlreadyExists`]
/// if an active entry already occupies the destination - no overwrite support
/// (see `docs/plans/fuse-mount-readwrite.md` for why this is a deliberate,
/// documented limitation rather than an oversight).
pub fn rename_entry(
    conn: &Connection,
    id: i64,
    new_parent_id: i64,
    new_name: &str,
) -> Result<(), Error> {
    if find_tree_entry(conn, new_parent_id, new_name)?.is_some() {
        return Err(Error::AlreadyExists {
            parent_id: new_parent_id,
            name: new_name.to_string(),
        });
    }
    conn.execute(
        "UPDATE tree_entries SET parent_id = ?1, name = ?2 WHERE id = ?3 AND deleted_at IS NULL",
        params![new_parent_id, new_name, id],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Chunking, RepositorySettings};

    // Returns the TempDir alongside the Connection so the caller keeps it alive:
    // WAL mode needs to create sidecar `-wal`/`-shm` files next to the database
    // file for as long as the connection is used, which requires the directory to
    // still exist.
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

    #[test]
    fn find_tree_entry_returns_none_for_a_missing_entry() {
        let (_temp_dir, conn) = test_connection();
        assert_eq!(find_tree_entry(&conn, 0, "missing").unwrap(), None);
    }

    #[test]
    fn insert_directory_creates_and_is_idempotent() {
        let (_temp_dir, conn) = test_connection();

        let id = insert_directory(&conn, 0, "sub", 1000).unwrap();
        let id_again = insert_directory(&conn, 0, "sub", 2000).unwrap();

        assert_eq!(id, id_again);
        let entry = find_tree_entry(&conn, 0, "sub").unwrap().unwrap();
        assert_eq!(entry.id, id);
        assert_eq!(entry.kind, EntryKind::Dir);
        assert_eq!(entry.content_id, None);
    }

    #[test]
    fn insert_directory_rejects_an_existing_file_of_the_same_name() {
        let (_temp_dir, conn) = test_connection();
        conn.execute(
            "INSERT INTO tree_entries (parent_id, name, time, kind) VALUES (0, 'sub', 0, 'file')",
            (),
        )
        .unwrap();

        let result = insert_directory(&conn, 0, "sub", 1000);

        assert!(
            matches!(result, Err(Error::NotADirectory { parent_id: 0, name }) if name == "sub")
        );
    }

    #[test]
    fn touch_mtime_updates_time_in_place() {
        let (_temp_dir, conn) = test_connection();
        let id = insert_directory(&conn, 0, "sub", 1000).unwrap();

        touch_mtime(&conn, id, 2000).unwrap();

        let entry = find_tree_entry(&conn, 0, "sub").unwrap().unwrap();
        assert_eq!(entry.time_millis, 2000);
    }

    #[test]
    fn rename_entry_moves_to_a_new_parent_and_name() {
        let (_temp_dir, conn) = test_connection();
        let src = insert_directory(&conn, 0, "src", 0).unwrap();
        let dst = insert_directory(&conn, 0, "dst", 0).unwrap();
        let id = insert_directory(&conn, src, "child", 0).unwrap();

        rename_entry(&conn, id, dst, "renamed").unwrap();

        assert_eq!(find_tree_entry(&conn, src, "child").unwrap(), None);
        let entry = find_tree_entry(&conn, dst, "renamed").unwrap().unwrap();
        assert_eq!(entry.id, id);
    }

    #[test]
    fn rename_entry_rejects_an_existing_target() {
        let (_temp_dir, conn) = test_connection();
        let id = insert_directory(&conn, 0, "a", 0).unwrap();
        insert_directory(&conn, 0, "b", 0).unwrap();

        let result = rename_entry(&conn, id, 0, "b");

        assert!(matches!(result, Err(Error::AlreadyExists { parent_id: 0, name }) if name == "b"));
    }
}
