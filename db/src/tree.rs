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

/// A row from `tree_entries`, as returned by [`find_tree_entry`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TreeEntryRow {
    pub id: i64,
    pub kind: EntryKind,
    pub content_id: Option<i64>,
}

/// Looks up the active (non-soft-deleted) child of `parent_id` named `name`.
pub fn find_tree_entry(
    conn: &Connection,
    parent_id: i64,
    name: &str,
) -> Result<Option<TreeEntryRow>, Error> {
    conn.query_row(
        "SELECT id, kind, content_id FROM tree_entries
         WHERE parent_id = ?1 AND name = ?2 AND deleted_at IS NULL",
        params![parent_id, name],
        |row| {
            let kind: String = row.get(1)?;
            Ok(TreeEntryRow {
                id: row.get(0)?,
                kind: EntryKind::from_db_str(&kind),
                content_id: row.get(2)?,
            })
        },
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
}
