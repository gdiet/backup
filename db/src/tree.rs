use rusqlite::{Connection, OptionalExtension, params};

use crate::Error;

/// The kind of a `tree_entries` row - see the schema doc comment in
/// `migrations.rs` for why this is needed (a directory and a file with no
/// content decided yet - a still-open mount `create()` placeholder, see
/// `crate::EMPTY_CONTENT_ID`'s own doc comment - are otherwise
/// indistinguishable: both have `content_id IS NULL`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryKind {
    Dir,
    File,
}

impl EntryKind {
    /// `pub(crate)`: needed by [`crate::undelete`], which (like
    /// [`row_to_tree_entry_at`]) parses a hand-written `kind` column read
    /// alongside other columns `TreeEntryRow` doesn't carry (`parent_id`/
    /// `deleted_at`), rather than through [`get_tree_entry`]/
    /// [`find_tree_entry`].
    pub(crate) fn from_db_str(s: &str) -> Self {
        match s {
            "dir" => EntryKind::Dir,
            "file" => EntryKind::File,
            other => unreachable!("tree_entries.kind CHECK constraint violated: {other:?}"),
        }
    }

    /// The `tree_entries.kind` column value for this variant - the inverse of
    /// [`EntryKind::from_db_str`]. `pub(crate)`: needed by
    /// [`insert_historical_tree_entry`], which (unlike [`apply_backup_batch`]
    /// and [`insert_directory`]) inserts either kind through one shared
    /// function rather than a hardcoded literal per call site.
    ///
    /// [`apply_backup_batch`]: crate::apply_backup_batch
    pub(crate) fn as_db_str(self) -> &'static str {
        match self {
            EntryKind::Dir => "dir",
            EntryKind::File => "file",
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

/// Whether `id` exists and, if so, whether it's currently soft-deleted -
/// `None` if `id` doesn't exist at all. Like [`parent_id`], a standalone
/// lookup for the one bit `TreeEntryRow` doesn't carry - used by `backup
/// undelete`'s CLI layer to distinguish "no such id" from "exists but isn't
/// deleted" before calling [`crate::undelete`].
pub fn is_deleted(conn: &Connection, id: i64) -> Result<Option<bool>, Error> {
    conn.query_row(
        "SELECT deleted_at IS NOT NULL FROM tree_entries WHERE id = ?1",
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

/// Inserts a tree entry with an explicit `deleted_at`, bypassing the
/// active-name conflict/replace handling that [`crate::apply_backup_batch`]
/// and [`insert_directory`] apply for an *incremental* backup run. Used by
/// the Scala repository migration tool, which replays a full historical tree
/// (including already soft-deleted entries) rather than folding updates into
/// a single current state: each old entry becomes exactly one new row,
/// carrying whatever `deleted_at` it already had in the old repository.
///
/// Multiple historical rows may share a `(parent_id, name)` (the norm for a
/// repeatedly overwritten path); at most one may be active (`deleted_at ==
/// None`) at a time, enforced the same way as everywhere else by the
/// schema's partial unique index - inserting a second active row for the
/// same `(parent_id, name)` fails with a uniqueness violation, surfaced as
/// [`Error::Sqlite`].
///
/// Returns the new row's id, for the caller to use as `parent_id` when
/// recursing into a migrated directory's own children.
///
/// [`Error::Sqlite`]: crate::Error::Sqlite
pub fn insert_historical_tree_entry(
    conn: &Connection,
    parent_id: i64,
    name: &str,
    time_millis: i64,
    deleted_at: Option<i64>,
    kind: EntryKind,
    content_id: Option<i64>,
) -> Result<i64, Error> {
    conn.execute(
        "INSERT INTO tree_entries (parent_id, name, time, deleted_at, kind, content_id)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            parent_id,
            name,
            time_millis,
            deleted_at,
            kind.as_db_str(),
            content_id
        ],
    )?;
    Ok(conn.last_insert_rowid())
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

/// Settles `id`'s `content_id` to [`crate::EMPTY_CONTENT_ID`] if it's still
/// `NULL` - a no-op otherwise (including for an entry that's already settled
/// as the shared empty content). Used by the mount's `release`, for exactly
/// one case: a `create()`'d file closed without ever being written to. No
/// persist ever runs for that file (nothing was written, so `dirty` stays
/// `false` - see `Inner::release`), which would otherwise leave `content_id
/// IS NULL` as this row's *permanent* state - breaking the very invariant
/// `EMPTY_CONTENT_ID` exists to establish (that `NULL` means "still open",
/// never "settled"). A bare `touch` through the mount is exactly this case,
/// and real POSIX `touch` is expected to just work.
///
/// Deliberately an `UPDATE` in place, not the usual soft-delete-and-reinsert
/// replace pattern (see `docs/plans/implemented/mount-rename-overwrite.md`
/// for that one, or `rename_entry`'s own doc comment) - there is no history
/// to preserve here (the row was never independently observable with any
/// other content), and this is the one narrow, deliberate exception to
/// "never mutate `content_id` in place": since `tree_entries_ref_count_ins`/
/// `_del` (`migrations.rs`) only fire on `INSERT`/`DELETE`, never `UPDATE`,
/// this manually bumps `contents.ref_count` for `EMPTY_CONTENT_ID` itself in
/// the same transaction, rather than relying on a trigger that won't fire.
pub fn finalize_as_empty_if_undecided(conn: &mut Connection, id: i64) -> Result<(), Error> {
    let tx = conn.transaction()?;
    let updated = tx.execute(
        "UPDATE tree_entries SET content_id = ?1 WHERE id = ?2 AND content_id IS NULL",
        params![crate::EMPTY_CONTENT_ID, id],
    )?;
    if updated > 0 {
        tx.execute(
            "UPDATE contents SET ref_count = ref_count + 1 WHERE id = ?1",
            [crate::EMPTY_CONTENT_ID],
        )?;
    }
    tx.commit()?;
    Ok(())
}

/// Moves `id` to a new `(parent_id, new_name)` - a no-op (`Ok(())`) if that's
/// already where `id` is (matches the Scala prototype's own
/// `oldParts.sameElements(newParts) => OK` check: nothing to replace and
/// nothing to move). If a *different* active entry already occupies the
/// destination:
/// - `no_replace` set → always fails with [`Error::AlreadyExists`],
///   regardless of kind (matches `RENAME_NOREPLACE`/`renameat2(2)`).
/// - Otherwise, a kind-compatible entry (file replacing file, empty
///   directory replacing empty directory) is soft-deleted (`deleted_at =
///   deleted_at`, the same value `id` would get from an ordinary `unlink`/
///   `rmdir`) and `id` takes its place in the same transaction - real POSIX
///   `rename(2)` replace semantics, matching what `rm target && mv source
///   target` would already leave behind through two separate operations
///   (see `docs/plans/mount-rename-overwrite.md`). An incompatible kind, or
///   a non-empty target directory, fails with [`Error::TargetIsADirectory`]/
///   [`Error::TargetIsAFile`]/[`Error::TargetNotEmpty`] respectively rather
///   than attempting anything.
pub fn rename_entry(
    conn: &mut Connection,
    id: i64,
    new_parent_id: i64,
    new_name: &str,
    no_replace: bool,
    deleted_at: i64,
) -> Result<(), Error> {
    let tx = conn.transaction()?;

    if let Some(existing) = find_tree_entry(&tx, new_parent_id, new_name)? {
        if existing.id == id {
            return Ok(());
        }
        if no_replace {
            return Err(Error::AlreadyExists {
                parent_id: new_parent_id,
                name: new_name.to_string(),
            });
        }
        let moving_kind = get_tree_entry(&tx, id)?
            .expect("id is the entry rename_entry was called to move - must still exist")
            .kind;
        match (moving_kind, existing.kind) {
            (EntryKind::File, EntryKind::Dir) => {
                return Err(Error::TargetIsADirectory {
                    parent_id: new_parent_id,
                    name: new_name.to_string(),
                });
            }
            (EntryKind::Dir, EntryKind::File) => {
                return Err(Error::TargetIsAFile {
                    parent_id: new_parent_id,
                    name: new_name.to_string(),
                });
            }
            (EntryKind::Dir, EntryKind::Dir) => {
                if !crate::list_children(&tx, existing.id)?.is_empty() {
                    return Err(Error::TargetNotEmpty {
                        parent_id: new_parent_id,
                        name: new_name.to_string(),
                    });
                }
            }
            (EntryKind::File, EntryKind::File) => {}
        }
        tx.execute(
            "UPDATE tree_entries SET deleted_at = ?1 WHERE id = ?2",
            params![deleted_at, existing.id],
        )?;
    }

    tx.execute(
        "UPDATE tree_entries SET parent_id = ?1, name = ?2 WHERE id = ?3 AND deleted_at IS NULL",
        params![new_parent_id, new_name, id],
    )?;
    tx.commit()?;
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
    fn insert_historical_tree_entry_preserves_an_explicit_deleted_at() {
        let (_temp_dir, conn) = test_connection();

        let active =
            insert_historical_tree_entry(&conn, 0, "a.txt", 1000, None, EntryKind::File, None)
                .unwrap();
        let deleted =
            insert_historical_tree_entry(&conn, 0, "b.txt", 500, Some(900), EntryKind::File, None)
                .unwrap();

        let active_row = get_tree_entry(&conn, active).unwrap().unwrap();
        assert_eq!(active_row.name, "a.txt");
        let deleted_at: Option<i64> = conn
            .query_row(
                "SELECT deleted_at FROM tree_entries WHERE id = ?1",
                [deleted],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(deleted_at, Some(900));
        // The soft-deleted row is invisible to find_tree_entry (active-only).
        assert_eq!(find_tree_entry(&conn, 0, "b.txt").unwrap(), None);
    }

    #[test]
    fn insert_historical_tree_entry_allows_several_historical_rows_for_one_name() {
        let (_temp_dir, conn) = test_connection();

        let first =
            insert_historical_tree_entry(&conn, 0, "a.txt", 100, Some(200), EntryKind::File, None)
                .unwrap();
        let second =
            insert_historical_tree_entry(&conn, 0, "a.txt", 300, Some(400), EntryKind::File, None)
                .unwrap();
        let active =
            insert_historical_tree_entry(&conn, 0, "a.txt", 500, None, EntryKind::File, None)
                .unwrap();

        assert_ne!(first, second);
        assert_ne!(second, active);
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM tree_entries WHERE parent_id = 0 AND name = 'a.txt'",
                (),
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 3);
        assert_eq!(
            find_tree_entry(&conn, 0, "a.txt").unwrap().unwrap().id,
            active
        );
    }

    #[test]
    fn insert_historical_tree_entry_rejects_a_second_active_row_for_the_same_name() {
        let (_temp_dir, conn) = test_connection();
        insert_historical_tree_entry(&conn, 0, "a.txt", 100, None, EntryKind::File, None).unwrap();

        let result =
            insert_historical_tree_entry(&conn, 0, "a.txt", 200, None, EntryKind::File, None);

        assert!(matches!(result, Err(Error::Sqlite(_))));
    }

    #[test]
    fn insert_historical_tree_entry_can_create_a_directory_with_children() {
        let (_temp_dir, conn) = test_connection();
        let dir_id =
            insert_historical_tree_entry(&conn, 0, "sub", 1000, None, EntryKind::Dir, None)
                .unwrap();

        let file_id =
            insert_historical_tree_entry(&conn, dir_id, "a.txt", 1000, None, EntryKind::File, None)
                .unwrap();

        let entry = get_tree_entry(&conn, file_id).unwrap().unwrap();
        assert_eq!(entry.kind, EntryKind::File);
        let parent = find_tree_entry(&conn, 0, "sub").unwrap().unwrap();
        assert_eq!(parent.kind, EntryKind::Dir);
        assert_eq!(parent.id, dir_id);
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
        let (_temp_dir, mut conn) = test_connection();
        let src = insert_directory(&conn, 0, "src", 0).unwrap();
        let dst = insert_directory(&conn, 0, "dst", 0).unwrap();
        let id = insert_directory(&conn, src, "child", 0).unwrap();

        rename_entry(&mut conn, id, dst, "renamed", false, 999).unwrap();

        assert_eq!(find_tree_entry(&conn, src, "child").unwrap(), None);
        let entry = find_tree_entry(&conn, dst, "renamed").unwrap().unwrap();
        assert_eq!(entry.id, id);
    }

    #[test]
    fn rename_entry_is_a_noop_for_a_self_rename() {
        let (_temp_dir, mut conn) = test_connection();
        let id = insert_directory(&conn, 0, "a", 0).unwrap();

        rename_entry(&mut conn, id, 0, "a", false, 999).unwrap();

        let entry = find_tree_entry(&conn, 0, "a").unwrap().unwrap();
        assert_eq!(entry.id, id);
    }

    #[test]
    fn rename_entry_with_no_replace_rejects_an_existing_target() {
        let (_temp_dir, mut conn) = test_connection();
        let id = insert_directory(&conn, 0, "a", 0).unwrap();
        insert_directory(&conn, 0, "b", 0).unwrap();

        let result = rename_entry(&mut conn, id, 0, "b", true, 999);

        assert!(matches!(result, Err(Error::AlreadyExists { parent_id: 0, name }) if name == "b"));
    }

    #[test]
    fn rename_entry_replaces_a_compatible_existing_target() {
        let (_temp_dir, mut conn) = test_connection();
        let id = insert_directory(&conn, 0, "a", 0).unwrap();
        let replaced_id = insert_directory(&conn, 0, "b", 0).unwrap();

        rename_entry(&mut conn, id, 0, "b", false, 999).unwrap();

        let entry = find_tree_entry(&conn, 0, "b").unwrap().unwrap();
        assert_eq!(entry.id, id, "the moved entry now occupies the name");
        let replaced = get_tree_entry(&conn, replaced_id).unwrap().unwrap();
        let deleted_at: Option<i64> = conn
            .query_row(
                "SELECT deleted_at FROM tree_entries WHERE id = ?1",
                [replaced.id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            deleted_at,
            Some(999),
            "the replaced entry is soft-deleted, not gone"
        );
    }

    #[test]
    fn rename_entry_rejects_a_file_replacing_a_directory() {
        let (_temp_dir, mut conn) = test_connection();
        conn.execute(
            "INSERT INTO tree_entries (parent_id, name, time, kind) VALUES (0, 'a', 0, 'file')",
            (),
        )
        .unwrap();
        let id = find_tree_entry(&conn, 0, "a").unwrap().unwrap().id;
        insert_directory(&conn, 0, "b", 0).unwrap();

        let result = rename_entry(&mut conn, id, 0, "b", false, 999);

        assert!(
            matches!(result, Err(Error::TargetIsADirectory { parent_id: 0, name }) if name == "b")
        );
    }

    #[test]
    fn rename_entry_rejects_a_directory_replacing_a_file() {
        let (_temp_dir, mut conn) = test_connection();
        let id = insert_directory(&conn, 0, "a", 0).unwrap();
        conn.execute(
            "INSERT INTO tree_entries (parent_id, name, time, kind) VALUES (0, 'b', 0, 'file')",
            (),
        )
        .unwrap();

        let result = rename_entry(&mut conn, id, 0, "b", false, 999);

        assert!(matches!(result, Err(Error::TargetIsAFile { parent_id: 0, name }) if name == "b"));
    }

    #[test]
    fn rename_entry_rejects_replacing_a_nonempty_directory() {
        let (_temp_dir, mut conn) = test_connection();
        let id = insert_directory(&conn, 0, "a", 0).unwrap();
        let b = insert_directory(&conn, 0, "b", 0).unwrap();
        insert_directory(&conn, b, "child", 0).unwrap();

        let result = rename_entry(&mut conn, id, 0, "b", false, 999);

        assert!(matches!(result, Err(Error::TargetNotEmpty { parent_id: 0, name }) if name == "b"));
    }

    fn content_ref_count(conn: &Connection, content_id: i64) -> i64 {
        conn.query_row(
            "SELECT ref_count FROM contents WHERE id = ?1",
            [content_id],
            |row| row.get(0),
        )
        .unwrap()
    }

    #[test]
    fn finalize_as_empty_if_undecided_settles_a_placeholder() {
        let (_temp_dir, mut conn) = test_connection();
        // A mount create() placeholder: content_id left NULL, matching what
        // ContentSource::Known(None) inserts.
        conn.execute(
            "INSERT INTO tree_entries (id, parent_id, name, time, kind) VALUES (1, 0, 'a.txt', 0, 'file')",
            (),
        )
        .unwrap();
        let ref_count_before = content_ref_count(&conn, crate::EMPTY_CONTENT_ID);

        finalize_as_empty_if_undecided(&mut conn, 1).unwrap();

        let entry = get_tree_entry(&conn, 1).unwrap().unwrap();
        assert_eq!(entry.content_id, Some(crate::EMPTY_CONTENT_ID));
        assert_eq!(
            content_ref_count(&conn, crate::EMPTY_CONTENT_ID),
            ref_count_before + 1,
            "the INSERT/DELETE-only ref_count triggers don't fire for this UPDATE, \
             so finalize_as_empty_if_undecided must bump it by hand"
        );
    }

    #[test]
    fn finalize_as_empty_if_undecided_is_a_noop_for_an_entry_with_real_content() {
        let (_temp_dir, mut conn) = test_connection();
        conn.execute(
            "INSERT INTO contents (id, length, hash) VALUES (2, 3, x'AA')",
            (),
        )
        .unwrap();
        conn.execute(
            "INSERT INTO tree_entries (id, parent_id, name, time, kind, content_id) VALUES (1, 0, 'a.txt', 0, 'file', 2)",
            (),
        )
        .unwrap();

        finalize_as_empty_if_undecided(&mut conn, 1).unwrap();

        let entry = get_tree_entry(&conn, 1).unwrap().unwrap();
        assert_eq!(
            entry.content_id,
            Some(2),
            "already has real content - must not be overwritten"
        );
        assert_eq!(content_ref_count(&conn, crate::EMPTY_CONTENT_ID), 0);
    }

    #[test]
    fn finalize_as_empty_if_undecided_is_a_noop_for_an_already_settled_empty_file() {
        let (_temp_dir, mut conn) = test_connection();
        conn.execute(
            "INSERT INTO tree_entries (id, parent_id, name, time, kind, content_id) VALUES (1, 0, 'a.txt', 0, 'file', ?1)",
            [crate::EMPTY_CONTENT_ID],
        )
        .unwrap();
        let ref_count_before = content_ref_count(&conn, crate::EMPTY_CONTENT_ID);

        finalize_as_empty_if_undecided(&mut conn, 1).unwrap();

        assert_eq!(
            content_ref_count(&conn, crate::EMPTY_CONTENT_ID),
            ref_count_before,
            "already settled - must not double-count"
        );
    }
}
