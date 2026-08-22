//! Directory-tree operations against `tree_entries` - REQ-TREE-001/002/004/005/008,
//! REQ-MOUNT-002/003/009. Directories only for now: `kind` is always [`EntryKind::Dir`] in
//! practice today, since nothing yet creates a file entry (REQ-STORAGE-007's byte store does not
//! exist yet) - the rename logic below still handles a file target correctly regardless, since
//! REQ-MOUNT-009 already specifies that case and the cost of also handling it now is small.
//!
//! `pub(crate)` only: never part of `db`'s public API directly (DESIGN-METADATA-006) - reached
//! exclusively through [`crate::Repository`]'s own methods, which own connection access.
//!
//! Name comparison is whatever SQLite's default `TEXT` comparison does - case-sensitive, byte-
//! exact - which is not a deliberate decision here: tree namespace case-sensitivity is an open
//! question (see "Tree namespace case-sensitivity" in `requirements/open-questions.md`), and this
//! is simply what falls out of not having decided otherwise yet.

use rusqlite::{Connection, OptionalExtension, params};

use crate::Error;

const KIND_DIR: i64 = 0;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryKind {
    Dir,
    File,
}

impl EntryKind {
    fn from_db(kind: i64) -> Self {
        if kind == KIND_DIR {
            EntryKind::Dir
        } else {
            EntryKind::File
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Entry {
    pub id: i64,
    pub kind: EntryKind,
    pub time_millis: i64,
}

fn get_by_id(conn: &Connection, id: i64) -> Result<Option<Entry>, Error> {
    let row: Option<(i64, i64)> = conn
        .query_row(
            "SELECT kind, time FROM tree_entries WHERE id = ?1 AND deleted_at IS NULL",
            params![id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    Ok(row.map(|(kind, time_millis)| Entry {
        id,
        kind: EntryKind::from_db(kind),
        time_millis,
    }))
}

fn require_dir(conn: &Connection, id: i64) -> Result<Entry, Error> {
    let entry = get_by_id(conn, id)?.ok_or(Error::NoSuchEntry(id))?;
    if entry.kind != EntryKind::Dir {
        return Err(Error::WrongKind(id));
    }
    Ok(entry)
}

fn find_child_id(conn: &Connection, parent_id: i64, name: &str) -> Result<Option<i64>, Error> {
    Ok(conn
        .query_row(
            "SELECT id FROM tree_entries WHERE parent_id = ?1 AND name = ?2 AND deleted_at IS NULL",
            params![parent_id, name],
            |row| row.get(0),
        )
        .optional()?)
}

fn touch(conn: &Connection, id: i64, time_millis: i64) -> Result<(), Error> {
    conn.execute(
        "UPDATE tree_entries SET time = ?1 WHERE id = ?2",
        params![time_millis, id],
    )?;
    Ok(())
}

pub(crate) fn resolve_path(conn: &Connection, path: &str) -> Result<Option<Entry>, Error> {
    let mut current_id = 0i64;
    for component in path.split('/').filter(|c| !c.is_empty()) {
        match find_child_id(conn, current_id, component)? {
            Some(id) => current_id = id,
            None => return Ok(None),
        }
    }
    get_by_id(conn, current_id)
}

pub(crate) fn list_children(
    conn: &Connection,
    parent_id: i64,
) -> Result<Vec<(String, EntryKind)>, Error> {
    require_dir(conn, parent_id)?;

    let mut stmt = conn.prepare(
        "SELECT name, kind FROM tree_entries \
         WHERE parent_id = ?1 AND deleted_at IS NULL AND id != 0",
    )?;
    let rows = stmt.query_map(params![parent_id], |row| {
        let name: String = row.get(0)?;
        let kind: i64 = row.get(1)?;
        Ok((name, EntryKind::from_db(kind)))
    })?;
    rows.collect::<Result<Vec<_>, _>>().map_err(Error::from)
}

pub(crate) fn mkdir(
    conn: &Connection,
    parent_id: i64,
    name: &str,
    time_millis: i64,
) -> Result<i64, Error> {
    require_dir(conn, parent_id)?;

    let result = conn.execute(
        "INSERT INTO tree_entries (parent_id, name, time, kind) VALUES (?1, ?2, ?3, ?4)",
        params![parent_id, name, time_millis, KIND_DIR],
    );
    match result {
        Ok(_) => {
            let id = conn.last_insert_rowid();
            touch(conn, parent_id, time_millis)?;
            Ok(id)
        }
        Err(rusqlite::Error::SqliteFailure(err, _))
            if err.code == rusqlite::ErrorCode::ConstraintViolation =>
        {
            Err(Error::EntryAlreadyExists {
                parent_id,
                name: name.to_string(),
            })
        }
        Err(err) => Err(err.into()),
    }
}

pub(crate) fn rmdir(conn: &Connection, id: i64, time_millis: i64) -> Result<(), Error> {
    if id == 0 {
        // Not guarded by any DB trigger (tree_entries_protect_root only blocks a real DELETE,
        // never a soft-delete UPDATE) - guarded here instead.
        return Err(Error::CannotRemoveRoot);
    }
    require_dir(conn, id)?;

    let has_live_children: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM tree_entries WHERE parent_id = ?1 AND deleted_at IS NULL)",
        params![id],
        |row| row.get(0),
    )?;
    if has_live_children {
        return Err(Error::DirectoryNotEmpty(id));
    }

    let parent_id: i64 = conn.query_row(
        "SELECT parent_id FROM tree_entries WHERE id = ?1",
        params![id],
        |row| row.get(0),
    )?;

    conn.execute(
        "UPDATE tree_entries SET deleted_at = ?1 WHERE id = ?2",
        params![time_millis, id],
    )?;
    touch(conn, parent_id, time_millis)?;
    Ok(())
}

/// Whether `ancestor_id` is `descendant_id` itself, or one of its ancestors (walking up via
/// `parent_id` toward the root).
fn is_ancestor_or_self(
    conn: &Connection,
    ancestor_id: i64,
    descendant_id: i64,
) -> Result<bool, Error> {
    let mut walk = descendant_id;
    loop {
        if walk == ancestor_id {
            return Ok(true);
        }
        if walk == 0 {
            return Ok(false);
        }
        walk = conn.query_row(
            "SELECT parent_id FROM tree_entries WHERE id = ?1",
            params![walk],
            |row| row.get(0),
        )?;
    }
}

pub(crate) fn rename(
    conn: &Connection,
    old_parent_id: i64,
    old_name: &str,
    new_parent_id: i64,
    new_name: &str,
    no_replace: bool,
    time_millis: i64,
) -> Result<(), Error> {
    let old_id =
        find_child_id(conn, old_parent_id, old_name)?.ok_or(Error::NoSuchEntry(old_parent_id))?;

    // Same source and target: a no-op, not a cycle.
    if old_parent_id == new_parent_id && old_name == new_name {
        return Ok(());
    }

    require_dir(conn, new_parent_id)?;

    let old_entry = get_by_id(conn, old_id)?.ok_or(Error::NoSuchEntry(old_id))?;
    if old_entry.kind == EntryKind::Dir && is_ancestor_or_self(conn, old_id, new_parent_id)? {
        return Err(Error::WouldCreateCycle);
    }

    if let Some(target_id) = find_child_id(conn, new_parent_id, new_name)? {
        if no_replace {
            return Err(Error::EntryAlreadyExists {
                parent_id: new_parent_id,
                name: new_name.to_string(),
            });
        }
        let target_entry = get_by_id(conn, target_id)?.ok_or(Error::NoSuchEntry(target_id))?;
        // REQ-MOUNT-009: a directory on either side of the collision is always refused, never
        // silently replaced or merged - only a file replacing an existing file goes through.
        if old_entry.kind == EntryKind::Dir || target_entry.kind == EntryKind::Dir {
            return Err(Error::EntryAlreadyExists {
                parent_id: new_parent_id,
                name: new_name.to_string(),
            });
        }
        conn.execute(
            "UPDATE tree_entries SET deleted_at = ?1 WHERE id = ?2",
            params![time_millis, target_id],
        )?;
    }

    conn.execute(
        "UPDATE tree_entries SET parent_id = ?1, name = ?2 WHERE id = ?3",
        params![new_parent_id, new_name, old_id],
    )?;

    touch(conn, old_parent_id, time_millis)?;
    if new_parent_id != old_parent_id {
        touch(conn, new_parent_id, time_millis)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::{Error, RepositorySettings, init_repository, open_repository};

    // Returns the TempDir alongside the Repository - it must outlive every use of the
    // Repository (dropping it deletes the directory the open connection points at).
    fn repo() -> (crate::Repository, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let repo_root = dir.path().join("repo");
        let settings = RepositorySettings::new(Some(20), 1_700_000_000_000);
        init_repository(&repo_root, settings).expect("init must succeed");
        let repo = open_repository(&repo_root).expect("open must succeed");
        (repo, dir)
    }

    #[test]
    fn mkdir_creates_an_entry_findable_by_path_and_bumps_the_parent() {
        let (repo, _dir) = repo();
        let id = repo.mkdir(0, "a", 100).expect("mkdir must succeed");

        let entry = repo
            .resolve_path("/a")
            .expect("resolve must succeed")
            .expect("entry must exist");
        assert_eq!(entry.id, id);
        assert_eq!(entry.kind, crate::EntryKind::Dir);

        let root = repo.resolve_path("/").unwrap().unwrap();
        assert_eq!(root.time_millis, 100);
    }

    #[test]
    fn mkdir_refuses_a_colliding_name() {
        let (repo, _dir) = repo();
        repo.mkdir(0, "a", 100).expect("first mkdir must succeed");

        let err = repo.mkdir(0, "a", 200).unwrap_err();
        assert!(matches!(err, Error::EntryAlreadyExists { .. }));
    }

    #[test]
    fn mkdir_refuses_a_nonexistent_parent() {
        let (repo, _dir) = repo();
        let err = repo.mkdir(999, "a", 100).unwrap_err();
        assert!(matches!(err, Error::NoSuchEntry(999)));
    }

    #[test]
    fn list_children_lists_only_live_direct_children() {
        let (repo, _dir) = repo();
        repo.mkdir(0, "a", 100).unwrap();
        let b_id = repo.mkdir(0, "b", 100).unwrap();
        repo.mkdir(b_id, "nested", 100).unwrap();
        let mut names: Vec<String> = repo
            .list_children(0)
            .unwrap()
            .into_iter()
            .map(|(name, _)| name)
            .collect();
        names.sort();
        assert_eq!(names, vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn rmdir_soft_deletes_an_empty_directory_and_bumps_the_parent() {
        let (repo, _dir) = repo();
        let id = repo.mkdir(0, "a", 100).unwrap();
        repo.rmdir(id, 200).expect("rmdir must succeed");

        assert!(repo.resolve_path("/a").unwrap().is_none());
        let root = repo.resolve_path("/").unwrap().unwrap();
        assert_eq!(root.time_millis, 200);
    }

    #[test]
    fn rmdir_refuses_a_nonempty_directory() {
        let (repo, _dir) = repo();
        let id = repo.mkdir(0, "a", 100).unwrap();
        repo.mkdir(id, "b", 100).unwrap();

        let err = repo.rmdir(id, 200).unwrap_err();
        assert!(matches!(err, Error::DirectoryNotEmpty(_)));
    }

    #[test]
    fn rmdir_refuses_the_root() {
        let (repo, _dir) = repo();
        let err = repo.rmdir(0, 100).unwrap_err();
        assert!(matches!(err, Error::CannotRemoveRoot));
    }

    #[test]
    fn rename_moves_an_entry_to_a_new_parent_and_name() {
        let (repo, _dir) = repo();
        let a = repo.mkdir(0, "a", 100).unwrap();
        let b = repo.mkdir(0, "b", 100).unwrap();

        repo.rename(0, "a", b, "renamed", false, 200)
            .expect("rename must succeed");

        assert!(repo.resolve_path("/a").unwrap().is_none());
        let moved = repo.resolve_path("/b/renamed").unwrap().unwrap();
        assert_eq!(moved.id, a);
        assert_eq!(repo.resolve_path("/").unwrap().unwrap().time_millis, 200);
        assert_eq!(repo.resolve_path("/b").unwrap().unwrap().time_millis, 200);
    }

    #[test]
    fn rename_onto_itself_is_a_no_op() {
        let (repo, _dir) = repo();
        repo.mkdir(0, "a", 100).unwrap();
        repo.rename(0, "a", 0, "a", false, 200)
            .expect("self-rename must succeed");
        assert!(repo.resolve_path("/a").unwrap().is_some());
    }

    #[test]
    fn rename_refuses_replacing_an_existing_directory() {
        let (repo, _dir) = repo();
        repo.mkdir(0, "a", 100).unwrap();
        repo.mkdir(0, "b", 100).unwrap();

        let err = repo.rename(0, "a", 0, "b", false, 200).unwrap_err();
        assert!(matches!(err, Error::EntryAlreadyExists { .. }));
    }

    #[test]
    fn rename_refuses_moving_a_directory_into_its_own_subtree() {
        let (repo, _dir) = repo();
        let a = repo.mkdir(0, "a", 100).unwrap();
        repo.mkdir(a, "b", 100).unwrap();

        let err = repo.rename(0, "a", a, "a", false, 200).unwrap_err();
        assert!(matches!(err, Error::WouldCreateCycle));
    }

    #[test]
    fn rename_replaces_an_existing_file_unless_no_replace_is_set() {
        let (repo, _dir) = repo();
        repo.with_connection(|conn| {
            conn.execute(
                "INSERT INTO contents (id, length, hash) \
                 VALUES (2, 0, X'0102030405060708090A0B0C0D0E0F1011121314')",
                (),
            )?;
            conn.execute(
                "INSERT INTO tree_entries (id, parent_id, name, time, content_id, kind) \
                 VALUES (1, 0, 'old.txt', 0, 2, 1)",
                (),
            )?;
            conn.execute(
                "INSERT INTO tree_entries (id, parent_id, name, time, content_id, kind) \
                 VALUES (2, 0, 'new.txt', 0, 2, 1)",
                (),
            )?;
            Ok(())
        })
        .unwrap();

        let err = repo
            .rename(0, "old.txt", 0, "new.txt", true, 200)
            .unwrap_err();
        assert!(matches!(err, Error::EntryAlreadyExists { .. }));

        repo.rename(0, "old.txt", 0, "new.txt", false, 200)
            .expect("replacing an existing file must succeed without no_replace");
        let replaced = repo.resolve_path("/new.txt").unwrap().unwrap();
        assert_eq!(replaced.id, 1);
    }
}
