//! Directory-tree operations against `tree_entries` - REQ-TREE-001/002/004/005/008,
//! REQ-MOUNT-002/003/009/010. [`settle_file`] is the one file-creating operation
//! (DESIGN-METADATA-008/DESIGN-MOUNT-011 in `docs/design/mount-write-path.md`): a file's content
//! is always already resolved to a `content_id` by the time it reaches this module, via
//! [`crate::content`] and [`crate::allocation`] - nothing here decides *what* a file's content is,
//! only how it lands in the tree.
//!
//! `pub(crate)` only: never part of `db`'s public API directly (DESIGN-METADATA-006) - reached
//! exclusively through [`crate::Repository`]'s own methods, which own connection access.
//!
//! Name comparison is case-sensitive at the storage level on every platform (REQ-MOUNT-010 in
//! `requirements/functional/mount.md`) - [`find_child_id`] additionally falls back to a
//! case-insensitive match (DESIGN-MOUNT-005 in `docs/design/tree-namespace-case-sensitivity.md`)
//! on a Windows build's exact-match miss. Every caller that needs to know whether a name already
//! exists goes through that one function - plain lookup ([`resolve_path`]), [`mkdir`]/[`settle_file`]'s
//! collision pre-check, and [`rename`]'s target-existence check - so `create`/`mkdir`/`rename`
//! running on a Windows build cannot itself introduce a case-only-differing pair, while one
//! already present (e.g. written from Linux) stays representable and reachable.

use std::collections::HashMap;

use rusqlite::{Connection, OptionalExtension, params};

use crate::Error;
use crate::name_cache::NameCache;

const KIND_DIR: i64 = 0;
const KIND_FILE: i64 = 1;

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
    /// `Some` for a file, `None` for a directory - `chk_tree_entries_kind_content_id` guarantees
    /// this matches `kind` exactly.
    pub content_id: Option<i64>,
    /// The entry's logical content size - always `0` for a directory; for a file, its content's
    /// own `contents.length`.
    pub size: u64,
}

pub(crate) fn get_by_id(conn: &Connection, id: i64) -> Result<Option<Entry>, Error> {
    let row: Option<(i64, i64, Option<i64>, Option<i64>)> = conn
        .query_row(
            "SELECT te.kind, te.time, te.content_id, c.length \
             FROM tree_entries te LEFT JOIN contents c ON c.id = te.content_id \
             WHERE te.id = ?1 AND te.deleted_at IS NULL",
            params![id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()?;
    Ok(row.map(|(kind, time_millis, content_id, length)| Entry {
        id,
        kind: EntryKind::from_db(kind),
        time_millis,
        content_id,
        size: length.unwrap_or(0) as u64,
    }))
}

fn require_dir(conn: &Connection, id: i64) -> Result<Entry, Error> {
    let entry = get_by_id(conn, id)?.ok_or(Error::NoSuchEntry(id))?;
    if entry.kind != EntryKind::Dir {
        return Err(Error::WrongKind(id));
    }
    Ok(entry)
}

fn find_child_id(
    conn: &Connection,
    cache: &mut NameCache,
    parent_id: i64,
    name: &str,
) -> Result<Option<i64>, Error> {
    let exact: Option<i64> = conn
        .query_row(
            "SELECT id FROM tree_entries WHERE parent_id = ?1 AND name = ?2 AND deleted_at IS NULL",
            params![parent_id, name],
            |row| row.get(0),
        )
        .optional()?;
    if exact.is_some() {
        return Ok(exact);
    }
    if !cfg!(windows) {
        return Ok(None);
    }
    find_child_id_case_insensitive(conn, cache, parent_id, name)
}

/// DESIGN-MOUNT-005's Unicode case fold for the lookup fallback below - Rust's full case mapping,
/// locale-independent by construction (deterministic regardless of the running system's locale).
/// Not guaranteed to match NTFS's own per-codepoint upcase table in every corner case (e.g. German
/// `ß`, whose uppercase form changes length) - see that design doc's "Known limitations".
fn fold_key(name: &str) -> String {
    name.to_uppercase()
}

/// Inserts `id` under `folded_key` into `candidates`, keeping the higher id if one is already
/// present there - DESIGN-MOUNT-005's deterministic tiebreak (the most recently created entry wins
/// a case-only collision). Used while populating a cache entry from an unordered `SELECT`, where
/// more than one live sibling can fold to the same key (e.g. a pair written from Linux, where
/// case-only collisions are not prevented - REQ-MOUNT-010).
fn insert_keeping_highest_id(candidates: &mut HashMap<String, i64>, folded_key: String, id: i64) {
    candidates
        .entry(folded_key)
        .and_modify(|existing| {
            if id > *existing {
                *existing = id;
            }
        })
        .or_insert(id);
}

/// [`find_child_id`]'s Windows-only fallback body, factored out and left unconditionally compiled
/// (not itself `#[cfg(windows)]`) so its actual query-and-match logic can be exercised by tests on
/// any platform, even though [`find_child_id`] only ever calls it on a real Windows build.
///
/// Consults `cache` first - DESIGN-MOUNT-017's experimental mitigation for the unindexed full scan
/// below being O(n) in the parent's live child count on every miss. The cached form is keyed by
/// folded name (`HashMap<String, i64>`, not a linearly-scanned list), so a lookup against an
/// already-warm entry is an `O(1)` average hash lookup, not another `O(n)` scan.
fn find_child_id_case_insensitive(
    conn: &Connection,
    cache: &mut NameCache,
    parent_id: i64,
    name: &str,
) -> Result<Option<i64>, Error> {
    let target_key = fold_key(name);
    cache.with_cached_or_populate(
        parent_id,
        || {
            // Folds each candidate's name once here, at population time - not the raw `name`, so
            // a lookup against an already-warm cache entry never has to fold again.
            let mut stmt = conn.prepare(
                "SELECT id, name FROM tree_entries \
                 WHERE parent_id = ?1 AND deleted_at IS NULL AND id != 0",
            )?;
            let rows = stmt.query_map(params![parent_id], |row| {
                let id: i64 = row.get(0)?;
                let name: String = row.get(1)?;
                Ok((id, name))
            })?;
            let mut candidates = HashMap::new();
            for row in rows {
                let (id, name) = row?;
                insert_keeping_highest_id(&mut candidates, fold_key(&name), id);
            }
            Ok(candidates)
        },
        |candidates| candidates.get(&target_key).copied(),
    )
}

fn touch(conn: &Connection, id: i64, time_millis: i64) -> Result<(), Error> {
    conn.execute(
        "UPDATE tree_entries SET time = ?1 WHERE id = ?2",
        params![time_millis, id],
    )?;
    Ok(())
}

pub(crate) fn resolve_path(
    conn: &Connection,
    cache: &mut NameCache,
    path: &str,
) -> Result<Option<Entry>, Error> {
    let mut current_id = 0i64;
    for component in path.split('/').filter(|c| !c.is_empty()) {
        match find_child_id(conn, cache, current_id, component)? {
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

/// The live entry `id`'s current `(parent_id, name)` - `None` if it does not exist or is
/// soft-deleted. Distinct from resolving a whole path: a caller that already has an id (e.g. a
/// background settle job resolving where its result belongs) needs this directly, reflecting any
/// `rename` that happened since the id was first obtained.
pub(crate) fn parent_and_name(conn: &Connection, id: i64) -> Result<Option<(i64, String)>, Error> {
    conn.query_row(
        "SELECT parent_id, name FROM tree_entries WHERE id = ?1 AND deleted_at IS NULL",
        params![id],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )
    .optional()
    .map_err(Error::from)
}

/// Sets `id`'s own modification time directly (REQ-MOUNT-003's `utimens`) - distinct from
/// [`touch`], which bumps a *parent* as a side effect of a structural change to it.
pub(crate) fn set_mtime(conn: &Connection, id: i64, time_millis: i64) -> Result<(), Error> {
    get_by_id(conn, id)?.ok_or(Error::NoSuchEntry(id))?;
    touch(conn, id, time_millis)
}

pub(crate) fn mkdir(
    conn: &Connection,
    cache: &mut NameCache,
    parent_id: i64,
    name: &str,
    time_millis: i64,
) -> Result<i64, Error> {
    require_dir(conn, parent_id)?;

    if find_child_id(conn, cache, parent_id, name)?.is_some() {
        return Err(Error::EntryAlreadyExists {
            parent_id,
            name: name.to_string(),
        });
    }

    let result = conn.execute(
        "INSERT INTO tree_entries (parent_id, name, time, kind) VALUES (?1, ?2, ?3, ?4)",
        params![parent_id, name, time_millis, KIND_DIR],
    );
    match result {
        Ok(_) => {
            let id = conn.last_insert_rowid();
            cache.note_inserted(parent_id, id, &fold_key(name));
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

/// Settles a background write job's already-resolved content into the tree
/// (DESIGN-METADATA-008/DESIGN-MOUNT-011): inserts a new file entry already at `content_id`,
/// never updating an existing row's `content_id` in place. If a live entry already occupies
/// `(parent_id, name)`, it is soft-deleted first and the new entry becomes a separate
/// REQ-TREE-004 history entry for that path - a directory at that name is refused instead
/// (REQ-MOUNT-009's "a directory on either side is always refused"), the same as [`rename`]'s own
/// replace check. Bumps the parent's modification time only for a genuinely new entry (nothing
/// live at that name before) - overwriting an existing file does not, matching REQ-TREE-005's
/// "a pure content change is not a change to the parent's set of entries" (see DESIGN-MOUNT-011).
///
/// Returns the new entry's id.
pub(crate) fn settle_file(
    conn: &Connection,
    cache: &mut NameCache,
    parent_id: i64,
    name: &str,
    time_millis: i64,
    content_id: i64,
) -> Result<i64, Error> {
    settle_file_impl(conn, cache, parent_id, name, time_millis, content_id, None)
}

/// Like [`settle_file`], except a live entry that is still exactly id `collapsible_placeholder_id`
/// is hard-deleted (no REQ-TREE-004 history entry) instead of soft-deleted - DESIGN-MOUNT-016's
/// narrow exception for a `create()`-only empty placeholder still untouched at its own file's
/// first real settle. Any other live entry there (already replaced by something else since, or
/// simply a different id) is soft-deleted as usual.
///
/// Returns the new entry's id.
pub(crate) fn settle_file_collapsing_placeholder(
    conn: &Connection,
    cache: &mut NameCache,
    parent_id: i64,
    name: &str,
    time_millis: i64,
    content_id: i64,
    collapsible_placeholder_id: i64,
) -> Result<i64, Error> {
    settle_file_impl(
        conn,
        cache,
        parent_id,
        name,
        time_millis,
        content_id,
        Some(collapsible_placeholder_id),
    )
}

#[allow(clippy::too_many_arguments)]
fn settle_file_impl(
    conn: &Connection,
    cache: &mut NameCache,
    parent_id: i64,
    name: &str,
    time_millis: i64,
    content_id: i64,
    collapsible_placeholder_id: Option<i64>,
) -> Result<i64, Error> {
    require_dir(conn, parent_id)?;

    let replaced = find_child_id(conn, cache, parent_id, name)?;
    if let Some(old_id) = replaced {
        let old_entry = get_by_id(conn, old_id)?.ok_or(Error::NoSuchEntry(old_id))?;
        if old_entry.kind == EntryKind::Dir {
            return Err(Error::EntryAlreadyExists {
                parent_id,
                name: name.to_string(),
            });
        }
        if collapsible_placeholder_id == Some(old_id) {
            // `old_id` still being live already proves it is still exactly the row the caller
            // inserted, holding its original content unmodified - tree_entries.id is
            // AUTOINCREMENT, so no later row can ever reuse it (see "Why tree_entries.id is
            // AUTOINCREMENT" in metadata-schema-with-contents-table.md).
            conn.execute("DELETE FROM tree_entries WHERE id = ?1", params![old_id])?;
        } else {
            conn.execute(
                "UPDATE tree_entries SET deleted_at = ?1 WHERE id = ?2",
                params![time_millis, old_id],
            )?;
        }
        // Simpler and safer than trying to patch the cached list in place for a replace - the
        // next miss just repopulates it.
        cache.invalidate(parent_id);
    }

    conn.execute(
        "INSERT INTO tree_entries (parent_id, name, time, content_id, kind) \
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![parent_id, name, time_millis, content_id, KIND_FILE],
    )?;
    let id = conn.last_insert_rowid();
    cache.note_inserted(parent_id, id, &fold_key(name));

    if replaced.is_none() {
        touch(conn, parent_id, time_millis)?;
    }
    Ok(id)
}

pub(crate) fn rmdir(
    conn: &Connection,
    cache: &mut NameCache,
    id: i64,
    time_millis: i64,
) -> Result<(), Error> {
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
    cache.invalidate(parent_id);
    touch(conn, parent_id, time_millis)?;
    Ok(())
}

/// Soft-deletes the live file entry `id` (REQ-TREE-002), bumping its parent's modification time -
/// removing a name is a structural change (REQ-TREE-005), unlike DESIGN-MOUNT-011's pure content
/// overwrite. A directory at `id` is refused; the caller's `rmdir` is the directory counterpart.
pub(crate) fn unlink_file(
    conn: &Connection,
    cache: &mut NameCache,
    id: i64,
    time_millis: i64,
) -> Result<(), Error> {
    let entry = get_by_id(conn, id)?.ok_or(Error::NoSuchEntry(id))?;
    if entry.kind != EntryKind::File {
        return Err(Error::WrongKind(id));
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
    cache.invalidate(parent_id);
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

#[allow(clippy::too_many_arguments)]
pub(crate) fn rename(
    conn: &Connection,
    cache: &mut NameCache,
    old_parent_id: i64,
    old_name: &str,
    new_parent_id: i64,
    new_name: &str,
    no_replace: bool,
    time_millis: i64,
) -> Result<(), Error> {
    let old_id = find_child_id(conn, cache, old_parent_id, old_name)?
        .ok_or(Error::NoSuchEntry(old_parent_id))?;

    // Same source and target: a no-op, not a cycle.
    if old_parent_id == new_parent_id && old_name == new_name {
        return Ok(());
    }

    require_dir(conn, new_parent_id)?;

    let old_entry = get_by_id(conn, old_id)?.ok_or(Error::NoSuchEntry(old_id))?;
    if old_entry.kind == EntryKind::Dir && is_ancestor_or_self(conn, old_id, new_parent_id)? {
        return Err(Error::WouldCreateCycle);
    }

    if let Some(target_id) = find_child_id(conn, cache, new_parent_id, new_name)? {
        // DESIGN-MOUNT-005: under the Windows lookup fallback, the match found here can be the
        // entry being renamed itself (a case-only respelling, e.g. install.txt -> Install.txt) -
        // not a distinct existing target, so not a collision at all. Falls through to the plain
        // rename below, which updates the stored spelling.
        if target_id != old_id {
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
    }

    conn.execute(
        "UPDATE tree_entries SET parent_id = ?1, name = ?2 WHERE id = ?3",
        params![new_parent_id, new_name, old_id],
    )?;
    // Simpler and safer than patching both cached lists in place (in particular the
    // same-parent-rename case) - the next miss just repopulates whichever one is touched again.
    cache.invalidate(old_parent_id);
    if new_parent_id != old_parent_id {
        cache.invalidate(new_parent_id);
    }

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

    /// Inserts a bare `contents` row (no chunks) for tests that only need a valid `content_id` to
    /// point a file entry at, not the dedup bookkeeping itself.
    fn insert_content(repo: &crate::Repository, id: i64, hash_byte: u8) -> i64 {
        insert_content_with_length(repo, id, hash_byte, 0)
    }

    fn insert_content_with_length(
        repo: &crate::Repository,
        id: i64,
        hash_byte: u8,
        length: i64,
    ) -> i64 {
        repo.with_connection(|conn, _cache| {
            conn.execute(
                "INSERT INTO contents (id, length, hash) VALUES (?1, ?2, ?3)",
                (id, length, vec![hash_byte; 20]),
            )?;
            Ok(())
        })
        .unwrap();
        id
    }

    #[test]
    fn settle_file_creates_a_findable_file_entry_and_bumps_the_parent() {
        let (repo, _dir) = repo();
        let content_id = insert_content(&repo, 1, 0xAA);

        let id = repo
            .settle_file(0, "a.txt", 100, content_id)
            .expect("settle_file must succeed");

        let entry = repo.resolve_path("/a.txt").unwrap().unwrap();
        assert_eq!(entry.id, id);
        assert_eq!(entry.kind, crate::EntryKind::File);
        assert_eq!(repo.resolve_path("/").unwrap().unwrap().time_millis, 100);
    }

    #[test]
    fn a_file_entry_exposes_its_content_id_and_size() {
        let (repo, _dir) = repo();
        let content_id = insert_content_with_length(&repo, 1, 0xAA, 12345);
        repo.settle_file(0, "a.txt", 100, content_id).unwrap();

        let entry = repo.resolve_path("/a.txt").unwrap().unwrap();
        assert_eq!(entry.content_id, Some(content_id));
        assert_eq!(entry.size, 12345);
    }

    #[test]
    fn a_directory_entry_has_no_content_id_and_zero_size() {
        let (repo, _dir) = repo();
        repo.mkdir(0, "a", 100).unwrap();

        let entry = repo.resolve_path("/a").unwrap().unwrap();
        assert_eq!(entry.content_id, None);
        assert_eq!(entry.size, 0);
    }

    #[test]
    fn settle_file_overwriting_an_existing_file_creates_a_new_history_entry() {
        let (repo, _dir) = repo();
        let content_a = insert_content(&repo, 1, 0xAA);
        let content_b = insert_content(&repo, 2, 0xBB);

        let first_id = repo.settle_file(0, "a.txt", 100, content_a).unwrap();
        // Root's own time must not move on the overwrite below - captured before it happens.
        let root_time_before = repo.resolve_path("/").unwrap().unwrap().time_millis;

        let second_id = repo.settle_file(0, "a.txt", 200, content_b).unwrap();

        assert_ne!(
            first_id, second_id,
            "overwrite must create a new entry, not update the old one in place"
        );
        let live = repo.resolve_path("/a.txt").unwrap().unwrap();
        assert_eq!(live.id, second_id);
        assert_eq!(
            repo.resolve_path("/").unwrap().unwrap().time_millis,
            root_time_before,
            "a pure content overwrite must not bump the parent's mtime (REQ-TREE-005/DESIGN-MOUNT-011)"
        );

        // The old entry is soft-deleted, not gone - still visible via a raw lookup by id.
        let old_deleted_at: Option<i64> = repo
            .with_connection(|conn, _cache| {
                Ok(conn.query_row(
                    "SELECT deleted_at FROM tree_entries WHERE id = ?1",
                    [first_id],
                    |row| row.get(0),
                )?)
            })
            .unwrap();
        assert_eq!(old_deleted_at, Some(200));
    }

    fn row_count(repo: &crate::Repository, id: i64) -> i64 {
        repo.with_connection(|conn, _cache| {
            Ok(conn.query_row(
                "SELECT COUNT(*) FROM tree_entries WHERE id = ?1",
                [id],
                |row| row.get(0),
            )?)
        })
        .unwrap()
    }

    #[test]
    fn settle_file_collapsing_placeholder_hard_deletes_the_expected_placeholder() {
        let (repo, _dir) = repo();
        let empty_content = insert_content(&repo, 0, 0xEE);
        let real_content = insert_content(&repo, 5, 0xAA);

        let placeholder_id = repo.settle_file(0, "a.txt", 100, empty_content).unwrap();
        let root_time_before = repo.resolve_path("/").unwrap().unwrap().time_millis;

        let new_id = repo
            .settle_file_collapsing_placeholder(0, "a.txt", 200, real_content, placeholder_id)
            .unwrap();

        assert_ne!(new_id, placeholder_id);
        let live = repo.resolve_path("/a.txt").unwrap().unwrap();
        assert_eq!(live.id, new_id);
        assert_eq!(live.content_id, Some(real_content));
        assert_eq!(
            row_count(&repo, placeholder_id),
            0,
            "the placeholder row must be gone entirely, not merely soft-deleted"
        );
        assert_eq!(
            repo.resolve_path("/").unwrap().unwrap().time_millis,
            root_time_before,
            "collapsing a placeholder is still a pure content change, not a structural one"
        );
    }

    #[test]
    fn settle_file_collapsing_placeholder_soft_deletes_when_the_live_entry_is_not_the_expected_id()
    {
        let (repo, _dir) = repo();
        let content_a = insert_content(&repo, 1, 0xAA);
        let content_b = insert_content(&repo, 2, 0xBB);

        let first_id = repo.settle_file(0, "a.txt", 100, content_a).unwrap();
        // A stale/wrong expected id (e.g. from a superseded generation) must not cause `first_id`
        // to be hard-deleted - only an exact match does that.
        let wrong_expected_id = first_id + 1000;

        let second_id = repo
            .settle_file_collapsing_placeholder(0, "a.txt", 200, content_b, wrong_expected_id)
            .unwrap();

        assert_ne!(first_id, second_id);
        assert_eq!(
            row_count(&repo, first_id),
            1,
            "a mismatched expected id must fall back to the ordinary, history-preserving replace"
        );
        let old_deleted_at: Option<i64> = repo
            .with_connection(|conn, _cache| {
                Ok(conn.query_row(
                    "SELECT deleted_at FROM tree_entries WHERE id = ?1",
                    [first_id],
                    |row| row.get(0),
                )?)
            })
            .unwrap();
        assert_eq!(old_deleted_at, Some(200));
    }

    #[test]
    fn settle_file_collapsing_placeholder_refuses_to_replace_a_directory() {
        let (repo, _dir) = repo();
        let dir_id = repo.mkdir(0, "a", 100).unwrap();
        let content_id = insert_content(&repo, 1, 0xAA);

        let err = repo
            .settle_file_collapsing_placeholder(0, "a", 200, content_id, dir_id)
            .unwrap_err();
        assert!(matches!(err, Error::EntryAlreadyExists { .. }));
    }

    #[test]
    fn settle_file_refuses_to_replace_a_directory() {
        let (repo, _dir) = repo();
        repo.mkdir(0, "a", 100).unwrap();
        let content_id = insert_content(&repo, 1, 0xAA);

        let err = repo.settle_file(0, "a", 200, content_id).unwrap_err();
        assert!(matches!(err, Error::EntryAlreadyExists { .. }));
    }

    #[test]
    fn settle_file_refuses_a_nonexistent_parent() {
        let (repo, _dir) = repo();
        let content_id = insert_content(&repo, 1, 0xAA);
        let err = repo.settle_file(999, "a.txt", 100, content_id).unwrap_err();
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
    fn set_mtime_updates_the_entry_itself_not_its_parent() {
        let (repo, _dir) = repo();
        let id = repo.mkdir(0, "a", 100).unwrap();
        repo.set_mtime(id, 300).expect("set_mtime must succeed");

        assert_eq!(repo.resolve_path("/a").unwrap().unwrap().time_millis, 300);
        assert_eq!(repo.resolve_path("/").unwrap().unwrap().time_millis, 100);
    }

    #[test]
    fn set_mtime_refuses_a_nonexistent_entry() {
        let (repo, _dir) = repo();
        let err = repo.set_mtime(999, 100).unwrap_err();
        assert!(matches!(err, Error::NoSuchEntry(999)));
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
    fn unlink_file_soft_deletes_a_live_file_and_bumps_the_parent() {
        let (repo, _dir) = repo();
        let content_id = insert_content(&repo, 1, 0xAA);
        let id = repo.settle_file(0, "a.txt", 100, content_id).unwrap();

        repo.unlink_file(id, 200).expect("unlink_file must succeed");

        assert!(repo.resolve_path("/a.txt").unwrap().is_none());
        let root = repo.resolve_path("/").unwrap().unwrap();
        assert_eq!(root.time_millis, 200);
    }

    #[test]
    fn unlink_file_refuses_a_directory() {
        let (repo, _dir) = repo();
        let id = repo.mkdir(0, "a", 100).unwrap();
        let err = repo.unlink_file(id, 200).unwrap_err();
        assert!(matches!(err, Error::WrongKind(_)));
    }

    #[test]
    fn unlink_file_refuses_a_nonexistent_entry() {
        let (repo, _dir) = repo();
        let err = repo.unlink_file(999, 200).unwrap_err();
        assert!(matches!(err, Error::NoSuchEntry(999)));
    }

    #[test]
    fn entry_by_id_returns_the_live_entry() {
        let (repo, _dir) = repo();
        let id = repo.mkdir(0, "a", 100).unwrap();
        let entry = repo.entry_by_id(id).unwrap().unwrap();
        assert_eq!(entry.id, id);
        assert_eq!(entry.kind, crate::EntryKind::Dir);
    }

    #[test]
    fn entry_by_id_returns_none_once_deleted() {
        let (repo, _dir) = repo();
        let id = repo.mkdir(0, "a", 100).unwrap();
        repo.rmdir(id, 200).unwrap();
        assert!(repo.entry_by_id(id).unwrap().is_none());
    }

    #[test]
    fn entry_by_id_returns_none_for_an_unknown_id() {
        let (repo, _dir) = repo();
        assert!(repo.entry_by_id(999).unwrap().is_none());
    }

    #[test]
    fn parent_and_name_reflects_a_rename() {
        let (repo, _dir) = repo();
        let a = repo.mkdir(0, "a", 100).unwrap();
        let b = repo.mkdir(0, "b", 100).unwrap();
        let id = repo.mkdir(a, "child", 100).unwrap();

        assert_eq!(
            repo.parent_and_name(id).unwrap(),
            Some((a, "child".to_string()))
        );

        repo.rename(a, "child", b, "renamed", false, 200).unwrap();
        assert_eq!(
            repo.parent_and_name(id).unwrap(),
            Some((b, "renamed".to_string()))
        );
    }

    #[test]
    fn parent_and_name_returns_none_once_deleted() {
        let (repo, _dir) = repo();
        let id = repo.mkdir(0, "a", 100).unwrap();
        repo.rmdir(id, 200).unwrap();
        assert!(repo.parent_and_name(id).unwrap().is_none());
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
        repo.with_connection(|conn, _cache| {
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

    // DESIGN-MOUNT-005: the fold/tiebreak logic itself, platform-independent - runs on every
    // platform regardless of which one actually reaches it through `find_child_id`.

    #[test]
    fn fold_key_folds_ascii_case() {
        assert_eq!(super::fold_key("Foo"), super::fold_key("foo"));
    }

    #[test]
    fn fold_key_folds_non_ascii_case_too() {
        assert_eq!(super::fold_key("café"), super::fold_key("CAFÉ"));
    }

    #[test]
    fn insert_keeping_highest_id_keeps_the_only_id_present() {
        let mut candidates = std::collections::HashMap::new();
        super::insert_keeping_highest_id(&mut candidates, super::fold_key("foo"), 1);
        assert_eq!(candidates.get(&super::fold_key("FOO")), Some(&1));
    }

    #[test]
    fn insert_keeping_highest_id_keeps_the_higher_id_regardless_of_insertion_order() {
        let key = super::fold_key("foo");
        let mut candidates = std::collections::HashMap::new();
        super::insert_keeping_highest_id(&mut candidates, key.clone(), 3);
        super::insert_keeping_highest_id(&mut candidates, key.clone(), 7);
        super::insert_keeping_highest_id(&mut candidates, key.clone(), 5);
        assert_eq!(candidates.get(&key), Some(&7));
    }

    // DESIGN-MOUNT-005's query-and-match fallback, called directly (bypassing `find_child_id`'s
    // `cfg!(windows)` dispatch) so the real SQLite-backed logic is exercised on every platform,
    // not only a real Windows build.

    #[test]
    fn find_child_id_case_insensitive_finds_a_case_variant_sibling() {
        let (repo, _dir) = repo();
        let id = repo.mkdir(0, "foo", 100).unwrap();

        let mut cache = super::NameCache::new(16);
        let found = repo
            .with_connection(|conn, _cache| {
                super::find_child_id_case_insensitive(conn, &mut cache, 0, "FOO")
            })
            .unwrap();
        assert_eq!(found, Some(id));
    }

    #[test]
    fn find_child_id_case_insensitive_prefers_the_highest_id_among_pre_existing_case_variants() {
        let (repo, _dir) = repo();
        // Two live siblings differing only by case, as if written from Linux (REQ-MOUNT-010 does
        // not prevent that there - only `find_child_id`'s Windows-only fallback prevents
        // *creating* a second one once a build reaches it). Inserted directly via SQL since
        // `mkdir` itself would refuse the second one, even running this test on a non-Windows
        // platform - this exercises the real population path in `find_child_id_case_insensitive`
        // itself, not just the standalone `insert_keeping_highest_id` tiebreak helper above.
        let higher_id = repo
            .with_transaction(|conn, _cache| {
                conn.execute(
                    "INSERT INTO tree_entries (parent_id, name, time, kind) VALUES (0, 'foo', 100, 0)",
                    [],
                )?;
                conn.execute(
                    "INSERT INTO tree_entries (parent_id, name, time, kind) VALUES (0, 'Foo', 100, 0)",
                    [],
                )?;
                Ok(conn.last_insert_rowid())
            })
            .unwrap();

        let mut cache = super::NameCache::new(16);
        let found = repo
            .with_connection(|conn, _cache| {
                super::find_child_id_case_insensitive(conn, &mut cache, 0, "FOO")
            })
            .unwrap();
        assert_eq!(found, Some(higher_id));
    }

    #[test]
    fn find_child_id_case_insensitive_returns_none_without_a_match() {
        let (repo, _dir) = repo();
        repo.mkdir(0, "foo", 100).unwrap();

        let mut cache = super::NameCache::new(16);
        let found = repo
            .with_connection(|conn, _cache| {
                super::find_child_id_case_insensitive(conn, &mut cache, 0, "bar")
            })
            .unwrap();
        assert_eq!(found, None);
    }

    #[test]
    fn find_child_id_case_insensitive_ignores_a_deleted_entry() {
        let (repo, _dir) = repo();
        let id = repo.mkdir(0, "foo", 100).unwrap();
        repo.rmdir(id, 200).unwrap();

        let mut cache = super::NameCache::new(16);
        let found = repo
            .with_connection(|conn, _cache| {
                super::find_child_id_case_insensitive(conn, &mut cache, 0, "FOO")
            })
            .unwrap();
        assert_eq!(found, None);
    }

    // A regression test for the cache itself: a removal must invalidate the *parent's* cached
    // entry, not the removed id's own - easy to get backwards (an earlier version of this branch
    // did exactly that), and a stale cache would silently resurrect a removed sibling for every
    // case-insensitive lookup against that parent from then on.
    #[test]
    fn find_child_id_case_insensitive_does_not_resurrect_a_sibling_removed_after_the_cache_was_warmed()
     {
        let (repo, _dir) = repo();
        let mut cache = super::NameCache::new(16);

        let a = repo
            .with_transaction(|conn, _cache| super::mkdir(conn, &mut cache, 0, "a", 100))
            .unwrap();

        let found = repo
            .with_connection(|conn, _cache| {
                super::find_child_id_case_insensitive(conn, &mut cache, 0, "A")
            })
            .unwrap();
        assert_eq!(
            found,
            Some(a),
            "the cache must be warm for the root directory now"
        );

        repo.with_transaction(|conn, _cache| super::rmdir(conn, &mut cache, a, 200))
            .unwrap();

        let found_after_removal = repo
            .with_connection(|conn, _cache| {
                super::find_child_id_case_insensitive(conn, &mut cache, 0, "A")
            })
            .unwrap();
        assert_eq!(found_after_removal, None);
    }

    // White-box coverage for every `tree_entries` mutation path's effect on the cache
    // (DESIGN-MOUNT-017 in `tree-namespace-case-sensitivity.md`), not just `mkdir`/`rmdir` above -
    // each test shares one `NameCache` instance across a warming lookup and the mutation, the same
    // way a real `Repository`'s single cache instance would, so a wrong or missing cache update
    // shows up as an incorrect `find_child_id_case_insensitive` result rather than being masked by
    // a fresh cache instance per call.

    #[test]
    fn mkdir_keeps_an_already_warmed_directory_correct_for_a_newly_created_sibling() {
        let (repo, _dir) = repo();
        let mut cache = super::NameCache::new(16);

        let a = repo
            .with_transaction(|conn, _cache| super::mkdir(conn, &mut cache, 0, "a", 100))
            .unwrap();
        assert_eq!(
            repo.with_connection(|conn, _cache| super::find_child_id_case_insensitive(
                conn, &mut cache, 0, "A"
            ))
            .unwrap(),
            Some(a),
            "the cache must be warm for the root directory now"
        );

        let b = repo
            .with_transaction(|conn, _cache| super::mkdir(conn, &mut cache, 0, "b", 100))
            .unwrap();
        // "b" must be found even though the cache was already warm before it was created - proves
        // `note_inserted` actually keeps an already-cached entry current, not just a freshly
        // populated one.
        assert_eq!(
            repo.with_connection(|conn, _cache| super::find_child_id_case_insensitive(
                conn, &mut cache, 0, "B"
            ))
            .unwrap(),
            Some(b)
        );
    }

    #[test]
    fn settle_file_keeps_an_already_warmed_directory_correct_for_a_newly_created_sibling() {
        let (repo, _dir) = repo();
        let mut cache = super::NameCache::new(16);
        let content_a = insert_content(&repo, 1, 0xAA);
        let content_b = insert_content(&repo, 2, 0xBB);

        let a = repo
            .with_transaction(|conn, _cache| {
                super::settle_file(conn, &mut cache, 0, "a.txt", 100, content_a)
            })
            .unwrap();
        assert_eq!(
            repo.with_connection(|conn, _cache| super::find_child_id_case_insensitive(
                conn, &mut cache, 0, "A.TXT"
            ))
            .unwrap(),
            Some(a),
            "the cache must be warm for the root directory now"
        );

        let b = repo
            .with_transaction(|conn, _cache| {
                super::settle_file(conn, &mut cache, 0, "b.txt", 100, content_b)
            })
            .unwrap();
        assert_eq!(
            repo.with_connection(|conn, _cache| super::find_child_id_case_insensitive(
                conn, &mut cache, 0, "B.TXT"
            ))
            .unwrap(),
            Some(b)
        );
    }

    #[test]
    fn settle_file_replacing_an_existing_file_keeps_the_cache_correct() {
        let (repo, _dir) = repo();
        let mut cache = super::NameCache::new(16);
        let content_a = insert_content(&repo, 1, 0xAA);
        let content_b = insert_content(&repo, 2, 0xBB);

        let first = repo
            .with_transaction(|conn, _cache| {
                super::settle_file(conn, &mut cache, 0, "a.txt", 100, content_a)
            })
            .unwrap();
        assert_eq!(
            repo.with_connection(|conn, _cache| super::find_child_id_case_insensitive(
                conn, &mut cache, 0, "A.TXT"
            ))
            .unwrap(),
            Some(first),
            "the cache must be warm for the root directory now"
        );

        let second = repo
            .with_transaction(|conn, _cache| {
                super::settle_file(conn, &mut cache, 0, "a.txt", 200, content_b)
            })
            .unwrap();
        assert_ne!(
            first, second,
            "settle_file creates a new history entry, not an in-place update"
        );

        // Must resolve to the new entry, not the replaced one - proves the invalidate-then-let-the-
        // next-miss-repopulate path actually runs, rather than a stale entry surviving the replace.
        assert_eq!(
            repo.with_connection(|conn, _cache| super::find_child_id_case_insensitive(
                conn, &mut cache, 0, "A.TXT"
            ))
            .unwrap(),
            Some(second)
        );
    }

    #[test]
    fn unlink_file_does_not_resurrect_a_file_removed_after_the_cache_was_warmed() {
        let (repo, _dir) = repo();
        let mut cache = super::NameCache::new(16);
        let content_id = insert_content(&repo, 1, 0xAA);

        let id = repo
            .with_transaction(|conn, _cache| {
                super::settle_file(conn, &mut cache, 0, "a.txt", 100, content_id)
            })
            .unwrap();
        assert_eq!(
            repo.with_connection(|conn, _cache| super::find_child_id_case_insensitive(
                conn, &mut cache, 0, "A.TXT"
            ))
            .unwrap(),
            Some(id),
            "the cache must be warm for the root directory now"
        );

        repo.with_transaction(|conn, _cache| super::unlink_file(conn, &mut cache, id, 200))
            .unwrap();

        assert_eq!(
            repo.with_connection(|conn, _cache| super::find_child_id_case_insensitive(
                conn, &mut cache, 0, "A.TXT"
            ))
            .unwrap(),
            None
        );
    }

    #[test]
    fn rename_within_the_same_directory_keeps_the_cache_correct_for_the_new_name() {
        let (repo, _dir) = repo();
        let mut cache = super::NameCache::new(16);

        let a = repo
            .with_transaction(|conn, _cache| super::mkdir(conn, &mut cache, 0, "a", 100))
            .unwrap();
        assert_eq!(
            repo.with_connection(|conn, _cache| super::find_child_id_case_insensitive(
                conn, &mut cache, 0, "A"
            ))
            .unwrap(),
            Some(a),
            "the cache must be warm for the root directory now"
        );

        repo.with_transaction(|conn, _cache| {
            super::rename(conn, &mut cache, 0, "a", 0, "renamed", false, 200)
        })
        .unwrap();

        assert_eq!(
            repo.with_connection(|conn, _cache| super::find_child_id_case_insensitive(
                conn, &mut cache, 0, "A"
            ))
            .unwrap(),
            None,
            "the old name must no longer resolve"
        );
        assert_eq!(
            repo.with_connection(|conn, _cache| super::find_child_id_case_insensitive(
                conn, &mut cache, 0, "RENAMED"
            ))
            .unwrap(),
            Some(a),
            "the new name must resolve, against the same, previously-warmed cache instance"
        );
    }

    #[test]
    fn rename_across_directories_keeps_the_cache_correct_in_both_parents() {
        let (repo, _dir) = repo();
        let mut cache = super::NameCache::new(16);

        let source = repo
            .with_transaction(|conn, _cache| super::mkdir(conn, &mut cache, 0, "source", 100))
            .unwrap();
        let target_dir = repo
            .with_transaction(|conn, _cache| super::mkdir(conn, &mut cache, 0, "target", 100))
            .unwrap();
        let a = repo
            .with_transaction(|conn, _cache| super::mkdir(conn, &mut cache, source, "a", 100))
            .unwrap();

        // Warm the cache for both the source and the (empty) target directory before the move.
        assert_eq!(
            repo.with_connection(|conn, _cache| super::find_child_id_case_insensitive(
                conn, &mut cache, source, "A"
            ))
            .unwrap(),
            Some(a)
        );
        assert_eq!(
            repo.with_connection(|conn, _cache| super::find_child_id_case_insensitive(
                conn, &mut cache, target_dir, "A"
            ))
            .unwrap(),
            None
        );

        repo.with_transaction(|conn, _cache| {
            super::rename(conn, &mut cache, source, "a", target_dir, "a", false, 200)
        })
        .unwrap();

        assert_eq!(
            repo.with_connection(|conn, _cache| super::find_child_id_case_insensitive(
                conn, &mut cache, source, "A"
            ))
            .unwrap(),
            None,
            "the old parent's cache must no longer show the moved entry"
        );
        assert_eq!(
            repo.with_connection(|conn, _cache| super::find_child_id_case_insensitive(
                conn, &mut cache, target_dir, "A"
            ))
            .unwrap(),
            Some(a),
            "the new parent's cache must show the moved entry"
        );
    }

    #[test]
    fn rename_replacing_an_existing_file_keeps_the_cache_correct() {
        let (repo, _dir) = repo();
        let mut cache = super::NameCache::new(16);
        let content_a = insert_content(&repo, 1, 0xAA);
        let content_b = insert_content(&repo, 2, 0xBB);

        let old = repo
            .with_transaction(|conn, _cache| {
                super::settle_file(conn, &mut cache, 0, "old.txt", 100, content_a)
            })
            .unwrap();
        repo.with_transaction(|conn, _cache| {
            super::settle_file(conn, &mut cache, 0, "new.txt", 100, content_b)
        })
        .unwrap();
        assert_eq!(
            repo.with_connection(|conn, _cache| super::find_child_id_case_insensitive(
                conn, &mut cache, 0, "OLD.TXT"
            ))
            .unwrap(),
            Some(old),
            "the cache must be warm for the root directory now"
        );

        repo.with_transaction(|conn, _cache| {
            super::rename(conn, &mut cache, 0, "old.txt", 0, "new.txt", false, 200)
        })
        .unwrap();

        // `rename` moves `old`'s own row rather than creating a new one, so "new.txt" must now
        // resolve to `old`'s id - not the replaced target's, and not a stale cached miss either.
        assert_eq!(
            repo.with_connection(|conn, _cache| super::find_child_id_case_insensitive(
                conn, &mut cache, 0, "OLD.TXT"
            ))
            .unwrap(),
            None
        );
        assert_eq!(
            repo.with_connection(|conn, _cache| super::find_child_id_case_insensitive(
                conn, &mut cache, 0, "NEW.TXT"
            ))
            .unwrap(),
            Some(old)
        );
    }

    // The base, case-sensitive behavior (REQ-MOUNT-010) must stay exactly as it was outside the
    // Windows fallback - this would fail if `find_child_id`'s `cfg!(windows)` dispatch above were
    // ever accidentally widened to every platform.

    #[test]
    #[cfg(not(windows))]
    fn mkdir_allows_a_case_only_variant_outside_windows() {
        let (repo, _dir) = repo();
        let foo = repo.mkdir(0, "foo", 100).unwrap();

        let variant = repo
            .mkdir(0, "Foo", 200)
            .expect("a case-only variant must succeed outside the Windows lookup fallback");
        assert_ne!(foo, variant);
    }

    // DESIGN-MOUNT-005's full Windows-only stack, through the public `Repository` API - cannot
    // run on this development platform, but is compiled and checked wherever a Windows build is
    // (the Docker cross-compile check, and real WinFSP via the `julius-winfsp-ssh` skill).

    #[test]
    #[cfg(windows)]
    fn mkdir_refuses_a_case_only_variant_on_windows() {
        let (repo, _dir) = repo();
        repo.mkdir(0, "foo", 100).unwrap();

        let err = repo.mkdir(0, "Foo", 200).unwrap_err();
        assert!(matches!(err, Error::EntryAlreadyExists { .. }));
    }

    #[test]
    #[cfg(windows)]
    fn rename_case_only_respelling_of_self_succeeds_and_updates_spelling() {
        // REQ-MOUNT-010's own example (install.txt -> Install.txt), exercised via mkdir since no
        // file-creation path exists yet (REQ-STORAGE-007) - the distinction does not matter here,
        // the self-identity check runs before any file/directory-kind logic.
        let (repo, _dir) = repo();
        let id = repo.mkdir(0, "install.txt", 100).unwrap();

        repo.rename(0, "install.txt", 0, "Install.txt", false, 200)
            .expect("a case-only respelling of the same entry must succeed");

        let entry = repo.resolve_path("/Install.txt").unwrap().unwrap();
        assert_eq!(entry.id, id);
        // Lookup stays case-insensitive regardless of which spelling is currently stored.
        assert_eq!(repo.resolve_path("/install.txt").unwrap().unwrap().id, id);
        let names: Vec<String> = repo
            .list_children(0)
            .unwrap()
            .into_iter()
            .map(|(name, _)| name)
            .collect();
        assert_eq!(names, vec!["Install.txt".to_string()]);
    }

    #[test]
    #[cfg(windows)]
    fn rename_refuses_a_different_entrys_case_variant_directory() {
        let (repo, _dir) = repo();
        repo.mkdir(0, "a", 100).unwrap();
        repo.mkdir(0, "B", 100).unwrap();

        // "b" does not exist exactly, but case-insensitively resolves to "B" - REQ-MOUNT-009's
        // directory-collision refusal still applies to that resolved target.
        let err = repo.rename(0, "a", 0, "b", false, 200).unwrap_err();
        assert!(matches!(err, Error::EntryAlreadyExists { .. }));
    }

    #[test]
    #[cfg(windows)]
    fn rename_replaces_a_different_entrys_case_variant_file() {
        let (repo, _dir) = repo();
        repo.with_connection(|conn, _cache| {
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
                 VALUES (2, 0, 'New.txt', 0, 2, 1)",
                (),
            )?;
            Ok(())
        })
        .unwrap();

        // "new.txt" does not exist exactly, but case-insensitively resolves to "New.txt" - the
        // existing file-replaces-file rule (REQ-MOUNT-009) still applies to that resolved target.
        repo.rename(0, "old.txt", 0, "new.txt", false, 200)
            .expect("replacing an existing file's case variant must succeed");
        let replaced = repo.resolve_path("/new.txt").unwrap().unwrap();
        assert_eq!(replaced.id, 1);
    }
}
