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

use rusqlite::{Connection, OptionalExtension, params};

use crate::Error;
use crate::name_cache::NameCache;

mod case_insensitive;
use case_insensitive::{find_child_id_case_insensitive, note_child_inserted};

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

/// One soft-deleted entry, as exposed through REQ-TREE-009's `[deleted]` addressing
/// (`requirements/functional/tree.md`) - an [`Entry`]'s own fields (kind, real
/// content-modification time, content) plus its own deletion timestamp.
#[derive(Debug, Clone, Copy)]
pub struct DeletedEntry {
    pub entry: Entry,
    pub deleted_at: i64,
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

/// The soft-deleted entry `id`, if it exists and is currently soft-deleted - `None` for a live or
/// nonexistent `id` alike, the same "only this one state" symmetry [`get_by_id`] has for the live
/// case. REQ-MOUNT-004/007's own read access to a `[deleted]`-addressed file's content needs this:
/// `crate::dedup_fs`'s `open`/`read` only ever have the raw id a prior `resolve` call already
/// established was soft-deleted, not the full path to re-resolve through
/// [`list_deleted_children`] again.
pub(crate) fn deleted_entry_by_id(
    conn: &Connection,
    id: i64,
) -> Result<Option<DeletedEntry>, Error> {
    conn.query_row(
        "SELECT te.kind, te.time, te.content_id, c.length, te.deleted_at \
         FROM tree_entries te LEFT JOIN contents c ON c.id = te.content_id \
         WHERE te.id = ?1 AND te.deleted_at IS NOT NULL",
        params![id],
        |row| {
            let length: Option<i64> = row.get(3)?;
            Ok(DeletedEntry {
                entry: Entry {
                    id,
                    kind: EntryKind::from_db(row.get(0)?),
                    time_millis: row.get(1)?,
                    content_id: row.get(2)?,
                    size: length.unwrap_or(0) as u64,
                },
                deleted_at: row.get(4)?,
            })
        },
    )
    .optional()
    .map_err(Error::from)
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
) -> Result<Vec<(String, Entry)>, Error> {
    require_dir(conn, parent_id)?;

    let mut stmt = conn.prepare(
        "SELECT te.id, te.name, te.kind, te.time, te.content_id, c.length \
         FROM tree_entries te LEFT JOIN contents c ON c.id = te.content_id \
         WHERE te.parent_id = ?1 AND te.deleted_at IS NULL AND te.id != 0",
    )?;
    let rows = stmt.query_map(params![parent_id], |row| {
        let id: i64 = row.get(0)?;
        let name: String = row.get(1)?;
        let kind: i64 = row.get(2)?;
        let time_millis: i64 = row.get(3)?;
        let content_id: Option<i64> = row.get(4)?;
        let length: Option<i64> = row.get(5)?;
        Ok((
            name,
            Entry {
                id,
                kind: EntryKind::from_db(kind),
                time_millis,
                content_id,
                size: length.unwrap_or(0) as u64,
            },
        ))
    })?;
    rows.collect::<Result<Vec<_>, _>>().map_err(Error::from)
}

/// Live entries anywhere in the repository whose own name matches `name_pattern` - REQ-QUERY-002.
/// `name_pattern` uses shell-style `*`/`?` wildcards (translated to SQL `LIKE`'s `%`/`_`), matched
/// case-insensitively - SQLite's own `LIKE` default, ASCII-only, the same scope REQ-MOUNT-010's own
/// case-folding elsewhere in this codebase settles for. Each match is paired with its own full path
/// from the root (`/`-separated, e.g. `/a/b/c.txt`), reconstructed via its ancestor chain - not the
/// bare name [`list_children`] already returns, since a caller searching "anywhere in the
/// repository" needs to know *where* a match actually is.
pub(crate) fn find(conn: &Connection, name_pattern: &str) -> Result<Vec<(String, Entry)>, Error> {
    let like_pattern = to_like_pattern(name_pattern);
    let mut stmt = conn.prepare(
        "SELECT te.id, te.kind, te.time, te.content_id, c.length \
         FROM tree_entries te LEFT JOIN contents c ON c.id = te.content_id \
         WHERE te.deleted_at IS NULL AND te.id != 0 AND te.name LIKE ?1 ESCAPE '\\'",
    )?;
    let rows = stmt.query_map(params![like_pattern], |row| {
        let id: i64 = row.get(0)?;
        let kind: i64 = row.get(1)?;
        let time_millis: i64 = row.get(2)?;
        let content_id: Option<i64> = row.get(3)?;
        let length: Option<i64> = row.get(4)?;
        Ok(Entry {
            id,
            kind: EntryKind::from_db(kind),
            time_millis,
            content_id,
            size: length.unwrap_or(0) as u64,
        })
    })?;
    let entries = rows.collect::<Result<Vec<_>, _>>()?;

    let mut results = Vec::with_capacity(entries.len());
    for entry in entries {
        let path = full_path(conn, entry.id)?;
        results.push((path, entry));
    }
    Ok(results)
}

/// `id`'s own full path from the root, `/`-separated (e.g. `/a/b/c.txt`) - walks the live ancestor
/// chain via `parent_id`, which is always unbroken up to the root for a live `id` (REQ-TREE-008: a
/// live entry can never have a soft-deleted parent). `id == 0` (the root itself) yields `/`.
fn full_path(conn: &Connection, id: i64) -> Result<String, Error> {
    if id == 0 {
        return Ok("/".to_string());
    }
    let mut segments = Vec::new();
    let mut current_id = id;
    while current_id != 0 {
        let (parent_id, name): (i64, String) = conn.query_row(
            "SELECT parent_id, name FROM tree_entries WHERE id = ?1",
            params![current_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        segments.push(name);
        current_id = parent_id;
    }
    segments.reverse();
    Ok(format!("/{}", segments.join("/")))
}

/// Translates `pattern`'s shell-style `*`/`?` wildcards into a SQL `LIKE` pattern (`%`/`_`),
/// backslash-escaping any of `LIKE`'s own special characters (`%`, `_`, `\`) that appear literally
/// in `pattern` so they are matched exactly rather than as wildcards - paired with the caller's own
/// `ESCAPE '\'` clause.
fn to_like_pattern(pattern: &str) -> String {
    let mut out = String::with_capacity(pattern.len());
    for ch in pattern.chars() {
        match ch {
            '*' => out.push('%'),
            '?' => out.push('_'),
            '%' | '_' | '\\' => {
                out.push('\\');
                out.push(ch);
            }
            other => out.push(other),
        }
    }
    out
}

/// Like [`require_dir`], but also accepts a soft-deleted directory - REQ-TREE-009's `[deleted]`
/// addressing needs to descend into an already-deleted directory's own soft-deleted children.
/// REQ-TREE-008 guarantees every child of a soft-deleted directory is itself soft-deleted (a live
/// child can never be created under a non-live parent), so there is no live case left to handle
/// once inside one.
fn require_dir_any_state(conn: &Connection, id: i64) -> Result<(), Error> {
    let kind: Option<i64> = conn
        .query_row(
            "SELECT kind FROM tree_entries WHERE id = ?1",
            params![id],
            |row| row.get(0),
        )
        .optional()?;
    match kind {
        Some(k) if k == KIND_DIR => Ok(()),
        Some(_) => Err(Error::WrongKind(id)),
        None => Err(Error::NoSuchEntry(id)),
    }
}

/// `parent_id`'s own soft-deleted children (REQ-TREE-009) - `parent_id` may itself be a live or
/// already soft-deleted directory (see [`require_dir_any_state`]). Each is paired with the raw
/// name it was stored under, exactly as a live child would be from [`list_children`] - REQ-TREE-009's
/// display-name disambiguation is a presentation concern for the caller, not decided here.
pub(crate) fn list_deleted_children(
    conn: &Connection,
    parent_id: i64,
) -> Result<Vec<(String, DeletedEntry)>, Error> {
    require_dir_any_state(conn, parent_id)?;

    let mut stmt = conn.prepare(
        "SELECT te.id, te.name, te.kind, te.time, te.content_id, c.length, te.deleted_at \
         FROM tree_entries te LEFT JOIN contents c ON c.id = te.content_id \
         WHERE te.parent_id = ?1 AND te.deleted_at IS NOT NULL",
    )?;
    let rows = stmt.query_map(params![parent_id], |row| {
        let id: i64 = row.get(0)?;
        let name: String = row.get(1)?;
        let kind: i64 = row.get(2)?;
        let time_millis: i64 = row.get(3)?;
        let content_id: Option<i64> = row.get(4)?;
        let length: Option<i64> = row.get(5)?;
        let deleted_at: i64 = row.get(6)?;
        Ok((
            name,
            DeletedEntry {
                entry: Entry {
                    id,
                    kind: EntryKind::from_db(kind),
                    time_millis,
                    content_id,
                    size: length.unwrap_or(0) as u64,
                },
                deleted_at,
            },
        ))
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
            note_child_inserted(cache, parent_id, id, name);
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
        conn.execute(
            "UPDATE tree_entries SET deleted_at = ?1 WHERE id = ?2",
            params![time_millis, old_id],
        )?;
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
    note_child_inserted(cache, parent_id, id, name);

    if replaced.is_none() {
        touch(conn, parent_id, time_millis)?;
    }
    Ok(id)
}

/// `settle_pending_write`'s outcome - either it landed, or its target was gone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SettleOutcome {
    /// The base row was still live; carries the new row's own id.
    Committed(i64),
    /// The base row was no longer live - a background settle job's own content lost a race
    /// against a concurrent delete of the file it belongs to (see `settle_pending_write`'s own
    /// doc comment). Nothing was written to the tree; `reclaimed_bytes` is what became eligible
    /// for reuse by reclaiming the now-unreferenced `content_id` immediately, the same cascade
    /// `purge_deleted_entry` already performs for its own orphaned content.
    Abandoned { reclaimed_bytes: u64 },
}

/// Commits a background settle job's finished content (DESIGN-MOUNT-006) against `base_row_id` -
/// the id of whatever row this generation's own content is meant to replace, resolved by the
/// caller (`crate::settle_pool::commit` in `crates/cli`) via `GenerationSlot::resolve_or_defer`:
/// a file's own row, for its first generation this session, or its immediate predecessor's own
/// resulting row otherwise, however deep the chain. Its liveness is re-verified right now, inside
/// this same transaction, rather than trusted from whenever the job was submitted or its
/// predecessor settled. Closes DESIGN-MOUNT-015's "Known limitation": a settle job that used to
/// commit blindly against a `(parent_id, name)` snapshot taken at release time could resurrect a
/// name a racing `unlink` had already removed, since nothing at that name any longer meant nothing
/// to replace, not nothing to write.
///
/// - Still live: `base_row_id`'s row is replaced (soft-deleted, or hard-deleted instead if
///   `collapsible_placeholder_id == Some(base_row_id)` - DESIGN-MOUNT-016) and the new content
///   lands at its *current* `(parent_id, name)` - correct even if the file was renamed since the
///   write began, not only if it stayed put.
/// - No longer live (a real `unlink`/`rmdir` raced this job and won): nothing is written -
///   [`SettleOutcome::Abandoned`], with `content_id`'s now-orphaned storage reclaimed immediately
///   rather than left to leak (REQ-STORAGE-004's reclaim sweep only ever walks soft-deleted
///   `tree_entries` rows, so content that never gained one is not something it would ever find).
///
/// No directory-kind check on `base_row_id`, unlike [`settle_file`]'s own name-based collision
/// check: `base_row_id` always originates from a real file's own id, by construction of its only
/// caller (`PendingFiles` in `crates/cli`, itself only ever populated from a mount `open`/`create`
/// call, both of which already refuse a directory) - trusted rather than re-checked here.
pub(crate) fn settle_pending_write(
    conn: &Connection,
    cache: &mut NameCache,
    base_row_id: i64,
    time_millis: i64,
    content_id: i64,
    collapsible_placeholder_id: Option<i64>,
) -> Result<SettleOutcome, Error> {
    let row: Option<(i64, String, Option<i64>)> = conn
        .query_row(
            "SELECT parent_id, name, deleted_at FROM tree_entries WHERE id = ?1",
            params![base_row_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    let live = match row {
        Some((parent_id, name, None)) => Some((parent_id, name)),
        _ => None,
    };
    let Some((parent_id, name)) = live else {
        let reclaimed_bytes = crate::content::reclaim_content(conn, content_id)?;
        return Ok(SettleOutcome::Abandoned { reclaimed_bytes });
    };

    if collapsible_placeholder_id == Some(base_row_id) {
        // See settle_file_impl's own identical branch: base_row_id still being live already
        // proves it is still exactly the row the caller inserted, holding its original content
        // unmodified (tree_entries.id is AUTOINCREMENT).
        conn.execute(
            "DELETE FROM tree_entries WHERE id = ?1",
            params![base_row_id],
        )?;
    } else {
        conn.execute(
            "UPDATE tree_entries SET deleted_at = ?1 WHERE id = ?2",
            params![time_millis, base_row_id],
        )?;
    }
    cache.invalidate(parent_id);

    conn.execute(
        "INSERT INTO tree_entries (parent_id, name, time, content_id, kind) \
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![parent_id, name, time_millis, content_id, KIND_FILE],
    )?;
    let id = conn.last_insert_rowid();
    note_child_inserted(cache, parent_id, id, &name);
    Ok(SettleOutcome::Committed(id))
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

/// `purge_deleted_entry`'s own result: how many descendants it purged alongside the entry itself
/// (not counting the entry itself - `0` for a file or an empty directory), and how many bytes its
/// reclaim cascade freed in the process (across the entry itself and every purged descendant).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PurgeResult {
    pub descendants: u64,
    pub reclaimed_bytes: u64,
}

/// Permanently removes the soft-deleted entry `id` from the tree - REQ-CLI-003's `--purge` case.
/// Refuses an `id` that does not exist ([`Error::NoSuchEntry`]) or that is still live
/// ([`Error::NotSoftDeleted`]) - only an entry already reached through REQ-TREE-009's `[deleted]`
/// addressing is eligible. If `id` is a directory with soft-deleted children (REQ-TREE-008
/// guarantees none of them are live) and `recursive` is `false`, refuses with
/// [`Error::DirectoryNotEmpty`] instead of purging them - the same shape of opt-in `rmdir` already
/// makes for a live, non-empty directory. With `recursive` `true`, its children are purged first,
/// since `tree_entries.parent_id`'s foreign key would otherwise refuse deleting a row still
/// referenced by a child.
///
/// The `tree_entries_ref_count_del` trigger (`migrations.rs`) decrements each purged file's
/// `contents.ref_count` as a side effect of its row's own deletion. Reaching zero there does not by
/// itself reclaim any stored bytes, so this also performs REQ-STORAGE-004's reclaim cascade
/// immediately afterward, scoped to exactly the `contents` rows this purge just orphaned (see
/// [`crate::content::reclaim_content`]) - "Purging a tree entry is also an arming point" in
/// `docs/design/stale-backup-detection.md` records why purge does this itself rather than leaving
/// it to a later, separate sweep.
pub(crate) fn purge_deleted_entry(
    conn: &Connection,
    id: i64,
    recursive: bool,
) -> Result<PurgeResult, Error> {
    if id == 0 {
        // tree_entries_protect_root's own DELETE trigger would already refuse this (unlike
        // rmdir/unlink_file's soft-delete UPDATE, which it does not cover), but only as a raw
        // SQLite abort - guarded here explicitly instead, for the same clear error every other
        // root-removal attempt gets.
        return Err(Error::CannotRemoveRoot);
    }
    let (kind, deleted_at, content_id): (i64, Option<i64>, Option<i64>) = conn
        .query_row(
            "SELECT kind, deleted_at, content_id FROM tree_entries WHERE id = ?1",
            params![id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?
        .ok_or(Error::NoSuchEntry(id))?;
    if deleted_at.is_none() {
        return Err(Error::NotSoftDeleted(id));
    }

    let mut result = PurgeResult::default();
    if kind == KIND_DIR {
        let child_ids: Vec<i64> = conn
            .prepare("SELECT id FROM tree_entries WHERE parent_id = ?1")?
            .query_map(params![id], |row| row.get(0))?
            .collect::<Result<Vec<_>, _>>()?;
        if !recursive && !child_ids.is_empty() {
            return Err(Error::DirectoryNotEmpty(id));
        }
        for child_id in child_ids {
            let child_result = purge_deleted_entry(conn, child_id, recursive)?;
            result.descendants += 1 + child_result.descendants;
            result.reclaimed_bytes += child_result.reclaimed_bytes;
        }
    }

    conn.execute("DELETE FROM tree_entries WHERE id = ?1", params![id])?;
    if let Some(content_id) = content_id {
        result.reclaimed_bytes += crate::content::reclaim_content(conn, content_id)?;
    }
    Ok(result)
}

/// Recovers the soft-deleted entry `id` back to a live entry at `(new_parent_id, new_name)` -
/// REQ-MOUNT-004's own "moved back out of the `[deleted]` view" recovery. Refuses an `id` that
/// does not exist ([`Error::NoSuchEntry`]) or is not currently soft-deleted
/// ([`Error::NotSoftDeleted`]). `new_parent_id` must be a live directory
/// ([`Error::WrongKind`]/[`Error::NoSuchEntry`], the same as [`rename`]'s own target-parent check).
/// Collision handling at `(new_parent_id, new_name)` mirrors [`rename`]'s own rules
/// (REQ-MOUNT-009): a directory on either side is always refused, `no_replace` always refuses,
/// otherwise an existing live file there is itself soft-deleted first. No cycle check is needed
/// the way [`rename`] needs one: `new_parent_id` being required live already rules out `id` being
/// one of its own ancestors, since REQ-TREE-008 guarantees every descendant of a soft-deleted
/// directory is itself soft-deleted too.
pub(crate) fn recover_deleted_entry(
    conn: &Connection,
    cache: &mut NameCache,
    id: i64,
    new_parent_id: i64,
    new_name: &str,
    no_replace: bool,
    time_millis: i64,
) -> Result<(), Error> {
    let (deleted_at, kind): (Option<i64>, i64) = conn
        .query_row(
            "SELECT deleted_at, kind FROM tree_entries WHERE id = ?1",
            params![id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?
        .ok_or(Error::NoSuchEntry(id))?;
    if deleted_at.is_none() {
        return Err(Error::NotSoftDeleted(id));
    }
    let kind = EntryKind::from_db(kind);

    require_dir(conn, new_parent_id)?;

    if let Some(target_id) = find_child_id(conn, cache, new_parent_id, new_name)? {
        if no_replace {
            return Err(Error::EntryAlreadyExists {
                parent_id: new_parent_id,
                name: new_name.to_string(),
            });
        }
        let target_entry = get_by_id(conn, target_id)?.ok_or(Error::NoSuchEntry(target_id))?;
        // REQ-MOUNT-009: a directory on either side of the collision is always refused.
        if kind == EntryKind::Dir || target_entry.kind == EntryKind::Dir {
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
        "UPDATE tree_entries SET parent_id = ?1, name = ?2, deleted_at = NULL WHERE id = ?3",
        params![new_parent_id, new_name, id],
    )?;
    cache.invalidate(new_parent_id);
    touch(conn, new_parent_id, time_millis)?;
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
    use crate::{Error, RepositorySettings, SettleOutcome, init_repository, open_repository};

    // Returns the TempDir alongside the Repository - it must outlive every use of the
    // Repository (dropping it deletes the directory the open connection points at).
    fn repo() -> (crate::Repository, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let repo_root = dir.path().join("repo");
        let settings = RepositorySettings::new(20, 1_700_000_000_000);
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
    fn settle_pending_write_commits_when_the_base_row_is_still_live() {
        let (repo, _dir) = repo();
        let old_content = insert_content(&repo, 1, 0xAA);
        let new_content = insert_content(&repo, 2, 0xBB);
        let base_id = repo.settle_file(0, "a.txt", 100, old_content).unwrap();
        let root_time_before = repo.resolve_path("/").unwrap().unwrap().time_millis;

        let outcome = repo
            .settle_pending_write(base_id, 200, new_content, None)
            .unwrap();

        let SettleOutcome::Committed(new_id) = outcome else {
            panic!("expected Committed, got {outcome:?}");
        };
        assert_ne!(new_id, base_id);
        let live = repo.resolve_path("/a.txt").unwrap().unwrap();
        assert_eq!(live.id, new_id);
        assert_eq!(live.content_id, Some(new_content));
        let old_deleted_at: Option<i64> = repo
            .with_connection(|conn, _cache| {
                Ok(conn.query_row(
                    "SELECT deleted_at FROM tree_entries WHERE id = ?1",
                    [base_id],
                    |row| row.get(0),
                )?)
            })
            .unwrap();
        assert_eq!(
            old_deleted_at,
            Some(200),
            "the old row must become its own history entry, not vanish"
        );
        assert_eq!(
            repo.resolve_path("/").unwrap().unwrap().time_millis,
            root_time_before,
            "replacing an already-live file's content is not a structural change"
        );
    }

    #[test]
    fn settle_pending_write_commits_at_the_current_location_after_a_rename() {
        let (repo, _dir) = repo();
        let dir_id = repo.mkdir(0, "moved-to", 100).unwrap();
        let old_content = insert_content(&repo, 1, 0xAA);
        let new_content = insert_content(&repo, 2, 0xBB);
        let base_id = repo.settle_file(0, "a.txt", 100, old_content).unwrap();

        // The file is renamed away while this write's settle job is still in flight - the commit
        // below must land at the new location, not the stale one the write started against.
        repo.rename(0, "a.txt", dir_id, "b.txt", false, 150)
            .unwrap();

        let outcome = repo
            .settle_pending_write(base_id, 200, new_content, None)
            .unwrap();

        assert!(matches!(outcome, SettleOutcome::Committed(_)));
        assert!(repo.resolve_path("/a.txt").unwrap().is_none());
        let live = repo.resolve_path("/moved-to/b.txt").unwrap().unwrap();
        assert_eq!(live.content_id, Some(new_content));
    }

    #[test]
    fn settle_pending_write_collapses_the_placeholder_when_requested() {
        let (repo, _dir) = repo();
        let empty_content = insert_content(&repo, 0, 0xEE);
        let real_content = insert_content(&repo, 5, 0xAA);
        let placeholder_id = repo.settle_file(0, "a.txt", 100, empty_content).unwrap();

        let outcome = repo
            .settle_pending_write(placeholder_id, 200, real_content, Some(placeholder_id))
            .unwrap();

        let SettleOutcome::Committed(new_id) = outcome else {
            panic!("expected Committed, got {outcome:?}");
        };
        assert_ne!(new_id, placeholder_id);
        assert_eq!(
            row_count(&repo, placeholder_id),
            0,
            "the placeholder row must be gone entirely, not merely soft-deleted"
        );
    }

    #[test]
    fn settle_pending_write_soft_deletes_when_the_placeholder_id_does_not_match_base_row_id() {
        let (repo, _dir) = repo();
        let old_content = insert_content(&repo, 1, 0xAA);
        let new_content = insert_content(&repo, 2, 0xBB);
        let base_id = repo.settle_file(0, "a.txt", 100, old_content).unwrap();
        // A stale/mismatched expected id (e.g. from a different, unrelated generation) must not
        // cause base_id to be hard-deleted - only an exact match does that.
        let unrelated_id = base_id + 1000;

        let outcome = repo
            .settle_pending_write(base_id, 200, new_content, Some(unrelated_id))
            .unwrap();

        assert!(matches!(outcome, SettleOutcome::Committed(_)));
        assert_eq!(
            row_count(&repo, base_id),
            1,
            "a mismatched expected id must fall back to the ordinary, history-preserving replace"
        );
        let old_deleted_at: Option<i64> = repo
            .with_connection(|conn, _cache| {
                Ok(conn.query_row(
                    "SELECT deleted_at FROM tree_entries WHERE id = ?1",
                    [base_id],
                    |row| row.get(0),
                )?)
            })
            .unwrap();
        assert_eq!(old_deleted_at, Some(200));
    }

    #[test]
    fn settle_pending_write_abandons_and_reclaims_when_the_base_row_was_unlinked() {
        let (repo, _dir) = repo();
        let old_content = insert_content(&repo, 1, 0xCC);
        let base_id = repo.settle_file(0, "a.txt", 100, old_content).unwrap();
        let (chunk_id, _ranges) = repo.reserve_and_insert_chunk(10, &[0xAA; 20]).unwrap();
        let new_content = repo
            .find_or_create_content(10, &[0xBB; 20], &[chunk_id])
            .unwrap();

        // The exact race DESIGN-MOUNT-015's own "Known limitation" describes: a client deletes the
        // file while its own just-finished write is still waiting to be committed by the
        // background settle job.
        repo.unlink_file(base_id, 150).unwrap();

        let outcome = repo
            .settle_pending_write(base_id, 200, new_content, None)
            .unwrap();

        assert_eq!(
            outcome,
            SettleOutcome::Abandoned {
                reclaimed_bytes: 10
            },
            "the sole reference's own chunk_extents range must be counted as reclaimed"
        );
        assert!(
            repo.resolve_path("/a.txt").unwrap().is_none(),
            "the file must not resurrect under its old name"
        );
        let chunk_left: i64 = repo
            .with_connection(|conn, _cache| {
                Ok(conn.query_row(
                    "SELECT COUNT(*) FROM chunks WHERE id = ?1",
                    [chunk_id],
                    |row| row.get(0),
                )?)
            })
            .unwrap();
        assert_eq!(
            chunk_left, 0,
            "abandoning a write must not leak the content it already wrote to the store"
        );
    }

    #[test]
    fn settle_pending_write_abandons_when_the_base_row_id_never_existed() {
        let (repo, _dir) = repo();
        let content_id = insert_content(&repo, 1, 0xAA);

        let outcome = repo
            .settle_pending_write(999, 200, content_id, None)
            .unwrap();

        assert_eq!(outcome, SettleOutcome::Abandoned { reclaimed_bytes: 0 });
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
    fn find_matches_a_name_anywhere_in_the_repository_with_its_full_path() {
        let (repo, _dir) = repo();
        let dir_id = repo.mkdir(0, "photos", 100).unwrap();
        let content_id = insert_content(&repo, 1, 0xAA);
        repo.settle_file(dir_id, "one.jpg", 100, content_id)
            .unwrap();

        let matches = repo.find("one.jpg").unwrap();
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].0, "/photos/one.jpg");
        assert_eq!(matches[0].1.kind, crate::EntryKind::File);
    }

    #[test]
    fn find_supports_star_and_question_mark_wildcards() {
        let (repo, _dir) = repo();
        let content_id = insert_content(&repo, 1, 0xAA);
        repo.settle_file(0, "a.jpg", 100, content_id).unwrap();
        let content_id_2 = insert_content(&repo, 2, 0xBB);
        repo.settle_file(0, "ab.jpg", 100, content_id_2).unwrap();
        let content_id_3 = insert_content(&repo, 3, 0xCC);
        repo.settle_file(0, "a.txt", 100, content_id_3).unwrap();

        let star_matches: Vec<String> = repo
            .find("*.jpg")
            .unwrap()
            .into_iter()
            .map(|(path, _)| path)
            .collect();
        assert_eq!(star_matches.len(), 2, "got: {star_matches:?}");

        let question_matches: Vec<String> = repo
            .find("a?jpg")
            .unwrap()
            .into_iter()
            .map(|(path, _)| path)
            .collect();
        assert_eq!(question_matches, vec!["/a.jpg".to_string()]);
    }

    #[test]
    fn find_is_case_insensitive() {
        let (repo, _dir) = repo();
        let content_id = insert_content(&repo, 1, 0xAA);
        repo.settle_file(0, "README.txt", 100, content_id).unwrap();

        let matches = repo.find("readme.txt").unwrap();
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].0, "/README.txt");
    }

    #[test]
    fn find_treats_a_literal_percent_or_underscore_in_the_pattern_literally() {
        let (repo, _dir) = repo();
        let content_id = insert_content(&repo, 1, 0xAA);
        repo.settle_file(0, "100%_done.txt", 100, content_id)
            .unwrap();
        let content_id_2 = insert_content(&repo, 2, 0xBB);
        repo.settle_file(0, "100X_done.txt", 100, content_id_2)
            .unwrap();

        let matches = repo.find("100%_done.txt").unwrap();
        assert_eq!(
            matches.len(),
            1,
            "a literal % in the pattern must not act as a SQL LIKE wildcard: {matches:?}"
        );
        assert_eq!(matches[0].0, "/100%_done.txt");
    }

    #[test]
    fn find_never_matches_a_soft_deleted_entry() {
        let (repo, _dir) = repo();
        let content_id = insert_content(&repo, 1, 0xAA);
        let id = repo.settle_file(0, "gone.txt", 100, content_id).unwrap();
        repo.unlink_file(id, 200).unwrap();

        assert!(repo.find("gone.txt").unwrap().is_empty());
    }

    #[test]
    fn find_reports_no_matches_as_an_empty_vec_not_an_error() {
        let (repo, _dir) = repo();
        assert!(repo.find("nothing-like-this-exists").unwrap().is_empty());
    }

    #[test]
    fn list_deleted_children_lists_only_soft_deleted_direct_children() {
        let (repo, _dir) = repo();
        let a_id = repo.mkdir(0, "a", 100).unwrap();
        repo.mkdir(0, "b", 100).unwrap();
        repo.rmdir(a_id, 200).unwrap();

        let deleted = repo.list_deleted_children(0).unwrap();
        assert_eq!(deleted.len(), 1);
        assert_eq!(deleted[0].0, "a");
        assert_eq!(deleted[0].1.entry.id, a_id);
        assert_eq!(deleted[0].1.deleted_at, 200);

        let live = repo.list_children(0).unwrap();
        assert_eq!(live.len(), 1);
        assert_eq!(live[0].0, "b");
    }

    #[test]
    fn list_deleted_children_keeps_every_independent_history_entry_for_the_same_name() {
        let (repo, _dir) = repo();
        let content_a = insert_content(&repo, 1, 0xAA);
        let content_b = insert_content(&repo, 2, 0xBB);
        repo.settle_file(0, "a.txt", 100, content_a).unwrap();
        repo.settle_file(0, "a.txt", 200, content_b).unwrap();
        // The second settle_file soft-deletes the first as a side effect, then is itself still
        // live - unlink it too so both history entries for "a.txt" are soft-deleted.
        let live = repo.resolve_path("/a.txt").unwrap().unwrap();
        repo.unlink_file(live.id, 300).unwrap();

        let mut deleted = repo.list_deleted_children(0).unwrap();
        deleted.sort_by_key(|(_, entry)| entry.deleted_at);
        assert_eq!(deleted.len(), 2);
        assert_eq!(deleted[0].1.deleted_at, 200);
        assert_eq!(deleted[1].1.deleted_at, 300);
    }

    #[test]
    fn list_deleted_children_descends_into_an_already_deleted_directory() {
        let (repo, _dir) = repo();
        let a_id = repo.mkdir(0, "a", 100).unwrap();
        let content_id = insert_content(&repo, 1, 0xAA);
        let file_id = repo.settle_file(a_id, "f.txt", 100, content_id).unwrap();
        repo.unlink_file(file_id, 150).unwrap();
        repo.rmdir(a_id, 200).unwrap();

        // "a" is itself soft-deleted now, but its own soft-deleted child "f.txt" is still
        // reachable by descending into it directly (REQ-TREE-009's nested [deleted] case).
        let deleted = repo.list_deleted_children(a_id).unwrap();
        assert_eq!(deleted.len(), 1);
        assert_eq!(deleted[0].0, "f.txt");
        assert_eq!(deleted[0].1.entry.id, file_id);
    }

    #[test]
    fn list_deleted_children_refuses_a_file() {
        let (repo, _dir) = repo();
        let content_id = insert_content(&repo, 1, 0xAA);
        let id = repo.settle_file(0, "a.txt", 100, content_id).unwrap();

        let err = repo.list_deleted_children(id).unwrap_err();
        assert!(matches!(err, Error::WrongKind(_)));
    }

    #[test]
    fn list_deleted_children_refuses_a_nonexistent_id() {
        let (repo, _dir) = repo();
        let err = repo.list_deleted_children(999).unwrap_err();
        assert!(matches!(err, Error::NoSuchEntry(999)));
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
    fn purge_deleted_entry_hard_deletes_a_soft_deleted_file() {
        let (repo, _dir) = repo();
        let content_id = insert_content(&repo, 1, 0xAA);
        let id = repo.settle_file(0, "a.txt", 100, content_id).unwrap();
        repo.unlink_file(id, 200).unwrap();

        let result = repo
            .purge_deleted_entry(id, true)
            .expect("purge must succeed");

        assert_eq!(
            row_count(&repo, id),
            0,
            "the purged row must be gone entirely, not merely soft-deleted"
        );
        assert_eq!(
            result.descendants, 0,
            "a plain file has no descendants to count"
        );
    }

    #[test]
    fn purge_deleted_entry_decrements_the_content_ref_count_while_still_referenced_elsewhere() {
        let (repo, _dir) = repo();
        let content_id = insert_content(&repo, 1, 0xAA);
        let id = repo.settle_file(0, "a.txt", 100, content_id).unwrap();
        // A second live entry shares the same content_id, so its ref_count survives the purge
        // below instead of reaching zero and being reclaimed away (see
        // purge_deleted_entry_reclaims_bytes_for_content_nothing_else_references for that case).
        repo.settle_file(0, "b.txt", 100, content_id).unwrap();
        repo.unlink_file(id, 200).unwrap();
        let ref_count_before: i64 = repo
            .with_connection(|conn, _cache| {
                Ok(conn.query_row(
                    "SELECT ref_count FROM contents WHERE id = ?1",
                    [content_id],
                    |row| row.get(0),
                )?)
            })
            .unwrap();

        repo.purge_deleted_entry(id, true).unwrap();

        let ref_count_after: i64 = repo
            .with_connection(|conn, _cache| {
                Ok(conn.query_row(
                    "SELECT ref_count FROM contents WHERE id = ?1",
                    [content_id],
                    |row| row.get(0),
                )?)
            })
            .unwrap();
        assert_eq!(ref_count_after, ref_count_before - 1);
    }

    #[test]
    fn purge_deleted_entry_refuses_a_live_entry() {
        let (repo, _dir) = repo();
        let content_id = insert_content(&repo, 1, 0xAA);
        let id = repo.settle_file(0, "a.txt", 100, content_id).unwrap();

        let err = repo.purge_deleted_entry(id, true).unwrap_err();
        assert!(matches!(err, Error::NotSoftDeleted(_)));
        assert_eq!(
            row_count(&repo, id),
            1,
            "a refused purge must not touch the row"
        );
    }

    #[test]
    fn purge_deleted_entry_refuses_a_nonexistent_id() {
        let (repo, _dir) = repo();
        let err = repo.purge_deleted_entry(999, true).unwrap_err();
        assert!(matches!(err, Error::NoSuchEntry(999)));
    }

    #[test]
    fn purge_deleted_entry_refuses_the_root() {
        let (repo, _dir) = repo();
        let err = repo.purge_deleted_entry(0, true).unwrap_err();
        assert!(matches!(err, Error::CannotRemoveRoot));
    }

    #[test]
    fn purge_deleted_entry_recursively_purges_a_soft_deleted_directorys_own_children() {
        let (repo, _dir) = repo();
        let dir_id = repo.mkdir(0, "a", 100).unwrap();
        let content_id = insert_content(&repo, 1, 0xAA);
        let file_id = repo.settle_file(dir_id, "f.txt", 100, content_id).unwrap();
        repo.unlink_file(file_id, 150).unwrap();
        repo.rmdir(dir_id, 200).unwrap();

        let result = repo
            .purge_deleted_entry(dir_id, true)
            .expect("purge of the directory must also purge its own deleted child");

        assert_eq!(
            result.descendants, 1,
            "the one deleted child must be counted"
        );
        assert_eq!(row_count(&repo, dir_id), 0);
        assert_eq!(
            row_count(&repo, file_id),
            0,
            "a soft-deleted directory's own soft-deleted child must be purged along with it - \
             otherwise its parent_id foreign key would refuse the parent's own deletion"
        );
    }

    #[test]
    fn purge_deleted_entry_non_recursive_refuses_a_directory_with_soft_deleted_children() {
        let (repo, _dir) = repo();
        let dir_id = repo.mkdir(0, "a", 100).unwrap();
        let content_id = insert_content(&repo, 1, 0xAA);
        let file_id = repo.settle_file(dir_id, "f.txt", 100, content_id).unwrap();
        repo.unlink_file(file_id, 150).unwrap();
        repo.rmdir(dir_id, 200).unwrap();

        let err = repo
            .purge_deleted_entry(dir_id, false)
            .expect_err("must refuse - the directory still has a soft-deleted child");
        assert!(matches!(err, Error::DirectoryNotEmpty(id) if id == dir_id));
        assert_eq!(
            row_count(&repo, dir_id),
            1,
            "a refused non-recursive purge must leave the directory untouched"
        );
        assert_eq!(
            row_count(&repo, file_id),
            1,
            "a refused non-recursive purge must leave the child untouched"
        );
    }

    #[test]
    fn purge_deleted_entry_non_recursive_succeeds_on_an_empty_soft_deleted_directory() {
        let (repo, _dir) = repo();
        let dir_id = repo.mkdir(0, "empty", 100).unwrap();
        repo.rmdir(dir_id, 200).unwrap();

        let result = repo.purge_deleted_entry(dir_id, false).expect(
            "must succeed - the directory has no children to conflict with recursive=false",
        );
        assert_eq!(result.descendants, 0);
        assert_eq!(row_count(&repo, dir_id), 0);
    }

    #[test]
    fn purge_deleted_entry_reclaims_bytes_for_content_nothing_else_references() {
        let (repo, _dir) = repo();
        let (chunk_id, _ranges) = repo.reserve_and_insert_chunk(10, &[0xAA; 20]).unwrap();
        let content_id = repo
            .find_or_create_content(10, &[0xBB; 20], &[chunk_id])
            .unwrap();
        let id = repo.settle_file(0, "a.txt", 100, content_id).unwrap();
        repo.unlink_file(id, 200).unwrap();

        let result = repo
            .purge_deleted_entry(id, true)
            .expect("purge must succeed");

        assert_eq!(
            result.reclaimed_bytes, 10,
            "the sole reference's own chunk_extents range must be counted as reclaimed"
        );
        let chunk_left: i64 = repo
            .with_connection(|conn, _cache| {
                Ok(conn.query_row(
                    "SELECT COUNT(*) FROM chunks WHERE id = ?1",
                    [chunk_id],
                    |row| row.get(0),
                )?)
            })
            .unwrap();
        assert_eq!(
            chunk_left, 0,
            "the orphaned chunk itself must be deleted, cascading to chunk_extents"
        );
    }

    #[test]
    fn purge_deleted_entry_does_not_reclaim_content_still_referenced_elsewhere() {
        let (repo, _dir) = repo();
        let (chunk_id, _ranges) = repo.reserve_and_insert_chunk(10, &[0xAA; 20]).unwrap();
        let content_id = repo
            .find_or_create_content(10, &[0xBB; 20], &[chunk_id])
            .unwrap();
        let first_id = repo.settle_file(0, "a.txt", 100, content_id).unwrap();
        // A second live entry shares the same content_id, keeping its ref_count above zero.
        repo.settle_file(0, "b.txt", 100, content_id).unwrap();
        repo.unlink_file(first_id, 200).unwrap();

        let result = repo
            .purge_deleted_entry(first_id, true)
            .expect("purge must succeed");

        assert_eq!(
            result.reclaimed_bytes, 0,
            "content still referenced by a live entry must not be reclaimed"
        );
        let content_left: i64 = repo
            .with_connection(|conn, _cache| {
                Ok(conn.query_row(
                    "SELECT COUNT(*) FROM contents WHERE id = ?1",
                    [content_id],
                    |row| row.get(0),
                )?)
            })
            .unwrap();
        assert_eq!(
            content_left, 1,
            "the contents row must survive while still referenced"
        );
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
    fn deleted_entry_by_id_returns_none_for_a_live_entry() {
        let (repo, _dir) = repo();
        let id = repo.mkdir(0, "a", 100).unwrap();
        assert!(repo.deleted_entry_by_id(id).unwrap().is_none());
    }

    #[test]
    fn deleted_entry_by_id_returns_the_entry_once_soft_deleted() {
        let (repo, _dir) = repo();
        let id = repo.mkdir(0, "a", 100).unwrap();
        repo.rmdir(id, 200).unwrap();
        let entry = repo.deleted_entry_by_id(id).unwrap().unwrap();
        assert_eq!(entry.entry.id, id);
        assert_eq!(entry.deleted_at, 200);
    }

    #[test]
    fn deleted_entry_by_id_returns_none_for_an_unknown_id() {
        let (repo, _dir) = repo();
        assert!(repo.deleted_entry_by_id(999).unwrap().is_none());
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

    #[test]
    fn recover_deleted_entry_makes_it_live_again_at_the_given_location() {
        let (repo, _dir) = repo();
        let content_id = insert_content(&repo, 1, 0xAA);
        let id = repo.settle_file(0, "a.txt", 100, content_id).unwrap();
        repo.unlink_file(id, 200).unwrap();
        assert!(repo.resolve_path("/a.txt").unwrap().is_none());

        repo.recover_deleted_entry(id, 0, "recovered.txt", false, 300)
            .expect("recovery must succeed");

        let entry = repo.resolve_path("/recovered.txt").unwrap().unwrap();
        assert_eq!(entry.id, id);
        assert!(
            repo.deleted_entry_by_id(id).unwrap().is_none(),
            "a recovered entry is no longer soft-deleted"
        );
    }

    #[test]
    fn recover_deleted_entry_can_recover_into_a_different_live_directory() {
        let (repo, _dir) = repo();
        let dest = repo.mkdir(0, "dest", 50).unwrap();
        let content_id = insert_content(&repo, 1, 0xAA);
        let id = repo.settle_file(0, "a.txt", 100, content_id).unwrap();
        repo.unlink_file(id, 200).unwrap();

        repo.recover_deleted_entry(id, dest, "a.txt", false, 300)
            .expect("recovery must succeed");

        assert_eq!(repo.resolve_path("/dest/a.txt").unwrap().unwrap().id, id);
    }

    #[test]
    fn recover_deleted_entry_bumps_the_new_parents_mtime() {
        let (repo, _dir) = repo();
        let dest = repo.mkdir(0, "dest", 50).unwrap();
        let content_id = insert_content(&repo, 1, 0xAA);
        let id = repo.settle_file(0, "a.txt", 100, content_id).unwrap();
        repo.unlink_file(id, 200).unwrap();

        repo.recover_deleted_entry(id, dest, "a.txt", false, 300)
            .unwrap();

        assert_eq!(
            repo.resolve_path("/dest").unwrap().unwrap().time_millis,
            300
        );
    }

    #[test]
    fn recover_deleted_entry_refuses_a_live_entry() {
        let (repo, _dir) = repo();
        let content_id = insert_content(&repo, 1, 0xAA);
        let id = repo.settle_file(0, "a.txt", 100, content_id).unwrap();

        let err = repo
            .recover_deleted_entry(id, 0, "a.txt", false, 200)
            .unwrap_err();
        assert!(matches!(err, Error::NotSoftDeleted(_)));
    }

    #[test]
    fn recover_deleted_entry_refuses_a_nonexistent_id() {
        let (repo, _dir) = repo();
        let err = repo
            .recover_deleted_entry(999, 0, "a.txt", false, 200)
            .unwrap_err();
        assert!(matches!(err, Error::NoSuchEntry(999)));
    }

    #[test]
    fn recover_deleted_entry_refuses_a_nonexistent_target_parent() {
        let (repo, _dir) = repo();
        let content_id = insert_content(&repo, 1, 0xAA);
        let id = repo.settle_file(0, "a.txt", 100, content_id).unwrap();
        repo.unlink_file(id, 200).unwrap();

        let err = repo
            .recover_deleted_entry(id, 999, "a.txt", false, 300)
            .unwrap_err();
        assert!(matches!(err, Error::NoSuchEntry(999)));
    }

    #[test]
    fn recover_deleted_entry_refuses_a_file_target_parent() {
        let (repo, _dir) = repo();
        let content_id = insert_content(&repo, 1, 0xAA);
        let other_content = insert_content(&repo, 2, 0xBB);
        let file_parent = repo
            .settle_file(0, "not-a-dir.txt", 50, other_content)
            .unwrap();
        let id = repo.settle_file(0, "a.txt", 100, content_id).unwrap();
        repo.unlink_file(id, 200).unwrap();

        let err = repo
            .recover_deleted_entry(id, file_parent, "a.txt", false, 300)
            .unwrap_err();
        assert!(matches!(err, Error::WrongKind(_)));
    }

    #[test]
    fn recover_deleted_entry_replaces_an_existing_live_file_unless_no_replace_is_set() {
        let (repo, _dir) = repo();
        let content_a = insert_content(&repo, 1, 0xAA);
        let content_b = insert_content(&repo, 2, 0xBB);
        let id = repo.settle_file(0, "a.txt", 100, content_a).unwrap();
        repo.unlink_file(id, 200).unwrap();
        repo.settle_file(0, "a.txt", 250, content_b).unwrap();

        let err = repo
            .recover_deleted_entry(id, 0, "a.txt", true, 300)
            .unwrap_err();
        assert!(matches!(err, Error::EntryAlreadyExists { .. }));

        repo.recover_deleted_entry(id, 0, "a.txt", false, 300)
            .expect("replacing an existing live file must succeed without no_replace");
        let live = repo.resolve_path("/a.txt").unwrap().unwrap();
        assert_eq!(live.id, id);
    }

    #[test]
    fn recover_deleted_entry_refuses_replacing_an_existing_live_directory() {
        let (repo, _dir) = repo();
        let content_id = insert_content(&repo, 1, 0xAA);
        let id = repo.settle_file(0, "a", 100, content_id).unwrap();
        repo.unlink_file(id, 200).unwrap();
        repo.mkdir(0, "a", 250).unwrap();

        let err = repo
            .recover_deleted_entry(id, 0, "a", false, 300)
            .unwrap_err();
        assert!(matches!(err, Error::EntryAlreadyExists { .. }));
    }
}
