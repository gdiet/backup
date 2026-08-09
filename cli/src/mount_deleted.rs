//! The synthetic `[deleted]` folder every mounted directory shows (see
//! `docs/plans/implemented/mount-deleted-folder.md`): browsing and reading
//! deleted entries, and recovering one by "moving" it out via `rename`
//! (which `DedupFs::rename`, in `mount.rs`, intercepts to call
//! `db::undelete` instead of `db::rename_entry` once this module has
//! resolved the source to a specific deleted entry).
//!
//! Nothing here is backed by a real `tree_entries` row - `[deleted]` itself
//! is injected into `readdir` output by the caller, and every lookup
//! within it goes through [`db::deleted_entries`], not the normal
//! active-only `db::resolve_path`/`db::find_tree_entry`. Because of that,
//! every *other* `MountFilesystem` method (`mkdir`/`create`/`unlink`/
//! `rmdir`/`utimens`/`truncate`) needs no special handling at all here: they
//! all resolve paths via the active-only functions, which simply don't find
//! a synthetic `[deleted]` component (unless a real entry by that name
//! exists there - see `split_deleted_path`'s "real entry wins" rule - in
//! which case they correctly behave as if it were an ordinary directory),
//! so they already fail with `ENOENT` on their own.

use mountfs::{Errno, Handle};
use rusqlite::Connection;

pub(crate) const DELETED_DIR_NAME: &str = "[deleted]";

/// Set on a [`Handle`]'s value to mark it as referring to a deleted entry
/// (opened via `[deleted]/...`) rather than an active one - `Handle` is a
/// bare `u64` (see `mountfs::Handle`), and real tree ids (SQLite `INTEGER
/// PRIMARY KEY`, always small and positive in practice) never come close to
/// using the top bit, so this is a safe, allocation-free way to tag a
/// handle without changing `Handle`'s shape or threading an extra enum
/// through every read-path call site.
const DELETED_HANDLE_BIT: u64 = 1 << 63;

pub(crate) fn deleted_handle(id: i64) -> Handle {
    Handle(DELETED_HANDLE_BIT | id as u64)
}

pub(crate) fn is_deleted_handle(handle: Handle) -> bool {
    handle.0 & DELETED_HANDLE_BIT != 0
}

pub(crate) fn deleted_handle_id(handle: Handle) -> i64 {
    (handle.0 & !DELETED_HANDLE_BIT) as i64
}

/// What a virtual path within `[deleted]` resolves to.
pub(crate) enum DeletedResolution {
    /// The deleted children directly under `root_id` should be listed -
    /// `root_id` is either the active directory `[deleted]` was found
    /// under (the bare `[deleted]` case), or an already-deleted directory
    /// reached by browsing further in (every descendant of a deleted
    /// directory is itself deleted - nothing new can be created under a
    /// directory once it's no longer resolvable - so `db::deleted_entries`
    /// applies identically either way).
    Listing { root_id: i64 },
    /// A specific deleted entry (file or directory) resolved by full path.
    Entry(db::TreeEntryRow),
}

/// If `path` passes through a synthetic `[deleted]` component (and isn't
/// shadowed by a real active entry of that name - see below), returns the
/// active directory id it was found under plus the remaining `/`-joined
/// virtual path past it (empty if `path` ends exactly at `[deleted]`
/// itself). Returns `None` for any path that doesn't involve `[deleted]`
/// at all, or where it's a real entry - all such paths are handled by the
/// existing active-tree resolution, unchanged.
///
/// "Real entry wins": if a directory already has a real, active child
/// literally named `[deleted]`, that real entry is resolved normally (this
/// function keeps walking through it like any other directory) rather than
/// being shadowed by the synthetic view - see the plan doc's design
/// decision on this.
pub(crate) fn split_deleted_path(
    conn: &Connection,
    path: &str,
) -> Result<Option<(i64, String)>, Errno> {
    let components: Vec<&str> = path.split('/').filter(|c| !c.is_empty()).collect();
    let mut current_id = 0i64;
    for (i, component) in components.iter().enumerate() {
        if *component == DELETED_DIR_NAME {
            let conflict =
                db::find_tree_entry(conn, current_id, DELETED_DIR_NAME).map_err(|_| Errno::EIO)?;
            if conflict.is_none() {
                let rest = components[i + 1..].join("/");
                return Ok(Some((current_id, rest)));
            }
            // A real `[deleted]` directory exists here - fall through and
            // resolve it like any other component below.
        }
        match db::find_tree_entry(conn, current_id, component).map_err(|_| Errno::EIO)? {
            Some(entry) if entry.kind == db::EntryKind::Dir => current_id = entry.id,
            _ => return Ok(None),
        }
    }
    Ok(None)
}

/// Parses a virtual path component back into its bare name and, if
/// present, the `[<id>]` disambiguation suffix - the reverse of
/// [`display_name`].
fn parse_component(component: &str) -> (&str, Option<i64>) {
    if component.ends_with(']')
        && let Some(open) = component.rfind(" [")
        && let Ok(id) = component[open + 2..component.len() - 1].parse::<i64>()
    {
        return (&component[..open], Some(id));
    }
    (component, None)
}

/// The name a deleted entry should be shown under in a listing: suffixed
/// with its id only when another entry in the same listing shares its bare
/// name (repeat-deletions of the same path) - see the plan doc's
/// disambiguation decision, mirroring `backup deleted`'s own approach.
fn display_name(name: &str, id: i64, name_is_ambiguous: bool) -> String {
    if name_is_ambiguous {
        format!("{name} [{id}]")
    } else {
        name.to_string()
    }
}

/// Resolves a virtual path (as returned by [`split_deleted_path`]) against
/// `scope_id`'s deleted descendants. `virtual_path` empty means "list
/// `scope_id`'s own deleted children"; otherwise walks it component by
/// component (each optionally `[<id>]`-suffixed, see [`parse_component`]),
/// requiring an exact, unambiguous match at every level. `None` means the
/// path doesn't resolve to anything (equivalent to `ENOENT`).
pub(crate) fn resolve_deleted(
    conn: &Connection,
    scope_id: i64,
    virtual_path: &str,
) -> Result<Option<DeletedResolution>, Errno> {
    if virtual_path.is_empty() {
        return Ok(Some(DeletedResolution::Listing { root_id: scope_id }));
    }
    let all = db::deleted_entries(conn, scope_id).map_err(|_| Errno::EIO)?;
    let mut matched_path = String::new();
    let mut matched: Option<&db::DeletedEntry> = None;
    for component in virtual_path.split('/') {
        let (name, maybe_id) = parse_component(component);
        let candidates: Vec<&db::DeletedEntry> = all
            .iter()
            .filter(|e| {
                let rest = if matched_path.is_empty() {
                    e.path.as_str()
                } else {
                    match e
                        .path
                        .strip_prefix(&matched_path)
                        .and_then(|s| s.strip_prefix('/'))
                    {
                        Some(rest) => rest,
                        None => return false,
                    }
                };
                rest == name
            })
            .collect();
        let chosen = match maybe_id {
            Some(id) => candidates.into_iter().find(|e| e.entry.id == id),
            None if candidates.len() == 1 => Some(candidates[0]),
            None => None,
        };
        let Some(chosen) = chosen else {
            return Ok(None);
        };
        matched_path = chosen.path.clone();
        matched = Some(chosen);
    }
    Ok(matched.map(|e| DeletedResolution::Entry(e.entry.clone())))
}

/// The `readdir` listing for `root_id`'s own deleted children (see
/// [`DeletedResolution::Listing`]), with `[<id>]` suffixes applied only
/// where more than one child shares a bare name.
pub(crate) fn list_deleted_children(
    conn: &Connection,
    root_id: i64,
) -> Result<Vec<mountfs::DirEntry>, Errno> {
    let all = db::deleted_entries(conn, root_id).map_err(|_| Errno::EIO)?;
    let direct: Vec<&db::DeletedEntry> = all.iter().filter(|e| !e.path.contains('/')).collect();

    let mut name_counts: std::collections::HashMap<&str, usize> = std::collections::HashMap::new();
    for e in &direct {
        *name_counts.entry(e.path.as_str()).or_insert(0) += 1;
    }

    Ok(direct
        .into_iter()
        .map(|e| mountfs::DirEntry {
            name: display_name(&e.path, e.entry.id, name_counts[e.path.as_str()] > 1),
            kind: match e.entry.kind {
                db::EntryKind::Dir => mountfs::FileKind::Directory,
                db::EntryKind::File => mountfs::FileKind::File,
            },
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_connection() -> (tempfile::TempDir, Connection) {
        let temp_dir = tempfile::tempdir().unwrap();
        let repo_root = temp_dir.path().join("repo");
        db::init_repository(
            &repo_root,
            &db::RepositorySettings::new(20, db::Chunking::Cdc).unwrap(),
        )
        .unwrap();
        let conn = db::open_repository(&repo_root)
            .unwrap()
            .open_write_connection()
            .unwrap();
        (temp_dir, conn)
    }

    fn insert_dir(conn: &Connection, parent_id: i64, name: &str) -> i64 {
        db::insert_directory(conn, parent_id, name, 0).unwrap()
    }

    fn insert_file(conn: &Connection, parent_id: i64, name: &str) -> i64 {
        conn.execute(
            "INSERT INTO tree_entries (parent_id, name, time, kind) VALUES (?1, ?2, 0, 'file')",
            rusqlite::params![parent_id, name],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    fn mark_deleted(conn: &Connection, id: i64, deleted_at: i64) {
        conn.execute(
            "UPDATE tree_entries SET deleted_at = ?1 WHERE id = ?2",
            rusqlite::params![deleted_at, id],
        )
        .unwrap();
    }

    #[test]
    fn split_deleted_path_finds_it_bare_and_nested() {
        let (_temp_dir, conn) = test_connection();
        let sub_id = insert_dir(&conn, 0, "sub");

        assert_eq!(
            split_deleted_path(&conn, "/[deleted]").unwrap(),
            Some((0, String::new()))
        );
        assert_eq!(
            split_deleted_path(&conn, "/sub/[deleted]").unwrap(),
            Some((sub_id, String::new()))
        );
        assert_eq!(
            split_deleted_path(&conn, "/sub/[deleted]/photo.jpg").unwrap(),
            Some((sub_id, "photo.jpg".to_string()))
        );
    }

    #[test]
    fn split_deleted_path_returns_none_without_a_deleted_component() {
        let (_temp_dir, conn) = test_connection();
        insert_dir(&conn, 0, "sub");
        assert_eq!(split_deleted_path(&conn, "/sub").unwrap(), None);
        assert_eq!(split_deleted_path(&conn, "/does-not-exist").unwrap(), None);
    }

    #[test]
    fn split_deleted_path_lets_a_real_entry_win() {
        let (_temp_dir, conn) = test_connection();
        let real_deleted_dir = insert_dir(&conn, 0, "[deleted]");
        insert_dir(&conn, real_deleted_dir, "inner");

        // The real directory is resolved normally, not shadowed.
        assert_eq!(split_deleted_path(&conn, "/[deleted]").unwrap(), None);
        assert_eq!(split_deleted_path(&conn, "/[deleted]/inner").unwrap(), None);
    }

    #[test]
    fn resolve_deleted_empty_virtual_path_is_a_listing() {
        let (_temp_dir, conn) = test_connection();
        match resolve_deleted(&conn, 0, "").unwrap() {
            Some(DeletedResolution::Listing { root_id }) => assert_eq!(root_id, 0),
            _ => panic!("expected a Listing resolution"),
        }
    }

    #[test]
    fn resolve_deleted_matches_an_unambiguous_bare_name() {
        let (_temp_dir, conn) = test_connection();
        let a_id = insert_file(&conn, 0, "a.txt");
        mark_deleted(&conn, a_id, 1000);

        match resolve_deleted(&conn, 0, "a.txt").unwrap() {
            Some(DeletedResolution::Entry(entry)) => assert_eq!(entry.id, a_id),
            _ => panic!("expected a resolved entry"),
        }
    }

    #[test]
    fn resolve_deleted_requires_an_id_suffix_when_ambiguous() {
        let (_temp_dir, conn) = test_connection();
        let first = insert_file(&conn, 0, "a.txt");
        mark_deleted(&conn, first, 1000);
        // A second, later deletion of the same name: insert-then-delete
        // again (mirrors two separate `store`+`del` runs at the same path).
        let second = insert_file(&conn, 0, "a.txt");
        mark_deleted(&conn, second, 2000);

        assert!(resolve_deleted(&conn, 0, "a.txt").unwrap().is_none());
        match resolve_deleted(&conn, 0, &format!("a.txt [{second}]"))
            .unwrap()
            .unwrap()
        {
            DeletedResolution::Entry(entry) => assert_eq!(entry.id, second),
            _ => panic!("expected a resolved entry"),
        }
    }

    #[test]
    fn resolve_deleted_walks_into_an_already_deleted_directory() {
        let (_temp_dir, conn) = test_connection();
        let dir_id = insert_dir(&conn, 0, "old-photos");
        let nested_id = insert_file(&conn, dir_id, "img.jpg");
        // A whole subtree soft-deleted atomically shares one deleted_at.
        mark_deleted(&conn, dir_id, 5000);
        mark_deleted(&conn, nested_id, 5000);

        match resolve_deleted(&conn, 0, "old-photos/img.jpg")
            .unwrap()
            .unwrap()
        {
            DeletedResolution::Entry(entry) => assert_eq!(entry.id, nested_id),
            _ => panic!("expected a resolved entry"),
        }

        // And it's independently reachable as its own listing root.
        match resolve_deleted(&conn, 0, "old-photos").unwrap().unwrap() {
            DeletedResolution::Entry(entry) => {
                assert_eq!(entry.id, dir_id);
                let children = list_deleted_children(&conn, entry.id).unwrap();
                assert_eq!(children.len(), 1);
                assert_eq!(children[0].name, "img.jpg");
            }
            _ => panic!("expected a resolved entry"),
        }
    }

    #[test]
    fn list_deleted_children_suffixes_only_ambiguous_names() {
        let (_temp_dir, conn) = test_connection();
        let unique = insert_file(&conn, 0, "unique.txt");
        mark_deleted(&conn, unique, 1000);
        let dup1 = insert_file(&conn, 0, "dup.txt");
        mark_deleted(&conn, dup1, 1000);
        let dup2 = insert_file(&conn, 0, "dup.txt");
        mark_deleted(&conn, dup2, 2000);

        let mut names: Vec<String> = list_deleted_children(&conn, 0)
            .unwrap()
            .into_iter()
            .map(|e| e.name)
            .collect();
        names.sort();
        assert_eq!(
            names,
            vec![
                "dup.txt [".to_string() + &dup1.to_string() + "]",
                "dup.txt [".to_string() + &dup2.to_string() + "]",
                "unique.txt".to_string(),
            ]
        );
    }

    #[test]
    fn parse_component_round_trips_display_name() {
        assert_eq!(parse_component("photo.jpg"), ("photo.jpg", None));
        assert_eq!(parse_component("photo.jpg [42]"), ("photo.jpg", Some(42)));
        // A name that merely contains brackets without being a valid
        // trailing " [<id>]" suffix is left alone.
        assert_eq!(parse_component("weird[name]"), ("weird[name]", None));
    }
}
