//! REQ-TREE-009's `[deleted]` path-segment addressing (`requirements/functional/tree.md`) - CLI
//! side: resolving a whole `dfs list`/`dfs restore` path that may name the reserved segment, and
//! formatting/matching the disambiguated display name a soft-deleted entry gets shown under when
//! more than one shares its original name.

use std::collections::HashMap;

/// REQ-TREE-009's reserved path segment.
pub const DELETED_SEGMENT: &str = "[deleted]";

/// What a repository path resolves to once REQ-TREE-009's `[deleted]` segment is taken into
/// account.
pub enum Resolved {
    /// An ordinary live entry - what plain [`db::Repository::resolve_path`] alone would return.
    /// Also what a path ending in `[deleted]` resolves to when a real, live entry already has
    /// that name (REQ-TREE-009: the real entry always wins).
    Live(db::Entry),
    /// The `[deleted]` segment itself, naming `parent_id`'s own soft-deleted children - the path
    /// ends exactly at `[deleted]`, with nothing after it.
    DeletedChildren { parent_id: i64 },
    /// One specific soft-deleted entry, addressed by its own disambiguated display name.
    Deleted(db::DeletedEntry),
}

/// Resolves `path` against `repo`, honoring REQ-TREE-009's `[deleted]` addressing anywhere it
/// appears in the path - not just as the final segment, since a soft-deleted directory's own
/// children are themselves always soft-deleted (REQ-TREE-008) and so need their own `[deleted]`
/// step to reach. `Ok(None)` if any segment does not resolve, the same as plain `resolve_path`.
pub fn resolve(repo: &db::Repository, path: &str) -> Result<Option<Resolved>, db::Error> {
    let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    if segments.is_empty() {
        return Ok(repo.resolve_path("/")?.map(Resolved::Live));
    }

    // Phase 1: ordinary live resolution, one segment at a time (re-walking from root each time -
    // negligible cost for the short paths this addresses), until either the whole path is
    // consumed or a `[deleted]` segment has no live entry of that name to collide with.
    let mut prefix = String::new();
    let mut current_parent_id = 0i64;
    let mut i = 0;
    loop {
        let segment = segments[i];
        prefix.push('/');
        prefix.push_str(segment);
        match repo.resolve_path(&prefix)? {
            Some(entry) => {
                if i == segments.len() - 1 {
                    return Ok(Some(Resolved::Live(entry)));
                }
                current_parent_id = entry.id;
                i += 1;
            }
            None if segment == DELETED_SEGMENT => break,
            None => return Ok(None),
        }
    }

    // Phase 2: `segments[i]` was `[deleted]` with no live collision - from here on, every
    // remaining segment alternates between naming one specific soft-deleted entry and, if there
    // is more path after it, another literal `[deleted]` to descend into it (REQ-TREE-008: a
    // soft-deleted directory never has live children, only soft-deleted ones).
    if i == segments.len() - 1 {
        return Ok(Some(Resolved::DeletedChildren {
            parent_id: current_parent_id,
        }));
    }
    i += 1;
    loop {
        let children = repo.list_deleted_children(current_parent_id)?;
        let Some(deleted_entry) = find_by_display_name(&children, segments[i]) else {
            return Ok(None);
        };
        if i == segments.len() - 1 {
            return Ok(Some(Resolved::Deleted(deleted_entry)));
        }
        i += 1;
        if segments[i] != DELETED_SEGMENT {
            return Ok(None);
        }
        if i == segments.len() - 1 {
            return Ok(Some(Resolved::DeletedChildren {
                parent_id: deleted_entry.entry.id,
            }));
        }
        i += 1;
        current_parent_id = deleted_entry.entry.id;
    }
}

/// Splits `name` into `(stem, extension-with-dot)` at its last splittable extension - a `.` not
/// at position `0`, so a dotfile like `.env` (or a name with no `.` at all) has no extension to
/// split off. REQ-TREE-009's disambiguation suffix goes before this split point, not just at the
/// end of the whole name.
fn split_extension(name: &str) -> (&str, &str) {
    match name.rfind('.') {
        Some(pos) if pos > 0 => (&name[..pos], &name[pos..]),
        _ => (name, ""),
    }
}

/// REQ-TREE-009's disambiguated display name for a soft-deleted entry named `base_name`, with
/// `suffix` (a deletion timestamp or an id - the caller decides which) inserted before its
/// extension.
fn disambiguated_name(base_name: &str, suffix: &str) -> String {
    let (stem, ext) = split_extension(base_name);
    format!("{stem} [{suffix}]{ext}")
}

/// REQ-TREE-009's own display names for `children` (a directory's soft-deleted children, as
/// [`db::Repository::list_deleted_children`] returns them): a bare name where it does not collide
/// with a sibling, the deletion-timestamp-suffixed form where it does, and - only for the rare
/// case where even that timestamp is shared down to the second by more than one sibling with the
/// same base name - the id-suffixed form instead, so two entries are never shown under the exact
/// same name. Order matches `children`'s own order.
pub fn display_names(children: &[(String, db::DeletedEntry)]) -> Vec<String> {
    let mut by_base_name: HashMap<&str, Vec<usize>> = HashMap::new();
    for (index, (name, _)) in children.iter().enumerate() {
        by_base_name.entry(name.as_str()).or_default().push(index);
    }

    let mut result = vec![String::new(); children.len()];
    for indices in by_base_name.into_values() {
        if let [index] = indices[..] {
            result[index] = children[index].0.clone();
            continue;
        }
        let mut by_timestamp_name: HashMap<String, Vec<usize>> = HashMap::new();
        for index in indices {
            let name = disambiguated_name(
                &children[index].0,
                &crate::time_format::format_deletion_suffix(children[index].1.deleted_at),
            );
            by_timestamp_name.entry(name).or_default().push(index);
        }
        for (timestamp_name, indices) in by_timestamp_name {
            if let [index] = indices[..] {
                result[index] = timestamp_name;
            } else {
                for index in indices {
                    result[index] = disambiguated_name(
                        &children[index].0,
                        &children[index].1.entry.id.to_string(),
                    );
                }
            }
        }
    }
    result
}

/// Matches `segment` (as a caller typed it, e.g. copied from `dfs list --show-deleted`'s own
/// output) against `children`'s own [`display_names`], rather than trying to parse the bracket
/// syntax back out of an arbitrary string - reconstructing and comparing each candidate is exact
/// and needs no assumptions about which of the id/timestamp forms `segment` uses.
fn find_by_display_name(
    children: &[(String, db::DeletedEntry)],
    segment: &str,
) -> Option<db::DeletedEntry> {
    display_names(children)
        .into_iter()
        .zip(children)
        .find_map(|(name, (_, entry))| (name == segment).then_some(*entry))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn repo_and_dir() -> (db::Repository, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let repo_root = dir.path().join("repo");
        db::init_repository(
            &repo_root,
            db::RepositorySettings::new(Some(20), 1_700_000_000_000),
        )
        .unwrap();
        let repo = db::open_repository(&repo_root).unwrap();
        (repo, dir)
    }

    #[test]
    fn split_extension_splits_at_the_last_dot_when_not_at_position_zero() {
        assert_eq!(split_extension("photo.jpg"), ("photo", ".jpg"));
        assert_eq!(split_extension("archive.tar.gz"), ("archive.tar", ".gz"));
        assert_eq!(split_extension(".env"), (".env", ""));
        assert_eq!(split_extension("README"), ("README", ""));
    }

    #[test]
    fn disambiguated_name_inserts_the_suffix_before_the_extension() {
        assert_eq!(
            disambiguated_name("photo.jpg", "2026-08-22_140414"),
            "photo [2026-08-22_140414].jpg"
        );
        assert_eq!(
            disambiguated_name(".env", "2026-08-22_140414"),
            ".env [2026-08-22_140414]"
        );
    }

    fn deleted_entry(id: i64, deleted_at: i64) -> db::DeletedEntry {
        db::DeletedEntry {
            entry: db::Entry {
                id,
                kind: db::EntryKind::File,
                time_millis: 1_000,
                content_id: None,
                size: 0,
            },
            deleted_at,
        }
    }

    #[test]
    fn display_names_leaves_an_unambiguous_name_bare() {
        let children = vec![("a.txt".to_string(), deleted_entry(1, 100))];
        assert_eq!(display_names(&children), vec!["a.txt".to_string()]);
    }

    #[test]
    fn display_names_suffixes_same_named_entries_with_their_own_deletion_timestamp() {
        let children = vec![
            ("a.txt".to_string(), deleted_entry(1, 946_684_800_000)),
            ("a.txt".to_string(), deleted_entry(2, 946_684_900_000)),
        ];
        let names = display_names(&children);
        assert_ne!(names[0], names[1]);
        assert!(names[0].starts_with("a ["));
        assert!(names[0].ends_with("].txt"));
    }

    #[test]
    fn display_names_falls_back_to_the_id_when_the_timestamp_suffix_still_collides() {
        // Same base name, same deletion second - the timestamp suffix alone would not
        // disambiguate them.
        let children = vec![
            ("a.txt".to_string(), deleted_entry(1, 100_000)),
            ("a.txt".to_string(), deleted_entry(2, 100_000)),
        ];
        let names = display_names(&children);
        assert_ne!(names[0], names[1]);
        assert!(names[0].contains(" [1]"), "got {}", names[0]);
        assert!(names[1].contains(" [2]"), "got {}", names[1]);
    }

    #[test]
    fn resolve_returns_live_for_an_ordinary_path() {
        let (repo, _dir) = repo_and_dir();
        repo.mkdir(0, "photos", 100).unwrap();

        let resolved = resolve(&repo, "/photos").unwrap().unwrap();
        assert!(matches!(resolved, Resolved::Live(entry) if entry.kind == db::EntryKind::Dir));
    }

    #[test]
    fn resolve_returns_none_for_a_path_that_does_not_exist() {
        let (repo, _dir) = repo_and_dir();
        assert!(resolve(&repo, "/nope").unwrap().is_none());
    }

    #[test]
    fn resolve_returns_deleted_children_for_a_bare_deleted_segment() {
        let (repo, _dir) = repo_and_dir();
        let photos = repo.mkdir(0, "photos", 100).unwrap();
        let content_id = repo.find_or_create_content(0, &[0xAAu8; 20], &[]).unwrap();
        let file_id = repo.settle_file(photos, "a.txt", 100, content_id).unwrap();
        repo.unlink_file(file_id, 200).unwrap();

        let resolved = resolve(&repo, "/photos/[deleted]").unwrap().unwrap();
        match resolved {
            Resolved::DeletedChildren { parent_id } => assert_eq!(parent_id, photos),
            _ => panic!("expected DeletedChildren"),
        }
    }

    #[test]
    fn resolve_addresses_one_specific_deleted_entry_by_its_bare_name_when_unambiguous() {
        let (repo, _dir) = repo_and_dir();
        let content_id = repo.find_or_create_content(0, &[0xAAu8; 20], &[]).unwrap();
        let file_id = repo.settle_file(0, "a.txt", 100, content_id).unwrap();
        repo.unlink_file(file_id, 200).unwrap();

        let resolved = resolve(&repo, "/[deleted]/a.txt").unwrap().unwrap();
        match resolved {
            Resolved::Deleted(entry) => {
                assert_eq!(entry.entry.id, file_id);
                assert_eq!(entry.deleted_at, 200);
            }
            _ => panic!("expected Deleted"),
        }
    }

    #[test]
    fn resolve_addresses_a_specific_deleted_entry_by_its_disambiguated_name() {
        let (repo, _dir) = repo_and_dir();
        let content_a = repo.find_or_create_content(1, &[0xAAu8; 20], &[]).unwrap();
        let content_b = repo.find_or_create_content(2, &[0xBBu8; 20], &[]).unwrap();
        let first = repo.settle_file(0, "a.txt", 100, content_a).unwrap();
        let second = repo.settle_file(0, "a.txt", 200, content_b).unwrap();
        repo.unlink_file(second, 300).unwrap();

        // Both history entries for "a.txt" are now soft-deleted (the first via settle_file's own
        // replace, the second via the explicit unlink above) - disambiguate by name.
        let children = repo.list_deleted_children(0).unwrap();
        let names = display_names(&children);
        let first_name = names[children
            .iter()
            .position(|(_, e)| e.entry.id == first)
            .unwrap()]
        .clone();

        let resolved = resolve(&repo, &format!("/[deleted]/{first_name}"))
            .unwrap()
            .unwrap();
        match resolved {
            Resolved::Deleted(entry) => assert_eq!(entry.entry.id, first),
            _ => panic!("expected Deleted"),
        }
    }

    #[test]
    fn resolve_lets_a_real_live_entry_win_over_the_deleted_segment() {
        let (repo, _dir) = repo_and_dir();
        repo.mkdir(0, "[deleted]", 100).unwrap();

        let resolved = resolve(&repo, "/[deleted]").unwrap().unwrap();
        assert!(matches!(resolved, Resolved::Live(entry) if entry.kind == db::EntryKind::Dir));
    }

    #[test]
    fn resolve_descends_into_an_already_deleted_directorys_own_deleted_children() {
        let (repo, _dir) = repo_and_dir();
        let a_id = repo.mkdir(0, "a", 100).unwrap();
        let content_id = repo.find_or_create_content(0, &[0xAAu8; 20], &[]).unwrap();
        let file_id = repo.settle_file(a_id, "f.txt", 100, content_id).unwrap();
        repo.unlink_file(file_id, 150).unwrap();
        repo.rmdir(a_id, 200).unwrap();

        let resolved = resolve(&repo, "/[deleted]/a/[deleted]/f.txt")
            .unwrap()
            .unwrap();
        match resolved {
            Resolved::Deleted(entry) => assert_eq!(entry.entry.id, file_id),
            _ => panic!("expected Deleted"),
        }
    }
}
