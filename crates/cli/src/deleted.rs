//! REQ-TREE-009's `[deleted]` path-segment addressing (`requirements/functional/tree.md`) - CLI
//! side: resolving a whole `dfs list`/`dfs restore` path that may name the reserved segment, and
//! formatting/matching the disambiguated display name a soft-deleted entry gets shown under when
//! more than one shares its original name. Also exposes the length-constrained variants and the
//! `[time]` naming/matching building blocks REQ-MOUNT-004/007/008's mount-side addressing needs on
//! top of this - see `crate::dedup_fs`.

use std::collections::HashMap;

/// REQ-TREE-009's reserved path segment.
pub const DELETED_SEGMENT: &str = "[deleted]";

/// REQ-MOUNT-008's own reserved segment, unlike [`DELETED_SEGMENT`] not part of REQ-TREE-009's
/// general addressing - only meaningful immediately inside a `[deleted]` view, never elsewhere,
/// and only ever consulted by the mount (`crate::dedup_fs`), never `dfs list`/`dfs restore`.
pub const TIME_SEGMENT: &str = "[time]";

/// What a repository path resolves to once REQ-TREE-009's `[deleted]` segment is taken into
/// account.
#[derive(Clone, Copy)]
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
/// children are themselves always soft-deleted too (REQ-TREE-008) and so need their own `[deleted]`
/// step to reach. `Ok(None)` if any segment does not resolve, the same as plain `resolve_path`. No
/// length constraint on a matched display name - the right choice for a caller with none of its
/// own (`dfs list`/`dfs restore`'s terminal-facing paths); see [`resolve_within`] for a caller that
/// has one (the mount, REQ-MOUNT-008).
pub fn resolve(repo: &db::Repository, path: &str) -> Result<Option<Resolved>, db::Error> {
    resolve_impl(repo, path, None)
}

/// Like [`resolve`], but a matched display name must additionally fit within `max_bytes` -
/// REQ-MOUNT-008's own length-constrained matching (`mountfs::MAX_NAME_BYTES` in practice), so a
/// path segment the mount handed a caller (via `readdir`) resolves back to the same entry the
/// caller was shown, not a name that only exists in an unconstrained context.
pub fn resolve_within(
    repo: &db::Repository,
    path: &str,
    max_bytes: usize,
) -> Result<Option<Resolved>, db::Error> {
    resolve_impl(repo, path, Some(max_bytes))
}

fn resolve_impl(
    repo: &db::Repository,
    path: &str,
    max_bytes: Option<usize>,
) -> Result<Option<Resolved>, db::Error> {
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

    // Phase 2: `segments[i]` was `[deleted]` with no live collision.
    if i == segments.len() - 1 {
        return Ok(Some(Resolved::DeletedChildren {
            parent_id: current_parent_id,
        }));
    }
    resolve_deleted_children(repo, current_parent_id, &segments, i + 1, max_bytes)
}

/// Continues REQ-TREE-009's `[deleted]`-segment resolution from `parent_id`'s own soft-deleted
/// children, given `segments[next_index..]` still to consume - the shared tail both [`resolve`]'s
/// own walk and the mount's `[time]`-addressing (`crate::dedup_fs`, which independently matches
/// its own first segment via [`find_by_timestamped_name`] before handing off here) recurse
/// through, every remaining segment alternating between naming one specific soft-deleted entry and,
/// if there is more path after it, another literal `[deleted]` to descend into it (REQ-TREE-008: a
/// soft-deleted directory never has live children, only soft-deleted ones).
pub(crate) fn resolve_deleted_children(
    repo: &db::Repository,
    parent_id: i64,
    segments: &[&str],
    next_index: usize,
    max_bytes: Option<usize>,
) -> Result<Option<Resolved>, db::Error> {
    if next_index == segments.len() {
        return Ok(Some(Resolved::DeletedChildren { parent_id }));
    }
    let children = repo.list_deleted_children(parent_id)?;
    let Some(deleted_entry) = find_by_display_name(&children, segments[next_index], max_bytes)
    else {
        return Ok(None);
    };
    continue_from_deleted_entry(repo, deleted_entry, segments, next_index + 1, max_bytes)
}

/// Continues resolution once `deleted_entry` has just been matched by whichever name scheme the
/// caller used (REQ-TREE-009's bare/timestamp/id-suffixed form via [`resolve_deleted_children`], or
/// REQ-MOUNT-008's always-timestamp-prefixed `[time]` form via `crate::dedup_fs`) -
/// `segments[next_index..]` is whatever remains of the path after the segment that named it.
pub(crate) fn continue_from_deleted_entry(
    repo: &db::Repository,
    deleted_entry: db::DeletedEntry,
    segments: &[&str],
    next_index: usize,
    max_bytes: Option<usize>,
) -> Result<Option<Resolved>, db::Error> {
    if next_index == segments.len() {
        return Ok(Some(Resolved::Deleted(deleted_entry)));
    }
    if segments[next_index] != DELETED_SEGMENT {
        return Ok(None);
    }
    resolve_deleted_children(
        repo,
        deleted_entry.entry.id,
        segments,
        next_index + 1,
        max_bytes,
    )
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

/// Truncates `s` to at most `budget` bytes, never splitting a UTF-8 character.
fn truncate_to_byte_budget(s: &str, budget: usize) -> &str {
    if s.len() <= budget {
        return s;
    }
    let mut end = budget;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

/// Builds `stem` followed by `fixed` (whatever must survive intact - a disambiguation
/// suffix/prefix, and for the suffix form, the original extension too), truncating `stem` - never
/// `fixed`, the part actually meant to stay unique - if the result would otherwise exceed
/// `max_bytes`. `None` means no constraint at all (e.g. `dfs list`'s terminal output).
fn fit_stem(stem: &str, fixed: &str, max_bytes: Option<usize>) -> String {
    match max_bytes {
        Some(max) if stem.len() + fixed.len() > max => {
            let budget = max.saturating_sub(fixed.len());
            format!("{}{fixed}", truncate_to_byte_budget(stem, budget))
        }
        _ => format!("{stem}{fixed}"),
    }
}

/// REQ-TREE-009's disambiguated display name for a soft-deleted entry named `base_name`, with
/// `suffix` (a deletion timestamp or an id - the caller decides which) inserted before its
/// extension, respecting `max_bytes` (see [`fit_stem`]).
fn disambiguated_name(base_name: &str, suffix: &str, max_bytes: Option<usize>) -> String {
    let (stem, ext) = split_extension(base_name);
    fit_stem(stem, &format!(" [{suffix}]{ext}"), max_bytes)
}

/// REQ-TREE-009's own display names for `children` (a directory's soft-deleted children, as
/// [`db::Repository::list_deleted_children`] returns them): a bare name where it does not collide
/// with a sibling, the deletion-timestamp-suffixed form where it does, and - only for the rare
/// case where even that timestamp is shared down to the second by more than one sibling with the
/// same base name - the id-suffixed form instead, so two entries are never shown under the exact
/// same name. Order matches `children`'s own order. No length constraint; see
/// [`display_names_within`] for a caller that has one.
pub fn display_names(children: &[(String, db::DeletedEntry)]) -> Vec<String> {
    display_names_impl(children, None)
}

/// Like [`display_names`], but every name respects `max_bytes` (see [`fit_stem`]) -
/// REQ-MOUNT-008's own length-constrained display (`mountfs::MAX_NAME_BYTES` in practice).
pub fn display_names_within(
    children: &[(String, db::DeletedEntry)],
    max_bytes: usize,
) -> Vec<String> {
    display_names_impl(children, Some(max_bytes))
}

fn display_names_impl(
    children: &[(String, db::DeletedEntry)],
    max_bytes: Option<usize>,
) -> Vec<String> {
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
                max_bytes,
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
                        max_bytes,
                    );
                }
            }
        }
    }
    result
}

/// Matches `segment` (as a caller typed it, e.g. copied from `dfs list --show-deleted`'s own
/// output) against `children`'s own [`display_names`]/[`display_names_within`], rather than trying
/// to parse the bracket syntax back out of an arbitrary string - reconstructing and comparing each
/// candidate is exact and needs no assumptions about which of the id/timestamp forms `segment`
/// uses.
fn find_by_display_name(
    children: &[(String, db::DeletedEntry)],
    segment: &str,
    max_bytes: Option<usize>,
) -> Option<db::DeletedEntry> {
    display_names_impl(children, max_bytes)
        .into_iter()
        .zip(children)
        .find_map(|(name, (_, entry))| (name == segment).then_some(*entry))
}

/// Builds `prefix` followed by a space and `base_name`, truncating `base_name` - never `prefix` -
/// if the result would otherwise exceed `max_bytes` (see [`fit_stem`]'s own reasoning; the
/// direction is reversed here since REQ-MOUNT-008's prefix comes first, not last).
fn timestamped_name(base_name: &str, prefix: &str, max_bytes: Option<usize>) -> String {
    let fixed = format!("{prefix} ");
    match max_bytes {
        Some(max) if fixed.len() + base_name.len() > max => {
            let budget = max.saturating_sub(fixed.len());
            format!("{fixed}{}", truncate_to_byte_budget(base_name, budget))
        }
        _ => format!("{fixed}{base_name}"),
    }
}

/// REQ-MOUNT-008's own `[time]` view names for `children`: each entry's deletion timestamp always
/// prefixed (unlike [`display_names`]'s suffix-only-when-ambiguous form), so a plain alphabetic
/// sort of the view also sorts chronologically - falling back to the entry's own id as the prefix
/// instead, independent of any length constraint, only in the rare case two entries share both the
/// same name and the same deletion second (the timestamp's one-second resolution is not enough to
/// tell them apart then, the same edge case [`display_names`] falls back to an id suffix for).
/// `max_bytes` is REQ-TREE-009's own length constraint the calling context imposes
/// (`mountfs::MAX_NAME_BYTES` for the mount - REQ-MOUNT-008), truncating the base name - never the
/// prefix - if even the unambiguous form does not fit; `None` for no constraint.
pub fn timestamped_display_names(
    children: &[(String, db::DeletedEntry)],
    max_bytes: Option<usize>,
) -> Vec<String> {
    let mut by_key: HashMap<(&str, String), Vec<usize>> = HashMap::new();
    for (index, (name, entry)) in children.iter().enumerate() {
        let second = crate::time_format::format_deletion_suffix(entry.deleted_at);
        by_key
            .entry((name.as_str(), second))
            .or_default()
            .push(index);
    }

    let mut result = vec![String::new(); children.len()];
    for ((_, second), indices) in by_key {
        if let [index] = indices[..] {
            result[index] = timestamped_name(&children[index].0, &second, max_bytes);
        } else {
            for index in indices {
                let id_prefix = format!("[{}]", children[index].1.entry.id);
                result[index] = timestamped_name(&children[index].0, &id_prefix, max_bytes);
            }
        }
    }
    result
}

/// Matches `segment` against `children`'s own [`timestamped_display_names`] - REQ-MOUNT-008's
/// `[time]`-addressed lookup, the same reconstruct-and-compare approach [`find_by_display_name`]
/// uses for the base scheme.
pub(crate) fn find_by_timestamped_name(
    children: &[(String, db::DeletedEntry)],
    segment: &str,
    max_bytes: Option<usize>,
) -> Option<db::DeletedEntry> {
    timestamped_display_names(children, max_bytes)
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
            db::RepositorySettings::new(20, 1_700_000_000_000),
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
            disambiguated_name("photo.jpg", "2026-08-22_140414", None),
            "photo [2026-08-22_140414].jpg"
        );
        assert_eq!(
            disambiguated_name(".env", "2026-08-22_140414", None),
            ".env [2026-08-22_140414]"
        );
    }

    #[test]
    fn disambiguated_name_truncates_the_stem_not_the_suffix_when_over_budget() {
        let long_stem = "a".repeat(50);
        let name = format!("{long_stem}.jpg");
        let result = disambiguated_name(&name, "2026-08-22_140414", Some(30));
        assert!(result.len() <= 30, "got {} bytes: {result}", result.len());
        assert!(
            result.ends_with(" [2026-08-22_140414].jpg"),
            "the suffix and extension must survive intact: {result}"
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
    fn display_names_within_truncates_when_the_disambiguated_form_does_not_fit() {
        let long_stem = "a".repeat(50);
        let children = vec![
            (
                format!("{long_stem}.jpg"),
                deleted_entry(1, 946_684_800_000),
            ),
            (
                format!("{long_stem}.jpg"),
                deleted_entry(2, 946_684_900_000),
            ),
        ];
        let names = display_names_within(&children, 30);
        assert_ne!(names[0], names[1]);
        for name in &names {
            assert!(name.len() <= 30, "got {} bytes: {name}", name.len());
            assert!(name.ends_with(".jpg"), "extension must survive: {name}");
        }
    }

    #[test]
    fn timestamped_display_names_always_prefixes_even_an_unambiguous_name() {
        let children = vec![("a.txt".to_string(), deleted_entry(1, 946_684_800_000))];
        let names = timestamped_display_names(&children, None);
        assert_eq!(names[0], "2000-01-01_000000 a.txt");
    }

    #[test]
    fn timestamped_display_names_sorts_chronologically_by_construction() {
        // Deliberately inserted out of chronological order (index 0 is the later deletion) - the
        // always-prefixed timestamp must still sort them back into deletion order.
        let children = vec![
            ("later.txt".to_string(), deleted_entry(1, 946_684_900_000)),
            ("earlier.txt".to_string(), deleted_entry(2, 946_684_800_000)),
        ];
        let names = timestamped_display_names(&children, None);
        let mut sorted = names.clone();
        sorted.sort();
        assert_eq!(
            sorted,
            vec![names[1].clone(), names[0].clone()],
            "a plain sort must reorder to the earlier deletion first: {names:?}"
        );
    }

    #[test]
    fn timestamped_display_names_falls_back_to_the_id_prefix_on_a_same_second_collision() {
        let children = vec![
            ("a.txt".to_string(), deleted_entry(1, 100_000)),
            ("a.txt".to_string(), deleted_entry(2, 100_000)),
        ];
        let names = timestamped_display_names(&children, None);
        assert_ne!(names[0], names[1]);
        assert!(names[0].starts_with("[1] "), "got {}", names[0]);
        assert!(names[1].starts_with("[2] "), "got {}", names[1]);
    }

    #[test]
    fn timestamped_display_names_within_truncates_the_base_name_not_the_prefix() {
        let long_stem = "a".repeat(50);
        let children = vec![(long_stem.clone(), deleted_entry(1, 946_684_800_000))];
        let names = timestamped_display_names(&children, Some(30));
        assert!(
            names[0].len() <= 30,
            "got {} bytes: {}",
            names[0].len(),
            names[0]
        );
        assert!(
            names[0].starts_with("2000-01-01_000000 "),
            "the prefix must survive intact: {}",
            names[0]
        );
    }

    #[test]
    fn find_by_timestamped_name_matches_a_prefixed_name_back_to_its_entry() {
        let children = vec![("a.txt".to_string(), deleted_entry(1, 946_684_800_000))];
        let found = find_by_timestamped_name(&children, "2000-01-01_000000 a.txt", None).unwrap();
        assert_eq!(found.entry.id, 1);
    }

    #[test]
    fn find_by_timestamped_name_returns_none_without_a_match() {
        let children = vec![("a.txt".to_string(), deleted_entry(1, 946_684_800_000))];
        assert!(find_by_timestamped_name(&children, "nope", None).is_none());
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

    #[test]
    fn resolve_within_matches_a_name_truncated_the_same_way_display_names_within_shows_it() {
        let (repo, _dir) = repo_and_dir();
        let long_stem = "a".repeat(50);
        let content_a = repo.find_or_create_content(1, &[0xAAu8; 20], &[]).unwrap();
        let content_b = repo.find_or_create_content(2, &[0xBBu8; 20], &[]).unwrap();
        let name = format!("{long_stem}.jpg");
        let first = repo.settle_file(0, &name, 100, content_a).unwrap();
        let second = repo.settle_file(0, &name, 200, content_b).unwrap();
        repo.unlink_file(second, 300).unwrap();

        let children = repo.list_deleted_children(0).unwrap();
        let shown = display_names_within(&children, 30);
        let first_shown = shown[children
            .iter()
            .position(|(_, e)| e.entry.id == first)
            .unwrap()]
        .clone();

        let resolved = resolve_within(&repo, &format!("/[deleted]/{first_shown}"), 30)
            .unwrap()
            .unwrap();
        match resolved {
            Resolved::Deleted(entry) => assert_eq!(entry.entry.id, first),
            _ => panic!("expected Deleted"),
        }
    }
}
