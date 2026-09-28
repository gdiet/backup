//! REQ-TREE-009's `[deleted]`/`[all]`/`[all]/[by-time]` path-segment addressing
//! (`requirements/functional/tree.md`) - resolving a whole `dfs list`/`dfs restore`/`dfs del`/mount
//! path that may name a reserved segment, and formatting/matching the display names entries get
//! shown under. Shared by the mount (`crate::dedup_fs`) and every CLI command that reaches
//! soft-deleted content - both strip their own `[show-deleted]`/`[purge-deleted]` root prefix
//! before calling in here, which knows nothing about that prefix itself.

use std::collections::HashMap;

use crate::time_format::TimeDisplay;

/// REQ-TREE-009's reserved path segment - the "most recent version per name" view.
pub const DELETED_SEGMENT: &str = "[deleted]";
/// REQ-TREE-009's reserved segment for the full, disambiguated history - what `[deleted]` itself
/// used to mean before the "most recent per name" default. Only meaningful directly inside a
/// `[deleted]` view, never elsewhere.
pub const ALL_SEGMENT: &str = "[all]";
/// REQ-TREE-009's reserved segment for the same entries [`ALL_SEGMENT`] shows, chronologically
/// named instead. Only meaningful directly inside an `[all]` view, never elsewhere.
pub const BY_TIME_SEGMENT: &str = "[by-time]";
/// DESIGN-MOUNT-020's mount-root entry point for REQ-MOUNT-004's read/recovery view - always
/// present through the mount, and `dfs list`/`dfs restore`/`dfs del`'s own optional, cosmetic
/// prefix (REQ-CLI-007/DESIGN-MOUNT-024): [`resolve`]/[`resolve_within`] themselves need no
/// prefix at all, a bare `[deleted]` path keeps working exactly as it always has - see
/// [`strip_show_deleted_prefix`].
pub const SHOW_DELETED_SEGMENT: &str = "[show-deleted]";
/// The mount-root entry point for REQ-MOUNT-007's purge-capable view - present only on a
/// `--read-write --purge` mount. No CLI counterpart (DESIGN-MOUNT-024): `dfs del --purge`'s own
/// flag decides permission, not which root a path is prefixed with.
pub const PURGE_DELETED_SEGMENT: &str = "[purge-deleted]";

/// Strips a leading `[show-deleted]` segment from `path`, if present - `dfs list`/`dfs restore`/
/// `dfs del`'s own optional, cosmetic counterpart to the mount's mandatory root prefix
/// (DESIGN-MOUNT-024): these commands are always deliberate, explicit invocations, so unlike the
/// mount there is no risk of a naive tool wandering in unannounced, and [`resolve`]/
/// [`resolve_within`] already resolve a bare `[deleted]` path without needing this prefix at all.
/// Kept purely so a path `dfs list` itself printed (which shows `[show-deleted]` as an ordinary,
/// discoverable root entry) can be pasted back in unchanged.
pub fn strip_show_deleted_prefix(path: &str) -> &str {
    let trimmed = path.trim_end_matches('/');
    match trimmed
        .strip_prefix('/')
        .and_then(|s| s.strip_prefix(SHOW_DELETED_SEGMENT))
    {
        Some("") => "/",
        Some(rest) if rest.starts_with('/') => rest,
        _ => path,
    }
}

/// What a repository path resolves to once REQ-TREE-009's addressing is taken into account.
#[derive(Clone, Copy)]
pub enum Resolved {
    /// An ordinary live entry - what plain [`db::Repository::resolve_path`] alone would return.
    /// Also what a path ending in `[deleted]` resolves to when a real, live entry already has
    /// that name (REQ-TREE-009: the real entry always wins).
    Live(db::Entry),
    /// The `[deleted]` segment itself - `parent_id`'s own soft-deleted children, filtered to at
    /// most one entry per distinct original name (see [`latest_by_name`]). The path ends exactly
    /// at `[deleted]`, with nothing after it.
    DeletedChildren { parent_id: i64 },
    /// The `[deleted]/[all]` segment - `parent_id`'s own soft-deleted children, every one of them
    /// (REQ-TREE-004's full history), disambiguated (see [`display_names`]).
    AllDeletedChildren { parent_id: i64 },
    /// The `[deleted]/[all]/[by-time]` segment - the same entries as `AllDeletedChildren`, shown
    /// chronologically (see [`timestamped_display_names`]) instead.
    AllByTimeDeletedChildren { parent_id: i64 },
    /// One specific soft-deleted entry, addressed by whichever of the three views above named it.
    Deleted(db::DeletedEntry),
}

/// Resolves `path` against `repo`, honoring REQ-TREE-009's addressing anywhere it appears in the
/// path - not just as the final segment, since a soft-deleted directory's own children are
/// themselves always soft-deleted too (REQ-TREE-008) and so are reached the same way, one level
/// down, without needing another literal `[deleted]` segment (REQ-TREE-009: that marker signals
/// only the live/dead crossing itself, not every level beneath it). `Ok(None)` if any segment does
/// not resolve, the same as plain `resolve_path`. No length constraint on a matched display name -
/// the right choice for a caller with none of its own (`dfs list`/`dfs restore`/`dfs del`'s
/// terminal-facing paths); see [`resolve_within`] for a caller that has one (the mount).
/// `display` decides which timezone a matched `[all]/[by-time]` segment's timestamps render in
/// (REQ-OPERABILITY-008) - irrelevant to every other segment, which stays UTC-unconditional
/// (REQ-TREE-009's own stable-identity guarantee).
pub fn resolve(
    repo: &db::Repository,
    path: &str,
    display: TimeDisplay,
) -> Result<Option<Resolved>, db::Error> {
    resolve_impl(repo, path, None, display)
}

/// Like [`resolve`], but a matched display name must additionally fit within `max_bytes` - the
/// mount's own length-constrained matching (`mountfs::MAX_NAME_BYTES` in practice), so a path
/// segment the mount handed a caller (via `readdir`) resolves back to the same entry the caller
/// was shown, not a name that only exists in an unconstrained context.
pub fn resolve_within(
    repo: &db::Repository,
    path: &str,
    max_bytes: usize,
    display: TimeDisplay,
) -> Result<Option<Resolved>, db::Error> {
    resolve_impl(repo, path, Some(max_bytes), display)
}

fn resolve_impl(
    repo: &db::Repository,
    path: &str,
    max_bytes: Option<usize>,
    display: TimeDisplay,
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

    // Phase 2: `segments[i]` was `[deleted]` with no live collision - resolve everything after it
    // against `current_parent_id`'s own soft-deleted children.
    resolve_deleted_view(
        repo,
        current_parent_id,
        &segments,
        i + 1,
        max_bytes,
        display,
    )
}

/// Resolves `segments[next_index..]` against `parent_id`'s own soft-deleted children - reached
/// either right after a literal `[deleted]` segment, or recursively, one level further down into
/// an already-dead directory's own children, without needing another literal `[deleted]` segment
/// there (REQ-TREE-009: no live/dead boundary left to signal once already inside dead territory).
fn resolve_deleted_view(
    repo: &db::Repository,
    parent_id: i64,
    segments: &[&str],
    next_index: usize,
    max_bytes: Option<usize>,
    display: TimeDisplay,
) -> Result<Option<Resolved>, db::Error> {
    if next_index == segments.len() {
        return Ok(Some(Resolved::DeletedChildren { parent_id }));
    }
    if segments[next_index] == ALL_SEGMENT {
        return resolve_all_view(
            repo,
            parent_id,
            segments,
            next_index + 1,
            max_bytes,
            display,
        );
    }
    let children = repo.list_deleted_children(parent_id)?;
    let Some(entry) = latest_by_name(&children)
        .into_iter()
        .find(|(name, _)| name == segments[next_index])
        .map(|(_, entry)| entry)
    else {
        return Ok(None);
    };
    continue_into_deleted_entry(repo, entry, segments, next_index + 1, max_bytes, display)
}

/// Resolves `segments[next_index..]` against `parent_id`'s full, disambiguated soft-deleted-child
/// history - reached right after `[deleted]/[all]`.
fn resolve_all_view(
    repo: &db::Repository,
    parent_id: i64,
    segments: &[&str],
    next_index: usize,
    max_bytes: Option<usize>,
    display: TimeDisplay,
) -> Result<Option<Resolved>, db::Error> {
    if next_index == segments.len() {
        return Ok(Some(Resolved::AllDeletedChildren { parent_id }));
    }
    if segments[next_index] == BY_TIME_SEGMENT {
        return resolve_by_time_view(
            repo,
            parent_id,
            segments,
            next_index + 1,
            max_bytes,
            display,
        );
    }
    let children = repo.list_deleted_children(parent_id)?;
    let Some(entry) = find_by_display_name(&children, segments[next_index], max_bytes) else {
        return Ok(None);
    };
    continue_into_deleted_entry(repo, entry, segments, next_index + 1, max_bytes, display)
}

/// Resolves `segments[next_index..]` against `parent_id`'s full soft-deleted-child history,
/// chronologically named - reached right after `[deleted]/[all]/[by-time]`.
fn resolve_by_time_view(
    repo: &db::Repository,
    parent_id: i64,
    segments: &[&str],
    next_index: usize,
    max_bytes: Option<usize>,
    display: TimeDisplay,
) -> Result<Option<Resolved>, db::Error> {
    if next_index == segments.len() {
        return Ok(Some(Resolved::AllByTimeDeletedChildren { parent_id }));
    }
    let children = repo.list_deleted_children(parent_id)?;
    let Some(entry) = find_by_timestamped_name(&children, segments[next_index], max_bytes, display)
    else {
        return Ok(None);
    };
    continue_into_deleted_entry(repo, entry, segments, next_index + 1, max_bytes, display)
}

/// Continues resolution once `deleted_entry` has just been matched, by whichever of the three
/// views named it - `segments[next_index..]` is whatever remains of the path after the segment
/// that named it. If `deleted_entry` is itself a directory and more path remains, that remainder
/// addresses its own soft-deleted children directly (`resolve_deleted_view` again), with no
/// further literal `[deleted]` segment required.
fn continue_into_deleted_entry(
    repo: &db::Repository,
    deleted_entry: db::DeletedEntry,
    segments: &[&str],
    next_index: usize,
    max_bytes: Option<usize>,
    display: TimeDisplay,
) -> Result<Option<Resolved>, db::Error> {
    if next_index == segments.len() {
        return Ok(Some(Resolved::Deleted(deleted_entry)));
    }
    resolve_deleted_view(
        repo,
        deleted_entry.entry.id,
        segments,
        next_index,
        max_bytes,
        display,
    )
}

/// REQ-TREE-009's `[deleted]` view: `children` filtered to at most one entry per distinct
/// original name - whichever was deleted most recently - paired with that name unchanged. No
/// disambiguation and no length constraint: at most one entry per name reaches this view, so it
/// is already unique by construction, and its name was already a valid entry name before (nothing
/// added that could push it over a length limit).
pub fn latest_by_name(children: &[(String, db::DeletedEntry)]) -> Vec<(String, db::DeletedEntry)> {
    let mut latest: HashMap<&str, db::DeletedEntry> = HashMap::new();
    for (name, entry) in children {
        latest
            .entry(name.as_str())
            .and_modify(|existing| {
                if entry.deleted_at > existing.deleted_at {
                    *existing = *entry;
                }
            })
            .or_insert(*entry);
    }
    latest
        .into_iter()
        .map(|(name, entry)| (name.to_string(), entry))
        .collect()
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

/// REQ-TREE-009's disambiguated display name (`[all]`) for a soft-deleted entry named
/// `base_name`, with `suffix` (a deletion timestamp or an id - the caller decides which) inserted
/// before its extension, respecting `max_bytes` (see [`fit_stem`]).
fn disambiguated_name(base_name: &str, suffix: &str, max_bytes: Option<usize>) -> String {
    let (stem, ext) = split_extension(base_name);
    fit_stem(stem, &format!(" [{suffix}]{ext}"), max_bytes)
}

/// Like [`disambiguated_name`], but with both the deletion timestamp and the entry's own id
/// appended as separate bracketed suffixes - used only when the timestamp suffix alone still
/// collides (REQ-TREE-009's id fallback), so the id becomes an additional disambiguator rather
/// than replacing the timestamp outright: the id must be unique regardless, but that is no reason
/// to also throw away the timestamp's own information value (REQ-TREE-009's rationale for leading
/// with it in the first place - "more informative at a glance than an opaque id"). Truncates the
/// stem - never either bracketed suffix - if the combination would not otherwise fit `max_bytes`.
fn disambiguated_name_with_id(
    base_name: &str,
    timestamp: &str,
    id: i64,
    max_bytes: Option<usize>,
) -> String {
    let (stem, ext) = split_extension(base_name);
    fit_stem(stem, &format!(" [{timestamp}] [{id}]{ext}"), max_bytes)
}

/// REQ-TREE-009's own display names for `children` under `[all]` (a directory's *full*
/// soft-deleted-child history, as [`db::Repository::list_deleted_children`] returns them): a bare
/// name where it does not collide with a sibling, the deletion-timestamp-suffixed form where it
/// does, and - only for the rare case where even that timestamp is shared down to the second by
/// more than one sibling with the same base name - the timestamp with the entry's own id
/// additionally appended ([`disambiguated_name_with_id`]), so two entries are never shown under
/// the exact same name without losing the timestamp's own information value along the way. Order
/// matches `children`'s own order. No length constraint; see [`display_names_within`] for a
/// caller that has one.
pub fn display_names(children: &[(String, db::DeletedEntry)]) -> Vec<String> {
    display_names_impl(children, None)
}

/// Like [`display_names`], but every name respects `max_bytes` (see [`fit_stem`]) - the mount's
/// own length-constrained display (`mountfs::MAX_NAME_BYTES` in practice).
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
                    let timestamp =
                        crate::time_format::format_deletion_suffix(children[index].1.deleted_at);
                    result[index] = disambiguated_name_with_id(
                        &children[index].0,
                        &timestamp,
                        children[index].1.entry.id,
                        max_bytes,
                    );
                }
            }
        }
    }
    result
}

/// Matches `segment` (as a caller typed it, e.g. copied from a `dfs list`-shown `[all]` line)
/// against `children`'s own [`display_names`]/[`display_names_within`], rather than trying to
/// parse the bracket syntax back out of an arbitrary string - reconstructing and comparing each
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
/// direction is reversed here since the prefix comes first, not last).
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

/// Builds `time_prefix` followed by `base_name` with `id`'s own disambiguating suffix inserted
/// before its extension (matching [`disambiguated_name`]'s own bracketed-before-extension form) -
/// used only when the timestamp's one-second resolution does not by itself tell two same-named
/// entries apart. The id is appended as a suffix rather than replacing the timestamp prefix
/// outright: dropping the timestamp there would silently break [`timestamped_display_names`]'s one
/// reason to exist, a plain alphabetic sort staying chronological. Truncates the stem - never the
/// timestamp prefix or the id suffix, the parts that must survive intact - if the combination would
/// not otherwise fit `max_bytes`.
fn timestamped_name_with_id_suffix(
    base_name: &str,
    time_prefix: &str,
    id: i64,
    max_bytes: Option<usize>,
) -> String {
    let (stem, ext) = split_extension(base_name);
    let prefix = format!("{time_prefix} ");
    let suffix = format!(" [{id}]{ext}");
    match max_bytes {
        Some(max) if prefix.len() + stem.len() + suffix.len() > max => {
            let budget = max.saturating_sub(prefix.len() + suffix.len());
            format!("{prefix}{}{suffix}", truncate_to_byte_budget(stem, budget))
        }
        _ => format!("{prefix}{stem}{suffix}"),
    }
}

/// REQ-TREE-009's `[all]/[by-time]` view names for `children`: each entry's deletion timestamp
/// always prefixed (unlike [`display_names`]'s suffix-only-when-ambiguous form), so a plain
/// alphabetic sort of the view also sorts chronologically - falling back to also appending the
/// entry's own id as a trailing suffix ([`timestamped_name_with_id_suffix`]), independent of any
/// length constraint, only in the rare case two entries share both the same name and the same
/// deletion second (the timestamp's one-second resolution is not enough to tell them apart then,
/// the same edge case [`display_names`] falls back to an id suffix for) - the timestamp prefix
/// itself always stays, so this fallback never costs the view its own chronological sortability.
/// `max_bytes` is the mount's own length constraint (`mountfs::MAX_NAME_BYTES`), truncating the
/// base name - never the prefix or, once needed, the id suffix - if even the unambiguous form
/// does not fit; `None` for no constraint. `display` is REQ-OPERABILITY-008's local-vs-UTC choice.
pub fn timestamped_display_names(
    children: &[(String, db::DeletedEntry)],
    max_bytes: Option<usize>,
    display: TimeDisplay,
) -> Vec<String> {
    let mut by_key: HashMap<(&str, String), Vec<usize>> = HashMap::new();
    for (index, (name, entry)) in children.iter().enumerate() {
        let second =
            crate::time_format::format_deletion_suffix_for_display(entry.deleted_at, display);
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
                result[index] = timestamped_name_with_id_suffix(
                    &children[index].0,
                    &second,
                    children[index].1.entry.id,
                    max_bytes,
                );
            }
        }
    }
    result
}

/// Matches `segment` against `children`'s own [`timestamped_display_names`] - the same
/// reconstruct-and-compare approach [`find_by_display_name`] uses for the `[all]` scheme.
fn find_by_timestamped_name(
    children: &[(String, db::DeletedEntry)],
    segment: &str,
    max_bytes: Option<usize>,
    display: TimeDisplay,
) -> Option<db::DeletedEntry> {
    timestamped_display_names(children, max_bytes, display)
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
            disambiguated_name("photo.jpg", "2026-08-22_14-04-14", None),
            "photo [2026-08-22_14-04-14].jpg"
        );
        assert_eq!(
            disambiguated_name(".env", "2026-08-22_14-04-14", None),
            ".env [2026-08-22_14-04-14]"
        );
    }

    #[test]
    fn disambiguated_name_truncates_the_stem_not_the_suffix_when_over_budget() {
        let long_stem = "a".repeat(50);
        let name = format!("{long_stem}.jpg");
        let result = disambiguated_name(&name, "2026-08-22_14-04-14", Some(30));
        assert!(result.len() <= 30, "got {} bytes: {result}", result.len());
        assert!(
            result.ends_with(" [2026-08-22_14-04-14].jpg"),
            "the suffix and extension must survive intact: {result}"
        );
    }

    #[test]
    fn disambiguated_name_with_id_truncates_the_stem_not_either_bracketed_suffix() {
        let long_stem = "a".repeat(50);
        let name = format!("{long_stem}.jpg");
        let result = disambiguated_name_with_id(&name, "2026-08-22_14-04-14", 42, Some(40));
        assert!(result.len() <= 40, "got {} bytes: {result}", result.len());
        assert!(
            result.ends_with(" [2026-08-22_14-04-14] [42].jpg"),
            "both bracketed suffixes and the extension must survive intact: {result}"
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
    fn latest_by_name_leaves_a_single_entry_unchanged() {
        let children = vec![("a.txt".to_string(), deleted_entry(1, 100))];
        let latest = latest_by_name(&children);
        assert_eq!(latest.len(), 1);
        assert_eq!(latest[0].0, "a.txt");
        assert_eq!(latest[0].1.entry.id, 1);
        assert_eq!(latest[0].1.deleted_at, 100);
    }

    #[test]
    fn latest_by_name_keeps_only_the_most_recently_deleted_entry_per_name() {
        let children = vec![
            ("a.txt".to_string(), deleted_entry(1, 100)),
            ("a.txt".to_string(), deleted_entry(2, 200)),
            ("b.txt".to_string(), deleted_entry(3, 150)),
        ];
        let mut latest = latest_by_name(&children);
        latest.sort_by(|(name, _), (other, _)| name.cmp(other));
        assert_eq!(latest.len(), 2);
        assert_eq!(latest[0].0, "a.txt");
        assert_eq!(latest[0].1.entry.id, 2);
        assert_eq!(latest[0].1.deleted_at, 200);
        assert_eq!(latest[1].0, "b.txt");
        assert_eq!(latest[1].1.entry.id, 3);
        assert_eq!(latest[1].1.deleted_at, 150);
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
    fn display_names_appends_the_id_without_dropping_the_timestamp_when_it_alone_still_collides() {
        // Same base name, same deletion second - the timestamp suffix alone would not
        // disambiguate them, but that is no reason to lose it entirely: it stays visible,
        // with the id appended as a second, genuinely unique suffix.
        let children = vec![
            ("a.txt".to_string(), deleted_entry(1, 100_000)),
            ("a.txt".to_string(), deleted_entry(2, 100_000)),
        ];
        let names = display_names(&children);
        assert_ne!(names[0], names[1]);
        assert_eq!(names[0], "a [1970-01-01_00-01-40] [1].txt");
        assert_eq!(names[1], "a [1970-01-01_00-01-40] [2].txt");
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
        let names = timestamped_display_names(&children, None, TimeDisplay::Utc);
        assert_eq!(names[0], "2000-01-01_00-00-00Z a.txt");
    }

    #[test]
    fn timestamped_display_names_sorts_chronologically_by_construction() {
        // Deliberately inserted out of chronological order (index 0 is the later deletion) - the
        // always-prefixed timestamp must still sort them back into deletion order.
        let children = vec![
            ("later.txt".to_string(), deleted_entry(1, 946_684_900_000)),
            ("earlier.txt".to_string(), deleted_entry(2, 946_684_800_000)),
        ];
        let names = timestamped_display_names(&children, None, TimeDisplay::Utc);
        let mut sorted = names.clone();
        sorted.sort();
        assert_eq!(
            sorted,
            vec![names[1].clone(), names[0].clone()],
            "a plain sort must reorder to the earlier deletion first: {names:?}"
        );
    }

    #[test]
    fn timestamped_display_names_falls_back_to_a_trailing_id_suffix_on_a_same_second_collision() {
        let children = vec![
            ("a.txt".to_string(), deleted_entry(1, 100_000)),
            ("a.txt".to_string(), deleted_entry(2, 100_000)),
        ];
        let names = timestamped_display_names(&children, None, TimeDisplay::Utc);
        assert_ne!(names[0], names[1]);
        // The timestamp prefix must survive the fallback too - dropping it would break this
        // view's whole reason to exist, staying sortable chronologically by plain name.
        assert_eq!(names[0], "1970-01-01_00-01-40Z a [1].txt");
        assert_eq!(names[1], "1970-01-01_00-01-40Z a [2].txt");
    }

    #[test]
    fn timestamped_display_names_within_truncates_the_stem_not_the_timestamp_or_id_suffix() {
        let long_stem = "a".repeat(50);
        let children = vec![
            (
                format!("{long_stem}.jpg"),
                deleted_entry(1, 946_684_800_000),
            ),
            (
                format!("{long_stem}.jpg"),
                deleted_entry(2, 946_684_800_000),
            ),
        ];
        let names = timestamped_display_names(&children, Some(40), TimeDisplay::Utc);
        assert_ne!(names[0], names[1]);
        for name in &names {
            assert!(name.len() <= 40, "got {} bytes: {name}", name.len());
            assert!(
                name.starts_with("2000-01-01_00-00-00Z "),
                "the timestamp prefix must survive: {name}"
            );
            assert!(name.ends_with(".jpg"), "the extension must survive: {name}");
        }
        assert!(names[0].contains("[1]"), "got {}", names[0]);
        assert!(names[1].contains("[2]"), "got {}", names[1]);
    }

    #[test]
    fn timestamped_display_names_within_truncates_the_base_name_not_the_prefix() {
        let long_stem = "a".repeat(50);
        let children = vec![(long_stem.clone(), deleted_entry(1, 946_684_800_000))];
        let names = timestamped_display_names(&children, Some(30), TimeDisplay::Utc);
        assert!(
            names[0].len() <= 30,
            "got {} bytes: {}",
            names[0].len(),
            names[0]
        );
        assert!(
            names[0].starts_with("2000-01-01_00-00-00Z "),
            "the prefix must survive intact: {}",
            names[0]
        );
    }

    #[test]
    fn find_by_timestamped_name_matches_a_prefixed_name_back_to_its_entry() {
        let children = vec![("a.txt".to_string(), deleted_entry(1, 946_684_800_000))];
        let found = find_by_timestamped_name(
            &children,
            "2000-01-01_00-00-00Z a.txt",
            None,
            TimeDisplay::Utc,
        )
        .unwrap();
        assert_eq!(found.entry.id, 1);
    }

    #[test]
    fn find_by_timestamped_name_returns_none_without_a_match() {
        let children = vec![("a.txt".to_string(), deleted_entry(1, 946_684_800_000))];
        assert!(find_by_timestamped_name(&children, "nope", None, TimeDisplay::Utc).is_none());
    }

    #[test]
    fn strip_show_deleted_prefix_removes_a_leading_show_deleted_segment() {
        assert_eq!(strip_show_deleted_prefix("/[show-deleted]"), "/");
        assert_eq!(
            strip_show_deleted_prefix("/[show-deleted]/a/[deleted]/b.txt"),
            "/a/[deleted]/b.txt"
        );
    }

    #[test]
    fn strip_show_deleted_prefix_leaves_a_path_without_the_prefix_unchanged() {
        assert_eq!(
            strip_show_deleted_prefix("/a/[deleted]/b.txt"),
            "/a/[deleted]/b.txt"
        );
        assert_eq!(
            strip_show_deleted_prefix("/[show-deleted]extra"),
            "/[show-deleted]extra"
        );
    }

    #[test]
    fn resolve_returns_live_for_an_ordinary_path() {
        let (repo, _dir) = repo_and_dir();
        repo.mkdir(0, "photos", 100).unwrap();

        let resolved = resolve(&repo, "/photos", TimeDisplay::Utc)
            .unwrap()
            .unwrap();
        assert!(matches!(resolved, Resolved::Live(entry) if entry.kind == db::EntryKind::Dir));
    }

    #[test]
    fn resolve_returns_none_for_a_path_that_does_not_exist() {
        let (repo, _dir) = repo_and_dir();
        assert!(resolve(&repo, "/nope", TimeDisplay::Utc).unwrap().is_none());
    }

    #[test]
    fn resolve_returns_deleted_children_for_a_bare_deleted_segment() {
        let (repo, _dir) = repo_and_dir();
        let photos = repo.mkdir(0, "photos", 100).unwrap();
        let content_id = repo.find_or_create_content(0, &[0xAAu8; 20], &[]).unwrap();
        let file_id = repo.settle_file(photos, "a.txt", 100, content_id).unwrap();
        repo.unlink_file(file_id, 200).unwrap();

        let resolved = resolve(&repo, "/photos/[deleted]", TimeDisplay::Utc)
            .unwrap()
            .unwrap();
        match resolved {
            Resolved::DeletedChildren { parent_id } => assert_eq!(parent_id, photos),
            _ => panic!("expected DeletedChildren"),
        }
    }

    #[test]
    fn resolve_addresses_the_most_recent_entry_for_a_name_by_its_bare_unmodified_name() {
        let (repo, _dir) = repo_and_dir();
        let content_a = repo.find_or_create_content(1, &[0xAAu8; 20], &[]).unwrap();
        let content_b = repo.find_or_create_content(2, &[0xBBu8; 20], &[]).unwrap();
        let _first = repo.settle_file(0, "a.txt", 100, content_a).unwrap();
        let second = repo.settle_file(0, "a.txt", 200, content_b).unwrap();
        repo.unlink_file(second, 300).unwrap();

        let resolved = resolve(&repo, "/[deleted]/a.txt", TimeDisplay::Utc)
            .unwrap()
            .unwrap();
        match resolved {
            // settle_file's own replace already soft-deleted `first`; `second` was soft-deleted
            // last (at 300), so it is the one "a.txt" addresses.
            Resolved::Deleted(entry) => assert_eq!(entry.entry.id, second),
            _ => panic!("expected Deleted"),
        }
    }

    #[test]
    fn resolve_addresses_an_older_entry_only_through_all() {
        let (repo, _dir) = repo_and_dir();
        let content_a = repo.find_or_create_content(1, &[0xAAu8; 20], &[]).unwrap();
        let content_b = repo.find_or_create_content(2, &[0xBBu8; 20], &[]).unwrap();
        let first = repo.settle_file(0, "a.txt", 100, content_a).unwrap();
        let second = repo.settle_file(0, "a.txt", 200, content_b).unwrap();
        repo.unlink_file(second, 300).unwrap();

        let children = repo.list_deleted_children(0).unwrap();
        let names = display_names(&children);
        let first_name = names[children
            .iter()
            .position(|(_, e)| e.entry.id == first)
            .unwrap()]
        .clone();

        let resolved = resolve(
            &repo,
            &format!("/[deleted]/[all]/{first_name}"),
            TimeDisplay::Utc,
        )
        .unwrap()
        .unwrap();
        match resolved {
            Resolved::Deleted(entry) => assert_eq!(entry.entry.id, first),
            _ => panic!("expected Deleted"),
        }
        // Not reachable through the bare, "most recent" view - that name is already taken by
        // `second`.
        assert!(
            resolve(&repo, &format!("/[deleted]/{first_name}"), TimeDisplay::Utc)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn resolve_returns_all_deleted_children_for_a_bare_all_segment() {
        let (repo, _dir) = repo_and_dir();
        let content_id = repo.find_or_create_content(0, &[0xAAu8; 20], &[]).unwrap();
        let file_id = repo.settle_file(0, "a.txt", 100, content_id).unwrap();
        repo.unlink_file(file_id, 200).unwrap();

        let resolved = resolve(&repo, "/[deleted]/[all]", TimeDisplay::Utc)
            .unwrap()
            .unwrap();
        assert!(matches!(
            resolved,
            Resolved::AllDeletedChildren { parent_id: 0 }
        ));
    }

    #[test]
    fn resolve_addresses_an_entry_through_all_by_time() {
        let (repo, _dir) = repo_and_dir();
        let content_id = repo.find_or_create_content(0, &[0xAAu8; 20], &[]).unwrap();
        let file_id = repo.settle_file(0, "a.txt", 100, content_id).unwrap();
        repo.unlink_file(file_id, 946_684_800_000).unwrap();

        let resolved = resolve(
            &repo,
            "/[deleted]/[all]/[by-time]/2000-01-01_00-00-00Z a.txt",
            TimeDisplay::Utc,
        )
        .unwrap()
        .unwrap();
        match resolved {
            Resolved::Deleted(entry) => assert_eq!(entry.entry.id, file_id),
            _ => panic!("expected Deleted"),
        }
    }

    #[test]
    fn resolve_returns_all_by_time_deleted_children_for_a_bare_by_time_segment() {
        let (repo, _dir) = repo_and_dir();
        let content_id = repo.find_or_create_content(0, &[0xAAu8; 20], &[]).unwrap();
        let file_id = repo.settle_file(0, "a.txt", 100, content_id).unwrap();
        repo.unlink_file(file_id, 200).unwrap();

        let resolved = resolve(&repo, "/[deleted]/[all]/[by-time]", TimeDisplay::Utc)
            .unwrap()
            .unwrap();
        assert!(matches!(
            resolved,
            Resolved::AllByTimeDeletedChildren { parent_id: 0 }
        ));
    }

    #[test]
    fn resolve_lets_a_real_live_entry_win_over_the_deleted_segment() {
        let (repo, _dir) = repo_and_dir();
        repo.mkdir(0, "[deleted]", 100).unwrap();

        let resolved = resolve(&repo, "/[deleted]", TimeDisplay::Utc)
            .unwrap()
            .unwrap();
        assert!(matches!(resolved, Resolved::Live(entry) if entry.kind == db::EntryKind::Dir));
    }

    #[test]
    fn resolve_descends_into_an_already_deleted_directorys_own_children_without_a_second_deleted_segment()
     {
        let (repo, _dir) = repo_and_dir();
        let a_id = repo.mkdir(0, "a", 100).unwrap();
        let content_id = repo.find_or_create_content(0, &[0xAAu8; 20], &[]).unwrap();
        let file_id = repo.settle_file(a_id, "f.txt", 100, content_id).unwrap();
        repo.unlink_file(file_id, 150).unwrap();
        repo.rmdir(a_id, 200).unwrap();

        // No second `[deleted]` needed between `a` and `f.txt` - `a` is already dead, so its own
        // children appear directly.
        let resolved = resolve(&repo, "/[deleted]/a/f.txt", TimeDisplay::Utc)
            .unwrap()
            .unwrap();
        match resolved {
            Resolved::Deleted(entry) => assert_eq!(entry.entry.id, file_id),
            _ => panic!("expected Deleted"),
        }
    }

    #[test]
    fn resolve_reaches_all_and_all_by_time_recursively_within_an_already_deleted_directory() {
        let (repo, _dir) = repo_and_dir();
        let a_id = repo.mkdir(0, "a", 100).unwrap();
        let content_x = repo.find_or_create_content(1, &[0xAAu8; 20], &[]).unwrap();
        let content_y = repo.find_or_create_content(2, &[0xBBu8; 20], &[]).unwrap();
        let older = repo.settle_file(a_id, "f.txt", 100, content_x).unwrap();
        let newer = repo.settle_file(a_id, "f.txt", 150, content_y).unwrap();
        repo.unlink_file(newer, 175).unwrap();
        repo.rmdir(a_id, 200).unwrap();

        // Bare name inside the already-dead `a` reaches the newest "f.txt".
        let resolved = resolve(&repo, "/[deleted]/a/f.txt", TimeDisplay::Utc)
            .unwrap()
            .unwrap();
        assert!(matches!(resolved, Resolved::Deleted(e) if e.entry.id == newer));

        // `a`'s own `[all]` reaches the full history, including the older, superseded entry.
        let all_resolved = resolve(&repo, "/[deleted]/a/[all]", TimeDisplay::Utc)
            .unwrap()
            .unwrap();
        assert!(matches!(
            all_resolved,
            Resolved::AllDeletedChildren { parent_id } if parent_id == a_id
        ));
        let children = repo.list_deleted_children(a_id).unwrap();
        let names = display_names(&children);
        let older_name = names[children
            .iter()
            .position(|(_, e)| e.entry.id == older)
            .unwrap()]
        .clone();
        let older_resolved = resolve(
            &repo,
            &format!("/[deleted]/a/[all]/{older_name}"),
            TimeDisplay::Utc,
        )
        .unwrap()
        .unwrap();
        assert!(matches!(older_resolved, Resolved::Deleted(e) if e.entry.id == older));
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

        let resolved = resolve_within(
            &repo,
            &format!("/[deleted]/[all]/{first_shown}"),
            30,
            TimeDisplay::Utc,
        )
        .unwrap()
        .unwrap();
        match resolved {
            Resolved::Deleted(entry) => assert_eq!(entry.entry.id, first),
            _ => panic!("expected Deleted"),
        }
    }
}
