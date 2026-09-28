//! `dfs list` - REQ-QUERY-001 in requirements/functional/query.md, REQ-CLI-007 in
//! requirements/functional/cli-commands.md. Lists a directory's live, direct children without
//! mounting; deletion history is reached through REQ-TREE-009's `[deleted]`/`[all]`/
//! `[all]/[by-time]` addressing (`crate::deleted`), always present - `[show-deleted]` itself shows
//! up as an ordinary entry in the repository root's own listing, the same way the mount exposes it
//! (REQ-MOUNT-004), and a path naming it is stripped and resolved the same as one without it
//! (`deleted::strip_show_deleted_prefix`).

use std::path::Path;

use crate::deleted::{self, ALL_SEGMENT, BY_TIME_SEGMENT, DELETED_SEGMENT, Resolved};
use crate::entry_format::{format_line, kind_label};
use crate::time_format::TimeDisplay;

/// The listing "kind" column value for a synthetic marker row (`[deleted]`, `[show-deleted]`,
/// `[all]`, `[by-time]`) - distinct from `dir`/`file` so it is never confused with a real,
/// identically-named live directory (REQ-TREE-009's real-wins rule already shows that as an
/// ordinary `dir` entry, without needing this marker at all).
const VIRTUAL_KIND: &str = "virt";

fn try_run(
    repo_path: &Path,
    default_path_used: bool,
    target_path: &str,
    assume_read_only_medium: bool,
    display: TimeDisplay,
) -> Result<String, String> {
    // DESIGN-METADATA-013: validates an explicit --assume-read-only-medium against what actually
    // happens, rather than trusting it blindly - see the called function's own doc comment.
    let repo = match db::open_repository_read_only_with_medium_assertion(
        repo_path,
        assume_read_only_medium,
    ) {
        Ok(repo) => repo,
        Err(db::Error::NoRepositoryHere(_)) if default_path_used => {
            return Err(format!(
                "error: no repository found at the default location ({}).\n\
                 Pass a repository path explicitly instead.",
                repo_path.display()
            ));
        }
        Err(err) => return Err(format!("error: {err}")),
    };

    let stripped = deleted::strip_show_deleted_prefix(target_path);
    let resolved = match deleted::resolve(&repo, stripped, display) {
        Ok(Some(resolved)) => resolved,
        Ok(None) => return Err(format!("error: no such repository path: {target_path}")),
        Err(err) => return Err(format!("error: {err}")),
    };

    match resolved {
        Resolved::Live(entry) => list_live(&repo, target_path, &entry, display),
        Resolved::DeletedChildren { parent_id } => {
            list_deleted(&repo, target_path, parent_id, display)
        }
        Resolved::AllDeletedChildren { parent_id } => {
            list_all_deleted(&repo, target_path, parent_id, display)
        }
        Resolved::AllByTimeDeletedChildren { parent_id } => {
            list_all_by_time_deleted(&repo, target_path, parent_id, display)
        }
        // REQ-TREE-008: a soft-deleted directory's own children are always themselves
        // soft-deleted. No live/dead boundary left to signal here, so its own children are
        // listed directly under DESIGN-MOUNT-021's same "most recent per name" default - never a
        // repeated `[deleted]` segment one level down.
        Resolved::Deleted(entry) if entry.entry.kind == db::EntryKind::Dir => {
            list_deleted(&repo, target_path, entry.entry.id, display)
        }
        Resolved::Deleted(_) => Err(format!("error: {target_path} is not a directory")),
    }
}

fn list_live(
    repo: &db::Repository,
    target_path: &str,
    dir: &db::Entry,
    display: TimeDisplay,
) -> Result<String, String> {
    let children = match repo.list_children(dir.id) {
        Ok(children) => children,
        Err(db::Error::WrongKind(_)) => {
            return Err(format!("error: {target_path} is not a directory"));
        }
        Err(err) => return Err(format!("error: {err}")),
    };
    // REQ-TREE-009: a real live entry already named `[deleted]` (or, at the root,
    // `[show-deleted]`) wins outright - it is already in `children` above, listed like any other
    // live entry, so the marker below is only added when nothing real occupies that name yet.
    let already_real = |name: &str| children.iter().any(|(n, _)| n == name);

    let mut rows: Vec<(String, String)> = children
        .iter()
        .map(|(name, entry)| {
            (
                name.clone(),
                format_line(
                    kind_label(entry.kind),
                    entry.size,
                    entry.time_millis,
                    name,
                    display,
                ),
            )
        })
        .collect();

    if !already_real(DELETED_SEGMENT) {
        let deleted_children = repo
            .list_deleted_children(dir.id)
            .map_err(|err| format!("error: {err}"))?;
        if let Some(most_recent) = deleted_children.iter().map(|(_, e)| e.deleted_at).max() {
            rows.push((
                DELETED_SEGMENT.to_string(),
                format_line(VIRTUAL_KIND, 0, most_recent, DELETED_SEGMENT, display),
            ));
        }
    }
    // DESIGN-MOUNT-020/024: `[show-deleted]` appears only in the real repository root's own
    // listing, the same as the mount's own root readdir - never inline elsewhere.
    if dir.id == 0 && !already_real(deleted::SHOW_DELETED_SEGMENT) {
        rows.push((
            deleted::SHOW_DELETED_SEGMENT.to_string(),
            format_line(
                VIRTUAL_KIND,
                0,
                dir.time_millis,
                deleted::SHOW_DELETED_SEGMENT,
                display,
            ),
        ));
    }

    if rows.is_empty() {
        return Ok(format!("{target_path}: empty"));
    }
    rows.sort_by(|(a, _), (b, _)| a.cmp(b));
    Ok(rows
        .into_iter()
        .map(|(_, line)| line)
        .collect::<Vec<_>>()
        .join("\n"))
}

/// `[deleted]` itself: `parent_id`'s soft-deleted children filtered to the most recent version per
/// name (`deleted::latest_by_name`), under their own unmodified names, plus an `[all]` marker once
/// there is any history at all.
fn list_deleted(
    repo: &db::Repository,
    target_path: &str,
    parent_id: i64,
    display: TimeDisplay,
) -> Result<String, String> {
    let children = repo
        .list_deleted_children(parent_id)
        .map_err(|err| format!("error: {err}"))?;
    if children.is_empty() {
        return Ok(format!("{target_path}: empty"));
    }

    let mut rows: Vec<(String, String)> = deleted::latest_by_name(&children)
        .into_iter()
        .map(|(name, entry)| {
            let line = format_line(
                kind_label(entry.entry.kind),
                entry.entry.size,
                entry.entry.time_millis,
                &name,
                display,
            );
            (name, line)
        })
        .collect();
    rows.push((
        ALL_SEGMENT.to_string(),
        format_line(VIRTUAL_KIND, 0, 0, ALL_SEGMENT, display),
    ));
    rows.sort_by(|(a, _), (b, _)| a.cmp(b));
    Ok(rows
        .into_iter()
        .map(|(_, line)| line)
        .collect::<Vec<_>>()
        .join("\n"))
}

/// `[deleted]/[all]`: `parent_id`'s full, disambiguated soft-deleted-child history, plus a
/// `[by-time]` marker once there is any history at all.
fn list_all_deleted(
    repo: &db::Repository,
    target_path: &str,
    parent_id: i64,
    display: TimeDisplay,
) -> Result<String, String> {
    let children = repo
        .list_deleted_children(parent_id)
        .map_err(|err| format!("error: {err}"))?;
    if children.is_empty() {
        return Ok(format!("{target_path}: empty"));
    }

    let mut rows: Vec<(String, String)> = deleted::display_names(&children)
        .into_iter()
        .zip(&children)
        .map(|(display_name, (_, entry))| {
            (
                display_name.clone(),
                format_line(
                    kind_label(entry.entry.kind),
                    entry.entry.size,
                    entry.entry.time_millis,
                    &display_name,
                    display,
                ),
            )
        })
        .collect();
    rows.push((
        BY_TIME_SEGMENT.to_string(),
        format_line(VIRTUAL_KIND, 0, 0, BY_TIME_SEGMENT, display),
    ));
    rows.sort_by(|(a, _), (b, _)| a.cmp(b));
    Ok(rows
        .into_iter()
        .map(|(_, line)| line)
        .collect::<Vec<_>>()
        .join("\n"))
}

/// `[deleted]/[all]/[by-time]`: the same entries as [`list_all_deleted`], chronologically named -
/// no further marker, since `[by-time]` is not itself nested any deeper.
fn list_all_by_time_deleted(
    repo: &db::Repository,
    target_path: &str,
    parent_id: i64,
    display: TimeDisplay,
) -> Result<String, String> {
    let children = repo
        .list_deleted_children(parent_id)
        .map_err(|err| format!("error: {err}"))?;
    if children.is_empty() {
        return Ok(format!("{target_path}: empty"));
    }

    // A plain alphabetic sort already sorts chronologically here - the name itself always leads
    // with the timestamp (REQ-TREE-009).
    let mut rows: Vec<(String, String)> =
        deleted::timestamped_display_names(&children, None, display)
            .into_iter()
            .zip(&children)
            .map(|(display_name, (_, entry))| {
                (
                    display_name.clone(),
                    format_line(
                        kind_label(entry.entry.kind),
                        entry.entry.size,
                        entry.entry.time_millis,
                        &display_name,
                        display,
                    ),
                )
            })
            .collect();
    rows.sort_by(|(a, _), (b, _)| a.cmp(b));
    Ok(rows
        .into_iter()
        .map(|(_, line)| line)
        .collect::<Vec<_>>()
        .join("\n"))
}

pub fn run(
    repo_path: &Path,
    default_path_used: bool,
    target_path: &str,
    assume_read_only_medium: bool,
    display: TimeDisplay,
) {
    match try_run(
        repo_path,
        default_path_used,
        target_path,
        assume_read_only_medium,
        display,
    ) {
        Ok(message) => println!("{message}"),
        Err(message) => {
            eprintln!("{message}");
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn try_run_gives_an_actionable_message_when_the_default_path_holds_no_repository() {
        let repo_path = std::env::temp_dir().join("dfs-list-test-no-default-repository-here");

        let message = try_run(&repo_path, true, "/", false, TimeDisplay::Utc)
            .expect_err("must fail - repo_path holds no repository");
        assert!(
            message.contains("no repository"),
            "expected the actionable default-path message, got: {message}"
        );
        assert!(
            message.contains("explicitly"),
            "expected a hint to pass the path explicitly, got: {message}"
        );
    }

    // DESIGN-METADATA-013's actual CLI-level wiring check, mirroring `db`'s own
    // `open_repository_read_only_immutable_succeeds_on_a_pristine_repository_over_an_unwritable_directory`
    // test (which already covers the underlying mechanism in full) - this one just confirms
    // `--assume-read-only-medium` actually reaches it through `list`, one representative command
    // rather than all of `list`/`find`/`stats`/`restore`/`db-backup`/`mount`, which all thread it
    // through identically. Unix-only for the same chmod-based reason as that test.
    #[cfg(unix)]
    #[test]
    fn assume_read_only_medium_lets_list_open_a_pristine_repository_over_an_unwritable_directory() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let repo_root = dir.path().join("repo");
        db::init_repository(
            &repo_root,
            db::RepositorySettings::new(20, 1_700_000_000_000),
        )
        .unwrap();
        let meta_dir = repo_root.join("meta");
        let original_permissions = std::fs::metadata(&meta_dir).unwrap().permissions();
        std::fs::set_permissions(&meta_dir, std::fs::Permissions::from_mode(0o555)).unwrap();

        let without_flag = try_run(&repo_root, false, "/", false, TimeDisplay::Utc);
        let with_flag = try_run(&repo_root, false, "/", true, TimeDisplay::Utc);
        std::fs::set_permissions(&meta_dir, original_permissions).unwrap(); // before any assertion

        assert!(
            without_flag.is_err(),
            "expected the plain open to fail against a pristine repository on an unwritable \
             directory, got: {without_flag:?}"
        );
        let message = with_flag.unwrap();
        // Never truly "empty": [show-deleted] is always present (DESIGN-MOUNT-020) - matches
        // try_run_reports_an_empty_root_as_holding_only_show_deleted's own assertion shape.
        assert_eq!(message.lines().count(), 1);
        assert!(message.starts_with("virt"));
        assert!(message.ends_with(deleted::SHOW_DELETED_SEGMENT));
    }

    fn setup() -> (db::Repository, tempfile::TempDir) {
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
    fn try_run_reports_an_empty_root_as_holding_only_show_deleted() {
        let (repo, dir) = setup();
        drop(repo);
        let repo_root = dir.path().join("repo");

        let message = try_run(&repo_root, false, "/", false, TimeDisplay::Utc)
            .expect("must succeed - root exists");
        // Never truly "empty": [show-deleted] is always present (DESIGN-MOUNT-020).
        assert_eq!(message.lines().count(), 1);
        assert!(message.starts_with("virt"));
        assert!(message.ends_with(deleted::SHOW_DELETED_SEGMENT));
    }

    #[test]
    fn try_run_lists_direct_children_sorted_by_name_with_kind_size_and_mtime() {
        let (repo, dir) = setup();
        repo.mkdir(0, "b-dir", 1_700_000_000_000).unwrap();
        let content_id = repo
            .find_or_create_content(3, b"AAAAAAAAAAAAAAAAAAAA", &[])
            .unwrap();
        repo.settle_file(0, "a-file.txt", 1_700_000_000_000, content_id)
            .unwrap();
        drop(repo);
        let repo_root = dir.path().join("repo");

        let message =
            try_run(&repo_root, false, "/", false, TimeDisplay::Utc).expect("must succeed");
        let lines: Vec<&str> = message.lines().collect();
        // Two real children plus the always-present [show-deleted] entry - "[" sorts before
        // lowercase letters, so [show-deleted] comes first alphabetically.
        assert_eq!(lines.len(), 3);
        assert!(
            lines[0].ends_with(deleted::SHOW_DELETED_SEGMENT) && lines[0].starts_with("virt"),
            "expected [show-deleted] first (alphabetical - '[' sorts before letters), got: {}",
            lines[0]
        );
        assert!(
            lines[1].contains("a-file.txt") && lines[1].starts_with("file"),
            "expected the file entry second (alphabetical), got: {}",
            lines[1]
        );
        assert!(
            lines[1].contains(" 3 "),
            "expected the file's logical size (3 bytes), got: {}",
            lines[1]
        );
        assert!(
            lines[2].contains("b-dir") && lines[2].starts_with("dir"),
            "expected the directory entry third (alphabetical), got: {}",
            lines[2]
        );
    }

    #[test]
    fn try_run_reports_a_missing_path_clearly() {
        let (repo, dir) = setup();
        drop(repo);
        let repo_root = dir.path().join("repo");

        let message = try_run(
            &repo_root,
            false,
            "/does-not-exist",
            false,
            TimeDisplay::Utc,
        )
        .expect_err("must fail - the path does not exist");
        assert!(
            message.contains("no such repository path"),
            "expected a no-such-path message, got: {message}"
        );
    }

    #[test]
    fn try_run_refuses_to_list_a_file_as_if_it_were_a_directory() {
        let (repo, dir) = setup();
        let content_id = repo
            .find_or_create_content(0, b"BBBBBBBBBBBBBBBBBBBB", &[])
            .unwrap();
        repo.settle_file(0, "a.txt", 1_700_000_000_000, content_id)
            .unwrap();
        drop(repo);
        let repo_root = dir.path().join("repo");

        let message = try_run(&repo_root, false, "/a.txt", false, TimeDisplay::Utc)
            .expect_err("must fail - a.txt is a file, not a directory");
        assert!(
            message.contains("not a directory"),
            "expected a not-a-directory message, got: {message}"
        );
    }

    fn delete_a_file(repo: &db::Repository, name: &str, deleted_at: i64) -> i64 {
        let content_id = repo
            .find_or_create_content(0, format!("{name}-hash-000000").as_bytes(), &[])
            .unwrap();
        let id = repo
            .settle_file(0, name, 1_700_000_000_000, content_id)
            .unwrap();
        repo.unlink_file(id, deleted_at).unwrap();
        id
    }

    #[test]
    fn root_listing_always_shows_deleted_and_show_deleted_once_history_exists() {
        let (repo, dir) = setup();
        delete_a_file(&repo, "gone.txt", 1_700_000_100_000);
        drop(repo);
        let repo_root = dir.path().join("repo");

        let message =
            try_run(&repo_root, false, "/", false, TimeDisplay::Utc).expect("must succeed");
        assert!(message.contains(VIRTUAL_KIND));
        assert!(message.contains(DELETED_SEGMENT));
        assert!(message.contains(deleted::SHOW_DELETED_SEGMENT));
    }

    #[test]
    fn show_deleted_is_shown_at_root_even_without_any_deletion_history() {
        let (repo, dir) = setup();
        repo.mkdir(0, "a", 1_700_000_000_000).unwrap();
        drop(repo);
        let repo_root = dir.path().join("repo");

        let message =
            try_run(&repo_root, false, "/", false, TimeDisplay::Utc).expect("must succeed");
        assert!(message.contains(deleted::SHOW_DELETED_SEGMENT));
        assert!(!message.contains(DELETED_SEGMENT));
    }

    #[test]
    fn a_real_live_directory_named_deleted_wins_over_the_marker() {
        let (repo, dir) = setup();
        delete_a_file(&repo, "gone.txt", 1_700_000_100_000);
        repo.mkdir(0, DELETED_SEGMENT, 1_700_000_000_000).unwrap();
        drop(repo);
        let repo_root = dir.path().join("repo");

        let message =
            try_run(&repo_root, false, "/", false, TimeDisplay::Utc).expect("must succeed");
        let deleted_lines: Vec<&str> = message
            .lines()
            .filter(|line| line.ends_with(DELETED_SEGMENT))
            .collect();
        assert_eq!(
            deleted_lines.len(),
            1,
            "the real directory must be shown exactly once, not duplicated by a marker: {message}"
        );
        assert!(
            deleted_lines[0].starts_with("dir"),
            "got: {}",
            deleted_lines[0]
        );
    }

    #[test]
    fn listing_the_deleted_segment_shows_its_children_under_their_unmodified_name() {
        let (repo, dir) = setup();
        delete_a_file(&repo, "gone.txt", 1_700_000_100_000);
        drop(repo);
        let repo_root = dir.path().join("repo");

        let message = try_run(
            &repo_root,
            false,
            &format!("/{DELETED_SEGMENT}"),
            false,
            TimeDisplay::Utc,
        )
        .expect("must succeed");
        assert!(message.contains("gone.txt"));
        assert!(message.starts_with("file") || message.contains("\nfile"));
        assert!(
            message.contains(ALL_SEGMENT),
            "the [all] marker must appear once there is any history: {message}"
        );
    }

    #[test]
    fn listing_the_deleted_segment_shows_only_the_latest_version_per_name() {
        let (repo, dir) = setup();
        delete_a_file(&repo, "gone.txt", 1_700_000_100_000);
        let content_id = repo
            .find_or_create_content(1, b"gone.txt-hash-000001", &[])
            .unwrap();
        let id = repo
            .settle_file(0, "gone.txt", 1_700_000_150_000, content_id)
            .unwrap();
        repo.unlink_file(id, 1_700_000_200_000).unwrap();
        drop(repo);
        let repo_root = dir.path().join("repo");

        let message = try_run(
            &repo_root,
            false,
            &format!("/{DELETED_SEGMENT}"),
            false,
            TimeDisplay::Utc,
        )
        .expect("must succeed");
        let gone_lines: Vec<&str> = message.lines().filter(|l| l.contains("gone")).collect();
        assert_eq!(
            gone_lines.len(),
            1,
            "only the most recent version should appear under the plain name: {message}"
        );
        assert!(gone_lines[0].ends_with("gone.txt"), "got: {message}");
    }

    #[test]
    fn listing_all_disambiguates_same_named_entries() {
        let (repo, dir) = setup();
        delete_a_file(&repo, "gone.txt", 1_700_000_100_000);
        let content_id = repo
            .find_or_create_content(1, b"gone.txt-hash-000001", &[])
            .unwrap();
        let id = repo
            .settle_file(0, "gone.txt", 1_700_000_150_000, content_id)
            .unwrap();
        repo.unlink_file(id, 1_700_000_200_000).unwrap();
        drop(repo);
        let repo_root = dir.path().join("repo");

        let message = try_run(
            &repo_root,
            false,
            &format!("/{DELETED_SEGMENT}/{ALL_SEGMENT}"),
            false,
            TimeDisplay::Utc,
        )
        .expect("must succeed");
        let lines: Vec<&str> = message.lines().filter(|l| l.contains("gone [")).collect();
        assert_eq!(lines.len(), 2);
        assert_ne!(lines[0], lines[1]);
        assert!(
            message.contains(BY_TIME_SEGMENT),
            "the [by-time] marker must appear once there is any history: {message}"
        );
    }

    #[test]
    fn listing_all_by_time_shows_chronologically_prefixed_names() {
        let (repo, dir) = setup();
        delete_a_file(&repo, "gone.txt", 946_684_800_000);
        drop(repo);
        let repo_root = dir.path().join("repo");

        let message = try_run(
            &repo_root,
            false,
            &format!("/{DELETED_SEGMENT}/{ALL_SEGMENT}/{BY_TIME_SEGMENT}"),
            false,
            TimeDisplay::Utc,
        )
        .expect("must succeed");
        assert!(message.contains("2000-01-01_00-00-00Z gone.txt"));
    }

    #[test]
    fn a_deleted_directory_addressed_directly_lists_its_own_children_without_a_second_deleted_segment()
     {
        let (repo, dir) = setup();
        let a_id = repo.mkdir(0, "a", 1_700_000_000_000).unwrap();
        let content_id = repo.find_or_create_content(0, &[0xAAu8; 20], &[]).unwrap();
        let file_id = repo
            .settle_file(a_id, "f.txt", 1_700_000_000_000, content_id)
            .unwrap();
        repo.unlink_file(file_id, 1_700_000_100_000).unwrap();
        repo.rmdir(a_id, 1_700_000_200_000).unwrap();
        drop(repo);
        let repo_root = dir.path().join("repo");

        let message = try_run(
            &repo_root,
            false,
            &format!("/{DELETED_SEGMENT}/a"),
            false,
            TimeDisplay::Utc,
        )
        .expect("must succeed - a's own children are reached directly, no second [deleted] needed");
        assert!(message.contains("f.txt"));
    }

    #[test]
    fn a_show_deleted_prefixed_path_resolves_the_same_as_the_bare_one() {
        let (repo, dir) = setup();
        delete_a_file(&repo, "gone.txt", 1_700_000_100_000);
        drop(repo);
        let repo_root = dir.path().join("repo");

        let via_prefix = try_run(
            &repo_root,
            false,
            &format!("/{}/{DELETED_SEGMENT}", deleted::SHOW_DELETED_SEGMENT),
            false,
            TimeDisplay::Utc,
        )
        .expect("must succeed");
        let bare = try_run(
            &repo_root,
            false,
            &format!("/{DELETED_SEGMENT}"),
            false,
            TimeDisplay::Utc,
        )
        .expect("must succeed");
        assert_eq!(via_prefix, bare);
    }
}
