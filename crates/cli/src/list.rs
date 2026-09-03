//! `dfs list` - REQ-QUERY-001 in requirements/functional/query.md, REQ-CLI-007 in
//! requirements/functional/cli-commands.md. Lists a directory's live, direct children without
//! mounting; `--show-deleted` additionally reveals REQ-TREE-009's `[deleted]` addressing segment
//! where a directory has soft-deleted children, and a path naming `[deleted]` explicitly lists
//! them directly - see `crate::deleted`.

use std::path::Path;

use crate::deleted::{self, DELETED_SEGMENT, Resolved};
use crate::time_format::format_time;

/// The listing "kind" column value for REQ-TREE-009's `[deleted]` marker row - distinct from
/// `dir`/`file` so it is never confused with a real, identically-named live directory (which
/// `[deleted]` itself already shows as an ordinary `dir` entry, per REQ-TREE-009's real-wins
/// rule, without needing this marker at all).
const VIRTUAL_KIND: &str = "virt";

fn try_run(
    repo_path: &Path,
    default_path_used: bool,
    target_path: &str,
    show_deleted: bool,
) -> Result<String, String> {
    let repo = match db::open_repository_read_only(repo_path) {
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

    let resolved = match deleted::resolve(&repo, target_path) {
        Ok(Some(resolved)) => resolved,
        Ok(None) => return Err(format!("error: no such repository path: {target_path}")),
        Err(err) => return Err(format!("error: {err}")),
    };

    match resolved {
        Resolved::Live(entry) => list_live(&repo, target_path, entry.id, show_deleted),
        Resolved::DeletedChildren { parent_id } => list_deleted(&repo, target_path, parent_id),
        Resolved::Deleted(entry) if entry.entry.kind == db::EntryKind::Dir => Err(format!(
            "error: {target_path} is a soft-deleted directory - list its own deleted children \
             via {target_path}/{DELETED_SEGMENT}"
        )),
        Resolved::Deleted(_) => Err(format!("error: {target_path} is not a directory")),
    }
}

fn list_live(
    repo: &db::Repository,
    target_path: &str,
    dir_id: i64,
    show_deleted: bool,
) -> Result<String, String> {
    let children = match repo.list_children(dir_id) {
        Ok(children) => children,
        Err(db::Error::WrongKind(_)) => {
            return Err(format!("error: {target_path} is not a directory"));
        }
        Err(err) => return Err(format!("error: {err}")),
    };
    // REQ-TREE-009: a real live entry already named `[deleted]` wins outright - it is already in
    // `children` above, listed like any other live entry, so the marker below is only added when
    // nothing real occupies that name yet there is deletion history to reveal.
    let already_real = children.iter().any(|(name, _)| name == DELETED_SEGMENT);

    let mut rows: Vec<(String, String)> = children
        .iter()
        .map(|(name, entry)| {
            (
                name.clone(),
                format_line(kind_label(entry.kind), entry.size, entry.time_millis, name),
            )
        })
        .collect();

    if show_deleted && !already_real {
        let deleted_children = repo
            .list_deleted_children(dir_id)
            .map_err(|err| format!("error: {err}"))?;
        if let Some(most_recent) = deleted_children.iter().map(|(_, e)| e.deleted_at).max() {
            rows.push((
                DELETED_SEGMENT.to_string(),
                format_line(VIRTUAL_KIND, 0, most_recent, DELETED_SEGMENT),
            ));
        }
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

fn list_deleted(
    repo: &db::Repository,
    target_path: &str,
    parent_id: i64,
) -> Result<String, String> {
    let children = repo
        .list_deleted_children(parent_id)
        .map_err(|err| format!("error: {err}"))?;
    if children.is_empty() {
        return Ok(format!("{target_path}: empty"));
    }

    let mut lines: Vec<(String, String)> = deleted::display_names(&children)
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
                ),
            )
        })
        .collect();
    lines.sort_by(|(a, _), (b, _)| a.cmp(b));
    Ok(lines
        .into_iter()
        .map(|(_, line)| line)
        .collect::<Vec<_>>()
        .join("\n"))
}

fn kind_label(kind: db::EntryKind) -> &'static str {
    match kind {
        db::EntryKind::Dir => "dir",
        db::EntryKind::File => "file",
    }
}

fn format_line(kind: &str, size: u64, time_millis: i64, name: &str) -> String {
    format!("{kind:<4} {size:>12} {} {name}", format_time(time_millis))
}

pub fn run(repo_path: &Path, default_path_used: bool, target_path: &str, show_deleted: bool) {
    match try_run(repo_path, default_path_used, target_path, show_deleted) {
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

        let message = try_run(&repo_path, true, "/", false)
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

    fn setup() -> (db::Repository, tempfile::TempDir) {
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
    fn try_run_reports_an_empty_root() {
        let (repo, dir) = setup();
        drop(repo);
        let repo_root = dir.path().join("repo");

        let message = try_run(&repo_root, false, "/", false).expect("must succeed - root exists");
        assert_eq!(message, "/: empty");
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

        let message = try_run(&repo_root, false, "/", false).expect("must succeed");
        let lines: Vec<&str> = message.lines().collect();
        assert_eq!(lines.len(), 2);
        assert!(
            lines[0].contains("a-file.txt") && lines[0].starts_with("file"),
            "expected the file entry first (alphabetical), got: {}",
            lines[0]
        );
        assert!(
            lines[0].contains(" 3 "),
            "expected the file's logical size (3 bytes), got: {}",
            lines[0]
        );
        assert!(
            lines[1].contains("b-dir") && lines[1].starts_with("dir"),
            "expected the directory entry second (alphabetical), got: {}",
            lines[1]
        );
    }

    #[test]
    fn try_run_reports_a_missing_path_clearly() {
        let (repo, dir) = setup();
        drop(repo);
        let repo_root = dir.path().join("repo");

        let message = try_run(&repo_root, false, "/does-not-exist", false)
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

        let message = try_run(&repo_root, false, "/a.txt", false)
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
    fn show_deleted_is_off_by_default() {
        let (repo, dir) = setup();
        delete_a_file(&repo, "gone.txt", 1_700_000_100_000);
        drop(repo);
        let repo_root = dir.path().join("repo");

        let message = try_run(&repo_root, false, "/", false).expect("must succeed");
        assert_eq!(
            message, "/: empty",
            "no [deleted] marker without --show-deleted"
        );
    }

    #[test]
    fn show_deleted_reveals_the_deleted_marker_when_history_exists() {
        let (repo, dir) = setup();
        delete_a_file(&repo, "gone.txt", 1_700_000_100_000);
        drop(repo);
        let repo_root = dir.path().join("repo");

        let message = try_run(&repo_root, false, "/", true).expect("must succeed");
        assert!(message.contains(VIRTUAL_KIND));
        assert!(message.contains(DELETED_SEGMENT));
    }

    #[test]
    fn show_deleted_adds_no_marker_when_nothing_was_ever_deleted() {
        let (repo, dir) = setup();
        repo.mkdir(0, "a", 1_700_000_000_000).unwrap();
        drop(repo);
        let repo_root = dir.path().join("repo");

        let message = try_run(&repo_root, false, "/", true).expect("must succeed");
        assert!(!message.contains(DELETED_SEGMENT));
    }

    #[test]
    fn a_real_live_directory_named_deleted_wins_over_the_marker() {
        let (repo, dir) = setup();
        delete_a_file(&repo, "gone.txt", 1_700_000_100_000);
        repo.mkdir(0, DELETED_SEGMENT, 1_700_000_000_000).unwrap();
        drop(repo);
        let repo_root = dir.path().join("repo");

        let message = try_run(&repo_root, false, "/", true).expect("must succeed");
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
    fn listing_the_deleted_segment_directly_shows_its_children() {
        let (repo, dir) = setup();
        delete_a_file(&repo, "gone.txt", 1_700_000_100_000);
        drop(repo);
        let repo_root = dir.path().join("repo");

        let message = try_run(&repo_root, false, &format!("/{DELETED_SEGMENT}"), false)
            .expect("must succeed");
        assert!(message.contains("gone.txt"));
        assert!(message.starts_with("file"));
    }

    #[test]
    fn listing_the_deleted_segment_disambiguates_same_named_entries() {
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

        let message = try_run(&repo_root, false, &format!("/{DELETED_SEGMENT}"), false)
            .expect("must succeed");
        let lines: Vec<&str> = message.lines().collect();
        assert_eq!(lines.len(), 2);
        assert_ne!(lines[0], lines[1]);
        assert!(lines[0].contains("gone ["));
        assert!(lines[1].contains("gone ["));
    }

    #[test]
    fn a_deleted_directory_addressed_directly_points_at_its_own_deleted_segment() {
        let (repo, dir) = setup();
        let a_id = repo.mkdir(0, "a", 1_700_000_000_000).unwrap();
        repo.rmdir(a_id, 1_700_000_100_000).unwrap();
        drop(repo);
        let repo_root = dir.path().join("repo");

        let message = try_run(&repo_root, false, &format!("/{DELETED_SEGMENT}/a"), false)
            .expect_err("must fail - a deleted directory needs its own [deleted] step");
        assert!(message.contains(DELETED_SEGMENT));
    }
}
