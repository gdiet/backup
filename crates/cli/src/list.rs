//! `dfs list` - REQ-QUERY-001 in requirements/functional/query.md. Lists a directory's live,
//! direct children without mounting.

use std::path::Path;

use crate::time_format::format_time;

fn try_run(repo_path: &Path, default_path_used: bool, target_path: &str) -> Result<String, String> {
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

    let entry = match repo.resolve_path(target_path) {
        Ok(Some(entry)) => entry,
        Ok(None) => return Err(format!("error: no such repository path: {target_path}")),
        Err(err) => return Err(format!("error: {err}")),
    };

    let mut children = match repo.list_children(entry.id) {
        Ok(children) => children,
        Err(db::Error::WrongKind(_)) => {
            return Err(format!("error: {target_path} is not a directory"));
        }
        Err(err) => return Err(format!("error: {err}")),
    };
    if children.is_empty() {
        return Ok(format!("{target_path}: empty"));
    }
    children.sort_by(|(a, _), (b, _)| a.cmp(b));

    let lines: Vec<String> = children
        .into_iter()
        .map(|(name, entry)| {
            let kind = match entry.kind {
                db::EntryKind::Dir => "dir",
                db::EntryKind::File => "file",
            };
            format!(
                "{kind:<4} {:>12} {} {name}",
                entry.size,
                format_time(entry.time_millis)
            )
        })
        .collect();
    Ok(lines.join("\n"))
}

pub fn run(repo_path: &Path, default_path_used: bool, target_path: &str) {
    match try_run(repo_path, default_path_used, target_path) {
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

        let message =
            try_run(&repo_path, true, "/").expect_err("must fail - repo_path holds no repository");
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

        let message = try_run(&repo_root, false, "/").expect("must succeed - root exists");
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

        let message = try_run(&repo_root, false, "/").expect("must succeed");
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

        let message = try_run(&repo_root, false, "/does-not-exist")
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

        let message = try_run(&repo_root, false, "/a.txt")
            .expect_err("must fail - a.txt is a file, not a directory");
        assert!(
            message.contains("not a directory"),
            "expected a not-a-directory message, got: {message}"
        );
    }
}
