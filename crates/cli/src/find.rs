//! `dfs find` - REQ-QUERY-002 in `requirements/functional/query.md`. Searches live entries anywhere
//! in the repository by a case-insensitive name pattern (`*`/`?` wildcards), without mounting.

use std::path::Path;

use crate::entry_format::{format_line, kind_label};

fn try_run(repo_path: &Path, default_path_used: bool, pattern: &str) -> Result<String, String> {
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

    let mut matches = repo.find(pattern).map_err(|err| format!("error: {err}"))?;
    if matches.is_empty() {
        return Ok(format!("no matches for {pattern}"));
    }
    matches.sort_by(|(a, _), (b, _)| a.cmp(b));
    Ok(matches
        .into_iter()
        .map(|(path, entry)| {
            format_line(kind_label(entry.kind), entry.size, entry.time_millis, &path)
        })
        .collect::<Vec<_>>()
        .join("\n"))
}

pub fn run(repo_path: &Path, default_path_used: bool, pattern: &str) {
    match try_run(repo_path, default_path_used, pattern) {
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

    fn create_file(repo: &db::Repository, parent: i64, name: &str, hash_byte: u8) {
        let content_id = repo
            .find_or_create_content(0, &[hash_byte; 20], &[])
            .unwrap();
        repo.settle_file(parent, name, 1_700_000_000_000, content_id)
            .unwrap();
    }

    #[test]
    fn try_run_gives_an_actionable_message_when_the_default_path_holds_no_repository() {
        let repo_path = std::env::temp_dir().join("dfs-find-test-no-default-repository-here");

        let message =
            try_run(&repo_path, true, "*").expect_err("must fail - repo_path holds no repository");
        assert!(
            message.contains("no repository"),
            "expected the actionable default-path message, got: {message}"
        );
    }

    #[test]
    fn try_run_finds_a_match_anywhere_in_the_tree_with_its_full_path() {
        let (repo, dir) = setup();
        let dir_id = repo.mkdir(0, "photos", 1_700_000_000_000).unwrap();
        create_file(&repo, dir_id, "one.jpg", 0xAA);
        drop(repo);
        let repo_root = dir.path().join("repo");

        let message = try_run(&repo_root, false, "one.jpg").expect("must succeed");
        assert!(message.contains("/photos/one.jpg"));
        assert!(message.starts_with("file"));
    }

    #[test]
    fn try_run_reports_no_matches_clearly_rather_than_an_empty_string() {
        let (repo, dir) = setup();
        drop(repo);
        let repo_root = dir.path().join("repo");

        let message =
            try_run(&repo_root, false, "nope-*").expect("an empty result is not itself an error");
        assert!(message.contains("no matches"));
    }

    #[test]
    fn try_run_supports_wildcards_and_sorts_results_by_path() {
        let (repo, dir) = setup();
        create_file(&repo, 0, "b.txt", 0xAA);
        create_file(&repo, 0, "a.txt", 0xBB);
        drop(repo);
        let repo_root = dir.path().join("repo");

        let message = try_run(&repo_root, false, "*.txt").expect("must succeed");
        let lines: Vec<&str> = message.lines().collect();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].contains("/a.txt"));
        assert!(lines[1].contains("/b.txt"));
    }
}
