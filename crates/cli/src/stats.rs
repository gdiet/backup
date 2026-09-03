//! `dfs stats` - REQ-QUERY-003 in `requirements/functional/query.md`. Repository-wide or
//! path-scoped item counts and size statistics, without mounting.

use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::time_format::format_time;

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is after the Unix epoch")
        .as_millis() as i64
}

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
        Ok(Some(entry)) if entry.kind == db::EntryKind::Dir => entry,
        Ok(Some(_)) => return Err(format!("error: {target_path} is not a directory")),
        Ok(None) => return Err(format!("error: no such repository path: {target_path}")),
        Err(err) => return Err(format!("error: {err}")),
    };

    // REQ-QUERY-003: repository age is repository-wide only - id 0 is the only entry that is
    // "the whole repository", never merely a directory that happens to be empty of ancestors.
    if entry.id == 0 {
        let stats = repo.stats().map_err(|err| format!("error: {err}"))?;
        Ok(format_stats(
            target_path,
            &stats,
            Some(repo.settings().creation_time_millis()),
        ))
    } else {
        let stats = repo
            .stats_for(entry.id)
            .map_err(|err| format!("error: {err}"))?;
        Ok(format_stats(target_path, &stats, None))
    }
}

fn format_stats(target_path: &str, stats: &db::Stats, creation_time_millis: Option<i64>) -> String {
    let mut lines = vec![
        format!(
            "{target_path}: {} dir(s), {} file(s)",
            stats.dirs, stats.files
        ),
        format!("logical size:   {} bytes", stats.logical_size),
        format!("physical size:  {} bytes", stats.physical_size),
        format!(
            "dedup ratio:    {}",
            dedup_ratio_label(stats.logical_size, stats.physical_size)
        ),
    ];
    if let Some(creation_time_millis) = creation_time_millis {
        lines.push(format!(
            "repository age: {} (created {})",
            age_label(creation_time_millis),
            format_time(creation_time_millis)
        ));
    }
    lines.join("\n")
}

/// `logical_size / physical_size`, e.g. `"1.25x"` - `"n/a"` when `physical_size` is `0` (an empty
/// scope), since the ratio is meaningless with nothing actually stored.
fn dedup_ratio_label(logical_size: u64, physical_size: u64) -> String {
    if physical_size == 0 {
        return "n/a".to_string();
    }
    format!("{:.2}x", logical_size as f64 / physical_size as f64)
}

/// How long ago `creation_time_millis` was, as a whole number of days - REQ-STORAGE-008's
/// millisecond precision is not itself meaningful to a human reading a repository's age.
fn age_label(creation_time_millis: i64) -> String {
    let age_days = (now_millis() - creation_time_millis).max(0) / 86_400_000;
    format!("{age_days} day(s)")
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

    fn create_file(repo: &db::Repository, parent: i64, name: &str, length: i64, hash_byte: u8) {
        let (chunk_id, _ranges) = repo
            .reserve_and_insert_chunk(length, &[hash_byte; 20])
            .unwrap();
        let content_id = repo
            .find_or_create_content(length, &[hash_byte.wrapping_add(1); 20], &[chunk_id])
            .unwrap();
        repo.settle_file(parent, name, 1_700_000_000_000, content_id)
            .unwrap();
    }

    #[test]
    fn try_run_gives_an_actionable_message_when_the_default_path_holds_no_repository() {
        let repo_path = std::env::temp_dir().join("dfs-stats-test-no-default-repository-here");

        let message =
            try_run(&repo_path, true, "/").expect_err("must fail - repo_path holds no repository");
        assert!(
            message.contains("no repository"),
            "expected the actionable default-path message, got: {message}"
        );
    }

    #[test]
    fn try_run_reports_repository_wide_stats_including_age() {
        let (repo, dir) = setup();
        create_file(&repo, 0, "a.txt", 10, 0xAA);
        drop(repo);
        let repo_root = dir.path().join("repo");

        let message = try_run(&repo_root, false, "/").expect("must succeed");
        assert!(message.contains("1 file(s)"));
        assert!(message.contains("logical size:   10 bytes"));
        assert!(message.contains("repository age"));
        assert!(
            message.contains("created 2023-11-14T22:13:20Z"),
            "got: {message}"
        );
    }

    #[test]
    fn try_run_reports_path_scoped_stats_without_age() {
        let (repo, dir) = setup();
        let a_id = repo.mkdir(0, "a", 1_700_000_000_000).unwrap();
        create_file(&repo, a_id, "in-a.txt", 10, 0xAA);
        create_file(&repo, 0, "outside.txt", 99, 0xBB);
        drop(repo);
        let repo_root = dir.path().join("repo");

        let message = try_run(&repo_root, false, "/a").expect("must succeed");
        assert!(message.contains("1 file(s)"));
        assert!(message.contains("logical size:   10 bytes"));
        assert!(
            !message.contains("repository age"),
            "path-scoped stats must not report repository age: {message}"
        );
    }

    #[test]
    fn try_run_reports_a_dedup_ratio_reflecting_shared_content() {
        let (repo, dir) = setup();
        let (chunk_id, _ranges) = repo.reserve_and_insert_chunk(10, &[0xAA; 20]).unwrap();
        let content_id = repo
            .find_or_create_content(10, &[0xBB; 20], &[chunk_id])
            .unwrap();
        repo.settle_file(0, "one.txt", 1_700_000_000_000, content_id)
            .unwrap();
        repo.settle_file(0, "two.txt", 1_700_000_000_000, content_id)
            .unwrap();
        drop(repo);
        let repo_root = dir.path().join("repo");

        let message = try_run(&repo_root, false, "/").expect("must succeed");
        assert!(message.contains("logical size:   20 bytes"));
        assert!(message.contains("physical size:  10 bytes"));
        assert!(message.contains("2.00x"), "got: {message}");
    }

    #[test]
    fn try_run_reports_an_empty_repository_with_an_na_dedup_ratio() {
        let (repo, dir) = setup();
        drop(repo);
        let repo_root = dir.path().join("repo");

        let message = try_run(&repo_root, false, "/").expect("must succeed");
        assert!(message.contains("0 dir(s), 0 file(s)"));
        assert!(message.contains("dedup ratio:    n/a"));
    }

    #[test]
    fn try_run_reports_a_missing_path_clearly() {
        let (repo, dir) = setup();
        drop(repo);
        let repo_root = dir.path().join("repo");

        let message = try_run(&repo_root, false, "/does-not-exist")
            .expect_err("must fail - the path does not exist");
        assert!(message.contains("no such repository path"));
    }

    #[test]
    fn try_run_refuses_a_file() {
        let (repo, dir) = setup();
        create_file(&repo, 0, "a.txt", 10, 0xAA);
        drop(repo);
        let repo_root = dir.path().join("repo");

        let message = try_run(&repo_root, false, "/a.txt")
            .expect_err("must fail - a.txt is a file, not a directory");
        assert!(message.contains("not a directory"));
    }
}
