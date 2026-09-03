//! `dfs db-backup` - REQ-MAINTENANCE-001 in `requirements/functional/maintenance.md`. Backs up a
//! repository's metadata to a fresh, timestamped, self-contained SQLite file in a target directory,
//! without mounting.

use std::path::Path;

use crate::time_format::format_deletion_suffix;

fn try_run(
    repo_path: &Path,
    default_path_used: bool,
    target_dir: &Path,
    time_millis: i64,
) -> Result<String, String> {
    if !target_dir.is_dir() {
        return Err(format!(
            "error: target {} is not a directory",
            target_dir.display()
        ));
    }

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

    let target_path = target_dir.join(format!(
        "dedupfs-backup-{}.sqlite3",
        format_deletion_suffix(time_millis)
    ));
    repo.backup_metadata(&target_path)
        .map(|()| format!("backed up repository metadata to {}", target_path.display()))
        .map_err(|err| format!("error: {err}"))
}

pub fn run(repo_path: &Path, default_path_used: bool, target_dir: &Path, time_millis: i64) {
    match try_run(repo_path, default_path_used, target_dir, time_millis) {
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

    #[test]
    fn try_run_gives_an_actionable_message_when_the_default_path_holds_no_repository() {
        let repo_path = std::env::temp_dir().join("dfs-db-backup-test-no-default-repository-here");
        let target_dir = tempfile::tempdir().unwrap();

        let message = try_run(&repo_path, true, target_dir.path(), 1_700_000_100_000)
            .expect_err("must fail - repo_path holds no repository");
        assert!(
            message.contains("no repository"),
            "expected the actionable default-path message, got: {message}"
        );
    }

    #[test]
    fn try_run_refuses_a_target_that_is_not_a_directory() {
        let (repo, dir) = setup();
        drop(repo);
        let repo_root = dir.path().join("repo");
        let not_a_dir = dir.path().join("not-a-dir.txt");
        std::fs::write(&not_a_dir, b"hi").unwrap();

        let message = try_run(&repo_root, false, &not_a_dir, 1_700_000_100_000)
            .expect_err("must fail - the target is not a directory");
        assert!(message.contains("not a directory"));
    }

    #[test]
    fn try_run_writes_a_timestamped_self_contained_backup_file() {
        let (repo, dir) = setup();
        repo.mkdir(0, "a", 1_700_000_000_000).unwrap();
        drop(repo);
        let repo_root = dir.path().join("repo");
        let target_dir = tempfile::tempdir().unwrap();

        let message =
            try_run(&repo_root, false, target_dir.path(), 1_700_000_100_000).expect("must succeed");
        assert!(message.contains("backed up repository metadata to"));

        let entries: Vec<_> = std::fs::read_dir(target_dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        assert_eq!(entries.len(), 1, "got: {entries:?}");
        assert!(entries[0].starts_with("dedupfs-backup-"));
        assert!(entries[0].ends_with(".sqlite3"));
    }
}
