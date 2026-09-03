//! `dfs db-restore` - REQ-MAINTENANCE-002 in `requirements/functional/maintenance.md`. Restores a
//! repository's metadata from a prior REQ-MAINTENANCE-001 backup file, wholesale-replacing the
//! live metadata store, without mounting.

use std::path::Path;

fn try_run(
    repo_path: &Path,
    default_path_used: bool,
    backup_path: &Path,
) -> Result<String, String> {
    if !backup_path.is_file() {
        return Err(format!(
            "error: backup file {} does not exist",
            backup_path.display()
        ));
    }
    if let Err(err) = db::ensure_repository_exists(repo_path) {
        return match err {
            db::Error::NoRepositoryHere(_) if default_path_used => Err(format!(
                "error: no repository found at the default location ({}).\n\
                 Pass a repository path explicitly instead.",
                repo_path.display()
            )),
            err => Err(format!("error: {err}")),
        };
    }

    // Held for the rest of this run (DESIGN-MAINTENANCE-001 in
    // `docs/design/repository-locking.md`) - REQ-MAINTENANCE-004's exclusivity applies to a
    // metadata restore like any other mutating operation (REQ-MAINTENANCE-002's own text).
    let _write_lock = db::acquire_write_lock(repo_path).map_err(|err| format!("error: {err}"))?;

    db::restore_metadata(repo_path, backup_path)
        .map(|()| {
            format!(
                "restored repository metadata from {}",
                backup_path.display()
            )
        })
        .map_err(|err| format!("error: {err}"))
}

pub fn run(repo_path: &Path, default_path_used: bool, backup_path: &Path) {
    match try_run(repo_path, default_path_used, backup_path) {
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
        let repo_path = std::env::temp_dir().join("dfs-db-restore-test-no-default-repository-here");
        let backup_path = std::env::temp_dir().join("dfs-db-restore-test-backup.sqlite3");
        std::fs::write(&backup_path, b"irrelevant - fails before this is opened").unwrap();

        let message = try_run(&repo_path, true, &backup_path)
            .expect_err("must fail - repo_path holds no repository");
        assert!(
            message.contains("no repository"),
            "expected the actionable default-path message, got: {message}"
        );
        let _ = std::fs::remove_file(&backup_path);
    }

    #[test]
    fn try_run_refuses_a_backup_path_that_does_not_exist() {
        let (repo, dir) = setup();
        drop(repo);
        let repo_root = dir.path().join("repo");
        let missing_backup = dir.path().join("does-not-exist.sqlite3");

        let message = try_run(&repo_root, false, &missing_backup)
            .expect_err("must fail - the backup file does not exist");
        assert!(message.contains("does not exist"));
    }

    #[test]
    fn try_run_restores_the_backed_up_metadata_over_the_live_repository() {
        let (repo, dir) = setup();
        repo.mkdir(0, "before-backup", 1_700_000_000_000).unwrap();
        let backup_path = dir.path().join("backup.sqlite3");
        repo.backup_metadata(&backup_path).unwrap();
        repo.mkdir(0, "after-backup", 1_700_000_100_000).unwrap();
        drop(repo);
        let repo_root = dir.path().join("repo");

        let message = try_run(&repo_root, false, &backup_path).expect("restore must succeed");
        assert!(message.contains("restored repository metadata"));

        let repo = db::open_repository(&repo_root).unwrap();
        assert!(repo.resolve_path("/before-backup").unwrap().is_some());
        assert!(repo.resolve_path("/after-backup").unwrap().is_none());
    }

    #[test]
    fn try_run_refuses_a_file_that_is_not_a_dedupfs_backup() {
        let (repo, dir) = setup();
        repo.mkdir(0, "still-here", 1_700_000_000_000).unwrap();
        drop(repo);
        let repo_root = dir.path().join("repo");
        let bogus_backup = dir.path().join("bogus.sqlite3");
        std::fs::write(&bogus_backup, b"not a database").unwrap();

        let message = try_run(&repo_root, false, &bogus_backup)
            .expect_err("must fail - not a genuine backup");
        assert!(message.contains("does not look like"));

        let repo = db::open_repository(&repo_root).unwrap();
        assert!(
            repo.resolve_path("/still-here").unwrap().is_some(),
            "a refused restore must leave the live repository untouched"
        );
    }
}
