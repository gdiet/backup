//! `dfs unlock`'s real implementation - DESIGN-MAINTENANCE-003 in
//! `docs/design/repository-locking.md`, REQ-MAINTENANCE-008's explicit, human-invoked counterpart
//! to `mount --read-write`'s unconditional refusal when the repository's write lock file is merely
//! present (`db::acquire_write_lock`, DESIGN-MAINTENANCE-002). Checks whether the lock is genuinely
//! stale via an OS-level `flock` test - the same test `acquire_write_lock` itself no longer
//! performs on an existing file - and clears it only when it actually is; an actively held lock is
//! left untouched. REQ-CLI-006's default repository path applies here too, same as `mount`.

use std::path::Path;

fn try_run(repo_path: &Path, default_path_used: bool) -> Result<String, String> {
    match db::open_repository(repo_path) {
        Ok(_repo) => {}
        Err(db::Error::NoRepositoryHere(_)) if default_path_used => {
            return Err(format!(
                "error: no repository found at the default location ({}).\n\
                 Pass a repository path explicitly instead.",
                repo_path.display()
            ));
        }
        Err(err) => return Err(format!("error: {err}")),
    }

    match db::unlock_stale_write_lock(repo_path) {
        Ok(db::UnlockOutcome::NotLocked) => Ok(format!(
            "{} is not locked - nothing to do.",
            repo_path.display()
        )),
        Ok(db::UnlockOutcome::RemovedStaleLock { previous_marker }) => Ok(match previous_marker {
            Some(marker) => format!(
                "removed a stale write lock on {} (previously: {marker})",
                repo_path.display()
            ),
            None => format!("removed a stale write lock on {}", repo_path.display()),
        }),
        Ok(db::UnlockOutcome::StillLocked { marker }) => Err(match marker {
            Some(marker) => format!(
                "error: {} is still locked ({marker}) - refusing to remove an active lock",
                repo_path.display()
            ),
            None => format!(
                "error: {} is still locked by another process - refusing to remove an active lock",
                repo_path.display()
            ),
        }),
        Err(err) => Err(format!("error: {err}")),
    }
}

pub fn run(repo_path: &Path, default_path_used: bool) {
    match try_run(repo_path, default_path_used) {
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
        let repo_path = std::env::temp_dir().join("dfs-unlock-test-no-default-repository-here");

        let message =
            try_run(&repo_path, true).expect_err("must fail - repo_path holds no repository");
        assert!(
            message.contains("no repository"),
            "expected the actionable default-path message, got: {message}"
        );
        assert!(
            message.contains("explicitly"),
            "expected a hint to pass the path explicitly, got: {message}"
        );
    }

    #[test]
    fn try_run_reports_nothing_to_do_when_the_repository_is_not_locked() {
        let repo_path = std::env::temp_dir().join("dfs-unlock-test-not-locked-repo");
        let _ = std::fs::remove_dir_all(&repo_path);
        db::init_repository(
            &repo_path,
            db::RepositorySettings::new(Some(20), 1_700_000_000_000),
        )
        .expect("repository setup for this test must succeed");

        let message = try_run(&repo_path, false).expect("must succeed - nothing to unlock");
        assert!(
            message.contains("not locked"),
            "expected a not-locked message, got: {message}"
        );

        std::fs::remove_dir_all(&repo_path).expect("test cleanup must succeed");
    }

    #[test]
    fn try_run_removes_a_stale_lock_left_behind_by_an_unclean_exit() {
        let repo_path = std::env::temp_dir().join("dfs-unlock-test-stale-lock-repo");
        let _ = std::fs::remove_dir_all(&repo_path);
        db::init_repository(
            &repo_path,
            db::RepositorySettings::new(Some(20), 1_700_000_000_000),
        )
        .expect("repository setup for this test must succeed");
        std::fs::write(
            db::meta_dir(&repo_path).join("lock"),
            b"locked by some-other-host, process 999999, time 1",
        )
        .expect("writing a leftover lock file for this test must succeed");

        let message = try_run(&repo_path, false).expect("must succeed - the lock is stale");
        assert!(
            message.contains("removed"),
            "expected a removed-lock message, got: {message}"
        );

        db::acquire_write_lock(&repo_path)
            .expect("acquiring the write lock must succeed once the stale lock is cleared");

        std::fs::remove_dir_all(&repo_path).expect("test cleanup must succeed");
    }

    #[test]
    fn try_run_refuses_to_remove_an_actively_held_lock() {
        let repo_path = std::env::temp_dir().join("dfs-unlock-test-active-lock-repo");
        let _ = std::fs::remove_dir_all(&repo_path);
        db::init_repository(
            &repo_path,
            db::RepositorySettings::new(Some(20), 1_700_000_000_000),
        )
        .expect("repository setup for this test must succeed");
        let held = db::acquire_write_lock(&repo_path)
            .expect("acquiring the write lock for the first time must succeed");

        let message = try_run(&repo_path, false).expect_err("must refuse - the lock is active");
        assert!(
            message.contains("still locked"),
            "expected a still-locked message, got: {message}"
        );

        drop(held);
        std::fs::remove_dir_all(&repo_path).expect("test cleanup must succeed");
    }
}
