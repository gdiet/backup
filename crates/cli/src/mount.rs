//! `dfs mount`'s real implementation - REQ-MOUNT-001. Read-only by default (REQ-MOUNT-002);
//! `--read-write` opts into the directory operations REQ-MOUNT-003 requires (mkdir/rmdir/rename/
//! utimens - see `dedup_fs::DedupFs`). No cross-process repository locking yet
//! (REQ-MAINTENANCE-004) - out of scope for this first mount milestone. REQ-CLI-006's default
//! repository path (`crate::repo_path`) applies here too - see `try_run`'s `default_path_used`.

use std::path::Path;

use crate::dedup_fs::DedupFs;

/// `mount`'s core logic, separated from `main`'s process-exit/eprintln side effects so the error
/// path stays testable without touching a real mount. `default_path_used` distinguishes a
/// `repo_path` the operator gave explicitly from one resolved via
/// [`crate::repo_path::default_repo_path`], so a failure can point at passing the path explicitly
/// only when there was no explicit path already (REQ-CLI-006, REQ-OPERABILITY-004).
fn try_run(
    repo_path: &Path,
    mountpoint: &Path,
    read_write: bool,
    default_path_used: bool,
) -> Result<(), String> {
    let repo = match db::open_repository(repo_path) {
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

    // Linux's FUSE backend refuses to mount onto a path that does not already exist, and only
    // reports that via a raw line on stderr, not through this crate's `Err`. Catching it here
    // first gives an actionable message instead (REQ-OPERABILITY-004) - Windows needs no such
    // check, since WinFSP creates the mountpoint itself.
    #[cfg(target_os = "linux")]
    if !mountpoint.is_dir() {
        return Err(format!(
            "error: mountpoint {} does not exist. Create it first (e.g. `mkdir -p`) - unlike on \
             Windows, this platform's FUSE backend does not create the mountpoint directory \
             itself.",
            mountpoint.display()
        ));
    }

    if let Err(err) = mountfs::preflight() {
        return Err(format!("error: {err}"));
    }
    if let Err(err) = mountfs::mount(DedupFs::new(repo, read_write), mountpoint, !read_write) {
        return Err(format!("mount failed: {err}"));
    }
    Ok(())
}

pub fn run(repo_path: &Path, mountpoint: &Path, read_write: bool, default_path_used: bool) {
    if let Err(message) = try_run(repo_path, mountpoint, read_write, default_path_used) {
        eprintln!("{message}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn try_run_gives_an_actionable_message_when_the_default_path_holds_no_repository() {
        // No filesystem setup needed: a path that simply does not exist already answers
        // db::open_repository with NoRepositoryHere, before try_run ever reaches mountfs.
        let repo_path = std::env::temp_dir().join("dfs-mount-test-no-default-repository-here");
        let mountpoint = std::env::temp_dir().join("dfs-mount-test-unused-mountpoint");

        let message = try_run(&repo_path, &mountpoint, false, true)
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

    #[cfg(target_os = "linux")]
    #[test]
    fn try_run_gives_an_actionable_message_when_the_mountpoint_does_not_exist() {
        let repo_path = std::env::temp_dir().join("dfs-mount-test-mountpoint-does-not-exist-repo");
        let _ = std::fs::remove_dir_all(&repo_path);
        db::init_repository(
            &repo_path,
            db::RepositorySettings::new(Some(20), 1_700_000_000_000),
        )
        .expect("repository setup for this test must succeed");
        let mountpoint = std::env::temp_dir().join("dfs-mount-test-mountpoint-does-not-exist-mnt");
        let _ = std::fs::remove_dir_all(&mountpoint);

        let message = try_run(&repo_path, &mountpoint, false, false)
            .expect_err("must fail - mountpoint does not exist");
        assert!(
            message.contains("does not exist"),
            "expected an actionable does-not-exist message, got: {message}"
        );

        std::fs::remove_dir_all(&repo_path).expect("test cleanup must succeed");
    }

    #[test]
    fn try_run_surfaces_the_raw_error_when_an_explicit_path_holds_no_repository() {
        let repo_path = std::env::temp_dir().join("dfs-mount-test-no-explicit-repository-here");
        let mountpoint = std::env::temp_dir().join("dfs-mount-test-unused-mountpoint");

        let message = try_run(&repo_path, &mountpoint, false, false)
            .expect_err("must fail - repo_path holds no repository");
        assert!(
            !message.contains("Pass a repository path"),
            "did not expect the default-path hint for an explicit path, got: {message}"
        );
    }
}
