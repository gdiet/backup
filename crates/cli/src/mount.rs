//! `dfs mount`'s real implementation - REQ-MOUNT-001. Read-only by default (REQ-MOUNT-002);
//! `--read-write` opts into the directory operations REQ-MOUNT-003 requires (mkdir/rmdir/rename/
//! utimens - see `dedup_fs::DedupFs`) and, before any of that, the whole-session repository write
//! lock (REQ-MAINTENANCE-004, DESIGN-MAINTENANCE-001 in
//! `docs/design/repository-locking.md`) - a read-only mount never acquires it. REQ-CLI-006's
//! default repository path (`crate::repo_path`) applies here too - see `try_run`'s
//! `default_path_used`.

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
    spill_dir: Option<&Path>,
) -> Result<(), String> {
    // A read-only mount uses a genuinely read-only connection (DESIGN-METADATA-003) rather than
    // open_repository's write-mode one - it needs neither WAL/foreign_keys/auto_vacuum setup nor
    // migration, and it keeps working on a filesystem where a write-mode open is unreliable
    // (Error::ConnectionUnreliable's case) even though the mount itself never writes.
    let open = if read_write {
        db::open_repository
    } else {
        db::open_repository_read_only
    };
    let repo = match open(repo_path) {
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

    // Held for the rest of this function, across the blocking `mountfs::mount` call below, for
    // as long as this read-write mount session runs (DESIGN-MOUNT-008) - dropped, releasing the
    // lock, once `mountfs::mount` returns after unmount. A read-only mount never acquires it
    // (REQ-MAINTENANCE-004: read-only operations are unaffected).
    let _write_lock = if read_write {
        Some(db::acquire_write_lock(repo_path).map_err(|err| format!("error: {err}"))?)
    } else {
        None
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

    // Checked eagerly, before the blocking mount call, rather than left to surface lazily the
    // first time the write cache actually needs to spill (DESIGN-MOUNT-018) - the same
    // fail-fast-with-an-actionable-message treatment as the mountpoint check above.
    if let Some(dir) = spill_dir
        && !dir.is_dir()
    {
        return Err(format!(
            "error: --spill-dir {} does not exist or is not a directory. Create it first, or \
             omit --spill-dir to use the OS temp directory instead.",
            dir.display()
        ));
    }

    if let Err(err) = mountfs::preflight() {
        return Err(format!("error: {err}"));
    }
    let store = store::ByteStore::new(db::data_dir(repo_path), !read_write);
    let fs = DedupFs::new(
        repo,
        store,
        read_write,
        repo_path,
        spill_dir.map(Path::to_path_buf),
    )
    .map_err(|err| format!("error: could not open the write-failure log: {err}"))?;
    if let Err(err) = mountfs::mount(fs, mountpoint, !read_write) {
        return Err(format!("mount failed: {err}"));
    }
    Ok(())
}

pub fn run(
    repo_path: &Path,
    mountpoint: &Path,
    read_write: bool,
    default_path_used: bool,
    spill_dir: Option<&Path>,
) {
    if let Err(message) = try_run(
        repo_path,
        mountpoint,
        read_write,
        default_path_used,
        spill_dir,
    ) {
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
        // NoRepositoryHere (both db::open_repository and db::open_repository_read_only share the
        // same ensure_repository_exists guard for this), before try_run ever reaches mountfs.
        let repo_path = std::env::temp_dir().join("dfs-mount-test-no-default-repository-here");
        let mountpoint = std::env::temp_dir().join("dfs-mount-test-unused-mountpoint");

        let message = try_run(&repo_path, &mountpoint, false, true, None)
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

        let message = try_run(&repo_path, &mountpoint, false, false, None)
            .expect_err("must fail - mountpoint does not exist");
        assert!(
            message.contains("does not exist"),
            "expected an actionable does-not-exist message, got: {message}"
        );

        std::fs::remove_dir_all(&repo_path).expect("test cleanup must succeed");
    }

    #[test]
    fn try_run_gives_an_actionable_message_when_spill_dir_does_not_exist() {
        let repo_path = std::env::temp_dir().join("dfs-mount-test-spill-dir-does-not-exist-repo");
        let _ = std::fs::remove_dir_all(&repo_path);
        db::init_repository(
            &repo_path,
            db::RepositorySettings::new(Some(20), 1_700_000_000_000),
        )
        .expect("repository setup for this test must succeed");
        // Created (not just built as a path), so the platform-specific mountpoint check above
        // this one in try_run - which on Linux requires the mountpoint to already exist - never
        // gets in the way of reaching the spill_dir check this test actually targets.
        let mountpoint = std::env::temp_dir().join("dfs-mount-test-spill-dir-mountpoint");
        std::fs::create_dir_all(&mountpoint).expect("test mountpoint setup must succeed");
        let spill_dir = std::env::temp_dir().join("dfs-mount-test-spill-dir-that-does-not-exist");
        let _ = std::fs::remove_dir_all(&spill_dir);

        let message = try_run(&repo_path, &mountpoint, false, false, Some(&spill_dir))
            .expect_err("must fail - spill_dir does not exist");
        assert!(
            message.contains("--spill-dir"),
            "expected the actionable spill-dir message, got: {message}"
        );
        assert!(
            message.contains("does not exist"),
            "expected an actionable does-not-exist message, got: {message}"
        );

        std::fs::remove_dir_all(&repo_path).expect("test cleanup must succeed");
        std::fs::remove_dir_all(&mountpoint).expect("test cleanup must succeed");
    }

    #[test]
    fn try_run_surfaces_the_raw_error_when_an_explicit_path_holds_no_repository() {
        let repo_path = std::env::temp_dir().join("dfs-mount-test-no-explicit-repository-here");
        let mountpoint = std::env::temp_dir().join("dfs-mount-test-unused-mountpoint");

        let message = try_run(&repo_path, &mountpoint, false, false, None)
            .expect_err("must fail - repo_path holds no repository");
        assert!(
            !message.contains("Pass a repository path"),
            "did not expect the default-path hint for an explicit path, got: {message}"
        );
    }

    #[test]
    fn try_run_read_write_refuses_a_repository_another_process_already_locked_for_writing() {
        let repo_path =
            std::env::temp_dir().join("dfs-mount-test-read-write-refused-while-locked-repo");
        let _ = std::fs::remove_dir_all(&repo_path);
        db::init_repository(
            &repo_path,
            db::RepositorySettings::new(Some(20), 1_700_000_000_000),
        )
        .expect("repository setup for this test must succeed");
        let mountpoint = std::env::temp_dir().join("dfs-mount-test-unused-mountpoint-locked");

        // Simulates a second process already holding the write lock - try_run must be refused
        // before ever reaching the (blocking) mountfs::mount call below.
        let _held_elsewhere = db::acquire_write_lock(&repo_path)
            .expect("acquiring the write lock for the first time must succeed");

        let message = try_run(&repo_path, &mountpoint, true, false, None)
            .expect_err("must fail - the write lock is already held");
        assert!(
            message.contains("already locked"),
            "expected an actionable already-locked message, got: {message}"
        );

        std::fs::remove_dir_all(&repo_path).expect("test cleanup must succeed");
    }
}
