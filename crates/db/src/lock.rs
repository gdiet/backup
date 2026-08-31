//! Cross-process, whole-session mutating-operation exclusivity - DESIGN-MAINTENANCE-001/002/003 in
//! `docs/design/repository-locking.md`. An OS advisory lock (`flock`/`LockFileEx`, via the
//! `fd-lock` crate) on a dedicated file inside `meta/`, held for as long as the returned
//! [`WriteLock`] stays alive - acquired only through an exclusive-create attempt that is trusted
//! outright and never overridden by a `flock` fallback, and recording a diagnostic marker (who
//! holds it) once acquired. A lock file left behind by a process that exited without releasing (a
//! crash, a hard kill) is not cleared automatically by acquisition - only by an explicit human
//! decision, [`try_unlock_stale_write_lock`] below (`dfs unlock`, DESIGN-MAINTENANCE-003), which
//! is where `flock` is actually used to tell an active holder apart from a stale leftover.

use std::fs::{File, OpenOptions};
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::Error;

const LOCK_FILE: &str = "lock";

/// Held for as long as the caller wants exclusive, repository-mutating access to a repository -
/// drop to release it.
#[derive(Debug)]
pub struct WriteLock {
    // Never read directly: this field's only job is to exist for as long as `WriteLock` does, so
    // its own `Drop` releases the underlying OS lock - not a genuine dead-code finding.
    #[allow(dead_code)]
    guard: fd_lock::RwLockWriteGuard<'static, File>,
    path: PathBuf,
}

impl Drop for WriteLock {
    fn drop(&mut self) {
        // DESIGN-MAINTENANCE-002: delete while still holding the lock, immediately before `guard`
        // itself drops right after this method returns (a struct's own fields drop, in
        // declaration order, only once its `Drop::drop` body finishes) - never the other way
        // around, which would let a competing acquirer's `create_new` attempt already succeed and
        // then have this delete remove its lock file out from under it. Best-effort: nothing
        // useful to do if this fails, the process is exiting either way.
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Attempts to acquire `meta_dir`'s write lock, failing immediately (never blocking) if another
/// process already holds it, or merely might - REQ-MAINTENANCE-004's "refused rather than allowed
/// to proceed". `meta_dir` is a repository's `meta/` directory, already known to exist by the time
/// this is called (e.g. because [`crate::open_repository`] against the same `repo_root` already
/// succeeded).
pub(crate) fn try_acquire_write_lock(meta_dir: &Path) -> Result<WriteLock, Error> {
    let path = meta_dir.join(LOCK_FILE);

    // DESIGN-MAINTENANCE-002: exclusive file creation (`O_CREAT|O_EXCL`) is a more broadly correct
    // atomicity primitive than `flock` alone on some network filesystems - its "already exists"
    // signal is trusted outright and refused immediately, never overridden by falling back to a
    // `flock` check on the existing file. Doing so would defeat the reason this gate exists in the
    // first place: on exactly the filesystems where `flock` may not propagate correctly across
    // machines, a `flock` fallback could silently grant a second, actively-writing session the
    // lock right after `create_new` correctly reported a conflict.
    let file = match OpenOptions::new().write(true).create_new(true).open(&path) {
        Ok(file) => file,
        Err(err) if err.kind() == ErrorKind::AlreadyExists => {
            return Err(Error::AlreadyLocked(meta_dir.to_path_buf()));
        }
        Err(err) => return Err(err.into()),
    };

    // Leaked deliberately: this lock is meant to be held for as long as the process that
    // acquired it keeps running (one repository-mutating session), so there is no meaningful
    // later point to reclaim the allocation at - the OS reclaims the file descriptor/handle
    // itself (and with it, the advisory lock) unconditionally on process exit, however it exits.
    let lock: &'static mut fd_lock::RwLock<File> = Box::leak(Box::new(fd_lock::RwLock::new(file)));

    let mut guard = match lock.try_write() {
        Ok(guard) => guard,
        // Only reachable through an extremely narrow race with a concurrent `dfs unlock`
        // invocation also attempting `flock` on the very file this call just exclusively created
        // a moment ago - treated the same as any other lock contention.
        Err(err) if err.kind() == ErrorKind::WouldBlock => {
            return Err(Error::AlreadyLocked(meta_dir.to_path_buf()));
        }
        // Anything else (REQ-OPERABILITY-004: a foreseeable failure, not a raw OS error left to
        // stand on its own) - most plausibly the underlying storage not actually enforcing
        // locking at all, the "Known limitation" DESIGN-MAINTENANCE-001 documents for a
        // network-mounted repository.
        Err(source) => {
            return Err(Error::LockUnavailable {
                path: meta_dir.to_path_buf(),
                source,
            });
        }
    };

    // Best-effort diagnostic marker (DESIGN-MAINTENANCE-002) - never consulted by
    // `try_acquire_write_lock` itself, only read back by [`try_unlock_stale_write_lock`] and by a
    // human reading the file directly, to see who holds (or held) it. A failure to write it is
    // not itself a locking failure.
    let _ = write_diagnostic_marker(&mut guard);

    Ok(WriteLock { guard, path })
}

/// Writes a line identifying this process - hostname, process id, and acquisition time - into
/// `file`, which `try_acquire_write_lock` guarantees is freshly created and therefore empty.
fn write_diagnostic_marker(file: &mut File) -> std::io::Result<()> {
    let hostname = gethostname::gethostname();
    let pid = std::process::id();
    let time_millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    writeln!(
        file,
        "locked by {}, process {pid}, time {time_millis}",
        hostname.to_string_lossy()
    )
}

/// The result of [`crate::unlock_stale_write_lock`] - DESIGN-MAINTENANCE-003's manual counterpart
/// to [`crate::acquire_write_lock`]'s unconditional refusal (DESIGN-MAINTENANCE-002) on a
/// merely-present lock file.
#[derive(Debug)]
pub enum UnlockOutcome {
    /// No lock file was present - nothing to do.
    NotLocked,
    /// The lock file existed, but nothing currently held its `flock` - a leftover from a process
    /// that exited without releasing (a crash, a hard kill). Removed. Carries the previous
    /// holder's diagnostic marker, if it could be read.
    RemovedStaleLock { previous_marker: Option<String> },
    /// The lock file existed and its `flock` is still actively held - left untouched. Carries the
    /// current holder's diagnostic marker, if it could be read, so the caller can tell an operator
    /// who holds it.
    StillLocked { marker: Option<String> },
}

/// Checks whether `meta_dir`'s write lock is genuinely stale - an OS-level `flock` test, not a
/// heuristic over the diagnostic marker's content - and clears it if so. Never removes an actively
/// held lock. This is the only place a leftover lock file from an unclean exit is ever cleared;
/// [`try_acquire_write_lock`] refuses outright instead of attempting this itself, since doing so
/// automatically on every acquisition would be exactly the `flock`-fallback this design rejects.
pub(crate) fn try_unlock_stale_write_lock(meta_dir: &Path) -> Result<UnlockOutcome, Error> {
    let path = meta_dir.join(LOCK_FILE);

    let file = match OpenOptions::new().write(true).open(&path) {
        Ok(file) => file,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(UnlockOutcome::NotLocked),
        Err(err) => return Err(err.into()),
    };
    let marker = read_marker(&path);

    let mut lock = fd_lock::RwLock::new(file);
    match lock.try_write() {
        Ok(guard) => {
            // Nobody holds the flock - genuinely stale. Delete while still holding it, before
            // `guard` drops, same ordering rationale as `WriteLock::drop`: this way, a session
            // that legitimately still held the write lock at the moment this ran could never have
            // had its own lock file deleted out from under it (its `flock` would have made the
            // `try_write` above fail instead).
            std::fs::remove_file(&path)?;
            drop(guard);
            Ok(UnlockOutcome::RemovedStaleLock {
                previous_marker: marker,
            })
        }
        Err(err) if err.kind() == ErrorKind::WouldBlock => {
            Ok(UnlockOutcome::StillLocked { marker })
        }
        Err(source) => Err(Error::LockUnavailable {
            path: meta_dir.to_path_buf(),
            source,
        }),
    }
}

/// Best-effort read of the diagnostic marker at `path` - `None` on any I/O error (the file
/// disappearing under us, a permissions issue) or if it is empty, since either case leaves nothing
/// meaningful to report back to a caller.
fn read_marker(path: &Path) -> Option<String> {
    std::fs::read_to_string(path)
        .ok()
        .map(|content| content.trim().to_string())
        .filter(|content| !content.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_second_write_lock_attempt_is_refused_while_the_first_is_held() {
        let dir = tempfile::tempdir().unwrap();
        let first = try_acquire_write_lock(dir.path()).expect("first attempt must succeed");

        let err = try_acquire_write_lock(dir.path()).unwrap_err();
        assert!(matches!(err, Error::AlreadyLocked(_)));

        drop(first);
    }

    #[test]
    fn the_write_lock_is_available_again_once_released() {
        let dir = tempfile::tempdir().unwrap();
        let first = try_acquire_write_lock(dir.path()).expect("first attempt must succeed");
        drop(first);

        try_acquire_write_lock(dir.path()).expect("must succeed once the first lock is released");
    }

    #[test]
    fn acquiring_the_lock_creates_the_lock_file_if_missing() {
        let dir = tempfile::tempdir().unwrap();
        let _lock = try_acquire_write_lock(dir.path()).unwrap();
        assert!(dir.path().join(LOCK_FILE).is_file());
    }

    #[test]
    fn releasing_the_lock_deletes_the_file() {
        let dir = tempfile::tempdir().unwrap();
        let lock = try_acquire_write_lock(dir.path()).unwrap();
        drop(lock);
        assert!(
            !dir.path().join(LOCK_FILE).exists(),
            "DESIGN-MAINTENANCE-002: a clean release must delete the lock file, not just unlock it"
        );
    }

    #[test]
    fn a_leftover_lock_file_from_an_unclean_exit_refuses_acquisition_until_explicitly_unlocked() {
        let dir = tempfile::tempdir().unwrap();
        // Simulates what an unclean exit (a crash, a hard kill) leaves behind: the file itself,
        // holding a previous session's marker, but nothing currently holding its `flock`.
        std::fs::write(
            dir.path().join(LOCK_FILE),
            b"locked by some-other-host, process 999999, time 1",
        )
        .unwrap();

        let err = try_acquire_write_lock(dir.path()).unwrap_err();
        assert!(
            matches!(err, Error::AlreadyLocked(_)),
            "DESIGN-MAINTENANCE-002: a merely-present lock file must refuse acquisition outright, \
             never silently fall back to flock to decide whether it is stale - got: {err:?}"
        );
    }

    #[test]
    fn the_diagnostic_marker_records_the_process_id() {
        let dir = tempfile::tempdir().unwrap();
        let _lock = try_acquire_write_lock(dir.path()).unwrap();
        let content = std::fs::read_to_string(dir.path().join(LOCK_FILE)).unwrap();
        assert!(
            content.contains(&std::process::id().to_string()),
            "expected the current process id in the marker, got: {content:?}"
        );
    }

    #[test]
    fn unlock_reports_not_locked_when_no_lock_file_exists() {
        let dir = tempfile::tempdir().unwrap();
        let outcome = try_unlock_stale_write_lock(dir.path()).unwrap();
        assert!(matches!(outcome, UnlockOutcome::NotLocked));
    }

    #[test]
    fn unlock_removes_a_stale_lock_file_and_reports_its_previous_marker() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join(LOCK_FILE),
            b"locked by some-other-host, process 999999, time 1",
        )
        .unwrap();

        let outcome = try_unlock_stale_write_lock(dir.path()).unwrap();
        match outcome {
            UnlockOutcome::RemovedStaleLock { previous_marker } => {
                assert_eq!(
                    previous_marker.as_deref(),
                    Some("locked by some-other-host, process 999999, time 1")
                );
            }
            other => panic!("expected RemovedStaleLock, got {other:?}"),
        }
        assert!(!dir.path().join(LOCK_FILE).exists());
    }

    #[test]
    fn unlock_leaves_an_actively_held_lock_untouched() {
        let dir = tempfile::tempdir().unwrap();
        let held = try_acquire_write_lock(dir.path()).expect("acquiring the lock must succeed");

        let outcome = try_unlock_stale_write_lock(dir.path()).unwrap();
        match outcome {
            UnlockOutcome::StillLocked { marker } => {
                assert!(
                    marker.is_some(),
                    "expected the active holder's diagnostic marker"
                );
            }
            other => panic!("expected StillLocked, got {other:?}"),
        }
        assert!(
            dir.path().join(LOCK_FILE).exists(),
            "an actively held lock must not be removed"
        );

        drop(held);
    }

    #[test]
    fn a_fresh_acquisition_succeeds_again_after_unlocking_a_stale_lock() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join(LOCK_FILE),
            b"locked by some-other-host, process 999999, time 1",
        )
        .unwrap();

        try_unlock_stale_write_lock(dir.path()).unwrap();

        try_acquire_write_lock(dir.path()).expect("must succeed once the stale lock is cleared");
    }
}
