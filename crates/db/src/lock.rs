//! Cross-process, whole-session mutating-operation exclusivity - DESIGN-MAINTENANCE-001/002 in
//! `docs/design/repository-locking.md`. An OS advisory lock (`flock`/`LockFileEx`, via the
//! `fd-lock` crate) on a dedicated file inside `meta/`, held for as long as the returned
//! [`WriteLock`] stays alive - preceded by an exclusive-create attempt that narrows the
//! acquisition race further on filesystems where `flock` itself does not propagate correctly
//! (DESIGN-MAINTENANCE-002), and recording a diagnostic marker (who holds it) once acquired.

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
        // around, which would let a competing acquirer's fallback-and-`flock` attempt already
        // succeed and then have this delete remove its lock file out from under it. Best-effort:
        // nothing useful to do if this fails, the process is exiting either way.
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Attempts to acquire `meta_dir`'s write lock, failing immediately (never blocking) if another
/// process already holds it - REQ-MAINTENANCE-004's "refused rather than allowed to proceed".
/// `meta_dir` is a repository's `meta/` directory, already known to exist by the time this is
/// called (e.g. because [`crate::open_repository`] against the same `repo_root` already
/// succeeded).
pub(crate) fn try_acquire_write_lock(meta_dir: &Path) -> Result<WriteLock, Error> {
    let path = meta_dir.join(LOCK_FILE);

    // DESIGN-MAINTENANCE-002: exclusive creation is a more broadly correct atomicity primitive
    // than `flock` alone on some network filesystems - tried first, ahead of `flock`, not instead
    // of it. A leftover file from a process that exited without releasing (a crash, a hard kill)
    // is not itself proof of an active holder, so `AlreadyExists` falls back to the same
    // `flock`-based acquisition below rather than refusing outright.
    let file = match OpenOptions::new().write(true).create_new(true).open(&path) {
        Ok(file) => file,
        Err(err) if err.kind() == ErrorKind::AlreadyExists => {
            OpenOptions::new().write(true).open(&path)?
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

    // Best-effort diagnostic marker (DESIGN-MAINTENANCE-002) - never consulted by the locking
    // logic itself, only for a human reading the file directly to see who holds it. A failure to
    // write it is not itself a locking failure.
    let _ = write_diagnostic_marker(&mut guard);

    Ok(WriteLock { guard, path })
}

/// Overwrites `file`'s content with a line identifying this process - hostname, process id, and
/// acquisition time - discarding whatever it held before (a stale marker from the file's previous
/// holder, on the `AlreadyExists` fallback path above).
fn write_diagnostic_marker(file: &mut File) -> std::io::Result<()> {
    file.set_len(0)?;
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
    fn a_leftover_lock_file_from_an_unclean_exit_is_still_acquirable() {
        let dir = tempfile::tempdir().unwrap();
        // Simulates what an unclean exit (a crash, a hard kill) leaves behind: the file itself,
        // holding a previous session's marker, but nothing currently holding its `flock`.
        std::fs::write(
            dir.path().join(LOCK_FILE),
            b"locked by some-other-host, process 999999, time 1",
        )
        .unwrap();

        let lock = try_acquire_write_lock(dir.path())
            .expect("a leftover file with no active flock holder must still be acquirable");
        drop(lock);
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
    fn acquiring_over_a_stale_marker_overwrites_it_rather_than_appending() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join(LOCK_FILE),
            b"a much longer stale marker than the new one will be",
        )
        .unwrap();

        let _lock = try_acquire_write_lock(dir.path()).unwrap();
        let content = std::fs::read_to_string(dir.path().join(LOCK_FILE)).unwrap();
        assert!(
            !content.contains("stale"),
            "expected the stale marker to be fully overwritten, got: {content:?}"
        );
    }
}
