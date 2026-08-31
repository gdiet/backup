//! Cross-process, whole-session mutating-operation exclusivity - DESIGN-MAINTENANCE-001 in
//! `docs/design/repository-locking.md`. An OS advisory lock (`flock`/`LockFileEx`, via the
//! `fd-lock` crate) on a dedicated file inside `meta/`, held for as long as the returned
//! [`WriteLock`] stays alive.

use std::fs::{File, OpenOptions};
use std::io::ErrorKind;
use std::path::Path;

use crate::Error;

const LOCK_FILE: &str = "lock";

/// Held for as long as the caller wants exclusive, repository-mutating access to a repository -
/// drop to release it.
#[derive(Debug)]
// Never read: this field's only job is to exist for as long as `WriteLock` does, so its `Drop`
// releases the underlying OS lock - not a genuine dead-code finding.
#[allow(dead_code)]
pub struct WriteLock(fd_lock::RwLockWriteGuard<'static, File>);

/// Attempts to acquire `meta_dir`'s write lock, failing immediately (never blocking) if another
/// process already holds it - REQ-MAINTENANCE-004's "refused rather than allowed to proceed".
/// `meta_dir` is a repository's `meta/` directory, already known to exist by the time this is
/// called (e.g. because [`crate::open_repository`] against the same `repo_root` already
/// succeeded).
pub(crate) fn try_acquire_write_lock(meta_dir: &Path) -> Result<WriteLock, Error> {
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .open(meta_dir.join(LOCK_FILE))?;

    // Leaked deliberately: this lock is meant to be held for as long as the process that
    // acquired it keeps running (one repository-mutating session), so there is no meaningful
    // later point to reclaim the allocation at - the OS reclaims the file descriptor/handle
    // itself (and with it, the advisory lock) unconditionally on process exit, however it exits.
    let lock: &'static mut fd_lock::RwLock<File> = Box::leak(Box::new(fd_lock::RwLock::new(file)));

    match lock.try_write() {
        Ok(guard) => Ok(WriteLock(guard)),
        Err(err) if err.kind() == ErrorKind::WouldBlock => {
            Err(Error::AlreadyLocked(meta_dir.to_path_buf()))
        }
        // Anything else (REQ-OPERABILITY-004: a foreseeable failure, not a raw OS error left to
        // stand on its own) - most plausibly the underlying storage not actually enforcing
        // locking at all, the "Known limitation" DESIGN-MAINTENANCE-001 documents for a
        // network-mounted repository.
        Err(source) => Err(Error::LockUnavailable {
            path: meta_dir.to_path_buf(),
            source,
        }),
    }
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
}
