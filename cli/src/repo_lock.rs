//! Cross-process mutual exclusion for repository-mutating commands that
//! can't safely run concurrently with each other or with themselves -
//! currently only `compact-store`, which relocates live chunks' bytes
//! and would race with `store`/`mount --read-write`/`reclaim-space` (or a
//! second `compact-store`) touching the same `chunk_extents` rows and
//! store bytes at once. See `docs/plans/implemented/compact-store.md`'s
//! "Exclusivity while running" decision - other commands aren't guarded
//! by this today.
//!
//! Backed by the OS's own advisory file locking
//! (`std::fs::File::try_lock`, stabilized in the standard library - no
//! extra dependency needed) rather than a plain "does this marker file
//! exist" convention: the OS releases the lock automatically when the
//! holding process's file handle closes, for any reason, including
//! `SIGKILL` or a crash - a stale marker file left behind by a killed
//! process would otherwise block every future run until someone manually
//! removes it, which is exactly the kind of crash-safety gap this
//! project has been auditing for elsewhere (see `docs/plans/implemented/
//! compact-store.md`'s "Crash-safety today").

use std::fs::File;
use std::io;
use std::path::Path;

/// Held for as long as this value is alive; the lock releases automatically
/// (via the OS, not any code here) when it's dropped or the process exits.
pub struct RepoLock {
    _file: File,
}

impl RepoLock {
    /// Attempts to acquire the lock file at `meta_dir/.lock`, creating it
    /// if it doesn't exist yet. Returns `Ok(None)` - not an error - if
    /// another process already holds it, so callers can print a clear,
    /// specific message rather than a generic I/O failure.
    pub fn try_acquire(meta_dir: &Path) -> io::Result<Option<Self>> {
        let file = File::options()
            .write(true)
            .create(true)
            .truncate(false)
            .open(meta_dir.join(".lock"))?;
        match file.try_lock() {
            Ok(()) => Ok(Some(Self { _file: file })),
            Err(std::fs::TryLockError::WouldBlock) => Ok(None),
            Err(std::fs::TryLockError::Error(err)) => Err(err),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_second_acquire_attempt_fails_while_the_first_is_held() {
        let dir = tempfile::tempdir().unwrap();
        let first = RepoLock::try_acquire(dir.path()).unwrap();
        assert!(first.is_some());

        let second = RepoLock::try_acquire(dir.path()).unwrap();
        assert!(second.is_none());
    }

    #[test]
    fn dropping_the_lock_lets_a_later_acquire_succeed() {
        let dir = tempfile::tempdir().unwrap();
        let first = RepoLock::try_acquire(dir.path()).unwrap();
        drop(first);

        let second = RepoLock::try_acquire(dir.path()).unwrap();
        assert!(second.is_some());
    }
}
