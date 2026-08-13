//! Cross-process mutual exclusion for repository-mutating commands that
//! can't safely run concurrently with each other or with themselves -
//! `store`, `mount --read-write`, `compact-store`, and `reclaim-space`:
//! the commands that physically allocate/relocate store bytes or change
//! what byte ranges are free to reuse, and would race with one another
//! touching the same `chunk_extents` rows and store bytes at once. See
//! `docs/plans/cross-process-repository-locking.md` for the full
//! reasoning, including why this is deliberately *not* generalized to
//! read-only commands or to purely metadata-mutating ones (`del`,
//! `undelete`, `fix-problems`, `db compact`) - those stay lock-free.
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
use std::thread;
use std::time::{Duration, Instant};

/// How often [`RepoLock::acquire`] retries while waiting for the lock to
/// free up. `std::fs::File::try_lock` has no built-in timeout (it's either
/// instantaneous or `lock` blocks forever), so a bounded wait has to be
/// built as a poll loop - fine-grained enough that a `--lock-wait` given in
/// whole seconds doesn't overshoot by much, coarse enough not to spin.
const POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Held for as long as this value is alive; the lock releases automatically
/// (via the OS, not any code here) when it's dropped or the process exits.
pub struct RepoLock {
    _file: File,
}

impl RepoLock {
    /// Attempts to acquire the lock file at `meta_dir/.lock`, creating it
    /// if it doesn't exist yet, waiting up to `wait` for it to free up if
    /// another process already holds it (polling every [`POLL_INTERVAL`]).
    /// `Duration::ZERO` tries exactly once, no waiting. Returns `Ok(None)`,
    /// not an error, if the lock is still held once `wait` elapses, so
    /// callers can print a clear, specific message rather than a generic
    /// I/O failure.
    pub fn acquire(meta_dir: &Path, wait: Duration) -> io::Result<Option<Self>> {
        let file = File::options()
            .write(true)
            .create(true)
            .truncate(false)
            .open(meta_dir.join(".lock"))?;
        let deadline = Instant::now() + wait;
        loop {
            match file.try_lock() {
                Ok(()) => return Ok(Some(Self { _file: file })),
                Err(std::fs::TryLockError::Error(err)) => return Err(err),
                Err(std::fs::TryLockError::WouldBlock) => {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        return Ok(None);
                    }
                    thread::sleep(POLL_INTERVAL.min(remaining));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_second_acquire_attempt_fails_while_the_first_is_held_and_wait_is_zero() {
        let dir = tempfile::tempdir().unwrap();
        let first = RepoLock::acquire(dir.path(), Duration::ZERO).unwrap();
        assert!(first.is_some());

        let second = RepoLock::acquire(dir.path(), Duration::ZERO).unwrap();
        assert!(second.is_none());
    }

    #[test]
    fn dropping_the_lock_lets_a_later_acquire_succeed() {
        let dir = tempfile::tempdir().unwrap();
        let first = RepoLock::acquire(dir.path(), Duration::ZERO).unwrap();
        drop(first);

        let second = RepoLock::acquire(dir.path(), Duration::ZERO).unwrap();
        assert!(second.is_some());
    }

    #[test]
    fn acquire_with_a_nonzero_wait_times_out_and_returns_none_if_never_released() {
        let dir = tempfile::tempdir().unwrap();
        let _first = RepoLock::acquire(dir.path(), Duration::ZERO)
            .unwrap()
            .unwrap();

        let started = Instant::now();
        let second = RepoLock::acquire(dir.path(), Duration::from_millis(300)).unwrap();
        assert!(second.is_none());
        assert!(started.elapsed() >= Duration::from_millis(300));
    }

    #[test]
    fn acquire_with_a_nonzero_wait_succeeds_once_the_holder_releases_mid_wait() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path().to_path_buf();
        let first = RepoLock::acquire(&dir_path, Duration::ZERO)
            .unwrap()
            .unwrap();

        let releaser = thread::spawn(move || {
            thread::sleep(Duration::from_millis(150));
            drop(first);
        });

        let second = RepoLock::acquire(&dir_path, Duration::from_secs(2)).unwrap();
        assert!(second.is_some());
        releaser.join().unwrap();
    }
}
