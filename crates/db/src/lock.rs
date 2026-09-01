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
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::Error;

const LOCK_FILE: &str = "lock";

/// How many extra attempts [`create_new_lock_file_with_pending_delete_retry`] makes, and how long
/// it waits between them, before giving up on a `PermissionDenied` result. Chosen generously
/// relative to the microseconds-wide race window it targets (confirmed via a live two-thread race
/// test, not assumed - see this module's own tests) - `10 * 1ms` adds at most 10ms of latency to a
/// single acquisition attempt in the worst case, negligible next to how rarely acquisition itself
/// happens, while still being orders of magnitude longer than the window ever needs to close.
const PENDING_DELETE_RETRY_ATTEMPTS: u32 = 10;
const PENDING_DELETE_RETRY_DELAY: Duration = Duration::from_millis(1);

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
    let file = match create_new_lock_file_with_pending_delete_retry(&path) {
        Ok(file) => file,
        Err(err) if err.kind() == ErrorKind::AlreadyExists => {
            return Err(Error::AlreadyLocked(meta_dir.to_path_buf()));
        }
        Err(source) => {
            return Err(Error::LockFileInaccessible {
                path: meta_dir.to_path_buf(),
                source,
            });
        }
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

/// `OpenOptions::new().write(true).create_new(true).open(path)`, with a short bounded retry
/// specifically for `ErrorKind::PermissionDenied`.
///
/// `WriteLock::drop` deletes the lock file while its own `flock` handle is still open, closing
/// that handle only microseconds later - on Windows/NTFS, a deleted-but-still-open file's
/// directory entry stays in a "pending delete" state until every handle referencing it closes.
/// A `create_new` attempt landing in that narrow window has been confirmed live (a two-thread
/// race test in this module) to observe `PermissionDenied`, not `AlreadyExists` - a case the
/// caller's own `AlreadyExists` handling does not catch.
///
/// `PermissionDenied` is not treated as proof of this race, though: the exact same OS error also
/// covers a genuine, persistent access-rights problem (wrong ACL, a read-only mount), which this
/// retry must not silently reinterpret as "someone else holds the lock" - a real permissions
/// problem would still be failing after these retries, at which point the caller surfaces it as
/// its own, distinct error rather than folding it into `AlreadyLocked`.
fn create_new_lock_file_with_pending_delete_retry(path: &Path) -> std::io::Result<File> {
    let mut attempts_left = PENDING_DELETE_RETRY_ATTEMPTS;
    loop {
        match OpenOptions::new().write(true).create_new(true).open(path) {
            Err(err) if err.kind() == ErrorKind::PermissionDenied && attempts_left > 0 => {
                attempts_left -= 1;
                std::thread::sleep(PENDING_DELETE_RETRY_DELAY);
            }
            result => return result,
        }
    }
}

/// Writes a placeholder byte 0 (`\n`) followed by a line identifying this process - hostname, OS,
/// process id, and acquisition time - into `file`, which `try_acquire_write_lock` guarantees is
/// freshly created and therefore empty.
///
/// Byte 0 is deliberately never part of the actual marker content: fd-lock 4.0.4's Windows
/// implementation locks exactly that one byte (`LockFileEx`/`UnlockFile` both called with offset 0,
/// length 1 - confirmed against its actual source, not assumed), and Windows file locks are
/// mandatory, not advisory like Unix `flock` - any other handle attempting to read a locked byte
/// range is refused by the OS outright, even one opened by the same process. Reserving byte 0 as a
/// fixed, content-free anchor lets [`read_marker`] read everything after it without ever touching
/// the locked range, confirmed live: a second handle reading from byte 1 onward succeeds even while
/// another thread holds the lock. Applied on both platforms - Linux's advisory `flock` never needed
/// this, but branching the on-disk format by OS would cost more than one shared format does.
///
/// The OS name is included because hostname alone does not distinguish a lock acquired by a native
/// Windows session from one acquired by a WSL2 session on the same machine: WSL2 reports the same
/// hostname as its Windows host by default (confirmed on this project's own `julius` - both report
/// `julius`), and the two also have independent process-id namespaces, so even the process id could
/// coincidentally collide between the two without actually being the same process.
fn write_diagnostic_marker(file: &mut File) -> std::io::Result<()> {
    let hostname = gethostname::gethostname();
    let os = std::env::consts::OS;
    let pid = std::process::id();
    let time_millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    writeln!(file)?; // byte 0: the lock anchor, never part of the marker content itself
    writeln!(
        file,
        "locked by {} ({os}), process {pid}, time {time_millis}",
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
///
/// Reads starting at byte 1, deliberately skipping the fixed anchor byte
/// [`write_diagnostic_marker`] reserves at byte 0 - see that function's doc comment for why:
/// reading byte 0 itself would collide with fd-lock's Windows byte-range lock and fail with a
/// mandatory-locking OS error while the write lock is actively held by another handle, exactly the
/// case this needs to work for (an operator asking `dfs unlock` who currently holds a live lock).
fn read_marker(path: &Path) -> Option<String> {
    let mut file = File::open(path).ok()?;
    file.seek(SeekFrom::Start(1)).ok()?;
    let mut content = String::new();
    file.read_to_string(&mut content).ok()?;
    let content = content.trim().to_string();
    (!content.is_empty()).then_some(content)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Writes a synthetic lock file simulating what an unclean exit leaves behind, in the same
    /// format `write_diagnostic_marker` actually produces (byte-0 anchor included) - so tests
    /// reading it back through `read_marker` see real content, not the anchor byte's placeholder
    /// swallowing the first character.
    fn write_synthetic_marker(path: &Path) {
        std::fs::write(path, b"\nlocked by some-other-host, process 999999, time 1").unwrap();
    }

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
        write_synthetic_marker(&dir.path().join(LOCK_FILE));

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
        // read_marker, not a raw std::fs::read_to_string: on Windows, byte 0 is locked while the
        // lock is held (see write_diagnostic_marker's doc comment) - reading the raw file directly
        // is expected to fail here, exactly the case read_marker exists to work around.
        let content = read_marker(&dir.path().join(LOCK_FILE)).unwrap();
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
        write_synthetic_marker(&dir.path().join(LOCK_FILE));

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
        write_synthetic_marker(&dir.path().join(LOCK_FILE));

        try_unlock_stale_write_lock(dir.path()).unwrap();

        try_acquire_write_lock(dir.path()).expect("must succeed once the stale lock is cleared");
    }

    #[test]
    fn a_racing_acquirer_never_sees_anything_but_already_locked_or_success() {
        // Verifies the release path's actual concern (see WriteLock's Drop impl and
        // agent-todos/verify-lock-file-delete-pending-on-real-windows.md): Windows/NTFS leaves a
        // deleted-but-still-open file's directory entry in a "pending delete" state until every
        // handle referencing it closes, which here follows only microseconds after the explicit
        // `remove_file` call (once `Drop::drop` returns and `guard`'s own field-drop releases the
        // flock and closes its handle). A second acquirer's `create_new` landing in that window
        // must resolve to exactly `AlreadyLocked` (before the release) or a clean success (after
        // it) - never a different, unhandled error kind, which would mean `try_acquire_write_lock`
        // needs its own bounded retry around that specific case.
        //
        // This is a same-process, two-thread race rather than two real OS processes: the NTFS
        // pending-delete mechanism is a property of open file handles, not of which process holds
        // them, so two threads each opening their own independent handle (as two real processes
        // would) exercise the identical race. Repeated many times (not just once) since the
        // pending-delete window is only microseconds wide - a single attempt would likely miss it
        // entirely and prove nothing either way.
        use std::sync::{Arc, Barrier};

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_path_buf();

        for _ in 0..500 {
            let held = try_acquire_write_lock(&path).expect("acquiring the lock must succeed");

            let barrier = Arc::new(Barrier::new(2));
            let racer_barrier = Arc::clone(&barrier);
            let racer_path = path.clone();
            let racer = std::thread::spawn(move || {
                racer_barrier.wait();
                // Hammer acquisition attempts right through the release below, until one
                // succeeds (proving the lock does become available again) or a genuinely
                // unexpected error kind shows up (proving it does not, cleanly).
                loop {
                    match try_acquire_write_lock(&racer_path) {
                        Ok(lock) => return Ok(lock),
                        Err(Error::AlreadyLocked(_)) => continue,
                        Err(other) => return Err(other),
                    }
                }
            });

            barrier.wait();
            drop(held); // triggers the delete-then-release sequence under test, right now

            match racer.join().expect("racer thread must not panic") {
                Ok(lock) => drop(lock),
                Err(other) => panic!(
                    "a racing acquisition attempt hit something other than AlreadyLocked or \
                     success while racing the release: {other:?}"
                ),
            }
        }
    }
}
