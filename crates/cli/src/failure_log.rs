//! DESIGN-MOUNT-009's background write-failure log in `docs/design/mount-write-path.md`: a
//! plain, append-only file in `meta/`, plus the read-only degradation flag a `crates/store` I/O
//! failure sets - the mount session's single record of a [`crate::settle_pool::JobPool`] job that
//! did not end in [`crate::pending_files::GenerationSlot::mark_settled`], and (via
//! [`FailureLog::report_connection_dead_once`]) of the shared `db::Repository` connection itself
//! becoming unusable, whether that was found by a background job or a synchronous FUSE call.

use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::Path;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

const FILE_NAME: &str = "write-failures.log";

/// A background settle job's outcome, once it did not end in success - what [`FailureLog::record`]
/// needs to write one line.
pub struct Failure<'a> {
    pub parent_id: i64,
    pub name: &'a str,
    pub time_millis: i64,
    /// Whether this failure's cause is not specific to this one job - the log line's own
    /// "systemic"/"isolated" category text ([`crate::settle_pool::JobError::is_systemic`]).
    pub systemic: bool,
    /// Whether this failure should also degrade the session's future write-intent opens to
    /// read-only ([`crate::settle_pool::JobError::write_degrades_session`]) - narrower than
    /// `systemic`: a systemic `db::Error` is reported via
    /// [`FailureLog::report_connection_dead_once`] instead, since it kills reads too, not only
    /// writes.
    pub degrades_writes: bool,
    pub message: String,
}

pub struct FailureLog {
    file: Mutex<File>,
    degraded: AtomicBool,
    connection_dead: AtomicBool,
}

impl FailureLog {
    /// Opens (creating if missing) the log file inside `meta_dir` (`crates/db`'s
    /// [`db::meta_dir`]) - kept open for the log's whole lifetime rather than reopened per
    /// record, so concurrent settle jobs serialize on one already-open file instead of each
    /// paying to open/close it.
    pub fn open(meta_dir: &Path) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(meta_dir.join(FILE_NAME))?;
        Ok(Self {
            file: Mutex::new(file),
            degraded: AtomicBool::new(false),
            connection_dead: AtomicBool::new(false),
        })
    }

    /// Whether a `crates/store` I/O failure has already degraded this session's future
    /// write-intent opens to read-only - DESIGN-MOUNT-009: once `true`, it never resets until the
    /// mount is unmounted and started again.
    pub fn is_degraded(&self) -> bool {
        self.degraded.load(Ordering::Acquire)
    }

    /// Appends one line for `failure` and, if it degrades writes, degrades the session
    /// ([`Self::is_degraded`]) - a write failure that cannot be recorded (the log file itself
    /// became unwritable) still degrades the session on a write-degrading failure, since silently
    /// continuing to queue more doomed work would be worse than losing this one log line.
    pub fn record(&self, failure: Failure<'_>) {
        if failure.degrades_writes {
            self.degraded.store(true, Ordering::Release);
        }
        let category = if failure.systemic {
            "systemic"
        } else {
            "isolated"
        };
        let line = format!(
            "{} {category} parent_id={} name={:?}: {}\n",
            failure.time_millis, failure.parent_id, failure.name, failure.message
        );
        if let Ok(mut file) = self.file.lock() {
            let _ = file.write_all(line.as_bytes());
        }
    }

    /// Reports, at most once per session, that the shared `db::Repository` connection itself has
    /// become unusable (`db::Error::Poisoned` and the like) - distinct from [`Self::record`]'s
    /// per-job outcome: every future call through that connection, read or write, background job
    /// or synchronous FUSE call alike, already independently returns its own `EIO` on its own
    /// merits, so there is nothing left to gate here (unlike [`Self::record`]'s
    /// `degrades_writes`) - only one actionable line, the first time, telling the operator what
    /// happened and that the process needs restarting. `swap` (not a plain `is_degraded`-style
    /// load-then-store) is what makes "at most once" hold under concurrent callers: only the
    /// caller that actually flips the flag from `false` to `true` writes the line.
    pub fn report_connection_dead_once(&self, time_millis: i64, message: &str) {
        if self.connection_dead.swap(true, Ordering::AcqRel) {
            return;
        }
        let line = format!(
            "{time_millis} connection dead: {message} - every further call this session will \
             fail; unmount and restart the process\n"
        );
        if let Ok(mut file) = self.file.lock() {
            let _ = file.write_all(line.as_bytes());
        }
        eprintln!(
            "dfs: connection dead: {message} - every further call this session will fail; \
             unmount and restart the process"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recording_an_isolated_failure_appends_a_line_and_does_not_degrade() {
        let dir = tempfile::tempdir().unwrap();
        let log = FailureLog::open(dir.path()).unwrap();
        log.record(Failure {
            parent_id: 1,
            name: "a.txt",
            time_millis: 100,
            systemic: false,
            degrades_writes: false,
            message: "no such entry".to_string(),
        });
        assert!(!log.is_degraded());
        let contents = std::fs::read_to_string(dir.path().join(FILE_NAME)).unwrap();
        assert!(contents.contains("isolated"));
        assert!(contents.contains("parent_id=1"));
        assert!(contents.contains("\"a.txt\""));
        assert!(contents.contains("no such entry"));
    }

    #[test]
    fn recording_a_write_degrading_failure_degrades_the_session() {
        let dir = tempfile::tempdir().unwrap();
        let log = FailureLog::open(dir.path()).unwrap();
        log.record(Failure {
            parent_id: 1,
            name: "a.txt",
            time_millis: 100,
            systemic: true,
            degrades_writes: true,
            message: "no space left on device".to_string(),
        });
        assert!(log.is_degraded());
    }

    #[test]
    fn recording_a_systemic_but_non_write_degrading_failure_does_not_degrade() {
        // The `db::Error`-connection-dead case: systemic (not this job's fault), but reported via
        // `report_connection_dead_once` instead of the write-degradation flag - see that method's
        // own doc comment for why (it kills reads too, not only writes).
        let dir = tempfile::tempdir().unwrap();
        let log = FailureLog::open(dir.path()).unwrap();
        log.record(Failure {
            parent_id: 1,
            name: "a.txt",
            time_millis: 100,
            systemic: true,
            degrades_writes: false,
            message: "an internal repository lock was poisoned".to_string(),
        });
        assert!(!log.is_degraded());
    }

    #[test]
    fn degradation_is_sticky_across_further_isolated_failures() {
        let dir = tempfile::tempdir().unwrap();
        let log = FailureLog::open(dir.path()).unwrap();
        log.record(Failure {
            parent_id: 1,
            name: "a.txt",
            time_millis: 100,
            systemic: true,
            degrades_writes: true,
            message: "no space left on device".to_string(),
        });
        log.record(Failure {
            parent_id: 2,
            name: "b.txt",
            time_millis: 200,
            systemic: false,
            degrades_writes: false,
            message: "unrelated".to_string(),
        });
        assert!(log.is_degraded());
    }

    #[test]
    fn appending_more_than_one_record_keeps_every_line() {
        let dir = tempfile::tempdir().unwrap();
        let log = FailureLog::open(dir.path()).unwrap();
        for i in 0..3 {
            log.record(Failure {
                parent_id: i,
                name: "a.txt",
                time_millis: 100 + i,
                systemic: false,
                degrades_writes: false,
                message: format!("failure {i}"),
            });
        }
        let contents = std::fs::read_to_string(dir.path().join(FILE_NAME)).unwrap();
        assert_eq!(contents.lines().count(), 3);
    }

    #[test]
    fn report_connection_dead_once_appends_a_line_but_does_not_degrade_writes() {
        let dir = tempfile::tempdir().unwrap();
        let log = FailureLog::open(dir.path()).unwrap();
        log.report_connection_dead_once(100, "an internal repository lock was poisoned");
        assert!(!log.is_degraded());
        let contents = std::fs::read_to_string(dir.path().join(FILE_NAME)).unwrap();
        assert!(contents.contains("connection dead"));
        assert!(contents.contains("an internal repository lock was poisoned"));
    }

    #[test]
    fn report_connection_dead_once_only_logs_the_first_occurrence() {
        let dir = tempfile::tempdir().unwrap();
        let log = FailureLog::open(dir.path()).unwrap();
        log.report_connection_dead_once(100, "first");
        log.report_connection_dead_once(200, "second");
        let contents = std::fs::read_to_string(dir.path().join(FILE_NAME)).unwrap();
        assert_eq!(contents.lines().count(), 1);
        assert!(contents.contains("first"));
        assert!(!contents.contains("second"));
    }
}
