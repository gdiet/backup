//! DESIGN-MOUNT-009's background write-failure log in `docs/design/mount-write-path.md`: a
//! plain, append-only file in `meta/`, plus the read-only degradation flag a systemic failure
//! sets - the mount session's single record of a [`crate::settle_pool::JobPool`] job that did not
//! end in [`crate::pending_files::GenerationSlot::mark_settled`].

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
    pub systemic: bool,
    pub message: String,
}

pub struct FailureLog {
    file: Mutex<File>,
    degraded: AtomicBool,
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
        })
    }

    /// Whether a systemic failure has already been recorded this session - DESIGN-MOUNT-009's
    /// read-only degradation: once `true`, it never resets until the mount is unmounted and
    /// started again.
    pub fn is_degraded(&self) -> bool {
        self.degraded.load(Ordering::Acquire)
    }

    /// Appends one line for `failure` and, if it is systemic, degrades the session
    /// ([`Self::is_degraded`]) - a write failure that cannot be recorded (the log file itself
    /// became unwritable) still degrades the session on a systemic failure, since silently
    /// continuing to queue more doomed work would be worse than losing this one log line.
    pub fn record(&self, failure: Failure<'_>) {
        if failure.systemic {
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
    fn recording_a_systemic_failure_degrades_the_session() {
        let dir = tempfile::tempdir().unwrap();
        let log = FailureLog::open(dir.path()).unwrap();
        log.record(Failure {
            parent_id: 1,
            name: "a.txt",
            time_millis: 100,
            systemic: true,
            message: "no space left on device".to_string(),
        });
        assert!(log.is_degraded());
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
            message: "no space left on device".to_string(),
        });
        log.record(Failure {
            parent_id: 2,
            name: "b.txt",
            time_millis: 200,
            systemic: false,
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
                message: format!("failure {i}"),
            });
        }
        let contents = std::fs::read_to_string(dir.path().join(FILE_NAME)).unwrap();
        assert_eq!(contents.lines().count(), 3);
    }
}
