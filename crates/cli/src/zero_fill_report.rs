//! What a mount session tells the operator when `--best-effort` replaces missing or unreadable
//! stored data by zero-value bytes (DESIGN-MOUNT-027 in `docs/design/mount-write-path.md`).
//!
//! A warning is printed once per missing or short stored data file, and once per distinct read
//! error, not once per read. Both sets are bounded, by the number of stored data files and by the
//! number of distinct errors, so they need no eviction. A summary is available for the end of the
//! session.

use std::collections::HashSet;
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// Why a read happened. A read for saving a modified file matters more than one for display,
/// because its zero-value bytes become part of the saved content.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadOrigin {
    Visible,
    Saving,
}

struct Sink {
    out: Box<dyn Write + Send>,
    warned_files: HashSet<PathBuf>,
    warned_errors: HashSet<String>,
}

pub struct ZeroFillReport {
    sink: Mutex<Sink>,
    zero_filled_reads: AtomicU64,
    zero_filled_reads_while_saving: AtomicU64,
    saving_warned: AtomicBool,
}

impl ZeroFillReport {
    /// Reports to stderr.
    pub fn new() -> Self {
        Self::with_output(Box::new(std::io::stderr()))
    }

    pub fn with_output(out: Box<dyn Write + Send>) -> Self {
        Self {
            sink: Mutex::new(Sink {
                out,
                warned_files: HashSet::new(),
                warned_errors: HashSet::new(),
            }),
            zero_filled_reads: AtomicU64::new(0),
            zero_filled_reads_while_saving: AtomicU64::new(0),
            saving_warned: AtomicBool::new(false),
        }
    }

    /// Records one read that touched the given missing or short stored data files.
    pub fn note(&self, origin: ReadOrigin, missing_or_short: &[PathBuf]) {
        let first_saving_read = self.count_read(origin);
        // A write failure on stderr is not a reason to fail the read.
        let mut sink = self.sink.lock().expect("not poisoned");
        for path in missing_or_short {
            if sink.warned_files.insert(path.clone()) {
                let _ = writeln!(
                    sink.out,
                    "warning: stored data file {} is missing or short - reads that touch it \
                     return zero-value bytes (0x00) instead",
                    path.display()
                );
            }
        }
        if first_saving_read {
            Self::write_saving_warning(&mut sink);
        }
    }

    /// Records one read that failed with `error` and was treated as missing data.
    pub fn note_read_error(&self, origin: ReadOrigin, error: &io::Error) {
        let first_saving_read = self.count_read(origin);
        let mut sink = self.sink.lock().expect("not poisoned");
        if sink.warned_errors.insert(error.to_string()) {
            let _ = writeln!(
                sink.out,
                "warning: reading stored data failed ({error}) - treated as missing data, reads \
                 that hit it return zero-value bytes (0x00) instead"
            );
        }
        if first_saving_read {
            Self::write_saving_warning(&mut sink);
        }
    }

    /// Counts the read, and returns whether it is the first one while saving.
    fn count_read(&self, origin: ReadOrigin) -> bool {
        self.zero_filled_reads.fetch_add(1, Ordering::Relaxed);
        origin == ReadOrigin::Saving && {
            self.zero_filled_reads_while_saving
                .fetch_add(1, Ordering::Relaxed);
            !self.saving_warned.swap(true, Ordering::Relaxed)
        }
    }

    fn write_saving_warning(sink: &mut Sink) {
        let _ = writeln!(
            sink.out,
            "warning: saving a modified file read missing stored data as zero-value bytes \
             (0x00) - the saved content contains them"
        );
    }

    /// A line for the end of the session, or `None` if no read was zero-filled.
    pub fn summary(&self) -> Option<String> {
        let reads = self.zero_filled_reads.load(Ordering::Relaxed);
        if reads == 0 {
            return None;
        }
        let (files, errors) = {
            let sink = self.sink.lock().expect("not poisoned");
            (sink.warned_files.len(), sink.warned_errors.len())
        };
        let saving = self.zero_filled_reads_while_saving.load(Ordering::Relaxed);
        let mut line = format!(
            "{reads} read(s) returned zero-value bytes (0x00) for missing or unreadable stored \
             data, touching {files} stored data file(s)"
        );
        if errors > 0 {
            line.push_str(&format!(
                ", and {errors} kind(s) of read error treated as missing data"
            ));
        }
        if saving > 0 {
            line.push_str(&format!(
                ", {saving} of them while saving modified files, whose saved content contains \
                 these zero-value bytes"
            ));
        }
        Some(line)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[derive(Clone, Default)]
    struct SharedBuffer(Arc<Mutex<Vec<u8>>>);

    impl Write for SharedBuffer {
        fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(data);
            Ok(data.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl SharedBuffer {
        fn text(&self) -> String {
            String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
        }
    }

    fn report() -> (ZeroFillReport, SharedBuffer) {
        let buffer = SharedBuffer::default();
        (
            ZeroFillReport::with_output(Box::new(buffer.clone())),
            buffer,
        )
    }

    #[test]
    fn warns_once_per_stored_data_file_however_often_it_is_read() {
        let (report, buffer) = report();
        let file = vec![PathBuf::from("00/00/0000000000")];
        for _ in 0..5 {
            report.note(ReadOrigin::Visible, &file);
        }
        assert_eq!(buffer.text().matches("warning:").count(), 1);
        assert!(buffer.text().contains("00/00/0000000000"));
        assert!(buffer.text().contains("zero-value bytes (0x00)"));
    }

    #[test]
    fn warns_separately_for_each_stored_data_file() {
        let (report, buffer) = report();
        report.note(
            ReadOrigin::Visible,
            &[PathBuf::from("a"), PathBuf::from("b")],
        );
        report.note(ReadOrigin::Visible, &[PathBuf::from("b")]);
        assert_eq!(buffer.text().matches("warning:").count(), 2);
    }

    #[test]
    fn a_read_while_saving_warns_once_per_session_in_addition() {
        let (report, buffer) = report();
        let file = vec![PathBuf::from("a")];
        report.note(ReadOrigin::Saving, &file);
        report.note(ReadOrigin::Saving, &file);
        let text = buffer.text();
        assert_eq!(text.matches("saving a modified file").count(), 1);
        assert_eq!(text.matches("warning:").count(), 2);
    }

    #[test]
    fn a_visible_read_never_gives_the_saving_warning() {
        let (report, buffer) = report();
        report.note(ReadOrigin::Visible, &[PathBuf::from("a")]);
        assert!(!buffer.text().contains("saving a modified file"));
    }

    #[test]
    fn warns_once_per_distinct_read_error_however_often_it_happens() {
        let (report, buffer) = report();
        let denied = io::Error::from(io::ErrorKind::PermissionDenied);
        for _ in 0..4 {
            report.note_read_error(ReadOrigin::Visible, &denied);
        }
        report.note_read_error(ReadOrigin::Visible, &io::Error::from_raw_os_error(5));
        let text = buffer.text();
        assert_eq!(text.matches("warning:").count(), 2, "{text}");
        assert!(text.contains("treated as missing data"), "{text}");
    }

    #[test]
    fn a_read_error_while_saving_gives_the_saving_warning_too() {
        let (report, buffer) = report();
        report.note_read_error(ReadOrigin::Saving, &io::Error::from_raw_os_error(5));
        assert!(buffer.text().contains("saving a modified file"));
    }

    #[test]
    fn summary_mentions_read_errors_treated_as_missing_data() {
        let (report, _) = report();
        report.note_read_error(ReadOrigin::Visible, &io::Error::from_raw_os_error(5));
        let summary = report.summary().unwrap();
        assert!(summary.contains("1 kind(s) of read error"), "{summary}");
    }

    #[test]
    fn summary_is_none_without_any_zero_filled_read() {
        let (report, _) = report();
        assert_eq!(report.summary(), None);
    }

    #[test]
    fn summary_counts_reads_files_and_reads_while_saving() {
        let (report, _) = report();
        report.note(ReadOrigin::Visible, &[PathBuf::from("a")]);
        report.note(
            ReadOrigin::Saving,
            &[PathBuf::from("a"), PathBuf::from("b")],
        );
        let summary = report.summary().unwrap();
        assert!(summary.starts_with("2 read(s)"), "{summary}");
        assert!(summary.contains("2 stored data file(s)"), "{summary}");
        assert!(summary.contains("1 of them while saving"), "{summary}");
    }
}
