//! An optional [`MountFilesystem`] decorator that logs every call - its arguments and result -
//! before delegating unchanged to the wrapped implementation.
//!
//! Exists for the class of question that is otherwise only inferable indirectly from a caller's
//! own behavior (a file manager's error dialog, an exit code, a directory listing before and
//! after) - most concretely, whether a given call reached this crate's [`MountFilesystem`]
//! implementation at all, or was refused earlier by the OS/WinFSP itself before ever dispatching
//! here. See `docs/design/deleted-view-directory-recursion.md`'s live-verification section for
//! the case that motivated this.

use std::io::Write;
use std::sync::Mutex;
use std::time::Instant;

use crate::{Attr, DirEntry, Errno, Handle, MountFilesystem, StatfsInfo};

/// Wraps `inner`, logging every [`MountFilesystem`] call to `log` - one line per call,
/// tab-separated: seconds elapsed since this wrapper was created, the method name, its own
/// arguments, and `->` followed by the result - before delegating to `inner` unchanged. Writes
/// are serialized through an internal lock, since multiple dispatch threads call concurrently
/// (DESIGN-MOUNT-006), and best-effort: a write failure is silently dropped rather than turning a
/// logging problem into a mount failure - this is a diagnostic aid, never something the mount
/// session's own correctness depends on.
pub struct LoggingFilesystem<F> {
    inner: F,
    log: Mutex<Box<dyn Write + Send>>,
    start: Instant,
}

impl<F> LoggingFilesystem<F> {
    pub fn new(inner: F, log: impl Write + Send + 'static) -> Self {
        Self {
            inner,
            log: Mutex::new(Box::new(log)),
            start: Instant::now(),
        }
    }

    fn log_line(&self, line: std::fmt::Arguments) {
        // A poisoned lock (a prior write panicked mid-call) is not a reason to stop logging
        // every later call too - `Mutex::lock`'s `Err` still hands back the guard.
        let mut log = self
            .log
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let _ = writeln!(log, "{:>10.3}\t{line}", self.start.elapsed().as_secs_f64());
    }
}

impl<F: MountFilesystem> MountFilesystem for LoggingFilesystem<F> {
    fn getattr(&self, path: &str) -> Result<Attr, Errno> {
        let result = self.inner.getattr(path);
        self.log_line(format_args!("getattr\t{path}\t-> {result:?}"));
        result
    }

    fn readdir(&self, path: &str) -> Result<Vec<DirEntry>, Errno> {
        let result = self.inner.readdir(path);
        match &result {
            Ok(entries) => {
                let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
                self.log_line(format_args!(
                    "readdir\t{path}\t-> Ok, {} entries {names:?}",
                    entries.len()
                ));
            }
            Err(err) => self.log_line(format_args!("readdir\t{path}\t-> {err:?}")),
        }
        result
    }

    fn open(&self, path: &str, write_intent: bool) -> Result<Handle, Errno> {
        let result = self.inner.open(path, write_intent);
        self.log_line(format_args!(
            "open\t{path}\twrite_intent={write_intent}\t-> {result:?}"
        ));
        result
    }

    fn read(&self, handle: Handle, offset: u64, size: u32) -> Result<Vec<u8>, Errno> {
        let result = self.inner.read(handle, offset, size);
        match &result {
            Ok(data) => self.log_line(format_args!(
                "read\t{handle:?}\toffset={offset}\tsize={size}\t-> Ok, {} bytes",
                data.len()
            )),
            Err(err) => self.log_line(format_args!(
                "read\t{handle:?}\toffset={offset}\tsize={size}\t-> {err:?}"
            )),
        }
        result
    }

    fn release(&self, handle: Handle) {
        self.inner.release(handle);
        self.log_line(format_args!("release\t{handle:?}"));
    }

    fn statfs(&self) -> Result<StatfsInfo, Errno> {
        let result = self.inner.statfs();
        self.log_line(format_args!("statfs\t-> {result:?}"));
        result
    }

    fn mkdir(&self, path: &str) -> Result<(), Errno> {
        let result = self.inner.mkdir(path);
        self.log_line(format_args!("mkdir\t{path}\t-> {result:?}"));
        result
    }

    fn create(&self, path: &str) -> Result<Handle, Errno> {
        let result = self.inner.create(path);
        self.log_line(format_args!("create\t{path}\t-> {result:?}"));
        result
    }

    fn unlink(&self, path: &str) -> Result<(), Errno> {
        let result = self.inner.unlink(path);
        self.log_line(format_args!("unlink\t{path}\t-> {result:?}"));
        result
    }

    fn rmdir(&self, path: &str) -> Result<(), Errno> {
        let result = self.inner.rmdir(path);
        self.log_line(format_args!("rmdir\t{path}\t-> {result:?}"));
        result
    }

    fn rename(&self, old_path: &str, new_path: &str, no_replace: bool) -> Result<(), Errno> {
        let result = self.inner.rename(old_path, new_path, no_replace);
        self.log_line(format_args!(
            "rename\t{old_path}\t{new_path}\tno_replace={no_replace}\t-> {result:?}"
        ));
        result
    }

    fn utimens(&self, path: &str, mtime_millis: i64) -> Result<(), Errno> {
        let result = self.inner.utimens(path, mtime_millis);
        self.log_line(format_args!(
            "utimens\t{path}\tmtime_millis={mtime_millis}\t-> {result:?}"
        ));
        result
    }

    fn chmod(&self, path: &str) -> Result<(), Errno> {
        let result = self.inner.chmod(path);
        self.log_line(format_args!("chmod\t{path}\t-> {result:?}"));
        result
    }

    fn chown(&self, path: &str) -> Result<(), Errno> {
        let result = self.inner.chown(path);
        self.log_line(format_args!("chown\t{path}\t-> {result:?}"));
        result
    }

    fn write(&self, handle: Handle, offset: u64, data: &[u8]) -> Result<u32, Errno> {
        let result = self.inner.write(handle, offset, data);
        self.log_line(format_args!(
            "write\t{handle:?}\toffset={offset}\tlen={}\t-> {result:?}",
            data.len()
        ));
        result
    }

    fn truncate(&self, path: &str, size: u64) -> Result<(), Errno> {
        let result = self.inner.truncate(path, size);
        self.log_line(format_args!("truncate\t{path}\tsize={size}\t-> {result:?}"));
        result
    }

    fn on_unmount(&self) {
        self.log_line(format_args!("on_unmount"));
        self.inner.on_unmount();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    struct Fake;

    impl MountFilesystem for Fake {
        fn getattr(&self, _path: &str) -> Result<Attr, Errno> {
            Err(Errno::ENOENT)
        }
        fn readdir(&self, _path: &str) -> Result<Vec<DirEntry>, Errno> {
            Ok(vec![DirEntry {
                name: "a.txt".to_string(),
                kind: crate::FileKind::File,
            }])
        }
        fn open(&self, _path: &str, _write_intent: bool) -> Result<Handle, Errno> {
            Ok(Handle(1))
        }
        fn read(&self, _handle: Handle, _offset: u64, _size: u32) -> Result<Vec<u8>, Errno> {
            Ok(vec![1, 2, 3])
        }
        fn release(&self, _handle: Handle) {}
        fn statfs(&self) -> Result<StatfsInfo, Errno> {
            Ok(StatfsInfo::default())
        }
        fn rmdir(&self, _path: &str) -> Result<(), Errno> {
            Err(Errno::ENOTEMPTY)
        }
    }

    /// A `Write` implementation backed by a shared buffer, so a test can both hand it to
    /// [`LoggingFilesystem::new`] (which takes ownership) and still read back what was written.
    #[derive(Clone)]
    struct SharedBuf(Arc<Mutex<Vec<u8>>>);

    impl Write for SharedBuf {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().write(buf)
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn every_call_is_logged_and_still_delegates_the_real_result() {
        let buf = SharedBuf(Arc::new(Mutex::new(Vec::new())));
        let fs = LoggingFilesystem::new(Fake, buf.clone());

        assert_eq!(fs.getattr("/a").unwrap_err(), Errno::ENOENT);
        assert_eq!(fs.readdir("/").unwrap().len(), 1);
        assert_eq!(fs.rmdir("/deleted").unwrap_err(), Errno::ENOTEMPTY);

        let written = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
        let lines: Vec<&str> = written.lines().collect();
        assert_eq!(lines.len(), 3, "got: {written}");
        assert!(
            lines[0].contains("getattr\t/a\t-> Err(Errno(2))"),
            "{}",
            lines[0]
        );
        assert!(
            lines[1].contains("readdir\t/\t-> Ok, 1 entries [\"a.txt\"]"),
            "{}",
            lines[1]
        );
        assert!(
            lines[2].contains("rmdir\t/deleted\t-> Err(Errno(39))"),
            "{}",
            lines[2]
        );
    }

    #[test]
    fn a_write_failure_never_propagates_as_a_call_failure() {
        struct AlwaysFails;
        impl Write for AlwaysFails {
            fn write(&mut self, _buf: &[u8]) -> std::io::Result<usize> {
                Err(std::io::Error::other("disk full, say"))
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let fs = LoggingFilesystem::new(Fake, AlwaysFails);
        // Must still delegate and return the real result, not panic or somehow surface the
        // logging failure as part of the mount operation's own outcome.
        assert_eq!(fs.getattr("/a").unwrap_err(), Errno::ENOENT);
    }
}
