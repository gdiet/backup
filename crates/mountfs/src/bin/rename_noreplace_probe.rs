//! Test-only helper binary: mounts a fixed three-file filesystem and logs
//! every `rename` call's observed `no_replace` value to the path given as
//! `argv[2]` - used by `tests/rename_noreplace.rs` to empirically confirm
//! what bit convention WinFSP's `cygfuse` layer actually uses for its
//! `rename` callback's `flags` parameter (see
//! `agent-todos/verify-rename-noreplace-against-real-winfsp.md`).
//!
//! `rename` always returns `Ok(())` regardless of `no_replace` - this
//! binary only observes what the dispatch layer derived from the real
//! Win32-level call, it does not exercise this crate's own accept/reject
//! logic for that flag.

use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Mutex;

use mountfs::{Attr, DirEntry, Errno, FileKind, Handle, MountFilesystem, StatfsInfo};

const FILES: [&str; 3] = ["/target.txt", "/source_a.txt", "/source_b.txt"];

struct RenameProbeFs {
    log: Mutex<std::fs::File>,
}

impl MountFilesystem for RenameProbeFs {
    fn getattr(&self, path: &str) -> Result<Attr, Errno> {
        if path == "/" {
            return Ok(Attr {
                kind: FileKind::Directory,
                size: 0,
                mtime_millis: 0,
            });
        }
        if FILES.contains(&path) {
            return Ok(Attr {
                kind: FileKind::File,
                size: 0,
                mtime_millis: 0,
            });
        }
        Err(Errno::ENOENT)
    }

    fn readdir(&self, path: &str) -> Result<Vec<DirEntry>, Errno> {
        if path != "/" {
            return Err(Errno::ENOTDIR);
        }
        Ok(FILES
            .iter()
            .map(|name| DirEntry {
                name: name.trim_start_matches('/').to_string(),
                kind: FileKind::File,
            })
            .collect())
    }

    fn open(&self, path: &str, _write_intent: bool) -> Result<Handle, Errno> {
        if FILES.contains(&path) {
            Ok(Handle(0))
        } else {
            Err(Errno::ENOENT)
        }
    }

    fn read(&self, _handle: Handle, _offset: u64, _size: u32) -> Result<Vec<u8>, Errno> {
        Ok(Vec::new())
    }

    fn release(&self, _handle: Handle) {}

    fn statfs(&self) -> Result<StatfsInfo, Errno> {
        Ok(StatfsInfo {
            block_size: 512,
            max_name_length: mountfs::MAX_NAME_BYTES as u32,
            ..Default::default()
        })
    }

    fn rename(&self, old_path: &str, new_path: &str, no_replace: bool) -> Result<(), Errno> {
        let mut log = self.log.lock().unwrap();
        writeln!(log, "old={old_path} new={new_path} no_replace={no_replace}").unwrap();
        log.flush().unwrap();
        Ok(())
    }
}

fn main() {
    let mut args = std::env::args().skip(1);
    let mountpoint = args
        .next()
        .expect("usage: rename_noreplace_probe <mountpoint> <log-path>");
    let log_path = PathBuf::from(
        args.next()
            .expect("usage: rename_noreplace_probe <mountpoint> <log-path>"),
    );

    let log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .expect("failed to open log path");
    let fs = RenameProbeFs {
        log: Mutex::new(log),
    };

    if let Err(err) = mountfs::mount(fs, &PathBuf::from(mountpoint), false) {
        eprintln!("mount failed: {err}");
        std::process::exit(1);
    }
}
