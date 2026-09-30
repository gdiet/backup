//! Test-only helper binary: mounts an empty filesystem at `argv[1]` and exits with status 0 if
//! `mountfs::mount` returns `Ok`, or 1 (after printing the error) if it returns `Err`. After an
//! `Ok`, it exits with status 2 instead if a stop signal's disposition is not back to the default.
//!
//! Used by `tests/signal_stop.rs`, which sends a signal to this process. That needs a process of
//! its own, because libfuse tracks a single mount for signal handling per process.

use std::path::PathBuf;

use mountfs::{Attr, DirEntry, Errno, FileKind, Handle, MountFilesystem, StatfsInfo};

struct EmptyFs;

impl MountFilesystem for EmptyFs {
    fn getattr(&self, path: &str) -> Result<Attr, Errno> {
        if path == "/" {
            Ok(Attr {
                kind: FileKind::Directory,
                size: 0,
                mtime_millis: 0,
            })
        } else {
            Err(Errno::ENOENT)
        }
    }

    fn readdir(&self, _path: &str) -> Result<Vec<DirEntry>, Errno> {
        Ok(Vec::new())
    }

    fn open(&self, _path: &str, _write_intent: bool) -> Result<Handle, Errno> {
        Err(Errno::ENOENT)
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
}

fn main() {
    let mountpoint = std::env::args()
        .nth(1)
        .expect("usage: signal_stop_probe <mountpoint>");
    if let Err(err) = mountfs::mount(EmptyFs, &PathBuf::from(mountpoint), true) {
        eprintln!("mount failed: {err}");
        std::process::exit(1);
    }
    #[cfg(target_os = "linux")]
    for signal in [libc::SIGHUP, libc::SIGINT, libc::SIGTERM] {
        let mut action: libc::sigaction = unsafe { std::mem::zeroed() };
        let queried = unsafe { libc::sigaction(signal, std::ptr::null(), &mut action) };
        if queried != 0 || action.sa_sigaction != libc::SIG_DFL {
            eprintln!("signal {signal} does not have its default disposition after the mount");
            std::process::exit(2);
        }
    }
}
