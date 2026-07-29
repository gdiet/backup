//! Test-only helper binary: mounts a small in-memory `MountFilesystem` at
//! the path given as `argv[1]` and blocks until killed.
//!
//! Exists because `mountfs::windows::mount` has no working in-process
//! clean-shutdown path yet (see `docs/plans/cross-platform-mount-crate.md`,
//! "Windows checkpoint") - `mountfs/src/windows/mod.rs`'s integration test
//! runs this as a real child process instead, so it can terminate the
//! mount by killing the process (exactly how a user would stop `backup
//! mount` on Windows today: close the console / Ctrl+C / task manager)
//! rather than by calling anything WinFSP-specific from inside it.

use std::collections::BTreeMap;
use std::path::PathBuf;

use mountfs::{Attr, DirEntry, Errno, FileKind, Handle, MountFilesystem};

struct TestFs {
    files: BTreeMap<&'static str, &'static [u8]>,
}

impl MountFilesystem for TestFs {
    fn getattr(&self, path: &str) -> Result<Attr, Errno> {
        if path == "/" || path == "/sub" {
            return Ok(Attr {
                kind: FileKind::Directory,
                size: 0,
                mtime_millis: 0,
            });
        }
        match self.files.get(path) {
            Some(content) => Ok(Attr {
                kind: FileKind::File,
                size: content.len() as u64,
                mtime_millis: 0,
            }),
            None => Err(Errno::ENOENT),
        }
    }

    fn readdir(&self, path: &str) -> Result<Vec<DirEntry>, Errno> {
        let prefix = if path == "/" {
            "/".to_string()
        } else {
            format!("{path}/")
        };
        let mut names: Vec<DirEntry> = self
            .files
            .keys()
            .filter_map(|full_path| {
                let rest = full_path.strip_prefix(&prefix)?;
                if rest.contains('/') {
                    None
                } else {
                    Some(DirEntry {
                        name: rest.to_string(),
                        kind: FileKind::File,
                    })
                }
            })
            .collect();
        if path == "/" {
            names.push(DirEntry {
                name: "sub".to_string(),
                kind: FileKind::Directory,
            });
        }
        Ok(names)
    }

    fn open(&self, path: &str, write_intent: bool) -> Result<Handle, Errno> {
        if write_intent {
            return Err(Errno::EROFS);
        }
        self.files
            .keys()
            .position(|&k| k == path)
            .map(|index| Handle(index as u64))
            .ok_or(Errno::ENOENT)
    }

    fn read(&self, handle: Handle, offset: u64, size: u32) -> Result<Vec<u8>, Errno> {
        let content = *self
            .files
            .values()
            .nth(handle.0 as usize)
            .ok_or(Errno::EIO)?;
        let start = (offset as usize).min(content.len());
        let end = start.saturating_add(size as usize).min(content.len());
        Ok(content[start..end].to_vec())
    }

    fn release(&self, _handle: Handle) {}

    fn statfs(&self) -> Result<mountfs::StatfsInfo, Errno> {
        Ok(mountfs::StatfsInfo {
            block_size: 512,
            max_name_length: 255,
            ..Default::default()
        })
    }
}

fn main() {
    let mountpoint = std::env::args()
        .nth(1)
        .expect("usage: windows-mount-spike-helper <mountpoint>");

    let mut files = BTreeMap::new();
    files.insert("/top.txt", b"top level content".as_slice());
    files.insert("/sub/nested.txt", b"hello from a subdirectory".as_slice());
    let fs = TestFs { files };

    if let Err(err) = mountfs::mount(fs, &PathBuf::from(mountpoint), true) {
        eprintln!("mount failed: {err}");
        std::process::exit(1);
    }
}
