//! The smallest complete [`MountFilesystem`] implementation: a single
//! read-only file at the mount root. Start here before `memfs`.
//!
//! Run with `cargo run --example hellofs -- <mountpoint>`, then read
//! `<mountpoint>/hello.txt`. Unmount with `fusermount3 -u <mountpoint>`
//! (Linux) or Ctrl+C (Windows).

use std::path::Path;

use mountfs::{Attr, DirEntry, Errno, FileKind, Handle, MountFilesystem, StatfsInfo};

const CONTENT: &[u8] = b"Hello from mountfs!\n";

struct HelloFs;

impl MountFilesystem for HelloFs {
    fn getattr(&self, path: &str) -> Result<Attr, Errno> {
        match path {
            "/" => Ok(Attr {
                kind: FileKind::Directory,
                size: 0,
                mtime_millis: 0,
            }),
            "/hello.txt" => Ok(Attr {
                kind: FileKind::File,
                size: CONTENT.len() as u64,
                mtime_millis: 0,
            }),
            _ => Err(Errno::ENOENT),
        }
    }

    fn readdir(&self, path: &str) -> Result<Vec<DirEntry>, Errno> {
        if path != "/" {
            return Err(Errno::ENOTDIR);
        }
        Ok(vec![DirEntry {
            name: "hello.txt".to_string(),
            kind: FileKind::File,
        }])
    }

    fn open(&self, path: &str, write_intent: bool) -> Result<Handle, Errno> {
        if write_intent {
            return Err(Errno::EROFS);
        }
        if path == "/hello.txt" {
            Ok(Handle(0))
        } else {
            Err(Errno::ENOENT)
        }
    }

    fn read(&self, _handle: Handle, offset: u64, size: u32) -> Result<Vec<u8>, Errno> {
        let start = (offset as usize).min(CONTENT.len());
        let end = start.saturating_add(size as usize).min(CONTENT.len());
        Ok(CONTENT[start..end].to_vec())
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
        .expect("usage: hellofs <mountpoint>");
    if let Err(err) = mountfs::mount(HelloFs, Path::new(&mountpoint), true) {
        eprintln!("mount failed: {err}");
        std::process::exit(1);
    }
}
