//! `dfs mount`'s current implementation: a temporary, in-memory playground
//! filesystem, standing in until repository mounting exists. Not a REQ-MOUNT
//! implementation - deliberately not wired to any real repository, and says
//! so up front (both on the terminal and inside the mount itself) so it is
//! never mistaken for one.

use std::path::Path;

use mountfs::{Attr, DirEntry, Errno, FileKind, Handle, MountFilesystem, StatfsInfo};

const CONTENT: &[u8] = b"This is a temporary in-memory playground filesystem mounted by `dfs \
mount`, standing in for a real DedupFS repository until repository mounting is implemented. \
Nothing here is persisted.\n";

struct PlaygroundFs;

impl MountFilesystem for PlaygroundFs {
    fn getattr(&self, path: &str) -> Result<Attr, Errno> {
        match path {
            "/" => Ok(Attr {
                kind: FileKind::Directory,
                size: 0,
                mtime_millis: 0,
            }),
            "/playground.txt" => Ok(Attr {
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
            name: "playground.txt".to_string(),
            kind: FileKind::File,
        }])
    }

    fn open(&self, path: &str, write_intent: bool) -> Result<Handle, Errno> {
        if write_intent {
            return Err(Errno::EROFS);
        }
        if path == "/playground.txt" {
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

pub fn run(mountpoint: &Path) {
    eprintln!(
        "This mounts a temporary in-memory playground filesystem, not a DedupFS repository -\n\
         real repository mounting is not implemented yet."
    );

    if let Err(err) = mountfs::preflight() {
        eprintln!("error: {err}");
        std::process::exit(1);
    }
    if let Err(err) = mountfs::mount(PlaygroundFs, mountpoint, true) {
        eprintln!("mount failed: {err}");
        std::process::exit(1);
    }
}
