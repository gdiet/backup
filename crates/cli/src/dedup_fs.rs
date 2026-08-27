//! `MountFilesystem` backed by a real, open `db::Repository` - REQ-MOUNT-001/002/003/009.
//! Directory operations only for now: `create`/`write`/`truncate`/`unlink` stay at
//! `mountfs`'s own default `EROFS` - `crates/store`'s byte store exists (REQ-STORAGE-007) but is
//! not wired in here yet.

use std::time::{SystemTime, UNIX_EPOCH};

use mountfs::{Attr, DirEntry, Errno, FileKind, Handle, MountFilesystem, StatfsInfo};

pub struct DedupFs {
    repo: db::Repository,
    read_write: bool,
}

impl DedupFs {
    pub fn new(repo: db::Repository, read_write: bool) -> Self {
        Self { repo, read_write }
    }
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is after the Unix epoch")
        .as_millis() as i64
}

fn to_errno(err: db::Error) -> Errno {
    match err {
        db::Error::NoSuchEntry(_) => Errno::ENOENT,
        db::Error::WrongKind(_) => Errno::ENOTDIR,
        db::Error::DirectoryNotEmpty(_) => Errno::ENOTEMPTY,
        db::Error::EntryAlreadyExists { .. } => Errno::EEXIST,
        db::Error::WouldCreateCycle => Errno::EINVAL,
        db::Error::CannotRemoveRoot => Errno::EINVAL,
        db::Error::RepositoryAlreadyExists(_)
        | db::Error::TargetNotEmpty(_)
        | db::Error::NoRepositoryHere(_)
        | db::Error::Poisoned
        | db::Error::WalUnavailable(_)
        | db::Error::Io(_)
        | db::Error::Sqlite(_)
        | db::Error::Migration(_) => Errno::EIO,
    }
}

fn kind_to_mountfs(kind: db::EntryKind) -> FileKind {
    match kind {
        db::EntryKind::Dir => FileKind::Directory,
        db::EntryKind::File => FileKind::File,
    }
}

/// Splits an absolute path (`/a/b/c`) into its parent (`/a/b`) and final component (`c`).
/// `/a` splits into (`/`, `a`).
fn split_path(path: &str) -> Result<(&str, &str), Errno> {
    let trimmed = path.trim_end_matches('/');
    match trimmed.rfind('/') {
        Some(0) => Ok(("/", &trimmed[1..])),
        Some(idx) => Ok((&trimmed[..idx], &trimmed[idx + 1..])),
        None => Err(Errno::EINVAL),
    }
}

impl DedupFs {
    fn resolve_required(&self, path: &str) -> Result<db::Entry, Errno> {
        self.repo
            .resolve_path(path)
            .map_err(to_errno)?
            .ok_or(Errno::ENOENT)
    }

    fn require_read_write(&self) -> Result<(), Errno> {
        if self.read_write {
            Ok(())
        } else {
            Err(Errno::EROFS)
        }
    }
}

impl MountFilesystem for DedupFs {
    fn getattr(&self, path: &str) -> Result<Attr, Errno> {
        let entry = self.resolve_required(path)?;
        Ok(Attr {
            kind: kind_to_mountfs(entry.kind),
            size: 0,
            mtime_millis: entry.time_millis,
        })
    }

    fn readdir(&self, path: &str) -> Result<Vec<DirEntry>, Errno> {
        let entry = self.resolve_required(path)?;
        if entry.kind != db::EntryKind::Dir {
            return Err(Errno::ENOTDIR);
        }
        let children = self.repo.list_children(entry.id).map_err(to_errno)?;
        Ok(children
            .into_iter()
            .map(|(name, kind)| DirEntry {
                name,
                kind: kind_to_mountfs(kind),
            })
            .collect())
    }

    fn open(&self, path: &str, _write_intent: bool) -> Result<Handle, Errno> {
        let entry = self.resolve_required(path)?;
        // No file ever has content to open yet - nothing creates a file entry today.
        match entry.kind {
            db::EntryKind::Dir => Err(Errno::EISDIR),
            db::EntryKind::File => Err(Errno::EIO),
        }
    }

    fn read(&self, _handle: Handle, _offset: u64, _size: u32) -> Result<Vec<u8>, Errno> {
        Err(Errno::EIO)
    }

    fn release(&self, _handle: Handle) {}

    fn statfs(&self) -> Result<StatfsInfo, Errno> {
        Ok(StatfsInfo {
            block_size: 512,
            max_name_length: mountfs::MAX_NAME_BYTES as u32,
            ..Default::default()
        })
    }

    fn mkdir(&self, path: &str) -> Result<(), Errno> {
        self.require_read_write()?;
        let (parent_path, name) = split_path(path)?;
        let parent = self.resolve_required(parent_path)?;
        self.repo
            .mkdir(parent.id, name, now_millis())
            .map_err(to_errno)?;
        Ok(())
    }

    fn rmdir(&self, path: &str) -> Result<(), Errno> {
        self.require_read_write()?;
        let entry = self.resolve_required(path)?;
        self.repo.rmdir(entry.id, now_millis()).map_err(to_errno)
    }

    fn rename(&self, old_path: &str, new_path: &str, no_replace: bool) -> Result<(), Errno> {
        self.require_read_write()?;
        let (old_parent_path, old_name) = split_path(old_path)?;
        let (new_parent_path, new_name) = split_path(new_path)?;
        let old_parent = self.resolve_required(old_parent_path)?;
        let new_parent = self.resolve_required(new_parent_path)?;
        self.repo
            .rename(
                old_parent.id,
                old_name,
                new_parent.id,
                new_name,
                no_replace,
                now_millis(),
            )
            .map_err(to_errno)
    }

    fn utimens(&self, path: &str, mtime_millis: i64) -> Result<(), Errno> {
        self.require_read_write()?;
        let entry = self.resolve_required(path)?;
        self.repo
            .set_mtime(entry.id, mtime_millis)
            .map_err(to_errno)
    }
}
