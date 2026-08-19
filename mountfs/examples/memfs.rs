//! A complete, read-write, in-memory [`MountFilesystem`] reference
//! implementation - unlike `hellofs`, exercises the full trait (real
//! directories, `create`/`write`/`mkdir`/`unlink`/`rmdir`/`rename`/
//! `truncate`/`utimens`) and shows how to structure one: every method
//! takes `&self`, not `&mut self`, so any implementation that actually
//! mutates state needs interior mutability - here, a single `Mutex` around
//! the whole tree.
//!
//! Run with `cargo run --example memfs -- <mountpoint>`, then read, write,
//! create, and delete files/directories through it with ordinary tools.
//! Unmount with `fusermount3 -u <mountpoint>` (Linux) or Ctrl+C (Windows).
//! Content does not persist across runs.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use mountfs::{Attr, DirEntry, Errno, FileKind, Handle, MountFilesystem, StatfsInfo};

enum Node {
    File { content: Vec<u8>, mtime_millis: i64 },
    Directory,
}

struct MemFs {
    // Keyed by full path ("/", "/sub", "/sub/file.txt", ...) - a flat map
    // is enough for a path-based trait; there is no need to model parent/
    // child links explicitly, `readdir` just filters by path prefix.
    entries: Mutex<BTreeMap<String, Node>>,
    // Maps each open Handle back to the path it was opened for, since
    // `read`/`write`/`release` only carry the handle, not the path.
    open_paths: Mutex<HashMap<u64, String>>,
    next_handle: AtomicU64,
}

impl Default for MemFs {
    fn default() -> Self {
        let mut entries = BTreeMap::new();
        entries.insert("/".to_string(), Node::Directory);
        Self {
            entries: Mutex::new(entries),
            open_paths: Mutex::new(HashMap::new()),
            next_handle: AtomicU64::new(0),
        }
    }
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// `None` for the root itself, which has no parent.
fn parent_of(path: &str) -> Option<&str> {
    if path == "/" {
        return None;
    }
    match path.rsplit_once('/') {
        Some(("", _)) => Some("/"),
        Some((parent, _)) => Some(parent),
        None => None,
    }
}

impl MemFs {
    fn allocate_handle(&self, path: &str) -> Handle {
        let id = self.next_handle.fetch_add(1, Ordering::Relaxed);
        self.open_paths.lock().unwrap().insert(id, path.to_string());
        Handle(id)
    }

    fn path_for(&self, handle: Handle) -> Result<String, Errno> {
        self.open_paths
            .lock()
            .unwrap()
            .get(&handle.0)
            .cloned()
            .ok_or(Errno::EIO)
    }
}

/// Shared precondition for `mkdir`/`create`: the parent must exist and be a
/// directory, and `path` itself must not already exist.
fn check_creatable(entries: &BTreeMap<String, Node>, path: &str) -> Result<(), Errno> {
    let parent = parent_of(path).ok_or(Errno::EEXIST)?; // path is "/", which always exists
    if !matches!(entries.get(parent), Some(Node::Directory)) {
        return Err(Errno::ENOENT);
    }
    if entries.contains_key(path) {
        return Err(Errno::EEXIST);
    }
    Ok(())
}

impl MountFilesystem for MemFs {
    fn getattr(&self, path: &str) -> Result<Attr, Errno> {
        match self.entries.lock().unwrap().get(path) {
            Some(Node::Directory) => Ok(Attr {
                kind: FileKind::Directory,
                size: 0,
                mtime_millis: 0,
            }),
            Some(Node::File {
                content,
                mtime_millis,
            }) => Ok(Attr {
                kind: FileKind::File,
                size: content.len() as u64,
                mtime_millis: *mtime_millis,
            }),
            None => Err(Errno::ENOENT),
        }
    }

    fn readdir(&self, path: &str) -> Result<Vec<DirEntry>, Errno> {
        let entries = self.entries.lock().unwrap();
        match entries.get(path) {
            Some(Node::Directory) => {}
            Some(Node::File { .. }) => return Err(Errno::ENOTDIR),
            None => return Err(Errno::ENOENT),
        }
        let prefix = if path == "/" {
            "/".to_string()
        } else {
            format!("{path}/")
        };
        Ok(entries
            .iter()
            .filter_map(|(full_path, node)| {
                let rest = full_path.strip_prefix(&prefix)?;
                if rest.is_empty() || rest.contains('/') {
                    None
                } else {
                    let kind = match node {
                        Node::Directory => FileKind::Directory,
                        Node::File { .. } => FileKind::File,
                    };
                    Some(DirEntry {
                        name: rest.to_string(),
                        kind,
                    })
                }
            })
            .collect())
    }

    fn open(&self, path: &str, _write_intent: bool) -> Result<Handle, Errno> {
        match self.entries.lock().unwrap().get(path) {
            Some(Node::File { .. }) => Ok(self.allocate_handle(path)),
            Some(Node::Directory) => Err(Errno::EISDIR),
            None => Err(Errno::ENOENT),
        }
    }

    fn read(&self, handle: Handle, offset: u64, size: u32) -> Result<Vec<u8>, Errno> {
        let path = self.path_for(handle)?;
        match self.entries.lock().unwrap().get(&path) {
            Some(Node::File { content, .. }) => {
                let start = (offset as usize).min(content.len());
                let end = start.saturating_add(size as usize).min(content.len());
                Ok(content[start..end].to_vec())
            }
            _ => Err(Errno::EIO),
        }
    }

    fn release(&self, handle: Handle) {
        self.open_paths.lock().unwrap().remove(&handle.0);
    }

    fn statfs(&self) -> Result<StatfsInfo, Errno> {
        let entries = self.entries.lock().unwrap();
        Ok(StatfsInfo {
            files: entries.len() as u64,
            block_size: 512,
            max_name_length: mountfs::MAX_NAME_BYTES as u32,
            ..Default::default()
        })
    }

    fn mkdir(&self, path: &str) -> Result<(), Errno> {
        let mut entries = self.entries.lock().unwrap();
        check_creatable(&entries, path)?;
        entries.insert(path.to_string(), Node::Directory);
        Ok(())
    }

    fn create(&self, path: &str) -> Result<Handle, Errno> {
        let mut entries = self.entries.lock().unwrap();
        check_creatable(&entries, path)?;
        entries.insert(
            path.to_string(),
            Node::File {
                content: Vec::new(),
                mtime_millis: now_millis(),
            },
        );
        drop(entries);
        Ok(self.allocate_handle(path))
    }

    fn unlink(&self, path: &str) -> Result<(), Errno> {
        let mut entries = self.entries.lock().unwrap();
        match entries.get(path) {
            Some(Node::File { .. }) => {
                entries.remove(path);
                Ok(())
            }
            Some(Node::Directory) => Err(Errno::EISDIR),
            None => Err(Errno::ENOENT),
        }
    }

    fn rmdir(&self, path: &str) -> Result<(), Errno> {
        let mut entries = self.entries.lock().unwrap();
        match entries.get(path) {
            Some(Node::Directory) => {}
            Some(Node::File { .. }) => return Err(Errno::ENOTDIR),
            None => return Err(Errno::ENOENT),
        }
        let prefix = format!("{path}/");
        if entries.keys().any(|k| k.starts_with(&prefix)) {
            return Err(Errno::ENOTEMPTY);
        }
        entries.remove(path);
        Ok(())
    }

    fn rename(&self, old_path: &str, new_path: &str, no_replace: bool) -> Result<(), Errno> {
        let mut entries = self.entries.lock().unwrap();
        if !entries.contains_key(old_path) {
            return Err(Errno::ENOENT);
        }
        if no_replace && entries.contains_key(new_path) {
            return Err(Errno::EEXIST);
        }
        let node = entries.remove(old_path).unwrap();
        entries.insert(new_path.to_string(), node);
        Ok(())
    }

    fn utimens(&self, path: &str, mtime_millis: i64) -> Result<(), Errno> {
        match self.entries.lock().unwrap().get_mut(path) {
            Some(Node::File {
                mtime_millis: stored,
                ..
            }) => {
                *stored = mtime_millis;
                Ok(())
            }
            Some(Node::Directory) => Ok(()), // no mtime tracked for directories
            None => Err(Errno::ENOENT),
        }
    }

    fn write(&self, handle: Handle, offset: u64, data: &[u8]) -> Result<u32, Errno> {
        let path = self.path_for(handle)?;
        match self.entries.lock().unwrap().get_mut(&path) {
            Some(Node::File {
                content,
                mtime_millis,
            }) => {
                let start = offset as usize;
                if content.len() < start {
                    content.resize(start, 0);
                }
                let end = start + data.len();
                if content.len() < end {
                    content.resize(end, 0);
                }
                content[start..end].copy_from_slice(data);
                *mtime_millis = now_millis();
                Ok(data.len() as u32)
            }
            _ => Err(Errno::EIO),
        }
    }

    fn truncate(&self, path: &str, size: u64) -> Result<(), Errno> {
        match self.entries.lock().unwrap().get_mut(path) {
            Some(Node::File {
                content,
                mtime_millis,
            }) => {
                content.resize(size as usize, 0);
                *mtime_millis = now_millis();
                Ok(())
            }
            Some(Node::Directory) => Err(Errno::EISDIR),
            None => Err(Errno::ENOENT),
        }
    }
}

fn main() {
    let mountpoint = std::env::args().nth(1).expect("usage: memfs <mountpoint>");
    if let Err(err) = mountfs::mount(MemFs::default(), Path::new(&mountpoint), false) {
        eprintln!("mount failed: {err}");
        std::process::exit(1);
    }
}
