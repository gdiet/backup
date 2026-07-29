//! Read-only FUSE mount (`backup mount <mountpoint>`) - see
//! `docs/plans/implemented/04-fuse-mount-readonly.md` for the design.
//! `docs/plans/fuse-mount-readwrite.md` covers a future read-write phase,
//! not implemented here.
//!
//! Every callback is answerable with functions the other commands already
//! use (`db::find_tree_entry`/`get_tree_entry`, `db::list_children`,
//! `db::file_size`, `db::ordered_content_chunks`, and `chunk_store`'s
//! multi-part-aware chunk reader) - this module is almost entirely wiring
//! those up to `fuser`'s callback table, not new logic.

use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Mutex;
use std::time::{Duration, UNIX_EPOCH};

use clap::Args;
use fuser::{
    Config, Errno, FileAttr, FileHandle, FileType, Filesystem, FopenFlags, Generation, INodeNo,
    LockOwner, MountOption, OpenAccMode, OpenFlags, ReplyAttr, ReplyData, ReplyDirectory,
    ReplyEntry, ReplyOpen, ReplyStatfs, Request,
};
use rusqlite::Connection;
use store::{LongTermStore, ReadIntegrity};

use crate::chunk_store::read_chunk_bytes;

#[derive(Args)]
pub struct MountArgs {
    /// Directory to mount the repository's file tree at. Must already exist
    /// and be empty.
    mountpoint: PathBuf,
}

/// How long the kernel may cache directory-entry/attribute replies before
/// re-asking. Generous, since nothing else writes to the repository through
/// this read-only mount - a concurrent `store`/`del`/`reclaim-space` run
/// against the same repository while it's mounted can still make cached
/// entries briefly stale, a known, accepted limitation of this phase (no
/// cache-invalidation notifications are sent).
const ATTR_TTL: Duration = Duration::from_secs(1);

/// FUSE reserves inode `1` for the mount root; our tree root is
/// `tree_entries.id = 0`. A fixed `+1`/`-1` shift bridges the two id spaces,
/// applied at the boundary of every callback - nothing else in this module
/// needs to know about the offset.
fn to_fuse_ino(tree_id: i64) -> INodeNo {
    INodeNo((tree_id + 1) as u64)
}

fn to_tree_id(ino: INodeNo) -> i64 {
    ino.0 as i64 - 1
}

pub fn run_mount(repo: &Path, args: MountArgs) -> ExitCode {
    if !args.mountpoint.is_dir() {
        eprintln!(
            "error: mountpoint '{}' is not an existing directory",
            args.mountpoint.display()
        );
        return ExitCode::FAILURE;
    }
    match std::fs::read_dir(&args.mountpoint) {
        Ok(mut entries) => {
            if entries.next().is_some() {
                eprintln!(
                    "error: mountpoint '{}' is not empty",
                    args.mountpoint.display()
                );
                return ExitCode::FAILURE;
            }
        }
        Err(err) => {
            eprintln!(
                "error: failed to read mountpoint '{}': {err}",
                args.mountpoint.display()
            );
            return ExitCode::FAILURE;
        }
    }

    let (fs, config) = match build_filesystem(repo) {
        Ok(v) => v,
        Err(msg) => {
            eprintln!("error: {msg}");
            return ExitCode::FAILURE;
        }
    };

    println!(
        "mounted read-only at {} (unmount with `fusermount -u {}` or `umount {}`)",
        args.mountpoint.display(),
        args.mountpoint.display(),
        args.mountpoint.display()
    );
    match fuser::mount(fs, &args.mountpoint, &config) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("error: mount failed: {err}");
            ExitCode::FAILURE
        }
    }
}

/// Builds the [`DedupFs`] and its mount [`Config`] for `repo`, without
/// touching the mountpoint itself - split out from [`run_mount`] so tests can
/// drive [`fuser::spawn_mount`] directly instead of the blocking, process-
/// exit-coupled [`fuser::mount`] call above.
fn build_filesystem(repo: &Path) -> Result<(DedupFs, Config), String> {
    let repository = db::open_repository(repo)
        .map_err(|err| format!("failed to open repository at {}: {err}", repo.display()))?;
    let conn = repository
        .open_read_connection()
        .map_err(|err| format!("failed to open the metadata database: {err}"))?;
    let data_store = LongTermStore::new(repository.data_dir(), true);
    let fs = DedupFs {
        conn: Mutex::new(conn),
        data_store,
    };

    let mut config = Config::default();
    config.mount_options = vec![
        MountOption::RO,
        MountOption::FSName("backup-dedup".to_string()),
    ];
    Ok((fs, config))
}

struct DedupFs {
    conn: Mutex<Connection>,
    data_store: LongTermStore,
}

impl DedupFs {
    /// Builds a [`FileAttr`] for `entry`, attributing ownership to whoever
    /// made the request - there's no real multi-user ownership model here,
    /// this is purely cosmetic (without the `default_permissions` mount
    /// option, the kernel doesn't enforce these bits anyway).
    fn attr_for(
        &self,
        conn: &Connection,
        entry: &db::TreeEntryRow,
        uid: u32,
        gid: u32,
    ) -> FileAttr {
        let (kind, perm, size) = match entry.kind {
            db::EntryKind::File => (
                FileType::RegularFile,
                0o444,
                db::file_size(conn, entry).unwrap_or(0) as u64,
            ),
            db::EntryKind::Dir => (FileType::Directory, 0o555, 0),
        };
        let mtime = UNIX_EPOCH + Duration::from_millis(entry.time_millis.max(0) as u64);
        FileAttr {
            ino: to_fuse_ino(entry.id),
            size,
            blocks: size.div_ceil(512),
            atime: mtime,
            mtime,
            ctime: mtime,
            crtime: mtime,
            kind,
            perm,
            nlink: 1,
            uid,
            gid,
            rdev: 0,
            blksize: 512,
            flags: 0,
        }
    }
}

impl Filesystem for DedupFs {
    fn lookup(&self, req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let Some(name) = name.to_str() else {
            reply.error(Errno::ENOENT);
            return;
        };
        let conn = self.conn.lock().expect("db connection mutex poisoned");
        match db::find_tree_entry(&conn, to_tree_id(parent), name) {
            Ok(Some(entry)) => {
                let attr = self.attr_for(&conn, &entry, req.uid(), req.gid());
                reply.entry(&ATTR_TTL, &attr, Generation(0));
            }
            Ok(None) => reply.error(Errno::ENOENT),
            Err(_) => reply.error(Errno::EIO),
        }
    }

    fn getattr(&self, req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        let conn = self.conn.lock().expect("db connection mutex poisoned");
        match db::get_tree_entry(&conn, to_tree_id(ino)) {
            Ok(Some(entry)) => {
                let attr = self.attr_for(&conn, &entry, req.uid(), req.gid());
                reply.attr(&ATTR_TTL, &attr);
            }
            Ok(None) => reply.error(Errno::ENOENT),
            Err(_) => reply.error(Errno::EIO),
        }
    }

    fn readdir(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        let tree_id = to_tree_id(ino);
        let conn = self.conn.lock().expect("db connection mutex poisoned");
        let parent_tree_id: i64 = match conn.query_row(
            "SELECT parent_id FROM tree_entries WHERE id = ?1",
            [tree_id],
            |row| row.get(0),
        ) {
            Ok(id) => id,
            Err(_) => {
                reply.error(Errno::ENOENT);
                return;
            }
        };
        let children = match db::list_children(&conn, tree_id) {
            Ok(children) => children,
            Err(_) => {
                reply.error(Errno::EIO);
                return;
            }
        };

        let mut entries: Vec<(INodeNo, FileType, String)> = vec![
            (ino, FileType::Directory, ".".to_string()),
            (
                to_fuse_ino(parent_tree_id),
                FileType::Directory,
                "..".to_string(),
            ),
        ];
        entries.extend(children.into_iter().map(|child| {
            let kind = match child.kind {
                db::EntryKind::Dir => FileType::Directory,
                db::EntryKind::File => FileType::RegularFile,
            };
            (to_fuse_ino(child.id), kind, child.name)
        }));

        for (i, (ino, kind, name)) in entries.into_iter().enumerate().skip(offset as usize) {
            // Each entry's offset is where the *next* readdir call should
            // resume - the kernel echoes it back verbatim as `offset` above.
            if reply.add(ino, (i + 1) as u64, kind, name) {
                break; // reply buffer full; the kernel will call again with a higher offset
            }
        }
        reply.ok();
    }

    fn open(&self, _req: &Request, _ino: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        if flags.acc_mode() != OpenAccMode::O_RDONLY {
            reply.error(Errno::EROFS);
            return;
        }
        // Stateless: no per-handle mutable state exists in this read-only
        // phase, so the inode number itself doubles as the file handle,
        // mirroring Scala's simplification (its FUSE handle is the file's
        // own database row id).
        reply.opened(FileHandle(0), FopenFlags::empty());
    }

    fn read(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyData,
    ) {
        let conn = self.conn.lock().expect("db connection mutex poisoned");
        let entry = match db::get_tree_entry(&conn, to_tree_id(ino)) {
            Ok(Some(entry)) => entry,
            Ok(None) => {
                reply.error(Errno::ENOENT);
                return;
            }
            Err(_) => {
                reply.error(Errno::EIO);
                return;
            }
        };
        let Some(content_id) = entry.content_id else {
            reply.data(&[]);
            return;
        };
        let chunks = match db::ordered_content_chunks(&conn, content_id) {
            Ok(chunks) => chunks,
            Err(_) => {
                reply.error(Errno::EIO);
                return;
            }
        };

        // Walk chunk boundaries to find the requested [want_start, want_end)
        // slice, reading only the chunks that actually overlap it rather
        // than the whole file.
        let want_start = offset;
        let want_end = want_start.saturating_add(u64::from(size));
        let mut result = Vec::new();
        let mut pos: u64 = 0;
        for chunk in chunks {
            let chunk_len = chunk.length as u64;
            let chunk_start = pos;
            pos += chunk_len;
            if pos <= want_start || chunk_start >= want_end {
                continue;
            }
            let (bytes, integrity) =
                match read_chunk_bytes(&conn, &self.data_store, chunk.chunk_id, chunk_len) {
                    Ok(result) => result,
                    Err(_) => {
                        reply.error(Errno::EIO);
                        return;
                    }
                };
            if let ReadIntegrity::Incomplete { .. } = integrity {
                reply.error(Errno::EIO);
                return;
            }
            let local_start = want_start.saturating_sub(chunk_start).min(chunk_len);
            let local_end = want_end.saturating_sub(chunk_start).min(chunk_len);
            result.extend_from_slice(&bytes[local_start as usize..local_end as usize]);
            if pos >= want_end {
                break;
            }
        }
        reply.data(&result);
    }

    // release: the default implementation already replies `ok()` - there's
    // nothing to flush in a read-only mount.

    fn statfs(&self, _req: &Request, _ino: INodeNo, reply: ReplyStatfs) {
        // Approximate/unused values - Scala's own Linux FUSE implementation
        // is a no-op here too; not worth over-building for a read-only mount.
        reply.statfs(0, 0, 0, 0, 0, 512, 255, 0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn fuse_and_tree_inode_numbers_round_trip() {
        assert_eq!(to_fuse_ino(0), INodeNo::ROOT);
        assert_eq!(to_tree_id(to_fuse_ino(42)), 42);
    }

    fn init_repo() -> (tempfile::TempDir, PathBuf) {
        let temp_dir = tempfile::tempdir().unwrap();
        let repo_root = temp_dir.path().join("repo");
        db::init_repository(
            &repo_root,
            &db::RepositorySettings::new(12, db::Chunking::Cdc).unwrap(),
        )
        .unwrap();
        (temp_dir, repo_root)
    }

    /// Writes `bytes` to the store and records a matching chunk/content/file
    /// in the metadata database - the same minimal stand-in for a real
    /// `store` run other commands' tests use.
    fn seed_file(repo_root: &Path, parent_id: i64, name: &str, bytes: &[u8]) {
        let data_store = LongTermStore::new(repo_root.join("data"), false);
        let start: i64 = {
            let repository = db::open_repository(repo_root).unwrap();
            let conn = repository.open_read_connection().unwrap();
            conn.query_row(
                "SELECT COALESCE(MAX(stop), 0) FROM chunk_extents",
                (),
                |row| row.get(0),
            )
            .unwrap()
        };
        data_store.write(start as u64, bytes).unwrap();

        let mut hash = [0u8; 20];
        blake3::Hasher::new()
            .update(bytes)
            .finalize_xof()
            .fill(&mut hash);

        let repository = db::open_repository(repo_root).unwrap();
        let mut conn = repository.open_write_connection().unwrap();
        db::apply_backup_batch(
            &mut conn,
            &[db::FileBackupRecord {
                parent_id,
                name: name.to_string(),
                time_millis: 0,
                chunks: if bytes.is_empty() {
                    vec![]
                } else {
                    vec![db::ChunkRef::New {
                        length: bytes.len() as u64,
                        hash: hash.to_vec(),
                        extents: vec![(start as u64, start as u64 + bytes.len() as u64)],
                    }]
                },
                content_hash: hash.to_vec(),
            }],
        )
        .unwrap();
    }

    /// End-to-end mount/read/unmount test: seeds a real repository with a
    /// nested file, mounts it via [`fuser::spawn_mount`] (non-blocking,
    /// unmounts automatically when the returned session is dropped, unlike
    /// [`run_mount`]'s blocking [`fuser::mount`]), and reads it back through
    /// ordinary `std::fs` calls - exercising `lookup`/`getattr`/`readdir`/
    /// `open`/`read` together the way a real FUSE client would.
    #[test]
    fn mounts_and_serves_a_real_repository_read_only() {
        let (_temp_dir, repo_root) = init_repo();
        let repository = db::open_repository(&repo_root).unwrap();
        let conn = repository.open_write_connection().unwrap();
        let sub_id = db::insert_directory(&conn, 0, "sub", 0).unwrap();
        drop(conn);
        seed_file(&repo_root, 0, "top.txt", b"top level content");
        seed_file(&repo_root, sub_id, "a.txt", b"hello fuse");

        let (fs, config) = build_filesystem(&repo_root).unwrap();
        let mount_dir = tempfile::tempdir().unwrap();
        let session = fuser::spawn_mount(fs, mount_dir.path(), &config)
            .expect("mounting requires /dev/fuse access - skip/investigate if this fails in CI");

        let mut names: Vec<String> = std::fs::read_dir(mount_dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        assert_eq!(names, vec!["sub", "top.txt"]);

        assert_eq!(
            std::fs::read(mount_dir.path().join("top.txt")).unwrap(),
            b"top level content"
        );
        assert_eq!(
            std::fs::read(mount_dir.path().join("sub").join("a.txt")).unwrap(),
            b"hello fuse"
        );
        assert_eq!(
            std::fs::metadata(mount_dir.path().join("top.txt"))
                .unwrap()
                .len(),
            17
        );
        assert!(
            std::fs::metadata(mount_dir.path().join("sub"))
                .unwrap()
                .is_dir()
        );

        drop(session); // unmounts
    }
}
