//! Read-only FUSE mount (`backup mount <mountpoint>`) - see
//! `docs/plans/implemented/04-fuse-mount-readonly.md` for the original
//! design and `docs/plans/cross-platform-mount-crate.md` for why this now
//! goes through the platform-abstracted `mountfs` crate (real libfuse3 on
//! Linux, WinFSP planned for Windows) instead of `fuser`'s Linux-only,
//! low-level `/dev/fuse` protocol.
//! `docs/plans/fuse-mount-readwrite.md` covers a future read-write phase,
//! not implemented here.
//!
//! Every [`mountfs::MountFilesystem`] method is answerable with functions
//! the other commands already use (`db::resolve_path`, `db::list_children`,
//! `db::file_size`, `db::ordered_content_chunks`, and `chunk_store`'s
//! multi-part-aware chunk reader) - this module is almost entirely wiring
//! those up to `mountfs`'s trait, not new logic. `mountfs`'s API is
//! path-based (matching `db::resolve_path`), so unlike the old `fuser`
//! version there's no inode-number bookkeeping here at all.

use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Mutex;

use clap::Args;
use mountfs::{Attr, DirEntry, Errno, FileKind, Handle, MountFilesystem};
use rusqlite::Connection;
use store::{LongTermStore, ReadIntegrity};

use crate::chunk_store::read_chunk_bytes;

#[derive(Args)]
pub struct MountArgs {
    /// Directory to mount the repository's file tree at.
    ///
    /// On Linux, must already exist and be empty (FUSE mounts onto an
    /// existing mountpoint). On Windows, must *not* already exist (WinFSP
    /// creates it itself as part of mounting).
    mountpoint: PathBuf,

    /// Allow structural changes through the mount: `mkdir`/`rmdir`/
    /// `unlink`/`rename`/creating empty files/touching timestamps (see
    /// `docs/plans/fuse-mount-readwrite.md`, phase 2a - writing actual
    /// file *content* isn't implemented yet regardless of this flag).
    /// Off by default: a mount is a much larger blast radius for a mistake
    /// (an editor autosave, a stray `rm -rf`, a build tool scribbling into
    /// it) than `store`/`restore`. Do not run `store`/`del`/
    /// `reclaim-space` against the same repository while a read-write
    /// mount is active - both need the single write connection this holds
    /// for the mount's whole lifetime.
    #[arg(short = 'w', long)]
    read_write: bool,
}

/// FUSE (Linux) mounts onto an existing, empty directory; WinFSP
/// (Windows) creates the mountpoint itself and errors if it's already
/// there ("mount point in use") - opposite preconditions, so this can't be
/// one platform-independent check.
#[cfg(target_os = "windows")]
fn validate_mountpoint(mountpoint: &Path) -> Result<(), String> {
    if mountpoint.exists() {
        return Err(format!(
            "mountpoint '{}' already exists - WinFSP creates it itself, remove it first",
            mountpoint.display()
        ));
    }
    Ok(())
}

#[cfg(not(target_os = "windows"))]
fn validate_mountpoint(mountpoint: &Path) -> Result<(), String> {
    if !mountpoint.is_dir() {
        return Err(format!(
            "mountpoint '{}' is not an existing directory",
            mountpoint.display()
        ));
    }
    match std::fs::read_dir(mountpoint) {
        Ok(mut entries) => {
            if entries.next().is_some() {
                return Err(format!(
                    "mountpoint '{}' is not empty",
                    mountpoint.display()
                ));
            }
        }
        Err(err) => {
            return Err(format!(
                "failed to read mountpoint '{}': {err}",
                mountpoint.display()
            ));
        }
    }
    Ok(())
}

#[cfg(target_os = "windows")]
fn unmount_hint(mountpoint: &Path) -> String {
    let _ = mountpoint;
    "unmount by closing this process (Ctrl+C) or killing it".to_string()
}

#[cfg(not(target_os = "windows"))]
fn unmount_hint(mountpoint: &Path) -> String {
    format!(
        "unmount with `fusermount3 -u {}` or `umount {}`",
        mountpoint.display(),
        mountpoint.display()
    )
}

pub fn run_mount(repo: &Path, args: MountArgs) -> ExitCode {
    if let Err(msg) = validate_mountpoint(&args.mountpoint) {
        eprintln!("error: {msg}");
        return ExitCode::FAILURE;
    }

    let fs = match build_filesystem(repo) {
        Ok(fs) => fs,
        Err(msg) => {
            eprintln!("error: {msg}");
            return ExitCode::FAILURE;
        }
    };

    println!(
        "mounted {} at {} ({})",
        if args.read_write {
            "read-write"
        } else {
            "read-only"
        },
        args.mountpoint.display(),
        unmount_hint(&args.mountpoint)
    );
    match mountfs::mount(fs, &args.mountpoint, !args.read_write) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("error: mount failed: {err}");
            ExitCode::FAILURE
        }
    }
}

/// Builds the [`DedupFs`] for `repo`, without touching the mountpoint
/// itself - split out from [`run_mount`] so tests can drive [`mountfs::mount`]
/// directly (in a background thread - it blocks until unmounted) instead of
/// going through the process-exit-coupled [`run_mount`] above.
fn build_filesystem(repo: &Path) -> Result<DedupFs, String> {
    let repository = db::open_repository(repo)
        .map_err(|err| format!("failed to open repository at {}: {err}", repo.display()))?;
    let conn = repository
        .open_read_connection()
        .map_err(|err| format!("failed to open the metadata database: {err}"))?;
    // Opened unconditionally (even for a read-only mount) - cheap, and
    // keeps DedupFs's shape identical regardless of --read-write; the
    // read_only flag passed to mountfs::mount is what actually keeps the
    // kernel/WinFSP from ever calling a write operation in that case, not
    // this connection's mere existence.
    let write_conn = repository
        .open_write_connection()
        .map_err(|err| format!("failed to open the metadata database for writing: {err}"))?;
    let data_store = LongTermStore::new(repository.data_dir(), true);
    Ok(DedupFs {
        conn: Mutex::new(conn),
        write_conn: Mutex::new(write_conn),
        data_store,
    })
}

struct DedupFs {
    conn: Mutex<Connection>,
    /// Held for the mount's whole lifetime - see `MountArgs::read_write`'s
    /// doc comment on why `store`/`del`/`reclaim-space` mustn't run
    /// concurrently against the same repository while this is open.
    write_conn: Mutex<Connection>,
    data_store: LongTermStore,
}

/// Splits an absolute mount path into its parent path and final component -
/// `db::resolve_path`'s own path-splitting rules apply to the parent half
/// (a leading `/` and empty components are both fine), so no special-casing
/// is needed for a root-level entry (`"/name"` splits to `("", "name")`,
/// and `db::resolve_path(conn, "")` is documented to resolve to the root).
fn split_parent(path: &str) -> (&str, &str) {
    path.rsplit_once('/').unwrap_or(("", path))
}

fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

impl MountFilesystem for DedupFs {
    fn getattr(&self, path: &str) -> Result<Attr, Errno> {
        let conn = self.conn.lock().expect("db connection mutex poisoned");
        let entry = db::resolve_path(&conn, path)
            .map_err(|_| Errno::EIO)?
            .ok_or(Errno::ENOENT)?;
        let (kind, size) = match entry.kind {
            db::EntryKind::Dir => (FileKind::Directory, 0),
            db::EntryKind::File => (
                FileKind::File,
                db::file_size(&conn, &entry).map_err(|_| Errno::EIO)? as u64,
            ),
        };
        Ok(Attr {
            kind,
            size,
            mtime_millis: entry.time_millis,
        })
    }

    fn readdir(&self, path: &str) -> Result<Vec<DirEntry>, Errno> {
        let conn = self.conn.lock().expect("db connection mutex poisoned");
        let dir = db::resolve_path(&conn, path)
            .map_err(|_| Errno::EIO)?
            .ok_or(Errno::ENOENT)?;
        let children = db::list_children(&conn, dir.id).map_err(|_| Errno::EIO)?;
        Ok(children
            .into_iter()
            .map(|child| DirEntry {
                name: child.name,
                kind: match child.kind {
                    db::EntryKind::Dir => FileKind::Directory,
                    db::EntryKind::File => FileKind::File,
                },
            })
            .collect())
    }

    fn open(&self, path: &str, write_intent: bool) -> Result<Handle, Errno> {
        if write_intent {
            return Err(Errno::EROFS);
        }
        let conn = self.conn.lock().expect("db connection mutex poisoned");
        let entry = db::resolve_path(&conn, path)
            .map_err(|_| Errno::EIO)?
            .ok_or(Errno::ENOENT)?;
        match entry.kind {
            db::EntryKind::File => Ok(Handle(entry.id as u64)),
            db::EntryKind::Dir => Err(Errno::EISDIR),
        }
    }

    fn read(&self, handle: Handle, offset: u64, size: u32) -> Result<Vec<u8>, Errno> {
        let conn = self.conn.lock().expect("db connection mutex poisoned");
        let entry = db::get_tree_entry(&conn, handle.0 as i64)
            .map_err(|_| Errno::EIO)?
            .ok_or(Errno::ENOENT)?;
        let Some(content_id) = entry.content_id else {
            return Ok(Vec::new());
        };
        let chunks = db::ordered_content_chunks(&conn, content_id).map_err(|_| Errno::EIO)?;

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
                read_chunk_bytes(&conn, &self.data_store, chunk.chunk_id, chunk_len)
                    .map_err(|_| Errno::EIO)?;
            if let ReadIntegrity::Incomplete { .. } = integrity {
                return Err(Errno::EIO);
            }
            let local_start = want_start.saturating_sub(chunk_start).min(chunk_len);
            let local_end = want_end.saturating_sub(chunk_start).min(chunk_len);
            result.extend_from_slice(&bytes[local_start as usize..local_end as usize]);
            if pos >= want_end {
                break;
            }
        }
        Ok(result)
    }

    fn release(&self, _handle: Handle) {
        // Stateless: nothing was allocated in `open` beyond the tree id
        // already carried in the handle itself.
    }

    fn statfs(&self) -> Result<mountfs::StatfsInfo, Errno> {
        // Approximate/unused values - Scala's own Linux FUSE implementation
        // is a no-op here too; not worth over-building for a read-only mount.
        Ok(mountfs::StatfsInfo {
            block_size: 512,
            max_name_length: 255,
            ..Default::default()
        })
    }

    fn mkdir(&self, path: &str) -> Result<(), Errno> {
        let (parent_path, name) = split_parent(path);
        let conn = self
            .write_conn
            .lock()
            .expect("write connection mutex poisoned");
        let parent = db::resolve_path(&conn, parent_path)
            .map_err(|_| Errno::EIO)?
            .ok_or(Errno::ENOENT)?;
        // insert_directory is idempotent (get-or-create) - fine for `store`,
        // but a real mkdir(2) must fail on an existing name (file or
        // directory alike), so the existence check happens here first.
        if db::find_tree_entry(&conn, parent.id, name)
            .map_err(|_| Errno::EIO)?
            .is_some()
        {
            return Err(Errno::EEXIST);
        }
        db::insert_directory(&conn, parent.id, name, now_millis()).map_err(|_| Errno::EIO)?;
        Ok(())
    }

    fn create(&self, path: &str) -> Result<Handle, Errno> {
        let (parent_path, name) = split_parent(path);
        let mut conn = self
            .write_conn
            .lock()
            .expect("write connection mutex poisoned");
        let parent = db::resolve_path(&conn, parent_path)
            .map_err(|_| Errno::EIO)?
            .ok_or(Errno::ENOENT)?;
        if db::find_tree_entry(&conn, parent.id, name)
            .map_err(|_| Errno::EIO)?
            .is_some()
        {
            return Err(Errno::EEXIST);
        }
        // No content/write-cache support yet (phase 2b) - this always
        // creates an empty file, the same shape `open`/`read` already
        // treat a zero-length file as (`content_id IS NULL`).
        db::apply_backup_batch(
            &mut conn,
            &[db::FileBackupRecord {
                parent_id: parent.id,
                name: name.to_string(),
                time_millis: now_millis(),
                chunks: Vec::new(),
                content_hash: Vec::new(),
            }],
        )
        .map_err(|_| Errno::EIO)?;
        let entry = db::find_tree_entry(&conn, parent.id, name)
            .map_err(|_| Errno::EIO)?
            .ok_or(Errno::EIO)?;
        Ok(Handle(entry.id as u64))
    }

    fn unlink(&self, path: &str) -> Result<(), Errno> {
        let conn = self
            .write_conn
            .lock()
            .expect("write connection mutex poisoned");
        let entry = db::resolve_path(&conn, path)
            .map_err(|_| Errno::EIO)?
            .ok_or(Errno::ENOENT)?;
        if entry.kind != db::EntryKind::File {
            return Err(Errno::EISDIR);
        }
        db::soft_delete(&conn, entry.id, now_millis()).map_err(|_| Errno::EIO)?;
        Ok(())
    }

    fn rmdir(&self, path: &str) -> Result<(), Errno> {
        let conn = self
            .write_conn
            .lock()
            .expect("write connection mutex poisoned");
        let entry = db::resolve_path(&conn, path)
            .map_err(|_| Errno::EIO)?
            .ok_or(Errno::ENOENT)?;
        if entry.kind != db::EntryKind::Dir {
            return Err(Errno::ENOTDIR);
        }
        if !db::list_children(&conn, entry.id)
            .map_err(|_| Errno::EIO)?
            .is_empty()
        {
            return Err(Errno::ENOTEMPTY);
        }
        db::soft_delete(&conn, entry.id, now_millis()).map_err(|_| Errno::EIO)?;
        Ok(())
    }

    fn rename(&self, old_path: &str, new_path: &str) -> Result<(), Errno> {
        let conn = self
            .write_conn
            .lock()
            .expect("write connection mutex poisoned");
        let entry = db::resolve_path(&conn, old_path)
            .map_err(|_| Errno::EIO)?
            .ok_or(Errno::ENOENT)?;
        let (new_parent_path, new_name) = split_parent(new_path);
        let new_parent = db::resolve_path(&conn, new_parent_path)
            .map_err(|_| Errno::EIO)?
            .ok_or(Errno::ENOENT)?;
        db::rename_entry(&conn, entry.id, new_parent.id, new_name).map_err(|err| match err {
            db::Error::AlreadyExists { .. } => Errno::EEXIST,
            _ => Errno::EIO,
        })
    }

    fn utimens(&self, path: &str, mtime_millis: i64) -> Result<(), Errno> {
        let conn = self
            .write_conn
            .lock()
            .expect("write connection mutex poisoned");
        let entry = db::resolve_path(&conn, path)
            .map_err(|_| Errno::EIO)?
            .ok_or(Errno::ENOENT)?;
        db::touch_mtime(&conn, entry.id, mtime_millis).map_err(|_| Errno::EIO)?;
        Ok(())
    }
}

// This test's design (in-process mount thread, `fusermount3 -u` to
// unmount, a pre-existing empty directory as the mountpoint) is Linux/FUSE-
// specific throughout - see `validate_mountpoint`'s doc comment for why
// Windows needs the opposite mountpoint precondition, and
// `mountfs::windows`'s doc comment for why an in-process `fuse_exit`-style
// unmount doesn't work there yet. `cli/tests/windows_mount.rs` covers the
// equivalent ground on Windows instead, via a child process (the real
// `backup` binary) killed from the outside.
#[cfg(all(test, not(target_os = "windows")))]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::time::{Duration, Instant};

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
    /// nested file, mounts it via [`mountfs::mount`] in a background thread
    /// (it blocks until unmounted, unlike the old `fuser::spawn_mount` this
    /// replaces), and reads it back through ordinary `std::fs` calls -
    /// exercising every `MountFilesystem` method together the way a real
    /// FUSE client would.
    #[test]
    fn mounts_and_serves_a_real_repository_read_only() {
        let (_temp_dir, repo_root) = init_repo();
        let repository = db::open_repository(&repo_root).unwrap();
        let conn = repository.open_write_connection().unwrap();
        let sub_id = db::insert_directory(&conn, 0, "sub", 0).unwrap();
        drop(conn);
        seed_file(&repo_root, 0, "top.txt", b"top level content");
        seed_file(&repo_root, sub_id, "a.txt", b"hello fuse");

        let fs = build_filesystem(&repo_root).unwrap();
        let mount_dir = tempfile::tempdir().unwrap();
        let mount_path = mount_dir.path().to_path_buf();
        let handle = {
            let mount_path = mount_path.clone();
            std::thread::spawn(move || mountfs::mount(fs, &mount_path, true))
        };

        // The mountpoint exists (and reads as empty) before the mount is
        // live, so "readdir succeeds" alone isn't a valid readiness signal
        // - wait for it to actually start reporting our entries.
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut names: Vec<String>;
        loop {
            names = std::fs::read_dir(&mount_path)
                .map(|entries| {
                    entries
                        .filter_map(|e| e.ok())
                        .map(|e| e.file_name().to_string_lossy().into_owned())
                        .collect()
                })
                .unwrap_or_default();
            if !names.is_empty() {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "mount did not become ready within 5s \
                 (requires /dev/fuse access - investigate if this fails in CI)"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
        names.sort();
        assert_eq!(names, vec!["sub".to_string(), "top.txt".to_string()]);

        assert_eq!(
            std::fs::read(mount_path.join("top.txt")).unwrap(),
            b"top level content"
        );
        assert_eq!(
            std::fs::read(mount_path.join("sub").join("a.txt")).unwrap(),
            b"hello fuse"
        );
        assert_eq!(
            std::fs::metadata(mount_path.join("top.txt")).unwrap().len(),
            17
        );
        assert!(std::fs::metadata(mount_path.join("sub")).unwrap().is_dir());

        let status = std::process::Command::new("fusermount3")
            .arg("-u")
            .arg(&mount_path)
            .status()
            .expect("failed to run fusermount3 -u");
        assert!(status.success(), "fusermount3 -u failed: {status}");

        handle
            .join()
            .expect("mount thread panicked")
            .expect("mount() returned an error");
    }

    /// End-to-end phase 2a (structural read-write) test: mounts
    /// `--read-write`, and exercises `mkdir`/`create`-empty-file/
    /// `utimens`/`rename`/`rmdir`/`unlink` through ordinary `std::fs`
    /// calls - not `write`/content-bearing files yet (phase 2b, not
    /// implemented).
    #[test]
    fn mounts_read_write_and_supports_structural_changes() {
        let (_temp_dir, repo_root) = init_repo();
        // A marker only the mounted filesystem (not the plain, pre-mount
        // host directory) would ever report - `create_dir`/similar probes
        // against the mountpoint itself trivially "succeed" even before
        // the mount is live (they just create a real directory on the
        // host), so readiness has to be detected by content that can only
        // come from the mount, same as the read-only test above.
        {
            let repository = db::open_repository(&repo_root).unwrap();
            let conn = repository.open_write_connection().unwrap();
            db::insert_directory(&conn, 0, "marker", 0).unwrap();
        }

        let fs = build_filesystem(&repo_root).unwrap();
        let mount_dir = tempfile::tempdir().unwrap();
        let mount_path = mount_dir.path().to_path_buf();
        let handle = {
            let mount_path = mount_path.clone();
            std::thread::spawn(move || mountfs::mount(fs, &mount_path, false))
        };

        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let names: Vec<String> = std::fs::read_dir(&mount_path)
                .map(|entries| {
                    entries
                        .filter_map(|e| e.ok())
                        .map(|e| e.file_name().to_string_lossy().into_owned())
                        .collect()
                })
                .unwrap_or_default();
            if !names.is_empty() {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "mount did not become ready within 5s"
            );
            std::thread::sleep(Duration::from_millis(50));
        }

        // mkdir + create (empty file).
        std::fs::create_dir(mount_path.join("sub")).unwrap();
        assert!(std::fs::metadata(mount_path.join("sub")).unwrap().is_dir());
        std::fs::write(mount_path.join("sub").join("empty.txt"), b"").unwrap();
        assert_eq!(
            std::fs::metadata(mount_path.join("sub").join("empty.txt"))
                .unwrap()
                .len(),
            0
        );

        // mkdir on an existing name fails.
        assert!(std::fs::create_dir(mount_path.join("sub")).is_err());

        // utimens.
        let new_mtime = std::time::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        std::fs::File::open(mount_path.join("sub").join("empty.txt"))
            .unwrap()
            .set_times(std::fs::FileTimes::new().set_modified(new_mtime))
            .unwrap();
        assert_eq!(
            std::fs::metadata(mount_path.join("sub").join("empty.txt"))
                .unwrap()
                .modified()
                .unwrap(),
            new_mtime
        );

        // rename.
        std::fs::rename(
            mount_path.join("sub").join("empty.txt"),
            mount_path.join("sub").join("renamed.txt"),
        )
        .unwrap();
        assert!(std::fs::metadata(mount_path.join("sub").join("empty.txt")).is_err());
        assert!(
            std::fs::metadata(mount_path.join("sub").join("renamed.txt"))
                .unwrap()
                .is_file()
        );

        // rmdir on a non-empty directory fails; unlink then rmdir succeeds.
        assert!(std::fs::remove_dir(mount_path.join("sub")).is_err());
        std::fs::remove_file(mount_path.join("sub").join("renamed.txt")).unwrap();
        std::fs::remove_dir(mount_path.join("sub")).unwrap();
        assert!(std::fs::metadata(mount_path.join("sub")).is_err());

        let status = std::process::Command::new("fusermount3")
            .arg("-u")
            .arg(&mount_path)
            .status()
            .expect("failed to run fusermount3 -u");
        assert!(status.success(), "fusermount3 -u failed: {status}");

        handle
            .join()
            .expect("mount thread panicked")
            .expect("mount() returned an error");
    }
}
