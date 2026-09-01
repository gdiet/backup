//! `MountFilesystem` backed by a real, open `db::Repository` - REQ-MOUNT-001/002/003/009.
//! Read-only operations, directory structure, and content writes
//! (`create`/`write`/`truncate`/`unlink`, DESIGN-MOUNT-006/009/010/012/013/015 in
//! `docs/design/mount-write-path.md`) are all wired in: a write-intent open/create registers with
//! [`crate::pending_files::PendingFiles`], `write`/`truncate` land in its write-cache chain, and
//! `release` hands a fully-released generation off to [`crate::settle_pool::JobPool`]'s background
//! settle job - never blocking the releasing call itself (DESIGN-MOUNT-006). A job that fails is
//! recorded in [`crate::failure_log::FailureLog`], degrading the session to read-only on a
//! systemic failure (DESIGN-MOUNT-009).

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use mountfs::{Attr, DirEntry, Errno, FileKind, Handle, MountFilesystem, StatfsInfo};

use crate::failure_log::{Failure, FailureLog};
use crate::pending_files::{NewGeneration, PendingFiles};
use crate::settle_pool::{JobPool, SettleJob};
use crate::write_cache::MemoryBudget;

pub struct DedupFs {
    repo: Arc<db::Repository>,
    store: Arc<store::ByteStore>,
    read_write: bool,
    cdc_target_size_bits: Option<u32>,
    pending: PendingFiles,
    pool: JobPool,
    budget: Arc<MemoryBudget>,
    temp_dir: PathBuf,
    /// `None` for a read-only mount, which never submits a settle job that could produce a
    /// failure to log (DESIGN-MOUNT-009) in the first place.
    failure_log: Option<Arc<FailureLog>>,
}

impl DedupFs {
    /// `repo_root` is only needed to open DESIGN-MOUNT-009's failure log alongside the metadata
    /// database (`db::meta_dir`) - `repo`/`store` are otherwise already fully open.
    pub fn new(
        repo: db::Repository,
        store: store::ByteStore,
        read_write: bool,
        repo_root: &Path,
    ) -> io::Result<Self> {
        let cdc_target_size_bits = repo.settings().cdc_target_size_bits();
        let repo = Arc::new(repo);
        let store = Arc::new(store);
        let failure_log = if read_write {
            Some(Arc::new(FailureLog::open(&db::meta_dir(repo_root))?))
        } else {
            None
        };
        let worker_count = std::thread::available_parallelism()
            .map(std::num::NonZero::get)
            .unwrap_or(1);
        let failure_log_for_pool = failure_log.clone();
        let pool = JobPool::new(
            worker_count,
            Arc::clone(&repo),
            Arc::clone(&store),
            cdc_target_size_bits,
            move |job: &SettleJob, err| {
                if let Some(log) = &failure_log_for_pool {
                    log.record(Failure {
                        parent_id: job.parent_id,
                        name: &job.name,
                        time_millis: now_millis(),
                        systemic: err.is_systemic(),
                        message: err.to_string(),
                    });
                }
            },
        );
        Ok(Self {
            repo,
            store,
            read_write,
            cdc_target_size_bits,
            pending: PendingFiles::new(),
            pool,
            budget: Arc::new(MemoryBudget::default()),
            temp_dir: std::env::temp_dir(),
            failure_log,
        })
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
        // Never actually reaches here: mountfs's own `!read_write` guard already refuses every
        // structural/write call before `DedupFs` ever calls into `Repository` - but a precise
        // mapping (rather than lumping it into the generic EIO catch-all below) documents intent
        // for a `db::Error` matched exhaustively.
        db::Error::ReadOnlyRepository => Errno::EROFS,
        db::Error::RepositoryAlreadyExists(_)
        | db::Error::TargetNotEmpty(_)
        | db::Error::NoRepositoryHere(_)
        | db::Error::SchemaNeedsMigration(_)
        | db::Error::Poisoned
        | db::Error::WalUnavailable(_)
        // Never actually reaches here: `mount::try_run` opens the repository (read-only or
        // read-write) and acquires the write lock once, before a `DedupFs` exists at all - these
        // arms exist only because `db::Error` is matched exhaustively.
        | db::Error::AlreadyLocked(_)
        | db::Error::LockUnavailable { .. }
        | db::Error::LockFileInaccessible { .. }
        | db::Error::ConnectionUnreliable(_)
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

    /// DESIGN-MOUNT-009: refuses a new content write once a systemic background settle failure
    /// has degraded this session to read-only - checked by a write-intent `open`/`create`, a bare
    /// `truncate`, and `write` itself (for a handle that was already open before the session
    /// degraded). Directory structure operations (`mkdir`/`rmdir`/`rename`/`utimens`) and
    /// `unlink` are unaffected: none of them need `crates/store` space, so none of them are
    /// doomed to repeat whatever systemic cause (e.g. storage full) triggered the degradation.
    fn require_not_degraded(&self) -> Result<(), Errno> {
        if self
            .failure_log
            .as_ref()
            .is_some_and(|log| log.is_degraded())
        {
            Err(Errno::EROFS)
        } else {
            Ok(())
        }
    }

    /// `file_id`'s durably committed content, as [`NewGeneration`]'s `base_content_id`/
    /// `base_size` need it - skipped (and left as the harmless `(None, 0)`, since unused) when a
    /// writable generation already exists, so a tight sequence of `write` calls on the same
    /// handle only ever pays for this lookup once.
    fn base_for_write(&self, file_id: i64) -> Result<(Option<i64>, u64), Errno> {
        if self.pending.has_writable(file_id) {
            return Ok((None, 0));
        }
        let entry = self.repo.entry_by_id(file_id).map_err(to_errno)?;
        Ok(entry.map_or((None, 0), |entry| (entry.content_id, entry.size)))
    }

    fn new_generation<'a>(
        &'a self,
        base_content_id: Option<i64>,
        base_size: u64,
    ) -> NewGeneration<'a> {
        NewGeneration {
            budget: &self.budget,
            temp_dir: &self.temp_dir,
            base_content_id,
            base_size,
        }
    }

    /// Releases one write-intent handle on `file_id` and, if that leaves behind a generation
    /// ready to settle, hands it to the background job pool - the common tail of `release` and a
    /// standalone `truncate` (see its own doc comment for why it also needs this).
    fn release_and_maybe_submit(&self, file_id: i64) {
        let Some(generation) = self.pending.release(file_id) else {
            return;
        };
        // The entry may have been renamed (settles under its current location) or unlinked
        // (nothing to settle under anymore - see DESIGN-MOUNT-015's "Known limitation" for the
        // narrower race that remains once a job is already queued or running) since this
        // generation was created.
        if let Ok(Some((parent_id, name))) = self.repo.parent_and_name(file_id) {
            self.pool.submit(SettleJob {
                parent_id,
                name,
                time_millis: now_millis(),
                generation,
            });
        }
    }
}

impl MountFilesystem for DedupFs {
    fn getattr(&self, path: &str) -> Result<Attr, Errno> {
        let entry = self.resolve_required(path)?;
        let size = if entry.kind == db::EntryKind::File {
            self.pending.current_size(entry.id).unwrap_or(entry.size)
        } else {
            entry.size
        };
        Ok(Attr {
            kind: kind_to_mountfs(entry.kind),
            size,
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

    fn open(&self, path: &str, write_intent: bool) -> Result<Handle, Errno> {
        let entry = self.resolve_required(path)?;
        if entry.kind == db::EntryKind::Dir {
            return Err(Errno::EISDIR);
        }
        if write_intent {
            self.require_read_write()?;
            self.require_not_degraded()?;
        }
        // Every open counts toward the same handle count, read or write intent alike - a
        // lingering reader delays a written generation's hand-off to the settle pool, which only
        // costs latency, not correctness (DESIGN-MOUNT-007 keeps its content visible regardless).
        self.pending.open(entry.id);
        Ok(Handle(entry.id as u64))
    }

    fn read(&self, handle: Handle, offset: u64, size: u32) -> Result<Vec<u8>, Errno> {
        let file_id = handle.0 as i64;
        let repo = self.repo.as_ref();
        let store = self.store.as_ref();
        let resolve_content = |content_id: i64, position: u64, len: u32| {
            crate::content_reader::read_content(repo, store, content_id, position, len)
                .map_err(|errno| io::Error::from_raw_os_error(errno.0))
        };
        if let Some(result) = self.pending.read(file_id, offset, size, &resolve_content) {
            return result.map_err(|_| Errno::EIO);
        }
        let entry = self
            .repo
            .entry_by_id(file_id)
            .map_err(to_errno)?
            .ok_or(Errno::EIO)?;
        let content_id = entry.content_id.expect(
            "kind=File entries always have a content_id (chk_tree_entries_kind_content_id)",
        );
        crate::content_reader::read_content(&self.repo, &self.store, content_id, offset, size)
    }

    fn release(&self, handle: Handle) {
        self.release_and_maybe_submit(handle.0 as i64);
    }

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

    fn create(&self, path: &str) -> Result<Handle, Errno> {
        self.require_read_write()?;
        self.require_not_degraded()?;
        let (parent_path, name) = split_path(path)?;
        let parent = self.resolve_required(parent_path)?;
        // DESIGN-MOUNT-015: settles the canonical empty content immediately, so the new file has
        // a real tree_entries.id (and is visible to getattr/readdir/a second open) from the
        // start - no separate in-memory bookkeeping needed for "not yet in the database" at all.
        let empty_content_id = crate::settle::settle(
            &self.repo,
            &self.store,
            self.cdc_target_size_bits,
            0,
            |_, _| Ok(Vec::new()),
        )
        .map_err(|_| Errno::EIO)?;
        let id = self
            .repo
            .settle_file(parent.id, name, now_millis(), empty_content_id)
            .map_err(to_errno)?;
        // DESIGN-MOUNT-016: marks this row eligible for collapsing (hard delete instead of
        // history) once its first real write settles, still untouched.
        self.pending.open_freshly_created(id);
        Ok(Handle(id as u64))
    }

    fn unlink(&self, path: &str) -> Result<(), Errno> {
        self.require_read_write()?;
        let entry = self.resolve_required(path)?;
        if entry.kind != db::EntryKind::File {
            return Err(Errno::EISDIR);
        }
        self.repo
            .unlink_file(entry.id, now_millis())
            .map_err(to_errno)
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

    fn write(&self, handle: Handle, offset: u64, data: &[u8]) -> Result<u32, Errno> {
        self.require_not_degraded()?;
        let file_id = handle.0 as i64;
        let (base_content_id, base_size) = self.base_for_write(file_id)?;
        self.pending
            .write(
                file_id,
                offset,
                data,
                self.new_generation(base_content_id, base_size),
            )
            .map_err(|_| Errno::EIO)?;
        // DESIGN-MOUNT-006's backpressure delay - see crate::backpressure's own doc comment.
        std::thread::sleep(crate::backpressure::write_backpressure_delay(
            self.pool.backlog_spilled_bytes(),
            data.len(),
        ));
        Ok(data.len() as u32)
    }

    fn truncate(&self, path: &str, size: u64) -> Result<(), Errno> {
        self.require_read_write()?;
        self.require_not_degraded()?;
        let entry = self.resolve_required(path)?;
        if entry.kind != db::EntryKind::File {
            return Err(Errno::EISDIR);
        }
        let (base_content_id, base_size) = self.base_for_write(entry.id)?;
        // A bare `truncate(path, ...)` (real POSIX allows one with no open handle at all) still
        // needs to flow through the same open -> write-cache -> release -> settle pipeline as an
        // ordinary write - bracketing it in its own open/release pair does exactly that. If a
        // handle is already open on this file, this only bumps and un-bumps the same count
        // around it, never reaching zero and so never prematurely handing off a generation still
        // being actively written through that other handle.
        self.pending.open(entry.id);
        self.pending.truncate(
            entry.id,
            size,
            self.new_generation(base_content_id, base_size),
        );
        self.release_and_maybe_submit(entry.id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::{Duration, Instant};

    /// `fs` and `verify_repo`/`verify_store` point at the same repository, opened separately -
    /// `fs` owns the connection actually driving the mount, `verify_repo`/`verify_store` let a
    /// test inspect what a background settle job eventually commits, which `release`/`truncate`
    /// deliberately never wait for (DESIGN-MOUNT-006).
    fn setup(read_write: bool) -> (DedupFs, db::Repository, store::ByteStore, tempfile::TempDir) {
        let repo_dir = tempfile::tempdir().unwrap();
        let repo_root = repo_dir.path().join("repo");
        db::init_repository(
            &repo_root,
            db::RepositorySettings::new(Some(12), 1_700_000_000_000),
        )
        .unwrap();
        let fs_repo = db::open_repository(&repo_root).unwrap();
        let verify_repo = db::open_repository(&repo_root).unwrap();
        let verify_store = store::ByteStore::new(db::data_dir(&repo_root), true);
        let fs_store = store::ByteStore::new(db::data_dir(&repo_root), !read_write);
        let fs = DedupFs::new(fs_repo, fs_store, read_write, &repo_root).unwrap();
        (fs, verify_repo, verify_store, repo_dir)
    }

    /// Polls (bounded) until `verify_repo` sees a live entry at `path` sized `expected_size` -
    /// there is no synchronous "flush" to wait on directly, since a non-blocking `release()` is
    /// DESIGN-MOUNT-006's whole point.
    fn wait_for_settled(verify_repo: &db::Repository, path: &str, expected_size: u64) -> db::Entry {
        for _ in 0..500 {
            if let Some(entry) = verify_repo.resolve_path(path).unwrap()
                && entry.size == expected_size
            {
                return entry;
            }
            thread::sleep(Duration::from_millis(10));
        }
        panic!("{path} did not settle to size {expected_size} within the deadline");
    }

    #[test]
    fn create_then_release_settles_an_empty_file() {
        let (fs, verify_repo, _store, _dir) = setup(true);
        let handle = fs.create("/a.txt").unwrap();
        fs.release(handle);
        let entry = wait_for_settled(&verify_repo, "/a.txt", 0);
        assert_eq!(entry.kind, db::EntryKind::File);
    }

    #[test]
    fn write_then_release_settles_the_written_content() {
        let (fs, verify_repo, verify_store, _dir) = setup(true);
        let handle = fs.create("/a.txt").unwrap();
        assert_eq!(fs.write(handle, 0, b"hello world").unwrap(), 11);
        fs.release(handle);

        let entry = wait_for_settled(&verify_repo, "/a.txt", 11);
        let content_id = entry.content_id.unwrap();
        let data =
            crate::content_reader::read_content(&verify_repo, &verify_store, content_id, 0, 11)
                .unwrap();
        assert_eq!(data, b"hello world");
    }

    #[test]
    fn write_consults_the_pool_and_sleeps_once_backlog_is_present() {
        let (mut fs, _verify_repo, _store, _dir) = setup(true);
        // A tiny budget makes the write below spill immediately.
        fs.budget = Arc::new(MemoryBudget::new(1));

        // Release a real, spilled generation - DESIGN-MOUNT-013's hand-off submits a SettleJob
        // carrying its ~4 MiB of spilled_bytes to fs.pool. A single job's backlog contribution
        // only clears once `run_job` finishes entirely (settle_pool.rs's worker_loop), so it stays
        // fully present, not partially drained, for as long as this one job is still in flight.
        let spilling = fs.create("/spills.txt").unwrap();
        fs.write(spilling, 0, &vec![0xABu8; 4 * 1024 * 1024])
            .unwrap();
        fs.release(spilling);

        // Timed immediately after - the settle job above is almost certainly still running, so
        // fs.pool.backlog_spilled_bytes() (the same signal write() itself consults) should still
        // be near its full ~4 MiB, giving this write its own 4 MiB call a real, measurable delay
        // (~22 ms at this crate's SLOPE_DIVISOR) well above ordinary scheduling noise.
        let other = fs.create("/other.txt").unwrap();
        let start = Instant::now();
        fs.write(other, 0, &vec![0u8; 4 * 1024 * 1024]).unwrap();
        let elapsed = start.elapsed();
        fs.release(other);

        assert!(
            elapsed >= Duration::from_millis(5),
            "write() did not add a backlog-driven delay - elapsed only {elapsed:?}"
        );
    }

    #[test]
    fn read_before_release_sees_the_in_progress_write() {
        let (fs, _verify_repo, _store, _dir) = setup(true);
        let handle = fs.create("/a.txt").unwrap();
        fs.write(handle, 0, b"hello world").unwrap();
        let data = fs.read(handle, 0, 11).unwrap();
        assert_eq!(data, b"hello world");
        fs.release(handle);
    }

    #[test]
    fn getattr_reflects_an_in_progress_write_before_release() {
        let (fs, _verify_repo, _store, _dir) = setup(true);
        let handle = fs.create("/a.txt").unwrap();
        fs.write(handle, 0, b"hello world").unwrap();
        assert_eq!(fs.getattr("/a.txt").unwrap().size, 11);
        fs.release(handle);
    }

    #[test]
    fn bare_truncate_without_an_open_handle_still_settles() {
        let (fs, verify_repo, _store, _dir) = setup(true);
        let handle = fs.create("/a.txt").unwrap();
        fs.release(handle);
        wait_for_settled(&verify_repo, "/a.txt", 0);

        fs.truncate("/a.txt", 5).unwrap();
        wait_for_settled(&verify_repo, "/a.txt", 5);
    }

    #[test]
    fn unlink_removes_a_settled_file() {
        let (fs, verify_repo, _store, _dir) = setup(true);
        let handle = fs.create("/a.txt").unwrap();
        fs.release(handle);
        wait_for_settled(&verify_repo, "/a.txt", 0);

        fs.unlink("/a.txt").unwrap();
        assert!(fs.resolve_required("/a.txt").is_err());
    }

    #[test]
    fn overwriting_an_existing_file_settles_a_new_generation_with_the_new_content() {
        let (fs, verify_repo, verify_store, _dir) = setup(true);
        let first = fs.create("/a.txt").unwrap();
        fs.write(first, 0, b"one").unwrap();
        fs.release(first);
        let first_entry = wait_for_settled(&verify_repo, "/a.txt", 3);

        let second = fs.open("/a.txt", true).unwrap();
        fs.write(second, 0, b"two-two").unwrap();
        fs.release(second);
        let second_entry = wait_for_settled(&verify_repo, "/a.txt", 7);

        assert_ne!(
            first_entry.id, second_entry.id,
            "a new history entry, not an in-place update"
        );
        let data = crate::content_reader::read_content(
            &verify_repo,
            &verify_store,
            second_entry.content_id.unwrap(),
            0,
            7,
        )
        .unwrap();
        assert_eq!(data, b"two-two");
    }

    #[test]
    fn write_operations_are_rejected_on_a_read_only_mount() {
        let (fs, _verify_repo, _store, _dir) = setup(false);
        assert_eq!(fs.create("/a.txt").unwrap_err(), Errno::EROFS);
        assert_eq!(fs.truncate("/a.txt", 5).unwrap_err(), Errno::EROFS);
        assert_eq!(fs.unlink("/a.txt").unwrap_err(), Errno::EROFS);
    }

    #[test]
    fn a_systemic_settle_failure_degrades_the_session_to_read_only_and_is_logged() {
        let repo_dir = tempfile::tempdir().unwrap();
        let repo_root = repo_dir.path().join("repo");
        db::init_repository(
            &repo_root,
            db::RepositorySettings::new(Some(12), 1_700_000_000_000),
        )
        .unwrap();
        let fs_repo = db::open_repository(&repo_root).unwrap();
        // A read-only store deterministically fails every real chunk write, standing in for
        // DESIGN-MOUNT-009's systemic case (e.g. storage full) without actually needing to fill a
        // disk. `create`'s own empty-content settle never calls `store.write` at all (no chunks),
        // so it still succeeds even here - only a settle with real bytes hits this.
        let fs_store = store::ByteStore::new(db::data_dir(&repo_root), true);
        let fs = DedupFs::new(fs_repo, fs_store, true, &repo_root).unwrap();

        let handle = fs.create("/a.txt").unwrap();
        fs.write(handle, 0, b"hello").unwrap();
        fs.release(handle);

        // The failure is recorded asynchronously (DESIGN-MOUNT-006's non-blocking `release`) -
        // poll a fresh write-intent open until it starts observing the degradation.
        let mut degraded = false;
        for _ in 0..500 {
            if fs.create("/probe.txt") == Err(Errno::EROFS) {
                degraded = true;
                break;
            }
            thread::sleep(Duration::from_millis(10));
        }
        assert!(degraded, "session did not degrade to read-only in time");

        let log = std::fs::read_to_string(db::meta_dir(&repo_root).join("write-failures.log"))
            .expect("the failure log file must exist");
        assert!(log.contains("systemic"), "log contents: {log}");
        assert!(log.contains("a.txt"), "log contents: {log}");
    }
}
