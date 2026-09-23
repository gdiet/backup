//! `MountFilesystem` backed by a real, open `db::Repository` - REQ-MOUNT-001/002/003/009.
//! Read-only operations, directory structure, and content writes
//! (`create`/`write`/`truncate`/`unlink`, DESIGN-MOUNT-006/009/010/012/013/015 in
//! `docs/design/mount-write-path.md`) are all wired in: a write-intent open/create registers with
//! [`crate::pending_files::PendingFiles`], `write`/`truncate` land in its write-cache chain, and
//! `release` hands a fully-released generation off to [`crate::settle_pool::JobPool`]'s background
//! settle job - never blocking the releasing call itself (DESIGN-MOUNT-006). A job that fails is
//! recorded in [`crate::failure_log::FailureLog`] (DESIGN-MOUNT-009), degrading the session's
//! future write-intent opens to read-only on a `crates/store` I/O failure specifically; a failure
//! of the shared metadata connection itself, from either a background job or a synchronous call
//! (`to_errno_reporting_connection_death`), is reported once instead of degrading a flag, since it
//! affects every future call equally, reads included.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use mountfs::{Attr, DirEntry, Errno, FileKind, Handle, MountFilesystem, StatfsInfo};

use crate::failure_log::{Failure, FailureLog};
use crate::pending_files::{NewGeneration, PendingFiles};
use crate::ram_budget::{self, DispatchPool};
use crate::settle_pool::{JobPool, SettleJob};
use crate::write_cache::MemoryBudget;

/// Runtime tuning knobs [`DedupFs::new`] needs beyond its structural parameters - grouped to keep
/// that constructor's own parameter count reasonable, and to give `crate::mount` a single value
/// to build once from its own CLI flags and pass through unchanged.
pub struct Tuning {
    /// DESIGN-MEMORY-001's operator-configurable RAM budget total (`--ram-budget-mb`), in bytes.
    pub ram_budget_gross_bytes: u64,
    /// DESIGN-MOUNT-006's write() backpressure delay formula's two constants
    /// (`--backpressure-free-zone-bytes`/`--backpressure-slope-divisor`).
    pub backpressure_free_zone_bytes: u64,
    pub backpressure_slope_divisor: u128,
}

pub struct DedupFs {
    repo: Arc<db::Repository>,
    store: Arc<store::ByteStore>,
    read_write: bool,
    cdc_target_size_bits: u32,
    pending: PendingFiles,
    pool: JobPool,
    budget: Arc<MemoryBudget>,
    temp_dir: PathBuf,
    /// `None` for a read-only mount, which never submits a settle job that could produce a
    /// failure to log (DESIGN-MOUNT-009) in the first place.
    failure_log: Option<Arc<FailureLog>>,
    backpressure_free_zone_bytes: u64,
    backpressure_slope_divisor: u128,
}

impl DedupFs {
    /// `repo_root` is only needed to open DESIGN-MOUNT-009's failure log alongside the metadata
    /// database (`db::meta_dir`) - `repo`/`store` are otherwise already fully open. `spill_dir`
    /// is the write cache's spillover directory (DESIGN-MOUNT-010/018) - `None` defaults to the
    /// OS temp directory, the same as before this parameter existed. The caller is responsible
    /// for validating a given `spill_dir` actually exists (`crate::mount`'s eager check) - this
    /// constructor does not fail just because a `Some` value happens not to.
    /// `tuning.ram_budget_gross_bytes` is DESIGN-MEMORY-001's operator-configurable total
    /// (`--ram-budget-mb`) - any `--cache-size` override must already have been applied to
    /// `repo`'s connection before this call, so it is reflected in the `cache_size` reserve read
    /// back below.
    pub fn new(
        repo: db::Repository,
        store: store::ByteStore,
        read_write: bool,
        repo_root: &Path,
        spill_dir: Option<PathBuf>,
        tuning: Tuning,
    ) -> io::Result<Self> {
        let cdc_target_size_bits = repo.settings().cdc_target_size_bits();
        let worker_count = std::thread::available_parallelism()
            .map(std::num::NonZero::get)
            .unwrap_or(1);
        // DESIGN-MEMORY-001 (`docs/design/ram-budget.md`): the shared write-cache budget is the
        // gross configurable limit minus this connection's own `cache_size` and a stack reserve
        // for the CDC/hash/persist pool plus a real mount session's own FUSE/WinFSP dispatch pool.
        let cache_size_bytes = repo.cache_size_bytes().map_err(io::Error::other)?;
        let caching_budget_bytes = ram_budget::caching_budget_bytes(
            tuning.ram_budget_gross_bytes,
            cache_size_bytes,
            worker_count as u64,
            DispatchPool::Fuse,
        );
        let max_chunk_size = cdc::ChunkerConfig::new(Some(cdc_target_size_bits))
            .expect("cdc_target_size_bits was already validated when the repository was created")
            .max_chunk_size()
            .expect("Some(bits) always yields a bounded max_chunk_size");
        ram_budget::check_fits_max_chunk_size(caching_budget_bytes, max_chunk_size)
            .map_err(io::Error::other)?;
        let repo = Arc::new(repo);
        let store = Arc::new(store);
        let failure_log = if read_write {
            Some(Arc::new(FailureLog::open(&db::meta_dir(repo_root))?))
        } else {
            None
        };
        let failure_log_for_pool = failure_log.clone();
        let pool = JobPool::new(
            worker_count,
            Arc::clone(&repo),
            Arc::clone(&store),
            cdc_target_size_bits,
            move |job: &SettleJob, err| {
                if let Some(log) = &failure_log_for_pool {
                    if let Some(db_err) = err.kills_connection() {
                        log.report_connection_dead_once(now_millis(), &db_err.to_string());
                    }
                    log.record(Failure {
                        parent_id: job.parent_id,
                        name: &job.name,
                        time_millis: now_millis(),
                        systemic: err.is_systemic(),
                        degrades_writes: err.write_degrades_session(),
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
            budget: Arc::new(MemoryBudget::new(caching_budget_bytes)),
            temp_dir: spill_dir.unwrap_or_else(std::env::temp_dir),
            failure_log,
            backpressure_free_zone_bytes: tuning.backpressure_free_zone_bytes,
            backpressure_slope_divisor: tuning.backpressure_slope_divisor,
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
        // Never actually reaches here either: `DedupFs` never calls `purge_deleted_entry`
        // (REQ-CLI-003's `--purge` case is CLI-only, not exposed through the mount).
        | db::Error::NotSoftDeleted(_)
        // Never actually reaches here either: `DedupFs` never calls `backup_metadata`/
        // `restore_metadata` (REQ-MAINTENANCE-001/002 are CLI-only, not exposed through the mount).
        | db::Error::InvalidBackup(_)
        | db::Error::PathNotUtf8(_)
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
            .map_err(|e| self.to_errno_reporting_connection_death(e))?
            .ok_or(Errno::ENOENT)
    }

    fn require_read_write(&self) -> Result<(), Errno> {
        if self.read_write {
            Ok(())
        } else {
            Err(Errno::EROFS)
        }
    }

    /// DESIGN-MOUNT-009: refuses a new content write once a `crates/store` I/O failure (e.g.
    /// storage full) has degraded this session to read-only - checked by a write-intent
    /// `open`/`create`, a bare `truncate`, and `write` itself (for a handle that was already open
    /// before the session degraded). Directory structure operations (`mkdir`/`rmdir`/`rename`/
    /// `utimens`) and `unlink` are unaffected: none of them need `crates/store` space, so none of
    /// them are doomed to repeat this same cause. A failure of the shared metadata connection
    /// itself (a poisoned lock and the like) does not degrade this flag either - see
    /// [`Self::to_errno_reporting_connection_death`].
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

    /// [`to_errno`], plus DESIGN-MOUNT-009's one-time report if `err` means the shared
    /// `db::Repository` connection itself has become unusable
    /// (`crate::settle_pool::is_systemic_db_error`) - every call reaches this the same way a
    /// background job's [`db::Error`] does, so both sides of DESIGN-MOUNT-009 go through the same
    /// classification. Deliberately still returns [`to_errno`]'s ordinary mapping (`EIO` for every
    /// variant this classifies as systemic) rather than `EROFS`: unlike a `crates/store` failure,
    /// this affects every future call through the connection equally, reads included, so there is
    /// nothing to gate - only to report once.
    fn to_errno_reporting_connection_death(&self, err: db::Error) -> Errno {
        if let Some(log) = &self.failure_log
            && crate::settle_pool::is_systemic_db_error(&err)
        {
            log.report_connection_dead_once(now_millis(), &err.to_string());
        }
        to_errno(err)
    }

    /// `file_id`'s durably committed content, as [`NewGeneration`]'s `base_content_id`/
    /// `base_size` need it - skipped (and left as the harmless `(None, 0)`, since unused) when a
    /// writable generation already exists, so a tight sequence of `write` calls on the same
    /// handle only ever pays for this lookup once.
    fn base_for_write(&self, file_id: i64) -> Result<(Option<i64>, u64), Errno> {
        if self.pending.has_writable(file_id) {
            return Ok((None, 0));
        }
        let entry = self
            .repo
            .entry_by_id(file_id)
            .map_err(|e| self.to_errno_reporting_connection_death(e))?;
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
        let children = self
            .repo
            .list_children(entry.id)
            .map_err(|e| self.to_errno_reporting_connection_death(e))?;
        Ok(children
            .into_iter()
            .map(|(name, entry)| DirEntry {
                name,
                kind: kind_to_mountfs(entry.kind),
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
            .map_err(|e| self.to_errno_reporting_connection_death(e))?
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
            .map_err(|e| self.to_errno_reporting_connection_death(e))?;
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
            |_| {},
        )
        .map_err(|_| Errno::EIO)?;
        let id = self
            .repo
            .settle_file(parent.id, name, now_millis(), empty_content_id)
            .map_err(|e| self.to_errno_reporting_connection_death(e))?;
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
            .map_err(|e| self.to_errno_reporting_connection_death(e))
    }

    fn rmdir(&self, path: &str) -> Result<(), Errno> {
        self.require_read_write()?;
        let entry = self.resolve_required(path)?;
        self.repo
            .rmdir(entry.id, now_millis())
            .map_err(|e| self.to_errno_reporting_connection_death(e))
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
            .map_err(|e| self.to_errno_reporting_connection_death(e))
    }

    fn utimens(&self, path: &str, mtime_millis: i64) -> Result<(), Errno> {
        self.require_read_write()?;
        let entry = self.resolve_required(path)?;
        self.repo
            .set_mtime(entry.id, mtime_millis)
            .map_err(|e| self.to_errno_reporting_connection_death(e))
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
            self.pool.bytes_in_persist_queue(),
            data.len(),
            self.backpressure_free_zone_bytes,
            self.backpressure_slope_divisor,
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

    fn default_tuning() -> Tuning {
        Tuning {
            ram_budget_gross_bytes: ram_budget::DEFAULT_GROSS_BUDGET_BYTES,
            backpressure_free_zone_bytes: crate::backpressure::DEFAULT_FREE_ZONE_BYTES,
            backpressure_slope_divisor: crate::backpressure::DEFAULT_SLOPE_DIVISOR,
        }
    }

    /// `fs` and `verify_repo`/`verify_store` point at the same repository, opened separately -
    /// `fs` owns the connection actually driving the mount, `verify_repo`/`verify_store` let a
    /// test inspect what a background settle job eventually commits, which `release`/`truncate`
    /// deliberately never wait for (DESIGN-MOUNT-006).
    fn setup(read_write: bool) -> (DedupFs, db::Repository, store::ByteStore, tempfile::TempDir) {
        let repo_dir = tempfile::tempdir().unwrap();
        let repo_root = repo_dir.path().join("repo");
        db::init_repository(
            &repo_root,
            db::RepositorySettings::new(12, 1_700_000_000_000),
        )
        .unwrap();
        let fs_repo = db::open_repository(&repo_root).unwrap();
        let verify_repo = db::open_repository(&repo_root).unwrap();
        let verify_store = store::ByteStore::new(db::data_dir(&repo_root), true);
        let fs_store = store::ByteStore::new(db::data_dir(&repo_root), !read_write);
        let fs = DedupFs::new(
            fs_repo,
            fs_store,
            read_write,
            &repo_root,
            None,
            default_tuning(),
        )
        .unwrap();
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
        // A zero free zone so this test's realistic ~4 MiB backlog (well under
        // DEFAULT_FREE_ZONE_BYTES's 1 GB) still produces a measurable delay - the default's own
        // free zone exists to keep small, ordinary backlogs delay-free, exactly what this test
        // needs to see past to exercise the formula at all.
        fs.backpressure_free_zone_bytes = 0;

        // Release a real, spilled generation - DESIGN-MOUNT-013's hand-off submits a SettleJob
        // carrying its ~4 MiB of spilled_bytes to fs.pool. A single job's backlog contribution
        // only clears once `run_job` finishes entirely (settle_pool.rs's worker_loop), so it stays
        // fully present, not partially drained, for as long as this one job is still in flight.
        let spilling = fs.create("/spills.txt").unwrap();
        fs.write(spilling, 0, &vec![0xABu8; 4 * 1024 * 1024])
            .unwrap();
        fs.release(spilling);

        // Timed immediately after - the settle job above is almost certainly still running, so
        // fs.pool.bytes_in_persist_queue() (the same signal write() itself consults) should still
        // be near its full ~4 MiB, giving this write its own 4 MiB call a real, measurable delay
        // well above ordinary scheduling noise.
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
    fn the_shared_budget_is_fully_available_again_once_every_generation_settles() {
        let (mut fs, verify_repo, _store, _dir) = setup(true);
        // A small, known-size budget: a 10-byte write claims a handle's whole fair share
        // (DESIGN-MOUNT-019, half of what is available). If a settled generation's reservation
        // were never returned to the shared budget, it would shrink by 10 bytes every round below
        // and a later file would eventually be forced to spill even though nothing is still open.
        fs.budget = Arc::new(MemoryBudget::new(20));

        for (name, byte) in [("/a.txt", 0xAAu8), ("/b.txt", 0xBBu8), ("/c.txt", 0xCCu8)] {
            let handle = fs.create(name).unwrap();
            fs.write(handle, 0, &[byte; 10]).unwrap();
            assert_eq!(
                fs.pending.spilled_bytes(handle.0 as i64),
                0,
                "{name}'s write should have fit entirely in memory if the shared budget was \
                 actually returned once the previous file settled"
            );
            fs.release(handle);
            wait_for_settled(&verify_repo, name, 10);
        }
    }

    /// Spawns a real libfuse3 mount of `fs` on its own thread and blocks until it is actually
    /// ready to serve requests - `fs`'s mounted tree starts empty, so "readiness" has to be an
    /// actual write attempt succeeding, not a "listing is non-empty" check (mirroring
    /// `crates/mountfs/src/linux/mod.rs`'s own `DispatchProbeFs` real-mount test).
    fn mount_for_test(
        fs: DedupFs,
        mount_path: &std::path::Path,
    ) -> thread::JoinHandle<io::Result<()>> {
        let handle = {
            let mount_path = mount_path.to_path_buf();
            thread::spawn(move || mountfs::mount(fs, &mount_path, false))
        };
        let probe_path = mount_path.join("_ready_probe.txt");
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if std::fs::write(&probe_path, b"x").is_ok() {
                let _ = std::fs::remove_file(&probe_path);
                break;
            }
            assert!(
                Instant::now() < deadline,
                "mount did not become ready within 5s (requires /dev/fuse access)"
            );
            thread::sleep(Duration::from_millis(50));
        }
        // Found the hard way: a real client operation issued immediately after this probe
        // succeeds can still return success without ever reaching this filesystem's own
        // dispatch handlers at all (confirmed via temporary tracing - no create()/write() call
        // observed server-side, yet the client-side syscalls reported Ok) - some libfuse3/kernel
        // warm-up still settling in the moment right after the very first successful request,
        // not anything specific to this probe's own file. A short, fixed pause here reliably
        // avoided it in practice; there is no more precise readiness signal available than the
        // probe above already uses.
        thread::sleep(Duration::from_millis(200));
        handle
    }

    fn unmount_and_join(mount_path: &std::path::Path, handle: thread::JoinHandle<io::Result<()>>) {
        let status = std::process::Command::new("fusermount3")
            .arg("-u")
            .arg(mount_path)
            .status()
            .expect("failed to run fusermount3 -u");
        assert!(status.success(), "fusermount3 -u failed: {status}");
        handle
            .join()
            .expect("mount thread panicked")
            .expect("mount() returned an error");
    }

    /// Writes `payload` to a brand new file at `path` and forces a real flush through the FUSE
    /// dispatch path before returning - a plain buffered `write(2)` can be satisfied entirely from
    /// the kernel's page cache and never actually reach this filesystem's own `write()` dispatch
    /// at all (found the hard way while writing `DispatchProbeFs`'s own real-mount test), so
    /// `sync_all` is not optional here.
    fn timed_synced_write(path: &std::path::Path, payload: &[u8]) -> Duration {
        let start = Instant::now();
        let mut file = std::fs::File::create(path).expect("create against the mount must succeed");
        std::io::Write::write_all(&mut file, payload)
            .expect("write against the mount must succeed");
        file.sync_all()
            .expect("sync_all against the mount must succeed");
        start.elapsed()
    }

    #[test]
    fn real_mount_write_backpressure_delay_grows_then_drains_with_the_persist_queue() {
        let (mut fs, verify_repo, _store, _dir) = setup(true);
        // Free zone at zero and a small slope so a modest, quickly-achievable backlog (a few MB,
        // not DEFAULT_FREE_ZONE_BYTES's 1 GB) produces a clearly measurable delay: gives ~50ms at
        // a ~2 MB backlog and a 128 KiB write (this test's own payload size below), derived the
        // same way DEFAULT_SLOPE_DIVISOR's own doc comment derives its anchor.
        fs.backpressure_free_zone_bytes = 0;
        fs.backpressure_slope_divisor = 5_242_880_000;

        let mount_dir = tempfile::tempdir().unwrap();
        let mount_path = mount_dir.path().to_path_buf();
        let handle = mount_for_test(fs, &mount_path);

        let payload = vec![7u8; 128 * 1024];

        // Baseline: nothing has been released yet, so the persist queue is empty - this write
        // should see no meaningful delay.
        let baseline = timed_synced_write(&mount_path.join("baseline.txt"), &payload);

        // Release a real, sizeable generation - DESIGN-MOUNT-013's hand-off submits it to the
        // background pool, adding its full size to bytes_in_persist_queue until each of its
        // chunks settles (DESIGN-MOUNT-006's incremental per-chunk drain).
        let big_payload = vec![9u8; 8 * 1024 * 1024];
        {
            let mut big =
                std::fs::File::create(mount_path.join("big.txt")).expect("create big.txt");
            std::io::Write::write_all(&mut big, &big_payload).expect("write big.txt");
            big.sync_all().expect("sync_all big.txt");
        } // dropped here - closes the handle, triggering release()

        // Timed immediately after - the settle jobs for big.txt's chunks are almost certainly
        // still in flight, so this write should see a real, measurable delay well above baseline.
        let during_backlog = timed_synced_write(&mount_path.join("during-backlog.txt"), &payload);

        // Wait for big.txt to fully settle - once its entry shows its final size, none of its
        // bytes remain in the persist queue.
        wait_for_settled(&verify_repo, "/big.txt", big_payload.len() as u64);

        // Timed once the persist queue has drained - should be fast again, similar to baseline.
        let after_drain = timed_synced_write(&mount_path.join("after-drain.txt"), &payload);

        unmount_and_join(&mount_path, handle);

        println!(
            "backpressure delay through a real mount: baseline={baseline:?} \
             during_backlog={during_backlog:?} after_drain={after_drain:?}"
        );
        assert!(
            during_backlog > baseline * 3 && during_backlog > Duration::from_millis(10),
            "expected a clearly measurable delay while the persist queue was backlogged: \
             baseline={baseline:?} during_backlog={during_backlog:?}"
        );
        assert!(
            after_drain < during_backlog / 2,
            "expected the delay to drop again once the persist queue drained: \
             during_backlog={during_backlog:?} after_drain={after_drain:?}"
        );
    }

    // `open_o_sync` below uses `std::os::unix::fs::OpenOptionsExt`/`libc::O_SYNC` directly, with
    // no cross-platform equivalent reached for yet - unlike `real_mount_` tests elsewhere that
    // only *fail at runtime* on a platform they were not written for (already an accepted,
    // `--skip real_mount`-handled case), this one does not even compile on Windows without this
    // gate, breaking `cargo test` for the whole crate there. Found and fixed while verifying an
    // unrelated Windows-specific fix in `crates/cli/src/ingest.rs` on real Windows.
    #[cfg(target_os = "linux")]
    #[test]
    fn real_mount_concurrent_handles_converge_toward_the_per_handle_halving_formula() {
        // `write_cache.rs` only spills a write that does not fit a handle's current share, never
        // content already resident in memory from an earlier write (DESIGN-MOUNT-019). B's single
        // write below is also its *first* write, so nothing was memory-resident yet for it to
        // keep - its whole payload spills, same as it would for any handle whose very first write
        // already exceeds its share. The externally-observable signal this test actually checks is
        // binary, not a size split: does a handle spill *at all* for a write comfortably within a
        // "first, alone" equilibrium, but not within a "second, after the first already claimed
        // its share" one.
        let (mut fs, _verify_repo, _store, _dir) = setup(true);
        // 1,000,000-byte budget: DESIGN-MOUNT-019's first-handle-alone equilibrium is half of
        // that (500,000), second-handle-after-the-first's is half of what is left (~250,000).
        fs.budget = Arc::new(MemoryBudget::new(1_000_000));
        let spill_dir = tempfile::tempdir().unwrap();
        fs.temp_dir = spill_dir.path().to_path_buf();

        let mount_dir = tempfile::tempdir().unwrap();
        let mount_path = mount_dir.path().to_path_buf();
        let handle = mount_for_test(fs, &mount_path);

        // 400,000 bytes: comfortably under the 500,000 a first, alone handle can hold (so A must
        // not spill), comfortably over the ~250,000 a second handle competing for what A left
        // behind can hold (so B must spill). A single `write_all` call this size reaches this
        // filesystem's own `write()` dispatch as one real FUSE call, not several smaller ones
        // (confirmed directly, with temporary tracing, while writing this test) - the formula's
        // own per-call fairness math still holds regardless of whether it runs once or several
        // times per handle, so this test does not depend on which happens.
        let write_size = 400_000;
        // Distinct fill bytes so the spill file found below can be matched back to whichever
        // handle actually produced it, by content, rather than assumed.
        let payload_a = vec![0xAAu8; write_size];
        let payload_b = vec![0xBBu8; write_size];

        // `O_SYNC`, not a `sync_all()` call after the fact: `fsync` is `Unimplemented` in
        // `crates/mountfs/src/linux/sys.rs`'s `fuse_operations`, so an explicit fsync request
        // does not reliably force this filesystem's own `write()` dispatch to have run yet - found
        // the hard way, an earlier version of this test relying on `sync_all()` saw zero
        // `try_acquire_share` calls at all for a second file written this way. `O_SYNC` instead
        // makes the kernel treat every `write(2)` itself as synchronous, which does not depend on
        // this filesystem implementing fsync.
        fn open_o_sync(path: &std::path::Path) -> std::fs::File {
            use std::os::unix::fs::OpenOptionsExt;
            std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .custom_flags(libc::O_SYNC)
                .open(path)
                .expect("O_SYNC create against the mount must succeed")
        }

        // Handle A: opened and written first, alone - must stay entirely memory-resident. Kept
        // open (not dropped) until after the spill-directory check below.
        let mut file_a = open_o_sync(&mount_path.join("a.txt"));
        std::io::Write::write_all(&mut file_a, &payload_a).expect("write a.txt");

        // Handle B: opened only once A already holds its own 500,000-byte share - must spill.
        let mut file_b = open_o_sync(&mount_path.join("b.txt"));
        std::io::Write::write_all(&mut file_b, &payload_b).expect("write b.txt");

        // Inspect the spill directory directly while both handles are still open (not yet
        // released) - DedupFs's own internals have already been moved into the mount thread
        // above, so this is the only way left to observe which cache actually spilled. Content is
        // read *before* either handle is closed below - once closed, release() hands the
        // generation to the background settle pool, and `unmount_and_join` (via `JobPool`'s own
        // `Drop`) waits for that to fully finish, including `SpillFile`'s own `Drop` deleting the
        // spill file - by then there would be nothing left to read.
        let spill_entries: Vec<_> = std::fs::read_dir(spill_dir.path())
            .expect("read spill_dir")
            .map(|entry| entry.expect("read spill_dir entry"))
            .collect();
        assert_eq!(
            spill_entries.len(),
            1,
            "expected exactly one handle (B) to have spilled, found {} spill file(s)",
            spill_entries.len()
        );
        let spilled_content = std::fs::read(spill_entries[0].path()).expect("read spill file");

        drop(file_a);
        drop(file_b);
        unmount_and_join(&mount_path, handle);

        println!(
            "handle-cap halving through a real mount: one spill file found, {} bytes, first byte \
             0x{:02x}",
            spilled_content.len(),
            spilled_content.first().copied().unwrap_or(0)
        );
        assert_eq!(
            spilled_content.len(),
            write_size,
            "the spilled handle's cache should hold its full write once spilled, not just the \
             overflow past its share"
        );
        assert!(
            spilled_content.iter().all(|&b| b == 0xBB),
            "expected the spilled cache to be B's content (0xBB), not A's (0xAA) - A, opened \
             first and alone, should have stayed entirely memory-resident instead"
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
            db::RepositorySettings::new(12, 1_700_000_000_000),
        )
        .unwrap();
        let fs_repo = db::open_repository(&repo_root).unwrap();
        // A read-only store deterministically fails every real chunk write, standing in for
        // DESIGN-MOUNT-009's systemic case (e.g. storage full) without actually needing to fill a
        // disk. `create`'s own empty-content settle never calls `store.write` at all (no chunks),
        // so it still succeeds even here - only a settle with real bytes hits this.
        let fs_store = store::ByteStore::new(db::data_dir(&repo_root), true);
        let fs = DedupFs::new(fs_repo, fs_store, true, &repo_root, None, default_tuning()).unwrap();

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

    #[test]
    fn a_systemic_db_error_from_a_synchronous_call_is_reported_once_without_degrading_writes() {
        // Exercises `to_errno_reporting_connection_death` directly with a synthetic
        // `db::Error::Poisoned`, the same way `tree.rs`'s own tests bypass a platform gate to
        // reach the logic underneath it: there is no way to actually poison `db::Repository`'s
        // internal lock through its public API from here, and doing so is `db`'s own concern, not
        // this method's - what this method owns is the mapping/reporting once a systemic
        // `db::Error` surfaces, which this reaches directly.
        let (fs, _verify_repo, _store, repo_dir) = setup(true);
        let repo_root = repo_dir.path().join("repo");

        let errno = fs.to_errno_reporting_connection_death(db::Error::Poisoned);
        assert_eq!(errno, Errno::EIO);
        assert!(
            !fs.failure_log.as_ref().unwrap().is_degraded(),
            "a connection-dead report must not degrade write-intent opens - reads are equally \
             broken, unlike a crates/store I/O failure"
        );

        let log_path = db::meta_dir(&repo_root).join("write-failures.log");
        let log = std::fs::read_to_string(&log_path).expect("the failure log file must exist");
        assert_eq!(log.lines().count(), 1, "log contents: {log}");
        assert!(log.contains("connection dead"), "log contents: {log}");

        // A second occurrence still maps to the same errno, but must not add a second line.
        let errno_again = fs.to_errno_reporting_connection_death(db::Error::Poisoned);
        assert_eq!(errno_again, Errno::EIO);
        let log_after_second = std::fs::read_to_string(&log_path).unwrap();
        assert_eq!(
            log_after_second.lines().count(),
            1,
            "log contents: {log_after_second}"
        );
    }
}
