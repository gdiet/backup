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

use crate::deleted;
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
    /// REQ-MOUNT-004's base opt-in (`--show-deleted`): makes REQ-TREE-009's `[deleted]` view (and
    /// REQ-MOUNT-008's own `[time]` presentation of it) visible and browsable. Available on a
    /// read-only mount too - recovery via move-out needs `read_write` as well, but browsing does
    /// not.
    pub show_deleted: bool,
    /// REQ-MOUNT-007's second, escalating opt-in (`--purge`): additionally allows permanently
    /// purging an entry from inside the view. Meaningless without `show_deleted`, and without
    /// `read_write` - nothing mutating is ever allowed on a read-only mount regardless of this
    /// flag.
    pub allow_purge: bool,
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
    /// The repository's own `data/` directory (`db::data_dir`) - [`Self::statfs`]'s target for
    /// [`mountfs::disk_space`], a single-path query rather than an "enumerate every mounted
    /// filesystem" one specifically to avoid recursing into this process's own mount point (see
    /// that function's own doc comment).
    data_dir: PathBuf,
    /// `None` for a read-only mount, which never submits a settle job that could produce a
    /// failure to log (DESIGN-MOUNT-009) in the first place.
    failure_log: Option<Arc<FailureLog>>,
    backpressure_free_zone_bytes: u64,
    backpressure_slope_divisor: u128,
    show_deleted: bool,
    allow_purge: bool,
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
            data_dir: db::data_dir(repo_root),
            failure_log,
            backpressure_free_zone_bytes: tuning.backpressure_free_zone_bytes,
            backpressure_slope_divisor: tuning.backpressure_slope_divisor,
            show_deleted: tuning.show_deleted,
            allow_purge: tuning.allow_purge,
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
        // `unlink`/`rmdir`/`rename` against a `[deleted]`-addressed entry (REQ-MOUNT-007) resolve
        // the path and call `purge_deleted_entry`/`recover_deleted_entry` as two separate calls,
        // each its own transaction - a concurrent recovery of the same entry racing in between
        // (single-threaded per call, but nothing stops a different handle) is the one way this
        // reaches here for real, not a normal user-facing path.
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

/// What a mount path resolves to once REQ-MOUNT-004/007/008's `[deleted]`/`[time]` addressing is
/// taken into account - `crate::deleted::Resolved` plus [`TimeChildren`](Self::TimeChildren), the
/// mount-only `[time]` view REQ-MOUNT-008 adds on top of it (never reached by `dfs list`/`dfs
/// restore`, so it has no place in `crate::deleted::Resolved` itself).
#[derive(Clone, Copy)]
enum MountPath {
    Live(db::Entry),
    /// The `[deleted]` segment itself, naming `parent_id`'s own soft-deleted children.
    DeletedChildren {
        parent_id: i64,
    },
    /// The `[deleted]/[time]` segment - the same children as `DeletedChildren`, displayed with
    /// REQ-MOUNT-008's always-timestamp-prefixed names instead.
    TimeChildren {
        parent_id: i64,
    },
    /// One specific soft-deleted entry, addressed by its own disambiguated (`[deleted]`) or
    /// always-prefixed (`[time]`) display name - both name the same entry, indistinguishable from
    /// here on.
    Deleted(db::DeletedEntry),
}

impl From<deleted::Resolved> for MountPath {
    fn from(resolved: deleted::Resolved) -> Self {
        match resolved {
            deleted::Resolved::Live(entry) => MountPath::Live(entry),
            deleted::Resolved::DeletedChildren { parent_id } => {
                MountPath::DeletedChildren { parent_id }
            }
            deleted::Resolved::Deleted(entry) => MountPath::Deleted(entry),
        }
    }
}

impl DedupFs {
    /// Resolves `path`, honoring REQ-MOUNT-004/007/008's `[deleted]`/`[time]` addressing when
    /// `self.show_deleted` is on - entirely inert when it is off, in which case this is exactly
    /// `self.repo.resolve_path(path).map(MountPath::Live)`: `[deleted]`/`[time]` are not special
    /// at all, the same as any other name that happens not to exist (REQ-MOUNT-004's own
    /// off-by-default opt-in).
    ///
    /// `[time]` only ever appears immediately after a `[deleted]`-view resolution (REQ-MOUNT-008:
    /// a second presentation of that one view, not its own independently addressable segment
    /// anywhere else) - found by scanning for a `[time]` segment whose own preceding path segments
    /// resolve to exactly [`MountPath::DeletedChildren`], so a live entry (or a soft-deleted one,
    /// reached through a *different* `[deleted]` step) literally named `[time]` elsewhere is never
    /// shadowed by this. One known, narrow limitation this does not resolve: a soft-deleted entry
    /// literally named `[time]`, sitting directly inside the very `[deleted]` view being addressed,
    /// becomes unreachable by that literal name once `[time]`'s own synthetic view takes the same
    /// slot - REQ-TREE-009 solves the analogous, far more likely outer collision (a live entry
    /// named `[deleted]`) explicitly; this inner one is not addressed by any agreed requirement
    /// today and is left as a documented gap rather than inventing an unrequested escaping scheme.
    fn resolve_mount_path(&self, path: &str) -> Result<Option<MountPath>, db::Error> {
        if !self.show_deleted {
            return Ok(self.repo.resolve_path(path)?.map(MountPath::Live));
        }
        let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
        for time_index in 0..segments.len() {
            if segments[time_index] != deleted::TIME_SEGMENT {
                continue;
            }
            let prefix = segments[..time_index].join("/");
            let Some(deleted::Resolved::DeletedChildren { parent_id }) =
                deleted::resolve_within(&self.repo, &prefix, mountfs::MAX_NAME_BYTES)?
            else {
                continue;
            };
            if time_index + 1 == segments.len() {
                return Ok(Some(MountPath::TimeChildren { parent_id }));
            }
            let children = self.repo.list_deleted_children(parent_id)?;
            let Some(entry) = deleted::find_by_timestamped_name(
                &children,
                segments[time_index + 1],
                Some(mountfs::MAX_NAME_BYTES),
            ) else {
                return Ok(None);
            };
            return Ok(deleted::continue_from_deleted_entry(
                &self.repo,
                entry,
                &segments,
                time_index + 2,
                Some(mountfs::MAX_NAME_BYTES),
            )?
            .map(MountPath::from));
        }
        Ok(
            deleted::resolve_within(&self.repo, path, mountfs::MAX_NAME_BYTES)?
                .map(MountPath::from),
        )
    }

    fn resolve_mount_path_required(&self, path: &str) -> Result<MountPath, Errno> {
        self.resolve_mount_path(path)
            .map_err(|e| self.to_errno_reporting_connection_death(e))?
            .ok_or(Errno::ENOENT)
    }

    /// The live entry at `parent_path`, required to be a real, live directory - `mkdir`/`create`/
    /// `rename`/`utimens`'s own shared parent-resolution step. Refuses with `EACCES` rather than
    /// `ENOENT`/`ENOTDIR` when `parent_path` resolves to any of REQ-MOUNT-004/007/008's synthetic
    /// `[deleted]`/`[time]` locations - REQ-MOUNT-007: never a false success creating or moving
    /// something into the view, always a clear, deliberate refusal instead.
    fn resolve_live_parent(&self, parent_path: &str) -> Result<db::Entry, Errno> {
        match self.resolve_mount_path_required(parent_path)? {
            MountPath::Live(entry) => Ok(entry),
            MountPath::DeletedChildren { .. }
            | MountPath::TimeChildren { .. }
            | MountPath::Deleted(_) => Err(Errno::EACCES),
        }
    }

    /// The most recently soft-deleted child's own deletion timestamp among `parent_id`'s
    /// soft-deleted children, as [`Attr::mtime_millis`] for the `[deleted]`/`[time]` view
    /// containers themselves - `0` if there are none (a view is always addressable directly, per
    /// REQ-TREE-009, even where nothing has ever been deleted at that location). Not covered by
    /// any requirement's own guarantee, unlike a *contained* entry's real mtime (REQ-MOUNT-008);
    /// matches `crate::list.rs`'s own established convention for the same synthetic marker.
    fn synthetic_dir_attr(&self, parent_id: i64) -> Result<Attr, Errno> {
        let children = self
            .repo
            .list_deleted_children(parent_id)
            .map_err(|e| self.to_errno_reporting_connection_death(e))?;
        let mtime_millis = children
            .iter()
            .map(|(_, e)| e.deleted_at)
            .max()
            .unwrap_or(0);
        Ok(Attr {
            kind: FileKind::Directory,
            size: 0,
            mtime_millis,
        })
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
        // parent_id/name are a best-effort snapshot, only ever used for a failure-log message -
        // the commit itself (crate::settle_pool::commit, via GenerationSlot::resolve_or_defer)
        // always resolves its actual target fresh, by id, regardless of whether this snapshot is
        // still accurate by the time it runs (DESIGN-MOUNT-015's fix, covering a chain of any
        // depth) - so every generation is submitted unconditionally, with nothing to gate on here.
        let (parent_id, name) = self
            .repo
            .parent_and_name(file_id)
            .ok()
            .flatten()
            .unwrap_or_default();
        self.pool.submit(SettleJob {
            parent_id,
            name,
            time_millis: now_millis(),
            generation,
        });
    }

    /// Appends REQ-TREE-009's `[deleted]` marker entry to `result` if `parent_id` has any
    /// soft-deleted children - `readdir`'s own convention for both a live directory and a
    /// soft-deleted directory's synthetic view (REQ-TREE-008: the latter's children are always
    /// themselves soft-deleted, so the same marker-if-nonempty rule applies one level down).
    /// Mirrors `crate::list.rs`'s established convention for the same marker.
    fn push_deleted_marker(&self, result: &mut Vec<DirEntry>, parent_id: i64) -> Result<(), Errno> {
        let children = self
            .repo
            .list_deleted_children(parent_id)
            .map_err(|e| self.to_errno_reporting_connection_death(e))?;
        if !children.is_empty() {
            result.push(DirEntry {
                name: deleted::DELETED_SEGMENT.to_string(),
                kind: FileKind::Directory,
            });
        }
        Ok(())
    }

    /// `readdir` for [`MountPath::DeletedChildren`]: `parent_id`'s own soft-deleted children under
    /// REQ-TREE-009's disambiguated names, plus REQ-MOUNT-008's `[time]` marker so its own
    /// chronological presentation of the same data stays discoverable by browsing.
    fn readdir_deleted_children(&self, parent_id: i64) -> Result<Vec<DirEntry>, Errno> {
        let children = self
            .repo
            .list_deleted_children(parent_id)
            .map_err(|e| self.to_errno_reporting_connection_death(e))?;
        let mut result: Vec<DirEntry> =
            deleted::display_names_within(&children, mountfs::MAX_NAME_BYTES)
                .into_iter()
                .zip(&children)
                .map(|(name, (_, entry))| DirEntry {
                    name,
                    kind: kind_to_mountfs(entry.entry.kind),
                })
                .collect();
        result.push(DirEntry {
            name: deleted::TIME_SEGMENT.to_string(),
            kind: FileKind::Directory,
        });
        Ok(result)
    }

    /// `readdir` for [`MountPath::TimeChildren`]: the same children as
    /// [`Self::readdir_deleted_children`], under REQ-MOUNT-008's always-timestamp-prefixed names
    /// instead - no further synthetic entries, since `[time]` is not itself nested any deeper.
    fn readdir_time_children(&self, parent_id: i64) -> Result<Vec<DirEntry>, Errno> {
        let children = self
            .repo
            .list_deleted_children(parent_id)
            .map_err(|e| self.to_errno_reporting_connection_death(e))?;
        Ok(
            deleted::timestamped_display_names(&children, Some(mountfs::MAX_NAME_BYTES))
                .into_iter()
                .zip(&children)
                .map(|(name, (_, entry))| DirEntry {
                    name,
                    kind: kind_to_mountfs(entry.entry.kind),
                })
                .collect(),
        )
    }
}

impl MountFilesystem for DedupFs {
    fn getattr(&self, path: &str) -> Result<Attr, Errno> {
        match self.resolve_mount_path_required(path)? {
            MountPath::Live(entry) => {
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
            MountPath::DeletedChildren { parent_id } | MountPath::TimeChildren { parent_id } => {
                self.synthetic_dir_attr(parent_id)
            }
            // REQ-MOUNT-008: `st_mtime` is always the entry's own real, stored modification time,
            // never the deletion time, in either presentation - `entry.time_millis` already is.
            MountPath::Deleted(entry) => Ok(Attr {
                kind: kind_to_mountfs(entry.entry.kind),
                size: entry.entry.size,
                mtime_millis: entry.entry.time_millis,
            }),
        }
    }

    fn readdir(&self, path: &str) -> Result<Vec<DirEntry>, Errno> {
        match self.resolve_mount_path_required(path)? {
            MountPath::Live(entry) => {
                if entry.kind != db::EntryKind::Dir {
                    return Err(Errno::ENOTDIR);
                }
                let children = self
                    .repo
                    .list_children(entry.id)
                    .map_err(|e| self.to_errno_reporting_connection_death(e))?;
                // REQ-TREE-009: a real live entry already named `[deleted]` wins outright - it is
                // already in `children` above, so the marker below is only added when nothing
                // real occupies that name yet (matches `crate::list.rs`'s own convention).
                let already_real = children
                    .iter()
                    .any(|(name, _)| name == deleted::DELETED_SEGMENT);
                let mut result: Vec<DirEntry> = children
                    .into_iter()
                    .map(|(name, child)| DirEntry {
                        name,
                        kind: kind_to_mountfs(child.kind),
                    })
                    .collect();
                if self.show_deleted && !already_real {
                    self.push_deleted_marker(&mut result, entry.id)?;
                }
                Ok(result)
            }
            MountPath::DeletedChildren { parent_id } => self.readdir_deleted_children(parent_id),
            MountPath::TimeChildren { parent_id } => self.readdir_time_children(parent_id),
            MountPath::Deleted(entry) => {
                if entry.entry.kind != db::EntryKind::Dir {
                    return Err(Errno::ENOTDIR);
                }
                // REQ-TREE-008: a soft-deleted directory's own children are always themselves
                // soft-deleted, so browsing further one level down works the same way the base
                // `[deleted]` marker does for a live directory - never any live children to list.
                let mut result = Vec::new();
                self.push_deleted_marker(&mut result, entry.entry.id)?;
                Ok(result)
            }
        }
    }

    fn open(&self, path: &str, write_intent: bool) -> Result<Handle, Errno> {
        match self.resolve_mount_path_required(path)? {
            MountPath::Live(entry) => {
                if entry.kind == db::EntryKind::Dir {
                    return Err(Errno::EISDIR);
                }
                if write_intent {
                    self.require_read_write()?;
                    self.require_not_degraded()?;
                }
                // Every open counts toward the same handle count, read or write intent alike - a
                // lingering reader delays a written generation's hand-off to the settle pool,
                // which only costs latency, not correctness (DESIGN-MOUNT-007 keeps its content
                // visible regardless).
                self.pending.open(entry.id);
                Ok(Handle(entry.id as u64))
            }
            MountPath::DeletedChildren { .. } | MountPath::TimeChildren { .. } => {
                Err(Errno::EISDIR)
            }
            MountPath::Deleted(entry) => {
                if entry.entry.kind == db::EntryKind::Dir {
                    return Err(Errno::EISDIR);
                }
                if write_intent {
                    return Err(Errno::EACCES);
                }
                // Deliberately not registered with `self.pending` - a soft-deleted entry's
                // content never changes, so there is nothing for `write`/`truncate`/`release` to
                // track here (`release`'s underlying `PendingFiles::release` already safely
                // no-ops for a `file_id` never registered via `pending.open`).
                Ok(Handle(entry.entry.id as u64))
            }
        }
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
        let live_entry = self
            .repo
            .entry_by_id(file_id)
            .map_err(|e| self.to_errno_reporting_connection_death(e))?;
        // A `Handle` opened against a `MountPath::Deleted` entry (REQ-MOUNT-004) never registers
        // with `self.pending` (see `open`'s own doc comment), so its content is only ever found
        // here, not above - falls back to the soft-deleted lookup when the live one comes up
        // empty.
        let content_id = match live_entry {
            Some(entry) => entry.content_id,
            None => {
                let deleted_entry = self
                    .repo
                    .deleted_entry_by_id(file_id)
                    .map_err(|e| self.to_errno_reporting_connection_death(e))?
                    .ok_or(Errno::EIO)?;
                deleted_entry.entry.content_id
            }
        }
        .expect("kind=File entries always have a content_id (chk_tree_entries_kind_content_id)");
        crate::content_reader::read_content(&self.repo, &self.store, content_id, offset, size)
    }

    fn release(&self, handle: Handle) {
        self.release_and_maybe_submit(handle.0 as i64);
    }

    fn statfs(&self) -> Result<StatfsInfo, Errno> {
        let block_size: u32 = 512;
        let (total_bytes, available_bytes) =
            mountfs::disk_space(&self.data_dir).map_err(|_| Errno::EIO)?;
        Ok(StatfsInfo {
            blocks: total_bytes / block_size as u64,
            blocks_free: available_bytes / block_size as u64,
            blocks_available: available_bytes / block_size as u64,
            block_size,
            max_name_length: mountfs::MAX_NAME_BYTES as u32,
            ..Default::default()
        })
    }

    fn mkdir(&self, path: &str) -> Result<(), Errno> {
        self.require_read_write()?;
        let (parent_path, name) = split_path(path)?;
        let parent = self.resolve_live_parent(parent_path)?;
        self.repo
            .mkdir(parent.id, name, now_millis())
            .map_err(|e| self.to_errno_reporting_connection_death(e))?;
        Ok(())
    }

    fn create(&self, path: &str) -> Result<Handle, Errno> {
        self.require_read_write()?;
        self.require_not_degraded()?;
        let (parent_path, name) = split_path(path)?;
        let parent = self.resolve_live_parent(parent_path)?;
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
        match self.resolve_mount_path_required(path)? {
            MountPath::Live(entry) => {
                if entry.kind != db::EntryKind::File {
                    return Err(Errno::EISDIR);
                }
                self.repo
                    .unlink_file(entry.id, now_millis())
                    .map_err(|e| self.to_errno_reporting_connection_death(e))
            }
            // REQ-MOUNT-007: the view itself is never a delete target.
            MountPath::DeletedChildren { .. } | MountPath::TimeChildren { .. } => {
                Err(Errno::EACCES)
            }
            MountPath::Deleted(entry) => {
                if entry.entry.kind != db::EntryKind::File {
                    return Err(Errno::EISDIR);
                }
                if !self.allow_purge {
                    return Err(Errno::EACCES);
                }
                self.repo
                    .purge_deleted_entry(entry.entry.id, false)
                    .map(|_| ())
                    .map_err(|e| self.to_errno_reporting_connection_death(e))
            }
        }
    }

    fn rmdir(&self, path: &str) -> Result<(), Errno> {
        self.require_read_write()?;
        match self.resolve_mount_path_required(path)? {
            MountPath::Live(entry) => self
                .repo
                .rmdir(entry.id, now_millis())
                .map_err(|e| self.to_errno_reporting_connection_death(e)),
            // REQ-MOUNT-007: the view itself is never a delete target.
            MountPath::DeletedChildren { .. } | MountPath::TimeChildren { .. } => {
                Err(Errno::EACCES)
            }
            MountPath::Deleted(entry) => {
                if entry.entry.kind != db::EntryKind::Dir {
                    return Err(Errno::ENOTDIR);
                }
                if !self.allow_purge {
                    return Err(Errno::EACCES);
                }
                // REQ-MOUNT-007's non-recursive refusal: `purge_deleted_entry(_, false)` refuses
                // `ENOTEMPTY` while soft-deleted children remain, matching ordinary `rmdir`'s own
                // "target must be empty" contract rather than `dfs del --purge`'s recursive one.
                self.repo
                    .purge_deleted_entry(entry.entry.id, false)
                    .map(|_| ())
                    .map_err(|e| self.to_errno_reporting_connection_death(e))
            }
        }
    }

    fn rename(&self, old_path: &str, new_path: &str, no_replace: bool) -> Result<(), Errno> {
        self.require_read_write()?;
        let (new_parent_path, new_name) = split_path(new_path)?;
        match self.resolve_mount_path_required(old_path)? {
            // REQ-MOUNT-007: renaming the view itself is always refused, under either opt-in.
            MountPath::DeletedChildren { .. } | MountPath::TimeChildren { .. } => {
                Err(Errno::EACCES)
            }
            // REQ-MOUNT-004's recovery move-out: `resolve_live_parent` below already refuses
            // (`EACCES`) a `new_path` that resolves into or within the view itself, so this is
            // never reached for anything but a genuine move into the live tree.
            MountPath::Deleted(entry) => {
                let new_parent = self.resolve_live_parent(new_parent_path)?;
                self.repo
                    .recover_deleted_entry(
                        entry.entry.id,
                        new_parent.id,
                        new_name,
                        no_replace,
                        now_millis(),
                    )
                    .map_err(|e| self.to_errno_reporting_connection_death(e))
            }
            MountPath::Live(_) => {
                let (old_parent_path, old_name) = split_path(old_path)?;
                let old_parent = self.resolve_live_parent(old_parent_path)?;
                let new_parent = self.resolve_live_parent(new_parent_path)?;
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
        }
    }

    fn utimens(&self, path: &str, mtime_millis: i64) -> Result<(), Errno> {
        self.require_read_write()?;
        match self.resolve_mount_path_required(path)? {
            MountPath::Live(entry) => self
                .repo
                .set_mtime(entry.id, mtime_millis)
                .map_err(|e| self.to_errno_reporting_connection_death(e)),
            // REQ-MOUNT-007: nothing mutating is allowed against the view beyond the recovery
            // move-out (`rename`) and, under the second opt-in, purging (`unlink`/`rmdir`).
            MountPath::DeletedChildren { .. }
            | MountPath::TimeChildren { .. }
            | MountPath::Deleted(_) => Err(Errno::EACCES),
        }
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
        let entry = match self.resolve_mount_path_required(path)? {
            MountPath::Live(entry) => entry,
            // REQ-MOUNT-007: nothing mutating is allowed against the view beyond the recovery
            // move-out (`rename`) and, under the second opt-in, purging (`unlink`/`rmdir`).
            MountPath::DeletedChildren { .. }
            | MountPath::TimeChildren { .. }
            | MountPath::Deleted(_) => return Err(Errno::EACCES),
        };
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
            show_deleted: false,
            allow_purge: false,
        }
    }

    /// `fs` and `verify_repo`/`verify_store` point at the same repository, opened separately -
    /// `fs` owns the connection actually driving the mount, `verify_repo`/`verify_store` let a
    /// test inspect what a background settle job eventually commits, which `release`/`truncate`
    /// deliberately never wait for (DESIGN-MOUNT-006).
    fn setup(read_write: bool) -> (DedupFs, db::Repository, store::ByteStore, tempfile::TempDir) {
        setup_with_tuning(read_write, default_tuning())
    }

    /// Like [`setup`], but with REQ-MOUNT-004/007's `show_deleted`/`allow_purge` opt-ins
    /// (otherwise off in [`default_tuning`]) explicitly chosen.
    fn setup_deleted_view(
        read_write: bool,
        show_deleted: bool,
        allow_purge: bool,
    ) -> (DedupFs, db::Repository, store::ByteStore, tempfile::TempDir) {
        setup_with_tuning(
            read_write,
            Tuning {
                show_deleted,
                allow_purge,
                ..default_tuning()
            },
        )
    }

    fn setup_with_tuning(
        read_write: bool,
        tuning: Tuning,
    ) -> (DedupFs, db::Repository, store::ByteStore, tempfile::TempDir) {
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
        let fs = DedupFs::new(fs_repo, fs_store, read_write, &repo_root, None, tuning).unwrap();
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
    fn statfs_reports_the_repository_data_dir_s_real_free_space() {
        let (fs, _verify_repo, _store, _dir) = setup(true);
        let info = fs.statfs().unwrap();
        assert!(info.blocks > 0, "blocks={}", info.blocks);
        assert!(
            info.blocks_available > 0,
            "blocks_available={}",
            info.blocks_available
        );
        assert_eq!(info.blocks_free, info.blocks_available);
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
    /// ready to serve requests.
    ///
    /// Readiness is detected by polling `mount_path`'s own device number (`stat(2)`'s `st_dev`)
    /// until it differs from what it was before the mount thread was spawned, rather than by a
    /// write attempt succeeding - `mount_path` is a real, already-existing directory (from
    /// `tempfile::tempdir()`), and a write against it can succeed trivially against that
    /// underlying directory itself, before libfuse's `mount(2)` call has actually attached over
    /// it - found the hard way: an earlier version of this helper used exactly such a write-based
    /// probe, which reliably reported "ready" while `st_dev` still matched the pre-mount value,
    /// confirmed via a still-failing `fusermount3 -u` immediately after ("entry ... not found in
    /// /etc/mtab"). Once `st_dev` actually changes, every operation against `mount_path` is
    /// necessarily routed through FUSE, so no separate write-based check is needed on top of it.
    ///
    /// Linux-only, like its two callers below: unmounting relies on `fusermount3`
    /// (`unmount_and_join`), which has no Windows equivalent this project uses, and
    /// `crates/mountfs/src/windows/mod.rs` has no working in-process clean-shutdown call at all -
    /// see the callers' own `#[cfg(target_os = "linux")]` for the full reasoning.
    #[cfg(target_os = "linux")]
    fn mount_for_test(
        fs: DedupFs,
        mount_path: &std::path::Path,
    ) -> thread::JoinHandle<io::Result<()>> {
        use std::os::unix::fs::MetadataExt;
        let dev_before_mount = std::fs::metadata(mount_path)
            .expect("mount_path must exist before mounting")
            .dev();
        let handle = {
            let mount_path = mount_path.to_path_buf();
            thread::spawn(move || mountfs::mount(fs, &mount_path, false))
        };
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let dev_now = std::fs::metadata(mount_path)
                .expect("mount_path must remain statable while waiting for the mount")
                .dev();
            if dev_now != dev_before_mount {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "mount did not become ready within 5s (requires /dev/fuse access)"
            );
            thread::sleep(Duration::from_millis(5));
        }
        handle
    }

    #[cfg(target_os = "linux")]
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
    #[cfg(target_os = "linux")]
    fn timed_synced_write(path: &std::path::Path, payload: &[u8]) -> Duration {
        let start = Instant::now();
        let mut file = std::fs::File::create(path).expect("create against the mount must succeed");
        std::io::Write::write_all(&mut file, payload)
            .expect("write against the mount must succeed");
        file.sync_all()
            .expect("sync_all against the mount must succeed");
        start.elapsed()
    }

    // `unmount_and_join` above unconditionally calls `fusermount3`, a Linux-only tool with no
    // Windows equivalent this project uses - and, unlike a `real_mount_` test that fails cleanly
    // at runtime on a platform it was not written for (the accepted, `--skip real_mount`-handled
    // case), this test mounts on an in-process background thread with no way to unmount it again
    // on Windows at all: `crates/mountfs/src/windows/mod.rs` has no working in-process
    // clean-shutdown call (see `windows_mount_spike_helper.rs`'s own doc comment - Windows-specific
    // real-mount tests use a separate child process, killed via `Child::kill`, specifically
    // because of this). Confirmed on real Windows/WinFSP: running this test there leaves its mount
    // thread permanently blocked inside `mountfs::mount()` (never reaching `unmount_and_join`
    // either way, since the test panics at `wait_for_settled` first) - which then leaves WinFSP
    // itself in a state bad enough that even a *subsequent, unrelated* mount attempt starts
    // failing with "mount point in use". Gated the same way its sibling test above already is, and
    // for the same underlying reason (see that test's own comment) - this is a structural
    // Linux-only dependency, not a timing or backpressure-formula bug worth chasing further on
    // Windows as currently written.
    #[cfg(target_os = "linux")]
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
        assert!(fs.resolve_mount_path_required("/a.txt").is_err());
    }

    /// Creates, releases, and soft-deletes (unlinks) a file named `a.txt` at the repository root -
    /// the common starting point for the REQ-MOUNT-004/007/008 tests below.
    fn create_and_delete_a_file(fs: &DedupFs, verify_repo: &db::Repository) {
        let handle = fs.create("/a.txt").unwrap();
        fs.write(handle, 0, b"hello").unwrap();
        fs.release(handle);
        wait_for_settled(verify_repo, "/a.txt", 5);
        fs.unlink("/a.txt").unwrap();
    }

    #[test]
    fn the_deleted_view_is_entirely_inert_without_the_show_deleted_opt_in() {
        let (fs, verify_repo, _store, _dir) = setup(true);
        create_and_delete_a_file(&fs, &verify_repo);

        let root = fs.readdir("/").unwrap();
        assert!(
            !root.iter().any(|e| e.name == deleted::DELETED_SEGMENT),
            "the [deleted] marker must not appear when show_deleted is off"
        );
        assert!(fs.getattr("/[deleted]").is_err());
        assert!(fs.getattr("/[deleted]/a.txt").is_err());
    }

    #[test]
    fn show_deleted_reveals_the_marker_and_lists_soft_deleted_children() {
        let (fs, verify_repo, _store, _dir) = setup_deleted_view(true, true, false);
        create_and_delete_a_file(&fs, &verify_repo);

        let root = fs.readdir("/").unwrap();
        let marker = root
            .iter()
            .find(|e| e.name == deleted::DELETED_SEGMENT)
            .expect("the [deleted] marker must appear once there is deletion history");
        assert_eq!(marker.kind, FileKind::Directory);

        let view = fs.readdir("/[deleted]").unwrap();
        assert!(view.iter().any(|e| e.name == deleted::TIME_SEGMENT));
        let entry = view
            .iter()
            .find(|e| e.name == "a.txt")
            .expect("the soft-deleted file must be listed under its own (unambiguous) name");
        assert_eq!(entry.kind, FileKind::File);

        let attr = fs.getattr("/[deleted]/a.txt").unwrap();
        assert_eq!(attr.kind, FileKind::File);
        assert_eq!(attr.size, 5);
    }

    #[test]
    fn deleted_entries_are_readable_through_the_view() {
        let (fs, verify_repo, _store, _dir) = setup_deleted_view(true, true, false);
        create_and_delete_a_file(&fs, &verify_repo);

        let handle = fs.open("/[deleted]/a.txt", false).unwrap();
        let data = fs.read(handle, 0, 5).unwrap();
        assert_eq!(data, b"hello");
        fs.release(handle);
    }

    #[test]
    fn write_intent_against_a_deleted_entry_is_refused() {
        let (fs, verify_repo, _store, _dir) = setup_deleted_view(true, true, false);
        create_and_delete_a_file(&fs, &verify_repo);

        assert_eq!(
            fs.open("/[deleted]/a.txt", true).unwrap_err(),
            Errno::EACCES
        );
    }

    #[test]
    fn the_time_view_lists_the_same_entry_with_its_deletion_timestamp_prefixed() {
        let (fs, verify_repo, _store, _dir) = setup_deleted_view(true, true, false);
        create_and_delete_a_file(&fs, &verify_repo);

        let view = fs.readdir("/[deleted]/[time]").unwrap();
        assert_eq!(view.len(), 1);
        assert!(view[0].name.ends_with("a.txt"));
        assert_ne!(view[0].name, "a.txt");
    }

    #[test]
    fn recovery_via_rename_moves_a_deleted_entry_back_to_the_live_tree() {
        let (fs, verify_repo, _store, _dir) = setup_deleted_view(true, true, false);
        create_and_delete_a_file(&fs, &verify_repo);

        fs.rename("/[deleted]/a.txt", "/restored.txt", false)
            .unwrap();

        let attr = fs.getattr("/restored.txt").unwrap();
        assert_eq!(attr.kind, FileKind::File);
        assert_eq!(attr.size, 5);
        assert!(
            fs.readdir("/[deleted]")
                .unwrap()
                .iter()
                .all(|e| e.name == deleted::TIME_SEGMENT),
            "only the always-present [time] marker should remain once a.txt is gone"
        );
    }

    #[test]
    fn renaming_the_view_itself_is_always_refused() {
        let (fs, verify_repo, _store, _dir) = setup_deleted_view(true, true, true);
        create_and_delete_a_file(&fs, &verify_repo);

        assert_eq!(
            fs.rename("/[deleted]", "/somewhere", false).unwrap_err(),
            Errno::EACCES
        );
    }

    #[test]
    fn moving_a_live_entry_into_the_view_is_refused() {
        let (fs, verify_repo, _store, _dir) = setup_deleted_view(true, true, false);
        create_and_delete_a_file(&fs, &verify_repo);
        let handle = fs.create("/b.txt").unwrap();
        fs.release(handle);
        wait_for_settled(&verify_repo, "/b.txt", 0);

        assert_eq!(
            fs.rename("/b.txt", "/[deleted]/b.txt", false).unwrap_err(),
            Errno::EACCES
        );
    }

    #[test]
    fn purge_is_refused_without_the_second_opt_in() {
        let (fs, verify_repo, _store, _dir) = setup_deleted_view(true, true, false);
        create_and_delete_a_file(&fs, &verify_repo);

        assert_eq!(fs.unlink("/[deleted]/a.txt").unwrap_err(), Errno::EACCES);
    }

    #[test]
    fn purge_permanently_removes_the_entry_under_the_second_opt_in() {
        let (fs, verify_repo, _store, _dir) = setup_deleted_view(true, true, true);
        create_and_delete_a_file(&fs, &verify_repo);

        fs.unlink("/[deleted]/a.txt").unwrap();
        assert!(fs.getattr("/[deleted]/a.txt").is_err());
        assert!(
            fs.readdir("/[deleted]")
                .unwrap()
                .iter()
                .all(|e| e.name == deleted::TIME_SEGMENT),
            "only the always-present [time] marker should remain once a.txt is gone"
        );
    }

    #[test]
    fn rmdir_on_the_view_itself_is_always_refused() {
        let (fs, verify_repo, _store, _dir) = setup_deleted_view(true, true, true);
        create_and_delete_a_file(&fs, &verify_repo);

        assert_eq!(fs.rmdir("/[deleted]").unwrap_err(), Errno::EACCES);
    }

    #[test]
    fn rmdir_of_a_deleted_directory_refuses_ontop_of_soft_deleted_children_non_recursively() {
        let (fs, verify_repo, _store, _dir) = setup_deleted_view(true, true, true);
        fs.mkdir("/dir").unwrap();
        let handle = fs.create("/dir/child.txt").unwrap();
        fs.release(handle);
        wait_for_settled(&verify_repo, "/dir/child.txt", 0);
        fs.unlink("/dir/child.txt").unwrap();
        fs.rmdir("/dir").unwrap();

        assert_eq!(fs.rmdir("/[deleted]/dir").unwrap_err(), Errno::ENOTEMPTY);
    }

    #[test]
    fn mkdir_and_create_refuse_a_parent_inside_the_view() {
        let (fs, verify_repo, _store, _dir) = setup_deleted_view(true, true, false);
        create_and_delete_a_file(&fs, &verify_repo);

        assert_eq!(fs.mkdir("/[deleted]/sub").unwrap_err(), Errno::EACCES);
        assert_eq!(fs.create("/[deleted]/new.txt").unwrap_err(), Errno::EACCES);
    }

    #[test]
    fn utimens_and_truncate_refuse_a_target_inside_the_view() {
        let (fs, verify_repo, _store, _dir) = setup_deleted_view(true, true, false);
        create_and_delete_a_file(&fs, &verify_repo);

        assert_eq!(
            fs.utimens("/[deleted]/a.txt", 123).unwrap_err(),
            Errno::EACCES
        );
        assert_eq!(
            fs.truncate("/[deleted]/a.txt", 0).unwrap_err(),
            Errno::EACCES
        );
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
