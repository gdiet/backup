//! FUSE/WinFSP mount (`backup mount <mountpoint>`, read-only or
//! `--read-write`) - see `docs/plans/implemented/04-fuse-mount-readonly.md`
//! for the original read-only design, `docs/plans/implemented/
//! 05-cross-platform-mount-crate.md` for why this goes through the
//! platform-abstracted `mountfs` crate (real libfuse3 on Linux, real
//! WinFSP on Windows) instead of `fuser`'s Linux-only, low-level
//! `/dev/fuse` protocol, and `docs/plans/implemented/
//! 06-fuse-mount-readwrite.md` for the read-write phases (structural ops,
//! then content writes) built on top of it.
//!
//! Every [`mountfs::MountFilesystem`] method is answerable with functions
//! the other commands already use (`db::resolve_path`, `db::list_children`,
//! `db::file_size`, `db::ordered_content_chunks`, and `chunk_store`'s
//! multi-part-aware chunk reader) - this module is almost entirely wiring
//! those up to `mountfs`'s trait, not new logic. `mountfs`'s API is
//! path-based (matching `db::resolve_path`), so unlike the old `fuser`
//! version there's no inode-number bookkeeping here at all.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::thread::JoinHandle;

use cdc::ChunkerConfig;
use clap::Args;
use mountfs::{Attr, DirEntry, Errno, FileKind, Handle, MountFilesystem};
use rusqlite::Connection;
use store::{LongTermStore, ReadIntegrity};

use crate::chunk_store::{self, SpaceAllocator, read_chunk_bytes};
use crate::mount_deleted::{self, DeletedResolution};
use crate::ram_budget_check::check_ram_budget;
use crate::repo_lock;
use crate::spilling_chunker::{SpilledChunk, SpillingHashingChunker};
use crate::store::{Blake3Hasher, HASH_LENGTH};
use crate::temp_dir::{create_spill_dir, validate_temp_dir};
use spillcache::{RamBudget, WriteCache};

/// Default RAM budget for `backup mount --read-write`'s write cache (see
/// `MountArgs::write_cache_mb`), shared across every file open for writing
/// at once, *and* reused as the budget for in-flight persist chunk
/// buffering (see [`Inner::persist`]) - a modest default: both are soft
/// buffers that spill to disk once exceeded (see `spillcache::
/// WriteCache`), not something that needs to be generous to work
/// correctly, and `check_ram_budget` (called from `run_mount`) refuses to
/// start if even this much risks swapping.
const DEFAULT_WRITE_CACHE_MB: u64 = 128;

/// Content is read from/persisted to the write cache in pieces this large
/// at a time (during `write`'s lazy materialization and during `release`'s
/// persist pipeline) - bounds peak memory use for a single file regardless
/// of its total size, mirroring `store.rs`'s own `READ_BUFFER_SIZE`
/// streaming loop.
const PERSIST_CHUNK_SIZE: u64 = 256 * 1024;

/// How long [`Inner::enqueue_persist`] sleeps between checks while waiting
/// for queued persist bytes to drop back under
/// [`Inner::spill_backpressure_threshold_bytes`] - a plain poll rather
/// than a wake-based wait, since this check only runs once per `release`/
/// bare `truncate` (not a hot path), making the periodic-wakeup cost
/// negligible - and it avoids threading a notification channel from
/// `spillcache` (a low-level, mount-unaware crate) back into this
/// mount-specific policy.
const SPILL_BACKPRESSURE_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(10);

#[derive(Args)]
pub struct MountArgs {
    /// Directory to mount the repository's file tree at.
    ///
    /// On Linux, must already exist and be empty (FUSE mounts onto an
    /// existing mountpoint). On Windows, must *not* already exist (WinFSP
    /// creates it itself as part of mounting).
    mountpoint: PathBuf,

    /// Allow structural changes through the mount: `mkdir`/`rmdir`/
    /// `unlink`/`rename`/creating and writing files/touching timestamps
    /// (see `docs/plans/implemented/06-fuse-mount-readwrite.md`). Off by default: a mount
    /// is a much larger blast radius for a mistake (an editor autosave, a
    /// stray `rm -rf`, a build tool scribbling into it) than `store`/
    /// `restore`. Exclusive against `store`/`compact-store`/`reclaim-space`
    /// (or a second `--read-write` mount) for the mount's whole lifetime,
    /// enforced automatically via the repository's lock file - see
    /// `--lock-wait` and `docs/plans/cross-process-repository-locking.md`.
    /// `del`/`undelete`/`fix-problems`/`db compact` remain safe to run
    /// concurrently (metadata-only, no physical store-byte conflict).
    #[arg(short = 'w', long)]
    read_write: bool,

    /// RAM budget, in megabytes, for buffering in-progress writes before
    /// they're persisted to the store - shared across every file
    /// currently open for writing. A file's writes that exceed this
    /// budget spill to a temp file instead of failing (see
    /// `docs/plans/implemented/06-fuse-mount-readwrite.md`'s "Phase 2b" section). Only
    /// meaningful with `--read-write`.
    #[arg(long, default_value_t = DEFAULT_WRITE_CACHE_MB)]
    write_cache_mb: u64,

    /// Start anyway if `--write-cache-mb` looks large enough, relative to
    /// currently available RAM, to risk pushing the machine into swapping
    /// (see `check_write_cache_budget`) - without this, that condition is
    /// a startup error, not just a warning, since swapping is a
    /// machine-wide problem, not just a slowdown local to the mount. Only
    /// meaningful with `--read-write`.
    #[arg(long)]
    allow_swap_risk: bool,

    /// Directory to create this mount's write-cache spillover directory in
    /// (see `--write-cache-mb`) - must already exist and be writable.
    /// Defaults to the OS temp directory (`std::env::temp_dir()`) if not
    /// given. For best throughput, point this at the fastest disk
    /// available, ideally not the same physical drive as the repository.
    /// Only meaningful with `--read-write`.
    #[arg(long)]
    temp: Option<PathBuf>,

    /// Serve missing or short store data as zero bytes for exactly the
    /// affected range, instead of failing that read with an I/O error. Off
    /// by default (see docs/plans/mount-zero-fill-missing.md for why): once
    /// this is on, a reader has no way to tell zero-filled bytes from real
    /// ones, so only turn it on when you specifically want best-effort
    /// access to a file you already know is affected (e.g. via `backup
    /// problems`) rather than a hard failure.
    #[arg(long)]
    zero_fill_missing: bool,

    /// How long to wait, in seconds, for the repository's lock to become
    /// free if another `store`/`mount --read-write`/`compact-store`/
    /// `reclaim-space` run already holds it, before giving up. Default:
    /// don't wait, fail immediately. Only meaningful with `--read-write`.
    #[arg(long = "lock-wait", default_value_t = 0)]
    lock_wait_secs: u64,
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
    if let Some(temp) = &args.temp
        && let Err(msg) = validate_temp_dir(temp)
    {
        eprintln!("error: {msg}");
        return ExitCode::FAILURE;
    }
    if args.read_write
        && let Err(msg) =
            check_ram_budget("write-cache-mb", args.write_cache_mb, args.allow_swap_risk)
    {
        eprintln!("error: {msg}");
        return ExitCode::FAILURE;
    }

    let fs = match build_filesystem(
        repo,
        args.read_write,
        args.write_cache_mb,
        args.temp.as_deref(),
        args.zero_fill_missing,
        std::time::Duration::from_secs(args.lock_wait_secs),
    ) {
        Ok(fs) => fs,
        Err(msg) => {
            eprintln!("error: {msg}");
            return ExitCode::FAILURE;
        }
    };

    if let Err(err) = mountfs::preflight() {
        eprintln!("error: {err}");
        return ExitCode::FAILURE;
    }

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
    if args.zero_fill_missing {
        println!(
            "zero-fill-missing enabled: files with missing or short store data \
             will read as zero-filled instead of failing with an I/O error"
        );
    }
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
fn build_filesystem(
    repo: &Path,
    read_write: bool,
    write_cache_mb: u64,
    temp: Option<&Path>,
    zero_fill_missing: bool,
    lock_wait: std::time::Duration,
) -> Result<DedupFs, String> {
    // A read-only mount (see docs/plans/read-only-repository-access.md)
    // must never open a read-write connection - the repository directory
    // may genuinely be read-only on disk (e.g. bind-mounted `:ro`).
    let repository = if read_write {
        db::open_repository(repo)
    } else {
        db::open_repository_read_only(repo)
    }
    .map_err(|err| format!("failed to open repository at {}: {err}", repo.display()))?;

    // Exclusive against every other command that physically
    // allocates/relocates store bytes (`store`, `compact-store`,
    // `reclaim-space`, or a second `--read-write` mount) - see
    // `docs/plans/cross-process-repository-locking.md`. `None` for a
    // read-only mount: it never touches store bytes, so it stays lock-free
    // like every other read-only command (and must, to keep working
    // against a genuinely `:ro`-mounted repository directory).
    let repo_lock = if read_write {
        match repo_lock::RepoLock::acquire(&db::meta_dir(repo), lock_wait) {
            Ok(Some(lock)) => Some(lock),
            Ok(None) => {
                return Err(
                    "another command is already running against this repository \
                     (meta/.lock is held) - try again once it finishes, or pass \
                     --lock-wait to wait"
                        .to_string(),
                );
            }
            Err(err) => return Err(format!("failed to acquire the repository lock: {err}")),
        }
    } else {
        None
    };

    let conn = repository
        .open_read_connection()
        .map_err(|err| format!("failed to open the metadata database: {err}"))?;
    // `None` for a read-only mount - see `Inner::write_conn`'s doc comment.
    let write_conn =
        if read_write {
            Some(repository.open_write_connection().map_err(|err| {
                format!("failed to open the metadata database for writing: {err}")
            })?)
        } else {
            None
        };
    // Seeded once, like `store`'s own allocator - reused across every
    // persist for this mount's whole lifetime (not just one command's). Uses
    // the always-open read connection, not `write_conn` (`None` for a
    // read-only mount anyway) - a plain read, no reason to need more.
    let extents = db::chunk_extents_sorted(&conn)
        .map_err(|err| format!("failed to determine free store space: {err}"))?;
    let allocator = SpaceAllocator::from_sorted_extents(&extents);
    let chunker_config = ChunkerConfig::new(match repository.settings().chunking() {
        db::Chunking::Cdc => Some(repository.settings().cdc_target_size_bits()),
        db::Chunking::None => None,
    })
    .expect("validated by RepositorySettings");
    let data_dir = repository.data_dir();
    // `read_only` mirrors the `--read-write` flag, not hardcoded `true`
    // like the read-only-only phase used: a read-write mount's persist
    // pipeline needs to actually write new chunk bytes to the store.
    let data_store = LongTermStore::new(&data_dir, !read_write);
    // A dedicated, uniquely-named spill directory for write-cache overflow
    // (see `spillcache::WriteCache`) - created empty here, removed whole
    // in `Inner::on_unmount` once every spill file in it (each deleted by
    // its own `WriteCache`'s `Drop`) is gone. Under `--temp` if given
    // (already validated by `run_mount`), otherwise under the OS default -
    // see `create_spill_dir`'s doc comment for why this goes through
    // `tempfile::Builder` rather than `std::process::id()`.
    let spill_dir = create_spill_dir("backup-mount-write-cache-", temp)
        .map_err(|err| format!("failed to create write-cache temp dir: {err}"))?;

    // The persist queue and its background thread (see `persist_worker`)
    // - `Inner` is wrapped in `Arc` (via `DedupFs`) specifically so this
    // thread and the FUSE/WinFSP dispatch threads can share the same
    // state safely. Unbounded: the actual backpressure point moved from
    // channel capacity to `enqueue_persist`'s own byte-based wait (see
    // `Inner::spill_backpressure_threshold_bytes`), so nothing should ever
    // need to block *inside* `send` itself any more.
    let (persist_tx, persist_rx) = mpsc::channel::<PersistJob>();
    let inner = Arc::new(Inner {
        read_only: !read_write,
        zero_fill_missing,
        _repo_lock: repo_lock,
        conn: Mutex::new(conn),
        write_conn: Mutex::new(write_conn),
        data_store,
        allocator,
        chunker_config,
        ram_budget: Arc::new(RamBudget::new(write_cache_mb * 1024 * 1024)),
        spill_dir,
        spill_id_seq: AtomicU64::new(0),
        write_states: Mutex::new(HashMap::new()),
        write_states_cv: Condvar::new(),
        persist_tx: Mutex::new(Some(persist_tx)),
        persist_thread: Mutex::new(None),
        queued_persist_bytes: AtomicU64::new(0),
        spill_backpressure_threshold_bytes: write_cache_mb * 1024 * 1024,
        disk_space: Mutex::new(DiskSpaceCache::new(data_dir)),
    });
    let worker_inner = Arc::clone(&inner);
    let handle = std::thread::spawn(move || persist_worker(worker_inner, persist_rx));
    *inner
        .persist_thread
        .lock()
        .expect("persist thread mutex poisoned") = Some(handle);

    Ok(DedupFs(inner))
}

/// One closed-and-dirty file's worth of unpersisted changes, handed from
/// `release`/bare `truncate` to [`persist_worker`] via [`Inner::persist_tx`].
/// See [`Inner::enqueue_persist`]'s doc comment for why this is queued
/// rather than persisted inline on the calling thread.
struct PersistJob {
    tree_id: i64,
    cache: WriteCache,
    mtime_millis: i64,
    /// `cache.size()`, captured before `cache` moves into this job -
    /// [`persist_worker`] reports this back via
    /// [`Inner::queued_persist_bytes`] once done, so
    /// [`Inner::enqueue_persist`]'s backpressure check doesn't need the
    /// (by then already-consumed) `cache` back to know how much to
    /// release.
    queued_bytes: u64,
}

/// The single background thread every persist actually runs on (spawned
/// once in [`build_filesystem`], joined in [`Inner::on_unmount`]) - moving
/// persist off whichever FUSE/WinFSP worker thread called `release`/bare
/// `truncate` is what fixes the worker-pool-exhaustion failure mode (see
/// `Inner::enqueue_persist`'s doc comment): that thread now only has to
/// enqueue a job (fast, unless backpressure is active) instead of
/// blocking for as long as the target store's disk takes. Serial by
/// design, mirroring the Scala prototype's own single background persist
/// thread - also means at most one persist is ever actually writing to
/// the store at a time, which if anything makes the pre-existing,
/// deliberately-tolerated chunk-write race (`db::apply_backup_batch`'s
/// `ON CONFLICT DO NOTHING` handling) less likely to fire, not more.
fn persist_worker(inner: Arc<Inner>, jobs: mpsc::Receiver<PersistJob>) {
    for job in jobs {
        inner.persist(job.tree_id, job.cache, job.mtime_millis);
        inner
            .queued_persist_bytes
            .fetch_sub(job.queued_bytes, Ordering::Relaxed);
        inner.finish_persisting(job.tree_id);
    }
}

/// How long a [`DiskSpaceCache`] answer is trusted before doing a fresh,
/// real disk-space query. `statfs` can be called quite often by a real
/// client (a Windows/SMB client in particular typically checks free space
/// before every save), and each real query costs at least one syscall -
/// this trades a little staleness for not paying that cost on every single
/// call.
const DISK_SPACE_REFRESH_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);

/// Caches the (total, available) bytes of the filesystem underlying the
/// repository's `data/` directory - see [`Inner::statfs`] for why this
/// needs to be a *real* value at all, not the zeroed-out placeholder it
/// used to be. Backed by [`mountfs::disk_space`] - a single, targeted query
/// of exactly `data_dir`, deliberately not an "enumerate every mounted
/// filesystem" approach (that was this cache's first version, built on
/// `sysinfo::Disks`; see [`mountfs::disk_space`]'s own doc comment for why
/// that was a real, reproduced-in-production deadlock risk for a process
/// that is itself serving a mount, not just a style preference).
struct DiskSpaceCache {
    data_dir: PathBuf,
    last_refresh: std::time::Instant,
    total_available: (u64, u64),
}

impl DiskSpaceCache {
    fn new(data_dir: PathBuf) -> Self {
        let total_available = mountfs::disk_space(&data_dir).unwrap_or((0, 0));
        Self {
            data_dir,
            last_refresh: std::time::Instant::now(),
            total_available,
        }
    }

    /// The real (total, available) bytes for `data_dir`'s filesystem,
    /// refreshed at most once every [`DISK_SPACE_REFRESH_INTERVAL`] - a
    /// single `statvfs`/`GetDiskFreeSpaceExW` call, cheap enough to call
    /// from `statfs` directly rather than needing its own background
    /// thread. Keeps the last known-good value on a query error (e.g. a
    /// transient/racy unmount of the underlying filesystem) rather than
    /// reporting zero.
    fn total_available(&mut self) -> (u64, u64) {
        if self.last_refresh.elapsed() >= DISK_SPACE_REFRESH_INTERVAL
            && let Ok(fresh) = mountfs::disk_space(&self.data_dir)
        {
            self.total_available = fresh;
            self.last_refresh = std::time::Instant::now();
        }
        self.total_available
    }
}

/// Holds every bit of state a mount needs, shared (via `Arc`, see
/// [`DedupFs`]) between the FUSE/WinFSP dispatch threads and the
/// background persist thread ([`persist_worker`]).
struct Inner {
    /// Mirrors `--read-write`'s absence - checked explicitly by every
    /// mutating `MountFilesystem` method rather than trusted to the
    /// platform's own `-oro`/`ReadOnlyVolume` mount-level enforcement:
    /// WinFSP does *not* actually block a write-intent `CreateFileW`+
    /// `WriteFile` at the driver level the way Linux's `MS_RDONLY` blocks
    /// the `write(2)` syscall before it ever reaches FUSE (found via a
    /// real regression once `write` was wired into the Windows dispatch -
    /// a `--read-write`-less mount silently accepted writes into the
    /// cache, only discovering `store is read-only` once persist ran).
    read_only: bool,
    /// Mirrors `--zero-fill-missing` - see `MountArgs`' own doc comment.
    /// Checked once, in `read_persisted`, at the single point missing/short
    /// store data would otherwise become `Errno::EIO`.
    zero_fill_missing: bool,
    /// Held for the mount's whole lifetime when `--read-write` (`None`
    /// otherwise) - see `MountArgs::read_write`'s doc comment and
    /// `docs/plans/cross-process-repository-locking.md`. Not
    /// drop-order-sensitive like `conn`/`write_conn` below (releasing the
    /// cross-process lock has no bearing on this process's own SQLite
    /// connection/checkpoint behavior), so its position in this struct
    /// doesn't matter.
    _repo_lock: Option<repo_lock::RepoLock>,
    /// **Must stay declared before `write_conn` below.** Rust drops struct
    /// fields in declaration order, and that's the only thing that makes
    /// `write_conn` (not this one) the *last* of the two to close when
    /// `Inner` itself is finally dropped (the last `Arc<Inner>` clone
    /// going away, once `on_unmount` has already joined `persist_worker` -
    /// see its own doc comment - so there's no lingering background thread
    /// holding a reference past that point either). That ordering is what
    /// lets SQLite auto-checkpoint and remove `-wal`/`-shm` on a clean
    /// `--read-write` unmount: only a write-capable connection closing
    /// *last* can do that (see `docs/plans/implemented/
    /// read-only-repository-access.md`'s addendum on `store` for what goes
    /// wrong when a read connection ends up closing after the write one
    /// instead). Reordering these two fields would silently break this.
    conn: Mutex<Connection>,
    /// `None` for a read-only mount (`--read-write` not given) - never
    /// opened at all in that case, so a genuinely read-only repository
    /// directory (see `docs/plans/read-only-repository-access.md`) can be
    /// mounted read-only too. Every call site that locks this is already
    /// behind its own `if self.read_only { return Err(Errno::EROFS) }`
    /// check, so `.expect`ing `Some` there is safe - reaching it at all
    /// already proves this is a `--read-write` mount. `Some` and held for
    /// the mount's whole lifetime otherwise - see `MountArgs::read_write`'s
    /// doc comment on why `store`/`del`/`reclaim-space` mustn't run
    /// concurrently against the same repository while this is open. Must
    /// stay declared *after* `conn` above - see that field's own comment.
    write_conn: Mutex<Option<Connection>>,
    data_store: LongTermStore,
    /// Reserves store space for new chunks written by the phase 2b persist
    /// pipeline - see `chunk_store::SpaceAllocator`.
    allocator: SpaceAllocator,
    chunker_config: ChunkerConfig,
    ram_budget: Arc<RamBudget>,
    /// This mount's private temp directory for [`WriteCache`] disk
    /// spillover - see `build_filesystem`.
    spill_dir: PathBuf,
    spill_id_seq: AtomicU64,
    /// One entry per tree id with an open write-intent handle, or with a
    /// persist in flight for it - see [`FileWriteState`] and "Phase 2b" in
    /// `docs/plans/implemented/06-fuse-mount-readwrite.md`.
    write_states: Mutex<HashMap<i64, FileWriteState>>,
    /// Paired with `write_states` - notified whenever an entry's
    /// `persisting` flag clears (see [`FileWriteState::persisting`] and
    /// [`Inner::wait_while_persisting`]).
    write_states_cv: Condvar,
    /// The sending half of the persist queue - `None` once
    /// [`Inner::on_unmount`] has taken it, which is what lets
    /// [`persist_worker`]'s loop (and thus the thread join right after)
    /// actually finish. Cloned out (not sent through while holding this
    /// lock) by `enqueue_persist` purely to keep the lock's critical
    /// section small - unlike the old bounded channel, `send` on this
    /// unbounded one never itself blocks.
    persist_tx: Mutex<Option<mpsc::Sender<PersistJob>>>,
    /// The background thread `persist_worker` runs on - `None` before
    /// `build_filesystem` finishes spawning it, and after `on_unmount` has
    /// joined it.
    persist_thread: Mutex<Option<JoinHandle<()>>>,
    /// Total bytes across every [`PersistJob`] currently queued or being
    /// persisted (not bytes still accumulating in an open, not-yet-closed
    /// file) - the actual backpressure signal for
    /// [`Inner::enqueue_persist`], replacing the old fixed job-count gate.
    /// See [`Inner::spill_backpressure_threshold_bytes`] and
    /// `docs/plans/memory-pressure-backpressure.md` for why.
    queued_persist_bytes: AtomicU64,
    /// How many bytes' worth of [`Inner::queued_persist_bytes`] can
    /// accumulate before a *new* `release`/bare `truncate` call starts
    /// blocking its own FUSE/WinFSP worker thread waiting for room - see
    /// [`Inner::enqueue_persist`]. Tied to `--write-cache-mb` (the same
    /// figure `ram_budget` uses as its own RAM ceiling) rather than a
    /// separate flag: a user who already sized that for their machine has
    /// implicitly said how much buffered-but-not-yet-durable data they're
    /// comfortable with, and reusing the number avoids a second knob to
    /// explain.
    ///
    /// Replaces the older fixed *job-count* gate (`PERSIST_QUEUE_CAPACITY
    /// = 4`) - see `docs/plans/memory-pressure-backpressure.md`'s
    /// benchmarks for what motivated the change: a job-count gate
    /// throttled small files earlier than any real pressure justified
    /// (~27% aggregate throughput lost to `N = 4` vs. a much larger count,
    /// for many 1 MB files under a throttled disk) while providing *no*
    /// protection at all for large files, letting 300 MB (6x a single
    /// 50 MB file) accumulate unpersisted with zero client-visible
    /// backpressure under that same `N = 4`.
    spill_backpressure_threshold_bytes: u64,
    /// Real, periodically-refreshed free/total space of the filesystem
    /// underlying `data/` - see [`Inner::statfs`] for why this exists at
    /// all (a real bug, not a nicety) and [`DiskSpaceCache`] for the
    /// caching.
    disk_space: Mutex<DiskSpaceCache>,
}

/// Thin wrapper making [`Inner`] (shared with the background persist
/// thread, [`persist_worker`], via `Arc`) implement [`MountFilesystem`] -
/// every method just forwards to the identically-signatured method on
/// `Inner`.
struct DedupFs(Arc<Inner>);

/// Per-open-file write-side state, keyed by tree id (the same id used as
/// this file's [`Handle`]) - refcounts every open handle (read *and*
/// write-intent alike, matching the Scala prototype's `Handles`: even a
/// concurrent read-only opener must keep seeing a consistent picture for
/// as long as its handle is open) and lazily materializes a [`WriteCache`]
/// only once a real `write`/`truncate` happens.
struct FileWriteState {
    open_count: u32,
    cache: Option<WriteCache>,
    /// `true` once `cache` has unpersisted changes. `write`/`truncate`
    /// requests: default `EROFS`, same rationale as the phase 2a methods
    /// set it; `release` hands off to the persist queue (see
    /// [`Inner::enqueue_persist`]) and clears it once `open_count` reaches
    /// `0`.
    dirty: bool,
    /// `true` while this entry's persist is either queued or actually
    /// running on the background thread ([`persist_worker`]) - `cache` is
    /// `None` at that point (already handed off), but the entry itself
    /// stays in the map for the whole persist rather than being removed
    /// upfront. This matters because closing a file descriptor does *not*
    /// wait for FUSE's `release` callback to finish (release is
    /// inherently best-effort/asynchronous per the FUSE contract), and
    /// persist itself now runs asynchronously too (see
    /// [`Inner::enqueue_persist`]) - a program that closes and immediately
    /// reopens/reads the same file can otherwise race ahead of the persist
    /// and see neither the write cache (already taken) nor the new DB
    /// content (not committed yet). [`Inner::wait_while_persisting`]
    /// blocks a racing `read`/`write`/`truncate`/`getattr` on this exact
    /// entry (not the whole mount) until it clears - mirrors the Scala
    /// prototype's `Handle.readLock`/`DataEntry`'s read-write lock, minus
    /// the multi-generation "persisting queue" (see the "Phase 2b" notes
    /// in `docs/plans/implemented/06-fuse-mount-readwrite.md` for why
    /// that's out of scope here: at most one persist per file is ever in
    /// flight in this implementation, a later writer simply waits for it).
    persisting: bool,
    /// Refreshed on every `write`/`truncate` (real filesystems bump mtime
    /// on a content change) - `getattr` prefers this over the persisted
    /// entry's own `time_millis` while a cache is live, and it's what gets
    /// committed as the new entry's `time_millis` on persist.
    mtime_millis: i64,
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

/// Maps a [`db::Error`] from `db::rename_entry`/`db::undelete`'s
/// replace-on-conflict handling (see `docs/plans/mount-rename-overwrite.md`)
/// to the `Errno` a real `rename(2)` would give for the same situation -
/// shared by both call sites in [`Inner::rename`] (the ordinary path and the
/// `[deleted]`-recovery path), which hit exactly the same set of variants.
fn map_rename_error(err: db::Error) -> Errno {
    match err {
        db::Error::AlreadyExists { .. } => Errno::EEXIST,
        db::Error::TargetIsADirectory { .. } => Errno::EISDIR,
        db::Error::TargetIsAFile { .. } => Errno::ENOTDIR,
        db::Error::TargetNotEmpty { .. } => Errno::ENOTEMPTY,
        _ => Errno::EIO,
    }
}

impl MountFilesystem for DedupFs {
    fn getattr(&self, path: &str) -> Result<Attr, Errno> {
        self.0.getattr(path)
    }

    fn readdir(&self, path: &str) -> Result<Vec<DirEntry>, Errno> {
        self.0.readdir(path)
    }

    fn open(&self, path: &str, write_intent: bool) -> Result<Handle, Errno> {
        self.0.open(path, write_intent)
    }

    fn read(&self, handle: Handle, offset: u64, size: u32) -> Result<Vec<u8>, Errno> {
        self.0.read(handle, offset, size)
    }

    fn release(&self, handle: Handle) {
        self.0.release(handle)
    }

    fn write(&self, handle: Handle, offset: u64, data: &[u8]) -> Result<u32, Errno> {
        self.0.write(handle, offset, data)
    }

    fn truncate(&self, path: &str, size: u64) -> Result<(), Errno> {
        self.0.truncate(path, size)
    }

    fn statfs(&self) -> Result<mountfs::StatfsInfo, Errno> {
        self.0.statfs()
    }

    fn mkdir(&self, path: &str) -> Result<(), Errno> {
        self.0.mkdir(path)
    }

    fn create(&self, path: &str) -> Result<Handle, Errno> {
        self.0.create(path)
    }

    fn unlink(&self, path: &str) -> Result<(), Errno> {
        self.0.unlink(path)
    }

    fn rmdir(&self, path: &str) -> Result<(), Errno> {
        self.0.rmdir(path)
    }

    fn rename(&self, old_path: &str, new_path: &str, no_replace: bool) -> Result<(), Errno> {
        self.0.rename(old_path, new_path, no_replace)
    }

    fn utimens(&self, path: &str, mtime_millis: i64) -> Result<(), Errno> {
        self.0.utimens(path, mtime_millis)
    }

    fn on_unmount(&self) {
        self.0.on_unmount()
    }
}

impl Inner {
    fn getattr(&self, path: &str) -> Result<Attr, Errno> {
        {
            let conn = self.conn.lock().expect("db connection mutex poisoned");
            if let Some((scope_id, virtual_path)) = mount_deleted::split_deleted_path(&conn, path)?
            {
                return match mount_deleted::resolve_deleted(&conn, scope_id, &virtual_path)? {
                    Some(DeletedResolution::Listing { .. }) => Ok(Attr {
                        kind: FileKind::Directory,
                        size: 0,
                        mtime_millis: now_millis(),
                    }),
                    Some(DeletedResolution::Entry(entry)) => {
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
                    None => Err(Errno::ENOENT),
                };
            }
        }
        let entry = self.resolve_active_entry(path)?;
        let (kind, mut size) = match entry.kind {
            db::EntryKind::Dir => (FileKind::Directory, 0),
            db::EntryKind::File => {
                let conn = self.conn.lock().expect("db connection mutex poisoned");
                let size = db::file_size(&conn, &entry).map_err(|_| Errno::EIO)? as u64;
                (FileKind::File, size)
            }
        };
        let mut mtime_millis = entry.time_millis;
        // Prefer a live write cache's own size/mtime over the persisted
        // entry's - lets `stat` on a file mid-edit see its true current
        // size before any of it has actually been persisted (phase 2b).
        if entry.kind == db::EntryKind::File {
            let states = self
                .write_states
                .lock()
                .expect("write states mutex poisoned");
            if let Some(state) = states.get(&entry.id)
                && let Some(cache) = &state.cache
            {
                size = cache.size();
                mtime_millis = state.mtime_millis;
            }
        }
        Ok(Attr {
            kind,
            size,
            mtime_millis,
        })
    }

    fn readdir(&self, path: &str) -> Result<Vec<DirEntry>, Errno> {
        let conn = self.conn.lock().expect("db connection mutex poisoned");
        if let Some((scope_id, virtual_path)) = mount_deleted::split_deleted_path(&conn, path)? {
            return match mount_deleted::resolve_deleted(&conn, scope_id, &virtual_path)? {
                Some(DeletedResolution::Listing { root_id }) => {
                    mount_deleted::list_deleted_children(&conn, root_id)
                }
                Some(DeletedResolution::Entry(entry)) if entry.kind == db::EntryKind::Dir => {
                    mount_deleted::list_deleted_children(&conn, entry.id)
                }
                Some(DeletedResolution::Entry(_)) => Err(Errno::ENOTDIR),
                None => Err(Errno::ENOENT),
            };
        }
        // Deliberately not `resolve_active_entry` (see its own doc comment
        // for when that's required): this only ever resolves a
        // *directory*, whose id/row is never replaced by a content-change
        // persist the way a file's is - directories only change via
        // mkdir/rmdir/rename, ordinary in-place tree mutations.
        let dir = db::resolve_path(&conn, path)
            .map_err(|_| Errno::EIO)?
            .ok_or(Errno::ENOENT)?;
        let children = db::list_children(&conn, dir.id).map_err(|_| Errno::EIO)?;
        let mut result: Vec<DirEntry> = children
            .into_iter()
            .map(|child| DirEntry {
                name: child.name,
                kind: match child.kind {
                    db::EntryKind::Dir => FileKind::Directory,
                    db::EntryKind::File => FileKind::File,
                },
            })
            .collect();
        // Only shown when there's actually something deleted to see, and
        // only if a real active entry doesn't already occupy that name
        // here (see `mount_deleted::split_deleted_path`'s "real entry
        // wins" rule). Unconditional visibility was tried first (matching
        // a real recycle bin's own "always there" convention) and reverted
        // after a real regression: Windows' own directory-emptiness check
        // (`RemoveDirectory`/`std::fs::remove_dir`) refuses to remove a
        // directory with *any* visible entry, synthetic or not, before our
        // own `rmdir` (which only checks real children) ever gets called -
        // an always-present `[deleted]` would make every directory
        // permanently non-removable through the mount.
        if db::has_deleted_children(&conn, dir.id).map_err(|_| Errno::EIO)?
            && db::find_tree_entry(&conn, dir.id, mount_deleted::DELETED_DIR_NAME)
                .map_err(|_| Errno::EIO)?
                .is_none()
        {
            result.push(DirEntry {
                name: mount_deleted::DELETED_DIR_NAME.to_string(),
                kind: FileKind::Directory,
            });
        }
        Ok(result)
    }

    fn open(&self, path: &str, write_intent: bool) -> Result<Handle, Errno> {
        // No longer rejects `write_intent` outright (a leftover from the
        // read-only-only phase): phase 2a's `utimens`/`chmod` on an
        // *existing* file need a write-intent open to succeed too - on
        // Windows in particular, `SetFileTime` requires the handle to
        // carry `FILE_WRITE_ATTRIBUTES`, which only a write-capable
        // `CreateFileW` grants (unlike POSIX `futimens`, permission-checked
        // by ownership rather than by how the fd was opened - so this
        // never came up while verifying the Linux side alone). A genuinely
        // read-only mount (`backup mount` without `--read-write`) is
        // unaffected: `-oro`/`ReadOnlyVolume` rejects a write-intent open
        // before it ever reaches this method, on both platforms.
        let _ = write_intent;
        {
            let conn = self.conn.lock().expect("db connection mutex poisoned");
            if let Some((scope_id, virtual_path)) = mount_deleted::split_deleted_path(&conn, path)?
            {
                return match mount_deleted::resolve_deleted(&conn, scope_id, &virtual_path)? {
                    Some(DeletedResolution::Entry(entry)) if entry.kind == db::EntryKind::File => {
                        Ok(mount_deleted::deleted_handle(entry.id))
                    }
                    Some(_) => Err(Errno::EISDIR),
                    None => Err(Errno::ENOENT),
                };
            }
        }
        let entry = self.resolve_active_entry(path)?;
        if entry.kind != db::EntryKind::File {
            return Err(Errno::EISDIR);
        }
        self.register_open(entry.id, entry.time_millis);
        Ok(Handle(entry.id as u64))
    }

    fn read(&self, handle: Handle, offset: u64, size: u32) -> Result<Vec<u8>, Errno> {
        if mount_deleted::is_deleted_handle(handle) {
            return self.read_persisted(
                mount_deleted::deleted_handle_id(handle),
                offset,
                u64::from(size),
            );
        }
        let tree_id = handle.0 as i64;
        // A live write cache (phase 2b) takes priority over the persisted
        // content - an app that reads back a file it's mid-editing must
        // see its own unpersisted writes, not stale on-disk bytes.
        self.wait_while_persisting(tree_id);
        let mut states = self
            .write_states
            .lock()
            .expect("write states mutex poisoned");
        if let Some(state) = states.get_mut(&tree_id)
            && let Some(cache) = state.cache.as_mut()
        {
            return cache
                .read_filling_gaps(offset, u64::from(size), |gap_pos, gap_len| {
                    self.read_persisted(tree_id, gap_pos, gap_len)
                        .unwrap_or_else(|_| vec![0u8; gap_len as usize])
                })
                .map_err(|_| Errno::EIO);
        }
        drop(states);
        self.read_persisted(tree_id, offset, u64::from(size))
    }

    fn release(&self, handle: Handle) {
        // A deleted-entry handle was never registered in `write_states`
        // (`open` returns early for those, before `register_open`) - there
        // is nothing to release.
        if mount_deleted::is_deleted_handle(handle) {
            return;
        }
        let tree_id = handle.0 as i64;
        let mut states = self
            .write_states
            .lock()
            .expect("write states mutex poisoned");
        let Some(state) = states.get_mut(&tree_id) else {
            return;
        };
        state.open_count = state.open_count.saturating_sub(1);
        if state.open_count > 0 {
            return;
        }
        if !state.dirty {
            states.remove(&tree_id);
            return;
        }
        // Take the cache out and mark this entry as persisting rather than
        // removing it outright - see [`FileWriteState::persisting`] for
        // why a racing `read`/`write`/`truncate`/`getattr` on this same
        // `tree_id` must keep observing *something* consistent (blocking
        // via `wait_while_persisting`) until [`Inner::finish_persisting`]
        // clears it - which now happens on the background persist thread
        // (see `persist_worker`), not necessarily this one.
        let cache = state.cache.take().expect("dirty implies a live cache");
        state.persisting = true;
        let mtime_millis = state.mtime_millis;
        drop(states);

        self.enqueue_persist(tree_id, cache, mtime_millis);
    }

    fn write(&self, handle: Handle, offset: u64, data: &[u8]) -> Result<u32, Errno> {
        if self.read_only {
            return Err(Errno::EROFS);
        }
        let tree_id = handle.0 as i64;
        self.wait_while_persisting(tree_id);
        let mut states = self
            .write_states
            .lock()
            .expect("write states mutex poisoned");
        let state = states.get_mut(&tree_id).ok_or(Errno::EIO)?;
        if state.cache.is_none() {
            state.cache = Some(self.new_write_cache(tree_id)?);
        }
        state
            .cache
            .as_mut()
            .expect("just ensured Some")
            .write(offset, data)
            .map_err(|_| Errno::EIO)?;
        state.dirty = true;
        state.mtime_millis = now_millis();
        Ok(data.len() as u32)
    }

    fn truncate(&self, path: &str, size: u64) -> Result<(), Errno> {
        if self.read_only {
            return Err(Errno::EROFS);
        }
        let entry = self.resolve_active_entry(path)?;
        if entry.kind != db::EntryKind::File {
            return Err(Errno::EISDIR);
        }
        let tree_id = entry.id;
        let mtime_millis = now_millis();

        let mut states = self
            .write_states
            .lock()
            .expect("write states mutex poisoned");
        if let Some(state) = states.get_mut(&tree_id) {
            if state.cache.is_none() {
                state.cache = Some(self.new_write_cache(tree_id)?);
            }
            state
                .cache
                .as_mut()
                .expect("just ensured Some")
                .truncate(size);
            state.dirty = true;
            state.mtime_millis = mtime_millis;
            return Ok(());
        }
        // No open handle for this file (a bare `truncate(2)`/`O_TRUNC`
        // without a held write handle) - nothing will ever call `release`
        // to persist this later, so this hands it to the background
        // persist thread itself (same pipeline a normal close uses - see
        // `enqueue_persist`/`persist_worker`). Registers a persisting-only
        // placeholder in the *same* lock hold as the check above (so no
        // concurrent `open`/`register_open` for this exact id can slip in
        // between) before dropping the lock - without this, a racing
        // `open` could resolve to this about-to-be-replaced tree id and
        // bind a `Handle` to it that can never observe this truncate's
        // effect (see `resolve_active_entry`'s doc comment), since
        // persisting is no longer synchronous with this call returning.
        states.insert(
            tree_id,
            FileWriteState {
                open_count: 0,
                cache: None,
                dirty: false,
                persisting: true,
                mtime_millis,
            },
        );
        drop(states);

        let mut cache = match self.new_write_cache(tree_id) {
            Ok(cache) => cache,
            Err(err) => {
                // Roll back the placeholder - nothing will ever persist
                // for it now, so nothing should ever wait on it either.
                self.finish_persisting(tree_id);
                return Err(err);
            }
        };
        cache.truncate(size);
        self.enqueue_persist(tree_id, cache, mtime_millis);
        Ok(())
    }

    fn statfs(&self) -> Result<mountfs::StatfsInfo, Errno> {
        // Used to report zeroed-out placeholder values here unconditionally
        // (matching Scala's own Linux FUSE implementation, also a no-op) -
        // harmless enough over a direct Linux/WSL FUSE mount, but a real
        // bug once re-exported over Samba: SMB clients (Windows in
        // particular) check free space via this before permitting a save,
        // and zero free space made every save through a Samba-re-exported
        // `--read-write` mount fail with a "not enough space on disk"
        // dialog despite plenty of real space being available. Now reports
        // the real, periodically-refreshed free/total space of the
        // filesystem underlying `data/` - see `DiskSpaceCache`.
        let (total, available) = self
            .disk_space
            .lock()
            .expect("disk space cache mutex poisoned")
            .total_available();
        let block_size: u64 = 512;
        Ok(mountfs::StatfsInfo {
            block_size: block_size as u32,
            max_name_length: mountfs::MAX_NAME_BYTES as u32,
            blocks: total / block_size,
            blocks_free: available / block_size,
            blocks_available: available / block_size,
            files: 0,
            files_free: 0,
        })
    }

    fn mkdir(&self, path: &str) -> Result<(), Errno> {
        if self.read_only {
            return Err(Errno::EROFS);
        }
        let (parent_path, name) = split_parent(path);
        let mut guard = self
            .write_conn
            .lock()
            .expect("write connection mutex poisoned");
        let conn = guard
            .as_mut()
            .expect("only reached when read_write - checked by the read_only guard above");
        let parent = db::resolve_path(conn, parent_path)
            .map_err(|_| Errno::EIO)?
            .ok_or(Errno::ENOENT)?;
        // insert_directory is idempotent (get-or-create) - fine for `store`,
        // but a real mkdir(2) must fail on an existing name (file or
        // directory alike), so the existence check happens here first.
        if db::find_tree_entry(conn, parent.id, name)
            .map_err(|_| Errno::EIO)?
            .is_some()
        {
            return Err(Errno::EEXIST);
        }
        db::insert_directory(conn, parent.id, name, now_millis()).map_err(|_| Errno::EIO)?;
        Ok(())
    }

    fn create(&self, path: &str) -> Result<Handle, Errno> {
        if self.read_only {
            return Err(Errno::EROFS);
        }
        let (parent_path, name) = split_parent(path);
        let mut guard = self
            .write_conn
            .lock()
            .expect("write connection mutex poisoned");
        let conn = guard
            .as_mut()
            .expect("only reached when read_write - checked by the read_only guard above");
        let parent = db::resolve_path(conn, parent_path)
            .map_err(|_| Errno::EIO)?
            .ok_or(Errno::ENOENT)?;
        if db::find_tree_entry(conn, parent.id, name)
            .map_err(|_| Errno::EIO)?
            .is_some()
        {
            return Err(Errno::EEXIST);
        }
        // Starts empty - the same shape `open`/`read` already treat a
        // zero-length file as (`content_id IS NULL`); an ensuing `write`
        // (phase 2b) materializes a real write cache for it on demand,
        // same as `open`ing an existing file for writing.
        let time_millis = now_millis();
        db::apply_backup_batch(
            conn,
            &[db::FileBackupRecord {
                parent_id: parent.id,
                name: name.to_string(),
                time_millis,
                content: db::ContentSource::Resolved {
                    chunks: Vec::new(),
                    content_hash: Vec::new(),
                },
            }],
        )
        .map_err(|_| Errno::EIO)?;
        let entry = db::find_tree_entry(conn, parent.id, name)
            .map_err(|_| Errno::EIO)?
            .ok_or(Errno::EIO)?;
        drop(guard);
        self.register_open(entry.id, time_millis);
        Ok(Handle(entry.id as u64))
    }

    fn unlink(&self, path: &str) -> Result<(), Errno> {
        if self.read_only {
            return Err(Errno::EROFS);
        }
        let mut guard = self
            .write_conn
            .lock()
            .expect("write connection mutex poisoned");
        let conn = guard
            .as_mut()
            .expect("only reached when read_write - checked by the read_only guard above");
        let entry = db::resolve_path(conn, path)
            .map_err(|_| Errno::EIO)?
            .ok_or(Errno::ENOENT)?;
        if entry.kind != db::EntryKind::File {
            return Err(Errno::EISDIR);
        }
        db::soft_delete(conn, entry.id, now_millis()).map_err(|_| Errno::EIO)?;
        Ok(())
    }

    fn rmdir(&self, path: &str) -> Result<(), Errno> {
        if self.read_only {
            return Err(Errno::EROFS);
        }
        let mut guard = self
            .write_conn
            .lock()
            .expect("write connection mutex poisoned");
        let conn = guard
            .as_mut()
            .expect("only reached when read_write - checked by the read_only guard above");
        let entry = db::resolve_path(conn, path)
            .map_err(|_| Errno::EIO)?
            .ok_or(Errno::ENOENT)?;
        if entry.kind != db::EntryKind::Dir {
            return Err(Errno::ENOTDIR);
        }
        if !db::list_children(conn, entry.id)
            .map_err(|_| Errno::EIO)?
            .is_empty()
        {
            return Err(Errno::ENOTEMPTY);
        }
        db::soft_delete(conn, entry.id, now_millis()).map_err(|_| Errno::EIO)?;
        Ok(())
    }

    fn rename(&self, old_path: &str, new_path: &str, no_replace: bool) -> Result<(), Errno> {
        if self.read_only {
            return Err(Errno::EROFS);
        }
        let mut guard = self
            .write_conn
            .lock()
            .expect("write connection mutex poisoned");
        let conn = guard
            .as_mut()
            .expect("only reached when read_write - checked by the read_only guard above");

        // The recovery gesture: dragging something out of `[deleted]`
        // arrives here as an ordinary `rename` (confirmed against real
        // Windows Explorer - see the plan doc's resolved spike) - recognize
        // a source under `[deleted]/...` and call `db::undelete` instead of
        // `db::rename_entry`. The destination always lands exactly where
        // the caller asked (not wherever the entry was originally deleted
        // from), and a directory always recovers recursively at the same
        // `deleted_at` scope - there's no way for a drag gesture to express
        // anything narrower, matching `undelete --recursive`'s own
        // semantics (see the plan doc's "Directory rename-out scope"
        // decision). `no_replace`/replace-on-conflict (see
        // `docs/plans/mount-rename-overwrite.md`) apply here exactly like
        // the ordinary rename path below - a client that already confirmed
        // "yes, replace" doesn't care whether the source came from
        // `[deleted]` or not.
        if let Some((scope_id, virtual_path)) = mount_deleted::split_deleted_path(conn, old_path)? {
            let Some(DeletedResolution::Entry(entry)) =
                mount_deleted::resolve_deleted(conn, scope_id, &virtual_path)?
            else {
                return Err(Errno::ENOENT);
            };
            if mount_deleted::split_deleted_path(conn, new_path)?.is_some() {
                // Moving between two deleted views isn't a recovery
                // gesture and has no defined meaning here.
                return Err(Errno::EIO);
            }
            let (new_parent_path, new_name) = split_parent(new_path);
            let new_parent = db::resolve_path(conn, new_parent_path)
                .map_err(|_| Errno::EIO)?
                .ok_or(Errno::ENOENT)?;
            let recursive = entry.kind == db::EntryKind::Dir;
            let count = db::undelete(
                conn,
                entry.id,
                recursive,
                Some((new_parent.id, new_name)),
                no_replace,
                now_millis(),
            )
            .map_err(map_rename_error)?;
            return if count > 0 {
                Ok(())
            } else {
                Err(Errno::ENOENT)
            };
        }

        let entry = db::resolve_path(conn, old_path)
            .map_err(|_| Errno::EIO)?
            .ok_or(Errno::ENOENT)?;
        let (new_parent_path, new_name) = split_parent(new_path);
        let new_parent = db::resolve_path(conn, new_parent_path)
            .map_err(|_| Errno::EIO)?
            .ok_or(Errno::ENOENT)?;
        db::rename_entry(
            conn,
            entry.id,
            new_parent.id,
            new_name,
            no_replace,
            now_millis(),
        )
        .map_err(map_rename_error)
    }

    fn utimens(&self, path: &str, mtime_millis: i64) -> Result<(), Errno> {
        if self.read_only {
            return Err(Errno::EROFS);
        }
        let mut guard = self
            .write_conn
            .lock()
            .expect("write connection mutex poisoned");
        let conn = guard
            .as_mut()
            .expect("only reached when read_write - checked by the read_only guard above");
        let entry = db::resolve_path(conn, path)
            .map_err(|_| Errno::EIO)?
            .ok_or(Errno::ENOENT)?;
        db::touch_mtime(conn, entry.id, mtime_millis).map_err(|_| Errno::EIO)?;
        Ok(())
    }

    fn on_unmount(&self) {
        // Flush any files still open (and dirty) at shutdown - mirrors the
        // Scala prototype's `Backend.close()`, which does the same before
        // tearing down its own write-cache temp directory. A clean
        // unmount only happens once every FUSE-level handle is closed in
        // the ordinary case, but WinFSP/libfuse can still reach this with
        // handles outstanding on a forced/lazy unmount. Persisted directly
        // here rather than through `enqueue_persist` - shutdown is the one
        // place backpressure doesn't matter, and this guarantees every
        // still-dirty file is flushed before the queue is closed below.
        let dirty: Vec<(i64, WriteCache, i64)> = {
            let mut states = self
                .write_states
                .lock()
                .expect("write states mutex poisoned");
            states
                .drain()
                .filter_map(|(tree_id, state)| {
                    if state.dirty {
                        state
                            .cache
                            .map(|cache| (tree_id, cache, state.mtime_millis))
                    } else {
                        None
                    }
                })
                .collect()
        };
        for (tree_id, cache, mtime_millis) in dirty {
            self.persist(tree_id, cache, mtime_millis);
        }

        // Close the persist queue and wait for the background thread (see
        // `persist_worker`) to finish flushing anything a `release`/bare
        // `truncate` already handed off before this unmount started -
        // otherwise a just-closed file's queued-but-not-yet-persisted
        // changes would be silently lost when `spill_dir` is removed
        // below.
        self.persist_tx
            .lock()
            .expect("persist queue mutex poisoned")
            .take();
        if let Some(handle) = self
            .persist_thread
            .lock()
            .expect("persist thread mutex poisoned")
            .take()
        {
            let _ = handle.join();
        }

        let _ = std::fs::remove_dir_all(&self.spill_dir);
    }

    /// Creates (or reuses, bumping its refcount) a [`FileWriteState`] entry
    /// for `tree_id` - called by both `open` and `create` so `release`'s
    /// refcounting sees every open handle, not just write-intent ones (a
    /// concurrent reader must keep seeing a consistent picture for as long
    /// as *its* handle stays open too - see [`FileWriteState`]'s doc
    /// comment).
    fn register_open(&self, tree_id: i64, time_millis: i64) {
        let mut states = self
            .write_states
            .lock()
            .expect("write states mutex poisoned");
        states
            .entry(tree_id)
            .or_insert_with(|| FileWriteState {
                open_count: 0,
                cache: None,
                dirty: false,
                persisting: false,
                mtime_millis: time_millis,
            })
            .open_count += 1;
    }

    /// Blocks the calling thread while `tree_id` has a persist queued or
    /// running (see [`FileWriteState::persisting`]) - called before any
    /// read or mutation of `write_states`' entry for `tree_id` so a racing
    /// `read`/`write`/`truncate`/`getattr` can't observe the gap between a
    /// dirty cache being handed off to the persist queue and
    /// [`Inner::finish_persisting`] actually clearing it once committed.
    fn wait_while_persisting(&self, tree_id: i64) {
        let mut states = self
            .write_states
            .lock()
            .expect("write states mutex poisoned");
        while states.get(&tree_id).is_some_and(|state| state.persisting) {
            states = self
                .write_states_cv
                .wait(states)
                .expect("write states mutex poisoned");
        }
    }

    /// Resolves `path` to its currently-active tree entry, the way every
    /// path-based method (`getattr`/`open`/`truncate`) needs to: plain
    /// `db::resolve_path` isn't enough on its own, because a content
    /// change replaces a file's tree entry with a *new* row (a new id -
    /// `apply_backup_batch`'s soft-delete-old-insert-new pattern, never an
    /// in-place update) rather than mutating the old one, and that
    /// replacement isn't visible on `self.conn` until the persist actually
    /// commits (see [`FileWriteState::persisting`]). Resolving naively
    /// while a persist for this exact path is in flight would silently
    /// return the about-to-be-replaced old row - wrong for `getattr`
    /// (stale size) and worse for `open` (binds a `Handle` to an id that's
    /// about to be soft-deleted and can never see the new content, since
    /// nothing ever updates that old row again). Waits out any persist
    /// racing this specific id and re-resolves, rather than the whole
    /// mount blocking on unrelated files.
    ///
    /// **Any new method added to this `impl` block that resolves a *file*
    /// path (not a directory - see `readdir`'s own comment for why that one
    /// is exempt) and then keeps using the resolved id/entry after
    /// releasing the lock it was resolved under must go through this
    /// function** (or, for an id already in hand, [`Inner::wait_while_persisting`]
    /// directly) instead of calling `db::resolve_path` on `self.conn`
    /// itself - nothing in the type system enforces this, it has to stay a
    /// convention. A method that resolves and acts *within the same lock
    /// hold* (no gap for a persist to land in between, the way
    /// `mkdir`/`create`/`unlink`/`rmdir`/`rename`/`utimens` already do
    /// today via `write_conn`) doesn't need it.
    fn resolve_active_entry(&self, path: &str) -> Result<db::TreeEntryRow, Errno> {
        loop {
            let conn = self.conn.lock().expect("db connection mutex poisoned");
            let entry = db::resolve_path(&conn, path)
                .map_err(|_| Errno::EIO)?
                .ok_or(Errno::ENOENT)?;
            drop(conn);
            if entry.kind == db::EntryKind::File {
                let is_persisting = {
                    let states = self
                        .write_states
                        .lock()
                        .expect("write states mutex poisoned");
                    states.get(&entry.id).is_some_and(|state| state.persisting)
                };
                if is_persisting {
                    self.wait_while_persisting(entry.id);
                    continue;
                }
            }
            return Ok(entry);
        }
    }

    /// A private, never-yet-created spill path for a new [`WriteCache`].
    fn spill_path(&self) -> PathBuf {
        let id = self.spill_id_seq.fetch_add(1, Ordering::Relaxed);
        self.spill_dir.join(id.to_string())
    }

    /// Builds a fresh [`WriteCache`] for `tree_id`, seeded with its
    /// current persisted size (`0` for a just-`create`d file, which has no
    /// persisted content at all yet).
    fn new_write_cache(&self, tree_id: i64) -> Result<WriteCache, Errno> {
        let conn = self.conn.lock().expect("db connection mutex poisoned");
        let entry = db::get_tree_entry(&conn, tree_id)
            .map_err(|_| Errno::EIO)?
            .ok_or(Errno::ENOENT)?;
        let initial_size = db::file_size(&conn, &entry).map_err(|_| Errno::EIO)? as u64;
        drop(conn);
        Ok(WriteCache::new(
            Arc::clone(&self.ram_budget),
            self.spill_path(),
            initial_size,
        ))
    }

    /// Reads `[offset, offset+size)` of `tree_id`'s *persisted* content
    /// only (never consults a live write cache) - the phase 1 read path,
    /// factored out so it can also serve as the "old content" gap-filler
    /// for a live [`WriteCache`]'s [`WriteCache::read_filling_gaps`] (both
    /// in [`Inner::read`] and in [`Inner::persist`]).
    fn read_persisted(&self, tree_id: i64, offset: u64, size: u64) -> Result<Vec<u8>, Errno> {
        let conn = self.conn.lock().expect("db connection mutex poisoned");
        let entry = db::get_tree_entry(&conn, tree_id)
            .map_err(|_| Errno::EIO)?
            .ok_or(Errno::ENOENT)?;
        let Some(content_id) = entry.content_id else {
            return Ok(Vec::new());
        };
        let chunks = db::ordered_content_chunks(&conn, content_id).map_err(|_| Errno::EIO)?;

        let want_start = offset;
        let want_end = want_start.saturating_add(size);
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
            if let ReadIntegrity::Incomplete { .. } = integrity
                && !self.zero_fill_missing
            {
                return Err(Errno::EIO);
            }
            // `bytes` is already zero-filled for any unreadable range by
            // `store::read` itself (see its own doc comment) - with
            // `zero_fill_missing` set, there's nothing left to do here but
            // use it, same as the `Complete` case.
            let local_start = want_start.saturating_sub(chunk_start).min(chunk_len);
            let local_end = want_end.saturating_sub(chunk_start).min(chunk_len);
            result.extend_from_slice(&bytes[local_start as usize..local_end as usize]);
            if pos >= want_end {
                break;
            }
        }
        Ok(result)
    }

    /// Hands `cache` off to the background persist thread ([`persist_worker`])
    /// instead of persisting on the calling thread - blocks only once
    /// [`Inner::queued_persist_bytes`] (measured *before* this job's own
    /// contribution - see below) already exceeds
    /// [`Inner::spill_backpressure_threshold_bytes`] (the intended
    /// backpressure point). The caller must already have set `persisting =
    /// true` on this `tree_id`'s [`FileWriteState`] before calling this
    /// (both `release` and bare `truncate` do) so a racing `read`/
    /// `getattr`/`open` on the same id blocks via `wait_while_persisting`
    /// until [`Inner::finish_persisting`] clears it, rather than observing
    /// stale pre-persist content after this call has already returned.
    fn enqueue_persist(&self, tree_id: i64, cache: WriteCache, mtime_millis: i64) {
        // Checked against bytes already queued *before* this job - not
        // after adding it - so one file bigger than the whole threshold
        // still gets sent immediately when nothing else is queued, rather
        // than blocking forever waiting for pressure only it itself
        // created (the queue can only ever drain by this job actually
        // being sent and persisted).
        while self.queued_persist_bytes.load(Ordering::Relaxed)
            > self.spill_backpressure_threshold_bytes
        {
            std::thread::sleep(SPILL_BACKPRESSURE_POLL_INTERVAL);
        }
        let queued_bytes = cache.size();
        self.queued_persist_bytes
            .fetch_add(queued_bytes, Ordering::Relaxed);

        // Cloned out from under the lock rather than sent while holding it
        // - keeps the critical section small, even though `send` on this
        // unbounded channel never itself blocks (unlike the old bounded
        // one).
        let tx = self
            .persist_tx
            .lock()
            .expect("persist queue mutex poisoned")
            .clone();
        let job = PersistJob {
            tree_id,
            cache,
            mtime_millis,
            queued_bytes,
        };
        let delivered = match tx {
            Some(tx) => tx.send(job).is_ok(),
            None => false,
        };
        if !delivered {
            // Only reachable if `on_unmount` already closed the queue -
            // by that point no new FUSE calls should be arriving, but if
            // one still raced in, don't leave this entry wedged forever.
            eprintln!(
                "mount: persist queue already closed (unmounting) - discarding unsaved \
                 changes for tree id {tree_id}"
            );
            self.queued_persist_bytes
                .fetch_sub(queued_bytes, Ordering::Relaxed);
            self.finish_persisting(tree_id);
        }
    }

    /// Clears `tree_id`'s `persisting` flag once its persist has actually
    /// finished (or been abandoned - see [`Inner::enqueue_persist`]'s
    /// already-closed-queue fallback), and removes the [`FileWriteState`]
    /// entry if nothing reopened it in the meantime. The same cleanup
    /// `release` used to do inline before persisting moved to a background
    /// thread - factored out so both [`persist_worker`] and
    /// `enqueue_persist`'s fallback can reach it.
    fn finish_persisting(&self, tree_id: i64) {
        let mut states = self
            .write_states
            .lock()
            .expect("write states mutex poisoned");
        if let Some(state) = states.get_mut(&tree_id) {
            state.persisting = false;
            if state.open_count == 0 {
                states.remove(&tree_id);
            }
        }
        drop(states);
        self.write_states_cv.notify_all();
    }

    /// Drains `cache`'s full current content into the store and commits it
    /// via `apply_backup_batch` - the phase 2b persist pipeline. Reuses
    /// `store.rs`'s own chunking/dedup machinery
    /// (`SpillingHashingChunker`/`Blake3Hasher`/`chunk_store`) rather than
    /// duplicating it, streamed in [`PERSIST_CHUNK_SIZE`] pieces so peak
    /// memory use doesn't scale with the file's total size even though
    /// `cache` may itself have spilled to disk. `SpillingHashingChunker`
    /// buffers each in-progress chunk's own bytes in a `WriteCache` sharing
    /// `self.ram_budget` (not a plain `Vec<u8>`) - without that, a large
    /// CDC chunk or (under `chunking: none`) the entire file would need to
    /// be fully RAM-resident again here even though `cache` itself is
    /// already bounded (see `docs/plans/implemented/bounded-memory-io-pipeline.md`).
    ///
    /// Best-effort: this method's callers (`persist_worker`, and
    /// `on_unmount` for handles still outstanding at shutdown) can't
    /// propagate an error back through the FUSE contract, so any failure
    /// here is logged to stderr and the unpersisted changes are simply
    /// lost - an accepted limitation (see `docs/plans/implemented/
    /// 06-fuse-mount-readwrite.md`'s "Phase 2b" notes) rather than a real
    /// per-write error path like `write`'s own `Result`.
    fn persist(&self, tree_id: i64, mut cache: WriteCache, mtime_millis: i64) {
        let (parent_id, name) = {
            let conn = self.conn.lock().expect("db connection mutex poisoned");
            let entry = match db::get_tree_entry(&conn, tree_id) {
                Ok(Some(entry)) => entry,
                Ok(None) => {
                    eprintln!(
                        "mount: tree entry {tree_id} no longer exists - discarding unsaved changes"
                    );
                    return;
                }
                Err(err) => {
                    eprintln!(
                        "error: mount: failed to look up tree entry {tree_id}: {err} - discarding unsaved changes"
                    );
                    return;
                }
            };
            let parent_id = match db::parent_id(&conn, tree_id) {
                Ok(Some(id)) => id,
                _ => {
                    eprintln!(
                        "error: mount: failed to determine the parent of '{}' - discarding unsaved changes",
                        entry.name
                    );
                    return;
                }
            };
            // Skip persisting entirely if this entry was removed/replaced
            // (e.g. unlinked) while still open for writing - matches
            // ordinary filesystem semantics: writes to an unlinked-but-
            // still-open file never reappear once the last handle closes.
            match db::find_tree_entry(&conn, parent_id, &entry.name) {
                Ok(Some(active)) if active.id == tree_id => {}
                _ => {
                    eprintln!(
                        "mount: '{}' was removed or replaced while open for writing - discarding unsaved changes",
                        entry.name
                    );
                    return;
                }
            }
            (parent_id, entry.name)
        };

        let size = cache.size();
        let mut chunker = SpillingHashingChunker::new(
            Blake3Hasher(blake3::Hasher::new()),
            self.chunker_config.chunker(),
            Arc::clone(&self.ram_budget),
            || self.spill_path(),
        );
        let mut content_hasher = blake3::Hasher::new();
        let mut chunk_refs: Vec<db::ChunkRef> = Vec::new();
        let mut pos = 0u64;
        let chunked: Result<(), String> = (|| {
            while pos < size {
                let n = PERSIST_CHUNK_SIZE.min(size - pos);
                let bytes = cache
                    .read_filling_gaps(pos, n, |gap_pos, gap_len| {
                        self.read_persisted(tree_id, gap_pos, gap_len)
                            .unwrap_or_else(|_| vec![0u8; gap_len as usize])
                    })
                    .map_err(|err| format!("failed reading the write cache: {err}"))?;
                let chunks = chunker
                    .next(&bytes)
                    .map_err(|err| format!("chunk buffering failed: {err}"))?;
                for chunk in chunks {
                    self.resolve_persist_chunk(chunk, &mut chunk_refs, &mut content_hasher)?;
                }
                pos += n;
            }
            let flushed = chunker
                .flush()
                .map_err(|err| format!("chunk buffering failed: {err}"))?;
            if let Some(chunk) = flushed {
                self.resolve_persist_chunk(chunk, &mut chunk_refs, &mut content_hasher)?;
            }
            Ok(())
        })();
        if let Err(err) = chunked {
            eprintln!(
                "error: mount: failed to persist changes to '{name}': {err} - changes have been lost"
            );
            return;
        }

        let mut content_hash = [0u8; HASH_LENGTH];
        content_hasher.finalize_xof().fill(&mut content_hash);

        let record = db::FileBackupRecord {
            parent_id,
            name: name.clone(),
            time_millis: mtime_millis,
            content: db::ContentSource::Resolved {
                chunks: chunk_refs,
                content_hash: content_hash.to_vec(),
            },
        };
        let mut guard = self
            .write_conn
            .lock()
            .expect("write connection mutex poisoned");
        let write_conn = guard
            .as_mut()
            .expect("only reached when read_write - persist is only ever invoked via a write path");
        if let Err(err) = db::apply_backup_batch(write_conn, &[record]) {
            eprintln!(
                "error: mount: failed to persist changes to '{name}': {err} - changes have been lost"
            );
        }
    }

    /// Resolves one completed chunk against the dedup index during
    /// persist, mirroring `store.rs`'s own `resolve_chunk` - a chunk hit
    /// reuses the existing chunk id, a miss reserves store space and
    /// writes the chunk's bytes. Also feeds the chunk's length and hash
    /// into `content_hasher` (see `contents.hash` in `db/src/migrations.rs`).
    fn resolve_persist_chunk(
        &self,
        chunk: SpilledChunk,
        chunk_refs: &mut Vec<db::ChunkRef>,
        content_hasher: &mut blake3::Hasher,
    ) -> Result<(), String> {
        let length_hash = chunk.length_hash;
        content_hasher.update(&length_hash.length.to_le_bytes());
        content_hasher.update(&length_hash.hash);

        let existing = {
            let conn = self.conn.lock().expect("db connection mutex poisoned");
            db::find_chunk(&conn, length_hash.length, &length_hash.hash)
        }
        .map_err(|err| format!("dedup lookup failed: {err}"))?;

        let chunk_ref = match existing {
            Some(id) => db::ChunkRef::Existing {
                id,
                length: length_hash.length,
            },
            None => {
                // `chunk.bytes` (dropped here on the dedup-hit branch above
                // without ever being drained) is a `WriteCache`, not a plain
                // `Vec<u8>` - see `SpillingHashingChunker`'s doc comment.
                let mut bytes = chunk.bytes;
                let extents = chunk_store::write_chunk_from_cache(
                    &self.data_store,
                    &self.allocator,
                    &mut bytes,
                    length_hash.length,
                    None,
                )
                .map_err(|err| format!("store write failed: {err}"))?;
                db::ChunkRef::New {
                    length: length_hash.length,
                    hash: length_hash.hash,
                    extents,
                }
            }
        };
        chunk_refs.push(chunk_ref);
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
                content: db::ContentSource::Resolved {
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
                },
            }],
        )
        .unwrap();
    }

    /// Creates a directory directly via `db::insert_directory` - the
    /// structural counterpart to `seed_file` above, for tests that need a
    /// tree shape (not just a file's content) in place before exercising
    /// the mount.
    fn seed_dir(repo_root: &Path, parent_id: i64, name: &str) -> i64 {
        let repository = db::open_repository(repo_root).unwrap();
        let conn = repository.open_write_connection().unwrap();
        db::insert_directory(&conn, parent_id, name, 0).unwrap()
    }

    fn build_test_filesystem(repo_root: &Path) -> DedupFs {
        build_filesystem(
            repo_root,
            true,
            DEFAULT_WRITE_CACHE_MB,
            None,
            false,
            Duration::ZERO,
        )
        .unwrap()
    }

    #[test]
    fn map_rename_error_matches_real_rename_semantics() {
        assert_eq!(
            map_rename_error(db::Error::AlreadyExists {
                parent_id: 0,
                name: "x".to_string()
            }),
            Errno::EEXIST
        );
        assert_eq!(
            map_rename_error(db::Error::TargetIsADirectory {
                parent_id: 0,
                name: "x".to_string()
            }),
            Errno::EISDIR
        );
        assert_eq!(
            map_rename_error(db::Error::TargetIsAFile {
                parent_id: 0,
                name: "x".to_string()
            }),
            Errno::ENOTDIR
        );
        assert_eq!(
            map_rename_error(db::Error::TargetNotEmpty {
                parent_id: 0,
                name: "x".to_string()
            }),
            Errno::ENOTEMPTY
        );
    }

    #[test]
    fn rename_replaces_a_compatible_active_target_by_default() {
        let (_temp_dir, repo_root) = init_repo();
        seed_file(&repo_root, 0, "a.txt", b"aaa");
        seed_file(&repo_root, 0, "b.txt", b"bbb");
        let fs = build_test_filesystem(&repo_root);

        fs.rename("/a.txt", "/b.txt", false).unwrap();

        let repository = db::open_repository(&repo_root).unwrap();
        let conn = repository.open_read_connection().unwrap();
        assert_eq!(db::find_tree_entry(&conn, 0, "a.txt").unwrap(), None);
        let entry = db::find_tree_entry(&conn, 0, "b.txt").unwrap().unwrap();
        assert_eq!(
            db::file_size(&conn, &entry).unwrap(),
            3,
            "b.txt now holds a.txt's content"
        );
    }

    #[test]
    fn rename_with_no_replace_still_fails_with_eexist_on_a_compatible_target() {
        let (_temp_dir, repo_root) = init_repo();
        seed_file(&repo_root, 0, "a.txt", b"aaa");
        seed_file(&repo_root, 0, "b.txt", b"bbb");
        let fs = build_test_filesystem(&repo_root);

        let result = fs.rename("/a.txt", "/b.txt", true);

        assert_eq!(result, Err(Errno::EEXIST));
    }

    #[test]
    fn rename_is_a_noop_for_a_self_rename() {
        let (_temp_dir, repo_root) = init_repo();
        seed_file(&repo_root, 0, "a.txt", b"aaa");
        let fs = build_test_filesystem(&repo_root);

        fs.rename("/a.txt", "/a.txt", false).unwrap();

        let repository = db::open_repository(&repo_root).unwrap();
        let conn = repository.open_read_connection().unwrap();
        let entry = db::find_tree_entry(&conn, 0, "a.txt").unwrap().unwrap();
        assert_eq!(db::file_size(&conn, &entry).unwrap(), 3);
    }

    #[test]
    fn rename_rejects_a_file_replacing_a_nonempty_directory() {
        let (_temp_dir, repo_root) = init_repo();
        seed_file(&repo_root, 0, "a.txt", b"aaa");
        let b = seed_dir(&repo_root, 0, "b");
        seed_file(&repo_root, b, "child.txt", b"c");
        let fs = build_test_filesystem(&repo_root);

        let result = fs.rename("/a.txt", "/b", false);

        assert_eq!(result, Err(Errno::EISDIR));
    }

    /// End-to-end regression test for the original bug report
    /// (`docs/plans/mount-rename-overwrite.md`): recovering a file from
    /// `[deleted]` onto an existing active file of the same name used to
    /// hang the client with `EEXIST` instead of replacing it.
    #[test]
    fn rename_recovers_from_deleted_and_replaces_an_existing_active_target() {
        let (_temp_dir, repo_root) = init_repo();
        let dir1 = seed_dir(&repo_root, 0, "1");
        let dir2 = seed_dir(&repo_root, 0, "2");
        seed_file(&repo_root, dir1, "file.txt", b"hello");
        seed_file(&repo_root, dir2, "file.txt", b"hello");
        {
            let repository = db::open_repository(&repo_root).unwrap();
            let conn = repository.open_write_connection().unwrap();
            let entry = db::find_tree_entry(&conn, dir1, "file.txt")
                .unwrap()
                .unwrap();
            db::soft_delete(&conn, entry.id, 500).unwrap();
        }

        let fs = build_test_filesystem(&repo_root);
        fs.rename("/1/[deleted]/file.txt", "/2/file.txt", false)
            .unwrap();

        let repository = db::open_repository(&repo_root).unwrap();
        let conn = repository.open_read_connection().unwrap();
        let entry = db::find_tree_entry(&conn, dir2, "file.txt")
            .unwrap()
            .unwrap();
        assert_eq!(db::file_size(&conn, &entry).unwrap(), 5);
    }

    #[test]
    fn build_filesystem_refuses_read_write_when_the_lock_is_already_held() {
        let (_temp_dir, repo_root) = init_repo();
        let _lock = repo_lock::RepoLock::acquire(&db::meta_dir(&repo_root), Duration::ZERO)
            .unwrap()
            .unwrap();

        let result = build_filesystem(
            &repo_root,
            true,
            DEFAULT_WRITE_CACHE_MB,
            None,
            false,
            Duration::ZERO,
        );

        assert!(result.is_err());
    }

    /// A read-only mount never touches the lock at all - it must keep
    /// working exactly as if nothing else were running, same as every
    /// other read-only command (and the same reason a read-only mount must
    /// work against a genuinely `:ro`-mounted repository directory).
    #[test]
    fn build_filesystem_read_only_ignores_an_existing_lock() {
        let (_temp_dir, repo_root) = init_repo();
        let _lock = repo_lock::RepoLock::acquire(&db::meta_dir(&repo_root), Duration::ZERO)
            .unwrap()
            .unwrap();

        let result = build_filesystem(
            &repo_root,
            false,
            DEFAULT_WRITE_CACHE_MB,
            None,
            false,
            Duration::ZERO,
        );

        assert!(result.is_ok());
    }

    /// A read-only mount (no `--read-write`) must never open a read-write
    /// connection to the metadata database - see
    /// `docs/plans/read-only-repository-access.md`. If it did, this would
    /// fail: a chmod-read-only database file can't be opened for writing.
    #[cfg(unix)]
    #[test]
    fn build_filesystem_read_only_works_even_when_the_database_file_is_not_writable() {
        use std::os::unix::fs::PermissionsExt;

        let (_temp_dir, repo_root) = init_repo();
        let db_path = db::db_file_path(&repo_root);
        let mut perms = std::fs::metadata(&db_path).unwrap().permissions();
        perms.set_mode(0o444);
        std::fs::set_permissions(&db_path, perms).unwrap();

        if let Err(err) = build_filesystem(
            &repo_root,
            false,
            DEFAULT_WRITE_CACHE_MB,
            None,
            false,
            Duration::ZERO,
        ) {
            panic!("{err}");
        }
    }

    /// Regression test for a real bug (not just a nicety): `statfs` used to
    /// unconditionally report zero total/free space, which - re-exported
    /// over Samba to a real Windows client - made every save fail with a
    /// "not enough space on disk" dialog despite plenty of real space being
    /// available (harmless over a direct Linux/WSL FUSE mount, which
    /// doesn't consult `statfs` before a write the way SMB clients do). No
    /// real mount needed to exercise this - `statfs` is a plain
    /// `MountFilesystem` method callable directly.
    #[test]
    fn statfs_reports_real_nonzero_free_space_not_the_old_always_zero_placeholder() {
        let (_temp_dir, repo_root) = init_repo();
        let fs = build_filesystem(
            &repo_root,
            false,
            DEFAULT_WRITE_CACHE_MB,
            None,
            false,
            Duration::ZERO,
        )
        .unwrap();

        let info = fs.statfs().unwrap();

        assert!(info.blocks > 0, "{info:?}");
        assert!(info.blocks_free > 0, "{info:?}");
        assert!(info.blocks_available > 0, "{info:?}");
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

        let fs = build_filesystem(
            &repo_root,
            false,
            DEFAULT_WRITE_CACHE_MB,
            None,
            false,
            Duration::ZERO,
        )
        .unwrap();
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

        let fs = build_filesystem(
            &repo_root,
            true,
            DEFAULT_WRITE_CACHE_MB,
            None,
            false,
            Duration::ZERO,
        )
        .unwrap();
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

    /// End-to-end phase 2b (content writes) test: mounts `--read-write`,
    /// and exercises real byte-level `write`/`read`/`truncate` through
    /// ordinary `std::fs` calls - a fresh file's content round-trips, an
    /// in-place overwrite lands correctly, `set_len` grows (zero-padding)
    /// and shrinks correctly, and identical final content across two
    /// different files still dedupes to one `contents` row, same as
    /// `store`'s own dedup.
    #[test]
    fn mounts_read_write_and_supports_content_writes() {
        let (_temp_dir, repo_root) = init_repo();
        {
            let repository = db::open_repository(&repo_root).unwrap();
            let conn = repository.open_write_connection().unwrap();
            db::insert_directory(&conn, 0, "marker", 0).unwrap();
        }

        let fs = build_filesystem(
            &repo_root,
            true,
            DEFAULT_WRITE_CACHE_MB,
            None,
            false,
            Duration::ZERO,
        )
        .unwrap();
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

        // Write content to a new file, read it back.
        std::fs::write(mount_path.join("a.txt"), b"hello write cache").unwrap();
        assert_eq!(
            std::fs::read(mount_path.join("a.txt")).unwrap(),
            b"hello write cache"
        );
        assert_eq!(
            std::fs::metadata(mount_path.join("a.txt")).unwrap().len(),
            17,
            "\"hello write cache\" is 17 bytes"
        );

        // Overwrite in the middle via an explicit seek+write.
        {
            use std::io::{Seek, SeekFrom, Write};
            let mut f = std::fs::OpenOptions::new()
                .write(true)
                .open(mount_path.join("a.txt"))
                .unwrap();
            f.seek(SeekFrom::Start(6)).unwrap();
            f.write_all(b"WRITE").unwrap();
        }
        assert_eq!(
            std::fs::read(mount_path.join("a.txt")).unwrap(),
            b"hello WRITE cache"
        );

        // set_len grow zero-pads; set_len shrink drops the tail.
        {
            let f = std::fs::OpenOptions::new()
                .write(true)
                .open(mount_path.join("a.txt"))
                .unwrap();
            f.set_len(20).unwrap();
        }
        let grown = std::fs::read(mount_path.join("a.txt")).unwrap();
        assert_eq!(grown.len(), 20);
        assert_eq!(&grown[..17], b"hello WRITE cache");
        assert_eq!(&grown[17..], &[0u8, 0, 0]);

        {
            let f = std::fs::OpenOptions::new()
                .write(true)
                .open(mount_path.join("a.txt"))
                .unwrap();
            f.set_len(5).unwrap();
        }
        assert_eq!(std::fs::read(mount_path.join("a.txt")).unwrap(), b"hello");

        // A second, unrelated file ending up with the same content must
        // dedupe to the same `contents` row.
        std::fs::write(mount_path.join("b.txt"), b"hello").unwrap();
        assert_eq!(std::fs::read(mount_path.join("b.txt")).unwrap(), b"hello");

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

        // Verify persisted state directly against the DB, after unmount
        // (so the mount's own write connection is guaranteed closed).
        let repository = db::open_repository(&repo_root).unwrap();
        let conn = repository.open_read_connection().unwrap();
        let a = db::resolve_path(&conn, "a.txt").unwrap().unwrap();
        let b = db::resolve_path(&conn, "b.txt").unwrap().unwrap();
        assert_eq!(
            a.content_id, b.content_id,
            "identical final content must dedupe to one contents row"
        );
        assert_eq!(db::file_size(&conn, &a).unwrap(), 5);
    }

    /// A bare `truncate(2)`/`O_TRUNC` on a file with no open handle now
    /// persists asynchronously (see `Inner::truncate`'s bare-path branch
    /// and `enqueue_persist`) - this exercises that path specifically
    /// (`std::fs::write` with `.truncate(true)` and no prior open) and
    /// verifies a `read` immediately after still observes the truncated
    /// (here: emptied) content, not the pre-truncate bytes, proving the
    /// persisting placeholder actually blocks the race.
    #[test]
    fn bare_truncate_without_a_handle_persists_before_a_racing_read_returns() {
        let (_temp_dir, repo_root) = init_repo();
        {
            let repository = db::open_repository(&repo_root).unwrap();
            let conn = repository.open_write_connection().unwrap();
            db::insert_directory(&conn, 0, "marker", 0).unwrap();
        }
        seed_file(&repo_root, 0, "a.txt", b"hello world");

        let fs = build_filesystem(
            &repo_root,
            true,
            DEFAULT_WRITE_CACHE_MB,
            None,
            false,
            Duration::ZERO,
        )
        .unwrap();
        let mount_dir = tempfile::tempdir().unwrap();
        let mount_path = mount_dir.path().to_path_buf();
        let handle = {
            let mount_path = mount_path.clone();
            std::thread::spawn(move || mountfs::mount(fs, &mount_path, false))
        };

        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if std::fs::read(mount_path.join("a.txt")).is_ok() {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "mount did not become ready within 5s"
            );
            std::thread::sleep(Duration::from_millis(50));
        }

        // A real bare `truncate(2)` on the path, via the `truncate(1)`
        // utility - deliberately not `OpenOptions::truncate(true)`: an
        // O_TRUNC *open* goes through `MountFilesystem::open` (which never
        // sees the O_TRUNC bit at all - `dispatch_open` only forwards
        // `write_intent`) rather than `MountFilesystem::truncate`, so it
        // wouldn't actually exercise the bare-truncate path this test is
        // for. `truncate(1)` issues the real path-only `truncate(2)`
        // syscall with no open handle involved, which does dispatch to
        // `Inner::truncate`'s bare-path branch.
        let status = std::process::Command::new("truncate")
            .arg("-s")
            .arg("0")
            .arg(mount_path.join("a.txt"))
            .status()
            .expect("failed to run truncate(1)");
        assert!(status.success(), "truncate -s 0 failed: {status}");

        assert_eq!(std::fs::read(mount_path.join("a.txt")).unwrap(), b"");

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

    /// End-to-end regression for `docs/plans/implemented/bounded-memory-io-pipeline.md`:
    /// `chunking: none` makes an entire file one chunk (see
    /// `cdc::SingleChunkChunker`), so `Inner::persist`'s `SpillingHashingChunker`
    /// must buffer it via disk spillover rather than needing it RAM-resident.
    /// `write_cache_mb: 0` forces spillover for every byte, both in the
    /// write cache itself and in the persist-time chunk buffer - the most
    /// aggressive case for both tiers at once.
    #[test]
    fn chunking_none_with_a_zero_byte_write_cache_still_round_trips_correctly() {
        let temp_dir = tempfile::tempdir().unwrap();
        let repo_root = temp_dir.path().join("repo");
        db::init_repository(
            &repo_root,
            &db::RepositorySettings::new(12, db::Chunking::None).unwrap(),
        )
        .unwrap();
        {
            let repository = db::open_repository(&repo_root).unwrap();
            let conn = repository.open_write_connection().unwrap();
            db::insert_directory(&conn, 0, "marker", 0).unwrap();
        }

        let fs = build_filesystem(&repo_root, true, 0, None, false, Duration::ZERO).unwrap();
        let mount_dir = tempfile::tempdir().unwrap();
        let mount_path = mount_dir.path().to_path_buf();
        let handle = {
            let mount_path = mount_path.clone();
            std::thread::spawn(move || mountfs::mount(fs, &mount_path, false))
        };

        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if std::fs::read_dir(&mount_path)
                .map(|mut e| e.next().is_some())
                .unwrap_or(false)
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "mount did not become ready within 5s"
            );
            std::thread::sleep(Duration::from_millis(50));
        }

        // Varied, non-trivial content spanning several `PERSIST_CHUNK_SIZE`
        // (256 KiB) pieces.
        let content: Vec<u8> = (0u32..300_000).map(|i| (i % 251) as u8).collect();
        std::fs::write(mount_path.join("a.txt"), &content).unwrap();
        assert_eq!(std::fs::read(mount_path.join("a.txt")).unwrap(), content);

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

        let repository = db::open_repository(&repo_root).unwrap();
        let conn = repository.open_read_connection().unwrap();
        let a = db::resolve_path(&conn, "a.txt").unwrap().unwrap();
        assert_eq!(db::file_size(&conn, &a).unwrap(), content.len() as i64);
    }

    // Manual before/after benchmarks for
    // docs/plans/memory-pressure-backpressure.md - not part of the normal
    // suite (#[ignore], slow and needs a real `libfuse3` mount), run
    // explicitly and their output captured to a committed results file
    // when comparing the persist-queue gate's behavior across a change.
    // Run with, e.g.:
    //   BACKUP_BENCH_THROTTLE_STORE_MBPS=30 cargo test --release --package cli \
    //     --bin backup mount::tests::bench_many_small_files -- --ignored --nocapture

    /// Sequential `open+write+close` for `BACKUP_BENCH_FILES` files of
    /// `BACKUP_BENCH_FILE_SIZE` bytes each (defaults: 200 files, 1 MB),
    /// with `BACKUP_BENCH_THROTTLE_STORE_MBPS` optionally capping the
    /// simulated datastore disk (see `store::bench_throttle_store_write`).
    /// Prints total wall-clock/throughput plus per-file latency
    /// percentiles - the shape (smooth vs. bursty/long-tailed) matters as
    /// much as the aggregate number. Payloads are precomputed with
    /// distinct content per file *before* timing starts: byte-identical
    /// payloads would CDC-dedup to one chunk after the first file (every
    /// later "write" then costs zero store I/O, defeating the point), and
    /// generating them inside the timed loop would count CPU-bound fill
    /// time as write time.
    #[test]
    #[ignore]
    fn bench_many_small_files() {
        let (_temp_dir, repo_root) = init_repo();
        {
            let repository = db::open_repository(&repo_root).unwrap();
            let conn = repository.open_write_connection().unwrap();
            db::insert_directory(&conn, 0, "marker", 0).unwrap();
        }

        let fs = build_filesystem(
            &repo_root,
            true,
            DEFAULT_WRITE_CACHE_MB,
            None,
            false,
            Duration::ZERO,
        )
        .unwrap();
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

        let file_count: usize = std::env::var("BACKUP_BENCH_FILES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(200);
        let file_size: usize = std::env::var("BACKUP_BENCH_FILE_SIZE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1_000_000);
        let sub = mount_path.join("marker");

        let payloads: Vec<Vec<u8>> = (0..file_count)
            .map(|i| {
                let pattern = (i as u64).wrapping_mul(2_654_435_761).to_le_bytes();
                let mut payload = vec![0u8; file_size];
                for chunk in payload.chunks_mut(8) {
                    chunk.copy_from_slice(&pattern[..chunk.len()]);
                }
                payload
            })
            .collect();

        let mut iter_millis = Vec::with_capacity(file_count);
        let overall_start = Instant::now();
        for (i, payload) in payloads.iter().enumerate() {
            let start = Instant::now();
            std::fs::write(sub.join(format!("f{i}.bin")), payload).unwrap();
            iter_millis.push(start.elapsed().as_secs_f64() * 1000.0);
        }
        let total = overall_start.elapsed();

        iter_millis.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let p50 = iter_millis[iter_millis.len() / 2];
        let p95 = iter_millis[iter_millis.len() * 95 / 100];
        let max = *iter_millis.last().unwrap();
        let total_bytes = file_count * file_size;
        println!(
            "files={file_count} size={file_size}B total_bytes={total_bytes}B \
             close_loop={total:?} close_loop_throughput={:.2}MB/s p50={p50:.1}ms \
             p95={p95:.1}ms max={max:.1}ms",
            total_bytes as f64 / total.as_secs_f64() / 1_000_000.0
        );

        // Unmounting has to wait for the background persist thread to
        // fully drain (see `on_unmount`) - times how long that actually
        // takes, separately from the close-loop above, since `close()`
        // returning doesn't mean the file's bytes are safely on disk yet.
        let drain_start = Instant::now();
        let status = std::process::Command::new("fusermount3")
            .arg("-u")
            .arg(&mount_path)
            .status()
            .expect("failed to run fusermount3 -u");
        assert!(status.success(), "fusermount3 -u failed: {status}");
        let drain = drain_start.elapsed();
        println!(
            "drain_on_unmount={drain:?} end_to_end={:?} end_to_end_throughput={:.2}MB/s",
            total + drain,
            total_bytes as f64 / (total + drain).as_secs_f64() / 1_000_000.0
        );
        handle
            .join()
            .expect("mount thread panicked")
            .expect("mount() returned an error");
    }

    /// Writes `BACKUP_BENCH_LARGE_FILES` large files (default 6, 50 MB
    /// each) with `write_cache_mb: 1` (forces near-immediate spill for
    /// every byte) against a datastore throttled slow enough that
    /// `persist_worker` can't keep up, sampling the mount's spill
    /// directory's on-disk size after each file closes - demonstrating
    /// directly how much spilled-but-unpersisted data can accumulate at
    /// once under whatever gate `enqueue_persist` currently uses.
    #[test]
    #[ignore]
    fn bench_large_files_spill_pressure() {
        let (_temp_dir, repo_root) = init_repo();
        {
            let repository = db::open_repository(&repo_root).unwrap();
            let conn = repository.open_write_connection().unwrap();
            db::insert_directory(&conn, 0, "marker", 0).unwrap();
        }

        let fs = build_filesystem(&repo_root, true, 1, None, false, Duration::ZERO).unwrap();
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

        // `create_spill_dir("backup-mount-write-cache-", None)` creates
        // exactly one fresh subdirectory of the OS temp dir for this
        // mount - find it by its unique prefix (rather than reaching into
        // `Inner`, private with no accessor) instead of walking the whole
        // temp dir, which can be very slow if it holds unrelated content.
        let spill_root = std::fs::read_dir(std::env::temp_dir())
            .unwrap()
            .flatten()
            .map(|e| e.path())
            .find(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with("backup-mount-write-cache-"))
            })
            .expect("build_filesystem must have created its spill dir by now");
        let du = |root: &Path| -> u64 {
            fn walk(dir: &Path, total: &mut u64) {
                let Ok(entries) = std::fs::read_dir(dir) else {
                    return;
                };
                for entry in entries.flatten() {
                    if let Ok(meta) = entry.metadata() {
                        if meta.is_dir() {
                            walk(&entry.path(), total);
                        } else {
                            *total += meta.len();
                        }
                    }
                }
            }
            let mut total = 0;
            walk(root, &mut total);
            total
        };
        let baseline_temp_bytes = du(&spill_root);

        let file_count: usize = std::env::var("BACKUP_BENCH_LARGE_FILES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(6);
        let file_size: usize = std::env::var("BACKUP_BENCH_LARGE_FILE_SIZE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(50_000_000);
        let sub = mount_path.join("marker");

        let payloads: Vec<Vec<u8>> = (0..file_count)
            .map(|i| {
                let pattern = (i as u64).wrapping_mul(2_654_435_761).to_le_bytes();
                let mut payload = vec![0u8; file_size];
                for chunk in payload.chunks_mut(8) {
                    chunk.copy_from_slice(&pattern[..chunk.len()]);
                }
                payload
            })
            .collect();

        let mut peak_spilled = 0u64;
        let overall_start = Instant::now();
        for (i, payload) in payloads.iter().enumerate() {
            let start = Instant::now();
            std::fs::write(sub.join(format!("big{i}.bin")), payload).unwrap();
            let elapsed = start.elapsed();
            let spilled = du(&spill_root).saturating_sub(baseline_temp_bytes);
            peak_spilled = peak_spilled.max(spilled);
            println!(
                "file {i}: close={elapsed:?} spill_dir_now={:.1}MB peak_so_far={:.1}MB",
                spilled as f64 / 1_000_000.0,
                peak_spilled as f64 / 1_000_000.0
            );
        }
        let write_loop = overall_start.elapsed();

        // `fusermount3 -u` itself only requests the unmount - it doesn't
        // wait for `on_unmount`'s drain (persist_worker fully catching
        // up) to finish. That happens inside `mountfs::mount`'s own
        // blocking call, on the background thread `handle` joins below -
        // timed separately, since it's the number that actually answers
        // "how long until every closed file's bytes are safely durable".
        let unmount_start = Instant::now();
        let status = std::process::Command::new("fusermount3")
            .arg("-u")
            .arg(&mount_path)
            .status()
            .expect("failed to run fusermount3 -u");
        assert!(status.success(), "fusermount3 -u failed: {status}");
        let unmount_request = unmount_start.elapsed();
        handle
            .join()
            .expect("mount thread panicked")
            .expect("mount() returned an error");
        let drain = unmount_start.elapsed();

        println!(
            "files={file_count} size={file_size}B write_loop={write_loop:?} \
             unmount_request={unmount_request:?} full_drain={drain:?} \
             peak_spilled={:.1}MB ({:.1}x a single file)",
            peak_spilled as f64 / 1_000_000.0,
            peak_spilled as f64 / file_size as f64
        );
    }
}
