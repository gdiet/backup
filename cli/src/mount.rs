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

use cdc::CdcConfig;
use clap::Args;
use mountfs::{Attr, DirEntry, Errno, FileKind, Handle, MountFilesystem};
use rusqlite::Connection;
use store::{LongTermStore, ReadIntegrity};

use crate::chunk_store::{self, SpaceAllocator, read_chunk_bytes};
use crate::ram_budget_check::check_ram_budget;
use crate::spilling_chunker::{SpilledChunk, SpillingHashingChunker};
use crate::store::{Blake3Hasher, HASH_LENGTH, make_chunker};
use crate::temp_dir::{create_spill_dir, validate_temp_dir};
use crate::write_cache::{RamBudget, WriteCache};

/// Default RAM budget for `backup mount --read-write`'s write cache (see
/// `MountArgs::write_cache_mb`), shared across every file open for writing
/// at once, *and* reused as the budget for in-flight persist chunk
/// buffering (see [`Inner::persist`]) - a modest default: both are soft
/// buffers that spill to disk once exceeded (see `write_cache::
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

/// How many closed-and-dirty files' persists can be queued ahead of the
/// background persist thread (see `persist_worker`) before a *new*
/// `release`/bare `truncate` call starts blocking its own FUSE/WinFSP
/// worker thread waiting for room - the actual backpressure point. A
/// small constant, not a CLI flag: each queued job already holds its own
/// `WriteCache` (RAM-budgeted with disk spillover via `Inner::ram_budget`/
/// `spill_dir`, same as any other open file), so this only bounds how many
/// *recently closed* files can have unpersisted changes in flight at
/// once, not memory directly - a handful is enough to smooth a burst of
/// closes without needing to be generous. See
/// `docs/plans/bounded-memory-io-pipeline.md`'s "Mount-specific detail"
/// section for the failure mode this exists to fix (synchronous persist
/// exhausting the whole worker-thread pool under a sustained slow target
/// disk).
const PERSIST_QUEUE_CAPACITY: usize = 4;

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
    /// `restore`. Do not run `store`/`del`/`reclaim-space` against the
    /// same repository while a read-write mount is active - both need the
    /// single write connection this holds for the mount's whole lifetime.
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
    ) {
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
fn build_filesystem(
    repo: &Path,
    read_write: bool,
    write_cache_mb: u64,
    temp: Option<&Path>,
) -> Result<DedupFs, String> {
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
    // Seeded once, like `store`'s own allocator - reused across every
    // persist for this mount's whole lifetime (not just one command's).
    let extents = db::chunk_extents_sorted(&write_conn)
        .map_err(|err| format!("failed to determine free store space: {err}"))?;
    let allocator = SpaceAllocator::from_sorted_extents(&extents);
    let cdc_config = match repository.settings().chunking() {
        db::Chunking::Cdc => Some(
            CdcConfig::new(repository.settings().cdc_target_size_bits())
                .expect("validated by RepositorySettings"),
        ),
        db::Chunking::None => None,
    };
    // `read_only` mirrors the `--read-write` flag, not hardcoded `true`
    // like the read-only-only phase used: a read-write mount's persist
    // pipeline needs to actually write new chunk bytes to the store.
    let data_store = LongTermStore::new(repository.data_dir(), !read_write);
    // A dedicated, uniquely-named spill directory for write-cache overflow
    // (see `write_cache::WriteCache`) - created empty here, removed whole
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
    // state safely.
    let (persist_tx, persist_rx) = mpsc::sync_channel::<PersistJob>(PERSIST_QUEUE_CAPACITY);
    let inner = Arc::new(Inner {
        read_only: !read_write,
        conn: Mutex::new(conn),
        write_conn: Mutex::new(write_conn),
        data_store,
        allocator,
        cdc_config,
        ram_budget: Arc::new(RamBudget::new(write_cache_mb * 1024 * 1024)),
        spill_dir,
        spill_id_seq: AtomicU64::new(0),
        write_states: Mutex::new(HashMap::new()),
        write_states_cv: Condvar::new(),
        persist_tx: Mutex::new(Some(persist_tx)),
        persist_thread: Mutex::new(None),
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
/// See [`PERSIST_QUEUE_CAPACITY`]'s doc comment for why this is queued
/// rather than persisted inline on the calling thread.
struct PersistJob {
    tree_id: i64,
    cache: WriteCache,
    mtime_millis: i64,
}

/// The single background thread every persist actually runs on (spawned
/// once in [`build_filesystem`], joined in [`Inner::on_unmount`]) - moving
/// persist off whichever FUSE/WinFSP worker thread called `release`/bare
/// `truncate` is what fixes the worker-pool-exhaustion failure mode (see
/// [`PERSIST_QUEUE_CAPACITY`]'s doc comment): that thread now only has to
/// enqueue a job (fast, unless the queue is already full) instead of
/// blocking for as long as the target store's disk takes. Serial by
/// design, mirroring the Scala prototype's own single background persist
/// thread - also means at most one persist is ever actually writing to
/// the store at a time, which if anything makes the pre-existing,
/// deliberately-tolerated chunk-write race (`db::apply_backup_batch`'s
/// `ON CONFLICT DO NOTHING` handling) less likely to fire, not more.
fn persist_worker(inner: Arc<Inner>, jobs: mpsc::Receiver<PersistJob>) {
    for job in jobs {
        inner.persist(job.tree_id, job.cache, job.mtime_millis);
        inner.finish_persisting(job.tree_id);
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
    conn: Mutex<Connection>,
    /// Held for the mount's whole lifetime - see `MountArgs::read_write`'s
    /// doc comment on why `store`/`del`/`reclaim-space` mustn't run
    /// concurrently against the same repository while this is open.
    write_conn: Mutex<Connection>,
    data_store: LongTermStore,
    /// Reserves store space for new chunks written by the phase 2b persist
    /// pipeline - see `chunk_store::SpaceAllocator`.
    allocator: SpaceAllocator,
    cdc_config: Option<CdcConfig>,
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
    /// lock) by `enqueue_persist`, since `send` is the part that can block
    /// once the queue is full.
    persist_tx: Mutex<Option<mpsc::SyncSender<PersistJob>>>,
    /// The background thread `persist_worker` runs on - `None` before
    /// `build_filesystem` finishes spawning it, and after `on_unmount` has
    /// joined it.
    persist_thread: Mutex<Option<JoinHandle<()>>>,
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
    /// [`PERSIST_QUEUE_CAPACITY`]) - a program that closes and immediately
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

    fn rename(&self, old_path: &str, new_path: &str) -> Result<(), Errno> {
        self.0.rename(old_path, new_path)
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
        let entry = self.resolve_active_entry(path)?;
        if entry.kind != db::EntryKind::File {
            return Err(Errno::EISDIR);
        }
        self.register_open(entry.id, entry.time_millis);
        Ok(Handle(entry.id as u64))
    }

    fn read(&self, handle: Handle, offset: u64, size: u32) -> Result<Vec<u8>, Errno> {
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
        // Approximate/unused values - Scala's own Linux FUSE implementation
        // is a no-op here too; not worth over-building for a read-only mount.
        Ok(mountfs::StatfsInfo {
            block_size: 512,
            max_name_length: 255,
            ..Default::default()
        })
    }

    fn mkdir(&self, path: &str) -> Result<(), Errno> {
        if self.read_only {
            return Err(Errno::EROFS);
        }
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
        if self.read_only {
            return Err(Errno::EROFS);
        }
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
        // Starts empty - the same shape `open`/`read` already treat a
        // zero-length file as (`content_id IS NULL`); an ensuing `write`
        // (phase 2b) materializes a real write cache for it on demand,
        // same as `open`ing an existing file for writing.
        let time_millis = now_millis();
        db::apply_backup_batch(
            &mut conn,
            &[db::FileBackupRecord {
                parent_id: parent.id,
                name: name.to_string(),
                time_millis,
                chunks: Vec::new(),
                content_hash: Vec::new(),
            }],
        )
        .map_err(|_| Errno::EIO)?;
        let entry = db::find_tree_entry(&conn, parent.id, name)
            .map_err(|_| Errno::EIO)?
            .ok_or(Errno::EIO)?;
        drop(conn);
        self.register_open(entry.id, time_millis);
        Ok(Handle(entry.id as u64))
    }

    fn unlink(&self, path: &str) -> Result<(), Errno> {
        if self.read_only {
            return Err(Errno::EROFS);
        }
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
        if self.read_only {
            return Err(Errno::EROFS);
        }
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
        if self.read_only {
            return Err(Errno::EROFS);
        }
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
        if self.read_only {
            return Err(Errno::EROFS);
        }
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

    /// Hands `cache` off to the background persist thread ([`persist_worker`])
    /// instead of persisting on the calling thread - blocks only once
    /// [`PERSIST_QUEUE_CAPACITY`] persists are already queued ahead of it
    /// (the intended backpressure point). The caller must already have set
    /// `persisting = true` on this `tree_id`'s [`FileWriteState`] before
    /// calling this (both `release` and bare `truncate` do) so a racing
    /// `read`/`getattr`/`open` on the same id blocks via
    /// `wait_while_persisting` until [`Inner::finish_persisting`] clears
    /// it, rather than observing stale pre-persist content after this call
    /// has already returned.
    fn enqueue_persist(&self, tree_id: i64, cache: WriteCache, mtime_millis: i64) {
        // Cloned out from under the lock rather than sent while holding
        // it - `send` is the blocking part once the queue is full, and
        // holding `persist_tx`'s mutex through that would serialize every
        // *unrelated* enqueue attempt behind whichever one happened to
        // fill the queue first, not just the ones actually contending for
        // queue space.
        let tx = self
            .persist_tx
            .lock()
            .expect("persist queue mutex poisoned")
            .clone();
        let job = PersistJob {
            tree_id,
            cache,
            mtime_millis,
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
    /// already bounded (see `docs/plans/bounded-memory-io-pipeline.md`).
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
            make_chunker(&self.cdc_config),
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
            chunks: chunk_refs,
            content_hash: content_hash.to_vec(),
        };
        let mut write_conn = self
            .write_conn
            .lock()
            .expect("write connection mutex poisoned");
        if let Err(err) = db::apply_backup_batch(&mut write_conn, &[record]) {
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

        let fs = build_filesystem(&repo_root, false, DEFAULT_WRITE_CACHE_MB, None).unwrap();
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

        let fs = build_filesystem(&repo_root, true, DEFAULT_WRITE_CACHE_MB, None).unwrap();
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

        let fs = build_filesystem(&repo_root, true, DEFAULT_WRITE_CACHE_MB, None).unwrap();
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

        let fs = build_filesystem(&repo_root, true, DEFAULT_WRITE_CACHE_MB, None).unwrap();
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

    /// End-to-end regression for `docs/plans/bounded-memory-io-pipeline.md`:
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

        let fs = build_filesystem(&repo_root, true, 0, None).unwrap();
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
}
