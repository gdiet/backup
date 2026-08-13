use std::cell::RefCell;
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use cdc::{ChunkHasher, ChunkerConfig};
use clap::Args;
use rayon::ThreadPoolBuilder;
use rayon::prelude::*;
use rusqlite::Connection;
use walkdir::WalkDir;

use crate::backup_ignore::{self, IgnoreRule, OwnIgnoreFile};
use crate::chunk_store::{self, SpaceAllocator};
use crate::io_limiter::IoLimiter;
use crate::ram_budget_check::check_ram_budget;
use crate::repo_lock::RepoLock;
use crate::spilling_chunker::{SpilledChunk, SpillingHashingChunker};
use crate::temp_dir::{create_spill_dir, validate_temp_dir};
use spillcache::RamBudget;

/// Number of bytes read from a file at a time.
const READ_BUFFER_SIZE: usize = 64 * 1024;

/// Default RAM budget, in megabytes, for buffering an in-progress chunk's
/// bytes while its dedup status is resolved (see [`BackupArgs::chunk_buffer_mb`]
/// and [`crate::spilling_chunker::SpillingHashingChunker`]) - shared across
/// every concurrent worker, same spirit as `mount --read-write`'s
/// `--write-cache-mb` (see its doc comment in `mount.rs`).
const DEFAULT_CHUNK_BUFFER_MB: u64 = 128;

/// Number of hash bytes taken from blake3's extendable output, for both chunk
/// hashes and the content hash. `pub(crate)`: `mount.rs`'s phase 2b persist
/// pipeline reuses this and [`Blake3Hasher`] rather than duplicating them.
pub(crate) const HASH_LENGTH: usize = 20;

/// Records queued by workers are flushed to the database once this many have
/// accumulated, or after `WRITE_BATCH_IDLE_TIMEOUT` since the last flush,
/// whichever comes first - so small backup runs don't stall waiting for a full
/// batch.
const WRITE_BATCH_SIZE: usize = 200;
const WRITE_BATCH_IDLE_TIMEOUT: Duration = Duration::from_millis(200);

#[derive(Args)]
pub struct BackupArgs {
    /// Create missing target directories.
    #[arg(short = 'p', long = "create-dirs", conflicts_with = "target_exists")]
    create_dirs: bool,

    /// Require target to be an existing directory.
    #[arg(short = 't', long = "target-exists")]
    target_exists: bool,

    /// Number of concurrent chunking threads (1-32). Default: rayon's global thread
    /// pool (one thread per CPU core).
    #[arg(short = 'c', long, value_parser = clap::value_parser!(u32).range(1..=32))]
    concurrency: Option<u32>,

    /// Maximum number of concurrent writes into the repository's data store
    /// (1-32). This is independent of `--concurrency`: `--concurrency`
    /// controls how many threads run the CPU-bound read/chunk/hash
    /// pipeline, while this controls how many of those threads may be
    /// inside a store write at once - the hardware-optimal number of
    /// concurrent I/O operations against the disk/network share behind the
    /// repository is often much smaller (or larger) than the number of CPU
    /// cores. Default: unlimited (as many concurrent store writes as
    /// `--concurrency` allows chunking threads). See
    /// `docs/plans/implemented/bounded-memory-io-pipeline.md`.
    #[arg(long, value_parser = clap::value_parser!(u32).range(1..=32))]
    store_io_parallelism: Option<u32>,

    /// RAM budget, in megabytes, for buffering an in-progress chunk's bytes
    /// while its dedup status is resolved - shared across every concurrent
    /// worker (see `--concurrency`). A chunk that exceeds this budget spills
    /// to a temp file instead of failing; without this, a single large CDC
    /// chunk, or an entire file under `chunking: none`, would need to be
    /// fully RAM-resident at once (see
    /// `docs/plans/implemented/bounded-memory-io-pipeline.md`).
    #[arg(long, default_value_t = DEFAULT_CHUNK_BUFFER_MB)]
    chunk_buffer_mb: u64,

    /// Start anyway if `--chunk-buffer-mb` looks large enough, relative to
    /// currently available RAM, to risk pushing the machine into swapping.
    #[arg(long)]
    allow_swap_risk: bool,

    /// Directory to create this run's chunk-buffer spillover directory in
    /// (see `--chunk-buffer-mb`) - must already exist and be writable.
    /// Defaults to the OS temp directory (`std::env::temp_dir()`) if not
    /// given. For best throughput, point this at the fastest disk
    /// available, ideally not the same physical drive as either a source
    /// or the repository itself.
    #[arg(long)]
    temp: Option<PathBuf>,

    /// A repository path (not a filesystem path) to an earlier backup run,
    /// used to skip reading/chunking/hashing a source file entirely when a
    /// same-named file exists there with matching size and modified time -
    /// the new tree entry just reuses that file's content, with no I/O on
    /// the source file at all. `*`/`?` wildcards are resolved per
    /// `/`-separated path segment, picking the alphabetically last match at
    /// each segment - e.g. `backup/????/????.??.??_*` resolves to the
    /// latest-named year, then within it the latest-named run. Before use,
    /// checked against the given sources for plausibility (see
    /// `--force-reference`); see `docs/plans/implemented/backup-reference.md`
    /// for the full design.
    #[arg(long)]
    reference: Option<PathBuf>,

    /// Skip the plausibility check `--reference` normally runs first (that
    /// the reference directory's contents look similar enough to the given
    /// sources to plausibly be an earlier backup of them) - use when you're
    /// confident the reference is correct despite the check failing. No
    /// effect without `--reference`.
    #[arg(long)]
    force_reference: bool,

    /// How long to wait, in seconds, for the repository's lock to become
    /// free if another `store`/`mount --read-write`/`compact-store`/
    /// `reclaim-space` run already holds it, before giving up. Default:
    /// don't wait, fail immediately.
    #[arg(long = "lock-wait", default_value_t = 0)]
    lock_wait_secs: u64,

    /// One or more source paths followed by the target path in the repository.
    #[arg(required = true, num_args = 2.., value_name = "PATH")]
    paths: Vec<PathBuf>,
}

/// A [`ChunkHasher`] backed by [`blake3::Hasher`].
///
/// A local newtype is required because Rust's orphan rule forbids implementing a
/// foreign trait (`cdc::ChunkHasher`) for a foreign type (`blake3::Hasher`) directly.
pub(crate) struct Blake3Hasher(pub(crate) blake3::Hasher);

impl ChunkHasher for Blake3Hasher {
    fn update(&mut self, data: &[u8]) {
        self.0.update(data);
    }

    fn finalize_reset(&mut self) -> Vec<u8> {
        let mut hash = [0u8; HASH_LENGTH];
        self.0.finalize_xof().fill(&mut hash);
        self.0.reset();
        hash.to_vec()
    }
}

/// Shared state for one `store` run, read by every worker thread and the writer
/// thread. `abort`/`warnings` are also cloned out separately by [`run_store`] so
/// their final values can be read after this whole context (and the [`Mutex`]
/// around the channel [`mpsc::Sender`] it owns) has been dropped. Dropping this
/// context alone doesn't make the writer thread's `Receiver` see the channel
/// disconnect and exit, though - see [`run_store`]'s own `outlasting_tx` for
/// the extra `Sender` clone that also has to go first.
struct RunContext {
    repository: db::Repository,
    chunker_config: ChunkerConfig,
    data_store: store::LongTermStore,
    /// Reserves store space for new chunks' bytes, reusing gaps left by past
    /// `reclaim-space` runs before falling back to appending - see
    /// `chunk_store::SpaceAllocator`. Seeded once from every extent
    /// currently in the repository; multiple workers reserve from it
    /// concurrently under its own internal lock.
    allocator: SpaceAllocator,
    /// Bounds concurrent `data_store` writes independently of how many
    /// chunking workers `--concurrency` runs - see [`BackupArgs::
    /// store_io_parallelism`]. `None` when the flag wasn't given: every
    /// worker writes to the store without waiting for a permit, same as
    /// before this existed.
    io_limiter: Option<IoLimiter>,
    abort: Arc<AtomicBool>,
    warnings: Arc<AtomicU64>,
    sender: Mutex<mpsc::Sender<db::FileBackupRecord>>,
    /// Shared RAM budget for [`SpillingHashingChunker`]'s per-worker
    /// in-progress chunk buffering - see [`BackupArgs::chunk_buffer_mb`].
    chunk_buffer_budget: Arc<RamBudget>,
    /// This run's private temp directory for chunk-buffer disk spillover -
    /// see [`run_store`], removed whole once every spill file in it (each
    /// deleted by its own `WriteCache`'s `Drop`) is gone.
    spill_dir: PathBuf,
    spill_id_seq: AtomicU64,
}

impl RunContext {
    /// A private, never-yet-created spill path for a new chunk buffer -
    /// mirrors `mount.rs`'s `Inner::spill_path`.
    fn spill_path(&self) -> PathBuf {
        let id = self.spill_id_seq.fetch_add(1, Ordering::Relaxed);
        self.spill_dir.join(id.to_string())
    }
}

thread_local! {
    // One read connection per worker OS thread, opened lazily on first use and
    // reused for every file that thread processes - see the db crate's
    // module-level doc comment on why this is one-per-thread, not one-per-file
    // or a single connection shared across threads.
    static READ_CONNECTION: RefCell<Option<Connection>> = const { RefCell::new(None) };
}

fn with_read_connection<R>(
    ctx: &RunContext,
    f: impl FnOnce(&Connection) -> R,
) -> Result<R, db::Error> {
    READ_CONNECTION.with(|cell| {
        let mut slot = cell.borrow_mut();
        if slot.is_none() {
            *slot = Some(ctx.repository.open_read_connection()?);
        }
        Ok(f(slot.as_ref().expect("just set above")))
    })
}

pub fn run_store(repo: &Path, args: BackupArgs) -> ExitCode {
    if let Err(msg) = check_ram_budget(
        "chunk-buffer-mb",
        args.chunk_buffer_mb,
        args.allow_swap_risk,
    ) {
        eprintln!("error: {msg}");
        return ExitCode::FAILURE;
    }
    if let Some(temp) = &args.temp
        && let Err(msg) = validate_temp_dir(temp)
    {
        eprintln!("error: {msg}");
        return ExitCode::FAILURE;
    }

    let (sources, target) = args.paths.split_at(args.paths.len() - 1);
    let target = &target[0];

    let source_errors: Vec<String> = sources
        .iter()
        .filter_map(|source| check_source_readable(source).err())
        .collect();
    if !source_errors.is_empty() {
        for msg in &source_errors {
            eprintln!("error: {msg}");
        }
        return ExitCode::FAILURE;
    }

    let repository = match db::open_repository(repo) {
        Ok(r) => r,
        Err(err) => {
            eprintln!(
                "error: failed to open repository at {}: {err}",
                repo.display()
            );
            return ExitCode::FAILURE;
        }
    };
    let chunker_config = ChunkerConfig::new(match repository.settings().chunking() {
        db::Chunking::Cdc => Some(repository.settings().cdc_target_size_bits()),
        db::Chunking::None => None,
    })
    .expect("validated by RepositorySettings");

    // Exclusive against every other command that physically
    // allocates/relocates store bytes (`store` itself, `mount --read-write`,
    // `compact-store`, `reclaim-space`) - see
    // `docs/plans/cross-process-repository-locking.md`. Held for this whole
    // run via `_lock`'s `Drop`.
    let _lock = match RepoLock::acquire(
        &db::meta_dir(repo),
        Duration::from_secs(args.lock_wait_secs),
    ) {
        Ok(Some(lock)) => lock,
        Ok(None) => {
            eprintln!(
                "error: another command is already running against this repository \
                 (meta/.lock is held) - try again once it finishes, or pass --lock-wait to wait"
            );
            return ExitCode::FAILURE;
        }
        Err(err) => {
            eprintln!("error: failed to acquire the repository lock: {err}");
            return ExitCode::FAILURE;
        }
    };

    // A single connection drives the up-front target/reference resolution and
    // directory pass below (all on the main thread, before any parallel work
    // starts), then is handed to the writer thread by value - see
    // RunContext's doc comment.
    let mut main_conn = match repository.open_write_connection() {
        Ok(conn) => conn,
        Err(err) => {
            eprintln!("error: failed to open the metadata database: {err}");
            return ExitCode::FAILURE;
        }
    };

    let ref_root_id = match &args.reference {
        None => None,
        Some(reference) => match resolve_reference(&main_conn, reference) {
            Ok(id) => {
                if !args.force_reference
                    && let Err(msg) = validate_reference(&main_conn, sources, id)
                {
                    eprintln!("error: {msg}");
                    return ExitCode::FAILURE;
                }
                Some(id)
            }
            Err(msg) => {
                eprintln!("error: {msg}");
                return ExitCode::FAILURE;
            }
        },
    };

    let target_id = match resolve_target(&main_conn, target, args.create_dirs, args.target_exists) {
        Ok(id) => id,
        Err(msg) => {
            eprintln!("error: {msg}");
            return ExitCode::FAILURE;
        }
    };

    let mut files: Vec<(PathBuf, i64)> = Vec::new();
    let mut reference_hits: Vec<db::FileBackupRecord> = Vec::new();
    let mut warning_count = 0u64;
    for source in sources {
        if let Err(msg) = walk_and_create_dirs(
            &main_conn,
            source,
            target_id,
            ref_root_id,
            &mut files,
            &mut reference_hits,
            &mut warning_count,
        ) {
            eprintln!("error: {msg}");
            return ExitCode::FAILURE;
        }
    }

    if !reference_hits.is_empty()
        && let Err(err) = db::apply_backup_batch(&mut main_conn, &reference_hits)
    {
        eprintln!("error: failed to apply reference-hit backup batch: {err}");
        return ExitCode::FAILURE;
    }

    let extents = match db::chunk_extents_sorted(&main_conn) {
        Ok(extents) => extents,
        Err(err) => {
            eprintln!("error: failed to determine free store space: {err}");
            return ExitCode::FAILURE;
        }
    };
    let allocator = SpaceAllocator::from_sorted_extents(&extents);

    let data_store = store::LongTermStore::new(repository.data_dir(), false);
    let (tx, rx) = mpsc::channel::<db::FileBackupRecord>();
    // A second, otherwise-unused `Sender` clone, kept alive on this (the
    // main) thread and dropped explicitly below, only *after* the
    // `pool.broadcast` call down there has already, synchronously, closed
    // every chunking worker's read connection - see that call's own comment
    // for why a synchronous close is necessary in the first place, not just
    // dropping the pool. Without `outlasting_tx`, the channel disconnects
    // (and the writer thread below, which owns `main_conn`, finishes) as
    // soon as `ctx` drops - i.e. as soon as `pool.install(run)` returns,
    // well before `pool.broadcast` even runs - reopening exactly the same
    // race `pool.broadcast` closes. Confirmed by real, reproducible
    // failures (both with and without `--concurrency`, see
    // `cli/tests/store_checkpoint.rs`) that this ordering matters in
    // practice, not just in theory: a non-empty, tens-of-KB leftover `-wal`
    // after a `store` run that itself exited perfectly cleanly.
    let outlasting_tx = tx.clone();
    let abort = Arc::new(AtomicBool::new(false));
    let warnings = Arc::new(AtomicU64::new(warning_count));

    // A dedicated, uniquely-named spill directory for chunk-buffer
    // overflow (see `spilling_chunker::SpillingHashingChunker`) - created
    // empty here, removed whole below once every spill file in it (each
    // deleted by its own `WriteCache`'s `Drop`) is gone. Under `--temp` if
    // given (already validated above), otherwise under the OS default -
    // see `create_spill_dir`'s doc comment for why this goes through
    // `tempfile::Builder` rather than `std::process::id()`.
    let spill_dir = match create_spill_dir("backup-store-chunk-buffer-", args.temp.as_deref()) {
        Ok(dir) => dir,
        Err(err) => {
            eprintln!("error: failed to create chunk-buffer temp dir: {err}");
            return ExitCode::FAILURE;
        }
    };

    let ctx = Arc::new(RunContext {
        repository,
        chunker_config,
        data_store,
        allocator,
        io_limiter: args.store_io_parallelism.map(IoLimiter::new),
        abort: Arc::clone(&abort),
        warnings: Arc::clone(&warnings),
        sender: Mutex::new(tx),
        chunk_buffer_budget: Arc::new(RamBudget::new(args.chunk_buffer_mb * 1024 * 1024)),
        spill_dir: spill_dir.clone(),
        spill_id_seq: AtomicU64::new(0),
    });

    let writer_abort = Arc::clone(&abort);
    let writer_handle = thread::spawn(move || run_writer(main_conn, rx, writer_abort));

    // `ctx` is moved into this closure and dropped when it returns (after
    // all files are processed), which drops *its* Sender - but the channel
    // doesn't actually disconnect yet even then, because `outlasting_tx`
    // (see its own comment above) still holds one more clone open at that
    // point. `abort`/`warnings` are separate `Arc` clones, unaffected by
    // any of this.
    let run = move || {
        files
            .into_par_iter()
            .for_each(|(path, parent_id)| process_file(&ctx, &path, parent_id));
    };
    {
        // Always a real, scoped `ThreadPool` - even with no `--concurrency`
        // given, rather than just calling `run()` directly (which would use
        // rayon's own *global* pool instead, whose threads are never torn
        // down within this process's lifetime at all - see the original
        // version of this comment in git history for the full story of
        // *that* bug). Omitting `.num_threads()` entirely (rather than
        // querying `rayon::current_num_threads()` to match it) applies
        // rayon's own default sizing logic unchanged, without the side
        // effect of also spinning up the (otherwise entirely unused)
        // global pool just to ask it how big it'd be.
        let mut pool_builder = ThreadPoolBuilder::new();
        if let Some(concurrency) = args.concurrency {
            pool_builder = pool_builder.num_threads(concurrency as usize);
        }
        let pool = pool_builder.build().expect("failed to build thread pool");
        pool.install(run);
        // Explicitly, *synchronously* close every worker's read connection
        // here, rather than trusting the pool's own `Drop` right below to
        // have done it by the time this block ends: `ThreadPool::drop`
        // only *signals* its worker threads to stop (`Registry::terminate`,
        // a latch set plus a wakeup) - it does not join them, so the
        // actual OS thread teardown (and with it, the `READ_CONNECTION`
        // thread-local's own `Drop`) can still be running well after
        // `ThreadPool::drop` has already returned. Confirmed the hard way:
        // under enough concurrent system load, that gap was wide enough
        // for `main_conn` (in the writer thread, woken by `outlasting_tx`
        // dropping below) to close *before* a worker's read connection
        // actually had - reproduced with a real, non-empty leftover `-wal`
        // (tens of KB, not mere noise) despite every other ordering
        // safeguard here already being in place. `ThreadPool::broadcast`
        // runs its closure on every worker thread and, unlike `drop`,
        // genuinely blocks until all of them have finished it - see its
        // own doc comment ("only after all threads have completed").
        pool.broadcast(|_ctx| READ_CONNECTION.with(|cell| *cell.borrow_mut() = None));
    } // `pool` dropped here; every worker's read connection is already
    // provably closed by the `broadcast` above, regardless of how long
    // the pool's own (fire-and-forget) thread teardown still takes.

    // Only now does the channel actually disconnect - see `outlasting_tx`'s
    // own comment for why this ordering is what makes `main_conn` (owned
    // by the writer thread joined right below) the last connection to
    // close, letting SQLite auto-checkpoint on a clean run.
    drop(outlasting_tx);

    writer_handle.join().expect("writer thread panicked");
    let _ = std::fs::remove_dir_all(&spill_dir);

    if abort.load(Ordering::Relaxed) {
        eprintln!("error: backup aborted after a fatal error");
        return ExitCode::FAILURE;
    }
    let warning_count = warnings.load(Ordering::Relaxed);
    if warning_count > 0 {
        println!("backup completed with {warning_count} warning(s)");
    } else {
        println!("backup completed successfully");
    }
    ExitCode::SUCCESS
}

fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn path_mtime_millis(path: &Path) -> i64 {
    std::fs::metadata(path)
        .and_then(|m| m.modified())
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Checks `source` against a same-named candidate reference tree entry:
/// if its size and modified time match, returns `Some((time_millis,
/// content_id))` - everything `--reference` needs to record a hit, with no
/// further source I/O. `None` on any mismatch, *or* if `source`'s metadata
/// can't be read at all (falls through to normal processing, where the
/// worker's own open/read will surface and report that failure - this
/// function only ever short-circuits a match, never turns an unreadable
/// source into a hard error).
fn matching_reference(
    conn: &Connection,
    source: &Path,
    ref_entry: &db::TreeEntryRow,
) -> Result<Option<(i64, Option<i64>)>, db::Error> {
    let Ok(metadata) = std::fs::metadata(source) else {
        return Ok(None);
    };
    let Some(source_time_millis) = metadata
        .modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_millis() as i64)
    else {
        return Ok(None);
    };
    let ref_size = db::file_size(conn, ref_entry)?;
    if metadata.len() as i64 == ref_size && source_time_millis == ref_entry.time_millis {
        Ok(Some((source_time_millis, ref_entry.content_id)))
    } else {
        Ok(None)
    }
}

/// Resolves `target`'s id in the repository tree, creating path components per
/// the flag semantics:
/// - `target_exists`: every component must already exist; missing ones are a
///   hard error (fail before any source is touched).
/// - `create_dirs`: every missing component is created, however many there are.
/// - neither flag: only the *last* component may be missing (`mkdir`, not
///   `mkdir -p`); a missing intermediate component is a hard error.
fn resolve_target(
    conn: &Connection,
    target: &Path,
    create_dirs: bool,
    target_exists: bool,
) -> Result<i64, String> {
    let components: Vec<String> = target
        .components()
        .filter_map(|c| match c {
            std::path::Component::Normal(s) => Some(s.to_string_lossy().into_owned()),
            _ => None,
        })
        .collect();
    if components.is_empty() {
        return Err(format!(
            "target path '{}' has no path components",
            target.display()
        ));
    }

    let mut parent_id = 0i64; // repository root
    let last = components.len() - 1;
    for (i, name) in components.iter().enumerate() {
        match db::find_tree_entry(conn, parent_id, name).map_err(|e| e.to_string())? {
            Some(entry) if entry.kind == db::EntryKind::Dir => parent_id = entry.id,
            Some(_) => {
                return Err(format!(
                    "'{name}' in target path '{}' already exists as a file, not a directory",
                    target.display()
                ));
            }
            None if target_exists => {
                return Err(format!(
                    "target '{}' does not exist (--target-exists given)",
                    target.display()
                ));
            }
            None if create_dirs || i == last => {
                parent_id = db::insert_directory(conn, parent_id, name, now_millis())
                    .map_err(|e| e.to_string())?;
            }
            None => {
                return Err(format!(
                    "target directory component '{name}' does not exist; pass --create-dirs to create missing parent directories"
                ));
            }
        }
    }
    Ok(parent_id)
}

/// Resolves `reference`'s `/`-separated path against the repository tree,
/// one segment at a time from the root: each segment is wildcard-matched
/// (`*`/`?`, via [`backup_ignore::wildcard_match`]) against the current
/// node's directory children, picking the alphabetically last match (already
/// sorted ascending by [`db::list_children`], so the last filtered match is
/// the alphabetically last one) - e.g. `????/????.??.??_*` resolves the year
/// segment to the latest-named year directory, then within it the latest-
/// named run. Errors if a segment has no matching directory, or if the
/// fully-resolved reference directory turns out to have no children at all.
fn resolve_reference(conn: &Connection, reference: &Path) -> Result<i64, String> {
    let mut current_id = 0i64;
    let mut resolved_path = String::from("/");
    for component in reference.components() {
        let std::path::Component::Normal(segment) = component else {
            continue;
        };
        let segment = segment.to_string_lossy();
        let children = db::list_children(conn, current_id).map_err(|e| e.to_string())?;
        let matched = children
            .iter()
            .rfind(|c| {
                c.kind == db::EntryKind::Dir && backup_ignore::wildcard_match(&segment, &c.name)
            })
            .ok_or_else(|| {
                format!("reference directory not found while resolving '{resolved_path}{segment}'")
            })?;
        current_id = matched.id;
        resolved_path.push_str(&matched.name);
        resolved_path.push('/');
    }
    if db::list_children(conn, current_id)
        .map_err(|e| e.to_string())?
        .is_empty()
    {
        return Err(format!("reference directory '{resolved_path}' is empty"));
    }
    println!("reference:          {resolved_path}");
    Ok(current_id)
}

/// Fuzzy plausibility check for a resolved `--reference` directory against
/// the actual sources about to be backed up - a guard against a typo'd or
/// unrelated reference silently "working" (matching almost nothing, so every
/// file falls back to a full read/hash anyway, quietly defeating the whole
/// point of `--reference`) rather than being caught up front. Skipped
/// entirely when `--force-reference` is given.
///
/// Builds two comparable listings and requires them to overlap enough: the
/// reference directory's top-level children (directories bare, files marked
/// with a leading `:` so a same-named file and directory don't collide in
/// the comparison), plus one extra level into any reference subdirectory
/// whose name matches one of `sources`' basenames *as a directory* - and the
/// mirror image built from the real `sources` paths (top-level basenames,
/// plus one level into any source directory whose basename matches one of
/// the reference's top-level directory names).
fn validate_reference(conn: &Connection, sources: &[PathBuf], ref_id: i64) -> Result<(), String> {
    let ref_children = db::list_children(conn, ref_id).map_err(|e| e.to_string())?;
    let ref_dir_names: std::collections::HashSet<&str> = ref_children
        .iter()
        .filter(|c| c.kind == db::EntryKind::Dir)
        .map(|c| c.name.as_str())
        .collect();
    let source_dir_names: std::collections::HashSet<String> = sources
        .iter()
        .filter(|s| s.is_dir())
        .map(|s| source_basename(s))
        .collect();

    let mut reference_listing: Vec<String> = Vec::new();
    for child in &ref_children {
        match child.kind {
            db::EntryKind::File => reference_listing.push(format!(":{}", child.name)),
            db::EntryKind::Dir => {
                reference_listing.push(child.name.clone());
                if source_dir_names.contains(&child.name) {
                    for grandchild in
                        db::list_children(conn, child.id).map_err(|e| e.to_string())?
                    {
                        let listed = match grandchild.kind {
                            db::EntryKind::File => format!(":{}", grandchild.name),
                            db::EntryKind::Dir => grandchild.name,
                        };
                        reference_listing.push(format!("{}/{listed}", child.name));
                    }
                }
            }
        }
    }

    let mut source_listing: Vec<String> = Vec::new();
    for source in sources {
        let name = source_basename(source);
        if !source.is_dir() {
            source_listing.push(format!(":{name}"));
            continue;
        }
        source_listing.push(name.clone());
        if ref_dir_names.contains(name.as_str()) {
            let entries = std::fs::read_dir(source).map_err(|e| e.to_string())?;
            for entry in entries {
                let entry = entry.map_err(|e| e.to_string())?;
                let child_name = entry.file_name().to_string_lossy().into_owned();
                let is_dir = entry.file_type().map_err(|e| e.to_string())?.is_dir();
                let listed = if is_dir {
                    child_name
                } else {
                    format!(":{child_name}")
                };
                source_listing.push(format!("{name}/{listed}"));
            }
        }
    }

    let reference_set: std::collections::HashSet<&String> = reference_listing.iter().collect();
    let source_set: std::collections::HashSet<&String> = source_listing.iter().collect();
    let intersect_count = source_set.intersection(&reference_set).count();
    let max_len = reference_listing.len().max(source_listing.len());
    if max_len as f64 > intersect_count as f64 * 1.6 + 1.0 {
        return Err(format!(
            "not enough matches ({intersect_count}) between source ({} entries) and reference ({} entries) - pass --force-reference to skip this check",
            source_listing.len(),
            reference_listing.len()
        ));
    }
    println!(
        "reference validation OK, {intersect_count} matches between source ({} entries) and reference ({} entries)",
        source_listing.len(),
        reference_listing.len()
    );
    Ok(())
}

fn source_basename(source: &Path) -> String {
    source
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default()
}

/// Checks that `source` itself (just this one node, not anything beneath it)
/// exists and is actually readable, before any repository/target work
/// starts. `walk_and_create_dirs` below already logs-and-skips access errors
/// it hits *during* the tree walk (an unreadable subdirectory shouldn't abort
/// the whole backup), but that per-entry recovery means a source that's
/// completely inaccessible from the start (typo'd path, missing permission)
/// would otherwise only ever surface as a buried warning while the command
/// still exits successfully - this catches that case up front instead, the
/// same way [`resolve_target`] validates the target before any work starts.
fn check_source_readable(source: &Path) -> Result<(), String> {
    let metadata = std::fs::metadata(source)
        .map_err(|err| format!("cannot access source '{}': {err}", source.display()))?;
    if metadata.is_dir() {
        std::fs::read_dir(source)
            .map_err(|err| format!("cannot read source directory '{}': {err}", source.display()))?;
    } else if metadata.is_file() {
        File::open(source)
            .map_err(|err| format!("cannot read source file '{}': {err}", source.display()))?;
    } else {
        return Err(format!(
            "source '{}' is neither a regular file nor a directory",
            source.display()
        ));
    }
    Ok(())
}

/// Walks `source` (a file or a directory) in a single pass, creating the
/// `tree_entries` row for each directory as it's encountered (so children always
/// have an already-resolved parent, since `WalkDir` yields parents before their
/// children) and collecting `(path, parent_id)` for each regular file found.
/// `source` itself keeps its own name as a child of `target_id` - e.g. backing up
/// `a/b` into target `t` produces `t/b`, mirroring the source's own basename
/// rather than merging its contents directly into `t`.
///
/// Errors accessing individual entries, and name conflicts (a file already
/// exists where a directory is needed), are logged and skipped - the affected
/// subtree is silently omitted, but the run continues. This deliberately avoids
/// the failure mode found in the tool this replaces, where an unreadable
/// subdirectory crashes the entire backup.
///
/// Entries excluded by a `.backupignore` (see `docs/plans/backupignore.md`) are
/// skipped the same way - silently, no warning - via a second map,
/// `ignore_scopes`, threaded alongside `dir_ids` the same way: keyed by
/// directory path, holding the rules inherited by (and to be propagated
/// further from) that directory's children.
///
/// A third map, `ref_ids`, threaded the same way again, mirrors `dir_ids` but
/// walks the `ref_root_id` reference tree (see
/// `docs/plans/implemented/backup-reference.md`) in parallel by name, not by
/// path - seeded at depth 0 from `ref_root_id` itself instead of a parent
/// lookup, same as `dir_ids` is seeded from `target_id`. A file whose
/// same-named reference-tree counterpart matches on size and modified time
/// is recorded into `reference_hits` (its content reused as-is, no chunking)
/// instead of `files`.
fn walk_and_create_dirs(
    conn: &Connection,
    source: &Path,
    target_id: i64,
    ref_root_id: Option<i64>,
    files: &mut Vec<(PathBuf, i64)>,
    reference_hits: &mut Vec<db::FileBackupRecord>,
    warnings: &mut u64,
) -> Result<(), String> {
    let mut dir_ids: HashMap<PathBuf, i64> = HashMap::new();
    let mut ignore_scopes: HashMap<PathBuf, Vec<IgnoreRule>> = HashMap::new();
    let mut ref_ids: HashMap<PathBuf, i64> = HashMap::new();

    let mut walker = WalkDir::new(source).into_iter();
    while let Some(entry) = walker.next() {
        let entry = match entry {
            Ok(entry) => entry,
            Err(err) => {
                eprintln!("warning: failed to access entry: {err}");
                *warnings += 1;
                continue;
            }
        };

        let parent_id = if entry.depth() == 0 {
            Some(target_id)
        } else {
            entry.path().parent().and_then(|p| dir_ids.get(p).copied())
        };
        // `None` means the parent directory itself was skipped above (name
        // conflict or access error); silently skip this entry too, without
        // repeating a warning for every descendant of an already-reported issue.
        let Some(parent_id) = parent_id else {
            continue;
        };

        // Cloned (not borrowed) so `ignore_scopes` can be mutated further down
        // in the same iteration (inserting this directory's own child scope)
        // without a borrow conflict; rule lists are small (a handful of lines
        // per `.backupignore`), so this is cheap.
        let inherited: Vec<IgnoreRule> = entry
            .path()
            .parent()
            .and_then(|p| ignore_scopes.get(p))
            .cloned()
            .unwrap_or_default();
        let inherited_ref_id = if entry.depth() == 0 {
            ref_root_id
        } else {
            entry.path().parent().and_then(|p| ref_ids.get(p).copied())
        };
        let name = entry.file_name().to_string_lossy().into_owned();

        if entry.file_type().is_dir() {
            if backup_ignore::matches_dir_skip(&inherited, &name) {
                walker.skip_current_dir();
                continue;
            }
            let own_rules = match backup_ignore::read_own_ignore_file(entry.path()) {
                OwnIgnoreFile::Empty => {
                    walker.skip_current_dir();
                    continue;
                }
                OwnIgnoreFile::Absent => Vec::new(),
                OwnIgnoreFile::Rules(rules) => rules,
            };

            match db::insert_directory(conn, parent_id, &name, path_mtime_millis(entry.path())) {
                Ok(id) => {
                    dir_ids.insert(entry.path().to_path_buf(), id);
                    ignore_scopes.insert(
                        entry.path().to_path_buf(),
                        backup_ignore::child_scope(&inherited, &own_rules, &name),
                    );
                    if let Some(ref_parent) = inherited_ref_id {
                        let ref_entry = db::find_tree_entry(conn, ref_parent, &name)
                            .map_err(|e| e.to_string())?;
                        if let Some(ref_entry) = ref_entry
                            && ref_entry.kind == db::EntryKind::Dir
                        {
                            ref_ids.insert(entry.path().to_path_buf(), ref_entry.id);
                        }
                    }
                }
                Err(db::Error::NotADirectory { .. }) => {
                    eprintln!(
                        "warning: '{}' already exists as a file, skipping directory",
                        entry.path().display()
                    );
                    *warnings += 1;
                }
                Err(err) => {
                    return Err(format!(
                        "failed to create directory '{}': {err}",
                        entry.path().display()
                    ));
                }
            }
        } else if entry.file_type().is_file() {
            if backup_ignore::matches_file_skip(&inherited, &name) {
                continue;
            }
            let reference_hit = match inherited_ref_id {
                Some(ref_parent) => {
                    match db::find_tree_entry(conn, ref_parent, &name).map_err(|e| e.to_string())? {
                        Some(ref_entry) if ref_entry.kind == db::EntryKind::File => {
                            matching_reference(conn, entry.path(), &ref_entry)
                                .map_err(|e| e.to_string())?
                        }
                        _ => None,
                    }
                }
                None => None,
            };
            match reference_hit {
                Some((time_millis, content_id)) => reference_hits.push(db::FileBackupRecord {
                    parent_id,
                    name,
                    time_millis,
                    content: db::ContentSource::Known(content_id),
                }),
                None => files.push((entry.path().to_path_buf(), parent_id)),
            }
        }
        // Symlinks and other special files are silently skipped, matching the
        // pre-existing behavior of the walk this replaces.
    }

    Ok(())
}

/// Distinguishes the two failure classes a worker can hit: a source-read problem
/// is isolated to this one file (log, skip, continue), while a store or database
/// problem is systemic (abort the whole run) - see the module-level rationale in
/// the plan this implements.
enum WorkerError {
    SourceRead(io::Error),
    Fatal(String),
}

fn process_file(ctx: &RunContext, path: &Path, parent_id: i64) {
    if ctx.abort.load(Ordering::Relaxed) {
        return;
    }

    let file = match File::open(path) {
        Ok(f) => f,
        Err(err) => return warn(ctx, &format!("failed to open {}: {err}", path.display())),
    };
    let time_millis = match file.metadata().and_then(|m| m.modified()) {
        Ok(t) => t
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0),
        Err(err) => {
            return warn(
                ctx,
                &format!("failed to read mtime of {}: {err}", path.display()),
            );
        }
    };
    let Some(name) = path.file_name().map(|n| n.to_string_lossy().into_owned()) else {
        return warn(ctx, &format!("path has no file name: {}", path.display()));
    };

    match read_and_chunk(ctx, file) {
        Ok((chunks, content_hash)) => {
            let record = db::FileBackupRecord {
                parent_id,
                name,
                time_millis,
                content: db::ContentSource::Resolved {
                    chunks,
                    content_hash: content_hash.to_vec(),
                },
            };
            let send_result = ctx
                .sender
                .lock()
                .expect("sender mutex poisoned")
                .send(record);
            if send_result.is_err() {
                // The writer thread already exited (its own fatal error already
                // set `abort` and logged); nothing more to report here.
                ctx.abort.store(true, Ordering::Relaxed);
            }
        }
        Err(WorkerError::SourceRead(err)) => {
            warn(ctx, &format!("failed to read {}: {err}", path.display()));
        }
        Err(WorkerError::Fatal(msg)) => {
            eprintln!("error: {msg}");
            ctx.abort.store(true, Ordering::Relaxed);
        }
    }
}

fn warn(ctx: &RunContext, msg: &str) {
    eprintln!("warning: {msg}");
    ctx.warnings.fetch_add(1, Ordering::Relaxed);
}

/// Reads and chunks `file`, resolving each chunk against the dedup index and
/// writing new chunks' bytes to the store as they're found, and returns the
/// resolved chunk list together with the hash over the ordered chunk sequence
/// (see `contents.hash` in `db/src/migrations.rs`). Chunk bytes are buffered
/// via `SpillingHashingChunker` (RAM-budgeted, spills to disk - see
/// `ctx.chunk_buffer_budget`/`spill_path`), not a plain `Vec<u8>`: without
/// that, a single large CDC chunk, or the entire file under `chunking:
/// none`, would need to be fully RAM-resident at once (see
/// `docs/plans/implemented/bounded-memory-io-pipeline.md`).
fn read_and_chunk(
    ctx: &RunContext,
    mut file: File,
) -> Result<(Vec<db::ChunkRef>, [u8; HASH_LENGTH]), WorkerError> {
    let mut chunker = SpillingHashingChunker::new(
        Blake3Hasher(blake3::Hasher::new()),
        ctx.chunker_config.chunker(),
        Arc::clone(&ctx.chunk_buffer_budget),
        || ctx.spill_path(),
    );
    let mut content_hasher = blake3::Hasher::new();
    let mut chunk_refs = Vec::new();
    let mut buf = [0u8; READ_BUFFER_SIZE];

    loop {
        let n = file.read(&mut buf).map_err(WorkerError::SourceRead)?;
        if n == 0 {
            break;
        }
        let chunks = chunker
            .next(&buf[..n])
            .map_err(|err| WorkerError::Fatal(format!("chunk buffering failed: {err}")))?;
        for chunk in chunks {
            resolve_chunk(ctx, chunk, &mut chunk_refs, &mut content_hasher)?;
        }
    }
    let flushed = chunker
        .flush()
        .map_err(|err| WorkerError::Fatal(format!("chunk buffering failed: {err}")))?;
    if let Some(chunk) = flushed {
        resolve_chunk(ctx, chunk, &mut chunk_refs, &mut content_hasher)?;
    }

    let mut content_hash = [0u8; HASH_LENGTH];
    content_hasher.finalize_xof().fill(&mut content_hash);
    Ok((chunk_refs, content_hash))
}

/// Resolves one completed chunk against the dedup index: reuses an existing
/// chunk id on a hit, or reserves store space and writes the chunk's bytes on a
/// miss. Also feeds the chunk's length and hash into `content_hasher`.
fn resolve_chunk(
    ctx: &RunContext,
    chunk: SpilledChunk,
    chunk_refs: &mut Vec<db::ChunkRef>,
    content_hasher: &mut blake3::Hasher,
) -> Result<(), WorkerError> {
    let length_hash = chunk.length_hash;
    content_hasher.update(&length_hash.length.to_le_bytes());
    content_hasher.update(&length_hash.hash);

    let existing = with_read_connection(ctx, |conn| {
        db::find_chunk(conn, length_hash.length, &length_hash.hash)
    })
    .map_err(|err| WorkerError::Fatal(format!("dedup lookup failed: {err}")))?
    .map_err(|err| WorkerError::Fatal(format!("dedup lookup failed: {err}")))?;

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
                &ctx.data_store,
                &ctx.allocator,
                &mut bytes,
                length_hash.length,
                ctx.io_limiter.as_ref(),
            )
            .map_err(|err| WorkerError::Fatal(format!("store write failed: {err}")))?;
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

fn run_writer(
    mut conn: Connection,
    rx: mpsc::Receiver<db::FileBackupRecord>,
    abort: Arc<AtomicBool>,
) {
    let mut batch = Vec::with_capacity(WRITE_BATCH_SIZE);
    loop {
        match rx.recv_timeout(WRITE_BATCH_IDLE_TIMEOUT) {
            Ok(record) => {
                batch.push(record);
                if batch.len() >= WRITE_BATCH_SIZE && !flush(&mut conn, &mut batch, &abort) {
                    return;
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                if !batch.is_empty() && !flush(&mut conn, &mut batch, &abort) {
                    return;
                }
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }
    if !batch.is_empty() {
        flush(&mut conn, &mut batch, &abort);
    }
}

/// Applies `batch` and clears it. Returns `false` (having already logged the
/// error and set `abort`) if the batch failed to apply, so the caller can stop
/// the writer thread.
fn flush(conn: &mut Connection, batch: &mut Vec<db::FileBackupRecord>, abort: &AtomicBool) -> bool {
    match db::apply_backup_batch(conn, batch) {
        Ok(()) => {
            batch.clear();
            true
        }
        Err(err) => {
            eprintln!("error: failed to write backup batch: {err}");
            abort.store(true, Ordering::Relaxed);
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

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

    fn backup_args(mut paths: Vec<PathBuf>) -> BackupArgs {
        BackupArgs {
            create_dirs: true,
            target_exists: false,
            concurrency: Some(2),
            store_io_parallelism: None,
            chunk_buffer_mb: DEFAULT_CHUNK_BUFFER_MB,
            allow_swap_risk: false,
            temp: None,
            reference: None,
            force_reference: false,
            lock_wait_secs: 0,
            paths: std::mem::take(&mut paths),
        }
    }

    fn conn(repo_root: &Path) -> Connection {
        Connection::open(repo_root.join("meta").join("repository.sqlite3")).unwrap()
    }

    fn count(c: &Connection, table: &str) -> i64 {
        c.query_row(&format!("SELECT COUNT(*) FROM {table}"), (), |row| {
            row.get(0)
        })
        .unwrap()
    }

    #[test]
    fn run_store_fails_fast_for_a_missing_source_without_touching_the_target() {
        let (_temp_dir, repo_root) = init_repo();
        let source_dir = tempfile::tempdir().unwrap();
        let missing_source = source_dir.path().join("does-not-exist");

        let exit = run_store(
            &repo_root,
            backup_args(vec![missing_source, PathBuf::from("target")]),
        );

        assert_eq!(exit, ExitCode::FAILURE);
        let c = conn(&repo_root);
        assert_eq!(
            count(&c, "tree_entries"),
            1,
            "only the root entry - the target must never have been created"
        );
    }

    #[test]
    fn run_store_refuses_when_the_lock_is_already_held() {
        let (_temp_dir, repo_root) = init_repo();
        let _lock = RepoLock::acquire(&db::meta_dir(&repo_root), Duration::ZERO)
            .unwrap()
            .unwrap();
        let source_dir = tempfile::tempdir().unwrap();
        std::fs::write(source_dir.path().join("a.txt"), b"hello").unwrap();

        let exit = run_store(
            &repo_root,
            backup_args(vec![
                source_dir.path().to_path_buf(),
                PathBuf::from("target"),
            ]),
        );

        assert_eq!(exit, ExitCode::FAILURE);
    }

    /// `--temp` (see `BackupArgs::temp`) is validated up front, before any
    /// other command work - like `run_store_fails_fast_for_a_missing_source_
    /// without_touching_the_target` above, checks this by confirming the
    /// target was never created, not just that the exit code is a failure.
    #[test]
    fn run_store_fails_fast_for_a_nonexistent_temp_dir_without_touching_the_target() {
        let (_temp_dir, repo_root) = init_repo();
        let source_dir = tempfile::tempdir().unwrap();
        std::fs::write(source_dir.path().join("a.txt"), b"hello").unwrap();
        let missing_temp = source_dir.path().join("does-not-exist-temp");

        let mut args = backup_args(vec![
            source_dir.path().to_path_buf(),
            PathBuf::from("target"),
        ]);
        args.temp = Some(missing_temp);

        let exit = run_store(&repo_root, args);

        assert_eq!(exit, ExitCode::FAILURE);
        let c = conn(&repo_root);
        assert_eq!(
            count(&c, "tree_entries"),
            1,
            "only the root entry - the target must never have been created"
        );
    }

    #[cfg(unix)]
    #[test]
    fn run_store_fails_fast_for_an_unwritable_temp_dir_without_touching_the_target() {
        let (_temp_dir, repo_root) = init_repo();
        let source_dir = tempfile::tempdir().unwrap();
        std::fs::write(source_dir.path().join("a.txt"), b"hello").unwrap();
        let temp_parent = tempfile::tempdir().unwrap();
        let locked_temp = temp_parent.path().join("locked");
        std::fs::create_dir(&locked_temp).unwrap();
        std::fs::set_permissions(&locked_temp, std::fs::Permissions::from_mode(0o000)).unwrap();

        let mut args = backup_args(vec![
            source_dir.path().to_path_buf(),
            PathBuf::from("target"),
        ]);
        args.temp = Some(locked_temp.clone());

        let exit = run_store(&repo_root, args);

        std::fs::set_permissions(&locked_temp, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert_eq!(exit, ExitCode::FAILURE);
        let c = conn(&repo_root);
        assert_eq!(
            count(&c, "tree_entries"),
            1,
            "only the root entry - the target must never have been created"
        );
    }

    /// Confirms `--temp` actually redirects chunk-buffer spillover: with a
    /// custom `--temp` dir given, the spill directory `run_store` creates
    /// (see `create_spill_dir`) must appear under it - never under the OS
    /// default (`std::env::temp_dir()`), since `create_spill_dir` only
    /// ever calls `tempdir_in` when a custom dir is given, not `tempdir`.
    /// `chunk_buffer_mb: 0` (and `chunking: none`, so the whole file is
    /// one chunk) forces the very first byte written to spill to disk,
    /// so this also confirms a real spill *file* lands under the custom
    /// dir, not just the empty container directory.
    #[test]
    fn run_store_uses_a_custom_temp_dir_for_chunk_buffer_spillover() {
        use std::time::Instant;

        let temp_dir = tempfile::tempdir().unwrap();
        let repo_root = temp_dir.path().join("repo");
        db::init_repository(
            &repo_root,
            &db::RepositorySettings::new(12, db::Chunking::None).unwrap(),
        )
        .unwrap();
        let source_dir = tempfile::tempdir().unwrap();
        // Large enough that chunking/hashing/writing isn't instantaneous,
        // giving the polling loop below a real window to observe the spill
        // directory before `run_store` removes it at the end of the run.
        let content: Vec<u8> = (0u32..2_000_000).map(|i| (i % 251) as u8).collect();
        std::fs::write(source_dir.path().join("a.txt"), &content).unwrap();

        let custom_temp = tempfile::tempdir().unwrap();
        let mut args = backup_args(vec![
            source_dir.path().to_path_buf(),
            PathBuf::from("target"),
        ]);
        args.chunk_buffer_mb = 0;
        args.concurrency = Some(1);
        args.temp = Some(custom_temp.path().to_path_buf());

        let handle = thread::spawn(move || run_store(&repo_root, args));

        let deadline = Instant::now() + Duration::from_secs(10);
        let mut spill_dir_path = None;
        while Instant::now() < deadline {
            if let Ok(entries) = std::fs::read_dir(custom_temp.path()) {
                for entry in entries.flatten() {
                    if entry
                        .file_name()
                        .to_string_lossy()
                        .starts_with("backup-store-chunk-buffer-")
                    {
                        spill_dir_path = Some(entry.path());
                        break;
                    }
                }
            }
            if spill_dir_path.is_some() {
                break;
            }
            thread::sleep(Duration::from_millis(5));
        }
        let spill_dir_path = spill_dir_path
            .expect("expected a backup-store-chunk-buffer-* spill dir under the custom --temp dir");
        assert!(spill_dir_path.starts_with(custom_temp.path()));

        let exit = handle.join().expect("run_store thread panicked");
        assert_eq!(exit, ExitCode::SUCCESS);
        assert!(
            !spill_dir_path.exists(),
            "spill dir should be removed once run_store finishes"
        );
    }

    #[cfg(unix)]
    #[test]
    fn run_store_fails_fast_for_an_unreadable_source_directory() {
        let (_temp_dir, repo_root) = init_repo();
        let source_dir = tempfile::tempdir().unwrap();
        let unreadable = source_dir.path().join("locked");
        std::fs::create_dir(&unreadable).unwrap();
        std::fs::set_permissions(&unreadable, std::fs::Permissions::from_mode(0o000)).unwrap();

        let exit = run_store(
            &repo_root,
            backup_args(vec![unreadable.clone(), PathBuf::from("target")]),
        );

        std::fs::set_permissions(&unreadable, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert_eq!(exit, ExitCode::FAILURE);
        let c = conn(&repo_root);
        assert_eq!(count(&c, "tree_entries"), 1, "only the root entry");
    }

    #[test]
    fn backs_up_files_creates_tree_and_dedupes_identical_content() {
        let (_temp_dir, repo_root) = init_repo();
        let source_dir = tempfile::tempdir().unwrap();
        std::fs::write(source_dir.path().join("a.txt"), b"hello world").unwrap();
        std::fs::write(source_dir.path().join("b.txt"), b"hello world").unwrap();
        std::fs::create_dir(source_dir.path().join("sub")).unwrap();
        std::fs::write(source_dir.path().join("sub").join("c.txt"), b"different").unwrap();

        let exit = run_store(
            &repo_root,
            backup_args(vec![
                source_dir.path().to_path_buf(),
                PathBuf::from("target"),
            ]),
        );
        assert_eq!(exit, ExitCode::SUCCESS);

        let c = conn(&repo_root);
        // root -> target -> <source basename> -> {a.txt, b.txt, sub/c.txt}
        let source_name = source_dir.path().file_name().unwrap().to_str().unwrap();
        let target_id: i64 = c
            .query_row(
                "SELECT id FROM tree_entries WHERE parent_id = 0 AND name = 'target'",
                (),
                |row| row.get(0),
            )
            .unwrap();
        let source_id: i64 = c
            .query_row(
                "SELECT id FROM tree_entries WHERE parent_id = ?1 AND name = ?2",
                rusqlite::params![target_id, source_name],
                |row| row.get(0),
            )
            .unwrap();
        let (a_content, b_content): (i64, i64) = (
            c.query_row(
                "SELECT content_id FROM tree_entries WHERE parent_id = ?1 AND name = 'a.txt'",
                [source_id],
                |row| row.get(0),
            )
            .unwrap(),
            c.query_row(
                "SELECT content_id FROM tree_entries WHERE parent_id = ?1 AND name = 'b.txt'",
                [source_id],
                |row| row.get(0),
            )
            .unwrap(),
        );
        assert_eq!(
            a_content, b_content,
            "identical content must share one contents row"
        );
        assert_eq!(
            count(&c, "contents"),
            3,
            "two distinct contents: 'hello world' and 'different', plus the \
             always-seeded EMPTY_CONTENT_ID row"
        );
        assert_eq!(
            count(&c, "chunks"),
            2,
            "small files below the chunk size: one chunk each"
        );

        // The stored bytes must be readable back.
        let data_store = store::LongTermStore::new(repo_root.join("data"), true);
        let (start, stop): (i64, i64) = c
            .query_row(
                "SELECT ce.start, ce.stop FROM chunk_extents ce
                 JOIN content_chunks cc ON cc.chunk_id = ce.chunk_id
                 WHERE cc.content_id = ?1",
                [a_content],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        let mut buf = vec![0u8; (stop - start) as usize];
        let integrity = data_store.read(start as u64, &mut buf).unwrap();
        assert_eq!(integrity, store::ReadIntegrity::Complete);
        assert_eq!(buf, b"hello world");
    }

    // A previous version of this test module had in-process sanity checks
    // here (does a clean `store` run leave the repository immediately
    // readable, no pending `-wal`) - both a permanent one
    // (`assert_store_leaves_no_pending_wal_behind`) and later a temporary
    // diagnostic one (`diag_store_with_default_concurrency_leaves_no_pending_wal_behind`,
    // kept around just long enough to chase a second, deeper bug under
    // heavy stress - see `docs/plans/implemented/read-only-repository-access.md`'s
    // "Second correction" for what it found: `ThreadPool::drop` doesn't
    // join its worker threads, only `ThreadPool::broadcast` does). Both
    // removed for the same reason: the race being observed doesn't
    // reproduce reliably when `run_store` is called in-process the way unit
    // tests do, only via a real, separately-spawned `backup` process, so an
    // in-process assertion here is either flaky (occasional real failures,
    // worse than no assertion at all in a suite every commit is gated on)
    // or, once the fix genuinely holds, redundant with the real regression
    // coverage - see `cli/tests/store_checkpoint.rs` (confirmed: 45/45
    // passes across 15 repeated full-workspace stress runs) for that.

    /// `Some(id)` if `parent_id` has a child named `name`, `None` otherwise -
    /// used by the `.backupignore` tests below to assert an entry's absence,
    /// not just its presence.
    fn find_child(c: &Connection, parent_id: i64, name: &str) -> Option<i64> {
        match c.query_row(
            "SELECT id FROM tree_entries WHERE parent_id = ?1 AND name = ?2",
            rusqlite::params![parent_id, name],
            |row| row.get(0),
        ) {
            Ok(id) => Some(id),
            Err(rusqlite::Error::QueryReturnedNoRows) => None,
            Err(err) => panic!("query failed: {err}"),
        }
    }

    #[test]
    fn backupignore_empty_file_skips_the_whole_directory() {
        let (_temp_dir, repo_root) = init_repo();
        let source_dir = tempfile::tempdir().unwrap();
        std::fs::write(source_dir.path().join("kept.txt"), b"kept").unwrap();
        std::fs::create_dir(source_dir.path().join("skip")).unwrap();
        std::fs::write(source_dir.path().join("skip").join(".backupignore"), b"").unwrap();
        std::fs::write(source_dir.path().join("skip").join("inside.txt"), b"x").unwrap();

        let exit = run_store(
            &repo_root,
            backup_args(vec![
                source_dir.path().to_path_buf(),
                PathBuf::from("target"),
            ]),
        );
        assert_eq!(exit, ExitCode::SUCCESS);

        let c = conn(&repo_root);
        let source_name = source_dir.path().file_name().unwrap().to_str().unwrap();
        let target_id = find_child(&c, 0, "target").unwrap();
        let source_id = find_child(&c, target_id, source_name).unwrap();

        assert!(find_child(&c, source_id, "kept.txt").is_some());
        assert!(
            find_child(&c, source_id, "skip").is_none(),
            "a directory with an empty .backupignore must not be created at all"
        );
    }

    #[test]
    fn backupignore_excludes_a_matching_file_and_a_matching_directory() {
        let (_temp_dir, repo_root) = init_repo();
        let source_dir = tempfile::tempdir().unwrap();
        std::fs::write(
            source_dir.path().join(".backupignore"),
            b"secret.txt\ntemp/\n",
        )
        .unwrap();
        std::fs::write(source_dir.path().join("secret.txt"), b"x").unwrap();
        std::fs::write(source_dir.path().join("kept.txt"), b"kept").unwrap();
        std::fs::create_dir(source_dir.path().join("temp")).unwrap();
        std::fs::write(source_dir.path().join("temp").join("inside.txt"), b"x").unwrap();

        let exit = run_store(
            &repo_root,
            backup_args(vec![
                source_dir.path().to_path_buf(),
                PathBuf::from("target"),
            ]),
        );
        assert_eq!(exit, ExitCode::SUCCESS);

        let c = conn(&repo_root);
        let source_name = source_dir.path().file_name().unwrap().to_str().unwrap();
        let target_id = find_child(&c, 0, "target").unwrap();
        let source_id = find_child(&c, target_id, source_name).unwrap();

        assert!(find_child(&c, source_id, "kept.txt").is_some());
        assert!(find_child(&c, source_id, "secret.txt").is_none());
        assert!(find_child(&c, source_id, "temp").is_none());
    }

    /// The scenario that demonstrates the fix of a bug present in the Scala
    /// tool this ports (see `docs/plans/backupignore.md`): a multi-segment
    /// rule like `log*/*.log` must only filter matching files inside a
    /// matching directory, not skip the whole directory outright.
    #[test]
    fn backupignore_multi_segment_rule_only_excludes_matching_files_not_the_whole_directory() {
        let (_temp_dir, repo_root) = init_repo();
        let source_dir = tempfile::tempdir().unwrap();
        std::fs::write(source_dir.path().join(".backupignore"), b"log*/*.log\n").unwrap();
        std::fs::create_dir(source_dir.path().join("logs")).unwrap();
        std::fs::write(source_dir.path().join("logs").join("app.log"), b"x").unwrap();
        std::fs::write(source_dir.path().join("logs").join("keep.txt"), b"kept").unwrap();

        let exit = run_store(
            &repo_root,
            backup_args(vec![
                source_dir.path().to_path_buf(),
                PathBuf::from("target"),
            ]),
        );
        assert_eq!(exit, ExitCode::SUCCESS);

        let c = conn(&repo_root);
        let source_name = source_dir.path().file_name().unwrap().to_str().unwrap();
        let target_id = find_child(&c, 0, "target").unwrap();
        let source_id = find_child(&c, target_id, source_name).unwrap();
        let logs_id = find_child(&c, source_id, "logs")
            .expect("the 'logs' directory itself must still be backed up");

        assert!(find_child(&c, logs_id, "keep.txt").is_some());
        assert!(find_child(&c, logs_id, "app.log").is_none());
    }

    #[test]
    fn backupignore_can_exclude_itself() {
        let (_temp_dir, repo_root) = init_repo();
        let source_dir = tempfile::tempdir().unwrap();
        std::fs::write(
            source_dir.path().join(".backupignore"),
            b"# Do not store the .backupignore itself in the backup.\n.backupignore\n",
        )
        .unwrap();
        std::fs::write(source_dir.path().join("kept.txt"), b"kept").unwrap();

        let exit = run_store(
            &repo_root,
            backup_args(vec![
                source_dir.path().to_path_buf(),
                PathBuf::from("target"),
            ]),
        );
        assert_eq!(exit, ExitCode::SUCCESS);

        let c = conn(&repo_root);
        let source_name = source_dir.path().file_name().unwrap().to_str().unwrap();
        let target_id = find_child(&c, 0, "target").unwrap();
        let source_id = find_child(&c, target_id, source_name).unwrap();

        assert!(find_child(&c, source_id, "kept.txt").is_some());
        assert!(find_child(&c, source_id, ".backupignore").is_none());
    }

    fn content_id_of(c: &Connection, id: i64) -> Option<i64> {
        c.query_row(
            "SELECT content_id FROM tree_entries WHERE id = ?1",
            [id],
            |row| row.get(0),
        )
        .unwrap()
    }

    /// The reference path must be taken (not just coincidental chunk-level
    /// dedup): the source file's bytes are changed to different content of
    /// the *same length*, but its modified time is reset to the original
    /// value, so a real re-read/re-hash would produce a different
    /// content_id - only reusing the reference's content_id without ever
    /// reading the new bytes reproduces the *old* one.
    #[test]
    fn reference_hit_reuses_the_reference_content_id_without_rereading_the_source() {
        let (_temp_dir, repo_root) = init_repo();
        let source_dir = tempfile::tempdir().unwrap();
        let file_path = source_dir.path().join("a.txt");
        std::fs::write(&file_path, b"hello world").unwrap();
        let original_mtime = std::fs::metadata(&file_path).unwrap().modified().unwrap();

        let exit = run_store(
            &repo_root,
            backup_args(vec![source_dir.path().to_path_buf(), PathBuf::from("run1")]),
        );
        assert_eq!(exit, ExitCode::SUCCESS);

        let source_name = source_dir.path().file_name().unwrap().to_str().unwrap();
        let original_content_id = {
            let c = conn(&repo_root);
            let run1_id = find_child(&c, 0, "run1").unwrap();
            let src_id = find_child(&c, run1_id, source_name).unwrap();
            let a_id = find_child(&c, src_id, "a.txt").unwrap();
            content_id_of(&c, a_id)
        };

        // Same length (11 bytes), different bytes - a real re-hash would not
        // match the original content_id.
        std::fs::write(&file_path, b"HELLO WORLD").unwrap();
        std::fs::OpenOptions::new()
            .write(true)
            .open(&file_path)
            .unwrap()
            .set_modified(original_mtime)
            .unwrap();

        let mut args = backup_args(vec![source_dir.path().to_path_buf(), PathBuf::from("run2")]);
        args.reference = Some(PathBuf::from("run1"));
        args.force_reference = true;
        let exit = run_store(&repo_root, args);
        assert_eq!(exit, ExitCode::SUCCESS);

        let c = conn(&repo_root);
        let run2_id = find_child(&c, 0, "run2").unwrap();
        let src_id = find_child(&c, run2_id, source_name).unwrap();
        let a_id = find_child(&c, src_id, "a.txt").unwrap();
        assert_eq!(
            content_id_of(&c, a_id),
            original_content_id,
            "a reference hit must reuse the old content_id even though the \
             bytes on disk changed - proves the source was never re-read"
        );
    }

    #[test]
    fn reference_mismatch_falls_back_to_normal_processing() {
        let (_temp_dir, repo_root) = init_repo();
        let source_dir = tempfile::tempdir().unwrap();
        let file_path = source_dir.path().join("a.txt");
        std::fs::write(&file_path, b"hello world").unwrap();

        let exit = run_store(
            &repo_root,
            backup_args(vec![source_dir.path().to_path_buf(), PathBuf::from("run1")]),
        );
        assert_eq!(exit, ExitCode::SUCCESS);

        let source_name = source_dir.path().file_name().unwrap().to_str().unwrap();
        let original_content_id = {
            let c = conn(&repo_root);
            let run1_id = find_child(&c, 0, "run1").unwrap();
            let src_id = find_child(&c, run1_id, source_name).unwrap();
            let a_id = find_child(&c, src_id, "a.txt").unwrap();
            content_id_of(&c, a_id)
        };

        // Different content, mtime left alone (naturally advances) - a
        // straightforward change, not a spoofed match.
        std::fs::write(&file_path, b"a completely different, longer body").unwrap();

        let mut args = backup_args(vec![source_dir.path().to_path_buf(), PathBuf::from("run2")]);
        args.reference = Some(PathBuf::from("run1"));
        args.force_reference = true;
        let exit = run_store(&repo_root, args);
        assert_eq!(exit, ExitCode::SUCCESS);

        let c = conn(&repo_root);
        let run2_id = find_child(&c, 0, "run2").unwrap();
        let src_id = find_child(&c, run2_id, source_name).unwrap();
        let a_id = find_child(&c, src_id, "a.txt").unwrap();
        assert_ne!(
            content_id_of(&c, a_id),
            original_content_id,
            "size/mtime mismatch must fall back to actually reading the new content"
        );
    }

    #[test]
    fn run_store_fails_fast_for_an_unresolvable_reference_without_touching_the_target() {
        let (_temp_dir, repo_root) = init_repo();
        let source_dir = tempfile::tempdir().unwrap();
        std::fs::write(source_dir.path().join("a.txt"), b"hello").unwrap();

        let mut args = backup_args(vec![
            source_dir.path().to_path_buf(),
            PathBuf::from("target"),
        ]);
        args.reference = Some(PathBuf::from("does-not-exist"));
        let exit = run_store(&repo_root, args);

        assert_eq!(exit, ExitCode::FAILURE);
        let c = conn(&repo_root);
        assert_eq!(
            count(&c, "tree_entries"),
            1,
            "only the root entry - the target must never have been created"
        );
    }

    #[test]
    fn resolve_reference_wildcard_picks_the_alphabetically_last_match_per_segment() {
        let (_temp_dir, repo_root) = init_repo();
        let source_dir = tempfile::tempdir().unwrap();
        std::fs::write(source_dir.path().join("a.txt"), b"x").unwrap();

        for year in ["2024", "2025"] {
            for stamp in ["01.01", "06.15", "12.31"] {
                let exit = run_store(
                    &repo_root,
                    backup_args(vec![
                        source_dir.path().to_path_buf(),
                        PathBuf::from(format!("backup/{year}/{stamp}")),
                    ]),
                );
                assert_eq!(exit, ExitCode::SUCCESS);
            }
        }

        let c = conn(&repo_root);
        let resolved = resolve_reference(&c, Path::new("backup/????/??.??")).unwrap();
        let expected = find_child(&c, 0, "backup")
            .and_then(|backup_id| find_child(&c, backup_id, "2025"))
            .and_then(|year_id| find_child(&c, year_id, "12.31"))
            .expect("the latest year/timestamp directories must exist");
        assert_eq!(
            resolved, expected,
            "must resolve to the alphabetically last match at each segment independently"
        );
    }

    #[test]
    fn force_reference_bypasses_a_failing_validation() {
        let (_temp_dir, repo_root) = init_repo();

        // Establishes a reference tree whose top-level name has nothing to
        // do with the sources used below.
        let unrelated = tempfile::tempdir().unwrap();
        std::fs::write(unrelated.path().join("x.txt"), b"x").unwrap();
        let exit = run_store(
            &repo_root,
            backup_args(vec![unrelated.path().to_path_buf(), PathBuf::from("ref")]),
        );
        assert_eq!(exit, ExitCode::SUCCESS);

        // Two differently-named sources with no overlap against 'ref' at
        // all, so the fuzzy validation has nothing to match.
        let source_a = tempfile::tempdir().unwrap();
        std::fs::write(source_a.path().join("a.txt"), b"a").unwrap();
        let source_b = tempfile::tempdir().unwrap();
        std::fs::write(source_b.path().join("b.txt"), b"b").unwrap();

        let mut args = backup_args(vec![
            source_a.path().to_path_buf(),
            source_b.path().to_path_buf(),
            PathBuf::from("target"),
        ]);
        args.reference = Some(PathBuf::from("ref"));
        let exit = run_store(&repo_root, args);
        assert_eq!(
            exit,
            ExitCode::FAILURE,
            "validation should reject an unrelated reference"
        );

        let mut args = backup_args(vec![
            source_a.path().to_path_buf(),
            source_b.path().to_path_buf(),
            PathBuf::from("target"),
        ]);
        args.reference = Some(PathBuf::from("ref"));
        args.force_reference = true;
        let exit = run_store(&repo_root, args);
        assert_eq!(
            exit,
            ExitCode::SUCCESS,
            "--force-reference must skip the validation"
        );
    }

    #[test]
    fn rerunning_the_same_backup_creates_no_new_chunks_or_contents() {
        let (_temp_dir, repo_root) = init_repo();
        let source_dir = tempfile::tempdir().unwrap();
        std::fs::write(source_dir.path().join("a.txt"), b"hello world").unwrap();

        let paths = || vec![source_dir.path().to_path_buf(), PathBuf::from("target")];
        assert_eq!(
            run_store(&repo_root, backup_args(paths())),
            ExitCode::SUCCESS
        );
        let c = conn(&repo_root);
        let (chunks_before, contents_before, entries_before) = (
            count(&c, "chunks"),
            count(&c, "contents"),
            count(&c, "tree_entries"),
        );
        drop(c);

        assert_eq!(
            run_store(&repo_root, backup_args(paths())),
            ExitCode::SUCCESS
        );
        let c = conn(&repo_root);
        assert_eq!(count(&c, "chunks"), chunks_before);
        assert_eq!(count(&c, "contents"), contents_before);
        assert_eq!(
            count(&c, "tree_entries"),
            entries_before,
            "unchanged content must refresh the existing entry, not add a new one"
        );
    }

    /// Acceptance test for `docs/plans/implemented/03-chunk-extents.md`: space freed by
    /// deleting and reclaiming a chunk must be reused by a later `store` run,
    /// instead of the data store only ever growing.
    #[test]
    fn a_deleted_and_reclaimed_chunks_space_is_reused_by_a_later_store_run() {
        let (_temp_dir, repo_root) = init_repo();
        let source_dir = tempfile::tempdir().unwrap();
        std::fs::write(source_dir.path().join("a.txt"), b"first content").unwrap();
        std::fs::write(source_dir.path().join("b.txt"), b"second content!!").unwrap();
        let source_name = source_dir.path().file_name().unwrap().to_str().unwrap();
        let paths = || vec![source_dir.path().to_path_buf(), PathBuf::from("target")];

        assert_eq!(
            run_store(&repo_root, backup_args(paths())),
            ExitCode::SUCCESS
        );

        let repository = db::open_repository(&repo_root).unwrap();
        let (a_id, a_start, a_stop): (i64, i64, i64) = {
            let read_conn = repository.open_read_connection().unwrap();
            let a_entry = db::resolve_path(&read_conn, &format!("target/{source_name}/a.txt"))
                .unwrap()
                .unwrap();
            let (start, stop) = read_conn
                .query_row(
                    "SELECT ce.start, ce.stop FROM chunk_extents ce
                     JOIN content_chunks cc ON cc.chunk_id = ce.chunk_id
                     WHERE cc.content_id = ?1",
                    [a_entry.content_id.unwrap()],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap();
            (a_entry.id, start, stop)
        };

        let mut write_conn = repository.open_write_connection().unwrap();
        db::soft_delete(&write_conn, a_id, 1_000_000).unwrap();
        db::reclaim_space(&mut write_conn, 1_000_000).unwrap();
        drop(write_conn);
        // Remove the source file too - otherwise the second `run_store` below
        // would just back it up again (its old chunk row is gone, so it'd be
        // treated as new content), which would itself claim the freed gap and
        // defeat this test's isolation of "does a later, unrelated new chunk
        // reuse the gap".
        std::fs::remove_file(source_dir.path().join("a.txt")).unwrap();

        // Content sized to exactly fill the gap left by a.txt, with bytes
        // distinct from anything already stored so it's treated as new.
        let gap_len = (a_stop - a_start) as usize;
        let filler: Vec<u8> = (0..gap_len).map(|i| (i % 200 + 1) as u8).collect();
        std::fs::write(source_dir.path().join("c.txt"), &filler).unwrap();

        assert_eq!(
            run_store(&repo_root, backup_args(paths())),
            ExitCode::SUCCESS
        );

        let read_conn = repository.open_read_connection().unwrap();
        let c_entry = db::resolve_path(&read_conn, &format!("target/{source_name}/c.txt"))
            .unwrap()
            .unwrap();
        let (c_start, c_stop): (i64, i64) = read_conn
            .query_row(
                "SELECT ce.start, ce.stop FROM chunk_extents ce
                 JOIN content_chunks cc ON cc.chunk_id = ce.chunk_id
                 WHERE cc.content_id = ?1",
                [c_entry.content_id.unwrap()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();

        assert_eq!(
            (c_start, c_stop),
            (a_start, a_stop),
            "c.txt must reuse exactly the space freed by deleting+reclaiming a.txt"
        );
    }

    /// End-to-end regression for `BackupArgs::store_io_parallelism`: several
    /// workers (`--concurrency`) each write a distinct (non-duplicate,
    /// multi-piece) chunk at the same time while store writes are capped to
    /// one at a time. Guards against the `IoLimiter` wiring in
    /// `resolve_chunk`/`chunk_store::write_chunk_from_cache` deadlocking or
    /// otherwise breaking the run under real concurrent contention - the
    /// gating behavior of `IoLimiter` itself has its own focused tests in
    /// `io_limiter`.
    #[test]
    fn run_store_with_store_io_parallelism_one_still_backs_up_every_file_correctly() {
        let (_temp_dir, repo_root) = init_repo();
        let source_dir = tempfile::tempdir().unwrap();
        // Distinct content per file (not just distinct filenames) so every
        // file produces its own new chunk, each one large enough to span
        // several `DRAIN_PIECE_SIZE` write pieces - each piece acquires and
        // releases the single I/O permit in turn.
        let file_count = 6;
        for i in 0..file_count {
            let content: Vec<u8> = (0u32..300_000)
                .map(|b| ((b + i * 37) % 251) as u8)
                .collect();
            std::fs::write(source_dir.path().join(format!("f{i}.txt")), &content).unwrap();
        }

        let mut args = backup_args(vec![
            source_dir.path().to_path_buf(),
            PathBuf::from("target"),
        ]);
        args.concurrency = Some(4);
        args.store_io_parallelism = Some(1);

        let exit = run_store(&repo_root, args);

        assert_eq!(exit, ExitCode::SUCCESS);
        let c = conn(&repo_root);
        assert_eq!(
            count(&c, "contents"),
            file_count as i64 + 1,
            "every file has distinct content, so each must get its own contents row, \
             plus the always-seeded EMPTY_CONTENT_ID row"
        );
    }

    /// End-to-end regression for `docs/plans/implemented/bounded-memory-io-pipeline.md`:
    /// `chunking: none` makes the entire file one chunk (see
    /// `cdc::SingleChunkChunker`), so `SpillingHashingChunker` must buffer
    /// it via disk spillover rather than needing it RAM-resident. A
    /// `chunk_buffer_mb: 0` budget forces spillover for every single byte
    /// written, the most aggressive case - if draining a spilled chunk
    /// (`chunk_store::write_chunk_from_cache`) or its dedup handling were
    /// broken, this would corrupt the stored content instead of just being
    /// slow.
    #[test]
    fn chunking_none_with_a_zero_byte_chunk_buffer_still_round_trips_correctly() {
        let temp_dir = tempfile::tempdir().unwrap();
        let repo_root = temp_dir.path().join("repo");
        db::init_repository(
            &repo_root,
            &db::RepositorySettings::new(12, db::Chunking::None).unwrap(),
        )
        .unwrap();

        let source_dir = tempfile::tempdir().unwrap();
        // Varied, non-trivial content - large enough to span many
        // `READ_BUFFER_SIZE`/spill-write pieces.
        let content: Vec<u8> = (0u32..300_000).map(|i| (i % 251) as u8).collect();
        std::fs::write(source_dir.path().join("a.txt"), &content).unwrap();
        std::fs::write(source_dir.path().join("b.txt"), &content).unwrap();

        let args = BackupArgs {
            create_dirs: true,
            target_exists: false,
            concurrency: Some(2),
            store_io_parallelism: None,
            chunk_buffer_mb: 0,
            allow_swap_risk: false,
            temp: None,
            reference: None,
            force_reference: false,
            lock_wait_secs: 0,
            paths: vec![source_dir.path().to_path_buf(), PathBuf::from("target")],
        };
        let exit = run_store(&repo_root, args);
        assert_eq!(exit, ExitCode::SUCCESS);

        let c = conn(&repo_root);
        assert_eq!(
            count(&c, "contents"),
            2,
            "a.txt and b.txt have identical content - must dedupe to one contents row \
             even though chunking:none means only one (whole-file) chunk is ever compared - \
             plus the always-seeded EMPTY_CONTENT_ID row"
        );
        assert_eq!(count(&c, "chunks"), 1);

        let source_name = source_dir.path().file_name().unwrap().to_str().unwrap();
        let a_entry = db::resolve_path(&c, &format!("target/{source_name}/a.txt"))
            .unwrap()
            .unwrap();
        let (start, stop): (i64, i64) = c
            .query_row(
                "SELECT ce.start, ce.stop FROM chunk_extents ce
                 JOIN content_chunks cc ON cc.chunk_id = ce.chunk_id
                 WHERE cc.content_id = ?1",
                [a_entry.content_id.unwrap()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!((stop - start) as usize, content.len());

        let data_store = store::LongTermStore::new(repo_root.join("data"), true);
        let mut buf = vec![0u8; content.len()];
        let integrity = data_store.read(start as u64, &mut buf).unwrap();
        assert_eq!(integrity, store::ReadIntegrity::Complete);
        assert_eq!(buf, content);
    }
}
