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
/// around the channel [`mpsc::Sender`] it owns) has been dropped, which is what
/// lets the writer thread's `Receiver` see the channel disconnect and exit.
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

    // A single connection drives the up-front target resolution and directory
    // pass below (all on the main thread, before any parallel work starts), then
    // is handed to the writer thread by value - see RunContext's doc comment.
    let main_conn = match repository.open_write_connection() {
        Ok(conn) => conn,
        Err(err) => {
            eprintln!("error: failed to open the metadata database: {err}");
            return ExitCode::FAILURE;
        }
    };

    let target_id = match resolve_target(&main_conn, target, args.create_dirs, args.target_exists) {
        Ok(id) => id,
        Err(msg) => {
            eprintln!("error: {msg}");
            return ExitCode::FAILURE;
        }
    };

    let mut files: Vec<(PathBuf, i64)> = Vec::new();
    let mut warning_count = 0u64;
    for source in sources {
        if let Err(msg) = walk_and_create_dirs(
            &main_conn,
            source,
            target_id,
            &mut files,
            &mut warning_count,
        ) {
            eprintln!("error: {msg}");
            return ExitCode::FAILURE;
        }
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

    // `ctx` is moved into this closure and dropped when it returns (after all
    // files are processed), which drops its Sender and lets the writer thread's
    // Receiver see the channel disconnect and finish - see RunContext's doc
    // comment. `abort`/`warnings` are separate Arc clones, unaffected by that.
    let run = move || {
        files
            .into_par_iter()
            .for_each(|(path, parent_id)| process_file(&ctx, &path, parent_id));
    };
    match args.concurrency {
        Some(concurrency) => {
            let pool = ThreadPoolBuilder::new()
                .num_threads(concurrency as usize)
                .build()
                .expect("failed to build thread pool");
            pool.install(run);
        }
        None => run(),
    }

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
fn walk_and_create_dirs(
    conn: &Connection,
    source: &Path,
    target_id: i64,
    files: &mut Vec<(PathBuf, i64)>,
    warnings: &mut u64,
) -> Result<(), String> {
    let mut dir_ids: HashMap<PathBuf, i64> = HashMap::new();
    let mut ignore_scopes: HashMap<PathBuf, Vec<IgnoreRule>> = HashMap::new();

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
            files.push((entry.path().to_path_buf(), parent_id));
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
            paths: std::mem::take(&mut paths),
        }
    }

    fn conn(repo_root: &Path) -> Connection {
        Connection::open(repo_root.join("meta").join("repository.db")).unwrap()
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
            2,
            "two distinct contents: 'hello world' and 'different'"
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
            file_count as i64,
            "every file has distinct content, so each must get its own contents row"
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
            paths: vec![source_dir.path().to_path_buf(), PathBuf::from("target")],
        };
        let exit = run_store(&repo_root, args);
        assert_eq!(exit, ExitCode::SUCCESS);

        let c = conn(&repo_root);
        assert_eq!(
            count(&c, "contents"),
            1,
            "a.txt and b.txt have identical content - must dedupe to one contents row \
             even though chunking:none means only one (whole-file) chunk is ever compared"
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
