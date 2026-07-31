//! Migrates an old Scala repository into a fresh Rust repository.
//!
//! Input is the zipped SQL script produced by the Scala tool's `fsc db-backup`
//! (`org.h2.tools.Script`, see the plan doc this implements:
//! `docs/plans/implemented/scala-rust-store-migration.md`) - a portable, plain-SQL
//! reconstruction of the old H2 `TreeEntries`/`DataEntries` tables - plus the old
//! repository's `data/` directory (read-only; Rust's own `store::LongTermStore`
//! reads it directly, no conversion needed, since the byte store's on-disk layout
//! is 1:1 compatible between Scala and Rust).
//!
//! High level flow:
//! 1. [`script_import::build_staging_db`] loads the script's `TreeEntries`/
//!    `DataEntries` rows into a temporary staging SQLite database shaped like
//!    the old schema, sidestepping any need for an H2 driver or a real SQL
//!    parser - just enough statement splitting to pull out `INSERT` statements
//!    for those two tables and ignore everything else (`CREATE TABLE`,
//!    `ALTER TABLE`, sequence/comment noise, other tables).
//! 2. [`Migration::walk_directory`] walks the staging tree depth-first from the
//!    root, recreating every entry - active *and* soft-deleted, preserving full
//!    history/restore capability - in the target repository via
//!    `db::insert_historical_tree_entry`.
//! 3. Each old file's content is read back from the old `data/` directory,
//!    re-chunked and re-hashed through the same pipeline `store`'s `store`
//!    command uses (`SpillingHashingChunker` + `db::find_chunk` dedup +
//!    `chunk_store::write_chunk_from_cache` + `db::resolve_content`), written
//!    into the *new* repository's own `data/` directory. Multiple old tree
//!    entries sharing one old `dataId` are only ever chunked once (see
//!    [`Migration::data_id_cache`]).
//! 4. A summary is printed, notably comparing the old repository's whole-file-
//!    dedup storage size against the new repository's chunk-level-dedup
//!    storage size - see [`Stats`].

use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;

use cdc::CdcConfig;
use clap::Args;
use rusqlite::Connection;

use crate::chunk_store::{self, SpaceAllocator};
use crate::format::readable_bytes;
use crate::ram_budget_check::check_ram_budget;
use crate::spilling_chunker::{SpilledChunk, SpillingHashingChunker};
use crate::store::{Blake3Hasher, HASH_LENGTH, make_chunker};
use crate::write_cache::RamBudget;

/// Number of bytes read from the old data store at a time while re-chunking
/// one old file's content - mirrors `store.rs`'s `READ_BUFFER_SIZE`.
const READ_BUFFER_SIZE: usize = 64 * 1024;

/// Same default as `store --chunk-buffer-mb` (see its doc comment for the
/// full rationale) - this tool reuses the identical RAM-budgeted,
/// disk-spilling chunk buffer.
const DEFAULT_CHUNK_BUFFER_MB: u64 = 128;

#[derive(Args)]
pub struct MigrateScalaRepoArgs {
    /// Path to the H2 SQL script export produced by the Scala tool's
    /// `fsc db-backup` command - either the zipped script as produced
    /// directly, or an already-unzipped `.sql` file.
    #[arg(long)]
    script: PathBuf,

    /// Path to the old Scala repository's `data/` directory. Opened
    /// read-only - the old repository is never modified.
    #[arg(long = "old-data")]
    old_data: PathBuf,

    /// RAM budget, in megabytes, for buffering an in-progress chunk's bytes
    /// while its dedup status is resolved - see `store --chunk-buffer-mb`'s
    /// doc comment for the full rationale; this tool reuses the same
    /// RAM-budgeted, disk-spilling chunk buffer.
    #[arg(long, default_value_t = DEFAULT_CHUNK_BUFFER_MB)]
    chunk_buffer_mb: u64,

    /// Start anyway if `--chunk-buffer-mb` looks large enough, relative to
    /// currently available RAM, to risk pushing the machine into swapping.
    #[arg(long)]
    allow_swap_risk: bool,
}

pub fn run_migrate_scala_repo(repo: &Path, args: MigrateScalaRepoArgs) -> ExitCode {
    if let Err(msg) = check_ram_budget(
        "chunk-buffer-mb",
        args.chunk_buffer_mb,
        args.allow_swap_risk,
    ) {
        eprintln!("error: {msg}");
        return ExitCode::FAILURE;
    }

    if let Err(err) = std::fs::metadata(&args.old_data) {
        eprintln!(
            "error: cannot access old data directory '{}': {err}",
            args.old_data.display()
        );
        return ExitCode::FAILURE;
    }

    let repository = match db::open_repository(repo) {
        Ok(r) => r,
        Err(err) => {
            eprintln!(
                "error: failed to open target repository at {}: {err}",
                repo.display()
            );
            eprintln!(
                "hint: the target repository must already be initialized (run \
                 'backup -r {} init' first) with a fresh, empty repository - this \
                 tool migrates into an existing but empty repository, it doesn't \
                 create one",
                repo.display()
            );
            return ExitCode::FAILURE;
        }
    };

    let mut write_conn = match repository.open_write_connection() {
        Ok(c) => c,
        Err(err) => {
            eprintln!("error: failed to open the target metadata database: {err}");
            return ExitCode::FAILURE;
        }
    };

    let existing_entries: i64 = match write_conn.query_row(
        "SELECT COUNT(*) FROM tree_entries WHERE id != 0",
        (),
        |row| row.get(0),
    ) {
        Ok(n) => n,
        Err(err) => {
            eprintln!("error: failed to inspect target repository: {err}");
            return ExitCode::FAILURE;
        }
    };
    if existing_entries > 0 {
        eprintln!(
            "error: target repository at {} is not empty (it already has tree \
             entries besides the root) - migrate into a fresh, empty repository",
            repo.display()
        );
        return ExitCode::FAILURE;
    }

    let cdc_config = match repository.settings().chunking() {
        db::Chunking::Cdc => Some(
            CdcConfig::new(repository.settings().cdc_target_size_bits())
                .expect("validated by RepositorySettings"),
        ),
        db::Chunking::None => None,
    };

    let script_text = match load_script_text(&args.script) {
        Ok(text) => text,
        Err(msg) => {
            eprintln!("error: {msg}");
            return ExitCode::FAILURE;
        }
    };

    // One scoped work directory for both the staging database and the
    // chunk-buffer spillover files - a plain `TempDir` (not `.keep()`'d, unlike
    // `store.rs`'s spill dir) is enough since, unlike `store`'s multi-threaded
    // pipeline, this whole migration runs sequentially within this function's
    // stack frame: nothing outlives it, so RAII cleanup on drop (success *or*
    // error return) is all that's needed.
    let work_dir = match tempfile::Builder::new()
        .prefix("migrate-scala-repo-")
        .tempdir()
    {
        Ok(dir) => dir,
        Err(err) => {
            eprintln!("error: failed to create a working temp directory: {err}");
            return ExitCode::FAILURE;
        }
    };
    let staging_path = work_dir.path().join("staging.db");
    let spill_dir = work_dir.path().join("chunk-buffer");
    if let Err(err) = std::fs::create_dir(&spill_dir) {
        eprintln!("error: failed to create chunk-buffer temp dir: {err}");
        return ExitCode::FAILURE;
    }

    let (staging_conn, staging_stats) =
        match script_import::build_staging_db(&script_text, &staging_path) {
            Ok(result) => result,
            Err(msg) => {
                eprintln!("error: {msg}");
                return ExitCode::FAILURE;
            }
        };
    println!(
        "Loaded {} tree entries and {} data entries from the script export.",
        staging_stats.tree_entries, staging_stats.data_entries
    );

    let extents = match db::chunk_extents_sorted(&write_conn) {
        Ok(e) => e,
        Err(err) => {
            eprintln!("error: failed to inspect target repository free space: {err}");
            return ExitCode::FAILURE;
        }
    };
    let allocator = SpaceAllocator::from_sorted_extents(&extents);
    let new_store = store::LongTermStore::new(repository.data_dir(), false);
    let old_store = store::LongTermStore::new(&args.old_data, true);
    let ram_budget = Arc::new(RamBudget::new(args.chunk_buffer_mb * 1024 * 1024));

    let tx = match write_conn.transaction() {
        Ok(tx) => tx,
        Err(err) => {
            eprintln!("error: failed to start the migration transaction: {err}");
            return ExitCode::FAILURE;
        }
    };

    let mut migration = Migration {
        tx,
        staging: &staging_conn,
        old_store: &old_store,
        new_store: &new_store,
        allocator: &allocator,
        cdc_config: &cdc_config,
        ram_budget,
        spill_dir,
        data_id_cache: HashMap::new(),
        stats: Stats::default(),
    };

    match migration.walk_directory(0, 0) {
        Ok(()) => match migration.tx.commit() {
            Ok(()) => {
                print_summary(&migration.stats);
                ExitCode::SUCCESS
            }
            Err(err) => {
                eprintln!("error: failed to commit the migration: {err}");
                ExitCode::FAILURE
            }
        },
        Err(msg) => {
            eprintln!("error: {msg}");
            eprintln!(
                "migration aborted; the target repository was left unchanged (the \
                 in-progress transaction was rolled back)"
            );
            ExitCode::FAILURE
        }
    }
}

/// Reads `path` as UTF-8 SQL script text, transparently unzipping it first if
/// it's a zip archive (detected by magic bytes, not by file extension - the
/// zip produced by `fsc db-backup` and an already-unzipped `.sql` file should
/// both just work regardless of what the user named them). A zip archive is
/// expected to contain exactly one `.sql`-named entry, or - if none is named
/// that way - exactly one entry of any name.
fn load_script_text(path: &Path) -> Result<String, String> {
    let mut file =
        File::open(path).map_err(|err| format!("failed to open '{}': {err}", path.display()))?;
    let mut magic = [0u8; 4];
    let n = file
        .read(&mut magic)
        .map_err(|err| format!("failed to read '{}': {err}", path.display()))?;
    let is_zip = n == 4 && magic == *b"PK\x03\x04";

    if !is_zip {
        let mut text = String::new();
        file.read_to_string(&mut text)
            .map_err(|err| format!("failed to read '{}' as text: {err}", path.display()))?;
        return Ok(text);
    }

    let file =
        File::open(path).map_err(|err| format!("failed to reopen '{}': {err}", path.display()))?;
    let mut archive = zip::ZipArchive::new(file).map_err(|err| {
        format!(
            "failed to read '{}' as a zip archive: {err}",
            path.display()
        )
    })?;

    let mut sql_index = None;
    for i in 0..archive.len() {
        let entry = archive
            .by_index(i)
            .map_err(|err| format!("failed to read zip entry {i}: {err}"))?;
        if entry.name().to_ascii_lowercase().ends_with(".sql") {
            sql_index = Some(i);
            break;
        }
    }
    let index = match sql_index {
        Some(i) => i,
        None if archive.len() == 1 => 0,
        None => {
            return Err(format!(
                "'{}' contains no '.sql' entry and more than one file; expected a \
                 single H2 script export (as produced by 'fsc db-backup')",
                path.display()
            ));
        }
    };
    let mut entry = archive
        .by_index(index)
        .map_err(|err| format!("failed to read zip entry {index}: {err}"))?;
    let mut text = String::new();
    entry
        .read_to_string(&mut text)
        .map_err(|err| format!("failed to read SQL script from '{}': {err}", entry.name()))?;
    Ok(text)
}

/// Migration run state, threaded through the recursive tree walk. Holds the
/// single write transaction for the whole run (see `run_migrate_scala_repo`'s
/// doc comment on why one big transaction, committed only at the very end, is
/// a deliberate choice here: on any failure it leaves the target repository
/// exactly as empty as it started, matching this tool's "just re-run against
/// a fresh repository" recovery story with no extra effort).
struct Migration<'a> {
    tx: rusqlite::Transaction<'a>,
    staging: &'a Connection,
    old_store: &'a store::LongTermStore,
    new_store: &'a store::LongTermStore,
    allocator: &'a SpaceAllocator,
    cdc_config: &'a Option<CdcConfig>,
    ram_budget: Arc<RamBudget>,
    spill_dir: PathBuf,
    /// Old Scala `dataId` -> new repository `content_id`, `None` for an old
    /// `dataId` whose bytes couldn't be recovered (see [`Self::resolve_content_id`]).
    /// Ensures a `dataId` shared by several old tree entries (Scala's own
    /// whole-file dedup) is only ever chunked once.
    data_id_cache: HashMap<i64, Option<i64>>,
    stats: Stats,
}

/// Counts and sizes reported at the end of a migration run.
#[derive(Debug, Default)]
struct Stats {
    dirs: u64,
    deleted_dirs: u64,
    files: u64,
    deleted_files: u64,
    empty_files: u64,
    /// Tree entries omitted because their old content couldn't be recovered
    /// (e.g. blacklisted: a `dataId` whose `DataEntries` rows have had their
    /// storage allocation removed, `start == stop == 0`).
    skipped: u64,
    warnings: u64,
    /// Distinct old `dataId`s successfully re-chunked (i.e. `data_id_cache`
    /// entries resolving to `Some`).
    distinct_contents: u64,
    /// Sum, over each distinct migrated old `dataId`, of its *old* physical
    /// storage size (`DataEntries.stop - start`, summed across parts) - the
    /// bytes a naive whole-file copy would have needed, already deduplicated
    /// at the whole-file level the way Scala itself already did.
    old_storage_bytes: u64,
    /// Sum of chunk lengths actually newly written to the new repository's
    /// data store during this run (`db::ChunkRef::New` chunks only) - the new
    /// repository's actual physical storage size after chunk-level dedup,
    /// since the target starts out empty.
    new_storage_bytes: u64,
}

fn print_summary(stats: &Stats) {
    println!("Scala repository migration completed.");
    println!(
        "  Directories migrated: {} ({} soft-deleted)",
        stats.dirs, stats.deleted_dirs
    );
    println!(
        "  Files migrated: {} ({} soft-deleted, {} empty)",
        stats.files, stats.deleted_files, stats.empty_files
    );
    if stats.skipped > 0 {
        println!(
            "  Skipped (old content unavailable, e.g. blacklisted): {}",
            stats.skipped
        );
    }
    if stats.warnings > 0 {
        println!("  Warnings: {}", stats.warnings);
    }
    println!(
        "  Distinct old file contents re-chunked: {}",
        stats.distinct_contents
    );
    println!(
        "  Old storage referenced (whole-file dedup only): {}",
        readable_bytes(stats.old_storage_bytes)
    );
    println!(
        "  New storage written (chunk-level dedup):        {}",
        readable_bytes(stats.new_storage_bytes)
    );
    let saved = stats
        .old_storage_bytes
        .saturating_sub(stats.new_storage_bytes);
    let percent = if stats.old_storage_bytes > 0 {
        saved as f64 / stats.old_storage_bytes as f64 * 100.0
    } else {
        0.0
    };
    println!(
        "  Additional space saved by chunk-level dedup: {} ({percent:.1}%)",
        readable_bytes(saved)
    );
}

impl Migration<'_> {
    fn warn(&mut self, msg: &str) {
        eprintln!("warning: {msg}");
        self.stats.warnings += 1;
    }

    /// Recursively migrates every child (active *and* soft-deleted) of
    /// `old_parent_id` in the staging tree into `new_parent_id` in the target
    /// repository. `old_parent_id == new_parent_id == 0` for the initial call
    /// (both repositories' roots); `id != parent_id` in the underlying query
    /// (mirroring `db::soft_delete`'s and `query.rs`'s own subtree walks)
    /// keeps this from trying to visit the root as its own child, and
    /// incidentally also skips any self-referencing "unrooted" row a Scala
    /// `reclaim-space` run might have left mid-flight (not reachable from the
    /// root by construction, so never visited here regardless - see the
    /// `Database.unrootAndMarkDeleted`/`deleteUnrootedEntries` pair in the
    /// Scala source).
    fn walk_directory(&mut self, old_parent_id: i64, new_parent_id: i64) -> Result<(), String> {
        for child in script_import::staging_children(self.staging, old_parent_id)? {
            let deleted_at = if child.deleted == 0 {
                None
            } else {
                Some(child.deleted)
            };

            match child.data_id {
                None => {
                    let new_id = db::insert_historical_tree_entry(
                        &self.tx,
                        new_parent_id,
                        &child.name,
                        child.time,
                        deleted_at,
                        db::EntryKind::Dir,
                        None,
                    )
                    .map_err(|err| format!("failed to create directory '{}': {err}", child.name))?;
                    self.stats.dirs += 1;
                    if deleted_at.is_some() {
                        self.stats.deleted_dirs += 1;
                    }
                    self.walk_directory(child.id, new_id)?;
                }
                Some(-1) => {
                    self.insert_file(new_parent_id, &child, deleted_at, None)?;
                }
                Some(old_data_id) => match self.resolve_content_id(old_data_id)? {
                    Some(content_id) => {
                        self.insert_file(new_parent_id, &child, deleted_at, Some(content_id))?;
                    }
                    None => {
                        self.warn(&format!(
                            "skipping '{}' (old dataId {old_data_id} has no recoverable \
                             data - blacklisted or corrupt?)",
                            child.name
                        ));
                        self.stats.skipped += 1;
                    }
                },
            }
        }
        Ok(())
    }

    fn insert_file(
        &mut self,
        new_parent_id: i64,
        child: &script_import::StagingTreeEntry,
        deleted_at: Option<i64>,
        content_id: Option<i64>,
    ) -> Result<(), String> {
        db::insert_historical_tree_entry(
            &self.tx,
            new_parent_id,
            &child.name,
            child.time,
            deleted_at,
            db::EntryKind::File,
            content_id,
        )
        .map_err(|err| format!("failed to create file '{}': {err}", child.name))?;
        self.stats.files += 1;
        if deleted_at.is_some() {
            self.stats.deleted_files += 1;
        }
        if content_id.is_none() {
            self.stats.empty_files += 1;
        }
        Ok(())
    }

    /// Resolves `old_data_id` to a new-repository `content_id`, re-chunking
    /// its bytes the first time it's seen and reusing the cached result for
    /// any later tree entry that shares the same old `dataId` (Scala's own
    /// whole-file dedup). `None` means the old data couldn't be recovered
    /// (see [`Stats::skipped`]).
    fn resolve_content_id(&mut self, old_data_id: i64) -> Result<Option<i64>, String> {
        if let Some(&cached) = self.data_id_cache.get(&old_data_id) {
            return Ok(cached);
        }
        let parts = script_import::staging_data_parts(self.staging, old_data_id)?;
        let result = if parts.is_empty() {
            None
        } else {
            Some(self.chunk_and_store(old_data_id, &parts)?)
        };
        self.data_id_cache.insert(old_data_id, result);
        Ok(result)
    }

    /// Reads `old_data_id`'s bytes back from the old data store (concatenating
    /// `parts` in order) and re-chunks/re-hashes/re-stores them into the new
    /// repository, returning the resulting `content_id`.
    fn chunk_and_store(&mut self, old_data_id: i64, parts: &[(u64, u64)]) -> Result<i64, String> {
        // Self-contained (doesn't borrow `self`), unlike a closure calling
        // back into a `&mut self` method would: `chunker` stays alive across
        // this whole function, including later calls to `self.resolve_chunk`
        // - a closure borrowing `self` here would conflict with those.
        // Restarting the counter at 0 for every old `dataId` is safe (not a
        // spill-path collision risk) only because this tool is
        // single-threaded and fully sequential: every `WriteCache` handed
        // out by one `chunk_and_store` call (and thus every spill file it
        // may have created) is resolved and dropped - see `resolve_chunk` -
        // before the next call starts.
        let spill_dir = self.spill_dir.clone();
        let mut spill_seq = 0u64;
        let mut chunker = SpillingHashingChunker::new(
            Blake3Hasher(blake3::Hasher::new()),
            make_chunker(self.cdc_config),
            Arc::clone(&self.ram_budget),
            move || {
                spill_seq += 1;
                spill_dir.join(format!("{old_data_id}-{spill_seq}"))
            },
        );
        let mut content_hasher = blake3::Hasher::new();
        let mut chunk_refs = Vec::new();
        let mut read_buf = vec![0u8; READ_BUFFER_SIZE];
        let mut incomplete_reads = false;

        for &(start, stop) in parts {
            let mut pos = start;
            while pos < stop {
                let n = ((stop - pos).min(READ_BUFFER_SIZE as u64)) as usize;
                let integrity = self
                    .old_store
                    .read(pos, &mut read_buf[..n])
                    .map_err(|err| format!("failed reading old data store at {pos}: {err}"))?;
                if matches!(integrity, store::ReadIntegrity::Incomplete { .. }) {
                    incomplete_reads = true;
                }
                let chunks = chunker
                    .next(&read_buf[..n])
                    .map_err(|err| format!("chunk buffering failed: {err}"))?;
                for chunk in chunks {
                    self.resolve_chunk(chunk, &mut chunk_refs, &mut content_hasher)?;
                }
                pos += n as u64;
            }
        }
        if let Some(chunk) = chunker
            .flush()
            .map_err(|err| format!("chunk buffering failed: {err}"))?
        {
            self.resolve_chunk(chunk, &mut chunk_refs, &mut content_hasher)?;
        }

        if incomplete_reads {
            self.warn(&format!(
                "old data for dataId {old_data_id} was missing or shorter than \
                 expected in one or more places; the gap was filled with zero bytes"
            ));
        }

        let mut content_hash = [0u8; HASH_LENGTH];
        content_hasher.finalize_xof().fill(&mut content_hash);
        let content_id = db::resolve_content(&self.tx, &chunk_refs, &content_hash)
            .map_err(|err| {
                format!("failed to resolve content for old dataId {old_data_id}: {err}")
            })?
            .expect(
                "parts is non-empty and every part has stop > start, so chunking \
                 produces at least one chunk - resolve_content only returns None \
                 for zero chunks",
            );

        let old_storage_bytes: u64 = parts.iter().map(|&(s, e)| e - s).sum();
        self.stats.old_storage_bytes += old_storage_bytes;
        self.stats.distinct_contents += 1;

        Ok(content_id)
    }

    /// Resolves one completed chunk against the dedup index: reuses an
    /// existing chunk id on a hit, or reserves store space and writes the
    /// chunk's bytes into the *new* repository's data store on a miss. Also
    /// feeds the chunk's length and hash into `content_hasher` - mirrors
    /// `store.rs`'s `resolve_chunk`, single-connection instead of a
    /// thread-local read connection since this tool is single-threaded.
    fn resolve_chunk(
        &mut self,
        chunk: SpilledChunk,
        chunk_refs: &mut Vec<db::ChunkRef>,
        content_hasher: &mut blake3::Hasher,
    ) -> Result<(), String> {
        let length_hash = chunk.length_hash;
        content_hasher.update(&length_hash.length.to_le_bytes());
        content_hasher.update(&length_hash.hash);

        let existing = db::find_chunk(&self.tx, length_hash.length, &length_hash.hash)
            .map_err(|err| format!("dedup lookup failed: {err}"))?;

        let chunk_ref = match existing {
            Some(id) => db::ChunkRef::Existing {
                id,
                length: length_hash.length,
            },
            None => {
                let mut bytes = chunk.bytes;
                let extents = chunk_store::write_chunk_from_cache(
                    self.new_store,
                    self.allocator,
                    &mut bytes,
                    length_hash.length,
                )
                .map_err(|err| format!("store write failed: {err}"))?;
                self.stats.new_storage_bytes += length_hash.length;
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

/// Loads the Scala H2 SQL script export into a temporary staging SQLite
/// database shaped like the old `TreeEntries`/`DataEntries` schema, and reads
/// it back for [`Migration`]'s tree walk. Kept separate from the rest of this
/// module: this is the one part of the tool that has to deal with the H2
/// export's SQL dialect, everything downstream only ever sees plain,
/// already-typed rows.
mod script_import {
    use rusqlite::{Connection, params};
    use std::path::Path;

    /// Staging schema: same columns as Scala's `TreeEntries`/`DataEntries`
    /// (see `db/Database.scala`'s `tableDefinitions`), renamed to this
    /// project's `snake_case` convention. No foreign keys/uniqueness
    /// constraints beyond what's needed to store the data - the old
    /// database already enforced its own invariants; this is a working copy,
    /// not a second source of truth.
    const STAGING_SCHEMA: &str = "
        CREATE TABLE tree_entries (
          id        INTEGER PRIMARY KEY,
          parent_id INTEGER NOT NULL,
          name      TEXT    NOT NULL,
          time      INTEGER NOT NULL,
          deleted   INTEGER NOT NULL,
          data_id   INTEGER
        );
        CREATE INDEX staging_tree_entries_parent_idx ON tree_entries(parent_id);
        CREATE TABLE data_entries (
          id     INTEGER NOT NULL,
          seq    INTEGER NOT NULL,
          length INTEGER,
          start  INTEGER NOT NULL,
          stop   INTEGER NOT NULL,
          hash   BLOB,
          PRIMARY KEY (id, seq)
        );
    ";

    /// A `TreeEntries` row read back from the staging database.
    #[derive(Debug, Clone, PartialEq)]
    pub(super) struct StagingTreeEntry {
        pub id: i64,
        pub name: String,
        pub time: i64,
        /// `0` for an active entry, otherwise the deletion timestamp -
        /// matches Scala's own encoding (see `Database.scala`'s
        /// `tableDefinitions` doc comment: "deleted == 0 for regular files,
        /// deleted == timestamp for deleted files, because NULL does not
        /// work with UNIQUE").
        pub deleted: i64,
        /// `None` for a directory, `Some(-1)` for an empty (0-length) file
        /// (Scala's own sentinel - see `Backend.scala`'s persist logic),
        /// `Some(id)` (`id >= 0`) otherwise, referencing `data_entries.id`.
        pub data_id: Option<i64>,
    }

    /// Row counts imported into the staging database, for the tool's initial
    /// progress line.
    pub(super) struct StagingStats {
        pub tree_entries: usize,
        pub data_entries: usize,
    }

    /// Parses `script_text` and loads its `TreeEntries`/`DataEntries` rows
    /// into a fresh SQLite database at `staging_path`, returning an open
    /// connection to it plus the row counts imported.
    pub(super) fn build_staging_db(
        script_text: &str,
        staging_path: &Path,
    ) -> Result<(Connection, StagingStats), String> {
        let mut conn = Connection::open(staging_path)
            .map_err(|err| format!("failed to create staging database: {err}"))?;
        conn.execute_batch(STAGING_SCHEMA)
            .map_err(|err| format!("failed to create staging schema: {err}"))?;

        let cleaned = strip_line_comments(script_text);
        let mut stats = StagingStats {
            tree_entries: 0,
            data_entries: 0,
        };

        let tx = conn
            .transaction()
            .map_err(|err| format!("failed to start staging import transaction: {err}"))?;
        for stmt in iter_statements(&cleaned) {
            if !starts_with_ci(stmt, "insert") {
                continue;
            }
            let parsed = parse_insert(stmt).ok_or_else(|| {
                format!(
                    "failed to parse an INSERT statement from the script export \
                     (unsupported syntax?): {}",
                    truncate_for_error(stmt)
                )
            })?;

            match parsed.table.as_str() {
                "TREEENTRIES" => {
                    let positions =
                        column_positions(&parsed.columns, &TREE_ENTRIES_DEFAULT_COLUMNS)
                            .ok_or_else(|| {
                                "a TreeEntries INSERT is missing an expected column".to_string()
                            })?;
                    for tuple in &parsed.tuples {
                        let row = tree_entry_from_tuple(tuple, &positions).ok_or_else(|| {
                            format!("failed to parse a TreeEntries row: {tuple:?}")
                        })?;
                        tx.execute(
                            "INSERT INTO tree_entries (id, parent_id, name, time, deleted, data_id)
                             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                            params![row.0, row.1, row.2, row.3, row.4, row.5],
                        )
                        .map_err(|err| {
                            format!("failed to import TreeEntries row {}: {err}", row.0)
                        })?;
                        stats.tree_entries += 1;
                    }
                }
                "DATAENTRIES" => {
                    let positions =
                        column_positions(&parsed.columns, &DATA_ENTRIES_DEFAULT_COLUMNS)
                            .ok_or_else(|| {
                                "a DataEntries INSERT is missing an expected column".to_string()
                            })?;
                    for tuple in &parsed.tuples {
                        let row = data_entry_from_tuple(tuple, &positions).ok_or_else(|| {
                            format!("failed to parse a DataEntries row: {tuple:?}")
                        })?;
                        tx.execute(
                            "INSERT INTO data_entries (id, seq, length, start, stop, hash)
                             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                            params![row.0, row.1, row.2, row.3, row.4, row.5],
                        )
                        .map_err(|err| {
                            format!(
                                "failed to import DataEntries row (id {}, seq {}): {err}",
                                row.0, row.1
                            )
                        })?;
                        stats.data_entries += 1;
                    }
                }
                // Context (db version marker) or anything else - not needed
                // for migration, silently ignored.
                _ => {}
            }
        }
        tx.commit()
            .map_err(|err| format!("failed to commit staging import: {err}"))?;

        Ok((conn, stats))
    }

    fn truncate_for_error(s: &str) -> String {
        const LIMIT: usize = 200;
        if s.len() <= LIMIT {
            s.to_string()
        } else {
            format!("{}...", &s[..LIMIT])
        }
    }

    /// The active *and* soft-deleted direct children of `parent_id`, ordered
    /// by id (irrelevant for correctness - [`super::Migration::walk_directory`]
    /// doesn't need chronological order, see its own doc comment - but
    /// deterministic, which is convenient for tests).
    pub(super) fn staging_children(
        conn: &Connection,
        parent_id: i64,
    ) -> Result<Vec<StagingTreeEntry>, String> {
        let mut stmt = conn
            .prepare(
                "SELECT id, name, time, deleted, data_id FROM tree_entries
                 WHERE parent_id = ?1 AND id != ?1 ORDER BY id",
            )
            .map_err(|err| format!("staging query failed: {err}"))?;
        stmt.query_map([parent_id], |row| {
            Ok(StagingTreeEntry {
                id: row.get(0)?,
                name: row.get(1)?,
                time: row.get(2)?,
                deleted: row.get(3)?,
                data_id: row.get(4)?,
            })
        })
        .and_then(Iterator::collect)
        .map_err(|err| format!("staging query failed: {err}"))
    }

    /// `data_id`'s storage parts, in `seq` order, as `(start, stop)` - mirrors
    /// Scala's own `Database.parts` (`db/Database.scala`): zero-size parts
    /// (`start == stop`, left behind when the old repository "blacklisted" -
    /// removed the storage allocation for - this data) are filtered out, the
    /// same way Scala's own `parts` does. An empty result means no bytes are
    /// recoverable for this `data_id` at all.
    pub(super) fn staging_data_parts(
        conn: &Connection,
        data_id: i64,
    ) -> Result<Vec<(u64, u64)>, String> {
        let mut stmt = conn
            .prepare("SELECT start, stop FROM data_entries WHERE id = ?1 ORDER BY seq")
            .map_err(|err| format!("staging query failed: {err}"))?;
        let rows: Vec<(i64, i64)> = stmt
            .query_map([data_id], |row| Ok((row.get(0)?, row.get(1)?)))
            .and_then(Iterator::collect)
            .map_err(|err| format!("staging query failed: {err}"))?;
        Ok(rows
            .into_iter()
            .filter(|&(start, stop)| stop > start)
            .map(|(start, stop)| (start as u64, stop as u64))
            .collect())
    }

    // --- SQL script parsing ---
    //
    // Deliberately not a general SQL parser: just enough statement splitting
    // and `INSERT ... VALUES (...), (...), ...` tuple parsing to pull typed
    // rows out of the two tables this tool cares about, per the plan doc this
    // implements ("sidesteps any need for ... a hand-written SQL parser
    // beyond basic statement splitting"). Every helper below works on raw
    // bytes for delimiter scanning (quotes, parens, commas, semicolons,
    // `--`) rather than `char`s: every delimiter it looks for is ASCII, and
    // UTF-8 guarantees non-ASCII bytes never collide with an ASCII byte
    // value, so byte-indexed scanning is safe and avoids repeated UTF-8
    // decoding of file/directory names that may contain arbitrary non-ASCII
    // characters.

    #[derive(Debug, Clone, PartialEq)]
    enum Value {
        Null,
        Int(i64),
        Text(String),
        Blob(Vec<u8>),
    }

    impl Value {
        fn as_i64(&self) -> Option<i64> {
            match self {
                Value::Int(v) => Some(*v),
                _ => None,
            }
        }

        fn as_text(&self) -> Option<&str> {
            match self {
                Value::Text(v) => Some(v),
                _ => None,
            }
        }
    }

    struct ParsedInsert {
        table: String,
        columns: Option<Vec<String>>,
        tuples: Vec<Vec<Value>>,
    }

    const TREE_ENTRIES_DEFAULT_COLUMNS: [&str; 6] =
        ["ID", "PARENTID", "NAME", "TIME", "DELETED", "DATAID"];
    const DATA_ENTRIES_DEFAULT_COLUMNS: [&str; 6] =
        ["ID", "SEQ", "LENGTH", "START", "STOP", "HASH"];

    /// For each of `defaults` (in order), the index into a tuple parsed
    /// under `columns` where that field's value lives - the identity mapping
    /// if `columns` is `None` (no explicit column list in the `INSERT`, so
    /// values are positional in `defaults`' order, exactly the physical
    /// column order H2's `Script` tool emits by default), otherwise resolved
    /// by name (case-insensitive via `columns` already being upper-cased by
    /// [`parse_insert`]). `None` if `columns` is given but is missing one of
    /// `defaults`.
    fn column_positions(columns: &Option<Vec<String>>, defaults: &[&str]) -> Option<Vec<usize>> {
        match columns {
            None => Some((0..defaults.len()).collect()),
            Some(cols) => defaults
                .iter()
                .map(|want| cols.iter().position(|c| c == want))
                .collect(),
        }
    }

    // A named struct would need the same 6 fields spelled out anyway, for a
    // tuple that's immediately destructured once at its single call site -
    // not worth a whole extra type for.
    #[allow(clippy::type_complexity)]
    fn tree_entry_from_tuple(
        tuple: &[Value],
        positions: &[usize],
    ) -> Option<(i64, i64, String, i64, i64, Option<i64>)> {
        let id = tuple.get(positions[0])?.as_i64()?;
        let parent_id = tuple.get(positions[1])?.as_i64()?;
        let name = tuple.get(positions[2])?.as_text()?.to_string();
        let time = tuple.get(positions[3])?.as_i64()?;
        let deleted = tuple.get(positions[4])?.as_i64()?;
        let data_id = match tuple.get(positions[5])? {
            Value::Null => None,
            v => Some(v.as_i64()?),
        };
        Some((id, parent_id, name, time, deleted, data_id))
    }

    // Same reasoning as `tree_entry_from_tuple` above.
    #[allow(clippy::type_complexity)]
    fn data_entry_from_tuple(
        tuple: &[Value],
        positions: &[usize],
    ) -> Option<(i64, i64, Option<i64>, i64, i64, Option<Vec<u8>>)> {
        let id = tuple.get(positions[0])?.as_i64()?;
        let seq = tuple.get(positions[1])?.as_i64()?;
        let length = match tuple.get(positions[2])? {
            Value::Null => None,
            v => Some(v.as_i64()?),
        };
        let start = tuple.get(positions[3])?.as_i64()?;
        let stop = tuple.get(positions[4])?.as_i64()?;
        let hash = match tuple.get(positions[5])? {
            Value::Null => None,
            Value::Blob(b) => Some(b.clone()),
            _ => None?,
        };
        Some((id, seq, length, start, stop, hash))
    }

    fn starts_with_ci(s: &str, prefix: &str) -> bool {
        s.len() >= prefix.len()
            && s.as_bytes()[..prefix.len()].eq_ignore_ascii_case(prefix.as_bytes())
    }

    fn strip_prefix_ci<'a>(s: &'a str, prefix: &str) -> Option<&'a str> {
        if starts_with_ci(s, prefix) {
            Some(&s[prefix.len()..])
        } else {
            None
        }
    }

    /// Strips `--`-to-end-of-line comments (H2's `Script` tool emits row-count
    /// sanity-check comments like `-- 3 +/- SELECT COUNT(*) FROM ...;` between
    /// statements), respecting single-quoted string literals so a `--` or `;`
    /// inside a file/directory name is never mistaken for a comment/statement
    /// boundary.
    fn strip_line_comments(script: &str) -> String {
        let bytes = script.as_bytes();
        let mut out = String::with_capacity(script.len());
        let mut start = 0usize;
        let mut i = 0usize;
        let mut in_string = false;
        while i < bytes.len() {
            let b = bytes[i];
            if in_string {
                if b == b'\'' {
                    if bytes.get(i + 1) == Some(&b'\'') {
                        i += 2;
                        continue;
                    }
                    in_string = false;
                }
                i += 1;
                continue;
            }
            match b {
                b'\'' => {
                    in_string = true;
                    i += 1;
                }
                b'-' if bytes.get(i + 1) == Some(&b'-') => {
                    out.push_str(&script[start..i]);
                    while i < bytes.len() && bytes[i] != b'\n' {
                        i += 1;
                    }
                    start = i;
                }
                _ => i += 1,
            }
        }
        out.push_str(&script[start..]);
        out
    }

    /// Splits `script` into top-level, semicolon-terminated statements,
    /// respecting quoted string literals (H2/SQL `''`-doubled-quote escaping)
    /// so a `;` inside a name is never mistaken for a statement boundary.
    /// Empty statements (blank lines between real ones) are skipped.
    fn iter_statements(script: &str) -> impl Iterator<Item = &str> {
        let mut rest = script;
        std::iter::from_fn(move || {
            loop {
                if rest.is_empty() {
                    return None;
                }
                let bytes = rest.as_bytes();
                let mut in_string = false;
                let mut end = None;
                let mut i = 0usize;
                while i < bytes.len() {
                    let b = bytes[i];
                    if in_string {
                        if b == b'\'' {
                            if bytes.get(i + 1) == Some(&b'\'') {
                                i += 2;
                                continue;
                            }
                            in_string = false;
                        }
                    } else if b == b'\'' {
                        in_string = true;
                    } else if b == b';' {
                        end = Some(i);
                        break;
                    }
                    i += 1;
                }
                let (stmt, remainder) = match end {
                    Some(i) => (&rest[..i], &rest[i + 1..]),
                    None => (rest, ""),
                };
                rest = remainder;
                let trimmed = stmt.trim();
                if trimmed.is_empty() {
                    continue;
                }
                return Some(trimmed);
            }
        })
    }

    /// Reads one identifier part: a `"..."`-quoted identifier (H2/SQL
    /// `""`-doubled-quote escaping) or a bare `[A-Za-z0-9_$]+` run. Returns
    /// the identifier and the remainder of `s` right after it.
    fn read_identifier_part(s: &str) -> Option<(String, &str)> {
        let s = s.trim_start();
        if let Some(rest) = s.strip_prefix('"') {
            let bytes = rest.as_bytes();
            let mut ident = String::new();
            let mut i = 0usize;
            loop {
                if i >= bytes.len() {
                    return None; // unterminated quoted identifier
                }
                if bytes[i] == b'"' {
                    if bytes.get(i + 1) == Some(&b'"') {
                        ident.push('"');
                        i += 2;
                        continue;
                    }
                    return Some((ident, &rest[i + 1..]));
                }
                let ch = rest[i..]
                    .chars()
                    .next()
                    .expect("i is a valid char boundary");
                ident.push(ch);
                i += ch.len_utf8();
            }
        } else {
            let end = s
                .find(|c: char| !(c.is_alphanumeric() || c == '_' || c == '$'))
                .unwrap_or(s.len());
            if end == 0 {
                return None;
            }
            Some((s[..end].to_string(), &s[end..]))
        }
    }

    /// Reads a possibly schema-qualified identifier (`schema.name`,
    /// `"schema"."name"`, or just `name`), returning only the final
    /// (unqualified) part, upper-cased for case-insensitive matching.
    fn read_qualified_identifier(s: &str) -> Option<(String, &str)> {
        let (mut last, mut rest) = read_identifier_part(s)?;
        loop {
            let trimmed = rest.trim_start();
            match trimmed.strip_prefix('.') {
                Some(after_dot) => {
                    let (part, after) = read_identifier_part(after_dot)?;
                    last = part;
                    rest = after;
                }
                None => break,
            }
        }
        Some((last.to_ascii_uppercase(), rest))
    }

    /// Given `s` starting with `(` (after trimming), returns the text
    /// between the matching close paren (respecting nesting and quoted
    /// strings) and the remainder of `s` right after it.
    fn take_parenthesized(s: &str) -> Option<(&str, &str)> {
        let rest = s.trim_start().strip_prefix('(')?;
        let bytes = rest.as_bytes();
        let mut depth = 1i32;
        let mut in_string = false;
        let mut i = 0usize;
        while i < bytes.len() {
            let b = bytes[i];
            if in_string {
                if b == b'\'' {
                    if bytes.get(i + 1) == Some(&b'\'') {
                        i += 2;
                        continue;
                    }
                    in_string = false;
                }
            } else {
                match b {
                    b'\'' => in_string = true,
                    b'(' => depth += 1,
                    b')' => {
                        depth -= 1;
                        if depth == 0 {
                            return Some((&rest[..i], &rest[i + 1..]));
                        }
                    }
                    _ => {}
                }
            }
            i += 1;
        }
        None
    }

    /// Splits `s` on top-level commas (respecting quoted strings and
    /// parenthesis nesting).
    fn split_top_level(s: &str) -> Vec<&str> {
        let bytes = s.as_bytes();
        let mut parts = Vec::new();
        let mut start = 0usize;
        let mut depth = 0i32;
        let mut in_string = false;
        let mut i = 0usize;
        while i < bytes.len() {
            let b = bytes[i];
            if in_string {
                if b == b'\'' {
                    if bytes.get(i + 1) == Some(&b'\'') {
                        i += 2;
                        continue;
                    }
                    in_string = false;
                }
            } else {
                match b {
                    b'\'' => in_string = true,
                    b'(' => depth += 1,
                    b')' => depth -= 1,
                    b',' if depth == 0 => {
                        parts.push(&s[start..i]);
                        start = i + 1;
                    }
                    _ => {}
                }
            }
            i += 1;
        }
        parts.push(&s[start..]);
        parts
    }

    fn parse_hex(hex: &str) -> Option<Vec<u8>> {
        if !hex.len().is_multiple_of(2) {
            return None;
        }
        (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).ok())
            .collect()
    }

    fn parse_value(token: &str) -> Option<Value> {
        let token = token.trim();
        if token.eq_ignore_ascii_case("null") {
            return Some(Value::Null);
        }
        if let Some(inner) = token.strip_prefix('\'').and_then(|t| t.strip_suffix('\'')) {
            return Some(Value::Text(inner.replace("''", "'")));
        }
        if token.len() >= 3
            && (token.starts_with("X'") || token.starts_with("x'"))
            && token.ends_with('\'')
        {
            return parse_hex(&token[2..token.len() - 1]).map(Value::Blob);
        }
        token.parse::<i64>().ok().map(Value::Int)
    }

    fn parse_insert(stmt: &str) -> Option<ParsedInsert> {
        let rest = strip_prefix_ci(stmt.trim_start(), "insert")?;
        let rest = strip_prefix_ci(rest.trim_start(), "into")?.trim_start();
        let (table, rest) = read_qualified_identifier(rest)?;
        let rest = rest.trim_start();

        let (columns, rest) = if rest.starts_with('(') {
            let (inside, after) = take_parenthesized(rest)?;
            let cols = split_top_level(inside)
                .into_iter()
                .map(|c| read_qualified_identifier(c.trim()).map(|(name, _)| name))
                .collect::<Option<Vec<_>>>()?;
            (Some(cols), after.trim_start())
        } else {
            (None, rest)
        };

        let rest = strip_prefix_ci(rest, "values")?.trim_start();

        let mut tuples = Vec::new();
        let mut cursor = rest;
        loop {
            cursor = cursor.trim_start();
            if cursor.is_empty() {
                break;
            }
            let (inside, after) = take_parenthesized(cursor)?;
            let values = split_top_level(inside)
                .into_iter()
                .map(parse_value)
                .collect::<Option<Vec<_>>>()?;
            tuples.push(values);
            cursor = after.trim_start();
            match cursor.strip_prefix(',') {
                Some(stripped) => cursor = stripped,
                None => break,
            }
        }

        Some(ParsedInsert {
            table,
            columns,
            tuples,
        })
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn parses_a_plain_insert_without_a_column_list() {
            let parsed = parse_insert(
                "INSERT INTO \"PUBLIC\".\"TREEENTRIES\" VALUES\n\
                 (0, 0, '', 1000, 0, NULL),\n\
                 (1, 0, 'a.txt', 1000, 0, -1)",
            )
            .unwrap();
            assert_eq!(parsed.table, "TREEENTRIES");
            assert_eq!(parsed.columns, None);
            assert_eq!(parsed.tuples.len(), 2);
            assert_eq!(
                parsed.tuples[0],
                vec![
                    Value::Int(0),
                    Value::Int(0),
                    Value::Text(String::new()),
                    Value::Int(1000),
                    Value::Int(0),
                    Value::Null,
                ]
            );
            assert_eq!(parsed.tuples[1][5], Value::Int(-1));
        }

        #[test]
        fn parses_an_insert_with_an_explicit_column_list_in_any_order() {
            let parsed = parse_insert(
                "insert into TREEENTRIES (\"DATAID\", \"NAME\", \"ID\", \"PARENTID\", \"TIME\", \"DELETED\") \
                 values (5, 'x', 2, 0, 100, 0)",
            )
            .unwrap();
            let positions =
                column_positions(&parsed.columns, &TREE_ENTRIES_DEFAULT_COLUMNS).unwrap();
            let row = tree_entry_from_tuple(&parsed.tuples[0], &positions).unwrap();
            assert_eq!(row, (2, 0, "x".to_string(), 100, 0, Some(5)));
        }

        #[test]
        fn parses_hash_blob_and_null_length_data_entries_row() {
            let parsed = parse_insert(
                "INSERT INTO \"PUBLIC\".\"DATAENTRIES\" VALUES (7, 1, 42, 1000, 1042, X'0102ff')",
            )
            .unwrap();
            let positions =
                column_positions(&parsed.columns, &DATA_ENTRIES_DEFAULT_COLUMNS).unwrap();
            let row = data_entry_from_tuple(&parsed.tuples[0], &positions).unwrap();
            assert_eq!(row, (7, 1, Some(42), 1000, 1042, Some(vec![1, 2, 0xff])));

            let parsed2 =
                parse_insert("INSERT INTO DATAENTRIES VALUES (7, 2, NULL, 1042, 2000, NULL)")
                    .unwrap();
            let row2 = data_entry_from_tuple(&parsed2.tuples[0], &positions).unwrap();
            assert_eq!(row2, (7, 2, None, 1042, 2000, None));
        }

        #[test]
        fn a_semicolon_or_double_dash_inside_a_quoted_name_does_not_split_statements() {
            let script = "INSERT INTO TREEENTRIES VALUES (1, 0, 'a;b--c', 0, 0, NULL);\n\
                 INSERT INTO TREEENTRIES VALUES (2, 0, 'd', 0, 0, NULL);";
            let statements: Vec<&str> = iter_statements(script).collect();
            assert_eq!(statements.len(), 2);
            let parsed = parse_insert(statements[0]).unwrap();
            assert_eq!(parsed.tuples[0][2], Value::Text("a;b--c".to_string()));
        }

        #[test]
        fn doubled_single_quotes_are_unescaped() {
            let parsed =
                parse_insert("INSERT INTO TREEENTRIES VALUES (1, 0, 'it''s', 0, 0, NULL)").unwrap();
            assert_eq!(parsed.tuples[0][2], Value::Text("it's".to_string()));
        }

        #[test]
        fn strip_line_comments_removes_h2_row_count_comments_but_keeps_string_content() {
            let script = "-- 2 +/- SELECT COUNT(*) FROM PUBLIC.TREEENTRIES;\n\
                          INSERT INTO TREEENTRIES VALUES (1, 0, 'has -- inside', 0, 0, NULL);";
            let cleaned = strip_line_comments(script);
            assert!(cleaned.contains("'has -- inside'"));
            let statements: Vec<&str> = iter_statements(&cleaned).collect();
            assert_eq!(statements.len(), 1);
            assert!(statements[0].starts_with("INSERT INTO TREEENTRIES"));
        }

        #[test]
        fn non_insert_statements_are_ignored_by_the_caller_via_starts_with_ci() {
            assert!(starts_with_ci("INSERT INTO x VALUES (1)", "insert"));
            assert!(!starts_with_ci(
                "CREATE TABLE \"PUBLIC\".\"TREEENTRIES\"(...)",
                "insert"
            ));
            assert!(!starts_with_ci("ALTER TABLE x ADD CONSTRAINT y", "insert"));
        }

        #[test]
        fn build_staging_db_imports_both_tables_and_ignores_everything_else() {
            let script = "\
                CREATE TABLE \"PUBLIC\".\"CONTEXT\"(\"KEY\" VARCHAR, \"VALUE\" VARCHAR);\n\
                INSERT INTO \"PUBLIC\".\"CONTEXT\" VALUES ('db version', '3');\n\
                CREATE SEQUENCE \"PUBLIC\".\"IDSEQ\" START WITH 3;\n\
                -- 2 +/- SELECT COUNT(*) FROM PUBLIC.TREEENTRIES;\n\
                INSERT INTO \"PUBLIC\".\"TREEENTRIES\" VALUES\n\
                (0, 0, '', 1000, 0, NULL),\n\
                (1, 0, 'a.txt', 1000, 0, 0);\n\
                ALTER TABLE \"PUBLIC\".\"TREEENTRIES\" ADD CONSTRAINT pk PRIMARY KEY(ID);\n\
                INSERT INTO \"PUBLIC\".\"DATAENTRIES\" VALUES (0, 1, 5, 0, 5, X'0011223344');\n";

            let temp_dir = tempfile::tempdir().unwrap();
            let (conn, stats) =
                build_staging_db(script, &temp_dir.path().join("staging.db")).unwrap();
            assert_eq!(stats.tree_entries, 2);
            assert_eq!(stats.data_entries, 1);

            let children = staging_children(&conn, 0).unwrap();
            assert_eq!(children.len(), 1);
            assert_eq!(children[0].name, "a.txt");
            assert_eq!(children[0].data_id, Some(0));

            let parts = staging_data_parts(&conn, 0).unwrap();
            assert_eq!(parts, vec![(0, 5)]);
        }

        #[test]
        fn build_staging_db_errors_on_an_unparseable_insert_for_a_relevant_table() {
            let temp_dir = tempfile::tempdir().unwrap();
            let result = build_staging_db(
                "INSERT INTO TREEENTRIES VALUES (this is not valid);",
                &temp_dir.path().join("staging.db"),
            );
            assert!(result.is_err());
        }

        #[test]
        fn staging_data_parts_filters_out_blacklisted_zero_size_parts() {
            let temp_dir = tempfile::tempdir().unwrap();
            let (conn, _) = build_staging_db(
                "INSERT INTO DATAENTRIES VALUES (0, 1, 5, 0, 0, NULL);",
                &temp_dir.path().join("staging.db"),
            )
            .unwrap();

            assert_eq!(
                staging_data_parts(&conn, 0).unwrap(),
                Vec::<(u64, u64)>::new()
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;

    /// A small, deterministic xorshift64 PRNG - just needs to produce
    /// varied, reproducible bytes (not cryptographic quality) so CDC chunk
    /// boundaries in test fixtures are stable across runs.
    fn pseudo_bytes(seed: u64, len: usize) -> Vec<u8> {
        let mut state = seed | 1;
        (0..len)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                (state & 0xff) as u8
            })
            .collect()
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    /// A hand-built fixture standing in for a real `fsc db-backup` export:
    /// an old `data/` directory with real bytes (written directly via
    /// `store::LongTermStore::write`, mirroring how other tests in this
    /// codebase build fake stores) plus a `.sql` script text in the same
    /// shape H2's `Script` tool produces (schema-qualified quoted
    /// identifiers, multi-row `VALUES`, `--` row-count comments, stray
    /// `CREATE`/`ALTER` noise our parser must ignore).
    ///
    /// Covers every scenario called for by the plan this implements: nested
    /// directories, a mix of active and soft-deleted entries, an empty file,
    /// two files (`big1.txt`/`big2.txt`) with a large shared prefix and
    /// suffix but a different middle section - different whole-file content
    /// (and thus, under Scala's MD5 whole-file dedup, no sharing at all) but
    /// overlapping CDC chunks - and a "blacklisted" entry (a `dataId` whose
    /// storage allocation was removed, `start == stop == 0`) that must be
    /// skipped rather than crash the migration.
    struct Fixture {
        _old_data_dir: tempfile::TempDir,
        old_data_path: PathBuf,
        _script_dir: tempfile::TempDir,
        script_path: PathBuf,
        readme_bytes: Vec<u8>,
        old_txt_bytes: Vec<u8>,
        big1: Vec<u8>,
        big2: Vec<u8>,
    }

    fn build_fixture() -> Fixture {
        let old_data_dir = tempfile::tempdir().unwrap();
        let old_data_path = old_data_dir.path().join("data");
        let old_store = store::LongTermStore::new(&old_data_path, false);

        let readme_bytes = b"Hello World!".to_vec();
        old_store.write(0, &readme_bytes).unwrap();

        let old_txt_bytes = pseudo_bytes(42, 20);
        old_store.write(100, &old_txt_bytes).unwrap();

        // A long shared prefix and suffix around a differing middle: under
        // Scala's whole-file MD5 dedup these two files share nothing (their
        // overall bytes differ), but under CDC the many chunks entirely
        // within the untouched prefix/suffix regions dedupe cleanly.
        let prefix = pseudo_bytes(1, 40_000);
        let suffix = pseudo_bytes(2, 40_000);
        let middle_a = pseudo_bytes(3, 20_000);
        let middle_b = pseudo_bytes(4, 20_000);
        let mut big1 = prefix.clone();
        big1.extend_from_slice(&middle_a);
        big1.extend_from_slice(&suffix);
        let mut big2 = prefix;
        big2.extend_from_slice(&middle_b);
        big2.extend_from_slice(&suffix);
        assert_ne!(big1, big2, "test setup: the two files must actually differ");
        old_store.write(1_000, &big1).unwrap();
        old_store.write(200_000, &big2).unwrap();

        let h = hex(&[0xABu8; 16]);
        let script = format!(
            "-- H2 2.3.232 (2024-08-25) SCRIPT export\n\
             CREATE USER IF NOT EXISTS \"SA\" SALT '00' HASH '00' ADMIN;\n\
             CREATE SEQUENCE \"PUBLIC\".\"IDSEQ\" START WITH 8 BELONGS_TO_TABLE;\n\
             CREATE CACHED TABLE \"PUBLIC\".\"CONTEXT\"(\n\
             \x20\x20\"KEY\" CHARACTER VARYING(255) NOT NULL,\n\
             \x20\x20\"VALUE\" CHARACTER VARYING(255) NOT NULL\n\
             );\n\
             -- 1 +/- SELECT COUNT(*) FROM PUBLIC.CONTEXT;\n\
             INSERT INTO \"PUBLIC\".\"CONTEXT\" VALUES\n\
             ('db version', '3');\n\
             CREATE CACHED TABLE \"PUBLIC\".\"TREEENTRIES\"(\n\
             \x20\x20\"ID\" BIGINT NOT NULL,\n\
             \x20\x20\"PARENTID\" BIGINT NOT NULL,\n\
             \x20\x20\"NAME\" CHARACTER VARYING(255) NOT NULL,\n\
             \x20\x20\"TIME\" BIGINT NOT NULL,\n\
             \x20\x20\"DELETED\" BIGINT NOT NULL,\n\
             \x20\x20\"DATAID\" BIGINT\n\
             );\n\
             -- 8 +/- SELECT COUNT(*) FROM PUBLIC.TREEENTRIES;\n\
             INSERT INTO \"PUBLIC\".\"TREEENTRIES\" VALUES\n\
             (0, 0, '', 500, 0, NULL),\n\
             (1, 0, 'docs', 1000, 0, NULL),\n\
             (2, 1, 'readme.txt', 1100, 0, 10),\n\
             (3, 1, 'old.txt', 900, 1500, 11),\n\
             (4, 0, 'empty.txt', 1200, 0, -1),\n\
             (5, 0, 'big1.txt', 1300, 0, 20),\n\
             (6, 0, 'big2.txt', 1300, 0, 21),\n\
             (7, 0, 'blacklisted.txt', 1400, 0, 99);\n\
             ALTER TABLE \"PUBLIC\".\"TREEENTRIES\" ADD CONSTRAINT \"PUBLIC\".\"PK_TREEENTRIES\" PRIMARY KEY(\"ID\");\n\
             CREATE CACHED TABLE \"PUBLIC\".\"DATAENTRIES\"(\n\
             \x20\x20\"ID\" BIGINT NOT NULL,\n\
             \x20\x20\"SEQ\" INTEGER NOT NULL,\n\
             \x20\x20\"LENGTH\" BIGINT,\n\
             \x20\x20\"START\" BIGINT NOT NULL,\n\
             \x20\x20\"STOP\" BIGINT NOT NULL,\n\
             \x20\x20\"HASH\" BINARY(16)\n\
             );\n\
             -- 5 +/- SELECT COUNT(*) FROM PUBLIC.DATAENTRIES;\n\
             INSERT INTO \"PUBLIC\".\"DATAENTRIES\" VALUES\n\
             (10, 1, 12, 0, 12, X'{h}'),\n\
             (11, 1, 20, 100, 120, X'{h}'),\n\
             (20, 1, 100000, 1000, 101000, X'{h}'),\n\
             (21, 1, 100000, 200000, 300000, X'{h}'),\n\
             (99, 1, 5000, 0, 0, NULL);\n\
             ALTER TABLE \"PUBLIC\".\"DATAENTRIES\" ADD CONSTRAINT \"PUBLIC\".\"PK_DATAENTRIES\" PRIMARY KEY(\"ID\", \"SEQ\");\n"
        );

        let script_dir = tempfile::tempdir().unwrap();
        let script_path = script_dir.path().join("export.sql");
        std::fs::write(&script_path, script).unwrap();

        Fixture {
            _old_data_dir: old_data_dir,
            old_data_path,
            _script_dir: script_dir,
            script_path,
            readme_bytes,
            old_txt_bytes,
            big1,
            big2,
        }
    }

    fn init_target_repo() -> (tempfile::TempDir, PathBuf) {
        let temp_dir = tempfile::tempdir().unwrap();
        let repo_root = temp_dir.path().join("repo");
        db::init_repository(
            &repo_root,
            // A small target chunk size so a ~100 KB fixture file reliably
            // spans many chunks within a fast test.
            &db::RepositorySettings::new(12, db::Chunking::Cdc).unwrap(),
        )
        .unwrap();
        (temp_dir, repo_root)
    }

    fn migrate_args(fixture: &Fixture) -> MigrateScalaRepoArgs {
        MigrateScalaRepoArgs {
            script: fixture.script_path.clone(),
            old_data: fixture.old_data_path.clone(),
            chunk_buffer_mb: DEFAULT_CHUNK_BUFFER_MB,
            allow_swap_risk: false,
        }
    }

    /// Reads a content's full bytes back from the target repository's data
    /// store (concatenating its chunks in order) and asserts they match
    /// `expected`.
    fn assert_content_bytes(conn: &Connection, repo: &Path, content_id: i64, expected: &[u8]) {
        let data_store = store::LongTermStore::new(repo.join("data"), true);
        let mut actual = Vec::new();
        for chunk in db::ordered_content_chunks(conn, content_id).unwrap() {
            let (bytes, integrity) = chunk_store::read_chunk_bytes(
                conn,
                &data_store,
                chunk.chunk_id,
                chunk.length as u64,
            )
            .unwrap();
            assert_eq!(integrity, store::ReadIntegrity::Complete);
            actual.extend(bytes);
        }
        assert_eq!(actual, expected);
    }

    #[test]
    fn migrates_a_full_scala_repository_end_to_end() {
        let fixture = build_fixture();
        let (_temp_dir, target_repo) = init_target_repo();

        let exit = run_migrate_scala_repo(&target_repo, migrate_args(&fixture));
        assert_eq!(exit, ExitCode::SUCCESS);

        let repository = db::open_repository(&target_repo).unwrap();
        let conn = repository.open_read_connection().unwrap();

        // Nested directory structure.
        let docs = db::resolve_path(&conn, "docs").unwrap().unwrap();
        assert_eq!(docs.kind, db::EntryKind::Dir);

        // An active file's content round-trips exactly.
        let readme = db::resolve_path(&conn, "docs/readme.txt").unwrap().unwrap();
        assert_content_bytes(
            &conn,
            &target_repo,
            readme.content_id.unwrap(),
            &fixture.readme_bytes,
        );

        // A soft-deleted entry is invisible to active-only path resolution,
        // but its row (and content) still exists - restore-from-history
        // capability is preserved, per this tool's "migrate everything"
        // design decision.
        assert_eq!(db::resolve_path(&conn, "docs/old.txt").unwrap(), None);
        let (deleted_at, old_content_id): (Option<i64>, Option<i64>) = conn
            .query_row(
                "SELECT deleted_at, content_id FROM tree_entries WHERE name = 'old.txt'",
                (),
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert!(deleted_at.is_some());
        assert_content_bytes(
            &conn,
            &target_repo,
            old_content_id.unwrap(),
            &fixture.old_txt_bytes,
        );

        // An empty file has no content row, same as a `store`-backed one.
        let empty = db::resolve_path(&conn, "empty.txt").unwrap().unwrap();
        assert_eq!(empty.content_id, None);

        // A "blacklisted" dataId (no recoverable bytes) is skipped entirely,
        // not inserted with bogus/empty content.
        assert_eq!(db::resolve_path(&conn, "blacklisted.txt").unwrap(), None);
        let blacklisted_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM tree_entries WHERE name = 'blacklisted.txt'",
                (),
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(blacklisted_count, 0);

        // The two large files: different whole-file content (so distinct
        // `contents` rows, exactly as Scala's own MD5 dedup would have kept
        // them separate too), but round-trip correctly and share at least
        // one chunk - the very dedup Scala's whole-file model could never
        // find.
        let big1 = db::resolve_path(&conn, "big1.txt").unwrap().unwrap();
        let big2 = db::resolve_path(&conn, "big2.txt").unwrap().unwrap();
        assert_content_bytes(&conn, &target_repo, big1.content_id.unwrap(), &fixture.big1);
        assert_content_bytes(&conn, &target_repo, big2.content_id.unwrap(), &fixture.big2);
        assert_ne!(
            big1.content_id, big2.content_id,
            "different overall bytes must still be distinct contents rows"
        );

        let chunks1: std::collections::HashSet<i64> =
            db::ordered_content_chunks(&conn, big1.content_id.unwrap())
                .unwrap()
                .into_iter()
                .map(|c| c.chunk_id)
                .collect();
        let chunks2: std::collections::HashSet<i64> =
            db::ordered_content_chunks(&conn, big2.content_id.unwrap())
                .unwrap()
                .into_iter()
                .map(|c| c.chunk_id)
                .collect();
        assert!(
            chunks1.intersection(&chunks2).count() > 0,
            "big1.txt/big2.txt share a long common prefix and suffix - chunk-level \
             dedup must find shared chunks between them despite their distinct \
             whole-file content"
        );

        // The physical storage actually written is well under the naive sum
        // of both files' sizes - the "space saved by chunk-level dedup"
        // this tool's summary reports on.
        let physical_bytes: i64 = conn
            .query_row(
                "SELECT COALESCE(MAX(stop), 0) FROM chunk_extents",
                (),
                |row| row.get(0),
            )
            .unwrap();
        assert!(
            (physical_bytes as usize) < fixture.big1.len() + fixture.big2.len(),
            "chunk-level dedup between big1.txt/big2.txt must save some physical space"
        );
    }

    #[test]
    fn reads_a_zipped_script_export_transparently() {
        let fixture = build_fixture();
        let zip_dir = tempfile::tempdir().unwrap();
        let zip_path = zip_dir.path().join("dedupfs-232_2024-01-01_backup.zip");
        {
            let file = File::create(&zip_path).unwrap();
            let mut writer = zip::ZipWriter::new(file);
            writer
                .start_file(
                    "dedupfs-232_2024-01-01_backup.sql",
                    zip::write::SimpleFileOptions::default(),
                )
                .unwrap();
            let script_bytes = std::fs::read(&fixture.script_path).unwrap();
            writer.write_all(&script_bytes).unwrap();
            writer.finish().unwrap();
        }

        let (_temp_dir, target_repo) = init_target_repo();
        let mut args = migrate_args(&fixture);
        args.script = zip_path;

        let exit = run_migrate_scala_repo(&target_repo, args);
        assert_eq!(exit, ExitCode::SUCCESS);

        let repository = db::open_repository(&target_repo).unwrap();
        let conn = repository.open_read_connection().unwrap();
        assert!(
            db::resolve_path(&conn, "docs/readme.txt")
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn refuses_to_migrate_into_a_non_empty_target_repository() {
        let fixture = build_fixture();
        let (_temp_dir, target_repo) = init_target_repo();
        {
            let repository = db::open_repository(&target_repo).unwrap();
            let conn = repository.open_write_connection().unwrap();
            db::insert_directory(&conn, 0, "preexisting", 0).unwrap();
        }

        let exit = run_migrate_scala_repo(&target_repo, migrate_args(&fixture));
        assert_eq!(exit, ExitCode::FAILURE);
    }

    #[test]
    fn fails_cleanly_for_an_uninitialized_target_repository() {
        let fixture = build_fixture();
        let temp_dir = tempfile::tempdir().unwrap();
        let target_repo = temp_dir.path().join("does-not-exist");

        let exit = run_migrate_scala_repo(&target_repo, migrate_args(&fixture));
        assert_eq!(exit, ExitCode::FAILURE);
    }

    #[test]
    fn fails_cleanly_for_a_missing_old_data_directory() {
        let fixture = build_fixture();
        let (_temp_dir, target_repo) = init_target_repo();
        let mut args = migrate_args(&fixture);
        args.old_data = fixture.old_data_path.join("does-not-exist");

        let exit = run_migrate_scala_repo(&target_repo, args);
        assert_eq!(exit, ExitCode::FAILURE);

        // Nothing must have been committed to the target repository.
        let repository = db::open_repository(&target_repo).unwrap();
        let conn = repository.open_read_connection().unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM tree_entries", (), |row| row.get(0))
            .unwrap();
        assert_eq!(count, 1, "only the root entry");
    }
}
