//! Phase 2 of the Scala-repository migration (DESIGN-MIGRATION-001 in
//! `docs/design/scala-migration-tool.md`): walks the staging tree built by `crate::scala_import`
//! and recreates it - live and soft-deleted entries alike (REQ-MIGRATION-001) - in every requested
//! destination, reading each distinct old content reference exactly once and feeding it to every
//! destination's own chunker in the same pass (REQ-MIGRATION-004, DESIGN-MIGRATION-003). A
//! migrated chunk's bytes are never written anywhere - they already exist at a known position in
//! the old, shared `data/` directory (REQ-MIGRATION-005) - only recorded there via
//! `db::Repository::register_existing_chunk` (DESIGN-MIGRATION-006). `db::Repository::
//! insert_migrated_entry` (DESIGN-MIGRATION-007) recreates each tree entry. What has been migrated
//! is recorded in progress tables inside each destination itself, written in the same batch
//! transaction as the entries they describe (DESIGN-MIGRATION-005) - so the walk is resumable and a
//! hard kill can never leave an entry without its record.
//!
//! The staging tree is walked, and the old bytes are read, on one thread. Everything that differs
//! per destination - chunking and hashing a read window, the `db` commits behind it, creating a
//! tree entry - runs on one scoped thread per target (see [`parallel_map`]), since the targets
//! share nothing but the read-only source bytes.

use std::fmt;
use std::io;
use std::thread;

use cdc::{Chunker, ChunkerConfig, ConfiguredChunker};
use rusqlite::Connection;

use crate::scala_import::{self, ImportError, StagingTreeEntry};
use crate::settle::HASH_WIDTH;

/// The source's own root id - confirmed against a real export: the root row is `(0, 0, '', ...)`,
/// its own parent. [`walk_children`] relies on this to skip the root's own row when it turns up as
/// a "child" of itself.
const OLD_ROOT_ID: i64 = 0;
/// Read window used to stream a distinct old content reference's bytes through every pending
/// target's own chunker - bounded so migrating a large file never needs its complete byte stream in
/// memory at once, the same reasoning as `crate::settle`'s own read window (a different constant
/// since the two are otherwise unrelated).
const READ_WINDOW: usize = 4 * 1024 * 1024;
/// A target's batch transaction is committed once it has run this many write operations - large
/// enough that commits stop mattering for speed, small enough that a failed run loses little and
/// the write-ahead log stays small.
const BATCH_OPS: u64 = 5000;
/// How many referencing paths an error or warning about missing old data names.
pub const PATHS_SHOWN: usize = 3;

/// What a migration run is told to do, beyond the targets themselves.
#[derive(Debug, Clone, Copy)]
pub struct Settings {
    /// Commit a target's batch once it has run this many write operations.
    pub batch_ops: u64,
    /// Continue past old data that is missing or too short, taking the missing bytes as zeros,
    /// instead of stopping (DESIGN-MIGRATION-008).
    pub tolerate_missing_data: bool,
}

impl Settings {
    pub fn new(tolerate_missing_data: bool) -> Self {
        Self {
            batch_ops: BATCH_OPS,
            tolerate_missing_data,
        }
    }
}

#[derive(Debug)]
pub enum MigrateContentError {
    Staging(ImportError),
    Db(db::Error),
    Read(io::Error),
    /// The old data store has missing or short bytes for this old `dataId`, and the caller did not
    /// choose to tolerate that (DESIGN-MIGRATION-008). Carries what an operator needs to decide
    /// between repairing the data and tolerating the gap.
    MissingOldData {
        data_id: i64,
        /// The backing files that were missing or too short, relative to `data/`.
        missing_files: Vec<String>,
        /// A few of the tree entries that use this content.
        used_by: Vec<String>,
    },
}

impl fmt::Display for MigrateContentError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MigrateContentError::Staging(err) => write!(f, "{err}"),
            MigrateContentError::Db(err) => write!(f, "{err}"),
            MigrateContentError::Read(err) => write!(f, "{err}"),
            MigrateContentError::MissingOldData {
                data_id,
                missing_files,
                used_by,
            } => write!(
                f,
                "old data for dataId {data_id} is missing or incomplete in the existing data/ \
                 directory (missing or too short: {}; used by: {}). Restore the missing data \
                 files and run the migration again, or pass --tolerate-missing-data to continue \
                 with zeros in place of the missing bytes",
                missing_files.join(", "),
                if used_by.is_empty() {
                    "an entry not found in the export".to_string()
                } else {
                    used_by.join(", ")
                }
            ),
        }
    }
}

impl std::error::Error for MigrateContentError {}

impl From<ImportError> for MigrateContentError {
    fn from(err: ImportError) -> Self {
        MigrateContentError::Staging(err)
    }
}

impl From<db::Error> for MigrateContentError {
    fn from(err: db::Error) -> Self {
        MigrateContentError::Db(err)
    }
}

/// One destination this migration writes into: its own repository, adopted against the shared
/// `data/` (see `crate::migrate_scala_repo`), with its migration progress tables prepared.
pub type Target<'a> = &'a db::Repository;

/// Runs `work` for every item on its own scoped thread and returns the results in item order. With
/// a single item it runs inline instead, so a one-target migration pays no thread overhead. Every
/// thread runs to completion even if another one fails - the caller reports the first error in item
/// order - and a panic on any thread is re-raised here.
///
/// One thread per call is only reasonable while the work per item is large next to a thread spawn
/// (a few dozen microseconds) - true for the per-entry `db` commits and per-window hashing this is
/// used for today.
fn parallel_map<T: Sync, R: Send>(items: &[T], work: impl Fn(usize, &T) -> R + Sync) -> Vec<R> {
    if let [only] = items {
        return vec![work(0, only)];
    }
    thread::scope(|scope| {
        let work = &work;
        let handles: Vec<_> = items
            .iter()
            .enumerate()
            .map(|(i, item)| scope.spawn(move || work(i, item)))
            .collect();
        handles.into_iter().map(join_or_resume).collect()
    })
}

/// [`parallel_map`] for work that needs `&mut` access to its item.
fn parallel_map_mut<T: Send, R: Send>(
    items: &mut [T],
    work: impl Fn(usize, &mut T) -> R + Sync,
) -> Vec<R> {
    if let [only] = items {
        return vec![work(0, only)];
    }
    thread::scope(|scope| {
        let work = &work;
        let handles: Vec<_> = items
            .iter_mut()
            .enumerate()
            .map(|(i, item)| scope.spawn(move || work(i, item)))
            .collect();
        handles.into_iter().map(join_or_resume).collect()
    })
}

fn join_or_resume<R>(handle: thread::ScopedJoinHandle<'_, R>) -> R {
    handle
        .join()
        .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
}

/// Counts of work actually performed by one [`migrate`] call - `0` for both on a fully-resumed run
/// that finds everything already migrated.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct MigrationStats {
    /// New `(tree entry, target)` pairs created - counted per target, since each target's own
    /// destination tree is independent.
    pub tree_entries_created: u64,
    /// Distinct old `dataId`s actually read and re-chunked this run (REQ-MIGRATION-004: one read
    /// regardless of how many targets still needed it) - not incremented for a `dataId` every
    /// target had already cached.
    pub contents_migrated: u64,
    /// Of those, how many had missing old bytes that were tolerated (DESIGN-MIGRATION-008).
    pub damaged_contents: u64,
}

/// Migrates the whole staging tree into every one of `targets`, each of which must already have its
/// migration progress tables prepared (`db::Repository::migration_prepare`).
///
/// Every target runs inside one batch transaction at a time (`db::Repository::migration_begin_batch`),
/// committed whenever it has grown past [`BATCH_OPS`] and once more at the end - always at a point
/// where every entry and content written so far has its progress record in the same batch. If the
/// migration fails, every target rolls back to its last commit, so what is on disk is always such a
/// consistent state and the next run resumes from it.
pub fn migrate(
    staging: &Connection,
    old_store: &store::ByteStore,
    targets: &[Target],
    settings: Settings,
) -> Result<MigrationStats, MigrateContentError> {
    migrate_in_batches(staging, old_store, targets, settings)
}

/// [`migrate`], kept as its own function so tests can hand in a tiny batch size.
fn migrate_in_batches(
    staging: &Connection,
    old_store: &store::ByteStore,
    targets: &[Target],
    settings: Settings,
) -> Result<MigrationStats, MigrateContentError> {
    let begun = parallel_map(targets, |_, repo| repo.migration_begin_batch());
    if let Some(err) = begun.into_iter().find_map(Result::err) {
        roll_back_all(targets);
        return Err(err.into());
    }
    let result = walk_and_commit(staging, old_store, targets, settings);
    if result.is_err() {
        roll_back_all(targets);
    }
    result
}

fn walk_and_commit(
    staging: &Connection,
    old_store: &store::ByteStore,
    targets: &[Target],
    settings: Settings,
) -> Result<MigrationStats, MigrateContentError> {
    let mut stats = MigrationStats::default();
    let new_root_ids = vec![0i64; targets.len()];
    walk_children(
        staging,
        old_store,
        targets,
        OLD_ROOT_ID,
        &new_root_ids,
        settings,
        &mut stats,
    )?;
    for result in parallel_map(targets, |_, repo| repo.migration_commit_batch()) {
        result?;
    }
    Ok(stats)
}

/// Best effort: a target whose batch is not open (already committed, or never begun) just reports
/// an error here, which there is nothing useful to do about.
fn roll_back_all(targets: &[Target]) {
    for result in parallel_map(targets, |_, repo| repo.migration_rollback_batch()) {
        let _ = result;
    }
}

/// Ends a target's current batch and starts the next one once it has grown past `batch_ops`.
/// Only called at points where everything written so far is consistent with its progress records.
fn commit_if_due(repo: &db::Repository, batch_ops: u64) -> Result<(), db::Error> {
    if repo.migration_batch_ops()? >= batch_ops {
        repo.migration_commit_batch()?;
        repo.migration_begin_batch()?;
    }
    Ok(())
}

/// Migrates every child of `old_parent_id`, recursing into each directory. `new_parent_ids` gives
/// each target's own new id for `old_parent_id` (index-aligned with `targets`) - every target
/// shares the root at id `0`, but diverges from there on, since each destination assigns its own
/// tree entry ids independently.
fn walk_children(
    staging: &Connection,
    old_store: &store::ByteStore,
    targets: &[Target],
    old_parent_id: i64,
    new_parent_ids: &[i64],
    settings: Settings,
    stats: &mut MigrationStats,
) -> Result<(), MigrateContentError> {
    for child in scala_import::staging_children(staging, old_parent_id)? {
        if child.id == OLD_ROOT_ID {
            // The root is its own parent in the source - never recreate it as a "child" of
            // itself.
            continue;
        }
        migrate_entry(
            staging,
            old_store,
            targets,
            &child,
            new_parent_ids,
            settings,
            stats,
        )?;
    }
    Ok(())
}

/// Migrates one old tree entry into every target (skipping any target that already has it,
/// resuming from a prior run), then recurses into its own children if it is a directory.
fn migrate_entry(
    staging: &Connection,
    old_store: &store::ByteStore,
    targets: &[Target],
    child: &StagingTreeEntry,
    new_parent_ids: &[i64],
    settings: Settings,
    stats: &mut MigrationStats,
) -> Result<(), MigrateContentError> {
    let content_ids: Vec<Option<i64>> = match child.data_id {
        None => vec![None; targets.len()],
        Some(data_id) => resolve_content(staging, old_store, targets, data_id, settings, stats)?
            .into_iter()
            .map(Some)
            .collect(),
    };

    let results = parallel_map(
        targets,
        |i, target| -> Result<(i64, bool), MigrateContentError> {
            match target.migration_migrated_id(child.id)? {
                Some(id) => Ok((id, false)),
                None => {
                    let id = target.insert_migrated_entry(
                        new_parent_ids[i],
                        &child.name,
                        child.time,
                        child.deleted_at,
                        content_ids[i],
                    )?;
                    target.migration_record_migrated(child.id, id)?;
                    commit_if_due(target, settings.batch_ops)?;
                    Ok((id, true))
                }
            }
        },
    );
    let mut new_ids = Vec::with_capacity(targets.len());
    for result in results {
        let (new_id, created) = result?;
        if created {
            stats.tree_entries_created += 1;
        }
        new_ids.push(new_id);
    }

    if child.data_id.is_none() {
        walk_children(
            staging, old_store, targets, child.id, &new_ids, settings, stats,
        )?;
    }
    Ok(())
}

/// Resolves `data_id` into every target's own `content_id`, reusing whichever targets' progress
/// records already have it cached and reading the old bytes at most once for the rest
/// (REQ-MIGRATION-004). `data_id == -1` (REQ-MIGRATION-001's "explicit zero-length file") needs no
/// read at all - it dedups the same way any other zero-length content would.
fn resolve_content(
    staging: &Connection,
    old_store: &store::ByteStore,
    targets: &[Target],
    data_id: i64,
    settings: Settings,
    stats: &mut MigrationStats,
) -> Result<Vec<i64>, MigrateContentError> {
    let mut content_ids: Vec<Option<i64>> = Vec::with_capacity(targets.len());
    let mut pending = Vec::new();
    for (i, target) in targets.iter().enumerate() {
        match target.migration_cached_content(data_id)? {
            Some(id) => content_ids.push(Some(id)),
            None => {
                content_ids.push(None);
                pending.push(i);
            }
        }
    }
    if pending.is_empty() {
        return Ok(content_ids
            .into_iter()
            .map(|id| id.expect("every index above was just matched to Some"))
            .collect());
    }
    stats.contents_migrated += 1;

    if data_id == -1 {
        for &i in &pending {
            let content_id = empty_content_id(targets[i])?;
            targets[i].migration_record_content(data_id, content_id)?;
            content_ids[i] = Some(content_id);
        }
        return Ok(content_ids
            .into_iter()
            .map(|id| id.expect("every pending index was just resolved above"))
            .collect());
    }

    let parts = scala_import::staging_data_parts(staging, data_id)?;
    let total_len: u64 = parts.iter().map(|&(start, stop)| stop - start).sum();

    let mut settlers: Vec<(usize, MigrationSettler)> = pending
        .iter()
        .map(|&i| (i, MigrationSettler::new(targets[i], &parts, data_id)))
        .collect();

    let mut missing_files: Vec<String> = Vec::new();
    let mut read_buf = vec![0u8; READ_WINDOW];
    for &(start, stop) in &parts {
        let mut pos = start;
        while pos < stop {
            let n = ((stop - pos) as usize).min(READ_WINDOW);
            let integrity = old_store
                .read(pos, &mut read_buf[..n])
                .map_err(MigrateContentError::Read)?;
            // `read` has already zero-filled whatever was missing, so the window can be used as it
            // is - if the caller chose to tolerate that.
            let incomplete =
                if let store::ReadIntegrity::Incomplete { missing_or_short } = integrity {
                    let files = missing_or_short
                        .iter()
                        .map(|path| format!("data/{}", path.to_string_lossy().replace('\\', "/")));
                    if !settings.tolerate_missing_data {
                        return Err(MigrateContentError::MissingOldData {
                            data_id,
                            missing_files: files.collect(),
                            used_by: scala_import::staging_paths_for_data_id(
                                staging,
                                data_id,
                                PATHS_SHOWN,
                            )?,
                        });
                    }
                    for file in files {
                        if !missing_files.contains(&file) {
                            missing_files.push(file);
                        }
                    }
                    true
                } else {
                    false
                };
            let window = &read_buf[..n];
            let fed = parallel_map_mut(
                &mut settlers,
                |_, (_, settler)| -> Result<(), MigrateContentError> {
                    settler.feed(window, incomplete)?;
                    // Only chunks are pending here, and re-registering a chunk after a resume finds it
                    // again by hash - so committing in the middle of a long file is safe.
                    commit_if_due(settler.repo, settings.batch_ops)?;
                    Ok(())
                },
            );
            for result in fed {
                result?;
            }
            pos += n as u64;
        }
    }

    let damaged_detail = (!missing_files.is_empty()).then(|| missing_files.join(", "));
    if let Some(detail) = &damaged_detail {
        stats.damaged_contents += 1;
        let used_by = scala_import::staging_paths_for_data_id(staging, data_id, PATHS_SHOWN)?;
        eprintln!(
            "warning: old data for dataId {data_id} is missing or incomplete ({detail}; used by: \
             {}) - continuing with zeros in place of the missing bytes",
            used_by.join(", ")
        );
    }

    // Most old files are smaller than one chunk, so for them all of the real per-target work -
    // hashing the one chunk, its `db` commits, the content row - happens here, not in the feed loop
    // above.
    let finished = parallel_map_mut(
        &mut settlers,
        |_, (i, settler)| -> Result<i64, MigrateContentError> {
            let content_id = settler.finish(total_len)?;
            targets[*i].migration_record_content(data_id, content_id)?;
            if let Some(detail) = &damaged_detail {
                targets[*i].migration_record_damaged(data_id, detail)?;
            }
            commit_if_due(targets[*i], settings.batch_ops)?;
            Ok(content_id)
        },
    );
    for ((i, _), result) in settlers.iter().zip(finished) {
        content_ids[*i] = Some(result?);
    }
    Ok(content_ids
        .into_iter()
        .map(|id| id.expect("every index was either already cached or just resolved above"))
        .collect())
}

/// The `content_id` a genuinely zero-length file always dedups onto - the same content hash
/// `crate::settle::settle` would compute for a zero-length file (a `blake3::Hasher` that was never
/// fed anything), without needing that function's own windowed-read/store machinery, which a
/// zero-length file never actually exercises.
fn empty_content_id(repo: &db::Repository) -> Result<i64, MigrateContentError> {
    let mut hash = [0u8; HASH_WIDTH];
    blake3::Hasher::new().finalize_xof().fill(&mut hash);
    Ok(repo.find_or_create_content(0, &hash, &[])?)
}

/// Translates a logical byte range `[logical_start, logical_end)` - within the concatenation of
/// `parts` in order - into the corresponding absolute byte extents in the old data store: one
/// `(start, stop)` pair per `parts` entry the range overlaps, in order. A chunk usually maps to
/// exactly one extent, but can straddle a `parts` boundary (the source's own storage for one file
/// is not always contiguous - see `crate::scala_import::staging_data_parts`), in which case it maps
/// to more than one.
fn map_to_old_store_extents(
    parts: &[(u64, u64)],
    logical_start: u64,
    logical_end: u64,
) -> Vec<(u64, u64)> {
    let mut extents = Vec::new();
    let mut part_logical_start = 0u64;
    for &(start, stop) in parts {
        let part_logical_end = part_logical_start + (stop - start);
        let overlap_start = logical_start.max(part_logical_start);
        let overlap_end = logical_end.min(part_logical_end);
        if overlap_start < overlap_end {
            let offset = overlap_start - part_logical_start;
            let len = overlap_end - overlap_start;
            extents.push((start + offset, start + offset + len));
        }
        part_logical_start = part_logical_end;
        if part_logical_start >= logical_end {
            break;
        }
    }
    extents
}

/// The hash recorded for a chunk that overlaps missing old data (DESIGN-MIGRATION-008). The plain
/// hash of the zero-filled bytes must never be used: it would let a later write of real all-zero
/// content of the same length dedup onto this chunk, and would then serve different bytes once the
/// missing data is restored. This value is unique per `(dataId, position, length)`, so it never
/// matches real content, and it is the same on every run, so a resume finds the chunk again.
fn missing_data_marker_hash(data_id: i64, chunk_start: u64, length: u64) -> [u8; HASH_WIDTH] {
    let mut hasher =
        blake3::Hasher::new_derive_key("dedupfs scala migration: chunk over missing old data");
    hasher.update(&data_id.to_le_bytes());
    hasher.update(&chunk_start.to_le_bytes());
    hasher.update(&length.to_le_bytes());
    let mut hash = [0u8; HASH_WIDTH];
    hasher.finalize_xof().fill(&mut hash);
    hash
}

/// Streaming chunker/hasher state for re-chunking one distinct old content reference into one
/// target - one instance per `(data_id, target)` pair still needing work, fed the same bytes as
/// every other pending target's own instance in lockstep, each producing its own chunk boundaries
/// since each target's own `--cdc-target-size-bits` differs.
struct MigrationSettler<'a> {
    repo: &'a db::Repository,
    parts: &'a [(u64, u64)],
    data_id: i64,
    chunker: ConfiguredChunker,
    chunk_buffer: Vec<u8>,
    chunk_ids: Vec<i64>,
    content_hasher: blake3::Hasher,
    /// This settler's own logical position, within the concatenation of `parts`, of the end of the
    /// last chunk it has completed so far - see `map_to_old_store_extents`.
    chunk_boundary: u64,
    /// How many bytes have been fed so far, in the same logical coordinates.
    fed_position: u64,
    /// The logical ranges of the fed windows that were zero-filled for missing old data. A chunk
    /// overlapping any of them gets [`missing_data_marker_hash`] instead of its real hash.
    missing_ranges: Vec<(u64, u64)>,
}

impl<'a> MigrationSettler<'a> {
    fn new(repo: &'a db::Repository, parts: &'a [(u64, u64)], data_id: i64) -> Self {
        let bits = repo.settings().cdc_target_size_bits();
        let config = ChunkerConfig::new(Some(bits))
            .expect("cdc_target_size_bits was already validated when this repository was adopted");
        Self {
            repo,
            parts,
            data_id,
            chunker: config.chunker(),
            chunk_buffer: Vec::new(),
            chunk_ids: Vec::new(),
            content_hasher: blake3::Hasher::new(),
            chunk_boundary: 0,
            fed_position: 0,
            missing_ranges: Vec::new(),
        }
    }

    /// Feeds one window. `incomplete` says that the window contains zero-filled stand-ins for
    /// missing old bytes - chunk boundaries are then found on those zeros, but the chunks they end
    /// up in are marked, see [`missing_data_marker_hash`].
    fn feed(&mut self, data: &[u8], incomplete: bool) -> Result<(), MigrateContentError> {
        if incomplete {
            self.missing_ranges
                .push((self.fed_position, self.fed_position + data.len() as u64));
        }
        let mut bytes_into_chunk = self.chunker.bytes_into_chunk();
        let lengths = self.chunker.next(data);
        let mut rest = data;
        for length in lengths {
            let end_in_rest = (length - bytes_into_chunk) as usize;
            self.chunk_buffer.extend_from_slice(&rest[..end_in_rest]);
            rest = &rest[end_in_rest..];
            self.complete_chunk()?;
            bytes_into_chunk = 0;
        }
        self.chunk_buffer.extend_from_slice(rest);
        self.fed_position += data.len() as u64;
        Ok(())
    }

    fn complete_chunk(&mut self) -> Result<(), MigrateContentError> {
        let length = self.chunk_buffer.len() as i64;
        let chunk_start = self.chunk_boundary;
        let chunk_end = chunk_start + length as u64;
        self.chunk_boundary = chunk_end;

        let over_missing_data = self
            .missing_ranges
            .iter()
            .any(|&(start, end)| chunk_start < end && start < chunk_end);
        let chunk_hash = if over_missing_data {
            missing_data_marker_hash(self.data_id, chunk_start, length as u64)
        } else {
            let hash = blake3::hash(&self.chunk_buffer);
            let mut chunk_hash = [0u8; HASH_WIDTH];
            chunk_hash.copy_from_slice(&hash.as_bytes()[..HASH_WIDTH]);
            chunk_hash
        };

        let chunk_id = match self.repo.find_chunk(length, &chunk_hash)? {
            Some(id) => id,
            None => {
                let extents = map_to_old_store_extents(self.parts, chunk_start, chunk_end);
                self.repo
                    .register_existing_chunk(length, &chunk_hash, &extents)?
            }
        };
        self.chunk_ids.push(chunk_id);
        self.content_hasher.update(&(length as u64).to_le_bytes());
        self.content_hasher.update(&chunk_hash);
        self.chunk_buffer.clear();
        Ok(())
    }

    fn finish(&mut self, total_len: u64) -> Result<i64, MigrateContentError> {
        if let Some(length) = self.chunker.flush() {
            debug_assert_eq!(
                length as usize,
                self.chunk_buffer.len(),
                "the chunker's own reported final-chunk length must match what was buffered for it"
            );
            self.complete_chunk()?;
        }
        let mut content_hash = [0u8; HASH_WIDTH];
        self.content_hasher.finalize_xof().fill(&mut content_hash);
        Ok(self
            .repo
            .find_or_create_content(total_len as i64, &content_hash, &self.chunk_ids)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn map_to_old_store_extents_maps_a_range_entirely_within_one_part() {
        let parts = [(1000, 1100)];
        assert_eq!(map_to_old_store_extents(&parts, 10, 30), vec![(1010, 1030)]);
    }

    #[test]
    fn map_to_old_store_extents_maps_a_range_spanning_the_whole_of_several_parts() {
        let parts = [(1000, 1010), (2000, 2010)];
        assert_eq!(
            map_to_old_store_extents(&parts, 0, 20),
            vec![(1000, 1010), (2000, 2010)]
        );
    }

    #[test]
    fn map_to_old_store_extents_splits_a_range_straddling_a_part_boundary() {
        let parts = [(1000, 1010), (2000, 2020)];
        // Logical range [5, 15): 5 bytes from the end of part 0, 5 bytes from the start of part 1.
        assert_eq!(
            map_to_old_store_extents(&parts, 5, 15),
            vec![(1005, 1010), (2000, 2005)]
        );
    }

    #[test]
    fn map_to_old_store_extents_stops_once_the_logical_end_is_covered() {
        let parts = [(1000, 1010), (2000, 2010), (3000, 3010)];
        // Entirely within part 0 - must not even look at parts 1/2 (harmless here since the
        // function is pure, but documents the early-exit this test's name promises).
        assert_eq!(map_to_old_store_extents(&parts, 0, 10), vec![(1000, 1010)]);
    }

    /// A subdirectory holding a live file with real content (`dataId` 100), an explicit
    /// zero-length file, and a soft-deleted file sharing the *same* old `dataId` as the live one -
    /// the source's own whole-file dedup, which this migration turns into ordinary content-level
    /// dedup on the new side.
    const SCRIPT: &str = r#"
CREATE CACHED TABLE "PUBLIC"."TREEENTRIES"(
    "ID" BIGINT NOT NULL, "PARENTID" BIGINT NOT NULL, "NAME" CHARACTER VARYING(255) NOT NULL,
    "TIME" BIGINT NOT NULL, "DELETED" BIGINT DEFAULT 0 NOT NULL, "DATAID" BIGINT DEFAULT NULL
);
INSERT INTO "PUBLIC"."TREEENTRIES" VALUES
(0, 0, '', 1000, 0, NULL),
(1, 0, 'dir', 1001, 0, NULL),
(2, 1, 'file.txt', 1002, 0, 100),
(3, 1, 'empty.txt', 1003, 0, -1),
(4, 1, 'gone.txt', 1004, 2000, 100);
CREATE CACHED TABLE "PUBLIC"."DATAENTRIES"(
    "ID" BIGINT NOT NULL, "SEQ" INTEGER NOT NULL, "LENGTH" BIGINT, "START" BIGINT NOT NULL,
    "STOP" BIGINT NOT NULL, "HASH" BINARY(16)
);
INSERT INTO "PUBLIC"."DATAENTRIES" VALUES
(100, 1, 11, 0, 11, X'00');
"#;
    const CONTENT_BYTES: &[u8] = b"hello world";

    /// Builds the staging database from [`SCRIPT`] plus an old store already holding
    /// [`CONTENT_BYTES`] at the exact position `dataId` 100 points at.
    fn build_source(dir: &tempfile::TempDir) -> (Connection, store::ByteStore) {
        let staging_path = dir.path().join("staging.db");
        scala_import::import(SCRIPT, &staging_path).unwrap();
        let staging = scala_import::open(&staging_path).unwrap();

        let old_store = store::ByteStore::new(dir.path().join("old-data"), false);
        old_store.write(0, CONTENT_BYTES).unwrap();
        (staging, old_store)
    }

    fn new_repo(dir: &tempfile::TempDir, name: &str, bits: u32) -> db::Repository {
        let repo_root = dir.path().join(name);
        db::init_repository(
            &repo_root,
            db::RepositorySettings::new(bits, 1_700_000_000_000),
        )
        .unwrap();
        let repo = db::open_repository(&repo_root).unwrap();
        repo.migration_prepare().unwrap();
        repo
    }

    #[test]
    fn migrate_recreates_the_tree_and_content_across_a_directory_and_a_soft_delete() {
        let dir = tempfile::tempdir().unwrap();
        let (staging, old_store) = build_source(&dir);
        let repo = new_repo(&dir, "dest", 12);
        let targets = [&repo];

        let stats = migrate(&staging, &old_store, &targets, Settings::new(false)).unwrap();
        assert_eq!(
            stats.tree_entries_created, 4,
            "dir, file.txt, empty.txt, gone.txt"
        );
        assert_eq!(
            stats.contents_migrated, 2,
            "dataId 100 once, dataId -1 once"
        );

        let file_entry = repo.resolve_path("/dir/file.txt").unwrap().unwrap();
        let extents = repo
            .resolve_extents(file_entry.content_id.unwrap())
            .unwrap();
        assert_eq!(
            extents.len(),
            1,
            "small contiguous content should be one extent"
        );
        let mut buf = vec![0u8; CONTENT_BYTES.len()];
        old_store.read(extents[0].0, &mut buf).unwrap();
        assert_eq!(buf, CONTENT_BYTES);

        let empty_entry = repo.resolve_path("/dir/empty.txt").unwrap().unwrap();
        assert_eq!(
            repo.resolve_extents(empty_entry.content_id.unwrap())
                .unwrap(),
            Vec::new()
        );

        // gone.txt was soft-deleted from the moment of creation (old deleted = 2000) - must not
        // resolve as live...
        assert!(repo.resolve_path("/dir/gone.txt").unwrap().is_none());
        // ...but its history survives, sharing file.txt's own content_id.
        let dir_entry = repo.resolve_path("/dir").unwrap().unwrap();
        let deleted = repo.list_deleted_children(dir_entry.id).unwrap();
        let (_, gone) = deleted.iter().find(|(name, _)| name == "gone.txt").unwrap();
        assert_eq!(gone.deleted_at, 2000);
        assert_eq!(gone.entry.content_id, file_entry.content_id);
    }

    /// The regression this batch design exists for: a migration that fails partway (standing in for
    /// a kill or a power loss - what is on disk afterwards is whatever was last committed) must leave
    /// every migrated entry and its progress record together, or neither - and resuming must then
    /// finish the job without duplicating or failing on anything already there.
    #[test]
    fn a_failed_migration_leaves_entries_and_progress_records_consistent_and_resumes_cleanly() {
        // Batch sizes from "commit after every entry" to "commit rarely", so both a commit landing
        // right between an entry's two writes (if that were possible) and a lost uncommitted tail get
        // exercised.
        for batch_ops in [1u64, 2, 3, 5] {
            let dir = tempfile::tempdir().unwrap();
            let old_store = store::ByteStore::new(dir.path().join("old-data"), false);
            // Six readable files (dataIds 11-16) and a seventh (dataId 17) whose bytes do not exist
            // yet, so the run fails when it reaches it, after several batches were committed.
            let mut tree = String::from("(0, 0, '', 1000, 0, NULL), (1, 0, 'd', 1001, 0, NULL)");
            let mut data = String::new();
            for k in 1..=7i64 {
                tree.push_str(&format!(
                    ", ({}, 1, 'f{k}', {}, 0, {})",
                    k + 1,
                    1001 + k,
                    10 + k
                ));
                data.push_str(&format!(
                    "{}({}, 1, 10, {}, {}, X'00')",
                    if k == 1 { "" } else { ", " },
                    10 + k,
                    k * 100,
                    k * 100 + 10
                ));
                if k < 7 {
                    old_store
                        .write(k as u64 * 100, format!("content-{}", 10 + k).as_bytes())
                        .unwrap();
                }
            }
            let script = format!(
                "INSERT INTO \"PUBLIC\".\"TREEENTRIES\" VALUES {tree};\n\
                 INSERT INTO \"PUBLIC\".\"DATAENTRIES\" VALUES {data};"
            );
            let staging_path = dir.path().join("staging.db");
            scala_import::import(&script, &staging_path).unwrap();
            let staging = scala_import::open(&staging_path).unwrap();

            let repo = new_repo(&dir, "dest", 12);
            let targets = [&repo];
            let err = migrate_in_batches(
                &staging,
                &old_store,
                &targets,
                Settings {
                    batch_ops,
                    tolerate_missing_data: false,
                },
            )
            .expect_err("the seventh file's bytes are missing");
            assert!(
                matches!(err, MigrateContentError::MissingOldData { data_id: 17, .. }),
                "batch size {batch_ops}: {err}"
            );

            let path_of = |old_id: i64| match old_id {
                1 => "/d".to_string(),
                n => format!("/d/f{}", n - 1),
            };
            let mut already_migrated = 0;
            for old_id in 1..=8 {
                let record = repo.migration_migrated_id(old_id).unwrap();
                let entry = repo.resolve_path(&path_of(old_id)).unwrap().map(|e| e.id);
                assert_eq!(
                    record, entry,
                    "batch size {batch_ops}, old entry {old_id}: record and entry must agree"
                );
                already_migrated += i64::from(record.is_some());
            }
            assert!(
                (2..8).contains(&already_migrated),
                "batch size {batch_ops}: expected a partly migrated state, got {already_migrated}"
            );

            old_store.write(700, b"content-17").unwrap();
            let stats = migrate_in_batches(
                &staging,
                &old_store,
                &targets,
                Settings {
                    batch_ops,
                    tolerate_missing_data: false,
                },
            )
            .unwrap_or_else(|err| panic!("batch size {batch_ops}: resume failed: {err}"));
            assert_eq!(
                stats.tree_entries_created,
                8 - already_migrated as u64,
                "batch size {batch_ops}: resume must create exactly what was missing"
            );
            let dir_id = repo.resolve_path("/d").unwrap().unwrap().id;
            assert_eq!(repo.list_children(dir_id).unwrap().len(), 7);
            for k in 1..=7 {
                assert_eq!(
                    read_back(&repo, &old_store, &format!("/d/f{k}")),
                    format!("content-{}", 10 + k).into_bytes(),
                    "batch size {batch_ops}, file f{k}"
                );
            }
        }
    }

    /// A file whose second part lies in a backing data file that does not exist (the store's first
    /// data file holds 100 MB, so position 250,000,000 is in the third), next to an intact file.
    /// Returns the staging database, the old store, and the bytes of the intact first part.
    fn source_with_a_missing_data_file(
        dir: &tempfile::TempDir,
    ) -> (Connection, store::ByteStore, Vec<u8>, Vec<u8>) {
        let intact = pseudo_random_bytes(2_000_000, 3);
        let gap_part_one = pseudo_random_bytes(2_000_000, 4);
        let old_store = store::ByteStore::new(dir.path().join("old-data"), false);
        old_store.write(0, &intact).unwrap();
        old_store.write(3_000_000, &gap_part_one).unwrap();

        let script = "INSERT INTO \"PUBLIC\".\"TREEENTRIES\" VALUES \
                      (0, 0, '', 1000, 0, NULL), (1, 0, 'ok.bin', 1001, 0, 20), \
                      (2, 0, 'gap.bin', 1002, 0, 21);\n\
                      INSERT INTO \"PUBLIC\".\"DATAENTRIES\" VALUES \
                      (20, 1, 2000000, 0, 2000000, X'00'), \
                      (21, 1, 3000000, 3000000, 5000000, X'00'), \
                      (21, 2, NULL, 250000000, 251000000, NULL);";
        let staging_path = dir.path().join("staging.db");
        scala_import::import(script, &staging_path).unwrap();
        (
            scala_import::open(&staging_path).unwrap(),
            old_store,
            intact,
            gap_part_one,
        )
    }

    #[test]
    fn missing_old_data_stops_the_migration_with_a_helpful_error_and_a_restart_stops_again() {
        let dir = tempfile::tempdir().unwrap();
        let (staging, old_store, _, _) = source_with_a_missing_data_file(&dir);
        let repo = new_repo(&dir, "dest", 14);
        let targets = [&repo];

        for attempt in 1..=2 {
            let err = migrate(&staging, &old_store, &targets, Settings::new(false))
                .expect_err("the second part of gap.bin is in a data file that does not exist");
            let MigrateContentError::MissingOldData {
                data_id,
                missing_files,
                used_by,
            } = &err
            else {
                panic!("attempt {attempt}: unexpected error {err}");
            };
            assert_eq!(*data_id, 21, "attempt {attempt}");
            assert!(
                !missing_files.is_empty() && missing_files.iter().all(|f| f.starts_with("data/")),
                "attempt {attempt}: {missing_files:?}"
            );
            assert_eq!(used_by, &vec!["/gap.bin".to_string()], "attempt {attempt}");
            let message = err.to_string();
            assert!(
                message.contains("--tolerate-missing-data") && message.contains("/gap.bin"),
                "attempt {attempt}: {message}"
            );
        }
        assert!(
            repo.resolve_path("/gap.bin").unwrap().is_none(),
            "nothing of the failed content may have been recorded"
        );
    }

    #[test]
    fn tolerated_missing_data_is_marked_never_hashed_as_zeros_and_listed() {
        let dir = tempfile::tempdir().unwrap();
        let (staging, old_store, intact, gap_part_one) = source_with_a_missing_data_file(&dir);
        let repos = [new_repo(&dir, "dest-14", 14), new_repo(&dir, "dest-16", 16)];
        let targets: Vec<Target> = repos.iter().collect();

        let stats = migrate(&staging, &old_store, &targets, Settings::new(true)).unwrap();
        assert_eq!(
            stats.damaged_contents, 1,
            "one content, however many targets"
        );

        // The bytes as anything reading through the store sees them: zeros where data is missing.
        let expected_gap: Vec<u8> = gap_part_one
            .iter()
            .copied()
            .chain(std::iter::repeat_n(0u8, 1_000_000))
            .collect();
        for repo in &repos {
            let listed = repo.migration_damaged().unwrap();
            assert_eq!(listed.len(), 1);
            assert_eq!(listed[0].0, 21);
            assert!(listed[0].1.contains("data/"), "{}", listed[0].1);

            assert!(read_back(repo, &old_store, "/ok.bin") == intact);
            assert!(read_back(repo, &old_store, "/gap.bin") == expected_gap);

            let content_id = repo
                .resolve_path("/gap.bin")
                .unwrap()
                .unwrap()
                .content_id
                .unwrap();
            let (mut clean, mut marked) = (0, 0);
            for chunk in repo.resolve_chunks(content_id).unwrap() {
                let mut bytes = Vec::new();
                for &(start, stop) in &chunk.extents {
                    let mut buf = vec![0u8; (stop - start) as usize];
                    old_store.read(start, &mut buf).unwrap();
                    bytes.extend(buf);
                }
                let real_hash = blake3::hash(&bytes).as_bytes()[..HASH_WIDTH].to_vec();
                if chunk.extents.iter().any(|&(start, _)| start >= 250_000_000) {
                    marked += 1;
                    assert_ne!(
                        chunk.hash, real_hash,
                        "a chunk over missing data must not carry the hash of its zero-filled bytes"
                    );
                    assert_eq!(
                        repo.find_chunk(chunk.length as i64, &real_hash).unwrap(),
                        None,
                        "real zero-filled content must not find this chunk"
                    );
                } else {
                    clean += 1;
                    assert_eq!(chunk.hash, real_hash, "intact chunks keep their real hash");
                }
            }
            assert!(clean > 0 && marked > 0, "clean {clean}, marked {marked}");
        }
    }

    #[test]
    fn migrate_run_twice_creates_nothing_new_the_second_time() {
        let dir = tempfile::tempdir().unwrap();
        let (staging, old_store) = build_source(&dir);
        let repo = new_repo(&dir, "dest", 12);
        let targets = [&repo];

        let first = migrate(&staging, &old_store, &targets, Settings::new(false)).unwrap();
        assert!(first.tree_entries_created > 0);

        let second = migrate(&staging, &old_store, &targets, Settings::new(false)).unwrap();
        assert_eq!(second, MigrationStats::default());
    }

    #[test]
    fn migrate_reads_content_shared_across_targets_only_once() {
        let dir = tempfile::tempdir().unwrap();
        let (staging, old_store) = build_source(&dir);
        let repo_a = new_repo(&dir, "dest-a", 12);
        let repo_b = new_repo(&dir, "dest-b", 16);
        let targets = [&repo_a, &repo_b];

        let stats = migrate(&staging, &old_store, &targets, Settings::new(false)).unwrap();
        // Still exactly one read of dataId 100 (plus one for the -1 empty-file case), even though
        // two targets both needed it (REQ-MIGRATION-004).
        assert_eq!(stats.contents_migrated, 2);

        for repo in [&repo_a, &repo_b] {
            let file_entry = repo.resolve_path("/dir/file.txt").unwrap().unwrap();
            let extents = repo
                .resolve_extents(file_entry.content_id.unwrap())
                .unwrap();
            let mut buf = vec![0u8; CONTENT_BYTES.len()];
            old_store.read(extents[0].0, &mut buf).unwrap();
            assert_eq!(buf, CONTENT_BYTES);
        }
    }

    /// Deterministic, high-entropy filler (xorshift) - content-defined chunking only finds
    /// boundaries in data with some entropy, unlike a repeated byte.
    fn pseudo_random_bytes(len: usize, seed: u64) -> Vec<u8> {
        let mut state = seed;
        (0..len)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                (state >> 24) as u8
            })
            .collect()
    }

    /// Reads a migrated file's whole content back through its destination's own recorded extents,
    /// straight from the old store the extents point into.
    fn read_back(repo: &db::Repository, old_store: &store::ByteStore, path: &str) -> Vec<u8> {
        let entry = repo.resolve_path(path).unwrap().unwrap();
        let mut content = Vec::new();
        for (start, stop) in repo.resolve_extents(entry.content_id.unwrap()).unwrap() {
            let mut buf = vec![0u8; (stop - start) as usize];
            old_store.read(start, &mut buf).unwrap();
            content.extend(buf);
        }
        content
    }

    #[test]
    fn migrate_reassembles_a_multi_part_multi_window_file_identically_in_every_target() {
        let dir = tempfile::tempdir().unwrap();
        // Two separate, non-contiguous parts in the old store, together larger than two read
        // windows, so both the per-window fan-out and a chunk straddling the part boundary get
        // exercised for real (unlike the tiny single-chunk file every other test uses).
        let part_one = pseudo_random_bytes(5_000_000, 1);
        let part_two = pseudo_random_bytes(4_500_000, 2);
        let old_store = store::ByteStore::new(dir.path().join("old-data"), false);
        old_store.write(1_000_000, &part_one).unwrap();
        old_store.write(20_000_000, &part_two).unwrap();
        let expected: Vec<u8> = part_one.iter().chain(&part_two).copied().collect();

        let script = "INSERT INTO \"PUBLIC\".\"TREEENTRIES\" VALUES \
                      (0, 0, '', 1000, 0, NULL), (1, 0, 'big.bin', 1001, 0, 7);\n\
                      INSERT INTO \"PUBLIC\".\"DATAENTRIES\" VALUES \
                      (7, 1, 9500000, 1000000, 6000000, X'00'), \
                      (7, 2, NULL, 20000000, 24500000, NULL);";
        let staging_path = dir.path().join("staging.db");
        scala_import::import(script, &staging_path).unwrap();
        let staging = scala_import::open(&staging_path).unwrap();

        let repos = [
            new_repo(&dir, "dest-14", 14),
            new_repo(&dir, "dest-16", 16),
            new_repo(&dir, "dest-18", 18),
        ];
        let targets: Vec<Target> = repos.iter().collect();

        let stats = migrate(&staging, &old_store, &targets, Settings::new(false)).unwrap();
        assert_eq!(stats.contents_migrated, 1);

        let mut chunk_counts = Vec::new();
        for repo in &repos {
            let content = read_back(repo, &old_store, "/big.bin");
            assert!(
                content == expected,
                "migrated content differs from the original bytes"
            );
            let content_id = repo
                .resolve_path("/big.bin")
                .unwrap()
                .unwrap()
                .content_id
                .unwrap();
            chunk_counts.push(repo.resolve_chunks(content_id).unwrap().len());
        }
        // Each target really chunked on its own target size: finer targets produce more chunks.
        assert!(
            chunk_counts[0] > chunk_counts[1] && chunk_counts[1] > chunk_counts[2],
            "expected strictly fewer chunks as the target size grows, got {chunk_counts:?}"
        );
        assert!(chunk_counts[2] > 5, "got {chunk_counts:?}");
    }
}
