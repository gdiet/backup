//! Phase 2 of the Scala-repository migration (DESIGN-MIGRATION-001 in
//! `docs/design/scala-migration-tool.md`): walks the staging tree built by `crate::scala_import`
//! and recreates it - live and soft-deleted entries alike (REQ-MIGRATION-001) - in every requested
//! destination, reading each distinct old content reference exactly once and feeding it to every
//! destination's own chunker in the same pass (REQ-MIGRATION-004, DESIGN-MIGRATION-003). A
//! migrated chunk's bytes are never written anywhere - they already exist at a known position in
//! the old, shared `data/` directory (REQ-MIGRATION-005) - only recorded there via
//! `db::Repository::register_existing_chunk` (DESIGN-MIGRATION-006). `db::Repository::
//! insert_migrated_entry` (DESIGN-MIGRATION-007) recreates each tree entry, and
//! `crate::migration_progress` (DESIGN-MIGRATION-005) makes the whole walk resumable.

use std::fmt;
use std::io;

use cdc::{Chunker, ChunkerConfig, ConfiguredChunker};
use rusqlite::Connection;

use crate::migration_progress::{self, ProgressError};
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

#[derive(Debug)]
pub enum MigrateContentError {
    Staging(ImportError),
    Progress(ProgressError),
    Db(db::Error),
    Read(io::Error),
    /// The old data store had missing or short bytes for this old `dataId` - a real content
    /// reference should never be incomplete in a healthy source repository, so this is reported
    /// rather than silently zero-filled.
    IncompleteOldData(i64),
}

impl fmt::Display for MigrateContentError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MigrateContentError::Staging(err) => write!(f, "{err}"),
            MigrateContentError::Progress(err) => write!(f, "{err}"),
            MigrateContentError::Db(err) => write!(f, "{err}"),
            MigrateContentError::Read(err) => write!(f, "{err}"),
            MigrateContentError::IncompleteOldData(data_id) => write!(
                f,
                "old data for dataId {data_id} was missing or shorter than expected in the \
                 existing data/ directory"
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

impl From<ProgressError> for MigrateContentError {
    fn from(err: ProgressError) -> Self {
        MigrateContentError::Progress(err)
    }
}

impl From<db::Error> for MigrateContentError {
    fn from(err: db::Error) -> Self {
        MigrateContentError::Db(err)
    }
}

/// One destination this migration writes into: its own repository (adopted against the shared
/// `data/` - see `crate::migrate_scala_repo`) and its own progress record.
pub struct Target<'a> {
    pub repo: &'a db::Repository,
    pub progress: &'a Connection,
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
}

/// Migrates the whole staging tree into every one of `targets`.
pub fn migrate(
    staging: &Connection,
    old_store: &store::ByteStore,
    targets: &[Target],
) -> Result<MigrationStats, MigrateContentError> {
    let mut stats = MigrationStats::default();
    let new_root_ids = vec![0i64; targets.len()];
    walk_children(
        staging,
        old_store,
        targets,
        OLD_ROOT_ID,
        &new_root_ids,
        &mut stats,
    )?;
    Ok(stats)
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
    stats: &mut MigrationStats,
) -> Result<(), MigrateContentError> {
    for child in scala_import::staging_children(staging, old_parent_id)? {
        if child.id == OLD_ROOT_ID {
            // The root is its own parent in the source - never recreate it as a "child" of
            // itself.
            continue;
        }
        migrate_entry(staging, old_store, targets, &child, new_parent_ids, stats)?;
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
    stats: &mut MigrationStats,
) -> Result<(), MigrateContentError> {
    let content_ids: Vec<Option<i64>> = match child.data_id {
        None => vec![None; targets.len()],
        Some(data_id) => resolve_content(staging, old_store, targets, data_id, stats)?
            .into_iter()
            .map(Some)
            .collect(),
    };

    let mut new_ids = Vec::with_capacity(targets.len());
    for (i, target) in targets.iter().enumerate() {
        let new_id = match migration_progress::migrated_id(target.progress, child.id)? {
            Some(id) => id,
            None => {
                let id = target.repo.insert_migrated_entry(
                    new_parent_ids[i],
                    &child.name,
                    child.time,
                    child.deleted_at,
                    content_ids[i],
                )?;
                migration_progress::record_migrated(target.progress, child.id, id)?;
                stats.tree_entries_created += 1;
                id
            }
        };
        new_ids.push(new_id);
    }

    if child.data_id.is_none() {
        walk_children(staging, old_store, targets, child.id, &new_ids, stats)?;
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
    stats: &mut MigrationStats,
) -> Result<Vec<i64>, MigrateContentError> {
    let mut content_ids: Vec<Option<i64>> = Vec::with_capacity(targets.len());
    let mut pending = Vec::new();
    for (i, target) in targets.iter().enumerate() {
        match migration_progress::cached_content(target.progress, data_id)? {
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
            let content_id = empty_content_id(targets[i].repo)?;
            migration_progress::record_content(targets[i].progress, data_id, content_id)?;
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
        .map(|&i| (i, MigrationSettler::new(targets[i].repo, &parts)))
        .collect();

    let mut read_buf = vec![0u8; READ_WINDOW];
    for &(start, stop) in &parts {
        let mut pos = start;
        while pos < stop {
            let n = ((stop - pos) as usize).min(READ_WINDOW);
            let integrity = old_store
                .read(pos, &mut read_buf[..n])
                .map_err(MigrateContentError::Read)?;
            if matches!(integrity, store::ReadIntegrity::Incomplete { .. }) {
                return Err(MigrateContentError::IncompleteOldData(data_id));
            }
            for (_, settler) in &mut settlers {
                settler.feed(&read_buf[..n])?;
            }
            pos += n as u64;
        }
    }

    for (i, settler) in settlers {
        let content_id = settler.finish(total_len)?;
        migration_progress::record_content(targets[i].progress, data_id, content_id)?;
        content_ids[i] = Some(content_id);
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

/// Streaming chunker/hasher state for re-chunking one distinct old content reference into one
/// target - one instance per `(data_id, target)` pair still needing work, fed the same bytes as
/// every other pending target's own instance in lockstep, each producing its own chunk boundaries
/// since each target's own `--cdc-target-size-bits` differs.
struct MigrationSettler<'a> {
    repo: &'a db::Repository,
    parts: &'a [(u64, u64)],
    chunker: ConfiguredChunker,
    chunk_buffer: Vec<u8>,
    chunk_ids: Vec<i64>,
    content_hasher: blake3::Hasher,
    /// This settler's own logical position, within the concatenation of `parts`, of the end of the
    /// last chunk it has completed so far - see `map_to_old_store_extents`.
    chunk_boundary: u64,
}

impl<'a> MigrationSettler<'a> {
    fn new(repo: &'a db::Repository, parts: &'a [(u64, u64)]) -> Self {
        let bits = repo.settings().cdc_target_size_bits();
        let config = ChunkerConfig::new(Some(bits))
            .expect("cdc_target_size_bits was already validated when this repository was adopted");
        Self {
            repo,
            parts,
            chunker: config.chunker(),
            chunk_buffer: Vec::new(),
            chunk_ids: Vec::new(),
            content_hasher: blake3::Hasher::new(),
            chunk_boundary: 0,
        }
    }

    fn feed(&mut self, data: &[u8]) -> Result<(), MigrateContentError> {
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
        Ok(())
    }

    fn complete_chunk(&mut self) -> Result<(), MigrateContentError> {
        let hash = blake3::hash(&self.chunk_buffer);
        let chunk_hash = &hash.as_bytes()[..HASH_WIDTH];
        let length = self.chunk_buffer.len() as i64;

        let chunk_start = self.chunk_boundary;
        let chunk_end = chunk_start + length as u64;
        self.chunk_boundary = chunk_end;

        let chunk_id = match self.repo.find_chunk(length, chunk_hash)? {
            Some(id) => id,
            None => {
                let extents = map_to_old_store_extents(self.parts, chunk_start, chunk_end);
                self.repo
                    .register_existing_chunk(length, chunk_hash, &extents)?
            }
        };
        self.chunk_ids.push(chunk_id);
        self.content_hasher.update(&(length as u64).to_le_bytes());
        self.content_hasher.update(chunk_hash);
        self.chunk_buffer.clear();
        Ok(())
    }

    fn finish(mut self, total_len: u64) -> Result<i64, MigrateContentError> {
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
        db::open_repository(&repo_root).unwrap()
    }

    fn new_progress(dir: &tempfile::TempDir, name: &str) -> Connection {
        migration_progress::open_or_create(&dir.path().join(name)).unwrap()
    }

    #[test]
    fn migrate_recreates_the_tree_and_content_across_a_directory_and_a_soft_delete() {
        let dir = tempfile::tempdir().unwrap();
        let (staging, old_store) = build_source(&dir);
        let repo = new_repo(&dir, "dest", 12);
        let progress = new_progress(&dir, "progress.db");
        let targets = [Target {
            repo: &repo,
            progress: &progress,
        }];

        let stats = migrate(&staging, &old_store, &targets).unwrap();
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

    #[test]
    fn migrate_run_twice_creates_nothing_new_the_second_time() {
        let dir = tempfile::tempdir().unwrap();
        let (staging, old_store) = build_source(&dir);
        let repo = new_repo(&dir, "dest", 12);
        let progress = new_progress(&dir, "progress.db");
        let targets = [Target {
            repo: &repo,
            progress: &progress,
        }];

        let first = migrate(&staging, &old_store, &targets).unwrap();
        assert!(first.tree_entries_created > 0);

        let second = migrate(&staging, &old_store, &targets).unwrap();
        assert_eq!(second, MigrationStats::default());
    }

    #[test]
    fn migrate_reads_content_shared_across_targets_only_once() {
        let dir = tempfile::tempdir().unwrap();
        let (staging, old_store) = build_source(&dir);
        let repo_a = new_repo(&dir, "dest-a", 12);
        let repo_b = new_repo(&dir, "dest-b", 16);
        let progress_a = new_progress(&dir, "progress-a.db");
        let progress_b = new_progress(&dir, "progress-b.db");
        let targets = [
            Target {
                repo: &repo_a,
                progress: &progress_a,
            },
            Target {
                repo: &repo_b,
                progress: &progress_b,
            },
        ];

        let stats = migrate(&staging, &old_store, &targets).unwrap();
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
}
