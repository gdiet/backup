use rusqlite_migration::{M, Migrations};

/// Schema for database version 1.
///
/// - `repository_settings` holds the single row of per-repository settings chosen
///   at `init` time. The hash algorithm (blake3) is not a setting: it's fixed in
///   code, so it has no column here.
/// - `chunks` is the content-addressable chunk store: the deduplication key is
///   `(length, hash)`, one row per unique chunk. `ref_count` is the number of
///   `content_chunks` rows referencing this chunk, maintained by triggers; a
///   chunk with `ref_count = 0` is unreferenced and can be purged. A chunk's
///   physical bytes are not necessarily one contiguous range - see
///   `chunk_extents` below.
/// - `chunk_extents` records where a chunk's bytes actually live in the data
///   store: 1..N half-open byte ranges `[start, stop)` per chunk, `seq`-ordered
///   (concatenating the extents in `seq` order reconstructs the chunk's bytes).
///   Kept separate from `chunks` rather than as denormalized `start`/`stop`
///   columns there so a chunk can be reassembled from several non-contiguous
///   ranges: once `reclaim-space` deletes an unreferenced chunk (cascading into
///   its extents, freeing those byte ranges), a later `store` run can reuse the
///   resulting gaps for a new chunk's bytes - satisfying one allocation by
///   spanning several old gaps if needed - instead of the data store only ever
///   growing. `ON DELETE CASCADE` on `chunk_id` keeps this free of orphans
///   whenever a `chunks` row is purged, the same pattern `content_chunks` uses
///   for `contents`.
/// - `contents` is one row per distinct file content (an ordered sequence of
///   chunks); `length` is the total logical file size. `hash` is a hash over the
///   ordered sequence of chunk lengths and hashes (not over the raw file bytes
///   directly - it's derived from data already computed while chunking, so it
///   costs nothing extra to obtain), used to deduplicate `contents` rows the same
///   way `chunks.hash` deduplicates chunks: files with byte-identical content
///   share one `contents` row instead of each getting their own. An empty file
///   hashes the empty chunk sequence, so all empty files share one `contents` row
///   with zero `content_chunks`. Multiple `tree_entries` can reference the same
///   content. `ref_count` is the number of `tree_entries` rows referencing this
///   content, maintained by triggers; a content with `ref_count = 0` is
///   unreferenced and can be purged.
/// - `content_chunks` records, for each content, the ordered sequence of chunks
///   that make it up (chunks themselves may be shared across contents). It needs
///   no `ref_count` of its own: it has no dependents other than `contents`, so
///   `ON DELETE CASCADE` on `content_id` is sufficient to keep it free of orphans
///   whenever an unreferenced content is purged.
/// - `tree_entries` is the file system tree. `kind` distinguishes a directory from
///   a file; this is needed because `content_id IS NULL` alone is ambiguous
///   between "directory" (never has content) and "empty file" (has content
///   conceptually, but zero chunks) - `kind` is the sole authority for that
///   distinction, `content_id` is simply `NULL` for both directories and empty
///   files. The root entry (id 0) is its own parent (`parent_id = 0`); this is the
///   only way to give it a well-defined, fixed anchor while keeping `parent_id`
///   non-null everywhere - which matters because SQL treats every `NULL` as
///   distinct from every other `NULL` for
///   uniqueness purposes, so a nullable `parent_id` would silently defeat the
///   partial unique index below for all top-level entries. Soft-deleted entries
///   have a non-null `deleted_at`; the partial unique index below allows any
///   number of deleted entries to share a `(parent_id, name)`, while still
///   preventing duplicate *active* entries. Soft-deleted entries still reference
///   their content (to keep it recoverable), so they still hold their `ref_count`
///   contribution; content only becomes unreferenced once a `tree_entries` row is
///   actually deleted (e.g. by a future retention/purge step), not when it is
///   merely soft-deleted. The root row is seeded by this migration itself (its
///   `time` doesn't depend on anything the application passes in, unlike
///   `repository_settings`, so there's no reason to insert it from Rust code
///   instead).
///
/// Orphan cleanup ("garbage collection") is `DELETE FROM contents WHERE
/// ref_count = 0` followed by `DELETE FROM chunks WHERE ref_count = 0`, in that
/// order: deleting an unreferenced content cascades into deleting its
/// `content_chunks` rows, which in turn decrements the `ref_count` of chunks that
/// only that content used, so those chunks must be swept in a second pass.
///
/// Two indexes were added to this schema after its initial version, both
/// folded directly in here rather than shipped as separate migrations -
/// nothing built on this crate has been released yet, so there's no reason
/// to track schema history across steps nobody has been migrated past;
/// see `migrations::tests::a_database_already_past_the_given_migration_list_fails_to_open`
/// for what that would cost once it *does* matter (verified empirically by
/// exporting the one real, but disposable/test, repository this project
/// has ever produced to SQL and reimporting it against a squashed schema -
/// twice now, each time this schema changed). Once something real
/// depends on a given schema shape, this stops being free and schema
/// changes go back to being genuine new migrations appended below, not
/// edits here: `rusqlite_migration` refuses to open a database whose
/// `user_version` exceeds the number of migrations it's given.
///
/// - `tree_entries_deleted_at_idx` indexes the soft-deleted-entry side
///   (`reclaim_space`'s cutoff `DELETE` and `db::query::deleted_entries`,
///   i.e. `backup deleted`, both filter/join on it, otherwise an unindexed
///   full table scan; partial, `WHERE deleted_at IS NOT NULL`, since the
///   large majority of rows in a real repository are active, not deleted -
///   no benefit to indexing those).
/// - `tree_entries_parent_id_idx` is a plain (non-partial) index on
///   `parent_id` alone, covering every row regardless of `deleted_at` -
///   fixing a confirmed, severe performance bug in `deleted_entries`
///   itself: its recursive walk must follow deleted ancestors too (see
///   that function's own doc comment), so it can't filter on `deleted_at
///   IS NULL` - which means it couldn't use `tree_entries_active_name_idx`
///   either, since that index doesn't cover deleted rows. Without any
///   usable index, SQLite fell back to a full table scan of `tree_entries`
///   at *every* level of the recursive walk. Measured against the real
///   ~7.16M-row `dedup/` repository: unscoped `backup deleted` never
///   finished in under an hour before this index existed (`EXPLAIN QUERY
///   PLAN` showed `SCAN t` inside the recursive step); with an equivalent
///   index in place, the same query completed in 1m48s, returning the
///   exact same row count. Doesn't replace `tree_entries_active_name_idx`:
///   that one is still strictly better for every active-only lookup
///   (`resolve_path`, `find_tree_entry`, `subtree_entries_with_paths`,
///   etc.) - smaller (partial) and covers `name` too, which this index
///   doesn't. See `docs/plans/deleted-entries-performance.md` for the full
///   investigation.
const SCHEMA_V1: &str = "
CREATE TABLE repository_settings (
  id                   INTEGER PRIMARY KEY,
  cdc_target_size_bits INTEGER NOT NULL,
  chunking             TEXT    NOT NULL,
  CONSTRAINT chk_repository_settings_id CHECK (id = 1),
  CONSTRAINT chk_repository_settings_cdc_target_size_bits CHECK (cdc_target_size_bits BETWEEN 10 AND 30),
  CONSTRAINT chk_repository_settings_chunking CHECK (chunking IN ('cdc', 'none'))
);

CREATE TABLE chunks (
  id        INTEGER PRIMARY KEY,
  length    INTEGER NOT NULL,
  hash      BLOB    NOT NULL,
  ref_count INTEGER NOT NULL DEFAULT 0,
  UNIQUE (length, hash),
  CONSTRAINT chk_chunks_ref_count CHECK (ref_count >= 0)
);

CREATE TABLE chunk_extents (
  chunk_id INTEGER NOT NULL REFERENCES chunks(id) ON DELETE CASCADE,
  seq      INTEGER NOT NULL,
  start    INTEGER NOT NULL,
  stop     INTEGER NOT NULL,
  PRIMARY KEY (chunk_id, seq)
);
CREATE INDEX chunk_extents_start_idx ON chunk_extents(start);

CREATE TABLE contents (
  id        INTEGER PRIMARY KEY,
  length    INTEGER NOT NULL,
  hash      BLOB    NOT NULL,
  ref_count INTEGER NOT NULL DEFAULT 0,
  UNIQUE (hash),
  CONSTRAINT chk_contents_ref_count CHECK (ref_count >= 0)
);

CREATE TABLE content_chunks (
  content_id INTEGER NOT NULL REFERENCES contents(id) ON DELETE CASCADE,
  seq        INTEGER NOT NULL,
  chunk_id   INTEGER NOT NULL REFERENCES chunks(id),
  PRIMARY KEY (content_id, seq)
);
CREATE INDEX content_chunks_chunk_id_idx ON content_chunks(chunk_id);

CREATE TABLE tree_entries (
  id         INTEGER PRIMARY KEY,
  parent_id  INTEGER NOT NULL REFERENCES tree_entries(id),
  name       TEXT    NOT NULL,
  time       INTEGER NOT NULL,
  deleted_at INTEGER,
  content_id INTEGER REFERENCES contents(id),
  kind       TEXT    NOT NULL,
  CONSTRAINT chk_tree_entries_kind CHECK (kind IN ('dir', 'file'))
);
CREATE UNIQUE INDEX tree_entries_active_name_idx ON tree_entries(parent_id, name) WHERE deleted_at IS NULL;
CREATE INDEX tree_entries_content_id_idx ON tree_entries(content_id);
CREATE INDEX tree_entries_deleted_at_idx ON tree_entries(deleted_at) WHERE deleted_at IS NOT NULL;
CREATE INDEX tree_entries_parent_id_idx ON tree_entries(parent_id);

CREATE TRIGGER content_chunks_ref_count_ins AFTER INSERT ON content_chunks BEGIN
  UPDATE chunks SET ref_count = ref_count + 1 WHERE id = NEW.chunk_id;
END;
CREATE TRIGGER content_chunks_ref_count_del AFTER DELETE ON content_chunks BEGIN
  UPDATE chunks SET ref_count = ref_count - 1 WHERE id = OLD.chunk_id;
END;

CREATE TRIGGER tree_entries_ref_count_ins AFTER INSERT ON tree_entries
  WHEN NEW.content_id IS NOT NULL
BEGIN
  UPDATE contents SET ref_count = ref_count + 1 WHERE id = NEW.content_id;
END;
CREATE TRIGGER tree_entries_ref_count_del AFTER DELETE ON tree_entries
  WHEN OLD.content_id IS NOT NULL
BEGIN
  UPDATE contents SET ref_count = ref_count - 1 WHERE id = OLD.content_id;
END;

INSERT INTO tree_entries (id, parent_id, name, time, kind)
  VALUES (0, 0, '', CAST(strftime('%s', 'now') AS INTEGER) * 1000, 'dir');
";

/// All database migrations, in order. Applying them is tracked via SQLite's built-in
/// `PRAGMA user_version`, so no separate schema-version table is needed.
pub(crate) fn migrations() -> Migrations<'static> {
    Migrations::new(vec![M::up(SCHEMA_V1)])
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    #[test]
    fn a_fresh_schema_includes_the_deleted_at_index() {
        let mut conn = Connection::open_in_memory().unwrap();
        migrations().to_latest(&mut conn).unwrap();

        let index_exists: bool = conn
            .query_row(
                "SELECT COUNT(*) > 0 FROM sqlite_master
                 WHERE type = 'index' AND name = 'tree_entries_deleted_at_idx'",
                (),
                |row| row.get(0),
            )
            .unwrap();
        assert!(index_exists);
    }

    #[test]
    fn tree_entries_parent_id_idx_is_non_partial() {
        let mut conn = Connection::open_in_memory().unwrap();
        migrations().to_latest(&mut conn).unwrap();

        let indexed_columns: Option<String> = conn
            .query_row(
                "SELECT sql FROM sqlite_master
                 WHERE type = 'index' AND name = 'tree_entries_parent_id_idx'",
                (),
                |row| row.get(0),
            )
            .unwrap();
        let sql = indexed_columns.unwrap();
        assert!(sql.contains("parent_id"));
        assert!(
            !sql.to_uppercase().contains("WHERE"),
            "must be non-partial to cover deleted rows too: {sql}"
        );
    }

    /// Documents, with a throwaway two-step schema (standing in for the
    /// real `SCHEMA_V1`+`SCHEMA_V2` pair that was actually squashed - see
    /// `SCHEMA_V1`'s own doc comment), why folding a later migration back
    /// into an earlier one isn't free once something has already been
    /// migrated past that point: `rusqlite_migration` refuses to open a
    /// database whose recorded `user_version` is higher than the number of
    /// migrations it's given, rather than silently treating it as current.
    #[test]
    fn a_database_already_past_the_given_migration_list_fails_to_open() {
        const STEP_ONE: &str = "CREATE TABLE t (id INTEGER PRIMARY KEY);";
        const STEP_TWO: &str = "CREATE INDEX t_id_idx ON t(id);";

        let mut conn = Connection::open_in_memory().unwrap();
        Migrations::new(vec![M::up(STEP_ONE), M::up(STEP_TWO)])
            .to_latest(&mut conn)
            .unwrap(); // user_version = 2

        let squashed = Migrations::new(vec![M::up(STEP_ONE)]); // only 1 step
        assert!(squashed.to_latest(&mut conn).is_err());
    }
}
