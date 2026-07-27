use rusqlite_migration::{M, Migrations};

/// Schema for database version 1.
///
/// - `repository_settings` holds the single row of per-repository settings chosen
///   at `init` time. The hash algorithm (blake3) is not a setting: it's fixed in
///   code, so it has no column here.
/// - `chunks` is the content-addressable chunk store: the deduplication key is
///   `(length, hash)`, one row per unique chunk. `stop` is exclusive, i.e. each
///   chunk occupies the half-open byte range `[start, stop)` in the data store.
///   `ref_count` is the number of `content_chunks` rows referencing this chunk,
///   maintained by triggers; a chunk with `ref_count = 0` is unreferenced and can
///   be purged.
/// - `contents` is one row per distinct file content (an ordered sequence of
///   chunks); `length` is the total logical file size. Multiple `tree_entries` can
///   reference the same content. `ref_count` is the number of `tree_entries` rows
///   referencing this content, maintained by triggers; a content with
///   `ref_count = 0` is unreferenced and can be purged.
/// - `content_chunks` records, for each content, the ordered sequence of chunks
///   that make it up (chunks themselves may be shared across contents). It needs
///   no `ref_count` of its own: it has no dependents other than `contents`, so
///   `ON DELETE CASCADE` on `content_id` is sufficient to keep it free of orphans
///   whenever an unreferenced content is purged.
/// - `tree_entries` is the file system tree. The root entry (id 0) is its own
///   parent (`parent_id = 0`); this is the only way to give it a well-defined,
///   fixed anchor while keeping `parent_id` non-null everywhere - which matters
///   because SQL treats every `NULL` as distinct from every other `NULL` for
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
  start     INTEGER NOT NULL,
  stop      INTEGER NOT NULL,
  ref_count INTEGER NOT NULL DEFAULT 0,
  UNIQUE (length, hash),
  CONSTRAINT chk_chunks_ref_count CHECK (ref_count >= 0)
);

CREATE TABLE contents (
  id        INTEGER PRIMARY KEY,
  length    INTEGER NOT NULL,
  ref_count INTEGER NOT NULL DEFAULT 0,
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
  content_id INTEGER REFERENCES contents(id)
);
CREATE UNIQUE INDEX tree_entries_active_name_idx ON tree_entries(parent_id, name) WHERE deleted_at IS NULL;
CREATE INDEX tree_entries_content_id_idx ON tree_entries(content_id);

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

INSERT INTO tree_entries (id, parent_id, name, time)
  VALUES (0, 0, '', CAST(strftime('%s', 'now') AS INTEGER) * 1000);
";

/// All database migrations, in order. Applying them is tracked via SQLite's built-in
/// `PRAGMA user_version`, so no separate schema-version table is needed.
pub(crate) fn migrations() -> Migrations<'static> {
    Migrations::new(vec![M::up(SCHEMA_V1)])
}
