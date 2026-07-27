use rusqlite_migration::{M, Migrations};

/// Schema for database version 1.
///
/// - `repository_settings` holds the single row of per-repository settings chosen
///   at `init` time. The hash algorithm (blake3) is not a setting: it's fixed in
///   code, so it has no column here.
/// - `chunks` is the content-addressable chunk store: the deduplication key is
///   `(length, hash)`, one row per unique chunk. `stop` is exclusive, i.e. each
///   chunk occupies the half-open byte range `[start, stop)` in the data store.
/// - `contents` is one row per distinct file content (an ordered sequence of
///   chunks); `length` is the total logical file size. Multiple `tree_entries` can
///   reference the same content.
/// - `content_chunks` records, for each content, the ordered sequence of chunks
///   that make it up (chunks themselves may be shared across contents).
/// - `tree_entries` is the file system tree. The root entry (id 0) is its own
///   parent (`parent_id = 0`); this is the only way to give it a well-defined,
///   fixed anchor while keeping `parent_id` non-null everywhere - which matters
///   because SQL treats every `NULL` as distinct from every other `NULL` for
///   uniqueness purposes, so a nullable `parent_id` would silently defeat the
///   partial unique index below for all top-level entries. Soft-deleted entries
///   have a non-null `deleted_at`; the partial unique index below allows any
///   number of deleted entries to share a `(parent_id, name)`, while still
///   preventing duplicate *active* entries. The root row is seeded by this
///   migration itself (its `time` doesn't depend on anything the application
///   passes in, unlike `repository_settings`, so there's no reason to insert it
///   from Rust code instead).
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
  id     INTEGER PRIMARY KEY,
  length INTEGER NOT NULL,
  hash   BLOB    NOT NULL,
  start  INTEGER NOT NULL,
  stop   INTEGER NOT NULL,
  UNIQUE (length, hash)
);

CREATE TABLE contents (
  id     INTEGER PRIMARY KEY,
  length INTEGER NOT NULL
);

CREATE TABLE content_chunks (
  content_id INTEGER NOT NULL REFERENCES contents(id),
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

INSERT INTO tree_entries (id, parent_id, name, time)
  VALUES (0, 0, '', CAST(strftime('%s', 'now') AS INTEGER) * 1000);
";

/// All database migrations, in order. Applying them is tracked via SQLite's built-in
/// `PRAGMA user_version`, so no separate schema-version table is needed.
pub(crate) fn migrations() -> Migrations<'static> {
    Migrations::new(vec![M::up(SCHEMA_V1)])
}
