use rusqlite_migration::{M, Migrations};

/// Schema for database version 1.
///
/// - `repository_settings` holds the single row of per-repository settings chosen
///   at `init` time.
/// - `contents` is the deduplication key: one row per unique `(length, hash)` pair.
/// - `chunks` records where each content's chunks are physically stored, referencing
///   `contents` by id. `stop` is exclusive, i.e. each chunk occupies the half-open
///   byte range `[start, stop)` in the data store.
/// - `tree_entries` is the file system tree. The root entry (id 0) is its own parent.
const SCHEMA_V1: &str = "
CREATE TABLE repository_settings (
  id                   INTEGER PRIMARY KEY CHECK (id = 1),
  cdc_target_size_bits INTEGER NOT NULL CHECK (cdc_target_size_bits BETWEEN 10 AND 30),
  chunking             TEXT    NOT NULL CHECK (chunking IN ('cdc', 'none')),
  hash_algorithm       TEXT    NOT NULL CHECK (hash_algorithm IN ('blake3'))
);

CREATE TABLE contents (
  id     INTEGER PRIMARY KEY,
  length INTEGER NOT NULL,
  hash   BLOB    NOT NULL,
  UNIQUE (length, hash)
);

CREATE TABLE chunks (
  content_id INTEGER NOT NULL REFERENCES contents(id),
  seq        INTEGER NOT NULL,
  start      INTEGER NOT NULL,
  stop       INTEGER NOT NULL,
  PRIMARY KEY (content_id, seq)
);
CREATE INDEX chunks_stop_idx ON chunks(stop);

CREATE TABLE tree_entries (
  id         INTEGER PRIMARY KEY,
  parent_id  INTEGER NOT NULL REFERENCES tree_entries(id),
  name       TEXT    NOT NULL,
  time       INTEGER NOT NULL,
  deleted    INTEGER NOT NULL DEFAULT 0,
  content_id INTEGER REFERENCES contents(id),
  UNIQUE (parent_id, name, deleted)
);
CREATE INDEX tree_entries_content_id_idx ON tree_entries(content_id);
";

/// All database migrations, in order. Applying them is tracked via SQLite's built-in
/// `PRAGMA user_version`, so no separate schema-version table is needed.
pub(crate) fn migrations() -> Migrations<'static> {
    Migrations::new(vec![M::up(SCHEMA_V1)])
}
