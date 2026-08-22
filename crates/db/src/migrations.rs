//! The `v1` schema migration - DESIGN-METADATA-004 in
//! `docs/design/metadata-storage.md`, concrete SQL in
//! `docs/design/metadata-schema-with-contents-table.md`.
//!
//! Pre-release, `v1` is edited in place rather than followed by `v2`, `v3`,
//! ... - see "Pre-release: a single, freely rewritten `v1` migration" under
//! DESIGN-METADATA-005 in `docs/design/metadata-storage.md`.

use rusqlite_migration::{M, Migrations};

const V1: &str = r#"
CREATE TABLE repository_settings (
  id                   INTEGER PRIMARY KEY,
  cdc_target_size_bits INTEGER,
  creation_time        INTEGER NOT NULL,
  CONSTRAINT chk_repository_settings_id CHECK (id = 1),
  CONSTRAINT chk_repository_settings_cdc_target_size_bits CHECK (
    cdc_target_size_bits IS NULL OR cdc_target_size_bits BETWEEN 6 AND 30
  )
);

-- AUTOINCREMENT (unlike every other table's id): a purged entry's id must never be reused by a
-- later insert - see "Why tree_entries.id is AUTOINCREMENT" in metadata-schema-with-contents-table.md.
CREATE TABLE tree_entries (
  id         INTEGER PRIMARY KEY AUTOINCREMENT,
  parent_id  INTEGER NOT NULL REFERENCES tree_entries(id),
  name       TEXT    NOT NULL,
  time       INTEGER NOT NULL,
  deleted_at INTEGER,
  content_id INTEGER REFERENCES contents(id),
  kind       INTEGER NOT NULL,
  CONSTRAINT chk_tree_entries_kind CHECK (kind IN (0, 1)),
  CONSTRAINT chk_tree_entries_kind_content_id CHECK (
    (kind = 0 AND content_id IS NULL) OR (kind = 1 AND content_id IS NOT NULL)
  ),
  CONSTRAINT chk_tree_entries_name_nonempty CHECK (id = 0 OR name != '')
);
CREATE UNIQUE INDEX tree_entries_active_name_idx ON tree_entries(parent_id, name) WHERE deleted_at IS NULL;
CREATE INDEX tree_entries_content_id_idx ON tree_entries(content_id);
CREATE INDEX tree_entries_deleted_at_idx ON tree_entries(deleted_at) WHERE deleted_at IS NOT NULL;
CREATE INDEX tree_entries_parent_id_idx ON tree_entries(parent_id);

CREATE TABLE contents (
  id        INTEGER PRIMARY KEY,
  length    INTEGER NOT NULL,
  hash      BLOB    NOT NULL,
  ref_count INTEGER NOT NULL DEFAULT 0,
  UNIQUE (length, hash),
  CONSTRAINT chk_contents_ref_count CHECK (ref_count >= 0),
  CONSTRAINT chk_contents_hash_length CHECK (length(hash) = 20)
);

CREATE TABLE content_chunks (
  content_id INTEGER NOT NULL REFERENCES contents(id) ON DELETE CASCADE,
  seq        INTEGER NOT NULL,
  chunk_id   INTEGER NOT NULL REFERENCES chunks(id),
  PRIMARY KEY (content_id, seq)
);
CREATE INDEX content_chunks_chunk_id_idx ON content_chunks(chunk_id);

CREATE TABLE chunks (
  id        INTEGER PRIMARY KEY,
  length    INTEGER NOT NULL,
  hash      BLOB    NOT NULL,
  ref_count INTEGER NOT NULL DEFAULT 0,
  UNIQUE (length, hash),
  CONSTRAINT chk_chunks_ref_count CHECK (ref_count >= 0),
  CONSTRAINT chk_chunks_hash_length CHECK (length(hash) = 20)
);

CREATE TABLE chunk_extents (
  chunk_id INTEGER NOT NULL REFERENCES chunks(id) ON DELETE CASCADE,
  seq      INTEGER NOT NULL,
  start    INTEGER NOT NULL,
  stop     INTEGER NOT NULL,
  PRIMARY KEY (chunk_id, seq),
  CONSTRAINT chk_chunk_extents_range CHECK (stop > start)
);
CREATE INDEX chunk_extents_start_idx ON chunk_extents(start);

-- Chunk-level ref-counting.
CREATE TRIGGER content_chunks_ref_count_ins AFTER INSERT ON content_chunks BEGIN
  UPDATE chunks SET ref_count = ref_count + 1 WHERE id = NEW.chunk_id;
END;
CREATE TRIGGER content_chunks_ref_count_del AFTER DELETE ON content_chunks BEGIN
  UPDATE chunks SET ref_count = ref_count - 1 WHERE id = OLD.chunk_id;
END;

-- Content-level ref-counting: a tree entry created with content_id already set, or purged.
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

-- Guard the root tree entry against deletion by code that does not know it is special.
CREATE TRIGGER tree_entries_protect_root BEFORE DELETE ON tree_entries
  WHEN OLD.id = 0
BEGIN
  SELECT RAISE(ABORT, 'cannot delete the root tree entry');
END;

-- Guard cdc_target_size_bits/creation_time against being changed after creation.
CREATE TRIGGER repository_settings_cdc_target_size_bits_immutable
  BEFORE UPDATE OF cdc_target_size_bits ON repository_settings
BEGIN
  SELECT RAISE(ABORT, 'cdc_target_size_bits is fixed for the repository''s lifetime (REQ-STORAGE-003)');
END;
CREATE TRIGGER repository_settings_creation_time_immutable
  BEFORE UPDATE OF creation_time ON repository_settings
BEGIN
  SELECT RAISE(ABORT, 'creation_time is fixed for the repository''s lifetime (REQ-STORAGE-008)');
END;

-- Root tree entry: id = 0, its own parent. KIND_DIR = 0. time = 0 is immediately superseded once
-- anything is created at top level (REQ-TREE-005). No seed row for contents - an empty file's
-- content is found or inserted through the ordinary dedup lookup like any other content.
INSERT INTO tree_entries (id, parent_id, name, time, kind) VALUES (0, 0, '', 0, 0);
"#;

pub fn migrations() -> Migrations<'static> {
    Migrations::new(vec![M::up(V1)])
}
