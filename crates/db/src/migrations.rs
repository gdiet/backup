//! The `v1` schema migration - DESIGN-METADATA-004 in
//! `docs/design/metadata-storage.md`, concrete SQL in
//! `docs/design/metadata-schema-with-contents-table.md`.
//!
//! Pre-release, `v1` is edited in place rather than followed by `v2`, `v3`,
//! ... - see "Pre-release: a single, freely rewritten `v1` migration" under
//! DESIGN-METADATA-005 in `docs/design/metadata-storage.md`.

use rusqlite_migration::{M, Migrations};

const V1: &str = r#"
-- Single-row (id = 1) settings fixed at repository creation - see DESIGN-METADATA-009
-- (Repository settings) in metadata-schema-with-contents-table.md.
CREATE TABLE repository_settings (
  id                   INTEGER PRIMARY KEY,
  -- NULL selects whole-file chunking (no CDC); a value selects CDC chunking with that
  -- target_size_bits - mirrors cdc::ChunkerConfig's own Option<u32> shape exactly.
  cdc_target_size_bits INTEGER,
  -- Unix epoch milliseconds, matching tree_entries.time's own unit - REQ-STORAGE-008. The actual
  -- creation moment for a natively created repository; a migrated repository's source root tree
  -- entry's own time for one adopted from Scala.
  creation_time        INTEGER NOT NULL,
  CONSTRAINT chk_repository_settings_id CHECK (id = 1),
  CONSTRAINT chk_repository_settings_cdc_target_size_bits CHECK (
    cdc_target_size_bits IS NULL OR cdc_target_size_bits BETWEEN 6 AND 30
  )
);

-- The directory/file tree. AUTOINCREMENT (unlike every other table's id): a purged entry's id must
-- never be reused by a later insert - see "Why tree_entries.id is AUTOINCREMENT" in
-- metadata-schema-with-contents-table.md.
CREATE TABLE tree_entries (
  id         INTEGER PRIMARY KEY AUTOINCREMENT,
  parent_id  INTEGER NOT NULL REFERENCES tree_entries(id),
  -- Empty only for the root entry (id = 0), enforced below.
  name       TEXT    NOT NULL,
  time       INTEGER NOT NULL,
  deleted_at INTEGER,
  -- NULL only for a directory (kind = KIND_DIR, always). A file's row is only ever inserted once
  -- its content is settled - including an empty file, whose zero-chunk content resolves through
  -- the same dedup lookup as any other content (DESIGN-METADATA-008, "In-progress files are not
  -- written to the database") - never before that point.
  content_id INTEGER REFERENCES contents(id),
  -- KIND_DIR = 0 / KIND_FILE = 1 (see the seed INSERT below) - kept as a separate column rather
  -- than inferred from content_id; see "Why kind is a separate column" in
  -- metadata-schema-with-contents-table.md.
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

-- Each content id is for a unique (length, hash) *file* (as opposed to chunk) content pair - a
-- file's content, while a logical entity, consists of a sequence of 0..N chunks. Deduplicating
-- whole-file content here, not just at the chunk level, saves real metadata space in practice -
-- see "Empirical measurement" in metadata-schema-comparison.md.
CREATE TABLE contents (
  id        INTEGER PRIMARY KEY,
  -- Not database-enforced - see "Consistency risk of a stored length" in
  -- metadata-schema-comparison.md. Kept in sync by the repository's integrity check
  -- (REQ-INTEGRITY-001), not a schema-level constraint.
  length    INTEGER NOT NULL,
  -- BLAKE3 over the chunk sequence's (length, hash) pairs, not the file's raw bytes directly,
  -- truncated to 20 bytes - see DESIGN-METADATA-007 (Hash computation) in
  -- metadata-schema-with-contents-table.md.
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

-- Each chunk id is for a unique (length, hash) *chunk* (as opposed to file). A chunk, while a
-- logical entity, can still be physically spread across multiple extents (see chunk_extents
-- below).
CREATE TABLE chunks (
  id        INTEGER PRIMARY KEY,
  length    INTEGER NOT NULL,
  -- BLAKE3 of the chunk's raw bytes, truncated to 20 bytes (same width as contents.hash - see
  -- "Hash width" in metadata-schema-with-contents-table.md's "Magic values").
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

-- Guard the root tree entry against deletion by code that does not know it is special - see
-- "Magic values" in metadata-schema-with-contents-table.md. Without this, protection would rest
-- purely on convention (e.g. a cleanup routine remembering to skip this row) - the kind of gap
-- this trigger closes structurally.
CREATE TRIGGER tree_entries_protect_root BEFORE DELETE ON tree_entries
  WHEN OLD.id = 0
BEGIN
  SELECT RAISE(ABORT, 'cannot delete the root tree entry');
END;

-- Guard cdc_target_size_bits against being changed after creation - see "Repository settings"
-- (DESIGN-METADATA-009) in metadata-schema-with-contents-table.md. A deliberate repository-wide
-- re-chunking tool can still DROP/CREATE this trigger around its own work, same as
-- DESIGN-METADATA-005 already does with PRAGMA foreign_keys=OFF for a table rebuild.
CREATE TRIGGER repository_settings_cdc_target_size_bits_immutable
  BEFORE UPDATE OF cdc_target_size_bits ON repository_settings
BEGIN
  SELECT RAISE(ABORT, 'cdc_target_size_bits is fixed for the repository''s lifetime (REQ-STORAGE-003)');
END;
-- Guard creation_time against being changed after creation - see "Repository settings"
-- (DESIGN-METADATA-009) in metadata-schema-with-contents-table.md.
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
