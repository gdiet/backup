# Metadata Schema Proposal: No `contents` Table

One of two schema proposals being compared for point 4 of
[`metadata-storage.md`](metadata-storage.md); see
[`metadata-schema-comparison.md`](metadata-schema-comparison.md) for the trade-offs against the
alternative in [`metadata-schema-with-contents-table.md`](metadata-schema-with-contents-table.md).
Neither is decided yet.

This proposal removes whole-file (`contents`-level) deduplication entirely: `tree_entries` gains
its own `length` column, and `content_chunks` references a tree entry directly instead of an
intermediate `contents` row. Chunk-level deduplication (`chunks`, `chunk_extents`) is unaffected -
stored bytes stay fully deduplicated either way; what changes is only how a file's chunk sequence
is anchored in the metadata.

## Schema

```sql
CREATE TABLE tree_entries (
  id         INTEGER PRIMARY KEY,
  parent_id  INTEGER NOT NULL REFERENCES tree_entries(id),
  name       TEXT    NOT NULL,
  time       INTEGER NOT NULL,
  deleted_at INTEGER,
  length     INTEGER,
  kind       TEXT    NOT NULL,
  CONSTRAINT chk_tree_entries_kind CHECK (kind IN ('dir', 'file'))
);
CREATE UNIQUE INDEX tree_entries_active_name_idx ON tree_entries(parent_id, name) WHERE deleted_at IS NULL;
CREATE INDEX tree_entries_deleted_at_idx ON tree_entries(deleted_at) WHERE deleted_at IS NOT NULL;
CREATE INDEX tree_entries_parent_id_idx ON tree_entries(parent_id);

CREATE TABLE content_chunks (
  entry_id INTEGER NOT NULL REFERENCES tree_entries(id) ON DELETE CASCADE,
  seq      INTEGER NOT NULL,
  chunk_id INTEGER NOT NULL REFERENCES chunks(id),
  PRIMARY KEY (entry_id, seq)
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
```

Renamed `content_chunks.content_id` to `entry_id`: it now references `tree_entries(id)`, not a
`contents` row, and keeping the old name would misdescribe what it points to.

Dropped: the `tree_entries_content_id_idx` index (no `content_id` column left to index) and the
`contents_id_idx`-style content-lookup path generally - see "Finding duplicate content" below for
what replaces it.

## Triggers

```sql
-- Chunk-level ref-counting: unchanged from the other proposal, and from rust/db - the only
-- ref-count trigger this schema needs at all.
CREATE TRIGGER content_chunks_ref_count_ins AFTER INSERT ON content_chunks BEGIN
  UPDATE chunks SET ref_count = ref_count + 1 WHERE id = NEW.chunk_id;
END;
CREATE TRIGGER content_chunks_ref_count_del AFTER DELETE ON content_chunks BEGIN
  UPDATE chunks SET ref_count = ref_count - 1 WHERE id = OLD.chunk_id;
END;

-- Guards the root tree entry against deletion - same rationale as the other proposal, unrelated
-- to whether a contents table exists.
CREATE TRIGGER tree_entries_protect_root BEFORE DELETE ON tree_entries
  WHEN OLD.id = 0
BEGIN
  SELECT RAISE(ABORT, 'cannot delete the root tree entry');
END;
```

No content-level ref-counting trigger exists in this proposal, and none is needed:
`content_chunks` rows now belong to exactly the one tree entry that owns them (created together,
`ON DELETE CASCADE`-removed together), never shared and never re-pointed at a different owner in
place. The entire class of problem the `AFTER UPDATE OF content_id` trigger closes in the other
proposal does not exist here, because there is no `content_id` column left to update.

## Magic values

- **Root tree entry**: `id = 0`, its own parent (`parent_id = 0`) - same as the other proposal,
  same rationale (a non-null `parent_id` everywhere, so the partial unique index on
  `(parent_id, name)` enforces uniqueness at the top level too).
- **Hash width**: 20 bytes (160 bits) on `chunks.hash` - same reasoning as the other proposal (see
  `metadata-storage.md` point 4). No `contents.hash` exists in this proposal to also constrain.

No `EMPTY_CONTENT_ID`-equivalent exists or is needed: an empty file is simply
`length = 0` with zero `content_chunks` rows, indistinguishable in kind from any other settled
file - just one with an empty chunk sequence. `length IS NULL` still means "not decided yet" (the
mount's `create()` placeholder before its first write), matching the three-state meaning
`content_id` carried in the other proposal, without a sentinel row to protect.

```sql
INSERT INTO tree_entries (id, parent_id, name, time, kind)
  VALUES (0, 0, '', CAST(strftime('%s', 'now') AS INTEGER) * 1000, 'dir');
```

## Finding duplicate content

Without a `contents` table, "which other files share this exact content" (in `rust/db`, a single
`WHERE content_id = ?` query - used by `blacklist process --delete-copies` to find every other
occurrence of a blacklisted file's content) has no equally direct equivalent, since
`content_chunks` no longer groups multiple tree entries under one shared identity.

Reasonably efficient replacement: narrow first, using the chunk index that already exists for an
unrelated purpose (`problems`' broken-chunk-to-affected-files lookup), then verify.

```sql
-- Step 1: candidates - entries whose first chunk matches the target content's first chunk.
SELECT entry_id FROM content_chunks WHERE chunk_id = ?1 AND seq = 0;
```

For each candidate, load its full ordered chunk sequence and compare it against the target's,
confirming a true match rather than merely a shared first chunk. A content-addressed `chunk_id` is
high-cardinality, so step 1 should narrow to a small candidate set in the typical case - close to
just the true duplicates themselves. The one case where this costs more than the other proposal's
direct lookup: many files that happen to share a first chunk (e.g. a common header) but differ
later - step 1 cannot distinguish those from true duplicates on its own, so step 2 has more
candidates to rule out. Still far cheaper than a full scan comparing every tree entry's chunk
sequence against the target.

## Diagram

```mermaid
erDiagram
    tree_entries {
        integer id PK
        integer parent_id FK
        text name
        integer length
        text kind
    }
    content_chunks {
        integer entry_id "PK, FK"
        integer seq PK
        integer chunk_id FK
    }
    chunks {
        integer id PK
        integer length UK
        blob hash UK
        integer ref_count
    }
    chunk_extents {
        integer chunk_id "PK, FK"
        integer seq PK
        integer start
        integer stop
    }

    tree_entries ||--o{ tree_entries : "parent_id"
    tree_entries ||--o{ content_chunks : "entry_id, ordered by seq"
    chunks ||--o{ content_chunks : "chunk_id"
    chunks ||--|{ chunk_extents : "chunk_id, ordered by seq"
```

`time`/`deleted_at` omitted from the diagram - present in the schema above, not relevant to the
content-sharing structure this diagram is about.
