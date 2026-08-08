# Scala → Rust repository migration tool

**Status**: implemented. Subcommand `migrate-scala-repo`
(`cli/src/migrate_scala_repo.rs`, `MigrateScalaRepoArgs`/
`run_migrate_scala_repo`). Kept under `docs/plans/implemented/` as a record
of the compatibility findings, the design decisions taken, and two later
revisions made after real-world testing (see the two "Update" sections
below) - both significant enough to be worth their own record rather than
silently editing the original design section.

## Is the byte store (`data/` directory) compatible? Yes.

Confirmed by direct comparison against the Scala source
(`store/LongTermStore.scala`'s `pathOffsetSize`) and Rust's own
`store::path_offset_size` (`store/src/lib.rs`) - both use the exact same
layout: 100,000,000-byte files (`fileSize`/`FILE_SIZE`, "must not change
without a migration script" per Scala's own comment), the same
`dir1/dir2/positionInFile` scheme (100 files per `dir2`, 100 `dir2`s per
`dir1`), the same path format. Rust's `store` crate even has a test
(`path_format_matches_scala`) asserting this. This 1:1 compatibility is what
later made the in-place, zero-copy design (see the first "Update" section
below) possible at all: not just "Rust can read Scala's bytes", but "Rust
can adopt Scala's `data/` directory as its own, unmodified, no translation
step of any kind."

## Is the metadata (database) compatible? No, on two independent levels.

1. **Engine**: Scala uses H2 (a Java-embedded SQL database, proprietary
   binary file format); Rust uses SQLite. Not readable by Rust directly.
2. **Content model**: Scala dedupes **whole files** - one `DataEntries` row
   per distinct file content, `hash` is the MD5 of the entire file. Rust
   dedupes at the **CDC (content-defined chunking) level** - `contents` is
   built from `chunks` (blake3-hashed, shared across any content containing
   that chunk) via `content_chunks`. There is no lossless, purely-mechanical
   translation between the two - migrated content is genuinely re-chunked
   and re-hashed through Rust's own chunking pipeline.

## Getting the H2 metadata out without an H2 reader in Rust

No H2 file-format reader was needed. The tool takes as input the zipped SQL
script Scala's own `fsc db-backup` already produces (`org.h2.tools.Script`,
plain `INSERT INTO ...` statements reconstructing `TreeEntries`/
`DataEntries`) - either the zip as produced directly, or an already-unzipped
`.sql` file (detected by magic bytes, not file extension). The script is
parsed with basic, purpose-built statement/tuple splitting (quote-aware
semicolon/comma splitting, `--`-comment stripping, `X'...'` hex blob,
`U&'...'` Unicode-escape (see the second "Update" section below), and
`NULL`/quoted-string literal parsing - see `migrate_scala_repo::
script_import`) - not a general SQL parser, just enough to reconstruct typed
`INSERT` rows for the two tables that matter, tolerant of both a positional
and an explicit-column-list `INSERT` form, and of schema-qualified/quoted
identifiers. Everything else in the script (`CREATE TABLE`, `ALTER TABLE`,
sequences, the `Context` table, row-count sanity comments) is ignored. The
parsed rows are loaded into a temporary staging SQLite database shaped like
Scala's schema, which the rest of the tool then walks with normal SQL.

No H2-vs-SQLite `INSERT` dialect massaging turned out to be needed beyond
this: H2 and SQLite happen to agree on `NULL`, quoted-string (`''`-escaped),
and `X'...'` hex blob literal syntax, so values parse identically once
extracted.

## Update: in-place, zero-copy adoption of `data/`

The tool originally (see "Migration flow, as originally built" below)
re-chunked, re-hashed, and **rewrote** every file's bytes into a fresh
`data/` directory under a separately-pre-initialized target repository -
logically correct, but wasteful in a way that only became apparent testing
against a real, large repository (see the sibling plan doc's session notes -
a ~2.4 TB repository, only ~10 GB of its `data/` directory available for the
test): every byte migration reads from the old store, it also *writes
again*, even though - per the byte-store-compatibility finding above - those
bytes already exist at a valid position in `data/` and don't need to move
at all. Worse, for content whose bytes are simply missing (this specific
test's truncated `data/`), the tool cheerfully chunked, hashed, and
**stored** gigabytes of zero-filled placeholder bytes for data that will
never be recoverable - filling the target disk with content-free padding,
faster than a real (mostly-photo/video, low-duplication) migration would
have. It also surfaced a second, independent bug: on abort (e.g. that disk
filling up), the already-written new-store bytes were never cleaned up
despite the tool's own "target repository was left unchanged" message -
true only at the metadata level.

The fix redesigns the tool around a simple realization: **every chunk this
tool will ever identify already exists somewhere in the old `data/`
directory** (Scala already stored it, just without chunk-level dedup) - so
migration only ever needs to *read* bytes (to compute new chunk boundaries
and hashes), never write any. Concretely:

- `--repo` now points directly at the *existing* Scala repository root
  (already containing `data/` and `fsdb/`), not a separately-initialized
  empty target - `db::adopt_repository_in_place` (new, in `db/src/lib.rs`)
  adds a `meta/` directory alongside them, refusing only if `meta/` already
  exists; `data/` and `fsdb/` are never touched. This also removes the
  separate `backup init` step and the `--old-data`/`--chunk-buffer-mb`/
  `--allow-swap-risk` flags entirely (there's no buffering to bound anymore
  - see below) - replaced by `--cdc-target-size-bits`/`--chunking`, mirroring
  `backup init`'s own flags, since this tool now does its own equivalent
  initialization.
- Chunking uses `cdc::HashingChunker` (hashes and discards each chunk's
  bytes - previously "kept as ... a plausible fit for a future dedup-only
  use case" per its own doc comment, and now exactly that) instead of
  `store.rs`'s `SpillingHashingChunker`, which exists specifically to retain
  a completed chunk's bytes for writing - no longer needed here, along with
  the RAM budget/spill-directory machinery that came with it.
- A new `map_to_old_store_extents` function translates a chunk's logical
  byte range (its offset within the concatenation of a file's `DataEntries`
  parts, tracked as chunks are resolved) into the corresponding absolute
  byte extent(s) in the *old* data store - usually one, but more if the
  chunk happens to straddle a `parts` boundary (Scala's own per-file storage
  isn't always contiguous), exactly like a `store` run's own multi-extent
  `ChunkRef::New` already supports. A dedup hit needs no extent at all (an
  existing `chunk_id` is reused, same as before); a miss's `ChunkRef::New`
  now carries extents pointing at bytes already on disk instead of bytes
  just written.
- Redundant physical bytes (two old whole files sharing a CDC chunk Scala's
  own whole-file dedup never noticed) are simply never referenced by more
  than one `chunk_extents` row - orphaned but harmless, exactly the same
  "loser's bytes never referenced" pattern `store.rs`'s own chunk-write-race
  handling already accepts elsewhere in this codebase. Missing data reads
  back as zero bytes both during migration (for hashing) and later (`check`/
  `restore`/`mount`) directly from the gap in `data/` - no placeholder bytes
  are ever materialized, at any repository size.
- **Failure cleanup, simplified along with everything else**: since nothing
  is ever written to `data/`, the only thing a failed run needs to clean up
  is the `meta/` directory it created - `run_migrate_scala_repo` now does so
  itself (best-effort `remove_dir_all`) before returning, so a re-run needs
  no manual intervention, restoring the "just re-run from scratch" promise
  that a growing, never-cleaned-up `data/` had quietly broken.

This is also strictly cheaper for the *plausibility-check* end of things:
the old design's disk and time cost scaled with total *logical* content
size (including unrecoverable/missing data, which could dwarf what's
actually present); the new design's cost is dominated by how much is
actually there to hash, and never grows the repository beyond what a
`store` run into an initially-empty repository would need to reach the same
logical state.

## Update: H2 `U&'...'` Unicode-escape string literals

Found testing against the same real repository referenced above: any
`TreeEntries.name` (or other string value) containing a non-ASCII character
- German filenames with umlauts, in this case, but the same applies to any
non-ASCII content - is emitted by H2's `Script` tool not as a plain
`'...'` literal, but as a SQL:2008 Unicode-escape string literal,
`U&'...'`, keeping the script file itself pure ASCII: `''` is a literal
quote (same doubling convention as a plain string), `\\` a literal
backslash, `\XXXX` a 4-hex-digit Unicode code point, `\+XXXXXX` a
6-hex-digit one (code points beyond the Basic Multilingual Plane, e.g.
emoji). `script_import::parse_value` didn't recognize this prefix at all
before this fix, so the *entire* `INSERT` statement containing even one such
string failed to parse - not a rare edge case: **92,513** occurrences in the
real export this was found against (one real German-language photo/document
archive). Fixed by `script_import::parse_unicode_escaped_string`, with unit
tests covering all four escape forms plus the prefix's case-insensitivity
(`u&'...'`).

## Migration flow, as originally built

*(Superseded by the first "Update" section above - kept for context on what
changed and why. Steps 1-2 are unaffected by either update; step 3
originally wrote bytes into a fresh target repository's `data/` directory,
now it reads-only and points metadata at the old repository's own `data/`
in place.)*

1. `--script`/`--old-data` point at the SQL export and the old repository's
   `data/` directory; the target is the already-initialized (via `backup
   init`), *empty* repository named by the global `-r`/`--repo` flag - the
   tool refuses to run against a repository that isn't already initialized,
   or that already has tree entries besides the root (no merging support).
2. `script_import::build_staging_db` parses the script into a temporary,
   `tempfile`-backed staging SQLite database.
3. `Migration::walk_directory` walks the staging tree depth-first from the
   root (both active *and* soft-deleted entries - see "Decisions" below),
   recreating each entry in the target repository via the new
   `db::insert_historical_tree_entry` (see below).
4. For each old file entry with a non-`-1`, non-`NULL` `dataId`: its
   `DataEntries` parts are read (in `seq` order, zero-size "blacklisted"
   parts filtered out exactly like Scala's own `Database.parts`) from the
   old `data/` directory via a read-only `store::LongTermStore`, then
   chunked/hashed/deduplicated through the same pipeline `store`'s own
   `store` command uses, writing into the *new*, initially-empty
   repository's own `data/` directory - the old one was only ever opened
   read-only. A `dataId` shared by several old tree entries (Scala's own
   whole-file dedup) was only ever chunked once, cached by old `dataId` ->
   new `content_id` (`Migration::data_id_cache`), reused across active and
   soft-deleted entries alike.
5. A `dataId` whose `DataEntries` rows have all had their storage allocation
   removed (`start == stop == 0`, the "blacklisting" mechanism -
   `Database.removeStorageAllocation`) has no recoverable bytes; that one
   tree entry is skipped with a warning rather than aborting the run or
   inventing placeholder content.
6. The whole walk runs inside one write transaction, committed only at the
   very end.
7. A summary is printed: counts (directories/files, active/soft-deleted,
   empty files, skipped entries, warnings), and the headline number this
   tool exists to surface - old (Scala whole-file-dedup) storage actually
   referenced by the migrated content vs. new (Rust chunk-dedup) storage
   actually needed, and the resulting savings.

### `db` crate changes made to support this

- `db::apply_backup_batch`'s per-record chunk/content dedup logic was
  factored out into a new `pub fn db::resolve_content` (chunks/contents
  insert-or-get, race-safe the same way `apply_backup_batch` already was).
  `apply_backup_batch` itself now just calls it - behavior unchanged, tests
  unchanged. The migration tool calls `resolve_content` directly rather than
  `apply_backup_batch`, since `apply_backup_batch`'s `tree_entries`
  insert/replace logic is built for an *incremental* backup run (exactly one
  active entry per name, replaced on content change) and doesn't fit
  replaying a full historical tree.
- A new `pub fn db::insert_historical_tree_entry` inserts a `tree_entries`
  row with an explicit `deleted_at`, for either `kind`, without
  `apply_backup_batch`'s active-name conflict/replace handling. Each old
  Scala row maps to exactly one call, independent of any other row for the
  same name - Scala's own `UNIQUE (parentId, name, deleted)` constraint
  already guarantees at most one *active* old row per `(parentId, name)`,
  so the new schema's equivalent partial unique index is never at risk of a
  false conflict when replaying history this way, and no chronological
  ordering/merging between old rows sharing a name is needed.
- `pub fn db::adopt_repository_in_place` (added for the first "Update"
  above): like `init_repository`, but requires only that `repo_root/meta`
  doesn't exist yet, not that `repo_root` itself doesn't - for adopting a
  directory (the old Scala repository root) that already has unrelated
  content (`data/`, `fsdb/`) alongside where `meta/` will go.

## Decisions made for the original implementation

(Resolves the two open questions the original rough plan left open, plus
one implementation detail worth recording. Decision 2's *mechanism* changed
with the in-place-adoption update above - nothing is written to `data/`
anymore, so there's no risk of a partially-migrated `data/` to roll back -
but the *promise* it makes ("just re-run from scratch, no manual cleanup")
is unchanged, now upheld by removing the freshly-created `meta/` on failure
instead of by one all-or-nothing transaction over a separate target's
`data/`.)

1. **Migrate everything, including soft-deleted/historical entries** - not
   just the currently-active tree. `walk_directory` visits every child
   (`deleted = 0` or not) of every directory it visits, preserving
   restore-from-history capability. A tree entry's own historical
   `deleted`/`time` values are carried over as-is (`deleted_at`/`time` on
   the new row), not synthesized from anything else.
2. **No resumability for v1.** A failure partway through aborts the whole
   run and requires no manual cleanup before retrying - see above for how.
3. **`ref_count`s need no special handling.** `chunks.ref_count`/
   `contents.ref_count` are maintained by the same SQL triggers
   (`migrations.rs`) regardless of what inserted the row - `resolve_content`
   and `insert_historical_tree_entry` trigger them exactly like
   `apply_backup_batch`/`insert_directory` do for an ordinary `store` run.

### Other implementation notes not called out as open questions in the rough plan

- **Single-threaded.** Unlike `store`'s parallel chunking pipeline, the
  migration walk and all chunking happen sequentially on one thread, using
  one write connection/transaction throughout. This trades away some
  performance on a very large repository for a much simpler implementation
  and failure story - a reasonable tradeoff for a batch, one-shot tool. If
  this ever needs to be faster, parallelizing the per-`dataId` chunk-and-hash
  step (independent work per old `dataId`, same shape as `store.rs`'s
  per-file workers) would be the natural next step - now cheaper to justify
  than before the in-place update, since there's no write-space allocator
  to coordinate across threads anymore, only read-only hashing.
- **Blacklisted content.** Not mentioned in the original rough plan (found
  during Scala source review of `DataEntries`/`Database.parts`): a `dataId`
  whose storage was explicitly removed (`Backend.scala`'s "blacklisting")
  leaves a `DataEntries` row with `start == stop == 0` but a non-zero
  `length`. Treated as unrecoverable content: the affected tree entry is
  skipped (counted and warned about), not migrated with fabricated bytes.
- **A progress indicator** (`Progress`, time-throttled, printed at most once
  every two seconds) was added after testing against the real ~2.4 TB
  repository referenced above made clear how long a real run can take
  without any feedback - deliberately approximate (percentage of total old
  storage bytes read, not exact), per the request that motivated it.

## Testing

`cli/src/migrate_scala_repo.rs` has three test groups:

- `script_import::tests` - unit tests for the SQL parsing/staging-import
  layer (plain and explicit-column-list `INSERT`s, `X'...'` hash blobs,
  `U&'...'` Unicode-escape strings (all four escape forms, case-
  insensitivity), `NULL` handling, quote/comment-aware statement splitting,
  and the blacklisted-zero-size-part filter), independent of any
  repository.
- `map_to_old_store_extents_tests` - unit tests for the logical-range-to-
  old-store-extents translation, including a chunk straddling a `parts`
  boundary.
- `tests` - end-to-end integration tests against a hand-built fixture
  standing in for a real `fsc db-backup` export: a hand-written `.sql`
  script text in the same shape H2's `Script` tool produces (schema-
  qualified quoted identifiers, multi-row `VALUES`, `--` row-count
  comments, stray `CREATE`/`ALTER` noise), paired with a fake old `data/`
  directory written directly via `store::LongTermStore::write`. Covers a
  nested directory tree, a mix of active and soft-deleted entries, an
  empty file, a blacklisted (unrecoverable) entry, two ~100 KB files
  sharing a long common prefix and suffix but a different middle section
  (different whole-file content, so no dedup under Scala's model, but
  overlapping CDC chunks under Rust's), a zip-wrapped script export, the
  "meta/ already exists"/"no data/ directory" guards, that `data/` is
  provably never written to (byte-for-byte size comparison before/after),
  and that a failed run removes the incomplete `meta/` directory so an
  immediate re-run (with a corrected script) succeeds with no manual
  cleanup.
