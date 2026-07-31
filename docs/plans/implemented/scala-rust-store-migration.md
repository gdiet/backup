# Scala → Rust repository migration tool

**Status**: implemented. New subcommand `migrate-scala-repo`
(`cli/src/migrate_scala_repo.rs`, `MigrateScalaRepoArgs`/
`run_migrate_scala_repo`). Kept under `docs/plans/implemented/` as a record
of the compatibility findings, the design decisions taken, and how they
compare to the original rough plan.

## Is the byte store (`data/` directory) compatible? Yes.

Confirmed by direct comparison against the Scala source
(`store/LongTermStore.scala`'s `pathOffsetSize`) and Rust's own
`store::path_offset_size` (`store/src/lib.rs`) - both use the exact same
layout: 100,000,000-byte files (`fileSize`/`FILE_SIZE`, "must not change
without a migration script" per Scala's own comment), the same
`dir1/dir2/positionInFile` scheme (100 files per `dir2`, 100 `dir2`s per
`dir1`), the same path format. Rust's `store` crate even has a test
(`path_format_matches_scala`) asserting this. A Rust `store::LongTermStore`
opened read-only against an existing Scala `data/` directory reads its raw
bytes directly, at whatever byte positions the old metadata says to look -
this is exactly how `migrate_scala_repo.rs` reads old file content, with no
conversion step of any kind.

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
semicolon/comma splitting, `--`-comment stripping, `X'...'` hex blob and
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

## Migration flow, as built

1. `--script`/`--old-data` point at the SQL export and the old repository's
   `data/` directory; the target is the already-initialized (via `backup
   init`), *empty* repository named by the global `-r`/`--repo` flag - the
   tool refuses to run against a repository that isn't already initialized,
   or that already has tree entries besides the root (no merging support -
   see "Decisions" below on why re-running against a fresh repository is the
   only supported recovery path).
2. `script_import::build_staging_db` parses the script into a temporary,
   `tempfile`-backed staging SQLite database.
3. `Migration::walk_directory` walks the staging tree depth-first from the
   root (both active *and* soft-deleted entries - see "Decisions"),
   recreating each entry in the target repository via the new
   `db::insert_historical_tree_entry` (see below).
4. For each old file entry with a non-`-1`, non-`NULL` `dataId`: its
   `DataEntries` parts are read (in `seq` order, zero-size "blacklisted"
   parts filtered out exactly like Scala's own `Database.parts`) from the
   old `data/` directory via a read-only `store::LongTermStore`, then
   chunked/hashed/deduplicated through the same pipeline `store`'s own
   `store` command uses (`SpillingHashingChunker` + `db::find_chunk` +
   `chunk_store::write_chunk_from_cache` + the new `db::resolve_content`),
   writing into the *new*, initially-empty repository's own `data/`
   directory - the old one is only ever opened read-only. A `dataId` shared
   by several old tree entries (Scala's own whole-file dedup) is only ever
   chunked once, cached by old `dataId` -> new `content_id`
   (`Migration::data_id_cache`), reused across active and soft-deleted
   entries alike.
5. A `dataId` whose `DataEntries` rows have all had their storage allocation
   removed (`start == stop == 0`, the "blacklisting" mechanism -
   `Database.removeStorageAllocation`) has no recoverable bytes; that one
   tree entry is skipped with a warning rather than aborting the run or
   inventing placeholder content.
6. The whole walk runs inside one write transaction, committed only at the
   very end - see "Decisions" on why this was a deliberate choice beyond
   what was strictly required.
7. A summary is printed: counts (directories/files, active/soft-deleted,
   empty files, skipped entries, warnings), and the headline number this
   tool exists to surface - old (Scala whole-file-dedup) storage actually
   referenced by the migrated content vs. new (Rust chunk-dedup) storage
   actually written, and the resulting savings.

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

## Decisions made for this implementation

(Resolves the two open questions the original rough plan left open, plus
one implementation detail worth recording.)

1. **Migrate everything, including soft-deleted/historical entries** - not
   just the currently-active tree. `walk_directory` visits every child
   (`deleted = 0` or not) of every directory it visits, preserving
   restore-from-history capability. A tree entry's own historical
   `deleted`/`time` values are carried over as-is (`deleted_at`/`time` on
   the new row), not synthesized from anything else.
2. **No resumability for v1.** A failure partway through aborts the whole
   run: `walk_directory` returns a plain `Result<(), String>`, and any error
   drops the migration's single write transaction (started once, up front)
   without committing, so the target repository is left exactly as empty as
   it started - "re-run from scratch against a fresh, empty target
   repository" needs no extra cleanup step, which is why one big transaction
   (rather than incremental batched commits, as `store`'s own writer thread
   uses) was chosen here: it was the simplest way to get a clean,
   non-partially-migrated failure state for free, not just the minimum bar
   the brief asked for.
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
  and failure story (see decision 2) - a reasonable v1 tradeoff for a
  batch, one-shot tool per the "no resumability needed" framing. If this
  ever needs to be faster, parallelizing the per-`dataId` chunk-and-store
  step (independent work per old `dataId`, same shape as `store.rs`'s
  per-file workers) would be the natural next step.
- **Target repository must pre-exist and be empty.** The tool does not
  create/initialize the target repository itself (unlike the rough plan's
  phrasing "a fresh Rust repository" might suggest) - it requires
  `backup init` to have already been run, and refuses to proceed if the
  target already has any tree entries besides the root. This avoids ever
  having to define merge semantics between pre-existing and migrated
  content.
- **Blacklisted content.** Not mentioned in the original rough plan (found
  during Scala source review of `DataEntries`/`Database.parts`): a `dataId`
  whose storage was explicitly removed (`Backend.scala`'s "blacklisting")
  leaves a `DataEntries` row with `start == stop == 0` but a non-zero
  `length`. Treated as unrecoverable content: the affected tree entry is
  skipped (counted and warned about), not migrated with fabricated bytes.

## Testing

`cli/src/migrate_scala_repo.rs` has two test groups:

- `script_import::tests` - unit tests for the SQL parsing/staging-import
  layer (plain and explicit-column-list `INSERT`s, `X'...'` hash blobs,
  `NULL` handling, quote/comment-aware statement splitting, and the
  blacklisted-zero-size-part filter), independent of any repository.
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
  overlapping CDC chunks under Rust's), a zip-wrapped script export, and
  the "not yet initialized"/"not empty" target-repository guards.
