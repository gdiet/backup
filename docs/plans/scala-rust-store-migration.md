# Scala → Rust repository migration tool

**Status**: not started - this is a rough plan, not a detailed one, per
request. Answers "is the Long Term Store compatible?" first, since that
determines most of the rest.

## Is the byte store (`data/` directory) compatible? Yes.

Confirmed by direct comparison against the Scala source
(`store/LongTermStore.scala`'s `pathOffsetSize`) and Rust's own
`store::path_offset_size` (`store/src/lib.rs`) - both use the exact same
layout: 100,000,000-byte files (`fileSize`/`FILE_SIZE`, "must not change
without a migration script" per Scala's own comment), the same
`dir1/dir2/positionInFile` scheme (100 files per `dir2`, 100 `dir2`s per
`dir1`), the same path format. Rust's `store` crate even has a test
(`path_format_matches_scala`) asserting this. **A Rust `store::
LongTermStore` can read an existing Scala `data/` directory's raw bytes
directly, at whatever byte positions the old metadata says to look.**

## Is the metadata (database) compatible? No, on two independent levels.

1. **Engine**: Scala uses H2 (a Java-embedded SQL database, proprietary
   binary file format); Rust uses SQLite. Not readable by Rust directly.
2. **Content model** (the bigger one - see `db/src/migrations.rs` for the
   Rust schema, and the Scala README's "Storage Format" section /
   `store/LongTermStore.scala` for Scala's): Scala dedupes **whole
   files** - one `DataEntries` row per distinct file content, `hash` is
   the **MD5 of the entire file**, and a `DataEntries` id can span
   multiple storage *parts* (`seq` 1..n) only because of **fragmented
   free space reuse**, never because of content chunking. Rust dedupes at
   the **CDC (content-defined chunking) level** - `contents` (one row per
   distinct whole-file byte sequence, like Scala's `DataEntries`) is
   built from `chunks` (one row per distinct *chunk*, blake3-hashed,
   shared across any content that happens to contain that chunk) via
   `content_chunks`. Two files that differ by one byte dedupe *nothing*
   under Scala's model but dedupe *almost entirely* under Rust's. There
   is no lossless, purely-mechanical translation from one to the other -
   migrated content genuinely needs to be **re-chunked and re-hashed**,
   exactly as the request that spawned this doc assumed.

## Getting the H2 metadata out without an H2 reader in Rust

No need to implement an H2 file-format reader. Scala already ships
`fsc db-backup` (`db/maintenance.scala`'s `sqlDbBackup`), which runs H2's
own `org.h2.tools.Script` to produce a **zipped, portable SQL script**
(plain `INSERT INTO ...` statements reconstructing `TreeEntries` and
`DataEntries`) - this is the exact interchange format needed, and it
already exists with no new Scala-side work. Plan:

1. User runs `fsc db-backup` (or points the tool at an existing one) to
   get the zipped SQL script.
2. The migration tool unzips it and loads the `TreeEntries`/`DataEntries`
   `INSERT` statements into a **temporary staging SQLite DB** with tables
   shaped like Scala's schema (see the Scala README's "Storage Format"
   for the exact columns) - lets the rest of the tool use normal SQL
   queries against the old data instead of hand-parsing a script, and
   sidesteps needing an H2 driver entirely. Likely needs small syntax
   massaging (H2 vs. SQLite `INSERT` dialect differences, if any show up
   in practice) but the statements are simple enough this should be
   mechanical.

## Rough migration flow

1. Load the old tree via the staging DB (as above).
2. Walk it depth-first, recreating directories in a fresh Rust repository
   (`db::insert_directory`).
3. For each old file entry with a non-null `dataId`: look up its
   `DataEntries` parts (`(start, stop)` ranges, in `seq` order), read
   those bytes from the **old** `data/` directory via Rust's own
   `store::LongTermStore` (works out of the box per the compatibility
   finding above), and run them through the same chunk-and-store pipeline
   `store.rs`/`cli::mount`'s persist path already use
   (`cli::spilling_chunker::SpillingHashingChunker` + `db::find_chunk`
   dedup + `chunk_store::write_chunk_from_cache`/`SpaceAllocator` +
   `apply_backup_batch` - see `docs/plans/bounded-memory-io-pipeline.md`
   for why chunk buffering goes through a spillable `WriteCache` rather
   than a plain in-memory buffer) - writing into a **new**, empty `data/`
   directory and a **new** repository, not in place. Multiple old tree
   entries sharing one `dataId` should only be chunked once (cache the
   old-`dataId` → new-`content_id` mapping).
4. Report a summary at the end, notably **space saved by chunk-level
   dedup**: sum of old `DataEntries` storage actually referenced by
   *active* tree entries (the bytes a naive whole-file copy would need)
   vs. the new repository's actual stored size after chunk dedup - the
   request that prompted this doc specifically expects this number to be
   interesting (files sharing partial content but not full-file hashes,
   which the whole-file model can never dedupe, should now collapse
   together at the chunk level).

## Open questions for when this gets planned in detail

- Whether to also migrate soft-deleted entries (Scala's `deleted != 0`
  rows) or only the active tree - migrating everything preserves restore-
  from-history capability but processes content nobody currently needs.
- Whether `ref_count`s (`chunks`/`contents`) can be computed purely from
  the migrated tree structure (almost certainly yes, same as any fresh
  `store` run) or need special handling.
- Progress/resumability for a large repository - this is a batch,
  presumably long-running, one-shot tool; whether it needs to survive
  being interrupted partway through is a real design question once sized
  against real repository data.
