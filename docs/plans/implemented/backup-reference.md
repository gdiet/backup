# `store`: reference-based backup

**Status**: implemented. `--reference`/`--force-reference` on `cli store`
(`cli/src/store.rs`: `resolve_reference`, `validate_reference`,
`matching_reference`, the `ref_ids`/`reference_hits` threading in
`walk_and_create_dirs`). Kept as a record of the Scala comparison and the
design decisions taken.

`.backupignore` used to be covered here too; it shipped separately, see
`docs/plans/implemented/backupignore.md`.

## Context

Scala's `reference`/`forceReference` (`BackupTool.scala`, see `backup`
lines 65-104, `process`/`processFile` lines 111-219, `validateReference`
lines 244-266, `findReferenceId` lines 284-296; user docs in
`scala/README.md:195-205`) lets a backup run skip reading/hashing/chunking
a source file entirely if a same-named file already exists at the
corresponding path in a **reference tree already inside the same
repository** (typically the previous backup run's target directory), with
matching size and mtime - in that case it just creates a new tree entry
pointing at the reference file's existing content, no I/O on the source
file at all.

This is worthwhile *in addition to* Rust's chunk-level dedup, not a
substitute for it: even with perfect dedup, an unchanged file still costs a
full read + CDC chunk + blake3 hash of every byte on every `store` run just
to *discover* that it dedupes against what's already stored. A metadata-only
check (size + mtime, both already available from a `stat()` the walk
already needs) skips that discovery cost entirely for the common case
(nightly backup of a mostly-unchanged tree). Chunk-level dedup still caps
the wasted *storage* on a miss; this only saves re-read/re-hash *time* on a
hit.

Caveat carried over unchanged from Scala: if a file's contents changed but
its size and mtime happen not to have (a real possibility, not just a
theoretical one - some tools preserve mtime on save, or two different
contents can coincidentally land on the same size), the changed content is
**not** detected and **not** stored.

## Reference resolution (`findReferenceId`/`validateReference`)

`reference=<path>` resolves against the repository tree **before** any
source is touched, in two steps:

1. **Wildcard resolution, per path segment** (`findReferenceId`,
   `BackupTool.scala:284-296`): `reference`'s `/`-separated path is resolved
   one segment at a time, starting from the repository root. At each
   segment, every **directory** child of the current node is wildcard-
   matched (`*`/`?`, the same `createWildcardPattern` used by
   `.backupignore` - already ported to Rust as `backup_ignore::wildcard_match`)
   against that segment; the **alphabetically last** match becomes the next
   node. E.g. `/backup/????/????.??.??_*` resolves the year segment to the
   latest-named year directory, then within it the timestamp segment to the
   latest-named run - a deliberate "pick the most recent matching backup"
   convenience, not just a single wildcard-glob lookup. A segment with no
   matching directory, or a fully-resolved reference directory with zero
   children, is a hard error (fails before touching any source).
2. **Fuzzy similarity check** (`validateReference`, `BackupTool.scala:244-266`),
   skipped when `forceReference=true`: builds two comparable listings -
   the reference directory's top-level children (files marked, e.g. with a
   `:` prefix, to distinguish same-named files/dirs; directories bare),
   plus one extra level into any reference subdirectory whose name matches
   one of the actual sources' basenames - and the mirror image from the
   real source paths (top-level basenames, plus one level into any source
   directory whose basename matches one of the reference's top-level
   directories). Fails if the two listings overlap too little
   (`max(sizes) > intersection * 1.6 + 1`) - a guard against a typo'd or
   unrelated `reference` silently "working" (matching almost nothing, so
   every file falls back to a full read/hash anyway, quietly defeating the
   whole point) rather than being caught up front.

## Per-file matching, during the walk (`process`/`processFile`)

Once resolved, the reference directory's id is threaded down through the
tree in parallel with the target id, by name - not re-resolved with
wildcards at each level, only the top segment is. For each directory
visited, `fs.child(referenceId, name)` (if the current reference id exists
and has such a child) becomes the reference id handed to *its* children;
for each file, the same lookup (if it resolves to a `FileEntry`) is the
*candidate* reference file. `processFile` then compares the candidate's
size and mtime against the source file's; on a match, it copies the
reference entry's `dataId` (Scala's whole-file content pointer) into a new
tree entry instead of reading/writing the source file's bytes at all
(`Backend.copyFile`, `Backend.scala:81-86` - literally `db.mkFile(...,
file.dataId)`, a metadata-only operation, no store I/O).

## Design for Rust

### `db` crate: `FileBackupRecord` needs a second content path

`db::FileBackupRecord` currently always carries `chunks: Vec<ChunkRef>` +
`content_hash: Vec<u8>`, resolved into a `content_id` by
`resolve_content` (`db/src/backup.rs:77-141`) inside `apply_backup_batch`.
A reference hit already knows the `content_id` directly (the reference
tree entry's own `content_id`) - there's nothing to resolve, no chunk or
content-hash work needed at all. Replace the two fields with an enum so
`apply_backup_batch`'s insert/refresh/soft-delete-and-replace logic (already
keyed purely on `content_id` equality, lines 174-215) stays exactly as-is,
just fed a `content_id` from either source:

```rust
pub enum ContentSource {
    /// Chunked/hashed/deduplicated normally; `apply_backup_batch` calls
    /// `resolve_content` to turn this into a `content_id`.
    Resolved { chunks: Vec<ChunkRef>, content_hash: Vec<u8> },
    /// Already-known `content_id`, reused as-is - e.g. copied from a
    /// reference file's tree entry, skipping chunking/hashing entirely.
    /// `None` for an empty file (mirrors `Resolved` with empty `chunks`).
    Known(Option<i64>),
}
```

`FileBackupRecord { parent_id, name, time_millis, content: ContentSource }`.
Update the three existing call sites that construct one directly:
`cli/src/store.rs`'s `process_file`/`read_and_chunk`, and `cli/src/mount.rs`'s
three (the empty-file-on-create path, the phase 2b persist path, and a test
helper) - all become `ContentSource::Resolved { chunks, content_hash }`,
unchanged in behavior.

### `cli`: new query support

Reference resolution needs two things `db::query` doesn't expose yet as
reusable functions, both cheap wrappers over what's already there:

- Per-segment wildcard directory resolution: use `db::query::list_children`
  (already sorted `kind ASC, name` - directories first, ascending
  alphabetically), filter to `EntryKind::Dir`, wildcard-match names via
  `backup_ignore::wildcard_match`, take the *last* match (already ascending,
  so this is the alphabetically-last one, matching Scala's `.sortBy(_.name)
  .lastOption`). No new `db` function needed - a private helper in
  `store.rs` composing `list_children` is enough.
- Size comparison: `db::query::file_size(conn, &entry)` already exists,
  used exactly as-is.

### `cli`: CLI flags

- `--reference <path>` (`Option<PathBuf>`, repository path syntax like
  `target` - parsed via `.components()`, not a filesystem path): the
  wildcard reference pattern.
- `--force-reference`: skip `validate_reference`. No-op if `--reference`
  wasn't given.

### `cli`: walk integration

`walk_and_create_dirs` already threads one `HashMap<PathBuf, i64>`
(`dir_ids`) and, since `.backupignore`, a second (`ignore_scopes`) through
a single flat `WalkDir` iteration, keyed by directory path. Add a third,
`ref_ids: HashMap<PathBuf, i64>`, populated the same way: seeded at depth 0
from the resolved top-level reference id (in place of a parent lookup,
mirroring how depth 0 uses `target_id` directly instead of a `dir_ids`
lookup); for each directory, if the parent has a reference id, look up a
same-named **directory** child via `find_tree_entry` and record it for that
directory's own children.

For each **file** entry: if the parent has a reference id, look up a
same-named **file** child via `find_tree_entry`. If found, compare
`db::query::file_size` and the entry's `time_millis` against the source's
own (one `std::fs::metadata` call - already needed for any file that has a
candidate, not an extra cost for files that don't). On a match, build a
`FileBackupRecord` with `ContentSource::Known(entry.content_id)` right there
(everything needed - `parent_id`, `name`, `time_millis`, `content_id` - is
already in hand, no source file I/O at all) and collect it into a new
`reference_hits: Vec<FileBackupRecord>` output, *instead of* pushing into
`files`. On no match (or no candidate, or no reference active), fall
through to the existing `files.push(...)` path unchanged.

This is a deliberate architectural adaptation, not a literal port: Scala
resolves and applies the reference decision inline, per file, interleaved
with the rest of `processFile`'s I/O; Rust's `store` already separates a
single-threaded structural walk (`walk_and_create_dirs`, producing
`tree_entries` rows and a flat file list) from parallel per-file I/O
workers (`process_file`). Reference matching is pure metadata (no source
bytes touched), so it belongs entirely in the structural-walk phase - a
reference hit never reaches a worker at all, it's just applied directly.

### `cli`: applying reference hits

`reference_hits` are metadata-only inserts, need no store writes, and are
already fully resolved - apply them with one `db::apply_backup_batch` call
against `main_conn`, right after `walk_and_create_dirs` returns and before
`main_conn` is moved into the writer thread (`run_store`,
`cli/src/store.rs:~298-313`). No channel/writer-thread involvement needed
for these at all.

### `cli`: `validate_reference`

Ports `BackupTool.scala:244-266`'s fuzzy-listing comparison as a private
`store.rs` function: build the reference-side listing via
`db::query::list_children` (two levels: the reference root, plus one level
into any child directory whose name matches a source basename) and the
source-side listing via `std::fs::read_dir` (mirrored structure), applying
the same `:`-prefix-for-files / bare-for-dirs convention so same-named
files and directories don't collide in the comparison sets, then the same
`max(len_a, len_b) as f64 > intersection as f64 * 1.6 + 1.0` threshold.

## Suggested sequencing

1. `db`: `ContentSource` enum, update `apply_backup_batch` and the three
   existing `FileBackupRecord` construction sites in `store.rs`/`mount.rs`.
2. `cli`: `resolve_reference` (wildcard per-segment resolution) + unit
   tests.
3. `cli`: `validate_reference` (fuzzy listing comparison) + unit tests.
4. `cli`: `--reference`/`--force-reference` flags, wire resolution +
   validation into `run_store` before the walk.
5. `cli`: `ref_ids` threading + per-file match/skip logic in
   `walk_and_create_dirs`, `reference_hits` output, applied via
   `apply_backup_batch` before the writer thread starts.
6. Integration tests in `store.rs`'s existing test module: a reference hit
   skips hashing (assert via shared `content_id`, not just equal bytes), a
   size/mtime mismatch falls back to normal processing, `--force-reference`
   bypasses a failing validation, an unresolvable reference path is a hard
   error before any source is touched (matching the existing
   `run_store_fails_fast_for_*` test style).

## Verification

- `cargo fmt --check && cargo clippy -- -D warnings && cargo test &&
  cargo doc --no-deps`, per `AGENTS.md`.
- Update `README.md`'s "Back Up Files And Directories" section with
  `--reference`/`--force-reference` docs, mirroring
  `scala/README.md:195-205`'s user-facing explanation.
- Once shipped, move this file under `docs/plans/implemented/`.
