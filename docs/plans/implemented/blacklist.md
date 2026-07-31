# Blacklist files

**Status**: implemented, as `backup blacklist add` / `backup blacklist
process` in `cli/src/blacklist.rs`.

## What it is (Scala reference)

A separate utility (`fsc blacklist` / `blacklist.bat`, `dedup.blacklist` in
`Main.scala`, `dedup.db.blacklist` in the `scala` checkout) for permanently
excluding specific content from the dedup store while keeping a record of
it in the tree - e.g. installer caches, `Thumbs.db`, virus-scanner
quarantine files, or anything else a user has decided is not worth backing
up but doesn't want silently reappearing on the next run.

Two things happen in Scala, either or both:
1. **Add external files to the blacklist**: files placed in a
   `blacklistDir` (default `blacklist`, resolved under `repo`) are hashed
   and copied into the tree under a `dfsBlacklist` directory (default
   `blacklist`, at the repository root) in a subdirectory named by
   timestamp, optionally deleting the originals (`deleteFiles`, default
   **true** - checked directly in `Main.scala`, not the `false` the initial
   stub of this doc assumed).
2. **Process the internal blacklist**: for every file already under
   `dfsBlacklist`, its storage allocation is removed (`db.
   removeStorageAllocation`) - reading the file afterwards yields all-zero
   bytes of the original length, the tree entry itself stays *visible*.
   Optionally (`deleteCopies`, default false) every *other* tree entry that
   shares the same content is also marked deleted, so the only trace left
   is the canonical copy under `dfsBlacklist` itself.

A database backup is taken first by default in Scala (`dbBackup`, default
true) - per the Scala README, restoring a database backup from *before* a
blacklisting run is only safe as long as no new files have been stored
since.

## Why this doesn't map directly onto the Rust dedup model

Scala dedupes whole files (one `dataId`/hash per file's full contents);
"remove this content's storage but keep the tree entry showing zeros" is a
natural operation there. The Rust rewrite dedupes at the CDC chunk level -
a blacklisted file's chunks may be shared with other, non-blacklisted files
entirely unrelated to whatever's being blacklisted (a common file header, a
zero-run, etc.), so "zero out this file's storage" doesn't have a clean
chunk-level equivalent: a chunk still referenced by something else can't be
zeroed without corrupting whatever else references it.

## Decision made for this implementation

**Plain soft-delete semantics, not Scala's "reads as zeros, entry stays
visible" behavior.** Processing a tree entry under the blacklist directory
runs the exact same [`db::soft_delete`] any other deletion in this codebase
already goes through (see `del.rs`), letting a later `reclaim-space` run
free any chunks that end up unreferenced - exactly like any other deletion.
This means the blacklist entry itself becomes inactive (not just
zero-filled-but-visible) once processed; there is no "zeroed but present"
content/tree-entry state anywhere in this implementation, and none was
added. This is a deliberate scope reduction agreed on before implementation
started, not an oversight - if a real need for the "reads as zeros, stays
listed" behavior shows up later, it should be designed deliberately as its
own feature rather than retrofitted here.

## What was built

- `backup blacklist add <BLACKLIST_DIR> [--dfs-blacklist NAME]
  [--delete-files]` - hashes and backs up `BLACKLIST_DIR`'s direct entries
  (files and subdirectories, each keeping its own name/structure) into the
  tree under `<dfs-blacklist>/<timestamp>` (`YYYYMMDD_HHMMSS`, UTC, via the
  existing `format::timestamp_for_filename`). Reuses `store::run_store`
  entirely for the hash-and-store pipeline, via a new
  `store::BackupArgs::for_paths` constructor (fixed, sensible defaults:
  create missing target dirs, default thread pool/RAM budget) - each direct
  entry of `BLACKLIST_DIR` is passed as its own source so the tree layout
  matches Scala's flat copy of the source directory's contents, rather than
  nesting everything one level deeper under `BLACKLIST_DIR`'s own name (how
  a plain single-source `store` run would place it).
  - `--delete-files` (default **off** - a deliberate deviation from Scala's
    `deleteFiles` default of `true`; deleting source data by default felt
    too easy to trigger by accident on a newly added command). When given,
    each original is deleted only after its corresponding tree entry is
    independently confirmed present in the database - not merely inferred
    from `run_store`'s overall exit code, which can still be a success even
    though an individual file was skipped with a logged warning (e.g.
    unreadable). Now-empty source subdirectories are removed afterward,
    same as Scala.
- `backup blacklist process [--dfs-blacklist NAME] [--delete-copies]
  [--backup]` - soft-deletes every active file entry under
  `<dfs-blacklist>`, and, with `--delete-copies`, every other active tree
  entry anywhere that shares a processed entry's `content_id` (via a new
  `db::entries_for_content` query - `db/src/query.rs` didn't have a way to
  look up "every active tree entry referencing this content" before this).
  - `--backup` (default **off**, another deliberate deviation from Scala's
    `dbBackup` default of `true`): since this command only ever
    soft-deletes tree entries - the same, already-reversible-until-
    `reclaim-space` mechanism `del` already uses without an automatic
    backup - there's no equivalent here to Scala's harder-to-reverse
    "zeroed but present" content mutation that motivated defaulting the
    backup on there. Kept as an opt-in flag anyway (mirroring
    `reclaim-space`'s own `--no-backup`, just inverted) for users who want
    the extra safety net for a bulk `--delete-copies` run.

Split into two subcommands (`add` and `process`) rather than one combined
command, since either half is independently useful (e.g. re-running
`process --delete-copies` without adding anything new) - grouped under one
`blacklist` parent subcommand with nested `Add`/`Process` variants, the
same pattern `db backup`/`db restore`/`db compact` already established in
this codebase for "one topic, several related actions".

## Deliberately left out

- Scala's "reads as zeros, entry stays visible" content state - see
  "Decision made for this implementation" above.
- Matching Scala's exact `blacklistDir`/`deleteFiles` defaults: this
  implementation requires `<BLACKLIST_DIR>` to be given explicitly (no
  default, and not resolved against `--repo` the way Scala's was - it's a
  plain filesystem path, resolved the same way every other filesystem-path
  argument in this CLI is), and defaults `--delete-files`/`--backup` to off
  rather than Scala's on, for the reasons above. `--dfs-blacklist` keeps
  Scala's `blacklist` default name.

## Testing

`cli/src/blacklist.rs`'s `tests` module covers both halves end to end
against a real temp repository (same helper style as `store.rs`/`del.rs`):
adding files (including dedup of identical content, and confirmed-original
deletion with empty-directory cleanup), processing (soft-delete of
blacklist entries, `--delete-copies` deleting/leaving-alone the right other
entries, no-op on a missing blacklist directory, the `--backup` flag
actually creating a backup file), and a combined add-then-process
end-to-end test. `db/src/query.rs` gained a matching unit test for
`entries_for_content`.
