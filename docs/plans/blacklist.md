# Blacklist files

**Status**: not started - this is a stub, not a plan. Not present in the
Rust `cli` at all.

## What it is (Scala reference)

A separate utility (`fsc blacklist` / `blacklist.bat`, `dedup.blacklist` in
`Main.scala`, `dedup.db.blacklist` in the `scala` checkout) for permanently
excluding specific content from the dedup store while keeping a record of
it in the tree - e.g. installer caches, `Thumbs.db`, virus-scanner
quarantine files, or anything else a user has decided is not worth backing
up but doesn't want silently reappearing on the next run.

Two things happen, either or both:
1. **Add external files to the blacklist**: files placed in a
   `blacklistDir` (default `blacklist`, resolved under `repo`) are hashed
   and copied into the tree under a `dfsBlacklist` directory (default
   `blacklist`, at the repository root) in a subdirectory named by
   timestamp, optionally deleting the originals (`deleteFiles`, default
   true).
2. **Process the internal blacklist**: for every file already under
   `dfsBlacklist`, its storage allocation is removed (`db.
   removeStorageAllocation` - the Scala equivalent of what this project
   calls soft-delete/reclaim, but applied to the *content*, not the tree
   entry: reading the file afterwards yields all-zero bytes of the
   original length, the tree entry itself stays). Optionally
   (`deleteCopies`, default false) every *other* tree entry that shares
   the same content is also marked deleted, so the only trace left is the
   canonical copy under `dfsBlacklist` itself.

A database backup is taken first by default (`dbBackup`, default true) -
per the Scala README, restoring a database backup from *before* a
blacklisting run is only safe as long as no new files have been stored
since.

## Why this doesn't map directly onto the Rust dedup model

Scala dedupes whole files (one `dataId`/hash per file's full contents,
see `docs/plans/scala-rust-store-migration.md`); "remove this content's
storage but keep the tree entry showing zeros" is a natural operation
there. The Rust rewrite dedupes at the CDC chunk level - a blacklisted
file's chunks may be shared with other, non-blacklisted files entirely
unrelated to whatever's being blacklisted (a common file header, a
zero-run, etc.), so "zero out this file's storage" doesn't have as clean
a chunk-level equivalent: reclaiming *only* the chunks unique to the
blacklisted file (via the existing `ref_count` machinery) is closer to
what `reclaim-space` already does than to a new standalone mechanism -
worth designing deliberately rather than assuming a 1:1 port once this is
actually planned.

## Rough shape if/when planned

- Reuse `db::soft_delete`/`reclaim_space` machinery rather than inventing
  a parallel "zeroed but present" content state, if a real need for the
  "reads as zeros" behavior (as opposed to "just delete it") shows up.
- The "add external files, hash, place under a timestamped tree
  directory" half is close to what `store` already does and could mostly
  reuse it.
- `deleteCopies` (delete every other entry sharing this content) needs a
  "find all tree entries referencing this `content_id`" query - not
  currently exposed anywhere in `db`.
