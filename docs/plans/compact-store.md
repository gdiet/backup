# `compact-store`: defragment the data store's physical layout

**Status**: proposed, not implemented. Requested alongside a broader
question - whether every command already leaves the repository in a clean,
resumable state if killed (`SIGINT`, `SIGKILL`, or power loss) mid-run. That
question was answered and acted on first (see "Crash-safety today" below) -
this plan is the deferred feature itself.

## The problem

`reclaim-space` hard-deletes soft-deleted `tree_entries` and purges
`chunks`/`contents` rows that drop to `ref_count = 0`, but per
`docs/plans/implemented/03-chunk-extents.md`, the physical bytes those
chunks occupied in `store::LongTermStore` are never actively moved -
they just become gaps that `db::free_space_summary` can report and that a
*future* `store` run's `SpaceAllocator` may or may not end up reusing,
depending on whether a later chunk happens to fit. A repository that had a
lot of data deleted and reclaimed, but sees little or no new `store`
activity afterward, keeps that freed space as permanent holes in its
physical files - `store::LongTermStore` never shrinks a file, so the
freed space is never returned to the OS.

`compact-store` closes that gap on demand: move every live chunk's bytes
so the store's used address space becomes one contiguous block starting at
0, then truncate the physical files down to that new, smaller size -
turning `free_space_summary`'s reported gaps into actual reclaimed disk
space, the way `db compact` (`PRAGMA incremental_vacuum` on the *metadata*
database - unrelated, different subsystem, see "Naming" below) already
does for the SQLite file.

## Crash-safety today

Before designing this, audited every state-mutating command for whether it
already survives being killed mid-run (`SIGINT`/`SIGKILL`/power loss) in a
resumable, non-corrupting state. Findings, now all fixed and shipped
(commit `4c10d7a0`):

- **Already safe by construction**: `store`, `mount --read-write`'s persist
  path, `del`, `reclaim-space` - all follow "write physical bytes to an
  unreferenced location first, commit the one DB transaction that
  references them second." SQLite's WAL rollback undoes an uncommitted
  transaction on its own; leftover unreferenced bytes from an interrupted
  write are harmless garbage, later overwritable.
- **Five real gaps found and fixed**: `db restore` and `db backup`'s zip
  step now stage to a temp path and `fs::rename` into place atomically
  instead of writing the live/final file directly;
  `fix-problems --replace-empty` now wraps soft-delete + empty-file
  insert in one transaction
  (`db::soft_delete_and_replace_with_empty`); `undelete` now wraps
  relocate + reactivate in one transaction; repository creation
  (`init_repository`/`adopt_repository_in_place`) now builds `meta/`
  under a `meta.tmp/` staging directory and atomically renames it into
  place, so a killed schema-creation window can never leave a half-valid
  `meta/` behind.
- **One accepted, documented exception**: `migrate_scala_repo`'s own
  long-running migration transaction. If killed *during* that transaction
  (after `adopt_repository_in_place` already succeeded), `meta/` is left
  validly existing but effectively empty, and a re-run's own
  `RepositoryAlreadyExists` check refuses to continue automatically - the
  existing "remove `meta/` and re-run" hint is the correct manual recovery
  path. This matches what was floated as a plausible exception up front
  ("vielleicht mit Ausnahme von Migration"): a one-shot, explicitly
  supervised operation, not part of normal repeated command usage.

So the established, proven pattern this plan should follow is: **write new
bytes to an unreferenced location, then commit one DB transaction that
switches the pointer** - never the reverse, and never split a single
logical move across more than one commit.

## Existing building blocks

Nothing here needs new infrastructure at the data-model level - the
multi-part `chunk_extents` design already built for gap-*reuse* (not
gap-*elimination*) provides everything a "move a chunk" step needs:

- `db::chunk_extents_sorted`/`cli::chunk_store::SpaceAllocator` - already
  compute every gap (plus the open trailing region) from the DB's current
  extents. The same computation, read the other way, also identifies
  which chunks currently sit at the *highest* positions (the ones to move
  first).
- `cli::chunk_store::read_chunk_bytes`/`write_chunk_from_cache` - already
  the only sanctioned way to move a chunk's bytes in or out of the store
  across however many extents it has. `write_chunk_from_cache` already
  reserves target space via the allocator and writes in bounded pieces.
- `cli::io_limiter::IoLimiter` - already exists to bound I/O concurrency
  against a possibly-slow repository disk; the large sequential read/write
  volume `compact-store` would generate is exactly the case it was built
  for.
- `cli::progress::Progress` - already the shared progress-bar type used by
  `check`/`problems`/`migrate-scala-repo`; a long compact run should use
  it too.

## Sketch

For each live chunk currently occupying the highest not-yet-packed
position (repeat until none remain above the target size):

1. Read the chunk's current bytes (`read_chunk_bytes`).
2. Reserve new space from the allocator, restricted to gaps *below* the
   chunk's current position (never move a chunk to a higher address - that
   would undo progress). Write the bytes there
   (`write_chunk_from_cache`-shaped write, to a still-unreferenced
   location).
3. In one DB transaction, replace that chunk's `chunk_extents` rows with
   the new ones.
4. The chunk's old position is now free - it either becomes a gap for a
   later step in this same run, or, once it's the highest-addressed thing
   left, becomes part of the truncatable tail.

A useful side effect: since step 2 can (and by construction, once enough
gap space accumulates below the tail, generally will) reserve a *single*
contiguous target range, a multi-extent chunk (one that only exists
because it was written by `store`'s own gap-spanning allocator) tends to
get coalesced back into one extent as it's moved - `compact-store`
incidentally un-fragments individual chunks, not just the store as a
whole.

Once no live chunk_extents remain at or above the new target size, the
address space `[0, target_size)` holds exactly the live data and
everything past it is dead. Truncate the physical files down to
`target_size`: delete whole shard files entirely past it, `set_len()`
the one shard straddling the boundary. `store::LongTermStore` has no
truncate operation today - a new, small addition needed for this
("What this touches" below).

**Resumability comes for free**, matching `docs/plans/implemented/
03-chunk-extents.md`'s own "no persisted free-list" choice: nothing about
this plan needs a saved progress log. A killed process leaves the DB
(committed per-chunk moves so far) and the store files exactly consistent
with each other; a re-run simply recomputes gaps/tail from current
`chunk_extents` and continues from wherever that leaves off. Truncation
at the very end is the same story - its target size is always freshly
recomputed from `MAX(stop)` over current `chunk_extents`, never a
remembered plan, so re-running a partially-truncated attempt just finishes
shrinking whatever's still oversized.

## The real hazard: DB backups become silently wrong, not just stale

This is more dangerous than ordinary backup staleness. Once any chunk has
been moved, a `meta/backups/*.zip` snapshot taken *before* that point
still contains `chunk_extents` rows pointing at the *old* positions - and
after compaction, those old positions aren't just gone, they've very
likely been overwritten by a *different* chunk's bytes during the move.
Restoring such a backup (`db restore`) wouldn't fail - it would silently
resolve some files to the wrong bytes. This needs to be caught
automatically, not just documented and hoped for.

## Open questions

- **Naming.** `compact-store` (top-level command, kebab-case matching
  `fix-problems`/`reclaim-space`) is this doc's placeholder - needs to
  read as clearly distinct from the existing `db compact` (SQLite
  `VACUUM`/`incremental_vacuum` on the metadata file only, unrelated
  subsystem). Alternatives: `defrag`, `pack`, `gc-store`. Pick one.
- **How to guard against the silent-wrong-restore hazard above.**
  Candidates, not mutually exclusive: (a) document prominently and print a
  loud warning after a successful run - relies on the user reading it at
  the right time, weakest guarantee; (b) require an explicit
  acknowledgment flag to run at all; (c) after a successful run, move
  every existing `meta/backups/*.zip` out of the way (or delete them)
  automatically; (d) stamp a "store generation" counter in
  `repository_settings`, bumped on every successful `compact-store` run,
  embedded in each backup's own metadata, and have `db restore` refuse
  (or require an override) if the backup's stamped generation doesn't
  match the live repository's current one. (d) is the only option that
  protects a user who restores a backup long after forgetting a
  compaction ever happened - recommend it as the real fix, with (c) as an
  optional belt-and-suspenders cleanup on top. Needs a decision either
  way.
- **Exclusivity while running.** Nothing in this codebase enforces
  cross-process mutual exclusion for a repository today (the "single
  writer" doc comment in `db/src/lib.rs` is a within-process connection
  discipline, not a cross-process lock) - running `compact-store`
  concurrently with `store`/`mount --read-write`/`reclaim-space` would
  race on the same `chunk_extents` rows and store bytes. Add a real
  repo-level lock for this (and retroactively for the other maintenance
  commands?), or keep relying on documented "don't run these
  concurrently" discipline, consistent with how the rest of the codebase
  currently handles it?
- **Verification depth.** After writing a chunk's bytes to their new
  location (step 2), re-read and re-hash before committing the extent
  switch (catches a copy bug before it becomes silent data corruption, at
  the cost of extra I/O/CPU - roughly doubling the read volume), or trust
  the write and rely on a later `check` run to catch problems? Given how
  bad a silent-copy-bug outcome would be here specifically (unlike most
  commands, a bug in this one can corrupt data that was previously fine),
  leaning toward verifying - but that's a real, measurable cost worth
  deciding deliberately, not defaulting into.
- **Full pack vs. bounded/incremental run.** Always run to full completion
  (one contiguous block, no gaps left) in a single invocation, or support
  a `--max-bytes`/time-boxed mode for very large repositories where a
  user wants to deliberately spread the work across several sessions
  rather than run one very long operation? Resumability (above) already
  makes an *interrupted* run safe either way - this question is only
  about whether to offer *voluntary* early stopping as a first-class flag
  from day one, or add it later only if it turns out to matter.
- **Should `compact-store` require or suggest running `reclaim-space`
  first?** Compacting before `reclaim-space` would move data that's about
  to become garbage anyway once `reclaim-space` next runs, wasting I/O -
  worth at least a README-level recommended order (`reclaim-space` then
  `compact-store`), possibly a warning if `compact-store` sees active
  soft-deleted-but-not-yet-reclaimed entries with an eligible cutoff.
  Enforcing this automatically feels like overreach; a printed hint
  probably suffices - confirm.

## What this touches

- `store`: a new truncate operation on `LongTermStore` (remove shard files
  entirely past a target size, `set_len()` the one straddling it).
- `db`: whichever backup-generation mechanism the second open question
  above settles on (new `repository_settings` column, or similar); no
  change to `chunk_extents` itself, which already supports everything the
  move step needs.
- `cli`: a new `compact_store` module/command wired into `main.rs`,
  reusing `chunk_store`'s allocator and read/write helpers, `io_limiter`,
  and `progress::Progress`.
- `README.md`: a new section (matching the existing "Database Backup,
  Restore, and Compaction" section's style), explicitly cross-linking the
  backup-invalidation hazard from wherever `db backup`/`db restore` are
  documented.
