# `compact-store`: defragment the data store's physical layout

**Naming decided**: `compact-store` (kebab-case, matching `fix-problems`/
`reclaim-space`; clearly distinct from `db compact`'s unrelated SQLite
`VACUUM`).

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
resolve some files to the wrong bytes.

**Resolved, but not scoped to this command**: working through this
surfaced that the identical hazard already exists today via ordinary
`store`/`mount --read-write` runs reusing a gap left by `reclaim-space` -
entirely independent of whether `compact-store` ever ships. Split out to
its own plan, `docs/plans/stale-backup-guard.md`: a `store_generation`
counter in `repository_settings`, bumped inside `reclaim_space` itself
(where gaps are *created* - sufficient to catch every real case, since a
gap can only ever come from there), stamped into every `db backup`
snapshot for free (it's just a DB dump), and checked by `db restore`
before it overwrites the live database - warns (doesn't refuse) when the
backup predates the live repository's current generation.
`compact-store`'s own contribution here is simple: always bump
`store_generation` unconditionally on a successful run.

## Decisions

- **Exclusivity while running.** Add a real repo-level lock file for
  `compact-store` - nothing in this codebase enforces cross-process mutual
  exclusion for a repository today (the "single writer" doc comment in
  `db/src/lib.rs` is a within-process connection discipline, not a
  cross-process lock), and running `compact-store` concurrently with
  `store`/`mount --read-write`/`reclaim-space` would race on the same
  `chunk_extents` rows and store bytes. Still open at implementation time:
  exact lock mechanism (a `meta/.lock` file held for the process's
  lifetime, checked/created at startup) and whether to retrofit it onto
  the other maintenance commands too, or scope it to just this one for
  now.
- **Verification depth.** No read-back/re-hash verification after writing
  a chunk's bytes to their new location - ordinary `store` doesn't re-read
  what it just wrote either (trusts the write, relies on a later `check`
  run to catch problems), and `compact-store` stays consistent with that
  existing risk posture rather than holding itself to a stricter bar.
- **Full pack vs. bounded/incremental run.** Always runs to full
  completion (one contiguous block, no gaps left) in a single invocation
  for now - no `--max-bytes`/time-boxed flag at first. Resumability
  (above) already makes an *interrupted* run safe either way, so adding
  voluntary early-stopping later, if a large repository turns out to need
  it, is a pure addition - nothing about this decision needs revisiting
  up front.
- **Ordering relative to `reclaim-space`.** No enforcement -
  `compact-store` doesn't check for or warn about pending
  soft-deleted-but-not-yet-reclaimed entries. Just a documented
  recommendation (`reclaim-space` before `compact-store` - compacting
  first would move data that's about to become garbage anyway once
  `reclaim-space` next runs, wasting I/O) in `README.md`.

## What this touches

- `store`: a new truncate operation on `LongTermStore` (remove shard files
  entirely past a target size, `set_len()` the one straddling it).
- `db`: bumping `store_generation` on a successful run - see
  `docs/plans/stale-backup-guard.md`, which this depends on (should land
  first, or alongside). No change to `chunk_extents` itself, which already
  supports everything the move step needs.
- `cli`: a new `compact_store` module/command wired into `main.rs`,
  reusing `chunk_store`'s allocator and read/write helpers, `io_limiter`,
  and `progress::Progress`; a new repo-level lock file mechanism (see
  "Exclusivity while running" above).
- `README.md`: a new section (matching the existing "Database Backup,
  Restore, and Compaction" section's style), cross-linking
  `stale-backup-guard.md`'s warning behavior.
