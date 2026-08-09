# Detect a metadata backup gone stale relative to the physical data store

**Status**: implemented. `store_generation` was folded directly into
`SCHEMA_V1` (no released version to preserve compatibility for yet - see
`migrations.rs`), and the real `dedup/` repository's live database was
patched with the same `ALTER TABLE` directly, matching the earlier
`auto_vacuum` fix's approach.

Originally split out of `docs/plans/compact-store.md` while working out
that plan's backup-invalidation open question - the underlying hazard
turned out to already be live in the shipped codebase, via ordinary
`store`/`mount --read-write` runs, entirely independent of whether
`compact-store` ever gets built.

**Trigger simplified from the original draft**: bump the counter in
`reclaim_space` (where gaps are *created*), not in every `store`/`mount
--read-write` write that *reuses* one - see "Fix" below for why that's
still sound, not just simpler.

## The problem, concretely

1. `store a.txt` (1000 bytes) - lands at `[0, 1000)`.
2. `db backup` - snapshot taken; its `chunk_extents` says `a.txt`'s content
   lives at `[0, 1000)`.
3. `del a.txt` + `reclaim-space` - `a.txt`'s `chunks` row (and its
   `chunk_extents`) are gone. `[0, 1000)` is now a gap:
   `db::free_space_summary` reports it, nothing references it.
4. `store b.txt` (1000 bytes) - per
   `docs/plans/implemented/03-chunk-extents.md`, `cli::chunk_store::
   SpaceAllocator` deliberately reuses gaps like this one for new chunks.
   `b.txt`'s bytes get written to `[0, 1000)`, physically overwriting what
   used to be `a.txt`'s content.
5. `db restore <the backup from step 2>` - the restored database still
   says `a.txt` lives at `[0, 1000)`. It does not fail. It silently
   resolves `a.txt` to `b.txt`'s bytes.

Nothing in this sequence is unusual or requires a bug - it's the intended,
documented behavior of gap reuse (the entire point of `03-chunk-extents.md`)
combined with the entirely separate, also-intended behavior of `db
backup`/`db restore`. The two features were never checked against each
other. `store` and `mount --read-write`'s persist path both go through the
same shared allocator (`cli::chunk_store::SpaceAllocator`/
`write_chunk_from_cache`), so both can trigger this - but neither is where
the fix below hooks in.

## Fix: a store-generation counter, stamped into every backup

Add a `store_generation` column to `repository_settings` (schema
migration), starting at `0`.

**Bump it where gaps are created, not where they're reused.** A gap in
the data store only ever comes from one place: `reclaim_space`'s `DELETE
FROM chunks WHERE ref_count = 0`, cascading to `chunk_extents` via `ON
DELETE CASCADE` (see the schema doc comment in `migrations.rs`). Nothing
else in the codebase ever deletes a `chunks` row - soft-delete (`del`)
never frees anything, a soft-deleted entry keeps holding its content's
`ref_count` until an actual hard-delete. So every real danger case (a
later `store`/`mount --read-write` write overwriting a position some
older backup still references) is necessarily preceded by a
`reclaim_space` run that created the gap it went on to reuse. Bumping
`store_generation` inside `reclaim_space` itself - in the same
transaction, guarded by `chunks_purged > 0` (see `ReclaimStats`) so a
no-op run doesn't bump it - therefore misses no real case. It's coarser
than bumping only when a gap actually gets reused later (a backup gets
flagged as soon as *any* eligible space exists, whether or not anything
ever ends up overwriting it), but that coarseness is exactly right for a
warn-only, "possibly unsafe, use at your own risk" guard, and it avoids
touching `SpaceAllocator`, `store.rs`'s workers, or `mount.rs`'s persist
path at all - a single one-line bump in one already-transactional
function. `compact-store` (see its own plan) always bumps this counter
on a successful run, unconditionally - relocating live chunks is its
entire purpose, no need to check anything first.

Because `store_generation` lives in `repository_settings`, every `db
backup` snapshot - being a plain database dump - automatically carries
whatever value was live at the moment it was taken. No separate stamping
step needed.

`db restore <file>`, before overwriting the live database (i.e. before
the already-fixed staged-copy-then-rename in `run_restore_db` - see the
crash-safety fixes in commit `4c10d7a0`), opens the backup file just far
enough to read its `store_generation` and compares it against the live
database's current value:

- Backup's generation == live generation: no gap-reusing write has
  happened since this backup was taken - safe, no warning.
- Backup's generation < live generation: at least one gap-reusing write
  happened after this backup, so restoring it may resolve some entries to
  the wrong bytes. Print a strong warning explaining exactly that, but
  don't refuse the restore outright - the user may have a specific reason
  to do it anyway (explicit decision made while discussing this plan: warn,
  don't hard-block).
- Backup predates this feature (no `store_generation` column/row at all):
  treat as unknown/possibly-stale - same warning, since the absence of the
  column can't be used to prove safety.

## What this touches

- `db`: schema migration adding `repository_settings.store_generation`; a
  small read/bump helper; `reclaim_space` bumps it (guarded by
  `chunks_purged > 0`) in its existing transaction; `db restore`'s
  pre-overwrite comparison and warning message.
- `cli/src/db_maintenance.rs`: `run_restore_db` reads and compares
  generations before proceeding.
- The future `compact-store` command: bumps `store_generation`
  unconditionally on a successful run (see `docs/plans/compact-store.md`).
- `README.md`: document the warning and what it means, near the existing
  `db backup`/`db restore` section.

## Resolved while implementing

- The warning includes the generation delta (`"this backup is N data-
  store-changing maintenance run(s) ... behind"`), not just a bare
  "stale" notice.
- `stats` surfaces the current `store_generation` as its own line,
  alongside the existing `free_space_summary` fragmentation line.
