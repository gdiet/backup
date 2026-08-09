# Detect a metadata backup gone stale relative to the physical data store

**Status**: proposed, not implemented. Split out of `docs/plans/compact-store.md`
while working out that plan's backup-invalidation open question - the
underlying hazard turned out to already be live in the shipped codebase,
via ordinary `store`/`mount --read-write` runs, entirely independent of
whether `compact-store` ever gets built. Tracked here on its own so it
isn't stuck waiting on that larger, still-undecided feature.

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
`write_chunk_from_cache`), so both can trigger this.

## Fix: a store-generation counter, stamped into every backup

Add a `store_generation` column to `repository_settings` (schema
migration), starting at `0`. Bump it by exactly one, in the same DB
transaction that commits the newly-written chunk's `chunk_extents` rows,
whenever a write actually consumed bytes from a *real* gap (not the
open-ended trailing region past the highest known position) - i.e.
exactly the case that can overwrite a range some earlier backup still
references. One increment per run that did any gap reuse is enough
granularity; no need to count how many gaps.

`SpaceAllocator` already distinguishes real gaps from the trailing region
internally (`reserve`'s `gaps[0]` vs. the sentinel `u64::MAX`-terminated
last entry) - it just needs to expose whether *any* reservation during
this run's lifetime touched a real one, via a simple `AtomicBool`
alongside its existing `Mutex<Vec<(start, stop)>>` (`false` by default,
set `true` the first time a real gap is consumed, read once by the caller
after the run's writes are done, right before that run's final commit).
`compact-store` (see its own plan) always bumps this counter on a
successful run - it doesn't need the same-gap-detection nuance, since
relocating live chunks is its entire purpose.

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
  small read/bump helper; `db restore`'s pre-overwrite comparison and
  warning message.
- `cli::chunk_store`: `SpaceAllocator` gains the `AtomicBool` "touched a
  real gap" flag and an accessor for it.
- `cli/src/store.rs` and `cli/src/mount.rs`: after a run/persist
  transaction's writes are done, check the allocator's flag and, if set,
  bump `store_generation` in that same commit.
- `cli/src/db_maintenance.rs`: `run_restore_db` reads and compares
  generations before proceeding.
- `README.md`: document the warning and what it means, near the existing
  `db backup`/`db restore` section.

## Open questions

- Exact warning wording/format - should it also say *how many* gap-reusing
  runs happened since (readable from the generation delta), or just
  "since"? A delta is easy to compute and probably more reassuring/alarming
  as appropriate ("1 run" vs. "47 runs").
- Whether `stats` should also surface the current `store_generation`
  alongside its existing `free_space_summary` fragmentation line, so a
  cautious user can check it before deciding whether an old backup is
  still likely safe, without having to attempt a restore first to find
  out.
