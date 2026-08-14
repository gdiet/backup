# Give every empty file a real, shared `content_id`

**Status**: implemented (2026-08-14). Supersedes `docs/plans/mount-empty-file-history-noise.md`
(deleted - its session-local `created_fresh` flag idea is no longer needed, see "Relationship to
the superseded plan" below). Surfaced during a `docs/plans/deleted-folder-ux-review.md` walkthrough
(2026-08-13).

## The problem this solved

`content_id IS NULL` on a `tree_entries` row used to be overloaded with three different meanings,
disambiguated only by context/`kind`, never by the data itself: a directory, a mount `create()`'s
transient placeholder, and a genuinely, permanently empty file. Meanings 2 and 3 being
indistinguishable is exactly what made an earlier, rejected fix (hard-delete a replaced row
whenever its `content_id IS NULL`) unsafe - it would have destroyed the recoverability of a real,
deliberately-empty file the same way it correctly discards a throwaway mount placeholder. See
`docs/plans/mount-rename-overwrite.md`'s "Rejected first approach" section (in the now-deleted
superseded doc) for the concrete failure scenario that ruled it out.

## What was built

`content_id IS NULL` on a *file* entry now means exactly one thing: "no content decided yet" (a
still-open mount `create()` placeholder). A directory is unaffected, still identified by `kind`
alone. Every genuinely empty file - whether from `store`, the mount, `fix-problems
--replace-empty`, or a migrated Scala repository - gets a real `content_id`: `db::EMPTY_CONTENT_ID`
(`= 1`), a fixed row seeded once in `migrations.rs`'s `SCHEMA_V1` (`length = 0`, `hash` = BLAKE3's
XOF output for an empty input, truncated to `HASH_LENGTH` = 20 bytes - the same hash any real empty
file's own content-hashing independently computes), not created on demand.

The implementation ended up simpler than originally sketched in a few places:

- **No new `ContentSource` variant needed.** The plan anticipated needing a `ContentSource::None`
  to let the mount's `create()` bypass `resolve_content`. `ContentSource::Known(Option<i64>)`
  already did exactly this (`Known(None)` inserts `content_id = NULL` directly, skipping
  `resolve_content` entirely) - `create()` just switched from `Resolved { chunks: vec![],
  content_hash: vec![] }` to `Known(None)`.
- **`resolve_content` short-circuits to a constant, not a general hash-based lookup.** The plan
  suggested dropping `resolve_content`'s empty-chunks short-circuit and letting it fall through the
  normal insert-or-get path. Actually implemented: `resolve_content` returns
  `Ok(EMPTY_CONTENT_ID)` directly for empty `chunks`, ignoring `content_hash` entirely in that
  case - simpler, and sidesteps any risk of different callers computing slightly different "empty"
  hashes and accidentally creating two rows. `resolve_content`'s return type changed from
  `Result<Option<i64>, Error>` to `Result<i64, Error>` - it never returns `None` any more, which
  let `migrate_scala_repo.rs` drop a now-unnecessary `.expect("resolve_content only returns None
  for zero chunks")`.
- **The "Scala unrecoverable content" open question turned out to be moot.** The plan worried about
  what `migrate_scala_repo.rs` should record for Scala's "content couldn't be recovered" case once
  `NULL` is reserved for the mount stub. Turns out that case never produces a `content_id` at all -
  `walk_directory`'s `None` arm (`resolve_content_id` returning `None` for unrecoverable data) skips
  creating the tree entry entirely (`stats.skipped += 1`, a warning printed), it doesn't insert a
  row with any `content_id`. Only `Some(-1)` (Scala's own "genuinely empty" sentinel) ever produced
  `content_id = None` in the old code, and that's the one case actually fixed (now resolves to
  `Some(EMPTY_CONTENT_ID)`).
- **`check`/`stats` needed no changes at all.** Both already treat `contents`/`content_chunks` as
  opaque, dedup-by-id data; a row with zero `content_chunks` (whether `EMPTY_CONTENT_ID` or a
  real one-off coincidence) was already handled correctly by existing queries. Verified by reading,
  not just assuming.

## Two real bugs found and fixed while implementing (not anticipated by the original plan)

1. **`reclaim_space` would have deleted the seeded row and broken every future empty file.**
   `DELETE FROM contents WHERE ref_count = 0` is unconditional - once nothing referenced
   `EMPTY_CONTENT_ID` (a plausible, ordinary state, not a bug), it would have been purged like any
   other unreferenced content. But `resolve_content` returns that id directly without re-checking
   it still exists, so the very next empty file would then violate the `content_id` foreign key.
   Fixed: `reclaim_space`'s `contents` sweep now excludes `EMPTY_CONTENT_ID` explicitly (`AND id !=
   ?1`) - see its own doc comment in `maintenance.rs`. Caught by writing the schema doc comment
   carefully enough to notice the claim ("purged and recreated on demand, like any other content")
   wasn't actually true once checked against `resolve_content`'s real (non-recreating) behavior.
2. **A `create()`'d file closed without ever being written would have stayed `content_id IS NULL`
   forever**, silently reintroducing the exact ambiguity this whole change exists to remove - a
   bare `touch` through the mount is a real, expected case, not an edge case to ignore. Fixed:
   `db::finalize_as_empty_if_undecided(conn, id)` (new, in `tree.rs`) settles `content_id` to
   `EMPTY_CONTENT_ID` if it's still `NULL` when `Inner::release` sees a non-dirty handle's last
   close - the one deliberate exception to "never mutate `content_id` in place" (there's no history
   to preserve for a row that was never independently observable with any other content). Since the
   `tree_entries_ref_count_*` triggers only fire on `INSERT`/`DELETE`, never `UPDATE`, this manually
   bumps `contents.ref_count` for `EMPTY_CONTENT_ID` in the same transaction rather than relying on
   a trigger that won't fire for it.

## Why no migration was needed

No real repositories existed yet at the time this landed - only disposable test instances - so this
became part of the initial schema seed (`SCHEMA_V1`) directly rather than a data migration. If this
schema ever needs to change again *after* a real repository exists, that will need an actual
migration, including a manual `ref_count` fixup for the same INSERT/DELETE-only-trigger reason
noted above (`UPDATE`ing existing `NULL` rows to point at a newly-seeded row wouldn't fire the
insert trigger either).

## Relationship to the superseded plan

`docs/plans/mount-empty-file-history-noise.md` proposed a session-local `created_fresh` flag on
`FileWriteState` to distinguish "this row was never independently observed" without touching the
schema, specifically to fix `apply_backup_batch`'s replace branch leaving a soft-deleted "ghost"
placeholder visible in `[deleted]` after every mount-created file. That's no longer needed: with
`content_id IS NULL` now unambiguous, `apply_backup_batch`'s replace branch could straightforwardly
be taught to hard-delete the old row whenever `existing.content_id.is_none()` - the exact heuristic
that doc originally rejected, now correct by construction. The superseded doc's file was deleted
since its proposed *mechanism* (the session flag) is obsolete, not because the underlying cosmetic
issue was fixed yet - **it has been since, 2026-08-14: see `apply_backup_batch`'s own doc comment
and replace branch (`db/src/backup.rs`), which now hard-deletes exactly when
`existing.content_id.is_none()`**, closing this out for real. Surfaced again by a concrete report -
copying a `.git` checkout's many files into the mount left one ghost entry per file, impossible to
miss at that volume even though the underlying mechanism had been present (and mostly unnoticed)
since the mount's read-write support first shipped.

## Verification

`cargo fmt --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test
--workspace`, `cargo doc --no-deps --workspace` all clean. New/updated tests: `db::backup::tests`
(empty file resolves to `EMPTY_CONTENT_ID`, two empty files dedup onto it, `Known(None)` still
leaves `content_id` unset); `db::tree::tests` (`finalize_as_empty_if_undecided` settles a
placeholder and bumps `ref_count` correctly, is a no-op for real content and for an
already-settled empty file); `cli::mount::tests` (a `create()`+no-write+`release()` cycle settles
as `EMPTY_CONTENT_ID` on release, two touched files dedup onto the same content); every pre-existing
test whose fixture hardcoded `contents.id = 1` (now taken by the seed) or asserted a raw `contents`
row count updated to account for the always-present seeded row.

**2026-08-14 follow-up** (the `apply_backup_batch` hard-delete described above): new
`db::backup::tests::a_create_placeholder_replaced_by_real_content_leaves_no_deleted_ghost` (mirrors
`create()` then `persist()` directly against `apply_backup_batch`, confirms the placeholder is gone
- not soft-deleted - and `deleted_entries` shows nothing) and
`cli::mount::tests::create_then_write_leaves_no_deleted_ghost` (the same thing end to end through a
real `DedupFs`: create, write, release, confirm `has_deleted_children` is `false`). Full suite
re-run clean.
