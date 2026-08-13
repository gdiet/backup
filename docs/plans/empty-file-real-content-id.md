# Give every empty file a real, shared `content_id`

**Status**: proposed, not started. Supersedes (and, once implemented, replaces) `docs/plans/
mount-empty-file-history-noise.md`'s narrower fix - see "Relationship to the superseded plan"
below. Surfaced during a `docs/plans/deleted-folder-ux-review.md` walkthrough (2026-08-13).

## The problem this solves

`content_id IS NULL` on a `tree_entries` row is currently overloaded with three different
meanings, disambiguated only by context/`kind`, never by the data itself:

1. A directory (never has content at all).
2. A mount `create()`'s placeholder - a file that exists but has no content *yet*, transient by
   nature (see `cli/src/mount.rs`'s `create`/`persist`).
3. A genuinely, permanently empty file - a real, settled steady state, used deliberately as such
   in several places:
   - `db::file_size` (`db/src/query.rs:192-208`) - `None` means "0 bytes", a normal answer.
   - `mount.rs`'s `read_persisted` (`cli/src/mount.rs:1438`) - `None` means "read back empty".
   - `restore.rs:397-398` - `None` means "write no chunks, just create the empty file".
   - `fix-problems --replace-empty` (`db::soft_delete_and_replace_...`, `db/src/maintenance.rs:
     43-46`) - *deliberately* inserts a fresh row with `content_id NULL` as the permanent repair
     result for a file whose real bytes were unrecoverable. Doc comment: "inserts a fresh
     zero-byte file ... with `content_id NULL`".
   - `migrate_scala_repo.rs:474-476` - counts `content_id.is_none()` as `stats.empty_files` when
     importing a legacy Scala repository, i.e. treats it as the normal "this file is empty" signal.

Meanings 2 and 3 being indistinguishable is exactly what made the originally-proposed mount fix
(hard-delete a replaced row whenever its `content_id IS NULL`) unsafe: it would have destroyed the
recoverability of a real, deliberately-empty file (meaning 3) the same way it correctly discards a
throwaway mount placeholder (meaning 2) - see `docs/plans/mount-empty-file-history-noise.md`'s
"Rejected first approach" for the concrete failure scenario that ruled it out.

## Proposal

Reserve `content_id IS NULL` on a *file* entry exclusively for meaning 2 (not-yet-decided,
transient - a directory is still separately identified via `kind`, unchanged). Give every
genuinely empty file a real `content_id`, pointing at one shared `contents` row for the empty byte
sequence (`length = 0`, `hash` = the hash of the empty chunk sequence, zero `content_chunks` rows)
- deduplicated the same way any other content already is.

This mostly falls out of machinery that already exists:

- `db::resolve_content` (`db/src/backup.rs:94-101`) currently short-circuits `if chunks.is_empty()
  { return Ok(None) }`. Dropping that short-circuit and letting it fall through the normal
  insert-or-get `contents` path already does the right thing unmodified: `total_length` sums to
  `0`, the `chunk_ids` loop is empty, zero `content_chunks` rows get inserted - no chunk-specific
  logic needs special-casing. The *first* empty file ever resolved creates the shared row; every
  one after that dedups onto it via the existing `hash` uniqueness, exactly like any other content.
- `DedupFs::create` (`cli/src/mount.rs`) must keep inserting `content_id = NULL` for its up-front
  placeholder - it needs to *not* go through `resolve_content`/`ContentSource::Resolved` for that
  specific insert once the short-circuit above is gone (today it incidentally gets `NULL` via that
  same short-circuit). Needs a distinct way to say "no content decided yet" explicitly - e.g. a new
  `ContentSource::None` variant alongside `Resolved`/`Known`, or a small dedicated insert path
  outside `apply_backup_batch`.
- `fix-problems --replace-empty` (`db/src/maintenance.rs:43-`) switches to inserting the shared
  empty content id instead of `NULL`.
- `migrate_scala_repo.rs` needs its empty-vs-unrecoverable distinction reconsidered: a Scala file
  that's genuinely empty should resolve to the shared id like any other empty file, but Scala's
  *separate* "content couldn't be recovered" case (`resolve_content_id` returning `None` for lost
  data - see its own doc comment) currently also produces `content_id = None` in the migrated repo,
  and would need its own explicit representation instead of quietly reusing `NULL` for two
  different things.
- `db::file_size`/`mount.rs::read_persisted`/`restore.rs` simplify once every *settled* file
  entry has a real `content_id`: their `None => 0` / `None => empty bytes` branches become
  unreachable for any row these functions are actually called against (a still-open mount
  placeholder is never restored/measured/read via the persisted-content path) and can likely drop
  the special case entirely rather than just keep tolerating it.

## Why no migration is needed right now

Normally, retrofitting this onto existing repositories would need a real schema migration: insert
the shared empty-content row, then backfill every existing `tree_entries` row with `kind='file' AND
content_id IS NULL` to point at it - and that backfill is a real trap, not just busywork: `UPDATE`
doesn't fire `tree_entries_ref_count_ins`/`_del` (`db/src/migrations.rs:186-195` - INSERT/DELETE
only), so the new row's `ref_count` would silently stay `0` unless the migration fixes it up by
hand afterward.

None of that applies today: there are no real repositories yet, only disposable test instances -
so this can simply become part of the initial schema definition (seed the shared empty-content row
directly at `init`, alongside the existing root tree entry seed) rather than a data migration at
all. If this lands after real repositories exist, revisit with an actual migration, including the
manual `ref_count` fixup noted above.

## Relationship to the superseded plan

`docs/plans/mount-empty-file-history-noise.md` proposed a session-local `created_fresh` flag on
`FileWriteState` to distinguish "this row was never independently observed" without touching the
schema. That still works, but is no longer needed once this lands: with `content_id IS NULL`
unambiguous, `apply_backup_batch`'s replace branch can safely hard-delete the old row whenever
`existing.content_id.is_none()` directly - the exact heuristic originally rejected, now made
correct by construction rather than by the caller's out-of-band bookkeeping. Once this plan is
implemented, delete the superseded doc's file entirely rather than leaving two competing designs
around.

## Open questions

- Exact mechanism for seeding the shared empty-content row - literal SQL in the schema's `CREATE
  TABLE`/seed block (same place the root `tree_entries` row is seeded), vs. lazily inserted the
  first time `resolve_content` sees an empty chunk list. The former guarantees a fixed, predictable
  id and existence from the very first query; the latter needs no schema change to the seed data at
  all. Leaning towards seeding it explicitly, for a predictable id and to avoid a lazy-insert path
  needing its own dedup-race handling that `resolve_content`'s general one already has to do anyway.
- What `migrate_scala_repo.rs` should record for Scala's "content unrecoverable" case once `NULL`
  is reserved for the mount stub meaning only - a distinct sentinel, or something outside
  `content_id` entirely (e.g. surfaced via `backup problems` instead).
- Whether any tooling (`check`, `stats`) needs adjustment for a `contents` row that has zero
  `content_chunks` by design rather than by accident - worth an explicit look rather than assuming
  existing zero-chunk handling (if any) already covers it correctly.
