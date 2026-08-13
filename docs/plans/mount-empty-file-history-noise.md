# `apply_backup_batch`: don't soft-delete a content-less placeholder row

**Status**: proposed, not started - surfaced while investigating `docs/plans/
mount-rename-overwrite.md`, during a `docs/plans/deleted-folder-ux-review.md` walkthrough
(2026-08-13). Design revised once already (see "Rejected first approach" below) - not yet
implemented.

## What was found

Simply creating a new file through a `--read-write` mount (create it, write some content, save)
leaves a "ghost" entry behind in `[deleted]` for a file the user never deleted - confirmed via a
direct FUSE repro (`mkdir`, `echo hello > file.txt`, no delete at all): `[deleted]` shows up under
the containing directory right away, holding a same-named, zero-byte, already-soft-deleted entry.

## Root cause

`DedupFs::create` (`cli/src/mount.rs`) inserts an empty (`content_id IS NULL`) `tree_entries` row
up front, via `apply_backup_batch`, so the file is immediately visible to `readdir`/`getattr`
before any bytes have been written (needed - many tools, and a bare `touch`, expect a just-created
file to exist right away). Once the first real write is persisted, `Inner::persist` calls
`apply_backup_batch` again for the same path; finding an existing active entry there with
different content, it takes the "replace" branch (`db/src/backup.rs:222-236`), which
**soft-deletes** the old row and inserts a new one. That branch exists so a real `store` re-run
preserves genuine version history across separate backup runs - but here the "old version" is just
the empty placeholder from moments earlier in the very same create-and-write operation, not
anything a user would ever want to recover.

## Rejected first approach

Originally proposed hard-deleting the replaced row (instead of soft-deleting) whenever its
`content_id IS NULL`, on the theory that a content-less row is never worth preserving. **This is
wrong and was not implemented** - `content_id IS NULL` does not uniquely identify a throwaway
placeholder. Per the `contents`/`tree_entries` schema doc comment (`db/src/migrations.rs:51-55`)
and `resolve_content`'s actual behavior (`db/src/backup.rs:94-101`, returns `Ok(None)`
unconditionally for an empty chunk list), `content_id IS NULL` is the *exact same* representation
for a directory, for `create()`'s placeholder, and for a genuinely empty file a user deliberately
made and kept (e.g. a `.keep`-style marker) - nothing in the data distinguishes them. Keying the
hard-delete decision off `content_id` alone would have silently destroyed a real user's empty
file's recoverability the first time it got overwritten with real content, and would have changed
`store`'s own empty-file versioning behavior too (`resolve_content` is shared by both callers) -
an unintended, unrelated behavior change.

(Side note, not yet acted on: the schema doc comment at `db/src/migrations.rs:34-45` claims "all
empty files share one `contents` row with zero `content_chunks`", which contradicts both the code
and the later statement at line 52-55 that `content_id` is simply `NULL` for empty files. Worth
fixing that comment - probably the earlier sentence is stale - as part of whichever change touches
this area next, per this repo's own doc-hygiene convention.)

## Proposed fix

Make the "no history worth keeping" judgment where it can actually be made correctly: in
`cli/src/mount.rs`, based on the *session's own knowledge* of whether this exact row was ever
independently observable, not by inferring it from the row's content after the fact.

- Add `created_fresh: bool` to `FileWriteState`, set to `true` only by `DedupFs::create` (not by
  `open` - both currently share `register_open`, which would need a way to distinguish the two
  callers, e.g. an extra parameter).
- Thread it through to `PersistJob` and into `Inner::persist`'s call into `apply_backup_batch` -
  e.g. a new explicit parameter/field meaning "replace without preserving the old row", rather
  than anything `apply_backup_batch` infers from `content_id`.
- `apply_backup_batch`'s replace branch keeps soft-deleting by default; hard-delete only happens
  when the caller explicitly says so via that flag.
- Correctness of the flag falls out of existing invariants, no extra bookkeeping needed: `persist`
  only ever runs once `open_count` reaches zero (see `FileWriteState` refcounting), so if `create`
  set `created_fresh = true` and nothing has closed-and-reopened this path in between, this
  persist is provably the first thing anyone has ever done with this row since it was created -
  there is no "old version" to lose. A `create()` immediately released without any write never
  reaches `persist` at all (no dirty cache), so a bare `touch` of a file that's then *never*
  written to is left alone, exactly as it should be.
- `store` never sets this flag - its empty-file versioning behavior is completely unchanged.

## Considered and rejected: a persisted "stub" vs. "empty" distinction

Also considered adding a schema-level way to tell "not yet written" apart from "genuinely empty"
(instead of the session-local flag above), and checked whether Scala's implementation already
does this - it doesn't. Scala has no `kind` column at all; it distinguishes file from directory
purely by `dataId IS NULL` (directory) vs. `NOT NULL` (file) (`db/Database.scala:310-313`), which
only works because every file, even an empty one, gets a non-null `dataId`. For "just created,
nothing written yet" it uses a sentinel `DataId(-1)` (`server/Backend.scala:114-117`) - but that
exact same sentinel is also the *permanent* value written for a genuinely empty file once closed
(`server/Backend.scala:138-139`: "For 0-length data entry explicitly set dataId -1 because it
might have contained something else before"). So Scala doesn't distinguish stub from empty either
- same ambiguity as Rust's `content_id IS NULL`, just spelled with a sentinel instead of `NULL`.

Scala also never hits this module's actual bug in the first place, for an unrelated reason:
persisting a file there is a plain `UPDATE TreeEntries SET dataId = ? WHERE id = ?` on the *same*
row (`db.setDataId`, called from `writeDataIdAndRemove`), never a soft-delete-and-reinsert. That's
only possible because Scala has no trigger-maintained `ref_count` to keep consistent - orphaned
`dataId`s are found by a full set-difference scan (`dataIdsInTree()` vs. `dataIdsInStorage()`,
`db/Database.scala:337-342`) rather than incrementally. Rust's `ref_count` triggers deliberately
fire only on `INSERT`/`DELETE` of `tree_entries`, never `UPDATE` (`db/src/migrations.rs:186-195`)
- a real, documented architectural trade-off (O(1) incremental ref-counting vs. Scala's cheaper
per-write but scan-based GC), not an oversight. Mutating `content_id` in place the way Scala
mutates `dataId` would break that invariant, so it's not an option here regardless.

Given neither precedent nor a clean minimal schema shape presents itself, and the session-local
`created_fresh` flag above already captures exactly the condition that matters without touching
schema, triggers, or `store`, no persisted stub/empty distinction is planned.

## Safety notes

Doesn't affect explicit user deletes (`unlink`/`rmdir` call `db::soft_delete` directly, untouched
by this) - a real, user-initiated delete of anything, including a genuinely empty file, remains
recoverable exactly as today. Only affects the one specific case the flag targets: a file created
and written to within the same uninterrupted mount session, never independently observed empty by
anything else in between.
