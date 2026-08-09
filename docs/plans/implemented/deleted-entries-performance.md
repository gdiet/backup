# Fix `backup deleted`'s unscoped performance; audit other commands for the same UX bar

**Status**: the confirmed bug below is implemented. The broader "every
command gives quick feedback" audit is scoped but not exhaustively
pre-solved - see its own section, still open.

## The confirmed bug: `deleted_entries` does a full table scan per tree level

This was flagged as a known, unquantified limitation when `backup deleted`
shipped (`docs/plans/implemented/soft-delete-recovery.md`). Now quantified
and root-caused against the real `dedup/` repository (~7.16M `tree_entries`
rows):

- **Before**: unscoped `backup deleted` was killed after 59m4s wall clock,
  still not finished. Its CPU time (`user 0m0.718s`, `sys 0m1.592s`) was
  negligible relative to the wall time - almost entirely blocked on disk
  I/O, not compute.
- **Root cause**, confirmed via `EXPLAIN QUERY PLAN` against the real
  database: `deleted_entries`' recursive CTE (`db/src/query.rs`) joins
  `tree_entries t ON t.parent_id = walk.id` at every recursive step
  *without* a `deleted_at IS NULL` filter (it can't have one - it needs to
  walk through deleted ancestors too, see the function's own doc comment).
  The only index covering `parent_id`, `tree_entries_active_name_idx`, is
  *partial* (`WHERE deleted_at IS NULL`), so it's unusable here. The query
  plan showed `SCAN t` (full table scan) inside the recursive step -
  repeated once per level of the walk, against a ~7M-row table.
- **Fix, verified empirically**: adding a plain, non-partial `CREATE INDEX
  ON tree_entries(parent_id)` changes the plan to `SEARCH t USING INDEX
  ... (parent_id=?)` at every step. Re-ran the same unscoped query with
  this index in place (as a temporary index, not yet committed to the
  schema): **1m48s**, down from "doesn't finish in under an hour" - and
  returned exactly 3807 rows, matching `stats`' reported deleted-entry
  total (3246 files + 561 dirs) exactly, confirming correctness alongside
  the speedup. The temporary index was dropped again after measuring; nothing
  has been changed in the schema yet.

### Design

- `CREATE INDEX tree_entries_parent_id_idx ON tree_entries(parent_id);`,
  non-partial deliberately - the whole problem was the existing partial
  index being unusable for a query that must see deleted rows too.
- Folded directly into `SCHEMA_V1`, not shipped as a separate `SCHEMA_V2`:
  nothing built on this crate is released yet, so - per the same reasoning
  that justified squashing the previous `SCHEMA_V2` back into `SCHEMA_V1`
  (see that doc comment) - there's still nothing to gain from tracking
  schema history across a step nobody outside this project has been
  migrated past. The real `dedup/` repository's `user_version` (already
  `1`) didn't need touching this time either: since the migration count
  stayed at one, only the index itself had to be added to that live file
  directly (a plain, additive `CREATE INDEX`) - no export/reimport dance
  needed, unlike the previous squash.
- Redundancy check: does this make `tree_entries_active_name_idx`
  (`parent_id, name` WHERE `deleted_at IS NULL`) redundant? No - that index
  is still strictly better for the active-only case (`resolve_path`,
  `find_tree_entry`, `subtree_entries_with_paths`, etc.), both because it's
  a smaller partial index and because it additionally covers `name`, which
  the new plain `parent_id`-only index doesn't. Keep both.

## Broader question: does every command give feedback in reasonable time?

The request that prompted this plan: CLI commands should be predictable to
the user - return a result quickly, or at least signal that a longer
operation is underway, rather than leaving the user guessing whether it
hung. `deleted_entries` above was a genuine bug (should have been fast, an
indexing oversight); the commands below are different in character -
inherently I/O-bound by real data volume, not a fixable query-plan defect -
but may still fail the same "predictable" bar:

- **`backup check` / `backup problems`, unscoped**: walks every chunk
  (344,536 in the real repository) and reads its actual bytes back from
  the store to verify length/hash - `check`'s job is specifically to touch
  all 2.39 TB of physical data, so no query fix makes this fast; it's
  comparable in nature to `migrate-scala-repo`'s ~1h47m run, which already
  has a progress indicator. Not benchmarked yet as part of this plan (a
  full run reads most of the repository's physical bytes - deliberately
  not run casually against the real repository without flagging that
  first). Likely candidate for a progress indicator (`checked N/344536
  chunks...`), not a performance fix.
- **`backup stats` / `backup list` / `backup find`**: already observed fast
  (a few seconds) against the real repository in the course of other work
  this session - `subtree_entries_with_paths`' recursive CTE already
  filters `deleted_at IS NULL` at each step, which - unlike
  `deleted_entries` - *can* and does use the existing partial index (also
  confirmed via `EXPLAIN QUERY PLAN`: `SEARCH ... USING INDEX
  tree_entries_active_name_idx`). No known issue here.
- **`backup restore`**: inherently bounded by how much is being restored;
  no whole-repository unscoped mode exists to worry about the same way.
- **`migrate-scala-repo`**: already has a progress indicator (added in an
  earlier round of work); not revisited here.

### Open question

Whether to add a progress indicator to `check`/`problems` now, as part of
landing the `deleted_entries` index fix, or treat it as a separate,
later increment. Leaning toward separate - the index fix above is a
correctness-adjacent bug fix ready to ship immediately, while a progress
indicator for `check`/`problems` is a smaller, independent UX addition that
doesn't need to block on it, and deserves its own quick benchmark (time per
chunk read, to decide the right reporting interval) rather than being
bundled in here as an afterthought.

## Verification checklist

- [x] Add the `tree_entries_parent_id_idx` index; add a migration test
  (`tree_entries_parent_id_idx_is_non_partial`).
- [x] Re-run the same timing check against the real `dedup/` repository
  through `open_repository`/`backup deleted` itself, not just the raw SQL
  measurement above: **1m58s** end to end (debug build), consistent with
  the 1m48s raw-SQL figure.
- [x] `cargo fmt --check && cargo clippy --workspace --all-targets -- -D
  warnings && cargo test --workspace && cargo doc --no-deps --workspace`.

The "does every command give feedback in reasonable time" audit above
identified one follow-up item (a progress indicator for `check`/
`problems`), deliberately scoped out of this plan's own deliverable -
tracked separately, not blocking this file's move to `implemented/`.
