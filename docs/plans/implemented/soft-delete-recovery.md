# List and recover soft-deleted entries (`deleted` / `undelete`)

**Status**: implemented.

**Follow-up shipped alongside this**: `backup restore --deleted <id>
[--recursive]` - restores a soft-deleted entry straight to disk without
undeleting it in the repository first, for when the goal is just getting
the bytes back rather than reactivating the tree entry. Not in the original
design below; added on request once `deleted`/`undelete` existed to build
on.

## Context

`del`'s own README section already says "a soft-deleted entry stays
recoverable" - true at the storage level (`deleted_at` is just a timestamp;
the row and its content survive until `reclaim-space` actually purges it,
per `migrations.rs`'s schema doc comment), but there's currently no CLI
mechanism to *act* on that recoverability short of restoring an entire
older `meta/repository.sqlite3` snapshot via `backup db restore`. This plan
adds two focused commands that don't require that:

- `backup deleted [path]` - list soft-deleted entries.
- `backup undelete <id>` - reactivate one.

Both only ever work within the `reclaim-space` retention window - once an
entry is actually hard-deleted, `backup db restore` from an older backup
remains the only recovery path, unchanged by this plan.

## Design

### `backup deleted [path]` - listing

New `db::query::deleted_entries(conn, root_id) -> Result<Vec<DeletedEntry>, Error>`,
structurally a sibling of the existing `subtree_entries_with_paths` (same
recursive-CTE-building-paths shape - see `db/src/query.rs`), but with two
differences instead of one: the walk itself must **not** filter to
`deleted_at IS NULL` at each step (a deleted entry's ancestors may
themselves be deleted, e.g. everything under a directory removed by `del
--recursive`), and the final `SELECT` filters to `deleted_at IS NOT NULL`
instead. `root_id = 0` (the repository root) means "search the whole
repository", exactly like `subtree_entries_with_paths`'s own convention -
`backup deleted` (no path) resolves to this. Given a `path` argument, the
CLI resolves it via the existing (active-only) `resolve_path` first, then
scopes the search to that subtree - so `path` must currently be a live,
resolvable directory; browsing *inside* an already-deleted directory by
path isn't possible (its own name isn't resolvable), only by walking from
the whole-repository listing's output.

```rust
pub struct DeletedEntry {
    pub path: String,       // reconstructed from *current* ancestor names -
                             // if an ancestor was renamed after this entry
                             // was deleted, the path reflects the rename,
                             // not the name at deletion time
    pub id: i64,
    pub kind: EntryKind,
    pub deleted_at: i64,
    pub content_id: Option<i64>,
}
```

**Q: what about an entry created, deleted, re-created, and deleted again at
the same path (the question raised in chat)?** Every soft-deleted row is
listed, independently - `deleted_at` is not part of the active-row
uniqueness constraint (`tree_entries_active_name_idx` is a *partial* unique
index, `WHERE deleted_at IS NULL`), so any number of deleted rows can
already coexist for one `(parent_id, name)` - this plan doesn't need a
schema change to support that, just needs to not collapse them in the
listing. Sorted by `path ASC, deleted_at DESC`, so every version of the
same path is grouped together, most recently deleted first - `id` (shown
in the output) is what disambiguates which one `undelete` should act on,
since path alone is no longer unique among deleted rows.

Output format (flat list, mirroring `list`'s `>`/`-` convention):

```
[12345] - docs/old-report.pdf  2.1 MB  deleted 2026-06-01 14:20:03
[12009] - docs/old-report.pdf  1.8 MB  deleted 2026-05-01 09:11:47
[11842] > docs/archive/          deleted 2026-04-15 08:00:00
```

A tree-rendered view was floated in chat as an optional nice-to-have; not
in this plan - the flat, sorted list already directly answers "which
versions of this path were deleted, and when", and a real ASCII-tree
renderer is enough additional formatting work to be worth its own follow-up
if it turns out to be wanted after using the flat form. Not a design
question, just a scope call - flagging it here rather than silently
dropping it.

### `backup undelete <id> [--recursive] [--to <path>]` - reactivation

New `db::undelete(conn, id, recursive, relocate_to: Option<(i64, &str)>) ->
Result<usize, Error>`:

```rust
pub fn undelete(
    conn: &Connection,
    id: i64,
    recursive: bool,
    relocate_to: Option<(i64, &str)>, // (new_parent_id, new_name)
) -> Result<usize, Error> {
    let (target_parent_id, target_name) = match relocate_to {
        Some(t) => t,
        None => /* id's current parent_id/name, via a small join/lookup */,
    };
    if find_tree_entry(conn, target_parent_id, target_name)?.is_some() {
        return Err(Error::AlreadyExists { parent_id: target_parent_id, name: target_name.to_string() });
    }
    let count = if recursive {
        /* UPDATE ... SET deleted_at = NULL WHERE deleted_at = (SELECT deleted_at FROM tree_entries WHERE id = ?1)
           AND id IN ( <same WITH RECURSIVE subtree(id) shape as soft_delete> ) */
    } else {
        /* UPDATE tree_entries SET deleted_at = NULL WHERE id = ?1 AND deleted_at IS NOT NULL */
    };
    if count > 0 && relocate_to.is_some() {
        /* UPDATE tree_entries SET parent_id = ?, name = ? WHERE id = ?1 - only the
           top entry itself moves; descendants already point at its id, unaffected */
    }
    Ok(count)
}
```

Mirrors `soft_delete`'s own recursive-CTE shape exactly, run in reverse:
`soft_delete` marks a whole subtree with one shared `deleted_at` in one
statement; `undelete --recursive` clears `deleted_at` for `id` and every
descendant that **still carries that exact same timestamp** - i.e. exactly
the set `soft_delete` originally touched together, no more. A descendant
independently deleted at a different time (impossible via `del` alone
today, since a soft-deleted entry isn't reachable by path to delete again -
but not ruled out in general, e.g. a future write path that inserts under a
deleted parent) is correctly left alone: its `deleted_at` won't match, so
it's simply not selected by the recursive-then-timestamp-filtered `UPDATE`.
Without `--recursive`, only `id` itself is reactivated - a reactivated
directory's descendants stay deleted until separately undeleted (findable
via `backup deleted <that directory's now-active path>`).

**Q: conflict with an existing active entry at the target (the question
raised in chat)?** Refuse by default with a clear error (reusing
`Error::AlreadyExists`, the same error `rename_entry` already produces for
the identical uniqueness conflict) - no silent auto-rename. `--to <path>`
resolves it explicitly: reactivate into a *different*, currently-unoccupied
location instead of the original one. `<path>`'s parent must already exist
as an active directory (resolved via `resolve_path`) - no `--create-dirs`-
style auto-creation, matching `rename_entry`'s existing behavior (no
auto-create there either). `--recursive` + `--to` together: only the named
entry itself is relocated; its reactivated descendants keep their existing
relative names/structure underneath it unchanged (they already reference
its `id` as `parent_id`, which doesn't change - only *its own* `parent_id`/
`name` do).

The CLI layer (`cli/src/undelete.rs`) does its own pre-checks before
calling `db::undelete`, mirroring `del.rs`'s existing style (`del` resolves
the path and checks `kind` itself before calling `db::soft_delete`, rather
than pushing CLI-friendly error messages into the `db` crate): look up `id`
via `get_tree_entry` (works regardless of active/deleted, already exists)
first, so "no such id" and "id N exists but isn't currently deleted" get
distinct, clear messages instead of both collapsing into `db::undelete`'s
generic `Ok(0)`.

Content/`ref_count` need no special handling: per the schema's own doc
comment, a soft-deleted entry already keeps holding its content's
`ref_count` contribution (only an actual `DELETE` releases it, which
`reclaim-space` is the only thing that does) - and the `tree_entries_ref_
count_*` triggers only fire on `INSERT`/`DELETE` of `tree_entries` rows,
never on `UPDATE`, so flipping `deleted_at` back to `NULL` is already
ref-count-neutral with zero extra code.

### Performance: a new index

`deleted_entries`'s walk touches every row in the tree regardless of how
many are actually deleted (same cost shape as `subtree_entries_with_paths`,
consistent with existing precedent) - acceptable for an occasional,
manually-invoked inspection command. `reclaim_space`'s existing `DELETE
... WHERE deleted_at IS NOT NULL AND deleted_at <= ?1` and `undelete`'s
non-recursive `UPDATE ... WHERE id = ?1 AND deleted_at IS NOT NULL` both
already filter or key on `id`/`deleted_at` without a dedicated index today.
Add `CREATE INDEX tree_entries_deleted_at_idx ON tree_entries(deleted_at)
WHERE deleted_at IS NOT NULL` - a small, low-risk win for `reclaim-space`
on a large, long-lived repository (which now genuinely exist, e.g. the
migrated ~7M-row repository at `dedup/`), independent of this plan's own
two new commands.

**This requires a new migration, not editing `SCHEMA_V1` in place** -
`migrations.rs`'s current single-migration setup assumed "no released data
exists yet" (true when written, no longer true: real repositories,
including that migrated one, already have schema v1 applied and tracked via
`PRAGMA user_version`). Add `SCHEMA_V2` with just the `CREATE INDEX`
statement, `Migrations::new(vec![M::up(SCHEMA_V1), M::up(SCHEMA_V2)])` -
first schema change since the original, first real exercise of this
project's migration story.

## Suggested sequencing

1. `db`: `SCHEMA_V2` migration (the new index) - update the "no released
   data exists yet" comment in `migrations.rs` while touching it.
2. `db`: `DeletedEntry` + `deleted_entries` query + unit tests.
3. `db`: `undelete` + unit tests (non-recursive, recursive/timestamp-scoped,
   conflict without `--to`, relocation with `--to`, content ref_count
   unaffected).
4. `cli`: `deleted.rs` (`DeletedArgs { path: Option<String> }`) + integration
   tests.
5. `cli`: `undelete.rs` (`UndeleteArgs { id: i64, recursive: bool, to:
   Option<String> }`) + integration tests, including the multi-version
   (deleted-then-recreated-then-deleted-again) and conflict/`--to` scenarios
   from chat.
6. Wire both into `main.rs`'s `Command` enum.

## Verification

- `cargo fmt --check && cargo clippy -- -D warnings && cargo test &&
  cargo doc --no-deps`, per `AGENTS.md`.
- Manual smoke test via the `run` skill: delete a file and a directory
  subtree in a temp repo, `backup deleted`, `backup undelete` both back,
  confirm `list`/`stats` see them again; delete-recreate-delete the same
  path twice and confirm both deleted rows are listed with distinct ids;
  provoke a conflict and confirm `--to` resolves it.
- Update `README.md` with `## List And Recover Deleted Entries` (or fold
  into the existing `## Delete A File Or A Directory` section - decide
  while writing it, whichever reads better) documenting both commands.
- Once shipped, move this file under `docs/plans/implemented/`.
