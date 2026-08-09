# List and fix files with missing/short store data (`problems` / `fix-problems`)

**Status**: implemented.

## Context

`mount`'s read path already does the right thing when store data behind a
file is physically missing or shorter than recorded: it surfaces an explicit
`EIO` for exactly the affected byte range, never silently zero-fills (see
`cli/src/mount.rs`'s `read_persisted`, and `backup check`'s equivalent
per-chunk detection in `cli/src/check.rs`). What's missing is a way to find
*which files* are affected without stumbling into them one read at a time,
and a way to clean them up in bulk: soft-delete them, optionally leaving a
0-byte placeholder behind at the same path with the same timestamp so the
tree doesn't just have a hole where the entry used to be.

Two new sibling commands, following the existing `deleted`/`undelete`
precedent (two flat top-level commands, not a nested subcommand like `db`):

- `backup problems [path]` - list active files affected by missing/short
  store data.
- `backup fix-problems [path] [--replace-empty]` - soft-delete every such
  file currently found in scope; with `--replace-empty`, also insert a
  0-byte replacement file at the same path, keeping the original's
  timestamp.

## Scope decision

"Problem" here means specifically `ReadIntegrity::Incomplete` - missing or
too-short data files, the same category `check` labels `MISSING` (and the
only category `mount` currently turns into `EIO` rather than a data
problem). `check`'s other two categories - `BAD` (length or hash mismatch,
i.e. wrong-but-present bytes) - are a different failure mode (corruption,
not absence) and out of scope: soft-delete-and-replace would destroy bytes
that are still there and might still be partially useful or manually
recoverable, which isn't this feature's job. If that turns out to matter
later, it's a separate, explicit extension - not bundled in here.

## Design

### Detection

Reuses `check`'s existing per-chunk detection almost entirely, just
re-aggregated to file granularity instead of printed per chunk:

1. Get the chunk scope exactly as `check` does today (`db::all_chunks` for
   the whole repository, or `check`'s `scoped_chunks(conn, path)` for a
   given path) - `check.rs`'s `scoped_chunks` becomes `pub(crate)` so
   `problems.rs` can reuse it instead of duplicating the walk.
2. For each chunk in scope, `read_chunk_bytes` it and keep the ids of every
   chunk that comes back `ReadIntegrity::Incomplete`.
3. New `db::query::contents_for_chunk(conn, chunk_id) -> Vec<i64>` (`SELECT
   DISTINCT content_id FROM content_chunks WHERE chunk_id = ?1`, index
   already exists on `chunk_id`) maps each broken chunk to the content(s)
   built from it.
4. The existing `db::entries_for_content(conn, content_id)` maps each
   broken content to every active file referencing it - naturally covers
   the dedup case where the same broken content is shared by more than one
   path.
5. New `db::query::path_of(conn, id) -> Result<Option<String>, Error>`
   reconstructs a single entry's full path by walking `parent_id` up to the
   root (the reverse of `resolve_path`) - cheap for the small, occasional
   result set this command deals with; unlike `subtree_entries_with_paths`,
   it doesn't walk the whole tree, so it's the wrong tool for anything that
   needs many paths at once.

Note: with `path` given, chunk *scanning* is scoped to files reachable from
`path`, but a broken chunk found that way may be shared (via dedup) by a
file living *outside* `path` - that file is still reported. This is
intentional, not a bug: it genuinely has the same problem, and hiding it
because it happens to be out of the scan scope would be misleading.

### `backup problems [path]`

Runs detection, prints one line per affected file (path, size, and how many
of its chunks are affected), exit code non-zero if anything was found -
mirroring `check`'s exit-code convention.

### `backup fix-problems [path] [--replace-empty]`

Runs the same detection fresh (not against a prior `problems` run's
output - there's no id-based selection to go stale, unlike `undelete`,
since re-running detection always reflects current reality) and, for every
affected file:

- `db::soft_delete(conn, id, now_millis())` - same as `del`, deletion
  timestamp is "now", not the file's own mtime.
- If `--replace-empty`: additionally `db::apply_backup_batch` with a
  `FileBackupRecord` at the same `parent_id`/`name`, `content:
  ContentSource::Resolved { chunks: vec![], content_hash: vec![] }` (an
  empty `chunks` list resolves to `content_id = NULL` without ever looking
  at `content_hash` - see `resolve_content`'s doc comment - so there's no
  need to compute a real hash here), and `time_millis` set to the
  *original* entry's `time_millis` - "same timestamp" per the request,
  meaning the replacement file's own mtime, not the deletion timestamp
  above. The explicit `soft_delete` runs first so `apply_backup_batch`'s
  `find_tree_entry` lookup no longer sees an active entry at that name and
  takes its plain-insert path, rather than its changed-content
  soft-delete-and-insert path (which would otherwise stamp the *old* row's
  `deleted_at` with the replacement's mtime instead of "now" - confusing
  for `backup deleted` to show later).

No id-based selection, no confirmation prompt - consistent with
`del`/`reclaim-space`'s existing philosophy (scoped bulk action, not a
guarded one-off).

## Verification checklist

- `cargo fmt --check && cargo clippy --workspace --all-targets -- -D
  warnings && cargo test --workspace && cargo doc --no-deps --workspace`.
- Update `README.md` with a `## Find And Fix Files With Missing Store
  Data` section near `## Check Integrity`, and its table-of-contents entry.
- Once shipped, move this file under `docs/plans/implemented/`.
