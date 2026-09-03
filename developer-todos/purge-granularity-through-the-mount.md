# Decide purge granularity through the mount (REQ-MOUNT-007)

**Noted**: 2026-09-03, while discussing REQ-MOUNT-004/007/008's implementation (the `[deleted]`
view and permanent-purge opt-in through the mount), right after implementing `dfs del`/`--purge`
(REQ-CLI-003) and its underlying `db::Repository::purge_deleted_entry`.
**Size**: medium - confirm with the developer before starting (this file exists specifically to
park that decision).
**Context**: `requirements/functional/mount.md` (REQ-MOUNT-004/007/008); `crates/db/src/tree.rs`'s
`purge_deleted_entry`; `crates/cli/src/del.rs`; REQ-TREE-008 in `requirements/functional/tree.md`
(non-cascading `rmdir`).

The developer asked how mount-side purge (REQ-MOUNT-007's second opt-in) should work, before
picking up the implementation. REQ-MOUNT-007 already settles the high-level model: two independent,
escalating mount-time opt-ins - the base one (REQ-MOUNT-004) makes the `[deleted]` view visible and
supports recovery by moving an entry out; a second, separate opt-in additionally allows deleting
*from inside* the view, which permanently purges (REQ-CLI-003's `--purge` operation, reached
through the mount). Without the second opt-in, any delete/create/rename/move into or within the
view (other than the recovery move-out) fails with a clear error (`EACCES`/`EPERM`), never a false
success.

One point REQ-MOUNT-007 does not spell out explicitly, but its own rationale text implies: **purge
granularity for a directory**. `db::Repository::purge_deleted_entry` (built for `dfs del --purge`)
is deliberately recursive - a CLI call names one path and must handle everything beneath it in one
shot. Reused as-is for the mount, this would be inconsistent with how a real recursive delete
actually reaches the mount: `rm -rf`/Explorer/Nautilus have no "delete recursively" syscall to
issue - they always walk the tree themselves, bottom-up, and issue one `unlink`/`rmdir` call per
entry. REQ-MOUNT-007's own wording already assumes this: "a recursive delete ... descending into a
directory's view under the second opt-in purges its history as it goes, incidentally settling that
such a directory can eventually be fully removed, **once its history is gone**" - i.e. step by
step, not in one cascading DB call.

**Proposed answer** (not yet decided): mount-side `rmdir` on a still-non-empty `[deleted]`-view
directory should refuse with the same `ENOTEMPTY` ordinary `rmdir` already gives for a live,
non-empty directory (REQ-TREE-008) - forcing the caller to empty it first, which any real
recursive-delete tool does anyway by walking bottom-up. Concretely: give
`db::Repository::purge_deleted_entry` a `recursive: bool` parameter (the same shape of decision
`dfs del`'s own `--recursive` flag already made) - the CLI's `--purge` keeps calling it with
`recursive = true` (today's unchanged, one-shot behavior); the mount's `unlink`/`rmdir` handlers
call it with `recursive = false`, which refuses `Error::DirectoryNotEmpty` (already mapped to
`Errno::ENOTEMPTY` in `crates/cli/src/dedup_fs.rs::to_errno`) if soft-deleted children remain.

Also still open, secondary to the granularity question above: how the two opt-ins are actually
exposed as `dfs mount` flags (naming - e.g. reusing `--show-deleted` for the base one, to match
`dfs list --show-deleted`'s vocabulary, and reusing `--purge` for the second, to match `dfs del
--purge`'s), and whether the base opt-in's visibility is gated behind `--read-write` or available on
a read-only mount too (REQ-MOUNT-004's text is not fully explicit about this).

When picked up: confirm the `recursive: bool` approach (or an alternative, e.g. two separate
functions instead of one flag) with the developer, then implement REQ-MOUNT-004/007/008's actual
mount-side wiring - none of it exists yet in `crates/cli/src/dedup_fs.rs`/`mount.rs` today.
