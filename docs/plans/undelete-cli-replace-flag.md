# `backup undelete`: optional `--replace`/overwrite flag?

**Status**: open question, not a design or an implementation - asking whether this is even wanted
before doing either. Surfaced while implementing `docs/plans/implemented/mount-rename-overwrite.md`
(2026-08-13).

## Background

That plan gave the mount's `rename`/`[deleted]`-recovery path real POSIX replace semantics: dragging
a recovered file onto an existing active file now replaces it (soft-deleting the old target,
recoverable via `[deleted]`) instead of failing. The CLI `backup undelete --to` command
deliberately did **not** get the same default - it still always fails if the target is occupied,
preserving its own `--to` help text's existing promise: "fails otherwise, rather than silently
renaming". Rationale at the time: no one asked for CLI-side replace, and the mount's case (a GUI
overwrite dialog the user already confirmed) doesn't obviously generalize to a bare CLI command run
without that same confirmation step.

## The question

Is CLI-side replace (`backup undelete --to <path> --replace`, or similar) actually wanted as a
feature? Not obviously needed - nothing currently forces a `backup undelete --to` user through an
occupied-target failure without recourse (`--to <a-different-path>` always works) - but also not
obviously unwanted, and cheap enough to ask about explicitly rather than silently deciding either
way.

## If wanted: rough shape

Trivial to wire, since the underlying capability already exists and is already exercised by the
mount:

- A new `--replace` (or `--force`/`--overwrite`, naming TBD) flag on `UndeleteArgs`
  (`cli/src/undelete.rs`), off by default (preserves today's behavior and the `--to` help text's
  existing promise unless explicitly opted into).
- `run_undelete` passes `!args.replace` as `db::undelete`'s `no_replace` argument instead of the
  current hardcoded `true`.
- `Error::TargetIsADirectory`/`TargetIsAFile`/`TargetNotEmpty` (added for the mount's own replace
  path) currently fall through `run_undelete`'s generic `Err(err) => eprintln!("error: {err}")`
  branch - fine as a first cut, but worth a dedicated, clearer message per variant if this ships,
  matching the existing dedicated `AlreadyExists` handling.
- `--to`'s own doc comment needs updating either way: either to describe the new flag, or (if this
  is decided against) left as-is with no change needed.
- README's `undelete` section and a new test (mirroring `cli::mount::tests`' existing
  `rename_replaces_a_compatible_active_target_by_default` pattern) if implemented.

## Not yet decided

Whether to build this at all - waiting on an explicit "yes" before doing so.
