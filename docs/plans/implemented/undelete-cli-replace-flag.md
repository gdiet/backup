# `backup undelete`: optional `--replace`/overwrite flag

**Status**: implemented (2026-08-14). Surfaced while implementing `docs/plans/implemented/
mount-rename-overwrite.md` (2026-08-13).

## Background

That plan gave the mount's `rename`/`[deleted]`-recovery path real POSIX replace semantics:
dragging a recovered file onto an existing active file now replaces it (soft-deleting the old
target, recoverable via `[deleted]`) instead of failing. The CLI `backup undelete --to` command
deliberately did not get the same default at the time - it kept always failing if the target was
occupied, preserving its own `--to` help text's existing promise. Filed as an open question rather
than decided silently either way.

## Decision

Yes, build it - as an explicit, off-by-default `--replace` flag, not a change to the default.

## What was built

- `UndeleteArgs` (`cli/src/undelete.rs`) gained `--replace` (`bool`, default `false`).
  `run_undelete` passes `!args.replace` as `db::undelete`'s `no_replace` argument, replacing the
  previous hardcoded `true`.
- Dedicated error messages for all three kind-mismatch variants
  (`TargetIsADirectory`/`TargetIsAFile`/`TargetNotEmpty`), matching the existing `AlreadyExists`
  handling rather than falling through the generic `Err(err) => eprintln!("error: {err}")` branch -
  these are common enough failure modes (a real user actively choosing `--replace` will hit them)
  to deserve a clear message, not the raw `db::Error` `Display` text.
  `AlreadyExists`'s own message now also mentions `--replace` as an alternative to `--to`.
- `--to`'s doc comment updated to mention `--replace` as the other way to resolve a conflict.
  README's `undelete` section documents the new flag and its kind-compatibility/non-empty-directory
  caveats.
- Two new tests: `--replace` reactivating over a compatible active entry (and the replaced entry
  ending up soft-deleted, not gone); `--replace` still refusing cleanly on an incompatible kind
  (an active directory occupying the name a file wants to reactivate into), leaving the deleted
  entry untouched.

Verified: `cargo fmt --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test
--workspace`, `cargo doc --no-deps --workspace` all clean.
