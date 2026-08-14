# `reclaim-space`: misleading WAL error instead of the lock message

**Status**: implemented (2026-08-14). Surfaced while verifying `docs/plans/implemented/
cross-process-repository-locking.md` on real Windows/WinFSP (julius, 2026-08-13) - see that doc's
"Windows verification status" section for the original observation.

## What was found

Running `reclaim-space` against a repository that another process already has open via
`mount --read-write` used to fail with:

```
error: failed to open repository at ...: found a pending write-ahead-log file (-wal) next to the
metadata database, not yet folded into it - run `db compact` once to clean this up before using a
read-only command
```

instead of the expected, correct-for-this-situation:

```
error: another command is already running against this repository (meta/.lock is held) - try
again once it finishes, or pass --lock-wait to wait
```

This was never a locking bug - `RepoLock` itself was verified working correctly (three separate
scenarios, all matching the documented design, see the cross-process-repository-locking doc). It
was specifically that `reclaim-space` never reached its own lock check in this case, and the
message it gave instead actively suggested the wrong remedy: `db compact` wasn't going to help
while another writer still had the repository open, and would most likely just have failed the
same way (or hit the same lock) itself.

## Root cause

`run_reclaim_space` (`cli/src/reclaim_space.rs`) ran its automatic pre-`reclaim-space` database
backup (`db_maintenance::run_backup`, skippable via `--no-backup`) *before* opening the repository
for its own purposes and *before* calling `RepoLock::acquire`. That backup step opens the database
**read-only** internally, which correctly refuses to back up a database with a live,
uncheckpointed `-wal` file (`db::open_repository_read_only`, see `Error::UncheckpointedWal` in
`db/src/error.rs`) - a real, legitimate safety check in general (a backup taken mid-WAL could be a
torn snapshot), it just wasn't the actual problem here: the WAL was live because another process
legitimately had the repository open right now, not because of a stale/uncommitted leftover from a
crash.

## Fix

Reordered: `RepoLock::acquire` now runs first, before the automatic backup step and before opening
the repository at all - `RepoLock::acquire` only ever needed `db::meta_dir(repo)`, a plain path
computation, so nothing else had to move to make this work. A concurrent writer is now caught by
the existing, correct "meta/.lock is held" message immediately, matching how
`store`/`mount --read-write`/`db restore` already acquire the lock as one of their first actions.
No new error variant needed - purely a reordering of existing steps.

Checked and confirmed rather than assumed: `compact-store` (the other open question from the
original write-up) has no automatic-backup step at all - its own lock acquisition already runs
right after `db::open_repository`, nothing to reorder there.

Verified: `cargo fmt --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test
--workspace`, `cargo doc --no-deps --workspace` all clean. New regression test
(`run_reclaim_space_checks_the_lock_before_attempting_the_automatic_backup`): with the lock already
held and `--no-backup` *not* given, asserts no file appears under `meta/backups/` - proof the
backup step never ran at all, not just that the command failed for some reason.
