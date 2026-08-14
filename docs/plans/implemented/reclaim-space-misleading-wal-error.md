# `reclaim-space`: misleading WAL error instead of the lock message

**Status**: proposed, not started. Surfaced while verifying `docs/plans/implemented/
cross-process-repository-locking.md` on real Windows/WinFSP (julius, 2026-08-13) - see that doc's
"Windows verification status" section for the original observation.

## What was found

Running `reclaim-space` against a repository that another process already has open via
`mount --read-write` fails with:

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

This isn't a locking bug - `RepoLock` itself was verified working correctly (three separate
scenarios, all matching the documented design). It's specifically that `reclaim-space` never
reaches its own lock check in this case, and the message it gives instead actively suggests the
wrong remedy: `db compact` isn't going to help while another writer still has the repository open,
and would most likely just fail the same way (or hit the same lock) itself.

## Why this matters

This isn't an obscure edge case - `reclaim-space` vs. an active `mount --read-write` is exactly
the scenario `RepoLock` exists to guard against, so it's a realistic way for a real user to
actually encounter this. Getting a confusing, wrong-remedy error here instead of the clear
"meta/.lock is held" message undermines the very thing the locking feature was built to make
clear.

## Root cause

`run_reclaim_space` (`cli/src/reclaim_space.rs:40-58`) runs its automatic pre-`reclaim-space`
database backup (`db_maintenance::run_backup`, skippable via `--no-backup`) *before* opening the
repository for its own purposes and *before* calling `RepoLock::acquire` (which only happens at
line 65, after that backup step and after `db::open_repository`). That backup step opens the
database **read-only** internally, which correctly refuses to back up a database with a live,
uncheckpointed `-wal` file (`db::open_repository_read_only`, see `Error::UncheckpointedWal` in
`db/src/error.rs`) - a real, legitimate safety check in general (a backup taken mid-WAL could be a
torn snapshot), it just isn't the actual problem here: the WAL is live because another process
legitimately has the repository open right now, not because of a stale/uncommitted leftover from
a crash.

## Proposed fix

Reorder: acquire `RepoLock` *before* running the automatic backup step, not after. Then a
concurrent writer is caught by the existing, correct "meta/.lock is held" message immediately,
before the backup step (or anything else) ever runs - matching how `store`/`mount --read-write`/
`db restore` already acquire the lock as one of their first actions. Holding the lock across the
backup step itself seems harmless or even desirable (the backup is meant to protect the following
destructive reclaim operation; there's no obvious reason it needs to run lock-free first).

No new error variant needed - this is purely a reordering of two existing steps. Worth checking
whether anything currently relies on the backup running before the lock is held (nothing found so
far, but not yet confirmed by actually attempting the change).

## Open questions

- Confirm there's no deliberate reason for the current ordering (e.g. avoiding holding the lock
  for a potentially-long backup) before just swapping the two steps - no such reason is documented
  at either call site today, but worth a deliberate check rather than assuming.
- Whether `compact-store`'s own automatic-backup-if-any (if it has one) has the same ordering
  issue - not checked yet.
