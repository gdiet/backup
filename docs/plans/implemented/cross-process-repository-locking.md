# Cross-process repository locking (generalized)

**Status**: implemented. `store`, `mount --read-write`, `compact-store`, `reclaim-space`, and
`db restore` now all take the repository's exclusive lock (`RepoLock::acquire`,
`cli/src/repo_lock.rs`), each with its own `--lock-wait <secs>` flag. `db restore`'s residual
incompatibility with concurrent *readers* (the lock doesn't cover them, by design - see "Is a
reader-side lock worth adding? No" below) is documented rather than enforced in code, per the
decision below. Verified on real Windows/WinFSP (julius, 2026-08-13) - see "Windows verification
status" below.

Written up after a conversation surfaced that at the time only `compact-store` took any
cross-process lock - every other repo-mutating command (`store`, `mount --read-write`, `del`,
`undelete`, `fix-problems`, `reclaim-space`, `db compact`, `db restore`, `migrate-scala-repo`)
could run concurrently with itself or with each other, with real corruption risk in some
combinations (see "Why this matters" below).

**Decision** (see "Is a reader-side lock worth adding?" below for the reasoning): scope this to
**writer-vs-writer exclusion**, where "writer" means every command that physically
allocates/relocates store bytes (`store`, `mount --read-write`, `compact-store`, `reclaim-space`)
*or* replaces the whole database file wholesale (`db restore` - initially scoped out of this plan
entirely, then added once review pointed out the same plain exclusive lock the other four already
need also protects `db restore` against those same four, for free, even though it can't protect it
against everything - see "`db restore`: takes the lock too, but only closes part of the gap"
below). No generic reader-side lock - read-only commands keep working exactly as they do today,
unconditionally.

## Why this matters (recap of the triggering discussion)

- Two concurrent `store` runs against the same repo: SQLite itself serializes metadata writes
  (contention at worst, not corruption), but `chunk_store.rs`'s `SpaceAllocator` is built once per
  *process* from a DB snapshot at startup and lives purely in memory - two processes can allocate
  the same byte range to two different chunks. `store/src/lib.rs`'s `LongTermStore` explicitly
  assumes each position is written exactly once; two writers breaks that silently. Real data
  corruption, not just an error.
- `compact-store` already has this exact problem in mind - see its own doc comment
  (`repo_lock.rs:1-8`) and `docs/plans/implemented/compact-store.md`'s "Exclusivity while running"
  section, which is explicit that **nothing else in the codebase enforces cross-process mutual
  exclusion today** - `db/src/lib.rs`'s "one writer connection" doc comment is a *within-process*
  discipline, not a cross-process guarantee.
- `db compact` (`PRAGMA incremental_vacuum`), by contrast, is documented (README.md) as safe to
  run at any time, including alongside a live writer - it doesn't need exclusivity. A naive "one
  global lock, all writers exclude each other and everything else" scheme would make this
  currently-fine combination fail, which would be a regression.

## Scheme

- Every command that writes store bytes (even only potentially, e.g. depending on a flag) -
  `store`, `mount --read-write`, `compact-store`, `reclaim-space` - plus `db restore` (which
  doesn't write store bytes, but replaces the whole database file, a different but equally real
  conflict with the same four) acquires an **exclusive** lock before doing anything. Default:
  don't wait, fail immediately if already held. Configurable: wait up to N seconds (flag,
  `--lock-wait <secs>`).
- No reader-side lock. Read-only commands (`list`, `find`, `deleted`, `problems`, `stats`, `check`,
  `restore`, `db backup`) and metadata-only writers (`del`, `undelete`, `fix-problems`,
  `db compact`) don't touch the lock at all, exactly as today. See "Is a reader-side lock worth
  adding?" for why this was considered and rejected rather than just left out by default. This is
  also exactly why `db restore` isn't *fully* covered even though it takes the lock - see its own
  section below.

## Is this clean/simple to build? Yes, with one caveat

`std::fs::File` already has the exact primitive the exclusive side maps onto: `try_lock`/`lock` -
stabilized in the standard library, no new dependency, confirmed compiling against this project's
toolchain (`rustc 1.97.0`, edition 2024) during this investigation. On Unix this is
`flock(LOCK_EX)`; on Windows, `LockFileEx` with `LOCKFILE_EXCLUSIVE_LOCK`. `repo_lock.rs` already
uses exactly this for `compact-store` - generalizing means wiring the existing mechanism into more
commands, not introducing a new one.

**No built-in timeout in `std`.** `try_lock` returns immediately (`Ok`/`WouldBlock`); `lock`
blocks indefinitely. There is no "block up to N seconds" primitive - "wait up to N seconds" needs
to be built as a poll loop (`try_lock` every ~50-100ms until success or the deadline), not a single
call. Small, well-understood, but real implementation work, not a one-line change.

## Is a reader-side lock worth adding? No

Initially proposed as "readers abort by default (0s wait) if they see an active writer, with an
override to read anyway." Investigated further and rejected: it would close no correctness gap
that writer-vs-writer exclusion doesn't already close, while regressing a routine workflow.

`docs/plans/implemented/compact-store.md`'s crash-safety audit (`compact-store.md:58-95`)
establishes that *every* store-byte-mutating command in this codebase already follows the same
rule: **write new bytes to an unreferenced location first, commit the one DB transaction that
switches the pointer second - never the reverse**. A chunk's old bytes are never overwritten
in-place while a reader's snapshot could still reference them; they only become reusable *after*
the commit that stops referencing them. A reader whose SQLite WAL snapshot predates that commit
keeps seeing the old, untouched bytes at the old location, regardless of what a concurrent writer
is doing. This is exactly why `stats`/`list`/`find`/`check` alongside a live `store` is safe today
without any lock at all.

The one remaining theoretical risk - a *different* writer reusing a just-freed byte range while an
exceptionally slow reader is still reading the old data there - is precisely what the writer-vs-
writer exclusive lock above already prevents (only one store-byte writer can ever be active at
once, so "freed by this run" and "reused by another run" can't overlap in time). It also already
exists today, unaddressed, in the *sequential* case (`reclaim-space` frees a range, a later `store`
run reuses it) and has never been treated as a problem in practice.

Given that, requiring every read command to check a lock and by default abort - forcing an
override flag onto the routine "check `stats` while a backup is running" workflow - would trade
real usability for no actual safety gain. Not implementing it.

## Command classification (verified by reading each subcommand's repository-open call, not assumed)

**Takes the new exclusive lock**: `store`, `mount --read-write`, `compact-store`, `reclaim-space`
(physically allocate/relocate store bytes - the original corruption risk this plan addresses), and
`db restore` (doesn't touch store bytes, but replaces the whole database file wholesale - a
different conflict, but just as real against the same four - see its own section below for why it
only gets *partial* protection from this).

**Stays lock-free, read-only against the repo** (`db::open_repository_read_only`, production code
path): `list`, `find`, `deleted`, `problems`, `stats`, `check`, `restore` (writes to the *restore
target*, not the repo), `db backup`.

**Stays lock-free, metadata-only writes** (`db::open_repository`, a real read-write connection, but
never touches store bytes - normal SQLite transactions, safe under the same snapshot-isolation
argument as the read-only commands): `del`, `undelete`, `fix-problems`, `db compact`,
`migrate-scala-repo` (a one-shot, explicitly supervised operation against what's normally a fresh
target repo - see `compact-store.md:82-90`'s crash-safety note on it).

Conditional: `mount` opens read-only by default, read-write only with `--read-write`
(`mount.rs:260-262`) - it only takes the exclusive lock in the latter case. The synthetic
`[deleted]` folder (`mount_deleted.rs`) is part of the same `mount` write connection, not a
separate command.

## `db restore`: takes the lock too, but only closes part of the gap

`db restore` replaces the entire metadata database file at once - it doesn't fit the "write new
bytes, then commit a pointer-switching transaction" pattern everything else follows, so it's unsafe
next to *any* concurrent access to the repository, not just the other four lock-taking commands:
a reader mid-read against the old file while it's being replaced sees undefined behavior, not a
clean snapshot.

**Initial version of this plan scoped `db restore` out of the lock entirely**, reasoning that
since full protection (readers included) wasn't achievable without either a second lock kind or
making every read command lock-aware (both rejected above), there was no point taking the lock at
all - document the whole requirement instead and leave enforcement to the operator, the same way
`migrate-scala-repo`'s narrower one-shot caveat is handled.

**That reasoning was wrong, caught in review**: "can't get full protection" doesn't imply "get no
protection." `db restore` can simply take the *same* plain exclusive lock the other four already
use - no new lock kind, no reader-awareness anywhere - and that closes the real, previously
completely unaddressed risk of `db restore` racing `store`/`mount --read-write`/`compact-store`/
`reclaim-space` (a decent lower bound: those four are the more likely, more automatable-to-guard
concurrent commands - a script/cron `store` running when someone happens to restore a database is
far more plausible than a human deliberately opening `check` mid-restore). What remains genuinely
unprotected - a concurrent *read-only* command - is exactly the same reader gap already accepted
everywhere else in this plan (see "Is a reader-side lock worth adding? No" above), not a new,
`db restore`-specific one. Documenting *that* narrower remaining gap (README.md, the command's own
`--help`/doc comment: still run this only when nothing else at all is accessing the repository) is
the right amount of manual-discipline reliance, not the whole thing.

## Interaction with genuinely read-only repositories

`docs/plans/implemented/read-only-repository-access.md` deliberately made every read-only command
work against a truly `:ro`-mounted repository directory (the Docker/Samba setup's original
trigger). Since this plan adds no reader-side lock, that's unaffected either way - read-only
commands never touch `meta/.lock` at all, on a `:ro` mount or otherwise.

Worth noting for completeness: `RepoLock::try_acquire`'s current implementation opens the lock file
with `.write(true).create(true)` - on a real `:ro` mount this fails with `EROFS`, landing in
`compact-store`'s generic "failed to acquire the repository lock" error branch
(`compact_store.rs:62-64`) rather than a clean "not applicable, repository is read-only" message.
That's pre-existing behavior, not something this plan changes - every command that would newly
take this lock (`store`, `mount --read-write`, `reclaim-space`, `db restore`) already requires a
writable repository to do anything useful anyway, so a confusing lock-acquisition error instead of
a confusing "read-only filesystem" error at the first actual write isn't a regression, just a
possible future polish item (a clearer message) if it comes up in practice.

## Windows verification status

Verified on real Windows/WinFSP (julius, native release build, 2026-08-13) - see
`agent-todos/done/verify-repo-lock-on-windows.md` for the full session record. Summary: a
`mount --read-write` lock holder, and a second real process (`store`) against the same
repository, confirmed all three documented behaviors -

1. Without `--lock-wait`: the second process fails immediately with the expected "meta/.lock is
   held" message.
2. With `--lock-wait <secs>`: the second process blocks, then succeeds once the holder is
   terminated (confirmed via `taskkill /F` on the holder's PID - `LockFileEx`'s OS-level release
   fires correctly even on a forceful/non-graceful termination, not just a clean shutdown).
3. A third access, started immediately after the abrupt kill with no `--lock-wait` at all,
   acquires the lock right away with no stale hold left behind - repeated as its own explicit pass
   after (2) already demonstrated the same thing incidentally.

One incidental, unrelated finding along the way: `reclaim-space` against a repository with an
actively-open `mount --read-write` elsewhere failed with a confusing "found a pending
write-ahead-log file ... run `db compact`" error instead of the expected lock message - not a
locking bug, `RepoLock` itself was already correct. `reclaim-space` ran its automatic `db backup`
step *before* acquiring the lock, and that backup step opens read-only
(`db::open_repository_read_only`), which correctly refuses a live, uncheckpointed WAL - it just
didn't mention "or another process currently has it open" as a possible cause, only suggesting
`db compact` (not actually the right remedy when the real cause is a concurrent writer). Fixed
(not just documented) in `docs/plans/implemented/reclaim-space-misleading-wal-error.md`: the lock
is now acquired first, so a concurrent writer is always caught by the correct message.

## Implementation (done)

1. `repo_lock.rs`'s `try_acquire` became `acquire(meta_dir, wait: Duration)` - a poll loop
   (`try_lock` every 100ms) when `wait` is non-zero, identical to the old behavior at
   `Duration::ZERO`. No shared-lock addition, per the decision above.
2. Exclusive acquisition wired into `store` (right after `db::open_repository`, before opening the
   write connection), `mount --read-write` (inside `build_filesystem`, held for the mount's whole
   lifetime via a new `Inner::_repo_lock` field - `None` for a read-only mount, which never touches
   the lock at all), and `reclaim-space` (after its own `db::open_repository`, after the optional
   automatic `db backup` step). Each command's `--lock-wait <secs>` flag defaults to `0`.
3. `compact-store` migrated onto the same `acquire` API - one code path now, not two.
4. `db restore` also wired onto the same `acquire` API (right after confirming the backup file
   exists, before extracting/staging anything) - added in a second pass after review caught that
   the initial "documented risk, not a code exception" framing was giving up more protection than
   necessary (see "`db restore`: takes the lock too, but only closes part of the gap" above). Its
   remaining, narrower gap (concurrent read-only commands, which the lock deliberately never
   covers for anything) is documented in its `DbCommand::Restore` doc comment (surfaces via
   `--help`), a code comment on `run_restore_db`, and README.md.
5. README.md gained a "Running Multiple Commands At Once" section covering all five commands'
   `--lock-wait` behavior and the exclusivity matrix between them, plus `db restore`'s residual
   manual-discipline requirement.

Verified: `cargo fmt --check`, `cargo clippy --workspace --all-targets -- -D warnings`,
`cargo test --workspace`, `cargo doc --no-deps --workspace` all clean. New tests: `repo_lock.rs`
covers `acquire` directly (zero-wait conflict, release-unblocks, wait-then-timeout,
wait-then-succeeds-mid-wait via a second thread); `store.rs`/`compact_store.rs`/`reclaim_space.rs`/
`db_maintenance.rs` each got a `_refuses_when_the_lock_is_already_held` test
(`compact_store.rs`/`db_maintenance.rs` also got a `_waits_for_the_lock_via_lock_wait_and_then_
succeeds` end-to-end test); `mount.rs` got both a refusal test and a same-lock-held
read-only-mount-is-unaffected test, directly against `build_filesystem` rather than a full FUSE
mount (no need to exercise the mount machinery itself just to prove the lock wiring).

Verified: real Windows/WinFSP behavior (julius, 2026-08-13) - see "Windows verification status"
above.
