# Cross-process repository locking (generalized)

**Status**: design only, not implemented. Written up after a conversation surfaced that today
only `compact-store` takes any cross-process lock (`cli/src/repo_lock.rs`) - every other
repo-mutating command (`store`, `mount --read-write`, `del`, `undelete`, `fix-problems`,
`reclaim-space`, `db compact`, `db restore`, `migrate-scala-repo`) can run concurrently with
itself or with each other today, with real corruption risk in some combinations (see "Why this
matters" below).

**Decision** (see "Is a reader-side lock worth adding?" below for the reasoning): scope this to
**writer-vs-writer exclusion only** (the commands that physically allocate/relocate store bytes:
`store`, `mount --read-write`, `compact-store`, `reclaim-space`). No generic reader-side lock -
read-only commands keep working exactly as they do today, unconditionally. `db restore`'s
incompatibility with concurrent readers is handled as a documentation note, not a code exception -
see its own section below.

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
  `store`, `mount --read-write`, `compact-store`, `reclaim-space` - acquires an **exclusive** lock
  before doing anything. Default: don't wait, fail immediately if already held. Configurable: wait
  up to N seconds (flag, e.g. `--lock-wait <secs>`).
- No reader-side lock. Read-only commands (`list`, `find`, `deleted`, `problems`, `stats`, `check`,
  `restore`, `db backup`) and metadata-only writers (`del`, `undelete`, `fix-problems`,
  `db compact`) don't touch the lock at all, exactly as today. See "Is a reader-side lock worth
  adding?" for why this was considered and rejected rather than just left out by default.

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

**Takes the new exclusive lock** (physically allocates/relocates store bytes, the actual
corruption risk this plan addresses): `store`, `mount --read-write`, `compact-store`,
`reclaim-space`.

**Stays lock-free, read-only against the repo** (`db::open_repository_read_only`, production code
path): `list`, `find`, `deleted`, `problems`, `stats`, `check`, `restore` (writes to the *restore
target*, not the repo), `db backup`.

**Stays lock-free, metadata-only writes** (`db::open_repository`, a real read-write connection, but
never touches store bytes - normal SQLite transactions, safe under the same snapshot-isolation
argument as the read-only commands): `del`, `undelete`, `fix-problems`, `db compact`,
`migrate-scala-repo` (a one-shot, explicitly supervised operation against what's normally a fresh
target repo - see `compact-store.md:82-90`'s crash-safety note on it).

**Special case, not covered by this scheme**: `db restore` - see its own section below.

Conditional: `mount` opens read-only by default, read-write only with `--read-write`
(`mount.rs:260-262`) - it only takes the exclusive lock in the latter case. The synthetic
`[deleted]` folder (`mount_deleted.rs`) is part of the same `mount` write connection, not a
separate command.

## `db restore`: documented risk, not a code exception

`db restore` replaces the entire metadata database file at once - it doesn't fit the "write new
bytes, then commit a pointer-switching transaction" pattern everything else follows, so it's not
just unsafe against other *writers* (which the exclusive lock above doesn't cover it against
either, since it's not in the "takes the lock" list) but against *any* concurrent access to the
repository, readers included: a reader mid-read against the old file while it's being replaced
sees undefined behavior, not a clean snapshot.

Decision: **don't extend the locking mechanism to cover this** - it would mean either giving `db
restore` a third lock flavor ("exclusive against literally everyone, readers included") that
nothing else needs, or forcing every read command to check a lock after all (the thing rejected
above). Given `db restore` is already a rare, deliberately manual, supervised operation (correcting
a `meta/repository.db` from a `db backup` archive, not something run as part of routine workflows),
the simpler answer is to document the requirement clearly (README.md and the command's own
`--help`/doc comment: *run this only when nothing else is accessing the repository*) and leave
enforcement to the operator, the same way this project already handles `migrate-scala-repo`'s
narrower one-shot caveat.

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
take this lock (`store`, `mount --read-write`, `reclaim-space`) already requires a writable
repository to do anything useful anyway, so a confusing lock-acquisition error instead of a
confusing "read-only filesystem" error at the first actual write isn't a regression, just a
possible future polish item (a clearer message) if it comes up in practice.

## Windows verification status

Not yet checked against a real Windows/WinFSP mount - `std::fs::File`'s lock methods are
documented as cross-platform (`LockFileEx` under the hood on Windows) and nothing here is
Unix-specific, but this project's own convention (see `agent-todos/README.md`) is to treat
"verified on Linux only" and "verified on real Windows" as different states for anything touching
`mount`, given `mount --read-write` runs in production on Windows via WinFSP. Once this is
actually implemented, add an `agent-todos/` entry (or verify directly if a Windows/WinFSP session
is available at the time) confirming exclusive locking on `meta/.lock` behaves as expected across
two real Windows processes, not just in theory.

## Suggested scope for a first implementation pass (not decided, just a starting point)

1. Extend `repo_lock.rs` with a poll-based wait-with-timeout wrapper around the existing exclusive
   `try_acquire` (the `--lock-wait <secs>` behavior) - no shared-lock addition needed, per the
   decision above.
2. Wire exclusive acquisition into `store`, `mount --read-write`, and `reclaim-space` (only
   `compact-store` has it today).
3. Migrate `compact-store`'s existing call onto the same wait-enabled API so there's exactly one
   code path, not two.
4. Document `db restore`'s exclusivity requirement in README.md and its own doc comment (no code
   change).
