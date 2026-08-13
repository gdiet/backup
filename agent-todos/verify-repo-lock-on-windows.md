# Verify the repository lock works correctly on real Windows/WinFSP

**Needs**: a real Windows machine (in practice, "julius" - see the `julius-winfsp-ssh` skill) to
run two real `backup` processes against the same repository and confirm the exclusive lock
(`std::fs::File::try_lock`/`lock`, via `cli/src/repo_lock.rs`) actually behaves as expected -
`LockFileEx`'s semantics under the hood are documented as equivalent to Unix `flock`, but this
hasn't been checked against a real Windows process pair, only reasoned about and tested on Linux.
**Size**: small - a focused check, not a new feature. No need to ask before starting, just report
findings (and fix anything that doesn't hold up, confirming with the user first if the fix is
more than trivial).
**Opened**: 2026-08-13, by a Linux/WSL2 session, implementing
`docs/plans/implemented/cross-process-repository-locking.md`.
**Context**: `docs/plans/implemented/cross-process-repository-locking.md` (the full design/
implementation) and `cli/src/repo_lock.rs` (the lock itself, used by `store`, `mount --read-write`,
`compact-store`, `reclaim-space`, and `db restore`).

## What to actually do

1. Build `backup` on julius (native, not cross-compiled - see the `wsl-windows-sync`/
   `julius-winfsp-ssh` skills for why).
2. Init a disposable test repository.
3. Start a long-running lock holder against it - easiest is `backup mount --read-write` on some
   mountpoint (blocks until unmounted, so it holds the lock for as long as needed), or a
   `compact-store --lock-wait 30` run against a repository big enough to take a few seconds.
4. From a second, real Windows process (a second terminal, not the same shell session - the lock
   is per-`File`-handle, so it needs to be a genuinely separate process), try `backup store` (or
   `reclaim-space`) against the same repository:
   - Without `--lock-wait`: confirm it fails immediately with the "meta/.lock is held" message.
   - With `--lock-wait <secs>` larger than how long the first process holds the lock: confirm it
     waits and then succeeds once the first process releases it (unmount / `compact-store`
     finishes).
5. Kill the lock-holding process abruptly (Ctrl+C, or `taskkill /F` if that doesn't propagate
   cleanly - see the `julius-winfsp-ssh` skill's SSH/Job-Object notes if running the holder over
   SSH) and confirm a third command afterward acquires the lock immediately, i.e. the OS actually
   released it rather than leaving a stale hold behind.

## Report back

Update `docs/plans/implemented/cross-process-repository-locking.md`'s "Windows verification
status"/top status line with the result. If everything behaves as expected, this is just closing
out the open question, not a code change. If something doesn't match (e.g. the lock doesn't
release on an abrupt kill, or waits/fails don't behave as documented), that's a real finding -
flag it clearly rather than silently working around it.
