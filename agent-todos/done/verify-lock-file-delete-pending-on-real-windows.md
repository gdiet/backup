# Verify the write-lock file's delete-then-release timing against a real Windows/NTFS mount

**Why parked**: needs a real Windows environment with a real NTFS volume and two genuinely
separate processes racing each other - this session runs in a Linux container with no Windows
target toolchain or real WinFSP/NTFS access.
**Size**: small (a focused runtime check once on real hardware, same pattern as
`verify-write-cache-sparse-file-on-real-windows.md`)
**Opened**: 2026-08-31, by Claude Code on the web session (branch `mount-read-write`)
**Context**: `crates/db/src/lock.rs`, DESIGN-MAINTENANCE-002 in
[`../docs/design/repository-locking.md`](../docs/design/repository-locking.md)

DESIGN-MAINTENANCE-002 has a release path delete the write-lock file while still holding its
`flock`, immediately before the guard itself drops - relying on Rust's default Windows share mode
(`FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE`, confirmed against the real
`std::os::windows::fs::OpenOptionsExt::share_mode` documentation) to make the delete succeed even
while this process's own handle is still open.

What has not been checked against a real Windows/NTFS mount: NTFS keeps a deleted-but-still-open
file's directory entry in a "pending delete" state until every handle referencing it closes, which
here follows only microseconds later (the `flock` guard drops right after the delete call). Two
things to confirm on real hardware:

- Does a second, genuinely separate process's `create_new` attempt, landing inside that narrow
  window, report `AlreadyExists` (meaning `try_acquire_write_lock` needs its own bounded retry
  around exactly that case, not implemented yet) - or does the OS already handle it transparently?
- More generally, run the actual two-process race (start one `dfs mount --read-write` process,
  unmount it, immediately start a second) a few times on a real Windows machine and confirm the
  second acquisition always succeeds promptly, with no leftover lock file and no spurious
  `AlreadyLocked`/`LockUnavailable` error.

Use the `julius-winfsp-ssh` skill to reach a real Windows machine for this check.

## Done

**Completed**: 2026-09-01, by a Claude Code Desktop-App session running natively on `julius`.

Wrote a repeated (500-iteration) two-thread race test directly in `lock.rs` (a same-process,
two-thread race exercises the identical NTFS pending-delete mechanism a genuine two-process race
would, since it is a property of open file handles, not of which process holds them) - one thread
holds and releases the lock in a loop, the other hammers `try_acquire_write_lock` throughout each
release. This surfaced a real, confirmed answer to the open question, and along the way a second,
unrelated real bug found only by finally running these tests on real Windows (both fixed together,
see `docs/design/repository-locking.md` and this commit's message for the full writeup):

- **The race is real and reachable**: a competing `create_new` landing in the pending-delete window
  reports `PermissionDenied` - not `AlreadyExists` as this todo's own question speculated - which
  `try_acquire_write_lock`'s original `AlreadyExists`-only handling did not catch, surfacing a raw,
  non-actionable `Error::Io` instead. Fixed with a short bounded retry
  (`create_new_lock_file_with_pending_delete_retry`, 10 attempts x 1ms) specifically for
  `PermissionDenied`, since that same OS error also covers a genuine, persistent permissions
  problem that must not be silently reinterpreted as lock contention - a result still failing after
  the retries now surfaces as its own distinct `Error::LockFileInaccessible`, not `AlreadyLocked`.
- **Separately found**: the diagnostic marker was unreadable via `read_marker` on Windows whenever
  the lock was actively held, because Windows file locks are mandatory (unlike Unix `flock`) and
  `read_marker` opened a second handle at byte 0, the exact byte fd-lock locks. Fixed by reserving
  byte 0 as a placeholder and reading the marker from byte 1 onward instead - confirmed live that
  this avoids the lock entirely. The marker now also records the OS name, since a WSL2 session on
  this same machine reports the identical hostname as native Windows (confirmed:
  `wsl.exe -d Debian -- hostname` → `julius`), which the developer pointed out while reviewing this.

The two-process framing in this todo's own "Do" list was not literally followed (two real `dfs`
processes vs. two threads) - see the test's own doc comment for why that distinction does not
matter for this specific OS mechanism.
