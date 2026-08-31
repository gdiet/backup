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
