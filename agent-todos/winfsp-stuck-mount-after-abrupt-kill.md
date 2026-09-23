# Does WinFSP get stuck after an abruptly-killed mount, outside of tests too?

**Why parked**: a side finding, not investigated - out of scope for the task that found it
(`agent-todos/done/real-mount-backpressure-test-fails-on-real-windows.md`, a test-only bug fix).
**Size**: small-medium - a focused empirical check on real Windows/WinFSP, similar in shape to the
dispatch-pool measurement todos already done.
**Opened**: 2026-09-23, by a Claude Code Desktop-App session on `julius` (native Windows, real
WinFSP).
**Context**: `agent-todos/done/real-mount-backpressure-test-fails-on-real-windows.md`'s own "Done"
section; `crates/mountfs/src/windows/mod.rs`.

While investigating a broken test, found that killing a process whose thread was blocked inside
`mountfs::mount()`'s WinFSP dispatch loop - abruptly, mid-syscall, not via a real unmount or a
graceful Ctrl+C shutdown - left WinFSP in a state where a *later, unrelated* mount attempt failed
with `Cannot create WinFsp-FUSE file system: mount point in use.`, reproducible across several
retries over roughly 30 seconds before eventually clearing on its own.

This was only observed via a test's own broken (Linux-only) mount/unmount mechanism running on
Windows, not via a real `dfs mount` session - so it is not known whether this is:

- Specific to the particular way that test's mount thread died (killed as a side effect of the
  whole test *process* ending while non-joined, not a `Ctrl+C`/console-signal-driven shutdown at
  all), or
- A more general WinFSP characteristic that a real `dfs mount --read-write` session would also hit
  if its own process crashed or was force-killed (Task Manager "End Task", a power loss, an
  unhandled panic) while a mount was live - which would matter operationally: an operator whose
  mount crashed might then find the *next* `dfs mount` attempt against the same repository also
  fails with a confusing "mount point in use" error, unrelated to the repository's own write-lock
  mechanism (DESIGN-MAINTENANCE-001) and not something `dfs unlock` addresses at all.

## Suggested approach

On real Windows/WinFSP: mount a throwaway repository via the real `dfs mount --read-write` CLI
(not a test's in-process thread), force-kill the process abruptly (Task Manager, or
`taskkill /F`/`Stop-Process -Force` - not Ctrl+C), then immediately attempt a fresh `dfs mount`
against the same repository and see whether it fails the same "mount point in use" way, and if so,
how long it takes to clear (retry with backoff, timing it) or whether anything short of a reboot
clears it. If reproducible, this may be worth an operator-facing note in `docs/development.md` or
wherever mount troubleshooting guidance would belong, or - if there turns out to be a reliable
programmatic recovery step - something `dfs mount` itself could attempt before giving up.
