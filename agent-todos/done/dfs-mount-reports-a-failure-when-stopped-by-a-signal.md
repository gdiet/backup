# `dfs mount` reports "mount failed" and exits 1 when stopped by SIGINT or SIGTERM

**Why parked**: out-of-scope finding. It came up while diagnosing the Samba container's shutdown
message, which had a different root cause in `entrypoint.sh` (see
`agent-todos/done/samba-mount-clean-shutdown-logs-mount-failed-code-8.md`).
**Size**: small to medium - the fix is easy to write, but the right behavior needs a decision.
**Opened**: 2026-09-30, by Linux/WSL2 session.
**Context**: `crates/mountfs/src/linux/mod.rs` (`mount`), `crates/cli/src/mount.rs`.

## The finding

On Linux, pressing Ctrl+C in a terminal running `dfs mount`, or sending it SIGTERM, ends it with:

```
mount failed: fuse_main_real exited with code 8
```

and exit status 1. Reproduced on the host with the debug build. An external `fusermount3 -u` makes
the same mount exit with 0 and no message.

libfuse installs its own SIGINT and SIGTERM handlers and ends its loop on a signal. `fuse_main_real`
then returns 8, which `linux::mount` turns into an `Err`. The documented way to stop a foreground
mount is Ctrl+C, so this looks like a failure to the user. The repository is still shut down
properly, because `on_unmount` runs and the lock is released.

## What the next attempt should do

1. Decide the intended behavior: a signal-initiated stop should probably be a normal end with exit
   status 0.
2. Work out how to tell that case apart. `fuse_main_real` returns 8 for every loop failure, so the
   exit code alone is not enough. One option is to install a signal flag before the call and treat
   code 8 as success only when that flag is set. Check that this does not conflict with libfuse's
   own handlers.
3. Add a `real_mount_` test that sends SIGTERM to a process mounting a test filesystem. Verify that
   it fails without the fix.
4. Check the Windows backend's Ctrl+C path for the same message.

## Resolution

Done 2026-09-30 by Linux/WSL2 session. A stop by SIGHUP, SIGINT or SIGTERM is now a normal end:
`mountfs::mount` returns `Ok(())` and `dfs mount` exits with 0. Every other return value 8 of
`fuse_main_real` (for example a read error on the FUSE device) stays an error.

The signal is recognized by an `init` callback that wraps libfuse's signal handlers with a flag
setter. The decision and the rejected alternatives are in DESIGN-MOUNT-026 in
`docs/design/mount-abstraction.md`. Regression tests are in `crates/mountfs/tests/signal_stop.rs`.
Both the match arm and the handler restoration were verified red, then green.

Known limitation: a signal before the first kernel request is still reported as an error.

The Windows backend was not changed. The existing documentation states that WinFSP already returns
cleanly on Ctrl+C. That was not re-verified. The Windows cross-build still compiles.
