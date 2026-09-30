# The Samba mount container logs "mount failed: fuse_main_real exited with code 8" on a clean `docker stop`

**Why parked**: out-of-scope finding. It came up while verifying the dead-mount detection in
`entrypoint.sh`.
**Size**: small to medium - the cause is unknown, so start by finding it.
**Opened**: 2026-09-30, by Linux/WSL2 session.
**Context**: `docker/samba-mount/entrypoint.sh` (`cleanup`); `dfs mount` in `crates/cli`.

## The finding

`docker stop` on a running container exits with status 0. The container log ends with:

```
shutting down...
mount failed: fuse_main_real exited with code 8
```

The message also appears with the entrypoint from before the dead-mount detection, so that change
did not cause it. The repository lock is released, and `dfs unlock` reports "not locked".

The `cleanup` comments in `entrypoint.sh` already mention this code 8 as an occasional race between
`fusermount3 -u` and the unmounting `dfs mount`. It appeared in both baseline runs here, so it looks
deterministic. The README claims a clean shutdown.

## What the next attempt should do

1. Find out why `fuse_main_real` returns 8 after an external `fusermount3 -u`. Check whether `dfs
   mount` treats a normal unmount-triggered loop exit as an error.
2. If it is a normal end, make `dfs mount` return success and print no "mount failed" line. Add a
   real-mount test in `crates/cli` or `crates/mountfs`.
3. If it is an error, decide whether the container should still report success.
