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

## Resolution

Done 2026-09-30 by Linux/WSL2 session. Cause: `entrypoint.sh` started `smbd` with
`--no-process-group`, so `smbd` shared the entrypoint's process group. When `cleanup` killed `smbd`,
its shutdown sent SIGTERM to that whole group, which included `dfs mount`. libfuse ends its loop on
a signal and `fuse_main_real` then returns 8. It was not a race with `fusermount3 -u`: an external
unmount alone makes `dfs mount` exit with 0, in the container as well as on the host.

Fix: `smbd` runs without `--no-process-group`, in its own process group. Verified in a real
container: `docker stop` takes about 0.2 s, exits with 0, logs no "mount failed" line, and Samba
still listens on port 445. The dead-`dfs mount` and dead-`smbd` detection still exits with 137.
`docker/samba-mount/README.md` documents this as problem 6.

Not fixed here: a direct SIGINT or SIGTERM to `dfs mount` outside the container still ends with
"mount failed: ... code 8" and exit status 1. See
`agent-todos/dfs-mount-reports-a-failure-when-stopped-by-a-signal.md`.
