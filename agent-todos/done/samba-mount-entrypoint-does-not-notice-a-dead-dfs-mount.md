# The Samba mount container does not notice when `dfs mount` dies after startup

**Why parked**: out-of-scope finding. It came up while diagnosing a crash of `dfs mount` inside the
demo container on `rust-deleted-view-synthetic-roots`. That task fixed the crash itself and added a
panic guard around the mount callbacks (DESIGN-MOUNT-025), so this gap was left for a dedicated
change.
**Size**: medium (confirm with the user first) - the change is small, but it decides what a dead
mount should do to the container, and it needs a real container run to verify.
**Opened**: 2026-09-30, by Linux/WSL2 session.
**Context**: `docker/samba-mount/entrypoint.sh`; `docker/samba-mount/README.md`.

## The finding

`entrypoint.sh` starts `dfs mount` in the background and waits until `/proc/mounts` shows the mount.
After it prints "mounted.", nothing checks `dfs mount` again. The script then only waits for
`smbd`.

If `dfs mount` dies later, `smbd` keeps running. The container stays "Up". Samba serves a
disconnected mountpoint ("Transport endpoint is not connected"). A Windows client reports "The
network name was not found". Nothing in `docker ps` or the container log points at the real cause.
This was observed for real when a panic in a FUSE `release` callback aborted the whole process.

The panic guard now makes that particular failure much less likely. Other causes can still kill
`dfs mount`, for example an out-of-memory kill or an external `kill`.

## What the next attempt should do

1. Decide the intended behavior. The simplest option is that the container exits with a non-zero
   status when `dfs mount` exits, so that Docker's restart policy or the operator sees the failure.
2. Implement it in `entrypoint.sh`, for example by polling `mount_process_alive` alongside `smbd`,
   or by replacing the plain `wait "$SMBD_PID"` with a loop that also watches `$MOUNT_PID`.
3. Keep the existing clean-shutdown path intact. `docker stop` must still unmount cleanly and leave
   no repository lock behind.
4. Verify in a real container: kill `dfs mount` from inside and confirm the container exits. Then
   confirm that `docker stop` still shuts down cleanly.

## Resolution

Done 2026-09-30 by Linux/WSL2 session. `entrypoint.sh` now watches both `smbd` and `dfs mount` in a
one-second loop instead of a plain `wait "$SMBD_PID"`. If either exits on its own, the script runs
the existing `cleanup` and exits with the dead process's status, or 1 if that status was 0. The
liveness check is the former `mount_process_alive`, generalized to `process_alive <pid>`. The
`docker stop` path is unchanged, and the loop uses `sleep & wait $!` so the trap still fires
immediately. `docker/samba-mount/README.md` documents the behavior.

Verified in a real container: `pkill -KILL dfs` inside it made the container exit with 137 and log
"dfs mount exited unexpectedly". `pkill -KILL smbd` did the same with "smbd exited unexpectedly".
`docker stop` still exited with 0 and left no repository lock.
