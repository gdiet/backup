#!/bin/sh
# Mounts the repository at $REPO via FUSE, waits for the mount to actually
# be live (checked via /proc/mounts, not just directory emptiness) - or
# fails fast with backup mount's own actionable error if it exits before
# that - creates the Samba user on first start, then runs smbd in the
# foreground. On SIGTERM/SIGINT, unmounts cleanly before exiting - see the
# Dockerfile in this directory for the image this drives.
set -eu

REPO=${REPO:-/repo}
MOUNTPOINT=/mnt/dedup
SMB_USER=${SMB_USER:-dedup}
SMB_PASSWORD=${SMB_PASSWORD:-dedup}
# Every `backup mount` flag (--read-write, --write-cache-mb, --temp,
# --allow-swap-risk, --zero-fill-missing, and any added later) is reachable
# through this single passthrough rather than one env var per flag - see
# README.md in this directory for examples. Deliberately word-split
# unquoted below (shellcheck SC2086), the standard way to turn one env var
# into several argv entries in POSIX sh (no arrays, unlike bash) - this
# means a value containing its own spaces (e.g. a --temp path) can't be
# expressed here; not a real limitation for the values these flags
# normally take.
MOUNT_ARGS=${MOUNT_ARGS:-}

mkdir -p "$MOUNTPOINT"

# shellcheck disable=SC2086
backup mount -r "$REPO" $MOUNT_ARGS "$MOUNTPOINT" &
MOUNT_PID=$!

# Whether $MOUNT_PID is still running - not just `kill -0`, which reports
# an unreaped zombie (a process that already exited but this shell hasn't
# `wait`ed for yet, exactly the state a just-failed backup mount is in
# right after it exits) as "alive". Linux-only (/proc), which is fine: this
# container only ever runs on Linux.
mount_process_alive() {
    [ -d "/proc/$MOUNT_PID" ] || return 1
    case "$(cut -d ' ' -f 3 "/proc/$MOUNT_PID/stat" 2>/dev/null)" in
        Z | '') return 1 ;;
        *) return 0 ;;
    esac
}

echo "waiting for the mount at $MOUNTPOINT to become live..."
i=0
while ! grep -q " $MOUNTPOINT fuse" /proc/mounts 2>/dev/null; do
    if ! mount_process_alive; then
        echo "error: backup mount exited before the mount became live - see the error above" >&2
        wait "$MOUNT_PID"
        exit $?
    fi
    i=$((i + 1))
    if [ "$i" -ge 50 ]; then
        echo "error: mount did not become ready in time" >&2
        exit 1
    fi
    sleep 0.2
done
echo "mounted."

cleanup() {
    echo "shutting down..."
    fusermount3 -u "$MOUNTPOINT" 2>/dev/null || true
    # Give backup mount a chance to notice the unmount and exit on its own
    # first, rather than SIGTERM-ing it immediately: its on_unmount handler
    # (see Inner::on_unmount in cli/src/mount.rs) flushes any still-dirty
    # writes and closes its database connections cleanly on a normal
    # return from main - a premature signal would cut that off mid-flight,
    # leaving a non-empty -wal behind (a --read-write mount's writes are
    # already durably committed either way, just not yet checkpointed -
    # see "Read-Only Commands Need A Clean Database" in the main README -
    # so this is about leaving a *clean* repository behind, not about data
    # loss). Only falls back to actually signaling it if it's still running
    # after a generous grace period.
    j=0
    while mount_process_alive; do
        j=$((j + 1))
        if [ "$j" -ge 100 ]; then
            echo "backup mount did not exit on its own within 20s - sending SIGTERM" >&2
            kill "$MOUNT_PID" 2>/dev/null || true
            break
        fi
        sleep 0.2
    done
    wait "$MOUNT_PID" 2>/dev/null || true
    exit 0
}
trap cleanup TERM INT

# Modern Windows clients don't reliably allow guest/anonymous SMB logons by
# default, so this uses one fixed Samba user instead of `guest ok` - created
# non-interactively on first start if it doesn't exist yet.
id "$SMB_USER" >/dev/null 2>&1 || adduser --disabled-password --gecos "" "$SMB_USER"
pdbedit -L | grep -q "^$SMB_USER:" || printf '%s\n%s\n' "$SMB_PASSWORD" "$SMB_PASSWORD" | smbpasswd -s -a "$SMB_USER"

smbd --foreground --no-process-group &
SMBD_PID=$!
wait "$SMBD_PID"
cleanup
