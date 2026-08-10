#!/bin/sh
# Mounts the repository at $REPO via FUSE, waits for the mount to actually
# be live (checked via /proc/mounts, not just directory emptiness), creates
# the Samba user on first start, then runs smbd in the foreground. On
# SIGTERM/SIGINT, unmounts cleanly before exiting - see the Dockerfile in
# this directory for the image this drives.
set -eu

REPO=${REPO:-/repo}
MOUNTPOINT=/mnt/dedup
SMB_USER=${SMB_USER:-dedup}
SMB_PASSWORD=${SMB_PASSWORD:-dedup}

mkdir -p "$MOUNTPOINT"

backup mount -r "$REPO" "$MOUNTPOINT" &
MOUNT_PID=$!

echo "waiting for the mount at $MOUNTPOINT to become live..."
i=0
while ! grep -q " $MOUNTPOINT fuse" /proc/mounts 2>/dev/null; do
    i=$((i + 1))
    if [ "$i" -ge 50 ]; then
        echo "mount did not become ready in time" >&2
        exit 1
    fi
    sleep 0.2
done
echo "mounted."

cleanup() {
    echo "shutting down..."
    fusermount3 -u "$MOUNTPOINT" 2>/dev/null || true
    kill "$MOUNT_PID" 2>/dev/null || true
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
