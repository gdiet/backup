#!/bin/sh
# Mounts the repository at $REPO via FUSE, waits for the mount to actually
# be live (checked via /proc/mounts, not just directory emptiness) - or
# fails fast with dfs mount's own actionable error if it exits before
# that - creates the Samba user on first start, then runs smbd in the
# foreground. On SIGTERM/SIGINT, unmounts cleanly before exiting. If `dfs
# mount` or smbd dies on its own afterwards, the container exits non-zero
# instead of staying "Up" while serving a dead mountpoint - see the
# Dockerfile in this directory for the image this drives.
set -eu

REPO=${REPO:-/repo}
MOUNTPOINT=/mnt/dedup
SMB_USER=${SMB_USER:-dedup}
SMB_PASSWORD=${SMB_PASSWORD:-dedup}
# Every `dfs mount` flag (--read-write, --spill-directory, --ram-budget-mb,
# --backpressure-free-zone-bytes, --backpressure-slope-divisor, --show-deleted,
# --purge, and any added later) is reachable through this single passthrough rather than one env
# var per flag - see README.md in this directory for examples.
# Deliberately word-split unquoted below (shellcheck SC2086), the standard
# way to turn one env var into several argv entries in POSIX sh (no
# arrays, unlike bash) - this means a value containing its own spaces
# (e.g. a --spill-directory path) can't be expressed here; not a real
# limitation for the values these flags normally take.
MOUNT_ARGS=${MOUNT_ARGS:-}

mkdir -p "$MOUNTPOINT"

# Everything below runs as root. With HOST_UID and HOST_GID set, files in $REPO and /logs are
# handed over to that owner at start and again on shutdown, so a bind-mounted directory stays
# usable from the host. Files written while the container runs stay root-owned until then.
fix_ownership() {
    [ -n "${HOST_UID:-}" ] && [ -n "${HOST_GID:-}" ] || return 0
    for dir in "$REPO" /logs; do
        [ ! -d "$dir" ] || chown -R "$HOST_UID:$HOST_GID" "$dir" || true
    done
}

# Opt-in: set CREATE_REPO_ARGS (possibly to the empty string) to create the repository at $REPO
# on first start. Without it, a missing repository is an error from `dfs mount` below.
# shellcheck disable=SC2086
if [ -n "${CREATE_REPO_ARGS+set}" ] && [ ! -d "$REPO/meta" ]; then
    dfs create-repo $CREATE_REPO_ARGS "$REPO"
fi
fix_ownership

# shellcheck disable=SC2086
dfs mount --repository "$REPO" $MOUNT_ARGS "$MOUNTPOINT" &
MOUNT_PID=$!

# Whether process $1 (a child of this shell) is still running - not just
# `kill -0`, which reports an unreaped zombie (a process that already exited
# but this shell hasn't `wait`ed for yet, exactly the state a just-failed dfs
# mount is in right after it exits) as "alive". Linux-only (/proc), which is
# fine: this container only ever runs on Linux.
process_alive() {
    [ -d "/proc/$1" ] || return 1
    case "$(cut -d ' ' -f 3 "/proc/$1/stat" 2>/dev/null)" in
        Z | '') return 1 ;;
        *) return 0 ;;
    esac
}

echo "waiting for the mount at $MOUNTPOINT to become live..."
i=0
while ! grep -q " $MOUNTPOINT fuse" /proc/mounts 2>/dev/null; do
    if ! process_alive "$MOUNT_PID"; then
        echo "error: dfs mount exited before the mount became live - see the error above" >&2
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

CLEANUP_DONE=0
EXIT_CODE=0
cleanup() {
    # Idempotency guard: this can genuinely be entered twice for one
    # shutdown - killing $SMBD_PID from inside this function (needed, see
    # below) makes the top-level `wait "$SMBD_PID"` below complete too,
    # which reaches its own plain (non-trap) call to `cleanup` right after
    # it - a real second invocation observed in practice, not just a
    # theoretical race, and left unguarded it made two concurrent copies
    # of the unmount/wait-for-`dfs mount` logic below interfere with each
    # other badly enough to break a clean shutdown.
    [ "$CLEANUP_DONE" = 1 ] && return 0
    CLEANUP_DONE=1
    echo "shutting down..."
    # Kill every smbd process - not just the main one - before attempting
    # to unmount: Samba forks a child smbd per active session (plus a
    # couple of background helpers it forks unconditionally at startup),
    # and a still-open SMB session (e.g. a live Windows Explorer window
    # browsing the share) keeps its own forked child holding an open
    # file/directory handle into the FUSE mount. That made `fusermount3
    # -u` below fail (busy) for as long as that handle stayed open, i.e.
    # indefinitely, not just transiently - reproduced for real (a CIFS
    # client left connected, then this container signaled): without this,
    # `fusermount3 -u` kept failing for the full 20s grace period below,
    # ending in the SIGTERM fallback instead of a clean exit. `pkill`, not
    # `kill "$SMBD_PID"`: killing only the main smbd doesn't kill its
    # already-forked children (they're independent processes, not torn
    # down just because their parent exits) - matching by name catches all
    # of them regardless of how many Samba happened to fork.
    pkill -TERM smbd 2>/dev/null || true
    k=0
    while pgrep smbd >/dev/null 2>&1; do
        k=$((k + 1))
        if [ "$k" -ge 25 ]; then
            echo "smbd did not exit within 5s - sending SIGKILL" >&2
            pkill -KILL smbd 2>/dev/null || true
            break
        fi
        sleep 0.2
    done
    # `|| true`: the mount may already be gone, for example if dfs mount died.
    # A --read-write mount's writes are already durably committed regardless
    # of a clean vs. abrupt unmount (see the comment below).
    fusermount3 -u "$MOUNTPOINT" 2>/dev/null || true
    # Give dfs mount a chance to notice the unmount and exit on its own
    # first, rather than SIGTERM-ing it immediately: its unmount handling
    # flushes any still-dirty writes and closes its database connections
    # cleanly on a normal return from main - a premature signal would cut
    # that off mid-flight, leaving a non-empty -wal behind (a
    # --read-write mount's writes are already durably committed either
    # way, just not yet checkpointed, so this is about leaving a *clean*
    # repository behind, not about data loss). Only falls back to
    # actually signaling it if it's still running after a generous grace
    # period.
    j=0
    while process_alive "$MOUNT_PID"; do
        j=$((j + 1))
        if [ "$j" -ge 100 ]; then
            echo "dfs mount did not exit on its own within 20s - sending SIGTERM" >&2
            kill "$MOUNT_PID" 2>/dev/null || true
            break
        fi
        sleep 0.2
    done
    wait "$MOUNT_PID" 2>/dev/null || true
    fix_ownership
    exit "$EXIT_CODE"
}
trap cleanup TERM INT

# Modern Windows clients don't reliably allow guest/anonymous SMB logons by
# default, so this uses one fixed Samba user instead of `guest ok` - created
# non-interactively on first start if it doesn't exist yet.
id "$SMB_USER" >/dev/null 2>&1 || adduser --disabled-password --gecos "" "$SMB_USER"
pdbedit -L | grep -q "^$SMB_USER:" || printf '%s\n%s\n' "$SMB_PASSWORD" "$SMB_PASSWORD" | smbpasswd -s -a "$SMB_USER"

# smbd runs in its own process group (no --no-process-group). Sharing this
# script's group made smbd's shutdown send SIGTERM to `dfs mount` as well, so
# `dfs mount` stopped before `cleanup` had unmounted it. Only this script
# decides when `dfs mount` stops.
smbd --foreground &
SMBD_PID=$!

# Watch both children. A plain `wait "$SMBD_PID"` would leave the container
# "Up" and serving a disconnected mountpoint ("Transport endpoint is not
# connected") if `dfs mount` died after startup, e.g. from an out-of-memory
# kill. `sleep & wait $!` instead of a plain `sleep`, so that a SIGTERM/SIGINT
# runs the trap immediately rather than after the sleep.
while process_alive "$SMBD_PID" && process_alive "$MOUNT_PID"; do
    sleep 1 &
    wait $!
done

# Reaching this point means a child exited on its own - a requested shutdown
# leaves through the trap above. Either exit is a failure, so the container
# must not report success, even if the dead process's own status was 0.
if process_alive "$MOUNT_PID"; then
    echo "error: smbd exited unexpectedly" >&2
    DEAD_PID=$SMBD_PID
else
    echo "error: dfs mount exited unexpectedly - see the error above" >&2
    DEAD_PID=$MOUNT_PID
fi
wait "$DEAD_PID" || EXIT_CODE=$?
[ "$EXIT_CODE" -ne 0 ] || EXIT_CODE=1
cleanup
