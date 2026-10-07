#!/usr/bin/env bash
# Removes everything setup.sh created. Run with sudo. Pass --delete-image to also delete backing.img.
set -uo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
state="$(cd "$here/../../../.local/slow-disk" && pwd)"
[ "$(id -u)" -eq 0 ] || { echo "run with sudo" >&2; exit 1; }
mountpoint -q "$state/mnt" && umount "$state/mnt"
[ -e /dev/mapper/dfs-slow ] && dmsetup remove dfs-slow
for l in $(losetup -j "$state/backing.img" -O NAME --noheadings 2>/dev/null); do losetup -d "$l"; done
rmdir /sys/fs/cgroup/dfs-slow 2>/dev/null
[ "${1:-}" = "--delete-image" ] && rm -f "$state/backing.img"
echo "torn down"
