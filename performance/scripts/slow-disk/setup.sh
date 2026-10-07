#!/usr/bin/env bash
# Creates a throttled block device for experiments on slow media. Run with sudo.
#
#   sudo ./setup.sh [--write-ms N] [--read-ms N] [--size-mb N] [--sync] [--wbps BYTES] [--iops N]
#
# Layers: sparse image file -> loop device -> dm-delay (latency per request) -> ext4 -> mount.
#   --write-ms / --read-ms  latency added to every write / read request (default 10 / 0)
#   --sync                  mount with -o sync: every write() waits for the device, like a Windows
#                           device set to "quick removal" (no write cache)
#   --wbps / --iops         optional bandwidth / write-IOPS cap through the cgroup v2 io controller
#                           (only affects processes started through ./enter-throttle.sh)
# State lives in <repository root>/.local/slow-disk (git-excluded): backing.img and the mount point
# mnt/, owned by the invoking user. Undo with sudo ./teardown.sh.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
state="$here/../../../.local/slow-disk"
mkdir -p "$state"
state="$(cd "$state" && pwd)"
write_ms=10; read_ms=0; size_mb=4096; sync_opt=""; wbps=""; iops=""
while [ $# -gt 0 ]; do
  case "$1" in
    --write-ms) write_ms="$2"; shift 2 ;;
    --read-ms) read_ms="$2"; shift 2 ;;
    --size-mb) size_mb="$2"; shift 2 ;;
    --sync) sync_opt="-o sync"; shift ;;
    --wbps) wbps="$2"; shift 2 ;;
    --iops) iops="$2"; shift 2 ;;
    *) echo "unknown option: $1" >&2; exit 2 ;;
  esac
done

[ "$(id -u)" -eq 0 ] || { echo "run with sudo" >&2; exit 1; }
owner="${SUDO_USER:-root}"
[ -e /dev/mapper/dfs-slow ] && { echo "dfs-slow already exists; run ./teardown.sh first" >&2; exit 1; }

modprobe dm-delay
truncate -s "${size_mb}M" "$state/backing.img"
loop="$(losetup --find --show "$state/backing.img")"
sectors="$(blockdev --getsz "$loop")"
# delay <dev> <offset> <read delay ms> <write dev> <write offset> <write delay ms>
dmsetup create dfs-slow --table "0 $sectors delay $loop 0 $read_ms $loop 0 $write_ms"
mkfs.ext4 -q -F /dev/mapper/dfs-slow
mkdir -p "$state/mnt"
# shellcheck disable=SC2086
mount $sync_opt /dev/mapper/dfs-slow "$state/mnt"
chown "$owner": "$state/mnt"

if [ -n "$wbps$iops" ]; then
  majmin="$(dmsetup info -c --noheadings -o major,minor dfs-slow | tr -s ' :' ':' | sed 's/^://')"
  mkdir -p /sys/fs/cgroup/dfs-slow
  limits="$majmin"
  [ -n "$wbps" ] && limits="$limits wbps=$wbps"
  [ -n "$iops" ] && limits="$limits wiops=$iops"
  echo "$limits" > /sys/fs/cgroup/dfs-slow/io.max
fi

echo "ready: $state/mnt  (write ${write_ms} ms, read ${read_ms} ms, ${sync_opt:-cached}${wbps:+, wbps=$wbps}${iops:+, wiops=$iops})"
