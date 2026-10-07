#!/usr/bin/env bash
# Ingest A/B on the throttled device. Usage:
#   measure.sh <files> <bytes-per-file> <runs> [outer]
# "outer" sets DFS_EXPERIMENT_OUTER_TX (one transaction around the whole ingest); without it, the
# default per-operation commits are measured. The "outer" mode needs the experiment patch from
# performance/notes/2026-10-07-ingest-commit-batching-on-throttled-device.md. The source tree lives
# in $SRC_BASE (default /tmp/dfs-slow-src), the repository on the throttled device under
# <repository root>/.local/slow-disk/mnt. Prints seconds for ingest, seconds including a final
# sync, and the write requests / MiB that reached the device (from /sys/block/dm-N/stat).
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/../../.." && pwd)"
state="$root/.local/slow-disk"
dfs="$root/target/release/dfs"
maxsec="${MAXSEC:-90}"  # abort a single ingest after this many seconds
files="$1"; size="$2"; runs="$3"; mode="${4:-default}"
src="${SRC_BASE:-/tmp/dfs-slow-src}/f$files-b$size"
dm="$(basename "$(readlink -f /dev/mapper/dfs-slow)")"

if [ ! -d "$src" ]; then
  mkdir -p "$src"
  for d in $(seq 0 19); do mkdir -p "$src/sub$d"; done
  for i in $(seq 1 "$files"); do head -c "$size" /dev/urandom > "$src/sub$((i % 20))/f$i"; done
fi

stat_field() { awk -v f="$1" '{print $f}' "/sys/block/$dm/stat"; }
for run in $(seq 1 "$runs"); do
  repo="$state/mnt/repo-$run"
  rm -rf "$repo"
  "$dfs" create-repo "$repo" > /dev/null
  sync
  w0=$(stat_field 5); s0=$(stat_field 7)
  t0=$(date +%s.%N)
  if [ "$mode" = "outer" ]; then DFS_EXPERIMENT_OUTER_TX=1 timeout "$maxsec" "$dfs" ingest --repository "$repo" "$src" / > /dev/null || echo "ingest aborted or failed (timeout ${maxsec}s?)" >&2
  else timeout "$maxsec" "$dfs" ingest --repository "$repo" "$src" / > /dev/null || echo "ingest aborted or failed (timeout ${maxsec}s?)" >&2; fi
  t1=$(date +%s.%N)
  sync
  t2=$(date +%s.%N)
  w1=$(stat_field 5); s1=$(stat_field 7)
  printf '%s run %d: ingest %.2fs, with sync %.2fs, device writes %d, %.1f MiB\n' \
    "$mode" "$run" "$(echo "$t1 - $t0" | bc)" "$(echo "$t2 - $t0" | bc)" "$((w1 - w0))" "$(echo "($s1 - $s0) / 2048" | bc -l)"
  rm -rf "$repo"
done
