#!/usr/bin/env bash
# Native zero-byte file creation, spread across several directories, sequential - see
# ../methodology.md's "Statistical approach" and "Workload catalog". 5 runs, each ~20 s; the
# counter keeps growing across runs and across repeated invocations of this script, per the "state
# between runs" rule - do not delete $root between runs of the same measurement.
#
# Before starting a *new* measurement, delete $root first so Scale has a clean starting point.
# Tool (record this in the measurement protocol): bash `touch`.

set -euo pipefail

root=~/dedupfs-perf/files0b
mkdir -p "$root"
for i in $(seq 0 19); do mkdir -p "$root/sub$i"; done
counter_file="$root/../counter-files0b.txt"
counter=$( [ -f "$counter_file" ] && cat "$counter_file" || echo 0 )

for run in 1 2 3 4 5; do
  SECONDS=0
  ops=0
  while (( SECONDS < 20 )); do
    counter=$((counter+1))
    touch "$root/sub$((counter % 20))/f$counter"
    ops=$((ops+1))
  done
  elapsed=$SECONDS
  echo "$run: $ops files, ${elapsed}s, $((ops/elapsed)) ops/s"
done
echo "$counter" > "$counter_file"
