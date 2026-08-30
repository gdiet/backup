#!/usr/bin/env bash
# Native directory listing (readdir), sequential - see ../methodology.md's "Statistical approach"
# and "Workload catalog". Builds one directory with a fixed, large number of entries once (large
# enough that per-entry cost dominates over fixed per-call overhead), then repeatedly lists it in
# full - nothing grows between runs or invocations; only the setup step is idempotent-once.
#
# Before starting a *new* measurement, delete $dir first so the entry count is rebuilt from
# scratch (not required otherwise - the directory stays fixed size across measurements).
# Tool (record this in the measurement protocol): bash `ls -U`, one process per listing.
#
# Scale here is "entries in the listed directory" (see the recording template), not "listings
# performed" - "$ops" below counts listing calls, each of which enumerates all $entry_count
# entries.

set -euo pipefail

dir=~/dedupfs-perf/listing/entries
entry_count=50000

mkdir -p "$dir"
existing=$(find "$dir" -mindepth 1 -maxdepth 1 | wc -l)
if (( existing < entry_count )); then
  for ((i = existing + 1; i <= entry_count; i++)); do
    : > "$dir/f$i"
  done
fi

for run in 1 2 3 4 5; do
  SECONDS=0
  ops=0
  while (( SECONDS < 20 )); do
    ls -U "$dir" > /dev/null
    ops=$((ops+1))
  done
  elapsed=$SECONDS
  echo "$run: $ops listings, ${elapsed}s, $((ops/elapsed)) ops/s"
done
