#!/usr/bin/env bash
# Native directory lookup, sequential - see ../methodology.md's "Statistical approach" and
# "Workload catalog". Builds a fixed-size tree once (large enough that lookup cost is not
# dominated by the whole tree fitting in some cache), then repeatedly looks up a
# pseudo-randomly chosen existing entry within it - unlike the create-workload scripts, nothing
# here grows between runs or invocations; only the setup step is idempotent-once.
#
# Before starting a *new* measurement, delete $root first so the tree is rebuilt from scratch (not
# required otherwise - the tree stays fixed size across measurements).
# Tool (record this in the measurement protocol): bash `test -d`.

set -euo pipefail

root=~/dedupfs-perf/lookup
tree_size=100000

mkdir -p "$root"
existing=$(find "$root" -mindepth 1 -maxdepth 1 -type d | wc -l)
if (( existing < tree_size )); then
  for ((i = existing + 1; i <= tree_size; i++)); do
    mkdir "$root/d$i"
  done
fi

for run in 1 2 3 4 5; do
  SECONDS=0
  ops=0
  while (( SECONDS < 20 )); do
    idx=$(( (RANDOM * 32768 + RANDOM) % tree_size + 1 ))
    [ -d "$root/d$idx" ]
    ops=$((ops+1))
  done
  elapsed=$SECONDS
  echo "$run: $ops lookups, ${elapsed}s, $((ops/elapsed)) ops/s"
done
