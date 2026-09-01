#!/usr/bin/env bash
# Native 100 B file read-back, pseudo-random - see ../methodology.md's "Statistical approach" and
# "Workload catalog". Reads back files file100b-create.sh created, picking a pseudo-randomly chosen
# existing file within whichever range it has produced so far (run file100b-create.sh first - this
# script only reads, it never grows the tree itself, so nothing here needs the "state between runs"
# bookkeeping the create scripts do).
#
# `idx` used to be `(ops % total) + 1`, restarting from file 1 on every one of the 5 runs (`ops`
# itself resets each run) - invisible while the whole tree fits in RAM, but at larger file sizes it
# produces a monotonic-looking upward throughput trend across runs that is a script artifact (later
# runs find more of the same low-index range still page-cache-warm from the previous run's pass over
# it), not a filesystem effect. Fixed to pick pseudo-randomly instead, matching dir-lookup.sh's own
# approach for the equivalent problem.
# Tool (record this in the measurement protocol): bash `cat` to /dev/null.

set -euo pipefail

root=~/dedupfs-perf/files100b
counter_file="$root/../counter-files100b.txt"
[ -f "$counter_file" ] || {
  echo "no files to read yet - run file100b-create.sh first" >&2
  exit 1
}
total=$(cat "$counter_file")

for run in 1 2 3 4 5; do
  SECONDS=0
  ops=0
  while (( SECONDS < 20 )); do
    idx=$(( (RANDOM * 32768 + RANDOM) % total + 1 ))
    cat "$root/sub$((idx % 20))/f$idx" > /dev/null
    ops=$((ops+1))
  done
  elapsed=$SECONDS
  echo "$run: $ops reads, ${elapsed}s, $((ops/elapsed)) ops/s"
done
