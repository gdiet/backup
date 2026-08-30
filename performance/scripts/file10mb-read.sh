#!/usr/bin/env bash
# Native 10 MB file read-back, sequential - see ../methodology.md's "Statistical approach" and
# "Workload catalog". Reads back files file10mb-create.sh created, cycling through whichever range
# it has produced so far (run file10mb-create.sh first - this script only reads, it never grows
# the tree itself, so nothing here needs the "state between runs" bookkeeping the create scripts
# do).
# Tool (record this in the measurement protocol): bash `cat` to /dev/null.

set -euo pipefail

root=~/dedupfs-perf/files10mb
counter_file="$root/../counter-files10mb.txt"
[ -f "$counter_file" ] || {
  echo "no files to read yet - run file10mb-create.sh first" >&2
  exit 1
}
total=$(cat "$counter_file")

for run in 1 2 3 4 5; do
  SECONDS=0
  ops=0
  while (( SECONDS < 20 )); do
    idx=$(( (ops % total) + 1 ))
    cat "$root/sub$((idx % 20))/f$idx" > /dev/null
    ops=$((ops+1))
  done
  elapsed=$SECONDS
  echo "$run: $ops reads, ${elapsed}s, $((ops/elapsed)) ops/s"
done
