#!/usr/bin/env bash
# Native 10 MB file read-back, pseudo-random - see ../methodology.md's "Statistical approach" and
# "Workload catalog". Reads back files file10mb-create.sh created, picking a pseudo-randomly chosen
# existing file within whichever range it has produced so far (run file10mb-create.sh first - this
# script only reads, it never grows the tree itself, so nothing here needs the "state between runs"
# bookkeeping the create scripts do).
#
# `idx` used to be `(ops % total) + 1`, restarting from file 1 on every one of the 5 runs (`ops`
# itself resets each run) - invisible while the whole tree fits in RAM, but at 10 MB (confirmed on
# a 2026-08-28 measurement, ~52.5 GB tree vs. 32 GB RAM) it produces a monotonic-looking upward
# throughput trend across runs that is a script artifact (later runs find more of the same
# low-index range still page-cache-warm from the previous run's pass over it), not a filesystem
# effect. Fixed to pick pseudo-randomly instead, matching dir-lookup.sh's own approach for the
# equivalent problem. The 100 B/30 KB read measurements taken before this fix are unaffected - the
# whole tree fits in RAM at those sizes either way, so the effect could not have shown up there; a
# 10 MB measurement taken before this fix should be redone.
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
    idx=$(( (RANDOM * 32768 + RANDOM) % total + 1 ))
    cat "$root/sub$((idx % 20))/f$idx" > /dev/null
    ops=$((ops+1))
  done
  elapsed=$SECONDS
  echo "$run: $ops reads, ${elapsed}s, $((ops/elapsed)) ops/s"
done
