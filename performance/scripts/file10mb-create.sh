#!/usr/bin/env bash
# Native 10 MB file creation, spread across several directories, sequential - see
# ../methodology.md's "Statistical approach", "Workload catalog" and "File-content workloads".
# 5 runs, each ~20 s; the counter keeps growing across runs and across repeated invocations of
# this script, per the "state between runs" rule - do not delete $root between runs of the same
# measurement. At this size the accumulated files reach several GB over the five runs - check free
# space first (see ../methodology.md's "File-content workloads" note).
#
# Before starting a *new* measurement, delete $root first so Scale has a clean starting point.
# Content comes from `perf-gen` (crates/perf-gen), seeded per file with the counter - at ~3 GB/s
# this stays a negligible fraction of the per-file work even at 10 MB, unlike this script's
# previous /dev/urandom-per-file approach (whose non-trivial cost at this size needed a separate
# "record generator-only throughput" note, no longer necessary - see
# developer-todos/perf-gen-shared-content-generator.md). At the 20-bit CDC default a 10 MB file is
# roughly 8-10 chunks, so this is the multi-chunk point of the size ladder. file10mb-read.sh reads
# these files.
# Tool (record this in the measurement protocol): bash redirection, content from `perf-gen`.

set -euo pipefail

size=10485760
root=~/dedupfs-perf/files10mb
repo_root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

mkdir -p "$root"
for i in $(seq 0 19); do mkdir -p "$root/sub$i"; done
counter_file="$root/../counter-files10mb.txt"
counter=$( [ -f "$counter_file" ] && cat "$counter_file" || echo 0 )

cargo build --release -p perf-gen --manifest-path "$repo_root_dir/Cargo.toml"
perf_gen="$repo_root_dir/target/release/perf-gen"

for run in 1 2 3 4 5; do
  SECONDS=0
  ops=0
  while (( SECONDS < 20 )); do
    counter=$((counter+1))
    "$perf_gen" "$size" "$counter" > "$root/sub$((counter % 20))/f$counter"
    ops=$((ops+1))
  done
  elapsed=$SECONDS
  echo "$run: $ops files, ${elapsed}s, $((ops/elapsed)) ops/s"
done
echo "$counter" > "$counter_file"
