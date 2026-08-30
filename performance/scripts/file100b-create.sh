#!/usr/bin/env bash
# Native 100 B file creation, spread across several directories, sequential - see
# ../methodology.md's "Statistical approach" and "Workload catalog". 5 runs, each ~20 s; the
# counter keeps growing across runs and across repeated invocations of this script, per the "state
# between runs" rule - do not delete $root between runs of the same measurement.
#
# Before starting a *new* measurement, delete $root first so Scale has a clean starting point.
# Content comes from `perf-gen` (crates/perf-gen), seeded per file with the counter, so uniqueness
# needs no bookkeeping of its own and generation stays outside the timed loop's own cost the same
# way the PowerShell counterpart's template-and-poke scheme does - see
# developer-todos/perf-gen-shared-content-generator.md for why plain `/dev/urandom` (this script's
# previous approach) was replaced. file100b-read.sh reads the files this script creates.
# Tool (record this in the measurement protocol): bash redirection, content from `perf-gen`.

set -euo pipefail

size=100
root=~/dedupfs-perf/files100b
repo_root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

mkdir -p "$root"
for i in $(seq 0 19); do mkdir -p "$root/sub$i"; done
counter_file="$root/../counter-files100b.txt"
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
