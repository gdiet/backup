#!/usr/bin/env bash
# location: dfs-mount, directory creation, sequential - see ../methodology.md's "Statistical
# approach" and "Workload catalog", and ../methodology.md's Location catalog entry for
# `dfs-mount`. Builds a release `dfs`, creates a repository, mounts it read-write via real
# libfuse3, and times `mkdir` against the *mounted* path instead of a native one - the mounted op
# set only covers directories today (db::Repository has no file-entry creation yet, same
# limitation ../../crates/db/examples/db_bench.rs's header comment documents), so this matches
# that benchmark's scope exactly, one layer further up the stack. Linux/WSL2 counterpart of
# dfs-mount-dir-create.ps1 (WinFSP) - see that script for the shared design; differences below are
# platform mechanics only (unmounting, stale-mount detection), not a different workload.
#
# 5 runs, each ~20 s; the counter (and the tree under the mount) keeps growing across runs and
# across repeated invocations of this script, per the "state between runs" rule.
#
# Before starting a *new* measurement, delete $base first (removes the repo, so the next run
# starts from an empty tree) - this script always clears a stale $mount_path itself on every run,
# see the comment at that step for why that one is not optional.
#
# Tool (record this in the measurement protocol): bash `mkdir` against a `dfs mount --read-write`
# mountpoint (real libfuse3, not WinFSP).
#
# Unmounts by sending the mount process SIGTERM (its default signal handling, via libfuse's own
# fuse_main_real, unmounts cleanly the same way Ctrl+C does interactively) and falling back to
# `fusermount3 -u` if it is still mounted after a short wait - not a graceful requirement for a
# benchmark's ephemeral, throwaway repository, just enough to leave a clean mountpoint for the next
# run.
#
# Validated against a real libfuse3 mount (WSL2/Ubuntu 24.04, kernel 6.6.87.2-microsoft-standard-
# WSL2) - one real bug found and fixed on the first run: unlike WinFSP, libfuse refuses to mount
# onto a path that does not already exist, so the mountpoint must be (re)created after clearing it,
# not left for the mount itself to create. A one-line libfuse warning ("Ignoring invalid max
# threads value ...") on startup is harmless noise from this libfuse3 build's own thread-pool
# defaults, not something this script or `mountfs` causes - ignore it.

set -euo pipefail

base=~/dedupfs-perf/dfs-mount-dirs
repo_root="$base/repo"
mount_path="$base/mnt"
counter_file="$base/counter.txt"
repo_root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

mkdir -p "$base"

cargo build --release -p cli --manifest-path "$repo_root_dir/Cargo.toml"
dfs_exe="$repo_root_dir/target/release/dfs"

if [ ! -d "$repo_root" ]; then
  "$dfs_exe" create-repo "$repo_root"
fi

# A leftover $mount_path from a previous run that crashed or was killed uncleanly could be either
# a stale, still-registered FUSE mount (unmounting it is the only way to make the directory usable
# again - a plain `rm -rf` against a live-but-orphaned mount fails or, worse, deletes through into
# the mounted filesystem) or just a plain leftover directory. Handle both: unmount defensively
# first (ignoring failure - there may be nothing mounted at all), then remove whatever remains.
fusermount3 -u "$mount_path" 2>/dev/null || true
rm -rf "$mount_path"
# Unlike WinFSP, libfuse does not create the mountpoint itself - it refuses to mount onto a path
# that does not already exist ("bad mount point: No such file or directory").
mkdir "$mount_path"

"$dfs_exe" mount --repo "$repo_root" "$mount_path" --read-write &
mount_pid=$!

cleanup() {
  kill "$mount_pid" 2>/dev/null || true
  deadline=$((SECONDS + 5))
  while mountpoint -q "$mount_path" 2>/dev/null && [ "$SECONDS" -lt "$deadline" ]; do
    sleep 0.2
  done
  fusermount3 -u "$mount_path" 2>/dev/null || true
}
trap cleanup EXIT

# The mounted tree can legitimately be empty at this point (a fresh repository has no entries), so
# "wait for a non-empty directory listing" does not work here as a readiness signal. Instead,
# retry the actual operation being benchmarked (mkdir) until it succeeds, then remove that probe
# entry so it does not pollute the Scale count below.
probe_path="$mount_path/_ready_probe"
deadline=$((SECONDS + 15))
while true; do
  if mkdir "$probe_path" 2>/dev/null; then
    rmdir "$probe_path"
    break
  fi
  if [ "$SECONDS" -ge "$deadline" ]; then
    echo "mount did not become ready within 15s (requires /dev/fuse access)" >&2
    exit 1
  fi
  sleep 0.2
done

counter=$( [ -f "$counter_file" ] && cat "$counter_file" || echo 0 )

for run in 1 2 3 4 5; do
  SECONDS=0
  ops=0
  while (( SECONDS < 20 )); do
    counter=$((counter+1))
    mkdir "$mount_path/d$counter"
    ops=$((ops+1))
  done
  elapsed=$SECONDS
  echo "$run: $ops dirs, ${elapsed}s, $((ops/elapsed)) ops/s"
done
echo "$counter" > "$counter_file"
