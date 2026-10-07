#!/usr/bin/env bash
# Runs a command inside the dfs-slow cgroup so that setup.sh's --wbps / --iops limits apply to it.
#   ./enter-throttle.sh dfs ingest ...
set -euo pipefail
[ -d /sys/fs/cgroup/dfs-slow ] || { echo "no dfs-slow cgroup; run setup.sh with --wbps or --iops" >&2; exit 1; }
exec sudo sh -c 'u="$1"; g="$2"; shift 2; echo $$ > /sys/fs/cgroup/dfs-slow/cgroup.procs && exec setpriv --reuid="$u" --regid="$g" --init-groups -- "$@"' sh "$(id -u)" "$(id -g)" "$@"
