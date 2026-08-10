#!/usr/bin/env bash
# Builds backup.exe (x86_64-pc-windows-msvc) via Docker + cargo-xwin, without
# needing a real Windows host or Visual Studio. See
# docs/plans/implemented/windows-docker-cross-build.md for the full
# rationale, and docker/windows-cross/Dockerfile for the image this drives.
#
# The Docker daemon is required (docker.com); on first run cargo-xwin
# downloads the MSVC CRT/SDK pieces it needs (a few hundred MB) - this is
# cached in a named Docker volume so later runs are fast.
#
# This only proves the code compiles/links for Windows - it does NOT
# exercise `backup mount` (WinFSP is a Windows kernel driver, unavailable
# under Linux/Wine). Run the real smoke test on a Windows machine with
# WinFSP installed before trusting a build produced this way.
#
# Usage: scripts/build-windows-docker.sh [output-dir]
#   output-dir defaults to target/release-docker (already gitignored via
#   /target/, unlike the repo root).
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
out_dir="${1:-$repo_root/target/release-docker}"

image_tag="backup-windows-cross"
cache_vol="backup-windows-cross-cache"
cargo_registry_vol="backup-windows-cross-cargo-registry"

mkdir -p "$out_dir"

docker build -t "$image_tag" -f "$repo_root/docker/windows-cross/Dockerfile" "$repo_root"

docker volume create "$cache_vol" >/dev/null
docker volume create "$cargo_registry_vol" >/dev/null

docker run --rm \
    -v "$repo_root:/workspace" \
    -v "$cache_vol:/root/.cache" \
    -v "$cargo_registry_vol:/usr/local/cargo/registry" \
    "$image_tag"

built_exe="$repo_root/target/x86_64-pc-windows-msvc/release/backup.exe"
cp "$built_exe" "$out_dir/backup.exe"
echo "Built $out_dir/backup.exe"
