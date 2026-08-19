#!/usr/bin/env bash
# Cross-compiles the workspace for x86_64-pc-windows-msvc via Docker +
# cargo-xwin, without needing a real Windows host or Visual Studio. See
# docs/design/mount-abstraction.md for the rationale, and
# docker/windows-cross/Dockerfile for the image this drives.
#
# The Docker daemon is required (docker.com); on first run cargo-xwin
# downloads the MSVC CRT/SDK pieces it needs (a few hundred MB) - cached in
# a named Docker volume so later runs are fast.
#
# This only proves the code compiles/links for Windows - it does NOT
# exercise real WinFSP behavior (a Windows kernel driver, unavailable
# under Linux/Wine). Run the real smoke test on a Windows machine with
# WinFSP installed (see the julius-winfsp-ssh skill) before trusting a
# build produced this way.
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

# Type-checks the whole workspace first (cheap, catches everything
# including crates with no Windows-specific binary of their own), then
# builds+links mountfs's spike-helper binary as a stronger signal - a
# successful link confirms the kernel32/advapi32 imports resolve, not just
# that the types check out.
docker run --rm \
    -v "$repo_root:/workspace" \
    -v "$cache_vol:/root/.cache" \
    -v "$cargo_registry_vol:/usr/local/cargo/registry" \
    "$image_tag" \
    bash -c "cargo xwin check --workspace --target x86_64-pc-windows-msvc \
        && cargo xwin build --release --target x86_64-pc-windows-msvc -p mountfs"

# The container runs as root, so files it writes into the mounted target/
# directory come out root-owned on the host - fix that up before handing
# control back, so a later native (non-Docker) `cargo build` on this host
# does not fail with a permission error on its own build lock files.
docker run --rm -v "$repo_root/target:/target" alpine \
    chown -R "$(id -u):$(id -g)" /target

built_exe="$repo_root/target/x86_64-pc-windows-msvc/release/windows_mount_spike_helper.exe"
cp "$built_exe" "$out_dir/windows_mount_spike_helper.exe"
echo "Built $out_dir/windows_mount_spike_helper.exe"
