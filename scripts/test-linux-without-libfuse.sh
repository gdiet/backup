#!/usr/bin/env bash
# Verifies mountfs::preflight() actually degrades gracefully (a clean
# message, not a crash) when libfuse3 is genuinely absent - not just
# documented/assumed (see mountfs/src/linux/sys.rs's exports() doc
# comment).
#
# Builds the preflight_check example here (this host's own libfuse3, if
# any, is irrelevant - nothing links against it at build time, only
# dlopen at runtime), then runs the resulting binary inside a minimal
# container that never had libfuse3 installed, so its absence is genuine
# rather than something merely hidden via LD_LIBRARY_PATH.
#
# Manual/on-demand - not part of `cargo test`, since it needs Docker and
# is really an environment-behavior check, not a unit test.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"

cargo build --release -p mountfs --example preflight_check --manifest-path "$repo_root/Cargo.toml"

binary="$repo_root/target/release/examples/preflight_check"

echo "--- Running inside a container without libfuse3 ---"
output=$(docker run --rm -v "$binary:/preflight_check:ro" debian:bookworm-slim /preflight_check)
echo "$output"

if [[ "$output" == *"library not available"* ]]; then
    echo "PASS: reported absence gracefully, no crash"
else
    echo "FAIL: expected a graceful 'library not available' message, got: $output" >&2
    exit 1
fi
