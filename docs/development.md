# Developing DedupFS

Building from source, running tests, and crate layout. For what the application *does*, see the
[main README](../README.md).

## Crates

| Crate | Description |
|---|---|
| `cdc` | Content-defined chunking library (rolling-fingerprint based); no I/O, pure bytes-in/chunk-boundaries-out |
| `mountfs` | Cross-platform repository mounting (Linux `libfuse3` / Windows WinFSP) behind a single trait |
| `cli` | The `dfs` binary - the command-line tool itself |

None of these currently depend on each other - `cli` does not yet call into `mountfs` or `cdc`
(no subcommands beyond `--help`/`--version` exist yet).

## Prerequisites

A Rust toolchain (`rustc`/`cargo`, e.g. via [rustup](https://rustup.rs)) supporting the 2024
edition. Beyond that, only [`mountfs`](../crates/mountfs/) has platform-specific needs, and only
at *runtime* (mounting), not for a plain build:

- **Linux**: `libfuse3` and `fuse3` installed to actually mount (`mountfs` resolves it dynamically
  via `dlopen`, not needed at build time - see
  [`docs/design/mount-abstraction.md`](design/mount-abstraction.md)). Debian/Ubuntu needs both
  packages, not just one: `apt install libfuse3-3 fuse3` - `libfuse3-3` alone is missing the
  `fusermount3` helper the mount actually needs.
- **Windows**: [WinFSP](https://github.com/winfsp/winfsp) installed to actually mount (same
  dynamic-loading story as libfuse3 above - not needed at build time). Building natively on
  Windows still needs the MSVC linker (Visual Studio Build Tools), the usual Rust
  `*-pc-windows-msvc` prerequisite; see "Build (release, Windows via Docker)" below for building
  a Windows binary without that installed at all.

## Build (debug)

```bash
cargo build
```

## Build (release)

```bash
cargo build --release -p cli
```

Produces `target/release/dfs`.

## Build (release, Windows via Docker)

Cross-builds `dfs.exe` for `x86_64-pc-windows-msvc` from Linux, using
[cargo-xwin](https://github.com/rust-cross/cargo-xwin) inside Docker - no Visual Studio install
needed. See [`docs/design/mount-abstraction.md`](design/mount-abstraction.md) for why
MSVC/`cargo-xwin` rather than mingw-w64.

```bash
# requires a running Docker daemon
scripts/build-windows-docker.sh
```

Produces `target/release-docker/dfs.exe` (or a directory passed as the first argument), alongside
`windows_mount_spike_helper.exe` - a `mountfs`-internal test helper the script also builds as a
stronger Windows link-check than a type-check alone gives (see the script's own comments); not a
release artifact itself. The first run downloads the MSVC CRT/SDK pieces `cargo-xwin` needs (a few
hundred MB); later runs reuse a Docker volume cache.

This only proves the code compiles/links for Windows - `dfs mount` cannot be exercised this way
(WinFSP is a Windows kernel driver, unavailable under Linux/Wine), so a manual smoke test on real
Windows with WinFSP installed is still required before trusting the result (see the
`julius-winfsp-ssh` skill).

## Run directly via cargo

```bash
# debug build, runs the cli crate's `dfs` binary
cargo run -p cli -- --help
cargo run -p cli -- --version

# release build
cargo run --release -p cli -- --version
```

## Tests

```bash
cargo test
```

By default this includes `mountfs`'s real-mount tests (`libfuse3` on Linux, WinFSP on Windows -
whichever the target OS compiles), each named with a `real_mount_` prefix. In an environment known
not to have `/dev/fuse`/WinFSP access (a sandboxed CI runner, a container without the FUSE device),
skip them explicitly rather than treating their failure as a real regression:

```bash
cargo test --workspace -- --skip real_mount
```

## Linter (Clippy)

```bash
cargo clippy
```

For stricter checks (recommended before committing):

```bash
cargo clippy -- -D warnings
```

## Formatting

```bash
cargo fmt         # apply formatting
cargo fmt --check # check without modifying (e.g. in CI)
```

## All checks at once

```bash
cargo build && cargo fmt --check && cargo clippy -- -D warnings && cargo test && cargo doc --no-deps
```

In an environment known not to have `/dev/fuse`/WinFSP access, replace `cargo test` above with
`cargo test --workspace -- --skip real_mount` (see "Tests" above).

## Other, manual checks

- `scripts/test-linux-without-libfuse.sh`: verifies `mountfs`'s preflight check degrades
  gracefully (a clean message, not a crash) when `libfuse3` is genuinely absent - needs Docker,
  not part of `cargo test`.
- `cargo run --release -p cdc --example bench`: single-threaded chunking throughput benchmark: see
  [`docs/design/cdc-chunking.md`](design/cdc-chunking.md) for a measured reference figure and why
  it isn't the workload's bottleneck.
