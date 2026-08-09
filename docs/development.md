# Developing `backup`

Building from source, running tests, and crate layout. For what the
application *does*, see the [main README](../README.md); for how it's
built internally, see [architecture.md](architecture.md).

## Crates

| Crate        | Description                                                                          |
|--------------|---------------------------------------------------------------------------------------|
| `cdc`        | Content-defined chunking library (rolling-fingerprint based); no I/O, pure bytes-in/chunk-boundaries-out |
| `store`      | Sequential on-disk byte store, file format compatible with the Scala `LongTermStore` |
| `db`         | SQLite-backed metadata: the file/directory tree, the dedup index, repository settings |
| `mountfs`    | Platform-abstracted repository mounting (Linux FUSE / Windows WinFSP)                |
| `spillcache` | RAM-budgeted, disk-spilling random-access byte buffer (`RamBudget`, `WriteCache`)     |
| `cli`        | Deduplicating backup application; orchestrates the crates above; builds the `backup` binary |

None of `cdc`, `store`, `db`, `mountfs`, or `spillcache` depend on each other - `cli` is the only crate that ties them together (see [architecture.md](architecture.md) for how, with a data-flow walkthrough for the `store` and `mount --read-write` commands).

## Prerequisites

A Rust toolchain (`rustc`/`cargo`, e.g. via [rustup](https://rustup.rs)) on
both platforms. Beyond that:

**Linux**:
- `libfuse3-dev` (Debian/Ubuntu) or `fuse3-devel` (Fedora) - build-time
  header for `mount`'s real-libfuse3 backend; `libfuse3`/`fuse3` itself is
  needed at runtime.
- `pkg-config` - used by `mountfs`'s `build.rs` to find the above.

**Windows**:
- Visual Studio Build Tools (the MSVC linker/`cl.exe` - the usual
  prerequisite for any Rust `*-pc-windows-msvc` build, not specific to
  this project).
- [WinFSP](https://github.com/winfsp/winfsp) installed - required at
  *runtime* for `mount` to work; **not** required at build time (`mountfs`
  resolves its DLL exports dynamically, no SDK/import library needed).

**Occasional, not needed for routine build/test** (both platforms):
- The standalone "Debugging Tools for Windows" (`cdb`/`windbg`, via the
  [Windows SDK installer](https://learn.microsoft.com/windows-hardware/drivers/debugger/debugger-download-tools) -
  select only that component) - for diagnosing a native crash inside
  WinFSP itself, not something routine development needs.
- `bindgen-cli` + `libclang` - only for re-running `bindgen` as a one-off
  sanity check against `mountfs/src/windows/sys.rs`'s hand-written struct
  layouts (see that file's doc comment) after touching them or updating
  the vendored WinFSP headers; `mountfs` itself never depends on `bindgen`
  or `libclang` at build time - this project deliberately hand-writes its
  FFI bindings instead of generating them (see
  `plans/implemented/05-cross-platform-mount-crate.md`). No Windows package manager
  (`winget`/`choco`/`scoop`) was available to install `libclang` there
  when this was last needed, so this was done from WSL instead, cross-
  target-parsing the headers for `x86_64-pc-windows-msvc` - works fine
  even though the crate itself only builds this way on native Windows:

  ```bash
  # WSL (Debian/Ubuntu)
  sudo apt-get install -y libclang-dev
  cargo install bindgen-cli

  # Symlink around clang's MSVC/SDK header lookup needing paths without
  # spaces/parens (only needed because we're parsing for a Windows target
  # from Linux - a real Windows+clang setup wouldn't need this) - adjust
  # the SDK/MSVC version numbers to what's actually installed.
  mkdir -p ~/winsdk-links
  ln -sfn "/mnt/c/Program Files (x86)/Windows Kits/10/Include/<version>/ucrt" ~/winsdk-links/ucrt
  ln -sfn "/mnt/c/Program Files (x86)/Windows Kits/10/Include/<version>/um" ~/winsdk-links/um
  ln -sfn "/mnt/c/Program Files (x86)/Windows Kits/10/Include/<version>/shared" ~/winsdk-links/shared
  ln -sfn "/mnt/c/Program Files (x86)/Microsoft Visual Studio/<edition>/BuildTools/VC/Tools/MSVC/<version>/include" ~/winsdk-links/msvc

  # From the repo root (rust/):
  bindgen mountfs/vendor/winfsp/fuse3/fuse.h \
    --no-layout-tests \
    --allowlist-type "fuse_operations|fuse_file_info|fuse_context|fuse_stat|fuse_statvfs|fsp_fuse_env" \
    -o /tmp/winfsp_bindgen.rs \
    -- -target x86_64-pc-windows-msvc \
       -I mountfs/vendor/winfsp/fuse3 -I mountfs/vendor/winfsp/fuse \
       -isystem ~/winsdk-links/msvc -isystem ~/winsdk-links/ucrt \
       -isystem ~/winsdk-links/um -isystem ~/winsdk-links/shared \
       -DFUSE_USE_VERSION=312 -x c
  ```

  Then diff `/tmp/winfsp_bindgen.rs`'s struct definitions against
  `mountfs/src/windows/sys.rs` by hand (field order, types, and - the one
  real bug this already caught once - bitfield packing).

## Build (debug)

```bash
# from rust/
cargo build
```

## Build (release / prod)

```bash
cargo build --release
```

## Run directly via cargo

```bash
# debug build, runs the cli crate's `backup` binary
cargo run -p cli -- <args>

# release build
cargo run --release -p cli -- <args>

# example
cargo run -p cli -- store --create-dirs source1 source2 target
```

## Tests

```bash
cargo test
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
cargo fmt --check && cargo clippy -- -D warnings && cargo test
```

## Performance

A simple single-threaded throughput benchmark runs the chunker on 7 MB of
pseudo-random data (seed 42) for 10 seconds, calling `next()` and `flush()`
per iteration — matching the Go benchmark exactly.

```bash
cargo run --release --example bench
```

Measured on Intel Core i7-1355U, 12 logical cores, 15 GB RAM, Ubuntu 24.04.4 LTS in WSL2 on Windows (single thread, `target_size_bits=20`, ~1 MB average chunk size):

| Implementation | Throughput |
|----------------|------------|
| Rust           | 2.78 GB/s  |
| Go             | 2.61 GB/s  |

Go benchmark for comparison:

```bash
go test -bench=BenchmarkCdc -benchtime=10s -count=1 ./internal/cdc
```

### Mount backpressure (persist-queue) benchmarks

`mount --read-write`'s persist path (see `docs/plans/implemented/
memory-pressure-backpressure.md`) gates how much unpersisted data can
accumulate before a `release`/bare `truncate` call starts blocking. Two
manual, real-`libfuse3`-mount benchmarks in `cli/src/mount.rs`'s
Linux-only test module measure this - not part of `cargo test`'s normal
run (`#[ignore]`), and Linux-only (the read-write FUSE test harness they
reuse doesn't compile on Windows):

```bash
# Many small files - throughput/latency under a simulated slow datastore disk.
BACKUP_BENCH_THROTTLE_STORE_MBPS=30 cargo test --release --package cli --bin backup \
  mount::tests::bench_many_small_files -- --ignored --nocapture

# A burst of large files - how much spilled-but-unpersisted data piles up,
# and how long the full drain takes once the burst is over.
BACKUP_BENCH_THROTTLE_STORE_MBPS=5 BACKUP_BENCH_LARGE_FILES=20 cargo test --release \
  --package cli --bin backup mount::tests::bench_large_files_spill_pressure -- --ignored --nocapture
```

Both use `store::bench_throttle_store_write` (`store/src/lib.rs`) to cap
`LongTermStore::write`'s throughput at `BACKUP_BENCH_THROTTLE_STORE_MBPS`
MB/s, simulating a slow target disk (e.g. ~30 MB/s for USB2) without
needing the real hardware - a no-op unless that variable is set, and
compiled into every build including release (see its own doc comment for
why it isn't `#[cfg(test)]`-gated: `cli`'s benchmarks pull `store` in as
an ordinary dependency, so a `store`-local test-only guard wouldn't take
effect there). `BACKUP_BENCH_FILES`/`BACKUP_BENCH_FILE_SIZE`/
`BACKUP_BENCH_LARGE_FILES`/`BACKUP_BENCH_LARGE_FILE_SIZE` tune the
workload shape; see the tests' own doc comments for defaults.

Real before/after results (same commands, same machine) from the change
that replaced the old fixed job-count gate with this byte-based one are
saved in `docs/plans/backpressure-bench/results.md` - including a
genuine nuance found while measuring: FUSE's `release` callback dispatches
asynchronously, so a small burst needs to be large enough to exhaust
libfuse's worker-thread pool before backpressure becomes visible to the
calling process at all.

### Hashing: Rust `blake3` vs. Java `MD5`

This project hashes chunks/content with `blake3` (truncated to 20 bytes -
see `cli::store::Blake3Hasher`); the Scala prototype it replaces hashed
whole files with Java's `MessageDigest("MD5")`. A raw single-threaded
throughput comparison - hashing a 128 MiB buffer in a loop for 5 seconds
(1 second discarded first as warm-up) - not a comparison of the
*algorithms'* cryptographic properties (MD5 is broken and isn't used here
for security), only of how fast each hashes bytes on the same hardware:

```bash
cd scrapbook/blake3-vs-md5
cargo run --release                    # Rust blake3
javac Md5Bench.java && java Md5Bench   # Java MD5 - needs a JDK on PATH
```

Measured on Intel Core i5-6200U (2 cores / 4 threads, 2.30 GHz), Rust
1.97.0, Java 21 (Temurin 21.0.6 on Windows / 21.0.7 on WSL2):

| Platform                                          | Rust `blake3` | Java `MD5` | Ratio |
|----------------------------------------------------|---------------|------------|-------|
| Windows 10 IoT Enterprise LTSC 2021                 | 2.74 GB/s     | 0.56 GB/s  | ~4.9x |
| WSL2 (Debian 12 "bookworm", kernel 6.6.87.2)        | 2.60 GB/s     | 0.55 GB/s  | ~4.7x |

`blake3` is roughly 5x faster than Java's `MD5` on this hardware, on both
platforms - consistent with `blake3`'s SIMD-friendly, tree-mode design
(parallelizable internally even within this single-threaded benchmark)
versus MD5's much older, purely serial construction. Benchmark source in
`scrapbook/blake3-vs-md5/` (its own standalone Cargo project, not part of
the `rust/` workspace or `cargo test`/CI - a one-off comparison, kept for
reference; see its own `README.md` for more detail).
