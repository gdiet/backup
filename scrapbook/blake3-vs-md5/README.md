# `blake3` (Rust) vs. `MD5` (Java): raw hashing throughput

A one-off comparison, not part of the `rust/` Cargo workspace (see this
directory's `Cargo.toml` - it declares its own empty `[workspace]` so
`cargo build --workspace`/`cargo test --workspace` at the repo root never
touch it) and not run as part of CI. Kept here for reference rather than
deleted after use, per the request that prompted it.

## Why

This project hashes chunks/content with `blake3`, truncated to 20 bytes
(see `cli::store::Blake3Hasher` in the main `cli` crate). The Scala
prototype it replaces hashed whole files with Java's
`MessageDigest("MD5")`. The question was simply: how much faster is the
former, in practice, on real hardware? Not a comparison of the
*algorithms'* cryptographic properties - MD5 is broken and was never used
here for security, only as the Scala prototype's dedup hash - only of raw
hashing throughput.

## Methodology

Both sides do the same thing: allocate a 128 MiB buffer, hash it in a
tight loop for 5 seconds (after a 1-second warm-up run that's discarded,
which matters far more for the JVM's JIT than for the AOT-compiled Rust
side, but both do it for symmetry), and report `GB/s = total bytes hashed
/ elapsed seconds`. Single-threaded on both sides - `blake3`'s crate has
optional multi-threaded modes (e.g. `update_rayon`), deliberately not used
here, since Java's `MessageDigest` has no equivalent to compare against.

- `src/main.rs` - Rust side. Run with `cargo run --release` (a debug build
  would badly understate `blake3`'s real throughput - the crate's SIMD
  code paths still work in debug, but with much of the surrounding
  overhead un-optimized).
- `Md5Bench.java` - Java side. Compile once with `javac Md5Bench.java`;
  the resulting `Md5Bench.class` runs on any Java 21+ runtime regardless
  of platform (a class file isn't platform-specific) - so it only needs
  compiling once even when comparing multiple OSes/JVMs.

## Results

Measured 2026-07-31. Rust 1.97.0. Java 21 (Temurin 21.0.6 on Windows,
Temurin 21.0.7 on WSL2 - two different bundled JREs happened to be
at hand, both Java 21 LTS, close enough for this comparison).

| Platform                                     | CPU                                          | Rust `blake3` | Java `MD5` | Ratio |
|-----------------------------------------------|-----------------------------------------------|---------------|------------|-------|
| Windows 10 IoT Enterprise LTSC 2021 (10.0.19044) | Intel Core i5-6200U, 2 cores / 4 threads, 2.30 GHz | 2.74 GB/s     | 0.56 GB/s  | ~4.9x |
| WSL2, Debian 12 "bookworm", kernel 6.6.87.2  | same physical CPU (WSL2 VM)                   | 2.60 GB/s     | 0.55 GB/s  | ~4.7x |

`blake3` is roughly 5x faster than Java's `MD5` on this hardware, fairly
consistently across both platforms (Windows vs. WSL2's lightweight-VM
overhead makes only a few percent difference for either side). Broadly
consistent with what `blake3`'s own design targets: SIMD-vectorized
compression and an internally tree-structured (parallelizable, though not
exercised here single-threaded) construction, versus MD5's much older,
purely serial Merkle-Damgård design.

## Reproducing

```bash
cd scrapbook/blake3-vs-md5
cargo run --release
javac Md5Bench.java && java Md5Bench
```

If Java isn't on `PATH`, point at any Java 21+ `javac`/`java` directly,
e.g. (Windows, Temurin):

```bash
"/path/to/jdk-21/bin/javac.exe" Md5Bench.java
"/path/to/jdk-21/bin/java.exe" Md5Bench
```
