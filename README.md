# Rust Components

This workspace contains Rust crates used by the backup application.

## Crates

| Crate   | Description                                                                          |
|---------|--------------------------------------------------------------------------------------|
| `cdc`   | Content-defined chunking library (rolling-fingerprint based)                         |
| `store` | Sequential on-disk byte store, file format compatible with the Scala `LongTermStore` |
| `cli`   | Deduplicating backup application; builds the `backup` binary                         |

## Development

### Build (debug)

```bash
# from rust/
cargo build
```

### Build (release / prod)

```bash
cargo build --release
```

### Run directly via cargo

```bash
# debug build, runs the cli crate's `backup` binary
cargo run -p cli -- <args>

# release build
cargo run --release -p cli -- <args>

# example
cargo run -p cli -- store --create-dirs source1 source2 target
```

### Tests

```bash
cargo test
```

### Linter (Clippy)

```bash
cargo clippy
```

For stricter checks (recommended before committing):

```bash
cargo clippy -- -D warnings
```

### Formatting

```bash
cargo fmt         # apply formatting
cargo fmt --check # check without modifying (e.g. in CI)
```

### All checks at once

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
