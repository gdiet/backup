# Rust Components

This workspace contains Rust crates used by the backup application.

## Crates

| Crate | Description |
|-------|-------------|
| `cdc` | Content-defined chunking library (rolling-fingerprint based) |

## Development

### Build

```bash
# from rust/
cargo build
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
