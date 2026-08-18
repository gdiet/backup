# Feature Comparison: Scala Implementation vs. Rust Implementation

Per-feature parity tracking against the Scala implementation.

Status values: `implemented` | `planned` | `not planned` (requires a one-line reason).

| Scala Feature | Status | Notes |
|---|---|---|
| Whole-file content deduplication (MD5-based) | planned | non-CDC, whole-file chunking exists as a library building block (see REQ-STORAGE-003), using `blake3` instead of MD5, but is not yet reachable through any command |
