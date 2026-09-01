# Feature Comparison: Scala Implementation vs. Rust Implementation

Per-feature parity tracking against the Scala implementation.

Status values: `implemented` | `planned` | `not planned` (requires a one-line reason).

| Scala Feature | Status | Notes |
|---|---|---|
| Whole-file content deduplication (MD5-based) | planned | non-CDC, whole-file chunking exists as a library building block (see REQ-STORAGE-003), using `blake3` instead of MD5, but is not yet reachable through any command |
| Mount read-write (real content writes: create/write/truncate/unlink) | implemented | `crates/cli/src/dedup_fs.rs`, DESIGN-MOUNT-006/007/008/009/010/012/013/015 in [`../docs/design/mount-write-path.md`](../docs/design/mount-write-path.md); DESIGN-MOUNT-009's failure log is a plain file only, not yet queryable |

## Non-Functional Parity

Beyond matching individual features one-for-one above, this implementation's performance and
usability are each at least as good as the Scala implementation's, and better where achievable -
not a specific feature to check off, but a standing bar the rewrite is held to throughout.

- **Performance**: this project's own `REQ-PERFORMANCE-*` requirements in
  [`../requirements/non-functional/performance.md`](../requirements/non-functional/performance.md)
  set an independently-motivated bar (matching or beating the *native* filesystem on slow storage),
  not a Scala comparison - meeting them does not by itself establish Scala parity, since the Scala
  implementation may itself already match or beat native storage for some operations (not yet
  measured here). Verifying this bullet needs an actual head-to-head measurement against the Scala
  implementation, not an inference from the native-filesystem requirements alone.
- **Usability**: not yet broken out into individual requirements the way performance is - judged
  holistically for now (installation effort, defaults, error messages, day-to-day operation) against
  the Scala implementation as the baseline to match or beat, per `../requirements/non-functional/operability.md`'s
  existing usability-adjacent requirements (REQ-OPERABILITY-001/003/004). Revisit this bullet if a
  concrete usability shortfall against Scala is ever identified and needs its own tracked
  requirement.
