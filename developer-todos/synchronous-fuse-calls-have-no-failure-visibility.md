# Synchronous FUSE calls have no failure visibility beyond their bare errno

**Noted**: 2026-09-02, during the `write-cache` branch experiment (DESIGN-MOUNT-017 in
`docs/design/tree-namespace-case-sensitivity.md`), while discussing what happens if a panic ever
poisons `Repository`'s internal lock.
**Size**: medium - confirm with the developer before starting. Touches DESIGN-MOUNT-009's existing
scope/design, not just a local code change.
**Context**: `crates/cli/src/dedup_fs.rs` (`to_errno`, `require_not_degraded`);
`crates/cli/src/failure_log.rs` (`FailureLog`); `crates/cli/src/settle_pool.rs`
(`is_systemic_db_error`); `docs/design/mount-write-path.md` (DESIGN-MOUNT-009).

## What was found

DESIGN-MOUNT-009's failure log/read-only-degradation mechanism (`crates/cli/src/failure_log.rs`)
only covers the **background settle-job path** (`write` -> cache -> `release` -> `JobPool`'s
background job, e.g. `settle_file`): a failure there gets one line appended to
`meta/write-failures.log`, and - if `is_systemic_db_error` classifies it as systemic (which
`db::Error::Poisoned` explicitly is) - flips an in-memory flag that `require_not_degraded` then
checks before any new *content* write.

Every **synchronous** FUSE call - `mkdir`, `rmdir`, `rename`, `unlink`, `lookup`/`resolve_path`,
`utimens`, i.e. everything that goes through `dedup_fs.rs`'s `to_errno` directly rather than through
`JobPool` - has none of this. A `db::Error` there (an I/O failure, a poisoned lock, anything) is
converted straight to an `Errno` and returned to the OS with:

- No line in `write-failures.log`.
- No effect on the degradation flag, regardless of how "systemic" the underlying cause is.
- No other record anywhere - the only visible trace of *why* is whatever Rust's default panic hook
  printed to stderr at the moment of the original panic (if that panic is what caused it), which
  depends entirely on whether the process's stderr is actually being captured wherever `dfs mount`
  is running.

This was found while discussing what a caller sees after a `Repository`-internal lock gets poisoned
(see `crates/db/src/lib.rs`'s `Error::Poisoned`, `docs/design/tree-namespace-case-sensitivity.md`'s
DESIGN-MOUNT-017): a panic while holding that lock during a synchronous call (`mkdir` in particular
is where DESIGN-MOUNT-017's experimental name cache does most of its work) would make every
subsequent synchronous call on that `Repository` silently return `EIO` forever, with nothing logged
and no degradation to read-only - not specific to the name-cache experiment itself, an existing gap
in `db::Error` handling generally that the experiment's discussion just happened to surface.

## Suggested next step

Not yet decided - options worth weighing once picked up: extend `FailureLog`'s degradation check to
synchronous calls too (would need `dedup_fs.rs`'s `to_errno` call sites to record through it, not
just `JobPool`'s `on_failure` hook); a lighter-weight fix (log the *first* occurrence of a given
`db::Error` variant via `eprintln!`/a log crate, without the full read-only-degradation semantics);
or a deliberate decision that synchronous-call visibility is out of scope for DESIGN-MOUNT-009 and
this is fine as-is, recorded as such rather than left implicit.
