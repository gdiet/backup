# `real_mount_write_backpressure_delay_grows_then_drains_with_the_persist_queue` fails on real Windows

**Why parked**: needs actual investigation (is this a genuine WinFSP-vs-libfuse3 behavioral
difference, a timing issue, or does the test's `unmount_and_join` helper's unconditional
`fusermount3 -u` call - a Linux-only tool - mean this test was never meant to run on Windows at
all?) rather than a quick fix - out of scope for the session that found it, which was verifying an
unrelated Windows-specific fix (`crates/cli/src/ingest.rs`'s directory-mtime `FILE_FLAG_BACKUP_SEMANTICS`
fix) and only incidentally got this test compiling at all by fixing a separate, unrelated compile
error first (see `crates/cli/src/dedup_fs.rs`'s `real_mount_concurrent_handles_converge_toward_the_per_handle_halving_formula`,
`#[cfg(target_os = "linux")]`-gated in the same commit for exactly this reason - this test was not
similarly gated, since it does compile on Windows, only fail at runtime).
**Size**: medium - confirm with the developer before starting. Needs real investigation on real
Windows/WinFSP to tell apart "this test needs a Windows-specific teardown/timing adjustment" from
"this is a genuine backpressure-behavior difference worth its own finding" from "this test was
only ever designed for Linux and needs the same `#[cfg(target_os = "linux")]` gate its sibling
test just got."
**Opened**: 2026-09-23, by a Claude Code Desktop-App session on `julius` (native Windows, real
WinFSP), while verifying an unrelated fix.
**Context**: `crates/cli/src/dedup_fs.rs` (`real_mount_write_backpressure_delay_grows_then_drains_with_the_persist_queue`,
`unmount_and_join`, `timed_synced_write`); `docs/design/mount-write-path.md` DESIGN-MOUNT-006 (the
backpressure formula this test exercises).

## What was observed

Running the full `cargo test --workspace` suite on real Windows (after fixing an unrelated compile
error blocking it entirely - see the commit history around this file's own opening), this test
failed:

```
thread 'dedup_fs::tests::real_mount_write_backpressure_delay_grows_then_drains_with_the_persist_queue' panicked at crates\cli\src\dedup_fs.rs:572:9:
/big.txt did not settle to size 8388608 within the deadline
```

This happens *before* the test ever reaches `unmount_and_join`'s `fusermount3 -u` call (a
Linux-only tool that would fail outright on Windows if reached) - the failure is in the
write/wait-for-settle phase itself, suggesting something about how this test's write pattern or
timing plays out differently against a real WinFSP mount than against libfuse3, not simply "this
test can never run on Windows at all." Not chased further - this needs someone to actually step
through what is different (does `write()` reach this filesystem's dispatch the same way? does the
backpressure delay/settle-pool draining behave the same? is `wait_for_settled`'s own deadline just
too short for WinFSP specifically?).

## Suggested approach

Mirror the investigative style of `agent-todos/done/determine-winfsp-dispatch-pool-and-stack-size.md`
and the `DispatchProbeFs` work in `crates/mountfs/src/linux/mod.rs`/`windows_dispatch_probe_helper.rs`:
add temporary instrumentation (or reuse `wait_for_settled`'s own polling, extended with logging) to
see exactly where/why the write does not reach the expected settled size in time on real WinFSP,
then decide whether the fix is a Windows-specific timing adjustment, a genuine formula/behavior
difference worth documenting, or (if this test's assumptions turn out to be fundamentally
libfuse3-specific, the same way its sibling test's `open_o_sync` helper was) a
`#[cfg(target_os = "linux")]` gate matching that sibling.
