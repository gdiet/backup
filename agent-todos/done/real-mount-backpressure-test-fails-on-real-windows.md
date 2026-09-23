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

## Done

**Completed**: 2026-09-23, by a Claude Code Desktop-App session on `julius` (native Windows, real
WinFSP).

Added temporary diagnostic `eprintln!`s to `DedupFs::create`/`write`/`release_and_maybe_submit` and
`settle_pool::run_job` (start/commit/error) to trace exactly where the write/settle sequence broke
down. Never got to see that trace, though - the very next run of the test failed even earlier,
before the mount ever became ready:

```
Cannot create WinFsp-FUSE file system: mount point in use.
```

This was reproducible on every subsequent attempt (waiting up to ~30s between retries did not
clear it), even though `tempfile::tempdir()` gives each attempt a fresh, unique mountpoint path -
ruling out a simple path collision. Considered and ruled out a fixed-`volname` collision
(`crates/mountfs/src/windows/mod.rs` hardcodes `volname=DedupFS` for every mount): a *separate*
real-WinFSP test using the identical volname
(`real_mount_dispatch_thread_pool_and_stack_size`, a different process) mounted successfully at the
same time this test's mountpoint was stuck - so the "in use" state is specific to something about
*this* test's own mount, not a global volname lock.

**Root cause**: `unmount_and_join` unconditionally shells out to `fusermount3 -u`, a Linux-only
tool with no Windows equivalent this project uses. This test mounts on an in-process background
thread (`mount_for_test`, `thread::spawn(move || mountfs::mount(...))`), inside the *same process*
running the test's own assertions - and `crates/mountfs/src/windows/mod.rs` has no working
in-process clean-shutdown call at all (confirmed by this crate's own existing convention:
`windows_mount_spike_helper.rs`'s doc comment explains exactly why its own real-mount tests use a
*separate child process*, torn down via `Child::kill`, specifically because no in-process shutdown
exists on Windows). So on Windows, this test has no way to cleanly unmount even in principle - and
since it panics at `wait_for_settled` before ever reaching `unmount_and_join` anyway, the mount
thread is left permanently blocked inside `mountfs::mount()`'s blocking WinFSP dispatch call when
the test process exits. An OS process exit normally reclaims every thread unconditionally, but a
thread killed abruptly mid-syscall inside WinFSP's own blocking dispatch loop - rather than exiting
through a real unmount or a graceful Ctrl+C shutdown - appears to leave the kernel-mode WinFSP
driver itself in a bad state, persistent enough to block a *later, unrelated* mount attempt from
this same test (reproduced twice in a row). This also very plausibly explains the *original*
"did not settle within the deadline" symptom that opened this TODO: that very first Windows run of
this test was, by definition, the first time anything in this codebase's history had mounted
DedupFS this way (in-process thread, `fusermount3` teardown) on real Windows - if an even earlier,
unrelated mount attempt in that same session had left WinFSP in a similarly degraded state, the
write/settle path could easily have been starved or stalled by that, rather than by any genuine
backpressure-formula or settle-pool bug.

Given this, the test is not fixable with a timing adjustment or a formula fix - its whole
mount/unmount mechanism is structurally Linux-only, the same underlying reason its sibling test
(`real_mount_concurrent_handles_converge_toward_the_per_handle_halving_formula`) already needed a
`#[cfg(target_os = "linux")]` gate. Applied the identical fix here, and additionally gated the
three test-only helpers this test (and its sibling) exclusively depend on -
`mount_for_test`/`unmount_and_join`/`timed_synced_write` - which would otherwise become dead code
on Windows once both callers are gated (`wait_for_settled` stays ungated: several ordinary,
non-real-mount unit tests use it too).

Diagnostic `eprintln!`s removed before committing (per this project's own established convention
for temporary instrumentation). Verified: full `cargo test --workspace` (with and without
`--skip real_mount`) passes cleanly on Windows afterward, including both real `windows_mount.rs`
WinFSP tests; the stuck "mount point in use" state cleared on its own by the time of that
verification run (not something this fix actively resolves - simply no longer triggered, since the
test that caused it no longer attempts to mount on Windows at all). Also verified on WSL2/Debian:
the full suite still passes there, and both `real_mount_*` tests in `dedup_fs.rs` still run (not
skipped) and pass under the `#[cfg(target_os = "linux")]` gate, unaffected by this change.

**Residual open question, not chased further**: whether WinFSP's stuck-mount state (triggered by
an abrupt in-process thread kill mid-dispatch) is something this project should actively guard
against more broadly - e.g. if a *real* mount session's dispatch thread were ever killed abruptly
outside a test (a crash, a forced process kill), would a later `dfs mount` attempt against the same
or a different repository hit the same "mount point in use" wall? Not investigated - this session's
fix only removes the one test that was reliably triggering it, not the underlying WinFSP
abrupt-kill behavior itself.
