# Determine WinFSP's dispatch-thread-pool size and per-thread stack size

**Why parked**: needs a real Windows machine with WinFSP installed and a real, user-opened
interactive terminal (or the `julius-winfsp-ssh` skill's remote Windows machine) to drive a real
mount under load and instrument it - not reproducible from a Linux/WSL2 session or a Docker
cross-compile check alone. See `crates/mountfs/CLAUDE.md` for when to escalate to a real
Windows/WinFSP environment versus what `scripts/build-windows-docker.sh` can already check
(compile/link only, not runtime behavior).
**Size**: medium - confirm with the developer before starting. Beyond the raw measurement, it may
turn out the numbers need platform-specific (`#[cfg(windows)]`) handling in the RAM-budget reserve
calculation, which is a real code-design question, not just "record a number."
**Opened**: 2026-09-23, by a Claude Code on the web session, `memory-design` branch, during the
RAM-budget/backpressure design conversation.
**Context**: `developer-todos/ram-budget-and-backpressure-redesign.md` - this is the Windows/WinFSP
half of that TODO's open question 1 ("FUSE/WinFSP dispatch-thread-pool size and stack size"), split
out because it needs a different environment than the Linux/libfuse3 half. Also see
`crates/mountfs/src/windows/` (the WinFSP backend) and
`agent-todos/done/wire-write-backpressure-delay.md` for the calibration methodology this mirrors
(a real mount under a workload that forces the relevant behavior, sampled via temporary
instrumentation).

## What is needed

The RAM-budget design in `developer-todos/ram-budget-and-backpressure-redesign.md` needs to reserve
memory for however many dispatch threads the mount's underlying OS mechanism can run concurrently,
each using however much stack space it actually allocates. The Linux/libfuse3 side of this is
already tracked as that TODO's own open question 1, to be resolved empirically (instrument a real
libfuse3 mount, per the existing precedent). This item is the same investigation for WinFSP:

1. **Dispatch-thread-pool size**: how many concurrent requests can WinFSP actually dispatch to this
   filesystem implementation at once? Check whether WinFSP exposes this as a configurable/queryable
   value (unlike libfuse3, which - as far as checked from Linux - exposes no direct "current pool
   size" query) before falling back to empirical measurement (drive enough concurrent, artificially
   slow operations against a real WinFSP mount, track peak concurrently-executing dispatch calls via
   a shared atomic counter).
2. **Per-thread stack size**: what stack size do WinFSP's own dispatch threads actually run with?
   Windows' per-thread stack size defaults differ from Linux's pthread defaults (commonly 1 MiB on
   Windows unless the executable or thread creation call requests otherwise) - do not assume the
   Linux-side number (once found) carries over; verify independently. If WinFSP creates its own
   threads (rather than deferring to the OS/CRT default), check whether it exposes or documents its
   own stack-size choice.
3. Compare both numbers against whatever the Linux/libfuse3 investigation found. If they differ
   meaningfully, the RAM-budget reserve calculation needs `#[cfg(windows)]`/`#[cfg(unix)]`-gated
   constants (or a runtime-detected value - see the `dfs self-check` idea in
   `developer-todos/ram-budget-and-backpressure-redesign.md`) rather than one shared constant.

## Suggested approach

Mirror `agent-todos/done/wire-write-backpressure-delay.md`'s own calibration methodology: mount a
repository read-write via real WinFSP, drive a workload that forces concurrent dispatch (multiple
large/slow writers), sample with temporary instrumentation (removed before the final commit, per
that TODO's own precedent), then report the findings back into
`developer-todos/ram-budget-and-backpressure-redesign.md` (or wherever the RAM-budget design has
landed by the time this is picked up) rather than only in this file.

## Done

**Completed**: 2026-09-22, by a Claude Code Desktop-App session running directly on `julius`
(native Windows, real WinFSP - no remote/SSH access needed, see `machines.md`).

Built `crates/mountfs/src/bin/windows_dispatch_probe_helper.rs`, a new test-only helper binary
mounting a writable, in-memory `MountFilesystem` whose `write()` implementation records (a) the
peak number of concurrently-executing `write()` calls via a shared atomic counter, and (b) each
distinct dispatch thread's real stack size via the Win32 `GetCurrentThreadStackLimits` API (`kernel32`,
Windows 8+) - both written to a results file from a background reporter thread every 200 ms, so a
test can read the final snapshot after killing the helper process (same "spawn as a child process,
kill it, no clean-shutdown handshake" shape `windows_mount_spike_helper.rs` already uses). Added
`real_mount_dispatch_thread_pool_and_stack_size` in `crates/mountfs/tests/windows_mount.rs`
(`#[ignore]`d, same reasoning as this file's own `on_unmount_runs_and_process_exits_cleanly_on_ctrl_c`
- a slow, load-driving observation test, not a per-run correctness check): mounts the probe
read-write, drives 24 concurrent native `std::thread`s each writing 3 files in turn (each `write()`
artificially delayed 150 ms, long enough for real concurrent dispatch to actually overlap), then
reads back the peak concurrency and the observed stack sizes.

### Findings (julius: Windows 10 IoT Enterprise LTSC build 19044, Intel Core i5-6200U - 2 cores/4
logical processors, WinFSP as installed on this machine - version not separately checked)

- **Dispatch-thread-pool size: 4**, reproduced identically across two separate runs (24 concurrent
  client threads, only 4 ever executing `write()` at once). This exactly matches the machine's own
  logical-processor count (`[Environment]::ProcessorCount` / `Win32_Processor` both confirm 4) -
  strong evidence WinFSP's own default dispatch concurrency scales with the CPU core count, the
  same shape `crates/cli/src/settle_pool.rs::JobPool` already uses
  (`available_parallelism()`-sized) for an entirely different pool. Not confirmed whether this is a
  hard OS/WinFSP default or something WinFSP negotiates dynamically - only observed on this one
  machine's core count, not tested against a machine with a different core count to see if the
  number moves with it.
- **Per-thread stack size: 1,048,576 bytes (1 MiB) for every dispatch thread observed**, both runs.
  This matches Windows' commonly-documented default thread stack size, suggesting WinFSP creates its
  dispatch threads with the ordinary OS/CRT default rather than requesting a custom size.

### For the RAM-budget design (`developer-todos/ram-budget-and-backpressure-redesign.md`)

Windows' own numbers (4 threads x 1 MiB = 4 MiB reserve, on *this* machine) turned out smaller than
Linux/libfuse3's commonly-documented 8 MiB-per-thread pthread default would suggest for a comparable
pool size - so a shared, platform-independent constant would either over-reserve on Windows or
under-reserve if Linux's real number (still to be measured - see
`agent-todos/determine-libfuse3-dispatch-pool-and-stack-size.md`) turns out larger. Update: this
matches the RAM-budget design's own already-anticipated need for `#[cfg(windows)]`/`#[cfg(unix)]`-gated
constants (or a runtime-detected value) rather than one shared one, per this file's original "Suggested
approach" - not a new finding, but this session's numbers are the first real data confirming the two
platforms' pool-size *mechanism* (both apparently tied to logical core count) can still coincide even
while the *stack-size* half clearly differs (1 MiB here vs. the ~8 MiB pthread default expected on
Linux). The dispatch-pool-size question itself is not fully closed either: whether WinFSP's pool
genuinely tracks core count (rather than a fixed default that happens to equal 4 on this one
4-logical-processor machine) is still only a strong inference from a single machine, not confirmed
against a machine with a different core count.
