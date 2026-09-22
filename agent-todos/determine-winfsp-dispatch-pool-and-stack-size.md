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

<Fill in once completed: what was measured, on what WinFSP version/Windows build, the actual
numbers found, and whether platform-specific handling was needed as a result.>
