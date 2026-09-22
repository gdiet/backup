# Determine libfuse3's dispatch-thread-pool size and per-thread stack size

**Why parked**: needs a real libfuse3 mount (`/dev/fuse` access that actually works, not just the
device node being present). Confirmed *not* available in this session (a `cargo test --workspace
real_mount` run failed with "mount did not become ready within 5s" despite `/dev/fuse` existing as
a device node - this remote environment's container does not support a real mount, the same known
limitation `crates/mountfs/CLAUDE.md` documents for "most agent sessions"). Needs an environment
where `cargo test --workspace -- --skip real_mount` is *not* necessary - e.g. a developer's own
WSL2/Linux machine, matching the precedent in `agent-todos/done/wire-write-backpressure-delay.md`
(that TODO was itself parked until a WSL2/Linux session picked it up).
**Size**: small-medium - the measurement itself is small; confirm with the developer only if the
findings suggest the RAM-budget design's provisional constant (see below) needs a materially
different value or platform-specific handling beyond what is already anticipated.
**Opened**: 2026-09-23, by a Claude Code on the web session (this same remote environment lacks
`/dev/fuse` mount capability), `memory-design` branch.
**Context**: `developer-todos/ram-budget-and-backpressure-redesign.md` - this is the Linux/libfuse3
half of that TODO's open question 1 ("FUSE/WinFSP dispatch-thread-pool size and stack size"); see
`agent-todos/determine-winfsp-dispatch-pool-and-stack-size.md` for the WinFSP counterpart, opened
alongside this file for the same reason. Also see
`agent-todos/done/wire-write-backpressure-delay.md` for the calibration methodology this mirrors.

## What is needed

The RAM-budget design needs to reserve memory for however many FUSE dispatch threads libfuse3 can
run concurrently against this project's mount, each using however much stack space it actually
allocates:

1. **Dispatch-thread-pool size**: `crates/mountfs/src/linux/sys.rs` marks the FUSE `init` callback
   (where `fuse_conn_info` thread-pool-relevant negotiation would happen) as `Unimplemented` - this
   project neither queries nor configures libfuse3's pool size today. libfuse3 does not appear (from
   what was checked without a working mount) to expose a direct "current pool size" query, so this
   likely needs indirect measurement: drive enough concurrent, artificially slow write operations
   against a real mount to force the pool to grow, and track the peak number of *concurrently
   executing* dispatch callbacks via a shared atomic counter (incremented on entry, decremented on
   exit, sampling the max) - the same style of empirical measurement
   `wire-write-backpressure-delay.md`'s own calibration already used.
2. **Per-thread stack size**: libfuse3's worker threads are created by its own C code via plain
   `pthread_create`, which inherits the *process's* default pthread stack size - commonly, but not
   universally, 8 MiB on Linux, distinct from Rust's own `std::thread::Builder` default of 2 MiB.
   Directly, precisely measurable from *inside* a real dispatch thread via
   `libc::pthread_getattr_np(pthread_self(), &mut attr)` +
   `libc::pthread_attr_getstack(&attr, &mut addr, &mut size)` (`libc` is already a workspace
   dependency) called from within an instrumented `write` callback during a real mount, reporting
   the observed size back through a shared atomic/channel.

## Provisional value in use until this is resolved

Implementation of `developer-todos/ram-budget-and-backpressure-redesign.md` is proceeding without
blocking on this - using a documented, conservative, CLI-overridable placeholder for the FUSE
thread-pool reserve (see that TODO / the resulting design doc for the actual constant chosen and
where it lives in code). Update that constant, and its surrounding comment, once this item's real
findings land - do not just file this as done without closing that loop.

## Suggested approach

Same shape as `agent-todos/done/wire-write-backpressure-delay.md`: mount a repository read-write via
real libfuse3, drive a workload that forces concurrent dispatch (multiple large/slow writers),
sample with temporary instrumentation (removed before the final commit, per that TODO's own
precedent - or made a permanent `#[ignore]`d `real_mount_`-prefixed test if
`developer-todos/ram-budget-and-backpressure-redesign.md`'s own "Can this be verified by a test?"
section is also being acted on at the same time), then report the findings back into that TODO (or
wherever the RAM-budget design has landed by the time this is picked up).

## Done

<Fill in once completed: what was measured, on what kernel/libfuse3 version, the actual numbers
found, and whether the provisional constant above needed to change as a result.>
