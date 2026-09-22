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

**Completed**: 2026-09-22, by a Claude Code Desktop-App session on `julius`, driving a WSL2/Debian
12 session on the same physical machine (`wsl-windows-sync`'s WSL clone at
`/home/georg/git/backup`) - kernel `6.18.33.2-microsoft-standard-WSL2`, `fusermount3` 3.14.0.

Added `DispatchProbeFs` to `crates/mountfs/src/linux/mod.rs`'s existing `real_mount_` test module,
mirroring the WinFSP side's approach but using the precise `pthread_getattr_np`/
`pthread_attr_getstack` route this file's own "Per-thread stack size" section above already
suggested, rather than the indirect approach the pool-size half still needed. A new `#[ignore]`d
`real_mount_dispatch_thread_pool_and_stack_size` test drives 24 concurrent native `std::thread`s
against it in-process (no separate child process needed here, unlike the Windows side, since the
mount runs on a background thread in the same process) and reads the shared counters back directly
after unmounting.

Two real bugs found and fixed along the way, both worth remembering for future work in this area:
- The probe initially passed `read_only: true` to `mount()` while relying on real writes going
  through - backwards from `mount()`'s own semantics. Harmless on Windows (WinFSP's read-only flag
  does not block writes at the driver level, already documented elsewhere in this crate), but
  Linux's kernel-level `-oro` genuinely enforced it, surfacing as "Read-only file system" errors
  until fixed.
- Small (a few bytes), unsynced writes were found to sometimes never reach this filesystem's own
  `write()` dispatch at all before `fusermount3 -u` - an ordinary buffered `write(2)` syscall
  returns once data is copied into the kernel's page cache, not once it reaches the FUSE daemon
  (this project's `mount()` requests no `direct_io`), so a tiny write can be satisfied entirely
  from cache and never actually dispatched before an unmount discards it. Fixed by writing a
  larger payload (256 KiB per call) and calling `sync_all()` to force a real flush - not a bug in
  the counting logic itself, a real characteristic of unsynced small writes worth remembering if
  this pattern comes up elsewhere (e.g. a future test of the mount's own write path).

### Findings (WSL2 on `julius`: Intel i5-6200U, 4 logical processors reported inside WSL2 via
`nproc` - same physical core count as the Windows/WinFSP measurement, notable since the two
platforms' pool sizes did *not* match despite that)

- **Dispatch-thread-pool size: 10**, reproduced identically across two separate runs (24 concurrent
  client threads, only 10 ever executing `write()` at once). This does **not** match this machine's
  own logical-processor count (4) - unlike the WinFSP measurement, where pool size matched core
  count exactly. A `libfuse3` warning appeared on every run - `Ignoring invalid max threads value
  4294967295 > max (100000).` - suggesting libfuse3 attempted some internal thread-count
  calculation that produced an invalid (`u32::MAX`) value and fell back to its own hardcoded
  default instead, which numbers like "10" ring a bell for from FUSE's own documented defaults, but
  this was not independently confirmed by reading libfuse3's own source for this version.
- **Per-thread stack size: 8,388,608 bytes (8 MiB) for every dispatch thread observed**, both runs -
  exactly matching the commonly-documented glibc default `pthread_create` stack size this file's own
  "Per-thread stack size" section above already anticipated, confirmed directly rather than assumed.

### For the RAM-budget design

Both platforms are now measured - see `agent-todos/done/determine-winfsp-dispatch-pool-and-stack-size.md`
for the Windows side (4 threads x 1 MiB) and `docs/design/ram-budget.md`'s "Provisional
dispatch-pool reserve" section for how both numbers compare against the current shared
`PROVISIONAL_DISPATCH_POOL_THREADS`/`PROVISIONAL_DISPATCH_THREAD_STACK_BYTES` constants
(`crates/cli/src/ram_budget.rs`) - both real measurements come in comfortably under the current
128 MiB (16 x 8 MiB) provisional reserve, so it is not under-reserved, but whether/how to tighten it
now that real numbers exist for both platforms is left as an explicit decision for the developer
rather than made silently here - see that design doc section for the concrete options.
