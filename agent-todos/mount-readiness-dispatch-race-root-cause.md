# Investigate the root cause of the mount-readiness dispatch race, and whether it generalizes beyond the test that found it

**Why parked**: out-of-scope finding - came up while implementing and debugging
`agent-todos/done/real-libfuse3-mount-backpressure-and-handle-cap-test.md`'s two `real_mount_*`
tests in `crates/cli/src/dedup_fs.rs`, and is a genuinely separate investigation from that task's
own scope (which just needed a reliable workaround for its own test helper, not a root-cause
diagnosis or a production-code assessment).
**Size**: medium/large (confirm with the user first)
**Opened**: 2026-09-22, by Linux/WSL2 session (real `/dev/fuse` access available here, same
environment this needs)
**Context**: `crates/cli/src/dedup_fs.rs`'s `mount_for_test` test helper (added by the same prior
session, see the commit implementing the two `real_mount_*` tests); `crates/cli/src/mount.rs`'s
`try_run` (the real `dfs mount` command path).

## The finding

While building `mount_for_test` (a test helper that mounts a `DedupFs` via `mountfs::mount` on a
background thread and waits for a readiness probe write to succeed before returning), several real
client-side filesystem operations issued immediately after that readiness probe succeeded were
observed to return success to the client **without the corresponding server-side `create()`/
`write()` handler ever being dispatched** - confirmed via temporary tracing instrumentation in
`DedupFs::create`/`DedupFs::write` that showed zero matching log lines for those operations, even
though the client-side syscalls (including `O_SYNC`-opened writes, which force the kernel to treat
the `write(2)` syscall itself as synchronous) reported `Ok`. This held across several consecutive
operations, from the same process, right after the probe. Inserting almost any unrelated syscall in
between - even a read-only `mountpoint -q` child-process invocation - reliably "unblocked" the
backlog, after which all previously-silent operations' dispatches fired at once.

The workaround applied in `mount_for_test` is a documented, fixed `thread::sleep(Duration::from_
millis(200))` right after the readiness probe succeeds. This is scoped to test infrastructure only.

One alternative, harmless explanation was ruled out before parking this: a fallback where the
kernel serves an `O_CREAT` open via `mknod()`+`open()` instead of a dedicated `create()` callback,
which would have meant the "missing" dispatches were never missing at all, just going through a
code path that was never traced (`DedupFs::open`/`open()` was never instrumented, only
`create()`/`write()`). This is ruled out here: `crates/mountfs/src/linux/sys.rs` has `mknod:
Unimplemented`, while `create` is implemented and wired (`create: Some(dispatch_create::<T>)` in
`crates/mountfs/src/linux/mod.rs`) - libfuse3 has no `mknod`+`open` fallback available to route
through for an `O_CREAT` open on a path that does not yet exist, so it must go through `create()` to
succeed at all. A second alternative - the kernel believing the path already existed, due to some
stale dentry, and going through `open()` instead of `create()` - is not ruled out, since `open()`
itself was never traced.

## What is not known

- The exact kernel/libfuse3 mechanism behind the race is unconfirmed. Candidate explanations, none
  verified: a libfuse3/kernel-internal warm-up or negotiation window settling shortly after the
  first successful request against a fresh mount; something specific to how `open()` is dispatched
  that was never directly observed (see above).
- Whether the race reproduces with realistic tools (`cp`, `rsync`, a file manager) instead of the
  narrow synthetic trigger pattern that found it (several back-to-back operations from the same
  process, immediately after a tight readiness-poll loop with no other syscalls interleaved). Real
  tools naturally interleave many other syscalls (`stat`, `readdir`, permission checks), which -
  per the empirical "any unrelated syscall unblocks it" observation above - plausibly avoid
  triggering this at all in practice, but that has not been tested.
- Whether it reproduces against the real `dfs mount` command path at all: `crates/cli/src/mount.rs`'s
  `try_run` calls `mountfs::preflight()` then `mountfs::mount(fs, mountpoint, !read_write)`
  directly, with no warmup/delay of any kind between them - i.e. nothing analogous to
  `mount_for_test`'s workaround protects the real binary today.

## Why this matters enough to track

If the race generalizes to realistic usage - most plausibly, a naive automated script that polls
mount readiness and then immediately issues a write with little other I/O in between - it would be
a genuine, if narrow, silent-data-loss risk: a write that is reported successful to the caller but
never reaches this filesystem's own persistence path. That would not be caught by this project's
existing tests, all of which either do not exercise a real mount at all, or (after this session's
work) explicitly work around the race rather than exposing it.

## What the next attempt should do

1. Add tracing to `DedupFs::open` (not just `create`/`write`) and re-run the original failing
   scenario (the tight readiness-poll-then-immediate-write pattern, without `mount_for_test`'s
   200ms workaround) to settle whether `open()` is involved.
2. Test whether the race reproduces using a realistic tool (`cp`, `rsync`) run against a real
   `dfs mount`, immediately after the mount becomes ready by whatever readiness signal that tool's
   caller would realistically use (not this project's own synthetic probe loop).
3. Depending on findings, assess whether the real `dfs mount` command path
   (`crates/cli/src/mount.rs::try_run`) needs its own protection (and if so, design it properly -
   e.g. via `docs/design/`) rather than reusing the test-only fixed-sleep workaround, which was
   never intended as a production fix.
