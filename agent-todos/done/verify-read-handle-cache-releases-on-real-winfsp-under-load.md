# Verify the read handle cache releases handles under a real WinFSP mount under load

**Why parked**: needs a real Windows environment with WinFSP installed, actually mounting and
driving read traffic under load - this session has no Windows target toolchain or real WinFSP
access.
**Size**: small (a focused runtime check once on real hardware, same pattern as
`agent-todos/done/verify-lock-file-delete-pending-on-real-windows.md`)
**Opened**: 2026-09-03, during a `write-cache` branch session (Linux container).
**Context**: `crates/store/src/lib.rs`'s thread-local read handle cache, DESIGN-STORE-004 in
[`../docs/design/byte-store.md`](../docs/design/byte-store.md) - specifically its "A shrinking
thread pool does not leak cached handles" section.

DESIGN-STORE-004's read handle cache is a thread-local LRU of open file handles, dropped (closing
the descriptor) via Rust's `thread_local!` destructor whenever the worker thread that used it
exits. On Linux this rides on `pthread`'s own TLS-destructor mechanism, confirmed to fire for any
`pthread` that exits cleanly - including libfuse's own C-created worker threads. Windows' TLS
destructors are documented to not run for some abnormal termination paths (e.g.
`TerminateThread`), so whether WinFSP's own worker-thread lifecycle (grows/shrinks its pool the
same way libfuse's does) reliably triggers the destructor and releases cached handles is not yet
confirmed - only reasoned about, by analogy to the Linux case.

`store` itself needs no more code to check this: `crates/cli/src/dedup_fs.rs`'s read path (which
uses `store::ByteStore` via `content_reader.rs`) carries no platform-specific gating at all, so it
already runs unmodified on Windows. What is missing is purely the empirical check on real hardware:

- Mount a repository read-write or read-only via WinFSP (`dfs mount`) with enough live file content
  to exercise many reads.
- Drive read traffic heavy and varied enough to make WinFSP's worker pool actually grow and then
  shrink again once idle (matching the "growing/shrinking thread pool" caveat DESIGN-STORE-004
  already accepts as a periodically-cold-cache cost, separate from this specific question).
- Confirm no descriptor/handle leak accumulates as the pool shrinks repeatedly - e.g. watch the
  process's open-handle count (Windows' own Handle Count in Task Manager / Process Explorer, or
  `Get-Process` in PowerShell) over several grow/shrink cycles and confirm it settles back down
  rather than trending upward.

Use the `julius-winfsp-ssh` skill to reach a real Windows machine for this check. Once confirmed
either way, update DESIGN-STORE-004's own "verified for Linux, not for Windows" wording in
`docs/design/byte-store.md` to reflect the actual, checked result - including what to do if a leak
is found (this todo does not attempt to design a fix in advance, since the actual failure mode, if
any, is not yet known).

## Done

**Completed**: 2026-09-03, by a Claude Code Desktop-App session already running directly on
`julius` (native Windows, real WinFSP) - the `julius-winfsp-ssh` skill was not needed, since it is
for reaching `julius` over SSH from elsewhere, not relevant when already running on the machine.

Set up a throwaway repository (`C:\dedupfs-perf-trial\handle-leak-check`, deleted afterward),
mounted it read-write, wrote 20 real files (~9 MB each, distinct content) through the mount, then
drove four sustained bursts of 16 concurrent native-OS-thread readers each (2,394 reads total
across ~61 s of cumulative concurrent load) against the mount.

Getting reliable sustained concurrent load working took most of the actual effort: PowerShell
background jobs (`Start-Job`) were dominated by per-job process-spawn overhead on this machine's
modest hardware (1 real read completed out of 16 jobs in 12 s); PowerShell runspace pools avoided
that but then hung under sustained (not just bursty) load at 7-8+ concurrent runspaces - a negative
control proved this was a client-side PowerShell/.NET runspace-pool issue, not a mount bug, by
reproducing the identical hang against a plain NTFS directory with no mount involved at all. Landed
on a small throwaway standalone Rust binary (`std::thread`/`std::fs` only, compiled directly with
`rustc`, not part of the workspace) as the load driver, which worked cleanly at 16 threads against
both the control directory and the mount.

Result: the mount process's total handle count (`Get-Process`'s `HandleCount`) started at 150,
rose once to 157 after the first burst, then stayed exactly flat at 157 through three more bursts
and a 10 s idle period - no upward trend across repeated grow/shrink cycles. `HandleCount` is a
coarse, all-kernel-handle-types metric (threads, sync objects, sections, files, ...), and no
finer-grained per-handle-type tool (Sysinternals `handle.exe`/Process Explorer) was available to
isolate the read cache's own file handles specifically - so this is a real, but imprecise, signal
against a leak, not a precise proof. Judged good enough to close the open question: a real leak
serious enough to matter in practice would very likely have produced a visible trend across four
bursts and an idle period, and none did.

Updated `docs/design/byte-store.md`'s DESIGN-STORE-004 "A shrinking thread pool does not leak
cached handles" section with the full result and its precision caveat, including a note that a more
precise, tool-assisted re-check remains open as future work if this ever becomes a live concern.
