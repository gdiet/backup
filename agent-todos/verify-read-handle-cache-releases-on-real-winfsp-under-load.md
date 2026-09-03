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
