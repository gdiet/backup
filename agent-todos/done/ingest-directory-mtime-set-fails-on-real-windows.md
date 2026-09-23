# `ingest`'s directory-mtime test fails with PermissionDenied on real Windows

**Why parked**: found while verifying an unrelated change (`dfs mount --spill-dir`) on real
Windows/WinFSP - out of scope for that task, and fixing Windows' directory-handle access-rights
correctly needs its own focused look, not a blind one-line change.
**Size**: small (a single function, `crates/cli/src/ingest.rs`'s test-only `filetime_set` helper -
though see below, the obvious-looking fix may not actually be correct on Windows).
**Opened**: 2026-09-04, during a `write-cache` branch session on `julius` (native Windows, real
WinFSP).
**Context**: `crates/cli/src/ingest.rs:613-633` (`set_mtime`/`filetime_set`).

`cargo test --workspace` on real native Windows fails one pre-existing test, unrelated to any
change made this session (confirmed via `git stash` - it fails identically against the unmodified
`write-cache` tip):

```
thread 'ingest::tests::try_run_imports_a_directory_recursively_preserving_structure_and_mtimes'
panicked at crates\cli\src\ingest.rs:624:34:
called `Result::unwrap()` on an `Err` value: Os { code: 5, kind: PermissionDenied, message: "Zugriff verweigert" }
```

`filetime_set`'s directory branch opens the path read-only (`fs::File::open(path)`) and then calls
`.set_modified(time)` on that handle - the file branch, one line below, already opens with
`.write(true)` instead and is not known to fail. `File::open`'s read-only handle plausibly lacks
whatever access right Windows' `SetFileTime` actually needs (`FILE_WRITE_ATTRIBUTES`, a narrower
right than generic write access) - but simply switching the directory branch to
`OpenOptions::new().write(true)` too may not be correct either: `CreateFileW` with generic
`GENERIC_WRITE` access commonly refuses to open a directory at all on Windows, which is presumably
why this branch was written to use `File::open` (read-only) in the first place rather than mirroring
the file branch directly. The actual fix likely needs the Windows-specific
`std::os::windows::fs::OpenOptionsExt::access_mode` extension trait to request exactly
`FILE_WRITE_ATTRIBUTES` (`0x100`) without full `GENERIC_WRITE`, gated `#[cfg(windows)]` - not
verified, needs someone to actually check on real Windows rather than reasoning about it further
here.

This test (and whatever code path in `ingest.rs`'s non-test code exercises directory-mtime setting
for real, if any - check whether `filetime_set` is only ever called from tests or also from
`ingest::run` itself) had apparently never been run against real Windows before this session, only
against Linux/WSL2 - same pattern as the earlier `crates/db/src/lock.rs` findings this branch's
predecessor session already fixed (see `agent-todos/done/verify-lock-file-delete-pending-on-real-windows.md`).
