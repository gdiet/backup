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

## Done

**Completed**: 2026-09-23, by a Claude Code Desktop-App session on `julius` (native Windows, real
WinFSP), after a short web research pass into how this is normally solved on Windows in Rust.

The `access_mode`/`FILE_WRITE_ATTRIBUTES` guess above turned out not to be the actual mechanism -
real research found the correct, well-established one: `CreateFileW` (what `File::open`/
`OpenOptions` call into) refuses to open a directory with write access **at all**, regardless of
which specific access right is requested, unless the caller also passes `FILE_FLAG_BACKUP_SEMANTICS`
- Microsoft's own documented way to ["obtain a handle to a
directory"](https://learn.microsoft.com/en-us/windows/win32/fileio/obtaining-a-handle-to-a-directory).
Confirmed this is the standard approach by reading the widely-used
[`filetime`](https://docs.rs/filetime) crate's actual Windows implementation, which opens with
plain `.write(true)` (the *same* access the file branch already used) plus
`.custom_flags(FILE_FLAG_BACKUP_SEMANTICS)` via `std::os::windows::fs::OpenOptionsExt` - no special
narrower access right needed after all.

Implemented as a new `directory_handle` helper in `crates/cli/src/ingest.rs`, `#[cfg(windows)]`
using that exact approach and `#[cfg(not(windows))]` keeping the original read-only open (Unix's
`futimens` accepts an `O_RDONLY` fd for this). `filetime_set`'s directory branch now calls it
instead of opening the path itself.

Verified directly: the previously-failing test now passes on real Windows
(`try_run_imports_a_directory_recursively_preserving_structure_and_mtimes`), and the full
workspace test suite passes on WSL2/Debian too (confirming the Unix branch is unaffected).

Along the way, found and fixed a second, unrelated pre-existing bug blocking `cargo test` from even
*compiling* on Windows at all: `crates/cli/src/dedup_fs.rs`'s
`real_mount_concurrent_handles_converge_toward_the_per_handle_halving_formula` used
`std::os::unix::fs::OpenOptionsExt`/`libc::O_SYNC` directly with no platform gate - fixed by adding
`#[cfg(target_os = "linux")]`, matching how other Linux-only `real_mount_` tests in this codebase
are already gated. A third, separate real-Windows test failure
(`real_mount_write_backpressure_delay_grows_then_drains_with_the_persist_queue`, a genuine runtime
failure once compilation was unblocked, not a compile error) was found but not fixed - filed
separately as `agent-todos/real-mount-backpressure-test-fails-on-real-windows.md`, since it needs
actual investigation rather than a quick fix.
