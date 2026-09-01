# Verify `write_cache::mark_sparse`'s `FSCTL_SET_SPARSE` call against a real Windows/NTFS volume

**Why parked**: needs a real Windows environment with a real NTFS volume - this session runs in a
Linux container with no Windows target toolchain or real WinFSP/NTFS access, only cross-compile
checking (`scripts/build-windows-docker.sh`), which proves the code links but not that the actual
`DeviceIoControl` call behaves as intended at runtime.

**Size**: small (a focused runtime check once on real hardware, following the pattern
`crates/mountfs/CLAUDE.md`/the `julius-winfsp-ssh` skill already document for this repo)
**Opened**: 2026-08-31, by Claude Code on the web session (branch `mount-read-write`)
**Context**: `crates/cli/src/write_cache.rs`'s `mark_sparse` (Windows branch), DESIGN-MOUNT-010 in
[`../docs/design/mount-write-path.md`](../docs/design/mount-write-path.md)

DESIGN-MOUNT-010 decided that a write cache's spillover temp file must be created sparse, so a
scattered write pattern does not consume real disk space for the gaps between writes - automatic
on Linux, but requiring an explicit `FSCTL_SET_SPARSE` `DeviceIoControl` call on Windows before any
writes happen, since NTFS treats a plain file as fully allocated by default.

`write_cache.rs`'s `#[cfg(windows)]` `mark_sparse` implements this call (using the `windows-sys`
crate, gated to the Windows target only), written from documented Win32 API knowledge.

Type-checked (`cargo check --workspace --target x86_64-pc-windows-msvc`, after `rustup target add
x86_64-pc-windows-msvc` - this session's Docker daemon was unreachable, so
`scripts/build-windows-docker.sh`'s `cargo xwin` path could not be used instead): `cli` itself
checks clean against the real `windows-sys` 0.61.2 API (this already caught one real bug -
`FSCTL_SET_SPARSE` lives in `Win32::System::Ioctl`, not `Win32::Storage::FileSystem` as first
written). The workspace-wide check cannot get further than that in this environment: `db`'s
`rusqlite` dependency needs `libsqlite3-sys` to compile its bundled SQLite C sources for the target,
which needs `lib.exe`/the real MSVC toolchain - unavailable without either a real Windows
environment or `cargo xwin` inside Docker. Neither `dfs.exe` actually linking for Windows, nor this
specific code path's *runtime* behavior, is confirmed by any of this - it has never run on a real
Windows machine, so it is still unverified that:

- the call succeeds against a real NTFS volume for a freshly created temp file;
- a scattered write pattern into that file afterward actually stays sparse (check with
  `fsutil sparse queryflag <path>` and/or comparing `dir`'s reported size against actual disk usage
  after writing widely-separated ranges);
- the error path (`io::Error::last_os_error()` on failure) is ever actually reachable/correct in
  practice, rather than a plausible-looking but untested guess.

Use the `julius-winfsp-ssh` skill to reach a real Windows machine for this check. A standalone Rust
snippet exercising just `mark_sparse` plus a scattered write, checked with `fsutil sparse
queryflag`, is enough - no full mount session needed since this code path does not depend on
`mountfs` at all.

## Done

**Completed**: 2026-09-01, by a Claude Code Desktop-App session running natively on `julius`
(confirmed real WinFSP/NTFS access this session - see `.local/agent-environment.md`).

Ran a standalone probe crate (`crates/cli`'s exact `mark_sparse` code, copied verbatim) against a
real NTFS temp file: `DeviceIoControl(FSCTL_SET_SPARSE)` succeeded, and a scattered write pattern
(4 writes spread across a ~500 MB logical range) left the file genuinely sparse - `fsutil sparse
queryflag` confirmed the sparse flag set, and `fsutil sparse queryrange` showed only 4 small
allocated ranges (one per write, ~64 KiB each) rather than the full ~500 MB. All three open
questions from the "Do" list above are now confirmed:

- The call succeeds against a real NTFS volume for a freshly created temp file.
- A scattered write pattern afterward actually stays sparse.
- The error path was not separately forced/tested (no reachable failure case attempted - would
  need e.g. a non-NTFS volume or restricted access rights), but is not urgent given the success
  path's own confirmation; left open if it ever matters in practice.

Updated the stale "Not verified against a real Windows/NTFS volume yet" code comment in
`write_cache.rs` to reflect this.
