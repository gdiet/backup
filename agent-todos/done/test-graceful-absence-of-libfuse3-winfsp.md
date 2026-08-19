# Test that mountfs handles a missing libfuse3/WinFSP gracefully, not just the found case

**Needs**: either a genuinely isolated environment/container without libfuse3 installed (Linux
side), or a decision to refactor `exports()` for dependency injection instead - see the two
options below.
**Size**: medium (confirm approach with the user before starting)
**Opened**: 2026-08-18, by Desktop App session (surfaced while reviewing `mountfs` for adoption
into a sibling project).

`linux::sys`/`windows::sys`'s `exports()` documents that the platform mount library is a
runtime-only dependency, resolved via `dlopen`/`LoadLibraryW`, and reports absence as a clean
`io::Result::Err` rather than panicking. No test actually exercises that path - every existing
mount test requires the real library to be present and working.

Two ways to close this:

1. Run the check in a child process where the library is genuinely unresolvable by the dynamic
   linker - on Linux this needs more than clearing `LD_LIBRARY_PATH`, since `libfuse3.so.3` loads
   via the standard system search path/`ld.so.cache`; a container or mount-namespace trick that
   actually hides the `.so` from that process would be needed. Confirms the real, unmodified
   production code path end to end, at the cost of a more involved test setup.
2. Refactor `exports()` (or the `dlopen`/`LoadLibraryW` call it wraps) behind a small seam a test
   can inject a resolution failure into, without needing the library to be genuinely absent from
   the system running the test. Simpler and more reliable to run anywhere (including CI), at the
   cost of a production-code change purely for testability.

Not urgent (the documented behavior is a reasoned expectation from reading the code, not a known
bug) - but worth closing before relying on "mounting fails gracefully when the library is missing"
as a tested guarantee rather than an assumption.

## Done

**Completed**: 2026-08-19, by Desktop App session (WSL side), with the Windows-side run relayed
back by a native Windows session and the developer.

Closed option 1 (a genuinely isolated environment) on both platforms, rather than the
dependency-injection refactor:

- **Linux**: added `scripts/test-linux-without-libfuse.sh`, mirroring the equivalent script already
  proven in the sibling `rust2` project - builds `mountfs/examples/preflight_check` here (nothing
  links against libfuse3 at build time, only `dlopen` at runtime), then runs it inside a
  `debian:bookworm-slim` container that never had libfuse3 installed. Output:
  `preflight: library not available (libfuse3 not found (libfuse3.so.3: cannot open shared object
  file: No such file or directory) - install it (Debian/Ubuntu: apt install libfuse3-3; Fedora:
  usually already present as fuse3))` - `PASS: reported absence gracefully, no crash`.
- **Windows**: added `mountfs/examples/preflight_check.rs` (did not previously exist in this crate,
  unlike `rust2/mountfs`), cross-built it for `x86_64-pc-windows-msvc` via
  `scripts/build-windows-docker.sh`'s Docker image, and ran it from native Windows PowerShell on
  this same WSL host's own Windows side (confirmed genuinely missing WinFSP), reached via a
  `\\wsl.localhost\...` UNC path straight to the WSL-built `.exe` - no Rust toolchain needed on the
  Windows side at all, working around that host having no `cargo`/`rustc` installed. Output:
  `preflight: library not available (WinFSP not found (winfsp-x64.dll) - install it from
  https://github.com/winfsp/winfsp)`, exit code `0`.

Confirms `exports()`'s documented graceful-absence behavior end to end, on the real, unmodified
production code path, on both platforms - no dependency-injection seam needed.
