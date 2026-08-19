# Test that WinFSP-absence is handled gracefully on Windows (Linux side already done)

**Needs**: a real, human-opened Windows/WSL terminal - the machine itself is already confirmed to
qualify (see below), what is missing is a session with working WSL2 interop, which an
agent-spawned shell does not have here.
**Size**: small (the binary is already built; this is a five-minute manual run)
**Opened**: 2026-08-18, by Desktop App session.
**Context**: `docs/design/mount-abstraction.md`'s "Why runtime dynamic loading, not build-time
linking" tracks this too.

`windows::sys`'s `exports()` documents that WinFSP is a runtime-only dependency, resolved via
`LoadLibraryW`, and reports absence as a clean `io::Result::Err` rather than panicking. Not yet
verified against a real Windows machine that genuinely lacks it.

The Linux equivalent is already done: `scripts/test-linux-without-libfuse.sh` builds the
`preflight_check` example here, then runs it inside a Docker container that never had libfuse3
installed, and confirms the graceful-absence message rather than a crash.

For Windows: this machine's own Windows host (reached from WSL2 via `/mnt/c/`) was checked and
genuinely has no WinFSP installed (`Program Files (x86)/WinFsp`, `Program Files/WinFsp`, and any
`winfsp*.dll` under `System32` are all absent) - exactly the environment this test needs, no
separate machine required. `mountfs/examples/preflight_check.rs` was already cross-built for it
(`target/x86_64-pc-windows-msvc/release/examples/preflight_check.exe`, via the existing
`scripts/build-windows-docker.sh` machinery).

What is missing is running it: from *this* agent's own shell, invoking any Windows `.exe` via
`/mnt/c/...` fails with "Exec format error" - `/proc/sys/fs/binfmt_misc/` has no `WSLInterop`
registration in this session, only the two control files (`register`/`status`), even though
`cmd.exe`/`powershell.exe` resolve fine via `PATH`. This matches the same class of limitation the
`julius-winfsp-ssh` skill already documents for the Ctrl+C test: an agent-harness-spawned shell
lacks something a real, human-opened terminal has (there, an attached console; here, working WSL
interop) - confirmed the same way, by trying it directly rather than assuming.

To finish this from a real terminal (WSL or plain Windows, opened by a human, not spawned by an
agent): run
`target/x86_64-pc-windows-msvc/release/examples/preflight_check.exe` (rebuild first via
`scripts/build-windows-docker.sh` if the `target/` directory was cleaned since) and confirm it
prints a "library not available" message rather than crashing.

Not urgent (the documented behavior is a reasoned expectation from reading the code, not a known
bug).

## Done

**Completed**: 2026-08-19, by Windows desktop-app session (native PowerShell on the same host this
TODO was opened from, no WSL interop involved).

Ran `preflight_check.exe` directly from Windows PowerShell via the `\\wsl.localhost\u24\...` UNC
path to the WSL-side build output (bypassing the WSL-interop binfmt_misc problem entirely, since
PowerShell executes native Windows binaries directly). Output:

```
preflight: library not available (WinFSP not found (winfsp-x64.dll) - install it from https://github.com/winfsp/winfsp)
```

Exit code 0. Confirms `windows::sys::exports()` handles WinFSP absence gracefully with a clean,
actionable error message rather than crashing/panicking, matching the already-verified Linux
behavior in `scripts/test-linux-without-libfuse.sh`.
