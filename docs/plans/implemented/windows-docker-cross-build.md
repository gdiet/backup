# Windows cross-build for `backup.exe` via Docker (using `cargo-xwin`)

**Status**: implemented (`docker/windows-cross/Dockerfile`, `scripts/build-windows-docker.sh`).
Verified end-to-end from this (Linux) side: the image builds, `scripts/build-windows-docker.sh`
produces a valid `backup.exe` (`file` confirms PE32+/x86-64), written to
`target/release-docker/` by default. Not yet verified: the actual `backup mount` smoke test on a
real Windows machine with WinFSP installed, comparing the Docker-built `.exe` against a natively
built one - still needed before treating Docker-built releases as equivalent to native ones.

## Context

Today `backup.exe` is only ever produced natively on a real Windows machine with Visual Studio
Build Tools installed (`AGENTS.md`, "Working Across Windows And WSL"). That works, but ties the
Windows build to that one machine. Goal now: a Docker image (usable from Linux — this WSL
checkout today, a CI runner later) that reproducibly builds `backup.exe` for Windows, without a
real Windows host or an installed Visual Studio.

The obvious first instinct — "Rust cross-compiling to Windows is standard, mingw-w64 is enough" —
undersells one project-specific wrinkle: `mountfs/src/windows/sys.rs` hand-binds WinFSP's
FUSE3-compatible API (no `windows-sys`/`winfsp-sys` bindings), including one already-found and
-fixed ABI bug (the `fuse_file_info` bitfield layout — see the comment there and
`docs/plans/implemented/05-cross-platform-mount-crate.md`, "Windows checkpoint" section). The only
Windows build ever verified so far was built with MSVC (Visual Studio Build Tools). To avoid
risking that already-validated ABI/toolchain behavior against an untested GNU/mingw combination,
this plan uses **`cargo-xwin`** (target `x86_64-pc-windows-msvc`, downloads the needed MSVC
CRT/SDK pieces itself, no real Visual Studio required) instead of mingw-w64 — chosen explicitly by
the user after being asked.

Sequencing: start with a local, ad-hoc setup (Dockerfile + wrapper script) run by hand as needed;
reuse the same image later for a CI/release pipeline (GitHub Actions or similar) — that's the
user's eventual target, but out of scope for this first pass.

## Why this should work (research findings)

- `mountfs/build.rs` links `libfuse3` only for `target_os == "linux"` via `pkg-config` — the
  Windows build skips that entirely, no Linux FUSE headers needed.
- The Windows backend (`mountfs/src/windows/sys.rs`) links **nothing** from WinFSP at build time:
  `fsp_fuse3_main_real`/`fsp_fuse3_get_context` are resolved at runtime via `LoadLibraryW`/
  `GetProcAddress` (with a registry fallback). The only Windows libraries linked at build time are
  `kernel32`/`advapi32` (`#[link(name = "kernel32")]` etc.) — a standard part of any Windows
  SDK/MSVC CRT environment, including the one `cargo-xwin` downloads.
- `mountfs/vendor/winfsp/` are reference headers only (never compiled) — no WinFSP SDK install
  needed at build time, locally or in the container.
- `db`'s `rusqlite` dependency with the `"bundled"` feature compiles SQLite from C sources via the
  `cc` crate — needs a real C cross-compiler for the target. `cargo-xwin` provides `clang-cl` for
  that (part of the LLVM/MSVC environment `xwin` downloads), a combination other projects already
  use successfully with `rusqlite bundled`.
- `Cargo.lock` already carries `windows-sys`/`windows-targets` as transitive dependencies (via
  `sysinfo`/`tempfile`) — both support the MSVC target as well as GNU, no blocker there.
- No existing `Dockerfile`/CI config in the repo — clean slate, nothing to reconcile.

## Known limitation (note in README, not a blocker)

A `backup.exe` cross-built in Docker (Linux) cannot be **run/tested** there — WinFSP itself is a
Windows kernel driver that doesn't exist under Linux/Wine. The Docker build therefore delivers a
compilable, linkable artifact (`cargo build`/`cargo check`, including `#[cfg(target_os =
"windows")]` code like `cli/src/mount.rs`/`cli/tests/windows_mount.rs`), but no substitute for the
existing manual smoke test on real Windows (README / plan doc 05, "Verification" section). That
manual step remains necessary before trusting a release build.

## Implementation

1. **Dockerfile** (`rust/docker/windows-cross/Dockerfile`):
   - Base image: official `rust:<pinned-version>-slim` (Debian-based, `clang`/`lld` packages
     available).
   - `rustup target add x86_64-pc-windows-msvc`.
   - `cargo install cargo-xwin`.
   - `clang`/`lld`/`llvm` packages via `apt` (needed by `cargo-xwin`/`clang-cl`).
   - Don't bake the `xwin` download into the image itself (license acceptance + cache size) —
     instead let `cargo xwin build` trigger it on first run, with the `xwin` cache directory
     mounted as a Docker volume so it isn't re-downloaded on every build.
   - `WORKDIR` on the mounted `rust/` checkout; mount the Cargo registry/target directory as
     volumes too, so incremental builds don't start from zero every run.
2. **Build command inside the container**:
   `cargo xwin build --release --target x86_64-pc-windows-msvc -p cli`
   (only the `cli` crate/the `backup` binary is needed for Windows, not the whole workspace —
   `cargo xwin check --workspace --target x86_64-pc-windows-msvc` separately as a sanity check
   for all crates).
3. **Wrapper script** (`rust/scripts/build-windows-docker.sh`): builds the image if needed, runs
   the container with the volume mounts above, copies the resulting
   `target/x86_64-pc-windows-msvc/release/backup.exe` to a given/default output location. Kept
   simple enough to lift directly into a future CI workflow step.
4. **Verifying this setup itself**:
   - `cargo xwin build --release --target x86_64-pc-windows-msvc -p cli` completes without errors
     inside the container and produces a `backup.exe` (check the PE header, e.g. `file
     backup.exe`, without executing it).
   - `cargo xwin check --workspace --target x86_64-pc-windows-msvc` (also covers `mountfs`,
     `cli/tests/windows_mount.rs` compile-only).
   - The actual functional smoke test (mount, `ls`/`cat`/`diff -r`) remains only possible on real
     Windows with WinFSP installed — not rebuilt here.
5. **Documentation**: a short paragraph in `rust/README.md`'s build section explaining there are
   now two ways to build `backup.exe` (native on Windows with VS Build Tools; via Docker/
   `cargo-xwin` from Linux) and what the Docker path does *not* cover (execution/mount testing).

## Not part of this first pass (future step, per user)

- A GitHub Actions workflow that builds this Docker image on release and attaches the `.exe` as
  an artifact — the wrapper-script approach in step 3 is deliberately shaped so it can be lifted
  directly into a workflow step later without reworking the Docker setup.

## Verification

- `./rust/scripts/build-windows-docker.sh` (or `docker build`/`docker run` directly) produces a
  `backup.exe` locally.
- `file backup.exe` reports a valid PE32+ executable for x86_64.
- User confirms the actual mount smoke test against the Docker-built `.exe` on their Windows
  machine before treating it as equivalent to the natively built one.
