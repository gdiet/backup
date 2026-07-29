# A platform-abstracted mount crate (Linux + Windows/WinFSP)

**Status**: implementation starting on branch `mountfs` (branched off `rust`), since it's a real architectural change (new crate, dropping or reworking the `fuser` dependency) rather than an incremental addition.

## Context

`backup mount` currently works read-only on Linux only (`cli/src/mount.rs`, `docs/plans/implemented/04-fuse-mount-readonly.md`), implemented directly against the `fuser` crate's `Filesystem` trait. `fuser` talks to Linux's `/dev/fuse` kernel protocol directly and has no Windows story at all (confirmed in an earlier conversation: it's categorized `os::unix-apis`, depends unconditionally on the Unix-only `nix` crate, and even its `libfuse`/`libfuse2`/`libfuse3` feature flags only link against libfuse's *low-level*, session-based C API - which WinFSP's FUSE compatibility layer does not implement).

The goal now: a dedicated crate that owns all of this platform-specific mounting logic, exposing a small, platform-independent interface to the rest of the tool (`cli`, which currently is the only thing that would depend on it) - so `db`, `store`, `cdc`, and the rest of `cli` never need to know or care whether they're running under Linux/FUSE or Windows/WinFSP. Read-only first (matching what's already implemented), with a clear extension point for the read-write design already sketched in `docs/plans/fuse-mount-readwrite.md`.

Development happens at different times under WSL2/Ubuntu and under native Windows with the Visual Studio Build Tools; only the Linux leg can actually be built and tested from this environment. Windows verification requires a real Windows machine with a current WinFSP installed, and is explicitly out of scope for what can be checked while writing/executing this plan from here - see "Verification" below.

## Research: what actually connects libfuse, WinFSP, and the Scala tool

This is the load-bearing research behind the recommendation below - worth recording since it directly contradicts the intuitive first guess ("just use `fuser`'s `libfuse` feature").

- **libfuse has two distinct C APIs**: a *high-level*, synchronous, path-based API (`fuse.h`/`fuse3/fuse.h`, `struct fuse_operations`, `fuse_main()`) and a *low-level*, asynchronous, inode-based API (`fuse_lowlevel.h`, `fuse_session_new()`, `struct fuse_lowlevel_ops`). `fuser`'s own native Linux implementation, and its `libfuse`/`libfuse2`/`libfuse3` feature flags, all target the **low-level** API.
- **The Scala tool (via jnr-fuse) uses the high-level API**: `Server.scala extends FuseStubFS` overrides exactly `getattr`, `mkdir`, `readdir`, `rename`, `rmdir`, `statfs`, `utimens`, `chmod`, `chown`, `create`, `open`, `truncate`, `write`, `read`, `release`, `unlink` - all path-based, all fields of `struct fuse_operations`. jnr-fuse calls the native entry point `fuse_main_real(argc, argv, FuseOperations op, op_size, user_data)` - the high-level API's classic mount call.
- **WinFSP's FUSE compatibility layer (`cygfuse`) only emulates the high-level API** - confirmed via WinFSP's own docs and wiki: "WinFSP provides a FUSE (high-level) API... supports both the FUSE 2.8 and FUSE 3.2 APIs." This is *exactly* the API surface the Scala tool already uses, which is why jnr-fuse works unmodified on Windows via WinFSP (with an `fsp_` symbol prefix under the hood - see below) but a `fuser`-based tool structurally cannot.
- **WinFSP's `inc/fuse3/fuse.h` is header-only and delay-loads WinFSP at runtime**: it `#define`s/inlines the standard names (`fuse_main`, `fuse_operations`, ...) as thin wrappers around `fsp_fuse_*`-prefixed calls (`FSP_FUSE_SYM` macro), which resolve the actual `winfsp-x64.dll` dynamically via `fsp_fuse_env()` at runtime, not at link time. Practically: **no import library is needed to build against this header**, and the built binary doesn't hard-require WinFSP to be installed at *build* time, only at *run* time (matching how the Scala/jnr-fuse tool already behaves, and how its own README already documents installing WinFSP as an end-user runtime prerequisite).
- **Licensing**: libfuse itself is dual LGPL-2.1/GPL-2.0 (the userspace library used by normal programs is the LGPL part) - dynamically linking against it from an MIT project is the standard, uncontroversial case LGPL exists for. WinFSP is GPLv3, but ships an explicit FLOSS exception (`License.txt`, quoted in full since it's the crux of this plan being viable at all):

  > As a special exception to GPLv3, Bill Zissimopoulos grants additional permissions to Free/Libre and Open Source Software ("FLOSS") without requiring that such software is covered by the GPLv3.
  > 1. Permission to link with a platform specific version of the WinFsp DLL (one of: winfsp-a64.dll, winfsp-x64.dll, winfsp-x86.dll, winfsp-msil.dll).
  > ... provided that the software: 1. Is distributed under a license that satisfies the [FSD]/[OSD]. 2. Includes the copyright notice "WinFsp - Windows File System Proxy, Copyright (C) Bill Zissimopoulos" and a link to the WinFsp repository in its user-interface and any user-facing documentation. 3. Is not linked or distributed with proprietary (non-FLOSS) software.

  This project is MIT (satisfies both definitions referenced), so the exception applies cleanly - **provided** the required attribution notice is added somewhere user-facing (README, `--version`/`about` output, or similar) once a Windows backend actually links against WinFSP. This is materially better than the `winfsp`/`winfsp-sys` Rust crates, which are flatly licensed GPL-3.0 by their maintainer (their own choice of wrapper license, not a restriction WinFSP itself imposes) - using those directly would very likely require this project's combined binary to comply with GPL-3.0, which the user has ruled out. Writing our own bindings directly against WinFSP's headers, under this project's own MIT license, sidesteps that entirely.
- **A small existing crate, `libfuse-sys`** (bindgen-generated bindings to the *high-level* `fuse_operations`/`fuse_main`, MIT wrapper around dynamically-linked LGPL-2.1 libfuse), confirms the high-level API is realistically bindable from Rust - but it's tiny (4 GitHub stars), Linux/macOS-only in its `build.rs` (pkg-config, no Windows branch), and has no evidence anyone has used it against WinFSP. Not proposed as a direct dependency (see "Approach" below), but useful validation that this general shape of binding works.

## Approach: one shared implementation against the high-level libfuse API, hand-bound

**Primary recommendation**: write our own small, hand-written (not bindgen-generated) `unsafe extern "C"` bindings to the high-level API - `struct fuse_operations`, `fuse_main_real`/`fuse_main`, `fuse_get_context`, `fuse_exit` - and exactly one Rust-side implementation of the operation-dispatch logic, built against:
- real system libfuse3 on Linux (via `pkg-config`, same as `libfuse-sys` does - requires `libfuse3-dev`/`fuse3-devel` installed at build time, `libfuse3` at runtime),
- WinFSP's own `fuse3/fuse.h` (vendored - a handful of small, unmodified C header files checked into this crate, sourced from the WinFSP repository, with the attribution notice above and a link to WinFSP's exact source commit/version they were copied from) on Windows, no import library, no build-time WinFSP install required.

Why this over `fuser` on Linux too, given `fuser` already works well there: the entire point of this crate is *one* implementation, not two - if the high-level API can serve both platforms (which the research above says it can), maintaining a second, purely-Linux, `fuser`-based implementation forever just for symmetry has no upside. The real trade-off worth naming honestly: this drops `fuser`'s major practical advantage on Linux - no system package dependency, since `fuser` talks to the kernel directly and needs nothing beyond the kernel FUSE module. Moving to the high-level C API means `libfuse3-dev` becomes a new **build-time** system dependency on Linux (and `libfuse3`/equivalent a runtime one) that doesn't exist today. Worth stating in the README once this lands.

**Concrete API surface needed** (matching what Scala/jnr-fuse already uses, restricted to what the read-only phase needs first): `getattr`, `readdir`, `open`, `read`, `release`, `statfs`. The remaining Scala-used operations (`mkdir`, `rename`, `rmdir`, `utimens`, `chmod`, `chown`, `create`, `truncate`, `write`, `unlink`) are exactly the read-write phase's callback list from `docs/plans/fuse-mount-readwrite.md` §5 - the same `struct fuse_operations` fields, just left as unimplemented/`ENOSYS` function pointers until that phase is taken up. No redesign needed to add them later.

### Fallback, if the shared approach hits a real blocker

The user has explicitly accepted this as a fine (if less clean) outcome, so it's recorded concretely rather than hand-waved: if implementing/testing on a real Windows box (see "Verification") surfaces a genuine blocker in the shared high-level-API approach (an ABI mismatch between what our bindings assume and what WinFSP's `fuse.h` actually provides, a `struct fuse_operations` layout difference between libfuse's and WinFSP's headers that bindgen/hand-binding can't paper over, or something else not visible from research alone) -

- **Linux keeps the existing `fuser`-based implementation** (already working, already tested, no reason to touch it if the unification isn't viable).
- **Windows gets its own backend**, still inside the same crate, still MIT, still under the WinFSP FLOSS exception - but talking to WinFSP's *native* API (`FSP_FILE_SYSTEM_INTERFACE`, the COM-style interface `winfsp.h` exposes, which is what the `winfsp`/`winfsp-sys` crates wrap) instead of the FUSE-compatibility shim. This needs our own minimal safe bindings to that native header too (same licensing reasoning as above - our own MIT bindings against WinFSP's headers, not the GPL-3.0 `winfsp-sys` crate), and a hand-written translation from WinFSP's native callback shape to this crate's shared `MountFilesystem` trait (below) - more code than the shared-implementation path, but each half (Linux/`fuser`, Windows/native-WinFSP) is well-trodden ground individually.

Either way, the crate boundary and the trait `cli` programs against (next section) stays the same - the fallback is purely an internal implementation-swap, invisible outside this crate.

## Crate design

New workspace member, suggested name `mountfs` (bikeshed-able) - `mountfs/src/lib.rs` plus platform modules.

### Public interface

A path-based trait (deliberately *not* inode-based, unlike `fuser::Filesystem` - the high-level libfuse API is path-based, WinFSP's native API is also predominantly path-based, and `db`'s own query functions are already path/id-based via `db::resolve_path` - an inode-numbering layer has no natural home on either backend and was only ever an artifact of `fuser`'s specific low-level design):

```rust
pub trait MountFilesystem: Send + Sync + 'static {
    fn getattr(&self, path: &str) -> Result<Attr, Errno>;
    fn readdir(&self, path: &str) -> Result<Vec<DirEntry>, Errno>;
    fn open(&self, path: &str, write_intent: bool) -> Result<Handle, Errno>;
    fn read(&self, handle: Handle, offset: u64, size: u32) -> Result<Vec<u8>, Errno>;
    fn release(&self, handle: Handle);
    fn statfs(&self) -> Result<StatfsInfo, Errno>;
    // Phase 2 (read-write) extends this trait with write/create/mkdir/unlink/
    // rmdir/rename/truncate/utimens/chmod/chown - not added until that phase.
}

pub fn mount(fs: impl MountFilesystem, mountpoint: &Path, read_only: bool) -> io::Result<()>;
```

`Attr`/`DirEntry`/`Handle`/`StatfsInfo`/`Errno` are this crate's own small, platform-independent types (not re-exports of `fuser` or any WinFSP-specific type) - `cli` builds them from `db::TreeEntryRow`/`db::file_size`/etc. without ever importing a platform-specific crate.

### What moves where

- `cli/src/mount.rs`'s current `DedupFs` (the `db`/`store`-backed logic: path resolution, chunk reading via `cli::chunk_store::read_chunk_bytes`, attribute construction) stays in `cli`, but implements `mountfs::MountFilesystem` instead of `fuser::Filesystem`, and calls `mountfs::mount(...)` instead of `fuser::mount(...)`.
- Everything platform-specific - the `fuser` dependency (or its replacement), inode-number bookkeeping, the FFI bindings, WinFSP header vendoring, build.rs platform detection - moves into the new `mountfs` crate. `cli`'s `Cargo.toml` drops its direct `fuser` dependency entirely once this lands.
- `db`, `store`, `cdc` are untouched - they were already platform-independent.

## Sequencing

1. **Spike, Linux only**: hand-write the FFI bindings and a minimal `mountfs` implementing just `getattr`+`readdir` against real libfuse3 (pkg-config), prove the shared-API concept end-to-end on the one platform this environment can actually test, without touching the existing `fuser`-based `cli/src/mount.rs` yet.
2. Flesh out `mountfs`'s Linux backend to the full read-only set (`open`/`read`/`release`/`statfs`), get it passing the same kind of real mount/unmount integration test `cli/src/mount.rs` already has.
3. Switch `cli` over to `mountfs` (`DedupFs` implements `MountFilesystem`, drop the `fuser`-based code and dependency). Full regression: existing manual smoke test (mount, `ls`/`cat`/`stat`/`diff`, unmount) plus the automated suite.
4. **On a real Windows machine** (Visual Studio Build Tools, current WinFSP installed): vendor WinFSP's `fuse3/fuse.h` headers, wire up `build.rs`'s Windows branch, build and manually smoke-test `backup mount` there. This is the actual go/no-go checkpoint for the shared-implementation approach.
5. If step 4 succeeds: done, add the WinFSP attribution notice (README/`--version`), document the new `libfuse3-dev` Linux build dependency, write a Windows section in the README (WinFSP install prerequisite, matching the Scala README's existing Windows section).
6. If step 4 blocks: implement the fallback Windows-native backend described above instead, same trait, same public interface - everything from step 3 onward on the `cli` side is unaffected either way.

## Not yet decided

- Exact crate name (`mountfs` is a placeholder).
- Whether WinFSP header vendoring lives directly in `mountfs/vendor/` or a separate `mountfs-winfsp-sys`-style internal crate - depends on how much Windows-specific code ends up existing once step 4/6 above is actually attempted.
- How `write_intent`/permission handling should be modeled in the trait once read-write is added - deferred to when `docs/plans/fuse-mount-readwrite.md` is actually implemented on top of this crate.
- CI: this environment can only ever validate the Linux leg. Whether/how to set up a Windows CI runner (GitHub Actions has Windows runners; WinFSP would need installing there too) is out of scope for this plan.

## Verification

- Linux: `cargo fmt --check && cargo clippy --all-targets -- -D warnings && cargo test` workspace-wide, plus a real mount/unmount integration test (as `cli/src/mount.rs` already has) and the same manual smoke test procedure (mount a real repository, `ls`/`cat`/`stat`/`diff` against the source, unmount) run against `mountfs` before and after the `cli` cutover in step 3, to confirm no behavioral regression.
- Windows: **cannot be done from this environment.** Requires an actual Windows machine with the Visual Studio Build Tools and a current WinFSP release installed - budget for this explicitly when picking up step 4/6, rather than assuming it'll just work because the Linux leg does.
