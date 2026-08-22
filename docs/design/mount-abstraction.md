# Mount Abstraction (FUSE / WinFSP)

Filesystem-style access (REQ-MOUNT-001 in
[`../../requirements/functional/mount.md`](../../requirements/functional/mount.md)) is served
through a single trait, `mountfs::MountFilesystem` - a small, path-based, synchronous, POSIX-flavored
interface (`getattr`, `readdir`, `open`/`read`/`write`/`release`, and the usual structural
operations) - implemented once and served on Linux via real libfuse3 and on Windows via WinFSP,
behind the identical trait and dispatch logic.

## DESIGN-MOUNT-001: Why the high-level FUSE API, not the low-level protocol

Status: implemented

FUSE-family libraries offer two distinct kinds of API: a *high-level*, synchronous, path-based API
(`struct fuse_operations`, `fuse_main()`) and a *low-level*, asynchronous, inode-based protocol
binding directly to the kernel's `/dev/fuse` interface. WinFSP's own FUSE compatibility layer
(`cygfuse`) emulates specifically the high-level API - it has no equivalent for the low-level
protocol at all, since that protocol is Linux-kernel-specific by construction.

Binding directly against the high-level API therefore gives one shared implementation covering
both platforms for free: Linux links against real libfuse3's implementation of it, Windows against
WinFSP's emulation of the identical shape. A low-level-protocol binding would only ever cover
Linux, requiring an entirely separate implementation path for Windows regardless of what that path
turned out to be.

### Alternatives considered and rejected

#### Windows' built-in virtualization APIs (ProjFS / CfApi)

Both are architecturally a "hydrate to local disk on access, evict later" cache model, not a
pass-through streaming model - reading through a mount backed by either would materialize accessed
content onto the local volume the mount lives on.

Rejected: for a large deduplicated repository on external or otherwise size-constrained storage,
silently mirroring accessed content onto the system volume defeats the point of not storing it
there twice - especially during a full-tree read/verify pass, which touches most of the
repository's content at once.

#### Dokan

A third-party-driver alternative to WinFSP, MIT-licensed (cleaner than WinFSP's GPL-plus-exception
terms).

Rejected: not pursued once binding against WinFSP's FUSE-compatible API directly proved viable -
switching drivers would add a second Windows-specific integration path for a licensing preference
alone, with no functional benefit.

#### A native WinFSP backend (`FSP_FILE_SYSTEM_INTERFACE`)

WinFSP's own native, non-FUSE-compatible interface - a COM-style callback interface, distinct from
its FUSE-compatibility shim.

Rejected as the primary approach (though it remains the fallback if the shared FUSE-shaped
approach ever hits a real blocker on Windows): it would need its own hand-written bindings and its
own translation into `MountFilesystem`, on top of the Linux backend's separate implementation -
twice the platform-specific code for no capability the FUSE-compatible shim does not already
provide.

## DESIGN-MOUNT-002: Why path-based, not inode-based

Status: implemented

The high-level API this crate binds against is path-based, not inode-based, and WinFSP's own
compatibility layer is as well - an inode-numbering layer would have no natural backing on either
platform's actual API and would only add a translation cost without abstracting anything real.
[`Handle`](../../crates/mountfs/src/lib.rs), the opaque per-open-file token this crate's trait uses
instead, is filesystem-chosen and stored verbatim by both backends, matching that shape.

## DESIGN-MOUNT-003: Why runtime dynamic loading, not build-time linking

Status: implemented

Both backends resolve their platform library's exports at runtime (`dlopen`/`dlsym` on Linux,
`LoadLibraryW`/`GetProcAddress` on Windows, with a registry-key fallback matching WinFSP's own
installer-writer convention) rather than linking against an import library or requiring headers at
build time. This means neither backend needs a *build-time* system dependency - only a *runtime*
one, present only once a mount is actually attempted. A build-time dependency would require every
contributor and CI environment to have the relevant SDK installed just to compile the workspace,
for a feature only a subset of actual runs exercise.

Both backends also fail gracefully - a clean `io::Result::Err` with an actionable message, not a
panic - when the platform library turns out to be absent at runtime, and this is verified on both
platforms, not just documented: `scripts/test-linux-without-libfuse.sh` runs the
`preflight_check` example inside a container that never had libfuse3 installed; the equivalent
Windows check ran the same example, cross-built via `scripts/build-windows-docker.sh`, directly
against a real Windows install genuinely missing WinFSP (see `agent-todos/done/
test-graceful-absence-of-libfuse3-winfsp.md` for both results).

## DESIGN-MOUNT-004: Licensing

Status: implemented

libfuse itself is dual LGPL-2.1/GPL-2.0 (the userspace library ordinary programs link against is
the LGPL part); dynamically loading it from an MIT/Apache-2.0 project is the standard case that
license exists for. WinFSP is GPLv3, but ships an explicit FLOSS exception permitting exactly this
kind of dynamic linking without requiring this project to be GPLv3-licensed itself, conditional on
carrying a specific attribution notice - see [`../../NOTICE.md`](../../NOTICE.md) for the exception's
full text and this project's compliance notes. Writing hand-written bindings directly against
WinFSP's own published headers (vendored - copied into this repository unmodified, under
`crates/mountfs/vendor/winfsp/`, rather than fetched at build time - see that directory's own
`NOTICE.md`), rather than depending on a wrapper crate that bundles its own, different license
terms, keeps the licensing story limited to WinFSP's own terms alone.

## Known limitations

- No working in-process clean-shutdown call on Windows (`fuse_exit`/`fsp_fuse3_exit` crashes
  inside `winfsp-x64.dll`) - not a functional gap in practice: WinFSP already unmounts cleanly and
  returns from its own blocking call on Ctrl+C without this crate doing anything, mirroring real
  libfuse's own `SIGINT` handling on Linux (see `windows`'s own module doc comment).
- WinFSP's Windows backend never reports a no-replace rename request to this crate. A plain move
  without an explicit overwrite request (what Explorer or PowerShell's `Move-Item` do by default)
  asks the filesystem not to replace an existing target - on Linux, that request reaches this
  crate's `rename` dispatch as `no_replace = true` (`RENAME_NOREPLACE`, `renameat2(2)`'s flag).
  Confirmed against real WinFSP (`crates/mountfs/tests/rename_noreplace.rs`) that Windows never surfaces
  this: WinFSP rejects a colliding no-replace move itself, before ever calling into this crate, so
  `rename` sees `no_replace = false` on every call that does reach it. The caller-visible result is
  the same either way - a colliding move is rejected on both platforms - but an implementation
  cannot itself observe or react to a no-replace request on Windows.

Revisit if: real-world use ever needs a mount mechanism outside the FUSE/WinFSP family (e.g. Samba
or WebDAV, both left open by REQ-MOUNT-001) - that would not fit this trait's current shape without
its own adapter, since the FUSE/WinFSP convergence this design leans on is specific to that family.

## Verifying the Windows backend from Linux

`scripts/build-windows-docker.sh` cross-compiles the workspace for `x86_64-pc-windows-msvc` via
Docker and `cargo-xwin` (no Visual Studio install needed - `cargo-xwin` downloads the MSVC
CRT/SDK pieces itself on first run). MSVC specifically, not mingw-w64: the Windows backend
hand-binds WinFSP's C API with `#[repr(C)]` structs (`windows/sys.rs`), and only an MSVC build has
ever been verified against real WinFSP - matching that toolchain avoids introducing a second,
untested ABI combination alongside it.

This proves the Windows backend compiles and links (`cargo xwin check --workspace`, then
`cargo xwin build -p mountfs`, which also produces a real, runnable
`windows_mount_spike_helper.exe`) - a genuine, automatable subset of the verification story. It
does **not** exercise real WinFSP behavior: WinFSP is a Windows kernel driver, unavailable under
Linux/Wine, so mount/unmount/read/write behavior still needs a real Windows machine with WinFSP
installed (see the `julius-winfsp-ssh` skill) - conveniently, the `.exe` this script produces is
exactly the artifact to copy over and run there.

## Updating the hand-written struct bindings

Neither backend generates its C struct bindings (`linux/sys.rs`, `windows/sys.rs`) automatically -
no `bindgen`, no build-time codegen. Each `#[repr(C)]` type is hand-transcribed once, field by
field, from the authoritative C definition (real system libfuse3 headers on Linux; the vendored,
unmodified copies under `crates/mountfs/vendor/winfsp/` on Windows, consulted purely as a struct-layout
reference and never compiled). This already found one real bug: WinFSP's `fuse3_file_info`
bitfield group is 33 bits total, which MSVC packs into two 4-byte storage units, not one - getting
that wrong silently shifted a later field's offset (see `windows/sys.rs`'s `fuse_file_info` doc
comment).

Redo this whenever targeting a WinFSP version newer than the one vendored (currently v2.0, see
`crates/mountfs/vendor/winfsp/NOTICE.md` for the exact tag/commit) - do not assume a newer version's
struct layouts are unchanged just because the API surface looks the same:

1. Fetch the new version's headers from WinFSP's own repository and diff them against the vendored
   copies (`fuse/`, `fuse3/`) to find every layout-relevant change - new/removed/reordered fields,
   changed field types, new or changed bitfield groups.
2. For each affected `#[repr(C)]` struct in `windows/sys.rs`, re-verify it field by field against
   the new header - do not assume "the field count looks right" is enough, given the bitfield
   packing gotcha above. Bitfields specifically need working out by hand every time, since Rust has
   no native bitfield syntax and `#[repr(C)]` does not replicate C's packing rules on its own.
3. Replace the vendored header copies with the new version and update
   `crates/mountfs/vendor/winfsp/NOTICE.md`'s referenced tag/commit.
4. Re-run the Docker cross-build check (see above), then verify against real WinFSP on real
   hardware before trusting the update - a struct layout mismatch is exactly the class of bug a
   compile-only check cannot catch.

The Linux side has no vendored copy (real system libfuse3 headers are consulted directly when
writing/updating the bindings), but the same field-by-field re-verification discipline applies if
targeting a libfuse3 version where `struct fuse_operations`'s layout has changed.
