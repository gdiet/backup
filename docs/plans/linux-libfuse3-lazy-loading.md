# Make `libfuse3` a runtime-only dependency on Linux (dlopen, like WinFSP on Windows)

## Context

Investigating an unrelated question ("can WSL run a cross-built `backup.exe`?") surfaced a real
asymmetry between the two mount backends. `mountfs/build.rs` links `libfuse3` unconditionally at
*build* time on Linux, via `pkg_config::Config::new().probe("fuse3")`, which makes
`libfuse3.so.3` a hard `NEEDED` entry in the resulting binary's dynamic section (confirmed via
`ldd`). Practical effect, confirmed by actually running the binary in a `debian:bookworm-slim`
container without `libfuse3` installed:

```
error while loading shared libraries: libfuse3.so.3: cannot open shared object file: No such file or directory
```

This fails at the OS loader level, before `main()` runs - so it's not just `mount` that's
unusable without `libfuse3`, it's the *entire* `backup` binary, including `--help`, `init`,
`store`, `restore`, everything.

Windows never had this problem, by design: `mountfs/src/windows/sys.rs` resolves WinFSP's DLL
exports lazily via `LoadLibraryW`/`GetProcAddress` on first actual use, so every subcommand except
`mount` works regardless of whether WinFSP is installed. A companion fix (see
`docs/plans/implemented/` - the "report missing WinFSP as a clean error instead of panicking"
commit) just made that lazy path itself fail cleanly instead of panicking, and added
`mountfs::preflight()` so `cli` can check availability *before* announcing a mount as started.

This plan brings Linux to the same model: `libfuse3` becomes a pure runtime dependency, loaded via
`dlopen`/`dlsym` only when `mount` actually runs, exactly mirroring the Windows backend's already
-working (and now cleanly-erroring) pattern. Two alternatives were considered - this one (single
binary, lazy load) and shipping two binaries (one with `mount`, one without, via a Cargo feature
cutting out `mount`/`mount_deleted`/the `mountfs` dependency). The two-binary route was rejected:
it doesn't fix the actual problem, it only offers an escape hatch via an extra release artifact -
the `mount`-capable binary would *still* refuse to even print `--help` without `libfuse3`
installed. The dlopen approach fixes the root cause and, as a bonus, reuses infrastructure
(`preflight()`, the lazy-`OnceLock<Result<Exports, String>>` pattern) that already exists for
Windows.

## Why this is tractable

- **Only two symbols needed at runtime**: `fuse_main_real` and `fuse_get_context` - confirmed by
  reading `mountfs/src/linux/sys.rs`'s current `unsafe extern "C" { ... }` block, which declares
  exactly these two and nothing else. Same shape as Windows's `Exports { main_real, get_context }`
  in `mountfs/src/windows/sys.rs`.
- **`libc` already provides `dlopen`/`dlsym`/`dlerror`** (`mountfs` already depends on `libc` for
  the rest of the Linux backend's POSIX types) - no new dependency needed, unlike Windows, which
  had to hand-write `extern "system"` blocks for `LoadLibraryW`/`GetProcAddress` since those
  aren't POSIX. `dlopen`/`dlsym` have been part of glibc/musl's always-linked C runtime since
  glibc 2.34 (2021) - no separate `-ldl` linker flag needed on any Rust-supported Linux target
  Cargo would produce today.
- **No registry-style fallback needed**: unlike WinFSP (which doesn't reliably end up on `PATH`,
  hence the `HKLM\...\WinFsp\InstallDir` registry lookup in `load_winfsp_dll`), `libfuse3.so.3` is
  a standard soname any Linux package manager installs into the normal dynamic linker search path
  (confirmed via `ldd` above: resolved from `/lib/x86_64-linux-gnu/`). A plain
  `dlopen("libfuse3.so.3", RTLD_NOW)` is enough.
- **Struct-layout risk is unchanged in kind, lower in practice**: this doesn't newly introduce
  "hand-written FFI structs instead of compiler-checked ones" - `mountfs/src/linux/sys.rs`'s
  `fuse_operations`/`fuse_file_info`/etc. are already hand-written today (not bindgen-generated),
  same as Windows's. What changes is that the *function pointer types* (`fuse_main_real`,
  `fuse_get_context`) move from build-time-linked `extern "C" { fn ... }` declarations to
  runtime-resolved fields on an `Exports` struct - the struct layouts these calls exchange data
  through are untouched. Real libfuse3's C ABI is also considerably more stable/portable across
  versions and distros than WinFSP's Windows-specific quirks (recall the `fuse_file_info` bitfield
  bug documented in `windows/sys.rs`) - lower incremental risk than the Windows work already done.
- **`cli` needs zero changes**: `cli/src/mount.rs:215` already calls `mountfs::preflight()` before
  printing the "mounted ..." line (added by the WinFSP fix). Linux's `preflight()` is currently a
  hardcoded `Ok(())` stub in `mountfs/src/lib.rs`; once it delegates to a real check, the exact
  same `cli` code path starts working correctly on Linux too, for free.

## Implementation

### 1. `mountfs/src/linux/sys.rs` - convert to lazy dlopen

Replace the current build-time-linked block:

```rust
unsafe extern "C" {
    pub fn fuse_main_real(argc: c_int, argv: *mut *mut c_char, op: *const fuse_operations, op_size: size_t, private_data: *mut c_void) -> c_int;
    pub fn fuse_get_context() -> *mut fuse_context;
}
```

with a Windows-`sys.rs`-mirroring lazy resolver (adapt names/messages, no registry fallback
needed):

```rust
use std::ffi::CStr;
use std::io;
use std::sync::OnceLock;

type FuseMainReal = unsafe extern "C" fn(
    c_int, *mut *mut c_char, *const fuse_operations, size_t, *mut c_void,
) -> c_int;
type FuseGetContext = unsafe extern "C" fn() -> *mut fuse_context;

struct Exports { main_real: FuseMainReal, get_context: FuseGetContext }
// SAFETY: same rationale as windows::sys::Exports - plain code pointers
// into a library that stays loaded and unchanged for the process's life.
unsafe impl Send for Exports {}
unsafe impl Sync for Exports {}

unsafe fn resolve<T: Copy>(handle: *mut c_void, name: &CStr) -> Option<T> {
    let addr = unsafe { libc::dlsym(handle, name.as_ptr()) };
    if addr.is_null() { None } else { Some(unsafe { std::mem::transmute_copy(&addr) }) }
}

fn exports() -> io::Result<&'static Exports> {
    static EXPORTS: OnceLock<Result<Exports, String>> = OnceLock::new();
    EXPORTS.get_or_init(|| {
        let handle = unsafe { libc::dlopen(c"libfuse3.so.3".as_ptr(), libc::RTLD_NOW) };
        if handle.is_null() {
            let reason = unsafe { CStr::from_ptr(libc::dlerror()) }.to_string_lossy().into_owned();
            return Err(format!(
                "libfuse3 not found ({reason}) - install it (Debian/Ubuntu: `apt install \
                 libfuse3-3`; Fedora: usually already present as `fuse3`)"
            ));
        }
        let main_real = unsafe { resolve(handle, c"fuse_main_real") }
            .ok_or("libfuse3.so.3 is missing fuse_main_real")?;
        let get_context = unsafe { resolve(handle, c"fuse_get_context") }
            .ok_or("libfuse3.so.3 is missing fuse_get_context")?;
        Ok(Exports { main_real, get_context })
    }).as_ref().map_err(|msg| io::Error::other(msg.clone()))
}

pub fn check_available() -> io::Result<()> {
    exports().map(|_| ())
}

pub unsafe fn fuse_main_real(
    argc: c_int, argv: *mut *mut c_char, op: *const fuse_operations, op_size: size_t,
    private_data: *mut c_void,
) -> io::Result<c_int> {
    let main_real = exports()?.main_real;
    Ok(unsafe { main_real(argc, argv, op, op_size, private_data) })
}

pub fn fuse_get_context() -> *mut fuse_context {
    // Only called from within a dispatch_* trampoline, i.e. after
    // fuse_main_real already resolved exports() successfully - same
    // invariant/rationale as windows::sys::fuse_get_context.
    let get_context = exports()
        .expect("fuse_get_context called before fuse_main_real resolved libfuse3")
        .get_context;
    unsafe { get_context() }
}
```

Note the real libfuse3 `fuse_get_context()` takes no arguments (unlike WinFSP's
`fsp_fuse3_get_context(env)`, which needs an opaque environment handle WinFSP's compatibility
layer requires) - keep that signature difference, don't copy the Windows `fuse_env()` plumbing
here, it doesn't apply.

### 2. `mountfs/src/linux/mod.rs` - propagate the `Result`

Mirror the change already made to `windows/mod.rs`'s `mount()` (same before/after shape,
`sys::fuse_main_real(...)` now returns `io::Result<c_int>` instead of `c_int`). Current code
(lines ~387-406):

```rust
let private_data = Box::into_raw(Box::new(fs));
let exit_code = unsafe {
    sys::fuse_main_real(
        args.len() as c_int,
        args.as_mut_ptr(),
        &ops,
        std::mem::size_of::<sys::fuse_operations>(),
        private_data.cast::<c_void>(),
    )
};
unsafe { (*private_data).on_unmount() };
unsafe { drop(Box::from_raw(private_data)) };

if exit_code == 0 {
    Ok(())
} else {
    Err(io::Error::other(format!(
        "fuse_main_real exited with code {exit_code}"
    )))
}
```

becomes:

```rust
let private_data = Box::into_raw(Box::new(fs));
let result = unsafe {
    sys::fuse_main_real(
        args.len() as c_int,
        args.as_mut_ptr(),
        &ops,
        std::mem::size_of::<sys::fuse_operations>(),
        private_data.cast::<c_void>(),
    )
};
// on_unmount() is a lifecycle hook for a mount that actually started - if
// fuse_main_real returned Err before that (libfuse3 not found), there was
// never a mount to unmount.
if result.is_ok() {
    unsafe { (*private_data).on_unmount() };
}
unsafe { drop(Box::from_raw(private_data)) };

match result? {
    0 => Ok(()),
    exit_code => Err(io::Error::other(format!(
        "fuse_main_real exited with code {exit_code}"
    ))),
}
```

Also add, right above `pub fn mount`:

```rust
/// Cheap check for whether libfuse3 is actually available right now,
/// without starting a mount - see `sys::exports`. Mirrors
/// `windows::preflight` for the same reason: lets a caller report a clean
/// error before announcing a mount as started rather than after.
pub fn preflight() -> io::Result<()> {
    sys::check_available()
}
```

### 3. `mountfs/src/lib.rs` - wire up the real preflight

Replace the current Linux stub:

```rust
#[cfg(target_os = "linux")]
pub fn preflight() -> std::io::Result<()> {
    Ok(())
}
```

with:

```rust
#[cfg(target_os = "linux")]
pub use linux::preflight;
```

(matching the `#[cfg(target_os = "windows")] pub use windows::{mount, preflight};` line already
there). The `#[cfg(not(any(...)))]` fallback stub for other platforms keeps its `Ok(())` - nothing
to check on a platform with no backend at all.

### 4. Drop the build-time link entirely

- Delete `mountfs/build.rs` - once neither platform links anything at build time, there's nothing
  left for it to do (its Windows branch was already just a comment: `// Windows (WinFSP) backend:
  not implemented yet` - itself stale since the Windows backend has existed for a while; deleting
  the file removes that stale comment too rather than needing a separate fix for it).
- Remove `pkg-config = "0.3"` from `mountfs/Cargo.toml`'s `[build-dependencies]` (and the now-empty
  `[build-dependencies]` section header itself).
- `libc` stays a dependency (already used for POSIX types elsewhere in the Linux backend, and now
  also for `dlopen`/`dlsym`/`dlerror`/`RTLD_NOW`).

### 5. Documentation

- `docs/development.md`'s Linux prerequisites currently list `libfuse3-dev`/`fuse3-devel` and
  `pkg-config` as *build-time* requirements. Replace with a runtime-only note, symmetric with the
  existing Windows/WinFSP bullet right below it:

  ```markdown
  **Linux**:
  - [libfuse3](https://github.com/libfuse/libfuse) installed - required at *runtime* for `mount`
    to work; **not** required at build time (`mountfs` resolves it dynamically via `dlopen`, no
    headers/`pkg-config` needed). Debian/Ubuntu: `apt install libfuse3-3`; Fedora: usually already
    present as `fuse3`.
  ```

- `README.md`'s existing Linux prerequisite line (`libfuse3`/`fuse3` installed) already describes
  the *end-user* runtime requirement correctly and needs no change - it never claimed this was a
  build-time need.
- `AGENTS.md` has no `libfuse3`-specific text to update.

## Verification

Standard workspace checks (`cargo fmt --check && cargo clippy --all-targets -- -D warnings &&
cargo test`, plus `cargo doc --no-deps`) cover the code-level change and should pass unmodified -
the existing real-libfuse3 integration tests in `mountfs/src/linux/mod.rs` and
`cli/src/mount.rs` exercise the "libfuse3 present" path, which behaves identically before and
after (dlopen succeeds, same function pointers, same calls).

The actual point of this change - behavior when `libfuse3` is *absent* - needs the same Docker
technique already used to diagnose the original problem, reused here to confirm the fix:

```bash
# 1. Build WITHOUT libfuse3(-dev) present at all, proving the build-time dependency is gone:
docker run --rm -v "$(pwd):/workspace" -w /workspace rust:1.97-slim-bookworm \
    cargo build --release -p cli

# 2. Run the result in a container with no libfuse3 installed - expect a clean
#    "error: libfuse3 not found (...)" and a non-zero, non-101 exit code, NOT
#    "error while loading shared libraries" and NOT a Rust panic:
docker run --rm -v "$(pwd)/target/release/backup:/backup:ro" debian:bookworm-slim \
    /backup mount -r /some/repo /some/mountpoint

# 3. Sanity check the rest of the binary now works fine without libfuse3 too:
docker run --rm -v "$(pwd)/target/release/backup:/backup:ro" debian:bookworm-slim \
    /backup --help

# 4. Confirm the "present" path still works: same run, but with libfuse3-3 installed
#    in the second container - should mount successfully, same as before this change.
```

Also update `mountfs/vendor/`-style dev docs if the bindgen-sanity-check note in
`docs/development.md` (currently Windows-only) turns out to need a Linux mirror - not expected
(libfuse3's ABI stability makes a periodic bindgen diff less valuable than it is for WinFSP), but
worth a second look once the struct-layout code is actually being edited for step 1 above.
