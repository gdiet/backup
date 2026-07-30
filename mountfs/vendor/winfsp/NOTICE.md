# Vendored WinFSP headers

The files under `fuse/` and `fuse3/` in this directory are unmodified
copies of WinFSP's own public headers, copied from:

https://github.com/winfsp/winfsp/tree/v2.0/inc

(tag `v2.0`, commit `5c03dd11ee92bf834ce378c78c0e191e83096298` - matching
WinFSP 2.0.23075, the version this crate was developed and tested against).

They're vendored (rather than fetched at build time) because WinFSP's
*runtime* installer - which is all an end user or CI machine needs to
actually mount a filesystem - doesn't ship headers at all (no `inc`
directory), only `bin/*.dll`. Only WinFSP's separate SDK/developer
package would provide them, and this crate deliberately avoids requiring
that: `mountfs`'s Windows backend links against nothing at build time and
resolves the handful of WinFSP DLL exports it needs (`fsp_fuse3_main_real`,
`fsp_fuse3_get_context`) at *runtime*, via `LoadLibraryW`/`GetProcAddress`,
falling back to the `HKLM\Software\WOW6432Node\WinFsp\InstallDir` registry
value WinFSP's own installer writes if the DLL isn't already on `PATH` -
the same fallback WinFSP's own `FspLoad` (`inc/winfsp/winfsp.h`) uses, just
reimplemented directly in `windows/sys.rs` instead of vendoring that much
larger header. These headers are consulted purely as the authoritative
reference for exact struct layouts (`fuse_operations`, `fuse_file_info`,
`fuse_stat`, `fuse_statvfs`, `fsp_fuse_env`, ...), which are then
hand-transcribed into `mountfs/src/windows/sys.rs` - not compiled directly
(unlike the Linux backend, where real system libfuse3 is linked and its
headers matter for the Linux build itself, not just as documentation).

## License

WinFSP is Copyright (C) Bill Zissimopoulos, licensed under GPLv3 with an
explicit FLOSS exception permitting linking with (and, per WinFSP's own
practice of publishing these headers for exactly this purpose, referencing
the structure of) a platform-specific WinFsp DLL from FLOSS software such
as this project (MIT-licensed) - see `docs/plans/cross-platform-mount-crate.md`
for the exception's full text and this project's compliance notes.

> WinFsp - Windows File System Proxy, Copyright (C) Bill Zissimopoulos
> https://github.com/winfsp/winfsp
