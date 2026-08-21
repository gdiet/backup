# Third-Party Notices

This project's own code is licensed under MIT OR Apache-2.0 (see
[`LICENSE-MIT`](LICENSE-MIT) / [`LICENSE-APACHE`](LICENSE-APACHE)). This
file collects notices required by third-party software it depends on but
does not itself distribute.

## WinFSP

[WinFSP](https://github.com/winfsp/winfsp) is Copyright (C) Bill
Zissimopoulos, licensed under GPLv3. The Windows mount backend
(`crates/mountfs/src/windows/`) dynamically loads WinFSP's `winfsp-x64.dll` at
runtime and calls its `fsp_fuse3_*` exports (see
[`crates/mountfs/src/windows/sys.rs`](crates/mountfs/src/windows/sys.rs)) - this counts
as "linking" for the purposes of the license below, even though there is
no build-time dependency on WinFSP (no import library, nothing declared in
this project's own `Cargo.toml`).

WinFSP ships an explicit FLOSS exception to GPLv3 that permits this
without requiring this project to be GPLv3-licensed itself, quoted here in
full since it is the reason the Windows mount feature is legally viable at
all:

> As a special exception to GPLv3, Bill Zissimopoulos grants additional
> permissions to Free/Libre and Open Source Software ("FLOSS") without
> requiring that such software is covered by the GPLv3.
>
> 1. Permission to link with a platform specific version of the WinFsp DLL
>    (one of: winfsp-a64.dll, winfsp-x64.dll, winfsp-x86.dll,
>    winfsp-msil.dll).
>
> ... provided that the software:
>
> 1. Is distributed under a license that satisfies the [FSD]/[OSD].
> 2. Includes the copyright notice "WinFsp - Windows File System Proxy,
>    Copyright (C) Bill Zissimopoulos" and a link to the WinFsp repository
>    in its user-interface and any user-facing documentation.
> 3. Is not linked or distributed with proprietary (non-FLOSS) software.

This project satisfies all three conditions: it is MIT/Apache-2.0
(satisfies both the [FSD] and the [OSD]), carries the required copyright
notice and repository link right here, and does not bundle any
proprietary software. The same notice also appears in
[`README.md`](README.md#winfsp-notice); a `--version`/`about` command
carrying it too is still open - see REQ-MOUNT-006 in
[`requirements/functional/mount.md`](requirements/functional/mount.md) -
and is needed before any user-facing release, since no command-line tool
exists yet for that notice to appear in.

> WinFsp - Windows File System Proxy, Copyright (C) Bill Zissimopoulos
> https://github.com/winfsp/winfsp

See also [`crates/mountfs/vendor/winfsp/NOTICE.md`](crates/mountfs/vendor/winfsp/NOTICE.md)
for the WinFSP header copies kept there and used as a struct-layout reference.

[FSD]: https://www.gnu.org/philosophy/free-sw.html
[OSD]: https://opensource.org/osd
