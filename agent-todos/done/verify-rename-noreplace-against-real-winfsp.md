# Verify `RENAME_NOREPLACE`'s flag-bit convention against real WinFSP

**Needs**: a real Windows machine with WinFSP installed and a real, user-opened interactive
terminal (same class of requirement as the Ctrl+C shutdown test - see `julius-winfsp-ssh` skill).
**Size**: small
**Opened**: 2026-08-18, by Desktop App session.
**Context**: `mountfs/src/lib.rs`'s `RENAME_NOREPLACE` constant doc comment already states the
gap explicitly, and `docs/design/mount-abstraction.md`'s "Unverified assumptions" section tracks it
too.

WinFSP's Windows backend is documented to emulate libfuse3's high-level API via its `cygfuse`
layer, so its `rename` callback's `flags` parameter is *expected* to use the same
`RENAME_NOREPLACE`/`RENAME_EXCHANGE` bit convention real libfuse3 does - reasoned from that
documented compatibility claim, not yet empirically confirmed against a real WinFSP mount.

Write a small test (or extend `mountfs/tests/windows_mount.rs`) that mounts a `MountFilesystem`
implementing `rename`, then exercises `renameat2(..., RENAME_NOREPLACE)`'s Windows-visible
equivalent (however that is reachable from a Windows client - needs its own short investigation)
against an existing target path, and confirms the call is rejected with `EEXIST` rather than
silently replacing it. If the bit convention turns out to differ from Linux's, `parse_rename_flags`
needs a Windows-specific variant instead of the single shared implementation it has today.

Not urgent (the current behavior is a reasoned default, not a known-wrong one) - worth doing
whenever `mountfs`'s Windows backend is next verified on real hardware anyway.

## Done

**Completed**: 2026-08-19, by Desktop App session, via the `julius-winfsp-ssh` machine.

Confirmed, empirically, that the bit convention does *not* match Linux's: WinFSP's `cygfuse` layer
never sets `RENAME_NOREPLACE`. Found via a small probe filesystem
(`mountfs/src/bin/rename_noreplace_probe.rs`) that logs every `rename` call's observed
`no_replace` value, driven first by raw `MoveFileExW` calls from a PowerShell script (three cases:
no-replace against an existing target, replace against an existing target, no-replace against a
nonexistent target), then turned into a permanent regression test
(`mountfs/tests/rename_noreplace.rs`), which passes running natively on Julius against real
WinFSP:

- A no-replace move against an existing target fails at the OS level (`ERROR_ALREADY_EXISTS`)
  without `MountFilesystem::rename` ever being called - WinFSP rejects the collision itself.
- Every call that does reach `rename` (a replacing move, or a no-replace move with no collision)
  arrives with `no_replace = false`, regardless of which Win32-level flag the original caller used.

Net effect: `parse_rename_flags` does not need a Windows-specific variant (there is no bit to
parse differently), and the caller-visible behavior stays correct (WinFSP itself blocks the
collision case) - but an implementation cannot itself observe or act on `no_replace = true` on
Windows. Documented in `RENAME_NOREPLACE`'s and `MountFilesystem::rename`'s doc comments
(`mountfs/src/lib.rs`) and moved from "Unverified assumptions" to "Known limitations" in
`docs/design/mount-abstraction.md`.
