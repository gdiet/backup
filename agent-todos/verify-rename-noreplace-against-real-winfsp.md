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
