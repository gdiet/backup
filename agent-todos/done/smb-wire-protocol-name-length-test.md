# Test SMB wire-protocol name-length limits against a WinFSP-hosted share

**Needs**: a real, non-Win32-API SMB client (e.g. Samba's `smbclient`) with network access to a
Windows machine that has WinFSP installed and can host an SMB share over a WinFSP mount. Neither
half is available in isolation on the two environments tried so far - see "Why this needs a
different environment" below.
**Size**: medium (real environment/network setup involved, not just a code edit) - confirm scope
with the user before starting, but no need to re-ask "should this be done at all", that's already
established.
**Opened**: 2026-08-12, by a Linux/WSL2 session, following up on native-session findings from
julius (a Windows/WinFSP machine reached over SSH/natively during that work).
**Context**: `docs/plans/long-path-handling-audit.md` (see "Native Windows SMB re-export..." under
"Windows findings" for what's already been established).

## What's already known

`mountfs::MAX_NAME_BYTES` (255 bytes) is enforced in the mount dispatch layer and confirmed
correct against real WinFSP, including through an SMB re-export of a WinFSP mount - but every
client used so far to test the SMB path (PowerShell/.NET, `net share`/`net use`) still goes
through Windows' own Win32 file API underneath, with its own `MAX_PATH` pre-checks dominating the
result. None of that rules out the SMB *wire protocol* having its own independent name/path length
limits, since a true SMB client bypasses that Win32 layer entirely.

## Why this needs a different environment

- Julius (the Windows/WinFSP machine used so far): no Docker (so `docker/samba-mount`'s Samba
  setup isn't available there), and its local WSL2 Debian distro has no `smbclient` installed and
  no passwordless `sudo` to install it non-interactively.
- The Linux/WSL2 side (where this was written): has Docker and can install/use `smbclient`
  easily, but has no WinFSP-hosted share to point it at - `docker/samba-mount` re-exports this
  project's own Linux FUSE mount, not a Windows/WinFSP one, so it doesn't exercise the same code
  path.

## What to actually do

1. Get a real SMB client (`smbclient`, or equivalent) that talks the wire protocol directly,
   somewhere with network access to a Windows/WinFSP host serving a share (julius, or another
   Windows machine with WinFSP) - e.g. install `smbclient` in julius's WSL2 distro (needs
   passwordless sudo or an interactive sudo prompt handled by the user; or a Linux machine with
   network access to julius otherwise).
2. Mount/create a repository, `backup mount --read-write` it via WinFSP, share it (or its parent
   directory - see the plan doc, a WinFSP mountpoint can't be the share root itself) via Windows
   SMB.
3. From the real SMB client, attempt creating a 255-byte and a 256-byte name (matching the
   existing boundary tests) directly over the SMB protocol - record the actual result (accepted/
   rejected, and with what error) without going through any Win32 client API.
4. Record findings in `docs/plans/long-path-handling-audit.md`, replacing the current "remain
   untested" framing. If this turns up a real gap (SMB accepting something WinFSP/NTFS itself
   would reject, or vice versa with a confusing error), decide whether it's worth a fix; if not,
   this is purely closing out the audit's last open question.

---

## Done (2026-08-13, Docker smbclient on Linux/WSL2 against real WinFSP on julius)

The environment split described above stopped applying once SSH connectivity from this
Linux/WSL2 session to julius was set up (see `AGENTS.md`, unrelated work) - Docker here can reach
julius's network directly, so `smbclient` never needed to run on julius's own WSL2 distro after
all. Full recipe and result now in `docs/plans/long-path-handling-audit.md`'s "Windows findings"
section. Summary: 255-byte name succeeds, 256-byte name fails with `NT_STATUS_OBJECT_NAME_INVALID`
- same boundary as every other client already tested, confirmed this time with zero Win32 API
involvement. No gap found. Disposable Windows account/share/mount/repository all cleaned up
afterward.
