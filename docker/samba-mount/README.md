# Experimental: mount a repository in Docker, re-export it via Samba

Runs `backup mount` inside a container and shares the result over SMB, so a Windows client can
browse it without installing WinFSP at all - reachable as an ordinary network share instead of a
local drive/mount. Alternative to the native Windows/WinFSP mount and the Linux/WSL2 mount this
project already supports natively.

**Status: experimental, on branch `docker-samba-mount`, not merged into `rust`.**

## Build and run

```bash
# from the rust/ repo root
docker build -t dedup-samba-mount -f docker/samba-mount/Dockerfile .

docker run --rm --cap-add SYS_ADMIN --device /dev/fuse \
    -v /path/to/repository:/repo \
    -p 445:445 \
    dedup-samba-mount
```

- `--cap-add SYS_ADMIN --device /dev/fuse`: required for the container to perform the actual FUSE
  mount syscall.
- `-v /path/to/repository:/repo` - **not** `:ro`: SQLite's WAL mode needs to create `-wal`/`-shm`
  sidecar files in the repository's `meta/` directory even for read-only queries, so the bind
  mount itself can't be read-only. This is independent of `backup mount`'s own read-only serving
  to SMB/FUSE clients (still the default; pass `--read-write` via `MOUNT_ARGS` - not currently
  wired up in `entrypoint.sh`, would need a small addition there - to change that).
- Default SMB credentials: user `dedup`, password `dedup` (override via `-e SMB_USER=... -e
  SMB_PASSWORD=...`). Fixed-user auth, not guest access - modern Windows clients don't reliably
  allow anonymous SMB logons by default.

Connect from Windows: `\\<host-or-container-ip>\dedup`, or `net use Z: \\<ip>\dedup /user:dedup
dedup`.

## Two non-obvious problems already found and fixed here

1. **`libfuse3-3` alone is not enough** - the `fuse3` package (providing the `fusermount3` setuid
   helper that performs the actual mount syscall) is also required. `libfuse3-3` alone produces
   `fuse_main_real exited with code 8`. Both packages are installed in the `Dockerfile`.
2. **FUSE mounts are only visible to the mounting user (here: root) by default**, regardless of
   the mounted files' reported permission bits, unless the mount passes FUSE's `allow_other`
   option - which `backup mount` doesn't (single-user tool by design). Since `smbd` runs
   filesystem operations as the *authenticated SMB user* (not root), this produced a blanket
   `NT_STATUS_ACCESS_DENIED` / `vfs_ChDir ... Permission denied` from Samba even though the mount
   itself was live and working. Fixed via `force user = root` in `smb.conf` - SMB login still
   requires valid credentials, but actual file access always runs as root, matching who did the
   mount. (The alternative - adding a `--allow-other` flag to `backup mount` itself - was
   deliberately not pursued here, to keep this experiment from touching the core `cli`/`mountfs`
   code.)

## Open problem: SMB access from an actual Windows host

Verified working end-to-end via `smbclient` from Linux (list, `cd`, `get` a file, content
correct). Verified `docker run -p 445:445` makes the container's port 445 reachable from the
Windows host (`Test-NetConnection -ComputerName <wsl-ip> -Port 445` → `TcpTestSucceeded: True`).

**But** actually mapping the share from Windows (`net use Z: \\<wsl-ip>\dedup /user:dedup dedup`)
failed with `System error 67 - the network name was not found` - despite the TCP port being
reachable, which points at an SMB-protocol-level or policy-level rejection, not a routing/firewall
block. Not chased down further from this environment - the Windows host used for that test appears
to be a corporate-managed device (domain-joined, restricted `C:\` root permissions observed
separately), and an enterprise policy blocking SMB client connections to non-domain servers is a
plausible cause, but unconfirmed. **Needs testing from an unmanaged/personal Windows machine (or
with someone able to check local GPO/Defender/firewall SMB client policy) to isolate whether this
is a config problem here or an environment/policy restriction on that specific machine.**

Things worth trying next, in rough order of likely payoff:
- Test from a different (non-corporate-managed) Windows machine or VM first, to rule out
  machine-specific policy entirely.
- Check `gpresult /r` / local security policy for SMB client restrictions (e.g. "Restrict NTLM",
  "Digitally sign communications") on the machine that failed.
- Try SMB1 explicitly disabled/enabled combinations, and `smb.conf`'s `server min protocol`/`server
  max protocol` range, in case the negotiated protocol version is the actual sticking point rather
  than a policy block.
- Try connecting by IP with an explicit port (`net use Z: \\<ip>@445\dedup` or via the `net view`
  / `Get-SmbConnection` cmdlets) in case name resolution (not raw TCP reachability) is the actual
  failure mode despite the passing `Test-NetConnection`.
