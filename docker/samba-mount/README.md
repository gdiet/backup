# Experimental: mount a repository in Docker, re-export it via Samba

Runs `backup mount` inside a container and shares the result over SMB, so a Windows client can
browse it without installing WinFSP at all - reachable as an ordinary network share instead of a
local drive/mount. Alternative to the native Windows/WinFSP mount and the Linux/WSL2 mount this
project already supports natively.

**Status: experimental, kept as a template** for a dedicated Linux dedup server with Samba (e.g. a
Raspberry Pi), not a maintained product. Core path (mount in Docker, serve via Samba, connect from
a real Windows host over the network) verified working end-to-end - see "SMB access from an actual
Windows host" below. Remaining known gap: `--read-write` isn't wired up in `entrypoint.sh` yet (see
below).

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
- `-v /path/to/repository:/repo:ro` - **now works** (verified end-to-end: container mounts, file
  content reads back correctly, host-side `meta/` stays completely untouched) as long as the
  repository has no pending write-ahead log - run `backup db compact` on it first if unsure (see
  "Read-Only Commands Need A Clean Database" in the main [README](../../README.md)). Without a
  prior `db compact`, the container's `backup mount` will fail to start with an actionable
  `UncheckpointedWal`/`MigrationsPending` error rather than silently misbehaving. Drop `:ro` if
  running with `--read-write` (see below) - a read-write mount always needs to write to `meta/`.
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

## SMB access from an actual Windows host - confirmed working

Verified end-to-end via `smbclient` from Linux (list, `cd`, `get` a file, content correct), and
since confirmed from a real Windows host too: browsing `\\<wsl-ip>` (the WSL2 guest's own address,
e.g. `172.31.109.141` - **not** `\\localhost`, and **not** the WSL2 subnet's gateway address,
which is the *host's* vEthernet adapter, not the guest running the container; check `ip route |
grep default` inside WSL2 if unsure which is which) in Windows Explorer prompts for credentials as
expected (`security = user` in `smb.conf`) - entering the default `dedup`/`dedup` login then shows
and serves the `dedup` share correctly.

One earlier data point turned out to be a false alarm, not a real blocker: an initial one-shot
`net use Z: \\<wsl-ip>\dedup /user:dedup dedup` failed with `System error 67 - the network name
was not found`, despite the TCP port being confirmedly reachable
(`Test-NetConnection -Port 445` → `TcpTestSucceeded: True`). That specific command was never
retried after the credential-prompt path above was confirmed working, so it's unclear whether
`net use`'s all-in-one syntax has its own quirk (e.g. around name resolution or supplying
credentials inline) or whether the container simply wasn't running yet at the time - either way,
the interactive Explorer path is proven to work, so that's the recommended way to connect; `net
use` with a fresh container and correct IP is worth a retry if a scriptable/drive-letter mapping
is specifically needed, but hasn't been re-verified since the working path was found.
