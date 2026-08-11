# Experimental: mount a repository in Docker, re-export it via Samba

Runs `backup mount` inside a container and shares the result over SMB, so a Windows client can
browse it without installing WinFSP at all - reachable as an ordinary network share instead of a
local drive/mount. Alternative to the native Windows/WinFSP mount and the Linux/WSL2 mount this
project already supports natively.

**Status: experimental, kept as a template** for a dedicated Linux dedup server with Samba (e.g. a
Raspberry Pi), not a maintained product. Core path (mount in Docker, serve via Samba, connect from
a real Windows host over the network) verified working end-to-end - see "SMB access from an actual
Windows host" below. Every `backup mount` flag, including `--read-write`, is reachable via
`MOUNT_ARGS` (see "Mount options" below) - smoke-tested for each flag, including the actionable
abort case.

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
  "Read-Only Commands Need A Clean Database" in the main [README](../../README.md)). See "Mount
  options" below for `--read-write` (which needs `:ro` dropped) and every other `backup mount`
  flag.
- Default SMB credentials: user `dedup`, password `dedup` (override via `-e SMB_USER=... -e
  SMB_PASSWORD=...`). Fixed-user auth, not guest access - modern Windows clients don't reliably
  allow anonymous SMB logons by default.

Connect from Windows: `\\<host-or-container-ip>\dedup`, or `net use Z: \\<ip>\dedup /user:dedup
dedup`.

## Mount options (`MOUNT_ARGS`)

`entrypoint.sh` runs `backup mount -r $REPO $MOUNT_ARGS $MOUNTPOINT` - `MOUNT_ARGS` is passed
through as-is, so every flag `backup mount` supports (see the main [README](../../README.md)'s
"Mount" section for the full reference) works here exactly as on the command line, with no
per-flag wiring to keep in sync. It's word-split on whitespace (no shell quoting inside the
value), which covers every flag below without issue - a value containing its own spaces (unlikely
for any of these) isn't expressible this way.

Read-only (default, no `MOUNT_ARGS` needed):

```bash
docker run --rm --cap-add SYS_ADMIN --device /dev/fuse \
    -v /path/to/repository:/repo:ro -p 445:445 dedup-samba-mount
```

Read-write - drop `:ro` from the volume mount (a read-write mount always needs to write to
`meta/`; see `MountArgs::read_write`'s own doc comment in `cli/src/mount.rs` for why `store`/
`del`/`reclaim-space` mustn't run against the same repository while this container is up):

```bash
docker run --rm --cap-add SYS_ADMIN --device /dev/fuse \
    -v /path/to/repository:/repo \
    -e MOUNT_ARGS="--read-write" \
    -p 445:445 dedup-samba-mount
```

Read-write with a larger write-cache budget and permission to risk swapping anyway (matches
`backup mount --write-cache-mb 512 --allow-swap-risk`):

```bash
docker run --rm --cap-add SYS_ADMIN --device /dev/fuse \
    -v /path/to/repository:/repo \
    -e MOUNT_ARGS="--read-write --write-cache-mb 512 --allow-swap-risk" \
    -p 445:445 dedup-samba-mount
```

Read-write with the write-cache spillover directory pointed at a dedicated, bind-mounted disk
instead of the container's own (ephemeral, usually small) filesystem - `--temp`'s target must
exist and be writable *inside the container*, so it needs its own `-v`, distinct from `/repo`:

```bash
docker run --rm --cap-add SYS_ADMIN --device /dev/fuse \
    -v /path/to/repository:/repo \
    -v /fast/local/disk:/spill \
    -e MOUNT_ARGS="--read-write --temp /spill" \
    -p 445:445 dedup-samba-mount
```

Read-only, serving missing/short store data as zero-filled instead of failing (matches `backup
mount --zero-fill-missing`):

```bash
docker run --rm --cap-add SYS_ADMIN --device /dev/fuse \
    -v /path/to/repository:/repo:ro \
    -e MOUNT_ARGS="--zero-fill-missing" \
    -p 445:445 dedup-samba-mount
```

**If the repository isn't ready** (pending migrations, a non-empty pending write-ahead log, or a
schema newer than this build understands - see "Read-Only Commands Need A Clean Database" in the
main README), `backup mount` fails immediately with the actionable error message for exactly that
condition, and the container exits right away with that message in `docker logs` - it does not
sit out the mount-readiness timeout first. Run `backup db compact` on the repository (outside the
container, or via `--read-write` once to let it apply pending migrations) and retry.

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
