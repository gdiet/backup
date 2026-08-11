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
abort case. **A real, authenticated `smbclient put`/`get` round trip against a live `--read-write`
container is now part of that verification too** (see "Two non-obvious problems" #3/#4 below) - an
earlier round of smoke-testing only confirmed each flag made the container *start* correctly, which
missed two real bugs that only a real write, through real Samba, over a real SMB session, actually
exercised.

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
3. **`statfs` (free-space reporting) deadlocked the whole mount solid the first time a real SMB
   client checked free space against a `--read-write` container** - Windows Explorer does this
   before permitting a save, so this hit real usage almost immediately, hanging so completely that
   only `wsl --shutdown` recovered the host. Root cause (in `cli/src/mount.rs`'s free-space code,
   not anything Samba-specific): the earlier implementation used the `sysinfo` crate's `Disks`
   list, which enumerates *every* mounted filesystem and calls `statvfs` on each one - including
   this container's own FUSE mount. That `statvfs` call blocks in the kernel until this same
   process answers it, which requires re-entering the very `statfs` handler that's already running
   (and, in the old code, already holding the cache's lock) - a self-deadlock. Fixed by replacing
   that "enumerate everything" approach with a single, targeted query of exactly the repository's
   `data/` directory (`mountfs::disk_space`, `statvfs`/`GetDiskFreeSpaceExW` directly - see its own
   doc comment), which can never touch this process's own mount point. Reproduced and confirmed
   fixed without needing another `wsl --shutdown`: `docker exec <container> df /mnt/dedup` hung
   solid with the old code, returns instantly with the fix.
4. **`read only = yes` in `smb.conf` silently made `--read-write` (via `MOUNT_ARGS`) do nothing**
   over real SMB - the FUSE mount itself was genuinely read-write, but Samba rejected every write
   with `NT_STATUS_ACCESS_DENIED` before it ever reached the filesystem, since that setting is
   static and has no idea what `MOUNT_ARGS` says. Fixed by setting `read only = no` instead
   (Samba's own default, confirmed via `testparm -v`, is `Yes` - simply removing the line is *not*
   equivalent and does not fix this). Real read-only enforcement doesn't need a Samba-level copy:
   it already happens one layer down, at the FUSE/kernel level (`backup mount` passes libfuse's own
   `-oro` whenever `--read-write` isn't given, which the kernel enforces regardless of what smbd
   does) - so `read only = no` here doesn't weaken anything, it just stops Samba from
   second-guessing a decision the FUSE mount already made correctly. Verified both directions via a
   real authenticated `smbclient put`: succeeds and round-trips correctly against a `--read-write`
   mount, still correctly rejected (`NT_STATUS_MEDIA_WRITE_PROTECTED`, from the real `EROFS`) against
   the default read-only mount.
5. **Shutdown (Ctrl+C/`docker stop`) could take the full 20s grace period and then still fail**
   if a real SMB client was still connected (e.g. a live Windows Explorer window) - Samba forks a
   child `smbd` per active session, which keeps its own open file/directory handle into the FUSE
   mount, and `entrypoint.sh` never killed `smbd` before trying to unmount. Fixed by killing every
   `smbd` process (`pkill`, not just the main one - its already-forked children don't die with
   it) before attempting the unmount, with an idempotency guard around `cleanup()` itself (it can
   genuinely run twice per shutdown - killing `smbd` from inside it makes a second, unguarded
   entry point complete too). Shutdown is now well under 2s even with a live session attached.
   One loose end, deliberately not chased further since it doesn't change behavior either way:
   `backup mount` can still log `fuse_main_real exited with code 8` around this point even with
   `smbd` fully gone - harmless (a `--read-write` mount's writes are already durably committed
   regardless of a clean vs. abrupt unmount, confirmed via `stats` working immediately afterward),
   most likely `fusermount3 -u` here simply racing something that had already unmounted the
   filesystem by the time it ran.

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
