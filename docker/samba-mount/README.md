# Experimental: mount a repository in Docker, re-export it via Samba

Runs `dfs mount` inside a container and shares the result over SMB, so a Windows client can browse
it without installing WinFSP at all - reachable as an ordinary network share instead of a local
drive/mount. Alternative to the native Windows/WinFSP mount and the Linux/WSL2 mount this project
already supports natively.

**Status: experimental developer utility, not a core part of the product** - useful for a
dedicated Linux dedup server with Samba (e.g. a Raspberry Pi), not held to the same verification
bar as `cli`/`mountfs`/`db`. Every `dfs mount` flag, including `--read-write`, is reachable via
`MOUNT_ARGS` (see "Mount options" below).

## Build and run

```bash
# from the rust/ repo root
docker build -t dedup-samba-mount -f docker/samba-mount/Dockerfile .

docker run --rm --init --cap-add SYS_ADMIN --device /dev/fuse \
    -v /path/to/repository:/repo \
    -p 445:445 \
    dedup-samba-mount
```

- `--cap-add SYS_ADMIN --device /dev/fuse`: required for the container to perform the actual FUSE
  mount syscall.
- `--init`: runs a minimal init (`tini`, Docker's built-in default for this flag) as PID 1 instead
  of `entrypoint.sh` itself. Not a fix for anything currently broken (`entrypoint.sh` already
  installs its own `TERM`/`INT` traps) - added as cheap, standard defense-in-depth for the other
  classic PID-1-in-a-container problem, zombie reaping: `smbd` forks a child per session (see
  "Non-obvious problems" #4 below), and killing `smbd` itself can transiently reparent an
  already-dying child to `entrypoint.sh` before it also exits.
- `/repo` is mounted **writable** even for a read-only `dfs mount` session (no `MOUNT_ARGS`
  needed for that, the default) - not because the container ever writes repository content, but
  because a genuinely `:ro` bind mount can make a read-only `dfs mount` fail outright against a
  pristine repository (see "Non-obvious problems" #5 below). Real read-only safety does not depend
  on the bind mount's own read/write mode at all - see #5 for why. To actually use `:ro` anyway
  (e.g. the repository genuinely lives on read-only media and nothing else could write to it),
  add `:ro` back and pass `--assume-read-only-medium` via `MOUNT_ARGS` - see #5. See "Mount
  options" below for `--read-write` and every other `dfs mount` flag.
- Default SMB credentials: user `dedup`, password `dedup` (override via `-e SMB_USER=... -e
  SMB_PASSWORD=...`). Fixed-user auth, not guest access - modern Windows clients don't reliably
  allow anonymous SMB logons by default.

Connect from Windows: `\\<host-or-container-ip>\dedup`, or `net use Z: \\<ip>\dedup /user:dedup
dedup`.

## Mount options (`MOUNT_ARGS`)

`entrypoint.sh` runs `dfs mount --repo $REPO $MOUNT_ARGS $MOUNTPOINT` - `MOUNT_ARGS` is passed
through as-is, so every flag `dfs mount --help` lists works here exactly as on the command line,
with no per-flag wiring to keep in sync. It's word-split on whitespace (no shell quoting inside
the value), which covers every flag below without issue - a value containing its own spaces
(unlikely for any of these) isn't expressible this way.

Read-only (default, no `MOUNT_ARGS` needed) - `/repo` still not bind-mounted `:ro`, see "Build and
run" above:

```bash
docker run --rm --init --cap-add SYS_ADMIN --device /dev/fuse \
    -v /path/to/repository:/repo -p 445:445 dedup-samba-mount
```

Read-write (holds the repository-wide write lock for as long as the container runs -
REQ-MAINTENANCE-004, `docs/design/repository-locking.md` - so no other mutating command should
run against the same repository while this container is up):

```bash
docker run --rm --init --cap-add SYS_ADMIN --device /dev/fuse \
    -v /path/to/repository:/repo \
    -e MOUNT_ARGS="--read-write" \
    -p 445:445 dedup-samba-mount
```

Read-write with a larger RAM budget (DESIGN-MEMORY-001, matches `dfs mount --ram-budget-mb 512`):

```bash
docker run --rm --init --cap-add SYS_ADMIN --device /dev/fuse \
    -v /path/to/repository:/repo \
    -e MOUNT_ARGS="--read-write --ram-budget-mb 512" \
    -p 445:445 dedup-samba-mount
```

Read-write with the write cache's spillover directory pointed at a dedicated, bind-mounted disk
instead of the container's own (ephemeral, usually small) filesystem - `--spill-dir`'s target must
exist and be writable *inside the container*, so it needs its own `-v`, distinct from `/repo`:

```bash
docker run --rm --init --cap-add SYS_ADMIN --device /dev/fuse \
    -v /path/to/repository:/repo \
    -v /fast/local/disk:/spill \
    -e MOUNT_ARGS="--read-write --spill-dir /spill" \
    -p 445:445 dedup-samba-mount
```

Read-only, also revealing REQ-TREE-009's `[deleted]` view (matches `dfs mount --show-deleted`):

```bash
docker run --rm --init --cap-add SYS_ADMIN --device /dev/fuse \
    -v /path/to/repository:/repo \
    -e MOUNT_ARGS="--show-deleted" \
    -p 445:445 dedup-samba-mount
```

**If the repository is not ready** (its schema needs a migration this build would normally apply
automatically on a write-mode open, `db::Error::SchemaNeedsMigration` - a read-only open cannot run
it), `dfs mount` fails immediately with that message, and the container exits right away with it
in `docker logs` - it does not sit out the mount-readiness timeout first. Run `dfs db-compact` on
the repository (outside the container, or start this container once with `--read-write`) and
retry.

## Non-obvious problems already found and addressed here

1. **`libfuse3-3` alone is not enough** - the `fuse3` package (providing the `fusermount3` setuid
   helper that performs the actual mount syscall) is also required. `libfuse3-3` alone produces
   `fuse_main_real exited with code 8`. Both packages are installed in the `Dockerfile`.
2. **FUSE mounts are only visible to the mounting user (here: root) by default**, regardless of
   the mounted files' reported permission bits, unless the mount passes FUSE's `allow_other`
   option - which `dfs mount` doesn't (single-user tool by design). Since `smbd` runs filesystem
   operations as the *authenticated SMB user* (not root), this produces a blanket
   `NT_STATUS_ACCESS_DENIED` / `vfs_ChDir ... Permission denied` from Samba even though the mount
   itself is live and working. Fixed via `force user = root` in `smb.conf` - SMB login still
   requires valid credentials, but actual file access always runs as root, matching who did the
   mount. (The alternative - adding an `--allow-other` flag to `dfs mount` itself - was
   deliberately not pursued here, to keep this experiment from touching the core `cli`/`mountfs`
   code.)
3. **`read only = yes` in `smb.conf` would silently make `--read-write` (via `MOUNT_ARGS`) do
   nothing** over real SMB - the FUSE mount itself would be genuinely read-write, but Samba would
   reject every write with `NT_STATUS_ACCESS_DENIED` before it ever reached the filesystem, since
   that setting is static and has no idea what `MOUNT_ARGS` says. `smb.conf` sets `read only = no`
   instead (Samba's own default, confirmed via `testparm -v`, is `Yes` - simply removing the line
   is *not* equivalent and does not fix this). Real read-only enforcement doesn't need a
   Samba-level copy: it already happens one layer down, at the FUSE/kernel level (`dfs mount`
   passes libfuse's own `-oro` whenever `--read-write` isn't given, which the kernel enforces
   regardless of what smbd does) - so `read only = no` here doesn't weaken anything, it just stops
   Samba from second-guessing a decision the FUSE mount already made correctly.
4. **Shutdown (Ctrl+C/`docker stop`) could take the full 20s grace period and then still fail** if
   a real SMB client was still connected (e.g. a live Windows Explorer window) - Samba forks a
   child `smbd` per active session, which keeps its own open file/directory handle into the FUSE
   mount, and `entrypoint.sh` never killed `smbd` before trying to unmount. Fixed by killing every
   `smbd` process (`pkill`, not just the main one - its already-forked children don't die with it)
   before attempting the unmount, with an idempotency guard around `cleanup()` itself (it can
   genuinely run twice per shutdown - killing `smbd` from inside it makes a second, unguarded entry
   point complete too). Shutdown is now well under 2s even with a live session attached.
5. **A `-v ...:/repo:ro` bind mount can make a read-only `dfs mount` fail outright** against a
   pristine repository (freshly created, never opened write-mode since), with `unable to open
   database file`. Root cause is in `db`, not here: the metadata database is in `journal_mode =
   WAL`, and opening a WAL-mode SQLite database at all - even via a read-only connection - needs to
   create a `-shm` file if one does not already exist, which needs a writable directory regardless
   of the connection's own read-only flag. Default here is to just not bind-mount `/repo` read-only
   at all (see "Build and run" above) - that loses nothing, since this container's actual read-only
   enforcement already happens at the FUSE/kernel level (`-oro`, same as problem 3 above) and at the
   `Repository` level (every mutating method refuses outright against a connection opened via
   `open_repository_read_only`), neither of which depends on the bind mount's own mode. `dfs
   mount --assume-read-only-medium` (DESIGN-METADATA-013 in
   `../../docs/design/metadata-storage.md`), added to resolve the underlying `db` limitation this
   found (`agent-todos/done/read-only-open-needs-a-writable-directory-for-wal-shm.md`), is the way
   to use a genuinely `:ro` bind mount here anyway - only pass it when the repository truly lives on
   read-only media (or is otherwise guaranteed unwritable by anything else for the container's whole
   lifetime): `-v /path/to/repository:/repo:ro -e MOUNT_ARGS="--assume-read-only-medium"`.

## Verification status

Verified directly against this implementation (2026-09-23, this port): container build; a
read-only mount coming up and serving real content correctly over a real, authenticated SMB
session (`smbclient`: `ls`, `cd`, `get`, content matching byte-for-byte); a `--read-write` mount
accepting a real `smbclient put`, round-tripping back correctly, and the written content still
present via `dfs list` after a clean container shutdown; shutdown with a live SMB session attached
completing in well under a second, not the full grace period; `smbclient` reporting the
repository's real free space for the share (matching `df` on the host almost exactly), once
`agent-todos/done/wire-disk-space-into-dedup-fs-statfs.md` resolved the zero-blocks gap this
README originally found.

`smb.conf`'s configuration itself (problems 1-4 above) was carried over unchanged from a prior
implementation's separately-verified version of this same experiment, including a real
authenticated `smbclient put`/`get` round trip and access from an actual Windows host over the
network - not independently re-verified against a real Windows client here, only against
`smbclient` from Linux as described above.
