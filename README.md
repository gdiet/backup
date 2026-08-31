# DedupFS

DedupFS is a lightweight, deduplicating filesystem well suited for backing up many files and
large volumes of data: storing the same content multiple times costs almost no additional space,
the repository is a path-addressable directory tree, and it can be used as if it were an ordinary
filesystem. See [`requirements/goals-non-goals.md`](requirements/goals-non-goals.md) for the full
scope, and [`requirements/`](requirements/) generally for the complete set of product
requirements.

This repository is the official successor to an earlier Scala implementation of the same idea —
see [`migration/`](migration/) for the migration path and feature-parity tracking.

## Status

Early development. The `dfs` command-line tool exists and can:

- create a new repository (`dfs create-repo`);
- mount an existing repository as a real filesystem (`dfs mount`), read-only by default - browsing
  directories and reading existing files both work;
- with `--read-write`, also create/remove/rename directories and set modification times, and real
  content writes (creating a file, writing to one, truncating, deleting) - all under a
  repository-wide write lock that keeps two writing sessions from running at once. Content changes
  are chunked, hashed, and deduplicated in the background after a file is closed, not while it is
  being written; a background failure is recorded to a plain log file in the repository's `meta/`
  directory, and a systemic failure (e.g. storage full) degrades the session to read-only until
  the mount is started again.

Underneath the CLI: [`cdc`](crates/cdc/) (content-defined chunking, the basis for sub-file
deduplication) and [`mountfs`](crates/mountfs/) (a cross-platform mount backend, Linux via
libfuse3, Windows via WinFSP, behind a single trait - see
[`docs/design/mount-abstraction.md`](docs/design/mount-abstraction.md) for the design).

## Known Limitations

- **Repository write locking on network-mounted storage**: opening a repository for writing (a
  read-write mount, in particular) takes an operating-system advisory lock to keep two writing
  sessions from running against the same repository at once. This is reliably enforced by the
  operating system for a repository on local or removable storage (an internal disk, an external/
  USB drive, in any filesystem format, FAT included) - it is *not* guaranteed on a
  network-mounted repository, where reliability depends on the network filesystem protocol's own
  locking support and how the client/server happen to be configured. Keeping a repository on
  local or removable storage avoids this limitation entirely. Whoever currently holds the lock is
  recorded (hostname, process id, acquisition time) in the lock file itself, inside the
  repository's `meta/` directory, for a human diagnosing an "already locked" report to check.
- **A crashed or forcibly killed process leaves the write lock in place, on any storage**: a
  process that opened a repository for writing and then exited without cleanly unmounting (a
  crash, a hard kill) leaves the lock file behind, and the next `mount --read-write` attempt is
  refused even though nothing is actually still using the repository - this is deliberate, not
  automatically recovered from, so that recovery is never a guess (see "Repository write locking
  on network-mounted storage" above for why). Run `dfs unlock PATH` to check whether the lock is
  actually still held and clear it if it is not; it reports who it was previously held by before
  removing it, and never touches a lock that genuinely is still held.
- **Deleting a file right after writing it can be resurrected by a lagging background settle**:
  a write's chunking/hashing/storage happens in the background after the file is closed; deleting
  the file before that finishes can still let it reappear once the background job completes,
  unaware the name was removed in the meantime. See DESIGN-MOUNT-015's "Known limitation" in
  [`docs/design/mount-write-path.md`](docs/design/mount-write-path.md) for the details.

## System Requirements

Mounting a repository needs a real filesystem-in-userspace driver:

- **Linux**: `libfuse3` and `fuse3` installed.
- **Windows**: [WinFSP](https://github.com/winfsp/winfsp) installed.

Every other planned operation is expected to need neither.

## WinFSP Notice

> WinFsp - Windows File System Proxy, Copyright (C) Bill Zissimopoulos
> https://github.com/winfsp/winfsp

The Windows mount backend links against WinFSP under its FLOSS exception to GPLv3 — see
[`NOTICE.md`](NOTICE.md) for the exception's full text and this project's compliance notes.

## For Developers

Building from source, running tests, crate layout etc. are covered in
[docs/development.md](docs/development.md), not here.

## License

Licensed under either of [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option. See
[`NOTICE.md`](NOTICE.md) for third-party notices (WinFSP).
