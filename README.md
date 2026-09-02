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

- **Network mounts and the WSL↔Windows bridge are unreliable**: DedupFS works reliably on local or
  removable storage (an internal disk, an external/USB drive, any filesystem format including
  FAT). Over a network mount, or across the WSL↔Windows bridge specifically, both basic database
  access and the write lock that keeps two writing sessions apart can fail unpredictably. Keep a
  repository on local or removable storage; if you must use a network mount, run `dfs` from the
  machine the storage physically lives on, and avoid two write sessions from different machines at
  once. Technical detail: [`docs/design/metadata-storage.md`](docs/design/metadata-storage.md) and
  [`docs/design/repository-locking.md`](docs/design/repository-locking.md).
- **A crash or forced kill can leave a repository locked**: run `dfs unlock PATH` to check for and
  clear a stale write lock left behind by a process that did not exit cleanly - it reports who
  held it, and never touches a lock that is genuinely still held.
- **Deleting a file right after writing it can occasionally undo the delete**: a short background
  step finishes committing the write after the file is closed; deleting the file in that narrow
  window can let it reappear once that step completes. See DESIGN-MOUNT-015's "Known limitation" in
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
