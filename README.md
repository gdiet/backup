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
  the mount is started again;
- list a directory's contents (`dfs list`) or restore repository paths to a real directory on disk
  (`dfs restore`, with optional content verification and best-effort recovery of missing or
  corrupted data) - both without mounting; `dfs list --show-deleted` also reveals a directory's own
  soft-deleted history at its `[deleted]` path, and both commands can address a specific
  soft-deleted entry directly through it, without reactivating it first;
- import one or more real filesystem paths into a repository directory (`dfs ingest`),
  deduplicating their content along the way, honoring per-directory `.backupignore` exclusion
  rules, continuing past a source file that cannot be read, and optionally accelerating a repeat
  run against an earlier ingest via `--reference`.

Underneath the CLI: [`cdc`](crates/cdc/) (content-defined chunking, the basis for sub-file
deduplication) and [`mountfs`](crates/mountfs/) (a cross-platform mount backend, Linux via
libfuse3, Windows via WinFSP, behind a single trait - see
[`docs/design/mount-abstraction.md`](docs/design/mount-abstraction.md) for the design).

## Known Limitations

- **Some network mounts are unreliable**: over certain network mounts, and in particular across the
  WSL↔Windows bridge, DedupFS operations can fail. Technical detail:
  [`docs/design/metadata-storage.md`](docs/design/metadata-storage.md) and
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
