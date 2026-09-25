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

Alpha: the `dfs` command-line tool can already be tried out end to end - create a repository,
mount it read-only or read-write, ingest real files into it, list/restore/delete repository paths,
browse and recover soft-deleted history - but it is not yet feature-complete, and its storage
format and command-line surface can still change incompatibly at any point. See
[`migration/feature-comparison.md`](migration/feature-comparison.md) for what is implemented,
planned, or explicitly not planned, feature by feature.

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
