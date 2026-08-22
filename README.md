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

Early development. What exists so far:

- [`cdc`](crates/cdc/) — content-defined chunking, the basis for sub-file deduplication.
- [`mountfs`](crates/mountfs/) — a cross-platform mount backend (Linux via libfuse3, Windows via WinFSP)
  behind a single trait. See [`docs/design/mount-abstraction.md`](docs/design/mount-abstraction.md)
  for the design.

No command-line tool exists yet — these are library crates, not yet wired into a usable
application.

## System Requirements

Once a command-line tool exists, mounting a repository will need a real filesystem-in-userspace
driver:

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
