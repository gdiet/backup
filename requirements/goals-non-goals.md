# Goals And Non-Goals

## Core

The defining characteristics of DedupFS, present regardless of how the software is designed or
implemented: a lightweight, deduplicating filesystem well suited for backing up many files and
large volumes of data — narrower in scope than a general-purpose deduplicating/checksumming
filesystem (ZFS, Btrfs), not a full storage-stack replacement.

- **Deduplication** — storing the same content multiple times costs almost no additional space. See
  REQ-STORAGE-001 in [`functional/storage.md`](functional/storage.md).
- **Path-addressable hierarchy** — this is a backup archive with a directory structure, not an
  opaque store addressed by key. See REQ-TREE-001 in [`functional/tree.md`](functional/tree.md).
- **Dependable filesystem-style access** — the repository can be used as if it were an ordinary
  filesystem, and reading content back — through that access or through a dedicated restore path
  — hands back exactly what was stored. See REQ-MOUNT-001 in
  [`functional/mount.md`](functional/mount.md) and REQ-RESTORE-001 in
  [`functional/restore.md`](functional/restore.md).
- **Integrity is knowable, not assumed** — whether stored data still matches what was stored can
  actually be checked, not just trusted; ordinary filesystems do not provide this, only
  checksumming ones like ZFS/Btrfs do, and it is what makes a lightweight filesystem trustworthy
  for a large, long-lived archive despite skipping their complexity. See REQ-INTEGRITY-001 in
  [`functional/integrity.md`](functional/integrity.md).
- **Low resource footprint and easy installation** — see REQ-OPERABILITY-001 in
  [`non-functional/operability.md`](non-functional/operability.md).
- **Mirrorable with generic file-sync tools** — a repository can be kept in sync with a secondary
  copy using ordinary tools, not repository-specific ones. See REQ-OPERABILITY-002 in
  [`non-functional/operability.md`](non-functional/operability.md).

## Goals

## Non-Goals

- **General-purpose filesystem semantics** — this does not aim to replicate everything a
  general-purpose filesystem provides: for example, fine-grained permissions, hard links, and
  block-level (random-write) access are explicitly out of scope. The goal is a backup archive
  usable like a
  filesystem for reading and organizing content, not a drop-in replacement for one.
