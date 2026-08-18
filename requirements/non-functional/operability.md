# Operability

### REQ-OPERABILITY-001: Low resource footprint and easy installation
Status: agreed
Importance: must

The software runs with a small, bounded memory footprint that does not grow with repository size
(e.g. 128 MB RAM), and can be installed and made ready to use with minimal effort — no separately
managed runtime, database server, or complex configuration beyond obtaining the software and
pointing it at a repository directory.

Rationale: operators running this against a personal backup archive, often on modest hardware,
should not need to provision resources or maintain infrastructure disproportionate to the simple
job of storing and retrieving files.

### REQ-OPERABILITY-002: Mirrorable with generic file-sync tools
Status: agreed
Importance: must

A repository can be kept in sync with a secondary copy using ordinary, repository-unaware
file-synchronization tools — comparing file size and modification time is enough to decide what
needs copying — without those tools needing to understand deduplication, chunk boundaries, or the
metadata format, and without re-transferring stored content that has not actually changed.
Metadata is small relative to the bulk data it describes, so re-transferring it in full on every
sync run stays cheap even though, unlike the bulk data, it does change on every run.

Rationale: operators maintaining an offline or secondary copy of a repository — especially a large
one — need that sync to be fast and to rely on tooling they already trust; building and trusting a
repository-specific sync mechanism would cost more than reusing what already works. This depends
on the storage layout described in REQ-STORAGE-007 in
[`../functional/storage.md`](../functional/storage.md).
