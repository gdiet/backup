# Migrating From Scala DedupFS

How to migrate an existing Scala-DedupFS repository to this implementation.

## Stored byte content

Reused as-is, no bytes rewritten: DESIGN-STORE-001 in
[`../docs/design/byte-store.md`](../docs/design/byte-store.md) deliberately matches Scala's own
on-disk byte-store layout exactly (same file-size constant, same position-to-path formula), so an
existing Scala repository's `data/` directory becomes this implementation's `data/` directory
unchanged - migration only ever reads those bytes to derive new metadata (new content-defined
chunk boundaries, new BLAKE3 hashes), per REQ-MIGRATION-002 in
[`../requirements/functional/repository-migration.md`](../requirements/functional/repository-migration.md).
Not yet implemented, and not yet verified end to end against a real Scala repository's `data/`
directory - DESIGN-STORE-001 is still `Status: draft`.

<!-- Remaining steps, prerequisites, metadata-migration specifics, rollback/fallback guidance. -->
