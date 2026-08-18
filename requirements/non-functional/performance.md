# Performance

### REQ-PERFORMANCE-001: Verification cost scales with physical, not logical, data volume
Status: draft
Importance: should

When verifying content that participates in deduplication (see REQ-STORAGE-002 in
[`functional/storage.md`](../functional/storage.md)), each distinct stored chunk is checked once
regardless of how many files or paths reference it — the cost of a verification run scales with
how much physically distinct data exists in the scoped range, not with the logical,
pre-deduplication size of what is being verified.

Rationale: deduplication's whole point is that shared content is stored once; a verification pass
that re-reads and re-hashes the same chunk once per referencing file would silently defeat that
benefit for exactly the operation (REQ-INTEGRITY-001 in
[`functional/integrity.md`](../functional/integrity.md)) most likely to be run against the whole
repository and most sensitive to cost.
