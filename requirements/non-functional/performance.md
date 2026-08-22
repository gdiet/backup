# Performance

### REQ-PERFORMANCE-001: Verification cost scales with physical, not logical, data volume
Status: agreed
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

### REQ-PERFORMANCE-002: Writing content scales with cross-stream parallelism
Status: agreed
Importance: must

Content arriving through multiple concurrent write streams — whether a directed import (see
REQ-INGEST-001 in [`../functional/ingest.md`](../functional/ingest.md)) processing many source
files, or a read-write mount (see REQ-MOUNT-003 in
[`../functional/mount.md`](../functional/mount.md)) with several files being written to it at
once — can be chunked and hashed on multiple CPU cores concurrently; one stream's chunking work
does not block another's. Chunking a single stream's content is not required to be internally
parallelized; the parallelism this needs to support is across concurrently-active streams, not
splitting one stream's chunking across multiple threads.

Rationale: the workload this is built for is many files and large volumes of data in aggregate
(see "Core" in [`../goals-non-goals.md`](../goals-non-goals.md)), not a few very large individual
files or a single write stream — cross-stream parallelism delivers the available speedup for that
shape of workload regardless of which write path the streams arrive through, without the added
complexity and reduced dedup quality that splitting a single stream's chunking across threads
would cost (forced, non-content-defined boundaries at the split points). Should a workload with
very large individual files/streams on storage fast enough for single-thread chunking speed to
become the bottleneck emerge, this would need revisiting — no such case is known today.

### REQ-PERFORMANCE-003: Reading stored content in smaller pieces does not cost proportionally more
Status: agreed
Importance: should

Reading a piece of stored content via several smaller reads costs close to what reading the same
bytes in one larger read would — overhead that is not proportional to the actual bytes
transferred does not dominate at the read sizes this project's mount (REQ-MOUNT-001 in
[`../functional/mount.md`](../functional/mount.md)) and restore (REQ-RESTORE-001 in
[`../functional/restore.md`](../functional/restore.md)) paths actually produce.

Rationale: a mount, in particular, reads a file through however many separate read calls the
client/OS chooses to issue, at a granularity this project does not control — a store whose own
per-call overhead is significant relative to typical read sizes would make ordinary file access
through the mount slower for reasons unrelated to how much data is actually being read.
