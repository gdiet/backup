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
[`../functional/mount.md`](../functional/mount.md)) with several files being written at once — can
be chunked and hashed on multiple CPU cores concurrently; one stream's chunking does not block
another's. A single stream's own chunking need not be internally parallelized - the parallelism
required is across streams, not within one.

Rationale: the workload this is built for is many files and large volumes of data in aggregate (see
"Core" in [`../goals-non-goals.md`](../goals-non-goals.md)), not a few very large individual files
or a single write stream — cross-stream parallelism delivers the available speedup for that shape
of workload regardless of which write path the streams arrive through, without the complexity and
reduced dedup quality splitting a single stream's chunking across threads would cost (forced,
non-content-defined boundaries at the split points). Should a workload with very large individual
streams on storage fast enough for single-thread chunking speed to bottleneck emerge, this would
need revisiting — no such case is known today.

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

### REQ-PERFORMANCE-004: Whole-file throughput on slow storage matches or beats native, even without deduplication
Status: agreed
Importance: should

On storage where the device itself is the bottleneck (a slow USB stick, an external drive over a
constrained link), read and write throughput for files through DedupFS is at least as good as
reading/writing the same files directly on the underlying filesystem — without relying on any
deduplication effect (identical, never-before-seen content) to get there.

Rationale: many small files are the case most likely to expose overhead from indirection (chunking,
hashing, the metadata database) — if DedupFS is not at least as fast as the native filesystem here
even before deduplication has a chance to help, an operator has a reason to avoid the read-write
mount for exactly the workload it targets. Coalescing what a native filesystem would treat as many
small, separate writes into fewer, larger writes against the underlying storage (REQ-STORAGE-007 in
[`../functional/storage.md`](../functional/storage.md)) is expected to be the mechanism that makes
this achievable, not just a wash.

### REQ-PERFORMANCE-005: Directory and empty-file operations on slow storage beat native
Status: agreed
Importance: should

On storage where the device itself is the bottleneck, directory operations (create, list, lookup)
and zero-byte-file operations through DedupFS are measurably faster than the same operations
directly on the underlying filesystem.

Rationale: these operations touch only metadata, and DedupFS answers them through the metadata
database (REQ-TREE-001 in [`../functional/tree.md`](../functional/tree.md)) rather than issuing the
many small filesystem-level metadata operations a native directory tree of the same shape would
need — fewer, larger operations against the slow device than the native filesystem's own metadata
handling would require for an equivalent tree.

### REQ-PERFORMANCE-006: On fast storage, chunking/hashing throughput scales with available CPU cores, including under a sequential mount write
Status: agreed
Importance: should

On storage fast enough that the device is no longer the bottleneck, a single write stream's
throughput is bounded by chunking and hashing cost (REQ-STORAGE-002 in
[`../functional/storage.md`](../functional/storage.md)), and concurrent write streams see
throughput scale with available CPU cores, up to REQ-PERFORMANCE-002's cross-stream parallelism.
This holds even for a client copying files one at a time onto a read-write mount (REQ-MOUNT-003 in
[`../functional/mount.md`](../functional/mount.md)): chunking and hashing happen when a file is
closed, not while it is being written, so the next file's data can already be arriving from the
client into the mount's cache while the previous file's chunking/hashing is still running —
sequential from the client's point of view, pipelined underneath.

Rationale: an operator copying files sequentially onto a mount should not need to know or care that
parallelism is what makes it fast — the speedup this requirement describes needs to be an
architectural property of how the mount defers and pipelines this work, not something that only
shows up when a client happens to write multiple files concurrently itself.
