# Mount Write-Path Scheduling

How the future mount write path (REQ-MOUNT-003's real content writes in
[`../../requirements/functional/mount.md`](../../requirements/functional/mount.md) -
`create`/`write`/`truncate`/`unlink` are still `mountfs`'s default `EROFS`, not yet implemented)
schedules its work.

## DESIGN-MOUNT-006: Separate dispatch pool from chunking/storage-write pool; non-blocking `release()`
Status: decided

Three areas have genuinely different scheduling needs: the FUSE/WinFSP dispatch callbacks
themselves (I/O-dispatch-latency-bound, thread count owned by libfuse/WinFSP, not this project);
CDC chunking and hashing (CPU-bound, ideally sized to CPU core count); and writing the result into
`store` (this project's own I/O, different characteristics again). The dispatch pool stays separate
from the other two, so slow chunking/hashing never ties up the limited pool libfuse/WinFSP manages
for every other concurrent mount request. Chunking/hashing and the storage write share one pool to
start; splitting them into two of their own is secondary, revisited only if a measured reason
emerges.

`release()` does not block until that pool finishes before returning. Backing up many
moderate-size files one after another is typically CPU-bound and sequential at the client (close
one file, then open the next) - a blocking `release()` would prevent files from ever actually
processing in parallel, however many workers the pool has, collapsing exactly the throughput a
separate pool exists to provide. A non-blocking `release()` (handing work off and returning
immediately, blocking only as ordinary backpressure once the pool's own queue is full) keeps that
overlap instead - this is the mechanism REQ-PERFORMANCE-006 in
[`../../requirements/non-functional/performance.md`](../../requirements/non-functional/performance.md)
describes: chunking and hashing happen once a file is closed, not while it is being written, so the
next file's data can already be arriving from the client while the previous file's chunking/hashing
is still running - sequential from the client's point of view, pipelined underneath. The same
non-blocking handoff is also what lets multiple concurrent write streams reach REQ-PERFORMANCE-002's
cross-stream parallelism, regardless of which write path (this mount, or a future directed import)
the streams arrive through.

Not decided here: how a failure discovered only during this background processing - after
`release()` has already returned success - gets surfaced to the user. See
[`../../requirements/open-questions.md`](../../requirements/open-questions.md)'s "Mount write-path
failure handling". How REQ-MAINTENANCE-004's single-writer scope relates to a background job
outliving the FUSE call that started it turned out not to need an answer at this level at all - see
DESIGN-MOUNT-008 below.

## DESIGN-MOUNT-007: Same-session readers see a write's in-progress state
Status: decided

Within the same mount session, a second read handle on a file being written sees content as soon
as it is physically written to `store`, even before the corresponding `chunk_extents` row is
committed in `db` - not only the file's last fully-committed state. REQ-TREE-006 in
[`../../requirements/functional/tree.md`](../../requirements/functional/tree.md) already leaves
this choice to the mount's own implementation ("either is acceptable"); this picks the more
permissive of the two.

Deliberately scoped to *this* mount session only - a completely separate process (a second `dfs
mount`, a `dfs query` run concurrently) does not get this same-process visibility; REQ-TREE-006's
cross-process guarantee (visible to a different process only once complete) applies there
unchanged. This scoping is also what avoids a crash-consistency problem the cross-process case
would otherwise have: the writer and this second reader share one process, so if that process
crashes, both go away together - no reader survives to have observed content that, once the
repository recovers, behaves as though it had never been written. A reader in a genuinely separate
process could outlive exactly that crash, which is why it does not get the same treatment here.

## DESIGN-MOUNT-008: A read-write mount holds the single-writer slot for its whole session
Status: decided

REQ-MAINTENANCE-004's "only one mutating operation runs against a repository at a time" is scoped,
for a read-write mount, to the mount's entire session - from the moment it starts with write access
until it unmounts and flushes - not to any individual FUSE call or any individual background
chunking/storage-write job. As long as a read-write mount process is running, no other
repository-mutating operation (another read-write mount, a directed import, reclaim, compaction,
...) can start; a second one is refused (or waits, per REQ-MAINTENANCE-006 in
[`../../requirements/functional/maintenance.md`](../../requirements/functional/maintenance.md)),
same as REQ-MAINTENANCE-004 already says for any other pair of mutating operations.

This is simpler than tracking DESIGN-MOUNT-006's background job pool's own queue/in-flight state
for concurrency-control purposes: the read-write mount session itself already holds the single-writer
slot for as long as it exists, so a reclaim/compaction request cannot race a background chunking job
that outlived the FUSE call that spawned it (the concern DESIGN-MOUNT-006's non-blocking `release()`
raised) - it stays refused (or waiting) for the whole time the mount is up, regardless of whether
any specific write is actually active at that instant.
