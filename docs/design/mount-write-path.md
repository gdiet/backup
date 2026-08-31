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

Backpressure once the shared pool falls behind is not an abrupt block/no-block threshold: `write()`
(already a synchronous FUSE call, unlike the deliberately non-blocking `release()` above) adds a
small delay that scales smoothly with how far behind the pipeline already is, rather than admitting
work at full speed until some fixed limit is hit and then stopping outright. The concrete signal
this delay scales with - and why it does not need its own separate backlog metric - is
DESIGN-MOUNT-010 below.

Not decided here: how a failure discovered only during this background processing - after
`release()` has already returned success - gets surfaced to the user. See
[`../../requirements/open-questions.md`](../../requirements/open-questions.md)'s "Mount write-path
failure handling". How REQ-MAINTENANCE-004's single-writer scope relates to a background job
outliving the FUSE call that started it turned out not to need an answer at this level at all - see
DESIGN-MOUNT-008 below.

## DESIGN-MOUNT-007: Same-session reads see a file's not-yet-persisted state
Status: decided

Within the same mount session, any read of a file whose content has been written but not yet
durably committed sees that content - not only a second read handle already open while the write
is happening, but also a handle opened fresh after `release()`, for as long as the corresponding
background chunking/hashing/storage-write/`chunk_extents`-commit job (DESIGN-MOUNT-006) has not yet
finished. This is ordinary POSIX same-process read-after-write behavior, the same immediacy
buffered I/O on a real filesystem already gives two file descriptors on the same host, extended to
cover the whole window a background persist job can still be in flight, not only the moment the
client is actively writing. REQ-TREE-006 in
[`../../requirements/functional/tree.md`](../../requirements/functional/tree.md) already leaves
the same-session case to the mount's own implementation ("either is acceptable"); this picks the
more permissive end of that range for the whole window, not just the actively-writing part of it.

Keeping this correct means the mount looks up a file's not-yet-persisted state by file identity,
not by open handle - a fresh `open()` on a file with a background job still running for it needs to
find the same pending state a handle that stayed open throughout would have seen, falling back to
the durably committed state (`chunk_extents` via `db`, bytes via `store`) only once nothing is
still pending for that file. DESIGN-MOUNT-010 below settles the concrete write cache this state
lives in - here, only its externally visible behavior matters, not how it is implemented.

Deliberately scoped to *this* mount session only - a completely separate process (a second `dfs
mount`, a `dfs query` run concurrently) does not get this same-process visibility; REQ-TREE-006's
cross-process guarantee (visible to a different process only once complete) applies there
unchanged. This scoping is also what avoids a crash-consistency problem the cross-process case
would otherwise have: the writer and any same-session reader share one process, so if that process
crashes, all of them go away together - no reader survives to have observed content that, once the
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

## DESIGN-MOUNT-009: Background write failures are logged to a file in `meta/`; a systemic failure degrades the mount to read-only
Status: decided

A background chunking/hashing/storage-write job (DESIGN-MOUNT-006) can fail after `release()` has
already returned success, with no FUSE call left to report it through. The first version of this
mechanism is a plain, append-only log file inside `meta/` (alongside the metadata database, per
DESIGN-REPOSITORY-001 in [`repository-layout.md`](repository-layout.md)), not a queryable database
table and not a dedicated mount-browsable path -
those give a caller a structured way to notice and enumerate failures without reading a log file,
which is real value but not needed for a first version; both are natural later extensions of this
same mechanism, once real usage shows the plain log is not enough. Each entry records enough to act
on later without re-deriving it from surrounding context: which file the job was writing, an error
category, the underlying error message, and when it happened.

Failures are treated differently depending on whether they are systemic or isolated. A systemic
failure - the kind where the underlying cause (storage full, the underlying volume gone) dooms
every other queued or future job just as certainly as the one that just failed - immediately
degrades the mount session to read-only: new write-intent opens fail with an actionable error
instead of queuing more work behind a job that would only fail the same way, and any jobs already
in flight run to completion or failure, landing in the same log either way. Recovering write access
means unmounting, addressing the underlying cause, and mounting again - there is no automatic
retry, and no attempt to tell a transient systemic blip (e.g. a network mount reconnecting on its
own) apart from a permanent one; both are treated the same way and left to the operator to
diagnose. `io::ErrorKind` (`StorageFull`, and I/O errors generally) is what marks a failure as
systemic; this project's own typed logic errors (a bug tied to one file's content) mark it as
isolated instead. An isolated failure only logs that one file's outcome and otherwise does not
affect the session - nothing else in flight is treated as suspect just because one file's job
failed.

### Alternative considered and rejected: attempting transient/permanent detection for systemic failures

Trying to tell a transient systemic condition (a brief disk-full spike, a network mount that
reconnects on its own moments later) apart from a permanent one, so that only a genuinely permanent
failure would degrade the mount, was considered and rejected for a first version: reliably
distinguishing the two needs either a retry-and-observe loop (which risks repeating the same
failure against every job still queued behind it, exactly what the read-only degradation exists to
avoid) or storage-specific heuristics that would not generalize across the range of things `store`
can be backed by. Degrading unconditionally on any systemic failure, with the operator deciding
when to remount, is simpler and fails safe; a smarter distinction is a later refinement, not a
prerequisite for a first version.

## DESIGN-MOUNT-010: Write cache buffers in memory up to a shared session budget, then spills to disk per file
Status: decided

DESIGN-MOUNT-007's not-yet-persisted state - a file's content from the moment it is written until
DESIGN-MOUNT-006's background job durably commits it - lives in memory first. Memory usage is
bounded by a single budget shared across the whole mount session, not one budget per file: it
covers every byte currently not durably committed anywhere in the session at once, both content
still arriving through an open handle and content already released but still waiting on its
background job. A session-wide budget is what actually bounds this mount's memory footprint - a
per-file budget would let enough concurrent writers multiply it without limit, defeating the point
of having one at all. The budget is configurable, defaulting to 256 MiB - large enough that the
common case (an ordinary file, one or a few concurrent writers) never spills at all, small enough
that even several sessions running on the same machine stay within a modest, predictable memory
footprint.

Once the shared budget is exhausted, further content spills to a private temporary file on local
disk, one per file whose write cache has grown past its share of the budget. This is transparent to
DESIGN-MOUNT-007's read-after-write behavior: a read against spilled content is served from that
temporary file instead of memory, with no visible difference to the caller between the two.

This same tracking is also what DESIGN-MOUNT-006's backpressure keys off, narrowed to one part of
it: the signal that scales the delay `write()` adds is the spilled-to-disk bytes belonging
specifically to files that have already been released and are waiting on DESIGN-MOUNT-006's
background job - not a file still being actively written through an open handle, even once its own
share of the budget has spilled. A file not yet released has no background job running for it yet
(chunking/hashing only start at `release()`), so delaying its `write()` calls would not relieve
anything - the backlog that delay exists to relieve is specifically the released, not-yet-persisted
work still waiting on the shared pool. Zero such bytes spilled means zero added delay; the delay
grows smoothly as that total grows, with no fixed threshold where it turns on abruptly. This still
reuses tracking the write cache already needs for its own memory management, rather than
introducing a wholly separate backlog metric (e.g. a queue-depth count) alongside it, and ties the
delay to an actual resource under pressure for the specific backlog it can affect - memory
exhausted for released-but-unpersisted work, spilling into slower disk I/O - rather than an
indirect proxy for it.

### Alternative considered and rejected: a per-file memory budget

Giving each file's write cache its own fixed memory budget, rather than one budget shared across
the whole session, was considered and rejected: it does not actually bound the mount's memory
footprint, since REQ-PERFORMANCE-002's cross-stream parallelism means several files can genuinely
be written at once - a per-file budget only bounds how much any *one* of them can use, and the
total scales with however many are concurrently open. A shared budget bounds the total directly,
which is the property actually wanted.

### Alternative considered and rejected: backpressure over all spilled bytes, including a still-open write

Scaling the backpressure delay off every currently spilled byte in the session, including a file
still being actively written and not yet released, was considered and rejected: chunking/hashing
for such a file has not started (DESIGN-MOUNT-006 starts it at `release()`), so nothing is draining
its spillover yet - delaying its own `write()` calls could not relieve that backlog, only slow the
one write responsible for it, with the delay climbing for the rest of that single write's duration
regardless of how the background pool is actually doing. Scoping the signal to released,
already-queued work ties the delay to a backlog `write()`'s throttling can actually help drain.
