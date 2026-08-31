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

Each spillover file is created sparse, and written to only at the exact positions a client's own
writes land at - a client writing non-contiguously (a scattered write pattern, or a large
`truncate`-then-fill sequence) leaves the gaps between those positions as real holes, not zero
bytes actually committed to disk. A filesystem that supports sparse files then never allocates real
space for those gaps, whatever fraction of the file they end up being. On Linux this happens
automatically - an ordinary file written at scattered offsets is already sparse, with no separate
step needed. A Windows build needs to request this explicitly (`FSCTL_SET_SPARSE`) before writing
to a spillover file at all, since NTFS treats a plain file as fully allocated by default; without
that call, a scattered write pattern would needlessly consume real disk space for every gap in
between.

### Alternative considered and rejected: writing spillover files in full, without sparse support

Always writing a spillover file's full byte range - zero-filling gaps between a client's actual
write positions instead of leaving them as holes - was considered and rejected: it defeats the
purpose of bounding memory usage with a byte budget in the first place if the resulting disk usage
can still be much larger than the content actually written, for exactly the workloads (scattered
writes, a `truncate`-then-partial-fill pattern) most likely to spill in meaningful volume to begin
with.

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

## DESIGN-MOUNT-011: Overwriting an existing file's content creates a new history entry
Status: implemented (crates/db/src/tree.rs)

Settling a background write job (DESIGN-MOUNT-006) for a file that already had a live
`tree_entries` row - as opposed to one just `create()`d - never updates that row's `content_id` in
place. DESIGN-METADATA-008 in
[`metadata-schema-with-contents-table.md`](metadata-schema-with-contents-table.md)'s "a
`tree_entries` row is inserted exactly once, already at its final `content_id`" applies uniformly to
every settling write, not only a brand new file: the previously-live entry is soft-deleted
(REQ-TREE-002 in [`../../requirements/functional/tree.md`](../../requirements/functional/tree.md))
and a new entry is inserted at the same path, holding the just-resolved `content_id`. The two
entries share no identity - the new one gets its own id, becoming a separate REQ-TREE-004 history
entry for that path, addressable and recoverable the same way any other deletion at that path
already is.

This is a direct consequence of DESIGN-METADATA-004's decision (in
[`metadata-storage.md`](metadata-storage.md)) not to build an `AFTER UPDATE OF content_id` trigger,
applied consistently rather than carving out an exception for the mount's overwrite case
specifically: introducing a real in-place `content_id` update for this one call site would reopen
exactly the ref-count-trigger complexity that decision rejected building at all, and would give a
mount-written overwrite a weaker survivability guarantee (recoverable only via a separate metadata
backup, REQ-MAINTENANCE-001 in
[`../../requirements/functional/maintenance.md`](../../requirements/functional/maintenance.md)) than
an ordinary delete-then-recreate already has.

REQ-TREE-005 is unaffected by this: soft-deleting and reinserting at the same path changes the
entry's own identity, not its parent's set of live entries as a whole, so the parent's own
modification time is not touched by this - same as any other pure content modification.

Rationale: an ordinary save through the mount (a text editor, a periodically rewritten log or VM
image) becomes a history entry every time, which does mean a frequent-save workload grows history
proportionally - but REQ-STORAGE-004 in
[`../../requirements/functional/storage.md`](../../requirements/functional/storage.md)'s
reclamation (with its own caller-chosen minimum age before an entry becomes eligible) already
exists to bound that growth without losing recent recoverability, and chunk/content-level
deduplication (REQ-STORAGE-001/002, same file) means only genuinely new bytes cost real storage
regardless of how often the metadata row itself gets replaced.

### Alternative considered and rejected: backpressure over all spilled bytes, including a still-open write

Scaling the backpressure delay off every currently spilled byte in the session, including a file
still being actively written and not yet released, was considered and rejected: chunking/hashing
for such a file has not started (DESIGN-MOUNT-006 starts it at `release()`), so nothing is draining
its spillover yet - delaying its own `write()` calls could not relieve that backlog, only slow the
one write responsible for it, with the delay climbing for the rest of that single write's duration
regardless of how the background pool is actually doing. Scoping the signal to released,
already-queued work ties the delay to a backlog `write()`'s throttling can actually help drain.
