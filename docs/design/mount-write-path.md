# Mount Write-Path Scheduling

How the mount write path (REQ-MOUNT-003's real content writes in
[`../../requirements/functional/mount.md`](../../requirements/functional/mount.md) -
`create`/`write`/`truncate`/`unlink`) schedules its work.

## DESIGN-MOUNT-006: Separate dispatch pool from chunking/storage-write pool; non-blocking `release()`
Status: implemented (crates/cli/src/settle_pool.rs, crates/cli/src/dedup_fs.rs)

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

### The delay formula (`crates/cli/src/backpressure.rs`)

The delay grows linearly with `backlog_spilled_bytes` and, deliberately, with the size of the
`write()` call it is being added to, capped at a fixed maximum regardless of either input. Scaling
by call size specifically keeps the delay's effective throttle - bytes of `write()` payload allowed
through per second, below the cap - independent of whatever write granularity the calling tool
happens to use, rather than punishing a tool that flushes in small chunks far harder than one
moving the same total bytes in large ones purely because it makes more, smaller calls. This was
verified as a real effect, not a theoretical one: a calibration mount session on WSL2/Linux driving
a real libfuse3 mount observed ~8 KiB as a typical `write()` length from ordinary buffered client
I/O (`std::io::BufWriter`'s default capacity) - well under the ~128 KiB a size-oblivious, purely
backlog-scaled version of this formula would implicitly need to assume as "the" write size for its
effective-bandwidth reasoning to hold.

The formula's slope constant and its 250 ms cap are anchored to the Scala predecessor's own
severity, not derived from first principles: `Backend.scala`'s equivalent delay reached its own
"severe but working" point (250 ms per 32 KiB persist-queue chunk, ~131 KiB/s effective) once its
signal - all queued bytes, not just spilled ones, further multiplied by queue *file count* - grew
large. This project's signal is narrower (DESIGN-MOUNT-010's spill-only bytes, no file-count
factor, since the common case never spills at all - see that section below) and a spilled byte here
is already a worse state than a merely-queued Scala byte, so the same 250 ms cap is kept, chosen to
land at the same effective-bandwidth floor (~512 KiB/s) once backlog reaches the level of a
genuinely severe, sustained overflow, independent of the caller's own write size. The cap itself
also bounds worst-case per-call latency directly - a FUSE/WinFSP dispatch thread blocked this long
is one fewer available for every other concurrent request meanwhile - which matters most for a
caller using unusually large individual `write()` calls, where the backlog-and-size-scaled delay
before capping would otherwise grow furthest past it.

Deliberately left out of this formula: a multiplicative factor for how many files are currently
queued (Scala's second factor) - `JobPool` tracks no such count, and revisiting that is its own,
separate, deliberately deferred scope.

How a failure discovered only during this background processing - after `release()` has already
returned success - gets surfaced is DESIGN-MOUNT-009 below; a queryable or mount-browsable form of
that same record beyond its first version's plain log file is left open, see
[`../../requirements/open-questions.md`](../../requirements/open-questions.md)'s "Queryable/
mount-browsable surfacing of background write failures". How REQ-MAINTENANCE-004's single-writer
scope relates to a background job outliving the FUSE call that started it turned out not to need an
answer at this level at all - see DESIGN-MOUNT-008 below.

## DESIGN-MOUNT-007: Same-session reads see a file's not-yet-persisted state
Status: implemented (crates/cli/src/pending_files.rs, crates/cli/src/dedup_fs.rs)

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
Status: implemented (crates/cli/src/mount.rs)

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
Status: implemented (crates/cli/src/failure_log.rs, crates/cli/src/dedup_fs.rs)

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
Status: implemented (crates/cli/src/write_cache.rs)

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

## DESIGN-MOUNT-012: The write cache tracks only session-written byte ranges, not a full copy
Status: implemented (crates/cli/src/write_cache.rs)

Opening an already-existing file for writing does not copy its current content into the write
cache up front. The cache starts empty and records only the byte ranges this session actually
writes - a small set of (position, bytes) entries, each entry replacing or splitting whatever
existing entries it overlaps, the same way `write()` at a given offset always behaves. Reading a
range not covered by any entry - a byte a client has not touched this session - falls back to the
file's content as it stood when this write session began: read through `crates/db`'s existing
content-resolution path (the same lookup a read-only open already needs) and `crates/store`,
resolving that logical range against the pre-existing content's own `chunk_extents`. A range
beyond both the tracked entries and that original size (grown by `truncate`) reads as zero.

This scales with how much of a file a session actually changes, not with the file's total size - a
one-byte change to a multi-gigabyte file costs one small cache entry and, later, one small
settling read for that byte plus whatever untouched bytes the resulting chunk boundaries happen to
span, not a full copy of the file into the cache the moment it is opened. `crates/store` still
being read-only-safe to use concurrently with itself (DESIGN-STORE-002) is what makes reading the
original content on demand, interleaved with the session's own in-progress writes, unproblematic.

Settling a write (DESIGN-MOUNT-011) still needs the file's complete resulting byte stream to chunk
and hash - the entries recorded here and the original-content fallback together are what a settling
job iterates over, in position order, to reconstruct it; neither on its own is the complete
picture.

### Alternative considered and rejected: copying the existing content into the cache on open

Reading a file's entire existing content into the write cache the moment it is opened for writing,
so every subsequent read is answered purely from the cache with no original-content fallback
needed, was considered and rejected: it makes the cost of opening any existing file for writing
proportional to that file's total size regardless of how much of it a session actually intends to
change, including immediately spilling most of it back out to disk under DESIGN-MOUNT-010's memory
budget for a large file - real, avoidable cost paid on every open, not just an implementation
inconvenience.

## DESIGN-MOUNT-013: Per-file write caches form a chain, so a fresh write-intent open never blocks on a still-settling one
Status: implemented (crates/cli/src/pending_files.rs)

DESIGN-MOUNT-007 requires looking up a file's not-yet-persisted state by file identity, but leaves
open what that lookup finds when a client releases every handle on a file, then opens it again for
writing before DESIGN-MOUNT-006's background job for the just-released write has finished settling.
Ordinary POSIX same-host coherence - the property DESIGN-MOUNT-007 extends across the whole
still-settling window - gives two guarantees at once that a real filesystem never has to reconcile
explicitly, because it has no comparable asynchronous-settling window at all: every reader sees one
single, up-to-date, coherent file, and a writer is never made to wait on outstanding I/O it did not
itself ask for. Both are asked for here directly: the fresh open must not block on the older job,
and it must still read exactly what that older job is in the middle of committing, for any byte
range it has not itself touched yet.

The file's not-yet-persisted state is therefore not one write cache but an ordered chain of them: at
most one active generation, currently receiving writes through an open handle, plus zero or more
settling generations behind it, each with its own DESIGN-MOUNT-006 job in flight, oldest job first.
Releasing the last handle on the active generation does not settle it inline - it moves to the back
of the settling chain, handed to DESIGN-MOUNT-006's pool, exactly as it already does today with a
single generation. A write-intent open that finds no active generation - the file's first write this
session, or every previous handle already released - creates a brand new one immediately, without
waiting for anything already in the settling chain to finish; this extends DESIGN-MOUNT-006's
non-blocking `release()` to opens as well, for the same reason: blocking here would tie a file's
write throughput to how far behind the shared pool happens to be for that one file, serializing a
workload that rewrites the same file repeatedly and quickly (an editor's save, a periodically
rewritten log) onto the pool's own backlog instead of just adding another lightweight link.

Correctness without blocking follows directly from DESIGN-MOUNT-012's fallback already being an
open-ended resolver, not necessarily the durably committed content: a new generation's fallback is
the chain's next-older generation, resolved against the live chain at the moment of the read, not a
copy taken when the new generation was created. Reading the newest generation therefore transparently
sees every older generation's writes for any range it has not itself touched, however many
generations are currently chained, terminating at the durably committed content
(`crates/db`/`crates/store`) once nothing older remains. A new generation's own `original_size`
(DESIGN-MOUNT-012) is likewise the next-older generation's size, not the durably committed size
directly, so the file's logical size stays correct while more than one generation is in flight.
Resolving against the live chain rather than a snapshot also means a generation's memory is freed the
moment its job settles and it is removed from the chain: a still-open hole that would have fallen
through it instead falls through to the durably committed content the job just produced, which is
exactly what removing that generation from the chain represents.

DESIGN-MOUNT-010's backpressure already scales `write()`'s delay off the released-but-unsettled
spilled bytes for a file, regardless of how many separate settling generations that total is spread
across - a client reopening and rewriting a file faster than the pool can drain it slows down through
that existing mechanism rather than needing a separate cap on chain depth.

### Alternative considered and rejected: blocking a fresh write-intent open until the file's settling job finishes

Waiting for a file's settling generation to finish before letting a new write-intent open proceed
was considered and rejected as the simpler-looking option: it reintroduces the coupling
DESIGN-MOUNT-006's non-blocking `release()` was built to avoid, one layer later - a file's write
throughput would depend on how far behind the shared pool happens to be, rather than on the client's
own pace.

### Alternative considered and rejected: copying a still-settling generation's tracked ranges into the new one eagerly

Copying the previous generation's own tracked byte ranges into a fresh generation at creation time,
rather than falling back to it live through the chain, was considered and rejected: it is
DESIGN-MOUNT-012's copy-on-open problem recurring one layer down, making creating a new generation
cost proportional to how much a still-in-flight previous generation had tracked, instead of the
constant-time operation it can otherwise stay.

## DESIGN-MOUNT-014: A newly created file gets a synthetic, session-local identity before it has a `tree_entries` row
Status: superseded-by DESIGN-MOUNT-015

## DESIGN-MOUNT-015: `create()` settles the canonical empty content immediately, giving a new file a real identity from the start
Status: implemented (crates/cli/src/dedup_fs.rs)

DESIGN-MOUNT-013's chain is keyed by file identity, which DESIGN-MOUNT-007 already requires -
straightforward for a file that already has a `tree_entries` row before this session ever touches
it, since that row's own id is exactly the identity to key on. A file this session `create()`s has
no such row yet under DESIGN-METADATA-008's "not-yet-settled files stay out of the database"
(`metadata-schema-with-contents-table.md`), which DESIGN-MOUNT-014 took as fixed and worked around
with a parallel, session-local identity space and a path index to resolve it from a bare path - real
complexity, duplicating machinery DESIGN-MOUNT-013 already built for the ordinary case of a
`tree_entries` row that already exists.

`create()` instead settles the file's content as the canonical empty content immediately - the same
zero-chunk, zero-length `contents` row [`crate::content::find_or_create_content`] already produces
for any file that happens to end up empty - via the ordinary [`crate::tree::settle_file`] call,
before returning a handle at all. This is not a placeholder or a sentinel: it is a real, valid,
already-meaningful state ("this file exists; its content is presently empty"), the exact same state
a client would see from `open(O_CREAT | O_TRUNC)` on an ordinary filesystem before writing anything.
A file this session later actually writes to and releases then settles exactly the way overwriting
any other existing file already does (DESIGN-MOUNT-011): a new `tree_entries` row, never an in-place
`content_id` update, so this creates no exception to that decision's own reasoning - a `create()`
immediately followed by real content simply produces two ordinary history entries instead of one, no
different in kind from an ordinary quick double-save.

The entire DESIGN-MOUNT-014 apparatus becomes unnecessary as a result: a newly created file has a
real `tree_entries.id` from the moment `create()` returns, so `getattr`/`readdir`/a second `open`
all resolve it exactly the way an existing file's overwrite-in-progress already needs to (the pending
generation's own current size, from DESIGN-MOUNT-013's chain, taking precedence over whatever size
is currently on the settled row) - one mechanism for both cases, not two. A collision between two
concurrent `create()` calls for the same new name is resolved by `settle_file`'s own existing
collision handling (the same uniqueness enforcement an ordinary overwrite's collision already relies
on), rather than a second, purpose-built check in a separate in-memory index.

This is also a closer match to genuine POSIX `creat()` semantics than DESIGN-MOUNT-014's approach
was: a real filesystem makes an empty file visible - to every process, not only the one that created
it - the instant `create()` returns, before any content has been written at all. Deliberately keeping
that same moment invisible outside the current session (as DESIGN-MOUNT-014's synthetic,
session-local identity would have) was not something REQ-TREE-006 actually required; REQ-TREE-006 is
about a *write's content* becoming visible to a different process only once complete, not about
whether the empty file's mere existence is visible immediately.

### Known limitation: a lagging settle job can resurrect a file's name after a racing `unlink`

If a client releases a written generation (queuing its DESIGN-MOUNT-006 settle job), then unlinks
the file before that job finishes, the job's own `settle_file` call - not knowing the name was
removed in the meantime - finds nothing live at that name anymore and inserts a fresh entry there
regardless, effectively resurrecting the name with the settled content. This is not new to this
decision (the same race exists for an ordinary overwrite's settle job racing a concurrent `unlink`,
independent of how a new file's first content came to be); it is called out here because
`create()`-then-quickly-`unlink()` is the shape most likely to surface it in practice, e.g. a client
that creates a temporary file and removes it again while a slow settle job is still catching up.
Closing this gap needs the settle job to notice a name was removed out from under it before
committing - not built yet; left as a known limitation of this first version rather than blocking it,
the same way DESIGN-MOUNT-009's failure handling and DESIGN-MOUNT-010's Windows sparse-file behavior
are each their own explicitly tracked gap rather than a silent one.

### Alternative considered and rejected: DESIGN-MOUNT-014's synthetic, session-local identity plus a path index

Assigning a newly created file its own identity space, disjoint from real `tree_entries.id` values,
with a separate in-memory `(parent, name) -> identity` index to resolve it from a bare path, was the
first decision reached here and is superseded by this one: it worked, but at the cost of a second,
parallel bookkeeping structure alongside DESIGN-MOUNT-013's chain, entirely to work around treating a
newly created file's initial state as "nothing yet" rather than "a real, empty file" - a distinction
that, once examined, was not actually load-bearing for anything DESIGN-METADATA-008 or
DESIGN-METADATA-004 require.

### Alternative considered and rejected: a placeholder `tree_entries` row with a sentinel standing in for "content not resolved yet"

Giving a newly created file a real database row right away, but with some sentinel (e.g. a null or
otherwise out-of-band `content_id`) standing in for "content not resolved yet" until settling
replaces it, was considered and rejected - this is what distinguishes it from the chosen approach
above, which uses a real, valid, already-meaningful `content_id` rather than a sentinel. A sentinel
would directly conflict with DESIGN-METADATA-004's decision (in
[`metadata-storage.md`](metadata-storage.md)) against building `content_id` update-in-place
machinery, and with DESIGN-METADATA-008's schema-level `CHECK` that a live file entry always has a
non-null `content_id` - relaxing either specifically to accommodate this one case would reintroduce
exactly the in-place-mutation and partial-state complexity those decisions were written to avoid,
for every reader of the schema, not only the mount's own write path.

## DESIGN-MOUNT-016: A `create()`-only empty placeholder, still untouched at its first real settle, is hard-deleted instead of historized
Status: implemented (crates/db/src/tree.rs, crates/cli/src/pending_files.rs, crates/cli/src/settle_pool.rs)

DESIGN-MOUNT-015's empty-content row exists purely so a newly created file has a real identity from
the moment `create()` returns - it is never independently meaningful the way an ordinary file's
content is. Left to DESIGN-MOUNT-011's ordinary rule, the file's first real write still soft-deletes
it as its own history entry: a permanent, empty, otherwise-content-free row for every file this mount
ever creates and then actually writes to - by far the common case, not an edge case, so this cost is
paid on essentially every file a client saves through the mount.

When the entry a settling job is about to replace is still exactly the row `create()` itself
inserted for this same file, unmodified since, that row is hard-deleted (`DELETE`, not the ordinary
soft-deleting `UPDATE ... SET deleted_at`) before the new, real-content row is inserted - it carries
nothing worth recovering (an empty file, that specific empty state never actually asked for by a
client, only ever a byproduct of how a new file's identity gets established) and correctly leaves
DESIGN-MOUNT-011's ordinary history-preserving replacement as the rule for every other case,
including a file that already existed with genuinely empty content before this session touched it.
This still needs no `content_id` update-in-place: the replacement is `DELETE` then `INSERT`, the same
shape DESIGN-MOUNT-011 already uses, just choosing `DELETE` instead of the soft-deleting `UPDATE` for
this one specific, narrow case - `tree_entries_ref_count_ins`/`_del`
(`metadata-schema-with-contents-table.md`) already correctly maintain `contents.ref_count` across a
plain `DELETE`, so no new trigger is needed either.

### Eligibility: only the file's own first generation, only when `create()` itself is what put it there

Two conditions together, both necessary:

- **Structural**: the settling generation must be the first one this session ever created for this
  file identity - DESIGN-MOUNT-013's chain never applies this to a second or later write, which
  always replaces a generation that itself holds real, already-meaningful content (even if that
  content happens to be empty - a deliberate `truncate` to zero on a file with prior content is a
  real, user-initiated state, not a byproduct, and REQ-TREE-004 in
  [`../../requirements/functional/tree.md`](../../requirements/functional/tree.md) already commits to
  every deletion staying its own independently recoverable history entry regardless of the deleted
  content's size - collapsing it would quietly carve out an undocumented exception to that guarantee).
- **Provenance, not just current content**: the row must be one `create()` itself inserted this
  session, not merely a row that happens to currently hold the canonical empty content - a file that
  was already empty before this session ever opened it is not this mechanism's concern, for the same
  REQ-TREE-004 reason above. This is recorded at `create()` time (`crates/cli/src/pending_files.rs`),
  not inferred later from the content itself.

### Safety: re-verified live, by id, inside the same transaction as the replacement

Since a settling job can run an arbitrary amount of time after it was queued (DESIGN-MOUNT-006's
non-blocking `release()`), what was eligible when queued is re-verified live at the moment of the
actual replacement, not trusted from when the generation was created: the row `create()` inserted
must still be exactly what is live at that `(parent_id, name)` right now. Skipping this check would
let a job that has become stale hard-delete whatever now actually occupies that name - not merely a
missing history entry (the acceptable cost `create()`-then-quick-`unlink()` already risks, per
DESIGN-MOUNT-015's own "Known limitation") but active loss of an unrelated, possibly
already-real-content entry: a second generation already chained and settled ahead of this one leaves
a real-content row at that id or name; an `unlink()` followed by a fresh, unrelated `create()` at the
same name leaves a different file's id there entirely.

Checking the row's own id already proves this, with no separate check of its content needed:
`tree_entries.id` is `AUTOINCREMENT` specifically so an id, once used, is never reused by a later row
(`metadata-schema-with-contents-table.md`'s "Why `tree_entries.id` is `AUTOINCREMENT`") - so a row
still live under the exact id `create()` returned is, by construction, still that exact row, holding
whatever content it was inserted with (never mutated in place, per DESIGN-MOUNT-011's own reasoning) -
still the canonical empty content, unconditionally.

### Alternative considered and rejected: collapsing whenever the entry being replaced currently holds empty content, regardless of provenance

Checking only "is the content about to be replaced empty" - without also requiring it to be a row
`create()` itself inserted this session - was considered and rejected: it reads as a strictly more
useful generalization (why not also collapse an already-empty file's first real write, not just a
brand new one?), but it quietly narrows REQ-TREE-004's "every deletion stays its own recoverable
history entry" guarantee for every already-empty file, not just this mechanism's own byproduct rows -
a real product guarantee to weaken, not an implementation detail to optimize, and not this design
decision's place to make unilaterally.
