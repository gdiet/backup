# Bounded-memory, backpressured I/O for `store` and `mount --read-write`

**Status**: the core memory-bounding problem is implemented for both
commands (see "Chunk-buffer spillover: implemented" below) - the mount
worker-thread-exhaustion/backpressure problem is also implemented (see
"Mount-specific detail: implemented"). What's left open is noted inline
where relevant; nothing here still says "do not implement from this doc."
This doc supersedes `docs/plans/mount-async-persist-and-backpressure.md`'s
scope (folds its content in below) - a chat discussion starting from "what
happens with very large CDC chunks or `chunking: none`" in `store`
revealed that `store` and `mount --read-write` share the same underlying
problem (bounding memory while reading from a source and writing to a
target that can each independently be slow, without corrupting data or
needlessly re-reading/re-writing), not two separate ones.

## Requirements identified so far

Not all of these were obvious up front; several only surfaced by working
through specific scenarios in conversation. Recorded here so they don't
have to be rediscovered.

- **The source can be slow.** For `store`, "the source" is whatever
  filesystem the backed-up files live on - possibly a network share, a
  slow external drive, etc. (`mount --read-write` doesn't have this
  problem in the same shape - see "Asymmetries" below.)
- **The repository (target store) can be slow.** Explicitly not assumed
  to be a fast local disk - can be remote and/or slow, for both `store`
  and `mount --read-write`.
- **Operational expectation: the (configurable) temp directory should be
  as fast as possible.** Already the documented expectation for the
  mount's existing write-cache spillover (see `write_cache.rs`) and
  matches the Scala prototype's own README guidance ("Configure The Temp
  Directory": fast SSD, not on the same physical drive as the repository
  or the source) - any new spillover mechanism for `store` should carry
  the same expectation and probably the same `--temp`-style
  configurability.
- **Very large files must not cause OOM, even with `chunking: none`** -
  and neither should many parallel medium-sized files add up to the same
  effect. Bounding a single buffer isn't enough if enough of them run
  concurrently; the bound needs to account for aggregate usage across
  whatever's running at once.
- **Operation speed must not be needlessly throttled by reading data
  twice or writing data unnecessarily.** Ruled out an initial idea (seek
  back and re-read a chunk from the original source once it's known to be
  new) specifically because of the next point, not primarily because of
  this one - but this one matters too: a second read can be genuinely
  slower than holding data in memory, especially once the OS page cache
  has evicted it (large file, memory pressure, or enough time/other I/O
  between the two reads).
- **`store`: source files can change at any time - they must not be
  assumed immutable.** This is what actually rules out "hash now, re-read
  the bytes later if it turns out to be a new chunk": the second read
  could see different bytes than what was hashed, registering a chunk
  under a hash that doesn't match what's actually stored - silent
  corruption, not just a performance problem. (The directory used as the
  spillover/cache temp directory *is* assumed not to change from outside
  the process - only this process writes to it.)

### Additions from earlier in the same conversation, not repeated above but still load-bearing

- **The existing chunk-write race must keep being tolerated, not
  "fixed."** Two workers can independently decide the same new chunk
  needs storing and both write it (see `db::apply_backup_batch`'s
  `ON CONFLICT DO NOTHING`/`INSERT OR IGNORE` handling and the
  `racing_batches_inserting_the_same_new_chunk_resolve_to_one_chunk_row`
  test) - accepted, wasted store space, self-heals on the next run since
  `SpaceAllocator` computes gaps purely from what's actually referenced in
  `chunk_extents` (see the chat answer on this before this doc existed -
  worth folding into a real design doc once written, not repeated here).
  Whatever gets built here shouldn't need to eliminate this race, only
  avoid making it worse.
- **`chunking: none` is a real, supported configuration**, not just an
  edge case of very-large-`target_size_bits` CDC - the whole file becomes
  one chunk, so any design needs to treat "the whole file is one
  (possibly huge) unit of work" as a normal case to handle, not a
  pathological one to merely tolerate.
- **The CDC chunker itself holds no bytes** (`CdcChunker`'s fields are all
  small scalars - position/mask/fingerprint state) - only
  `BufferingHashingChunker`'s own choice to accumulate a chunk's raw bytes
  (so it can hand them back for writing) creates the buffering/memory
  concern. `HashingChunker` (no byte buffering, hash-and-discard) already
  exists and doesn't have this problem at all - relevant for whatever
  "only fetch bytes when actually needed" idea eventually replaces the
  ruled-out re-read approach.
- **`store::LongTermStore::write` already splits a given buffer across
  multiple physical LTS files internally**, but still expects one
  complete `&[u8]` per call, not a stream - relevant to whether a target
  write can itself be done piecewise (from a spilled cache file, without
  ever holding a full oversized chunk in RAM at once) even for the
  write-out step, not just the read/buffer step.

## Asymmetries between `store` and `mount --read-write` (matters for the shared-vs-separate question below)

- **Data shape**: `store` reads a source file **linearly, once, forward
  only** - chunk buffering is inherently append-only, never overwritten.
  `mount --read-write`'s `write_cache::WriteCache` has to support
  **random-access overwrites** (a `write(2)` can land anywhere, including
  overwriting bytes written moments ago) - hence its
  `LengthSpanMap`/`ByteSpanMap` machinery, which `store`'s simpler
  append-only need has no reason to carry.
- **Who controls admission**: `store` decides for itself when to start
  processing the next file (today: a flat `rayon` `par_iter` over a
  pre-built file list) - an admission-control gate ("don't start a new
  file while the shared budget is over X%") fits naturally as a pull-based
  scheduler change. `mount --read-write` doesn't control when FUSE/WinFSP
  calls `write`/`release` - it can only react from *inside* those calls
  (e.g. block before returning), it can't refuse to be called in the
  first place. A design that assumes "we choose when to admit more work"
  doesn't transfer to the mount side unmodified.
- **Where "the source" lives, memory-wise**: for `mount --read-write`,
  the data being persisted already lives in `WriteCache` (RAM/local
  spillover, entirely under this process's control) by the time a persist
  runs - there's no separate "slow, external, possibly-mutating source"
  read involved at that stage the way there is for `store`. The "source
  can be slow" requirement above is really a `store`-specific concern;
  "the target can be slow" applies to both equally.
- **Existing read-side race-avoidance**: `mount`'s phase 2b already has
  `FileWriteState::persisting`/`wait_while_persisting` to stop a
  concurrent reader from observing a torn state between "handed off to
  persist" and "actually committed" (`docs/plans/implemented/
  06-fuse-mount-readwrite.md`'s implementation notes). `store` has no
  analogous concurrent-reader-of-the-same-file concern - nothing else
  reads a file `store` is currently backing up through this mechanism.

## Shared implementation or two?

**Decided and implemented** (see "Chunk-buffer spillover: implemented"
below) - kept as a record of the reasoning, not just the conclusion.
Revised after direct pushback on an earlier, too-strong version of this section
(originally argued `store`'s append-only pattern was a "structural
mismatch" for `mount`'s random-access `WriteCache`/span-map machinery -
that doesn't actually hold up): `write_cache::WriteCache` has **no
mount/FUSE-specific coupling at all** - it's already a generic budgeted
byte buffer with disk spillover. Used in a strictly append-only pattern
(every `write` call at `position == current size`, never below, never
past it), its `clear` (overwrite) and `zero` (hole) paths are simply never
taken, and `merge_around` collapses to "extend the one existing span"
every time - equivalent to a plain growing buffer, at the cost of a small
constant per-call overhead (`BTreeMap` remove/insert vs. a plain
`extend_from_slice`) that's immaterial at the call granularity this would
actually run at (a handful of KB-sized pieces per chunk, not per byte).
The unused random-access/hole capability costs nothing at runtime when
never exercised - it's present, not active. **`WriteCache` was reused
as the shared spillable-buffer primitive for both**, rather than also
building a separate dedicated append-only type - see "Chunk-buffer
spillover: implemented" below for how.

What *doesn't* transfer directly is the layer on top: **admission
control / when to even start reading more data**, given the "who
controls admission" asymmetry above - `store`'s natural shape is a
pull-based scheduler gate (it decides when to start the next file/chunk),
`mount`'s is a block-before-returning check inside a reactive callback it
doesn't control the timing of. `store` doesn't have such a gate today
(unlike mount's persist queue, below) - `--concurrency` is a static cap
chosen up front, not a dynamic backpressure mechanism. Left as a real
open item, not addressed by the chunk-buffer-spillover work: that work
only bounds *one worker's* peak memory (down from "the whole file" to
"the shared chunk-buffer budget"), not how many workers run at once.

## Chunk-buffer spillover: implemented

Closes the "very large files must not cause OOM, even with `chunking:
none`" requirement above, for both commands - previously the actual
biggest gap, since the mount backpressure work (below) only fixed
worker-thread exhaustion, not the underlying unbounded buffering.

**Root cause** (confirmed by reading the code, not assumed): `cdc::
BufferingHashingChunker` buffers a completed chunk's raw bytes in a plain
`Vec<u8>` so a caller can write a new (non-duplicate) chunk without
re-reading the source (necessary - see "Additions from earlier" above on
why re-reading isn't safe). For `chunking: none` specifically, `cdc::
SingleChunkChunker::next` *always* returns an empty boundary list (`Vec::
new()`) and only reports the one whole-file chunk from `flush()` at EOF -
so `BufferingHashingChunker`'s `buffer.extend_from_slice(data)` runs on
every single call with nothing ever draining it, meaning the entire file
ends up in that one `Vec<u8>` before anything else can happen. The same
mechanism, just bounded by the configured max chunk size instead of the
whole file, applies to a large CDC chunk (`target_size_bits` near its
max, ~16.6 GB worst case).

**Fix**: a new type, `cli::spilling_chunker::SpillingHashingChunker<H, C,
F>`, duplicating `BufferingHashingChunker`'s exact slicing/hashing logic
but buffering each in-progress chunk's bytes in a `write_cache::
WriteCache` instead of a `Vec<u8>` - the RAM-budgeted, disk-spilling
primitive `mount --read-write`'s write cache already used (see "Shared
implementation or two?" above: this is that reuse, realized). Each
completed chunk hands back its own detached `WriteCache` (`SpilledChunk`)
via `std::mem::replace`, mirroring exactly how `BufferingHashingChunker`
hands back its buffer via `std::mem::take` - the caller looks the chunk
up in the dedup index and either drops it (a hit - `WriteCache::Drop`
releases the RAM reservation and deletes any spill file) or drains it via
a new `chunk_store::write_chunk_from_cache` (a miss - reserves store
extents exactly as the old `write_chunk_bytes` did, but writes them out
by reading the `WriteCache` back in bounded pieces rather than requiring
the whole chunk contiguously in memory; `write_chunk_bytes` itself was
removed once nothing called it anymore).

**Wiring**: `store.rs`'s `read_and_chunk`/`resolve_chunk` and `mount.rs`'s
`Inner::persist`/`resolve_persist_chunk` both switched from
`BufferingHashingChunker`+`write_chunk_bytes` to
`SpillingHashingChunker`+`write_chunk_from_cache`. `mount` reuses its
*existing* `--write-cache-mb` budget and spill directory for this (the
persist-time chunk buffer and the write cache are never both being
filled for the same bytes at a truly simultaneous peak in a way that
matters enough to warrant a second budget - see below). `store` gained
its own new `--chunk-buffer-mb` (default 128, same default as mount) and
`--allow-swap-risk` flags, plus a private spill directory created at
startup and removed at the end of the run - `check_ram_budget` (factored
out of `mount.rs` into `cli::ram_budget_check`, since both commands now
need the identical startup guard) is called for both.

**A real, accepted memory-accounting subtlety for mount**: reusing the
same `RamBudget` for both the write cache and the persist-time chunk
buffer means both can be charged against it *simultaneously* while a
large file is being persisted (the write cache still holds the original
bytes while `Inner::persist` reads through it and re-buffers into the
chunker) - this is correct, not a double-counting bug: they really are
two separate copies in memory at that point, and the shared budget
reflects that true aggregate pressure rather than hiding it.

**A latent, pre-existing bug found and fixed while wiring this up**: both
`mount.rs`'s and `store.rs`'s spill directories were named from
`std::process::id()` - unique across *processes*, but not across
*invocations within one process*. `cargo test` runs many tests
concurrently in one process, and `build_filesystem`/`run_store` are each
called once per test - two concurrent tests collided on the same spill
directory name, silently overwriting each other's spill files (same
generated filenames, since each `WriteCache`'s spill-id counter starts
fresh at 0 per instance) and racing to `remove_dir_all` it out from under
each other on completion. Fixed by naming both via `tempfile::Builder`
(genuinely unique per call) instead - caught by a new test forcing heavy
spillover (`chunking_none_with_a_zero_byte_write_cache_still_round_trips_
correctly` in `mount.rs`, `chunking_none_with_a_zero_byte_chunk_buffer_
still_round_trips_correctly` in `store.rs`), which is what actually
surfaced it (nothing before forced enough concurrent spillover to hit the
collision in practice).

**`store`'s I/O-vs-CPU concurrency split: implemented.** `--concurrency`
still sizes one rayon pool for the whole read/chunk/hash pipeline, but that
conflated the hardware-optimal degree of parallelism for CPU-bound
chunking/hashing (≈ core count) with that of the I/O calls the same
workers make into `LongTermStore` (disk/network-share dependent, can be
much smaller or larger). Added `--store-io-parallelism` (`cli::io_limiter::
IoLimiter`, a plain `Mutex`+`Condvar` counting semaphore) - when given, every
individual `store::LongTermStore::write` call inside `chunk_store::
write_chunk_from_cache` acquires a permit first (per write piece, not per
whole chunk), blocking the calling worker until one is free. Deliberately
*not* a second thread pool with a channel-based API on `LongTermStore`
itself: every call site already blocks its calling thread until its own
I/O call returns, so a second pool would only add a cross-thread hand-off
(and, if built as an actor behind a channel, would force copying the
read/write buffer across it, undermining the zero-copy handle-cache work
above) without letting the caller do anything else meanwhile - a semaphore
gates access in place for less overhead and no new threads. Lives in `cli`
(`io_limiter.rs`), not in the `store` crate: how much I/O concurrency is
appropriate is a property of the workload/deployment (which command is
running, what's behind the repository), not of `LongTermStore` itself, which
stays the simple, stateless-per-call, call-from-any-thread primitive it
already was - unaware of how many callers exist or how they're scheduled.
`write_chunk_from_cache`/`RunContext` take an `Option<&IoLimiter>` so
`mount.rs`/`migrate_scala_repo.rs` (which don't have this flag) simply pass
`None`, unchanged behavior. Only wired into `store`'s write path so far -
`mount`/`restore`/`check`'s read/write paths could reuse the same
`IoLimiter` later if needed, not done here (no concrete need yet).

**Still open**: `store`'s own *admission* control (noted above - a static
`--concurrency`, not a dynamic backpressure gate like mount's persist
queue). `--store-io-parallelism` bounds concurrent I/O *once a worker
already started a file*, it doesn't gate *when* a new file starts being
read/chunked in the first place - that remains the open item. The temp/spill directory location is now configurable for both
commands via `--temp <DIR>` (`cli/src/temp_dir.rs`, wired into both
`store.rs`'s and `mount.rs`'s spill-directory creation) - closing the
"operational expectation" requirement above; both previously hardcoded
`std::env::temp_dir()` with no way to override it. `--temp` is validated
up front (must already exist and be writable) before any other command
work, the same way `check_ram_budget` already was.

## Mount-specific detail: implemented

The mount side of this problem (below) is now implemented (`cli/src/
mount.rs`) - a bounded `mpsc::sync_channel<PersistJob>` (capacity
`PERSIST_QUEUE_CAPACITY = 4`) plus a single dedicated background thread
(`persist_worker`) that every persist now actually runs on. `release`
(closing a dirty file) and bare `truncate`/`O_TRUNC` (no open handle) both
hand their `WriteCache` off via `Inner::enqueue_persist` instead of
persisting on the calling FUSE/WinFSP worker thread - that call only
blocks once `PERSIST_QUEUE_CAPACITY` persists are already queued ahead of
it, which is the actual backpressure point. `Inner` (holding all the state
`DedupFs` used to own directly) is wrapped in `Arc` and shared between the
FUSE/WinFSP dispatch threads and this one background thread; `DedupFs`
itself is now just a thin `MountFilesystem`-forwarding wrapper around
`Arc<Inner>`.

This directly fixes the worker-pool-exhaustion failure mode described
below: for the first `PERSIST_QUEUE_CAPACITY` closes in a burst, `release`
now returns almost immediately (enqueueing is fast; the actual slow I/O
happens on the one background thread instead), keeping every other
FUSE/WinFSP worker thread free to service unrelated requests throughout.
Only once the queue is genuinely full does a *new* close start blocking -
at that point it degrades to (but never worse than) today's-shipped
synchronous behavior, applied to a bounded number of worker threads
instead of all of them. This is deliberately a simpler mechanism than
Scala's `cacheLoadDelay` sleep formula (see below) - not ported, for the
same reasons noted there - but achieves the same practical goal (smooth a
burst, only degrade under genuinely sustained overload) with primitives
that don't need an unverified tuning constant.

Serializing persists onto one background thread (mirroring Scala's own
single background persist thread) also means at most one persist is ever
actually writing to the store at a time, which incidentally makes the
pre-existing, deliberately-tolerated chunk-write race (`db::
apply_backup_batch`'s `ON CONFLICT DO NOTHING` handling) less likely to
fire, not more - not a goal, just a side effect worth noting.

**A correctness gap closed as a side effect**: making persist
asynchronous meant bare `truncate`/`O_TRUNC` (no open handle) could no
longer rely on the calling thread blocking for the whole persist to keep
a racing `open`/`read`/`getattr` from observing stale pre-truncate content
after the `truncate(2)` call had already returned success. Fixed by
registering a `persisting = true` placeholder in `write_states` for that
tree id in the *same* lock hold as the "does a handle already exist"
check, before ever enqueueing - closing a window that, on inspection,
already existed (for the whole duration of the synchronous persist, not
just briefly) in the shipped synchronous code too, since it never
registered any placeholder for the bare-truncate case at all. Covered by
`mount::tests::bare_truncate_without_a_handle_persists_before_a_racing_read_returns`
(uses the real `truncate(1)` utility - deliberately not
`OpenOptions::truncate(true)`, which goes through `open`'s `write_intent`
flag rather than `MountFilesystem::truncate` and so doesn't actually
exercise this path at all, a real trap this doc is recording for next time).

The `PERSIST_QUEUE_CAPACITY` constant is fixed, not a CLI flag - see its
doc comment in `mount.rs` for why (queued jobs are already RAM-budgeted/
spillover-bounded the same as any other open file, so this only bounds
how many recently-closed files can have unpersisted changes in flight at
once, not memory directly).

What's *not* addressed here: `write` itself still isn't throttled beyond
the existing RAM-budget-driven spillover, deliberately - writes only
touch the local write cache, decoupled from the slow-target-disk concern
until persist time (see "Where 'the source' lives, memory-wise" above),
so there was no failure mode there to fix. And the `write_conn` mutex
contention noted below is unchanged (if anything, slightly reduced, since
persist's own `apply_backup_batch` commits now come from one thread
instead of potentially several).

### Original write-up (context for the above)

Kept here rather than lost - `mount`'s side of this problem was written
up in isolation first, before the `store` discussion revealed the shared
scope; the mount-specific mechanics below explain the failure mode the
implementation above fixes.

**Previously shipped behavior** (now superseded by the above):
`DedupFs::persist` ran synchronously
inside `release` (a file's last close) and inside a bare `truncate`/
`O_TRUNC` with no open handle - on whatever thread FUSE/WinFSP dispatched
that call to. No background queue, no backpressure: `write` calls are
never throttled regardless of how far behind the target disk is. On a
slow repository disk with a fast temp/cache disk, writing/closing many
files proceeds at the *temp* disk's speed right up until each file is
*closed* - at that point `release` blocks for as long as that file's
chunk-and-store pipeline takes against the *slow* disk. Because FUSE/
WinFSP dispatch to a bounded worker-thread pool, a burst of closes faster
than the slow disk can absorb will eventually occupy every worker thread
with a blocked persist - the mount then stops servicing *any* new
request, including unrelated reads on other files, until a thread frees
up (a hard stop, not a gradual slowdown; no data is lost - `FileWriteState
::persisting`/`wait_while_persisting` still behaves correctly, just
slowly). The single shared `write_conn` mutex is a secondary, narrower
bottleneck on top: every persist's final `apply_backup_batch` commit, and
every `mkdir`/`create`/`unlink`/`rename`/`utimens` call, contends on it.

**What the Scala prototype does differently** (ported for the cache
tiers, not this part): hands each closed file off to a single background
thread (a serial queue, not parallel) and applies backpressure in `write`
itself - `cacheLoadDelay = bytesInPersistQueue * persistQueueSize /
1_000_000_000` milliseconds, checked and slept on *before* accepting each
write chunk. Rising backlog smoothly increases the delay applied to *new*
writes, self-stabilizing rather than hard-blocking. Not ported in phase
2b: the formula's units/scale couldn't be verified without the original's
tuning history, and the multi-generation "persisting queue" (a file can
be written, closed, reopened, and rewritten again before the first flush
completes) added real complexity for uncertain benefit at the time.

**`FileWriteState::persisting`/`wait_while_persisting`** (phase 2b's
release/persist race fix, see `docs/plans/implemented/
06-fuse-mount-readwrite.md`'s implementation notes) should still work as
the read-side correctness mechanism under whatever async pipeline gets
built here - reads/re-opens of a file already queued for persist still
need to wait for *that* persist to actually land, whether it runs
synchronously or on a background thread.
