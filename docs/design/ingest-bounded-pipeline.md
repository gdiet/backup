# Ingest Content Pipeline

How `dfs ingest` (REQ-INGEST-001 in
[`../../requirements/functional/ingest.md`](../../requirements/functional/ingest.md)) turns source
file bytes into chunked, deduplicated, stored content within DESIGN-MEMORY-001's RAM budget (in
[`ram-budget.md`](ram-budget.md)).

## DESIGN-INGEST-001: Bounded, per-file-sequential, cross-file-parallel chunk pipeline
Status: implemented (crates/cli/src/ingest.rs)

Because REQ-STORAGE-003 no longer offers a whole-file chunking mode and caps content-defined
chunking's target size at 23 bits (96 MiB maximum chunk size), a source file - however large - can
always be processed chunk by chunk within a small, fixed memory allowance: read sequentially from the
source until a chunk boundary is found, hash and dedup-check that chunk immediately
(`crates/cli/src/settle.rs`'s existing `Settler::complete_chunk`), write it if new, then continue
reading for the next chunk. No file needs to be held in memory in full, and no spillover-to-disk
mechanism (DESIGN-MOUNT-010's mount-side answer to the same underlying problem, in
[`mount-write-path.md`](mount-write-path.md)) is needed here at all - the 96 MiB ceiling on any one
chunk is itself the bound.

Before starting, ingest checks the repository's own configured `cdc_target_size_bits`-derived maximum
chunk size against the available RAM budget (DESIGN-MEMORY-001 in
[`ram-budget.md`](ram-budget.md)) and refuses with an actionable error
(REQ-OPERABILITY-004 in
[`../../requirements/non-functional/operability.md`](../../requirements/non-functional/operability.md))
if it does not fit - a repository configured with smaller bits needs a correspondingly smaller
minimum budget to run at all, rather than unconditionally requiring room for the 23-bit ceiling's own
worst case.

Within that budget, multiple source files are processed in parallel - `N = min(ram_budget /
max_chunk_size, available_parallelism())` files in flight at once, each on its own worker thread (a
fixed-size pool of the same shape as `crates/cli/src/settle_pool.rs::JobPool`, applied here to whole
source files instead of mount write-cache generations, but with each submission individually
waitable rather than fire-and-forget - a directory's own REQ-INGEST-005 mtime touch must wait for
that directory's own direct file children to actually finish settling) - because CPU-bound
hashing/CDC scanning, not source read or store write I/O, is expected to be the limiting factor for
the common case of a fast source and a mostly-unchanged tree (REQ-PERFORMANCE-002 in
[`../../requirements/non-functional/performance.md`](../../requirements/non-functional/performance.md)).
Each individual file is processed strictly sequentially, one chunk at a time, on its own thread:
intra-file parallel chunking is deliberately not built - see "Alternative considered and rejected:
intra-file parallelism" below.

Directory traversal itself stays synchronous: recursing into a subdirectory blocks until that
subdirectory's own recursive call returns, and only that subdirectory's direct file children are
dispatched to the worker pool. Each directory level waits for all of its own submitted file jobs
before applying its own mtime touch, avoiding a race between an in-flight file settle (which bumps
its parent directory's mtime as a side effect) and the explicit, REQ-INGEST-005-mandated mtime
override for that directory.

### Alternative considered and rejected: intra-file parallelism

Splitting one large source file's own chunk-boundary scan across multiple threads was considered and
not built: this project's expected workload is many files and large aggregate volume, not a few
individual files large enough for single-file throughput to be the bottleneck (per DESIGN-CDC-002's
own reasoning against forced parallel-chunking boundaries, in
[`cdc-chunking.md`](cdc-chunking.md)) - the cross-file parallelism above already provides the
available speedup for that shape of workload. Revisit only if a concrete workload with very large
individual files, on storage fast enough for single-thread chunking speed to become the limiting
factor, turns up - none is known today.

### Alternative considered and rejected: per-file read/persist pipelining

Overlapping one file's own read/CDC/hash work for its next chunk with the current chunk's
still-in-flight persist - a lightweight, single-file analogue of DESIGN-MOUNT-010's mount write
cache, without that cache's shared-budget or spillover machinery - was considered and not built.

Writing `a` for read/CDC/hash time and `b` for persist time, one chunk costs `a + b` processed
sequentially versus close to `max(a, b)` pipelined, for a speedup ceiling of `(a + b) / max(a, b)`:
exactly `2x` (half the wall time) only where `a` and `b` are close to equal, falling back toward
`1x` (no benefit) as either stage comes to dominate the other - a fast local read/hash against a
slow persist target, or the reverse, both erode the ceiling toward zero. This ceiling is only
reachable at all when a file is effectively running alone: cross-file parallelism above already
provides the same overlap across every other concurrently active file, so pipelining inside one
file adds nothing where enough other files are running to keep the worker pool busy. The narrower
case where it could still matter - REQ-PERFORMANCE-002's own explicitly out-of-scope shape, a batch
dominated by very few large files, or any batch's natural tail-off as its smaller files finish
first - is real, but even there the reachable gain is a modest, bounded constant factor, not an
unbounded one. Given that ceiling, and REQ-PERFORMANCE-002's own rationale for treating this shape
of workload as secondary, no such pipelining is built for now. Revisit only if practical use turns
up a workload where it would measurably matter - none is known today.

### Supersedes `settle-whole-file-memory-bound.md`'s two-pass approach

[`settle-whole-file-memory-bound.md`](settle-whole-file-memory-bound.md) explored bounding
`Settler::chunk_buffer` for the (now-removed) whole-file chunking mode, where a single chunk covered
an entire file and could therefore grow arbitrarily large. With whole-file mode gone and every chunk
capped at 96 MiB by construction, that buffer's worst case is already the small, fixed bound this
decision relies on directly - no separate two-pass hash-then-write scheme is needed to achieve it.
