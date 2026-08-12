# Idea: let `persist_worker` use a small thread pool instead of exactly one thread

**Status**: candidate idea with supporting evidence, not implemented, not fully validated. This is
a sketch to come back to, not a plan ready to execute - see "What isn't validated yet" before
starting.

## The evidence for it

From `docs/plans/implemented/copy-performance-comparison.md`'s small-file profile (3,000 files x
4 KiB): even on a fast SSD, where disk speed is not a plausible excuse, `backup mount
--read-write`'s first write costs **31.2s vs. 4.27s for a plain copy of the same data (~7.3x)**,
and the mount server's own CPU time (26.7s) very nearly equals its wall-clock (31.2s) - i.e. it is
compute-saturated on its single `persist_worker` thread (`cli/src/mount.rs:358-378`, explicitly
documented as "serial by design", moved off the FUSE/WinFSP dispatch threads specifically to avoid
worker-pool exhaustion, not chosen for throughput) - not waiting on FUSE dispatch, the OS, or disk.
For the identical workload, `backup store` (`cli/src/store.rs`) is ~18x faster (1.75s), using a
real `rayon` thread pool sized to `--concurrency` (default: one thread per CPU core).

**This codebase already proves the relevant architecture works**, which is a stronger argument
than "CPU was busy so more threads would help" alone: `store`'s `run_writer` (`cli/src/store.rs`)
is *itself* a single dedicated thread holding the one SQLite write connection for the whole run,
fed via an `mpsc` channel from N parallel workers that each do the CPU-bound read/chunk/hash work
*and* the physical chunk-data write (`chunk_store::write_chunk_from_cache`, gated only by the
independent `--store-io-parallelism`/`IoLimiter`, not by the writer thread) - only the lightweight
metadata record (a `FileBackupRecord`) crosses into the single serialized writer. `persist_worker`
could plausibly adopt the identical split: a small bounded pool doing chunk/hash/physical-write in
parallel, funneling only metadata into one serialized SQLite-writing consumer - the same shape
that already gets `store` its ~18x number, on the same machine, same SQLite backend, same
single-writer constraint that SQLite itself imposes regardless.

## What isn't validated yet

- **Not implemented or measured** - everything above is inference from measured CPU saturation
  plus an architectural analogy to `store`, not a benchmark of an actual pooled `persist_worker`.
- **The large-file case complicates the story.** `store`'s parallelism advantage over mount
  *disappears and reverses* on a slow drive for large files (`store` 304.8s vs. mount 126.0s, see
  `docs/plans/implemented/copy-performance-comparison.md` finding 2) - and directly testing
  `--store-io-parallelism` there ruled out thread contention as the explanation (see
  `docs/plans/store-vs-mount-slow-drive-write-path.md`, still open). If whatever makes `store`'s
  physical writes costlier than mount's on a slow drive is inherent to the parallel-worker
  write path itself (as opposed to something else `store` does differently), pooling
  `persist_worker` could import that same regression for large files on a slow drive, even while
  helping the small-file case. **This should be resolved first** - or at least kept in mind if
  prototyping.
- **Original single-thread rationale needs re-checking, not just reasoned past.** `persist_worker`
  was made single-threaded specifically to avoid a worker-pool-exhaustion failure mode when many
  FUSE dispatch threads all queue persists concurrently (see `docs/plans/implemented/
  06-fuse-mount-readwrite.md`). A *small bounded* pool doesn't obviously reintroduce that failure
  mode (the original problem was unbounded/dispatch-thread-count-sized concurrency, not
  concurrency `> 1` per se), but that's an inference, not a re-verified fact - re-read that plan's
  reasoning before assuming a pool of e.g. 2-4 is automatically safe.
- **Correctness surface differs from `store`'s.** `store` processes a static source tree for the
  duration of one run. Mount's tree can be mutated concurrently by other live FUSE calls
  (`rename`/`unlink`/new `create`s) while `persist_worker` is mid-flight on an older job for the
  same or a related path - `store`'s parallel-worker design never had to reason about that, since
  nothing else touches its source tree while it runs. A pooled `persist_worker` would need its own
  answer for what happens when, say, a file gets renamed or deleted while a pool worker is still
  chunking its old `PersistJob` - not a problem `store`'s architecture had to solve, so it's not
  free to copy the pattern wholesale.

## Suggested next step

Resolve `docs/plans/store-vs-mount-slow-drive-write-path.md` first (or at least form a real
hypothesis for it) - if that turns out to be inherent to parallel physical writes on a slow
device, it changes whether pooling `persist_worker` is a clean win or a large-file/slow-drive
regression waiting to happen. Only after that, prototype a small bounded pool (e.g. 2-4 workers)
for `persist_worker`, re-run the small-file and large-file scenarios from
`docs/plans/implemented/copy-performance-comparison.md` on both a fast and a slow drive against
it, and work out the concurrent-mutation correctness question above before shipping.
