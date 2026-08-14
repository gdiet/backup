# Idea: decouple chunk/hash parallelism from physical-write thread count, for both `store` and `mount --read-write`

**Status**: candidate architecture with strong supporting evidence on both sides now (small-file
*and* large-file/slow-drive), not implemented. The blocking open question this doc's own
"Suggested next step" pointed at is now resolved (2026-08-14, see
`docs/plans/store-vs-mount-slow-drive-write-path.md`) - its answer changes the shape of the fix
from "just pool `persist_worker`" to something more specific, see "Actionable items" below.

## The evidence for it

### Small files: `persist_worker`'s single thread is a real, measured throughput ceiling

From `docs/plans/implemented/copy-performance-comparison.md`'s small-file profile (3,000 files x
4 KiB): even on a fast SSD, where disk speed is not a plausible excuse, `backup mount
--read-write`'s first write costs **31.2s vs. 4.27s for a plain copy of the same data (~7.3x)**,
and the mount server's own CPU time (26.7s) very nearly equals its wall-clock (31.2s) - i.e. it is
compute-saturated on its single `persist_worker` thread (`cli/src/mount.rs`, explicitly documented
as "serial by design", moved off the FUSE/WinFSP dispatch threads specifically to avoid
worker-pool exhaustion, not chosen for throughput) - not waiting on FUSE dispatch, the OS, or disk.
For the identical workload, `backup store` (`cli/src/store.rs`) is ~18x faster (1.75s), using a
real `rayon` thread pool sized to `--concurrency` (default: one thread per CPU core).

### Large files on a slow drive: `store`'s inline-per-worker physical write is *itself* the problem, independent of mount

`docs/plans/store-vs-mount-slow-drive-write-path.md` (resolved 2026-08-14, real hardware) found
that `store`'s large-file regression on a slow drive (304.8s vs. mount's 126.0s, ~2.4x slower) is
driven by **how many distinct OS threads ever call `LongTermStore::write`** over a run, not by
concurrent-write contention (`--store-io-parallelism`, already ruled out) or by physical
allocation layout (`SpaceAllocator::reserve` is a monotonic bump allocator regardless of thread
count - also ruled out). A `--concurrency` sweep on the real slow drive: **139.4s at
`--concurrency 1`, 242.4s at `2`, 309.4s at `4`** (default on the 4-logical-core test machine) - a
smooth gradient tracking thread count, and `--concurrency 1` alone closes almost the entire gap to
mount's 126.0s.

**Why this matters here**: `store`'s `resolve_chunk` (`cli/src/store.rs`) calls
`chunk_store::write_chunk_from_cache` *inline*, on whichever of the `--concurrency` rayon workers
is processing that file - so `--concurrency` doesn't just control chunking/hashing parallelism,
it also controls how many distinct threads take turns physically writing to the store. That's an
architectural conflation, not a fundamental requirement: nothing about CPU-parallel chunking
*requires* the thread that computed a chunk to also be the one that writes it. `store` already
proves the alternative shape works for its *metadata* writes - `run_writer` is a single dedicated
thread holding the one SQLite write connection, fed via `mpsc` from the N parallel chunk/hash
workers. It just doesn't (yet) apply that same split to the *physical chunk-byte* write, which
turns out to be exactly where the slow-drive cost lives.

**This reframes the original architectural analogy.** The evidence for pooling `persist_worker`
(above) and the evidence for *un-pooling* `store`'s physical writes (this section) point at the
same underlying shape from opposite directions: **N-way parallelism belongs on the CPU-bound
read/chunk/hash side; the physical `LongTermStore::write` call belongs on a single dedicated
thread**, for both commands. `store` already has this right for the *last mile* (SQLite writer)
but not for the *chunk data itself*; `mount --read-write` already has this right for chunk data
(one `persist_worker` thread) but has no parallelism at all on the read/chunk/hash side.

## Actionable items

Ordered by how directly each depends on what's now confirmed vs. still open. None of this is
implemented yet - each item below still needs its own real prototype-and-measure pass before being
trusted, per this project's own "verify, don't assume" convention.

1. **Give `store` a single dedicated physical-chunk-writer thread, decoupled from `--concurrency`**
   (mirrors `run_writer`'s existing shape, applied one level lower). Concretely: `resolve_chunk`'s
   dedup-miss branch stops calling `chunk_store::write_chunk_from_cache` inline; instead it sends
   the chunk's bytes (still as a `WriteCache`, not materialized to a `Vec<u8>` - keep the existing
   bounded-memory property) over a channel to one new dedicated writer thread, which drains it and
   returns the resulting extents (needs a response channel or a shared slot per in-flight chunk,
   since the calling worker still needs those extents to build its `ChunkRef::New`). This is the
   highest-confidence item here: it directly targets the specific, now-measured trigger
   (`--concurrency`'s thread count reaching `LongTermStore::write`), on the exact workload
   (large files, slow drive) where the regression was confirmed. **Verify**: re-run the
   `--concurrency` sweep from `docs/plans/store-vs-mount-slow-drive-write-path.md` against this
   changed `store` and confirm `--concurrency 4`'s wall-clock now tracks close to the
   already-measured `--concurrency 1` number (139.4s) instead of the current default's 309.4s -
   i.e. that decoupling the write thread, not just lowering `--concurrency`, is what closes the
   gap. Also re-run the small-file profile to confirm this doesn't regress `store`'s existing
   ~18x small-file advantage (a single writer thread could plausibly become the new bottleneck
   there if per-chunk handoff overhead is too high - worth checking, not assumed safe).
2. **Give `persist_worker` a small worker pool for chunk/hash, keeping physical writes on one
   thread** - the mount-side mirror of item 1, and the one this doc originally proposed, now
   scoped more precisely: don't pool the whole `PersistJob` (chunk + hash + physical write)
   the way the original "small bounded pool" framing implied - pool only the CPU-bound
   chunk/hash stage, and keep funneling the actual `LongTermStore::write` calls through the
   existing single `persist_worker` thread (or a still-single, differently-shaped writer thread
   if item 1's redesign produces a reusable shared writer component both commands can use - worth
   checking once item 1 exists, rather than building two separate one-off writer threads).
   **Verify**: re-run the small-file profile from `docs/plans/implemented/
   copy-performance-comparison.md` and confirm mount's write time moves toward `store`'s (currently
   ~18x apart); re-run the large-file/slow-drive profile too, specifically to confirm this
   *doesn't* reintroduce the item-1 regression on the mount side (a pool feeding a single writer
   thread should be immune to it by construction, but that's exactly the kind of assumption this
   project's conventions say to re-verify, not trust).
3. **Re-verify the original worker-pool-exhaustion rationale still holds** for whatever the
   chunk/hash pool size ends up being (see `docs/plans/implemented/06-fuse-mount-readwrite.md` and
   `docs/plans/implemented/bounded-memory-io-pipeline.md`'s "Mount-specific detail" for the
   original failure mode: FUSE/WinFSP dispatch threads blocking on synchronous persist, exhausting
   the dispatch pool). A small, *bounded* chunk/hash pool sitting behind the existing
   `enqueue_persist`/queue mechanism (dispatch threads still only ever enqueue, never block on
   the pool directly) shouldn't reintroduce this, but this is exactly the kind of inference this
   project's conventions say to re-check against the original doc's reasoning, not assume past.
4. **Work out the concurrent-mutation correctness question** before shipping either pool: unlike
   `store` (processes a static source tree for one run), mount's tree can be mutated by other live
   FUSE calls (`rename`/`unlink`/new `create`s) while a pool worker is still mid-flight on an older
   `PersistJob` for the same or a related path. Needs its own explicit answer - not free to copy
   `store`'s design wholesale, since `store` never had to reason about this.
5. **Once 1-4 are prototyped and measured**, re-run the *entire* `copy-performance-comparison.md`
   scenario matrix (both profiles, both drives) against both changes together, not just each in
   isolation - the small-file and large-file/slow-drive cases pull in different directions
   architecturally, so the combined behavior is worth confirming directly rather than assuming the
   two fixes compose cleanly.

## Still open / deliberately out of scope here

- **The exact device/driver-level reason multi-threaded write calls cost more than single-threaded
  ones on the slow test drive** is not confirmed (would need OS-level I/O tracing - see
  `docs/plans/store-vs-mount-slow-drive-write-path.md`'s "What's still open"). Doesn't block acting
  on item 1 above, which targets the *confirmed* trigger (thread count) regardless of the
  underlying mechanism - but worth keeping in mind that a different slow-drive device (different
  controller/firmware) might not exhibit the identical size of effect.
- **Whether a bounded pool size other than "1 writer thread" for the physical write is ever worth
  it** (e.g. 2 writer threads on a device that *does* tolerate some concurrency) - not measured;
  item 1 above targets exactly-1 first since that's what the sweep actually confirmed converges
  with mount's existing behavior, not a range.
