# Idea: decouple chunk/hash parallelism from physical-write thread count, for both `store` and `mount --read-write`

**Status**: mount side (items 2-4 below) implemented and verified 2026-08-14 - see "Implemented:
mount's chunk/hash pool" below for what shipped and how it was checked. `store`'s side (item 1) is
still just a candidate, not implemented - it's the higher-risk half (see item 1's own "Risk"
paragraph) and was deliberately sequenced *after* mount's lower-risk change, not before, despite
being listed first below (kept in its original position since the two items' own reasoning still
references each other).

## Implemented: mount's chunk/hash pool (2026-08-14)

Items 2 (the pool itself), 3 (worker-pool-exhaustion re-check), and 4 (concurrent-mutation
correctness) below are done, on `cli/src/mount.rs`:

- `persist_worker` (one thread, did chunking *and* physical writes) replaced by
  `persist_chunk_worker` (a pool, sized by the new `--persist-workers`, default one thread per CPU
  core - shares the job queue via `Arc<Mutex<mpsc::Receiver<PersistJob>>>` since `mpsc::Receiver`
  isn't itself `Clone`/`Sync`) feeding a single dedicated `persist_writer` thread that every
  physical `LongTermStore::write` call is funneled through regardless of pool size, via a new
  `PersistWriteRequest`/one-shot-response channel - exactly the split "Actionable items" below
  called for.
- **Item 3's answer, confirmed by re-reading, not just re-asserted**: dispatch threads only ever
  interact with `enqueue_persist` (a byte-budget check, then a non-blocking send on an unbounded
  channel) - never with "is a pool worker free," which is what actually made the original
  worker-pool-exhaustion fix work. Since that property doesn't depend on how many threads drain the
  queue on the other end, pooling the consumer side cannot reintroduce dispatch-thread exhaustion;
  the byte-budget gate (unchanged) remains the only place a dispatch thread can block.
- **Item 4's answer, turned out to need no new mechanism at all**: `FileWriteState::persisting`/
  `wait_while_persisting`/`resolve_active_entry` are already keyed per `tree_id`, not global, so
  they already tolerate arbitrary concurrent FUSE activity on other files even under the old
  single-threaded pool. `Inner::persist`'s own "was removed or replaced while open for writing"
  check is itself per-`tree_id` and re-verified at commit time regardless of how many other
  persists are mid-flight. And two pool workers independently discovering the same *new* chunk
  content at once is exactly the pre-existing, already-tolerated `apply_backup_batch` race
  (`ON CONFLICT DO NOTHING`, see `db::resolve_content`'s doc comment) `store`'s own N-way-parallel
  workers already exercise today - pooling mount's chunk/hash stage just means mount can hit that
  same, already-proven-safe race too, not a new one. Full reasoning now lives as a doc comment on
  `Inner::persist` itself, not just here.
- **Verification**: `cargo build`/`fmt`/`clippy --all-targets`/`test`/`doc` clean on both native
  Windows and, via the `wsl-windows-sync` skill, real Linux (`cargo test`: 195 passed, 0 failed,
  including real `libfuse3` mount integration tests exercising the new pool end to end) - plus the
  equivalent real-`WinFSP` integration tests already passing natively on Windows. Two new targeted
  tests (`pooled_persist_settles_many_concurrent_files_correctly`,
  `pooled_persist_dedupes_identical_new_content_raced_across_files`) drive genuine concurrent
  persists via `std::thread::scope`, not just FUSE dispatch timing - each run 15x in a row on Linux
  with zero flakiness before trusting them.
- **Not done as part of this**: no fresh performance re-measurement against
  `docs/plans/implemented/copy-performance-comparison.md`'s scenarios (item 5 below still applies -
  worth doing, especially the small-file case this pool specifically targets, but is its own
  separate pass, not a correctness gate for shipping the mechanism itself).

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

Ordered by how directly each depends on what's now confirmed vs. still open. Items 2-4 are done
(see "Implemented" above); items 1 and 5 are still just candidates and need their own real
prototype-and-measure pass before being trusted, per this project's own "verify, don't assume"
convention.

**Recommended starting point: item 2, not item 1** - despite item 1 being listed first (it targets
the more directly-measured trigger) and having a clean existing precedent to mirror
(`run_writer`), item 2 is the safer *first* slice for three independent reasons: (a) no identified
performance-regression risk (see item 1's own risk below - item 2 has no equivalent), (b) it only
touches `mount.rs`, which is the *worse* performer today in every scenario measured so far, so
there's nothing to regress; item 1 touches `store.rs`'s hot path, which already performs well in 3
of the 4 measured drive-x-file-size quadrants (only the slow-drive/large-file one is bad) - a
subtle bug there risks breaking something that currently works, and (c) the gap item 2 addresses
(mount's small-file write throughput, ~7x-18x slower than a plain copy/`store`) shows up on *every*
drive speed measured so far (both fast-ssd-C and slow-usb-I), while item 1's gap is specific to
slow/cheap destination drives - item 2 plausibly matters to more real workloads more of the time.
**"Starting with item 2" concretely means starting with items 3 and 4 below** (re-verifying the
worker-pool-exhaustion rationale, and working out the concurrent-mutation correctness question) -
those are real, unresolved design work, not optional preamble to skip before writing the pool
itself.

1. **Give `store` a single dedicated physical-chunk-writer thread, decoupled from `--concurrency`**
   (mirrors `run_writer`'s existing shape, applied one level lower). Concretely: `resolve_chunk`'s
   dedup-miss branch stops calling `chunk_store::write_chunk_from_cache` inline; instead it sends
   the chunk's bytes (still as a `WriteCache`, not materialized to a `Vec<u8>` - keep the existing
   bounded-memory property) over a channel to one new dedicated writer thread, which drains it and
   returns the resulting extents (needs a response channel or a shared slot per in-flight chunk,
   since the calling worker still needs those extents to build its `ChunkRef::New`). This directly
   targets the specific, now-measured trigger (`--concurrency`'s thread count reaching
   `LongTermStore::write`), on the exact workload (large files, slow drive) where the regression
   was confirmed.
   - **Risk, not just a "verify" footnote: this could regress `store`'s existing fast-SSD
     small-file advantage.** That advantage (1.75s vs. mount's 31.2s, ~18x, on `fast-ssd-C`) comes
     from `store`'s *whole* pipeline running in parallel today, physical writes included - forcing
     every one of potentially thousands of small chunk writes through one thread removes that,
     and `LongTermStore::write` opens a brand-new file handle on *every single call* (no
     handle-caching, unlike the read side - see its own doc comment), so what's lost isn't
     throughput headroom (a fast SSD has plenty) but the *overlap* between N threads' open+seek
     +write+close cycles. Whether that overlap was actually contributing meaningfully to the
     1.75s, or whether that number is dominated by something else (SQLite/dedup-lookup overhead,
     which stays parallel either way), is genuinely unknown until measured - don't assume safe.
   - **Possible mitigation, not yet evaluated**: make the physical-writer thread count a small
     tunable (e.g. `--store-write-threads`, default 1) instead of hardcoding exactly one -
     precedent already exists for exposing this kind of I/O-shape knob separately from
     `--concurrency` (`--store-io-parallelism`). Lets a slow-drive user keep the safe default while
     a fast-SSD user with a many-small-file workload can opt back into more write parallelism if
     benchmarking shows they need it - at the cost of one more flag to explain, and it doesn't
     remove the need to actually benchmark the default.
   - **Verify**: re-run the `--concurrency` sweep from
     `docs/plans/store-vs-mount-slow-drive-write-path.md` against this changed `store` and confirm
     `--concurrency 4`'s wall-clock now tracks close to the already-measured `--concurrency 1`
     number (139.4s) instead of the current default's 309.4s. **Just as importantly**, re-run the
     small-file profile on `fast-ssd-C` and confirm it doesn't regress below today's 1.75s by a
     meaningful margin - this is the one measurement in the whole plan most likely to produce an
     unpleasant surprise, given the risk above.
2. **DONE (2026-08-14, see "Implemented" above). Give `persist_worker` a small worker pool for
   chunk/hash, keeping physical writes on one thread** - the mount-side mirror of item 1, and the
   one this doc originally proposed, now
   scoped more precisely: don't pool the whole `PersistJob` (chunk + hash + physical write)
   the way the original "small bounded pool" framing implied - pool only the CPU-bound
   chunk/hash stage, and keep funneling the actual `LongTermStore::write` calls through the
   existing single `persist_worker` thread (or a still-single, differently-shaped writer thread
   if item 1's redesign produces a reusable shared writer component both commands can use - worth
   checking once item 1 exists, rather than building two separate one-off writer threads). Unlike
   item 1, no fast-SSD regression risk has been identified for this one: it only adds parallelism
   to a stage (`mount`'s chunk/hash) that's currently compute-saturated on a single thread with
   idle cores sitting next to it, and it doesn't touch the physical-write shape (already a single
   thread today, staying a single thread) - so there's no existing behavior on the write side to
   regress. **Verify**: re-run the small-file profile from `docs/plans/implemented/
   copy-performance-comparison.md` and confirm mount's write time moves toward `store`'s (currently
   ~18x apart); re-run the large-file/slow-drive profile too, specifically to confirm this
   *doesn't* reintroduce the item-1 regression on the mount side (a pool feeding a single writer
   thread should be immune to it by construction, but that's exactly the kind of assumption this
   project's conventions say to re-verify, not trust).
3. **DONE (2026-08-14, see "Implemented" above). Re-verify the original worker-pool-exhaustion
   rationale still holds** for whatever the
   chunk/hash pool size ends up being (see `docs/plans/implemented/06-fuse-mount-readwrite.md` and
   `docs/plans/implemented/bounded-memory-io-pipeline.md`'s "Mount-specific detail" for the
   original failure mode: FUSE/WinFSP dispatch threads blocking on synchronous persist, exhausting
   the dispatch pool). A small, *bounded* chunk/hash pool sitting behind the existing
   `enqueue_persist`/queue mechanism (dispatch threads still only ever enqueue, never block on
   the pool directly) shouldn't reintroduce this, but this is exactly the kind of inference this
   project's conventions say to re-check against the original doc's reasoning, not assume past.
4. **DONE (2026-08-14, see "Implemented" above). Work out the concurrent-mutation correctness
   question** before shipping either pool: unlike
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
