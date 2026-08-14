# Open question: why is `store`'s physical chunk-write path slower than mount's on a slow drive?

**Status**: root trigger identified and empirically confirmed (2026-08-14, real `slow-usb-I`
hardware, see "Update" below) - it's `--concurrency` (how many distinct OS threads ever call
`LongTermStore::write`), not `--store-io-parallelism` (how many may be *inside* a write call at
once) or physical allocation layout (ruled out by re-reading `SpaceAllocator::reserve` - see
below). The exact device/driver-level reason multi-threaded write-call churn costs so much more on
this hardware than single-threaded churn is still not confirmed via OS-level tracing (no `procmon`
available in the environment this was investigated in) - but the trigger is now precise enough to
act on. See `docs/plans/persist-worker-thread-pool.md` for what this changes about that plan.

## Update (2026-08-14): resolved via a `--concurrency` sweep on real `slow-usb-I` hardware

Run on the same machine (`julius` - confirmed by matching CPU model/core count) and drive
(`I:`, the same 3.75 GB USB stick) as the original measurement, same profile (4 files x 150 MiB
random content, source on fast `C:`, repository on `I:`), release build. **1 rep per point, not
median-of-3** (time budget) - less statistically solid than the original benchmark, but the effect
size and monotonic trend below are large enough not to be noise:

| `--concurrency` | wall-clock |
|---|---:|
| 1 | 139.4s |
| 2 | 242.4s |
| 4 (default on this 4-logical-core machine) | 309.4s |

309.4s vs. the original run's 304.8s at default concurrency confirms this reproduces cleanly on
the same hardware. **`--concurrency 1` alone closes almost the entire gap to mount's 126.0s**
(139.4s, ~2.2x faster than default, within ~11% of mount) - and the trend across 1/2/4 is a smooth
gradient, not a step, consistent with "more threads sharing the write path" as the actual
variable, not some threshold effect.

This was the *other* knob from the one already swept (`--store-io-parallelism`, ruled out
earlier below) - the two are easy to conflate but control different things: `--store-io-parallelism`
gates how many `LongTermStore::write` calls may be *executing concurrently* via a semaphore, without
changing which or how many distinct OS threads take turns making those calls over the run.
`--concurrency` controls the size of the rayon pool that does read+chunk+hash+write *inline, per
file* (`cli::store::resolve_chunk` calls `chunk_store::write_chunk_from_cache` directly on whichever
worker thread is processing that file) - so it's the one that actually determines how many distinct
threads ever call `LongTermStore::write` over the course of a run. `--store-io-parallelism 1` still
lets all 4 worker threads take turns calling `write` (never concurrently, but still 4 different
threads across the run); `--concurrency 1` is the only setting where literally one thread issues
every single write call for the whole run - the same shape mount's `persist_worker` already has.
That structural match plausibly explains why their timings converge (139.4s vs. 126.0s).

**Allocation-layout fragmentation, ruled out by re-reading the code** (a candidate this doc listed
below before this update): `chunk_store::write_chunk_from_cache` calls `SpaceAllocator::reserve`
once per chunk, which is a single-`Mutex`-protected bump allocator - it always hands out the
lowest available gap or extends the trailing region, so the *sequence of extents handed out* is
monotonically non-decreasing in store address space regardless of how many threads call `reserve`
concurrently. A fresh repository (no `reclaim-space`-left gaps, true for this benchmark) has no
gaps to complicate that further. So concurrent chunking workers cannot spatially scatter each
file's bytes across the store any more than one worker would - ruling this candidate out
definitively rather than just deprioritizing it.

**What's still open**: *why* multiple threads calling `LongTermStore::write` (each opening a fresh
file handle per call - see its own "Thread safety" doc comment) costs more on this device than one
thread doing the same total work serially, given the allocator already guarantees the *logical*
write-position sequence is identical either way. The leading hypothesis, not independently
confirmed: true OS-thread parallelism has no ordering guarantee on which write's syscall actually
lands on the device first, so even though positions are allocated monotonically, concurrent workers
can complete their writes to the disk *out of that order* - and a cheap USB flash controller
optimized for strictly sequential writes may fall back to much slower handling once it sees any
reordering, even among writes that are only slightly out of sequence. Confirming this would need
device-level I/O tracing (Windows `Process Monitor` or equivalent - not available in the
environment this was investigated in), so it's recorded as the most plausible mechanism, not a
proven one. Doesn't block acting on the now-confirmed trigger (`--concurrency`/thread count) either
way - see the actionable items in `docs/plans/persist-worker-thread-pool.md`.

## The observation

From `docs/plans/implemented/copy-performance-comparison.md` (large-file profile, 4 files x
150 MiB = 600 MB, on a slow USB stick, `julius`): `backup store`'s first run costs **304.8s**
median wall-clock, while writing the identical content through `backup mount --read-write` costs
only **126.0s** - `store` is **~2.4x slower**, despite `store` using up to 4 worker threads
(2 physical cores, hyperthreaded) against mount's single `persist_worker` thread. On a fast SSD,
the same comparison goes the other way as naively expected (`store` 3.82s vs. mount 10.2s,
`store` wins) - so this is specific to the slow drive.

## What's already ruled out

**Not thread contention on the physical write.** `--store-io-parallelism` gates concurrent
physical chunk writes independent of `--concurrency` (`cli/src/io_limiter.rs`, used at the
`chunk_store::write_chunk_from_cache` call site in `cli/src/store.rs`). Swept it across
`1`/`2`/`4` on the same slow drive, same profile, 2 reps each:

| `--store-io-parallelism` | wall (rep 1) | wall (rep 2) |
|---|---:|---:|
| 1 | 308.3s | 288.0s |
| 2 | 321.0s | 302.8s |
| 4 (~= default) | 284.5s | 290.6s |

No trend, all three within noise of each other, and all still ~2.3-2.5x slower than mount's
126.0s - including `parallelism=1`, which serializes physical chunk writes onto one thread and so
should structurally resemble mount's single-threaded writer most closely of the three. Whatever
the cause is, it isn't threads racing each other for the disk.

**Not raw compute.** CPU time for `store`'s large-file first run (~6-9s across all three
parallelism settings) is in the same ballpark as mount's (8.08s) - neither is CPU-bound here, both
are overwhelmingly wall-clock-vs-CPU I/O-wait.

## What was still open at this point (superseded - see "Update" above)

Kept for the historical record of what was actually checked and why, not as current guidance -
the "Update" section above supersedes this. Candidates considered at the time, and how the
`--concurrency` sweep resolved each:

- **Write call pattern** - resolved as a non-factor, not by measurement but by re-reading the
  code: `mount`'s persist path calls the exact same `chunk_store::write_chunk_from_cache` /
  `LongTermStore::write` functions `store` does (`cli/src/mount.rs`'s `resolve_persist_chunk`),
  same `DRAIN_PIECE_SIZE` piece size, same no-write-handle-caching behavior on both sides - there
  is no *pattern* difference to find here, the two paths are code-identical at this layer. The
  actual difference is only ever *how many distinct threads* invoke that identical code, which is
  what the "Update" section above confirms and quantifies.
- **Allocation pattern** - ruled out definitively, see "Allocation-layout fragmentation, ruled out
  by re-reading the code" in the "Update" section above.
- **The single SQLite writer thread** - not re-investigated directly, but implicitly deprioritized:
  the `--concurrency` sweep's effect size and monotonic trend are already fully explained by the
  physical-write thread-count mechanism, leaving little room for a second, separately-timed SQLite
  effect of comparable magnitude. Worth re-checking only if item 1 in
  `docs/plans/persist-worker-thread-pool.md` (decoupling `store`'s physical write onto its own
  thread) doesn't fully close the gap in practice.
- **Read side** - not re-investigated; still an untested assumption, though now a much less likely
  one given how cleanly the `--concurrency` sweep's numbers already explain the observed gap.

The original "Suggested next step" here (OS-level I/O tracing via `Process Monitor` or equivalent)
was never actually executed - no such tool was available in the environment this ended up being
investigated in - and turned out not to be necessary to identify the actionable trigger. It would
still be the way to confirm the "Update" section's leading hypothesis for *why* thread count
matters at the device level, if that's ever worth pinning down precisely.
