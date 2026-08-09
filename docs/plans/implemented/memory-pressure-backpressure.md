# Mount: replace the fixed persist-queue capacity with memory-pressure backpressure

**Status**: implemented, on branch `rust-backpressure` (`spillcache::
RamBudget`'s new spilled-bytes counter; `cli/src/mount.rs`'s
`enqueue_persist` gating on `queued_persist_bytes` vs.
`spill_backpressure_threshold_bytes`, tied to `--write-cache-mb`).
Real before/after benchmark results (same commands, same machine, real
`libfuse3` mount) in `docs/plans/backpressure-bench/results.md`: ~3.5x
throughput for many small files, ~20-27% less peak unpersisted data and
faster full-drain time for a large-file burst large enough to exhaust the
FUSE worker pool (a real, structural nuance discovered while measuring
this - see that file's "Important nuance").

Raised while discussing whether `WriteCache::write`'s return value should
report aggregate RAM-remaining/spillover state (`docs/plans/implemented/
bounded-memory-io-pipeline.md`'s successor discussion) - the concrete
question was whether a memory-pressure signal could *replace* an existing
backpressure mechanism rather than just add a new one, since replacing one
is a real simplification and adding one is just more surface area to
understand.

## The candidate

`mount --read-write`'s persist queue (`cli/src/mount.rs`,
`PERSIST_QUEUE_CAPACITY = 4`, see
`docs/plans/implemented/bounded-memory-io-pipeline.md`'s "Mount-specific
detail") gates `enqueue_persist` on a fixed *count* of queued
`PersistJob`s, regardless of their size. Its actual purpose (per that doc)
is bounding how much unpersisted, in-flight work can pile up before a
*new* `release`/bare-truncate starts blocking - a byte-level ceiling is
arguably a more faithful expression of that goal than an arbitrary job
count.

**The tracking this would need mostly already exists.** A `PersistJob`
carries its `WriteCache` by move, not by copy, into the queue - the
`RamBudget` charge for its RAM/spill usage stays live for exactly as long
as the job is unpersisted, with no extra bookkeeping. The only missing
piece is that `RamBudget` today only tracks *RAM* headroom
(`try_acquire`/`release`, see `spillcache/src/lib.rs` - now its own crate,
moved out of `cli` as part of this same round of work) - it has no
counter for *how much has spilled to disk*, which is the actual pressure
signal a blocking gate would need (RAM headroom alone doesn't distinguish
"nothing queued" from "everything's spilling to disk but RAM shows free
because nothing new is being retained").

## What this would change

- Add a second counter (bytes currently spilled, incremented in
  `FileCache::write`'s spill path, decremented in `keep`/`clear`/`Drop` the
  same way `ByteSpanMap` already tracks RAM) alongside `RamBudget` - or
  fold both into one small `Pressure`-style type with two independent
  getters, not one overloaded signed number (see the discussion this plan
  follows on why not to conflate the two).
- `Inner::enqueue_persist` blocks not on `queued_jobs.len() >=
  PERSIST_QUEUE_CAPACITY`, but on the shared budget's spilled-bytes count
  exceeding some threshold (needs a real number, not a guess - see "Open
  questions").
- `PERSIST_QUEUE_CAPACITY` and its doc comment go away; the `mpsc::
  sync_channel` itself likely stays (still need a channel to hand jobs to
  `persist_worker`), just with a capacity large enough to not be the
  actual gate, or reworked to an unbounded channel with the blocking check
  moved to before the send.

## What this would *not* replace

Checked both other existing backpressure-shaped mechanisms against the
same question - "would a memory-pressure signal correctly stand in for
this" - and both fail for a reason worth recording so it isn't
re-litigated later:

- **`cli::io_limiter::IoLimiter` (`--store-io-parallelism`)** bounds *I/O
  concurrency* against a possibly-slow repository disk/network share, not
  memory occupancy. A workload can have many concurrent small writes
  draining quickly (low memory pressure throughout) while still
  oversubscribing a slow disk's optimal concurrency - memory pressure
  would not detect that case at all. Different signal, not a substitute.
- **`store`'s admission control** was already concluded (see
  `docs/plans/implemented/bounded-memory-io-pipeline.md`'s closing note in
  "`store`'s I/O-vs-CPU concurrency split") to need no separate mechanism
  at all - the synchronous read/chunk/write loop plus the `--concurrency`-
  sized thread pool already gate it. Nothing to replace there.

## Validation approach

Before committing to a byte threshold (or even to this change at all), get
real numbers via two synthetic slow-disk benchmarks - simulated by a
temporary code change that caps write throughput to roughly USB2 speed
(~30 MB/s), rather than requiring actual USB2 hardware on hand:

- **Many small files, capped datastore disk.** Back up a large number of
  small files with the datastore write path throttled, and compare
  wall-clock/throughput between today's job-count gate
  (`PERSIST_QUEUE_CAPACITY = 4`) and a byte-based gate. Small files each
  carry little unpersisted-byte pressure, so a fixed job count may be
  throttling earlier than the actual pressure justifies - this test shows
  whether that's a real, measurable loss or just theoretical.
- **Very large files, capped spillover disk.** With the spillcache's
  disk-spill path throttled instead, back up files large enough to spill
  and check whether `N = 4` jobs is actually *too permissive* here - a
  single spilled `WriteCache` for a huge file can represent far more
  unpersisted bytes than 4 was ever calibrated for, so the byte threshold
  might need to gate *earlier* (a lower effective limit) than the current
  count-based one does in this case, not just replace it 1:1.

Both benchmarks should also check the UX goal raised alongside this plan:
backpressure should show up as writes *progressively slowing down* as
pressure rises, not running at full speed until a hard limit is hit and
then blocking a single `write` call for a long pause before snapping back
to fast. A hard cutoff - whether today's job-count check or a same-shaped
byte-threshold check - produces exactly that "stall, then burst" pattern;
a proportional/graduated throttle (e.g. inserting a small, increasing delay
per `write` as spilled bytes approach the threshold, rather than an
all-or-nothing block at it) would match the goal better. Which shape to
build should be decided from what these benchmarks actually show, not
assumed up front - see the added open question below.

## Validation results

Ran both benchmarks for real (WSL2/Debian, release build, real `libfuse3`
mount via the existing in-process test harness in `cli/src/mount.rs`'s
Linux-only test module - not against real USB2 hardware; throughput was
capped by a temporary `bench_throttle` hook added to `LongTermStore::
write`/`spillcache::FileCache::write`, gated by an env var, fully reverted
afterward). Two harness bugs worth flagging for whoever reruns this:
byte-identical payloads across files CDC-dedup to one chunk after the
first file, making every later "write" free (no real store I/O) and
defeating the point of the test; and generating payloads *inside* the
timed loop (even if not itself individually timed) still inflates the
outer wall-clock measurement - precompute payloads first.

**Many small files** (200 files x 1 MB, datastore throttled to 30 MB/s):
comparing today's `PERSIST_QUEUE_CAPACITY = 4` against a temporarily
patched `= 64` under otherwise identical conditions -

| capacity | end-to-end | throughput | close p50/p95/max |
|---|---|---|---|
| 4  | 10.14s | 19.72 MB/s | 52.8 / 65.5 / 103.4 ms |
| 64 | 7.40s  | 27.02 MB/s | 51.8 / 72.8 / 128.9 ms |

`N = 4` costs ~27% aggregate throughput versus `N = 64` here, even though
per-file p50 latency is nearly identical in both - the loss is in how far
the client can get ahead of the single-threaded `persist_worker` before
hitting backpressure, not in individual operation latency. Neither
capacity showed a dramatically bursty shape (max stayed within ~2x of
p50 in both cases) - the "runs fast, then stalls hard" failure mode this
plan worried about wasn't strongly present at this file size either way.
**Confirms the first hypothesis**: job-count gating measurably costs
throughput for many-small-files, since 4 (or even 64) tiny files' worth
of buffered data is nowhere near real memory/disk pressure.

**Large files** (6 files x 50 MB, `write_cache_mb: 1` forcing every byte
to spill, datastore throttled to 5 MB/s): the write loop - all 6
`close()` calls - finished in **1.37s**, with zero client-visible
slowdown. But peak spilled-and-unpersisted data reached **300 MB (6x a
single file)**, and unmounting (which waits for the drain) took roughly
**65 seconds** afterward. **Confirms the second hypothesis, more
severely than expected**: `N = 4` isn't just "a bit too permissive" for
large files, it provides *no observable backpressure at all* - no
slowdown, no warning, nothing - right up until unmount (or a crash, which
would lose all of that "successfully closed" but never-durable data)
reveals a large invisible backlog. This is arguably worse than the
"stall, then burst" pattern originally worried about: it's "looks
completely fine, then a nasty surprise."

**Informs, but doesn't fully settle, the open questions below**: a byte
threshold clearly needs to be small enough to have caught the 300 MB
large-file backlog (nowhere near current behavior) while staying
generous enough not to gate small-file bursts as early as `N = 4`
effectively does today (measured cost: ~27%) - a concrete number still
needs picking, not just inferred from these two data points. The
observed latency shape (no dramatic burstiness in the small-files case)
is a point in favor of trying a plain hard cutoff first, before building
the extra complexity of a graduated/proportional throttle - but that's
a leaning, not a decision made here.

## Decisions (resolved during implementation)

- **Threshold**: `write_cache_mb * 1024 * 1024` bytes - reuses the
  `--write-cache-mb` figure already there for the RAM budget rather than
  adding a second flag. No dedicated sweep of alternative values was run;
  this is a defensible default (see "Validation results" above for the
  reasoning), not a value separately tuned against the benchmarks.
- **Hard cutoff, not graduated throttle** - matches the leaning above (no
  dramatic burstiness observed from the old job-count gate either, so the
  simpler shape was tried first). Worth revisiting only if real usage
  shows the cutoff itself feels jarring once it does engage.
- **Single counter added to the existing `RamBudget`** (in `spillcache`),
  not a new combined `Pressure` type - the simpler of the two options this
  plan had left open, sufficient for `enqueue_persist`, its only consumer
  so far.
- **Gate lives on `Inner::queued_persist_bytes`** (bytes already
  queued-or-persisting), not directly on `RamBudget::spilled_bytes()`
  (mount-wide, including still-open files) - checked *before* adding the
  new job's own contribution, so a single file bigger than the whole
  threshold still gets admitted immediately when nothing else is queued,
  rather than deadlocking on pressure only it itself created.
