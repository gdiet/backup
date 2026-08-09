# Mount: replace the fixed persist-queue capacity with memory-pressure backpressure

**Status**: proposed, not implemented. Raised while discussing whether
`WriteCache::write`'s return value should report aggregate RAM-remaining/
spillover state (`docs/plans/implemented/bounded-memory-io-pipeline.md`'s
successor discussion) - the concrete question was whether a memory-pressure
signal could *replace* an existing backpressure mechanism rather than just
add a new one, since replacing one is a real simplification and adding one
is just more surface area to understand.

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

## Open questions (why this isn't implemented yet)

- What spilled-byte threshold should gate `enqueue_persist`? Unlike
  `PERSIST_QUEUE_CAPACITY = 4` (an arbitrary but easy-to-reason-about job
  count), a byte threshold needs an actual justified number - fixed
  fraction of `--write-cache-mb`? A separate flag? Needs real measurement
  (see "Validation approach") or at least a defensible default, not a
  guess.
- Hard cutoff or graduated throttle? A single threshold that blocks
  `enqueue_persist` outright reproduces the "runs fast, then stalls, then
  bursts back to fast" pattern this plan's discussion wants to avoid; a
  throttle that gradually slows writes as spilled bytes approach the
  threshold would feel smoother to the user but is more code and more
  behavior to reason about (what curve? where does it start?). Decide from
  the "Validation approach" benchmarks - if a hard cutoff already tests out
  smooth enough at a well-chosen threshold, the simpler shape wins by
  default.
- Whether a single combined `Pressure` type (RAM-remaining +
  spilled-bytes) is worth introducing now, or whether adding just the
  spilled-bytes counter to the existing `RamBudget` (in its new
  `spillcache` crate) is enough for this one consumer - decide once the
  threshold question above is settled, since that determines what the
  actual call site needs to read.
