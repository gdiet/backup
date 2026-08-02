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

## Open questions (why this isn't implemented yet)

- What spilled-byte threshold should gate `enqueue_persist`? Unlike
  `PERSIST_QUEUE_CAPACITY = 4` (an arbitrary but easy-to-reason-about job
  count), a byte threshold needs an actual justified number - fixed
  fraction of `--write-cache-mb`? A separate flag? Needs real measurement
  or at least a defensible default, not a guess.
- Whether a single combined `Pressure` type (RAM-remaining +
  spilled-bytes) is worth introducing now, or whether adding just the
  spilled-bytes counter to the existing `RamBudget` (in its new
  `spillcache` crate) is enough for this one consumer - decide once the
  threshold question above is settled, since that determines what the
  actual call site needs to read.
