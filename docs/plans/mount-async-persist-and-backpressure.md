# Mount: async persist pipeline and write backpressure

**Status**: not started - this is a stub, not a plan. Already flagged as
a deliberate, documented simplification in `docs/plans/implemented/
06-fuse-mount-readwrite.md`'s "Phase 2b - implementation notes" when
phase 2b (content writes) shipped; written up properly here because a
concrete follow-up question ("what happens on a slow repository disk with
a fast temp/cache disk, writing faster than the slow disk drains?") showed
the gap is more than theoretical.

## Current behavior (as shipped)

`DedupFs::persist` runs **synchronously** inside `release` (a file's last
close) and inside a bare `truncate`/`O_TRUNC` with no open handle - on
whatever thread FUSE/WinFSP dispatched that call to. There is no
background queue and no backpressure mechanism at all: `write` calls are
never throttled, no matter how far behind the target disk is.

Consequence, spelled out for the "slow repository disk, fast temp disk"
case: writing/closing many files proceeds at the *temp* disk's speed
(RAM budget, then spillover - see `cli::write_cache`) right up until each
file is *closed*. At that point `release` blocks the calling thread for
as long as that one file's chunk-and-store pipeline takes against the
*slow* disk - chunking/hashing is CPU-only and fast, but writing new
chunk bytes (`chunk_store::write_chunk_bytes`) and the final
`apply_backup_batch` DB commit both hit the slow disk directly. Because
FUSE/WinFSP dispatch to a bounded worker-thread pool, a burst of closes
faster than the slow disk can absorb them will eventually occupy every
worker thread with a blocked persist - at which point the mount stops
servicing *any* new request, including unrelated reads on other files,
until a thread frees up. This is a much worse failure mode than a smooth
slowdown: from "fast" to "the whole mount appears to hang" with no
gradual transition, and no data is lost (the correctness machinery -
`FileWriteState::persisting`, `wait_while_persisting` - still behaves
correctly under this, just slowly), but the responsiveness story is bad.

The single shared `write_conn` mutex is a secondary, narrower bottleneck
on top of this: every persist's final `apply_backup_batch` commit, and
every `mkdir`/`create`/`unlink`/`rename`/`utimens` call, contends on it -
so a slow-disk-bound commit briefly blocks unrelated *tree-structure*
operations too, independent of the worker-thread-exhaustion effect above.

## What the original (Scala-derived) design would have done differently

The Scala prototype's actual design (ported for the cache tiers, not for
this part - see `docs/plans/implemented/06-fuse-mount-readwrite.md`) hands
each closed file off to a **single background thread** (a serial queue,
not parallel) and applies **backpressure in `write` itself**:
`cacheLoadDelay = bytesInPersistQueue * persistQueueSize / 1_000_000_000`
milliseconds, checked and slept on *before* accepting each write chunk.
Rising backlog (more queued bytes, more queued files) smoothly increases
the delay applied to *new* writes, self-stabilizing rather than either
doing nothing (today's Rust behavior) or hard-blocking.

This was deliberately not ported in phase 2b: the formula's units/scale
couldn't be verified without the original's own tuning history, and the
multi-generation "persisting queue" (a file can be written, closed,
reopened, and rewritten again before the first flush completes) added
real complexity for uncertain benefit at the time.

## Rough shape if/when planned

- A background single-thread persist queue (mirroring Scala's
  `singleThreadStoreContext`) decouples `release`/`close` from the actual
  slow-disk write - closing a file would return promptly regardless of
  target disk speed, with the mount staying responsive.
- Still need *some* backpressure so an unbounded queue doesn't grow
  without limit if writes genuinely outpace the disk forever - either a
  bounded channel (blocking `write` once the queue is full - simple, but
  a hard stop rather than a smooth slowdown) or a from-scratch backpressure
  formula tuned against this project's own numbers rather than copying
  Scala's unverified constant.
- `FileWriteState::persisting`/`wait_while_persisting`'s per-file-id
  blocking (added for phase 2b's release/persist race, see the
  implementation notes) should still work as the read-side correctness
  mechanism under an async pipeline - reads/re-opens of a file already
  queued for persist still need to wait for *that* persist to actually
  land, whether it runs synchronously or on a background thread.
