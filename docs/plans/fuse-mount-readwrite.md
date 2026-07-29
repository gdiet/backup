# Read-write FUSE mount (Phase 2)

**Status**: design only, not implemented. Split out from the original combined FUSE mount plan once its read-only phase shipped - see `docs/plans/implemented/04-fuse-mount-readonly.md`, which this depends on (the mount command, inode mapping, and read path it adds are reused here unchanged). Also depends on `docs/plans/implemented/03-chunk-extents.md` (done - see §3 below).

## Context

The Scala tool this Rust rewrite replaces supports read-write FUSE mounts, with an in-process write cache (RAM, spilling to a temp file under memory pressure) and a backpressure mechanism that slows writers down as the async persist pipeline falls behind. This doc designs the Rust equivalent, modeled closely on the Scala design (`cache/{MemCache,FileCache,WriteCache,Allocation,CacheBase}.scala`, `server/{Handles,Handle}.scala`, `server/Backend.scala`).

Three design decisions were discussed and settled before writing this plan:

1. **Backpressure formula**: replicate Scala's `cacheLoadDelay = bytesInPersistQueue * persistQueueSize / 1_000_000_000` (ms), applied per write chunk, **uncapped** - not a bug to fix. It's a self-stabilizing negative-feedback throttle *because* it's applied synchronously before a chunk is accepted into the cache: rising backlog → rising delay → falling input rate → backlog growth slows. Capping the sleep either breaks that property (if nothing replaces it, backlog can now grow unboundedly once the cap is saturated) or, if paired with a hard block past some threshold, replaces a smooth slowdown with a jarring full stop - worse for the filesystem user in both cases, not better. Add nothing beyond breaking a single very-long sleep into small slices that recheck for shutdown - and only if that's genuinely ~3-4 lines (YAGNI otherwise).
2. **Content mutability for FUSE writes**: reuse the existing soft-delete-old-row + insert-new-row pattern `apply_backup_batch` already uses on a content change, rather than adding a new `AFTER UPDATE OF content_id` trigger for true in-place mutation. Accepted trade-off: a frequently-rewritten file (editor autosave, a log file kept open) accumulates soft-deleted history rows until `reclaim-space` runs, instead of behaving like a normal mutable filesystem with no history at all.
3. **Write-cache RAM budget**: auto-detected from total system RAM (new dependency: `sysinfo`) with a CLI override flag - there's no JVM heap ceiling to anchor Scala's `(maxMemory - 64MB) * 0.7` formula to, so this needs a Rust-native basis.

Platform scope: Linux only, same as the read-only phase (via the `fuser` crate). Cross-platform (WinFSP) support was investigated separately and is out of scope: WinFSP's FUSE compatibility layer (`cygfuse`) only emulates libfuse's *high-level*, path-based C API (`fuse.h`/`fuse3/fuse.h`, `fuse_main()`), not the *low-level*, inode-based session API `fuser` binds against - so `fuser` (and by extension this plan, which builds on it) has no path to WinFSP even via its `libfuse` feature flag. A Windows port would need a separate implementation against either the low-level libfuse API directly (no well-maintained, WinFSP-verified Rust crate found for this) or WinFSP's own native API (the `winfsp`/`winfsp-sys` crates - GPL-3.0, needs a licensing decision before use in this MIT project).

---

## 1. Write cache (per open file, analogous to Scala's `MemCache`/`FileCache`/`Allocation`/`WriteCache`)

- A process-wide RAM budget: `AtomicU64`, initialized from `sysinfo`-detected total system RAM × a configurable fraction, overridable via a CLI flag. Reservation via a lock-free CAS retry loop (`compare_exchange`), same shape as Scala's `tryAcquire` - non-blocking: fails fast to the disk-spillover path rather than waiting.
- Per-write-chunk (not per-file) decision between the RAM cache and a lazily-created sparse temp file: exactly Scala's granularity - a single open file's pending writes can and do end up split between RAM and disk, with the split able to shift on overwrite.
- A sparse "zero-hole" tracker for writes past current EOF / truncate-grow, storing only `(position, length)` pairs, never materializing real zero bytes until actually read.
- Read-back for a file that's mid-edit merges, in order: the live in-progress cache → any older not-yet-persisted generations still queued (a file can be written, closed, reopened, and rewritten again before the first flush completes - Scala keeps a small queue of these, not just one) → the already-persisted content (via the same chunk-read path the read-only phase's `read` uses).
- `memChunk`-equivalent size constant: re-derived from FUSE/I/O alignment (matching the `max_write` mount option), *not* from Scala's G1GC-humongous-object rationale, which has no meaning for Rust's allocator.

## 2. Persist pipeline (on last-handle release)

Reuses the `store` command's existing many-readers/one-writer architecture rather than inventing a new one: the accumulated write-cache content (merged with previously-persisted bytes for any untouched holes, exactly as Scala's `readFromLts`-equivalent does) is read once, run through the existing `BufferingHashingChunker` + chunk dedup lookup (replacing Scala's whole-file MD5 - our dedup granularity is already chunks, not whole files, which is a structurally better fit for a mutable live filesystem than hashing-the-whole-file-after-the-fact anyway), new chunks get written via `cli::chunk_store`'s existing `SpaceAllocator`/`write_chunk_bytes` (see §3 - already implemented, reuses gaps rather than only appending), and the tree is updated via the soft-delete-old + insert-new pattern per decision #2 above. Backpressure (§ formula above) is applied per chunk in the `write` callback, on whatever thread `fuser` invokes it from.

## 3. Prerequisite: store-space reuse after `reclaim-space`

**Implemented** - `docs/plans/implemented/03-chunk-extents.md` (multi-part chunk extents + `cli::chunk_store::SpaceAllocator`). Was a prerequisite for this phase (a write path allocating store space needed a real allocator, not just an append cursor), built ahead of it for that reason. The persist pipeline in §2 above builds directly on it.

## 4. Handle/refcount model

Mirrors Scala's `Handle{count, current_dataId, current_cache, persisting_queue}` keyed by the same id used as the FUSE inode/handle - multiple concurrent opens of the same file share one entry and a simple refcount; the write cache only gets handed to the persist pipeline once the count reaches zero.

## 5. New FUSE callbacks needed

`write`, `create`, `mkdir`, `rmdir`, `unlink`, `rename` (with a `copyWhenMoving`-equivalent decision deferred - not clearly needed without the GUI toggle mechanism that motivated it in Scala; revisit when this phase is actually planned in detail), `truncate`/`setattr` size changes, `utimens`. A single coarse `Mutex` for tree-structure-mutating operations, matching Scala's one global `synchronizeTreeModification` lock - simple, and Scala's own experience suggests it isn't a real bottleneck at typical single-mount FUSE call volumes.

---

## Not yet decided

- Exact sequencing/commit breakdown for implementation (not planned in this level of detail yet, unlike the read-only phase's plan).
- Whether `--read-only`/read-write mode selection is a flag on `backup mount` or a separate consideration once this phase exists.
