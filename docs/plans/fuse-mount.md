# Mount a FUSE dedup filesystem (read-only now, read-write designed for later) + fix `list <file>` missing mtime

## Context

The Scala tool this Rust rewrite replaces can be FUSE-mounted read-only or read-write, with an in-process write cache (RAM, spilling to a temp file under memory pressure) and a backpressure mechanism that slows writers down as the async persist pipeline falls behind. The user wants the same capability here, modeled closely on the Scala design, plus a small unrelated bug fix: `list <path-to-a-file>` doesn't show the file's mtime (only `list <path-to-a-directory>`'s per-child listing does), because both call the same shared `print_file_info` helper, which never prints a timestamp.

Research into the Scala implementation (`server/Server.scala`, `server/Backend.scala`, `server/{Handles,Handle}.scala`, `cache/{MemCache,FileCache,WriteCache,Allocation,CacheBase}.scala`, `server/FreeAreas.scala`, `Main.scala`'s `mount` entry point) surfaced the exact mechanisms this plan is based on - cited inline below.

Four design decisions were discussed and settled before writing this plan:
1. **Backpressure formula**: replicate Scala's `cacheLoadDelay = bytesInPersistQueue * persistQueueSize / 1_000_000_000` (ms), applied per write chunk, **uncapped** - not a bug to fix. It's a self-stabilizing negative-feedback throttle *because* it's applied synchronously before a chunk is accepted into the cache: rising backlog → rising delay → falling input rate → backlog growth slows. Capping the sleep either breaks that property (if nothing replaces it, backlog can now grow unboundedly once the cap is saturated) or, if paired with a hard block past some threshold, replaces a smooth slowdown with a jarring full stop - worse for the filesystem user in both cases, not better. Add nothing beyond breaking a single very-long sleep into small slices that recheck for shutdown - and only if that's genuinely ~3-4 lines (YAGNI otherwise).
2. **Content mutability for FUSE writes**: reuse the existing soft-delete-old-row + insert-new-row pattern `apply_backup_batch` already uses on a content change, rather than adding a new `AFTER UPDATE OF content_id` trigger for true in-place mutation. Accepted trade-off: a frequently-rewritten file (editor autosave, a log file kept open) accumulates soft-deleted history rows until `reclaim-space` runs, instead of behaving like a normal mutable filesystem with no history at all.
3. **Write-cache RAM budget**: auto-detected from total system RAM (new dependency: `sysinfo`) with a CLI override flag - there's no JVM heap ceiling to anchor Scala's `(maxMemory - 64MB) * 0.7` formula to, so this needs a Rust-native basis.
4. **Phasing**: **Phase 1 (this round): read-only mount, fully working.** No write cache, no backpressure, no schema change, no store-space allocator needed - it's almost entirely wiring `fuser` callbacks to already-built `db`/`store` query functions. **Phase 2 (designed here, implemented in a later round): read-write**, with the write cache, backpressure, and persist pipeline.

Platform scope: Linux only (via the `fuser` crate, which talks to `/dev/fuse` directly and shells out to `fusermount`/`fusermount3` for the privileged mount syscall - both confirmed present in this environment). Matches every other part of this Rust port, which has no Windows-specific code anywhere. Cross-platform (winfsp) support is out of scope, not attempted.

**Correction to an earlier claim**: while planning `reclaim-space` (already shipped), I said physical store space is never reclaimed "matching Scala's own `LongTermStore` limitation." That's only half true: Scala's `LongTermStore` class itself indeed has no delete/truncate operation - but a layer above it, `server/FreeAreas.scala`, computes gaps from deleted `DataEntries` at mount time and hands them back out (first-fit, splitting across possibly several non-contiguous gaps per reservation) for *new* writes to overwrite. So Scala *does* effectively reuse reclaimed space at the whole-file granularity, it just does it by letting new writes land on top of dead bytes rather than by any explicit free/shrink operation. Our current `store` design has no equivalent at all - see §3 below, likely a prerequisite to tackle before Phase 2 (and arguably independent of FUSE entirely).

---

## 0. Quick fix: `list <file>` missing mtime

`cli/src/format.rs`'s `print_file_info(path_label, name, size)` is shared by `stats <path>` and `list <path>` for the file case, and never prints a timestamp - unlike `list`'s directory-listing path (`print_entry` in `list.rs`), which does. Add a `time_millis: i64` parameter to `print_file_info`, print it on a line (matching the format `list`'s directory case already uses), and update both call sites (`list.rs`, `stats.rs`) and their tests. One small commit, first, independent of everything else in this plan.

---

## 1. Phase 1: read-only FUSE mount

### New pieces

- New dependency `fuser` (the standard, actively maintained Rust FUSE binding - implements the low-level `/dev/fuse` kernel protocol directly, no libfuse linkage needed for the protocol itself).
- New CLI subcommand `backup mount <mountpoint>` (`cli/src/mount.rs`), always read-only in this phase (a `--read-only`-shaped flag isn't needed yet since there's no other mode to select between; Phase 2 introduces the write path and the flag to choose).
- No `db`/`store` schema or API changes needed at all - every FUSE callback in this phase is answerable with functions that already exist: `db::resolve_path`/`db::get_tree_entry`, `db::list_children`, `db::file_size`, `db::ordered_content_chunks`, `store::LongTermStore::read`.

### Inode mapping

FUSE reserves inode `1` for the mount root by convention; our tree root is `tree_entries.id = 0`. Map `fuse_ino = tree_entries.id + 1` / `tree_entries.id = fuse_ino - 1` - a trivial, symmetric shift, applied at the boundary of every callback (not stored anywhere). The FUSE file handle (`fh`) reuses the inode directly, mirroring Scala's simplification (the file's own DB id *is* the FUSE handle, no separate handle-id allocator) - correct here too, since Phase 1 has no per-handle mutable state to key by something else.

### Callbacks implemented (via `fuser::Filesystem`)

| Callback | Backed by |
|---|---|
| `lookup(parent, name)` | `db::find_tree_entry(conn, parent-1, name)` (via `db::resolve_path`'s single-component form, or `find_tree_entry` directly) |
| `getattr(ino)` | `db::get_tree_entry(conn, ino-1)` + `db::file_size` for files |
| `readdir(ino, offset)` | `db::list_children(conn, ino-1)`, paginated via `offset` |
| `open(ino, flags)` | sets `fh = ino`; rejects any write-intent flag (`O_WRONLY`/`O_RDWR`) with `EROFS` since this phase is read-only |
| `read(ino, fh, offset, size)` | `db::ordered_content_chunks` for the entry's `content_id` + `store::LongTermStore::read` for the requested byte range, reusing the same range-mapping logic `restore`/`check` already have |
| `release` | no-op (nothing to flush in a read-only mount) |
| `statfs` | minimal/approximate values - Scala's own Linux implementation is a no-op here too, per research; not worth over-building |
| everything else (`write`, `mkdir`, `unlink`, `rename`, `create`, `truncate`, `setattr`'s write-affecting fields, etc.) | not implemented; `fuser`'s default trait methods already return `ENOSYS`, and/or explicit `EROFS` where a clearer signal helps (matches Scala's per-operation `EROFS` gate on the write-side callbacks) |

Permissions/uid/gid reported in `getattr`: read-only mode bits (matching Scala's Linux convention), owner reported as whoever's making the request (`req.uid()`/`req.gid()` from `fuser`'s `Request`, same as Scala reads from the FUSE call context) - there's no real multi-user ownership model here, this is cosmetic (FUSE without the `default_permissions` mount option doesn't have the kernel enforce these bits anyway; the mode shown is informational for tools like `ls -l`).

### Mount/unmount lifecycle

Use `fuser::mount2` (blocking - matches Scala's blocking `fs.mount(..., true, ...)` call). No signal handling added for this phase: unmounting is triggered externally (`fusermount -u <mountpoint>` / `umount`, run from another terminal), exactly like the Scala tool relies on jnr-fuse's own teardown rather than an explicit `sys.addShutdownHook` - no evidence of one there either. Mount-point validation before mounting: must already exist as a directory and be empty (matches Scala's non-Windows check).

### Sequencing (commits)

1. `list <file>` mtime fix (§0).
2. Add `fuser` dependency; `cli/src/mount.rs` skeleton (CLI arg parsing, mount-point validation, inode mapping helper, `mount2` wiring with a `Filesystem` impl that only handles `lookup`/`getattr` for now) + tests.
3. `readdir`.
4. `open`/`read`/`release` (the actual file-content path).
5. `statfs` + permission/uid/gid polish + README section.
6. Manual smoke test: mount a repo with real content, `ls`/`cat`/`stat` through the mountpoint, compare against `list`/`stats`/direct file reads, unmount.

---

## 2. Phase 2 (designed now, **not implemented this round**): read-write

Recorded here so Phase 1's code doesn't need reworking later.

### Write cache (per open file, analogous to Scala's `MemCache`/`FileCache`/`Allocation`/`WriteCache`)

- A process-wide RAM budget: `AtomicU64`, initialized from `sysinfo`-detected total system RAM × a configurable fraction, overridable via a CLI flag. Reservation via a lock-free CAS retry loop (`compare_exchange`), same shape as Scala's `tryAcquire` - non-blocking: fails fast to the disk-spillover path rather than waiting.
- Per-write-chunk (not per-file) decision between the RAM cache and a lazily-created sparse temp file: exactly Scala's granularity - a single open file's pending writes can and do end up split between RAM and disk, with the split able to shift on overwrite.
- A sparse "zero-hole" tracker for writes past current EOF / truncate-grow, storing only `(position, length)` pairs, never materializing real zero bytes until actually read.
- Read-back for a file that's mid-edit merges, in order: the live in-progress cache → any older not-yet-persisted generations still queued (a file can be written, closed, reopened, and rewritten again before the first flush completes - Scala keeps a small queue of these, not just one) → the already-persisted content (via the same chunk-read path Phase 1's `read` uses).
- `memChunk`-equivalent size constant: re-derived from FUSE/I/O alignment (matching the `max_write` mount option), *not* from Scala's G1GC-humongous-object rationale, which has no meaning for Rust's allocator.

### Persist pipeline (on last-handle release)

Reuses the store command's existing many-readers/one-writer architecture rather than inventing a new one: the accumulated write-cache content (merged with previously-persisted bytes for any untouched holes, exactly as Scala's `readFromLts`-equivalent does) is read once, run through the existing `BufferingHashingChunker` + chunk dedup lookup (replacing Scala's whole-file MD5 - our dedup granularity is already chunks, not whole files, which is a structurally better fit for a mutable live filesystem than hashing-the-whole-file-after-the-fact anyway), new chunks get appended via the store's existing atomic-cursor allocator (no free-list/gap-reuse allocator planned - matches the already-accepted decision that `reclaim-space` doesn't reclaim physical store bytes yet either), and the tree is updated via the soft-delete-old + insert-new pattern per decision #2 above. Backpressure (§ formula above) is applied per chunk in the `write` callback, on whatever thread `fuser` invokes it from.

### Handle/refcount model

Mirrors Scala's `Handle{count, current_dataId, current_cache, persisting_queue}` keyed by the same id used as the FUSE inode/handle - multiple concurrent opens of the same file share one entry and a simple refcount; the write cache only gets handed to the persist pipeline once the count reaches zero.

### New FUSE callbacks needed

`write`, `create`, `mkdir`, `rmdir`, `unlink`, `rename` (with a `copyWhenMoving`-equivalent decision deferred - not clearly needed without the GUI toggle mechanism that motivated it in Scala; revisit when Phase 2 is actually planned in detail), `truncate`/`setattr` size changes, `utimens`. A single coarse `Mutex` for tree-structure-mutating operations, matching Scala's one global `synchronizeTreeModification` lock - simple, and Scala's own experience suggests it isn't a real bottleneck at typical single-mount FUSE call volumes.

---

## 3. Prerequisite likely to tackle first (raised by the user, not yet decided): store-space reuse after `reclaim-space`

**The problem, concretely**: store 3×1000-byte (1-chunk) files, contiguous at `[0,1000)`, `[1000,2000)`, `[2000,3000)`. Delete the middle one and run `reclaim-space` - its `chunks` row is gone, but the physical bytes at `[1000,2000)` are still sitting in `store::LongTermStore`'s files, now referenced by nothing. Store a new 1200-byte file next: `cli/src/store.rs`'s worker allocates its position via `ctx.position_cursor.fetch_add(length, Ordering::SeqCst)`, a cursor seeded once from `SELECT COALESCE(MAX(stop), 0) FROM chunks` - it only ever knows "the current highest `stop` among rows that still exist," never revisits gaps left by deleted rows. So the new file lands at `[3000, 4200)`, and `[1000,2000)` is now a **permanent** hole - the store file only ever grows, even across repeated delete+reclaim+rewrite cycles. This is a real, currently-unaddressed gap, not something already designed around.

### Options considered

**Option A - full multi-part chunks, mirroring Scala's `DataEntries`/`FreeAreas` (and Go's abandoned `free_areas` bucket, which has the same shape but was never actually implemented there)**: add a `chunk_extents(chunk_id, seq, start, stop)` table (a chunk's bytes become the concatenation of 1..N physical extents, only conceptually one contiguous range in the common case), plus a free-list allocator that can satisfy one chunk's space need by spanning several small gaps at once (first-fit, splitting the last gap used). Most faithful to Scala/Go's *intent*, but ripples into every consumer of chunk byte ranges - `check`, `restore`, `store`'s own writer, and Phase 2's future FUSE read/write paths would all need to read/write N extents per chunk instead of one. Also: Scala's `FreeAreas.reserve` is guarded by a lock that's admittedly moot there (only ever called from Scala's one persist thread) - but *our* `store` command allocates space directly from many parallel rayon worker threads (a deliberate choice, so allocation doesn't have to funnel through the single DB-writer thread and become an I/O bottleneck), so a shared free-list here would need real synchronization (a `Mutex`) across genuinely-concurrent callers, not a vestigial one.

**Option B - keep chunks single-extent, add a same-run gap-tracker, no schema change**: `chunks.start`/`stop` stays exactly as is. At the start of a `store` run, compute the sorted list of gaps between existing chunks' `(start, stop)` ranges (the same information already used to seed the append cursor, just also keeping the holes instead of only the trailing edge) into an in-memory `Vec<(start,stop)>` shared via a `Mutex` across worker threads. A new chunk first tries to fit into a single gap big enough (first-fit; on a partial fit, split the gap and keep the remainder); only if no *single* gap is large enough does it fall back to the existing append-past-the-end cursor. No schema migration, no multi-part-chunk complexity anywhere else in the codebase. **User's objection (decisive): over time this risks accumulating many small, permanently-unfillable gaps** (never exactly the right size for a future chunk) - and we'd have no visibility into that without deliberately tracking/reporting it (e.g. in `stats`). Given Option A, done with good encapsulation, shouldn't need to spread multi-part complexity across the codebase, and the user is fine with coarse ("one accessor at a time, everyone else waits") locking since the allocator operations are tiny, fast, in-memory work - **Option A is the direction to take**, not B.

**Option C - do nothing yet, revisit later**: keep pure append-only. Not recommended: Phase 2's FUSE write path would otherwise *also* adopt the same flawed model, and a mounted filesystem plausibly produces many more small write/delete/rewrite cycles over its lifetime than batch `store` runs do - compounding the waste, and making it harder to retrofit a shared allocator later once two independent write paths (store's workers, and Phase 2's future persist thread) both depend on simple unconditional append.

### Decision: Option A, encapsulated

Build the multi-part chunk-extent model, but keep its complexity contained to one place rather than letting "a chunk is N extents" leak into every consumer:

- New table, e.g. `chunk_extents(chunk_id INTEGER REFERENCES chunks(id), seq INTEGER, start INTEGER, stop INTEGER, PRIMARY KEY (chunk_id, seq))` - `chunks` drops its own `start`/`stop` columns (or keeps them denormalized for the common single-extent case - to be decided during detailed design; likely cleaner to drop them and always go through extents, even if 99% of chunks have exactly one).
- A small, self-contained module (in `db` or `store`) exposing only "resolve chunk N's bytes as a single logical read/write," internally walking however many extents that takes - so `check`, `restore`, `store`, and the future FUSE read/write path all keep calling one function per chunk, unaware of how many physical extents back it.
- A free-list allocator: computed from `chunk_extents` gaps (same shape as Scala's `Database.freeAreas()`/`endOfStorageAndDataGaps`), first-fit, splitting across multiple gaps if needed to satisfy one allocation, guarded by a single coarse lock ("one accessor at a time, everyone else waits" - acceptable per the user, since these are small, fast, in-memory operations, not I/O) shared across the parallel `store` worker threads (a real synchronization need here, unlike Scala's single-persist-thread design where the equivalent lock is vestigial).
- `stats` should surface fragmentation visibility the user flagged as a real risk even with proper multi-part reuse (many small gaps that are individually usable but signal a fragmented store): e.g., free-list gap count and total free bytes, so a user can tell store health is degrading before it becomes a real problem.

### Not yet decided / to design in detail before implementing
- Exact schema for `chunk_extents` (whether `chunks.start`/`stop` are dropped entirely or kept as a denormalized single-extent fast path).
- Exact shape and location of the free-list structure (persisted table vs. recomputed at the start of each `store`/mount session like the append cursor is today) and its allocator API.
- Where the encapsulating module lives (`db` vs. `store` crate) and its exact function signatures.
- Whether/how this interacts with `reclaim-space` (does reclaiming now also need to feed freed extents back into a persisted free-list, or is recomputing from `chunk_extents` gaps at the start of each session sufficient?).
- Relationship to Phase 1/Phase 2 of the FUSE mount above: likely sequenced *before* Phase 2 (which would otherwise inherit the flawed append-only allocation), independent of Phase 1 (which is read-only and never allocates store space at all).

---

## Verification (Phase 1)

- `cargo fmt --check && cargo clippy -- -D warnings && cargo test` workspace-wide after every commit.
- Unit tests per callback against a temp repo with seeded rows (same pattern as every other command in this codebase).
- A real mount/unmount integration test or two, given `/dev/fuse` is accessible in this environment (confirmed: `crw-rw-rw-`, `fusermount`/`fusermount3` present) - mount into a temp directory, perform file ops, unmount, in-process.
- Manual smoke test via the `run` skill: build a repo with `store`, mount it, `ls`/`cat`/`stat` through the mountpoint from a shell, compare byte-for-byte and metadata against the source and against `list`/`stats` output, then unmount.
