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

## 3. Prerequisite: store-space reuse after `reclaim-space`

Raised during this planning session, detailed and decided separately in `docs/plans/chunk-extents.md` (multi-part chunk extents + an encapsulated free-list allocator, Option A there) - a prerequisite for Phase 2 (read-write) above, since that's the other place that would otherwise inherit today's flawed append-only space allocation. Independent of Phase 1 (read-only, never allocates store space). Not yet decided whether to build it before or after Phase 1.

---

## Verification (Phase 1)

- `cargo fmt --check && cargo clippy -- -D warnings && cargo test` workspace-wide after every commit.
- Unit tests per callback against a temp repo with seeded rows (same pattern as every other command in this codebase).
- A real mount/unmount integration test or two, given `/dev/fuse` is accessible in this environment (confirmed: `crw-rw-rw-`, `fusermount`/`fusermount3` present) - mount into a temp directory, perform file ops, unmount, in-process.
- Manual smoke test via the `run` skill: build a repo with `store`, mount it, `ls`/`cat`/`stat` through the mountpoint from a shell, compare byte-for-byte and metadata against the source and against `list`/`stats` output, then unmount.
