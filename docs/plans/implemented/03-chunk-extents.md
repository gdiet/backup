# Multi-part chunk extents: reuse store space freed by `reclaim-space`

## Context

Raised while planning the FUSE mount (see `docs/plans/fuse-mount-readwrite.md` §3, which points back here), but independent of it: our `store` command never reuses space freed by `reclaim-space`.

**The problem, concretely**: store 3×1000-byte (1-chunk) files, contiguous at `[0,1000)`, `[1000,2000)`, `[2000,3000)`. Delete the middle one and run `reclaim-space` - its `chunks` row is gone, but the physical bytes at `[1000,2000)` are still sitting in `store::LongTermStore`'s files, now referenced by nothing. Store a new 1200-byte file next: `cli/src/store.rs`'s worker allocates its position via `ctx.position_cursor.fetch_add(length, Ordering::SeqCst)`, a cursor seeded once from `SELECT COALESCE(MAX(stop), 0) FROM chunks` - it only ever knows "the current highest `stop` among rows that still exist," never revisits gaps left by deleted rows. So the new file lands at `[3000, 4200)`, and `[1000,2000)` is now a **permanent** hole - the store file only ever grows, even across repeated delete+reclaim+rewrite cycles.

(Correction to an earlier claim, made while planning `reclaim-space`, that this "matches Scala's own `LongTermStore` limitation": only half true. Scala's `LongTermStore` class itself indeed has no delete/truncate operation, but a layer above it, `server/FreeAreas.scala`, computes gaps from deleted `DataEntries` at mount time and hands them back out - first-fit, splitting across possibly several non-contiguous gaps per reservation - for *new* writes to overwrite. Scala *does* effectively reuse reclaimed space, just by letting new writes land on top of dead bytes rather than via any explicit free/shrink operation.)

## Options considered

**Option A - full multi-part chunks, mirroring Scala's `DataEntries`/`FreeAreas`** (and Go's `free_areas` bucket, same shape but never actually implemented there - abandoned/incomplete project): a chunk's bytes become the concatenation of 1..N physical extents; a free-list allocator can satisfy one allocation by spanning several small gaps at once (first-fit, splitting the last gap used).

**Option B - keep chunks single-extent, add a same-run gap-tracker, no schema change**: compute gaps between existing chunks' current `(start, stop)` ranges once per `store` run, try to fit a new chunk into a single gap, else fall back to appending. **Rejected**: risks accumulating many small, permanently-unfillable gaps over time (never exactly the right size for a future chunk), with no visibility into the problem getting worse.

**Option C - do nothing, keep pure append-only**: rejected - a future FUSE write path (see `fuse-mount.md` Phase 2) would inherit the same flaw, and a mounted filesystem likely produces far more small write/delete/rewrite cycles over its lifetime than batch `store` runs, compounding the waste and making it harder to retrofit a shared allocator later once two independent write paths depend on simple append.

## Decision: Option A, encapsulated

Multi-part chunks, but keep "a chunk is N extents" contained to one place rather than letting it leak into every consumer of chunk bytes.

### Schema

```sql
CREATE TABLE chunks (
  id        INTEGER PRIMARY KEY,
  length    INTEGER NOT NULL,
  hash      BLOB    NOT NULL,
  ref_count INTEGER NOT NULL DEFAULT 0,
  UNIQUE (length, hash),
  CONSTRAINT chk_chunks_ref_count CHECK (ref_count >= 0)
);

CREATE TABLE chunk_extents (
  chunk_id INTEGER NOT NULL REFERENCES chunks(id) ON DELETE CASCADE,
  seq      INTEGER NOT NULL,
  start    INTEGER NOT NULL,
  stop     INTEGER NOT NULL,
  PRIMARY KEY (chunk_id, seq)
);
```

`start`/`stop` move out of `chunks` into this new 1:N table; `chunks` keeps only its dedup identity (`length`, `hash`) and `ref_count`. `ON DELETE CASCADE` on `chunk_id` (same pattern already used for `content_chunks.content_id → contents.id`): when `reclaim-space` deletes a `chunks` row, its extents disappear with it, and those freed byte ranges are exactly what the next `store` session's gap computation picks up.

**No persisted free-list.** Gaps are recomputed at the start of each `store` run - `SELECT start, stop FROM chunk_extents ORDER BY start`, then derive gaps (and the open-ended trailing region) from that - the same shape as the current `MAX(stop)`-seeded cursor, and the same approach Scala's `Database.freeAreas()` takes (recomputed fresh on every mount, not incrementally maintained). Avoids a second, persisted piece of state that could drift or need crash-consistency handling of its own.

### Encapsulation

- **Allocator**: a pure in-memory structure, no DB access - `Mutex<Vec<(start, stop)>>` (sorted, last entry conceptually open-ended), `reserve(size) -> Vec<(start, stop)>`: first-fit, splits across multiple gaps if one alone doesn't suffice, falls back to appending past the known end. A single coarse lock is enough ("one accessor at a time, everyone else waits") - these are small, fast, in-memory operations, not I/O, so contention cost is negligible even shared across `store`'s parallel worker threads (unlike Scala, where the equivalent lock is vestigial since only one persist thread ever calls it - ours is a real synchronization point, since our worker threads allocate space directly rather than funneling through the single DB-writer thread).
- **Two helper functions, and only these two, know a chunk can be multi-part**: something like `read_chunk_bytes(conn, store, chunk_id) -> Vec<u8>` (looks up the chunk's extents, reads and concatenates) and its write-side counterpart (given bytes and a set of extents from the allocator, split the bytes and write each extent). Every current and future consumer - `check`'s hash verification, `restore`'s file reconstruction, `store`'s own writer, the FUSE mount's future read/write path - calls these instead of touching `store::LongTermStore::read`/`write` with a single `(start, stop)` directly.
- **Fragmentation visibility in `stats`**: since gaps are already computed once per `store` run, expose the same computation as a `db` function (e.g. `free_space_summary(conn) -> (gap_count, total_free_bytes)`) and surface it in `stats`'s repo-wide output, so growing fragmentation is visible before it becomes a real problem - directly addressing the risk that killed Option B, now as an observability feature rather than an accepted blind spot.

### What this touches

- `db`: the schema migration itself; `ChunkRef` (currently `New { length, hash, position }` → becomes `New { length, hash, extents: Vec<(u64,u64)> }`); `apply_backup_batch`'s chunk-insert logic (insert into `chunks`, then one row per extent into `chunk_extents`, instead of `start`/`stop` directly on `chunks`); a new `free_space_summary` query.
- `store` (or a small new module - exact location TBD): the allocator struct; the two read/write-chunk-bytes helpers.
- `cli`: `store.rs`'s worker calls the allocator instead of the atomic cursor and the new write helper instead of a direct `LongTermStore::write`; `check.rs`/`restore.rs` switch their single `store.read(chunk.start, ...)` call to the new read helper - otherwise unchanged; `stats.rs` gains the fragmentation summary line.

## Not yet decided / to design in detail before implementing

- Whether `chunks` keeps `start`/`stop` denormalized for the (presumably common) single-extent case, or always goes through `chunk_extents` even for one extent. Leaning toward always-through-the-table for simplicity (one code path, no "is this the common case" branch), but worth a final look at the query/insert overhead once real data volumes are known.
- Exact function signatures and which crate the allocator/helpers live in (`db`, `store`, or a small new glue module/crate).
- How `reclaim-space` and `stats`'s fragmentation summary share the gap-computation code (one `db` function used by both, presumably).
- Sequencing relative to the FUSE mount plan: this is a prerequisite for FUSE Phase 2 (read-write), independent of FUSE Phase 1 (read-only, never allocates store space). Not yet decided whether to build it before or after Phase 1 - Phase 1 has no dependency on it either way.
