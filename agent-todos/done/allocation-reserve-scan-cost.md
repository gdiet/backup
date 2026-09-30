# `allocation::reserve` scans all of `chunk_extents` on every call

**Why parked**: came up while cross-checking the Scala-migration-tool design against
`rust-1st-attempt` - unprompted, and out of scope for that task, which does not itself call this
function (see "Recording a migrated chunk at its existing position" in
`docs/design/scala-migration-tool.md`).
**Size**: medium (confirm with the user before starting)
**Opened**: 2026-09-29, by a Windows/Claude Code Desktop session
**Context**: `crates/db/src/allocation.rs`'s `reserve`, `docs/design/byte-store.md`'s
DESIGN-STORE-003.

## What was found

`reserve`'s own doc comment already states it "scans every `chunk_extents` row on every call; free
space is not cached or materialized anywhere" - a deliberate simplicity choice, not an oversight.
Reading the implementation shows this is not just "reads more rows than strictly needed" but a full
unbounded read: `SELECT start, stop FROM chunk_extents ORDER BY start` has no `WHERE`/`LIMIT`, and
the result is collected into a `Vec` in full before the gap-search loop even starts (so the loop's
own early `if remaining == 0 { break; }` does not save any query cost). `chunk_extents` does have an
index on `start` (`chunk_extents_start_idx`), so the `ORDER BY` does not need a separate sort pass,
but every row still gets read.

`reserve` is called once per newly-written, not-yet-deduplicated chunk (via
`content::reserve_and_insert_chunk`). That makes populating a repository with `N` distinct chunks
cost `O(N)` calls each scanning up to `O(N)` rows - `O(N^2)` total, not `O(N)`. Not yet benchmarked
against a realistic chunk count to see where this actually becomes noticeable in practice - the real
sample export used for the migration-tool work this session implies large real repositories can
reach on the order of several million distinct chunks, which is the scale where a quadratic cost
would plausibly start to matter, but this is not confirmed empirically.

## What to look at

1. Benchmark empirically (a throwaway script/test against a repository seeded with a realistic
   number of chunk_extents rows, per AGENTS.md's debugging discipline - do not just estimate from
   Big-O reasoning) at what chunk count this becomes a real bottleneck for ordinary `dfs ingest`/
   mount-write-path usage, not just migration.
2. If it is a real problem: whether a narrower query suffices (e.g. only reading extents from some
   remembered cursor position onward, revisiting earlier gaps only when reclaim actually creates
   one) or whether tracking free space needs a different shape entirely. `docs/design/byte-store.md`'s
   DESIGN-STORE-003 would need updating either way - it does not currently discuss this trade-off at
   all.
3. Whether the currently-being-built Scala-migration tool makes this materially worse in practice
   even though it never calls `reserve` itself: a very large migration followed by ordinary backup
   activity into the now-much-larger repository would be the first realistic case hitting a high
   `chunk_extents` row count from something other than gradual, incremental ingest.

## Data basis: the developer's real repository (added 2026-09-30)

The developer's own large backup repository is the first real case this item has to serve. Its
Scala-side statistics (from the real `fsc db-backup` export, confirmed by two independent counts):
6,763,253 tree entries (526,835 directories, 25,307 explicitly empty files, 6,211,111 files with
content), 727,141 distinct old contents, ~2.23 TB of distinct content, ~19.5 TB logical size.

**Expected production setting: 17, 18 or 19 bits, so roughly 3 to 10 million `chunk_extents` rows
from the first day after migrating** (and growing from there with every ordinary write). Every
ordinary write that creates a new, not yet deduplicated chunk - `dfs ingest` and a read-write mount
alike, for the whole life of the repository, not only the first one after migration - reads all of
those rows in `allocation::reserve`. So this is not a far-off scale concern: the repository starts in
the range where the full scan per new chunk is expected to hurt, and it has not been benchmarked yet.

Rough estimate of the row counts, scaled from the measurements on the 250 MB real test repository
(average chunk size about 1.18 x 2^bits, deduplication ratio per chunk size as measured there, about
one extent per unique chunk; real values may differ by a factor of about 1.5 either way):

| bits | unique chunks (= `chunk_extents` rows) | chunk occurrences | est. metadata database | x21 (1 live + 20 backups) |
|---|---|---|---|---|
| 16 | ~19.0 million | ~28.9 million | ~4.1 GB | ~86 GB |
| 17 | ~10.0 million | ~14.4 million | ~2.8 GB | ~59 GB |
| 18 | ~5.2 million | ~7.2 million | ~2.1 GB | ~44 GB |
| 19 | ~2.8 million | ~3.6 million | ~1.8 GB | ~37 GB |
| 20 | ~1.5 million | ~1.8 million | ~1.6 GB | ~33 GB |

The metadata size assumes about 200 bytes per tree entry (with its content row) and about 145 bytes
per chunk, both measured as marginal costs on the test repository. The tree part (~1.4 GB) is paid at
every chunk size, which is why the larger sizes look better here than they did on the small test
repository once all 21 copies are counted.

For the benchmark in "What to look at" above, 3 to 10 million rows is therefore the range to test at,
not an upper bound to extrapolate towards. A fixture with that many `chunk_extents` rows does not need
real data: the table can be filled directly.

## Additional candidates and findings (added 2026-09-30, by a cloud Claude Code session)

### Candidate A: an in-memory list of free gaps, built once per write session

Build a sorted list of the free gaps from `chunk_extents` once, when a write session starts. The last
gap runs from the end of the used byte range to `i64::MAX`. `reserve` then takes ranges from this
list instead of querying the database.

The Scala implementation works this way. It reads all `DataEntries` rows once at startup, but only when
the repository is opened read-write. It sorts them, derives the gaps, and appends the open-ended last
gap. Its `reserve` is a `synchronized` operation on the in-memory list. It never queries the database.

What makes this candidate fit here:

- **Thread safety comes for free.** `Repository` keeps its connection and its name cache together in
  one `Locked` struct behind one `Mutex`. The gap list can live in the same struct. Every `reserve`
  call already runs inside `with_transaction`, so no second lock is needed.
- **Nothing new is persisted.** The list is derived from `chunk_extents` at the start of every
  session. It cannot go stale across restarts, and there is no schema change or migration.
- **The single-writer slot makes the list valid for the whole session** (DESIGN-MOUNT-008). No other
  process can change `chunk_extents` while the session runs.

What differs from the Scala setting, and needs deliberate handling:

- **Reclaim can run inside a write session here.** `content::reclaim_content` is called from
  `tree.rs` (`purge_deleted_entry` and the purge cascade) and from `settle_pending_write`'s abandon
  path. Each of these deletes `chunks` rows and thereby creates gaps while the session is running.
  The in-memory list must learn about them. In Scala, reclaim only ran offline.
- **Transactions can roll back.** `with_transaction` rolls back when the closure returns `Err`. The
  two directions are not symmetric. A reservation that is taken from the list but rolled back only
  leaks space until the next session, which is safe. A gap that is added to the list but rolled back
  would make the allocator hand out space that is still in use, which corrupts data. Gaps freed by
  reclaim must therefore only enter the list after the surrounding transaction has committed.
- **The build cost moves to the session start.** It is one full scan instead of one per new chunk.
  Whether that start-up cost is acceptable at 3 to 10 million rows needs to be measured. Building
  lazily on the first `reserve` avoids the cost for sessions that never write a new chunk.
  `insert_chunk_at` (migration tool) needs no special handling as long as the tool does not share a
  session with ordinary allocation.

### Candidate B: a better index (measured in a small orienting probe, not yet the real benchmark)

The existing `chunk_extents_start_idx` is on `start` only. `EXPLAIN QUERY PLAN` reports
`SCAN chunk_extents USING INDEX chunk_extents_start_idx` for the current query. That means SQLite
walks the index and then looks up `stop` in the table for every row.

An index on `(start, stop)` makes the same query a `COVERING INDEX` scan without those lookups.
A throwaway probe in a scratch directory (2 million synthetic rows, Python `sqlite3`, WAL mode, this
container) gave about 1.1 s warm (2.9 s cold) with the current index and about 0.85 s with the
covering index. That is a modest gain of roughly 25 percent, and the scan remains linear. The probe
also showed that `SELECT stop FROM chunk_extents ORDER BY start DESC LIMIT 1` (the high-water mark)
answers in well under a millisecond with either index, because `chunk_extents` positions are
disjoint.

Conclusions so far:

- A covering index alone does not remove the `O(N^2)` behavior. It only lowers the constant.
- No index can answer "where is the first gap" without a scan, because in a repository without
  reclaimed space there is no gap, and proving that requires looking at every row. Some form of
  remembered knowledge is needed: the in-memory list (Candidate A), a watermark, or a persisted
  free-space table (Candidate C).
- The high-water-mark query is cheap. If a session could know that no gaps exist below the high-water
  mark, `reserve` would be `O(log N)` in the common case. That knowledge is exactly what Candidate A
  or Candidate C provides.
- Replacing `chunk_extents_start_idx` by a `(start, stop)` index needs a schema migration. The
  repository has no stability promise yet, but the change still has to be made deliberately.

### Candidate C: a persisted free-space table

A `free_extents` table, maintained in the same transaction as every reservation and every reclaim,
would make `reserve` an `O(log N)` lookup with no start-up cost and no cross-process staleness. The
price is a schema change plus a migration, coalescing of adjacent gaps on reclaim, and splitting of
gaps in `insert_chunk_at`. It is the most invasive option and would be considered only if the
in-memory list turns out to be insufficient (for example if start-up scan time at 10 million rows is
unacceptable).

### Decisions (2026-09-30, developer feedback)

- **Candidate C (persisted free-space table) is ruled out.**
- **Candidate A is the chosen direction.** The gap list is built once when a write session starts,
  for `ingest` and for a read-write mount alike. `ingest` needs the list in most cases, so building it
  lazily would not save anything there. For a read-write mount, a short delay of a few seconds at
  startup is better UX than the same delay at the first write.
- **The covering index `(start, stop)` from Candidate B is not implemented for now (YAGNI).** It stays
  documented here as an option that can still be applied if a measurement ever shows a need.
- **Possible later optimization for a read-write mount: build the list in the background.** Settle jobs
  already run asynchronously behind the write cache, so delaying their first `reserve` until the list
  is ready would fit the existing model. The scan would need its own read-only connection. Running it
  on the shared connection would block every read behind the connection mutex. In-session reclaim
  would also have to wait for the list, or be queued until it is ready. This is a moderate amount of
  code and is not part of the first implementation.

### Proposed order of work

1. Benchmark in Rust against a synthetic `chunk_extents` table of 1, 3, 5 and 10 million rows: the
   current `reserve`, and the scan that builds the in-memory list (start-up cost of Candidate A).
2. Implement Candidate A with the commit-only gap handling described above, and update
   DESIGN-STORE-003.
3. Verify with a regression test that fails against the old behavior, as AGENTS.md's debugging
   discipline requires.

## Done (2026-09-30, cloud Claude Code session)

Benchmarked in Rust against a synthetic, gap-free `chunk_extents` table (this container, release
build). The former `reserve_and_insert_chunk` cost about 0.12 to 0.18 s per new chunk at 1 million
rows, 0.37 to 0.45 s at 3 million rows, and 1.1 to 1.5 s at 10 million rows. Building the in-memory
gap list costs about the same as one such call: 0.13 s, 0.42 s and 1.4 to 2.2 s.

Implemented Candidate A as DESIGN-STORE-006 in `docs/design/byte-store.md`
(`crates/db/src/allocation.rs`, `Repository::load_free_space`, called by `ingest` and a read-write
mount right after they acquire the write lock). Regression tests in `allocation.rs` each failed when
the behavior they protect was removed: reuse of space freed by a purge in the same session, no leak
after a failed reservation, commit-only release of freed ranges, and discarding the list after
`register_existing_chunk`. The covering index and the background build stay documented there as
options that are not implemented.
