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
