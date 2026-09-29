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
