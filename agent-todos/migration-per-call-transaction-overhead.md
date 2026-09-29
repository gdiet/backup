# Migration's per-entry/per-chunk transaction commits dominate its own runtime

**Why parked**: came up while estimating real-scale migration duration for the developer's own
>2 TB repository - out of scope for the migration tool's own functional correctness (already done
and verified), but directly affects whether that tool is *practical* to actually run at that scale.
**Size**: medium (confirm with the user before starting - needs a real design decision on batch size/
boundaries, not just a mechanical change)
**Opened**: 2026-09-29, by a Windows/Claude Code Desktop session
**Context**: `crates/cli/src/migrate_content.rs` (`migrate_entry`'s calls to
`Repository::insert_migrated_entry` and `Repository::register_existing_chunk`, and
`crate::migration_progress`'s `record_migrated`/`record_content`); `crates/db/src/lib.rs`'s
`with_transaction` (commits once per call, by design, for `db`'s single-writer model's ordinary
mutating operations).

## What was found

Timed three real migrations of the hand-built Scala test repository (`.local/scala-test-repo/`,
251.88 MB content, 2177 tree entries) to decompose where migration time actually goes:

- One target, 18 bits: 21.04s.
- One target, 23 bits (near-zero chunk count - ~25 chunks vs ~785 at 18 bits, isolating
  tree-entry-creation cost from chunk-related cost): 18.91s.
- Five targets (16/17/18/19/20 bits) in one call, sharing one read of the source: 94.27s.

Decomposition: reading the 251.88 MB content once costs ~2.73s (~92 MB/s) - a small fraction of
total time. The dominant cost is tree-entry creation: ~16.1s for 2177 entries, ~7.4ms *per entry* -
because `insert_migrated_entry` and the progress record's own `record_migrated` each commit their
own transaction per call (`Repository::with_transaction`'s ordinary, correct behavior for a live
mount/CLI write path, but not designed with a several-million-row bulk load in mind). Chunk-related
work (`register_existing_chunk` + `record_content`) showed a similar per-call cost, ~2.7ms per chunk.

Scaled to the developer's own real repository (2.23 TB distinct content, 6,763,253 tree entries -
ratios ~8874x content, ~3107x entries, from `.local/scala-example-db/`'s own sidecar): migrating
`--cdc-target-size-bits 16 17 18 19 20` in one call is estimated at roughly **5 days**, with tree-
entry creation alone accounting for ~70 of those ~125 hours *regardless of which bits values are
requested* - REQ-MIGRATION-004's one-shared-read design only saves the I/O portion (~27 hours out of
~125), which turned out to be a much smaller share of the total than expected going in.

## What to look at

Batching multiple `tree_entries`/`chunk_extents`/progress-record rows into fewer, larger
transactions is very likely the single biggest lever to make a real multi-terabyte, multi-million-
entry migration take hours rather than days - the per-call transaction/fsync overhead, not I/O or
hashing, is what actually dominates at that scale. Needs a real design decision, not just a
mechanical change:

- Where the batch boundary goes (e.g. commit every N entries/chunks, or per directory subtree, or
  per some memory/row-count budget) and how that interacts with resumability (DESIGN-MIGRATION-005's
  progress record needs to stay consistent with what is actually committed in the destination - a
  crash mid-batch must not leave the progress record claiming more was migrated than actually landed
  durably).
- Whether this only matters for the migration tool's own bulk-insert path (temporary, removed with
  the tool per DESIGN-MIGRATION-004/006/007's own removal plans) or whether the same batching
  question is worth a more general look at `db`'s own write path - out of scope to decide here,
  flagged only so it is not missed.
- Re-time the same three-measurement approach (single target, a near-zero-chunk-count target, several
  targets together) after any change, against the same real test repository, to confirm the actual
  speedup before declaring this fixed.

## Update after the per-target threading item (2026-09-29)

Running the per-target work on one thread per target (`done/migration-parallel-targets.md`) brought
the five-target run from 94.3 s to 57.2 s, but only about 1.65x - not the ~4x a CPU-bound workload
would give. That is a further sign that the time goes into waiting for per-commit disk flushes,
which concurrent threads overlap only partly. Fewer, larger transactions attack that directly. They
would also make the per-entry thread spawns in `migrate_content::parallel_map` the dominant cost, so
the design should then move to persistent per-target worker threads fed through channels.
