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

## Correction and a crash-safety bug found while analysing this item (2026-09-29)

**The measured cost is the progress file's own commits, not the destination's.** The destination
databases run in WAL mode with `synchronous=NORMAL` (`crates/db/src/connection.rs`), where a commit
does not flush to disk. `crate::migration_progress` opened its file with SQLite's defaults instead
(rollback journal, `synchronous=FULL`), so every `record_migrated`/`record_content` created a journal
file, flushed it and deleted it. Timing the same runs with the progress file switched to WAL and
`synchronous=NORMAL` (experiment only, reverted): one target at 18 bits 20.0 s -> 3.3 s, five targets
(16-20 bits) 57.2 s -> 9.3 s - most of what is left is the shared read. The earlier estimate of about
5 days for the developer's real repository is therefore far too pessimistic for the current code.

**Resuming after a hard kill can fail.** Destination insert and progress record are two independent
commits in two files. Killing the process between them leaves an entry in the destination that the
progress record does not know about, and the resume then tries to insert it again. Reproduced on the
real test repository (one target, 18 bits): of four runs killed at 3/6/9/12 s, the one killed at 3 s
could not be resumed - `error: UNIQUE constraint failed: tree_entries.parent_id, tree_entries.name`,
every time, until someone cleans up by hand (violating REQ-MIGRATION-003). For a soft-deleted entry
the same window would not fail but silently insert a duplicate history row. The resume test that
exists only re-runs a *completed* migration, which never exercises this. A power loss adds a second
direction: with `synchronous=NORMAL` the destination can lose its last commits while the progress
file (`FULL`) still claims them.

## Done (2026-09-29, Windows/Claude Code Desktop session)

Fixed both findings together with one design change (DESIGN-MIGRATION-005, now rewritten): the
progress record moved out of its separate file into two tables inside each destination database,
written in the same batch transaction as the entries they describe and dropped when the migration
has finished. `db` gained temporary `Repository::migration_*` methods (`crates/db/src/migration.rs`):
an open batch transaction across many calls (every mutating call runs in a savepoint inside it), the
progress reads and writes, and a completion check that recognizes an already fully migrated
destination. `crates/cli/src/migrate_content.rs` commits every 5000 operations and only at points
where each entry has its record in the same batch; on failure every target rolls back to its last
commit. The separate `crate::migration_progress` module is gone.

Measured with the same method (release build, fresh copy of the real Scala test repository): one
target at 18 bits 20.0 s -> 1.8 s, five targets (16-20 bits) 57.2 s -> 4.6 s (94.3 s at the very
start of this series); physical sizes of all destinations identical to the earlier runs. The old
per-window thread fan-out stays.

Crash safety, verified two ways. `a_failed_migration_leaves_entries_and_progress_records_consistent_and_resumes_cleanly`
fails a migration partway at batch sizes 1/2/3/5, checks that every entry and its record agree and
that the resume creates exactly what was missing; verified red by moving the commit point between
the entry and its record (the old bug's shape). On the real test repository the process was killed
at ten points in time between 0.3 s and 3.0 s and resumed each time: all ten ended byte-identical in
size to the uninterrupted run, with the soft-deleted entries intact - where the separate-file design
had failed the resume in one of four earlier attempts.

Real-scale estimate, rough: the test run is now dominated by work that scales with the data, and the
source read no longer hides in the page cache at 2.23 TB, so it depends mostly on how fast the source
disk reads. Scaling the five-target compute time (4.6 s for 252 MB) by the content ratio (~8870)
gives roughly 11 hours, plus reading 2.23 TB at the disk's real speed (about 1.2 h at 500 MB/s, 6 h at
100 MB/s) - so on the order of half a day instead of the earlier 5-day estimate.
