# Migration feeds every `--cdc-target-size-bits` target sequentially on one thread

**Why parked**: came up while explaining the timing measurements behind the real-scale duration
estimate for the developer's own >2 TB repository (see `migration-per-call-transaction-overhead.md`
for the sibling finding) - out of scope for the migration tool's functional correctness, but
directly affects how long a several-target run takes.
**Size**: small to medium (the developer asked for this to be picked up right away, before the
transaction-batching item)
**Opened**: 2026-09-29, by a Windows/Claude Code Desktop session
**Context**: `crates/cli/src/migrate_content.rs` (`resolve_content`'s read loop, `migrate_entry`'s
per-target loop), `crates/cli/src/migration_progress.rs`, DESIGN-MIGRATION-003 in
`docs/design/scala-migration-tool.md`.

## What was found

REQ-MIGRATION-004's shared read only saves the repeated *reading* of the old bytes. Everything after
that read runs on one thread: `resolve_content` hands each read window to every pending target's
`MigrationSettler` in a plain `for` loop (target 1 fully - CDC boundaries, hashing, dedup lookup,
`register_existing_chunk` commits - then target 2, and so on), and `migrate_entry` creates the tree
entry for every target in another plain `for` loop. No `thread`/`rayon`/`async` anywhere in the
module. With five targets that is five targets' worth of hashing and SQLite commits back to back on
one core, although the targets are fully independent of each other (own `db::Repository`, own
progress record, own chunker state) and share only the read-only source bytes.

## What to look at

- Fan the per-target work out onto scoped threads: the content feed/finish per read window, and the
  per-target tree-entry insert per old entry. Results have to be collected in target order, and the
  first error reported the same way as today.
- `rusqlite::Connection` is `Send` but not `Sync`, so the progress record has to become shareable
  (e.g. a small wrapper around `Mutex<Connection>`) before a `&`-borrowing `Target` can be used from
  several threads.
- Spawning threads per old tree entry is only acceptable while each entry costs milliseconds (one
  commit per entry, as today). Once `migration-per-call-transaction-overhead.md`'s batching lands,
  per-entry work drops to microseconds and per-entry thread spawns would become the new overhead -
  the design should then move to persistent per-target worker threads fed through channels.
- Expected gain: only the CPU/commit part parallelizes (not the shared read), so the ceiling is
  roughly "slowest single target" instead of "sum of all targets". Re-time with the same three
  measurements as the sibling item (one target, a near-zero-chunk-count target, five targets
  together) on the real test repository to confirm the actual speedup.

## Done (2026-09-29, Windows/Claude Code Desktop session)

Implemented in `crates/cli/src/migrate_content.rs`: `parallel_map`/`parallel_map_mut` run the
per-target work on one scoped thread per target (inline for a single target), used for the per-window
`MigrationSettler::feed` fan-out, the final `finish` + progress record per target, and the per-entry
tree insert. `crate::migration_progress` became a `ProgressRecord` type with its connection behind a
`Mutex`, since `rusqlite::Connection` is `Send` but not `Sync`.

Measured with the same method as before (release build, fresh copy of the real Scala test
repository, same machine): one target at 18 bits 21.0 s -> 20.0 s, five targets (16-20 bits)
94.3 s -> 57.2 s (about 1.65x). The destination databases came out byte-identical in physical size
to the sequential run. The speedup is far below the ideal (slowest single target, roughly 25-30 s)
because every commit still ends in a disk flush, and five concurrent flushes to one disk overlap only
partly - the per-commit cost itself is what `migration-per-call-transaction-overhead.md` is about,
and batching is now the bigger remaining lever.

New test `migrate_reassembles_a_multi_part_multi_window_file_identically_in_every_target` covers what
no test did before: a ~9.5 MB file in two non-contiguous parts (more than two read windows, real
chunk boundaries including one straddling the part boundary) migrated into three targets at once,
read back byte for byte through each destination's own extents. Verified red by shifting
`map_to_old_store_extents` by one byte.
