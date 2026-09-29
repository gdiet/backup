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
