# Implement the Scala-repository migration tool

**Why parked**: a substantial, deliberately-staged feature - this session investigated and planned
it (requirements agreed, design decided) but did not start implementation, so a later session has a
concrete starting point instead of an empty branch.
**Size**: large (confirm with the user before starting)
**Opened**: 2026-09-29, by a Windows/Claude Code Desktop session
**Context**: `docs/design/scala-migration-tool.md` (DESIGN-MIGRATION-001/002/003/004/005),
REQ-MIGRATION-001 through 005 in `requirements/functional/repository-migration.md`, branch
`migration-tool`.

## What to build

The five decisions in `docs/design/scala-migration-tool.md` describe the shape: a non-resumable
metadata-import phase (parses the old system's SQL export via a small hand-written statement
splitter, delegating the actual row-data parsing to SQL execution against a scratch schema) feeding
a resumable content-migration phase (reads each distinct old content reference once, re-chunks and
re-hashes it, writes into one metadata database per requested `--cdc-target-size-bits` value,
sharing the one existing repository's own `data/` directory rather than each expecting its own, and
tracking its own resumable progress in a small separate SQLite file per destination -
DESIGN-MIGRATION-005).

Read that design doc and the five requirements before starting - this file does not repeat their
content, only points at where to start coding.

## Concrete starting references

A previous, now-retired Rust implementation attempt (tag `rust-1st-attempt`) already built and
real-world-tested a migration tool solving the same external-format problem this one faces -
useful as a working reference for the tricky parts, not as something to copy wholesale (its output
side targets an older metadata schema, and it has no resumability or multi-target-size support -
exactly the gaps this plan exists to close). Check it out read-only via the `local-reference-worktrees`
skill (`git worktree add .local/rust-1st-attempt rust-1st-attempt`) and look at
`cli/src/migrate_scala_repo.rs`, in particular:

- Its `db::resolve_content`/`ChunkRef::New { extents, .. }` design (in that tag's `db/src/backup.rs`
  and used from `cli/src/migrate_scala_repo.rs`'s `chunk_and_store`/`resolve_chunk`): lets a caller
  supply a chunk's byte extents directly instead of the ordinary path (allocate free space, then
  write there) - exactly what "recording a migrated chunk at its existing position" in
  `docs/design/scala-migration-tool.md` needs, since migration never writes to `data/` at all
  (REQ-MIGRATION-005). This project's own `crates/db/src/content.rs` does not yet have an equivalent
  to that split - its `reserve_and_insert_chunk` always calls `allocation::reserve` itself - so this
  needs a fresh, schema-appropriate implementation of the same idea, not a copy of the old one.
- The `script_import` module's statement-boundary splitter (`iter_statements`/`strip_line_comments`)
  - quote-aware, handles the export's actual comment/escaping shape correctly (verified this session
    against a real ~550 MB export). Worth reusing the *approach* even though DESIGN-MIGRATION-002
    replaces its own hand-written value parser (`parse_insert` and everything downstream of it) with
    SQL-engine-delegated execution instead.
  - `dataId`'s three-way meaning (`NULL` = directory, `-1` = an explicit zero-length file, `>= 0` a
    real content reference) - a real gotcha in the source format, not obvious from the schema alone.
  - Its own "no resumability for v1" note (in `docs/plans/implemented/scala-rust-store-migration.md`
    on that same tag) - confirms this gap was already known, not newly discovered.

A real production-scale sample export lives at
`.local/scala-example-db/dedupfs-232_2025-04-17_17-48_backup.zip` (machine-local, not committed -
check whether it also exists on whatever machine picks this up, or ask the developer for a copy).
Its own sidecar, `.local/scala-example-db/dedupfs-232_2025-04-17_17-48_backup.md`, records confirmed
row counts and size facts - useful for sizing tests and progress estimates, and for cross-checking
a from-scratch parser against known-correct numbers. The developer has said the largest migration
actually expected is not substantially bigger than this example - useful for sizing phase 2's
resumability testing (no need to construct or acquire a dramatically larger fixture).

## Suggested order

1. **Done** (`crates/cli/src/scala_import.rs`, wired up as `dfs migrate-scala-repo` in
   `crates/cli/src/migrate_scala_repo.rs`): the statement-boundary splitter and SQL-delegated row
   import (DESIGN-MIGRATION-002), producing the durable, once-built metadata import
   DESIGN-MIGRATION-001 describes (including its completion marker). Verified against the real
   sample export above - exact row counts match. One real finding along the way, now itself part of
   DESIGN-MIGRATION-002's own text: the source system's SQL export can emit standard SQL
   `U&'...'`-style Unicode-escape string literals for a stored name with a character its export
   apparently cannot represent directly - a literal form the SQL engine used here does not support
   at all, needing one small, targeted rewrite before execution (everything else about a kept
   statement stays untouched). The synthetic test fixtures in `scala_import.rs` did not happen to
   include this case; only a manual run against the real sample export caught it (deliberately not
   kept as a committed test - see `scala_import.rs`'s own comment on why not, right where that test
   used to be). `dfs migrate-scala-repo --script <path> --staging <path>` is usable today to
   build/reuse the staging database and report its counts - phase 2 (below) is what actually
   migrates content.
1b. **Done** (`crates/db/src/lib.rs`'s `adopt_repository`/`open_repository_at`,
    DESIGN-MIGRATION-004): the `db`-API addition phase 2 needs to write a metadata database against
    an already-populated `repo_root` (tolerates non-empty, never touches `data/`) at a location
    other than the conventional `meta/` (needed once more than one target size is requested). Both
    functions are temporary by design - see DESIGN-MIGRATION-004's own removal note.
2. **Decided** (DESIGN-MIGRATION-005): phase 2's durable progress record is a small separate SQLite
   file per destination metadata database, holding `content_cache(old_data_id, content_id)` and
   `migrated(old_tree_id, new_id)`. Not yet implemented.
2b. **Done** (`crates/db/src/content.rs`'s `insert_chunk_at`, `Repository::register_existing_chunk`,
    DESIGN-MIGRATION-006): the `db`-API addition phase 2 needs to record a migrated chunk's
    `chunk_extents` at its own known, caller-supplied position instead of asking
    `allocation::reserve` to find one - the ordinary `reserve_and_insert_chunk` cannot do this.
    Temporary by design, same removal plan as 1b.
3. **Partly done**: `crates/cli/src/migrate_scala_repo.rs` now accepts one or more
   `--cdc-target-size-bits` values (`--repository` is now also a required argument) and, after the
   staging import, adopts (or reuses, with a mismatched-size check) one destination metadata
   database per value via `db::adopt_repository`/`open_repository_at` - at the conventional `meta/`
   location for exactly one value, at a distinguishable `meta-<bits>bit/` location plus a printed
   rename reminder for several (DESIGN-MIGRATION-003/004). Still missing: the actual walk-and-migrate
   content loop against `crates/store` and the new `register_existing_chunk` from 2b above - this
   step only sets up empty destination databases so far, it does not migrate any content into them
   yet.
4. The resume path that consults the progress record from step 2, using it to skip content and tree
   entries already migrated.
5. A CLI entry point wiring the above together, matching this project's existing `crates/cli`
   conventions (argument parsing, error reporting, RAM-budget handling) rather than inventing new
   ones.

## Test and documentation notes

Red/green coverage for the resume path specifically (kill mid-migration, confirm a re-run skips
already-migrated content and does not corrupt or duplicate it - AGENTS.md's own debugging discipline
for this kind of guarantee), plus ordinary coverage for the statement splitter and the
multi-target-size fan-out.

`migration/from-scala.md` currently only covers the byte-store compatibility question - it needs the
actual migration steps, prerequisites, and rollback/fallback guidance filled in once the tool exists,
matching what actually got built rather than this plan.
