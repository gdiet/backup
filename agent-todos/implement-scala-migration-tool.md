# Implement the Scala-repository migration tool

**Why parked**: a substantial, deliberately-staged feature - this session investigated and planned
it (requirements agreed, design decided) but did not start implementation, so a later session has a
concrete starting point instead of an empty branch.
**Size**: large (confirm with the user before starting)
**Opened**: 2026-09-29, by a Windows/Claude Code Desktop session
**Context**: `docs/design/scala-migration-tool.md` (DESIGN-MIGRATION-001/002/003), REQ-MIGRATION-001
through 004 in `requirements/functional/repository-migration.md`, branch `migration-tool`.

## What to build

The three decisions in `docs/design/scala-migration-tool.md` describe the shape: a non-resumable
metadata-import phase (parses the old system's SQL export via a small hand-written statement
splitter, delegating the actual row-data parsing to SQL execution against a scratch schema) feeding
a resumable content-migration phase (reads each distinct old content reference once, re-chunks and
re-hashes it, writes into one destination repository per requested `--cdc-target-size-bits` value).
That file's own "Open question" section flags the one thing still undecided: the exact shape of
phase 2's durable progress record.

Read that design doc and the four requirements before starting - this file does not repeat their
content, only points at where to start coding.

## Concrete starting references

A previous, now-retired Rust implementation attempt (tag `rust-1st-attempt`) already built and
real-world-tested a migration tool solving the same external-format problem this one faces -
useful as a working reference for the tricky parts, not as something to copy wholesale (its output
side targets an older metadata schema, and it has no resumability or multi-target-size support -
exactly the gaps this plan exists to close). Check it out read-only via the `local-reference-worktrees`
skill (`git worktree add .local/rust-1st-attempt rust-1st-attempt`) and look at
`cli/src/migrate_scala_repo.rs`, in particular:

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

1. The statement-boundary splitter and SQL-delegated row import (DESIGN-MIGRATION-002), producing
   the ephemeral working structure DESIGN-MIGRATION-001 describes. Verify against the real sample
   export above before moving on - its confirmed row counts make this a cheap, precise correctness
   check.
2. Decide phase 2's durable progress record shape (the open question in the design doc).
3. Phase 2's walk-and-migrate logic against the current `db::Repository`/`crates/store` API,
   parameterized over one or more `--cdc-target-size-bits` values at once (DESIGN-MIGRATION-003).
4. The resume path that consults the progress record from step 2.
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
