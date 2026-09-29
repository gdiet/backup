# Implement the Scala-repository migration tool

**Why parked**: a substantial, deliberately-staged feature. All seven design decisions are now
implemented end to end (staging import, destination adoption, the walk-and-migrate content loop,
resumability) and verified both by synthetic-fixture tests and by an actual run against a real
Scala repository ("Real-data validation" below) - what is left (see "Remaining gaps" below) is
smaller CLI/documentation follow-up work, not core functionality.
**Size**: small (confirm with the user before starting)
**Opened**: 2026-09-29, by a Windows/Claude Code Desktop session
**Context**: `docs/design/scala-migration-tool.md` (DESIGN-MIGRATION-001 through 007),
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
2. **Done** (`crates/cli/src/migration_progress.rs`, DESIGN-MIGRATION-005): phase 2's durable
   progress record - a small separate SQLite file per destination metadata database, holding
   `content_cache(old_data_id, content_id)` and `migrated(old_tree_id, new_id)`.
2b. **Done** (`crates/db/src/content.rs`'s `insert_chunk_at`, `Repository::register_existing_chunk`,
    DESIGN-MIGRATION-006): the `db`-API addition phase 2 needs to record a migrated chunk's
    `chunk_extents` at its own known, caller-supplied position instead of asking
    `allocation::reserve` to find one - the ordinary `reserve_and_insert_chunk` cannot do this.
    Temporary by design, same removal plan as 1b.
2c. **Done** (`crates/db/src/tree.rs`'s `insert_migrated_entry`, `Repository::insert_migrated_entry`,
    DESIGN-MIGRATION-007): recreates one tree entry - live or already-deleted-from-birth - without
    `mkdir`/`settle_file`'s own liveness bookkeeping or parent-touching, since a migrated entry's
    `time`/`deleted_at` are themselves already-fixed migrated values. Temporary, same removal plan.
3. **Done** (`crates/cli/src/migrate_content.rs`, wired into `crates/cli/src/migrate_scala_repo.rs`):
   the walk-and-migrate content loop. Walks the staging tree root-first, recreating every entry via
   2c above; for each distinct old `dataId`, reads the old bytes at most once and feeds them to every
   still-pending target's own chunker in lockstep (DESIGN-MIGRATION-003/REQ-MIGRATION-004), recording
   new chunks via 2b above (never writing bytes - REQ-MIGRATION-005) and consulting/updating the
   progress record from step 2 throughout, so a resumed run only redoes whatever an interruption left
   unfinished. `db::adopt_repository`/`open_repository_at`'s own naming-and-hint behavior
   (DESIGN-MIGRATION-004) is unchanged from the previous increment. Once every destination has been
   fully migrated in one call, the staging import and every destination's own progress record are
   removed (DESIGN-MIGRATION-001) - best-effort, a cleanup failure does not undo an otherwise-
   successful migration. Covered by `crates/cli/src/migrate_content.rs`'s own tests (a full
   tree/directory/soft-delete/empty-file/whole-file-dedup scenario, a resumability check whose own
   regression-catching power was verified red/green per AGENTS.md's debugging discipline, and a
   two-target fan-out check) plus `crates/cli/src/migrate_scala_repo.rs`'s own CLI-level tests
   (including a failed-phase-2-leaves-staging-in-place-for-reuse scenario).

## Remaining gaps

Not blocking, but real gaps a later session should know about:

- **Decided against (YAGNI), not just deferred**: no `--ram-budget-mb` handling (unlike most other
  `crates/cli` commands). Checked concretely rather than left as a guess:
  `cdc::ChunkerConfig::max_chunk_size` bounds one in-progress chunk buffer to
  `base_size * (bits + 1)` regardless of file size (e.g. ~544 KB at 16 bits, ~10.5 MB at 20 bits) -
  the only structure that actually scales with input is one target's `chunk_ids: Vec<i64>` for the
  single old `dataId` currently being processed, bounded by that one file's own size divided by its
  average chunk size (e.g. a hypothetical 1 TB single file at 16 bits across three simultaneous
  target sizes tops out around 150 MB). Only a pathological choice (very small `bits`, or a single
  enormous file) would make this worth adding - not worth building for a case this unlikely.
- No `--verify`/`--best-effort` style flags for a partially-missing old `data/` (unlike
  `crate::restore`'s own two independent opt-ins) - a single incomplete read currently fails the
  whole run outright (`MigrateContentError::IncompleteOldData`), which is the safer default but not
  the only one a real operator might eventually want.
- `migration/from-scala.md` currently only covers the byte-store compatibility question - it needs
  the actual migration steps, prerequisites, and rollback/fallback guidance filled in, matching what
  actually got built rather than this plan.

## Real-data validation (done, 2026-09-29)

Ran `dfs migrate-scala-repo` against the hand-built real Scala test repository
(`.local/scala-test-repo/`, its final `db-backup` export) from a throwaway copy (never against the
fixture itself) - not just synthetic unit-test fixtures:

- All 2178 tree entries / 733 data entries imported correctly; migrating `--cdc-target-size-bits 16
  18 20` in one call read the source once and produced three independent metadata databases in
  ~57s, exactly as DESIGN-MIGRATION-003 intends.
- Full tree/history fidelity confirmed by hand via `dfs list --show-deleted`: both real soft-deletes
  from the fixture (`/build/[deleted]/release-v1-copy`, `/personal/downloads/[deleted]/{24.03.2026
  Steuerbescheinigung 2025 (1).pdf, Schulferien 2027 Bayern.pdf}`) came through correctly.
- `dfs stats` across the three target sizes on the same shared `data/`: physical size 165.57 MB (16
  bits) / 181.65 MB (18 bits) / 206.57 MB (20 bits), logical size identical (286,953,278 bytes) at
  all three, as expected - finer chunking finds more sub-file duplication. Notably, even 18-bit CDC
  dedup alone beats the source's own whole-file dedup (Scala's own reported physical size was 251.88
  MB) - CDC finds cross-file matches whole-file hashing structurally cannot. The metadata database
  itself moves the opposite way, as expected (more, smaller chunks means more `chunks`/
  `chunk_extents` rows): 798,720 bytes (16 bits) / 544,768 bytes (18 bits) / 487,424 bytes (20 bits) -
  a few hundred KB of metadata difference against tens of MB of physical-storage difference, clearly
  the right trade for this dataset.
- `dfs reclaim` on a separately-migrated single-target-size copy freed exactly 65,175 bytes - the
  size of the one genuinely-unique soft-deleted file (`Schulferien 2027 Bayern.pdf`); the other two
  soft-deleted items (a whole duplicate directory and one of three identical tax-document copies)
  freed nothing, since their content is still referenced by other live entries - exactly matching the
  fixture's own documented expectations.
- `dfs stats`'s reported physical size was unchanged before vs. after that reclaim
  (181,646,574 bytes both times) - direct, empirical confirmation that REQ-STORAGE-005 (on-demand
  defragmentation/store shrinking) is not implemented: `dfs reclaim` only marks freed byte ranges as
  reusable internally (REQ-STORAGE-004), it never relocates existing bytes or returns space to the
  filesystem, and no other command does either (`dfs db-compact` only VACUUMs the SQLite metadata
  file, per its own name).
- Found and fixed a real doc/UX bug along the way: `DESIGN-MIGRATION-004`'s own text, and the CLI's
  own printed rename hint, claimed a non-conventional metadata database could be "passed directly"
  to another `dfs` command as an alternative to renaming it - false, no `dfs` command accepts
  anything but `--repository <path>` resolved to `<path>/meta`. Both corrected.
