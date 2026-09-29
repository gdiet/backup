# Migrating An Existing Scala Repository

How the actual migration tool (REQ-MIGRATION-001/002/003/004/005 in
[`../../requirements/functional/repository-migration.md`](../../requirements/functional/repository-migration.md),
concrete path in [`../../migration/from-scala.md`](../../migration/from-scala.md)) reads the old
system's own SQL export and turns it into one or more new metadata databases against the existing
repository's own, unchanged byte store, as `dfs migrate-scala-repo` (`crates/cli/src/
migrate_scala_repo.rs`).

There is exactly one repository throughout: the existing one, adopted in place. Its `data/`
directory (REQ-MIGRATION-002) is read from, never duplicated, never written to, and never moved
(REQ-MIGRATION-005) - migrating into more than one `--cdc-target-size-bits` value
(DESIGN-MIGRATION-003) means more than one metadata database ends up next to that same, single
`data/` directory, not more than one repository each with its own copy.

## DESIGN-MIGRATION-001: Two phases - a durable, built-once metadata import, then a resumable content migration
Status: implemented (`crates/cli/src/scala_import.rs`, `crates/cli/src/migrate_content.rs`)

The old system's own SQL export describes the whole tree's metadata, but not the byte content
itself - parsing that export is orders of magnitude cheaper than reading the multi-terabyte content
it describes. Confirmed directly against a real production export (~550 MB, 6.7 million tree
entries): parsing it into a queryable working structure took on the order of a minute, while the
distinct content it references was on the order of two terabytes - the same shape REQ-MIGRATION-002
already assumes when it says migration only ever reads stored bytes as needed, never copies them
wholesale. Splitting migration into two phases follows directly from that size gap:

1. **Metadata import** (the export's tree/content bookkeeping, loaded into a working structure this
   migration queries against): built once per migration attempt, as an ordinary on-disk database
   next to the migration's own destination metadata database(s) rather than an in-memory one - the
   disk space it costs is trivial next to a multi-terabyte migration, and unlike an in-memory
   structure it does not compete with phase 2's own chunk-buffer memory for RAM. Marked complete
   only once its own import
   transaction has fully committed (see "Detecting a reusable import" below), so a run interrupted
   partway through this phase never leaves behind something that merely looks complete. On any run -
   the first attempt or a retry after an interruption anywhere in phase 1 or phase 2 - an existing,
   completed import is reused as-is; a missing or incomplete one is (re)built from scratch, safe to
   do because it depends only on the unchanging source export.
2. **Content migration** (walk the imported tree, read each distinct old content reference's bytes
   exactly once, re-chunk and re-hash it, write the result into the target repository): the
   genuinely expensive, potentially many-hours phase for a multi-terabyte source, and the one
   REQ-MIGRATION-003 resumability actually needs to cover. Tracks its own progress durably,
   separately from phase 1's own metadata import - which old content references have already been
   re-chunked, and under which new identifier - so a resumed run skips content it already migrated
   instead of re-reading and re-hashing it.

Both the metadata import and phase 2's own progress record are removed only once the entire
migration - every target size requested (DESIGN-MIGRATION-003) - has completed successfully; an
interrupted attempt leaves both in place, specifically so a resume has nothing left to redo beyond
wherever it actually stopped.

### Detecting a reusable import

An import that merely exists on disk is not enough to trust - a run killed partway through it would
leave a file that exists but reflects an incomplete parse, and starting phase 2 from that would
silently work from a truncated tree. The import writes a single completion marker (e.g. one row in
a dedicated table) as the last statement of its own import transaction; a resumed run trusts an
existing import only once that marker is present, and otherwise treats it exactly as it would treat
a missing one - rebuilding it from scratch.

### Rejected: an ephemeral, rebuild-every-run metadata import

Considered first: keeping the metadata import's own result only in memory (or a scratch file
discarded on exit), rebuilt in full on every invocation including a resume. Simpler in one respect -
no completion marker, nothing to clean up - but rejected once weighed against its actual cost: it
would repeat a multi-minute parse on every resume rather than once total across however many resumes
a long migration needs, and an in-memory structure specifically would compete for RAM against phase
2's own chunk-buffer budget for no benefit. Persisting the result avoids both, at the cost of one
small piece of bookkeeping (the completion marker above) that a durable design needs anyway.

### Rejected: making the metadata import itself resumable too

Checkpointing partway through the metadata import so it, too, could resume mid-parse from an
interruption was considered and rejected. The cost it would guard against - redoing a sub-few-minute
parse - is already small next to a multi-hour content migration, and the import has no meaningful
partial-progress concept to preserve in the first place: it either produces one complete,
self-consistent working structure from the whole export, or nothing usable at all (see "Detecting a
reusable import" above). Treating both phases as one combined resumability story would force every
step of the metadata import to be individually safe to replay in isolation, a real constraint on how
it can work (see DESIGN-MIGRATION-002) for a cost that was never the actual bottleneck.

## DESIGN-MIGRATION-002: Delegate the export's row-data parsing to SQL execution; hand-parse only its statement boundaries
Status: implemented (crates/cli/src/scala_import.rs)

The old system's SQL export (an `fsc db-backup`-produced script, see `migration/from-scala.md`)
mixes schema-definition statements - in that source system's own SQL dialect, not portable as-is -
with the actual row data. Confirmed directly against a real production export: row data is always
written as `INSERT INTO <table> VALUES (...), (...), ...` - one `INSERT` per table, covering every
row in that table as a single multi-tuple statement, never one `INSERT` per row - and nothing this
migration needs lives in any other statement type.

Given that shape, only a small amount of hand-written parsing is actually needed: splitting the
export's text into individual top-level statements - respecting the source SQL's own quoted-string
escaping, so a value containing a semicolon, a comment marker, or a quote character is never
mistaken for a statement boundary - and classifying each one as "row data for a table this
migration cares about" or "everything else, discard." The value parsing inside a kept `INSERT`
statement itself - its column list, its multi-row `VALUES` tuples, string/number/`NULL`/binary-literal
syntax - is handed to this migration's own SQL engine unmodified, executed as-is against a small
schema defined for this purpose with matching table/column names, rather than hand-parsed a second
time. A hand-written value parser duplicating that part of SQL's own grammar was considered and
rejected: an SQL engine's own parser already handles it correctly, with no additional surface for
this migration to get wrong.

One narrow exception, found against the same real production export: the source system's own SQL
dialect can emit a standard SQL Unicode-escape string literal (`U&'...'`, with `\XXXX`/`\+XXXXXX`
codepoint escapes) for a stored name containing a character its export apparently cannot represent
directly - a literal form this migration's own SQL engine does not support at all. Recognizing and
rewriting only that one literal form into an equivalent plain string literal, leaving every other
part of a kept statement's value syntax completely untouched, was accepted as the one deliberate,
narrow departure from "unmodified" this decision needs - the alternative (dropping SQL-engine
delegation entirely over one literal form) would have discarded this decision's whole point to
avoid a single, well-contained exception.

## DESIGN-MIGRATION-003: One read of the source, several `--cdc-target-size-bits` values, one metadata database per value
Status: implemented (`crates/cli/src/migrate_content.rs`'s `resolve_content`)

REQ-MIGRATION-004: migrating the same source content at more than one candidate target chunk size
does not need to read that content once per value compared. Reading a given piece of old content
once and feeding it to as many independently configured chunkers as target sizes were requested -
each producing its own chunk boundaries, hashes, and destination metadata database - reads the
source exactly as many times as REQ-MIGRATION-002 already implies for a single target size,
regardless of how many values are actually being compared.

This composes with DESIGN-MIGRATION-001's phase split unchanged: the metadata import has no
target-size dependence at all (chunk boundaries are entirely a phase-2 concept), so it runs exactly
once regardless of how many target sizes phase 2 then migrates into. Phase 2 creates one destination
metadata database per requested value (DESIGN-MIGRATION-004) and, for each distinct old content
reference it reads, updates every open destination's own progress tracking independently - one
value's migration falling behind, or needing to resume, never blocks or restarts the others.

## DESIGN-MIGRATION-004: Several metadata databases against one shared `data/`; the tool and its one `db`-API addition are temporary
Status: implemented (crates/db/src/lib.rs's `adopt_repository`/`open_repository_at`)

Adopting an existing repository in place (REQ-MIGRATION-002) means its `data/` directory is never
duplicated - so migrating into several `--cdc-target-size-bits` values at once (DESIGN-MIGRATION-003)
needs several metadata databases sitting next to that one, shared `data/`, not several repositories
each expecting to own a `data/` of their own. `db::init_repository`/`open_repository` do not support
this at all: both tie the metadata database and `data/` to the same `repo_root`, and `init_repository`
additionally refuses a non-empty `repo_root` - exactly what an already-populated Scala repository
root always is.

Two temporary functions add just enough to cover this: `db::adopt_repository(repo_root, meta_dir,
settings)` (like `init_repository`, but tolerates a non-empty `repo_root`, never creates or modifies
`data/` - it must already exist, an error otherwise, never silently created empty - and writes its
fresh metadata database at the given `meta_dir` rather than always `repo_root`'s own conventional
location) and `db::open_repository_at(meta_dir)` (the equivalent open, without `open_repository`'s
own repo_root-relative convention). Naming the result once created is left to the caller: migrating
into exactly one target size writes it at `repo_root`'s own conventional `meta/` location, so it is
immediately usable by every other command with no extra step; migrating into more than one writes
each at its own distinguishable, non-conventional location (e.g. `meta-18bit/`) instead, since only
one of them could ever occupy the conventional name - the tool prints an explicit reminder that
picking one and renaming it to `meta/` is the operator's own next step before any other command can
use it. No ordinary `dfs` command accepts a non-conventional metadata-database location directly
(every one of them always resolves `--repository <path>` to `<path>/meta`) - renaming is the only
way to make a chosen result usable.

Both functions, and the migration tool itself, are intended to be removed again - not a permanent
extension of `db`'s own repository-layout conventions, only a stopgap for the small number of
releases expected to actually need Scala-repository migration support (the developer's own plan: the
first one or two production releases that include it at all; an operator migrating after that grabs
an older release, migrates there, then upgrades normally). Both functions' own doc comments say so
directly, pointing back at this entry, so removing them later does not need rediscovering why they
exist first.

## DESIGN-MIGRATION-006: Recording a migrated chunk at its existing position
Status: implemented (`crates/db/src/content.rs`'s `insert_chunk_at`,
`Repository::register_existing_chunk`)

REQ-MIGRATION-005 means phase 2 never writes bytes anywhere - a chunk's content already sits at a
known position within `data/` (translated from the old system's own record of where it stored that
content). What phase 2 needs from `db` is therefore not "allocate space and write metadata for a
new chunk" (`crates/db/src/content.rs`'s existing `reserve_and_insert_chunk`, which always calls
`allocation::reserve` to find free space) but "record metadata for a chunk whose bytes already
exist at this caller-supplied position" - a distinct operation `db` did not expose before this
decision.

`Repository::register_existing_chunk(length, hash, extents)` fills that gap: like
`reserve_and_insert_chunk`, but takes the chunk's `chunk_extents` ranges directly from the caller
instead of asking `allocation::reserve` to find them. Temporary, the same as
`adopt_repository`/`open_repository_at` (DESIGN-MIGRATION-004) - only the Scala-repository
migration tool needs it, removed together with the tool itself.

This composes safely with the ordinary allocator with no extra bookkeeping: `allocation::reserve`
(`crates/db/src/allocation.rs`) derives free space entirely by scanning `chunk_extents` on every
call rather than tracking a separate high-water mark or free-list, so once migration's chunks are
recorded there, an ordinary future write through the adopted repository automatically treats those
ranges as occupied - no separate step to reconcile the allocator's own state against what migration
already claimed.

## DESIGN-MIGRATION-005: A durable progress record, one file per destination metadata database
Status: implemented (`crates/cli/src/migration_progress.rs`)

Phase 2 (DESIGN-MIGRATION-001) needs to skip already-migrated content and tree structure on a
resume rather than redo it - REQ-MIGRATION-003 exists specifically so an interruption near the end
of a multi-hour migration does not cost re-reading everything from the start. That needs two
distinct memoized facts, neither of which the destination metadata database's own schema can answer
on its own:

- **Content memoization**: which old `dataId`(s) have already been read, re-chunked, and resolved
  into which new `content_id` - `contents`/`chunks`/`chunk_extents` are content-addressed, not
  origin-addressed, so nothing there records which old `dataId` produced a given `content_id`.
- **Tree progress**: which old tree entries have already been recreated, and under which new id - a
  directory's own new id in particular, needed as the `parent_id` for its not-yet-migrated children.
  Inferring this from the destination's own tree by name/parent/timestamp was considered and
  rejected: REQ-MIGRATION-001's full soft-delete history means more than one old entry can share the
  same name at the same location (one live, others historically deleted), which makes name-based
  matching ambiguous.

The progress record is therefore a small, separate SQLite file - one per destination metadata
database, sitting alongside it, not merged into the destination's own schema (`db::Repository`'s
schema stays exactly what an ordinary repository has, with nothing migration-specific mixed into
it) - holding two tables: `content_cache(old_data_id, content_id)` and `migrated(old_tree_id,
new_id)`. Removed along with the destination's own staging import once that target size's migration
completes successfully (DESIGN-MIGRATION-001); left in place after an interrupted run, so a resume
has exactly the remaining, not-yet-migrated content and tree structure left to do.

### Rejected: no content memoization, a resume just re-migrates everything

Skipping this file entirely and letting an interrupted phase 2 simply restart from the beginning was
considered. It is safe - re-chunking and re-hashing already-migrated content is idempotent, and the
ordinary chunk/content dedup lookups prevent it from ever being written twice - and it satisfies
REQ-MIGRATION-003's literal wording ("re-run from scratch without manual cleanup"). It was rejected
because it defeats that requirement's own practical purpose for a migration large enough to need
resumability in the first place: an interruption shortly before completion would cost re-reading and
re-hashing a multi-terabyte source's entire content again - the exact repeated-read cost
REQ-MIGRATION-002/004 already go out of their way to avoid elsewhere in this same tool.

## DESIGN-MIGRATION-007: Recreating a migrated tree entry
Status: implemented (`crates/db/src/tree.rs`'s `insert_migrated_entry`,
`Repository::insert_migrated_entry`)

Phase 2 recreates the source's entire tree, live and soft-deleted entries alike
(REQ-MIGRATION-001), walking it root-first (a parent is always migrated before its children, since
a new child's `parent_id` must already exist). `db`'s existing entry-creating operations do not fit
this: `mkdir`/`settle_file` each assume a *live-now* operation - they check for a colliding live
child, soft-delete it if replacing a file, and bump the parent's own modification time as a side
effect of "something changed just now" - none of which applies here. A migrated entry's `deleted_at`
and `time` are themselves migrated values, already fixed by the source data; inserting a child must
never overwrite its already-migrated parent's own `time` the way an ordinary structural change
would.

`Repository::insert_migrated_entry(parent_id, name, time_millis, deleted_at, content_id)` is a
plain insert of exactly those columns, skipping every check above. This is safe specifically
because of how phase 2 uses it, not in general: root-first ordering guarantees every parent already
exists, and REQ-MIGRATION-001 means the source data's own invariants (at most one live entry per
name) are trusted rather than re-derived. A genuine violation still fails loudly against
`tree_entries_active_name_idx`, the same live-only uniqueness constraint `mkdir`/`settle_file`
themselves rely on - this function does not weaken it, only skips checking it pre-emptively.

Temporary, the same as `adopt_repository`/`open_repository_at` (DESIGN-MIGRATION-004) and
`register_existing_chunk` (DESIGN-MIGRATION-006) - only the Scala-repository migration tool needs
it, removed together with the tool itself.
