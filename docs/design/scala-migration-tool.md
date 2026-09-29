# Migrating An Existing Scala Repository

How the actual migration tool (REQ-MIGRATION-001/002/003/004 in
[`../../requirements/functional/repository-migration.md`](../../requirements/functional/repository-migration.md),
concrete path in [`../../migration/from-scala.md`](../../migration/from-scala.md)) reads the old
system's own SQL export and turns it into one or more new repositories. Not yet implemented -
written up as a starting point before coding begins, so the decisions already made do not need
re-deriving in a later session.

## DESIGN-MIGRATION-001: Two phases - a cheap, redo-from-scratch metadata import, then a resumable content migration
Status: decided

The old system's own SQL export describes the whole tree's metadata, but not the byte content
itself - re-parsing that export is orders of magnitude cheaper than re-reading the multi-terabyte
content it describes. Confirmed directly against a real production export (~550 MB, 6.7 million
tree entries): parsing it into a queryable working structure took on the order of a minute, while
the distinct content it references was on the order of two terabytes - the same shape REQ-MIGRATION-002
already assumes when it says migration only ever reads stored bytes as needed, never copies them
wholesale. Splitting migration into two phases follows directly from that size gap:

1. **Metadata import** (the export's tree/content bookkeeping, loaded into a working structure this
   migration queries against): never resumable, and does not need to be. On any run - the first
   attempt or a retry after an interruption anywhere in phase 2 - this phase runs again from
   scratch against the same, unmodified source export and produces the identical result every time.
   An ephemeral working structure, discarded unconditionally when the process exits, is exactly the
   right shape for something rebuilt in full on every invocation regardless; giving it any
   durability of its own would only be a second, redundant copy of information the source export
   already holds durably.
2. **Content migration** (walk the imported tree, read each distinct old content reference's bytes
   exactly once, re-chunk and re-hash it, write the result into the target repository): the
   genuinely expensive, potentially many-hours phase for a multi-terabyte source, and the one
   REQ-MIGRATION-003 resumability actually needs to cover. Tracks its own progress durably,
   separately from phase 1's ephemeral structure - which old content references have already been
   re-chunked, and under which new identifier - so a resumed run skips content it already migrated
   instead of re-reading and re-hashing it, while phase 1 keeps being redone unconditionally on
   every attempt regardless of how far phase 2 previously got.

### Rejected: making the metadata import itself resumable too

Checkpointing partway through the metadata import so it, too, could resume from an interruption was
considered and rejected. The cost it would guard against - redoing a sub-few-minute parse - is
already small next to a multi-hour content migration, and the import has no meaningful partial-progress
concept to preserve in the first place: it either produces one complete, self-consistent working
structure from the whole export, or nothing usable at all. Treating both phases as one combined
resumability story would force every step of the metadata import to be individually safe to replay
in isolation, a real constraint on how it can work (see DESIGN-MIGRATION-002) for a cost that was
never the actual bottleneck.

## DESIGN-MIGRATION-002: Delegate the export's row-data parsing to SQL execution; hand-parse only its statement boundaries
Status: decided

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

### The working structure never needs to survive past one process run

Following directly from DESIGN-MIGRATION-001: since the metadata import is redone in full on every
invocation regardless, the schema it builds for itself can be purely in-memory or a scratch file,
discarded unconditionally when the process exits. Nothing about it needs the durability phase 2's
own progress tracking requires.

## DESIGN-MIGRATION-003: One read of the source, several `--cdc-target-size-bits` values, one output repository per value
Status: decided

REQ-MIGRATION-004: migrating the same source content at more than one candidate target chunk size
does not need to read that content once per value compared. Reading a given piece of old content
once and feeding it to as many independently configured chunkers as target sizes were requested -
each producing its own chunk boundaries, hashes, and output repository - reads the source exactly
as many times as REQ-MIGRATION-002 already implies for a single target size, regardless of how many
values are actually being compared.

This composes with DESIGN-MIGRATION-001's phase split unchanged: the metadata import has no
target-size dependence at all (chunk boundaries are entirely a phase-2 concept), so it runs exactly
once regardless of how many target sizes phase 2 then migrates into. Phase 2 opens one destination
repository per requested value and, for each distinct old content reference it reads, updates every
open destination's own progress tracking independently - one value's migration falling behind, or
needing to resume, never blocks or restarts the others.

## Open question

The exact shape of phase 2's own durable progress record (DESIGN-MIGRATION-001) - a dedicated small
table alongside each destination repository's own metadata, or something inferred from the
destination's own tree state - is not yet decided.
