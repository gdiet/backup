# Metadata Storage

Re-examines this implementation's metadata-storage design from first principles (see "This Is A
Rewrite, Not A Port" in `AGENTS.md`). The sections below are worked through in order, top to
bottom: the storage engine choice (DESIGN-METADATA-001/002) settles both the Rust binding
(DESIGN-METADATA-010) and the writer/concurrency model (DESIGN-METADATA-003) - two separate
consequences of that same choice, not sequential to each other - and the writer/concurrency model
in turn bounds what the schema-level review (DESIGN-METADATA-004) is even evaluating against.
Crate structure (DESIGN-METADATA-006) is independent of the rest and deliberately left for last.

## DESIGN-METADATA-001: SQL database or something else

Status: decided

A SQL database. The metadata this needs to hold is genuinely relational: a filesystem tree
(parent/child, soft-delete), a many-to-many deduplication mapping between file contents and
content-addressed chunks with reference counts that must stay consistent on both sides, ordered
chunk sequences, byte-range extents, and multi-table writes that must succeed or fail as a unit
(registering a new tree entry can simultaneously introduce a new content row, several
content-chunk rows, and several ref-count updates). Ad-hoc queries - path filters, aggregations,
"which tree entries reference
chunk X" - are also a first-class need (`find`, `list`, `stats`, `check`). Referential integrity,
multi-table transactions, declarative ad-hoc queries, and trigger-maintained invariants are
exactly what a relational database is built to provide; any alternative has to reimplement some or
all of them in application code.

### Alternatives considered and rejected

**Embedded key-value store** (e.g. sled, redb, fjall) or **LMDB** (via heed): fast, ACID
transactions, but no native support for referential integrity, joins, or triggers - every
consistency invariant this schema gets from DB triggers (reference counting in particular) would
move into hand-written application code, exactly the kind of correctness-critical bookkeeping most
likely to go subtly wrong.

**A custom on-disk format**, purpose-built for this schema: viable for a genuinely simple,
single-purpose layout (the byte store already does this, successfully, for a sequential
write-once structure), but not for a schema with several cross-referencing, independently-queried
tables. Such a schema would mean building a bespoke index, transaction, and query layer, competing
against one of the most thoroughly tested pieces of software that exists (SQLite), for a role where
correctness matters enormously. Losing or corrupting the index effectively loses track of
otherwise-intact backed-up data.

**An embedded analytical (OLAP) engine** (e.g. DuckDB): optimized for large scans and columnar
aggregation, not the workload here - many small, transactional single-row/few-row writes
(OLTP-shaped), which a row-oriented engine serves better.

**A document/NoSQL store** (e.g. MongoDB): typically a separate server process, which conflicts
with REQ-OPERABILITY-001 in
[`../../requirements/non-functional/operability.md`](../../requirements/non-functional/operability.md)
(no separately managed database server); even an embedded document store would still face the same
referential-integrity and trigger gap as the key-value alternatives above, since it is
schema-flexible by design rather than built for normalized, cross-referencing tables.

Revisit if: the schema turns out to need far fewer cross-table relationships and ad-hoc query
shapes than expected, tipping the balance toward a simpler key-value layout - no evidence of that
today.

## DESIGN-METADATA-002: SQLite or another engine

Status: decided

SQLite. Relevant constraints: REQ-OPERABILITY-001 in
[`../../requirements/non-functional/operability.md`](../../requirements/non-functional/operability.md)
(no separately managed database server, small bounded memory footprint, easy installation) and
REQ-OPERABILITY-002 in the same file (mirrorable with generic file-sync tools - mirror-safety only
needs to hold while no process is using the repository, now documented directly in that
requirement). Correctness matters enormously for this role: losing or corrupting the metadata
index effectively loses track of otherwise-intact stored data.

SQLite satisfies all of this and more. It is embeddable with no server process at all, has full SQL
feature coverage including triggers, foreign keys, and partial indexes (exactly what
DESIGN-METADATA-001 established this schema needs), and is one of the most thoroughly tested pieces
of software that exists. It also has a mature Rust binding (`rusqlite`), which - via its "bundled"
feature - can be compiled directly into the binary with no runtime dependency at all, going beyond
"no separate server" to "nothing to install separately, period."

### Alternatives considered and rejected

**libSQL/Turso**: a SQLite fork adding features such as built-in networked replication - not
needed here (no networked-sync requirement), and a smaller, newer project carrying more fork-drift
risk than upstream SQLite for no corresponding benefit.

**Firebird Embedded**: a real embedded SQL option, but a substantially smaller ecosystem and
thinner, less actively maintained Rust bindings than SQLite - no advantage large enough to justify
that.

**A JVM-based engine** (e.g. H2): would pull in an entire JVM runtime as a dependency, directly
conflicting with REQ-OPERABILITY-001's low-footprint, no-separate-runtime requirement, in an
otherwise pure-Rust project.

**DuckDB**: excluded already in DESIGN-METADATA-001 for being OLAP-shaped rather than a fit for this
workload's many small transactional writes.

**A pure-Rust SQL engine**: none identified with maturity, feature coverage, or ecosystem support
comparable to SQLite.

Revisit if: a future requirement needs something SQLite structurally cannot provide (e.g. true
concurrent multi-writer transactions at a scale WAL mode cannot serve - see DESIGN-METADATA-003)
that a
specific alternative demonstrably solves.

## DESIGN-METADATA-010: Rust binding

Status: decided

`rusqlite`. Named in passing above as one reason SQLite fits, but not, until now, decided in its
own right - a gap worth closing explicitly, since every schema example, trigger, and migration hook
discussed in DESIGN-METADATA-004 and DESIGN-METADATA-005 already assumes its specific API shape
(parameter binding via `params![...]`, `&rusqlite::Transaction` in migration hooks).

The mature, de facto standard synchronous Rust binding for SQLite, paired with
`rusqlite_migration` (see DESIGN-METADATA-005) for migration tooling. Its "bundled" feature compiles
SQLite
directly into the binary, matching REQ-OPERABILITY-001's "nothing to install separately" bar noted
above.

### Alternatives considered and rejected

- **`sqlx`**: async-first (built around `tokio`/`async-std`), with compile-time-checked queries.
  Async is a structural mismatch here, not a stylistic one: DESIGN-METADATA-003's
  single-coordinated-writer
  model is synchronous throughout, and nothing else in this project runs an async runtime (the CLI,
  the FUSE mount via `fuser`, and `rayon`-based parallel chunking are all thread-based). Pulling in
  an async runtime solely for the database layer would be a large, unjustified dependency-surface
  increase for a mismatch, not a fit, with the rest of the architecture. It also targets several
  database engines, which SQLite-only `rusqlite_migration` deliberately does not.
- **The `sqlite` crate**: a lower-level, less widely used binding with a much smaller ecosystem and
  thinner tooling around it than `rusqlite` - no concrete advantage identified over the
  already-proven choice.
- **Raw `libsqlite3-sys` FFI directly, no wrapper**: would mean reimplementing what `rusqlite`
  already provides safely (row mapping, statement caching, parameter binding, error handling) -
  avoidable, unjustified risk and effort.

## DESIGN-METADATA-003: Writer/concurrency model

Status: decided

One coordinated writer, many concurrent readers, within a single writing process; concurrent
writing processes against the same repository are refused outright (REQ-MAINTENANCE-004 in
[`../../requirements/functional/maintenance.md`](../../requirements/functional/maintenance.md)),
not merely assumed not to happen.

SQLite itself only ever allows one write transaction in progress at a time, regardless of how many
connections attempt to write - WAL mode lets readers proceed concurrently with the single writer,
but grants no additional write concurrency. A single coordinated writer is therefore not an
arbitrary constraint but the correct way to use SQLite from a multi-threaded producer. It avoids
lock contention and retries between connections that would only ever serialize against each other
anyway. It can also batch several pending writes into one transaction, which independent
connections racing for the write lock cannot coordinate to do.

REQ-PERFORMANCE-002 in
[`../../requirements/non-functional/performance.md`](../../requirements/non-functional/performance.md)'s
cross-stream parallelism target is satisfied above this layer: the CPU-bound work of chunking and
hashing a stream's content happens fully in parallel across worker threads, entirely before any
database interaction. Those threads only hand off to the single coordinated writer once they have
something to persist (a chunk-existence check, an insert, reference-count bookkeeping) - a small
volume of work relative to the I/O of writing the chunk content itself, not a throughput bottleneck
for this workload.

### Alternatives considered and rejected

**Multiple writer connections**, relying on SQLite's busy-timeout/retry to arbitrate: strictly
worse than a single coordinated writer given SQLite's own single-write-transaction limit - only
adds contention and retry overhead, with no additional real concurrency to show for it.

**Sharding across multiple database files** (e.g. by directory): would fragment deduplication,
which needs a single global content-address space - a chunk already stored under one shard would
not be found when the same content is written again under another, either losing cross-shard
deduplication or requiring a separate global index anyway, defeating the purpose.

**An asynchronous or eventual write queue** (an operation is reported as complete before it is
actually persisted): unnecessary complexity for an embedded, single-machine tool, and conflicts
with stored content actually being durable once an operation reports it as stored.

Revisit if: multiple independent processes writing to the same repository at the same time becomes
an actual target scenario - no such scenario is known today.

## DESIGN-METADATA-004: Database structure

Status: decided

Keep a `contents` table, deduplicating whole-file content in addition to chunk-level
deduplication. See [`metadata-schema-comparison.md`](metadata-schema-comparison.md) for the
trade-offs weighed against the rejected alternative (no `contents` table) and the reasoning behind
the decision, and
[`metadata-schema-with-contents-table.md`](metadata-schema-with-contents-table.md) for the chosen
schema itself.

Reached by a full review of the schema, table by table, column by column, constraint by
constraint, trigger by trigger, six tables in total (`repository_settings`, `chunks`,
`chunk_extents`, `contents`, `content_chunks`, `tree_entries`). Findings that led to a change are
detailed below; several properties were reviewed and kept as sound without changes - in
particular: the `repository_settings` single-row-via-`CHECK(id = 1)` pattern; the partial unique
index enforcing one active `(parent_id, name)` per directory, including the self-referencing root
row that makes a non-null `parent_id` possible everywhere; a plain (non-partial) index on
`tree_entries.parent_id` (needed for a measured reason: an unscoped recursive query against a real
~7.16M-row repository never finished in under an hour without it, 1m48s with it); `ON DELETE
CASCADE` chains on `chunk_extents`/`content_chunks`; and soft-delete deliberately not decrementing
a content's reference count (a soft-deleted entry stays recoverable until actually purged).

### Alternative considered and rejected: a genuine `AFTER UPDATE OF content_id` trigger

A ref-count-maintaining `INSERT`/`DELETE` trigger pair only fires on `INSERT`/`DELETE` - a design
that also allows a row's content reference to be set later via an in-place `UPDATE` (e.g. a
placeholder row settling once its content becomes known) needs something more: either a genuine
`AFTER UPDATE OF content_id` trigger alongside the `INSERT`/`DELETE` pair, or every call site that
performs such an update remembering a manual, hand-written compensation step - fragile, since a
single forgotten call site would silently desynchronize the reference count from what actually
references it, with no immediate symptom.

A genuine trigger would close that gap so the invariant holds regardless of how many call sites
update `content_id` in place, present or future, with no call-site-specific manual compensation
needed at all:

```sql
CREATE TRIGGER tree_entries_ref_count_upd AFTER UPDATE OF content_id ON tree_entries
  WHEN NEW.content_id IS NOT OLD.content_id
BEGIN
  UPDATE contents SET ref_count = ref_count - 1 WHERE id = OLD.content_id;
  UPDATE contents SET ref_count = ref_count + 1 WHERE id = NEW.content_id;
END;
```

`IS NOT` (not `!=`) is required for correctness here: an ordinary inequality comparison evaluates
to unknown, not true, whenever either side is `NULL`, which would silently skip the trigger's
`WHEN` condition exactly in the two cases (a placeholder settling to real content, or content
being cleared) where the reference-count change actually matters. Each `UPDATE ... WHERE id =
OLD/NEW.content_id` naturally matches zero rows on its own when that side is `NULL` (`id = NULL`
is never true in SQL), so the trigger body needs no separate `NULL` handling beyond that. The
`WHEN` guard itself is not required for correctness (an update that re-sets `content_id` to its
existing value nets to an exact no-op, decrementing and immediately re-incrementing the same row
within the same transaction) - it exists to skip that pointless round-trip write, since
`AFTER UPDATE OF content_id` fires whenever a statement's `SET` clause mentions the column at all,
whether or not the value actually changes.

Rejected in favor of removing the underlying problem instead of patching around it: a
`tree_entries` row for a file is never inserted until its content is actually settled (see
"In-progress files are not written to the database" in `metadata-schema-with-contents-table.md`),
so `content_id` is never mutated in place at all - no in-place `UPDATE` call site exists for this
trigger to guard. Kept here for the `IS NOT`/`WHEN` reasoning above, still relevant to any future
trigger written against this general pattern.

### Guard the two fixed sentinel rows against deletion

Two rows were, at the time of this review, load-bearing fixed anchors that application code
protected only by convention, not by anything the database itself enforced: `tree_entries` id `0`
(the tree's root, its own parent) and `contents` id `1` (`EMPTY_CONTENT_ID`, the row every empty
file resolved to, deliberately kept even at `ref_count = 0`). A future code path that deletes
unreferenced rows without knowing about either exception - a simplified rewrite of a cleanup
routine, a generic admin command - would silently delete one of them; the failure that follows is
loud (the next insert referencing the now-missing row violates a foreign key) but avoidable
entirely. Cheap to close with a guard trigger per table - the `WHEN` condition is a single integer
comparison, evaluated per deleted row, negligible next to the cost of the `DELETE` itself:

```sql
CREATE TRIGGER contents_protect_empty_row BEFORE DELETE ON contents
  WHEN OLD.id = 1
BEGIN
  SELECT RAISE(ABORT, 'cannot delete the shared empty-content row');
END;

CREATE TRIGGER tree_entries_protect_root BEFORE DELETE ON tree_entries
  WHEN OLD.id = 0
BEGIN
  SELECT RAISE(ABORT, 'cannot delete the root tree entry');
END;
```

**Refined**: the guard trigger for the root tree entry (`tree_entries_protect_root`) still applies
as described. The guard trigger for the empty-content row (`contents_protect_empty_row`) does not:
neither schema proposal keeps `EMPTY_CONTENT_ID` as a distinct, pre-seeded sentinel at all - an
empty file's content is instead found or inserted through the same `(length, hash)` dedup lookup
used for any other content, eligible for ordinary purge like any other unreferenced row. See
"In-progress files are not written to the database" in `metadata-schema-with-contents-table.md` for
why - the disambiguation problem `EMPTY_CONTENT_ID` originally existed to solve (telling "no
content decided yet" apart from "decided, and empty") does not arise once a file's row is never
inserted before its content is settled. `metadata-schema-without-contents-table.md` never had an
`EMPTY_CONTENT_ID`-equivalent to begin with.

### Hash width: 160 bits (20 bytes), truncated from BLAKE3's full output

Both `chunks.hash` and `contents.hash` truncate BLAKE3's extendable-output function (XOF) to 20
bytes rather than storing its full 256-bit output. Truncating costs nothing in compute (the XOF
already computes enough internal state to extract more output; extracting less is not cheaper), so
the only real trade-off is a smaller stored/indexed value against a smaller collision margin.

Accidental collisions are not a realistic concern at this width, at any repository size this
project could plausibly reach: the birthday-bound collision probability for n hashed items in a
160-bit space is approximately n²/2¹⁶¹. Even for an implausibly large repository of 10^11 (one
hundred billion) chunks - already far beyond REQ-OPERABILITY-001's "modest hardware" scope - that
probability is about 3×10⁻²⁷, negligible next to far more likely failure modes such as an
undetected memory bit flip.

Deliberately engineered collisions are a different question: the relevant figure for a hash this
wide is its birthday-bound collision resistance, ~2⁸⁰ hash evaluations, not the full 2¹⁶⁰ - below
what current cryptographic guidance (generally at least a 128-bit security margin for a new
design) considers a comfortable margin, though still far beyond casual reach. Judged not to matter
here: exploiting an engineered collision would require an attacker to already control what content
lands in the victim's backup before it is stored. At that point, directly tampering with that
content is a far simpler attack than manufacturing a BLAKE3 collision. This is a threat model this
project (a personal backup tool, not a multi-tenant deduplication service where cross-tenant
collisions would be a genuinely different concern) does not need to defend against.

Revisit if: this project's actual scale or threat model changes materially from what is assumed
today (e.g. shared/multi-tenant use, or repositories large enough to make the
accidental-collision margin above worth recomputing).

### Missing sanity constraints

Neither `chunks.hash` nor `contents.hash` constrains its length, and `chunk_extents` has no
`CHECK` ensuring a half-open range is actually well-formed. Both are cheap, DB-enforced guards
against a bug elsewhere silently inserting malformed data that would otherwise surface much later,
if at all:

```sql
-- on chunks and contents:
CONSTRAINT chk_..._hash_length CHECK (length(hash) = 20)

-- on chunk_extents:
CONSTRAINT chk_chunk_extents_range CHECK (stop > start)
```

### `contents` should key on `(length, hash)`, matching `chunks`

`chunks` deduplicates on `(length, hash)` rather than `hash` alone, narrowing the damage a hash
collision could do: two different chunks that happened to collide would still be told apart if
their lengths differ. `contents` currently deduplicates on `hash` alone, even though it has its
own `length` column (the file's total logical size) sitting right there unused for this purpose.

The asymmetry does not hold up. `contents.hash` hashes the ordered sequence of each referenced
chunk's `(length, hash)` pair, not raw file bytes - but a hash's output is not less collision-prone
merely because its input is structured rather than raw. Two different chunk sequences can easily
share the same total length while differing in exact composition (e.g. one 100-byte chunk plus one
200-byte chunk vs. one 50-byte chunk plus one 250-byte chunk: 300 bytes either way, clearly
different content), so `contents.length` is not implied by a `contents.hash` collision any more
than `chunks.length` is implied by a `chunks.hash` collision. The only real difference is
cardinality - a typical repository has fewer `contents` rows than `chunks` rows, so the
birthday-bound collision probability is lower in absolute terms - a difference of degree, not of
kind, and not a reason to skip a safeguard that costs nothing (the column already exists).
Change `UNIQUE (hash)` to `UNIQUE (length, hash)` on `contents`, matching `chunks`.

### Why `contents.hash`/deduplicating whole contents at all

Worth stating explicitly, since it is not needed for integrity verification - chunk-level
verification (each `chunks.hash` checked against its own bytes, in the order `content_chunks`
records) already establishes a file's correctness on its own, transitively, with no need for a
whole-file-level hash. `contents`-level deduplication exists purely to avoid redundant metadata:
without it, every file with byte-identical content to another - a common case in backup workloads
(the same template copied across projects, the same photo in more than one album) - would get its
own `contents` row and its own full set of `content_chunks` rows, even though the underlying
chunks stay deduplicated either way. One instance of this is already load-bearing in the schema as
it stands: every empty file across an entire repository shares one `contents` row (found or
created through the ordinary dedup lookup, like any other content - see "Guard the two fixed
sentinel rows against deletion" above) rather than each getting its own. Computing the hash costs
nothing extra - it accumulates from data already produced while chunking.

### `contents.length`: stored, not derived - but not database-verified either

`contents.length` (the file's total logical size) could be derived by summing the lengths of its
referenced chunks instead of being stored directly. Kept as a stored column: `getattr`/`stat`/
`list`/`find`-style lookups need a file's size often and cheaply, and a stored integer read is far
cheaper than an aggregate join computed on every such call - the same column is also needed as a
real column, not a computed one, for the composite uniqueness key above. The cost is a consistency
risk a `CHECK` constraint cannot close (SQLite `CHECK` constraints cannot aggregate across other
tables): nothing today stops `contents.length` from silently drifting away from the actual sum of
its referenced chunks' lengths if it were ever set incorrectly at insert time. Rather than paying
for a derived value on every read to close that gap, worth ensuring the repository's integrity
check (REQ-INTEGRITY-001 in
[`../../requirements/functional/integrity.md`](../../requirements/functional/integrity.md))
explicitly compares stored `contents.length` against the actual sum, if it does not already.

### Root entry's seed `time`: no need to compute "now" at all

Computing the current time directly in SQL for the root tree entry's seed row
(`CAST(strftime('%s', 'now') AS INTEGER) * 1000`, converting seconds to the milliseconds this
schema uses elsewhere) is unwieldy compared to the same computation done in Rust, and, on
reflection, unnecessary in the first place: the root is `kind = KIND_DIR` like any other directory,
so REQ-TREE-005 in
[`../../requirements/functional/tree.md`](../../requirements/functional/tree.md) (a directory's
modification time reflects changes to its direct entries) applies to it the same as to any other
directory. Its seed value is superseded the moment anything is created at the top level - true for
essentially every repository that sees real use - so the value only needs to be a valid
placeholder, not a plausible "now." Both schema proposals seed it as `time = 0` instead, with a
comment explaining why, rather than computing an actual timestamp anywhere. The one cost: a freshly
initialized but never-populated repository would show the mounted root's modification time as the
Unix epoch rather than its actual creation time - cosmetic, not a correctness concern, but worth
noting so it does not read as a bug later.

Note in passing: REQ-TREE-005 itself (updating a directory's `time` when a direct entry is created,
removed, or renamed) is deliberately not a database trigger - see
[`directory-mtime-touch.md`](directory-mtime-touch.md) for why (a trigger would fire unconditionally
for a directed import's bulk population too, which needs different behavior per REQ-INGEST-005).
That decision is already made; only the write-path code implementing it does not exist yet.

## DESIGN-METADATA-011: Timestamp representation

Status: decided

Milliseconds since the Unix epoch (UTC), stored as a plain `INTEGER` column - every timestamp in
this schema (`tree_entries.time`, `repository_settings.creation_time` in
[`metadata-schema-with-contents-table.md`](metadata-schema-with-contents-table.md), REQ-STORAGE-008
in [`../../requirements/functional/storage.md`](../../requirements/functional/storage.md)) uses
this representation.

No requirement calls for finer precision: REQ-INGEST-005 and REQ-RESTORE-001 in
[`../../requirements/functional/ingest.md`](../../requirements/functional/ingest.md) and
[`../../requirements/functional/restore.md`](../../requirements/functional/restore.md) speak of a
modification time being "reflected"/"preserved", not matched to a specific precision, and
millisecond granularity is already far finer than what "did this file change" (REQ-INGEST-003)
needs in practice - two genuinely distinct edits to the same file landing inside the same
millisecond is not a realistic concern for this project's workload. Seconds alone would be a
real, avoidable precision loss against what modern filesystems actually report; nanoseconds would
cost nothing extra to store (SQLite `INTEGER` is 8 bytes regardless) but buy nothing either, given
the paragraph above - milliseconds is the point past which more precision has no identified use.

## DESIGN-METADATA-005: Migration approach

Status: decided

`rusqlite_migration`. Distinct from repository migration from the Scala implementation
(`migration/from-scala.md`), which is out of scope here.

`rusqlite_migration` is the right migration tooling for this project: purpose-built for exactly
the already-decided pairing (`rusqlite` against SQLite only, not a multi-backend abstraction),
tracks the applied schema version via `PRAGMA user_version`,
wraps each migration in a transaction, and - beyond plain SQL - supports running Rust code
alongside a migration's SQL via a hook with access to the live `&rusqlite::Transaction`
(`M::up_with_hook`), which the table-rebuild note below already relies on being available. No
alternative considered offered a concrete advantage: `refinery` does the same job but abstracts
over several database backends this project will never use; hand-rolling a `PRAGMA user_version`
check-and-apply loop would reimplement transaction wrapping and version bookkeeping
`rusqlite_migration` already does correctly, for no identified benefit; and any migration tool tied
to a different SQLite binding than `rusqlite` (e.g. `sqlx`'s own migration support) would reopen
the binding choice
decided in DESIGN-METADATA-002, not just the migration-tool choice.

Opening a repository for writing (`open_repository`) applies any pending migration automatically
and transparently; opening it read-only refuses instead of migrating silently. No separate,
explicit "upgrade" command is needed, since applying a migration already
requires the deliberate, consequential step of opening for writing - but a read-only session never
mutates the schema out from under a concurrent reader.

### Migration source: inline Rust constants, not separate `.sql` files

`rusqlite_migration` supports either: SQL embedded directly as Rust string literals in a migration
list, or separate `.sql` files loaded via `include_str!` or the crate's `from-directory` feature.
This project keeps inline Rust constants.

What favors separate files: real SQL syntax highlighting, formatting, and linting in editors and
SQL tooling without special configuration, and the ability to run a migration directly against a
scratch database (`sqlite3 < migration.sql`) or open it in a database GUI without first extracting
it from a string.

What decided against them: a migration needing a Rust hook - already a real, not hypothetical, case
(the table-rebuild note below) - would split across two files, an `.sql` file for the schema change
and a separate Rust function for the hook, connected only by naming convention or a comment, rather
than sitting together as one `M::up_with_hook(sql, hook)` call a reader can see in one place.
Separate files also add a discovery mechanism (either manual `include_str!` calls kept in sync with
the file list, or the `from-directory` feature's file-naming convention) that inline constants get
for free: migration order is just array order, with nothing to keep in sync. Both costs matter more
than the tooling convenience gained: this project's migrations are expected to be occasional,
deliberate, hand-reviewed schema changes accumulated over its lifetime, not a high-volume, rapidly
iterated stream where dedicated SQL-file tooling would pay for itself.

### Changing a constraint always needs a full table rebuild

Not itself a decision, but a mechanical constraint any migration approach chosen above has to work
within: SQLite's `ALTER TABLE` supports only `RENAME TO`, `RENAME COLUMN`, `ADD COLUMN`, and `DROP
COLUMN` - there is no `ADD CONSTRAINT`/`DROP CONSTRAINT`, and no way to change an existing `CHECK`
in place, named or not. Giving a `CHECK` a name (as this document's schema does throughout) buys
readability and shows up in the constraint-violation error message - it does not make the
constraint any easier to change later.

Changing one - e.g. widening `chk_tree_entries_kind_content_id` from two `kind` values to three if
REQ-TREE-007 (symbolic links) is ever implemented, see "Future extension: symbolic links" in
`metadata-schema-with-contents-table.md` - needs the table-rebuild procedure SQLite's own
documentation recommends for schema changes `ALTER TABLE` cannot express directly: create a new
table with the revised schema, copy the data across, drop the old table, rename the new one into
place, then recreate whatever indexes and triggers belonged to the original table (a rebuild drops
those along with the table). `tree_entries` specifically also has a self-referencing foreign key
(`parent_id REFERENCES tree_entries(id)`), which needs `PRAGMA foreign_keys=OFF` for the duration
and a `PRAGMA foreign_key_check` afterward, per that same documented procedure.

Considered and rejected as a shortcut: editing `sqlite_master.sql` directly via `PRAGMA
writable_schema=ON`. SQLite itself advises against this outside of manual recovery from
corruption - it validates nothing (neither existing data against the revised constraint, nor
consistency with the table's indexes and triggers) and risks corrupting the database file if done
incorrectly. No real alternative to the rebuild procedure above.

### Pre-release: a single, freely rewritten `v1` migration

Before this implementation's first release, the migration list holds exactly one migration
(`v1`), edited in place whenever a schema change is needed, rather than followed by a `v2`, `v3`,
and so on accumulating on top of it.

No released repository exists yet whose schema history the migration list would need to preserve,
so nothing forces a schema change to layer a new migration on top of a decision later found
wrong - `v1` itself can simply become correct. This also means the table-rebuild procedure above,
and the version-compatibility concerns it exists to manage, do not apply yet: rewriting `v1`
in place is not constrained by `ALTER TABLE`'s limitations the way a genuine post-release migration
would be, since there is no prior `v1` shape any already-migrated database needs to stay compatible
with.

The practical consequence for any repository created against an earlier shape of `v1` - in
particular, this project's own test and tryout repositories - is that it is not migrated forward:
it is simply deleted and recreated against the current `v1` once a schema change lands. `v1` only
ever needs to handle the schema shape it currently has, not any shape it used to have.

This ends at the first release: from that point on, a real repository's schema history has to be
preserved, and a schema change goes back to being a genuine new migration appended after `v1`, not
an edit to it - the table-rebuild procedure above becomes the actual mechanism for that, not merely
a documented fallback.

## DESIGN-METADATA-006: Crate structure

Status: decided

Its own crate, named `db`, with a deliberately narrow public interface. This workspace already
holds two crates built the same way (`cdc`, a `Chunker` trait behind a
narrow interface; `mountfs`, a mount abstraction behind its own) - a third for metadata management
is consistent with that pattern, not a new kind of ceremony introduced just for this. Named `db`:
short, unambiguous within the workspace, and consistent with the scoped-test invocation style used
elsewhere in this document (`cargo test -p db ...`).

More than precedent alone motivates a crate boundary here specifically: sections 4 and 5 above
establish that a correctness invariant depends on metadata access being funneled through curated
operations rather than ad hoc SQL. REQ-TREE-006 (atomic visibility) holds only because a file's
`tree_entries` row is never inserted before its content is settled - never with `content_id IS
NULL` as an in-progress placeholder. If code anywhere in `mountfs` or a future `cli` crate could
issue arbitrary SQL against the metadata database directly, that invariant would rest on discipline
alone. A crate boundary with a narrow `pub fn` surface - no `pub` `rusqlite::Connection`, no raw SQL
strings exposed to callers - makes it structurally enforced instead: violating it would require
first widening the crate's own public interface, not just writing a careless query somewhere else
in the workspace.

Also gains what `cdc` and `mountfs` already benefit from: scoped tests
(`cargo test -p db tree_entries_maintain_content_ref_count`-style, without a mount or CLI harness),
and a compile boundary - a migration or trigger change does not force a rebuild of unrelated crates,
and vice versa.

The exact shape of the public interface (which operations it exposes, their signatures) is
deliberately not decided here - premature ahead of actually implementing `store`/`restore`/etc.,
and better worked out against real call sites than designed speculatively in advance.

## DESIGN-METADATA-012: SQLite connection pragmas

Status: implemented (crates/db/src/connection.rs)

Every connection this crate opens (both `init_repository`'s and `open_repository`'s - there is
only one connection type today, per `Repository`'s own doc comment) is configured the same way,
via `crates/db/src/connection.rs`, in this order. The five settings split into two genuinely
different kinds, not one uniform "connection setup" step:

**Per-connection - SQLite keeps no record of these in the database file itself, so each of them
needs setting again on every single connection, forever, regardless of what any earlier connection
already set:**

1. `foreign_keys = ON` - off by default per connection in SQLite. Without it, this schema's
   `REFERENCES`/`ON DELETE CASCADE` constraints (DESIGN-METADATA-004) are pure documentation - not
   enforced, and cascade deletes silently do not happen at all.
2. `synchronous = NORMAL` - the standard pairing with WAL mode (below): safe against database
   corruption even on a crash or power loss, at the cost of potentially losing the last few
   committed transactions - an acceptable trade for the performance `FULL` would give up, and no
   worse than what DESIGN-REPOSITORY-001's own crash-safety reasoning already assumes at the
   file-rename level.
3. `busy_timeout = 5000` (milliseconds) - wait instead of failing outright with `SQLITE_BUSY` on
   *transient* contention (e.g. another connection mid-checkpoint). Not a substitute for
   REQ-MAINTENANCE-004's real cross-process exclusivity, which has its own separate mechanism
   (DESIGN-MAINTENANCE-001 in [`repository-locking.md`](repository-locking.md)) - this only
   smooths over momentary lock collisions.

**Persistent, whole-database properties - stored in the database file's own header, so any
connection that ever opens that file afterward already sees the effect, with no need to ask
again:**

4. `auto_vacuum = INCREMENTAL` - lets a future `db compact` (REQ-MAINTENANCE-003) reclaim SQLite's
   own internal free pages on demand (`PRAGMA incremental_vacuum`) rather than automatically on
   every commit (`FULL`) or never (`NONE`). Ordering matters here specifically: `auto_vacuum` only
   takes effect for free on a database that does not have any tables yet - once a table exists (or
   once the database has switched to WAL, next), changing it silently does nothing further until a
   full, blocking `VACUUM` is run. This is why it is set here, in Rust code, before running the `v1`
   migration - not inside the migration's own SQL, which only runs once the database already has
   (or is about to gain) its schema, and which has no notion of connection-level pragmas as part of
   its own, separately-tracked (`PRAGMA user_version`) versioning anyway.
5. `journal_mode = WAL` - what DESIGN-METADATA-003's "WAL mode lets readers proceed concurrently
   with the single writer" already assumes, but nothing before this decision ever actually set it;
   SQLite defaults to rollback-journal mode otherwise. Checked, not merely requested: SQLite
   silently falls back to a different mode instead of failing outright if WAL is unsupported (e.g.
   some network filesystems), so the pragma's returned value is compared against `"wal"` and
   surfaced as an error (`Error::WalUnavailable`) on a mismatch, rather than continuing under a
   silently different concurrency model than the rest of this design assumes.

   `Error::WalUnavailable` only ever catches that *silent* fallback - the `PRAGMA` itself
   succeeding, just not with `"wal"`. A filesystem that instead makes the `PRAGMA` call hard-fail
   bypasses it entirely: observed over a WSL<->Windows `9p`/`v9fs` bridge, where every
   connection-configuring `PRAGMA` (not only `journal_mode`) can fail outright with a locking- or
   I/O-category SQLite error. That case surfaces as `Error::ConnectionUnreliable` instead (see
   `crates/db/src/connection.rs`'s `wrap_unreliable_connection_error`) - a distinct failure mode
   from the silent-fallback one this pragma check exists to catch. See
   [`../../README.md`](../../README.md)'s "Known Limitations" for the short, user-facing summary,
   the same cross-reference [`repository-locking.md`](repository-locking.md) already carries for
   the separate write-lock limitation.

   In practice, this surfaces as `error: database is locked` or `error: disk I/O error`, sometimes
   for a single read, before any write lock is even attempted. Reliability is asymmetric (worse
   once the WAL side-files, `-wal`/`-shm`, already exist) and direction-dependent between the two
   sides of a WSL<->Windows bridge - a repository whose storage physically lives on one side can
   fail outright when opened from the other. SMB has tested reliably for single-process access
   (both a local loopback share and a real mapped network drive on a corporate DFS namespace);
   concurrent multi-process access over any network filesystem is untested and stays under
   DESIGN-MAINTENANCE-001's separate write-locking limitation
   ([`repository-locking.md`](repository-locking.md)) regardless.

Deliberately left untouched, since nothing has measured a need to: `cache_size`, `mmap_size`,
`temp_store`, `wal_autocheckpoint`.

### Re-asserting the persistent settings on every open is deliberate, not merely harmless

Since `auto_vacuum`/`journal_mode` are already durably set after the very first connection
(`init_repository`'s own, before `v1` ever runs), every later `open_repository` call re-requesting
them is redundant in the common case - but not removed, for a reason beyond "harmless enough to
leave alone": a repository this crate did not itself create - in particular, a future
REQ-MIGRATION-001 import from a Scala repository - would not otherwise be guaranteed to already
have either property set correctly. Re-asserting on every open makes `open_repository` a
self-normalizing check for that case too, at negligible per-open cost, rather than a second,
import-specific code path needing to remember to configure these the same way.

### The same configuration serves both a mount session and a bulk operation

Whether a caller issues many small transactions in quick succession (an interactive read-write
mount session) or fewer, larger ones (a bulk `store` run, a future migration or maintenance
command) does not change any of the above: DESIGN-METADATA-003 already establishes that SQLite
itself only ever admits one write transaction at a time regardless of connection count or
transaction size, so there is no scenario here where a different pragma configuration would
unlock more write concurrency - only `busy_timeout` ever masks contention, and it does so the same
way regardless of what kind of operation is contending.

### A lighter configuration for a genuinely read-only connection

Status: implemented (`crates/db/src/connection.rs`'s `configure_read_only_connection`,
`crates/db/src/lib.rs`'s `open_repository_read_only`)

`Repository::conn` from `open_repository` holds one connection shared between reads and writes -
the full configuration above applies to it. One from `open_repository_read_only` instead holds a
genuinely `SQLITE_OPEN_READ_ONLY` connection, needing only `busy_timeout` from the per-connection
group - `foreign_keys`/`synchronous` have nothing to enforce on a connection that never writes.
Nothing from the persistent group needs setting there either, for the ordinary reason any
persistent property does not (already true by the time a read-only connection opens): every
repository this crate creates is already durably in WAL mode from `init_repository` onward, so
there is nothing to gain from re-asserting it - confirmed empirically, a plain
`SQLITE_OPEN_READ_ONLY` connection can still issue `PRAGMA journal_mode = WAL` as a no-op when the
database is already in WAL mode, but `open_repository_read_only` does not attempt it, since
nothing needs it to. A read-only connection cannot reuse `configure_write_connection` regardless:
the rest of that function's `PRAGMA` calls (`foreign_keys`, `synchronous`, `auto_vacuum`) are
either pointless or would fail outright against a read-only connection.

Two further properties this split needed, beyond the connection-pragma question itself:

- **Migration.** `open_repository` always migrates automatically (DESIGN-METADATA-005); a
  read-only connection cannot (no write permission to run one), so `open_repository_read_only`
  checks `rusqlite_migration::Migrations::pending_migrations` instead - itself just a read of
  `PRAGMA user_version`, safe on a read-only connection - and refuses with an actionable
  `Error::SchemaNeedsMigration` if the schema is behind what this code expects, rather than either
  silently operating against a stale schema or surfacing a confusing raw SQL error partway through
  some later query. Opening the repository once with a write-capable operation (which migrates it)
  resolves this; a read-only open works again afterward.
- **Mutation refusal.** `Repository::with_transaction` - the one choke point every mutating method
  goes through - refuses outright (`Error::ReadOnlyRepository`) when the `Repository` came from
  `open_repository_read_only`, rather than letting each call site discover SQLite's own
  `SQLITE_READONLY` independently as a bare `Error::Sqlite`.

Used by a read-only mount (REQ-MOUNT-002) - `crates/cli/src/mount.rs` opens read-only via this
path, write-capable via `open_repository`, based on `--read-write`. Beyond letting a read-only
caller skip work it never needed, this also lets read-only operations keep working on a filesystem
where a full write-mode connection open is unreliable (`Error::ConnectionUnreliable`'s case,
observed over a WSL<->Windows 9p bridge - see README.md's "Known Limitations") even though the
caller never intended to write in the first place.

This is still the same one-connection-per-`Repository` model DESIGN-METADATA-003 describes, just
with two variants of that one connection depending on how it was opened - not yet
DESIGN-METADATA-003's fuller "reads split onto their own connection(s)" end state (multiple
concurrent read-only connections backing a single `Repository`, or read concurrency within a
read-write session); worth revisiting once real read concurrency under a mount actually needs it.

`crates/db/src/name_cache.rs`'s per-directory name cache (DESIGN-MOUNT-017 in
[`tree-namespace-case-sensitivity.md`](tree-namespace-case-sensitivity.md)) leans on exactly this
one-connection-per-`Repository` property for its own correctness: it holds no lock of its own at
all, living as a plain field alongside the connection inside the same `Mutex` (a `Locked { conn,
name_cache }` struct), so it is unreachable except through that one lock. A future split to multiple
concurrent connections backing one `Repository` would put more than one connection behind a single
cache and needs to revisit that cache's own access pattern alongside the split, not after it.
