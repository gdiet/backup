# Tree Namespace Case-Sensitivity

## DESIGN-MOUNT-005: Case-sensitive storage, with a Windows-only case-insensitive lookup fallback

Status: implemented

`tree_entries` name comparison (REQ-MOUNT-010 in
[`../../requirements/functional/mount.md`](../../requirements/functional/mount.md)) stays
case-sensitive at the storage level on every platform - no schema change, no `COLLATE` on the
column or its unique index, identical behavior to what the schema already does today. A
Windows-specific fallback sits on top of this, entirely in application code, not in the schema:
every place `crates/db/src/tree.rs` needs to answer "does this name already exist under this
parent" (`find_child_id`, used by lookup, by `mkdir`'s collision pre-check, and by `rename`'s
target-existence check) tries an exact, case-sensitive match first - the existing indexed path,
unchanged - and only on a miss, under a Windows build (`find_child_id_case_insensitive`, dispatched
via a `cfg!(windows)` runtime check kept deliberately un-cfg-gated itself so its logic stays
testable on every platform), fetches the parent's active children and compares them
case-insensitively in Rust (`case_insensitive_match`/`fold_key`). If more than one still matches,
the highest-`id` (most recently created) entry wins.

A rename whose fallback match resolves to the entry being renamed itself (not a different one) is
not a collision: the operation succeeds and updates the stored spelling in place, matching e.g.
renaming `install.txt` to `Install.txt` in Explorer on real NTFS. This needs an explicit identity
check (the fallback match's `id` compared against the source entry's `id`), not just "was a match
found" - conflating the two would make this case, and REQ-MOUNT-009's plain same-path no-op case,
indistinguishable from a real collision.

Because collision detection is reached through the same helper as lookup, `mkdir`/`create`/
`rename` running on a Windows build of `dfs` cannot itself introduce a case-only-differing pair -
even though the storage-level comparison rule never becomes case-insensitive, and a
case-only-differing pair already present (created on Linux, via a migration import, or by anything
writing to the repository's SQLite file directly rather than through `dfs` itself) remains fully
representable and does not get silently merged or refused. This is a property of the whole `dfs`
binary, not specifically of the mount - `crates/db/src/tree.rs` has no notion of "is this call
happening through a mount," only of which platform it was compiled for.

### Alternatives considered and rejected

#### Case-insensitive comparison baked into the schema (a `NOCASE` or custom `COLLATE` on `tree_entries.name` or its index)

The repository is a portable SQLite file, not tied to the platform that created it - the same file
can be opened by a Linux or a Windows build of `dfs` at different points in its life. Baking the
comparison rule into the column or index fixes it identically for whichever build opens the file
next, rather than letting it depend on which platform is actually accessing it - exactly backwards
from what is needed here.

#### Uniform case-insensitive semantics on all platforms, including Linux

Rejected independently of the schema-portability problem above: a real ext4 source tree can
legally, if usually only accidentally (a build artifact, an archive extracted from a case-sensitive
system), contain entries differing only in case. A tree namespace that is case-insensitive at the
storage level cannot represent that state without either refusing to store the second entry or
silently colliding it with the first - unacceptable data loss for a tool whose purpose is fidelity
to the source it backs up.

#### A dedicated SQL collation applied only to Windows-side queries, rather than a Rust-side fallback

Custom, via `rusqlite::Connection::create_collation`, or the built-in `NOCASE`.

Considered as a middle ground that avoids baking anything into the schema itself, by adding
`COLLATE` only at the query level. Rejected: SQLite cannot use the existing binary-collated index
for a differently-collated comparison. So a `COLLATE`-qualified query pays the same full-scan cost
as the chosen Rust-side fallback. This gives no performance advantage to justify the extra
implementation surface (collation registration, its own set of Unicode-corner-case tests) over
simply comparing already-fetched rows in application code. The built-in `NOCASE` specifically has
its own, independent problem regardless of performance: it only folds the 26 ASCII letters
(documented SQLite behavior, not an oversight) - two names differing only in a non-ASCII letter's
case (`café`/`CAFÉ`, `Müller`/`MÜLLER`) would not be recognized as colliding, silently
reintroducing exactly the same-everyday-Windows-app surprise this design exists to avoid, just
narrowed to non-ASCII names instead of eliminated.

#### A persisted, always-computed case-folded key column with its own ordinary index

Not rejected outright - deferred, as the planned escape hatch if the Rust-side scan-and-compare
fallback ever proves too slow for a very large directory (see "Revisit if" below), not built now
because nothing indicates it is needed yet at this project's expected scale. Unlike a schema-baked
collation, an always-computed column would not reintroduce the portability problem above: it is
just precomputed data every platform can ignore or use as it likes, not a comparison rule fixed
into the file - the platform-specific part would stay in *which* column a query filters on, not in
what the schema itself defines as "equal".

### Known limitations

- The case fold itself (`fold_key`) uses Rust's `str::to_uppercase()` - full Unicode case mapping,
  locale-independent (deterministic regardless of the running system's locale, which is a wanted
  property, not just a side effect). It is not guaranteed to match NTFS's own internal
  per-codepoint upcase table exactly in every corner case - full case mapping can change a
  string's length (e.g. German `ß` → `SS`), which a simpler per-character table may not do the
  same way. Not yet spot-checked against real WinFSP for the known Unicode `SpecialCasing`
  exceptions (`ß`, Turkish `İ`/`ı`, Greek final sigma) - do that (`julius-winfsp-ssh` skill) rather
  than trusting the Unicode data alone.
- Unicode normalization (NFC vs. NFD - e.g. `é` as one codepoint versus `e` plus a combining
  accent) is a separate, unrelated concern this decision does not address.

Revisit if: a directory's live entry count grows large enough that the Rust-side scan fallback
becomes measurably slow on a Windows build - see the persisted-fold-column alternative above,
and `developer-todos/performance-baselines-tree-and-content-operations.md` for the broader,
currently unanswered question of what "large enough to matter" actually is on real hardware.

Verification split by what a Linux development machine can actually exercise: the fold/tiebreak
logic and the SQLite query behind it are covered by ordinary, always-compiled unit tests
(`crates/db/src/tree/case_insensitive.rs`'s own `tests` module - `insert_keeping_highest_id_*`,
`find_child_id_case_insensitive_*`), since neither is itself `#[cfg(windows)]`-gated. The full
stack through `Repository::mkdir`/`rename` is covered by `#[cfg(windows)]`-gated tests in the same
module, compiled and checked only on a Windows build (the Docker cross-compile check, and real
WinFSP via the `julius-winfsp-ssh` skill) - not yet run against real WinFSP, only against a
same-process SQLite connection on a cross-compiled binary.

## DESIGN-MOUNT-017: An in-memory name cache for the case-insensitive lookup fallback

Status: decided

Implemented on branch `write-cache` (not yet merged into `mount-read-write`) - `crates/db/src/name_cache.rs`.

### Background

DESIGN-MOUNT-005's Rust-side scan fallback re-fetches and re-scans a directory's entire live child
list on every case-insensitive miss - a cost that grows with the parent's live child count, so
creating (or looking up) `n` entries in sequence under one already-large directory is O(n²) overall.
This was not a theoretical concern by the time it was investigated: a real `db-direct` `mkdir`
measurement on native Windows (`performance/measurements/2026-09-01-julius-db-direct-mkdir-native.md`)
showed throughput falling monotonically from 428.0 to 108.5 ops/s across 19,602 directories created
under one parent, and a query against a real, large Scala-DedupFS repository
(`developer-todos/done/windows-mkdir-degrades-in-large-directories.md`) found that while 78.5% of real
directories hold under 10 entries, a small number of outlier directories (0.09%, 469 of 511,269)
hold 1,000+ entries each and together account for ~16.3% of all files - exactly the directories this
cost concentrates in, not a uniform tax on every `mkdir`.

Simulating the Windows fallback path on non-Windows hardware (temporarily bypassing the
`cfg!(windows)` gate, `crates/db/examples/db_bench.rs`) reproduced the same monotonic decline there
too, confirming the cost is algorithmic - inherent to the unindexed full scan - not specific to
Windows I/O.

### Decision

A small, bounded, most-recently-used-first in-process cache
(`crates/db/src/name_cache.rs`, `NameCache`) of a handful of recently-touched directories' live
children, each kept as a `HashMap<folded_name, id>` (DESIGN-MOUNT-005's `fold_key` output, not the
raw stored name - a lookup against an already-warm entry never re-folds a candidate, only the
query's own target). One `NameCache` per `Repository` instance, holding no lock of its own. It lives
as a plain field alongside the connection inside `Repository`'s single `Mutex` (a `Locked { conn,
name_cache }` struct, not two separately-locked fields), so it is structurally unreachable except
from within `with_connection`/`with_transaction`'s closure, which already holds that one lock. This
is not merely a documented convention a caller could still get wrong: an alternative of a second,
nested `Mutex` was considered and rejected, since it would only ever be uncontended by convention,
not by construction - nothing would enforce that it is only ever taken from inside the first
`Mutex`'s critical section. Correctness of the merged lock is still a direct consequence of
DESIGN-METADATA-003's current
one-connection-per-`Repository` model, not an independent guarantee; see the note this decision adds
in `metadata-storage.md`'s "A lighter configuration for a genuinely read-only connection" section.

The number of cached *directories* (`NAME_CACHE_CAPACITY` in `crates/db/src/lib.rs`) is `16`, chosen
by feel ("a small LRU cache") - not by measurement, benchmarking, or a memory budget. It bounds only
that one axis: the number of distinct directories remembered, evicted least-recently-used first. It
does **not** bound how large any single cached directory's own map is - a cached entry holds every
live child of that directory, however many there are, so the *worst-case* memory cost scales with
`capacity x` the size of the largest directories actually touched, not with `capacity` alone.

Measured (`crates/db/examples/`, throwaway probe, not committed - see this decision's own commit
history for the exact numbers) rather than estimated: 16 cached directories at 6,000 entries each
(the shape of the largest real directory found in the Scala-repository sample above) with ~40-
character folded names cost **~11.5 MB** of process RSS (~126 bytes/entry, `HashMap`/`String`
overhead included). Negligible for an ordinary desktop/server process, and this is already the
pathological worst case for the current capacity - the real distribution above puts the overwhelming
majority of directories far below it - but nothing in the current implementation would prevent a
much larger footprint if `NAME_CACHE_CAPACITY` (or the size of directories actually being cached)
were increased later without revisiting this.

#### A cache is not free of a correctness obligation: keeping it and the database in sync

Unlike the persisted-fold-column alternative in DESIGN-MOUNT-005 above, this cache is a second,
independent representation of "what a directory's live children currently are" - the database
remains the single source of truth, but every mutation to `tree_entries` (`mkdir`, `settle_file`/
`settle_file_collapsing_placeholder`, `rmdir`, `unlink_file`, `rename`) must keep the cache
consistent with it, or a stale entry would silently produce a wrong answer (resurrecting a removed
sibling, missing a newly created one, or - worse - letting `mkdir`'s own collision check miss an
existing case-only-differing entry it must refuse). This is not incidental implementation detail;
it is part of the decision itself: adopting this cache commits to auditing every `tree_entries`
mutation path's effect on it, by inspection, whenever a new one is added or an existing one changes
shape - and covering each with a test that actually exercises the cache (not a fresh instance per
call, which would mask exactly this class of bug), not merely one that passes.

`crates/db/src/tree/case_insensitive.rs`'s own `tests` module does this today for every current
mutation path
(`mkdir_keeps_an_already_warmed_directory_correct_for_a_newly_created_sibling`,
`settle_file_keeps_an_already_warmed_directory_correct_for_a_newly_created_sibling`,
`settle_file_replacing_an_existing_file_keeps_the_cache_correct`,
`find_child_id_case_insensitive_does_not_resurrect_a_sibling_removed_after_the_cache_was_warmed`
(`rmdir`), `unlink_file_does_not_resurrect_a_file_removed_after_the_cache_was_warmed`, and
`rename_within_the_same_directory_keeps_the_cache_correct_for_the_new_name`/
`rename_across_directories_keeps_the_cache_correct_in_both_parents`/
`rename_replacing_an_existing_file_keeps_the_cache_correct`) - each sharing one `NameCache` instance
across a warming lookup and the mutation, verified (not just written green) by reverting the
corresponding cache-update call in turn and confirming the test goes red first. This audit obligation
does not go away once this decision ships; it applies to any future change to a mutation path too.

### Alternatives considered and rejected

#### SQLite's `LIKE` operator as a shortcut

Investigated empirically (`EXPLAIN QUERY PLAN` against the real `tree_entries` schema) rather than
reasoned from the SQLite documentation alone. Rejected for two independent reasons: a `LIKE`
predicate gets no index benefit beyond whatever the query's other `WHERE` clauses already provide
(`parent_id = ?`) - a binary-collated index cannot narrow a case-insensitive comparison, since case
variants are not adjacent in binary sort order - so it would cost the same full scan as the chosen
Rust-side fallback with none of its flexibility. And `LIKE`'s case-insensitivity is documented as
ASCII-only, reintroducing the same non-ASCII correctness gap (`café`/`CAFÉ` not recognized as
colliding) that DESIGN-MOUNT-005 already rejected `NOCASE` for above.

#### Truncating the cached name to a short fixed prefix, to save memory

Considered as a way to shrink this cache's memory footprint - store only e.g. the first 3 folded
characters instead of the full folded name. Measured against two synthetic 20,000-entry datasets:
for names with varied prefixes, a 3-character key narrowed the average candidate bucket from 20,000
to ~769 - a real improvement, but nowhere near `O(1)`. For a realistic bulk-generated pattern
(`IMG_00000001.jpg`, `IMG_00000002.jpg`, ...) - exactly the kind of directory that grows large in
practice (see "Background" above) - every one of the 20,000 entries shared the same 3-character
prefix, collapsing the cache to a single bucket and giving zero benefit over no cache at all.
Rejected: the memory saving (48-62% smaller in that same test) is not worth trading away exactly the
selectivity the large, patterned directories this cache targets need most.

### Known limitations

- Per-directory memory is unbounded (see "Decision" above) - only the number of cached directories
  is capped, not the size of any one of them.
- Only mitigates the cost of a repeated lookup against an already-warm directory. The very first
  miss against a directory not yet cached still pays the full unindexed scan DESIGN-MOUNT-005
  describes - unlike the persisted-fold-column alternative there, which would also help a cold,
  already-large directory's first touch.
- Depends on DESIGN-METADATA-003's current one-connection-per-`Repository` model for its
  single-lock correctness argument; a future read-connection split needs to revisit this
  cache's own locking alongside it (see `metadata-storage.md`).
- `NAME_CACHE_CAPACITY` is a private, hardcoded constant - not exposed via `RepositorySettings` or
  the CLI, so there is no way to disable or resize it without a code change.

Revisit if: a workload's cold-path cost (the first, not-yet-cached touch of an already-large
directory - see "Known limitations" above) turns out to matter in practice, which would favor
building the persisted-fold-column alternative in DESIGN-MOUNT-005 alongside or instead of this
cache; or if per-directory memory actually needs its own bound (e.g. a total cached-entry budget, or
skipping caching entirely for a directory beyond some size) rather than being left unbounded; or if
`NAME_CACHE_CAPACITY` ever needs to be measured/tuned rather than left as the current feel-based
guess.

### Verification

`crates/db/examples/db_bench.rs`, run with the `cfg!(windows)` gate temporarily bypassed to simulate
the fallback path on non-Windows hardware: the pre-cache monotonic decline (428.0 -> 108.5 ops/s
shape, reproduced locally as 588.3 -> 146.0 ops/s) became, after this cache
(`HashMap`-keyed, pre-folded), essentially flat across all 5 runs (~12,000-13,000 ops/s, no
discernible decline) - consistent with a warm lookup no longer scaling with the parent's live child
count.

Confirmed afterward on the actual target platform, not only in simulation:
`performance/measurements/2026-09-02-julius-db-direct-mkdir-native-with-name-cache.md`, a real
`db-direct` `mkdir` run on `julius` (native Windows/WinFSP), directly A/B'd against the pre-cache
baseline (`2026-09-01-julius-db-direct-mkdir-native.md`, same tool/machine/power profile/IO device).
The baseline's monotonic decline (428.0 -> 108.5 ops/s over 19,602 siblings) is gone: this run stays
flat (3583.7-3696.3 ops/s, no trend) across 363,421 siblings - roughly 18.5x more than the baseline
ever reached - with a ~18.5x higher mean throughput (3633.8 vs. 196.0 ops/s) at that scale, and the
gap still widening for any directory large enough to have hit the baseline's worst rungs. This was
the one open question before treating this decision as settled - whether the simulated result held
on real Windows/WinFSP, not just on non-Windows hardware with the platform gate bypassed - and it
does, qualitatively exactly (decline eliminated) even though the absolute ops/s naturally differs
from the simulation (different hardware).

The cold-path cost flagged under "Known limitations" - the first, not-yet-cached touch of an
already-large directory - was also measured directly, calling `find_child_id_case_insensitive`
once against a freshly populated, not-yet-cached 10,000-entry directory (40-character names, the
same query-and-fold logic the cache falls back to on any miss): ~4.0-5.5 ms in a release build,
~20-23 ms in a debug build. A one-time, per-directory cost of that size is small enough, next to any
single filesystem operation's own latency, that the "Revisit if" trigger above continues to track an
actual usage-relevant regression rather than a cost this decision already knowingly carries.

Correctness verification is the white-box mutation-path audit described under "Decision" above, not
a separate section, since it is part of what this decision actually commits to.
