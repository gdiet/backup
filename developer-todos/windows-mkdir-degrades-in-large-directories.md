# Windows mkdir (and likely other tree lookups) degrade badly in large directories

**Noted**: 2026-09-01, while investigating an apparent mount-vs-db-direct throughput discrepancy in
the first `location: db-direct` performance measurement.
**Size**: medium - confirm with the developer before starting a fix. The root cause is clear, but
the right fix (index the fallback query, cap/short-circuit it, restructure it entirely) needs
actual design thought, and touches DESIGN-MOUNT-005's existing case-insensitive-lookup decision.
**Context**: `crates/db/src/tree.rs:83-134` (`find_child_id`/`find_child_id_case_insensitive`);
`docs/design/tree-namespace-case-sensitivity.md` (DESIGN-MOUNT-005, the design this fallback
implements); `performance/measurements/2026-09-01-julius-db-direct-mkdir-native.md` (the
measurement that surfaced this).

## What was found

The `2026-09-01` `db-direct` `mkdir` measurement showed a striking monotonic slowdown across its 5
runs (428.0 -> 108.5 ops/s as the number of siblings under the repository root grew from 0 to
~19,600). A follow-up scaled-down `dfs-mount` trial (2 runs x 3 s, not documented as a formal
measurement) showed *higher* throughput (770-814 ops/s) than the `db-direct` numbers at a similar
scale, which initially looked like a real mount-vs-db-direct discrepancy worth investigating.

It was not a discrepancy. An instrumented, throwaway re-run of the `db_bench` loop (chunked
throughput logging every 250 `mkdir`s, reverted afterward, not committed) confirmed both
measurements sit on the exact same curve - the `dfs-mount` trial simply sampled a much lower
sibling-count range (0-4,764) than `db_bench`'s later runs (11,893-19,602), and throughput at
0-2,500 siblings (763-2,704 ops/s) matches the mount numbers closely. `mkdir` goes through the
identical `db::Repository::mkdir` call either way (the mount handler in
`crates/cli/src/dedup_fs.rs` just adds one extra path-resolution step on top), so this was always
expected to be the same underlying cost, not two different code paths.

**Root cause**: on Windows (`cfg!(windows)`), `find_child_id` (`tree.rs:83`) falls back to
`find_child_id_case_insensitive` (`tree.rs:122-134`) whenever the fast, indexed exact-name lookup
finds nothing - which is true for essentially every `mkdir` call creating a name that does not
already exist, i.e. the common case. That fallback runs `SELECT id, name FROM tree_entries WHERE
parent_id = ?1 AND deleted_at IS NULL AND id != 0` with **no `LIMIT`**, materializes every live
child of the parent into a `Vec`, and scans it in Rust with a per-row case fold. No index backs
this - cost grows linearly with the parent's live child count, so creating `n` siblings in
sequence under one directory costs O(n^2) in total. This is Windows-only (case-insensitivity is not
attempted on other platforms - see the `cfg!(windows)` gate at `tree.rs:94`) and affects every
caller of `find_child_id`, not just `mkdir` - at minimum, file creation and lookup go through the
same function.

## How much this actually matters (checked against a real backup, not assumed)

An initial framing of this ("realistic backup trees have thousands of files per directory") was
wrong and got corrected against real data: a read-only query against the Scala repository database
at `c:\Dateien\Computer\git\bdev\dedup\meta\repository.sqlite3` (7,159,746 live tree entries, a
real, large, previously-run backup) gives the actual distribution of live children per directory:

| Children per directory | Directories | Share of directories | Files held | Share of files |
|---|---|---|---|---|
| 0-9 | 401,552 | 78.54% | 1,127,053 | 15.74% |
| 10-49 | 87,728 | 17.16% | 1,734,137 | 24.22% |
| 50-99 | 11,193 | 2.19% | 767,985 | 10.73% |
| 100-499 | 9,675 | 1.89% | 1,911,844 | 26.70% |
| 500-999 | 652 | 0.13% | 453,143 | 6.33% |
| 1000-4999 | 400 | 0.08% | 763,577 | 10.66% |
| 5000+ | 69 | 0.01% | 402,007 | 5.61% |

(Maximum: 6,146 live children in one directory.)

So: the overwhelming majority of real directories (78.5%) are small (under 10 entries) and
essentially unaffected - this matches ordinary intuition about a typical source/document tree (e.g.
`C:\Dateien` itself). But a small number of outlier directories (469 of 511,269, 0.09%) hold 1,000+
entries each, and together those account for ~16.3% of all files in this one real backup. The cost
is real, just concentrated rather than pervasive - it shows up specifically when many entries are
created in sequence inside one already-large directory (an initial backup of such a directory, or
ongoing growth of e.g. a log or downloads folder), not as a general tax on every `mkdir`.

## Suggested next step

Not yet decided - options worth weighing once picked up: add an index that makes the
case-insensitive fallback itself indexed (e.g. on a case-folded name column) rather than a full
scan; short-circuit/cap the fallback scan; or reconsider whether the fallback needs to run
unconditionally on every miss versus some cheaper pre-check. Whatever the fix, it should get a
`db-direct` `mkdir` measurement re-run afterward (large-sibling-count regime specifically) to
confirm the O(n^2) shape is actually gone, not just "looks faster."
