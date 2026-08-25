# Tree Namespace Case-Sensitivity

## DESIGN-MOUNT-005: Case-sensitive storage, with a Windows-only case-insensitive lookup fallback

Status: decided

`tree_entries` name comparison (REQ-MOUNT-010 in
[`../../requirements/functional/mount.md`](../../requirements/functional/mount.md)) stays
case-sensitive at the storage level on every platform - no schema change, no `COLLATE` on the
column or its unique index, identical behavior to what the schema already does today. A
Windows-specific fallback sits on top of this, entirely in application code, not in the schema:
every place `crates/db/src/tree.rs` needs to answer "does this name already exist under this
parent" (`find_child_id`, used by lookup, by `mkdir`/`create`'s collision pre-check, and by
`rename`'s target-existence check) tries an exact, case-sensitive match first - the existing
indexed path, unchanged - and only on a miss, under a Windows build, fetches the parent's active
children and compares them case-insensitively in Rust. If more than one still matches, the
highest-`id` (most recently created) entry wins.

A rename whose fallback match resolves to the entry being renamed itself (not a different one) is
not a collision: the operation succeeds and updates the stored spelling in place, matching e.g.
renaming `install.txt` to `Install.txt` in Explorer on real NTFS. Implementing this needs an
explicit identity check (compare the fallback match's `id` against the source entry's `id`), not
just "was a match found" - conflating the two would make this case, and REQ-MOUNT-009's plain
same-path no-op case, indistinguishable from a real collision.

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
for a differently-collated comparison, so a `COLLATE`-qualified query pays the same full-scan cost
as the chosen Rust-side fallback - with no performance advantage to justify the extra
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

- The case fold itself (once implemented) uses Rust's `str::to_uppercase()`/`to_lowercase()` - full
  Unicode case mapping, locale-independent (deterministic regardless of the running system's
  locale, which is a wanted property, not just a side effect). It is not guaranteed to match NTFS's
  own internal per-codepoint upcase table exactly in every corner case - full case mapping can
  change a string's length (e.g. German `ß` → `SS`), which a simpler per-character table may not
  do the same way. Spot-check the known Unicode `SpecialCasing` exceptions (`ß`, Turkish `İ`/`ı`,
  Greek final sigma) against real WinFSP (`julius-winfsp-ssh` skill) once implemented, rather than
  trusting the Unicode data alone.
- Unicode normalization (NFC vs. NFD - e.g. `é` as one codepoint versus `e` plus a combining
  accent) is a separate, unrelated concern this decision does not address.

Revisit if: a directory's live entry count grows large enough that the Rust-side scan fallback
becomes measurably slow on a Windows build - see the persisted-fold-column alternative above,
and `developer-todos/performance-baselines-tree-and-content-operations.md` for the broader,
currently unanswered question of what "large enough to matter" actually is on real hardware.

## Not yet implemented

- The Windows-gated fallback-plus-tiebreak in `find_child_id` (`crates/db/src/tree.rs`).
- The self-rename-by-identity special case in `rename` (compare the fallback match's `id` against
  the entry being renamed, not just whether a match was found).
- Regression tests: ASCII fallback lookup and tiebreak; the `install.txt` → `Install.txt`
  self-rename case; `mkdir`/`create` refusing a case-only collision under a Windows build; the
  known Unicode-fold limitation above, spot-checked against real WinFSP.
- `crates/db/src/tree.rs`'s module doc comment, which currently describes case-sensitivity as an
  open, undecided question rather than this decision.
