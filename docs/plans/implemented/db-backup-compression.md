# Compress `backup db backup` snapshots

**Status**: implemented.

## Measured, not estimated

Zipped the most recent real `backup db backup` snapshot
(`dedup/meta/backups/repository_20260809_102331.sqlite3`, produced via
`VACUUM INTO`) with Windows' built-in `Compress-Archive -CompressionLevel
Optimal` (plain Deflate, comparable to what the `zip` crate would produce):

| | |
|---|---|
| Original | 759,635,968 bytes (760 MB) |
| Zipped | 279,094,436 bytes (279 MB) |
| Ratio | 36.7% of original (63.3% smaller) |
| Time | 104.0s |

Better than the "less than 50%" guess that prompted this - a SQLite file's
B-tree page overhead, repeated schema text, and (post `incremental_vacuum`
churn) partially-empty pages all compress well. The real cost is time: 104s
to compress a 760 MB database, on top of the ~36s `VACUUM INTO` already
takes for a database this size - compression would roughly triple-to-
quadruple total `db backup` wall time for a repository this size, and worse
for a larger one (the real, non-truncated source repository is presumably
larger still).

Also measured the fastest compression level against the same file, since a
much cheaper level that keeps most of the ratio would sidestep the
default/opt-in question below entirely:

| | Optimal | Fastest |
|---|---|---|
| Zipped | 279,094,436 bytes (36.7%) | 299,288,814 bytes (39.4%) |
| Time | 104.0s | 41.4s |

Fastest gives up only ~3 percentage points of ratio for less than half the
time - a clearly better default than Optimal. Whatever the `zip` crate's
equivalent fastest Deflate setting measures at should be re-checked before
finalizing (this benchmark used .NET's `Compress-Archive`, not the `zip`
crate itself, as a stand-in), but the shape of the tradeoff is unlikely to
flip.

## No new dependency needed

The `zip` crate (`version = "8.6.0", features = ["deflate"]`) is already a
direct `cli` dependency, used today by `migrate_scala_repo` to *read* the
old Scala export zip. Both writing (`db backup`) and reading (`db
restore`) sides can reuse it directly - no new crate to vet or add.

## Design

### Writing (`backup db backup`)

`VACUUM INTO` can only write a plain SQLite file - there's no way to have
SQLite stream its output directly into a zip entry. So the flow becomes:

1. `VACUUM INTO` a temporary path (e.g. the final filename with a
   `.tmp` suffix, in the same `meta/backups/` directory - same-volume
   rename avoidance, consistent with how other temp-then-finalize writes
   in this codebase already work).
2. Stream that temp file into a new `zip::ZipWriter` at the real target
   path (`repository_<timestamp>.sqlite3.zip` or similar - naming TBD, see
   open question below), using `SimpleFileOptions` with deflate
   compression (mirroring `migrate_scala_repo`'s existing writer usage).
3. Delete the uncompressed temp file.
4. On any failure partway, clean up both the temp file and any partial
   zip - same "leave nothing half-finished" standard `migrate-scala-repo`
   and other commands in this codebase already hold themselves to.

This briefly needs both the uncompressed and compressed copies on disk at
once (temp file exists until step 3) - worth calling out since for a very
large database that's a real, if transient, extra disk-space requirement
on top of the compression time cost above.

### Reading (`backup db restore`)

Detect a `.zip` extension (vs. today's bare `.sqlite3`) and extract to a
temp location first, then proceed with today's restore logic unchanged.
Both old, already-existing uncompressed backups (like the ones already on
disk today) and new zipped ones need to keep working - this is purely
additive, not a format migration.

### Should compression be the default, opt-out, or opt-in?

Given the real time cost measured above, this needs an explicit decision,
not just "of course, compress by default":

- Default on, `--no-compress` to skip (mirrors `reclaim-space`'s existing
  `--no-backup` flag naming/philosophy) - biases toward the space saving,
  costs time by default.
- Default off, `--compress` to opt in - biases toward speed, matches
  today's behavior unless asked.
- A faster compression level as the default trade-off (Deflate's lower
  levels are much faster for a smaller ratio) instead of exposing a flag
  at all - untested here (only "Optimal" was measured); worth benchmarking
  a fast level (e.g. level 1) against this same real file before deciding,
  since a meaningfully-faster-but-still-good-ratio level could sidestep
  needing a flag entirely.

Given the measurement above, leaning toward: compress by default using the
fastest Deflate level, no flag needed at all - 41s added to a 36s backup is
noticeable but not painful, and skips the whole default/opt-in design
question. `--no-compress` could still be added later if that turns out to
matter in practice, but isn't obviously needed up front.

## Verification checklist

- [x] Confirmed the `zip` crate's fastest Deflate setting (level 1) for
  real, against the real `dedup/` repository's database, release build:
  **30.8s total** (`VACUUM INTO` + zip together - actually *faster* than
  the ~36s `VACUUM INTO` alone used to take before compression existed,
  most likely just run-to-run variance rather than compression being
  free), producing a **399 MB** zip from the 760 MB original (52.6% of
  original, ~47% smaller) - a somewhat worse ratio than the
  `Compress-Archive -Fastest` stand-in measured (39.4%), but the "fast,
  no flag needed" conclusion holds regardless.
- [x] `cargo fmt --check && cargo clippy --workspace --all-targets -- -D
  warnings && cargo test --workspace && cargo doc --no-deps --workspace`.
- [x] Round-trip tests in `cli/src/db_maintenance.rs`: backup-then-restore
  (now implicitly through a zip), plus a dedicated test confirming an old,
  plain (pre-compression) `.sqlite3` backup still restores correctly.
- [x] Updated `README.md`'s `## Database Backup, Restore, and Compaction`
  section.
