# Rust Components

This workspace contains Rust crates used by the backup application.

## Crates

| Crate   | Description                                                                          |
|---------|--------------------------------------------------------------------------------------|
| `cdc`   | Content-defined chunking library (rolling-fingerprint based)                         |
| `store` | Sequential on-disk byte store, file format compatible with the Scala `LongTermStore` |
| `cli`   | Deduplicating backup application; builds the `backup` binary                         |

## Usage

All commands take `-r <repo>`/`--repo <repo>` for the repository directory
(default `../backup-repository`). Paths given to the commands below are
repository-relative (`/`-separated), not filesystem paths, unless noted
otherwise.

### Show Repository Statistics

```bash
backup stats
```

Shows repository-wide counts (live/deleted files and directories, distinct
chunks and contents), physical storage size, logical size (the total size as
if nothing were deduplicated), the resulting dedup ratio, and how much of
the physical storage is currently unused gaps left by `reclaim-space` that
a later `store` run hasn't reused yet.

To see statistics for a specific file or directory instead:

```bash
backup stats <path>
```

For a directory, this recursively sums files, subdirectories, and total
logical size under it. For a file, it shows the file's size.

### List Files

```bash
backup list <path>
```

Lists a directory's direct children (not recursive), directories first, then
files, each alphabetically. Files are shown with their size and last
modified time (UTC); directories are shown with a `>` marker. If `path` names
a file instead, shows the same file-information block as `stats <path>`.

### Find Files By Name Pattern

```bash
backup find <pattern>
```

Searches the whole repository (not scoped to a subdirectory) for entries
whose full path contains `pattern`, case-insensitively. `*` matches any run
of characters (including none), `?` matches exactly one character - e.g.
`backup find "jre/bin/java"` matches any path containing that sequence of
path segments, and `backup find "*.log"` matches any path ending in `.log`.
Prints one matching path per line. Exit code follows the `grep` convention:
`0` if anything matched, `1` if the search ran fine but found nothing.

### Check Integrity

```bash
backup check [path]
```

Verifies stored chunk data against the metadata database: for every chunk in
scope, reads its bytes back from the data store and checks its length and
content hash still match what's recorded, distinguishing missing/short data,
length mismatches, and hash mismatches. Also verifies (always for the whole
repository, regardless of `path`) that `ref_count` on chunks and contents
matches a live count of what actually references them - this should never
find anything, since it's already kept in sync by database triggers, so it's
a sanity check rather than an expected source of problems.

`path` is optional; omitted, it checks every chunk in the repository. Given a
file, checks just that file's chunks; given a directory, recurses through
everything under it. Exit code reflects the result: `0` if everything
checked out, non-zero if any problem was found - so this can be used in a
script/CI job, unlike the Scala tool's `check`, which always exits `0`
regardless of what it finds.

### Restore Files

```bash
backup restore <source...> <target>
```

Restores one or more repository paths to a real directory on disk. Each
source keeps its own name as a child of `target` (backing up `a/b` into
target `t` produces `t/b`; restoring `t/b` back to a target `out` produces
`out/b`), the same convention `store` uses for how sources land under a
target. A directory source restores recursively; last modified times are
restored from what was recorded at backup time (permissions and ownership
are not - matching what `store` itself doesn't capture either).

By default, an existing file at a restore destination is left untouched and
reported as a warning rather than overwritten; pass `--overwrite` to replace
it. A per-file or per-directory problem (a permission error, an existing
file without `--overwrite`) is logged and only that entry is skipped - the
rest of the restore still runs to completion.

### Delete A File Or A Directory

```bash
backup del <path>
backup del --recursive <path>
```

Soft-deletes a file, or (with `--recursive`, required - refused otherwise,
even for an empty directory) a directory and everything under it, all with
the same timestamp in one atomic step. Soft-deleted entries stay recoverable
and keep referencing their content until a future `reclaim-space` run
actually purges entries older than its retention window - unlike the Scala
tool this replaces, which has no such grace period.

### Database Backup, Restore, and Compaction

```bash
backup db backup
```

Creates a timestamped, fully self-contained snapshot of the metadata
database under `meta/backups/` via SQLite's `VACUUM INTO` - safe to run
against the live database (it never blocks or is blocked by the single
writer or any readers, and never touches the live file), unlike a plain file
copy of a database that might still be open elsewhere. Each run creates a
new file; nothing is ever overwritten.

```bash
backup db restore <file>
```

Restores the metadata database from a backup, **overwriting the live
database**. `<file>` can be a path, or just a filename to look up under
`meta/backups/`. Doesn't require the current database to be openable first
- this is the recovery path for when it isn't.

```bash
backup db compact
```

Reclaims free pages left behind by past deletions (`del`/`reclaim-space`),
shrinking the database file in place, via `PRAGMA incremental_vacuum`. Cheap
compared to a full `VACUUM`: repositories are created with
`auto_vacuum = INCREMENTAL`, which already tracks freed pages as they
happen, so this doesn't need ~2x disk space or a long exclusive lock the way
a full rewrite would.

### Reclaim Space

```bash
backup reclaim-space [--keep-days N] [--no-backup]
```

Hard-deletes entries soft-deleted more than `N` days ago (default `0` -
everything currently soft-deleted), then removes any `contents`/`chunks`
rows that are now unreferenced. This is the one genuinely irreversible
operation in this tool (`del` alone never is - a soft-deleted entry stays
recoverable until this runs), so it backs up the database first by default;
pass `--no-backup` to skip that.

Does not shrink the on-disk chunk store file itself (it has no
delete/truncate operation), but the byte ranges freed by a purged chunk
*are* tracked and reused: the next `store` run's space allocator reuses
those gaps for new chunks' bytes before appending past the end, so
repeated delete/reclaim/store cycles don't make the store grow without
bound. `stats` reports any gaps not yet reused as free space.

### Mount

```bash
backup mount <mountpoint>
```

Mounts the repository's file tree read-only via FUSE at `<mountpoint>`
(Linux only), so it can be browsed and read with ordinary tools (`ls`,
`cat`, `cp`, etc.) instead of `list`/`restore`. `<mountpoint>` must already
exist and be empty. Blocks until the filesystem is unmounted from another
terminal (`fusermount -u <mountpoint>` or `umount <mountpoint>`).

Read-write support (writes held in-process, only committed to the
repository once the write handle closes) is designed but not yet
implemented - see `docs/plans/fuse-mount.md`.

## Development

### Build (debug)

```bash
# from rust/
cargo build
```

### Build (release / prod)

```bash
cargo build --release
```

### Run directly via cargo

```bash
# debug build, runs the cli crate's `backup` binary
cargo run -p cli -- <args>

# release build
cargo run --release -p cli -- <args>

# example
cargo run -p cli -- store --create-dirs source1 source2 target
```

### Tests

```bash
cargo test
```

### Linter (Clippy)

```bash
cargo clippy
```

For stricter checks (recommended before committing):

```bash
cargo clippy -- -D warnings
```

### Formatting

```bash
cargo fmt         # apply formatting
cargo fmt --check # check without modifying (e.g. in CI)
```

### All checks at once

```bash
cargo fmt --check && cargo clippy -- -D warnings && cargo test
```

## Performance

A simple single-threaded throughput benchmark runs the chunker on 7 MB of
pseudo-random data (seed 42) for 10 seconds, calling `next()` and `flush()`
per iteration — matching the Go benchmark exactly.

```bash
cargo run --release --example bench
```

Measured on Intel Core i7-1355U, 12 logical cores, 15 GB RAM, Ubuntu 24.04.4 LTS in WSL2 on Windows (single thread, `target_size_bits=20`, ~1 MB average chunk size):

| Implementation | Throughput |
|----------------|------------|
| Rust           | 2.78 GB/s  |
| Go             | 2.61 GB/s  |

Go benchmark for comparison:

```bash
go test -bench=BenchmarkCdc -benchtime=10s -count=1 ./internal/cdc
```
