# `backup`: A Deduplicating Backup Application

`backup` stores your files in a repository with transparent content
deduplication: if you back up the same file contents multiple times - for
example because you back up "everything" into a dated directory every
week - the repository only stores those contents once. This means you can
keep many full backups over time without the storage growing anywhere near
as fast as a plain copy would.

This is a Rust rewrite of [DedupFS](../scala/README.md), the original Scala
implementation of the same idea. Where Scala deduplicates whole files (MD5
over the entire file), Rust deduplicates at the chunk level (content-defined
chunking, `blake3` hashes) - so two files that mostly overlap but differ in
a few places still share most of their storage, not just files that are
byte-for-byte identical. An existing Scala repository can be migrated into a
new Rust one; see [Migrate A Scala Repository](#migrate-a-scala-repository)
below.

Repository data lives in two places: a SQLite metadata database (the file/
directory tree and the dedup index) and a `data/` directory holding the
deduplicated byte content itself.

#### Table of contents

* [System Requirements](#system-requirements)
* [Basic Steps](#basic-steps)
* [Initialize A Repository](#initialize-a-repository)
* [Back Up Files And Directories](#back-up-files-and-directories)
* [Show Repository Statistics](#show-repository-statistics)
* [List Files](#list-files)
* [Find Files By Name Pattern](#find-files-by-name-pattern)
* [Check Integrity](#check-integrity)
* [Find And Fix Files With Missing Store Data](#find-and-fix-files-with-missing-store-data)
* [Restore Files](#restore-files)
* [Delete A File Or A Directory](#delete-a-file-or-a-directory)
* [List And Recover Deleted Entries](#list-and-recover-deleted-entries)
* [Database Backup, Restore, and Compaction](#database-backup-restore-and-compaction)
* [Reclaim Space](#reclaim-space)
* [Compact Store](#compact-store)
* [Mount](#mount)
* [Migrate A Scala Repository](#migrate-a-scala-repository)
* [For Developers](#for-developers)

## System Requirements

A single self-contained `backup` binary - no separate runtime needed.

Only [`mount`](#mount) has an extra runtime requirement, since it needs a
real filesystem-in-userspace driver:

- **Windows**: [WinFSP](https://github.com/winfsp/winfsp) installed (the
  runtime is enough).
- **Linux**: `libfuse3` and `fuse3` installed (Debian/Ubuntu: `apt install
  libfuse3-3 fuse3` - both packages, `libfuse3-3` alone is missing the
  `fusermount3` helper the mount actually needs; Fedora: already present
  on most systems as `fuse3`, which bundles both).

Every other command (`store`, `restore`, `list`, `find`, `stats`, `check`,
`del`, `db`, `reclaim-space`) works without either.

## Basic Steps

* Initialize a repository directory (once).
* Back up files and directories into it, as often as you like.
* Browse it with `list`/`find`/`stats`, or `mount` it read-only for
  ordinary tools to browse.
* Restore individual files/directories, or a full `mount --read-write` for
  ad-hoc editing, when needed.
* Periodically run `reclaim-space` to actually free the storage taken up by
  deleted files.

All commands take `-r <repo>`/`--repo <repo>` for the repository directory
(default `../backup-repository`). Paths given to the commands below are
repository-relative (`/`-separated), not filesystem paths, unless noted
otherwise.

## Initialize A Repository

```bash
backup --repo <repo-dir> init
```

Creates a new, empty repository at `<repo-dir>` (the directory itself may
already exist, but must not already contain a repository). Two options
control how content is deduplicated, fixed for the lifetime of the
repository:

- `--chunking <cdc|none>` (default `cdc`): `cdc` splits file content into
  variable-sized chunks using content-defined chunking, so partial matches
  between similar files are found, not just whole-file duplicates; `none`
  treats each whole file as a single chunk (cheaper to compute, but only
  catches exact duplicates).
- `--cdc-target-size-bits <n>` (default `20`, i.e. ~1 MB average chunk
  size): the average chunk size CDC targets, as `2^n` bytes. Smaller chunks
  find more overlap between similar files at the cost of more metadata
  overhead per byte stored; larger chunks are the reverse.

## Back Up Files And Directories

```bash
backup store [-p | -t] [-c <n>] [--store-io-parallelism <n>] [--chunk-buffer-mb <n>] [--temp <dir>] [--reference <path> [--force-reference]] <source...> <target>
```

Copies one or more source files/directories from the real file system into
the repository, deduplicating content as it goes. Each source keeps its own
name as a child of `target` - backing up `a/b` into target `t` produces
`t/b`. A source that's unreadable, or that disappears partway through
(a permission error, a subdirectory that isn't accessible), is logged as a
warning and skipped; the rest of the backup still runs to completion.

Target path handling:

- `-p`/`--create-dirs`: create every missing target path component.
- `-t`/`--target-exists`: require the full target path to already exist -
  fails up front otherwise, before touching any source.
- Neither flag: only the target path's *last* component may be missing
  (like plain `mkdir`, not `mkdir -p`); a missing intermediate component is
  an error.

Performance tuning (the defaults are reasonable for most cases):

- `-c`/`--concurrency <n>` (1-32): number of concurrent chunking threads.
  Default: one thread per CPU core.
- `--store-io-parallelism <n>` (1-32): caps how many of those threads may
  write into the repository's data store at once, independent of
  `--concurrency` - useful when the storage device's own optimal I/O
  concurrency doesn't match the CPU core count. Default: unlimited.
- `--chunk-buffer-mb <n>` (default `128`): RAM budget, shared across every
  concurrent worker, for buffering an in-progress chunk's bytes while its
  dedup status is resolved. A chunk that exceeds the budget spills to a
  temp file instead of failing. Refuses to start if this looks large
  enough, relative to currently available RAM, to risk pushing the machine
  into swapping - pass `--allow-swap-risk` to start anyway.
- `--temp <dir>`: directory for this run's chunk-buffer spillover (must
  already exist and be writable); defaults to the OS temp directory. For
  best throughput, point it at the fastest disk available, ideally not the
  same physical drive as either a source or the repository.

### Speeding Up A Backup With `--reference`

```bash
backup store --reference <path> [--force-reference] <source...> <target>
```

If a directory containing many and/or large files has been backed up
before and most files haven't changed since, `--reference <path>` can
**significantly accelerate** another backup of it: for each source file, if
a same-named file exists at the corresponding path under `<path>` (a
repository path - typically an earlier backup run's target) with the same
size and modified time, the new backup entry just reuses that file's
existing content - no reading, chunking, or hashing of the source file at
all. This is on top of (not a replacement for) the repository's normal
chunk-level dedup: even with perfect dedup, an unchanged file still costs a
full read+chunk+hash on every run just to *discover* that it dedupes -
`--reference` skips that discovery cost entirely for the common case
(nightly backup of a mostly-unchanged tree).

**Caveat**: if a file's contents changed but its size and modified time
happen not to have, the changed content is **not** detected and **not**
stored.

`<path>` resolves against the repository tree, one `/`-separated segment at
a time from the root; `*`/`?` wildcards in a segment are resolved against
that segment's directory children, picking the alphabetically last match -
e.g. `backup/????/????.??.??_*` resolves to the latest-named year, then
within it the latest-named run:

```bash
# First backup:
backup store src backup/2026.01.06
# Later backups, referencing the previous run:
backup store --reference "backup/????.??.??" src "backup/2026.01.13"
```

Before use, the resolved reference directory is checked against the given
sources for plausibility (do their top-level contents overlap enough to
plausibly be an earlier backup of them?) - a guard against a typo'd or
unrelated reference silently "working" (matching almost nothing, so every
file falls back to a full read/hash anyway). Pass `--force-reference` to
skip this check.

### Excluding Files / Directories From The Backup (`.backupignore`)

To exclude files or directories from a backup, put a file named
`.backupignore` into a source directory:

- An **empty** `.backupignore` excludes that whole directory (and
  everything under it) from the backup.
- A **non-empty** `.backupignore` is a list of ignore rules, one per line:
  - Lines are trimmed; empty lines and lines starting with `#` are ignored.
  - `*` is the "anything" wildcard, `?` is the "any single character"
    wildcard - nothing else is special (no `**`, no character classes, no
    escaping).
  - Rules for directories end with `/`, rules for files don't.
  - A rule applies to the directory it's found in, and - for a rule
    spanning more than one `/`-separated segment - propagates into
    matching subdirectories. For example, `log*/*.log` excludes `*.log`
    files inside any `log*`-named subdirectory, without excluding that
    directory itself or its other files.

`.backupignore` is not excluded automatically - like any other file, add a
rule for it yourself if you don't want it copied into the backup.

Example `.backupignore`:

```
# Do not store the .backupignore itself in the backup.
.backupignore
# Do not store the 'temp' subdirectory in the backup.
temp/
# In any subdirectories named 'log*', do not store
# files in the backup that are named '*.log'.
log*/*.log
```

## Show Repository Statistics

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

## List Files

```bash
backup list <path>
```

Lists a directory's direct children (not recursive), directories first, then
files, each alphabetically. Files are shown with their size and last
modified time (UTC); directories are shown with a `>` marker. If `path` names
a file instead, shows the same file-information block as `stats <path>`.

## Find Files By Name Pattern

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

## Check Integrity

```bash
backup check [path]
```

Verifies stored chunk data against the metadata database: for every chunk in
scope, reads its bytes back from the data store and checks its length and
content hash still match what's recorded, distinguishing missing/short data,
length mismatches, and hash mismatches. Also verifies (always for the whole
repository, regardless of `path`) that the reference counts on chunks and
contents match a live count of what actually references them - this should
never find anything, since it's already kept in sync automatically, so it's
a sanity check rather than an expected source of problems.

`path` is optional; omitted, it checks every chunk in the repository. Given a
file, checks just that file's chunks; given a directory, recurses through
everything under it. Exit code reflects the result: `0` if everything
checked out, non-zero if any problem was found - so this can be used in a
script/CI job. Since this reads every chunk's actual bytes back from disk,
an unscoped run against a large repository can take a while - a progress
line (bytes done/total, elapsed, ETA) is printed periodically so it's clear
the run is still going, not stuck.

## Find And Fix Files With Missing Store Data

```bash
backup problems [path]
```

Lists every active file affected by missing or short store data - the same
condition that makes `mount` return an I/O error reading that part of a
file (see [Mount](#mount)), just found in bulk instead of one read at a
time. `path` scopes the scan like `check` does (including the same
periodic progress line for a large, unscoped scan); a file whose broken
content is shared (via dedup) with a file outside `path` is still listed,
since it genuinely has the same problem. Wrong-but-present data (a hash or
length mismatch, `check`'s other problem categories) isn't in scope here -
that's corruption, not absence, and destroying it isn't this command's job.

```bash
backup fix-problems [path]
backup fix-problems [path] --replace-empty
```

Soft-deletes every file `problems` currently finds in scope. With
`--replace-empty`, also inserts a fresh 0-byte file at the same path,
keeping the original's last-modified time, so the tree still has something
there instead of a hole. Acts on whatever `problems` would currently find,
not on a cached result from an earlier run - there's no id to go stale in
between, unlike `undelete`.

## Restore Files

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

```bash
backup restore --deleted <id> <target>
backup restore --deleted <id> --recursive <target>
```

Restores a single *soft-deleted* entry (its `id`, as shown by `backup
deleted` below) straight to disk, without undeleting it in the repository
first - the entry stays deleted afterwards, this only copies its content
out. `<target>` is just the destination directory this time, not a list of
sources. For a deleted directory, `--recursive` also restores the
descendants that were deleted together with it (the same scope `backup
undelete --recursive` would reactivate - see below).

## Delete A File Or A Directory

```bash
backup del <path>
backup del --recursive <path>
```

Soft-deletes a file, or (with `--recursive`, required - refused otherwise,
even for an empty directory) a directory and everything under it, all with
the same timestamp in one atomic step. Soft-deleted entries stay recoverable
and keep referencing their content until a future `reclaim-space` run
actually purges entries older than its retention window.

## List And Recover Deleted Entries

```bash
backup deleted
backup deleted <path>
```

Lists every soft-deleted entry, or only those under `<path>`. Because the
same path can have been deleted (and re-created) more than once, each row is
its own independent entry, disambiguated by a numeric `id` and shown with
its deletion timestamp - sorted by path, most recently deleted first for a
given path:

```
[42] - Bilder/urlaub.jpg   2.3 MiB   deleted 2026-06-01 10:15:00
[41] > Bilder                        deleted 2026-05-30 09:00:00
```

```bash
backup undelete <id>
backup undelete <id> --to <path>
backup undelete <id> --recursive
```

Reactivates a deleted entry by the `id` shown above. Refuses (rather than
silently overwriting) if an active entry already occupies its original
location; pass `--to <path>` to reactivate it at a different location
instead. `--recursive` also reactivates descendants that were deleted at the
exact same timestamp - mirroring `del --recursive`'s cascade in reverse, so
it won't accidentally resurrect a descendant that was deleted separately,
before or after.

Prefer `backup restore --deleted <id>` above instead if the goal is just to
get the content back onto disk without touching the repository's own state.

## Database Backup, Restore, and Compaction

```bash
backup db backup
```

Creates a timestamped, fully self-contained, zip-compressed snapshot of the
metadata database under `meta/backups/` - safe to run against the live
database (it never blocks or is blocked by ongoing reads or writes, and
never touches the live file), unlike a plain file copy of a database that
might still be open elsewhere. Each run creates a new file; nothing is ever
overwritten. Compressed at a fast setting deliberately, not the smallest
possible: measured against a real ~760 MB database, the whole backup (dump
plus compression) took 30.8s and produced a 399 MB file (~47% smaller).

```bash
backup db restore <file>
```

Restores the metadata database from a backup, **overwriting the live
database**. `<file>` can be a path, or just a filename to look up under
`meta/backups/`. Doesn't require the current database to be openable first
- this is the recovery path for when it isn't. Accepts both the zipped
snapshots `db backup` produces and older, plain `.sqlite3` ones (from
before compression, or restored from elsewhere).

Prints a warning (but still proceeds) if the backup looks stale relative to
the data store's physical layout: `reclaim-space` and `compact-store` both
free up space that a later `store`/`mount --read-write` run may reuse for
different content, so a backup taken before one of those runs can - after
being restored - resolve some entries to the wrong physical bytes without
any other sign of a problem. Take a fresh backup before either of those
commands if you want to be able to restore back to exactly this point in
time.

```bash
backup db compact
```

Reclaims free pages left behind by past deletions (`del`/`reclaim-space`),
shrinking the database file in place, and checkpoints the write-ahead log
(folding pending writes into the main database file and removing the
`-wal`/`-shm` sidecar files next to it). Fast and safe to run any time - it
doesn't need extra disk space or a long exclusive lock, so there's no
reason to put it off.

### Read-Only Commands Need A Clean Database

`restore`, `stats`, `list`, `find`, `check`, `problems`, `deleted`, `db
backup`, and `mount` (without `--read-write`) never write to the metadata
database, and refuse up front rather than degrading silently if it isn't in
a state that guarantees a consistent read:

- **Pending migrations** - run any write command, or `db compact`, once to
  apply them.
- **A pending write-ahead log** (a non-empty `-wal` file next to
  `repository.sqlite3` - writes not yet folded into the main file) - run
  `db compact` to fold them in.
- **A schema newer than this build understands** - update `backup`.

This is also what makes it possible to bind-mount a repository directory
genuinely read-only (e.g. Docker's `-v /path:/repo:ro`, see
[`docker/samba-mount/`](docker/samba-mount/)): once `db compact` has run, a
read-only command never needs to write anything to `meta/` at all, not even
the `-wal`/`-shm` sidecars SQLite's WAL mode would otherwise create even
for a plain read.

### Usage Log

Every command run against a repository appends one line to
`meta/usage.log`: a timestamp, the command (e.g. `restore` or `db
backup`), and which optional flags were used (e.g. `overwrite,deleted`) -
never argument values (no paths, no ids). Purely local bookkeeping, never
transmitted anywhere, meant to make it possible to look back after a long
time and see which features actually got used - useful for deciding what's
safe to simplify or remove later. Best-effort: nothing ever fails or slows
down because of it.

### Inspecting Or Exporting The Metadata Database Directly

`meta/repository.sqlite3` is a plain SQLite database - no separate export
command is needed, the [SQLite command-line shell](https://www.sqlite.org/download.html)
(`sqlite3`) already covers both reading and exporting it directly:

```bash
sqlite3 <repo>/meta/repository.sqlite3 "SELECT COUNT(*) FROM tree_entries;"
```

```bash
sqlite3 <repo>/meta/repository.sqlite3 .dump > export.sql
```

`.dump` produces a portable SQL script (`CREATE TABLE`/`INSERT` statements)
of the whole database. Do this against a `backup db backup` snapshot rather
than the live database if you want a guaranteed-consistent point-in-time
export without holding a lock on the live one.

To reimport such a script, feed it into a **new, empty** database file
(`sqlite3 new.sqlite3 < export.sql`) - not one this application already
initialized, since the script's own `CREATE TABLE` statements would then
collide with the tables that already exist. The script doesn't carry the
schema version (`PRAGMA user_version`) this application tracks internally,
so a reimported database needs that set to match before this application
will open it again. It also doesn't carry `PRAGMA auto_vacuum` - this
application always tries to set `auto_vacuum = INCREMENTAL` on open, but
that request is silently ineffective on a database that already has
tables/data by the time it runs (SQLite can only enable `auto_vacuum` for
free on a table-less, empty database - anything already populated needs a
real `VACUUM` to actually take effect). A reimported database therefore
needs `sqlite3 new.sqlite3 "PRAGMA auto_vacuum = INCREMENTAL; VACUUM;"`
run once, or `backup db compact` (`PRAGMA incremental_vacuum`) will look
like it succeeds while silently doing nothing.

## Reclaim Space

```bash
backup reclaim-space [--keep-days N] [--no-backup]
```

Hard-deletes entries soft-deleted more than `N` days ago (default `0` -
everything currently soft-deleted), then removes any content that's now
unreferenced. This is the one genuinely irreversible operation (`del` alone
never is - a soft-deleted entry stays recoverable until this runs), so it
backs up the database first by default; pass `--no-backup` to skip that.

Does not shrink the on-disk data store file itself, but the byte ranges
freed by a purged chunk *are* tracked and reused: the next `store` run
reuses those gaps for new chunks' bytes before appending past the end, so
repeated delete/reclaim/store cycles don't make the store grow without
bound. `stats` reports any gaps not yet reused as free space.

## Compact Store

```bash
backup compact-store
```

Defragments the data store: relocates every live chunk so the store's used
address space becomes one contiguous block starting at 0, then shrinks the
backing files to match - turning `reclaim-space`'s reported free space (see
above) into space actually returned to the OS. Unrelated to `db compact`,
which only ever touches the metadata database file, not the byte store.

Run this only if you actually want the disk space back - unlike
`reclaim-space`, there's no ongoing cost to leaving gaps unpacked (`store`
already reuses them opportunistically), so this is a one-off, on-demand
operation, best run after `reclaim-space`, not before (compacting first
would relocate data that's about to become garbage anyway once
`reclaim-space` next runs).

Exclusive (refuses to run if another `compact-store` - or anything else
holding the repository's lock file - is already running against this
repository) and safe to interrupt at any point (`SIGINT`/`SIGKILL`/power
loss) and resume with another run.

**Invalidates older `db backup` snapshots** for restore purposes, the same
way `reclaim-space` followed by a `store` run already can (see "Database
Backup, Restore, and Compaction" above) - `db restore` warns automatically
when this has happened, but take a fresh backup first if you want to be
able to restore back to exactly this point in time.

## Mount

```bash
backup mount <mountpoint>
```

Mounts the repository's file tree read-only at `<mountpoint>`, so it can be
browsed and read with ordinary tools (`ls`, `cat`, `cp`, etc.) instead of
`list`/`restore`.

**Linux**: `<mountpoint>` must already exist and be empty. Blocks until
the filesystem is unmounted from another terminal (`fusermount3 -u
<mountpoint>` or `umount <mountpoint>`).

**Windows**: requires [WinFSP] to be installed (see
[System Requirements](#system-requirements) above). `<mountpoint>` must
*not* already exist - it's created as part of mounting. Blocks until the
process is stopped (Ctrl+C, closing the console, or killing it).

`<mountpoint>` can be either an unused drive letter or a path - WinFSP
supports both, and this tool passes whatever you give it straight through:

```bash
backup mount J:
backup mount C:\mnt\dedup
```

A drive letter shows up like any other drive (in Explorer, `dir`, etc.); a
path mount only shows up under that path (the parent directory must already
exist, same as the "must not already exist" rule above - only the final
component is created). Either way, the volume is labeled "DedupFS" (`vol
<drive>:` for a drive letter; a path mount's underlying volume carries the
same label too, visible via `GetVolumeInformation` even though `vol.exe`
itself only accepts a drive letter, not a path).

[WinFSP]: https://github.com/winfsp/winfsp - WinFsp - Windows File System
Proxy, Copyright (C) Bill Zissimopoulos, used here under its FLOSS
exception to the GPLv3 (see [NOTICE.md](NOTICE.md) for the full exception
text).

```bash
backup mount --read-write [--write-cache-mb <n>] [--temp <dir>] <mountpoint>
```

`--read-write` additionally allows structural changes
(`mkdir`/`rm`/`mv`/creating files) and real content writes - off by
default, since a mount is a much larger blast radius for a mistake than
`store`/`restore`. Writes are held in a per-file write cache (RAM up to
`--write-cache-mb`, default 128, then spilling to a temp file) and only
committed to the repository once the write handle closes. At startup,
refuses to run if `--write-cache-mb` looks large enough, relative to
currently available RAM, to risk pushing the machine into swapping - pass
`--allow-swap-risk` to start anyway. `--temp` overrides where the
write-cache spillover directory is created (must already exist and be
writable; defaults to the OS temp directory) - for best throughput, point
it at the fastest disk available, ideally not the same physical drive as
the repository.

### Recovering Deleted Files Through The Mount

Every directory in the mount shows a `[deleted]` entry whenever it has
something deleted directly in it (not shown at all otherwise - an empty
directory with no deletion history stays exactly that). Browse into it
like any other folder to see and read what was deleted there, including
descending into an already-deleted subdirectory. If the same name was
deleted more than once, each is disambiguated with its id, e.g. `photo.jpg
[42]` (matching `backup deleted`'s own id-based disambiguation) - plain
`photo.jpg` when there's only one.

To recover something (requires `--read-write`), drag it (or `mv`/rename
it) out of `[deleted]` to wherever you want it - this reactivates the
entry in the repository (via the same mechanism as `backup undelete`)
rather than copying it; a deleted directory always recovers together with
everything that was deleted alongside it at the same time, since a drag
gesture has no way to express anything narrower. If you'd rather just get
the bytes out without touching the repository's own state, use `backup
restore --deleted <id>` instead (see [Restore Files](#restore-files)) -
find the id by browsing `[deleted]` first, or via `backup deleted`.

If a directory already has a real, active subfolder literally named
`[deleted]`, that real folder is shown as-is - it always takes priority
over the synthetic view, which simply doesn't appear there.

**Known limitation**: once a directory has any deletion history, it can no
longer be removed through the mount (`rmdir`/"Delete folder"), even after
its real content is completely empty - Windows' own directory-emptiness
check runs before the mount ever gets a chance to only count real
entries, and it sees `[deleted]` as content too. Use `backup del
--recursive` from the command line instead if you need to remove a
directory like that.

```bash
backup mount --zero-fill-missing <mountpoint>
```

Missing or short store data normally makes the affected read fail with an
I/O error (see [Check Integrity](#check-integrity) and
[Find And Fix Files With Missing Store Data](#find-and-fix-files-with-missing-store-data)
for how to find and clean those up) - never silently zero-filled, so a
tool reading through the mount can always tell something's wrong rather
than copying out corrupted-looking-valid data. `--zero-fill-missing`
trades that guarantee away deliberately: reads overlapping missing data
return zero bytes for exactly the affected range instead of failing, for
when you already know a file is affected (e.g. via `backup problems`) and
still want best-effort access - a JPEG missing its last few KB may still
decode and display up to where the data stops, for instance. Off by
default; once it's on, there's no way to tell zero-filled bytes from real
ones, so only turn it on when you specifically want that trade-off.

## Migrate A Scala Repository

```bash
backup -r <scala-repo-root> migrate-scala-repo --script <export>
```

One-shot, in-place migration of an old Scala DedupFS repository into a Rust
one. `<scala-repo-root>` is the *existing* Scala repository directory
(already containing `data/` and `fsdb/`) - this adds a `meta/` directory
alongside them and adopts `data/` as its own, without copying or rewriting
a single byte: the byte store's on-disk layout is identical between Scala
and Rust, and every chunk this tool identifies already exists somewhere in
`data/` (Scala already stored it, just without chunk-level dedup), so
migration only ever *reads* bytes (to compute chunk boundaries and hashes)
and records metadata pointing at wherever they already are. No separate
`backup init` step is needed - refuses only if `<scala-repo-root>/meta`
already exists. `--script` is the H2 SQL script export produced by the
Scala tool's `fsc db-backup` command - either the zipped script as produced
directly, or an already-unzipped `.sql` file. `--cdc-target-size-bits`/
`--chunking` configure the new repository, same as `backup init`'s flags of
the same name (this tool does its own equivalent initialization).

Migrates the *entire* old tree, including soft-deleted history (not just
the currently-active files), so restore-from-history capability is
preserved. Every file's content is genuinely re-read, re-chunked, and
re-hashed through the same content-defined-chunking pipeline `store` uses
- expect this to take real time proportional to how much data is actually
present, not a quick metadata-only copy. The final summary reports how
much smaller the migrated repository's distinct content is than a naive
whole-file copy would have been, thanks to chunk-level dedup finding
sharing Scala's whole-file model never could - informational only, since no
bytes are ever copied to begin with.

A failure partway through removes the incomplete `meta` directory it
created before returning - recovery is simply re-running the command from
scratch, no manual cleanup needed (nothing under `data/` is ever touched,
so there's nothing to undo there).

## For Developers

Building from source, running tests, crate layout, internal architecture,
and performance benchmarks are covered in
[docs/development.md](docs/development.md) and
[docs/architecture.md](docs/architecture.md), not here.

## License

Licensed under either of [MIT](LICENSE-MIT) or
[Apache-2.0](LICENSE-APACHE), at your option. See [NOTICE.md](NOTICE.md)
for third-party notices (WinFSP).
