# DedupFS Manual

<!-- release-version-line -->
**Version:** development version. This manual describes the `dfs` built from the same commit.

`dfs --help` and `dfs <command> --help` list every command and option. This manual adds the
workflows that connect them.

## Getting started

The examples use `repo` as the repository path. Most commands take it as `--repository <path>`.
`create-repo` and `unlock` take it as an argument instead. Where no path is given, the repository is
the `dedupfs-repository` directory next to the `dfs` executable.

### Create a repository

```
dfs create-repo repo
```

The chunking granularity is fixed when the repository is created. `--cdc-target-size-bits` sets it
(6 to 23, default 20). It cannot be changed later.

### Import files

```
dfs ingest --repository repo ~/documents '+backups/[yyyy-MM-dd]'
```

The last argument is the target path inside the repository. Every segment must already exist,
unless it is prefixed with `+` (create on demand) or `!` (must be created freshly). A segment may
contain date and time placeholders in square brackets. The example creates `/backups/<today>` and
places `documents` below it.

Importing the same content again costs almost no additional space. `--reference` points at an
earlier ingest of the same sources. Files with the same name, size and modification time are then
linked to the existing content without being read again.

### Browse without mounting

```
dfs list --repository repo /backups
dfs find --repository repo 'report*'
dfs stats --repository repo
```

`find` matches names case-insensitively. `*` matches any run of characters, `?` exactly one.
`stats` reports the logical size, the physical size, and the deduplication ratio, for example `2.70x
(63.0 % saved)`. A ratio is never below 1.00x. The physical size counts the chunks that live files
use. It does not include content that only soft-deleted entries still hold, or gaps in `data/` that
were freed but not yet reused.

Without a path, `stats` reports on the whole repository. It then also gives the repository age, the
chunking target size, the number of stored chunks and chunk extents, the end of the stored data and
the unused space below it, the size of the metadata database, and the number of soft-deleted
entries. With a path, it reports only on that directory's subtree. Content that the subtree shares
with files outside of it counts as physical storage of the subtree as well, so the ratio of a path
describes the path alone.

### Mount the repository

```
dfs mount --repository repo /mnt/dedup
```

The mount is read-only by default. `--read-write` allows changes through the mount. On Linux, the
mount point must already exist, and the mount ends with Ctrl+C or `fusermount3 -u /mnt/dedup`. On
Windows, the mount point does not need to exist.

Mounting needs libfuse3 on Linux and WinFSP on Windows. All other commands need neither.

### Restore files

```
dfs restore --repository repo /backups/2026-10-05/documents ~/restored
```

The last argument is a directory on disk, which must already exist. A file that already exists at
the destination is not touched unless `--overwrite` is given. `--verify` checks every restored file
against its recorded hash.

### Delete and recover

```
dfs del --repository repo /backups/2026-10-05/documents/old.txt
```

A delete is soft. The entry stays recoverable, and the command prints where. Soft-deleted entries
appear below `[show-deleted]` at the repository root and in a `[deleted]` directory next to where
they were deleted. Through a read-write mount, an entry is recovered by moving it out of there.

### Reclaim space

```
dfs reclaim --repository repo --min-age-days 30
```

Space is freed only when soft-deleted entries are purged. `reclaim` purges every entry that has
been soft-deleted for at least `--min-age-days`. The default is 0, which purges **every**
soft-deleted entry immediately. A purged entry cannot be recovered.

## Repository maintenance

- `dfs db-backup <target-directory>` writes a timestamped backup of the repository metadata.
- `dfs db-restore <backup-file>` replaces the metadata with such a backup.
- `dfs db-compact` shrinks the metadata store after many deletions. Take a new backup afterwards.
- `dfs unlock [path]` clears a write lock that a crashed process left behind. It never removes a
  lock that is still held.

## Potentially dangerous options

Each of these options can make a command do something that is hard or impossible to undo. A command
prints a one-line warning at start when one of them is given. `--help` lists every option.

### `--best-effort` (`dfs mount`, `dfs restore`)

Stored data can be missing, for example after a data file was lost, or unreadable, for example
because of a storage error. By default, reading content that touches such data fails with an error.
With `--best-effort`, the affected part is replaced by zero-value bytes (0x00), and the read
succeeds. This helps when a file is mostly intact and the
remaining data is wanted, for example an image.

- `dfs mount --best-effort` applies to every read through the mount. With `--read-write`, it also
  applies when a modified file is saved. The saved content then contains zero-value bytes in place
  of the missing data. It is complete and valid in the repository, but it no longer matches the
  original, and restoring the missing data later does not repair it.
- `dfs restore --best-effort` writes zero-value bytes for missing data. It also keeps content that
  fails `--verify` instead of stopping at it.
- A storage error is treated like missing data. A short or unstable connection to the storage can
  therefore also produce zero-value bytes. With `--read-write`, these can be saved permanently.
- The command warns once for each data file that turns out to be missing or short, and once for each
  kind of read error. A mount prints a summary when it ends.

