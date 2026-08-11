# Audit: long file/directory names and paths, across all commands, on Linux and Windows

**Status**: not started - this is an investigation plan, not a design for a specific change. No
code has been written yet; nothing here is a confirmed bug, only a list of what needs to actually
be checked before we know whether one exists.

## Why this needs checking at all

Nothing in the current code validates or limits name/path length anywhere - confirmed by
inspection, not assumed:

- `tree_entries.name` is `TEXT NOT NULL` in the SQLite schema (`db/src/migrations.rs`) - no
  length constraint at the database layer.
- `cli/src/store.rs`'s directory-walking (`walkdir`) and path-resolution code
  (`resolve_or_create_target`, `db::resolve_path` in `db/src/query.rs`) split paths into
  components and look each one up/insert it - no length check anywhere in that path.
- Nothing greps for `NAME_MAX`, `MAX_PATH`, `PATH_MAX`, or any equivalent limit anywhere in
  `cli/` or `db/`.

That means today, a name/path that's too long for the *destination* OS is passed straight through
until whatever OS syscall it eventually hits (open/create/rename/mkdir - during `store`, during
`restore`, during a `mount`ed read/write) rejects it - with whatever raw error that syscall
happens to produce, not something this project has deliberately chosen or tested. Whether that's
actually a problem in practice (rare enough not to matter) or a real gap (confusing failures,
partial backups, or - worse - silently truncated/mismatched names) is exactly what this audit
needs to establish.

## Scope: every subcommand, both platforms

From `cli/src/main.rs`'s `Command` enum - the full list, nothing skipped:

| Command | Reads paths from | Writes paths to |
|---|---|---|
| `init` | - | - (no paths involved) |
| `store` | source filesystem (`walkdir`) | repository (`tree_entries.name`) |
| `restore` | repository | destination filesystem |
| `stats` | repository (optional path arg) | - |
| `list` | repository (path arg) | - |
| `find` | repository (name pattern) | - |
| `check` | repository | - |
| `problems` | repository | - |
| `fix-problems` | repository | repository (soft-delete, no new names) |
| `del` | repository (path arg) | repository (soft-delete) |
| `deleted` | repository | - |
| `undelete` | repository (path arg) | repository (reactivate) |
| `db backup`/`restore`/`compact` | - (whole-database ops) | - |
| `reclaim-space` | repository | repository (hard-delete) |
| `compact-store` | repository/store | repository/store (relocates chunks, not names) |
| `mount` (read-only) | repository, via FUSE/WinFSP | - |
| `mount --read-write` | repository, via FUSE/WinFSP | repository (`mkdir`/`create`/`rename`) |
| `migrate-scala-repo` | old Scala SQL export | repository |

The commands that actually *introduce* new names/paths into the repository - and are therefore
the ones that matter most here - are `store`, `mount --read-write` (`mkdir`/`create`/`rename`),
and `migrate-scala-repo`. Everything else either only *reads* existing (already-accepted) names,
or doesn't touch names at all. Still worth checking each read path too: a name that made it into
the database (e.g. written on Linux, long enough to be legal there) needs to behave sensibly when
`restore`d or `mount`ed on Windows, where the limit is tighter - that's a cross-platform case none
of the write-side commands alone would catch.

Run everything in this table against **both** a Linux (native or WSL2) and a Windows (native
mount via WinFSP, or the Samba re-export - see `docker/samba-mount/`) environment - the limits
below differ enough between the two that "checked on Linux" says nothing about Windows behavior
and vice versa.

## Concrete limits to check against

**Linux**:
- `NAME_MAX` = 255 **bytes** (not characters) per path component - matters for multi-byte UTF-8
  names, where 255 bytes can be well under 255 characters.
- `PATH_MAX` = 4096 total, but not uniformly enforced by every syscall - some operations fail
  earlier or later than others; needs checking per code path, not assumed from the constant alone.

**Windows**:
- Classic `MAX_PATH` = 260 characters, without a `\\?\`-prefixed path - relevant for any client
  application (Explorer, `cmd.exe`, plenty of older software) even on Windows versions where the
  registry-gated "long paths" opt-out limit no longer applies at the API level.
  With `\\?\` prefix (extended-length paths), the limit rises to roughly 32,767 characters -
  relevant to whether `mountfs`'s WinFSP backend (`mountfs/src/windows/`) surfaces paths in a way
  that lets clients opt into that longer limit, or caps them at the classic 260 regardless.
- NTFS per-component limit: 255 UTF-16 code units.

**FUSE/WinFSP layer itself** (`mountfs/`): both backends currently pass names/paths through
largely unvalidated (confirmed by reading `mountfs/src/linux/mod.rs` /`windows/mod.rs`'s dispatch
functions - no length check in either). Whether that's fine (the underlying OS/filesystem call
will reject it appropriately) or produces a worse failure mode (e.g. silent truncation somewhere
in a C string conversion) needs checking directly, not assumed either way.

## Two distinct failure modes to test separately

1. **Storing a source that's already too long for its own OS's limits.** Shouldn't normally
   happen (the source OS presumably already enforced its own limit when the file was created),
   but `store` reading from a network share or an already-migrated/copied tree from a
   more-permissive OS is a real scenario worth at least one test case.
2. **Restoring/mounting onto an OS with *tighter* limits than the one a name was originally
   stored under.** The more realistic risk: back up on Linux (255-byte components, deep trees
   under 4096 bytes total are easy to construct), then `restore` or `mount` on Windows, where a
   perfectly legal Linux path can exceed `MAX_PATH`. This is the case most likely to produce a
   confusing, late failure (mid-restore, after other files already succeeded) rather than a clean
   upfront rejection.

## What "checked" should mean for each cell

For each command × platform × failure-mode combination: construct a deliberately too-long
name/path, run the command, and record (not guess) what actually happens - a clean, actionable
error; a raw OS error message; a silent truncation; a partial/inconsistent result (e.g. `store`
succeeding for some files and failing for others without a clear summary); or a crash. Only once
that's tabulated does it make sense to decide whether anything needs fixing, and if so, where
(upfront validation in `store`/`mount`'s write paths, vs. just improving the error message at the
point of syscall failure, vs. leaving it as "not supported, OS error surfaces as-is" if that's an
acceptable, documented limitation).
