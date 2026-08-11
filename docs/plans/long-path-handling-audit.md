# Audit: long file/directory names and paths, across all commands, on Linux and Windows

**Status**: first round of empirical testing done (2026-08-11, Linux + a real Windows-backed
filesystem via WSL2's DrvFs `/mnt/c` - see "Findings" below); native WinFSP mount and raw SMB
protocol-level limits still untested (no WinFSP available in this environment). The one concrete
gap found (`mount --read-write`'s `mkdir`/`create`/`rename` accepting unbounded name lengths) is
**fixed** - see `MAX_ENTRY_NAME_BYTES`/`check_entry_name_length` in `cli/src/mount.rs` and
`Errno::ENAMETOOLONG` in `mountfs/src/lib.rs`. Everything else checked so far already behaved
reasonably without needing a code change (see "Findings").

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

## Findings (2026-08-11, empirical, Linux + real Windows-backed filesystem via WSL2 DrvFs)

All tests below used a throwaway repository, not `backup-repository/`. Every result is from
actually running the command, not reasoned about.

1. **Linux `NAME_MAX` boundary, `store`/`restore` round-trip**: a 255-byte (ext4's real limit)
   filename round-trips through `store` → `list` → `restore` with byte-identical content. A
   256-byte name can't even be *created* on ext4 in the first place (`File name too long`, before
   `backup` is ever involved) - confirms failure mode 1 ("source already too long for its own
   OS") is essentially unreachable via `store`, since `walkdir` can only ever see names the source
   filesystem itself already accepted.

2. **`mount --read-write`'s `create` has no length validation at all - the one real gap found.**
   Creating a file with a 256-byte name *through the FUSE mount* succeeded (exit 0) and the full
   256-byte name landed intact in `tree_entries` (`TEXT`, no length constraint - see above). This
   is the one command that can inject a name into the repository that no real source filesystem
   could ever have produced via `store`, since it's backed by SQLite, not a real directory entry.
   Confirmed the kernel/libfuse do **not** enforce `NAME_MAX` here on our virtual filesystem's
   behalf - `max_name_length` in `StatfsInfo` is advisory only, nothing rejects an actual create
   that exceeds it. Reading it back also isn't blocked: a fresh read-only `mount`'s `readdir` and
   `stat` on that same 256-byte entry both succeeded without any OS-level error - Linux's FUSE
   layer is fully permissive here, more so than a real disk filesystem would be.

3. **`restore` already handles a too-long *destination* name well - no fix needed.** Restoring
   that same 256-byte-named entry back to a real ext4 target produced a clear, specific warning
   (`warning: failed to create '.../bbb...': File name too long (os error 36)`), restored every
   other entry normally, and exited 0 with an accurate `restore completed with 1 warning(s)`
   summary - not a crash, not a silent drop, not a partial/inconsistent state.

4. **Deep paths (many components) round-trip cleanly through real Windows/NTFS, both
   directions - including well past the classic 260-character `MAX_PATH`.** Built a 20-level-deep
   source tree on Linux (508-character full path once restored under a `C:\Users\...` prefix),
   restored it onto real NTFS via WSL2's `/mnt/c` (DrvFs) - succeeded with no warning, content
   verified byte-identical. Then `store`d that same long-path tree back *from* `/mnt/c` as the
   source - also succeeded cleanly. This is genuinely useful, non-obvious data: at least through
   DrvFs, modern Windows/NTFS handles long paths transparently, without needing this project to do
   anything special (no `\\?\` prefixing attempted or apparently required). **Not yet tested**: a
   *native* WinFSP mount (no WinFSP available in this environment) or classic, non-long-path-aware
   Win32 client applications, which are a separate, real caveat even on a long-path-capable
   filesystem - a filesystem accepting a path doesn't guarantee every Windows application that
   might later open it does too.

### Conclusion so far

The practical risk is narrower than the original scope worried about: `store` can't produce an
unrestorable name (constrained by the real source filesystem), deep paths work fine at least via
DrvFs, and `restore` already degrades gracefully when it does hit a real OS rejection. The one
concrete, worth-fixing gap is **`mount --read-write`'s write-side dispatch
(`mkdir`/`create`/`rename` in `mountfs`/`cli/src/mount.rs`) accepting names with no length
validation at all** - the only path that can put an entry into a repository that's later
impossible to restore by name on *any* real filesystem. A reasonable fix: validate each new
name's length against a conservative, cross-platform-safe limit (e.g. 255 UTF-8 bytes, matching
Linux `NAME_MAX` and close to NTFS's 255-UTF-16-unit component limit) in those three dispatch
methods, returning `Errno` equivalent to `ENAMETOOLONG` instead of silently accepting it - not yet
implemented, a deliberate small follow-up rather than bundled into this audit.
