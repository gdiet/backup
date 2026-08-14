# Audit: long file/directory names and paths, across all commands, on Linux and Windows

**Status**: closed (2026-08-14). Empirically tested on Linux (2026-08-11) and, since, on a real
WinFSP-installed Windows machine ("julius", 2026-08-12 through 2026-08-14 - first remote via SSH,
then natively, see "Findings" below and "Windows findings" further down). The one concrete gap
found (`mount --read-write`'s `mkdir`/`create`/`rename` accepting unbounded *single-component* name
lengths) is **fixed and confirmed working via real WinFSP**, not just Linux/FUSE. As of 2026-08-12
the fix and its tests live in `mountfs` itself, not `cli` - see "Moved to `mountfs`" below for why
and where exactly. `restore` also now has its own regression test for the complementary case (a
too-long name already *in* the repository, restored to a real filesystem that rejects it) - see
"cli-level tests" below. Everything else checked so far already behaved reasonably without needing
a code change. Native Windows SMB re-export of a WinFSP mount was checked (2026-08-12, see
"Windows findings") and showed no length behavior beyond what direct WinFSP access already does.
The SMB *wire protocol's* own limits, independent of any Win32 client, were closed on 2026-08-13: a
real `smbclient` (Samba, run from a Docker container on the Linux/WSL2 side, network-reachable to
julius once SSH connectivity to it was set up) against a real WinFSP-mounted, SMB-shared
repository on julius confirmed the identical 255/256-byte boundary, with no gap. Finally, the one
item this doc had explicitly flagged as "not yet tested" since its very first (2026-08-11) revision
- **long total paths (deep nesting) via a real native WinFSP mount**, as opposed to single
long component names, or deep paths only exercised via `restore`/`store` against WSL2's DrvFs - was
closed on 2026-08-14; see "Deep-path findings" below.

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

**FUSE/WinFSP layer itself** (`mountfs/`): at the time this audit started, both backends passed
names/paths through completely unvalidated (confirmed by reading `mountfs/src/linux/mod.rs`/
`windows/mod.rs`'s dispatch functions - no length check in either). That's since been fixed - see
"Moved to `mountfs`" further down for where the check landed and why.

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
concrete, worth-fixing gap was **`mount --read-write`'s write-side dispatch
(`mkdir`/`create`/`rename` in `mountfs`/`cli/src/mount.rs`) accepting names with no length
validation at all** - the only path that can put an entry into a repository that's later
impossible to restore by name on *any* real filesystem. Fixed: each new name's length is now
checked against 255 UTF-8 bytes (matching Linux `NAME_MAX`, close to NTFS's 255-UTF-16-unit
component limit) in those three dispatch methods, returning `Errno::ENAMETOOLONG` instead of
silently accepting it - see "Windows findings" below for confirmation this also works correctly
through real WinFSP, not just Linux/FUSE.

## Windows findings (2026-08-12, empirical, real WinFSP on "julius")

Access via SSH to a real Windows machine with WinFSP installed (see the `julius-winfsp-ssh` skill
for how that connection is reached - link-local IPv6 from `ping` doesn't work from WSL2, needed an
IPv4 LAN address instead). Native `cargo build`/`test` there
turned out fast enough (dependencies
already cached from a prior checkout - a from-scratch single-crate rebuild was ~6s, a full
`cargo test --workspace` including recompiling `db`/`mountfs`/`cli` was ~47s) that cross-compiling
on Linux and copying binaries over wasn't necessary - just working natively there over SSH was
simpler.

- **Real, correction to an earlier claim in this doc**: `cli/src/mount.rs`'s own `mod tests`
  (where `write_ops_reject_a_name_longer_than_max_entry_name_bytes`, the unit test for this fix,
  lives) is `#[cfg(all(test, not(target_os = "windows")))]` - Linux-only. It never ran on Windows
  at all, contrary to what an earlier revision of this doc implied by citing a passing
  `cargo test --workspace` run on Windows as if it covered this. The real Windows-side
  verification needed its own test in `cli/tests/windows_mount.rs` (the existing pattern for
  Windows mount tests - subprocess-based, spawns the real `backup.exe` and drives it through real
  WinFSP, since in-process WinFSP mounting isn't supported the way libfuse's in-thread mounting on
  Linux is). Added:
  `mount_read_write_rejects_a_name_over_max_entry_name_bytes_via_the_backup_binary` - confirms via
  a real `backup mount --read-write` child process and real WinFSP: a 255-byte name is accepted,
  a 256-byte name is rejected. Passes.
- **`mounts_and_serves_the_full_read_only_op_set_via_real_winfsp`** (the existing `mountfs`-level
  WinFSP integration test) and the full `cli/tests/windows_mount.rs` suite (9 tests, including
  real read-write mounts, content writes, structural changes, and now the name-length check) all
  pass natively on real WinFSP. `cargo fmt --check`/`cargo clippy --all-targets -- -D warnings`
  both clean on Windows too.
- **Isolating the exact 255-vs-256-byte boundary via a real, interactive Win32 client tool (not
  just Rust's own `std::fs`) turned out impractical, and that's itself worth recording**: any
  near-maximal single *component* name, even under the shortest realistic mount path, already
  exceeds classic Windows' ~260-character `MAX_PATH` as a *whole path* - the two limits are close
  enough in magnitude that classic (non-long-path-aware) Windows APIs essentially always hit
  `MAX_PATH` first, regardless of how short the surrounding path is. The usual workaround
  (`\\?\`-prefixed "verbatim" paths, which opt into the much larger ~32,767-character limit)
  didn't cleanly address a WinFSP-created directory mountpoint in this setup either - both a
  255-byte and a 256-byte name failed identically with "the filename, directory name, or volume
  label syntax is incorrect" via `\\?\`, on the same connection/session as the mount itself (so
  not a session-scoping artifact - that was separately confirmed to be real for both drive-letter
  and plain-directory WinFSP mounts accessed from a *different* SSH session/logon than the one
  that created them, an unrelated finding also worth noting for future remote Windows testing).
  Whether that `\\?\` failure is a general WinFSP limitation or specific to this exact
  setup/WinFSP version wasn't chased further (diminishing returns). Rust's own `std::fs` sidestepped
  the whole issue by automatically using verbatim paths internally when needed - which is exactly
  why the new automated test above uses `std::fs` rather than trying to replicate a literal Win32
  client call.
- **Follow-up (2026-08-12, native session on julius): the `\\?\` workaround does work correctly
  against a WinFSP directory mount** - refines the point directly above, which reported *both* the
  255- and 256-byte cases failing identically. Retested via `New-Item` (PowerShell/.NET, a
  different Win32-API client than whatever produced the earlier result): a 255-byte name via
  `\\?\C:\...\mnt\<name>` **succeeds**, full name confirmed intact via a directory listing (no
  truncation); a 256-byte name **fails**, but with a different, more specific error ("the filename,
  directory name, or volume label syntax is incorrect") than this project's own `ENAMETOOLONG` -
  i.e. Windows' own NTFS-facing layer rejects it before the request ever reaches WinFSP's user-mode
  callback into this project's code, meaning a normal Win32 client can never actually exercise this
  project's own 255-byte check via `\\?\` - Windows already enforces the identical boundary one
  layer down. The cause of the earlier session's differing 255-byte result (same mount type, same
  general approach) wasn't identified - possibly a different mount path length, or a literal Win32
  `CreateFileW` call behaving differently than `New-Item`'s underlying `.NET` path resolution -
  low-value to chase further given the boundary itself is now confirmed correct.
- **Native Windows SMB re-export of a WinFSP mount was tried (2026-08-12), with one genuine new
  finding**: a WinFSP directory mountpoint **cannot be the share root itself** - both
  `New-SmbShare` and classic `net share` fail immediately with Windows System Error 32
  (`ERROR_SHARING_VIOLATION`), confirmed with two independent tools, before any SMB client ever
  connects. A plain (non-WinFSP) directory shares without issue for comparison, so this is specific
  to WinFSP's mountpoint, not a general share-creation problem in this environment. **Workaround
  that works**: share the mount's *parent* directory instead (an ordinary folder) - the mount then
  shows up and is fully browsable/writable as a subdirectory of that share
  (`\\host\share\mountdir\...`), reads/writes/`readdir` all confirmed working, including this
  project's own `[deleted]` soft-delete view. Once reached that way, the 255/256-byte boundary and
  the `\\?\`-over-SMB (`\\?\UNC\host\share\mountdir\<name>`) behavior were both byte-for-byte
  identical to direct local access (same errors, same successes) - no evidence the SMB layer itself
  changes any length behavior, at least as exercised through Windows' own SMB client stack. This
  doesn't rule out the SMB *wire protocol* having its own independent limits, since every client
  used here (PowerShell/.NET, `net share`/`net use`) still goes through the same Win32 file API and
  its `MAX_PATH` pre-checks that already dominated the local-only testing above - a client that
  talks SMB directly without that Win32 layer (e.g. Samba's `smbclient`) would be needed to fully
  rule that out, and wasn't available in this environment (no Docker on julius for
  `docker/samba-mount`; the local WSL2 Debian distro has no `smbclient` installed and no
  passwordless `sudo` to install it non-interactively).
- **Follow-up (2026-08-13), the SMB wire-protocol gap above closed**: once SSH connectivity from
  the Linux/WSL2 side to julius was set up (unrelated work, see `AGENTS.md`), the environment split
  that blocked this stopped applying - Docker (available on the Linux/WSL2 side) can reach julius's
  network directly, so a real `smbclient` never needed to run on julius itself. Concretely: a
  disposable local Windows account (`smbtest`, deleted afterward) was created on julius via `net
  user`, a repository was initialized and mounted read-write via WinFSP at `C:\Temp\smb-test\mnt`,
  and its *parent* `C:\Temp\smb-test` (not the mountpoint itself - see the workaround above) was
  shared via `net share`, granted to `smbtest`. From the Linux/WSL2 side, a `debian:bookworm-slim`
  Docker container with `smbclient` installed connected to `\\<julius-ip>\smbtest-share\mnt` and
  attempted creating both a 255-byte and a 256-byte directory name directly over the wire protocol,
  no Win32 API involved at any point:
  - 255 bytes: **succeeds**, confirmed via a subsequent `ls`.
  - 256 bytes: **fails**, with `NT_STATUS_OBJECT_NAME_INVALID` - a real SMB-level rejection, not a
    client-side pre-check (there is no Win32 `MAX_PATH` layer in `smbclient` to have done that).

  Same boundary, same outcome, as every other client tried against this project's own
  `MAX_NAME_BYTES = 255` check - no gap. This closes the "remain untested" state noted at the top
  of this document; the whole audit is now fully empirically checked, Linux and Windows, Win32 and
  raw SMB wire protocol alike. Test account, share, mount, and repository were all torn down
  afterward - nothing left behind on julius.

## Moved to `mountfs` (2026-08-12)

The name-length check and its tests originally lived in `cli` (`MAX_ENTRY_NAME_BYTES`/
`check_entry_name_length` in `cli/src/mount.rs`, called from `Inner::mkdir`/`create`/`rename`).
Moved to `mountfs` itself instead, since this is a property of the *mount protocol layer*, not
anything backup-specific: neither libfuse nor WinFSP enforce name length on a virtual
filesystem's behalf (that's the entire reason this bug existed in the first place), and the
`dispatch_mkdir`/`dispatch_create`/`dispatch_rename` trampolines that every `MountFilesystem`
implementor goes through are structurally identical on both platforms - the natural, single place
to guarantee this for any implementor, not something each one has to remember to add itself.
Also fixes an incidental duplication the move surfaced: `Inner::statfs()`'s advisory
`max_name_length: 255` and the enforcement's own `255` were two independent magic numbers that
happened to match, not the same source of truth. Now both read `mountfs::MAX_NAME_BYTES`.

- **`mountfs/src/lib.rs`**: `pub const MAX_NAME_BYTES: usize = 255` and a private
  `reject_if_name_too_long(path)` helper, called from both platforms' `dispatch_mkdir`/
  `dispatch_create`/`dispatch_rename` *before* calling into the implementor's own method - so an
  over-long name never even reaches `T::mkdir`/`create`/`rename`.
- **Tests moved with it**: `mountfs/src/linux/mod.rs`'s
  `dispatch_rejects_a_name_over_max_name_bytes_before_reaching_the_filesystem` and
  `mountfs/tests/windows_mount.rs`'s test of the same name - both reuse the crate's own existing
  lightweight `TestFs` fixture (already read-only, no changes needed to it) rather than needing a
  full backup repository: since the check runs *before* `T::mkdir` is ever called, even a
  read-only `TestFs` proves the mechanism - a normal-length name reaches `TestFs`'s own
  default-`EROFS` rejection, an over-long one never gets that far, and the two produce
  distinguishably different OS errors on the client side. `cli`'s own former unit test
  (`write_ops_reject_a_name_longer_than_max_entry_name_bytes`, which called `fs.mkdir()` etc.
  *directly*, bypassing the dispatch trampolines entirely) had to be removed rather than kept
  alongside - it would no longer exercise anything, since the check no longer lives in
  `Inner::mkdir` at all. Same for `cli/tests/windows_mount.rs`'s equivalent subprocess test.
- **Pitfall hit while writing the Linux `mountfs`-level test**: a freshly-created mountpoint
  directory exists as a real, empty, *writable* directory on disk before FUSE actually attaches
  to it - `read_dir(mount_path).is_ok()` alone is not a valid readiness signal (it trivially
  succeeds against that real pre-mount directory too), and following it blindly let both the
  255-byte and 256-byte `create_dir` attempts race straight past FUSE entirely, hitting the real
  host filesystem instead (silently "passing" for the wrong reason). Fixed by waiting for actual
  *content* (`TestFs`'s hardcoded `sub` entry) to appear via `readdir`, matching the pattern the
  crate's own pre-existing read-only mount test already used correctly.

## cli-level tests: `restore`'s own resilience

Separately from `mountfs`'s enforcement, `cli/src/restore.rs` now has
`restore_warns_and_continues_past_a_name_too_long_for_the_destination_filesystem`: seeds a
too-long name directly via the test module's `seed_file` helper (bypassing `mount` - `restore`
needs to stay robust against such a name regardless of how it got into the repository, e.g. an
already-existing repository from before the `mountfs` fix existed), then confirms `restore`
reports a warning, restores every other entry normally, and exits with `ExitCode::SUCCESS` - not
a crash, not an early abort, not a silent drop. Matches exactly what was manually verified during
the original audit, now as a permanent regression test.

`list`/`find` weren't given dedicated tests: they only ever print an existing name, no OS
filesystem interaction with that name's length is involved, so there's no realistic failure mode
to regress against. `migrate_scala_repo` is a more plausible way for a too-long name to end up in
a repository for real (a direct DB-to-DB import, no `mount` involved) and would be a reasonable
next candidate, but wasn't added now - it needs a full Scala-export test fixture, disproportionate
effort for this pass; noted here as a known, lower-priority open item rather than silently
skipped.

## Deep-path findings (2026-08-14, empirical, real WinFSP on "julius")

The one item left open since this doc's first (2026-08-11) revision: everything above tested a
single *long component name*; the separate question of a long *total* path (many nested
directories, each individually short/legal) via a real WinFSP mount was explicitly flagged "not
yet tested" and never revisited even once julius became available for the component-length work -
attention went to the actual bug (component length) and the SMB wire-protocol gap instead. Closed
now with `backup mount --read-write` on julius, native release build:

- Built a 20-level-deep directory tree (each level a 40-character name) under a fresh
  `--read-write` mount, via PowerShell's `New-Item`/`Set-Content` using `\\?\`-prefixed
  ("verbatim") paths - **766 characters classic, 770 verbatim, both well past classic `MAX_PATH`
  (260)**.
  - **Creation, content write, and read-back all succeeded** via the verbatim path - a file at the
    bottom of the tree round-tripped its content byte-for-byte.
  - **Listing an intermediate level (10 levels deep) via the verbatim path succeeded**, correctly
    showing the next level down as a child - confirms `mountfs`'s Windows dispatch and
    `db::resolve_path`'s component-by-component walk both handle a long, deeply-nested path
    correctly end to end, not just a single long leaf name.
  - **Accessing the exact same path via the classic (non-verbatim) API failed cleanly** - `Test-Path`
    returned `False`, `Get-ChildItem` reported an ordinary "path cannot be found" error. No crash,
    hang, or corruption - this is standard Windows behavior for any filesystem once a path exceeds
    `MAX_PATH` without opting into the extended-length API, not something this project's own code
    does right or wrong.
- **`backup find` against the resulting repository correctly reconstructs and prints the full
  20-level path with no truncation** - confirms the database itself stored it intact (needed a
  `db compact` first, since the mount had been killed abruptly to unmount, leaving an
  uncheckpointed WAL - expected, matches `docs/plans/implemented/cross-process-repository-locking.md`'s
  own findings, not a new issue).
- **`restore` of that same 20-level tree onto a fresh native Windows target (not WSL2's DrvFs this
  time) succeeded**, producing a 786-character destination path with byte-identical content -
  confirms the DrvFs-only result from the original 2026-08-11 finding also holds for a genuinely
  native Windows restore target, closing that doc's own "Not yet tested: a native WinFSP mount"
  callout on the `restore` side too.

Test repository, mount, and generated tree all torn down afterward - nothing left behind on
julius.

### Conclusion

No code changes resulted - both the mount and `restore` already handle long total paths correctly
on real Windows, the same way `std::fs`'s own automatic verbatim-path handling already explained
the DrvFs result. This was purely a verification gap (an untested claim, not a suspected bug), now
closed with real evidence instead of an inference from adjacent findings.
