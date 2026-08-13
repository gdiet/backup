# Mount: `rename` overwrite support

**Status**: implemented and verified on both real Linux/FUSE and real Windows/WinFSP (2026-08-13,
the latter on "julius" - see "Windows verification status" below for the full session record).
Real POSIX `rename(2)` replace semantics now work through the mount on both platforms, including
the recovery-from-`[deleted]` path that originally surfaced the bug.

## What was found

Dragging a file onto an existing name in a `--read-write` mount, confirming "yes, overwrite" in
Windows Explorer, left the copy/move dialog hanging (the mount itself stayed responsive and the
dialog could be closed) instead of completing or failing cleanly. Reproduced two ways:

1. Recovering a file from `[deleted]` onto an existing active file of the same name (the original
   report).
2. A completely ordinary move overwriting an existing file, no `[deleted]` involved at all -
   confirmed to fail identically, so this wasn't specific to the recovery gesture.

Directly reproduced over a real FUSE mount in WSL (no Samba/Docker involved) with plain `mv`:

```
mv: cannot overwrite '.../2/file.txt': File exists
```

`mv` itself handled that cleanly (printed the error, exited 1) - the hang was specific to how
Explorer's copy engine reacted to getting `EEXIST` back after a user had already confirmed
overwrite via `MOVEFILE_REPLACE_EXISTING`/SMB2 `ReplaceIfExists`, not a mount-level deadlock.

## Root cause

Both paths that can move an entry onto an already-occupied name rejected the conflict outright
instead of replacing the target:

- `db::rename_entry` (`db/src/tree.rs`) - the ordinary move/rename path.
- `db::undelete`'s `relocate_to` handling (`db/src/maintenance.rs`) - the `[deleted]` recovery
  path (`DedupFs::rename` in `cli/src/mount.rs` calls this instead of `rename_entry` once it
  recognizes the source is under `[deleted]`).

Both were documented, deliberate limitations from the original read-write mount design
(`docs/plans/implemented/06-fuse-mount-readwrite.md`, "Deliberately no overwrite-existing-target
support in the first cut"), which explicitly left this open in its "Not yet decided" section.

## Implementation (done)

Real POSIX `rename(2)` replace semantics, matching `renameat2(2)`'s own contract:

- If the target name is free, behaves exactly as before.
- If the target is occupied by a *compatible* entry (file replacing file, or an empty directory
  replacing an empty directory), replaces it: the conflicting target entry is soft-deleted (same
  `deleted_at` the caller would use for an ordinary `unlink`/`rmdir` - consistent with this tool's
  "nothing is really gone until `reclaim-space`" philosophy, the replaced target stays recoverable
  via `[deleted]`) and the source entry moves into place, both inside one transaction
  (`db::rename_entry`/`db::undelete` now take `&mut Connection` for this).
- If the target is occupied by an *incompatible* entry (file onto directory, non-empty directory
  onto anything, directory onto a file), returns a real error - `Errno::EISDIR`/`ENOTDIR`/
  `ENOTEMPTY` as appropriate, mirrored by three new `db::Error` variants
  (`TargetIsADirectory`/`TargetIsAFile`/`TargetNotEmpty`) - matches real `rename(2)`, confirmed
  against the Scala prototype's own `Server.scala::rename` (identical `EISDIR`/`ENOTDIR` mapping).
- A self-rename (source and target already the same entry) is a no-op, checked before any replace
  logic runs - without this, replacing would have soft-deleted the source and then tried to move
  the now-nonexistent row, silently orphaning it. Matches the Scala prototype's own
  `oldParts.sameElements(newParts) => OK` check (`Server.scala`).
- `no_replace` (`RENAME_NOREPLACE`, from `renameat2(2)`/real libfuse3's `rename` callback's
  `flags` parameter) still gets the old behavior - an existing target always fails with `EEXIST`
  regardless of kind, never replaced. Previously this `flags` parameter was received by both
  backends' dispatch trampolines and silently discarded (`mountfs/src/{linux,windows}/mod.rs`);
  now `mountfs::MountFilesystem::rename` takes an explicit `no_replace: bool`, and both backends
  parse the raw flags via a new shared `mountfs::parse_rename_flags`, which also rejects
  `RENAME_EXCHANGE` (an atomic two-way swap of two existing entries) outright with `EINVAL` -
  explicitly out of scope, never silently mishandled as an ordinary replace.
- The CLI `backup undelete --to` command deliberately keeps `no_replace: true` unconditionally -
  its own `--to` help text already promises "fails otherwise, rather than silently renaming" if
  the target is occupied, and silently replacing there would break that documented contract for a
  command a user runs deliberately, not through a GUI's own overwrite-confirmation prompt. Only
  the mount's `[deleted]`-folder drag-out gesture and ordinary mount `rename` get the new
  replace-by-default behavior, mirroring what real POSIX `rename(2)`/Explorer's own confirmed
  overwrite already implies.

Verified: `cargo fmt --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test
--workspace`, `cargo doc --no-deps --workspace` all clean. New tests: `mountfs::tests` covers
`parse_rename_flags` directly (default/`RENAME_NOREPLACE`/`RENAME_EXCHANGE`/combined/unrelated
bits); `db::tree::tests`/`db::maintenance::tests` cover `rename_entry`/`undelete`'s replace,
self-rename-no-op, `no_replace`-still-rejects, and all three kind-mismatch cases; `cli::mount::tests`
covers `map_rename_error`'s mapping directly plus four end-to-end tests against a real `DedupFs`
(built via `build_filesystem`, no actual OS-level mount needed) - ordinary replace, `no_replace`
still failing, self-rename no-op, and a full regression test for the original bug report (recover
from `[deleted]` onto an existing active file).

## Considered and rejected during design

Not implemented: a self-rename onto a differently-cased or otherwise-distinguishable-but-equal
path, or a rename where old/new differ only by trailing slash - these weren't reported as issues
and add complexity without a known real trigger; `db::resolve_path`'s existing path handling
already normalizes what it needs to for the cases that matter.

## Windows verification status

Verified on real Windows/WinFSP (julius, native release build, 2026-08-13). Interactive Explorer
drag-and-drop itself wasn't exercised (no GUI session available over plain SSH) - instead used
PowerShell's `Move-Item` against a real WinFSP `--read-write` mount, which goes through the same
real Win32 `MoveFileEx`/`SetFileInformationByHandle` APIs a real Explorer drag ultimately calls
into, `-Force` vs. not being the direct Win32-level equivalent of a confirmed-overwrite vs.
no-replace request:

- **The original bug report, replayed exactly**: `1\[deleted]\file.txt` (an 8-byte recovered file)
  moved with `Move-Item -Force` onto an already-occupied `2\file.txt` (7 bytes) - succeeded
  cleanly, no hang. `2\file.txt` afterward holds the recovered content, and a new `[deleted]`
  entry appeared under `2\` holding the replaced original - confirms both the replace itself and
  that it's recoverable, exactly as designed.
- **No-replace**: plain `Move-Item` (no `-Force`) onto an existing target failed immediately with
  a clean "a file cannot be created because it already exists" error (the same message .NET gives
  for `ERROR_FILE_EXISTS`) - no hang, and (confirmed via a clean, isolated repeat - see below)
  both source and destination files were left completely untouched.

One false alarm along the way, worth recording so it isn't re-investigated later: an *initial*
no-replace attempt appeared to leave the source file truncated to 0 bytes despite the move
correctly failing. Traced to the test harness, not the mount: that attempt used a single
inline `powershell -Command "..."` string quoted across bash → ssh → cmd.exe → PowerShell, which
failed with a PowerShell parser error before `Move-Item` ever ran - almost certainly a stray shell
redirection operator leaking through the mangled command line and truncating the file itself,
unrelated to `rename`/WinFSP at all. Switching to a staged `.ps1` script file (`scp`'d to julius
and run via `-File`, avoiding the multi-layer quoting entirely - see the `julius-winfsp-ssh`
skill's own advice on this) and repeating the exact same no-replace scenario on a fresh file pair
showed no corruption at all: both files fully intact after the (correctly) failed move. No code
changes resulted from this - it never was a real bug.
