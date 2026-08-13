# Mount: `rename` overwrite support

**Status**: not started - confirmed as an actionable item during a `docs/plans/
deleted-folder-ux-review.md` walkthrough (2026-08-13), not yet designed in detail or implemented.

## What was found

Dragging a file onto an existing name in a `--read-write` mount, confirming "yes, overwrite" in
Windows Explorer, leaves the copy/move dialog hanging (the mount itself stays responsive and the
dialog can be closed) instead of completing or failing cleanly. Reproduced two ways:

1. Recovering a file from `[deleted]` onto an existing active file of the same name (the original
   report).
2. A completely ordinary move overwriting an existing file, no `[deleted]` involved at all -
   confirmed to fail identically, so this isn't specific to the recovery gesture.

Directly reproduced over a real FUSE mount in WSL (no Samba/Docker involved) with plain `mv`:

```
mv: cannot overwrite '.../2/file.txt': File exists
```

`mv` itself handles that cleanly (prints the error, exits 1) - the hang is specific to how
Explorer's copy engine reacts to getting `EEXIST` back after a user has already confirmed
overwrite via `MOVEFILE_REPLACE_EXISTING`/SMB2 `ReplaceIfExists`, not a mount-level deadlock.

## Root cause

Both paths that can move an entry onto an already-occupied name reject the conflict outright
instead of replacing the target:

- `db::rename_entry` (`db/src/tree.rs:216`) - the ordinary move/rename path.
- `db::undelete`'s `relocate_to` handling (`db/src/maintenance.rs:113`) - the `[deleted]` recovery
  path (`DedupFs::rename` in `cli/src/mount.rs` calls this instead of `rename_entry` once it
  recognizes the source is under `[deleted]`).

Both are documented, deliberate limitations from the original read-write mount design
(`docs/plans/implemented/06-fuse-mount-readwrite.md:147`, "Deliberately no overwrite-existing-
target support in the first cut"), which explicitly left this open in its "Not yet decided"
section: "Whether `rename`'s no-overwrite limitation turns out to matter in practice once the
mount is actually used." This walkthrough answered that: yes, it does.

## Proposed shape (not yet fully designed)

Implement real POSIX `rename(2)` replace semantics instead of erroring:

- If the target name is free, behave exactly as today.
- If the target is occupied by a *compatible* entry (file replacing file, or an empty directory
  replacing an empty directory), replace it: soft-delete the conflicting target entry (consistent
  with this tool's existing "nothing is really gone until `reclaim-space`" philosophy - the
  replaced target becomes recoverable via `[deleted]`, matching what `rm target && mv source
  target` would already leave behind through two separate operations), then move the source entry
  into place - both inside one transaction, mirroring `db::undelete`'s existing relocate-then-
  reactivate transaction shape.
- If the target is occupied by an *incompatible* entry (file onto directory, non-empty directory
  onto anything, directory onto a file), keep returning a real error (`EISDIR`/`ENOTDIR`/
  `ENOTEMPTY` as appropriate) rather than attempting anything - matches real `rename(2)`.
- Share the conflict-resolution logic between `rename_entry` and `undelete`'s `relocate_to` case
  rather than duplicating it, since both need the identical replace-vs-reject decision.

## Open questions to settle before implementing

- Self-rename / no-op case (source and target already the same entry) - what should happen?
- Whether the replaced target's `deleted_at` should be "now" (consistent with an ordinary
  `unlink`) or something else.
- Whether `mountfs`'s platform abstraction (real libfuse3 on Linux, real WinFSP on Windows) needs
  anything special wired through to actually *offer* replace semantics to the client, or whether
  returning success from a plain `rename` callback is already enough on both platforms - needs
  checking against WinFSP's own rename/replace contract, not just the Linux side.
- Whether this closes the "Not yet decided" item in `docs/plans/implemented/
  06-fuse-mount-readwrite.md` outright, or whether that doc needs its own follow-up note once this
  ships.
