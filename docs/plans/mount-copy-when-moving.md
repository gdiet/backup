# Mount: `copyWhenMoving` option

**Status**: not started - this is a stub, not a plan. Not present in Rust's
`backup mount` at all; already noted as explicitly out of scope once
before, in `docs/plans/implemented/06-fuse-mount-readwrite.md`'s original
draft ("a `copyWhenMoving`-equivalent decision deferred - not clearly
needed without the GUI toggle mechanism that motivated it in Scala").

## What it is (Scala reference)

A runtime-toggleable mount state (`copyWhenMoving=true` at startup, or a
checkbox in the Scala server GUI - see `docs/plans/server-gui.md`). While
enabled, a `rename` of a *file* (not a directory) within the mount doesn't
actually move the tree entry - it copies it (new tree entry, same
`dataId`/content, matching Rust's existing dedup-by-content model) instead
of the plain in-place `parent_id`/`name` update `rename` normally does.

The Scala README's stated use case: build up a new backup incrementally by
first "moving" (really: fast copy-linking) an entire previous backup
snapshot into place, then doing a real incremental `store` run on top that
only touches files that actually changed - moving/copy-linking the
unchanged bulk this way is far faster than a real file-by-file copy
(reading + rehashing + rewriting), since it's just new tree rows pointing
at existing content.

## Why this doesn't obviously map onto the current design

Rust's `rename` (`db::rename_entry`) already has a stated, deliberate
limitation: no overwrite support, real POSIX `rename()` semantics not
fully implemented (see `docs/plans/implemented/06-fuse-mount-readwrite.md`).
`copyWhenMoving` changes `rename`'s fundamental semantics (move vs. copy)
based on mutable, runtime-toggled state - a bigger behavioral surface than
the "reject the hard cases with a real error" stance taken so far. It also
has no obvious trigger without the GUI's toggle (a `--copy-when-moving`
mount flag alone, fixed for the mount's whole lifetime, would cover the
"prime a new backup from an old one" use case without needing runtime
toggling - possibly enough, worth deciding once actually planned).

## Rough shape if/when planned

- Almost certainly wants to land *after* (or alongside) `docs/plans/
  server-gui.md` if runtime toggling is desired - a fixed mount-time flag
  is a much smaller, more self-contained increment if the GUI isn't being
  built regardless.
- `DedupFs::rename` would need a copy path: resolve the source entry, and
  instead of `db::rename_entry`, insert a *new* tree entry at the
  destination referencing the same `content_id`, leaving the source entry
  untouched (matching Scala's "copy, don't move" framing exactly).
