# Mount: a `[deleted]` virtual folder for browsing/recovering deletions

**Status**: sketch only - open questions listed below are deliberately
unresolved. Do not implement from this without going through them first.

## Motivation

`backup deleted`/`backup undelete`/`backup restore --deleted` (see
`docs/plans/implemented/soft-delete-recovery.md`) cover recovery from the
CLI, id by id. The natural next step - explicitly scoped out of that work
at the time, at the user's request, as "probably needs its own plan" - is
making deleted entries browsable and recoverable *inside the mount itself*,
so a file manager's normal drag-and-drop can pull something back out of
`[deleted]` the same way undeleting a file from a trash can works elsewhere.
This is a materially bigger change than the CLI commands: it touches live
filesystem semantics (`readdir`, `open`, `read`, and critically `rename`),
not just three new subcommands.

## Rough shape

A synthetic directory entry named `[deleted]` appears in `readdir` output
(root only, or every directory - see open questions), not backed by a real
`tree_entries` row. Listing it shows the current directory's own deleted
children (via `db::deleted_entries`, scoped to that directory rather than
the whole repository - the CLI's `backup deleted [path]` already does
exactly this scoping). Reading a file under `[deleted]/...` serves its
content read-only, same as any other file. The recovery gesture is a
`rename` from a path under `[deleted]/...` to a real destination path -
intercepted specially in `DedupFs::rename` (`cli/src/mount.rs`) to call
`db::undelete` instead of `db::rename_entry`, rather than actually treating
`[deleted]` as a real, renamable directory.

## Open questions

- **Name conflict with a real `[deleted]` entry.** If a directory already
  has a real, active child literally named `[deleted]`, the synthetic entry
  can't be synthesized there without shadowing it (or vice versa). Options:
  refuse to mount / warn once at mount time if any such conflict exists
  anywhere in the tree (expensive to check eagerly); pick a name less likely
  to collide (e.g. reserve a leading-`\0`-adjacent or otherwise
  filesystem-illegal-elsewhere name); or only synthesize `[deleted]` at the
  repository root (smaller conflict surface, but loses "trash right where
  you deleted it" locality - see next question). Not resolved.
- **Root-only vs. every directory.** Every directory showing its own
  `[deleted]` is more discoverable (you find what you just deleted right
  there) but multiplies the name-conflict surface above by every directory
  in the tree, and multiplies `readdir`'s cost (an extra `deleted_entries`
  query per directory listing, though scoped so probably cheap - see the
  known `deleted_entries` performance caveat in the soft-delete-recovery
  plan for the *unscoped* case, which doesn't directly apply here but is a
  reason to actually measure the scoped-per-directory cost before deciding).
  Root-only is simpler and matches how `backup deleted` defaults (whole
  repository) but requires remembering *where* something used to live to
  find it, rather than looking in the folder it disappeared from.
- **Disambiguating repeat-deletions of the same name.** `backup deleted`
  already handles "the same path deleted more than once" by showing each as
  its own id-tagged row (see the soft-delete-recovery plan's own resolved
  question on this). A mount listing has no natural place to show an id
  next to a filename without changing the visible name - e.g. suffixing
  `photo.jpg` as `photo.jpg [42]` when more than one deleted row shares that
  name, plain `photo.jpg` otherwise. Not decided, and interacts with the
  next question (what a rename *of* the suffixed name means).
- **What a directory rename-out actually recovers.** `backup undelete
  --recursive` reactivates exactly the descendants sharing the same
  `deleted_at` as the target - a coherent, deliberate scope. Dragging a
  *virtual* deleted directory out of `[deleted]` needs the same semantics,
  but "recursive" isn't a flag a file manager's drag gesture can express -
  it would need to always be recursive-with-same-`deleted_at`-scope
  (probably right, but worth stating as a decision, not an accident).
- **Read-only mounts.** Should `[deleted]` even appear when the mount was
  opened `--read-only`? Browsing/reading seems fine (nothing being
  mutated); the rename-based recovery gesture obviously can't work there.
  Showing the folder but having every rename-out fail with `EROFS` is
  consistent with how the rest of a read-only mount already behaves, but
  worth confirming that's not confusing UX (a trash folder you can look
  into but never empty).
- **File manager drag-and-drop reality.** A same-volume "move" isn't always
  a plain `rename(2)`/`MoveFile` syscall in practice - some file managers
  implement cross-directory drag as copy-then-delete-source, especially
  once a path *looks* unusual (bracketed name, synthetic directory). If
  that happens here, the "recovery" would actually be: read the deleted
  file's bytes out (fine, already supported), then call `unlink` on the
  *virtual* source path - which would need to also trigger the same
  undelete-or-move logic as a `rename`, but now decoupled from having a
  real destination path to hand (the copy already landed via a normal
  `create`+`write` at the real destination, driven by the file manager, not
  by this code). Needs testing against real file managers (Windows
  Explorer, at minimum) before assuming `rename` interception alone is
  sufficient.
- **Nested browsing into an already-deleted directory.** `backup deleted`
  itself only lets you scope by an *active* path (resolvable via the normal
  tree) - it can list what's under a deleted directory in its output, but
  can't take a deleted directory's own (unresolvable) path as a starting
  scope. Does `[deleted]` need to support browsing *into* a listed deleted
  directory (`[deleted]/old-photos/vacation/`) to see and selectively
  recover individual grandchildren, or is whole-subtree-at-once (via the
  directory-rename-out gesture above) enough? Real trash-folder UX
  (Windows/macOS) usually does support browsing in; whether it's worth the
  extra `readdir`-path-parsing complexity here is undecided.

## Explicitly not decided by this sketch

Whether this is worth building at all yet, relative to its size, versus the
CLI-only recovery path already shipped. This sketch exists so that
decision (and, if "yes", the open questions above) can be made deliberately
later, not to argue either way.
