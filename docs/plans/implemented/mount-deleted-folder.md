# Mount: a `[deleted]` virtual folder for browsing/recovering deletions

**Status**: implemented. One design decision below ("every directory")
was refined during implementation after a real test caught a consequence
neither the plan nor the original design discussion anticipated - see
"Refined during implementation" below.

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

## Design

A synthetic directory entry named `[deleted]` appears in `readdir` output
for **every** directory (not root-only), not backed by a real
`tree_entries` row. Listing it shows that directory's own deleted children
(via `db::deleted_entries`, scoped to that directory - the CLI's `backup
deleted [path]` already does exactly this scoping, so no new query is
needed, just calling it per-directory instead of once for the whole
repository). Reading a file under `[deleted]/...` serves its content
read-only, same as any other file. The recovery gesture is a `rename` from
a path under `[deleted]/...` to a real destination path - intercepted
specially in `DedupFs::rename` (`cli/src/mount.rs`) to call `db::undelete`
instead of `db::rename_entry`, rather than actually treating `[deleted]`
as a real, renamable directory.

### Decisions (resolved)

- **Name conflict with a real `[deleted]` entry**: the real entry always
  wins, locally. `readdir` only synthesizes the virtual `[deleted]` entry
  for a directory if that directory doesn't already have a real active
  child by that name - no global scan at mount time, no refusal to mount.
  A directory with a real `[deleted]` subfolder just doesn't get the
  synthetic trash view for that one directory; every other directory is
  unaffected.
- **Every directory, not root-only**: each directory shows its own deleted
  children, matching how a real trash-can/recycle-bin usually works
  (you find what you just deleted right where it disappeared from), not
  just `backup deleted`'s whole-repository default. The per-directory
  `deleted_entries` call is already scoped (not the expensive unscoped
  case that motivated `docs/plans/implemented/deleted-entries-performance.md`),
  so this should be cheap - worth confirming with a real measurement
  during implementation (see verification checklist), not just assumed.
  **Refined during implementation**: originally "always shown, even when
  empty" (matching how a real recycle bin icon is always present). A real
  Windows integration test caught a consequence neither this plan nor the
  original design discussion anticipated: `RemoveDirectory` (what
  `std::fs::remove_dir`/Explorer's "Delete folder" call) does its own
  directory-emptiness check via `readdir` *before* our own `rmdir` handler
  (which only counts real children) is ever reached - an unconditionally
  visible `[deleted]` would make *every* directory permanently
  non-removable through the mount, breaking the ordinary "delete the last
  file, then delete the now-empty folder" workflow. Fixed by only showing
  `[deleted]` when `db::has_deleted_children` is true (a new, cheap,
  `tree_entries_parent_id_idx`-backed query - see that function's own doc
  comment). Given the choice between reverting to root-only or keeping
  per-directory with a documented residual limitation, the latter was
  chosen: **a directory that currently has deletion history still can't be
  removed through the mount until that history is gone** (either
  recovered via `[deleted]` itself, or purged by `reclaim-space` past its
  retention window) - `cli/tests/windows_mount.rs`'s
  `mounts_read_write_and_supports_structural_changes_via_the_backup_binary`
  asserts this explicitly now, and it's documented in the README.
- **Disambiguating repeat-deletions**: `photo.jpg [42]` (id suffix) only
  when more than one deleted row in that directory's listing shares the
  name `photo.jpg`; plain `photo.jpg` otherwise - mirrors `backup
  deleted`'s own id-based disambiguation. A `rename` *of* a suffixed name
  needs to parse the `[<id>]` suffix back out to know which specific row
  to undelete (see "Nested browsing" below for the closely related parsing
  need).
- **Nested browsing into an already-deleted directory**: supported.
  `[deleted]/old-photos/vacation/...` must resolve - `readdir`/`getattr`/
  `open`/`read` on any path under a `[deleted]/...` prefix need their own
  resolution logic, separate from the normal active-tree
  `db::resolve_path` path: walk from the synthesized `[deleted]` entry
  down through `db::deleted_entries`' result set (which already returns
  full relative paths - see that function's doc comment), matching path
  components (including the `[<id>]` suffix disambiguation rule above)
  instead of doing normal active `tree_entries` lookups. This is the most
  code-shaped-different part of the whole feature: every read-side
  `MountFilesystem` method needs a branch that detects a `[deleted]/...`
  prefix and takes this alternate resolution path instead of the existing
  one.
- **Directory rename-out scope**: always recursive-with-same-`deleted_at`
  (mirrors `undelete --recursive`) - there's no way for a drag gesture to
  express "just this one file, not its siblings", so partial recovery of
  a dragged-out directory was never actually on the table.
- **Read-only mounts**: `[deleted]` still appears and is browsable/readable
  (nothing being mutated); a rename-out attempt fails with `EROFS`, same as
  every other mutating operation on a read-only mount already does.

### Resolved: file-manager drag-and-drop reality (was the remaining blocker)

A same-volume "move" isn't always a plain `rename(2)`/`MoveFile` syscall in
practice - some file managers implement cross-directory drag as
copy-then-delete-source, especially once a path *looks* unusual (bracketed
name, synthetic directory). Tested for real: instrumented `DedupFs::open`/
`write`/`create`/`unlink`/`rename` in `cli/src/mount.rs` with temporary
`eprintln!` logging, mounted a small disposable test repository read-write
at a drive letter, and dragged a file from one folder to another *within
that same mounted drive* in Windows Explorer. Observed call sequence:

```
open("/in/dnd_src/testfile.txt", write_intent=false)
open("/in/dnd_src/testfile.txt", write_intent=false)
rename("/in/dnd_src/testfile.txt", "/testfile.txt")
open("/testfile.txt", write_intent=false)
open("/testfile.txt", write_intent=false)
```

**Explorer calls `rename()` directly** for a same-drive move - the `open`
calls before/after are just its usual property/icon/thumbnail refresh
reads, not part of the move itself. No `create`/`write`/`unlink` sequence
at all. This means the simpler design holds: `rename` interception alone
is sufficient, and the more complex `unlink`-based fallback path sketched
in the original version of this section is **not needed** - dropped from
the design rather than kept as unused complexity. (Instrumentation was
temporary and has been reverted - `cli/src/mount.rs` is unchanged by this
spike.)

## Verification checklist

- [x] Measured `has_deleted_children`'s cost for real against the
  `dedup/` repository: `EXPLAIN QUERY PLAN` confirms `SEARCH tree_entries
  USING INDEX tree_entries_parent_id_idx (parent_id=?)`, and it returns
  instantly in practice.
- [x] `cargo fmt --check && cargo clippy --workspace --all-targets -- -D
  warnings && cargo test --workspace && cargo doc --no-deps --workspace`.
- [x] New unit tests in `cli/src/mount_deleted.rs` (path splitting, "real
  entry wins", id-suffix disambiguation, nested browsing, listing) and
  `db/src/query.rs` (`has_deleted_children`); two existing
  `cli/tests/windows_mount.rs` integration tests updated for the new
  `[deleted]` entry appearing in listings and the documented `rmdir`
  limitation.
- [x] Manually verified end-to-end against a real WinFSP mount: `[deleted]`
  appears only where expected, browsing/reading works, dragging a file out
  (`mv` in this case) correctly recovers it via `db::undelete` without
  copying, and the `rmdir` limitation reproduces exactly as documented.
- [x] Updated `README.md`'s `## Mount` section.
