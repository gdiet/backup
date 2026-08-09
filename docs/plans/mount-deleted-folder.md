# Mount: a `[deleted]` virtual folder for browsing/recovering deletions

**Status**: design decisions made (see below) - one empirical blocker
remains (file-manager drag-and-drop behavior) before implementation can
start responsibly. Not a sketch anymore, but not "ready to implement"
either until that spike is done.

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

### Remaining blocker: file-manager drag-and-drop reality (empirical, not a preference)

A same-volume "move" isn't always a plain `rename(2)`/`MoveFile` syscall in
practice - some file managers implement cross-directory drag as
copy-then-delete-source, especially once a path *looks* unusual (bracketed
name, synthetic directory). If that happens here, the "recovery" gesture
would actually arrive as: a normal `create`+`write` at the real
destination (already supported, no special handling needed - reads
`[deleted]/...` content just fine), followed by `unlink` on the *virtual*
source path - which would need to trigger the same undelete logic a
`rename` would, but now decoupled from ever having seen the real
destination path. `unlink` handling would need to become "if the path is
under `[deleted]/...`, call `db::soft_delete`... no - call nothing, since
the content was already read out; the entry should just stay soft-deleted,
matching `backup restore --deleted`'s "copy the bytes without touching
repository state" behavior, not `undelete`'s. This is a materially
different code path than the `rename` interception described above, and
whether it's even needed depends entirely on what real file managers
actually do - needs testing against Windows Explorer (at minimum) before
committing to `rename`-interception-only as sufficient. Treat this as a
short, dedicated spike before writing any other code for this feature, not
something to discover mid-implementation.

## Verification checklist (once the spike above is done)

- Confirm via the spike whether `unlink`-based recovery handling (see
  above) is actually needed, or whether `rename` interception alone
  suffices for the file managers this project cares about (at minimum
  Windows Explorer, given the primary platform).
- Measure per-directory `deleted_entries` cost for real (a `readdir` on a
  directory with many active children plus a nontrivial deleted-children
  count, against the real `dedup/` repository) before assuming it's cheap
  enough to run on every `readdir`.
- `cargo fmt --check && cargo clippy --workspace --all-targets -- -D
  warnings && cargo test --workspace && cargo doc --no-deps --workspace`.
- Once shipped, move this file under `docs/plans/implemented/`.
