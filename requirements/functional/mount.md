# Mount

### REQ-MOUNT-001: Mount the repository as a real filesystem
Status: agreed
Importance: must

The repository's file tree can be exposed through a standard file-access interface — a local
mount, or a network file-sharing protocol — so it can be browsed and read with ordinary tools
instead of dedicated commands.

Rationale: not every tool a user wants to point at their backups can be taught to talk to a
repository directly — a standard, filesystem-style view removes that limitation entirely.

### REQ-MOUNT-002: Read-only by default
Status: agreed
Importance: must

Mounting does not allow modifying repository content unless read-write access is explicitly
requested.

Rationale: a mount is a much larger blast radius for an accidental change than a single targeted
command — defaulting to read-only avoids exposing that risk unless it is specifically wanted.

### REQ-MOUNT-003: Optional read-write mount
Status: agreed
Importance: should

When explicitly enabled, the mount also supports structural changes (creating/removing/moving
entries), setting a file's or directory's modification time, and real content writes, committed
back into the repository. This is an ongoing, ad-hoc way to write into the repository, distinct
from the directed, one-shot import covered by REQ-INGEST-001 in [`ingest.md`](ingest.md).

Rationale: some workflows genuinely need to edit backed-up content in place (or organize a backup
tree) using ordinary file-manager operations rather than a sequence of dedicated commands.

### REQ-MOUNT-004: Deleted-entry browsing and recovery through the mount
Status: agreed
Importance: should

Agreed here means this design is the wanted answer, conditioned on confirming REQ-MOUNT-007/008's
details against real file managers before they count as settled - behavior this dependent on real
tools cannot be fully validated on paper.

A directory's deletion history is visible and browsable in place through the mount, off by default
and enabled only by an explicit mount-time opt-in; on a read-write mount, an entry can be recovered
by moving it out of that view into the live tree. REQ-MOUNT-007 covers other mutating operations
against the view; REQ-MOUNT-008 covers how entries are displayed and disambiguated.

Rationale: recovering a deleted file should be possible with the same ordinary file-manager
gesture (drag, cut-and-paste) a user would already reach for, not only via a separate command-line
step.

Rejected: always visible, with no opt-in required. A generic recursive tool (an indexer, a sync
job, `find`) walking the live tree would then also walk into every directory's deletion history
without expecting to - in tension with REQ-TREE-005's "do not surprise ordinary POSIX tools" goal.
An explicit opt-in trades away some discoverability for a user unaware the feature exists, in
exchange for never surprising a tool that has not asked for this view.

### REQ-MOUNT-005: Configurable handling of missing data on read
Status: agreed
Importance: should

By default, a read that overlaps missing or incomplete stored data fails visibly (an I/O error)
rather than silently returning incorrect bytes. A best-effort mode that returns zero bytes for the
affected range instead is available as an explicit, deliberate opt-in.

Rationale: silently returning wrong data through a mount is worse than an application seeing a
read error — but once a user already knows a file is affected and specifically wants whatever
partial data remains (e.g. a mostly-intact image), that should still be possible on request.

### REQ-MOUNT-006: WinFSP attribution wherever it is required
Status: agreed
Importance: must

Wherever a build links against WinFSP (directly or dynamically) to provide Windows mount support,
the copyright notice and repository link WinFSP's FLOSS exception requires (see
[`../../NOTICE.md`](../../NOTICE.md)) appears somewhere a user of that build actually sees — a
`--version`/about command and the README, not only an internal notice file a build process reads
but a user never opens.

Rationale: WinFSP's GPLv3 FLOSS exception is what makes using it from an MIT/Apache-2.0 project
legally viable at all, and is conditioned specifically on that notice reaching a user, not just
existing somewhere in the repository — a `NOTICE.md` nobody using the built software ever opens
does not satisfy that on its own.

### REQ-MOUNT-007: Two-tier permission model for the deleted-entry view
Status: agreed
Importance: should

Not yet verified for real: how Explorer/Thunar/Nautilus/`rm -rf` actually behave here - needs
instrumented testing against a live mount, not just documentation, before counting as settled.
Working assumption: a WinFSP-mounted volume is not eligible for the Windows Recycle Bin (like a
network drive or a non-NTFS volume - `$Recycle.Bin` is an NTFS/Explorer-specific convention), so an
Explorer delete against this mount reaches `unlink`/`rmdir` directly, with no intermediate "moved
to trash" step.

Two independent, escalating mount-time opt-ins. The base one (REQ-MOUNT-004) makes the view visible
and browsable, supporting the recovery move-out. A second opt-in additionally allows deleting an
entry from inside the view, permanently purging it (REQ-CLI-004's operation, reached through the
mount). Without the second opt-in, any delete/create/rename/move into or within the view - other
than the recovery move-out - fails with a clear error (`EACCES`/`EPERM`), never a false success.
Renaming the view itself is always refused, under either opt-in.

Rationale: an operator who wants to browse and recover, without risk of a script or careless
recursive delete destroying history, should be able to have exactly that - a second, separate
opt-in unlocks the trash-emptying behavior deliberately rather than as visibility's automatic
consequence. This also means a recursive delete (`rm -rf`, "delete folder") descending into a
directory's view under the second opt-in purges its history as it goes, incidentally settling that
such a directory can eventually be fully removed, once its history is gone.

Rejected: reporting success without actually purging under only the base opt-in, so a delete
attempt against the view would appear to succeed but do nothing. This would make the mount's return
value inconsistent with the repository's actual state - a caller (a script, a file manager's
optimistic UI update) would see the entry still present or reappearing, reading as a bug rather
than a deliberate safety feature. An honest refusal communicates the same safety property without
that risk.

### REQ-MOUNT-008: Disambiguating and displaying deleted entries in the view
Status: agreed
Importance: should

Agreed carries the same conditioning as REQ-MOUNT-004: a wanted design, not yet confirmed - that a
real Explorer/Thunar/Nautilus listing actually displays and sorts the suffixed/prefixed names as
assumed here is unverified.

Within the view, an entry whose bare name collides with another deleted entry at the same location
is shown with a deletion-timestamp suffix before its extension (`photo [2026-08-22_140414].jpg`;
`.env [2026-08-22_140414]` for a dot-file, since it has no splittable extension) - an unambiguous
name is shown as-is. If the timestamp-suffixed name would not fit `mountfs::MAX_NAME_BYTES`, the
shorter id-only form (`photo [42].jpg`) is used instead; if even that does not fit, the base name is
truncated, but the id suffix is always kept intact - it, unlike a truncated or timestamp-only name,
is the part meant to be unique (`tree_entries.id`, which can realistically reach 8-9 digits, not
just the one or two a short example suggests).

The view is additionally reachable at a `[time]` path directly under it, listing the same entries
with the timestamp always prefixed instead - a second presentation of the same data, not a
different one. `st_mtime` for any entry is always its own real, stored modification time, never the
deletion time, in either presentation.

Rationale: a deletion timestamp is more informative at a glance than an opaque id, and sorting by
name only gives a meaningful chronological order when the timestamp is always present and at the
start - which conflicts with keeping an unambiguous entry's original name intact. Offering both -
suffix-only-when-needed for recognizability, always-prefixed under `[time]` for chronological
browsing - serves both needs rather than picking one purpose for a single view to serve badly.

Rejected: repurposing `st_mtime` to show deletion time inside the view. This would contradict
REQ-TREE-005's "mtime is genuine content-modification time" guarantee for exactly the entries being
browsed, lose the real modification time as a visible fact, and require keeping a getattr-time-only
override cleanly separate from the stored value, so recovering an entry does not resurrect it with
a corrupted mtime.

Rejected: extending `mountfs::Attr` with a dedicated timestamp field (e.g. Windows's native "Date
created" column) instead. Workable on Windows, but Linux lacks an equally common file-manager
column for it, weakening the feature on one platform; the `[time]` view achieves the same
chronological-browsing goal identically on both, using only what the mount abstraction already
exposes.

### REQ-MOUNT-009: Rename/move semantics matching the host platform's native filesystem
Status: agreed
Importance: should

A rename or move through the mount behaves the way NTFS does via Explorer on Windows, or ext4 does
via a Linux file manager or `mv`, rather than inventing bespoke semantics:

- Renaming a file onto an existing file's name replaces it, unless the caller asked not to
  (`RENAME_NOREPLACE`/`renameat2(2)`); `mountfs::MountFilesystem::rename`'s own `no_replace`
  parameter carries this through on both platforms (see its doc comment in
  `crates/mountfs/src/lib.rs` for the Windows caveat).
- Renaming a directory onto an existing name - file or directory - is always refused (`EEXIST`),
  never silently replaced or merged. Native filesystems differ here (POSIX allows replacing an
  *empty* target directory; Windows generally does not), so "match the native filesystem" alone
  does not settle this case - refusing uniformly is never a surprise on either platform, and
  `mountfs`'s own trait already documents unconditional refusal as valid.
- Renaming a directory to become its own descendant is always refused - it would create a cycle a
  tree cannot represent. Distinct from renaming a path onto itself (the exact same source and
  target), which is not a cycle and succeeds as a no-op.
- Renaming into a location whose parent does not exist fails (`ENOENT`), matching
  `mountfs::MountFilesystem::mkdir`/`create`'s own "the parent must already exist" rule.

Rationale: a mount's whole purpose is to let ordinary tools operate on the repository without
knowing anything about it - behavior that surprises a real NTFS or ext4 volume undermines that
purpose exactly where it matters most (an interactive drag-and-drop or `mv` in a script).

How "an existing name" is determined - case-sensitively, with a Windows-only case-insensitive
fallback - is REQ-MOUNT-010's concern; the bullets above hold under either resolution.

### REQ-MOUNT-010: Tree namespace comparison stays case-sensitive, with a Windows-only lookup fallback
Status: agreed
Importance: should

Name comparison within one directory (`tree_entries` uniqueness, lookup, collision detection on
`mkdir`/`create`/`rename`) is case-sensitive everywhere, matching ext4 and requiring no
platform-specific storage behavior.

On a Windows build of `dfs`, every operation that needs to answer "does this name already exist
here" - lookup (`open`/`getattr`), `mkdir`/`create`'s collision check, `rename`'s target check
(REQ-MOUNT-009) - additionally falls back to a case-insensitive match when no exact match exists:
try an exact match first; if none exists, compare the parent's active entries case-insensitively,
and if more than one matches, use the most recently created (highest `id`). This applies to any
`dfs` command touching the tree (`create-repo`, `mount`, future writers alike), not only the mount,
since `mkdir`/`create`/`rename` are shared `crates/db/src/tree.rs` operations. The underlying
comparison never becomes case-insensitive at the storage level.

ASCII letters must fold the way Explorer/NTFS does - the load-bearing case an everyday application
(opening `Readme.TXT` when the file is actually `readme.txt`) depends on. Beyond ASCII, broadly
matching NTFS is enough; exact per-character Unicode case-folding (e.g. German `ß`, Turkish `İ`/`ı`)
is not required.

A rename whose fallback match resolves to the entry being renamed itself (not a different one)
succeeds and updates its stored spelling in place - e.g. renaming `install.txt` to `Install.txt`.

Rationale: this repository can be populated from a real ext4 tree, which can legally contain
entries differing only in case - a case-insensitive storage layer could not represent that without
losing data. But ordinary Windows software routinely performs case-insensitive lookups (opening
`Readme.TXT` for `readme.txt` just works on real NTFS) - case-sensitive with no exception would
surprise everyday Windows use, against REQ-MOUNT-009's own "behave like the native platform"
principle. Case-sensitive storage with a Windows-only lookup fallback satisfies both.

Rejected: case-insensitive comparison at the storage level (`NOCASE`/a custom `COLLATE` on
`tree_entries.name`). Besides the data-loss risk above, this repository's SQLite file is portable
between a Linux and a Windows build of `dfs` - fixing the comparison rule into the schema would fix
it identically regardless of which platform opens the file next.

Rejected: case-sensitive everywhere, with no platform exception - reproduces the Windows-lookup
surprise the Rationale above describes; kept as the base layer with the fallback added on top, not
discarded.
