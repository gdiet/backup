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

Agreement here means this working design is the current, wanted answer, explicitly conditioned on
confirming it against real file managers before any of its details (REQ-MOUNT-007/008 below) count
as settled rather than a plausible design - behavior this dependent on how real tools happen to act
cannot be fully validated on paper alone.

A directory's deletion history is visible and browsable in place through the mount, off by default
and enabled only by an explicit mount-time opt-in, and (on a read-write mount) an entry can be
recovered by moving it out of that view into the live tree. REQ-MOUNT-007 covers what other
mutating operations are, and are not, allowed against the view itself; REQ-MOUNT-008 covers how its
entries are displayed and disambiguated.

Rationale: recovering a deleted file should be possible with the same ordinary file-manager
gesture (drag, cut-and-paste) a user would already reach for, not only via a separate command-line
step.

Rejected: always visible, with no opt-in required. A generic recursive tool (an indexer, a sync
job, `find`) walking the live tree would then also walk into every directory's deletion history
without expecting to - in tension with REQ-TREE-005's "do not surprise ordinary POSIX tools" goal.
An explicit opt-in trades away some discoverability for a user unaware the feature exists, in
exchange for never surprising a tool that has not asked for this view.

### REQ-MOUNT-005: Configurable handling of missing data on read
Status: draft
Importance: should

By default, a read that overlaps missing or incomplete stored data fails visibly (an I/O error)
rather than silently returning incorrect bytes. A best-effort mode that returns zero bytes for the
affected range instead is available as an explicit, deliberate opt-in.

Rationale: silently returning wrong data through a mount is worse than an application seeing a
read error — but once a user already knows a file is affected and specifically wants whatever
partial data remains (e.g. a mostly-intact image), that should still be possible on request.

### REQ-MOUNT-006: WinFSP attribution wherever it is required
Status: draft
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

Not yet verified for real: how Explorer/Thunar/Nautilus/`rm -rf` actually behave against this -
needs real, instrumented testing (capturing actual syscalls against a live mount, not assuming
from documentation) before counting as settled. Working assumption in the meantime: a
WinFSP-mounted volume is not eligible for the Windows Recycle Bin at all (the same way a network
drive, or a volume not formatted NTFS, is not - `$Recycle.Bin` is an NTFS/Explorer-specific
convention a third-party filesystem driver does not participate in), so a delete through Explorer
against this mount should reach `unlink`/`rmdir` directly, with no intermediate "moved to the
Windows trash" step to account for.

Two independent, escalating mount-time opt-ins. The base one (REQ-MOUNT-004) makes the view
visible and browsable and supports the recovery move-out. A second, further opt-in additionally
allows deleting an entry from inside the view, which permanently purges it (REQ-CLI-004's
operation, reached through the mount instead of the CLI) rather than doing nothing. Without the
second opt-in, any attempt to delete, create, rename, or move something into or within the view -
other than the recovery move-out itself - fails with a clear error (`EACCES`/`EPERM`), never a
false success. Moving the view itself (renaming it) is always refused, under either opt-in.

Rationale: an operator who wants to browse and recover, without any risk that a script or a
careless recursive delete could destroy history, should be able to have exactly that; a second,
separate opt-in unlocks the trash-can-emptying behavior deliberately, rather than making it the
automatic consequence of merely turning on visibility. This also means a recursive delete tool
(`rm -rf`, a file manager's "delete folder") reaching into a directory's view under the second
opt-in purges its history as it descends, which incidentally resolves whether such a directory can
ever be fully removed at all - it can, once its history is gone, the same way an ordinary directory
becomes removable once its live contents are gone.

Rejected: reporting success without actually purging when only the base opt-in is active, so a
delete attempt against the view would appear to succeed but do nothing. This would make the
mount's own return value inconsistent with the repository's actual state - a caller (a script
checking the result, a file manager doing an optimistic UI update before its next refresh) would
observe the entry still present or reappearing, which reads as a bug rather than a deliberate
safety feature. An honest refusal communicates the same safety property without that risk.

### REQ-MOUNT-008: Disambiguating and displaying deleted entries in the view
Status: agreed
Importance: should

Agreement here carries the same conditioning REQ-MOUNT-004 states for this whole area: a working,
wanted design, not yet confirmed behavior - specifically, that a real Explorer/Thunar/Nautilus
listing actually displays and sorts the suffixed/prefixed names as assumed here has not yet been
verified.

Within the view, an entry whose bare name collides with another deleted entry at the same location
is shown with a deletion-timestamp suffix before its extension (`photo [2026-08-22_140414].jpg`;
`.env [2026-08-22_140414]` for a dot-file, at the end, since it has no splittable extension in the
usual sense) - an unambiguous name is shown as-is. If the timestamp-suffixed name would not fit
`mountfs::MAX_NAME_BYTES`, the shorter id-only form (`photo [42].jpg`) is used instead; if even
that does not fit, the base name is truncated to make room, but the id suffix is always kept
intact, since it - unlike a truncated or timestamp-only name - is the part of the display name
meant to be unique (`tree_entries.id`, which can realistically reach 8-9 decimal digits over a
long-lived, actively-used repository, not just the one or two digits a short example suggests).

The view is additionally reachable at a `[time]` path directly under it, listing the same entries
with the timestamp always prefixed to the display name instead - a second presentation of the same
underlying data, not a different one. `st_mtime` for any entry is always its own real, stored
modification time, never the deletion time, in either presentation.

Rationale: a deletion timestamp is more informative at a glance than an opaque id, and sorting a
plain listing by name only gives a meaningful chronological order when the timestamp is both always
present and at the start of the name - which conflicts with keeping an unambiguous entry's
original, recognizable name intact. Offering both - suffix-only-when-needed for recognizability,
always-prefixed under `[time]` for chronological browsing - serves both needs without compromising
either, rather than picking one purpose for a single view to serve badly.

Rejected: repurposing `st_mtime` to show deletion time while inside the view. This would silently
contradict REQ-TREE-005's own "mtime is genuine content-modification time" guarantee for exactly
the entries being browsed, lose the real modification time as a visible fact (a user checking "when
did I actually last edit this" would get the wrong answer), and require an implementation to keep
a getattr-time-only override cleanly separate from the stored value, so that recovering an entry
does not resurrect it with a corrupted mtime.

Rejected: extending `mountfs::Attr` with a new, dedicated timestamp field (e.g. surfaced as
Windows's native "Date created" column) to carry deletion time without touching the display name or
`st_mtime` at all. Workable on Windows, but Linux lacks an equally common, easily-enabled
file-manager column for an equivalent field, which would make the feature meaningfully weaker or
absent on one platform; the `[time]` view achieves the same chronological-browsing goal identically
on both, using only what the mount abstraction already exposes.

### REQ-MOUNT-009: Rename/move semantics matching the host platform's native filesystem
Status: draft
Importance: should

A rename or move through the mount behaves the way NTFS does via Explorer on Windows, or ext4 does
via a Linux file manager or `mv`, rather than inventing bespoke semantics - concretely:

- Renaming a file onto an existing file's name replaces it, unless the caller specifically asked
  not to (`RENAME_NOREPLACE`/`renameat2(2)`); `mountfs::MountFilesystem::rename`'s own `no_replace`
  parameter already carries this distinction through to the mount, on both platforms (see that
  method's doc comment in `crates/mountfs/src/lib.rs` for the Windows `no_replace` caveat).
- Renaming a directory onto an existing name - whether that name is a file or another directory -
  is always refused (`EEXIST`), never silently replaced or merged. Native filesystems differ here
  (POSIX `rename(2)` allows replacing an *empty* target directory; Windows generally does not, and
  neither platform's everyday file-manager workflow expects a silent replace-or-merge), so "match
  the native filesystem" alone does not settle this case - refusing uniformly is the one behavior
  that is never a surprise on either platform, and `mountfs`'s own trait already documents
  unconditional refusal as a valid, supported implementation choice.
- Renaming a directory to become its own descendant is always refused - on both platforms, this
  would create a cycle a tree structure cannot represent. Distinct from renaming a path onto
  itself, the exact same source and target path, which is not a cycle at all and succeeds as a
  no-op.
- Renaming into a location whose parent directory does not exist fails (`ENOENT`), matching
  `mountfs::MountFilesystem::mkdir`/`create`'s own "the parent must already exist" rule.

Rationale: a mount's whole purpose is to let ordinary tools operate on the repository without
knowing anything about it - behavior that would surprise a real NTFS or ext4 volume undermines that
purpose specifically where it matters most (an interactive drag-and-drop or `mv` in a script).
