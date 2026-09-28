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
Status: draft
Importance: should

Once agreed, "agreed" here would mean this design is the wanted answer, conditioned on confirming
REQ-MOUNT-007/008's details against real file managers before they count as settled - behavior this
dependent on real tools cannot be fully validated on paper. Still draft for the additional reason
given under "Open question" below.

A directory's deletion history is visible and browsable through the mount, but never inline within
the directory itself: a dedicated, always-present root entry, `[show-deleted]`, mirrors the live
tree's own structure, with REQ-TREE-009's `[deleted]` addressing (and its `[all]` and `[all]/[by-time]`
full-history views, in [`tree.md`](tree.md)) appearing at the corresponding location within that
mirror for every directory - never inline within the ordinary, live-only tree itself. Unlike an
earlier version of this requirement (see "Rejected" below), `[show-deleted]` needs no separate
mount-time opt-in of its own - it is always present and always browsable, regardless of
`--read-write`/read-only or `--purge`. On a read-write mount, an entry reached this way can be
recovered by moving it out into the live tree (REQ-MOUNT-013 in the case of a directory). REQ-MOUNT-007
covers purging and other mutating operations against the view; REQ-MOUNT-008 covers what the mount
specifically adds to REQ-TREE-009's own addressing/display rules.

Rationale: recovering a deleted file should be possible with the same ordinary file-manager
gesture (drag, cut-and-paste, copy) a user would already reach for, not only via a separate
command-line step. Keeping the view out of the ordinary, live tree entirely - reachable only through
one dedicated, clearly-named root entry - avoids the earlier inline design's own problems: a live
entry literally named `[deleted]` could shadow the view at that one location (REQ-TREE-009 already
handles this, but it is friction the new design does not create in the first place), and a mount-wide
flag that changed what an otherwise ordinary path revealed created its own class of bug - the exact
shape REQ-OPERABILITY-007 in
[`../non-functional/operability.md`](../non-functional/operability.md) exists to catch.

Rejected: `[deleted]` inline within every directory, gated by a mount-time `--show-deleted` opt-in
(an earlier version of this requirement). Worked for browsing, but coupled visibility to a flag that
changed the behavior of otherwise ordinary paths across the whole mount, and made a directory's own
deletion history collide, in the same namespace, with genuinely removing that history: removing the
last live child of a directory is itself an ordinary soft delete, which regenerated fresh `[deleted]`
content in the very directory a recursive purge had just finished emptying, making a directory with
any deletion history impossible to remove in a single pass. Verified live against a real WinFSP
mount before being abandoned.

Open question, not yet resolved: `[show-deleted]` being an always-present, ordinarily-named directory
at the mount root means a completely naive, unscoped recursive tool walking the whole mount from `/`
(not one that specifically asks for `[show-deleted]`) would still descend into it - the same class of
surprise the earlier, flag-gated design tried to avoid entirely, just now through one discoverable
entry point instead of scattered through every directory. Whether that smaller, more contained
exposure is an acceptable trade-off as-is has not been explicitly weighed.

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
legally viable at all. That exception is conditioned specifically on the notice reaching a user,
not just existing somewhere in the repository. A `NOTICE.md` nobody using the built software ever
opens does not satisfy that condition on its own.

### REQ-MOUNT-007: Two-tier permission model for the deleted-entry view
Status: agreed
Importance: should

Not yet verified for real: how Explorer/Thunar/Nautilus/`rm -rf` actually behave here - needs
instrumented testing against a live mount, not just documentation, before counting as settled.
Working assumption: a WinFSP-mounted volume is not eligible for the Windows Recycle Bin (like a
network drive or a non-NTFS volume - `$Recycle.Bin` is an NTFS/Explorer-specific convention), so an
Explorer delete against this mount reaches `unlink`/`rmdir` directly, with no intermediate "moved
to trash" step.

Two independent, escalating mount-time opt-ins, expressed structurally rather than as behavior-
changing flags (REQ-MOUNT-004). `[show-deleted]` needs no opt-in at all: browsing is always
available, and on a read-write mount, an entry can always be recovered from there by moving it out
into the live tree. A second, separate root entry, `[purge-deleted]`, mirrors the exact same
structure, but additionally allows permanently removing an entry from within the view
(`unlink`/`rmdir`) - reached through the mount, this is REQ-CLI-003's `--purge` operation. It is
shown only when the mount is both `--read-write` and `--purge`; without either, showing it would be
either useless (without `--read-write`, every mutating call there would fail with `EROFS` regardless)
or a standing invitation to destroy history without having deliberately opted into that specifically
(without `--purge`) - REQ-OPERABILITY-007's own principle in
[`../non-functional/operability.md`](../non-functional/operability.md), applied to visibility rather
than to refusing an already-given flag. Outside `[purge-deleted]`, any delete/create/rename/move
within the view other than the recovery move-out fails with a clear error (`EACCES`/`EPERM`), never a
false success. Renaming either view itself (the `[deleted]`, `[all]`, and `[all]/[by-time]` segments,
or `[show-deleted]`/`[purge-deleted]` themselves) is always refused.

A `rename()`/move call through the mount never silently substitutes a different name than the one
the caller actually requested for its destination, regardless of source - a returned success always
means the entry now exists exactly where and as named as asked. REQ-MOUNT-012 covers a separate,
explicit opt-in that lets an operator deliberately trade that literalism for convenience when
recovering an entry other than the most recent one for its name.

Rationale: an operator who wants to browse and recover, without risk of a script or careless
recursive delete destroying history, should be able to have exactly that - a second, separate
opt-in unlocks the trash-emptying behavior deliberately rather than as visibility's automatic
consequence. This also means a recursive delete (`rm -rf`, "delete folder") descending into
`[purge-deleted]` purges history as it goes, bottom-up, without ever regenerating fresh soft-deleted
content behind itself the way an earlier, inline design did (REQ-MOUNT-004's "Rejected" note) -
nothing under `[purge-deleted]` is ever live, so there is nothing left there for an ordinary delete
to soft-delete again.

Rejected: reporting success without actually purging under only the base opt-in, so a delete
attempt against the view would appear to succeed but do nothing. This would make the mount's return
value inconsistent with the repository's actual state. A caller - a script, or a file manager's
optimistic UI update - would see the entry still present or reappearing. That reads as a bug, not
as a deliberate safety feature; an honest refusal communicates the same safety property without
that risk. Verified live against a real WinFSP mount: a recursive-delete tool's own client-side "is
this really empty yet" check runs before it ever attempts the underlying `rmdir` at all, so a lying
success would never even have been exercised - it would not have achieved its original goal either.

Rejected: letting a `rename()`/move automatically substitute the true original name for whatever the
caller's destination path actually named, unconditionally. A `rename()` call's success has no
separate "here is what actually happened" return value, only success or failure - a result that does
not match what was actually requested is exactly the false-success problem the paragraph above
already rejects for `unlink`/`rmdir`, just for `rename`. REQ-MOUNT-012 keeps this available, but only
as a deliberate, mount-operator-level choice, never as unconditional default behavior any caller
could be surprised by.

### REQ-MOUNT-008: Mount-specific constraints on top of REQ-TREE-009's addressing
Status: agreed
Importance: should

Agreed carries the same conditioning as REQ-MOUNT-004: a wanted design, not yet confirmed - that a
real Explorer/Thunar/Nautilus listing actually displays and sorts the suffixed/prefixed names as
assumed here is unverified.

This requirement covers only what the mount adds on top of REQ-TREE-009's own addressing (in
[`tree.md`](tree.md)), which defines `[deleted]`, `[all]`, and `[all]/[by-time]` themselves. The "length
constraint the calling context imposes" that REQ-TREE-009 says decides when its timestamp suffix
gives way to its shorter id-only form is `mountfs::MAX_NAME_BYTES` here specifically - `dfs
list`/`dfs restore`'s own terminal-facing paths (REQ-CLI-007 in
[`cli-commands.md`](cli-commands.md)) have no such constraint. `st_mtime` for any entry reached
through either view is always its own real, stored modification time, never the deletion time.
Which timezone `[all]/[by-time]`'s prefixed timestamp renders in follows REQ-OPERABILITY-008 in
[`../non-functional/operability.md`](../non-functional/operability.md); `[all]`'s own disambiguation
suffix (used only for stable identity, not chronological browsing) stays UTC-unconditional, per that
same requirement.

Rationale: keeping this constraint mount-specific, rather than folding `mountfs::MAX_NAME_BYTES` into
REQ-TREE-009 itself, keeps the underlying addressing scheme free of a limit that is really about the
mount's own presentation layer - `dfs list`/`dfs restore` never need to truncate a name to fit a
directory-entry-length limit that does not apply to them.

Rejected: repurposing `st_mtime` to show deletion time inside the view. This would contradict
REQ-TREE-005's "mtime is genuine content-modification time" guarantee for exactly the entries being
browsed, lose the real modification time as a visible fact, and require keeping a getattr-time-only
override cleanly separate from the stored value, so recovering an entry does not resurrect it with
a corrupted mtime.

Rejected: extending `mountfs::Attr` with a dedicated timestamp field (e.g. Windows's native "Date
created" column) instead. Workable on Windows, but Linux lacks an equally common file-manager
column for it, weakening the feature on one platform; `[all]/[by-time]` achieves the same
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
losing data. But ordinary Windows software routinely performs case-insensitive lookups - opening
`Readme.TXT` for `readme.txt` just works on real NTFS. Case-sensitive with no exception would
surprise everyday Windows use, against REQ-MOUNT-009's own "behave like the native platform"
principle. Case-sensitive storage with a Windows-only lookup fallback satisfies both.

Rejected: case-insensitive comparison at the storage level (`NOCASE`/a custom `COLLATE` on
`tree_entries.name`). Besides the data-loss risk above, this repository's SQLite file is portable
between a Linux and a Windows build of `dfs` - fixing the comparison rule into the schema would fix
it identically regardless of which platform opens the file next.

Rejected: case-sensitive everywhere, with no platform exception - reproduces the Windows-lookup
surprise the Rationale above describes; kept as the base layer with the fallback added on top, not
discarded.

### REQ-MOUNT-011: Optional per-call debug log
Status: agreed
Importance: could

`--debug-log <PATH>` logs every call this mount session's own filesystem implementation receives -
its arguments and result - to `PATH`, one line per call, overwritten fresh on each invocation. Off
by default, and independent of `--purge`/`--restore-original-names` (REQ-MOUNT-012): it has no
bearing on repository content, only on this one session's own observability.

Rationale: REQ-MOUNT-007's own behavior around the deleted-entry view depends on exactly how a
real file manager or shell drives the mount - something otherwise only inferable indirectly, from
a caller's own error dialog or exit code, with no way to tell whether a given call reached this
project's own code at all or was refused earlier by the OS/mount driver itself. A per-call log
answers that directly.

### REQ-MOUNT-012: Optional automatic original-name restoration on recovery
Status: agreed
Importance: could

A mount-time opt-in, `--restore-original-names`: when given, moving an entry out of `[all]` or
`[all]/[by-time]` (REQ-TREE-009 in [`tree.md`](tree.md)) into the live tree via `rename()`
uses that entry's own true, stored name at the destination, regardless of what name the caller's
move actually specified - trading REQ-MOUNT-007's default literalism for the convenience of never
ending up with a timestamp baked into a recovered name. Requires `--read-write` - refused without it
(REQ-OPERABILITY-007 in [`../non-functional/operability.md`](../non-functional/operability.md)),
rather than silently having no effect. Has no effect on a copy (an entry read and then written
elsewhere) or on anything recovered directly through `[deleted]` itself, which already carries its
true name regardless of this flag.

Rationale: recovering an entry other than the most recent one for its name is the one remaining case
REQ-TREE-009's own default ("most recent per name, true name") does not already cover for free. An
operator who wants that same convenience there too should be able to choose it explicitly, without
it becoming a silent default any caller could be surprised by (REQ-MOUNT-007's own guarantee that
`rename()` never silently substitutes a name).

### REQ-MOUNT-013: Recovering a directory brings back its own recent history with it
Status: draft
Importance: should

Recovering a soft-deleted directory (moving it out of `[show-deleted]`/`[purge-deleted]` into the
live tree) recursively recovers exactly what REQ-TREE-009's own "most recent per name" view would
have shown for it and every one of its own soft-deleted descendants, at every nested level - not
literally every soft-deleted entry ever recorded beneath it. An older, superseded entry for a name
that already has a more recent soft-deleted sibling stays soft-deleted, unaffected, still reachable
afterward through the now-again-live directory's own `[deleted]`, `[all]`, and `[all]/[by-time]`
views, exactly as before the directory itself was deleted.

Because the directory being recovered lands either at an entirely new location (nothing existing to
collide with) or is refused outright at the top level by REQ-MOUNT-009's own directory-collision
rule before any of this cascading happens, no per-descendant collision handling is needed beyond
that already-existing top-level check.

Rationale: recovering a directory through the same ordinary drag/cut-and-paste gesture used for a
single file (REQ-MOUNT-004) should produce the same result an ordinary file-manager move would -
what browsing already showed at the source now exists at the destination too. Restoring only the
directory's own row, leaving every descendant still soft-deleted, would leave it looking empty
immediately after being "recovered", which does not match what recovering a directory should mean.
