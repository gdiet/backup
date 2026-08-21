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
Status: draft
Importance: must

Mounting does not allow modifying repository content unless read-write access is explicitly
requested.

Rationale: a mount is a much larger blast radius for an accidental change than a single targeted
command — defaulting to read-only avoids exposing that risk unless it is specifically wanted.

### REQ-MOUNT-003: Optional read-write mount
Status: draft
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

A directory's deletion history is visible and browsable in place through the mount, and (on a
read-write mount) an entry can be recovered by moving it out of that view into the live tree.

Rationale: recovering a deleted file should be possible with the same ordinary file-manager
gesture (drag, cut-and-paste) a user would already reach for, not only via a separate command-line
step.

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
