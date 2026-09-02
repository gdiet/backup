# CLI Commands

### REQ-CLI-001: Usage help without prior knowledge
Status: agreed
Importance: must

The command-line tool provides a way to request usage information — at the top level and for any
individual command — sufficient to discover available commands and their arguments without
consulting external documentation.

Rationale: a backup/repository tool is operated both interactively and from scripts; both need to
be able to discover its interface from the tool itself, not only from the README.

### REQ-CLI-002: Build identity on request
Status: agreed
Importance: must

The command-line tool provides a way to report its own build identity: its version, and, when the
running build links WinFSP, the attribution WinFSP's FLOSS exception requires — see REQ-MOUNT-006
in [`mount.md`](mount.md), which this requirement is the concrete implementation of.

Rationale: a build-identity command is the natural place a user actually looks, which is exactly
why REQ-MOUNT-006 names it as one of the required surfaces.

### REQ-CLI-003: Delete a tree entry without mounting
Status: agreed
Importance: should

A command deletes a file or directory directly against the repository, without requiring a mount
session first. Deleting a directory with live (not yet deleted) children requires an explicit,
separate opt-in to also delete them; the exact flag(s) for that opt-in, and their precise semantics
(e.g. distinguishing "recurse into live children" from other behavior a recursive-delete flag might
also need to control), are not yet decided.

Rationale: a mount session is not always convenient for a one-off or scripted deletion - a headless
host, an automation script. REQ-TREE-008 keeps the mount's own delete non-cascading, matching
ordinary POSIX `rmdir`; this command covers the bulk-subtree case directly and explicitly instead,
for a caller that does not want to mount at all.

### REQ-CLI-004: Permanently delete a specific soft-deleted entry
Status: agreed
Importance: could

A command permanently removes one or more specific soft-deleted entries, selected by their
history-entry identity (the same addressing REQ-RESTORE-002 uses), rather than requiring a general
bulk purge sweep (REQ-STORAGE-004 in [`storage.md`](storage.md)) to eventually reach them. This is
a distinct command from REQ-CLI-003's live-entry delete, since it addresses history entries rather
than live paths - the two commands take different kinds of argument for what to act on.

Rationale: REQ-STORAGE-004's bulk purge is a repository-wide maintenance sweep; a user who wants a
specific soft-deleted entry gone for good right now - something sensitive that was accidentally
backed up and then deleted, say - should not have to wait for, or explicitly trigger, a full sweep
just to get that guarantee for the one entry they actually care about.

### REQ-CLI-005: Create a new repository
Status: agreed
Importance: must

A command initializes a new, empty repository at a given path, accepting the chunking-granularity
configuration fixed at creation (REQ-STORAGE-003 in [`storage.md`](storage.md)). The target path may
already exist, as long as it is empty - a freshly mounted external drive's mount point already
exists once it is mountable at all, so can never itself be "not yet existing". If it exists and is
not empty, the command fails.

Where the chunking configuration is not given explicitly, it defaults to content-defined chunking
with a 20-bit target size (REQ-OPERABILITY-003 in
[`../non-functional/operability.md`](../non-functional/operability.md)) - an average chunk size a
little above 1 MiB, reasonable absent a specific reason to pick differently. `--whole-file`, or an
explicit target size, remain available as an override - since REQ-STORAGE-003 still fixes this
choice for the repository's lifetime, the command states that plainly wherever the choice is made
(its `--help` text and confirmation output), not just via the flag itself.

Rationale: every other command (store, mount, restore, ...) needs a repository to already exist -
this is the one, deliberate entry point that brings one into being. Refusing a non-empty target
protects against a mistaken re-run destroying real backed-up data, while still accepting the common
case of an already-existing, empty target directory.

### REQ-CLI-006: Default repository location relative to the executable
Status: agreed
Importance: should

Where a command needs a repository path and none is given explicitly, it defaults to a
`dedupfs-repository` directory next to the running executable itself - the directory containing
the executable, not the current working directory the command happens to be invoked from. If the
resolved default is unusable (unwritable for REQ-CLI-005's `create-repo`; does not actually hold a
repository for any other command), the command fails with a clear, actionable message asking for an
explicit `--repo`/path argument (REQ-OPERABILITY-004 in
[`../non-functional/operability.md`](../non-functional/operability.md)), never a raw filesystem
error standing in as the only explanation.

Rationale: DESIGN-CLI-003/004 in
[`../../docs/design/distribution.md`](../../docs/design/distribution.md) records the full
reasoning - in short, this only makes sense together with distributing the executable as a
portable download the operator places wherever they keep the repository (often removable/external
storage), rather than through a system installer; it satisfies REQ-OPERABILITY-003's general
"reasonable defaults" principle for this specific, recurring parameter.

### REQ-CLI-007: List soft-deleted entries without mounting
Status: draft
Importance: should

A command lists soft-deleted entries - at a given tree location, or repository-wide - without
requiring a mount session first. Each listed entry shows its history-entry identity (REQ-TREE-004
in [`tree.md`](tree.md)) and deletion timestamp, sufficient to address it for REQ-RESTORE-002's
direct restore or REQ-CLI-004's permanent delete. This is the CLI counterpart to REQ-MOUNT-004's
deleted-entry browsing and REQ-MOUNT-008's disambiguation, for a caller that does not want to mount
at all.

Rationale: REQ-RESTORE-002 and REQ-CLI-004 both act on a specific soft-deleted entry addressed by
its history-entry identity, but neither one discovers that identity itself. Without a way to list
soft-deleted entries outside a mount, a caller in the same position REQ-CLI-003 already covers for
live entries - a headless host, an automation script - has no way to find which entry to name.
