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
space-reclamation sweep (REQ-STORAGE-004 in [`storage.md`](storage.md)) to eventually reach them.
This is a distinct command from REQ-CLI-003's live-entry delete, since it addresses history entries
rather than live paths - the two commands take different kinds of argument for what to act on.

Rationale: REQ-STORAGE-004's reclamation is a repository-wide maintenance sweep; a user who wants a
specific soft-deleted entry gone for good right now - something sensitive that was accidentally
backed up and then deleted, say - should not have to wait for, or explicitly trigger, a full sweep
just to get that guarantee for the one entry they actually care about.

### REQ-CLI-005: Create a new repository
Status: draft
Importance: must

A command initializes a new, empty repository at a given path, accepting the chunking-granularity
configuration REQ-STORAGE-003 requires be fixed at creation. The target path may already exist as
long as it is empty (a freshly mounted external drive's mount point, in particular, already exists
by the time it is mountable at all - it cannot itself be "not yet existing"); if it exists and is
not empty, or already holds a repository, the command fails rather than silently reinitializing or
mixing into whatever is already there.

Rationale: every other command (store, mount, restore, ...) needs a repository to already exist -
this is the one, deliberate entry point that brings one into being. Refusing a non-empty or
already-initialized target protects against a mistaken re-run destroying real backed-up data, while
still accepting the common case of an already-existing, empty target directory.
