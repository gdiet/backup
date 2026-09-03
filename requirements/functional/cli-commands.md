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
session first, addressing its target the same way REQ-CLI-007's listing and REQ-RESTORE-002's
restore do: an ordinary live path, or one drilling through REQ-TREE-009's `[deleted]` segment (in
[`tree.md`](tree.md)) down to a specific soft-deleted entry.

A live target is soft-deleted, the same outcome as an ordinary mount-side delete. Deleting a
directory with live (not yet deleted) children requires an explicit, separate opt-in to also delete
them; the exact flag(s) for that opt-in, and their precise semantics (e.g. distinguishing "recurse
into live children" from other behavior a recursive-delete flag might also need to control), are
not yet decided.

A target reached through `[deleted]` instead names a specific soft-deleted entry rather than a live
one. Removing it this way is permanent, and only happens when the caller also passes an explicit
`--purge` flag; without it, the command refuses rather than silently doing nothing. `--purge`
performs REQ-STORAGE-004's reclaim cascade immediately, scoped to the purged entry (and, for a
directory, its descendants), rather than leaving that entry's storage for a later, separate sweep -
so the storage a purged entry held becomes eligible for reuse as soon as the command returns, not
only its `tree_entries` row gone. This keeps an irreversible removal from ever being this command's
ordinary, unmarked behavior just because a passed-in path happened to resolve under `[deleted]` -
such a path can otherwise reach the command unremarkably, e.g. pasted from a `dfs list
--show-deleted` line.

Rationale: a mount session is not always convenient for a one-off or scripted deletion - a headless
host, an automation script. REQ-TREE-008 keeps the mount's own delete non-cascading, matching
ordinary POSIX `rmdir`; this command covers the bulk-subtree case directly and explicitly instead,
for a caller that does not want to mount at all. Reusing REQ-TREE-009's own addressing for the
permanent-delete case, rather than a second command taking a different kind of argument for what to
act on, means a caller does not need to learn a separate way to name a soft-deleted entry just to
remove it for good - REQ-CLI-007 already established this same one-scheme-everywhere property for
`dfs list`/`dfs restore`. Gating that case behind `--purge` keeps its irreversibility an explicit,
deliberate choice rather than an incidental consequence of which path string happened to be passed.
REQ-STORAGE-004's bulk purge remains the separate, repository-wide maintenance sweep for aged
soft-deleted entries in general; a user who wants one specific soft-deleted entry gone for good
right now - something sensitive that was accidentally backed up and then deleted, say - should not
have to wait for, or explicitly trigger, a full sweep just to get that guarantee for the one entry
they actually care about.

### REQ-CLI-004: Permanently delete a specific soft-deleted entry
Status: moved-to REQ-CLI-003

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
Status: agreed
Importance: should

`dfs list` reveals REQ-TREE-009's `[deleted]` addressing segment (in [`tree.md`](tree.md)) as part
of an ordinary directory listing when a caller passes `--show-deleted`; without the flag, a listing
never shows it, so a script parsing plain `dfs list` output is never surprised by an extra entry
the moment some history exists somewhere in that directory. The listing clearly distinguishes the
segment from a real, identically-named directory - REQ-TREE-009 lets a real entry win that name, so
the two must never be shown indistinguishably.

Once a caller names `[deleted]` explicitly as part of the path given to `dfs list` (or `dfs
restore`), that request works without needing `--show-deleted` too - naming the reserved segment is
already exactly as explicit a request as the flag would be, and requiring both would only add
friction, not clarity. This is the CLI counterpart to REQ-MOUNT-004's deleted-entry browsing, using
REQ-TREE-009's own addressing directly instead of a separate, CLI-specific mechanism - for a caller
that does not want to mount at all.

Whether a repository-wide variant (not scoped to one location, listing every soft-deleted entry
anywhere) is still worth having, now that a caller can reach any location's own deletion history
directly by path without first discovering an opaque identity, is not decided - see "Repository-
wide deleted listing" in [`../open-questions.md`](../open-questions.md).

Rationale: REQ-RESTORE-002 and REQ-CLI-003's `--purge` case both act on a specific soft-deleted
entry, but neither discovers it. Reusing REQ-TREE-009's own addressing here - rather than a bespoke
CLI-only mechanism that also invents its own way to name a chosen result - means a caller learns one
addressing scheme and reuses it unchanged across `dfs list`, `dfs restore`, and `dfs del --purge`.
Off-by-default matches REQ-MOUNT-004's own reasoning for the identical concern: a generic script
parsing `dfs list`'s output should not see an unannounced extra row just because a deletion
happened somewhere in that directory at some point.
