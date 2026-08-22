# Open Questions

Unresolved requirement decisions — not yet ready for `Status: agreed` in a `functional/` or
`non-functional/` file. One entry per question; once resolved, move it into the relevant topic
file as a proper `REQ-...` entry (or drop it here, if resolved as "no requirement needed").

### Content blacklisting
Should the system support marking specific content as forbidden — removed if currently stored, and
rejected if a matching file is stored again later (read back as zero bytes instead)? Useful for
purging content that should not exist in the repository at all, as opposed to an ordinary delete.

### "Copy when moving" semantics
Should moving a file within the repository actually copy it, deliberately, so that only the target
of the move commits real changes and the source stays untouched until it does? This is a
throughput trick for a specific workflow (fast-clone last backup, then only overwrite what
changed) rather than a general filesystem semantic — worth deciding deliberately rather than
inheriting it as a default mount behavior.

### Shallow/data-less repository copies
Should it be possible to create a metadata-only copy of a repository (browsable, but reading a
file's content returns zeros until the real data is synced in later)? Useful for a lightweight
secondary/offline copy of a large repository, but has real footguns (space reclamation must never
run against a shallow copy) that need a deliberate answer, not an incidental one.

### Automatic metadata backup before a mutating session
Should starting a session that can modify the repository (a read-write mount, in particular)
automatically take a metadata backup first, or should backups stay a separate, explicit operation
the user runs on their own schedule? Automatic-before-risk is a safety net; explicit-only avoids
surprise disk usage and lets the user control backup cadence themselves.

### User interface beyond the command line
Is a graphical entry point (for mounting, or for routine operations) in scope, or is this
CLI-and-mount-only for the foreseeable future? Affects how much of "Goals And Non-Goals" needs to
say about interfaces at all.

### Repository creation date
The repository's creation date should be reliably determinable. Stub only - not yet formulated in
detail (what it is used for, where it is stored, how it survives migration/restore).

### Deleted-directory display path and non-cascading delete
When a deleted directory is listed (REQ-TREE-003 in
[`functional/tree.md`](functional/tree.md)), what path is it shown under - particularly once an
ancestor directory is later renamed, moved, or itself deleted, which could leave a deleted entry's
original path no longer resolving to anything live, or resolving through another deleted ancestor
entirely? Separately: can a directory be deleted while it still has live
(not yet deleted) children, or does deleting a directory require its children to already be
deleted first? Both need a deliberate answer, not an incidental one - the first affects how deleted
history stays navigable across later reorganization of the live tree, the second affects whether
delete is a single-entry or whole-subtree operation.
