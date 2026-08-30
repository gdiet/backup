# Open Questions

Unresolved requirement decisions — not yet ready for `Status: agreed` in a `functional/` or
`non-functional/` file. One entry per question; once resolved, move it into the relevant topic
file as a proper `REQ-...` entry (or drop it here, if resolved as "no requirement needed").

### Content blacklisting
Status: idea

Should the system support marking specific content as forbidden — removed if currently stored, and
rejected if a matching file is stored again later (read back as zero bytes instead)? Useful for
purging content that should not exist in the repository at all, as opposed to an ordinary delete.

### "Copy when moving" semantics
Status: idea

Should moving a file within the repository actually copy it, deliberately, so that only the target
of the move commits real changes and the source stays untouched until it does? This is a
throughput trick for a specific workflow (fast-clone last backup, then only overwrite what
changed) rather than a general filesystem semantic — worth deciding deliberately rather than
inheriting it as a default mount behavior.

### Shallow/data-less repository copies
Status: idea

Should it be possible to create a metadata-only copy of a repository (browsable, but reading a
file's content returns zeros until the real data is synced in later)? Useful for a lightweight
secondary/offline copy of a large repository, but has real footguns (space reclamation must never
run against a shallow copy) that need a deliberate answer, not an incidental one.

### Automatic metadata backup before a mutating session
Status: idea

Should starting a session that can modify the repository (a read-write mount, in particular)
automatically take a metadata backup first, or should backups stay a separate, explicit operation
the user runs on their own schedule? Automatic-before-risk is a safety net; explicit-only avoids
surprise disk usage and lets the user control backup cadence themselves.

### User interface beyond the command line
Status: idea

Is a graphical entry point (for mounting, or for routine operations) in scope, or is this
CLI-and-mount-only for the foreseeable future? Affects how much of "Goals And Non-Goals" needs to
say about interfaces at all.

### Mount write-path failure handling
Status: idea

DESIGN-MOUNT-006 in [`../docs/design/mount-write-path.md`](../docs/design/mount-write-path.md)
settles how the future mount write path schedules its work (a non-blocking `release()`, background
chunking/hashing/storage-write). Left open by that decision: how a failure discovered only during
that background processing - after `release()` has already returned success - gets surfaced to the
user, since it can no longer be reported through `close()`'s own return value. REQ-MOUNT-005's
"fail visibly" guarantee does not cover this at all today, since it is scoped to reads. Surfacing
such a failure needs its own durable mechanism - the write-side counterpart of REQ-INTEGRITY-002's
read-time remediation, for a write discovered incomplete or failed after the fact - not solved
here.

The systemic-vs-isolated distinction matters here too, for a related but separate reason: a
systemic failure (disk full, the underlying storage disappearing) discovered during background
processing means every other queued or future job is equally doomed - worth reacting to
defensively (e.g. an automatic switch to read-only) rather than letting a backlog of certain-to-
fail jobs run anyway. An isolated failure (a bug tied to one file's content, transient lock
contention) should only mark that one file's outcome as a problem to discover later, without
affecting anything else in flight. Telling the two apart is also unsolved here - `io::ErrorKind`
(`StorageFull`, generic I/O errors) versus this project's own typed logic errors is the natural
technical hook, but distinguishing a transient systemic blip (a network mount reconnecting on its
own) from a genuinely permanent one is still open.

Two related questions this entry originally also raised are now settled, not open anymore: which
processes can see a not-yet-committed write (DESIGN-MOUNT-007), and how REQ-MAINTENANCE-004's
single-writer scope relates to a background job outliving its FUSE call (DESIGN-MOUNT-008) - both
in [`../docs/design/mount-write-path.md`](../docs/design/mount-write-path.md).
