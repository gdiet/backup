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

One middle ground worth weighing explicitly rather than only the two extremes above: scoping
"automatic" to specifically the operations that relocate or invalidate already-stored bytes (space
reclamation, compaction - the same operations REQ-MAINTENANCE-007 in
[`functional/maintenance.md`](functional/maintenance.md) already warns a stale backup about),
rather than every mutating session uniformly. An ordinary read-write mount session, or a plain
ingest run, does not carry that same category of risk to an existing backup's validity - only
reclamation/compaction do.

### User interface beyond the command line
Status: idea

Is a graphical entry point (for mounting, or for routine operations) in scope, or is this
CLI-and-mount-only for the foreseeable future? Affects how much of "Goals And Non-Goals" needs to
say about interfaces at all.

### Numbered fallback for a colliding `[deleted]` name
Status: idea

REQ-TREE-009's `[deleted]` addressing segment yields to a real, identically-named live entry at the
same location - the real entry wins, and that location's soft-deleted children become unreachable
through this convention until the real entry is itself renamed or removed. Should a colliding
location instead fall back to `[deleted-1]`, `[deleted-2]`, and so on, trying each in turn until an
unused name is found, so the deleted view stays reachable everywhere regardless?

This trades away a fixed, universally memorable segment name for a per-location one a caller can
only discover by listing first - real cost for what should be an exceedingly rare collision. For a
mount specifically, it also raises a harder question a one-shot CLI call does not have: if a real
`[deleted]` is created while a session that already shows the plain `[deleted]` segment is still
running, does the visible segment need to rename itself under a still-open handle? Worth revisiting
only if the plain "real wins" rule turns out to matter in practice.

### Repository-wide deleted listing
Status: idea

REQ-CLI-007 in [`functional/cli-commands.md`](functional/cli-commands.md) lets a caller list and
address a *given location's* soft-deleted children directly, via REQ-TREE-009's `[deleted]` path
segment - drilling down level by level finds any specific deleted entry without an opaque
identifier. Is a separate, repository-wide listing (every soft-deleted entry anywhere, in one
command, not scoped to a location a caller already suspects) still worth having on top of that, for
a caller who does not know or suspect where a deleted entry used to live?

### Queryable/mount-browsable surfacing of background write failures
Status: idea

DESIGN-MOUNT-009 in [`../docs/design/mount-write-path.md`](../docs/design/mount-write-path.md)
gives background write failures (DESIGN-MOUNT-006) a first version: a plain log file in `meta/`,
plus a read-only degradation on a systemic `crates/store` failure. Left open as a later expansion
stage: a queryable record of failures (e.g. a database table) and/or a dedicated mount-browsable
path for surfacing them, so a caller can notice and enumerate failures without reading a log file -
not needed for a first version, but a natural next step once real usage shows the plain log is not
enough.
