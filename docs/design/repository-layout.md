# Repository Layout

How a repository is organized on disk at its root directory: what subdirectories and files
`create-repo` (REQ-CLI-005 in
[`../../requirements/functional/cli-commands.md`](../../requirements/functional/cli-commands.md))
produces, and how later opens (`store`, `mount`, ...) find their way back to the same layout.

## DESIGN-REPOSITORY-001: `meta/` and `data/`, created via an atomic staging rename

Status: draft

A repository's root directory holds exactly two top-level entries once created: `meta/`, holding
the metadata database (see [`metadata-storage.md`](metadata-storage.md) for the database itself),
and `data/`, holding the byte store (REQ-STORAGE-007 in
[`../../requirements/functional/storage.md`](../../requirements/functional/storage.md)'s
fixed-maximum-size, sequentially filled data files). Nothing else lives at the root by convention -
a user is free to place unrelated files there (that is what the "may already exist, if empty"
carve-out in REQ-CLI-005 is for - the directory is not exclusively DedupFS's the moment it exists,
only once `meta/` appears inside it), but nothing DedupFS itself writes touches the root directly.

`meta/` holds the SQLite database file (`repository.sqlite3`) plus whatever sidecar files SQLite
itself creates alongside it during ordinary operation (`-wal`/`-shm` in WAL mode). Creation builds
this directory's full contents in a sibling staging directory, `meta.tmp/`, first - applying the
schema migration and inserting the initial `repository_settings` row (see DESIGN-METADATA-009 in
[`metadata-schema-with-contents-table.md`](metadata-schema-with-contents-table.md)) against the
staging copy - and only renames `meta.tmp/` to `meta/` once that is fully committed and the
connection closed. `fs::rename` is atomic on the same volume on both Windows and POSIX, so a
process killed at any point before the rename leaves at most a `meta.tmp/` directory behind and no
`meta/` at all; one killed after the rename leaves a complete, valid `meta/`. Either way, a repeated
`create-repo` attempt sees an unambiguous state to act on - a leftover `meta.tmp/` is simply removed
and rebuilt from scratch, never resumed in place, since resuming would need to first re-verify
exactly how far a killed run got.

Whether a given root directory already holds a repository is decided by whether `meta/` exists
there, not whether the root directory itself does - this is what lets `create-repo` accept an
already-existing, empty root directory (see REQ-CLI-005) while still refusing to reinitialize a
directory that already holds one. `data/` is created directly, with no staging: unlike `meta/`, an
empty or partially populated `data/` left behind by an interrupted creation is harmless clutter, not
a correctness risk - nothing in `meta/` can reference bytes in `data/` until `meta/` itself exists,
so a `data/` directory alone, with no `meta/` next to it, is never mistaken for a valid repository.

`meta/` additionally reserves a `.lock` file, not yet written by anything - a namespace placeholder
for the cross-process locking REQ-MAINTENANCE-004 in
[`../../requirements/functional/maintenance.md`](../../requirements/functional/maintenance.md)
requires, so that building that mechanism later does not need to revisit this layout. Where a
metadata backup (REQ-MAINTENANCE-001 in the same file) is written is not decided here - an open
question for when that requirement is actually implemented, not before.

### Rejected: requiring the root directory to not yet exist

Creating the root directory itself as part of `create-repo` (refusing if it already exists at all,
the same way a plain `mkdir` without `-p` would) was considered and rejected: the most likely real
target for a repository is an already-mounted external drive, and a drive's mount point necessarily
already exists as a directory the moment it is mountable at all - it can never be "not yet
existing." Requiring exactly that would force the awkward workaround of creating a subdirectory
inside the actual mount point for the single most common case this command needs to serve well.
Accepting an existing-but-empty directory, as decided above, serves that case directly instead.

### Rejected: a single top-level database file, no `meta/` directory wrapper

Placing the SQLite file directly at the repository root (`repository.sqlite3`, no wrapping
directory) instead of inside `meta/` was considered and rejected on crash-safety grounds
specifically: an atomic staging-and-rename scheme needs to move however many files a run of SQLite
migrations happens to produce - the main database file, and potentially `-wal`/`-shm` sidecars -
into place as a single, all-or-nothing unit. A directory rename covers that uniformly, whatever the
exact file set turns out to be; a single-file rename would only cover the main database file itself,
leaving any sidecar file's own creation unprotected by the same guarantee.

### Rejected: one shared directory for both metadata and byte-store content

Keeping `meta/`'s and `data/`'s contents in one combined directory, rather than as two separate
ones, was considered and rejected: REQ-STORAGE-007's data files are structurally different from the
metadata database (bounded, sequentially filled, effectively immutable once sealed, versus a small,
frequently mutated random-access file) and serve different operational needs - a metadata-only
backup or copy (REQ-MAINTENANCE-001, or the still-open "shallow copy" question in
[`../../requirements/open-questions.md`](../../requirements/open-questions.md)) is then a matter of
copying one clearly bounded directory rather than filtering specific files out of a mixed one.
