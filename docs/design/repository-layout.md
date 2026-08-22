# Repository Layout

How a repository is organized on disk at its root directory: what subdirectories and files
`create-repo` (REQ-CLI-005 in
[`../../requirements/functional/cli-commands.md`](../../requirements/functional/cli-commands.md))
produces, and how later opens (`store`, `mount`, ...) find their way back to the same layout.

## DESIGN-REPOSITORY-001: `meta/` and `data/`, created via an atomic staging rename

Status: implemented

Implemented in `crates/db/src/lib.rs` (`init_repository`/`open_repository`).

A repository's root directory holds exactly two top-level entries once created: `meta/`, holding
the metadata database (see [`metadata-storage.md`](metadata-storage.md) for the database itself),
and `data/`, holding the byte store (REQ-STORAGE-007 in
[`../../requirements/functional/storage.md`](../../requirements/functional/storage.md)'s
fixed-maximum-size, sequentially filled data files). Nothing else lives at the root - REQ-CLI-005 in
[`../../requirements/functional/cli-commands.md`](../../requirements/functional/cli-commands.md)
requires the root directory to be empty at creation time, so `create-repo` only ever creates `meta/`
and `data/` there. `create-repo` does not create missing parent directories for the root itself - a
typo in the path fails outright rather than silently creating a deep, unintended directory tree.

`meta/` holds the SQLite database file (`repository.sqlite3`) plus whatever sidecar files SQLite
itself creates alongside it during ordinary operation (`-wal`/`-shm` in WAL mode). Creation builds
this directory's full contents in a sibling staging directory, `meta.tmp/`, first - applying the
schema migration and inserting the initial `repository_settings` row (see DESIGN-METADATA-009 in
[`metadata-schema-with-contents-table.md`](metadata-schema-with-contents-table.md)) against the
staging copy - and only renames `meta.tmp/` to `meta/` once that is fully committed and the
connection closed. `fs::rename` is atomic on the same volume on both Windows and POSIX, so a
process killed at any point before the rename leaves at most a `meta.tmp/` directory behind and no
`meta/` at all; one killed after the rename leaves a complete, valid `meta/`. Either way, a repeated
`create-repo` attempt sees an unambiguous state to act on: a leftover `meta.tmp/` is, like any other
unexpected content, not itself resumed or cleaned up automatically - it blocks a plain retry the same
way REQ-CLI-005's ordinary "target must be empty" rule does, and needs to be removed manually first.
Not resuming a killed run in place is deliberate: doing so would need to first re-verify exactly how
far that run got, rather than simply starting over.

Whether a given root directory already holds a repository is decided by whether `meta/` exists
there, not whether the root directory itself exists - this is what lets `create-repo` accept an
already-existing, empty root directory (see REQ-CLI-005) while still refusing to reinitialize a
directory that already holds one. `data/` is created directly, with no staging: unlike `meta/`, an
empty or partially populated `data/` left behind by an interrupted creation is harmless clutter, not
a correctness risk - nothing in `meta/` can reference bytes in `data/` until `meta/` itself exists,
so a `data/` directory alone, with no `meta/` next to it, is never mistaken for a valid repository.

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
