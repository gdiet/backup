# Ingest

Ingest covers directed, one-shot import of external filesystem content into the repository —
explicit source paths copied to a target path, as a discrete operation. This is distinct from
writing into the repository through a mounted view (see REQ-MOUNT-003 in
[`mount.md`](mount.md)), where the repository behaves like an ordinary filesystem for ongoing,
ad-hoc reads and writes rather than a bulk copy step.

### REQ-INGEST-001: Store files and directories into the repository
Status: agreed
Importance: must

One or more files/directories from the real filesystem can be copied into the repository, with
their content deduplicated along the way.

Rationale: bringing content in from the real filesystem is what gives every other operation —
restoring, browsing, checking, mounting — something to work with.

### REQ-INGEST-002: Per-directory exclusion rules
Status: agreed
Importance: should

A source directory can declare files and subdirectories to exclude from being stored, via a small
pattern-based rule file placed in that directory; rules apply to the directory they are found in
and propagate into matching subdirectories.

Rationale: not everything under a source tree is worth backing up (caches, temp files, logs), and
that decision is naturally scoped to the directory it applies to rather than a single
repository-wide configuration.

### REQ-INGEST-003: Accelerated re-ingest via reference
Status: agreed
Importance: should

An earlier backup of largely-the-same source tree can be supplied as a reference; a source file
matching a same-named, same-size, same-modified-time file under the reference is linked to that
existing content without re-reading, chunking, or hashing it.

Rationale: even with perfect deduplication, an unchanged file still costs a full read on every
backup run just to discover that it deduplicates — for the common case of a mostly-unchanged tree
backed up repeatedly (e.g. nightly), skipping that discovery cost entirely is a large, practical
speed difference.

### REQ-INGEST-005: Preserved timestamps
Status: agreed
Importance: must

A stored file's or directory's modification time reflects the source's own modification time at
the moment it was read, not when the import ran.

Rationale: REQ-INGEST-003's unchanged-file detection depends on comparing against real source
modification times; and a directory populated by import would otherwise end up showing only "when
the import happened" (REQ-TREE-005 in [`tree.md`](tree.md)), silently losing the source's own
history the moment it is imported.

### REQ-INGEST-004: Resilience to per-item failures
Status: draft
Importance: must

A source file or directory that cannot be read (a permission error, something that disappears
mid-run) is logged as a warning and skipped; the rest of the run completes rather than aborting. A
caller (script or tool) can tell the three possible outcomes of a run apart afterward - complete
success, success with some items skipped as warnings, or abort on an error - without having to
parse log output to do so.

The distinction is which side fails. A *source*-read failure - the file being backed up cannot be
read - is exactly the per-item, skip-and-continue case above. A *backend* failure - writing
already-read content into the repository fails (the store write itself, not the source read) - is
not: it triggers the third outcome instead, an immediate, controlled abort of the whole run, not a
per-item skip.

Rationale: a single unreadable file (a locked file, a permissions edge case) should not be able to
prevent backing up everything else that is readable, but a caller automating ingest still needs a
reliable way to notice that some items were silently skipped, not just a human reading the log. A
backend failure gets the opposite treatment because it is not expected to be specific to the one
item being processed when it was discovered - continuing to feed more items into a repository that
just failed to accept a write risks repeating, not containing, the same failure.
