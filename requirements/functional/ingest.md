# Ingest

Ingest covers directed, one-shot import of external filesystem content into the repository —
explicit source paths copied to a target path, as a discrete operation. This is distinct from
writing into the repository through a mounted view (see REQ-MOUNT-003 in
[`mount.md`](mount.md)), where the repository behaves like an ordinary filesystem for ongoing,
ad-hoc reads and writes rather than a bulk copy step.

### REQ-INGEST-001: Store files and directories into the repository
Status: draft
Importance: must

One or more files/directories from the real filesystem can be copied into the repository, with
their content deduplicated along the way.

Rationale: bringing content in from the real filesystem is what gives every other operation —
restoring, browsing, checking, mounting — something to work with.

### REQ-INGEST-002: Per-directory exclusion rules
Status: draft
Importance: should

A source directory can declare files and subdirectories to exclude from being stored, via a small
pattern-based rule file placed in that directory; rules apply to the directory they are found in
and propagate into matching subdirectories.

Rationale: not everything under a source tree is worth backing up (caches, temp files, logs), and
that decision is naturally scoped to the directory it applies to rather than a single
repository-wide configuration.

### REQ-INGEST-003: Accelerated re-ingest via reference
Status: draft
Importance: should

An earlier backup of largely-the-same source tree can be supplied as a reference; a source file
matching a same-named, same-size, same-modified-time file under the reference is linked to that
existing content without re-reading, chunking, or hashing it.

Rationale: even with perfect deduplication, an unchanged file still costs a full read on every
backup run just to discover that it deduplicates — for the common case of a mostly-unchanged tree
backed up repeatedly (e.g. nightly), skipping that discovery cost entirely is a large, practical
speed difference.

### REQ-INGEST-004: Resilience to per-item failures
Status: draft
Importance: must

A source file or directory that cannot be read (a permission error, something that disappears
mid-run) is logged as a warning and skipped; the rest of the run completes rather than aborting.

Rationale: a single unreadable file (a locked file, a permissions edge case) should not be able to
prevent backing up everything else that is readable.
