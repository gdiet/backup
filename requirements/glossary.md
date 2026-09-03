# Glossary

### Chunk
A content-defined (or, in whole-file mode, single) contiguous piece of a file's bytes, individually
hashed and deduplicated - the unit REQ-STORAGE-002 in [`functional/storage.md`](functional/storage.md)
deduplicates at.

### Gap
An unused byte range within the store, left behind once every chunk that used to occupy it has been
reclaimed - a candidate a future write reuses before extending the store further
([`functional/storage.md`](functional/storage.md)'s REQ-STORAGE-005).

### Purge
Hard-deleting a soft-deleted entry (REQ-TREE-002 in [`functional/tree.md`](functional/tree.md))
once it has aged past a caller-chosen minimum, ending its recoverability for good
(REQ-STORAGE-004 in [`functional/storage.md`](functional/storage.md)). Distinct from reclaim below:
purge acts on the tree entry itself, reclaim on the storage a purge frees.

### Reclaim
Marking a byte range no longer referenced by any live content as free for reuse by a future write
(REQ-STORAGE-004 in [`functional/storage.md`](functional/storage.md)). Reclaiming a range does not
itself alter or erase the bytes still physically on disk there, only their availability for being
overwritten later - see REQ-MAINTENANCE-007 in
[`functional/maintenance.md`](functional/maintenance.md) for the resulting staleness risk to an
already-taken metadata backup.

### Reference (ingest)
An earlier ingest's target tree, supplied to a later ingest run to accelerate it: a source file
matching a same-named, same-size, same-modified-time file under it is linked to that existing
content without being read again (REQ-INGEST-003 in
[`functional/ingest.md`](functional/ingest.md)). Qualified here because "reference" is also used
in its ordinary English sense throughout these documents - this entry covers only the ingest-specific
mechanism.

### Repository
A DedupFS backup store: its metadata (the file/directory tree and deduplication index) and its
stored byte content, addressed together as one unit and normally living under one repository
directory.
