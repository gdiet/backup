# Query

### REQ-QUERY-001: List directory contents
Status: agreed
Importance: must

A directory's direct contents (name, kind, size, last-modified time) can be listed without
mounting the repository.

Rationale: browsing the repository is a routine operation that should not require the overhead and
platform-specific setup of a mount.

### REQ-QUERY-002: Find entries by name/path pattern
Status: agreed
Importance: must

Entries anywhere in the repository can be searched by a case-insensitive name/path pattern with
wildcard support, independent of which directory they are in.

Rationale: finding "that one file somewhere in years of backups" by name is a core use case that
listing directories one at a time does not serve well.

### REQ-QUERY-003: Usage statistics
Status: agreed
Importance: should

Repository-wide or path-scoped statistics are available on demand:

- item counts, the number of empty files, and the average file size
- the oldest and newest modification time among the files
- logical size (as if nothing were deduplicated) versus actual physical storage used
- the resulting deduplication ratio, together with the share of space it saves
- the number of distinct chunks the files use, and their average size

Physical storage counts the chunks that live files use. A path-scoped report counts them within
that path only.

Repository-wide only, not path-scoped, the report also gives:

- the repository age, derived from the repository's creation date (REQ-STORAGE-008 in
  [`storage.md`](storage.md))
- the chunking target size (REQ-STORAGE-003 in [`storage.md`](storage.md))
- the number of stored chunks and chunk extents, and the average chunk size
- the end of the stored data and the space below that end that no chunk uses
- the size of the metadata database
- the number of soft-deleted entries

Rationale: understanding how much deduplication is actually saving, and how a repository is
growing, is what tells an operator whether the system is working as intended and when to reclaim
space. The chunk and extent figures show what the chunking target size costs in metadata. The end
of the stored data and the unused space below it show what the data directory occupies, which can
exceed the physical storage that live files use.
