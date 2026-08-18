# Query

### REQ-QUERY-001: List directory contents
Status: draft
Importance: must

A directory's direct contents (name, kind, size, last-modified time) can be listed without
mounting the repository.

Rationale: browsing the repository is a routine operation that should not require the overhead and
platform-specific setup of a mount.

### REQ-QUERY-002: Find entries by name/path pattern
Status: draft
Importance: must

Entries anywhere in the repository can be searched by a case-insensitive name/path pattern with
wildcard support, independent of which directory they are in.

Rationale: finding "that one file somewhere in years of backups" by name is a core use case that
listing directories one at a time does not serve well.

### REQ-QUERY-003: Usage statistics
Status: draft
Importance: should

Repository-wide or path-scoped statistics are available on demand: item counts, logical size
(as if nothing were deduplicated) versus actual physical storage used, and the resulting
deduplication ratio.

Rationale: understanding how much deduplication is actually saving, and how a repository is
growing, is what tells an operator whether the system is working as intended and when to reclaim
space.
