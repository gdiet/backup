# Storage

### REQ-STORAGE-001: Content-addressable deduplication
Status: agreed
Importance: must

Identical content is stored exactly once, regardless of how many files or paths reference it;
storing the same content again costs a small, constant amount of metadata, not another copy of
the bytes.

Rationale: this is the core value proposition — keeping many full backups over time without
storage growing anywhere near as fast as a plain copy would.

### REQ-STORAGE-002: Sub-file (chunk-level) deduplication
Status: draft
Importance: must

Content is deduplicated at a granularity smaller than a whole file (content-defined chunking), so
two files that mostly overlap but differ in a few places still share most of their storage, not
just files that are byte-for-byte identical.

Rationale: an alternative of deduplicating only byte-identical whole files was considered and
rejected — it misses the common case of large files that change slightly between backups (e.g. a
growing log or a lightly edited document), which then cost full additional storage on every run.

### REQ-STORAGE-003: Configurable chunking granularity
Status: draft
Importance: should

The chunking strategy - content-defined chunking (REQ-STORAGE-002) or a cheaper whole-file mode,
for cases where sub-file matching is not worth its overhead - is configurable at repository
creation and fixed for the repository's lifetime. For content-defined chunking specifically, its
target chunk size is configurable and fixed the same way; whole-file mode has no target size to
configure.

Rationale: smaller chunks find more overlap between similar files at the cost of more metadata
overhead per byte stored; the right trade-off depends on the data being stored, not on the
software. Whole-file mode trades away REQ-STORAGE-002's sub-file matching entirely, for cases where
even that overhead is not worth paying.

### REQ-STORAGE-004: Space reclamation
Status: draft
Importance: must

Storage occupied by content no longer referenced by any file can be reclaimed. Reclaimed byte
ranges are tracked and reused by future writes before the store grows further, so repeated
delete/write cycles do not make storage grow without bound. Reclaiming a range makes it eligible
for reuse without itself altering any byte still on disk - this is what creates the staleness risk
REQ-MAINTENANCE-007 in [`maintenance.md`](maintenance.md) requires a warning for, whether or not
the range is ever actually reused afterward.

Rationale: without reuse, a repository under continuous use (files added and removed over years)
would grow monotonically even though its live content stays roughly constant.

### REQ-STORAGE-005: On-demand store compaction
Status: draft
Importance: should

Beyond reclaiming and reusing gaps internally, the store can be defragmented on demand — live
content relocated into a contiguous layout and the backing storage shrunk to match, actually
returning freed space to the underlying filesystem/OS. Unlike reclamation's gradual reuse, this
relocation itself immediately changes the physical position of content that is still live; see
REQ-MAINTENANCE-007 in [`maintenance.md`](maintenance.md) for the resulting staleness risk to an
already-taken metadata backup.

Rationale: reclamation alone keeps growth bounded but never gives space back; an operator who
needs the disk space back (not just bounded future growth) needs an explicit, on-demand path to
get it, separate from routine operation.

### REQ-STORAGE-006: Durable, independently recoverable storage format
Status: draft
Importance: should

Stored content bytes are kept in a plain, simple on-disk layout rather than a proprietary or
compressed opaque format, so that even in a worst-case metadata-loss scenario, there is still a
realistic chance of recovering stored data by inspecting the storage directly.

Rationale: for a backup system specifically, resilience against metadata corruption matters more
than the storage-efficiency or convenience a more complex format might offer.

### REQ-STORAGE-007: Bounded, append-mostly data layout
Status: agreed
Importance: must

Stored byte content lives in fixed-maximum-size files, each filled sequentially; once a file is
full, it is not modified further except by an explicit space-reclamation or compaction step. At
any time, only a small, bounded number of the most recently created files can still be receiving
new data — every other data file is effectively immutable.

Rationale: bounding how much already-written data can still change, and never rewriting a file
once it is sealed, is what makes it possible for generic, repository-unaware tools to tell "this
file has not changed" cheaply and correctly (see REQ-OPERABILITY-002 in
[`../non-functional/operability.md`](../non-functional/operability.md)) — an unbounded or
frequently-rewritten layout would defeat that.
