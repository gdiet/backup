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
Status: agreed
Importance: must

Content is deduplicated at a granularity smaller than a whole file (content-defined chunking), so
two files that mostly overlap but differ in a few places still share most of their storage, not
just files that are byte-for-byte identical.

Rationale: an alternative of deduplicating only byte-identical whole files was considered and
rejected — it misses the common case of large files that change slightly between backups (e.g. a
growing log or a lightly edited document), which then cost full additional storage on every run.

### REQ-STORAGE-003: Configurable chunking granularity
Status: agreed
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

A soft-deleted entry (REQ-TREE-002 in [`tree.md`](tree.md)) only becomes eligible for reclamation
once it has stayed soft-deleted for at least a caller-chosen minimum age, defaulting to reclaiming
immediately (no minimum) when a caller does not specify one. This lets a routine or scheduled
reclamation run preserve a recent-deletion recovery window on its own, without the operator having
to time reclaim runs around it manually.

Rationale: without reuse, a repository under continuous use (files added and removed over years)
would grow monotonically even though its live content stays roughly constant. Without a
caller-chosen minimum age, an operator who wants both routine, automated reclamation and a
guaranteed recovery window for a recent accidental deletion would have to choose one or the other,
or build the timing logic themselves outside the tool.

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
Status: agreed
Importance: should

Stored content bytes are kept in a plain, simple on-disk layout rather than a proprietary or
compressed opaque format, so that even in a worst-case metadata-loss scenario, there is still a
realistic chance of recovering stored data by inspecting the storage directly. Bounded by sub-file
deduplication (REQ-STORAGE-002) itself: a file's chunks are not guaranteed to be stored
contiguously, so without metadata the plain layout does not reveal a file's chunk boundaries or
order - only that bytes are stored unencrypted and uncompressed, not hidden behind an opaque format.

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

### REQ-STORAGE-008: Repository creation date
Status: agreed
Importance: should

A repository records its own creation date, set once when the repository is created and never
changed afterward — independent of any individual file's modification time, and not derived from
the tree's root entry's own `time` field (deliberately superseded by ordinary modification-time
semantics — see "Root entry's seed `time`" in
[`../../docs/design/metadata-storage.md`](../../docs/design/metadata-storage.md)). Exposed as part
of repository statistics (REQ-QUERY-003 in [`query.md`](query.md)). For a repository migrated from
a Scala predecessor (REQ-MIGRATION-001 in
[`repository-migration.md`](repository-migration.md)), this is the source repository's own root
entry's `time` — confirmed against a real production Scala database that its schema never updates
that value after creation, so it reliably reflects that repository's own true creation date.

Rationale: knowing how long a repository has existed is basic operational context, the same kind
of fact REQ-QUERY-003 already gathers.
