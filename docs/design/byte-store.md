# Byte Store

How stored content bytes (chunk/content payloads, addressed by the metadata database's
`chunk_extents` in [`metadata-schema-with-contents-table.md`](metadata-schema-with-contents-table.md))
are physically laid out under `data/` (REQ-STORAGE-007 in
[`../../requirements/functional/storage.md`](../../requirements/functional/storage.md),
[`repository-layout.md`](repository-layout.md)), and how the crate providing this is shaped.

## DESIGN-STORE-001: On-disk layout matches Scala's own exactly
Status: decided

A single flat position (`u64` byte offset into the store as a whole) addresses every byte ever
written. The file physically holding a given position is `<dir1>/<dir2>/<file-start>` under
`data/`, each file exactly `100_000_000` bytes: `dir2 = (position / 100_000_000 / 100) % 100`,
`dir1 = position / 100_000_000 / 100 / 100`, `file-start` the position's file-aligned start,
written as a zero-padded 10-digit decimal. A read or write spanning a file boundary splits
transparently inside the store - nothing calling it needs to know file boundaries exist. Confirmed
directly against the real Scala implementation's own store: the same constant, the same
three-level division, the same zero-padded decimal filename - not approximately similar, the
identical formula.

This is chosen deliberately to match Scala's own layout, not arrived at independently and found to
coincide: REQ-MIGRATION-002 in
[`../../requirements/functional/repository-migration.md`](../../requirements/functional/repository-migration.md)
requires migrating an existing Scala repository without wholesale recopying its stored bytes, and
defers the concrete mechanism to `migration/from-scala.md`. An identical addressing formula is what
makes that possible as directly as REQ-MIGRATION-002 asks for: an adopted repository's `data/`
directory needs nothing rewritten at all, only new metadata built by rereading already-placed
bytes through unchanged addressing.

The scheme is otherwise independent of everything above it - chunk boundaries, hash algorithm,
whole-file-vs-content-defined chunking - it is pure physical byte placement, so REQ-STORAGE-002's
chunk-level deduplication (a departure from Scala's own whole-file granularity) changes nothing
about it. `100_000_000` itself is arbitrary from first principles - any fixed bound satisfies
REQ-STORAGE-007 - but is not free to change independently of the migration goal above: a different
file size would place the same position in a different physical file, breaking direct reuse of an
existing Scala `data/` directory.

### Independently justified by REQ-OPERABILITY-002, not just Scala compatibility

Even without the migration argument above, large fixed-size segments serve REQ-OPERABILITY-002's
"mirrorable with generic file-sync tools" goal (in
[`../../requirements/non-functional/operability.md`](../../requirements/non-functional/operability.md))
better than the seemingly more obvious alternative - one immutable file per chunk, see "Alternative
considered and rejected" below - given this project's actual scale and expected usage.

A size/mtime-based sync tool cannot tell that only the last few bytes of a growing segment changed;
it re-transfers that whole segment. This wastes at most one segment's worth of transfer
(`100_000_000` bytes, roughly half that on average) - a cost bounded by the segment size alone,
independent of the repository's total size or how long it has been since the last sync. What it is
*not* independent of is sync frequency relative to write volume: for an occasional sync against a
repository that has since grown by many times a single segment's size (a personal backup
repository's expected pattern), this waste is a rounding error against the genuinely new data
already being transferred; for frequent, small-increment syncs, the same fixed waste would be
proportionally far more expensive, since there is little genuinely new data to amortize it against.

### Alternative considered and rejected: a new address scheme, translated during migration

Choosing a different (perhaps "better" by some measure) layout and translating positions during
Scala migration was considered and rejected: migration would then need to rewrite every stored
byte into the new layout, exactly the wholesale recopy REQ-MIGRATION-002 exists to avoid - for a
layout property (file size, directory sharding depth) that has no identified benefit over the
existing one to justify that cost.

### Alternative considered and rejected: one file per chunk

One immutable file per chunk (keyed by `chunks.id`, never modified again once written, no shared
growing segments) would eliminate the waste described above entirely - an unchanged file is
skipped exactly by a sync tool, never blindly re-transferred - but was rejected on the other half
of the same trade-off: its file count scales with total data volume divided by chunk size, not by
a large fixed segment size. Content-defined chunking's own default lands chunks around 1 MiB;
against a multi-terabyte repository, that is two or more orders of magnitude more files than the
segment scheme above. A sync tool's own comparison pass has to stat every one of those files on
*every* run, regardless of how little actually changed since the last one - a cost proportional to
total repository size, paid in full each time, unlike the segment scheme's bounded,
sync-frequency-dependent waste above.

## DESIGN-STORE-002: The store crate is a dumb byte mover; allocation lives elsewhere
Status: decided

The store crate's entire public surface is reading and writing raw bytes at a given position, and
truncating the store to a given length - no awareness of chunks, content identity, deduplication,
or which byte ranges are currently free. Deciding *where* to write (REQ-STORAGE-004's reclaim and
reuse of freed ranges) and deciding *what* to write (chunk/content identity, REQ-STORAGE-001/002)
are both concerns of the caller, not this crate.

A read whose backing bytes are missing or short reports that fact back to the caller explicitly
(not a bare `io::Result<()>` that succeeds regardless) rather than silently substituting zero bytes
- the store-layer instance of REQ-MOUNT-005's "fail visibly by default" principle in
[`../../requirements/functional/mount.md`](../../requirements/functional/mount.md): this is the
layer that first discovers a gap, so it is the layer that must not paper over it before anything
above (REQ-INTEGRITY-002's remediation, or a mount's own opt-in zero-fill) ever gets a chance to
react to it. Reported as the affected file's own path (relative to the store's data directory),
not merely a byte count: REQ-INTEGRITY-002's remediation needs to know *which* files are affected
to act on them, not just how much is missing in total.

Rationale for the split itself: a small, single-purpose byte-mover with no dependency on the
metadata database is straightforward to test in isolation (feed it positions and bytes, read them
back) and reusable independent of allocation, which lives elsewhere - see DESIGN-STORE-003 below.

Opened with an explicit read-only flag, checked once at construction rather than per call:
a caller that only ever intends to read (`list`, `stats`, `check`, ...) opens the store that way,
and a write or truncate attempted against it fails immediately
(`io::ErrorKind::PermissionDenied`) rather than silently succeeding - a defensive guard against a
bug elsewhere accidentally mutating the store during what was meant to be a read-only operation,
not a substitute for REQ-MAINTENANCE-004's separate, cross-process exclusive lock in
[`../../requirements/functional/maintenance.md`](../../requirements/functional/maintenance.md),
which this has no awareness of.

`truncate_to` never scans the `data/` tree to discover what currently exists. The numbering
scheme itself (DESIGN-STORE-001) already says which file, and which `dir1`/`dir2` directory, a
given `len` falls into - so anything numbered higher than that is beyond `len` by construction,
whether or not it happens to exist, and a whole `dir1`/`dir2` numbered higher can be removed
outright (`remove_dir_all`) without inspecting its contents at all. Only the straddling file's own
`dir1`/`dir2` is ever actually listed, to find sibling files/directories above the boundary within
it. No assumption about gaps in what actually exists is needed as a result - unlike an approach
that probes sequentially outward and stops at the first missing file, which would (incorrectly)
treat a gap as the end of the store.

## DESIGN-STORE-004: Read handle cache

Status: draft

A small per-OS-thread LRU (capacity 8) of open read file handles, keyed by absolute path -
`read` reuses a cached handle for the same file across calls on the same thread rather than
opening one fresh every time. `write` does not get this treatment.

Measured directly (a runnable benchmark example cites this decision, not the reverse - see
"Reference Direction: One Way Only" in `docs/design/README.md`), within the cache's own capacity -
the case it exists for: roughly 2x faster than opening fresh per call, on this development
machine's local filesystem. Measured again against a real network filesystem mount over Wi-Fi to
another machine: consistently far larger, 130x-190x across four independent runs (roughly 150x on
average) - the full round trip a network mount pays for every fresh open dominates far more there
than on local disk, exactly the mount-driven small-read scenario this cache exists for. No write
cache was built to measure against - see "Alternative
considered and rejected" below for why, reasoning rather than a benchmark result, since there is
no write-side prototype here to have measured. The read/write asymmetry itself is expected: a write
call's own I/O (writing potentially a whole chunk at once) already dominates the cost of opening a
handle, but a single mount read at the FUSE/WinFSP layer can be much smaller than an average
chunk - bounded by whatever block size the kernel or client requests, not by chunk boundaries - so
reading one chunk back can mean many small read calls each independently paying the open cost
unless it is cached.

Deliberately per-thread, not one shared cache: avoids introducing a lock/contention point on what
is otherwise a fully lock-free read path.

### A growing/shrinking thread pool means a periodically cold cache - accepted for now

`mountfs`'s own FUSE dispatch (`crates/mountfs/src/linux/mod.rs`) does not pin a specific worker
thread count or single-threaded mode - it runs under libfuse's default multithreaded loop, whose
pool can grow under load and, depending on version/configuration, shrink again once idle (WinFSP's
own dispatch is presumed similar, not verified). A freshly spawned worker thread starts with an
empty cache, so pool churn means some fraction of requests periodically hit a cold cache, not just
requests on a thread's very first use. Accepted for now rather than designed around: this cache
exists for REQ-PERFORMANCE-003's steady-state per-call overhead, not to smooth over pool churn
itself, and nothing currently measures how much churn a real mount session actually sees. Revisit
if that turns out to matter once `store` is actually wired into a mount.

### A shrinking thread pool does not leak cached handles - verified for Linux, not for Windows

When a worker thread that has used this cache exits, Rust's `thread_local!` destructor runs the
same way regardless of which library created that thread: on Linux, it rides on `pthread`'s own
TLS-destructor mechanism, which fires for any `pthread` that exits cleanly, including libfuse's own
C-created worker threads, not only ones this process's own Rust code spawned directly. That drops
every cached `File`, closing its descriptor - a shrinking pool does not leak handles, as long as
the underlying OS thread actually exits cleanly rather than being forcibly terminated. Confirmed by
reasoning about Linux's `pthread`-based TLS specifically, not yet confirmed for WinFSP's own
worker-thread lifecycle on Windows, where TLS destructors are documented to not run for some
abnormal termination paths (e.g. `TerminateThread`) - revisit, ideally with an actual check against
a real WinFSP mount under load, once `store` is wired in on Windows.

### A cached handle is never invalidated - depends on the caller

A handle stays cached for as long as its thread's LRU has room for it, with no mechanism to
notice the underlying file was later removed or shrunk by `truncate_to` on a different call. On
the platforms this project targets, deleting a file does not invalidate handles already open to
it - a stale cached handle would go on returning the file's old content instead of correctly
reporting it missing. This is only safe because nothing is expected to call `truncate_to` for a
range a concurrent read could still legitimately want - a caller/allocator responsibility
(DESIGN-STORE-003 below), not something this cache enforces itself.

### Alternative considered and rejected: also caching write handles

A combined read/write cache (shared handles, synchronized for concurrent access) was considered
and rejected on reasoning, not a benchmark result - no write-side prototype was built to measure:
a `write` call's own I/O (the whole chunk, in one call) already scales with the actual bytes
transferred, unlike the fixed per-call cost of opening a handle, so the larger a chunk is, the
less a cached handle could plausibly save relative to the write itself - while adding real
complexity (synchronization across calls sharing a handle). Revisit if evidence of an actual
write-side bottleneck ever emerges - this is not that evidence, only an argument against
building the alternative speculatively.

## DESIGN-STORE-005: Lazy directory creation on write

Status: draft

`ByteStore::write` tries to open its target file directly first, and only creates its parent
`dir1`/`dir2` directories on a `NotFound` error from that open - not unconditionally before every
write. Confirming a directory already exists still costs a syscall even when there is nothing to
do, and after the first write into a given `dir1`/`dir2` pair, every later write into it would
otherwise pay that cost for no reason.

Measured directly: roughly 1.3-1.5x faster once the target directory already exists (the
steady-state case after the first write), on this development machine's local filesystem. Measured
again against a real network filesystem mount over Wi-Fi: no measurable benefit there - four
independent runs averaged almost exactly 1.0x (0.87x-1.15x), well within the link's own run-to-run
jitter. The round trip this skips is small relative to the total latency a network mount already
pays for the write itself, unlike on local disk where syscall count is what dominates the cost.

Unlike the read handle cache above, this has no real downside worth weighing against - no
cross-call state to keep correct, no invalidation question, just a cheaper path to the exact same
guarantee (the directory exists by the time the file is opened). Kept regardless of the network
mount result above: it costs nothing extra to keep, and the case it measurably helps - any
locally-attached storage, not only an internal disk but also, for many real backups, a fast
external USB drive used as the backup medium - is this project's actual write path, not a network
mount. The distinction the measurement above actually turns on is local-filesystem-call versus
network-round-trip, not internal-versus-external storage: a USB-attached drive is still reached
through the same local filesystem call path as an internal disk, with no network protocol in
between, so the syscall-count argument applies to it the same way.

## DESIGN-STORE-003: Where allocation and reclaim logic lives
Status: decided

A dedicated module inside `db` (parallel to `tree.rs`), not a new crate of its own or a place
inside `cli`, answers "which byte range is free to write new content into" and "which ranges did
reclaiming just free up" - REQ-STORAGE-004 (reclaim) and REQ-STORAGE-005 (compaction) in
[`../../requirements/functional/storage.md`](../../requirements/functional/storage.md) both need
this; this decision is about where the logic lives, independent of their own remaining details
(both still `Status: draft`).

The free-range state this logic answers from is derived entirely from `chunk_extents`, which
already lives in the metadata database - keeping the module there lets an allocation decision and
the extent row recording it happen inside the same database transaction, real atomicity instead of
a two-phase commit coordinated across a crate boundary. It also inherits DESIGN-METADATA-003's
single coordinated writer (in [`metadata-storage.md`](metadata-storage.md)) for free: every writer
already goes through `db::Repository`'s one connection, so the allocator needs no concurrency
mechanism of its own.

This module still only decides *where* - `crates/store`'s `ByteStore` remains the one place that
actually reads or writes bytes at a given position (DESIGN-STORE-002 above); `db` does not gain a
dependency on `store` for this. Whatever orchestrates a real content write (a future mount write
path, an ingest path) is the one holding both: it reserves a position from `db`, writes the bytes
through `store`, then records the resulting extent row back in `db` - an ordering this decision
makes available, not one it enforces.

### Alternative considered and rejected: a new dedicated crate

Would only earn its own crate the way `cdc`/`mountfs` do (see the crate-structuring guidance in
`.claude/rules/rust-code-quality.md`) if the logic were genuinely reusable outside this specific
application. It is not: the allocator is intrinsically shaped by `chunk_extents`' own schema and
this project's own REQ-STORAGE-004/005/007 semantics, not a generic byte-range allocator someone
else could plausibly reuse.

### Alternative considered and rejected: living inside `cli`

Both a future mount write path and any future CLI-driven ingest path need this logic, and both
already reach `db::Repository` today. Putting the allocator there instead of `cli` means one
implementation both entry points share, rather than `cli` re-coordinating `db` and `store`
separately at each call site - which would also risk exactly the write-then-crash-before-commit
inconsistency REQ-TREE-006 in [`../../requirements/functional/tree.md`](../../requirements/functional/tree.md)
exists to rule out.
