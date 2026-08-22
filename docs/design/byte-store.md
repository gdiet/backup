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
back) and reusable regardless of how allocation ends up being decided - see
DESIGN-STORE-003 below, not yet settled.

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

## DESIGN-STORE-003: Where allocation and reclaim logic lives
Status: draft

Not yet decided where the logic answering "which byte range is free to write new content into"
and "which ranges did reclaiming just free up" actually lives - a dedicated module inside `db`
(closest to `chunk_extents`, the data it is derived from), a new small crate of its own, or inside
`cli`. REQ-STORAGE-004 (reclaim) and REQ-STORAGE-005 (compaction) in
[`../../requirements/functional/storage.md`](../../requirements/functional/storage.md) both need
this, and neither is settled yet either (`Status: draft`).

Revisit once REQ-STORAGE-004/005 themselves settle, and once DESIGN-METADATA-003's single
coordinated writer (in [`metadata-storage.md`](metadata-storage.md)) is actually implemented -
whether the allocator needs its own concurrency handling at all depends on whether more than one
thread can ever reach it concurrently, which that decision governs.
