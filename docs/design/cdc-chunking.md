# CDC Chunking

Deduplication (REQ-STORAGE-002 in
[`../../requirements/functional/storage.md`](../../requirements/functional/storage.md)) splits
file content into chunks using content-defined chunking (CDC): each chunk boundary is found by a
Gear-hash-style rolling fingerprint, using a normalized scheme where the probability of a boundary
increases the longer the current chunk has been growing — keeping the resulting chunk sizes
clustered around a configurable target instead of spread across a long tail. The fingerprint's
dependence on preceding bytes is bounded and provable, not merely "local": each byte's contribution
decays by one bit-position per subsequent byte processed and is fully gone once as many further
bytes have been processed as the fingerprint register is wide (31 bytes for a 32-bit register with
31-bit table values, for example) — a boundary decision depends on a short, fixed-size trailing
window of content, never on the whole stream since the last boundary.

## DESIGN-CDC-001: Why content-defined, not fixed-size, chunking

Status: implemented

Fixed-size chunking (splitting every N bytes) breaks under insertion or deletion: a single byte
added near the start of a file shifts every following chunk boundary, so a file that changed only
slightly re-chunks as entirely new content on the next backup. Content-defined boundaries are a
function of local content rather than a running byte count, so an edit only disturbs the one or
two boundaries nearest it — everything before and after keeps the same boundaries, and therefore
the same chunk hashes, as before the edit. For a backup workload with files that grow or change
incrementally (logs, documents, databases), this is what makes chunk-level dedup actually pay off
run over run, not just on the first backup of an unchanged tree.

## DESIGN-CDC-002: Why normalized chunking, not a plain fixed-window rolling hash

Status: implemented

A fixed-window rolling hash (boundary decided purely by a hash of the last ~48-64 bytes, with no
memory of how long the current chunk has grown) can be evaluated at arbitrary offsets
independently — in principle parallelizable across a single large file, at the cost of a wider,
exponentially-tailed chunk size distribution (some large, resistant to dedup, would occur where a
normalized scheme would have forced a boundary sooner). Normalized chunking gives a tighter,
more predictable size distribution in exchange for being inherently sequential per stream:
whether a boundary is even eligible to be considered at a given position depends on how many bytes
have accumulated since the last one.

### Alternative considered and rejected: intra-file parallel chunking (forced boundaries at fixed super-block splits)

A single very large file could be split into large fixed-size super-blocks up front, each chunked
independently and in parallel, accepting a forced (non-content-defined) boundary at each split.

Rejected: this project's workload is many files and large volumes of data in aggregate, not a few
very large individual files (see "Core" in
[`../../requirements/goals-non-goals.md`](../../requirements/goals-non-goals.md)) — cross-stream
parallelism (REQ-PERFORMANCE-002 in
[`../../requirements/non-functional/performance.md`](../../requirements/non-functional/performance.md))
already delivers the available speedup for that shape of workload. Forcing extra non-content-defined
boundaries would cost dedup quality for a parallelism gain this project has no known use for. A
Gear-hash-style normalized chunker of this kind measures at roughly 2.8 GB/s single-threaded on
modern hardware, well above realistic backup-target I/O speeds (external drives, network shares),
so single-thread chunking speed is not expected to be the bottleneck in the first place.

Revisit if: a workload with very large individual files on storage fast enough for single-thread
chunking speed to become the bottleneck emerges (e.g. large-file transfer between very fast local
drives) — no such case is known today.

## DESIGN-CDC-003: An external content-defined-chunking library

Status: implemented

Mature options exist, but couple chunking to a `Read`/`AsyncRead`-based streaming model — they own
the I/O.

Rejected: this project's write paths — in particular a read-write mount's callback-driven writes —
need to feed arbitrary-sized byte slices as they arrive, without first adapting every write source
into a `Read` implementation. A chunker exposing a push-based interface (feed bytes via any number
of calls of any size, get back completed chunk boundaries; no ownership of how bytes arrive) fits
that integration shape directly, without an adapter layer.

Revisit if: a mature library offering an equivalent push-based, I/O-agnostic interface (or a
significant, benchmark-demonstrated throughput advantage large enough to justify building that
adapter layer) becomes available.
