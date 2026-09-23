# `settle`'s whole-file mode buffers unbounded memory; `ingest` also has no write buffering or parallelism

**Status**: all three original concerns are now resolved - the memory-bound problem, the
cross-file-parallelism gap, and (see below) the write-buffering/latency-hiding question, decided
against building for now rather than left unaddressed. Kept under the original title for
continuity with its own history.

**Noted**: 2026-09-04, during the informal WebDAV-network-drive performance exploration
(`performance/notes/2026-09-04-julius-h-webdav-network-drive.md`), after the developer's own
follow-up question about whether `--spill-dir` (DESIGN-MOUNT-018, added the same session) should
also apply to `dfs ingest`.
**Updated**: 2026-09-04, same day, in a follow-up conversation - a question about how a 4 GB file
is handled under `ingest` with `chunking=none` surfaced what the developer identified as this
TODO's actual core problem (below), distinct from the original network-latency finding (kept
further down, still open and still relevant).
**Size**: medium/large - confirm with the developer before starting. This spans three levels the
developer explicitly asked for: a **requirement**, a **design** decision, and the **implementation**
itself - not a one-line fix.
**Context**: `crates/cli/src/settle.rs` (`Settler::feed`/`complete_chunk`/`finish`, the shared
settle engine), `crates/cdc/src/lib.rs` (`SingleChunkChunker`), `crates/cli/src/ingest.rs`
(`ingest_file`, `run`), `crates/cli/src/write_cache.rs`/`crates/cli/src/settle_pool.rs` (the mount's
own write path, which settles through the same engine), `docs/design/mount-write-path.md`
(DESIGN-MOUNT-006/010), `requirements/functional/storage.md` (REQ-STORAGE-003, the chunking-strategy
requirement `--whole-file` is part of), `requirements/non-functional/performance.md`
(REQ-PERFORMANCE-*, a plausible but not yet decided home for a new memory-bound requirement).

## The core problem: `--whole-file` mode buffers the entire content in memory before writing anything

**Resolved** via `developer-todos/done/ram-budget-and-backpressure-redesign.md`'s implementation:
REQ-STORAGE-003 no longer offers a whole-file chunking mode at all (content is always
content-defined-chunked), so `chunk_buffer` cannot exceed the repository's own configured maximum
chunk size (96 MiB at the 23-bit ceiling) regardless of input size - see DESIGN-MEMORY-001 in
[`../../docs/design/ram-budget.md`](../../docs/design/ram-budget.md) and DESIGN-INGEST-001 in
[`../../docs/design/ingest-bounded-pipeline.md`](../../docs/design/ingest-bounded-pipeline.md). The
"three levels" (requirement/design/implementation) below were all completed as part of that
effort; kept here for the historical trail, not as a still-open ask.

Verified by code inspection (not yet by an actual large-file run). With `cdc_target_size_bits =
None` (`--whole-file`), `settle::settle`'s chunker is `cdc::SingleChunkChunker`
(`crates/cdc/src/lib.rs:289-310`), which never reports a chunk boundary from `next()` - only
`flush()`, once the entire input has already been fed, reports one chunk covering the whole thing.
In `Settler::feed` (`crates/cli/src/settle.rs:105-118`), this means `complete_chunk()` - the only
place that hashes a chunk, links/reserves it in `crates/db`, and writes its bytes to `crates/store`
- is never reached mid-stream: every 4 MiB `READ_WINDOW` (`:20`) read from the source just gets
appended to `chunk_buffer` (`extend_from_slice`, `:116`), which keeps growing for as long as the
file does. Only `finish()`, after the very last window, triggers the single `complete_chunk()` call
that finally hashes and writes the whole thing.

Concretely: ingesting (or mount-writing) a 4 GB file in `--whole-file` mode holds that entire 4 GB
as a single `Vec<u8>` in the process's own heap before a single byte reaches the store. This is not
scoped to `ingest` specifically - `settle::settle`/`Settler` is the one engine both `ingest_file`
and the mount's own background settle job (`settle_pool.rs`) go through, so a large whole-file write
through a read-write mount hits exactly the same unbounded growth; DESIGN-MOUNT-010's write cache
spills the *incoming* `write()` calls to disk once its own budget is exhausted, but that happens
before `settle::settle` ever runs - it does not change what `Settler` itself does with the content
once settling starts. CDC-chunked mode does not have this problem: a chunk boundary is found
periodically (based on `cdc_target_size_bits`), so `chunk_buffer` never holds more than roughly one
chunk's worth of data (a few MB at the default target size), regardless of the file's total size.

## What the developer wants tracked here

Explicit requirement, the developer's own framing: memory usage must not be allowed to grow
uncontrolled with input size - **unless** a deliberate investigation concludes that leaning on the
OS's own paging (letting a large buffer page out under memory pressure rather than this code
bounding it itself) is actually the best available answer here, in which case that trade-off gets
written down as a considered decision, not arrived at by default because nobody thought about it.
The developer's own expectation: the Scala implementation apparently already has this under
control, so this implementation should reach at least that bar too - not a reason to copy Scala's
specific approach (see `AGENTS.md`'s "This Is A Rewrite, Not A Port"), but a signal that "unbounded
growth is fine" is very unlikely to be the right answer here.

This needs work at three levels once picked up:

- **Requirement**: a memory-bound requirement (`requirements/non-functional/performance.md`,
  alongside REQ-PERFORMANCE-*, or a new area if that turns out to be a better fit) stating a bound
  applies to ingesting or mount-writing a single file regardless of its size - or, if OS paging is
  deliberately chosen instead after weighing it, a requirement/rationale that records that decision
  explicitly rather than leaving the bound unstated either way.
- **Design**: a `docs/design/` entry for how `settle`'s whole-file path actually achieves the
  bound - e.g. writing directly to the store in fixed-size pieces as bytes are read, rather than
  accumulating a `chunk_buffer` first (would likely need `crates/store`'s allocation to reserve a
  range of the final total size up front, or otherwise support an append-like growth pattern - not
  yet looked into). Worth checking how the Scala implementation actually solved this (see the
  `local-reference-worktrees` skill for a read-only checkout of it) as one input among others, not
  as a specification to carry forward unchanged.
- **Implementation**: once a design is settled, the actual code change to `crates/cli/src/settle.rs`
  (and possibly `crates/cdc`, if `SingleChunkChunker` itself needs to change how/when it reports
  chunk boundaries) plus a regression test that actually distinguishes bounded from unbounded
  behavior (e.g. asserting peak allocation stays within a bound for a large input, not just
  correctness of the written bytes - correctness already has coverage in `settle.rs`'s own tests).

## Original finding: `ingest` has no write buffering and no parallelism (network-latency angle)

The developer's own framing, quoted directly:

> Nein, ingest braucht --spill-dir nicht und hat auch keine Spillover-Fähigkeit. ingest_file ruft
> settle::settle direkt und synchron pro Datei auf (ingest.rs:344) - liest lokal, chunked/hasht,
> schreibt direkt und blockierend in den Store. Es gibt dort gar keine Zwischenspeicherung, von der
> aus überhaupt etwas "spillen" könnte; jeder Chunk geht sofort und ungepuffert auf H:. Das erklärt
> auch, warum CLI-Ingest bei großen Dateien so extrem langsam war: es hat von Anfang an die volle,
> ungepufferte Netzwerklatenz pro Chunk bezahlt - genau das, was der Mount-Write-Cache abfängt.
> Zusätzlich läuft ingest komplett sequenziell (kein available_parallelism), während der
> Mount-seitige Settle-Pool mit mehreren Worker-Threads parallel arbeitet - macht den ingest-Wert
> zu einer unteren Schranke, nicht direkt vergleichbar mit der Settle-Pool-Rate.

### What was actually observed

The informal WebDAV exploration this session showed `dfs ingest` had not finished writing even one
5 MB file's content 60 seconds into a two-file (10 MB) batch on a WebDAV-backed repository -
dramatically slower than the mount's own settle path (`--spill-dir`, DESIGN-MOUNT-010/006), even
once that path's real (unbuffered, settled) throughput was measured directly and found to be slow
in its own right (~0.17-0.21 MB/s, see the notes file above). Two independent factors compound in
`ingest`, neither present in the mount's write path:

- **No write cache at all** - `ingest_file_job` still calls `settle::settle` synchronously per
  file, reading from the local source and writing straight to `store::ByteStore` (which, for a
  repository on a slow medium, means straight onto that slow medium) with nothing buffering or
  absorbing the cost the way DESIGN-MOUNT-010's memory-then-local-SSD write cache does for a
  mounted session. **Decided against, for now** - see "Alternative considered and rejected:
  per-file read/persist pipelining" in
  [`../../docs/design/ingest-bounded-pipeline.md`](../../docs/design/ingest-bounded-pipeline.md): the
  reachable speedup from pipelining one file's read/hash against its own persist is bounded by a
  small constant factor (2x at best, only where the two stages take about equally long, falling
  toward no benefit as either dominates), and only reachable at all when a file runs effectively
  alone - cross-file parallelism already provides the same overlap whenever enough other files keep
  the worker pool busy. Real, but narrow: a batch dominated by very few large files, or any batch's
  natural tail-off as smaller files finish first.
- **No parallelism across files** - **resolved** by DESIGN-INGEST-001
  (`crates/cli/src/ingest.rs`'s `FileJobPool`): up to `min(ram_budget / max_chunk_size,
  available_parallelism())` files are now processed concurrently, one worker thread per file,
  mirroring `settle_pool.rs`'s `JobPool` shape.

### What is worth a closer look

Resolved while closing out this todo - see the design doc reference above for the write-buffering
question's own reasoning. Kept here for the historical trail:

- Whether `ingest`'s current single-threaded, unbuffered behavior is a real-world problem for its
  actual use case (REQ-INGEST-001's bulk import of a filesystem tree) or mostly shows up in an
  edge case like this session's slow-network-drive exploration - a large local-disk-to-local-disk
  ingest never pays anywhere near this cost. **Answered**: bounded to a modest constant factor even
  in the narrow case where it could matter at all (see above); not worth building preemptively.
- ~~Whether adding parallelism across files... would help meaningfully~~ - **done**: added via
  DESIGN-INGEST-001's `FileJobPool` (see above).
- ~~Whether a write cache/buffering layer makes sense for `ingest` at all~~ - **decided against,
  for now** (see above).
- If `ingest`'s current numbers are judged to be a real, worthwhile lower bound rather than a
  representative one, that is itself worth writing down somewhere more permanent than this todo -
  **done**: `docs/design/ingest-bounded-pipeline.md`'s new subsection is that permanent record.

## Done (2026-09-23)

The write-buffering question is the last of this todo's three original concerns to close: the
developer's own pipeline-speedup analysis (a two-stage `(a + b) / max(a, b)` ceiling, `2x` at best
where read/hash and persist take about equally long, and only reachable when a file runs
effectively alone) confirmed the reachable gain is a modest, bounded constant factor rather than
worth building preemptively - written up in `docs/design/ingest-bounded-pipeline.md`'s new
"Alternative considered and rejected: per-file read/persist pipelining" subsection, alongside the
existing "intra-file parallelism" one it sits next to. No code changes; decided against building,
not silently dropped.

