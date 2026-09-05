# Bounding Memory Use When Settling Whole-File (Non-CDC) Content

Open design question tracked from
[`../../developer-todos/ingest-has-no-write-buffering-or-parallelism.md`](../../developer-todos/ingest-has-no-write-buffering-or-parallelism.md)'s
"core problem" section: `--whole-file` mode (REQ-STORAGE-003's non-CDC chunking strategy) currently
holds an entire file's content in memory at once while settling it, with no bound tied to available
memory. This affects `dfs ingest` and a mounted read-write session's own background settle job
alike, since both settle through the same engine (`crates/cli/src/settle.rs`).

## Not yet decided: how the bound is actually achieved

Status: idea

### The problem, precisely

`settle::settle` (`crates/cli/src/settle.rs`) reads its input in fixed `READ_WINDOW` (4 MiB)
pieces (`:20`, `:65-73`) and feeds each one to a `Settler`, regardless of chunking mode. With
content-defined chunking, the chunker (`cdc::CdcChunker`) reports a chunk boundary periodically, so
`Settler::feed` (`:105-118`) only ever accumulates one chunk's worth of data in `chunk_buffer`
before `complete_chunk` (`:123-150`) hashes it, checks it against `crates/db`'s dedup index, writes
it if new, and clears the buffer - bounded by the chunking target size (a few MB at typical
settings), independent of the file's total size.

With `--whole-file` chunking, the chunker is `cdc::SingleChunkChunker`
(`crates/cdc/src/lib.rs:289-310`), which never reports a boundary from `next()` - only from
`flush()`, once the entire input has already been fed. `complete_chunk` is therefore reached
exactly once, at the very end, and `chunk_buffer` accumulates every `READ_WINDOW` read until then.
A 4 GB file in `--whole-file` mode holds that entire 4 GB as one `Vec<u8>` before a single byte
reaches `crates/store`.

### Working assumption, not yet backed by investigation

This document proceeds on the assumption that letting the buffer grow without any bound of its own
- relying on the operating system's own paging (virtual memory backed by swap) to absorb whatever
size results - is not an acceptable answer here. That assumption has not actually been investigated
or weighed against the alternative yet (no measurement of paging behavior under this workload, no
comparison of its cost against an explicit bound). It is adopted here only as a starting point to
make forward progress on a design; a real investigation could still end up concluding otherwise,
in which case this document's own conclusion changes accordingly.

### Candidate approach: hash in a first pass, write only if the content is new

`settle::settle`'s own `read: impl FnMut(u64, u32) -> io::Result<Vec<u8>>` parameter already takes
an explicit `position` on every call - the interface already supports reading the same input more
than once, in any order, not just a single forward pass. Both existing callers can already serve
that: `crates/cli/src/write_cache.rs`'s `WriteCache::read` (the mount's own settle path) is a
genuinely random-access reader by construction; `crates/cli/src/ingest.rs`'s `ingest_file` reads a
real, seekable `std::fs::File`, though its current closure (`:396-400`) ignores the `_pos` argument
and relies on the file cursor's own implicit sequential advancement - reading out of that strict
order would need it to seek explicitly instead, a small, mechanical change rather than a design
question of its own.

Built on that: for `--whole-file` mode specifically (content-defined chunking keeps its existing,
already-bounded path unchanged), replace the single accumulate-then-hash pass with two bounded
passes over the same `read` callback:

1. **Hash pass**: read every `READ_WINDOW` piece in order, feed it into an incremental hasher
   (`blake3::Hasher::update` accepts data piecemeal - no reason to hold prior windows once hashed),
   discard each piece immediately after. Memory use is `O(READ_WINDOW)` throughout, independent of
   file size. At the end, this pass has the chunk's own hash without ever having held more than one
   window in memory.
2. **Decide, then maybe write**: look up that hash the same way `complete_chunk` already does
   (`crates/db::Repository::find_chunk`). Already known - REQ-STORAGE-001's dedup applies exactly
   as it does today - and nothing further needs writing at all: the second pass is skipped
   entirely. Not already known: reserve the destination range(s) for the whole, now-known size
   up front (`crates/db::allocation::reserve`, the same allocator `reserve_and_insert_chunk` already
   calls), then read a second pass through the same `read` callback, writing each `READ_WINDOW`
   piece directly to its position in `crates/store` as it is read, never accumulating more than one
   window's worth in memory on this pass either.

### What this costs, compared to today

- **Content already known (an unchanged file re-backed-up, or identical content seen elsewhere)**:
  one read pass, zero store writes - strictly better than today, which still reads the whole file
  once but holds all of it in memory to get there.
- **Content genuinely new**: two read passes over the same source instead of one, plus one write
  pass - today's single read pass, traded for a second one, in exchange for turning an unbounded
  buffer into a small, fixed one. Whether this trade is worth it in practice (how expensive a second
  read actually is against the source `ingest` or the mount's write cache reads from) is not yet
  measured.

### Alternative considered: spill the hash pass's bytes to a local scratch file

Instead of re-reading the original `read` callback for the second pass, the hash pass could write
each window it hashes to a local temporary file as it goes (the same kind of local-disk spillover
DESIGN-MOUNT-010 already uses for the write cache), then the write pass reads from that file instead
of the original source. This avoids reading the original source twice, at the cost of a second,
self-managed copy of the whole file's bytes landing on local disk during the hash pass, whether or
not that copy ever turns out to be needed (i.e. even when the content turns out to already be
known, wasting a duplicate local write).

Not adopted as the leading candidate here because both existing callers' own sources already read
about as cheaply the second time as the first: `ingest`'s source is a real file on local disk by
definition (REQ-INGEST-001 imports from a real filesystem path), and the mount's own
`WriteCache::read` already resolves either from memory or from its own local-disk spillover
(DESIGN-MOUNT-010) - neither is the kind of genuinely expensive-to-re-read source (e.g. a slow
network path on the *source* side) this alternative would actually pay for itself against. Revisit
if a concrete case turns up where the source itself is expensive enough to re-read that avoiding a
second pass over it would be worth an unconditional local-disk copy instead.

### Open questions

- Where the resulting memory bound should actually be recorded as a requirement (most likely
  `requirements/non-functional/performance.md`, alongside REQ-PERFORMANCE-*, but not yet decided)
  and what its precise wording should be - a concrete bound (e.g. "a bounded, small multiple of
  `READ_WINDOW`, independent of content size") versus a looser "does not scale with input size"
  framing.
- Whether `crates/db::allocation::reserve`'s existing gap-scanning allocator handles a
  "reserve now, know you will write it a moment later, in order" access pattern the same way it
  already handles `reserve_and_insert_chunk`'s existing single-shot reservation, or needs any
  adjustment for this two-pass caller.
- What regression test actually demonstrates the bound - the working assumption above is that peak
  memory needs to be asserted directly (e.g. via an allocator-tracking harness or an OS-level
  measurement around a settle call), not inferred from the size of the input alone.

This gets a `DESIGN-...` ID once an approach is actually decided, per this directory's own
[`README.md`](README.md) ID scheme ("assign IDs only once a decision is actually settled") - not
before.
