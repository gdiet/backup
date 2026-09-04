# `ingest` has no write buffering and no parallelism - look closer at whether it should

**Noted**: 2026-09-04, during the informal WebDAV-network-drive performance exploration
(`performance/notes/2026-09-04-julius-h-webdav-network-drive.md`), after the developer's own
follow-up question about whether `--spill-dir` (DESIGN-MOUNT-018, added the same session) should
also apply to `dfs ingest`.
**Size**: medium - confirm before starting. Deciding whether/how to change `ingest`'s write
strategy is a real design question (buffering, parallelism, or neither), not a one-line fix.
**Context**: `crates/cli/src/ingest.rs` (`ingest_file`, `run`); `crates/cli/src/settle.rs`
(`settle::settle`, the shared engine); `crates/cli/src/write_cache.rs` and
`crates/cli/src/settle_pool.rs` (the mount's write cache and parallel settle pool, both absent from
`ingest`'s path); `docs/design/mount-write-path.md` (DESIGN-MOUNT-006/010); `requirements/functional/ingest.md`
(REQ-INGEST-001 and friends).

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

## What was actually observed

The informal WebDAV exploration this session showed `dfs ingest` had not finished writing even one
5 MB file's content 60 seconds into a two-file (10 MB) batch on a WebDAV-backed repository -
dramatically slower than the mount's own settle path (`--spill-dir`, DESIGN-MOUNT-010/006), even
once that path's real (unbuffered, settled) throughput was measured directly and found to be slow
in its own right (~0.17-0.21 MB/s, see the notes file above). Two independent factors compound in
`ingest`, neither present in the mount's write path:

- **No write cache at all** - `ingest_file` calls `settle::settle` synchronously per file, reading
  from the local source and writing straight to `store::ByteStore` (which, for a repository on a
  slow medium, means straight onto that slow medium) with nothing buffering or absorbing the cost
  the way DESIGN-MOUNT-010's memory-then-local-SSD write cache does for a mounted session.
- **No parallelism across files** - `crates/cli/src/settle_pool.rs`'s `JobPool` (used by the mount)
  runs `available_parallelism()` worker threads; `ingest`'s main loop processes files one at a time
  with nothing equivalent.

## What is worth a closer look

Not yet decided whether either of these is actually a problem worth fixing, or a reasonable
consequence of `ingest` being a different tool for a different job (a one-shot bulk import,
run once and waited on, versus a long-lived interactive mount session) - genuinely open, hence
"confirm before starting" above. Concretely worth examining:

- Whether `ingest`'s current single-threaded, unbuffered behavior is a real-world problem for its
  actual use case (REQ-INGEST-001's bulk import of a filesystem tree) or mostly shows up in an
  edge case like this session's slow-network-drive exploration - a large local-disk-to-local-disk
  ingest never pays anywhere near this cost.
- Whether adding parallelism across files (reusing `settle_pool.rs`'s `JobPool`, or something
  simpler) would help meaningfully without adding real complexity - `ingest` already reads/hashes/
  chunks/writes each file independently, which is the same shape of per-item work the mount's
  settle pool already parallelizes.
- Whether a write cache/buffering layer makes sense for `ingest` at all - unlike a mount session,
  `ingest` already knows the full source file up front (no incremental `write()` calls arriving
  from a live client to buffer between), so the *problem* DESIGN-MOUNT-010 solves (decoupling
  client-visible latency from network cost) may not even apply the same way; parallelism alone
  might be the more relevant lever here, not buffering.
- If `ingest`'s current numbers are judged to be a real, worthwhile lower bound rather than a
  representative one, that is itself worth writing down somewhere more permanent than this todo
  (e.g. a note in `requirements/functional/ingest.md` or a new `DESIGN-INGEST-*` entry) so a future
  performance comparison does not repeat the "ingest vs mount" mistake this session's own
  first-draft write-up made before being corrected.
