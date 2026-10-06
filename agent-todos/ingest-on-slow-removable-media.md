# Ingest on slow removable media: fewer commits, larger writes, relocatable metadata

**Why parked**: Out-of-scope finding. It came up while measuring `dfs ingest` against native
creation on the USB2 stick. The developer asked why ingest is slow there. The investigation found
causes but no change was requested yet. Each candidate below needs a decision first.
**Size**: medium/large (confirm with the user first)
**Opened**: 2026-10-06, by the Windows desktop session on `julius`
**Context**: [`performance/notes/2026-10-06-julius-ingest-on-usb2-stick.md`](../performance/notes/2026-10-06-julius-ingest-on-usb2-stick.md)
(numbers and method), `performance/measurements/2026-10-06-julius-ingest-*-usb.md`,
`crates/cli/src/ingest.rs`, `crates/cli/src/settle.rs`, `crates/store/src/lib.rs`

On the USB2 stick, ingest reaches 35% of the native throughput at 10 MB (3.7 against 10.5 MB/s)
and runs 2.2 times slower than native at 100 B. The investigation found two device properties
behind this. The stick writes larger blocks faster, and it handles small scattered writes badly.
The worker count and SQLite's `synchronous` setting do not matter.

Candidates, each with what it would gain and what it would cost:

1. **A relocatable metadata directory.** Keeping `meta/` on a fast disk and `data/` on the slow
   medium removed most of the cost in every experiment (100 B: 0.5 s with everything on the SSD,
   2.4-3.0 s with only the data on the stick, 15-27 s with both on the stick). It works today with
   an NTFS junction or a symbolic link. `dfs` has no option for it, and no command accepts a
   metadata directory other than `<repository>/meta`. A decision is needed on whether this belongs
   in the product, and how backups and `dfs db-restore` would treat such a layout.
2. **Fewer database commits during ingest.** A small file causes at least three commits (chunk,
   content, file). One commit per file would cut the small writes by about two thirds. One commit
   per batch of files would cut them further. The ordering between the data write and the
   database rows must stay crash-safe. Today the chunk row is committed before the bytes are
   written, so a batch must not make that window larger. The migration tool already batches
   commits with a crash-consistency argument that can serve as a model
   (`docs/design/scala-migration-tool.md`).
3. **Write combining for chunk bytes.** Collecting several chunks into blocks of several MB would
   bring the stick from about 5.7 to about 9 MB/s for 10 MB files. The gain exists only on devices
   like this stick. The cost is more memory per worker and a larger window of bytes that are
   registered but not yet written. Probably not worth it on its own.

Do not pursue: a setting for the number of ingest workers, or a change of SQLite `synchronous`.
Neither had a measurable effect.

Before starting, re-measure with `performance/scripts/ingest.ps1` on the stick, and check the
effect on an SSD as well, because a change must not make the SSD case slower.
