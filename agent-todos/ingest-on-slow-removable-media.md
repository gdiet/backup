# Ingest on slow removable media: fewer commits, larger writes, relocatable metadata

**Why parked**: Out-of-scope finding. It came up while measuring `dfs ingest` against native
creation on the USB2 stick. The developer asked why ingest is slow there. The investigation found
causes but no change was requested yet. Each candidate below needs a decision first.
**Size**: medium/large. **Do not start without talking to the developer first.** The decisions
below are theirs, and the first step is a measurement on the device that matters (see the next
paragraph).
**Opened**: 2026-10-06, by the Windows desktop session on `julius`
**Context**: [`performance/notes/2026-10-06-julius-ingest-on-usb2-stick.md`](../performance/notes/2026-10-06-julius-ingest-on-usb2-stick.md)
(numbers and method), `performance/measurements/2026-10-06-julius-ingest-*-usb.md`,
`crates/cli/src/ingest.rs`, `crates/cli/src/settle.rs`, `crates/store/src/lib.rs`

## The device that matters

The investigation ran on a slow USB2 stick, because that was the device at hand. The relevant
target for the developer is a fairly fast external USB hard disk. Its behavior differs from the
stick in the points that drive the findings below. A hard disk handles large sequential writes
well, so the effect of the write size (candidate 3) is probably small there. Its small
synchronous writes cost a head movement instead of a flash block merge, so the cost per commit
(candidate 2) may still matter, but this is unmeasured. Before any change, repeat the measurement
with `performance/scripts/ingest.ps1` and the split experiment of the note (`meta/` and `data/`
placed independently) on the actual USB hard disk. Only a measured gap there justifies work.

## Findings on the stick

On the USB2 stick, ingest reaches 35% of the native throughput at 10 MB (3.7 against 10.5 MB/s)
and runs 2.2 times slower than native at 100 B. Two device properties explain this. The stick
writes larger blocks faster, and it handles small scattered writes badly. The worker count and
SQLite's `synchronous` setting do not matter.

## Candidates

1. **A relocatable metadata directory.** Keeping `meta/` on a fast disk and `data/` on the slow
   medium removed most of the cost in every experiment (100 B: 0.5 s with everything on the SSD,
   2.4-3.0 s with only the data on the stick, 15-27 s with both on the stick). It works today with
   an NTFS junction or a symbolic link. `dfs` has no option for it, and no command accepts a
   metadata directory other than `<repository>/meta`. A decision is needed on whether this belongs
   in the product, and how backups and `dfs db-restore` would treat such a layout.
2. **Fewer database commits during ingest, that is, larger transactions.** A small file causes at
   least three commits (chunk, content, file). The mechanism exists: while a batch is open,
   `Repository::with_transaction` uses savepoints, and the migration tool commits that way every
   5,000 operations (`docs/design/scala-migration-tool.md`). Three things make ingest harder than
   the migration:
   - **One shared connection.** All ingest workers use the single connection of one `Repository`
     behind a mutex. An open transaction therefore belongs to all workers at once, and a commit
     also commits what another worker has half done. A transaction per file is not possible. What
     remains is a group commit for all workers, after a number of operations or after a time span.
   - **The order of row and bytes.** `Settler::complete_chunk` commits the chunk row
     (`reserve_and_insert_chunk`) first and writes the bytes afterwards. A larger batch must not
     widen that window. A commit must only happen once the bytes of every chunk in the batch are
     written, or the order must be reversed (write first, record afterwards). This is the real
     design question.
   - **A crash loses the running batch.** That is acceptable for ingest, which can simply be run
     again, but it must be decided deliberately.

   Estimated gain on the stick, from the 100 B experiment: the metadata costs 22-30 ms per file
   there (three commits), the data about 6-7 ms. One commit per 100 files would leave
   about the data part, roughly 2.5 times faster in total. On an SSD the gain is probably small
   (unmeasured). For 10 MB files it is small, because the write size of the chunks dominates.
3. **Write combining for chunk bytes.** Collecting several chunks into blocks of several MB would
   bring the stick from about 5.7 to about 9 MB/s for 10 MB files. The gain exists only on devices
   like this stick. The cost is more memory per worker and a larger window of bytes that are
   registered but not yet written. Probably not worth it, in particular if the relevant device is a
   hard disk.

Do not pursue: a setting for the number of ingest workers, or a change of SQLite `synchronous`.
Neither had a measurable effect on the stick.

Whatever is chosen, check the effect on an SSD as well, because a change must not make that case
slower.
