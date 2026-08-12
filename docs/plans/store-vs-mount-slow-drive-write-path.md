# Open question: why is `store`'s physical chunk-write path slower than mount's on a slow drive?

**Status**: open question, not investigated beyond ruling out the first (wrong) guess. No code
changed. Not a plan to implement anything yet - a note to come back to before assuming a cause.

## The observation

From `docs/plans/implemented/copy-performance-comparison.md` (large-file profile, 4 files x
150 MiB = 600 MB, on a slow USB stick, `julius`): `backup store`'s first run costs **304.8s**
median wall-clock, while writing the identical content through `backup mount --read-write` costs
only **126.0s** - `store` is **~2.4x slower**, despite `store` using up to 4 worker threads
(2 physical cores, hyperthreaded) against mount's single `persist_worker` thread. On a fast SSD,
the same comparison goes the other way as naively expected (`store` 3.82s vs. mount 10.2s,
`store` wins) - so this is specific to the slow drive.

## What's already ruled out

**Not thread contention on the physical write.** `--store-io-parallelism` gates concurrent
physical chunk writes independent of `--concurrency` (`cli/src/io_limiter.rs`, used at the
`chunk_store::write_chunk_from_cache` call site in `cli/src/store.rs`). Swept it across
`1`/`2`/`4` on the same slow drive, same profile, 2 reps each:

| `--store-io-parallelism` | wall (rep 1) | wall (rep 2) |
|---|---:|---:|
| 1 | 308.3s | 288.0s |
| 2 | 321.0s | 302.8s |
| 4 (~= default) | 284.5s | 290.6s |

No trend, all three within noise of each other, and all still ~2.3-2.5x slower than mount's
126.0s - including `parallelism=1`, which serializes physical chunk writes onto one thread and so
should structurally resemble mount's single-threaded writer most closely of the three. Whatever
the cause is, it isn't threads racing each other for the disk.

**Not raw compute.** CPU time for `store`'s large-file first run (~6-9s across all three
parallelism settings) is in the same ballpark as mount's (8.08s) - neither is CPU-bound here, both
are overwhelmingly wall-clock-vs-CPU I/O-wait.

## What's still open

Something about how `store` physically writes chunk bytes to the data store costs more per byte or
per chunk on this slow device than mount's write path does, at matched concurrency. Candidates not
yet checked:

- **Write call pattern**: does `store`'s `chunk_store::write_chunk_from_cache` path do more/smaller
  `write()` calls, more `fsync`/flush operations, or open the backing file more often per chunk
  than whatever mount's `persist_worker` path does for the same chunk? A slow USB stick is
  disproportionately sensitive to per-syscall overhead (each one can cost tens to hundreds of ms),
  so a difference invisible on a fast SSD could dominate here.
- **Allocation pattern**: does `store`'s target-size CDC chunking (default `2^20` = 1 MiB, see
  `init --cdc-target-size-bits`) produce more chunks, or chunks laid out less sequentially on the
  backing store files, than the mount write path does for the same source content? More/smaller
  physical writes, or more seeking between them, would hit a slow USB stick hard even at the same
  total byte count.
- **The single SQLite writer thread** (`run_writer` in `cli/src/store.rs`): CPU time doesn't
  implicate it, but wall-clock-vs-CPU doesn't rule out SQLite-side `fsync`/WAL-checkpoint I/O
  either - worth checking whether `store`'s batching (`WRITE_BATCH_SIZE`/`WRITE_BATCH_IDLE_TIMEOUT`)
  produces a different commit/fsync frequency than mount's own metadata-write path for the same
  workload.
- **Read side**: source files live on the fast SSD in both scenarios, so source reads shouldn't
  differ - but not explicitly confirmed source I/O isn't somehow serialized differently between
  the two code paths.

## Suggested next step

Before guessing further: instrument (or straceish-profile, e.g. Windows `Process Monitor` filtered
to the repository's data files) one `store` run and one mount-write run of the identical
large-file profile on the slow drive, and directly compare syscall counts/sizes/timing against the
store's backing files - the answer is almost certainly visible there rather than guessable from
wall-clock numbers alone.
