# Performance comparison: local copy vs. mount vs. `store`, small vs. large files

**Status**: methodology decided (see "Methodology decisions" below), ready to run - nothing has
actually been executed yet.

**Goal**: compare wall-clock performance across five workflows, each for two very different file
profiles (many small files, few large files), to see whether the comparison surfaces a genuine
design or implementation weakness worth fixing - not just "which is faster" for its own sake.

## The ten (now twelve) scenarios

For **both** a "many small files" source tree and a "few large files" source tree, run:

1. **Local OS copy, `a -> b`** (e.g. `cp -r`/`rsync -a`, no `backup` binary involved at all) - the
   baseline floor: whatever this project's tools cost on top of this is the price of chunking,
   hashing, dedup bookkeeping, and (for the mount scenarios) FUSE/WinFSP overhead.
2. **First copy of the local tree into a `backup mount --read-write`ed repository** (plain
   `cp -r`/equivalent *through the mount*, not `backup store`) - all-new content, exercises the
   mount's write path cold.
3. **Same copy repeated into the same mounted repository - both variants** (decided below): (3a)
   overwrite the same destination path, *and* (3b) copy to a second, differently-named destination
   with identical content - run and report both, since they exercise different code paths
   (`unlink`+`create`/`truncate` semantics plus dedup, vs. pure dedup without the overwrite path).
4. **`backup store` (no `--reference`), first run** - the dedicated bulk-backup path, cold.
5. **`backup store` (no `--reference`), repeated run of the same source** - same content again, but
   explicitly *without* the one feature (`--reference`) this project documents as the way to make a
   repeat backup fast (see "Without `--reference`..." under "A priori hypotheses" below) - measures
   the cost of chunk-level dedup *discovery* in isolation from `--reference`'s file-level shortcut.

## Methodology decisions

- **Scenario 3**: run both variants (3a overwrite, 3b second destination), not just one - see above.
- **Cache state between runs**: accept OS page/disk cache state as noise, averaged out over
  repetitions, rather than actively dropping caches between runs - simpler to execute (no root
  needed) and arguably more representative anyway, since a real user doesn't drop caches between
  copies either.
- **Repetitions**: 5 runs per scenario, report the median - a reasonable balance between confidence
  and total runtime, given the large-file scenarios already cost real time per run on their own.
- **Test data sizes**: use the example parameters below as given, not tailored to a specific
  real-world source tree - they're representative enough to check the hypotheses below (the
  small-file parallelism gap, large-file CDC/spill cost) without needing further tuning first.
- **Repository isolation between scenarios** (not explicitly re-confirmed, but necessary for any of
  the above to mean anything): each of the five workflows starts from its own freshly-initialized
  repository, except the intentional "first run" -> "repeat run" pairs within a workflow (2->3,
  4->5), which need to share one - otherwise cross-scenario dedup would contaminate results in ways
  that don't reflect any real user's workflow.

## Test data

- **Many small files**: e.g. 10,000 files, a few KB each (small enough that a single file is below
  the CDC chunker's minimum chunk size and so never internally splits - see "CDC chunking cost"
  below), nested a few directories deep to also exercise path-resolution/tree-insert overhead, not
  just raw byte throughput.
- **Few large files**: e.g. 5-10 files, each large enough (hundreds of MB to low GB) that CDC
  chunking, the `--chunk-buffer-mb` budget, and multi-extent storage all actually get exercised.
- Use content that isn't trivially compressible/all-zero (real dedup-relevant behavior depends on
  actual chunk boundaries) but *is* byte-identical between the "first run" and "repeat run" of the
  same scenario, since the repeat runs are specifically testing dedup behavior.

## Metrics to capture per run

- Wall-clock time (primary metric - what a user actually experiences).
- CPU time (user/sys) if available - distinguishes "slow because single-threaded" from "slow
  because waiting on disk."
- Peak RSS, to sanity-check against `--chunk-buffer-mb`'s default (128 MB) and catch unexpected
  spill-to-temp-file activity for the large-file profile.
- Final repository size after each `store`/mount-write scenario, to confirm dedup actually
  happened as expected (a repeat run that *doesn't* shrink relative to bytes-written would itself
  be a red flag, independent of timing).
- Machine's CPU core count, noted alongside results - directly relevant given the parallelism
  asymmetry below.

## A priori hypotheses, grounded in the current code (verify, don't assume)

These are concrete predictions worth checking the benchmark against - if the numbers *don't* match
one of these, that's itself worth investigating further.

### `store` parallelizes across files; `mount --read-write`'s persist path does not

`store` (`cli/src/store.rs`) walks all source files and processes them via a real `rayon`
`ThreadPool` (`into_par_iter()`), one chunking worker per file, up to `--concurrency` (default: one
thread per CPU core). For "many small files," this means N files chunk/hash/write concurrently.

`mount --read-write`'s write path is architecturally different: every file's actual persist
(chunking + store write) is queued as a `PersistJob` and executed on a **single** dedicated
background thread, `persist_worker` (`cli/src/mount.rs:358-378`) - explicitly documented as
"serial by design," moved off the FUSE/WinFSP dispatch threads specifically to avoid a
worker-pool-exhaustion failure mode, not chosen for throughput. Every FUSE dispatch thread can
still accept new `create`/`write` calls concurrently (buffering into RAM/spill), but the actual
chunk-and-store work for every file in the whole mount funnels through that one thread.

**Prediction**: for many small files, `store` should scale with CPU core count while
`mount --read-write` should not - the gap should be largest on a many-core machine and should grow
with file count. If the observed gap is much smaller than core-count would predict, something else
is the bottleneck (FUSE call latency itself, single SQLite writer contention, disk I/O) and that's
worth knowing. If the gap is *as large as* predicted, `persist_worker`'s single-thread design is a
legitimate throughput ceiling for this workload - worth a follow-up plan on whether it can safely
use a small pool instead of exactly one thread now that persisting is already off the dispatch
path (the original worker-pool-exhaustion reason for serializing doesn't obviously require
serializing to exactly one thread, just to *some* bounded number off the dispatch threads - not
re-investigated here, flagged as a possible next step depending on what the numbers show).

### Without `--reference`, a repeat `store` run should cost about as much as the first one

README.md's own documentation of `--reference` states explicitly: *"even with perfect dedup, an
unchanged file still costs a full read+chunk+hash on every run just to discover that it
dedupes."* Scenario 5 (repeat `store`, no `--reference`) is deliberately testing exactly this
documented cost. **Prediction**: scenario 5 should be close to scenario 4's time, not
dramatically faster - the only savings should come from skipping the physical store write itself
(the chunk lookup already finding an existing `chunks` row), not from skipping read/chunk/hash.
If scenario 5 turns out to be *much* faster than scenario 4 despite no `--reference`, that's
worth understanding (a shortcut somewhere not accounted for in the docs); if it's *not* faster at
all, that's a hint the store-write itself wasn't the dominant cost even on the first run, meaning
chunk-lookup/hash overhead already dominates - useful to know either way for prioritizing future
work.

### CDC chunking cost on the "few large files" profile

`cdc::ChunkerConfig` (`cdc/src/lib.rs`) - if the repository is configured for CDC chunking - runs
a rolling-hash scan over every byte of every file to find boundaries, regardless of file size.
Large files are exactly where this cost is most visible in isolation from per-file overhead (fixed
per-file costs like a `tree_entries` insert amortize away over a large file, unlike for the
small-file profile). This is also where `--chunk-buffer-mb`'s default 128 MB shared budget is most
likely to actually matter (a single very large in-flight chunk, or many concurrent large files
under `store`'s parallelism, could plausibly hit the spill-to-temp-file path) - worth watching for
spill activity (e.g. temp directory usage) during the large-file runs as a possible confound, not
just raw timing.

### Many small files: fixed per-file overhead may dominate over throughput

For the small-file profile, per-file costs that are irrelevant at large-file scale become the
whole story: a `tree_entries` row insert, a chunk lookup/insert, path-resolution walking parent
directories - all in SQLite, all inside whatever transaction-batching `store`/`mount` use. If the
small-file numbers look disproportionately worse than the large-file numbers relative to total
bytes moved, that points at fixed per-file/per-chunk overhead (probably DB-side) rather than raw
I/O or chunking throughput - worth a follow-up profiling pass specifically on the small-file case
if that's what the numbers show, rather than guessing further here.

## What counts as "a design/implementation weakness worth acting on"

Not every observed gap is one - some cost (chunking, hashing, dedup bookkeeping, FUSE overhead) is
the inherent price of what this project does that a plain `cp` doesn't. Worth flagging specifically
if:

- The `mount --read-write` vs. `store` gap for small files is far larger than CPU-core-count would
  explain (points at a bottleneck beyond the known single-threaded `persist_worker`, e.g. per-call
  FUSE/SQLite contention).
- The mount write path is dramatically slower than local OS copy even for the *first* (all-new,
  no-dedup-possible) small-file run - since that comparison has no dedup-discovery cost on either
  side, a large gap there is closer to "pure overhead," worth understanding before writing it off
  as inherent to FUSE.
- The large-file profile shows spill-to-disk activity by default at the documented 128 MB budget in
  a way that meaningfully hurts throughput - would suggest the default is miscalibrated for
  realistic large-file backups, not just an edge case.
- Final repository size after the repeat-content scenarios doesn't shrink as expected relative to
  bytes written - a correctness concern, not just a performance one, and would take priority over
  any timing finding.

## Suggested next step

Run the twelve scenarios (or a reduced subset first, e.g. just the small-file profile, since
that's where the sharpest predicted gap - `store`'s parallelism vs. `persist_worker`'s single
thread - lives), record the metrics, and compare against the predictions in this document rather
than starting from a blank slate. Where a prediction doesn't hold, that mismatch is itself the
interesting finding.
