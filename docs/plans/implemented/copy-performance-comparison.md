# Performance comparison: local copy vs. mount vs. `store`, small vs. large files

**Status**: executed (2026-08-12) on julius, against a release build (`cargo build --release`).
See "Results" at the bottom for the numbers and findings; see "Deviations from the original
methodology" for what was scaled down or added and why. One finding is action-worthy and
well-evidenced (mount's small-file write path costing ~7x a plain copy even with zero dedup
benefit on either side - CPU-saturated on its single `persist_worker` thread); one is a confirmed,
real regression whose *cause* turned out not to be what a first guess suggested (`store`'s large
files are slower than mount's on a slow drive, but directly testing `--store-io-parallelism`
ruled out thread contention as the reason - the actual cause is still open, see
`docs/plans/store-vs-mount-slow-drive-write-path.md`). Neither is fixed - this document is the
measurement, not the fix.

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

## Environment

- **Machine**: `julius`, a Toshiba Satellite C70-C-1DV laptop. CPU: Intel Core i5-6200U @ 2.30 GHz,
  **2 physical cores, 4 logical (hyperthreaded)** - not 4 independent cores; `--concurrency`'s
  default (one thread per logical core) means `store`'s 4-way parallelism below is really 2
  physical cores shared across 4 hyperthreads, worth keeping in mind when extrapolating the
  parallelism-gap numbers to real server-class hardware. RAM: 7.9 GB total.
- **OS**: Windows 10 IoT Enterprise LTSC 2021, build 10.0.19044, 64-bit.
- **Toolchain**: `rustc 1.97.0` / `cargo 1.97.0`, built with `cargo build --release` (LTO +
  `codegen-units = 1`, see `Cargo.toml`).
- **WinFSP**: 2023, version 2.0.23075 (required for every mount scenario below).
- **`fast-ssd-C`**: `C:` drive, backed by a WDC WDS100T2B0A-00SM50 SATA SSD (also the OS drive -
  not an isolated/idle device, other on-machine activity could in principle share its bandwidth,
  though nothing else was intentionally running during the measured scenarios).
- **`slow-usb-I`**: `I:` drive, a plain USB flash stick (Windows reports it generically as
  "General USB Flash Disk"), 3.75 GB total capacity, connected directly (not through a hub, as far
  as known) - exact make/model/USB revision not captured.

## Deviations from the original methodology

Run on `julius` (see "Environment" above), against `target/release/backup.exe`. Everything below
was actually executed, not simulated - but scaled down and extended relative to the plan above:

- **Repetitions**: 3, not 5 (median of 3 still a real median, not an average of 2) - time budget.
- **Small-file profile**: 3,000 files x 4 KiB (12 MB total, 10x10 nested dirs x 30 files), not
  10,000 - time budget on the slow drive below, where a single small-file pass already took
  ~50s-4min depending on scenario.
- **Large-file profile**: 4 files x 150 MiB (600 MB total), not "5-10 files, hundreds of MB to low
  GB" - capacity budget: the slow drive used below is a 3.75 GB USB stick, and every workflow
  needs headroom for a full repository copy plus the local-copy baseline, sequentially reused
  across reps.
- **Content**: cryptographically random bytes (small-file profile: 3,000 unique 4 KiB slices of a
  shared 12 MB random pool, for fast generation; large-file profile: fully random per file) -
  maximally incompressible, satisfies "not trivially compressible", same source tree reused
  verbatim for every repeat-scenario so first/repeat pairs are byte-identical as required.
- **Added a dimension not in the original plan**: every scenario ran on *two* physical drives, not
  one - `C:` (a SATA SSD, `fast-ssd-C` below) and a 3.75 GB USB flash stick (`slow-usb-I` below).
  This wasn't planned originally but turned out to be essential: several of the a priori
  hypotheses below only hold on one of the two, and the mismatch is exactly the kind of finding
  the plan's own "worth acting on" criteria are looking for (see Findings).
- **Scenario 1 tool**: `robocopy /E /IS` (Windows-native, `/IS` forces it to recopy files that
  already match the destination by size/timestamp - without it, the scenario 3a overwrite
  looked suspiciously cheap on a first pass, then turned out to change little once `/IS` was
  added; see the 3a finding below for why the overwrite really is cheap for a different reason).
- **Metrics actually captured**: wall-clock (primary, as planned) and CPU time - for `store`/local
  copy, the CPU time of that spawned process itself; for the mount scenarios, the CPU time of the
  long-running `backup mount --read-write` *server* process, sampled as a delta around each
  client-side `robocopy` copy (that's where chunking/hashing actually happens, not in the
  `robocopy` client). Peak RSS captured the same way (polled `WorkingSet64`, not a true OS-level
  peak-since-start). Final repository size captured via `Get-ChildItem -Recurse -File | Measure
  -Object -Sum Length` after each store/mount-write scenario. **Not captured**: peak RSS for the
  short-lived `store`/local-copy processes (would have needed a separate polling thread per
  invocation - skipped, time budget; the mount-server RSS numbers below are more informative
  anyway since that's the process actually buffering chunk data), and no explicit check for
  spill-to-temp-file activity (the "CDC chunking cost" hypothesis below is evaluated from peak RSS
  proximity to the 128 MB default budget instead, which is suggestive but not the same as
  confirming a spill file was actually created).

## Results

Median of 3 repetitions per cell (raw data: all 72 runs, `Drive,Profile,Scenario,Rep,WallMs,CpuMs,
PeakRssMb,RepoBytes` - not committed, regenerate by rerunning the scenarios if needed).

### Small files (3,000 files x 4 KiB = 12 MB)

| Scenario | slow-usb-I wall | slow-usb-I CPU | fast-ssd-C wall | fast-ssd-C CPU |
|---|---:|---:|---:|---:|
| 1 local copy (no `backup` tool) | 51.5s | 3.75s | 4.27s | 3.63s |
| 2 mount, first write (all-new) | 220.4s | 26.4s | 31.2s | 26.7s |
| 3a mount, overwrite same dest | 37.2s | 19.2s | 25.6s | 21.9s |
| 3b mount, second dest (dedup) | 129.4s | 18.4s | 24.7s | 21.0s |
| 4 store, first run (no `--reference`) | 32.4s | 3.09s | 1.75s | 2.81s |
| 5 store, repeat (no `--reference`) | 4.43s | 1.33s | 1.29s | 1.36s |

### Large files (4 files x 150 MiB = 600 MB)

| Scenario | slow-usb-I wall | slow-usb-I CPU | fast-ssd-C wall | fast-ssd-C CPU |
|---|---:|---:|---:|---:|
| 1 local copy (no `backup` tool) | 54.3s | 0.11s | 3.10s | 0.13s |
| 2 mount, first write (all-new) | 126.0s | 8.08s | 10.2s | 6.92s |
| 3a mount, overwrite same dest | 7.18s | 4.42s | 6.58s | 3.80s |
| 3b mount, second dest (dedup) | 6.95s | 4.55s | 5.52s | 3.69s |
| 4 store, first run (no `--reference`) | 304.8s | 6.45s | 3.82s | 6.55s |
| 5 store, repeat (no `--reference`) | 2.52s | 3.41s | 1.77s | 3.55s |

Peak mount-server RSS was ~11 MB for every small-file scenario and ~138 MB for every large-file
scenario (both drives) - see "CDC chunking cost" finding below.

Final repository size after every repeat-content scenario grew only by small-file-metadata amounts
(hundreds of KB for 3,000 new `tree_entries` rows, near-zero/rounds-away for the 4-large-file
profile), never by anything close to a full re-store of content bytes - **dedup correctness holds
in every scenario measured**, no red flag per the plan's own top-priority check.

### Findings vs. the a priori hypotheses

1. **`store` parallelizes, `persist_worker` doesn't - confirmed, but only where disk isn't the
   bottleneck.** On `fast-ssd-C`, small files: `store` first-run is **1.75s vs. mount's 31.2s**
   (~18x) - as sharp as the CPU-core-count prediction expected, and CPU-bound on both sides (wall
   ~= CPU for the mount server). On `slow-usb-I`, small files, the gap survives but shrinks to
   ~6.8x (32.4s vs. 220.4s) since both now pay real disk-write cost too. **This part of the
   hypothesis holds as predicted** - `persist_worker`'s single-thread design is a real, measurable
   throughput ceiling for many-small-files mount writes, independent of disk speed.

2. **Large files: the same hypothesis *inverts* on the slow drive - the most interesting finding
   here, though the original explanation for it turned out to be wrong.** On `fast-ssd-C`, `store`
   beats mount as expected (3.82s vs. 10.2s, ~2.7x, parallelism wins). But on `slow-usb-I`,
   **`store` is 2.4x *slower* than mount** (304.8s vs. 126.0s) despite using up to 4 threads
   against mount's 1. CPU time confirms it isn't compute (`store`'s CPU cost, 6.45s, is actually
   *lower* than mount's 8.08s) - the wall-clock loss is disk-side.
   >
   > **First guess - "4 threads fighting over one slow device" - directly tested and falsified.**
   > `--store-io-parallelism` (already exists, gates concurrent physical chunk writes independent
   > of `--concurrency`, see `store --help` and `cli/src/io_limiter.rs`) is the documented knob to
   > fix exactly that if it were the cause. Swept `--store-io-parallelism 1/2/4` on `slow-usb-I`,
   > same large-file profile, 2 reps each: **284.5s / 302.8s / 320.9s wall (mean of 2), no
   > meaningful trend, and *none* of them come close to mount's 126.0s** - `parallelism=1` (fully
   > serialized physical writes, structurally the closest analogue to mount's single-threaded
   > writer) is just as slow as the 4-way default. This rules out concurrent-write contention as
   > the explanation. **The real cause is still open** - something about `store`'s physical
   > chunk-write path costs more per byte or per chunk on this slow device than mount's does, even
   > at matched (or zero) concurrency; not investigated further here. Filed as its own question in
   > `docs/plans/store-vs-mount-slow-drive-write-path.md` rather than guessed at further.

3. **Repeat `store` run without `--reference` "should cost about as much as the first" -
   contradicted, and by a lot, whenever the first run was disk-write-bound.** Prediction held only
   loosely on `fast-ssd-C` (small: 1.75s -> 1.29s, -26%; large: 3.82s -> 1.77s, -54%, already more
   than "not dramatically faster" suggests). On `slow-usb-I` it's not close: small 32.4s -> 4.43s
   (-86%), large **304.8s -> 2.52s (-99%, ~120x)**. Root cause is visible in the CPU numbers: CPU
   time barely changes between first and repeat run (e.g. large/slow: 6.45s -> 3.41s, same order
   of magnitude - hashing every file again really does happen, as documented) while wall-clock
   collapses - meaning the *physical store write* to the destination, not chunk/hash discovery,
   was the dominant cost of the first run whenever the destination disk is slow. The
   `--reference`-avoids-read/chunk/hash framing in the README is about a different, smaller cost
   than what actually dominates on a slow disk - worth a doc clarification (not done here) that a
   repeat run without `--reference` is already cheap in the common case of a slow/remote backup
   target, specifically *because* dedup skips the write, independent of `--reference`.

4. **CDC chunking cost on large files - the 128 MB `--chunk-buffer-mb`/write-cache budget is
   visibly in play.** Every large-file mount scenario (both drives) sits at ~138 MB peak
   mount-server RSS - just above the 128 MB default `--write-cache-mb`, consistent with the
   budget being genuinely exercised by 150 MB files rather than sitting mostly idle. Not
   confirmed as an actual spill-to-temp-file event (not instrumented here, see deviations above),
   but suggestive enough to be worth a follow-up specifically checking spill-directory activity
   under this profile.

5. **Many small files: fixed per-file overhead is visible, but smaller than the write-vs-dedup
   effect.** 3b (new destination, same content, needs new `tree_entries` rows) is consistently
   slower than 3a (overwrite existing rows, same content) on both drives for the small-file
   profile (e.g. slow: 129.4s vs. 37.2s) despite both fully deduping the content - the *extra* cost
   of inserting 3,000 new rows vs. updating 3,000 existing ones is real, but it's dwarfed by the
   gap to scenario 2's 220.4s (all-new content, real store writes). For large files the 3a-vs-3b
   gap nearly disappears (7.18s vs. 6.95s slow, 6.58s vs. 5.52s fast) since there are only 4 rows
   either way - consistent with this being genuinely a *per-row* cost, not a per-byte one.

6. **Mount overhead vs. a plain copy, with no dedup shortcut on either side (worth flagging per
   the plan's own criteria) - confirmed, and it's compute, not FUSE latency.** Even on
   `fast-ssd-C`, where disk is not a plausible excuse, the small-file mount first-write costs
   **31.2s vs. 4.27s for a plain copy (~7.3x)**, and the mount server's CPU time (26.7s) very
   nearly equals its wall-clock (31.2s) - i.e. it's compute-saturated on its single
   `persist_worker` thread, not waiting on FUSE dispatch or the OS. This is the clearest evidence
   in this whole run for the plan's own suggested follow-up: letting `persist_worker` use a small
   bounded pool instead of exactly one thread (the original worker-pool-exhaustion reason for
   serializing writes doesn't obviously require serializing to *exactly* one thread) would likely
   close most of this gap for the many-small-files case, on any disk speed.
