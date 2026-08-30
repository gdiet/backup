# 10 MB file creation - native - julius/native Windows/USB stick

## Setup
- Date: 2026-08-28
- Machine: julius
- Execution environment: native Windows (Windows 10 IoT Enterprise LTSC, build 19044)
- Power profile: Balanced base scheme, "Best power efficiency"/Power Saver overlay ("Längste
  Akkulaufzeit") - confirmed via `powercfg /query <guid>` → `GUID-Alias: OVERLAY_SCHEME_MIN`. On AC
  power (developer-confirmed).
- IO device: external USB stick, drive `I:`, NTFS, labeled "USB Stick", ~4 GB total capacity.
  Measured write throughput ~8.7 MB/s via a single 100 MB `WriteAllBytes` probe immediately before
  this run - USB2-class speed, confirmed by the developer as a "slow USB2 stick". Free space
  checked before starting: 3.71 GB (after the `dir-create` run above, which used negligible space)
  - deliberately checked given this size's normal several-GB footprint on faster storage (see
  `../scripts/file10mb-create.ps1`'s own header warning); at this device's measured throughput, a
  20 s window was expected to write at most ~200 MB, well within budget.
- DedupFS build: n/a - native filesystem baseline, no DedupFS code exercised
- Isolation: none deliberate - ordinary interactive development machine, this Claude Code Desktop
  session present throughout, nothing else closed or checked beforehand.

## Workload
- Operation: File creation at 10 MB, unique content
- Location: native
- Tool: PowerShell `[System.IO.File]::WriteAllBytes` (same logic as
  `../scripts/file10mb-create.ps1`, run inline against `I:\dedupfs-perf\files10mb`)
- Mode: sequential
- Window: 20 s nominal - actual per-run elapsed overshoots to 20.05-21.07 s (a single 10 MB write
  at this throughput takes over a second and cannot be interrupted mid-call, same overshoot cause
  as `2026-08-28-julius-dir-listing-native.md`)
- Scale: 108 files total across the 5 runs (19-23 per run, time-boxed not count-fixed), spread
  round-robin across 20 subdirectories, first measurement against this tree. ~1.08 GB total
  written, leaving 2.65 GB free afterward.
- Content: 10 MB (10,485,760 B), unique per file (random template with fresh random bytes poked in
  every 64 KiB, generator outside the timed loop)

## Results
| Run | Result |
|---|---|
| 1 (discarded?) | 22 files, 20.05 s, 1.10 ops/s |
| 2 | 23 files, 20.59 s, 1.12 ops/s |
| 3 | 23 files, 21.07 s, 1.09 ops/s |
| 4 | 21 files, 20.71 s, 1.01 ops/s |
| 5 | 19 files, 20.05 s, 0.95 ops/s |

Mean: 1.05 ops/s Range: 0.95 - 1.12 ops/s (N=5)

## Notes
Run 1 is within the discard threshold, kept. A mild downward drift (1.12 → 1.09 → 1.01 → 0.95 from
run 2 onward) - much smaller in absolute terms than the internal-SSD 10 MB measurements' trends,
plausibly noise at this small a sample size (19-23 files/run) rather than a real device-level
effect, but in the same direction; not enough runs at this scale to tell apart from this
measurement alone.

Effective throughput: 1,080 MB / ~102.5 s total run time ≈ **10.5 MB/s** - close to, and somewhat
higher than, the standalone 8.7 MB/s single-file probe taken just before this run; both are
consistent with USB2-class sequential write speed. At this rate the naive per-run capacity
estimate used to plan this measurement (≤200 MB/run) held: actual usage was ~200-230 MB/run,
confirming the stick was never at meaningful risk of filling during this run.
