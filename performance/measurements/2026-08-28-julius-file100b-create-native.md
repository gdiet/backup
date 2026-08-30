# 100 B file creation - native - julius/native Windows/local SSD

## Setup
- Date: 2026-08-28
- Machine: julius
- Execution environment: native Windows (Windows 10 IoT Enterprise LTSC, build 19044)
- Power profile: Balanced base scheme, "Best power efficiency"/Power Saver overlay ("Längste
  Akkulaufzeit") - confirmed via `powercfg /query <guid>` → `GUID-Alias: OVERLAY_SCHEME_MIN`. On AC
  power (developer-confirmed).
- IO device: local SSD (julius's internal WDC WDS100T2B0A-00SM50, SATA - see `../machines.md`)
- DedupFS build: n/a - native filesystem baseline, no DedupFS code exercised
- Isolation: none deliberate - ordinary interactive development machine, this Claude Code Desktop
  session present throughout, nothing else closed or checked beforehand.

## Workload
- Operation: File creation at 100 B, unique content
- Location: native
- Tool: PowerShell `[System.IO.File]::WriteAllBytes` (`../scripts/file100b-create.ps1`)
- Mode: sequential
- Window: 20 s
- Scale: 24,281 files total across the 5 runs (4,509-5,520 per run, time-boxed not count-fixed),
  spread round-robin across 20 subdirectories under `C:\dedupfs-perf\files100b`, first measurement
  against this tree.
- Content: 100 B, unique per file (random template with fresh random bytes poked in, generator
  outside the timed loop - see `../methodology.md`'s "File-content workloads")

## Results
| Run | Result |
|---|---|
| 1 (discarded?) | 5,520 files, 20.04 s, 275.5 ops/s |
| 2 | 4,509 files, 20.01 s, 225.3 ops/s |
| 3 | 4,730 files, 20.01 s, 236.3 ops/s |
| 4 | 4,759 files, 20.02 s, 237.7 ops/s |
| 5 | 4,763 files, 20.00 s, 238.1 ops/s |

Mean: 242.6 ops/s Range: 225.3 - 275.5 ops/s (N=5)

## Notes
Run 1 (275.5 ops/s) is *faster* than the median of runs 2-5 (~237.0 ops/s), not slower - the
discard rule only triggers on a slower run 1, so this does not apply and there is nothing to
discard either way. The same run-1-faster pattern recurs in the 30 KB create measurement from this
same session (`2026-08-28-julius-file30kb-create-native.md`) - plausibly some one-time setup cost
in the other runs (e.g. filesystem metadata caching effects that help *later* look-ups more than
the very first batch of writes) rather than a warmup benefit specific to run 1; not investigated
further here. Runs 2-5 are otherwise flat (~225-238 ops/s).
