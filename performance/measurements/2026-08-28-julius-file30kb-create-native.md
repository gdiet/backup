# 30 KB file creation - native - julius/native Windows/local SSD

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
- Operation: File creation at 30 KB, unique content
- Location: native
- Tool: PowerShell `[System.IO.File]::WriteAllBytes` (`../scripts/file30kb-create.ps1`)
- Mode: sequential
- Window: 20 s
- Scale: 20,970 files total across the 5 runs (4,048-4,730 per run, time-boxed not count-fixed),
  spread round-robin across 20 subdirectories under `C:\dedupfs-perf\files30kb`, first measurement
  against this tree. ~615 MB total written.
- Content: 30 KB (30,720 B), unique per file (random template with fresh random bytes poked in
  every 64 KiB, generator outside the timed loop)

## Results
| Run | Result |
|---|---|
| 1 (discarded?) | 4,730 files, 20.01 s, 236.3 ops/s |
| 2 | 4,051 files, 20.04 s, 202.2 ops/s |
| 3 | 4,085 files, 20.01 s, 204.1 ops/s |
| 4 | 4,048 files, 20.01 s, 202.3 ops/s |
| 5 | 4,056 files, 20.03 s, 202.5 ops/s |

Mean: 209.5 ops/s Range: 202.2 - 236.3 ops/s (N=5)

## Notes
Run 1 (236.3 ops/s) is *faster* than runs 2-5 (~202-204 ops/s), the same pattern seen in the 100 B
create measurement from this same session (`2026-08-28-julius-file100b-create-native.md`) - see
that file's Notes for the same open question about the cause. Runs 2-5 are flat once past run 1.
