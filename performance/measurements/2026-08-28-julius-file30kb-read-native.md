# 30 KB file read - native - julius/native Windows/local SSD

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
- Operation: Reading back 30 KB files (analogous to the corresponding write)
- Location: native
- Tool: PowerShell `[System.IO.File]::ReadAllBytes` (`../scripts/file30kb-read.ps1`)
- Mode: sequential
- Window: 20 s
- Scale: 376,246 reads total across the 5 runs, against the 20,970-file tree created by
  `2026-08-28-julius-file30kb-create-native.md` immediately before this run (reads repeat across
  the fixed file set, ~615 MB total, well within page cache).
- Content: 30 KB, unique per file (as created)

## Results
| Run | Result |
|---|---|
| 1 (discarded?) | 75,313 reads, 20.01 s, 3763.2 ops/s |
| 2 | 76,223 reads, 20.01 s, 3809.4 ops/s |
| 3 | 76,260 reads, 20.01 s, 3811.1 ops/s |
| 4 | 75,535 reads, 20.01 s, 3775.3 ops/s |
| 5 | 72,915 reads, 20.00 s, 3645.7 ops/s |

Mean: 3760.9 ops/s Range: 3645.7 - 3811.1 ops/s (N=5)

## Notes
Run 1 is within the discard threshold, kept. Nearly identical ops/s to the 100 B read measurement
(3760.9 vs. 3964.4 ops/s mean - `2026-08-28-julius-file100b-read-native.md`) despite the 300x larger
file size - consistent with both being page-cache-warm reads dominated by per-call overhead rather
than actual bytes transferred (see that file's Notes); the working set here (~615 MB) still fits
comfortably in this machine's 8 GB RAM.
