# 100 B file read - native - julius/native Windows/local SSD

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
- Operation: Reading back 100 B files (analogous to the corresponding write)
- Location: native
- Tool: PowerShell `[System.IO.File]::ReadAllBytes` (`../scripts/file100b-read.ps1`)
- Mode: sequential
- Window: 20 s
- Scale: 396,641 reads total across the 5 runs, against the 24,281-file tree created by
  `2026-08-28-julius-file100b-create-native.md` immediately before this run (reads repeat across
  the fixed file set as the window runs longer than one full pass).
- Content: 100 B, unique per file (as created)

## Results
| Run | Result |
|---|---|
| 1 (discarded?) | 74,604 reads, 20.01 s, 3728.5 ops/s |
| 2 | 81,809 reads, 20.01 s, 4088.1 ops/s |
| 3 | 78,774 reads, 20.00 s, 3938.0 ops/s |
| 4 | 76,861 reads, 20.01 s, 3841.0 ops/s |
| 5 | 84,593 reads, 20.01 s, 4226.5 ops/s |

Mean: 3964.4 ops/s Range: 3728.5 - 4226.5 ops/s (N=5)

## Notes
Run 1 is within the discard threshold, kept. Reads run at roughly 16x the create throughput for the
same file size (3964.4 vs. 242.6 ops/s mean) - expected, since these are almost certainly
page-cache-warm reads of a working set this small (24,281 files x 100 B ≈ 2.4 MB total, far below
this machine's 8 GB RAM) rather than a fair reflection of cold-read SSD latency; not a workload
this size ladder currently isolates.
