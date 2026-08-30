# 10 MB file creation - native - julius/native Windows/local SSD

## Setup
- Date: 2026-08-28
- Machine: julius
- Execution environment: native Windows (Windows 10 IoT Enterprise LTSC, build 19044)
- Power profile: Balanced base scheme, "Best power efficiency"/Power Saver overlay ("Längste
  Akkulaufzeit") - confirmed via `powercfg /query <guid>` → `GUID-Alias: OVERLAY_SCHEME_MIN`. On AC
  power (developer-confirmed).
- IO device: local SSD (julius's internal WDC WDS100T2B0A-00SM50, SATA - see `../machines.md`).
  Free space checked before this run: ~103 GB free, well above what this workload needs.
- DedupFS build: n/a - native filesystem baseline, no DedupFS code exercised
- Isolation: none deliberate - ordinary interactive development machine, this Claude Code Desktop
  session present throughout, nothing else closed or checked beforehand.

## Workload
- Operation: File creation at 10 MB, unique content (spans several chunks at the default CDC
  chunking configuration)
- Location: native
- Tool: PowerShell `[System.IO.File]::WriteAllBytes` (`../scripts/file10mb-create.ps1`)
- Mode: sequential
- Window: 20 s
- Scale: 2,642 files total across the 5 runs (425-626 per run, time-boxed not count-fixed), spread
  round-robin across 20 subdirectories under `C:\dedupfs-perf\files10mb`, first measurement against
  this tree. ~26.4 GB total written (matches the observed ~27 GB free-space drop).
- Content: 10 MB (10,485,760 B), unique per file (random template with fresh random bytes poked in
  every 64 KiB, generator outside the timed loop)

## Results
| Run | Result |
|---|---|
| 1 (discarded?) | 592 files, 20.01 s, 29.6 ops/s |
| 2 | 626 files, 20.01 s, 31.3 ops/s |
| 3 | 551 files, 20.04 s, 27.5 ops/s |
| 4 | 448 files, 20.06 s, 22.3 ops/s |
| 5 | 425 files, 20.02 s, 21.2 ops/s |

Mean: 26.4 ops/s Range: 21.2 - 31.3 ops/s (N=5)

## Notes
Run 1 is within the discard threshold, kept. **Real monotonic-ish downward trend** after run 2
(31.3 → 27.5 → 22.3 → 21.2 ops/s, a ~32% drop from peak to run 5) - per `../methodology.md`'s
"state between runs" guidance, this is reported as a genuine scale-dependent finding, not
normalized away: throughput degrades noticeably as the accumulated write volume on this SSD grows
across the runs (peaking around 6 GB into the measurement, ending around 26 GB). Plausible causes
include SSD write-amplification/garbage-collection effects becoming visible only at this file size
and sustained write volume (not seen at 100 B/30 KB, where total bytes written stay in the hundreds
of MB) - not isolated further here; worth watching for on a repeat run, and worth comparing against
`3327`'s NVMe SSD if a same-size measurement is taken there.
