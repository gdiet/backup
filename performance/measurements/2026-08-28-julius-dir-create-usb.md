# Directory creation - native - julius/native Windows/USB stick

## Setup
- Date: 2026-08-28
- Machine: julius
- Execution environment: native Windows (Windows 10 IoT Enterprise LTSC, build 19044)
- Power profile: Balanced base scheme, "Best power efficiency"/Power Saver overlay ("Längste
  Akkulaufzeit") - confirmed via `powercfg /query <guid>` → `GUID-Alias: OVERLAY_SCHEME_MIN`. On AC
  power (developer-confirmed).
- IO device: external USB stick, drive `I:`, NTFS, labeled "USB Stick", ~4 GB total capacity
  (`Get-Volume`: 4,023,349,248 B). Measured write throughput ~8.7 MB/s via a single 100 MB
  `WriteAllBytes` probe before this session - USB2-class speed, confirmed by the developer as a
  "slow USB2 stick". Free space checked before starting: 3.71 GB.
- DedupFS build: n/a - native filesystem baseline, no DedupFS code exercised
- Isolation: none deliberate - ordinary interactive development machine, this Claude Code Desktop
  session present throughout, nothing else closed or checked beforehand.

## Workload
- Operation: Directory creation
- Location: native
- Tool: PowerShell `New-Item -ItemType Directory` (same logic as `../scripts/dir-create.ps1`, run
  inline against `I:\dedupfs-perf\dirs` rather than editing the script, which hardcodes `C:\...`)
- Mode: sequential
- Window: 20 s
- Scale: 15,850 directories total across the 5 runs (3,074-3,285 per run, time-boxed not
  count-fixed); first measurement against this tree.
- Content: n/a (directories, not files)

## Results
| Run | Result |
|---|---|
| 1 (discarded?) | 3,285 dirs, 20.11 s, 163.3 ops/s |
| 2 | 3,074 dirs, 20.00 s, 153.7 ops/s |
| 3 | 3,109 dirs, 20.00 s, 155.4 ops/s |
| 4 | 3,192 dirs, 20.00 s, 159.6 ops/s |
| 5 | 3,190 dirs, 20.01 s, 159.4 ops/s |

Mean: 158.3 ops/s Range: 153.7 - 163.3 ops/s (N=5)

## Notes
Run 1 is within the discard threshold, kept. Tight spread, no trend.

Directory creation, which writes no file content, is still ~5.8x slower on this USB2 stick than on
this same machine's internal SSD (158.3 vs. 929.1 ops/s mean, both native Windows, same power
profile - see `2026-08-27-julius-dir-create-native-powersaver.md`) - consistent with USB2's per-
command protocol latency dominating even a metadata-only operation, not just its raw throughput
ceiling (which is irrelevant here, since no bytes are written).
