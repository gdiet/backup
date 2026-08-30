# Directory listing - native - julius/WSL2 Debian 12/local SSD

## Setup
- Date: 2026-08-28
- Machine: julius
- Execution environment: WSL2, Debian 12 (bookworm), kernel 6.18.33.2-microsoft-standard-WSL2
  (WSL 2.7.12.0). Working directory `~/dedupfs-perf/listing` - WSL2's own native filesystem, not
  `/mnt/c/...`.
- Power profile: Balanced base scheme, "Best power efficiency"/Power Saver overlay ("Längste
  Akkulaufzeit") on the Windows host - confirmed via `powercfg /query <guid>` →
  `GUID-Alias: OVERLAY_SCHEME_MIN`; WSL2 has no separate power-mode setting of its own. On AC power
  (developer-confirmed).
- IO device: local SSD (julius's internal WDC WDS100T2B0A-00SM50, SATA, reached via WSL2's own
  virtual disk, not the DrvFs/9p bridge - see `../machines.md`)
- DedupFS build: n/a - native filesystem baseline, no DedupFS code exercised
- Isolation: none deliberate - ordinary interactive development machine, this Claude Code Desktop
  session present throughout, nothing else closed or checked beforehand.

## Workload
- Operation: Directory listing (readdir)
- Location: native
- Tool: bash `ls -U`, one process per listing (`../scripts/dir-listing.sh`)
- Mode: sequential
- Window: 20 s
- Scale: fixed 50,000-entry directory under `~/dedupfs-perf/listing`, built once before this
  measurement; 3,431 listings total across the 5 runs.
- Content: n/a (directory listing, no file content)

## Results
| Run | Result |
|---|---|
| 1 (discarded?) | 677 listings, 20 s, 33 ops/s |
| 2 | 698 listings, 20 s, 34 ops/s |
| 3 | 701 listings, 20 s, 35 ops/s |
| 4 | 702 listings, 20 s, 35 ops/s |
| 5 | 653 listings, 20 s, 32 ops/s |

Mean: 33.8 ops/s Range: 32 - 35 ops/s (N=5)

## Notes
Run 1 is within the discard threshold, kept. Tight, flat spread across all 5 runs - no trend, as
expected for a fixed-size directory.

**Same striking environment gap as directory lookup above**: ~66x faster than the equivalent
native-Windows measurement, same machine, same day (33.8 vs. 0.51 ops/s mean - see
`2026-08-28-julius-dir-listing-native.md`), despite this script paying one process-spawn per
listing (`ls -U` invoked fresh each time) where the PowerShell script reuses one long-running
process throughout. This rules out "process-spawn overhead" as the explanation for WSL2's
advantage here - if anything it should have worked against WSL2 - so the gap more likely reflects
`Get-ChildItem`'s own per-entry object-construction/formatting cost (it returns rich `FileInfo`
objects) against `ls -U`'s much thinner unsorted directory-entry read, rather than a filesystem-
level difference. Not disentangled further by this measurement, same caveat as the lookup
protocol above.
