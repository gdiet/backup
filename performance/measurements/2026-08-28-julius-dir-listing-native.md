# Directory listing - native - julius/native Windows/local SSD

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
- Operation: Directory listing (readdir)
- Location: native
- Tool: PowerShell `Get-ChildItem` (`../scripts/dir-listing.ps1`)
- Mode: sequential
- Window: 20 s nominal - actual per-run elapsed overshoots to 20.55-21.67 s (see Notes)
- Scale: fixed 50,000-entry directory under `C:\dedupfs-perf\listing`, built once before this
  measurement; 54 listings total across the 5 runs.
- Content: n/a (directory listing, no file content)

## Results
| Run | Result |
|---|---|
| 1 (discarded?) | 11 listings, 21.67 s, 0.51 ops/s |
| 2 | 11 listings, 21.11 s, 0.52 ops/s |
| 3 | 10 listings, 20.55 s, 0.49 ops/s |
| 4 | 11 listings, 21.46 s, 0.51 ops/s |
| 5 | 11 listings, 21.17 s, 0.52 ops/s |

Mean: 0.51 ops/s Range: 0.49 - 0.52 ops/s (N=5)

## Notes
Run 1 is within the discard threshold, kept. Each `Get-ChildItem` call over 50,000 entries takes
roughly 2 s and cannot be interrupted mid-call, so the fixed 20 s window overshoots by the time of
whichever call was in progress when the deadline passed (20.55-21.67 s actual, not exactly 20 s) -
the script's own loop-timing limitation at this scale, not a methodology deviation; ops/s is
computed from each run's own actual elapsed time, not the nominal 20 s. Listing this same 50,000-
entry tree is roughly 4,200x slower per-call than a single lookup against the (larger, 100,000-
entry) lookup tree in the companion `2026-08-28-julius-dir-lookup-native.md` measurement, as
expected for an operation whose cost scales with entry count rather than being O(1).
