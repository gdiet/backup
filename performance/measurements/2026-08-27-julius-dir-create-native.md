# Directory creation - native - julius/native Windows/local SSD

## Setup
- Date: 2026-08-27
- Machine: julius
- Execution environment: native Windows (Windows 10 IoT Enterprise LTSC, build 19044)
- Power profile: Balanced (`powercfg /getactivescheme` → `381b4222-f694-41f0-9685-ff5bb260df2e`)
- IO device: local SSD (julius's internal WDC WDS100T2B0A-00SM50, SATA - see `../machines.md`)
- DedupFS build: n/a - native filesystem baseline, no DedupFS code exercised (see
  `../scripts/README.md`)
- Isolation: none deliberate - an ordinary interactive development machine, with a Claude Code
  Desktop session (which produced this measurement) and the developer's own, separately connected
  VSCode-over-SSH session both present throughout. No other applications or background services
  (indexing, antivirus real-time scan, etc.) were closed or checked beforehand. Machine otherwise
  idle from a human's perspective; all four measurements in this batch ran back-to-back in the same
  session.

## Workload
- Operation: Directory creation
- Location: native
- Tool: PowerShell `New-Item -ItemType Directory` (`../scripts/dir-create.ps1`)
- Mode: sequential
- Window: 20 s
- Scale: 82,223 directories total across the 5 runs (15,270-17,159 per run, time-boxed not
  count-fixed); one growing tree under `C:\dedupfs-perf\dirs`, empty at the start of this, the
  first measurement taken against it.
- Content: n/a (directories, not files)

## Results
| Run | Result |
|---|---|
| 1 (discarded?) | 15,270 dirs, 20.00 s, 763.4 ops/s |
| 2 | 16,595 dirs, 20.01 s, 829.1 ops/s |
| 3 | 17,159 dirs, 20.00 s, 857.8 ops/s |
| 4 | 16,753 dirs, 20.01 s, 837.3 ops/s |
| 5 | 16,446 dirs, 20.01 s, 822.1 ops/s |

Mean: 821.9 ops/s Range: 763.4 - 857.8 ops/s (N=5)

## Notes
Run 1 (763.4 ops/s) came in ~8% below the median of runs 2-5 (~833.2 ops/s) - a mild warmup effect,
but well short of the 50%-slower discard threshold in `../methodology.md`, so it was kept. Runs 2-5
sit in a ~822-858 ops/s band with no monotonic trend despite the underlying tree growing by roughly
16,000-17,000 entries each run (reaching ~82,000 total by the end) - no scale-dependent slowdown is
visible within this single measurement's range.

**Retroactive addendum** (added after the fact, once a follow-up measurement revealed the gap):
this protocol's Setup only checked the base power scheme (Balanced) at the time, not the separate
power-mode overlay - that dimension was not yet known to be worth capturing. The developer reports
the overlay was "Höchstleistung"/Best Performance during this run, not independently verified by a
captured overlay GUID. See `2026-08-27-julius-dir-create-native-powersaver.md` for a same-day
follow-up under a confirmed "Energiesparmodus"/Power Saver overlay, and its Notes for the
counterintuitive comparison between the two.
