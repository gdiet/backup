# Zero-byte file creation - native - julius/native Windows/local SSD

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
- Operation: Zero-byte file creation, spread across several directories
- Location: native
- Tool: PowerShell `New-Item -ItemType File` (`../scripts/file0b-create.ps1`)
- Mode: sequential
- Window: 20 s
- Scale: 93,905 files total across the 5 runs (17,813-19,552 per run, time-boxed not count-fixed),
  spread round-robin across 20 subdirectories under `C:\dedupfs-perf\files0b`, empty at the start of
  this, the first measurement taken against it.
- Content: zero-byte (empty files)

## Results
| Run | Result |
|---|---|
| 1 (discarded?) | 17,813 files, 20.01 s, 890.3 ops/s |
| 2 | 19,179 files, 20.00 s, 958.8 ops/s |
| 3 | 17,952 files, 20.00 s, 897.4 ops/s |
| 4 | 19,552 files, 20.01 s, 977.3 ops/s |
| 5 | 19,409 files, 20.01 s, 970.0 ops/s |

Mean: 938.8 ops/s Range: 890.3 - 977.3 ops/s (N=5)

## Notes
Run 1 (890.3 ops/s) is ~8% below the median of runs 2-5 (~964.4 ops/s) - mild, well under the
50%-slower discard threshold in `../methodology.md`, so kept. Runs 2-5 oscillate in an
~897-977 ops/s band with no monotonic trend despite the tree growing to ~94,000 files by the end -
no scale-dependent slowdown is visible within this single measurement's range. Noticeably faster
than this same machine's directory creation (~939 vs ~822 ops/s mean) despite file creation
intuitively doing more work (an inode plus a directory-entry link, vs. just a directory entry) -
not investigated further here.

**Retroactive addendum** (added after the fact, once a follow-up measurement revealed the gap):
this protocol's Setup only checked the base power scheme (Balanced) at the time, not the separate
power-mode overlay - that dimension was not yet known to be worth capturing. The developer reports
the overlay was "Höchstleistung"/Best Performance during this run, not independently verified by a
captured overlay GUID. See `2026-08-27-julius-file0b-create-native-powersaver.md` for a same-day
follow-up under a confirmed "Energiesparmodus"/Power Saver overlay, and its Notes for the
counterintuitive comparison between the two.
