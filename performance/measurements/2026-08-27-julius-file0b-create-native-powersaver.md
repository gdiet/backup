# Zero-byte file creation - native - julius/native Windows/local SSD (Power Saver)

## Setup
- Date: 2026-08-27
- Machine: julius
- Execution environment: native Windows (Windows 10 IoT Enterprise LTSC, build 19044)
- Power profile: base scheme unchanged (`381b4222-...`, "Ausbalanciert"/Balanced), power-mode
  overlay switched to "Energiesparmodus" (Power Saver) just before this run - confirmed via
  registry (`ActiveOverlayAcPowerScheme` = `961cc777-2547-4f9d-8174-7d86181b8a7a`) - see
  `2026-08-27-julius-dir-create-native-powersaver.md`'s Setup for the full explanation of this
  overlay dimension.
- IO device: local SSD (julius's internal WDC WDS100T2B0A-00SM50, SATA - see `../machines.md`)
- DedupFS build: n/a - native filesystem baseline, no DedupFS code exercised (see
  `../scripts/README.md`)
- Isolation: none deliberate - same conditions as the companion same-day measurement (see there);
  the test directory was deleted first so this is a fresh tree, not a continuation of that run's.

## Workload
- Operation: Zero-byte file creation, spread across several directories
- Location: native
- Tool: PowerShell `New-Item -ItemType File` (`../scripts/file0b-create.ps1`)
- Mode: sequential
- Window: 20 s
- Scale: 110,131 files total across the 5 runs (21,414-22,451 per run, time-boxed not count-fixed),
  spread round-robin across 20 subdirectories under `C:\dedupfs-perf\files0b`, a fresh tree deleted
  before this run.
- Content: zero-byte (empty files)

## Results
| Run | Result |
|---|---|
| 1 (discarded?) | 21,912 files, 20.01 s, 1095.3 ops/s |
| 2 | 22,146 files, 20.01 s, 1106.5 ops/s |
| 3 | 22,451 files, 20.00 s, 1122.5 ops/s |
| 4 | 22,208 files, 20.01 s, 1110.1 ops/s |
| 5 | 21,414 files, 20.00 s, 1070.6 ops/s |

Mean: 1101.0 ops/s Range: 1070.6 - 1122.5 ops/s (N=5)

## Notes
Run 1 is within the discard threshold, kept.

**Counterintuitive finding**, same direction as the companion directory-creation Power Saver run:
this measurement (1101.0 ops/s mean) is ~17% *faster* than the same-day, same-machine run under
(developer-reported) Best Performance (938.8 ops/s - see
`2026-08-27-julius-file0b-create-native.md`) - the opposite of the naively expected direction. See
that companion Power Saver protocol's Notes for the same caveat about unconfirmed confounds; this
result reinforces that it is not a one-off fluke specific to directory creation, but neither
measurement pins down why.
