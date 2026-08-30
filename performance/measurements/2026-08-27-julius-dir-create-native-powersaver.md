# Directory creation - native - julius/native Windows/local SSD (Power Saver)

## Setup
- Date: 2026-08-27
- Machine: julius
- Execution environment: native Windows (Windows 10 IoT Enterprise LTSC, build 19044)
- Power profile: base scheme unchanged (`381b4222-...`, "Ausbalanciert"/Balanced), but the
  developer switched the power-mode overlay to "Energiesparmodus" (Power Saver) just before this
  run - confirmed via registry (`HKLM:\SYSTEM\CurrentControlSet\Control\Power\User\PowerSchemes`,
  `ActiveOverlayAcPowerScheme` = `961cc777-2547-4f9d-8174-7d86181b8a7a`, the well-known "Best power
  efficiency" overlay GUID; not resolved to a friendly name by any command tried, but this GUID is
  consistently documented as Power Saver). `powercfg /getactivescheme` alone does not show this
  overlay dimension - see the companion, non-"-powersaver" measurement from the same day for the
  contrast (developer-reported "Höchstleistung"/Best Performance overlay then, not independently
  captured at the time - see that file's Notes for the retroactive caveat).
- IO device: local SSD (julius's internal WDC WDS100T2B0A-00SM50, SATA - see `../machines.md`)
- DedupFS build: n/a - native filesystem baseline, no DedupFS code exercised (see
  `../scripts/README.md`)
- Isolation: none deliberate - same conditions as the companion same-day measurement (see there);
  the test directory was deleted first so this is a fresh tree, not a continuation of that run's.

## Workload
- Operation: Directory creation
- Location: native
- Tool: PowerShell `New-Item -ItemType Directory` (`../scripts/dir-create.ps1`)
- Mode: sequential
- Window: 20 s
- Scale: 92,937 directories total across the 5 runs (17,596-19,198 per run, time-boxed not
  count-fixed); a fresh tree under `C:\dedupfs-perf\dirs`, deleted before this run.
- Content: n/a (directories, not files)

## Results
| Run | Result |
|---|---|
| 1 (discarded?) | 18,036 dirs, 20.00 s, 901.7 ops/s |
| 2 | 17,596 dirs, 20.01 s, 879.5 ops/s |
| 3 | 19,066 dirs, 20.01 s, 952.6 ops/s |
| 4 | 19,041 dirs, 20.00 s, 951.9 ops/s |
| 5 | 19,198 dirs, 20.00 s, 959.8 ops/s |

Mean: 929.1 ops/s Range: 879.5 - 959.8 ops/s (N=5)

## Notes
Run 1 is within the discard threshold (well above 50% of the runs 2-5 median), kept.

**Counterintuitive finding**: this Power Saver run (929.1 ops/s mean) is ~13% *faster* than the
companion same-day, same-machine, same-operation run under (developer-reported) Best Performance
(821.9 ops/s - see `2026-08-27-julius-dir-create-native.md`). This is the opposite of the naively
expected direction. Not explained by this measurement - possible confounds include ordinary run-to-
run variance (each measurement here is a single 5-run sample, not repeated), background system
activity differing between the two sessions (Isolation was not controlled in either), and the
overlay setting's actual effect on this specific IO-bound, mostly-single-threaded workload possibly
being smaller or differently directed than the "Power Saver = slower" intuition assumes. Flagged
here rather than smoothed over; would need a repeated/controlled A/B run to say more.
