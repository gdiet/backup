# Directory creation - native - julius/WSL2 Debian 12/local SSD (Power Saver)

## Setup
- Date: 2026-08-27
- Machine: julius
- Execution environment: WSL2, Debian 12 (bookworm), kernel 6.18.33.2-microsoft-standard-WSL2
  (WSL 2.7.12.0). Working directory `~/dedupfs-perf/dirs` - WSL2's own native filesystem, not
  `/mnt/c/...` (see `../methodology.md`'s "Execution environment" note).
- Power profile: base scheme unchanged (`381b4222-...`, "Ausbalanciert"/Balanced), power-mode
  overlay switched to "Energiesparmodus" (Power Saver) just before this run - confirmed via
  registry (`ActiveOverlayAcPowerScheme` = `961cc777-2547-4f9d-8174-7d86181b8a7a`) on the Windows
  host; WSL2 has no separate power-mode setting of its own and shares the host's - see
  `2026-08-27-julius-dir-create-native-powersaver.md`'s Setup for the full explanation.
- IO device: local SSD (julius's internal WDC WDS100T2B0A-00SM50, SATA, reached via WSL2's own
  virtual disk, not the DrvFs/9p bridge - see `../machines.md`)
- DedupFS build: n/a - native filesystem baseline, no DedupFS code exercised (see
  `../scripts/README.md`)
- Isolation: none deliberate - same conditions as the companion same-day measurement (see there);
  `~/dedupfs-perf` was deleted first so this is a fresh tree, not a continuation of that run's.

## Workload
- Operation: Directory creation
- Location: native
- Tool: bash `mkdir` (`../scripts/dir-create.sh`)
- Mode: sequential
- Window: 20 s
- Scale: 41,784 directories total across the 5 runs (8,256-8,460 per run, time-boxed not
  count-fixed); a fresh tree under `~/dedupfs-perf/dirs`, deleted before this run.
- Content: n/a (directories, not files)

## Results
| Run | Result |
|---|---|
| 1 (discarded?) | 8,256 dirs, 20 s, 412 ops/s |
| 2 | 8,460 dirs, 20 s, 423 ops/s |
| 3 | 8,446 dirs, 20 s, 422 ops/s |
| 4 | 8,289 dirs, 20 s, 414 ops/s |
| 5 | 8,333 dirs, 20 s, 416 ops/s |

Mean: 417.4 ops/s Range: 412 - 423 ops/s (N=5)

## Notes
Run 1 is within the discard threshold, kept.

Essentially flat relative to the same-day, same-machine Best-Performance-overlay run (417.4 vs.
408.0 ops/s mean - see `2026-08-27-julius-dir-create-wsl2.md`), a ~2% difference well within
ordinary run-to-run noise - unlike the two native-Windows Power Saver runs above, this one shows no
notable power-mode effect either direction. The native-Windows-vs-WSL2 gap seen in the first batch
persists here too (417.4 vs. 929.1 ops/s this same session).
