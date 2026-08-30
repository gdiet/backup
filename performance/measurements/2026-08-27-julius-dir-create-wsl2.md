# Directory creation - native - julius/WSL2 Debian 12/local SSD

## Setup
- Date: 2026-08-27
- Machine: julius
- Execution environment: WSL2, Debian 12 (bookworm), kernel 6.18.33.2-microsoft-standard-WSL2
  (WSL 2.7.12.0). Working directory `~/dedupfs-perf/dirs` - WSL2's own native filesystem, not
  `/mnt/c/...` (see `../methodology.md`'s "Execution environment" note and
  `../scripts/README.md`).
- Power profile: Balanced (Windows host; `powercfg /getactivescheme` →
  `381b4222-f694-41f0-9685-ff5bb260df2e` - WSL2 has no separate `cpufreq` governor of its own here,
  it shares the host's scheduling).
- IO device: local SSD (julius's internal WDC WDS100T2B0A-00SM50, SATA, reached via WSL2's own
  virtual disk, not the DrvFs/9p bridge - see `../machines.md`)
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
- Tool: bash `mkdir` (`../scripts/dir-create.sh`)
- Mode: sequential
- Window: 20 s
- Scale: 40,847 directories total across the 5 runs (7,915-8,252 per run, time-boxed not
  count-fixed); one growing tree under `~/dedupfs-perf/dirs`, empty at the start of this, the first
  measurement taken against it.
- Content: n/a (directories, not files)

## Results
| Run | Result |
|---|---|
| 1 (discarded?) | 7,915 dirs, 20 s, 395 ops/s |
| 2 | 8,252 dirs, 20 s, 412 ops/s |
| 3 | 8,233 dirs, 20 s, 411 ops/s |
| 4 | 8,220 dirs, 20 s, 411 ops/s |
| 5 | 8,227 dirs, 20 s, 411 ops/s |

Mean: 408.0 ops/s Range: 395 - 412 ops/s (N=5)

## Notes
Run 1 (395 ops/s) is ~4% below the median of runs 2-5 (411 ops/s) - negligible, well under the
50%-slower discard threshold, so kept. Runs 2-5 are remarkably flat (411-412 ops/s) despite the
tree growing to ~41,000 directories by the end - no visible scale effect.

Notably slower than the native-Windows run on the same machine, same day (~408 vs ~822 ops/s mean -
see `2026-08-27-julius-dir-create-native.md`). This run stayed on WSL2's own filesystem, so the
DrvFs/9p bridge is not the explanation; the gap likely reflects WSL2's virtualized-disk IO path
and/or `mkdir` running as a separate coreutils process per call versus PowerShell's `New-Item`
cmdlet running in-process - not disentangled by this measurement. Also relevant per `../machines.md`:
this machine's WSL2 is memory-constrained (~3.8 GiB) relative to its 8 GB host, though this
workload is not obviously memory-bound.

**Retroactive addendum** (added after the fact, once a follow-up measurement revealed the gap):
this protocol's Setup only checked the base power scheme (Balanced) at the time, not the separate
power-mode overlay - that dimension was not yet known to be worth capturing. The developer reports
the overlay was "Höchstleistung"/Best Performance during this run, not independently verified by a
captured overlay GUID. See `2026-08-27-julius-dir-create-wsl2-powersaver.md` for a same-day
follow-up under a confirmed "Energiesparmodus"/Power Saver overlay - unlike the native-Windows pair,
this WSL2 pair showed essentially no power-mode effect either direction.
