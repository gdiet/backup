# Zero-byte file creation - native - julius/WSL2 Debian 12/local SSD

## Setup
- Date: 2026-08-27
- Machine: julius
- Execution environment: WSL2, Debian 12 (bookworm), kernel 6.18.33.2-microsoft-standard-WSL2
  (WSL 2.7.12.0). Working directory `~/dedupfs-perf/files0b` - WSL2's own native filesystem, not
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
- Operation: Zero-byte file creation, spread across several directories
- Location: native
- Tool: bash `touch` (`../scripts/file0b-create.sh`)
- Mode: sequential
- Window: 20 s
- Scale: 66,789 files total across the 5 runs (13,119-13,441 per run, time-boxed not count-fixed),
  spread round-robin across 20 subdirectories under `~/dedupfs-perf/files0b`, empty at the start of
  this, the first measurement taken against it.
- Content: zero-byte (empty files)

## Results
| Run | Result |
|---|---|
| 1 (discarded?) | 13,119 files, 20 s, 655 ops/s |
| 2 | 13,411 files, 20 s, 670 ops/s |
| 3 | 13,441 files, 20 s, 672 ops/s |
| 4 | 13,416 files, 20 s, 670 ops/s |
| 5 | 13,402 files, 20 s, 670 ops/s |

Mean: 667.4 ops/s Range: 655 - 672 ops/s (N=5)

## Notes
Run 1 (655 ops/s) is ~2% below the median of runs 2-5 (670 ops/s) - negligible, well under the
50%-slower discard threshold, so kept. Runs 2-5 are essentially flat (670-672 ops/s) despite the
tree growing to ~67,000 files by the end - no visible scale effect.

Notably slower than the native-Windows run on the same machine, same day (~667 vs ~939 ops/s mean -
see `2026-08-27-julius-file0b-create-native.md`), the same direction and rough magnitude as the
directory-creation gap above - consistent with a general WSL2-vs-native-Windows IO/process-overhead
difference on this machine rather than something specific to one operation. Not disentangled
further by this measurement.

**Retroactive addendum** (added after the fact, once a follow-up measurement revealed the gap):
this protocol's Setup only checked the base power scheme (Balanced) at the time, not the separate
power-mode overlay - that dimension was not yet known to be worth capturing. The developer reports
the overlay was "Höchstleistung"/Best Performance during this run, not independently verified by a
captured overlay GUID. See `2026-08-27-julius-file0b-create-wsl2-powersaver.md` for a same-day
follow-up under a confirmed "Energiesparmodus"/Power Saver overlay - unlike the other three pairs in
this batch, this one *did* slow down under Power Saver, in the naively expected direction.
