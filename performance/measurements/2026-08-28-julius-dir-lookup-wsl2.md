# Directory lookup - native - julius/WSL2 Debian 12/local SSD

## Setup
- Date: 2026-08-28
- Machine: julius
- Execution environment: WSL2, Debian 12 (bookworm), kernel 6.18.33.2-microsoft-standard-WSL2
  (WSL 2.7.12.0). Working directory `~/dedupfs-perf/lookup` - WSL2's own native filesystem, not
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
- Operation: Directory lookup
- Location: native
- Tool: bash `test -d` (`../scripts/dir-lookup.sh`)
- Mode: sequential
- Window: 20 s
- Scale: fixed 100,000-directory tree under `~/dedupfs-perf/lookup`, built once before this
  measurement; 5,035,069 lookups total across the 5 runs, each a pseudo-random existing entry.
- Content: n/a (directory lookups, no file content)

## Results
| Run | Result |
|---|---|
| 1 (discarded?) | 1,087,619 lookups, 20 s, 54,380 ops/s |
| 2 | 1,113,532 lookups, 20 s, 55,676 ops/s |
| 3 | 1,067,021 lookups, 20 s, 53,351 ops/s |
| 4 | 858,594 lookups, 20 s, 42,929 ops/s |
| 5 | 908,303 lookups, 20 s, 45,415 ops/s |

Mean: 50,350.2 ops/s Range: 42,929 - 55,676 ops/s (N=5)

## Notes
Run 1 is within the discard threshold, kept. There is a real step-down between runs 1-3
(~53,000-56,000 ops/s) and runs 4-5 (~43,000-45,000 ops/s, ~20% lower) - not a monotonic trend
(run 5 > run 4), and not explained by tree growth (the lookup tree is fixed-size). Not isolated
further here; possible causes include background interference on this uncontrolled machine
(Isolation was not managed) or WSL2/host scheduling effects under sustained CPU-bound load - `test
-d` at this rate is almost certainly CPU/syscall-bound, not IO-bound, given the enormous margin
over native Windows below.

**Striking environment gap**: ~25x faster than the equivalent native-Windows measurement, same
machine, same day (50,350.2 vs. 2,145.5 ops/s mean - see `2026-08-28-julius-dir-lookup-native.md`).
The opposite direction, and a far larger magnitude, than the roughly-2x native-Windows-faster
pattern seen for directory/file *creation* on this machine (see the 2026-08-27 measurements'
overview entries) - plausibly `test -d` (a single lightweight `stat`-family syscall) versus
PowerShell's `Test-Path` cmdlet carrying substantial per-call .NET/cmdlet-dispatch overhead that
swamps the actual filesystem cost at this operation's small per-call cost, rather than WSL2's
`ext4`-on-virtual-disk genuinely outperforming NTFS by 25x. Not disentangled further by this
measurement - a lower-level native-Windows lookup tool (e.g. a small compiled probe instead of a
PowerShell cmdlet) would be needed to separate "shell/cmdlet overhead" from "filesystem cost"
cleanly.
