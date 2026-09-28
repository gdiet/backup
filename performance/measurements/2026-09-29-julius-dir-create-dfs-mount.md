# Directory creation - dfs-mount - julius/native Windows/local SSD

## Setup
- Date: 2026-09-29
- Machine: julius
- Execution environment: native Windows (Windows 10 IoT Enterprise LTSC, build 19044)
- Power profile: Balanced base scheme, Power Saver overlay (`powercfg /getactivescheme` →
  `381b4222-f694-41f0-9685-ff5bb260df2e`; overlay GUID `961cc777-2547-4f9d-8174-7d86181b8a7a` →
  `powercfg /query` → `GUID-Alias: OVERLAY_SCHEME_MIN`, i.e. "Best power efficiency"/Power Saver)
- IO device: local SSD (julius's internal WDC WDS100T2B0A-00SM50, SATA - see `../machines.md`);
  `C:\dedupfs-perf`, the same internal SSD as the other `-native` measurements in this directory.
- DedupFS build: `8ac866e224ee03d8a5300f333dba6b3901a09689` on `rust`
- Isolation: none deliberate - an ordinary interactive development machine, with a Claude Code
  Desktop session (which produced this measurement) present throughout. No other applications or
  background services (indexing, antivirus real-time scan, etc.) were closed or checked
  beforehand.

## Workload
- Operation: Directory creation
- Location: dfs-mount
- Tool: PowerShell `New-Item -ItemType Directory` against a `dfs mount --read-write` mountpoint
  (`../scripts/dfs-mount-dir-create.ps1`), WinFSP.
- Mode: sequential
- Window: 20 s
- Scale: 31,604 directories total across the 5 runs (4,676-6,890 per run, time-boxed not
  count-fixed); one growing tree of sibling directories directly under the mount root, in a fresh
  repository.
- Content: n/a (directories, not files)

## Results
| Run | Result |
|---|---|
| 1 (discarded?) | 6,436 dirs, 20.01s, 321.7 ops/s |
| 2 | 4,676 dirs, 20.00s, 233.8 ops/s |
| 3 | 6,852 dirs, 20.00s, 342.5 ops/s |
| 4 | 6,890 dirs, 20.01s, 344.3 ops/s |
| 5 | 6,750 dirs, 20.01s, 337.2 ops/s |

Mean: 315.9 ops/s Range: 233.8 - 344.3 ops/s (N=5)

## Notes
Run 1 (321.7 ops/s) is only ~5% below the median of runs 2-5 (~339.85 ops/s), well under
`../methodology.md`'s 50%-slower discard threshold - kept as-is. Run 2 (233.8 ops/s) is the
clearest outlier of the five but there is no rule for discarding a run other than run 1; reported
as-is, noise on a non-isolated development machine.

This script's first real run against a real WinFSP install (this same session, immediately before
this one) found and fixed a genuine bug in its own readiness probe: `New-Item -ItemType Directory`
silently creates missing *parent* directories too, even without `-Force` (confirmed with a small
standalone test). If the probe ran before the mountpoint itself existed, it would silently create
the mountpoint as an ordinary native NTFS directory and "succeed" against that instead of the real
mount - every operation for the rest of that run then went to plain NTFS, not through DedupFS at
all, with no error anywhere. That first (invalid) run reported ~623-635 ops/s, close to native
Windows `mkdir`'s own power-saver baseline (929.1 ops/s, see
`2026-08-27-julius-dir-create-native-powersaver.md`) - consistent with the theory, since it was
in fact measuring native NTFS both times. Fixed by waiting for the mountpoint to exist
(`Test-Path`, which never creates anything) before ever calling `New-Item` against a path under
it; this run's own `dfs stats --repository` afterward confirmed the repository holds exactly
31,604 directories, matching Scale above - the fix is verified, not just plausible.

Against that same native-Windows Power-Saver baseline (929.1 ops/s), this dfs-mount result's mean
(315.9 ops/s) is roughly 2.9x slower - a real, substantial gap, though from a single 5-run
measurement on a non-isolated machine. Against `2026-09-01-julius-db-direct-mkdir-native.md`'s
db-direct result (196.0 ops/s mean, same Power-Saver overlay, but a monotonically slowing single
growing sibling directory rather than this script's own shape), dfs-mount is faster on this data -
suggestive that the mount layer itself does not add dominant overhead on top of `db::Repository`
for this operation, but the two protocols' trees differ enough in shape (db-direct's single
sibling directory that slows down as it grows, versus dfs-mount's directories created fresh each
run) that this is not a controlled A/B either.
