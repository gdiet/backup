# File creation (10 MB) - dfs-mount - julius/native Windows/local SSD

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
  background services were closed or checked beforehand.

## Workload
- Operation: File creation, 10 MB
- Location: dfs-mount
- Tool: PowerShell `[System.IO.File]::WriteAllBytes` against a `dfs mount --read-write`
  mountpoint (`../scripts/dfs-mount-file10mb-create.ps1`), WinFSP. Same once-filled-template-
  plus-poke content scheme as `file10mb-create.ps1`'s native measurement - unique content per
  file, generator kept out of the timed loop.
- Mode: sequential
- Window: 20 s
- Scale: 1,451 files total across the 5 runs (213-465 per run, time-boxed not count-fixed),
  spread across 20 subdirectories; ~14.9 GB written.
- Content: 10 MB per file, unique (verified via the repository's own post-run dedup ratio, see
  Notes)

## Results
| Run | Result |
|---|---|
| 1 (discarded?) | 465 files, 20.07s, 23.2 ops/s |
| 2 | 213 files, 20.00s, 10.6 ops/s |
| 3 | 264 files, 20.11s, 13.1 ops/s |
| 4 | 285 files, 20.02s, 14.2 ops/s |
| 5 | 224 files, 20.01s, 11.2 ops/s |

Mean: 14.46 ops/s (~144.6 MB/s) Range: 10.6 - 23.2 ops/s (N=5)

## Notes
Run 1 is *faster*, not slower, than the median of runs 2-5 (~12.15 ops/s) - the opposite direction
from the warmup effect the discard rule guards against, so run 1 is kept as-is, same shape as
`2026-08-27-julius-dir-create-native-powersaver.md`'s own run 1.

`dfs stats --repository` against the resulting repository afterward confirmed exactly 1,451 files,
14,921,236,480 bytes logical and physical, dedup ratio 1.00x - matching Scale above exactly and
confirming every file's content was genuinely unique (no accidental collision from the poke
scheme), the same verification this script's sibling
`2026-09-29-julius-dir-create-dfs-mount.md` needed after that script's readiness-probe bug was
found and fixed (this script shares the same probe pattern and received the same fix before this
run).

Against native Windows 10 MB file creation under the same Power-Saver overlay (26.4 ops/s mean,
`2026-08-28-julius-file10mb-create-native.md`), this dfs-mount result (14.46 ops/s) is roughly
1.8x slower - a real but not dramatic gap for a workload that goes through content-defined
chunking, BLAKE3 hashing, and a SQLite-backed metadata layer on top of the same underlying SSD
write, rather than a bare `WriteAllBytes` to NTFS.
