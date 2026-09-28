# File read (10 MB) - dfs-mount - julius/native Windows/local SSD

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
- Operation: File read, 10 MB
- Location: dfs-mount
- Tool: PowerShell `[System.IO.File]::ReadAllBytes`, pseudo-random index, against a `dfs mount`
  (read-only, no `--read-write`) mountpoint (`../scripts/dfs-mount-file10mb-read.ps1`), WinFSP.
  Reads back the files `2026-09-29-julius-file10mb-create-dfs-mount.md`'s run created.
- Mode: sequential
- Window: 20 s
- Scale: 1,522 reads total across the 5 runs (300-310 per run), pseudo-randomly chosen from the
  1,451 files the create run left behind.
- Content: 10 MB per file (read-only workload, no new content written)

## Results
| Run | Result |
|---|---|
| 1 (discarded?) | 302 reads, 20.07s, 15.1 ops/s |
| 2 | 301 reads, 20.04s, 15.0 ops/s |
| 3 | 306 reads, 20.04s, 15.3 ops/s |
| 4 | 303 reads, 20.02s, 15.1 ops/s |
| 5 | 310 reads, 20.02s, 15.5 ops/s |

Mean: 15.2 ops/s (~152 MB/s) Range: 15.0 - 15.5 ops/s (N=5)

## Notes
By far the tightest spread of any measurement in this directory (15.0-15.5 ops/s, ~3% swing) -
this script's readiness check (wait for a non-empty directory listing) needs no `New-Item` call at
all, so it was never at risk of the readiness-probe bug found and fixed in
`2026-09-29-julius-dir-create-dfs-mount.md`/`2026-09-29-julius-file10mb-create-dfs-mount.md`'s own
scripts; this run's numbers did not need that same re-verification.

Unlike every native-Windows read/create pair in this directory, where reads run several times
faster than the matching creates (page-cache-warm working sets - see `../overview.md`'s "File
read" section), dfs-mount reads (15.2 ops/s) are actually *slightly slower* than dfs-mount creates
(14.46 ops/s, essentially within noise of each other rather than a clear direction either way).
Plausible explanation, not confirmed: reading through the mount means resolving a content's chunk
list and reading each chunk's bytes back out of `crates/store`'s own extent layout, which need not
be one contiguous native read the way a plain NTFS file read is - the OS page cache warms the
*store's* underlying files, not something shaped like the logical 10 MB file being requested, so
the create side's own "page cache still warm from the write" advantage on native Windows may
simply not carry over the same way here. Not investigated further at this pass.

Against native Windows 10 MB file read under the same Power-Saver overlay
(`2026-09-01-julius-file10mb-read-native.md`'s corrected 20.4 ops/s mean, the index-bug fix
applied - power profile "(not captured)" there, but the same session's other Power-Saver
measurements make it the most likely candidate; see that protocol's own caveat), this dfs-mount
result (15.2 ops/s) is roughly 1.3x slower - a much smaller gap than file creation's ~1.8x, and
well within the kind of difference a single 5-run measurement on a non-isolated machine can
produce, rather than a clearly substantial regression.
