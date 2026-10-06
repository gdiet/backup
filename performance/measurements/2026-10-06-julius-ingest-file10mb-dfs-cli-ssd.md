# Ingest of pre-existing files of 10 MB - dfs-cli - julius/native Windows/local SSD

## Setup
- Date: 2026-10-06
- Machine: julius
- Execution environment: native Windows (Windows 10 IoT Enterprise LTSC, build 19044)
- Power profile: Balanced base scheme, Power Saver overlay (`ActiveOverlayAcPowerScheme` `961cc777-2547-4f9d-8174-7d86181b8a7a` -> `powercfg /query` -> `GUID-Alias: OVERLAY_SCHEME_MIN`, i.e. "Best power efficiency"/"Längste Akkulaufzeit"). On AC power (`Win32_Battery` `BatteryStatus` 2).
- IO device: source and repository both on julius's internal SSD (WDC WDS100T2B0A-00SM50, SATA - see `../machines.md`)
- DedupFS build: `d38d156ce1fbb09765943894b797d89c672b598d` on `rust` (release build)
- Isolation: none deliberate - an ordinary interactive development machine, with a Claude Code Desktop session (which produced this measurement) present throughout. During the SSD part of the series, that session listed files in a large measurement folder (a recursive count over ~6,400 files) and ran a git commit and push, so some SSD runs overlapped with that light disk and CPU activity (which runs is not known). The USB part ran with nothing from that session in parallel.

## Workload
- Operation: Ingest (import pre-existing files of 10 MB into a repository, not a mount)
- Location: dfs-cli
- Tool: `dfs ingest --repository <repo> <source-dir> /` via `../scripts/ingest.ps1 -Workload file10mb -Count 200`. The repository is created with `dfs create-repo` at the default 20-bit target size before the clock starts.
- Mode: sequential (one `dfs ingest` invocation per run; the ingest worker pool of DESIGN-INGEST-001 works across files internally and is not exposed as a knob here)
- Window: none fixed - each run is one `dfs ingest` call over the whole source tree and takes 12-17 s
- Scale: 200 files of 10 MB per run, ingested into a fresh repository each run (5 runs; the previous run's repository is deleted before the next one starts, untimed). The source tree is generated once, before the first run.
- Content: 10485760 B per file, unique per file (random template with fresh random bytes poked in every 64 KiB and in the last 8 bytes; generator outside the timed section)

## Results
| Run | Result |
|---|---|
| 1 (discarded?) | 200 files, 12.00 s, 16.7 ops/s, 166.7 MB/s |
| 2 | 200 files, 16.51 s, 12.1 ops/s, 121.1 MB/s |
| 3 | 200 files, 14.07 s, 14.2 ops/s, 142.1 MB/s |
| 4 | 200 files, 15.19 s, 13.2 ops/s, 131.7 MB/s |
| 5 | 200 files, 14.10 s, 14.2 ops/s, 141.8 MB/s |

Mean: 14.1 ops/s Range: 12.1 - 16.7 ops/s (N=5)
Mean: 140.7 MB/s Range: 121.1 - 166.7 MB/s

## Notes
Run 1 is within the discard threshold, kept. Run 1 is the fastest (166.6 MB/s) and run 2 the slowest (121.1 MB/s). The activity listed under Isolation overlapped with some SSD runs. Whether it slowed run 2 is not known.

Compared with native creation of 10 MB files on the same SSD and power profile (about 264 MB/s, `2026-08-28-julius-file10mb-create-native.md`), ingest reaches about 53% of that throughput. Ingest does more work per byte than a native write: it reads the source file, finds chunk boundaries, hashes every chunk, and writes metadata. On this two-core CPU under the Power Saver overlay, chunking and hashing are plausible limits. This was not profiled.

The earlier 30-file ingest measurement (`2026-09-29-julius-ingest-file10mb-dfs-cli.md`, 217.1 MB/s) used 300 MB, which fits in memory. This series uses 2 GB per run. A repeat of the earlier measurement is recorded in that file's addendum.