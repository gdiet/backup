# Ingest of pre-existing files of 100 B - dfs-cli - julius/native Windows/local SSD

## Setup
- Date: 2026-10-06
- Machine: julius
- Execution environment: native Windows (Windows 10 IoT Enterprise LTSC, build 19044)
- Power profile: Balanced base scheme, Power Saver overlay (`ActiveOverlayAcPowerScheme` `961cc777-2547-4f9d-8174-7d86181b8a7a` -> `powercfg /query` -> `GUID-Alias: OVERLAY_SCHEME_MIN`, i.e. "Best power efficiency"/"Längste Akkulaufzeit"). On AC power (`Win32_Battery` `BatteryStatus` 2).
- IO device: source and repository both on julius's internal SSD (WDC WDS100T2B0A-00SM50, SATA - see `../machines.md`)
- DedupFS build: `d38d156ce1fbb09765943894b797d89c672b598d` on `rust` (release build)
- Isolation: none deliberate - an ordinary interactive development machine, with a Claude Code Desktop session (which produced this measurement) present throughout. During the SSD part of the series, that session listed files in a large measurement folder (a recursive count over ~6,400 files) and ran a git commit and push, so some SSD runs overlapped with that light disk and CPU activity (which runs is not known). The USB part ran with nothing from that session in parallel.

## Workload
- Operation: Ingest (import pre-existing files of 100 B into a repository, not a mount)
- Location: dfs-cli
- Tool: `dfs ingest --repository <repo> <source-dir> /` via `../scripts/ingest.ps1 -Workload file100b -Count 6000`. The repository is created with `dfs create-repo` at the default 20-bit target size before the clock starts.
- Mode: sequential (one `dfs ingest` invocation per run; the ingest worker pool of DESIGN-INGEST-001 works across files internally and is not exposed as a knob here)
- Window: none fixed - each run is one `dfs ingest` call over the whole source tree and takes 8-11 s
- Scale: 6000 files of 100 B per run, ingested into a fresh repository each run (5 runs; the previous run's repository is deleted before the next one starts, untimed). The source tree is generated once, before the first run.
- Content: 100 B per file, unique per file (random template with fresh random bytes poked in every 64 KiB and in the last 8 bytes; generator outside the timed section)

## Results
| Run | Result |
|---|---|
| 1 (discarded?) | 6,000 files, 9.80 s, 612.2 ops/s |
| 2 | 6,000 files, 8.84 s, 678.7 ops/s |
| 3 | 6,000 files, 7.94 s, 755.7 ops/s |
| 4 | 6,000 files, 10.62 s, 565.0 ops/s |
| 5 | 6,000 files, 10.82 s, 554.5 ops/s |

Mean: 633.2 ops/s Range: 554.5 - 755.7 ops/s (N=5)

## Notes
Run 1 is within the discard threshold, kept. Runs 4 and 5 (565 and 555 ops/s) are about 25% slower than run 3 (756 ops/s). There is no trend before that. The activity listed under Isolation overlapped with some SSD runs. Whether it caused the slower runs is not known.

Compared with native creation of 100 B files on the same SSD and power profile (242.6 ops/s, `2026-08-28-julius-file100b-create-native.md`), ingest is about 2.6x faster. The likely reason is the same as for directories: native creation costs one file creation per file, while ingest records the files in one SQLite database. This was not investigated.