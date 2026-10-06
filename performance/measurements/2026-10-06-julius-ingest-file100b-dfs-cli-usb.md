# Ingest of pre-existing files of 100 B - dfs-cli - julius/native Windows/USB2 stick (repository)

## Setup
- Date: 2026-10-06
- Machine: julius
- Execution environment: native Windows (Windows 10 IoT Enterprise LTSC, build 19044)
- Power profile: Balanced base scheme, Power Saver overlay (`ActiveOverlayAcPowerScheme` `961cc777-2547-4f9d-8174-7d86181b8a7a` -> `powercfg /query` -> `GUID-Alias: OVERLAY_SCHEME_MIN`, i.e. "Best power efficiency"/"Längste Akkulaufzeit"). On AC power (`Win32_Battery` `BatteryStatus` 2).
- IO device: source on julius's internal SSD (WDC WDS100T2B0A-00SM50, SATA); repository on the external USB2 stick, drive `I:`, NTFS, labeled "USB Stick", ~4 GB (USB2-class write speed of ~8.7-10.5 MB/s, see `../machines.md`). Free space before the series: 3.70 GB
- DedupFS build: `d38d156ce1fbb09765943894b797d89c672b598d` on `rust` (release build)
- Isolation: none deliberate - an ordinary interactive development machine, with a Claude Code Desktop session (which produced this measurement) present throughout. During the SSD part of the series, that session listed files in a large measurement folder (a recursive count over ~6,400 files) and ran a git commit and push, so some SSD runs overlapped with that light disk and CPU activity (which runs is not known). The USB part ran with nothing from that session in parallel.

## Workload
- Operation: Ingest (import pre-existing files of 100 B into a repository, not a mount)
- Location: dfs-cli
- Tool: `dfs ingest --repository <repo> <source-dir> /` via `../scripts/ingest.ps1 -Workload file100b -Count 400`. The repository is created with `dfs create-repo` at the default 20-bit target size before the clock starts.
- Mode: sequential (one `dfs ingest` invocation per run; the ingest worker pool of DESIGN-INGEST-001 works across files internally and is not exposed as a knob here)
- Window: none fixed - each run is one `dfs ingest` call over the whole source tree and takes 8-11 s
- Scale: 400 files of 100 B per run, ingested into a fresh repository each run (5 runs; the previous run's repository is deleted before the next one starts, untimed). The source tree is generated once, before the first run.
- Content: 100 B per file, unique per file (random template with fresh random bytes poked in every 64 KiB and in the last 8 bytes; generator outside the timed section)

## Results
| Run | Result |
|---|---|
| 1 (discarded?) | 400 files, 8.26 s, 48.4 ops/s |
| 2 | 400 files, 7.96 s, 50.3 ops/s |
| 3 | 400 files, 8.19 s, 48.8 ops/s |
| 4 | 400 files, 10.77 s, 37.1 ops/s |
| 5 | 400 files, 9.80 s, 40.8 ops/s |

Mean: 45.1 ops/s Range: 37.1 - 50.3 ops/s (N=5)

## Notes
Run 1 is within the discard threshold, kept. Runs 1-3 are tight (48-50 ops/s). Runs 4 and 5 are slower (37-41 ops/s) with no explanation.

Compared with native creation of 100 B files on the same stick and power profile (100.9 ops/s, `2026-08-28-julius-file100b-create-usb.md`), ingest is about 2.2x slower. This is the opposite of the SSD result, where ingest is faster than native. Ingest writes the data file and the SQLite database (with its write-ahead log) to the stick. The cause was investigated afterwards, see `../notes/2026-10-06-julius-ingest-on-usb2-stick.md`. The metadata dominates: ingest commits at least three small database transactions per file, and each costs roughly 7-10 ms on this stick. With the metadata on the SSD and only the data on the stick, 400 files take 2.4-3.0 s instead of 8-10 s.