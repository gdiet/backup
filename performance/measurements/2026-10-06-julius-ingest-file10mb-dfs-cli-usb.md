# Ingest of pre-existing files of 10 MB - dfs-cli - julius/native Windows/USB2 stick (repository)

## Setup
- Date: 2026-10-06
- Machine: julius
- Execution environment: native Windows (Windows 10 IoT Enterprise LTSC, build 19044)
- Power profile: Balanced base scheme, Power Saver overlay (`ActiveOverlayAcPowerScheme` `961cc777-2547-4f9d-8174-7d86181b8a7a` -> `powercfg /query` -> `GUID-Alias: OVERLAY_SCHEME_MIN`, i.e. "Best power efficiency"/"Längste Akkulaufzeit"). On AC power (`Win32_Battery` `BatteryStatus` 2).
- IO device: source on julius's internal SSD (WDC WDS100T2B0A-00SM50, SATA); repository on the external USB2 stick, drive `I:`, NTFS, labeled "USB Stick", ~4 GB (USB2-class write speed of ~8.7-10.5 MB/s, see `../machines.md`). Free space before the series: 3.70 GB
- DedupFS build: `d38d156ce1fbb09765943894b797d89c672b598d` on `rust` (release build)
- Isolation: none deliberate - an ordinary interactive development machine, with a Claude Code Desktop session (which produced this measurement) present throughout. During the SSD part of the series, that session listed files in a large measurement folder (a recursive count over ~6,400 files) and ran a git commit and push, so some SSD runs overlapped with that light disk and CPU activity (which runs is not known). The USB part ran with nothing from that session in parallel.

## Workload
- Operation: Ingest (import pre-existing files of 10 MB into a repository, not a mount)
- Location: dfs-cli
- Tool: `dfs ingest --repository <repo> <source-dir> /` via `../scripts/ingest.ps1 -Workload file10mb -Count 15`. The repository is created with `dfs create-repo` at the default 20-bit target size before the clock starts.
- Mode: sequential (one `dfs ingest` invocation per run; the ingest worker pool of DESIGN-INGEST-001 works across files internally and is not exposed as a knob here)
- Window: none fixed - each run is one `dfs ingest` call over the whole source tree and takes 37-45 s
- Scale: 15 files of 10 MB per run, ingested into a fresh repository each run (5 runs; the previous run's repository is deleted before the next one starts, untimed). The source tree is generated once, before the first run.
- Content: 10485760 B per file, unique per file (random template with fresh random bytes poked in every 64 KiB and in the last 8 bytes; generator outside the timed section)

## Results
| Run | Result |
|---|---|
| 1 (discarded?) | 15 files, 38.10 s, 0.39 ops/s, 3.9 MB/s |
| 2 | 15 files, 45.34 s, 0.33 ops/s, 3.3 MB/s |
| 3 | 15 files, 36.93 s, 0.41 ops/s, 4.1 MB/s |
| 4 | 15 files, 42.91 s, 0.35 ops/s, 3.5 MB/s |
| 5 | 15 files, 43.06 s, 0.35 ops/s, 3.5 MB/s |

Mean: 0.37 ops/s Range: 0.33 - 0.41 ops/s (N=5)
Mean: 3.7 MB/s Range: 3.3 - 4.1 MB/s

## Notes
Run 1 is within the discard threshold, kept. The runs are consistent (3.3-4.1 MB/s) with no trend. Each run takes 37-45 s, longer than the usual 20 s window. A 20 s window would have held only about 7 files.

Compared with native creation of 10 MB files on the same stick and power profile (about 10.5 MB/s, `2026-08-28-julius-file10mb-create-usb.md`), ingest reaches about 35% of that throughput, so it is about 2.9x slower. Native writing already runs at the stick's raw speed, and ingest reaches only a third of it. The cause was investigated afterwards, see `../notes/2026-10-06-julius-ingest-on-usb2-stick.md`. The stick writes the 1.2 MB chunks of ingest at about 5.7 MB/s, while the 10 MB native writes reach about 9.3 MB/s. In addition, the SQLite metadata on the same stick stalls the device when its small writes interleave with the large ones. The number of workers and SQLite's `synchronous` setting have no measurable effect.