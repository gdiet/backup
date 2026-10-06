# Ingest of pre-existing directories - dfs-cli - julius/native Windows/USB2 stick (repository)

## Setup
- Date: 2026-10-06
- Machine: julius
- Execution environment: native Windows (Windows 10 IoT Enterprise LTSC, build 19044)
- Power profile: Balanced base scheme, Power Saver overlay (`ActiveOverlayAcPowerScheme` `961cc777-2547-4f9d-8174-7d86181b8a7a` -> `powercfg /query` -> `GUID-Alias: OVERLAY_SCHEME_MIN`, i.e. "Best power efficiency"/"Längste Akkulaufzeit"). On AC power (`Win32_Battery` `BatteryStatus` 2).
- IO device: source on julius's internal SSD (WDC WDS100T2B0A-00SM50, SATA); repository on the external USB2 stick, drive `I:`, NTFS, labeled "USB Stick", ~4 GB (USB2-class write speed of ~8.7-10.5 MB/s, see `../machines.md`). Free space before the series: 3.70 GB
- DedupFS build: `d38d156ce1fbb09765943894b797d89c672b598d` on `rust` (release build)
- Isolation: none deliberate - an ordinary interactive development machine, with a Claude Code Desktop session (which produced this measurement) present throughout. During the SSD part of the series, that session listed files in a large measurement folder (a recursive count over ~6,400 files) and ran a git commit and push, so some SSD runs overlapped with that light disk and CPU activity (which runs is not known). The USB part ran with nothing from that session in parallel.

## Workload
- Operation: Ingest (import pre-existing directories into a repository, not a mount)
- Location: dfs-cli
- Tool: `dfs ingest --repository <repo> <source-dir> /` via `../scripts/ingest.ps1 -Workload dir -Count 2000`. The repository is created with `dfs create-repo` at the default 20-bit target size before the clock starts.
- Mode: sequential (one `dfs ingest` invocation per run; the ingest worker pool of DESIGN-INGEST-001 works across files internally and is not exposed as a knob here)
- Window: none fixed - each run is one `dfs ingest` call over the whole source tree and takes 10-20 s
- Scale: 2000 directories per run, ingested into a fresh repository each run (5 runs; the previous run's repository is deleted before the next one starts, untimed). The source tree is generated once, before the first run.
- Content: n/a (directories, not files)

## Results
| Run | Result |
|---|---|
| 1 (discarded?) | 2,000 dirs, 11.44 s, 174.8 ops/s |
| 2 | 2,000 dirs, 9.99 s, 200.2 ops/s |
| 3 | 2,000 dirs, 18.32 s, 109.2 ops/s |
| 4 | 2,000 dirs, 11.00 s, 181.8 ops/s |
| 5 | 2,000 dirs, 20.07 s, 99.7 ops/s |

Mean: 153.1 ops/s Range: 99.7 - 200.2 ops/s (N=5)

## Notes
Run 1 is within the discard threshold, kept. The spread is wide (99.6-200.2 ops/s): runs 3 and 5 are about half as fast as runs 2 and 4, with no monotonic trend. The cause is not known. Each run starts right after the untimed deletion of the previous run's repository on the same stick, which may play a role. This was not checked.

Compared with native directory creation on the same stick and power profile (158.3 ops/s, `2026-08-28-julius-dir-create-usb.md`), ingest is about equal on average (153.1 ops/s) but much less stable.