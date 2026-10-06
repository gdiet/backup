# Ingest of pre-existing directories - dfs-cli - julius/native Windows/local SSD

## Setup
- Date: 2026-10-06
- Machine: julius
- Execution environment: native Windows (Windows 10 IoT Enterprise LTSC, build 19044)
- Power profile: Balanced base scheme, Power Saver overlay (`ActiveOverlayAcPowerScheme` `961cc777-2547-4f9d-8174-7d86181b8a7a` -> `powercfg /query` -> `GUID-Alias: OVERLAY_SCHEME_MIN`, i.e. "Best power efficiency"/"Längste Akkulaufzeit"). On AC power (`Win32_Battery` `BatteryStatus` 2).
- IO device: source and repository both on julius's internal SSD (WDC WDS100T2B0A-00SM50, SATA - see `../machines.md`)
- DedupFS build: `d38d156ce1fbb09765943894b797d89c672b598d` on `rust` (release build)
- Isolation: none deliberate - an ordinary interactive development machine, with a Claude Code Desktop session (which produced this measurement) present throughout. During the SSD part of the series, that session listed files in a large measurement folder (a recursive count over ~6,400 files) and ran a git commit and push, so some SSD runs overlapped with that light disk and CPU activity (which runs is not known). The USB part ran with nothing from that session in parallel.

## Workload
- Operation: Ingest (import pre-existing directories into a repository, not a mount)
- Location: dfs-cli
- Tool: `dfs ingest --repository <repo> <source-dir> /` via `../scripts/ingest.ps1 -Workload dir -Count 20000`. The repository is created with `dfs create-repo` at the default 20-bit target size before the clock starts.
- Mode: sequential (one `dfs ingest` invocation per run; the ingest worker pool of DESIGN-INGEST-001 works across files internally and is not exposed as a knob here)
- Window: none fixed - each run is one `dfs ingest` call over the whole source tree and takes 11-12 s
- Scale: 20000 directories per run, ingested into a fresh repository each run (5 runs; the previous run's repository is deleted before the next one starts, untimed). The source tree is generated once, before the first run.
- Content: n/a (directories, not files)

## Results
| Run | Result |
|---|---|
| 1 (discarded?) | 20,000 dirs, 10.86 s, 1,841.6 ops/s |
| 2 | 20,000 dirs, 11.48 s, 1,742.2 ops/s |
| 3 | 20,000 dirs, 11.32 s, 1,766.8 ops/s |
| 4 | 20,000 dirs, 11.50 s, 1,739.1 ops/s |
| 5 | 20,000 dirs, 11.94 s, 1,675.0 ops/s |

Mean: 1,752.9 ops/s Range: 1,675.0 - 1,841.6 ops/s (N=5)

## Notes
Run 1 is within the discard threshold, kept. The five runs are close (1,675-1,842 ops/s) with a mild downward drift from run 1 to run 5 that is within the noise of an unisolated machine.

Compared with native directory creation on the same SSD and power profile (929.1 ops/s, `2026-08-27-julius-dir-create-native-powersaver.md`), ingest creates directories about 1.9x faster. Native creation makes one NTFS call per directory. Ingest records directories as rows in one SQLite database, which may be cheaper per directory than a filesystem call. This was not investigated. The comparison is only indicative, because ingest also reads the source tree and the native runs used a 20 s window against a growing tree.