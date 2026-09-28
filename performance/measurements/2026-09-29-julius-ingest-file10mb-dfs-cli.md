# Ingest of pre-existing 10 MB files - dfs-cli - julius/native Windows/local SSD

## Setup
- Date: 2026-09-29
- Machine: julius
- Execution environment: native Windows (Windows 10 IoT Enterprise LTSC, build 19044)
- Power profile: Balanced base scheme, Power Saver overlay (`powercfg /getactivescheme` →
  `381b4222-f694-41f0-9685-ff5bb260df2e`; overlay GUID `961cc777-2547-4f9d-8174-7d86181b8a7a` →
  `powercfg /query` → `GUID-Alias: OVERLAY_SCHEME_MIN`, i.e. "Best power efficiency"/Power Saver)
- IO device: local SSD (julius's internal WDC WDS100T2B0A-00SM50, SATA - see `../machines.md`);
  source files and repository both under `C:\dedupfs-perf`, the same internal SSD as the other
  `-native` measurements in this directory.
- DedupFS build: `8ac866e224ee03d8a5300f333dba6b3901a09689` on `rust`
- Isolation: none deliberate - an ordinary interactive development machine, with a Claude Code
  Desktop session (which produced this measurement) present throughout. No other applications or
  background services were closed or checked beforehand.

## Workload
- Operation: Ingest (import pre-existing files into a repository, not a mount)
- Location: dfs-cli
- Tool: `dfs ingest --repository <repo> <source-dir> /` (`../scripts/ingest-file10mb.ps1`).
  Source files generated beforehand (not timed) with the same once-filled-template-plus-poke
  content scheme as `file10mb-create.ps1`'s native measurement - unique content per file.
- Mode: sequential (one `dfs ingest` invocation; DESIGN-INGEST-001's own worker pool does chunk
  work cross-file in parallel internally, not exposed as a knob here)
- Window: n/a - this is a single timed batch job, not a windowed request/response loop (see Notes
  for why the usual 5-runs-of-~20-seconds shape does not apply)
- Scale: 30 pre-existing files, 10 MB each (300 MB total)
- Content: 10 MB per file, unique (verified via the repository's own post-run dedup ratio, see
  Notes)

## Results
| Run | Result |
|---|---|
| 1 (only run) | 30 files, 314,572,800 bytes, 1.38s, 21.7 files/s, 217.1 MB/s |

N=1 - see Notes.

## Notes
Ingest is a one-shot batch job over a fixed set of already-existing files, not a request/response
operation that naturally repeats in a fixed time window the way every other measurement in this
directory does - re-running the same `dfs ingest` call again would hit REQ-INGEST-003's own
reference-acceleration path (or trivially dedupe) rather than repeating the same work, and
generating five independent 300 MB batches to get 5 genuinely comparable runs was judged out of
scope for this first pass (per the developer's own "not everything yet, just enough for a first
impression" framing for this round of measurements). A single timed run is recorded instead;
revisit with a proper multi-run protocol (fresh unique content per run, or a much larger single
batch broken into windowed sub-measurements) if ingest throughput becomes a specific question worth
its own targeted comparison.

`dfs stats --repository` against the resulting repository afterward confirmed exactly 30 files,
314,572,800 bytes logical and physical, dedup ratio 1.00x - matching Scale above and confirming
every file's content was genuinely unique. The single `dir(s)` entry ingest reports is the source
directory's own name recreated under `/` (`dfs ingest <source-dir> /` imports the directory itself,
not just its contents) - expected `dfs ingest` behavior, not a bug.

217.1 MB/s is higher than both `2026-09-29-julius-file10mb-create-dfs-mount.md`'s ~144.6 MB/s
(mount, single PowerShell-driven writer) and, within the same session's own noise, not far below
native Windows's own 10 MB creation baseline (~264 MB/s, `2026-08-28-julius-file10mb-create-
native.md`'s 26.4 ops/s under the same Power-Saver overlay) - plausibly DESIGN-INGEST-001's
cross-file worker-pool parallelism (several files' chunking/hashing/storage overlapping across
threads) closing most of the gap a single-threaded mount writer shows against native. Only one
data point at a small scale (300 MB, well within RAM/page-cache for this machine's own working
set) - not yet informative about how this holds up at the multi-terabyte scale a real migration
would actually run at, where source reads themselves become the dominant, page-cache-cold cost;
see the `rust-migration-cdc-bitwidth-compare` branch's own throwaway tool for that separate,
still-pending investigation.
