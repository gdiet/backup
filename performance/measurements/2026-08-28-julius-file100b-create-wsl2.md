# 100 B file creation - native - julius/WSL2 Debian 12/local SSD

## Setup
- Date: 2026-08-28
- Machine: julius
- Execution environment: WSL2, Debian 12 (bookworm), kernel 6.18.33.2-microsoft-standard-WSL2
  (WSL 2.7.12.0). Working directory `~/dedupfs-perf/files100b` - WSL2's own native filesystem, not
  `/mnt/c/...`. Rust toolchain (stable 1.98.0) installed via `rustup` this session, with the
  developer's explicit confirmation, specifically to build `crates/perf-gen` for this script.
- Power profile: Balanced base scheme, "Best power efficiency"/Power Saver overlay ("Längste
  Akkulaufzeit") on the Windows host - confirmed via `powercfg /query <guid>` →
  `GUID-Alias: OVERLAY_SCHEME_MIN`; WSL2 has no separate power-mode setting of its own. On AC power
  (developer-confirmed).
- IO device: local SSD (julius's internal WDC WDS100T2B0A-00SM50, SATA, reached via WSL2's own
  virtual disk - see `../machines.md`)
- DedupFS build: n/a - native filesystem baseline, no DedupFS code exercised
- Isolation: none deliberate - ordinary interactive development machine, this Claude Code Desktop
  session present throughout, nothing else closed or checked beforehand.

## Workload
- Operation: File creation at 100 B, unique content
- Location: native
- Tool: bash redirection, content from `perf-gen` (`../scripts/file100b-create.sh`,
  `cargo build --release -p perf-gen` run by the script itself)
- Mode: sequential
- Window: 20 s
- Scale: 10,766 files total across the 5 runs (2,079-2,198 per run, time-boxed not count-fixed),
  spread round-robin across 20 subdirectories under `~/dedupfs-perf/files100b`, first measurement
  against this tree.
- Content: 100 B, unique per file (`perf-gen`-generated, see `../methodology.md`'s "File-content
  workloads")

## Results
| Run | Result |
|---|---|
| 1 (discarded?) | 2,079 files, 20 s, 103 ops/s |
| 2 | 2,181 files, 20 s, 109 ops/s |
| 3 | 2,198 files, 20 s, 109 ops/s |
| 4 | 2,195 files, 20 s, 109 ops/s |
| 5 | 2,113 files, 20 s, 105 ops/s |

Mean: 107.0 ops/s Range: 103 - 109 ops/s (N=5)

## Notes
Run 1 is within the discard threshold, kept. Tight spread, no trend.

Roughly 44% of the equivalent native-Windows throughput, same machine, same day (107.0 vs.
242.6 ops/s mean - see `2026-08-28-julius-file100b-create-native.md`) - native ahead here, the same
direction as directory/zero-byte-file creation from the earlier 2026-08-27 measurements, unlike the
lookup/listing operations where WSL2 was dramatically *ahead* (see those protocols' Notes). Content
generation differs between the two sides (`perf-gen` binary per file here vs. an in-process
template-and-poke scheme on the `.ps1` side - see `../scripts/README.md`), a confound this
comparison does not control for.
