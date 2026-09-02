# Directory creation - db-direct, with DESIGN-MOUNT-017's name cache - julius/native Windows/local SSD

## Setup
- Date: 2026-09-02
- Machine: julius
- Execution environment: native Windows (Windows 10 IoT Enterprise LTSC, build 19044)
- Power profile: Balanced base scheme, Power Saver overlay (`powercfg /getactivescheme` →
  `381b4222-f694-41f0-9685-ff5bb260df2e`; overlay GUID `961cc777-2547-4f9d-8174-7d86181b8a7a` →
  `powercfg /query` → `GUID-Alias: OVERLAY_SCHEME_MIN`, i.e. "Best power efficiency"/Power Saver) -
  same overlay as the baseline measurement this A/Bs against.
- IO device: local SSD (julius's internal WDC WDS100T2B0A-00SM50, SATA - see `../machines.md`);
  the benchmark's default path resolves under `%TEMP%`, same as the baseline.
- DedupFS build: `826890dfeba9f2d932d17c5b511dd41abcedae2f` on `write-cache` (not yet merged;
  DESIGN-MOUNT-017 in `../../docs/design/tree-namespace-case-sensitivity.md`)
- Isolation: none deliberate - an ordinary interactive development machine, with a Claude Code
  Desktop session (which produced this measurement) present throughout. No other applications or
  background services (indexing, antivirus real-time scan, etc.) were closed or checked
  beforehand.

## Workload
- Operation: Directory creation
- Location: db-direct
- Tool: `db::Repository::mkdir` (`../../crates/db/examples/db_bench.rs`, `cargo run --release -p
  db --example db_bench`) - identical tool and invocation to the baseline, no code changes needed
  since `cfg!(windows)` already gates the cached fallback path on this platform.
- Mode: sequential
- Window: 20 s
- Scale: 363,421 directories total across the 5 runs (71,677-73,926 per run, time-boxed not
  count-fixed); one growing tree of sibling directories directly under the repository root, in a
  fresh repository created by the benchmark itself and deleted again when it exits - same shape as
  the baseline, ~19x more directories fit in the same 20 s windows.
- Content: n/a (directories, not files)

## Results
| Run | Result |
|---|---|
| 1 (discarded?) | 73,435 dirs, 20.00 s, 3671.7 ops/s |
| 2 | 71,677 dirs, 20.00 s, 3583.7 ops/s |
| 3 | 71,860 dirs, 20.00 s, 3593.0 ops/s |
| 4 | 72,523 dirs, 20.01 s, 3624.3 ops/s |
| 5 | 73,926 dirs, 20.00 s, 3696.3 ops/s |

Mean: 3633.8 ops/s Range: 3583.7 - 3696.3 ops/s (N=5)

## Notes
Run 1 (3671.7 ops/s) is at the *high* end of the range, not the low end - the discard rule (which
only discards an anomalously slow run 1) does not apply, and there is no warmup effect visible
here at all.

**Direct comparison against the baseline** (`2026-09-01-julius-db-direct-mkdir-native.md`, same
tool, same machine, same power profile, same IO device, no code differences other than being on
`write-cache`):

| | Baseline (no cache) | This run (with name cache) |
|---|---|---|
| Mean | 196.0 ops/s | 3633.8 ops/s |
| Range | 108.5 - 428.0 ops/s | 3583.7 - 3696.3 ops/s |
| Shape across 5 runs | monotonic decline, 428.0 -> 108.5 | flat, 3583.7-3696.3, no trend |
| Total siblings reached | 19,602 | 363,421 |

The baseline's defining feature - throughput falling as the parent directory's live child count
grows - is **gone** here: despite reaching a sibling count roughly 18.5x larger than the baseline
ever did (363,421 vs. 19,602), this run shows no decline at all, run-to-run variation stays inside
a ~3% band, and even run 5 (deepest into the largest tree by far) is not slower than run 2. This
confirms, on real Windows/WinFSP hardware for the first time, what DESIGN-MOUNT-017's own
"Verification" section had so far only shown via a `cfg!(windows)`-gate-bypassed simulation on
non-Windows hardware (there: pre-cache 588.3 -> 146.0 ops/s becoming a flat ~12,000-13,000 ops/s
post-cache). The qualitative finding (monotonic decline eliminated) reproduces exactly; the
absolute ops/s differs from that simulation in both directions (this real run's ~3,600-3,700 ops/s
sits well below the simulated ~12,000-13,000, while the baseline's real decline bottoms out lower
than the simulated pre-cache run's 146.0 too) - expected, since the simulation ran on different
hardware entirely, not evidence against the fix itself.

Absolute mean throughput improved ~18.5x over the baseline (196.0 -> 3633.8 ops/s) at this scale,
and the gap would only widen further into a directory large enough to have hit the baseline's
worst rungs (108.5 ops/s at ~19,600 siblings) - this run's own worst single value (3583.7 ops/s)
is already ~33x that.

This closes the last open question DESIGN-MOUNT-017 flagged before a keep/discard decision: the
cache's effect is real on the actual target platform, not an artifact of the non-Windows
simulation used to develop it.
