# Directory creation - db-direct - julius/native Windows/local SSD

## Setup
- Date: 2026-09-01
- Machine: julius
- Execution environment: native Windows (Windows 10 IoT Enterprise LTSC, build 19044)
- Power profile: Balanced base scheme, Power Saver overlay (`powercfg /getactivescheme` →
  `381b4222-f694-41f0-9685-ff5bb260df2e`; overlay GUID `961cc777-2547-4f9d-8174-7d86181b8a7a` →
  `powercfg /query` → `GUID-Alias: OVERLAY_SCHEME_MIN`, i.e. "Best power efficiency"/Power Saver)
- IO device: local SSD (julius's internal WDC WDS100T2B0A-00SM50, SATA - see `../machines.md`);
  the benchmark's default path resolves under `%TEMP%`, which is `C:\Users\Conny\AppData\Local\Temp`
  on this machine, i.e. the same internal SSD as the other `-native` measurements in this directory.
- DedupFS build: `fd660860f43faa95f83ef68af7cda2a66b7f5beb` on `mount-read-write`
- Isolation: none deliberate - an ordinary interactive development machine, with a Claude Code
  Desktop session (which produced this measurement) present throughout. No other applications or
  background services (indexing, antivirus real-time scan, etc.) were closed or checked
  beforehand.

## Workload
- Operation: Directory creation
- Location: db-direct
- Tool: `db::Repository::mkdir` (`../../crates/db/examples/db_bench.rs`, `cargo run --release -p
  db --example db_bench`)
- Mode: sequential
- Window: 20 s
- Scale: 19,602 directories total across the 5 runs (2,170-8,561 per run, time-boxed not
  count-fixed); one growing tree of sibling directories directly under the repository root, in a
  fresh repository created by the benchmark itself and deleted again when it exits.
- Content: n/a (directories, not files)

## Results
| Run | Result |
|---|---|
| 1 (discarded?) | 8,561 dirs, 20.00 s, 428.0 ops/s |
| 2 | 3,332 dirs, 20.02 s, 166.4 ops/s |
| 3 | 3,051 dirs, 20.00 s, 152.5 ops/s |
| 4 | 2,488 dirs, 20.00 s, 124.4 ops/s |
| 5 | 2,170 dirs, 20.01 s, 108.5 ops/s |

Mean: 196.0 ops/s Range: 108.5 - 428.0 ops/s (N=5)

## Notes
Run 1 is *faster*, not slower, than the median of runs 2-5 (~138.5 ops/s) - the opposite direction
from the warmup effect `../methodology.md`'s discard rule guards against, so that rule does not
apply here and run 1 is kept as-is (there is no rule for discarding an anomalously *fast* first
run, and inventing one after the fact would be exactly the kind of subjective judgment call the
existing rule is meant to avoid).

What is actually happening is a real, monotonic scale-dependent slowdown, not run-to-run noise:
throughput falls in every single run, 428.0 -> 166.4 -> 152.5 -> 124.4 -> 108.5 ops/s, a ~75%
drop from run 1 to run 5 and a ~35% drop even just from run 2 to run 5. Per `../methodology.md`'s
"state between runs" rule, this is a real finding about how `mkdir` scales with the number of
existing entries in the same directory, not something to normalize away by resetting the
repository between runs - all 19,602 directories are siblings under one root, so this is squarely
the "many entries in one directory" case, not an artifact of growing the whole tree.

The repository's `tree_entries` table (see `../../crates/db/src/lib.rs`) has no dedicated index
on `(parent_id)` beyond what SQLite's default primary-key/unique-constraint machinery provides
for lookup - `mkdir` needs to check for a same-name sibling before inserting, and if that check
(or the insert's own index maintenance) degrades from an effectively flat cost to something
closer to O(log n) or worse per call as sibling count grows into the thousands, a monotonic
per-run slowdown across a single ever-growing sibling directory is exactly the shape that would
produce. This has not been root-caused further (e.g. by comparing `EXPLAIN QUERY PLAN` for the
sibling-existence check, or re-running with each run's directories spread across many parent
directories instead of one) - flagging it as the most likely explanation given the data, not a
confirmed diagnosis. Worth a closer look before `mkdir` is exercised through `dfs-mount`/`dfs-cli`
at a scale where this would matter (see the sibling-count regime a real backup tree could reach).

This run used the Power Saver overlay (see Setup) - the same overlay as
`2026-08-27-julius-dir-create-native-powersaver.md`, the native-filesystem `mkdir` baseline most
directly comparable on that axis (929.1 ops/s mean), rather than that same day's non-"-powersaver"
companion (821.9 ops/s mean, developer-reported Best Performance overlay, not independently
captured). Against the Power-Saver-overlay baseline, this `db-direct` result's mean (196.0 ops/s)
is roughly 4.7x slower than the bare filesystem, and even its fastest single run (428.0 ops/s)
does not reach it - a substantial gap, though this is only one `db-direct` data point so far and
the two protocols' trees differ in shape (one growing sibling directory here versus a fresh tree
there), so the comparison is suggestive, not a controlled A/B.
