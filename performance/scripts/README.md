# Measurement Scripts

Runnable implementations of specific `../methodology.md` workload-catalog entries, each producing
the 5-runs-of-~20-seconds output the "Statistical approach" section asks for.

`location: native` - one pair of scripts (`.ps1` for native Windows, `.sh` for WSL2/Linux) per
workload:

- `dir-create.{ps1,sh}` - directory creation, native filesystem.
- `dir-lookup.{ps1,sh}` - directory lookup against a fixed 100,000-entry tree, built once and
  reused across runs/invocations (unlike the create-workload scripts, this tree does not grow).
- `dir-listing.{ps1,sh}` - full listing of a fixed 50,000-entry directory, built once and reused
  the same way.
- `file0b-create.{ps1,sh}` - zero-byte file creation, native filesystem, spread across 20
  subdirectories.
- `file100b-create.{ps1,sh}`, `file30kb-create.{ps1,sh}`, `file10mb-create.{ps1,sh}` - file creation
  at 100 B/30 KB/10 MB, native filesystem, spread across 20 subdirectories, content unique per file.
  10 MB spans several chunks at the default chunking configuration; the smaller two do not. Content
  is generated per `../methodology.md`'s "File-content workloads" note: the `.ps1` scripts write a
  once-filled random template with fresh random bytes poked in at intervals (generator out of the
  timed loop); the `.sh` scripts call `crates/perf-gen`'s binary once per file, building it
  themselves on each run (`cargo build --release -p perf-gen`), same as `dfs-mount-dir-create.sh`
  builds `dfs` - no separate build step needed. The two families use different mechanisms rather
  than a single shared one - see `../methodology.md`'s "File-content workloads" for why a naive
  `perf-gen`-per-file call works fine on Linux but not on Windows (`CreateProcess` overhead).
- `file100b-read.{ps1,sh}`, `file30kb-read.{ps1,sh}`, `file10mb-read.{ps1,sh}` - reads back whatever
  the matching `-create` script has produced so far; run the `-create` script at least once first.

Every `.ps1` script above has had scaled-down trial validation on `3327`'s native Windows
(PowerShell 5.1) - a smaller Scale and a shorter window, not a full 5-runs-of-20-seconds
measurement. The three `file*-read.ps1` scripts needed one fix (discarding the read `byte[]` with
`$null = ` rather than `| Out-Null`, which enumerates it element by element and so measures
pipeline overhead instead of read cost); the `file*-create.ps1` scripts were re-trialled after
adopting the template-and-poke content scheme, with per-file size and content-distinctness
confirmed; the rest ran clean.

On the `.sh` side, `dir-lookup.sh`, `dir-listing.sh`, and every `file*-{create,read}.sh` script
were each validated on `3327`'s WSL2 the same way (no bugs found). `file100b-create.sh`,
`file30kb-create.sh`, and `file10mb-create.sh` were re-trialled again after switching from
`/dev/urandom` to `perf-gen` (see the file-content bullet above): exact per-file size and content
distinctness both held, and per-file throughput improved noticeably at 30 KB and 10 MB (roughly
2x, in this session's own scaled-down trials) - see `../methodology.md`'s "File-content workloads"
for the trial numbers and the reasoning behind keeping the `.ps1` side on the older
template-and-poke scheme instead. The earlier `/dev/urandom`-throughput finding that originally
motivated the switch is recorded in
`agent-todos/done/wsl2-trial-renamed-file-content-sh-scripts.md`, now superseded.

A full 5-runs-of-20-seconds run on native Windows has not been done for any script yet; treat the
first one as a final check, same as `dfs-mount-dir-create.ps1` below.

`location: dfs-mount`:

- `dfs-mount-dir-create.ps1` - builds `dfs`, creates a repository, mounts it read-write, and times
  directory creation against the mounted path. Windows only, via WinFSP. First real run
  (2026-09-29, `julius`) found and fixed a real bug in its own readiness probe - `New-Item
  -ItemType Directory` silently creates a missing *parent* directory too, so a probe that ran
  before the mountpoint itself existed could create the mountpoint as a plain native directory and
  "succeed" against that instead of the real mount, with no error anywhere. Fixed by waiting for
  the mountpoint to exist first; see the script's own header comment.
- `dfs-mount-dir-create.sh` - the same workload via real libfuse3, for WSL2/native Linux. `3327`'s
  WSL2 has confirmed `/dev/fuse` access and validated this script directly (one real bug found and
  fixed on the first run - see the script's own header); `julius`'s WSL2 has not been checked yet.
  Bash `mkdir` (unlike PowerShell's `New-Item`) does not create missing parents on its own, so this
  script was never at risk of the `.ps1` side's readiness-probe bug above.
- `dfs-mount-file10mb-create.ps1` / `dfs-mount-file10mb-read.ps1` - 10 MB file creation and
  read-back against a mounted repository, Windows only via WinFSP. Same content scheme and
  20-subdirectory spread as `file10mb-create.ps1`/`file10mb-read.ps1`'s native versions, so the
  numbers are directly comparable. The create script shares (and received the same fix for)
  `dfs-mount-dir-create.ps1`'s readiness-probe bug above; the read script's own readiness check
  (wait for a non-empty directory listing) never calls `New-Item` and was never at risk of it.

`location: dfs-cli`:

- `ingest-file10mb.ps1` - generates 30 pre-existing, uniquely-content 10 MB files natively (same
  content scheme as `file10mb-create.ps1`, generation itself not timed), then times a single `dfs
  ingest` call importing them into a fresh repository. Ingest is a one-shot batch job, not a
  request/response loop, so this records one timed run rather than the usual 5-runs-of-~20-seconds
  shape - see the measurement protocol's Notes for why.
- `ingest.ps1` - `-Workload dir|file100b|file10mb -Count <n> -SourceRoot <dir> -RepoBase <dir>
  [-Runs 5]`. Generates the source tree once (not timed), then ingests it `-Runs` times, each into a
  fresh repository below `-RepoBase` that is created before and deleted after the timed `dfs ingest`
  call. Source and repository can sit on different devices, for example the source on the local SSD
  and the repository on a USB stick. A fresh repository per run replaces the "state accumulates
  between runs" rule, because a second ingest into the same repository would only deduplicate
  against the first. Both ingest scripts keep their scratch data below the temp folder or the
  folders given by the parameters.

`location: db-direct` - there is no script here - it is a Rust benchmark instead, since
`db-direct` means calling `db::Repository`'s methods directly, not shelling out to anything. See
`../../crates/db/examples/db_bench.rs` (directory creation only so far, since `db::Repository` has
no file-entry creation reachable without going through `crates/store` and the settle pipeline
`dfs-mount`/`dfs-cli` both use); run it with `cargo run --release -p db --example db_bench`. It
prints the same run/count/elapsed/ops-per-second shape as the scripts here.

## Running one

1. If this is the start of a *new* measurement (not a repeat of an existing one), delete the
   script's working directory first (`C:\dedupfs-perf\...` / `~/dedupfs-perf\...`, see the script
   header) so the run count/Scale starts clean.
2. Run the script. It prints one line per run: operation count, elapsed seconds, ops/s.
3. Alongside the script output, separately capture what `../methodology.md`'s Environment fields
   ask for: `Machine` (see `../machines.md`), `Execution environment`, `Power profile` (on Windows,
   both `powercfg /getactivescheme`'s base scheme *and* the separate power-mode overlay - see
   `../methodology.md`'s "Power profile" note, `powercfg` alone does not show the overlay), `IO
   device`, `Isolation`. `DedupFS build` is not applicable for the `native` scripts (no DedupFS
   code is exercised) - for any `dfs-mount`/`dfs-cli` script, record the git commit (and branch)
   the built `dfs.exe` came from.
4. Turn the output into a `../measurements/<date>-<machine>-<slug>.md` + `.yaml` pair, following
   `../methodology.md`'s recording template and sidecar schema. `Tool` is named in each script's
   header comment.

The loop overhead of the invoking shell/cmdlet (PowerShell `New-Item`, bash `mkdir`/`touch` as a
separate process per call) is included in the numbers on purpose rather than optimized away - see
`../methodology.md`'s "Purpose" section. Record the exact tool used (already in each script's
header) so later comparisons do not mistake shell overhead for filesystem cost.

The `.sh` scripts default to `~/dedupfs-perf/...` deliberately, not `/mnt/c/...` - see
`../methodology.md`'s "Execution environment" note on why a "WSL2" measurement needs to stay on
WSL2's native filesystem rather than crossing into the Windows host filesystem through the
DrvFs/9p bridge. Leave that as-is when running these scripts under WSL2.
