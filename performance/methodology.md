# Measurement Methodology

## Purpose

These measurements are a diagnostic check, not a precision benchmark: the goal is to see where
DedupFS already performs well and where an obvious problem or inconsistency shows up, not to
produce statistically rigorous numbers. The recording format and statistical approach below are
deliberately lightweight for that reason - resist the urge to extend a run, add more repetitions,
or otherwise chase precision beyond what it takes to tell "fine" from "worth investigating" apart.

## Recording a measurement

Each measurement protocol (`measurements/<date>-<machine>-<slug>.md`) records:

### Environment

- **Machine**: which physical/virtual machine the measurement ran on, by alias - see
  [`machines.md`](machines.md) for the alias-to-hardware mapping and how to add a new one.
- **Execution environment**: native Windows, WSL2, Docker inside WSL2, native Linux, or other -
  spelled out precisely (e.g. "WSL2, Ubuntu 24.04"), since the same machine can run more than one
  of these. Within WSL2, also be deliberate about the working directory: a path under
  `/mnt/c/...` crosses into the Windows host filesystem through the DrvFs/9p bridge, a different IO
  path from the Linux-native filesystem WSL2's own virtual disk provides (the same distinction the
  `wsl-windows-sync` skill draws for build/test performance, for a different reason here) - use a
  native path (e.g. under `~`) for a "WSL2" measurement unless the measurement is deliberately about
  that bridge itself, which is not in the location catalog today.
- **Power profile**: the OS power/performance profile in effect (e.g. Windows' "Best performance" /
  "Balanced" / "Power saver", or a Linux `cpufreq` governor). On Windows, this is two layers, not
  one: `powercfg /getactivescheme` reports only the base scheme (typically "Balanced"), not the
  power-mode overlay set via the battery icon/Settings ("Best performance" / "Balanced" /
  "Power saver"), which is what actually varies day to day and is not visible to that command at
  all - `powercfg /getactivescheme /overlay` does *not* exist as a switch (confirmed against this
  version's own `/?` output; do not guess at it if reaching for it out of memory). Get the active
  overlay's GUID from the registry - `reg.exe query
  "HKLM\SYSTEM\CurrentControlSet\Control\Power\User\PowerSchemes" /v ActiveOverlayAcPowerScheme`
  (plain `reg.exe`, works over a WSL-interop shell without needing PowerShell at all; the
  already-documented `Get-ItemProperty
  'HKLM:\SYSTEM\CurrentControlSet\Control\Power\User\PowerSchemes' -Name ActiveOverlayAcPowerScheme`
  is equivalent if already in a PowerShell session) - then resolve *that* GUID to a name
  automatically via `powercfg /query <guid>`, which prints a localized name and, more usefully
  across locales, a `GUID-Alias:` line (e.g. `OVERLAY_SCHEME_MAX` for "Best performance";
  `961cc777-2547-4f9d-8174-7d86181b8a7a` resolves to "Best power efficiency"/Power Saver the same
  way) - no hand-maintained GUID-to-name table needed, `/query` already knows every scheme and
  overlay by GUID, not only the ones `/list`'s own summary happens to show.
  `ActiveOverlayDcPowerScheme` (the battery-power equivalent) may be the all-zero GUID, meaning no
  DC-specific override is set, not an error - `powercfg /query` on the all-zero GUID itself
  correctly reports "does not exist".
  Record both the base scheme and the overlay, e.g. "Balanced base scheme, Best performance
  overlay", rather than assuming they are the same thing. Whether the machine was actually running
  on AC or battery at measurement time is not reliably queryable this way (this project's own CIM
  route for that - `Win32_Battery` - has been blocked by an "administrators only" restriction over
  a remote/interop session where this was tried) - ask the developer directly if it is not already
  obvious (a plugged-in laptop on a desk, say) and it might matter for interpreting the result.
- **IO device** (for IO-bound workloads): what the measured path actually writes to/reads from -
  e.g. a slow USB stick, a local SSD, an external USB3 drive, sshfs over Wi-Fi.
- **DedupFS build**: the git commit (and branch) of the code under test.
- **Isolation**: what was actually done to keep other processes from interfering (closed
  applications, disabled background services/indexers, otherwise idle machine, ...). Note what was
  *not* controlled for too, where relevant, rather than implying more isolation than there was.

### Workload

- **Operation** and **location** - see the catalogs below.
- **Tool**: the concrete command or tool that produced the measurement - e.g. a specific `dfs`
  subcommand, a native tool (`robocopy`, `cp`, PowerShell `Copy-Item`), a small throwaway script.
  Specific enough that, together with Scale/Content below, the run could be reproduced from this
  alone.
- **Mode**: sequential, or parallel with a given `N`.
- **Scale**: how many directories/files the run touches, and their size where relevant.
- **Content**: unique, non-deduplicating content per file, unless the measurement is deliberately
  about a dedup effect (see "Workloads that probe dedup effects" below).

### Statistical approach

5 runs, each sized to complete in about 20 seconds where possible. A fixed operation count would
make "ops/s" less directly comparable across runs of different lengths, but a fixed window is only
useful if it lets enough operations complete within it to give a meaningful within-run spread -
e.g. writing 10 MB files over a slow USB link might only complete a handful of operations in 20
seconds, too few to say anything about spread. When a 20-second window would not yield robust
numbers for a given operation, use a longer window instead, in consultation with the developer, and
record the window length actually used in the measurement protocol.

Discard run 1 only when it is more than 50% slower than the median of runs 2-5, as a one-off warmup
cost (cold caches, first-page-fault effects, ...); note the discard and the numbers that triggered
it explicitly. Below that threshold, keep run 1. Use this fixed threshold rather than a subjective
"looks like warmup" judgment call - a single 5-run measurement gives no basis to tell a genuine,
recurring warmup effect apart from one run just being noisy, so without a fixed rule the decision
too easily turns into after-the-fact justification for dropping whichever number is least
convenient. Report the mean and the min-max range over the remaining runs (N=4, or N=5 if nothing
was discarded), not the mean alone - call it a range, not a "confidence interval": a real
confidence interval implies a statistical guarantee that four or five runs cannot actually back up,
and this project does not need that guarantee (see "Purpose" above), just an honest sense of
spread.

For "sequential vs. N-way parallel," record only the two most informative points, not a full curve:
sequential, and whichever `N` gives the highest total throughput (report both that `N` and the
resulting throughput).

State between the 5 runs: keep going rather than resetting between runs - a run's directories/files
accumulate on top of whatever the previous runs in the same measurement left behind, growing the
tree/dataset the workload runs against as the runs progress. If that produces a (roughly) monotonic
trend across the 5 runs rather than noise around a stable value, that is itself a real finding
about how the operation scales with existing tree/data size, not something to normalize away by
resetting state - note it explicitly in the measurement protocol's Notes section.

## Workload catalog

Operations with no expected deduplication effect (unique content per file/chunk) - the initial,
"DedupFS must be fast even without dedup helping" set that
[`../requirements/non-functional/performance.md`](../requirements/non-functional/performance.md)'s
REQ-PERFORMANCE-004/005/006 describe:

- Directory creation
- Directory lookup, in a tree large enough that lookup cost is not dominated by the whole tree
  fitting in some cache
- Directory listing (readdir), in a directory with enough entries that per-entry cost dominates
  over fixed per-call overhead
- Zero-byte file creation, spread across several directories
- File creation at 100 B, 30 KB, and 10 MB, each with unique content - the 10 MB size spans several
  chunks at the default chunking configuration, the smaller two do not
- Reading back each of the above (analogous to the corresponding write)

### File-content workloads

The file-creation and file-read workloads generate unique content per file. Two rules keep that
from distorting the measurement:

- **Generation must not become the larger cost.** A per-file random fill is part of the measured
  loop unless deliberately hoisted out of it, and above roughly 1 MB per file a slow generator
  (PowerShell's `System.Random` manages only about 100 MB/s) becomes the larger cost, turning a
  "file creation" number into a generator benchmark instead. The PowerShell scripts fill one
  random template buffer once, before the runs, and per file overwrite 8 fresh random bytes at
  up-to-64 KiB intervals and in the final 8 bytes - the slow generator never runs inside the loop
  at all, only a handful of small pokes do. The shell scripts instead call `perf-gen`
  (`crates/perf-gen`, a tiny seeded-PRNG binary, ~3 GB/s) once per file, fully inside the loop -
  negligible even at 10 MB, unlike `/dev/urandom`, this script family's previous approach, whose
  read cost was no longer negligible at that size.

  The two platforms deliberately use different mechanisms rather than one shared scheme. A scaled-
  down trial (2 s window) measured calling `perf-gen` as a per-file subprocess against the
  PowerShell side's existing template-and-poke throughput:

  | size  | template+poke | `perf-gen`, one process **per file** | `perf-gen`, one process **per run** (streamed) |
  |-------|---------------|---------------------------------------|--------------------------------------------------|
  | 100 B | ~250 ops/s    | ~18 ops/s (14x slower)                 | ~196 ops/s (ok)                                  |
  | 30 KB | ~190 ops/s    | ~18 ops/s (10x slower)                 | ~176 ops/s (ok)                                  |
  | 10 MB | ~100 ops/s    | ~8 ops/s (12x slower)                  | ~8.5 ops/s (still ~12x slower)                   |

  Windows process creation (~50 ms per `CreateProcess`, CRT init, AV hook) dominates the per-file
  column; streaming one process per run rescues the small sizes but not 10 MB, where moving the
  whole payload across the process boundary through a pipe is itself the cost, not process count.
  Content was correct in every case (exact size, every file distinct) - this is a throughput
  finding, not a bug. Decision: native Windows stays on template-and-poke, no `perf-gen` there;
  `.sh` uses `perf-gen` regardless, since a fresh process is cheap enough on Linux for this not to
  matter, and it removes the `/dev/urandom` read-cost caveat. Both platforms end up with generation
  negligible in - or fully absent from - the timed loop, which is what actually keeps the two
  families' numbers comparable; a single shared mechanism was never the goal in itself.
- **Per-file uniqueness must survive content-defined chunking.** For the PowerShell scripts' poke
  scheme specifically, the poke spacing must stay well below the chunker's minimum chunk size
  (`2^(target_size_bits - 1)`, = 512 KiB at the 20-bit default) so that every chunk - including the
  smaller-than-minimum final chunk flushed at EOF, which is why the final 8 bytes are always poked
  - contains at least one poke and therefore differs between files. `perf-gen`'s output is random
  throughout, so this does not need separate checking on the shell side - any two files differ in
  every chunk already. When a chunked-path baseline is taken, confirm the store's post-run
  deduplication ratio is approximately 0 regardless of which script family produced the content: a
  non-zero ratio means content leaked into identical chunks and the number is contaminated.

Content accumulates across the 5 runs (see "State between the 5 runs" above), so a file-content
run's on-disk footprint is roughly throughput x total window x file size - at 10 MB this reaches
several GB. Check free space before starting, and delete `$root` afterwards.

These baselines are taken at the default chunking configuration (content-defined, 20-bit target)
unless a measurement protocol records otherwise. Baselining every chunk-size setting is out of
scope; a specific chunk-size question is measured as its own targeted comparison if it arises.

### Further workloads

Not yet tied to a specific REQ-PERFORMANCE-* requirement, but useful diagnostic data points:

- Deletion, of files and of directories - removing content that participates in deduplication
  (REQ-STORAGE-002 in [`../requirements/functional/storage.md`](../requirements/functional/storage.md))
  needs a reference-count decrement rather than a straightforward physical delete, a path with no
  non-dedup analogue among the operations above to compare it against.
- Rename/move, both within one directory and across directories - expected to be a metadata-only
  operation with no content rewrite; a naive implementation could regress to something costlier.
- Recursive tree walk (list and stat every entry under a subtree, `find`-style) - closer to what a
  real backup tool's scan or verify pass actually does than any single operation above in isolation.

### Workloads that probe dedup effects

Deliberately out of scope for this first pass. Measuring a dedup effect needs to control for
*which* content repeats and how (whole-file duplicates, partial/chunk-level overlap, ...), which is
a different methodology question from the above - worth its own addition to this file once the
non-dedup baseline exists to compare against.

## Location catalog

Where a workload above can be measured:

- Natively on the host filesystem (NTFS, ext4, FAT32, ...) - the baseline everything else is
  compared against.
- Through a mounted DedupFS - also varies by which physical device the repository itself lives on,
  independently of where the mount's client-side operations originate. Directory creation has a
  script today (`../scripts/dfs-mount-dir-create.ps1`, Windows/WinFSP only); other operations
  mirror `db-direct`'s limits, since the mount ultimately calls the same `db::Repository` methods.
- Through the `dfs` CLI tools, as the relevant commands come into existence.
- Directly against the DedupFS database, via a small Rust benchmark calling `db::Repository`'s
  methods with no mount or CLI layer in between - a ceiling any higher layer cannot exceed, since
  it still goes through the same calls underneath. Only meaningful for directories and zero-byte
  files; content chunking/storage is not reachable this way. Directory creation has a benchmark
  today (`crates/db/examples/db_bench.rs`); zero-byte files do not yet, since nothing in `db`
  creates a file-kind tree entry until REQ-STORAGE-007's byte store exists.

## Recording template

```markdown
# <operation> - <location> - <machine>/<environment>/<IO device>

## Setup
- Date:
- Machine:
- Execution environment:
- Power profile:
- IO device:
- DedupFS build:
- Isolation:

## Workload
- Operation:
- Location:
- Tool:
- Mode:
- Window: (per-run duration actually used; 20 s unless a longer window was needed, see
  methodology.md's "Statistical approach")
- Scale:
- Content:

## Results
| Run | Result |
|---|---|
| 1 (discarded?) | |
| 2 | |
| 3 | |
| 4 | |
| 5 | |

Mean: ... Range: ... - ... (N=...)

## Notes
```

## Machine-readable sidecar

Alongside `measurements/<date>-<machine>-<slug>.md`, a same-named
`measurements/<date>-<machine>-<slug>.yaml` carries the same Setup/Workload/Results fields in a
form a future aggregation script can read without parsing prose - the `.md` file stays the
human-readable record (including any free-text notes), the `.yaml` file is a derived, structured
summary of it, not a replacement. YAML over JSON: these are written by hand, and YAML's lack of
required quoting/commas and support for comments matters more here than any parsing convenience
JSON might offer a script that does not exist yet.

```yaml
date: 2026-09-01
machine: julius
environment: native-windows       # or: wsl2, docker-in-wsl2, native-linux, ...
power_profile: best-performance
io_device: local-ssd              # omit if not IO-bound
dfs_build: <git commit/branch>
operation: mkdir
location: native                  # native | dfs-mount | dfs-cli | db-direct
tool: mkdir (WSL2 coreutils)       # concrete command/tool used, see "Workload" above
mode: sequential                  # or: parallel
parallel_n:                       # the winning N, only when mode is parallel
window_seconds: 20                # per-run duration actually used; 20 unless a longer window
                                   # was needed to get enough operations, see "Statistical approach"
scale: 10000                      # directories/files touched
content_size:                     # bytes per file, where applicable
result:
  unit: ops_per_second            # or: bytes_per_second
  mean:
  range: [min, max]               # spread across the runs kept, not a statistical confidence interval
  n: 4                            # runs averaged over, after discarding warmup if any
  discarded_warmup_run: false
```

## Relationship to design-doc-embedded benchmarks

Not every performance number in this project needs to go through the process above. A narrow
benchmark validating one specific implementation decision - e.g. a runnable example crate
(`crates/store/examples/store_bench.rs` for DESIGN-STORE-004/005's read-handle-cache and
lazy-directory-creation choices) with its result reported directly in that design doc's own prose
- stays exactly that: quick, informal, and read alongside the decision it validates, per
`docs/design/README.md`'s "Reference Direction" (the benchmark cites the decision it informs, not
the other way around). Forcing every such A/B check through this directory's full
environment-tracked, file-per-run format would cost more than it buys for a check whose whole
point is a fast yes/no on one internal choice.

What is worth borrowing from this methodology either way: several runs rather than one, and
reporting a range/consistency across runs rather than a single number - cheap to do, and it is the
difference between "roughly 150x, 130x-190x across four runs" (trustworthy) and a single
unrepeated measurement (could be anything from a lucky or unlucky run).

The two do connect where a design-doc benchmark's subject overlaps a REQ-PERFORMANCE-*
requirement's scope - `store_bench.rs`, for instance, is the natural tool to produce a
`measurements/` entry (with the fuller environment tracking this directory asks for) if the
store's per-call read overhead (drafted as a requirement on `rust-store`, not yet merged here - a
requirement-numbering collision with this branch's own additions is expected and gets resolved at
merge time, not before) is ever validated systematically across machines, rather than something to
reinvent.
