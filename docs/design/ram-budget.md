# RAM Budget

How the application arrives at an explicit ceiling on the memory it uses for caching and buffering
not-yet-durable content (REQ-OPERABILITY-006 in
[`../../requirements/non-functional/operability.md`](../../requirements/non-functional/operability.md)),
shared by the mount write path ([`mount-write-path.md`](mount-write-path.md)) and the ingest pipeline
([`ingest-bounded-pipeline.md`](ingest-bounded-pipeline.md)).

## DESIGN-MEMORY-001: Startup RAM budget - gross limit minus reserves, fixed for the process lifetime
Status: implemented (crates/cli/src/ram_budget.rs, crates/db/src/connection.rs)

The application computes one number once, at startup, and never recomputes it while running: the
total bytes it may hold in caches and buffers for content not yet durably committed. It starts from
an operator-configurable gross limit - a sensible default requiring no configuration for typical use
(256 MiB, matching `crates/cli/src/write_cache.rs`'s existing `DEFAULT_BUDGET_BYTES`) - and subtracts
two reserves before anything is available for caching:

- **The database connection's own memory.** `db::Repository` holds exactly one SQLite connection for
  its whole lifetime, so this is a single, precisely readable number rather than an estimate:
  `PRAGMA cache_size` read back once at startup (also CLI-configurable, defaulting to leaving
  SQLite's own built-in default unchanged rather than overriding it).
- **Runtime and thread-stack overhead.** A fixed 2 MiB reserved per thread the CDC/hash/persist pool
  is configured to run (`available_parallelism()`-sized, per DESIGN-MOUNT-006), plus a
  provisionally-documented reserve for the FUSE/WinFSP dispatch pool - see "Provisional
  dispatch-pool reserve" below, since that pool's own thread count and stack size are not this
  project's own threads to size.

Fixed once at startup rather than adjusted dynamically (e.g. "30% of currently free non-swap RAM") is
a deliberate first cut: a fixed number is trivially reasoned about - an operator sizes it once against
their own machine and workload - and needs no live measurement of system-wide memory pressure, which
a dynamic scheme would have to get right across platforms to be trustworthy at all. Revisit only if a
fixed budget turns out to be a genuine practical limitation; not attempted here.

A repository whose own configured chunking granularity (REQ-STORAGE-003 in
[`../../requirements/functional/storage.md`](../../requirements/functional/storage.md)) cannot
possibly fit within the resulting budget is refused at startup (REQ-OPERABILITY-006), rather than
exceeding it once running - see "Why 23 bits is the chunking-granularity ceiling" below for the
concrete bound this check is against.

### Why 23 bits is the chunking-granularity ceiling

REQ-STORAGE-003 caps the configurable CDC target size at 23 bits - a 96 MiB theoretical maximum chunk
size - specifically so this budget check has a fixed, small number to check against, independent of
whatever a given repository's own configuration happens to be: at 23 bits, a repository can only ever
need a 96 MiB allowance for one chunk in flight, never an unbounded one. `crates/cdc`'s own
`ChunkerConfig` keeps its general validation range unchanged (6..=30 bits, `SingleChunkChunker` kept
available) - `cdc` is a crate with standalone value, held to a narrow-but-general public API
(`.claude/rules/rust-code-quality.md`), and the narrower, application-specific 23-bit ceiling has no
reason to constrain a hypothetical caller with a different memory budget of its own. `create-repo`'s
own validation (REQ-CLI-005 in
[`../../requirements/functional/cli-commands.md`](../../requirements/functional/cli-commands.md))
enforces the narrower bound instead, since the requirement it exists to satisfy - fitting inside this
application's own RAM budget - is this application's concern, not the general-purpose chunking
library's.

### Platform-specific dispatch-pool reserve

The FUSE/WinFSP dispatch pool's own thread count and per-thread stack size are measured on both
platforms - each was tracked as its own `agent-todos/` item, both now in `agent-todos/done/`
(`determine-libfuse3-dispatch-pool-and-stack-size.md`, `determine-winfsp-dispatch-pool-and-stack-size.md`).

WinFSP measured on `julius` (Intel i5-6200U, 2 cores/4 logical processors); libfuse3 measured on
both `julius`'s WSL2 and, separately, on `3327` (Intel i7-1355U, 10 cores/12 logical processors) -
each reproduced identically across two separate runs on every machine:

| Platform | Dispatch concurrency peak | Per-thread stack size |
|---|---|---|
| WinFSP (`julius`, Windows) | 4 (matches this machine's logical-processor count) | 1 MiB exactly |
| libfuse3 (`julius`, WSL2/Debian 12, same physical machine) | 10 (does **not** match logical-processor count) | 8 MiB exactly, matching glibc's documented default |
| libfuse3 (`3327`, WSL2/Ubuntu 24.04) | 10 - identical to `julius`, despite 12 logical processors here vs. 4 there | 8 MiB exactly, same as `julius` |

The two platforms' pool sizes do not track logical-processor count the same way: WinFSP's matched
its one measured machine's core count exactly, while libfuse3's did not - two machines with a 3x
difference in logical-processor count (4 vs. 12) landed on the exact same pool size of 10, evidence
that libfuse3's own number behaves as a fixed fallback rather than anything derived from the
machine's hardware. A single, shared, core-count-based formula would not describe both platforms
correctly, so the reserve is platform-specific (`crates/cli/src/ram_budget.rs`'s
`dispatch_pool_reserve_bytes`, `#[cfg(target_os = "linux")]`/`#[cfg(target_os = "windows")]`):

- **Linux/libfuse3**: a fixed `10 x 8 MiB = 80 MiB`, matching the reproduced fixed-fallback
  behavior above.
- **Windows/WinFSP**: `available_parallelism() x 1 MiB`, matching the one measured machine's
  core-count-scaling behavior.

Neither measurement has been reproduced across more than a couple of machines per platform (one for
WinFSP, two - agreeing - for libfuse3), so both remain estimates an operator can still override via
the RAM budget total, not guarantees; a future `dfs self-check` command (tracked as an idea, not yet
built - "On-demand memory/threading self-check" in
[`../../requirements/open-questions.md`](../../requirements/open-questions.md)) could recalibrate
this reserve automatically against the actual running environment instead of relying on measurements
taken elsewhere.
