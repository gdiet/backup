# RAM Budget

How the application arrives at an explicit ceiling on the memory it uses for caching and buffering
not-yet-durable content (REQ-OPERABILITY-006 in
[`../../requirements/non-functional/operability.md`](../../requirements/non-functional/operability.md)),
shared by the mount write path ([`mount-write-path.md`](mount-write-path.md)) and the ingest pipeline
([`ingest-bounded-pipeline.md`](ingest-bounded-pipeline.md)).

## DESIGN-MEMORY-001: Startup RAM budget - gross limit minus reserves, fixed for the process lifetime
Status: decided

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

### Provisional dispatch-pool reserve

The FUSE/WinFSP dispatch pool's own thread count and per-thread stack size are not yet measured on
either platform - both are tracked as their own `agent-todos/` items
(`agent-todos/determine-libfuse3-dispatch-pool-and-stack-size.md`,
`agent-todos/determine-winfsp-dispatch-pool-and-stack-size.md`). Until real numbers land from
whichever environment picks those up, the reserve uses a provisional, documented,
CLI-overridable estimate: 16 dispatch threads x 8 MiB (a pthread-created worker thread's own default
Linux stack size, distinct from - and larger than - Rust's 2 MiB `std::thread::Builder` default used
for this project's own threads) = 128 MiB. Both the thread count and the per-thread size are chosen
conservatively high rather than risking an under-reserved budget that then lets the mount's actual
memory use exceed the operator-visible ceiling once a real session's dispatch pool grows under load.
An operator whose own environment measures differently can override the reserve directly rather than
waiting for the agent-todos above to resolve; update this estimate (and the reserve's default in
code) once real measurements land, per those agent-todos' own closing instructions.
