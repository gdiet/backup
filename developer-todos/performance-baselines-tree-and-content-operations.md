# Performance baselines for tree and file-content operations across environments

**Noted**: 2026-08-25, mid-conversation during the tree-namespace-case-sensitivity design work
(REQ-MOUNT-010, DESIGN-MOUNT-005) - that discussion touched on whether a Rust-side scan-and-compare
fallback for Windows case-insensitive lookups stays fast enough for large directories, which
surfaced the broader, currently unanswered question below.
**Size**: large - confirm with the developer before starting. A real measurement effort across
multiple environments and configurations, not a quick benchmark.
**Context**: `requirements/functional/mount.md` (REQ-MOUNT-010); `docs/design/tree-namespace-case-sensitivity.md`
(DESIGN-MOUNT-005's "revisit if" trigger, which this would help answer with real numbers instead of
a guess).

Establish real performance baselines for:

- Tree operations (`mkdir`/`create`/`rename`/lookup/`readdir` in `crates/db`) across different
  environments and, separately, different power/energy settings (e.g. a laptop on battery-saver
  versus plugged in/performance mode) - relevant wherever a decision currently rests on an assumed
  directory size or scan cost rather than a measured one (see DESIGN-MOUNT-005's Rust-side
  case-insensitive scan fallback for a concrete example).
- File-content operations, equivalently, across different storage media, filesystems, and
  environments (an internal SSD versus external/USB storage versus a network share; ext4 versus
  NTFS versus others) - once REQ-STORAGE-007's byte store actually exists to measure.

The developer's own framing: "Performance Baselines für Tree-Operationen auf verschiedenen
Umgebungen / mit verschiedenen Energie-Settings; genauso auch für file content Operationen auf
verschiedenen Datenträgern / Dateisystemen und Umgebungen."

## Status as of 2026-09-01 - what is actually still open

Not done, still `large`/needs-confirmation as originally noted - but a lot of measurement
infrastructure and data now exists under `performance/`, so the useful thing at this point is a
precise account of what that data does and does not cover, checked directly against
`performance/measurements/*.yaml` (48 protocols) rather than assumed:

- **`location: native` only - the single biggest gap.** Every measurement taken so far is
  `location: native` (the host filesystem directly, NTFS/ext4) - `grep -h '^location:'
  performance/measurements/*.yaml | sort -u` returns exactly one value. `location: dfs-mount` and
  `location: db-direct` both have runnable tooling
  (`performance/scripts/dfs-mount-dir-create.{ps1,sh}`, `crates/db/examples/db_bench.rs`) that has
  only ever been trial-validated (scaled-down runs, bugs found and fixed), never actually run as a
  full 5-run measurement and written up - `performance/README.md`'s own wording already says as
  much. `location: dfs-cli` has no tooling at all yet (the CLI itself is still early - `create-repo`
  and `mount` exist, most subcommands do not). Concretely: **nothing measured so far says anything
  about DedupFS's own performance** - every number to date characterizes the native filesystem
  DedupFS is compared against, not DedupFS itself.
- **Operations `mkdir`/`create-file-*`/`dir-lookup`/`dir-listing`/`read-file-*` only.** `rename`,
  `delete` (file and directory), and a recursive tree walk (`find`-style) remain unmeasured -
  `methodology.md`'s "Further workloads" list still has no scripts built for any of them.
- **Power/energy-setting comparison only actually exists for two operations.** Directory creation
  and zero-byte-file creation on `julius` each have a real Best-Performance-vs-Power-Saver A/B pair;
  every other operation (lookup, listing, 100 B/30 KB/10 MB create and read) has exactly one
  power-mode data point each, no comparison - `grep -h '^power_profile:'
  performance/measurements/*.yaml | sort -u` shows only two distinct non-empty values across all 48
  files, both from that one pair of operations.
- **Storage/filesystem diversity is narrow.** `grep -h '^io_device:' performance/measurements/*.yaml
  | sort -u` shows exactly three: `local-ssd`, `local-nvme-ssd`, `usb2-stick` (the last one only for
  directory creation and 100 B/10 MB file creation on `julius` - no reads, no other directory
  operations). Filesystems tested: NTFS (native Windows) and ext4 (WSL2) only - no FAT32/exFAT (a
  realistic external-backup-drive scenario) and no network filesystem/share at all. DESIGN-
  MAINTENANCE-002's own design doc separately validated an SFTP/Wi-Fi network mount, but only for
  write-lock behavior specifically (`crates/store`'s read-cache/lazy-directory-creation decisions) -
  narrow, informal, not part of this systematic framework, and says nothing about ordinary
  tree/file-content throughput over a network path.
- **The byte store's own numbers exist, but not in this framework.** REQ-STORAGE-007's byte store
  (`crates/store`) now actually exists - the precondition this todo's file-content half was written
  against - and has its own micro-benchmark (`crates/store/examples/store_bench.rs`), but its
  results live only informally in `docs/design/byte-store.md`'s prose (validating DESIGN-STORE-004/
  005 specifically), never turned into a `performance/measurements/` entry with the fuller
  environment tracking this directory asks for - `methodology.md`'s "Relationship to design-doc-
  embedded benchmarks" section already names this exact gap as the natural next step, not yet taken.

None of the above blocks calling this todo done on its own - it is exactly the kind of large,
multi-environment effort the original note already flagged as needing the developer's own
confirmation before continuing, not something to keep expanding unprompted.
