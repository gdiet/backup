# Feature Comparison: Scala Implementation vs. Rust Implementation

Per-feature parity tracking against the Scala implementation.

Status values: `implemented` | `planned` | `not planned` (requires a one-line reason).

| Scala Feature | Status | Notes |
|---|---|---|
| Whole-file content deduplication (MD5-based) | planned | non-CDC, whole-file chunking exists as a library building block (see REQ-STORAGE-003), using `blake3` instead of MD5, but is not yet reachable through any command |
| Mount read-write (real content writes: create/write/truncate/unlink) | implemented | `crates/cli/src/dedup_fs.rs`, DESIGN-MOUNT-006/007/008/009/010/012/013/015 in [`../docs/design/mount-write-path.md`](../docs/design/mount-write-path.md); DESIGN-MOUNT-009's failure log is a plain file only, not yet queryable |
| `list` command (directory listing without mounting) | implemented | `crates/cli/src/list.rs`, REQ-QUERY-001, REQ-CLI-007's `--show-deleted` |
| `backup`/`restore` commands' restore direction (repository paths to a real directory on disk) | implemented | `crates/cli/src/restore.rs`, REQ-RESTORE-001/002/003/004 |
| Browsing/restoring soft-deleted entries without mounting | implemented | REQ-TREE-009 (`[deleted]` path addressing, `requirements/functional/tree.md`), `crates/cli/src/deleted.rs`; wired into `dfs list --show-deleted` and `dfs restore` |
| `ingest` command (real filesystem into the repository) | implemented | `crates/cli/src/ingest.rs`, `crates/cli/src/ignore_rules.rs`, `crates/cli/src/target_path.rs`, REQ-INGEST-001/002/003/004/005/006/007; REQ-INGEST-007's `[...]` date/time placeholders and `+`/`!` per-segment existence markers are DESIGN-CLI-006's concrete syntax |
| `check <path>` (single-file integrity check) | planned | REQ-INTEGRITY-001/002 in [`../requirements/functional/integrity.md`](../requirements/functional/integrity.md) - a broader design than Scala's (repository-wide or path-scoped, quick or thorough depth), not yet implemented by any command |
| `db-backup`/`db-restore` (metadata backup/restore) | implemented | `crates/cli/src/db_backup.rs`, `crates/cli/src/db_restore.rs`, REQ-MAINTENANCE-001/002; backup is `VACUUM INTO` (read-only, never blocks or is blocked by a concurrent mutating operation); REQ-MAINTENANCE-007's stale-backup warning after a future reclaim/compact run is not part of this - its own precondition, REQ-STORAGE-004/005, does not exist yet (see `../docs/design/stale-backup-detection.md`) |
| `db-compact` (metadata compaction) | implemented | `crates/cli/src/db_compact.rs`, REQ-MAINTENANCE-003 - `PRAGMA incremental_vacuum`, no long exclusive lock |
| `del <path>` (delete a tree entry without mounting) | implemented | `crates/cli/src/del.rs`, REQ-CLI-003; deliberately diverges from Scala's unconditional recursive delete - a live directory with live children needs an explicit `--recursive` opt-in. Also covers permanent purge of a specific soft-deleted entry (REQ-TREE-009 `[deleted]` addressing) behind an explicit `--purge` flag - REQ-CLI-004 is folded into REQ-CLI-003 |
| `find <matcher>` (search entries by name/path pattern) | implemented | `crates/cli/src/find.rs`, REQ-QUERY-002 - name/path pattern with `*`/`?` wildcards, case-insensitive |
| `stats` / `stats <path>` (repository or path statistics) | implemented | `crates/cli/src/stats.rs`, REQ-QUERY-003 - item counts, logical/physical size, dedup ratio; repository age is repository-wide only |
| `reclaimSpace` (bulk purge of aged soft-deleted entries and space reclamation) | planned | REQ-STORAGE-004 in [`../requirements/functional/storage.md`](../requirements/functional/storage.md), not yet implemented by any command |
| `blacklist` (mark content forbidden, rejected if re-stored) | planned | still an open, undecided idea - "Content blacklisting" in [`../requirements/open-questions.md`](../requirements/open-questions.md), not yet an agreed requirement |
| `mount`'s `gui` option (a graphical front end) | planned | still an open, undecided idea - "User interface beyond the command line" in [`../requirements/open-questions.md`](../requirements/open-questions.md) |
| `mount`'s `copyWhenMoving` option | planned | still an open, undecided idea - "'Copy when moving' semantics" in [`../requirements/open-questions.md`](../requirements/open-questions.md) |
| `mount`'s `dbBackup` option (auto-backup before a mutating session) | planned | still an open, undecided idea - "Automatic metadata backup before a mutating session" in [`../requirements/open-questions.md`](../requirements/open-questions.md) |
| `mount`'s default mountpoint (`J:\`/`/mnt/dedupfs` when none given) | not planned | a fixed OS-specific default is a much weaker fit here than REQ-CLI-006's "next to the executable" repository-path default, since a mountpoint has no natural relationship to where the software lives - see "Default mountpoint when none is given" in [`../requirements/open-questions.md`](../requirements/open-questions.md), left open to a genuinely better default rather than adopted for parity's sake |
| Graceful interrupt (Ctrl+C/SIGTERM) during a long `backup`/`restore` run | planned | still an open, undecided idea - "Graceful interrupt during a long-running ingest or restore run" in [`../requirements/open-questions.md`](../requirements/open-questions.md); not a data-safety gap here the way it guards against one in Scala, since REQ-INGEST-004's per-item resilience and temp-file-then-rename writes already make an abrupt kill safe, only incomplete |
| `dbMigrateStep1`/`dbMigrateStep2` (upgrading an old Scala database to a newer Scala schema version) | not planned | internal to Scala's own release history, unrelated to migrating a Scala repository into this implementation (see [`from-scala.md`](from-scala.md) for that separate concern) - this implementation has its own independent schema-migration mechanism (DESIGN-METADATA-005) |

## Non-Functional Parity

Beyond matching individual features one-for-one above, this implementation's performance and
usability are each at least as good as the Scala implementation's, and better where achievable -
not a specific feature to check off, but a standing bar the rewrite is held to throughout.

- **Performance**: this project's own `REQ-PERFORMANCE-*` requirements in
  [`../requirements/non-functional/performance.md`](../requirements/non-functional/performance.md)
  set an independently-motivated bar (matching or beating the *native* filesystem on slow storage),
  not a Scala comparison - meeting them does not by itself establish Scala parity, since the Scala
  implementation may itself already match or beat native storage for some operations (not yet
  measured here). Verifying this bullet needs an actual head-to-head measurement against the Scala
  implementation, not an inference from the native-filesystem requirements alone.
- **Usability**: not yet broken out into individual requirements the way performance is - judged
  holistically for now (installation effort, defaults, error messages, day-to-day operation) against
  the Scala implementation as the baseline to match or beat, per `../requirements/non-functional/operability.md`'s
  existing usability-adjacent requirements (REQ-OPERABILITY-001/003/004). Revisit this bullet if a
  concrete usability shortfall against Scala is ever identified and needs its own tracked
  requirement.
