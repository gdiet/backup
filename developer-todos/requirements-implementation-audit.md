# Audit whether every requirement's `Status` still matches reality

**Noted**: 2026-09-29, after finishing the Scala-repository migration tool and noticing along the
way that its own design doc had drifted from actual `Status` values (several `DESIGN-MIGRATION-...`
entries were still `decided` after the code implementing them had already shipped), and that
REQ-STORAGE-005 is `agreed` but has no implementation anywhere despite sounding, from its title
alone, like something `dfs reclaim`/`dfs db-compact` might already cover (they do not - see
`developer-todos/store-defragmentation.md`).
**Size**: medium to large - a full pass over every file under `requirements/`, confirm with the
developer before starting given the likely scope.
**Context**: `requirements/README.md`'s status scheme (`agreed`/`should`/`must` etc.) and directory
layout; `.claude/rules/design-docs.md`'s parallel `Status:` convention for `docs/design/`.

The developer's own request: go through `requirements/` systematically and check, requirement by
requirement, whether its current `Status` line still reflects reality - not just take each file's
own claim at face value. Concretely, for each `REQ-...` entry:

- If it reads as `agreed`/`should`/`must` with no obvious implementation, confirm that is actually
  still true (grep for the feature, check whether a `dfs` command or `db`/`store`/`cdc` API already
  covers it) rather than assuming an old, stale `Status` line.
- If a linked `docs/design/...` decision exists, cross-check that design doc's own `Status` too -
  the migration-tool experience this session showed these two can drift independently of each
  other and of the actual code.
- Flag (do not necessarily fix in the same pass) every requirement whose `Status` turns out wrong,
  with enough detail (file, line, what was found instead) for a follow-up session to correct it.

Not urgent, but worth doing once as a baseline sanity check rather than continuing to accumulate
requirements whose documented state nobody has re-verified against the actual, current code.

## Interim results

### Pass 1: `agreed` requirements with Importance `must` or `should` and no implementation

Done 2026-10-05, against commit `abae21b6`. Method: listed every `agreed` `must`/`should`
requirement, then examined those that no code or design document cites, or whose CLI command is
missing. The remaining requirements were matched only by code citations and the `dfs --help`
command list. They were not examined individually.

Not implemented:

- `REQ-INTEGRITY-001` (must): no verify command exists, neither the quick nor the thorough depth.
  `dfs restore --verify` only checks files that are being restored.
- `REQ-INTEGRITY-002` (should): nothing exists. It depends on `REQ-INTEGRITY-001`.
- `REQ-STORAGE-005` (should): nothing exists. See `developer-todos/store-defragmentation.md`.
- `REQ-MAINTENANCE-006` (should): no wait option exists. `docs/design/repository-locking.md`
  states that it is not addressed.
- `REQ-MOUNT-005` (should): partially implemented. Reads fail visibly by default
  (`crates/cli/src/content_reader.rs`, `docs/design/byte-store.md`). The best-effort opt-in for a
  mount is missing. `dfs mount` has no such option. `dfs restore --best-effort` exists.

Consequence: `REQ-PERFORMANCE-001` cannot be met while `REQ-INTEGRITY-001` does not exist.

Checked and implemented: `REQ-INTEGRITY-003` (`ref_count` triggers in `crates/db`),
`REQ-RESTORE-002` (`dfs restore` resolves `[deleted]` paths), `REQ-INGEST-002`, `REQ-INGEST-003`,
`REQ-INGEST-006`, `REQ-STORAGE-008`.

Not examined yet: `could` requirements, the `Status` lines of `docs/design/` decisions, and
whether `REQ-PERFORMANCE-004` and `REQ-PERFORMANCE-005` are met by measurements.

Update 2026-10-05: `REQ-MOUNT-005` is now fully implemented (`dfs mount --best-effort`,
DESIGN-MOUNT-027 in `docs/design/mount-write-path.md`).
