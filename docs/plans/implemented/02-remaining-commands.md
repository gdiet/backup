# Port remaining Scala CLI commands: stats, list, find, check, del, restore, db-backup/db-restore/db-compact, reclaim-space

## Context

The Scala tool this Rust rewrite replaces (`/home/georg/privat/bdev/scala`) has a mature CLI surface beyond `backup`/`init` that this Rust port doesn't have yet: repository statistics, directory listing, name search, integrity checking, deletion, restoring files out of the repository, database backup/restore/compaction, and orphan space reclamation. This plan ports that functionality, but **not 1:1** — the user explicitly asked to reconsider Scala's design choices rather than copy them, and research into the Scala implementation surfaced several concrete, fixable weaknesses (detailed inline below) that our different foundations (SQLite with real FK/CHECK constraints and recursive CTEs, ref-counted chunk/content dedup, vs. Scala's H2 + manual whole-file dedup with no reference counting) let us avoid outright rather than merely improve.

Command name mapping from Scala (`fsc <cmd>`) to this Rust CLI (`backup <cmd>`) — same names except `db-compact`/`db-backup`/`db-restore` become `db compact`/`db backup`/`db restore` (a `db` sub-subcommand group) and reclaim-space (a separate top-level Scala executable, not part of `fsc`) becomes an ordinary subcommand here since we don't have that split:

| Scala | Rust |
|---|---|
| `fsc stats` / `fsc stats <path>` | `backup stats [path]` |
| `fsc list <path>` | `backup list <path>` |
| `fsc find <pattern>` | `backup find <pattern>` |
| `fsc check <path>` | `backup check [path]` (path now optional - see §5) |
| `fsc del <path>` | `backup del <path> [--recursive]` |
| `fsc restore <source> <target>` | `backup restore <source...> <target>` (already stubbed) |
| `fsc db-backup` | `backup db backup` |
| `fsc db-restore [file]` | `backup db restore <file>` |
| `fsc db-compact` | `backup db compact` |
| `reclaimSpace` (separate exe) | `backup reclaim-space [--keep-days N]` |

---

## 1. VACUUM / compaction strategy (the user's explicit question)

**Decision: `PRAGMA auto_vacuum = INCREMENTAL` set at repository creation, plus a `db compact` command that runs `PRAGMA incremental_vacuum` on demand.** Not a plain `VACUUM`, not `auto_vacuum = FULL`.

Why not `FULL`: it eagerly moves pages and truncates the file on *every* commit that frees pages, adding overhead to every DELETE-containing transaction. Our workload is insert-heavy during normal `store` runs and delete-heavy only during the rare, explicit `reclaim-space`/`del` operations - paying that cost on every write for a benefit we only want occasionally is a bad trade.

Why not leave `auto_vacuum` at the default (`NONE`): freed pages go on SQLite's internal free list and get reused by future inserts (so nothing is *lost*), but the file itself never shrinks - after a large `reclaim-space` run, the file stays at its peak size forever with no way to reclaim it short of a full `VACUUM` (see below).

Why `INCREMENTAL` + explicit `db compact`: `INCREMENTAL` mode tracks freed pages without eagerly truncating (near-zero overhead on ordinary writes - it only changes bookkeeping on page-freeing operations, which we barely ever do outside of maintenance commands), and `PRAGMA incremental_vacuum` then reclaims them on demand, in the caller's own transaction, without needing ~2x disk space or an exclusive lock for the DB's entire lifetime the way a full `VACUUM` rewrite does. This pairs naturally with `reclaim-space`: run `reclaim-space` to free rows, then `db compact` to actually shrink the file - two explicit, separately-runnable, cheap steps, mirroring the two-step nature Scala already had (`reclaim-space` then `db-compact`/H2 `SHUTDOWN COMPACT`) but built on a mechanism designed for exactly this incremental use case instead of H2's all-or-nothing `SHUTDOWN COMPACT`.

`auto_vacuum` is a persistent, file-level setting like `journal_mode`, and (per SQLite's own rules) can only be established for free on a *brand new, empty* database, before any tables exist - so it goes in `open_connection` in `db/src/lib.rs` (same place as `journal_mode`, same "no-op on repeat" reasoning: `open_connection` runs before `init_repository`'s migrations create any tables), not in the migration SQL (same `rusqlite_migration` caveat about PRAGMAs inside migration transactions already documented there for `journal_mode`).

`db backup` (see §6) is a separate, unrelated mechanism (`VACUUM INTO`) that happens to *also* produce a fully compacted copy as a side effect - worth knowing about, but not a substitute for `db compact`, since it writes to a new file rather than compacting the live one.

---

## 2. Shared building blocks (land first, everything else depends on this)

New `db` module `db/src/query.rs` (read-only tree queries, used by `stats`/`list`/`find`/`check`/`restore`/`del`):
- `resolve_path(conn, path: &str) -> Result<Option<TreeEntryRow>, Error>` - walks `/`-separated components from the root via repeated `find_tree_entry` lookups (same pattern `store`'s `resolve_target` already uses inline; this factors it out for reuse and to stop duplicating it a third time).
- `list_children(conn, parent_id) -> Result<Vec<(TreeEntryRow, String)>, Error>` - direct children only, for `list`.
- `subtree_stats(conn, id) -> Result<SubtreeStats { files, dirs, total_logical_bytes }, Error>` - **one recursive CTE**, not Scala's N+1 recursive Scala-side walk (`db/maintenance.scala:129-148` builds this by repeatedly calling `children()` down the tree in application code).
- `subtree_entries_with_paths(conn, root_id) -> Result<Vec<(full_path, TreeEntryRow)>, Error>` - one recursive CTE building full paths bottom-up for every live entry under a subtree (root = the tree root for whole-repo `find`/`check`), replacing Scala's fragile two-stage "SQL LIKE on bare name, then walk parents in a loop to reconstruct the path, with a `.` `.replaceAll` that's actually a no-op and doesn't escape regex metacharacters" (`db/maintenance.scala:188-214`, flagged in research).
- `ordered_content_chunks(conn, content_id) -> Result<Vec<(chunk_id, length, start, stop)>, Error>` - for `restore` and `check`.

New `cli` module `cli/src/format.rs`: `readable_bytes(u64) -> String` (mirrors Scala's `Helpers.readableBytes`, `scala/.../Helpers.scala:13-18` - same thresholds/behavior, it's a good, reusable formatter) - the one shared formatting helper multiple commands need; no shared "table" abstraction, each command's output is simple enough not to need one (matches what Scala itself does - no shared table formatter existed there either).

No CLI/README changes in this step - it's pure plumbing, verified by unit tests on the new `db` queries (temp repo, seeded rows, assert query results) - each subsequent command's commit is what actually exposes something user-visible.

---

## 3. `stats` (repo-wide and per-path)

`backup stats` (no path): counts of live/deleted files and dirs, distinct chunk/content counts, **physical bytes stored** (`SELECT COALESCE(MAX(stop), 0) FROM chunks`), and - new, Scala never computes this anywhere - **logical bytes and a dedup ratio**: `SUM(contents.length) FROM tree_entries JOIN contents ... WHERE deleted_at IS NULL` (content counted once per referencing live file, i.e. "bytes as the user would see them without any dedup") divided by physical bytes stored. This is an easy, meaningful metric for us specifically because we track real dedup relationships (ref counts) that Scala's whole-file-only, no-refcounting design never had a cheap way to compute.

`backup stats <path>`: recursive subtree summary (files, dirs, total logical size) via `subtree_stats` - same output shape as Scala's per-path stats, computed in one query instead of Scala's recursive-call-per-directory pattern.

Output: plain `println!`, not a logging framework (this CLI has none, consistent with today's `store`/`init`) - deliberately not replicating Scala's inconsistency where whole-repo `stats` goes through SLF4J/logback while every other command (including `stats <path>` itself!) uses bare `println` (`db/maintenance.scala:114-148`, flagged in research). One output channel, always.

---

## 4. `list` and `find`

`backup list <path>`: direct children of a directory (or a 2-line file-info block if `path` names a file, same as `stats <path>` on a file - factor that 2-line block into one small shared function instead of duplicating it, unlike Scala which duplicates it verbatim in two places). Columns: name, type marker, size, **and mtime** (Scala's `list` conspicuously never shows timestamps at all, `db/maintenance.scala:150-166` - we track real mtimes, showing them is a straightforward improvement). Sort: directories before files, alphabetical within each group (same convention as Scala). Formatting: simple fixed small gutter/columns computed from content, not Scala's brittle "pad with dots to fixed 38 chars" (`"." * math.max(2, 38-file.name.length)`) which just looks wrong for long names.

`backup find <pattern>`: matches against the **full reconstructed path** (via `subtree_entries_with_paths` from the tree root) using a small hand-rolled `*`/`?` glob matcher (case-insensitive substring match) rather than pulling in a regex dependency for two wildcard characters, and rather than Scala's two-stage SQL-LIKE-then-regex approach with its acknowledged escaping bugs (unescaped `%`/`_` passed straight into SQL LIKE, and a `.`-escape that's actually a no-op, `db/maintenance.scala:188-214`, README's own "May Come Eventually" backlog). One matching pass, one matching semantics, no SQL wildcard leakage possible since we never build a LIKE pattern from user input at all.

---

## 5. `check`

The single biggest capability gap in Scala's version (research flagged this explicitly): `check` can only check **one named file**, prints "is a directory" and stops if given a directory, has no whole-repository sweep, and - worst - **always exits 0** regardless of whether it found `OK`, `BAD`, or `ERROR`, so a script/CI job has no way to detect a failed check at all.

Rust design: `backup check [path]` - **path is optional**; omitted means check the whole repository (mirrors `stats`'s existing no-path-means-everything convention, so it's a consistent CLI idiom, not a new one-off). For every chunk in scope (all `chunks` rows if whole-repo, or every chunk reachable from the given subtree's contents via `subtree_entries_with_paths` + `ordered_content_chunks` if scoped): read `[start, stop)` from the `store::LongTermStore`, verify `stop - start == length`, recompute the blake3-20 hash of the bytes read, compare to `chunks.hash`. Report three distinct outcomes per problem found (not Scala's blurred-together "everything becomes BAD or a WARN log"): **missing/short data** (an actual I/O condition, surfaced explicitly - not silently zero-filled and only rate-limited-logged the way `store::LongTermStore::read`'s `ReadIntegrity::Incomplete` already reports it to callers, which `check` will act on directly), **length mismatch**, **hash mismatch**. Also verify `chunks.ref_count`/`contents.ref_count` against a live `COUNT(*)` from their respective referencing tables (`content_chunks`, `tree_entries`) - cheap, and catches a trigger bug or manual DB tampering that the CHECK constraints alone wouldn't (they only guarantee `>= 0`, not "correct"). **Exit code reflects the result**: 0 only if everything checked out, non-zero if anything was BAD/missing/mismatched - directly fixing Scala's flagged gap.

Not in scope: any DB structural-integrity checking beyond ref-count verification (dangling `parent_id`/`content_id`/`chunk_id` references are structurally impossible here - our foreign keys are enforced with `foreign_keys = ON` on every connection, unlike the one FK Scala's schema *doesn't* have, `TreeEntries.dataId → DataEntries.id`, because H2's `DataEntries` PK is composite and non-unique on `id` alone).

---

## 6. `restore` (real implementation - already stubbed in `main.rs`)

Existing stub `RestoreArgs { paths: Vec<PathBuf> }` (sources... + target, same shape as `store`) gets a real body: for each source repo path, resolve it, then recursively (directories) or directly (files) write bytes to the target filesystem location - `mkdir` for directories, and for files: look up the ordered chunk list via `ordered_content_chunks`, read each `[start, stop)` range from the store, write the concatenated bytes to the target file.

Concrete improvements over Scala's `BackupTool.restore` (`BackupTool.scala:10-63`, several fragility points flagged in research):
- **Per-file/per-directory errors are logged and skipped, not fatal** - same philosophy `store` already established (an unreadable/uncreatable entry doesn't abort the whole restore), fixing Scala's uncaught-`FileOutputStream`-exception-kills-everything behavior and its silently-ignored `mkdir()` return value.
- **mtimes are restored** via `std::fs::File::set_times` (stable since Rust 1.75, no new dependency) using the stored `tree_entries.time` - Scala's `restore` sets neither mtime nor anything else, an asymmetry with its own `backup` (which does call `fs.setTime`) that we can just not have.
- **Overwrite protection at every level**, not just the top-level name (Scala only pre-checks the single top-level target name; nested collisions are only discovered when the write itself fails) - open target files with `create_new(true)` so any existing-file collision fails atomically and by default, with an explicit `--overwrite` flag to allow replacing.
- No permission/ownership restoration (matches `store`'s existing documented scope - no regression, both directions of this tool stay symmetric).

---

## 7. `del`

`backup del <path>` (file) or `backup del <path> --recursive` (directory - **required flag, refusing without it**, unlike Scala's unconditional recursion with no safety net at all beyond a README-only warning). Soft-delete only (`deleted_at`), matching the schema's existing design (recoverable until `reclaim-space` actually purges it - itself already a meaningful safety improvement over Scala, where `del` interacts with a system that has no grace period built in beyond "if required, run `reclaim-space` later").

Implementation: **one SQL statement**, not Scala's fragile hand-rolled bottom-up recursion with no transaction wrapping (research flagged this explicitly: a killed process mid-recursion leaves a partially-soft-deleted tree, which is *why* Scala's `reclaim-space` needs its whole iterative "unrooting" repair pass in the first place):
```sql
UPDATE tree_entries SET deleted_at = ?1
WHERE deleted_at IS NULL AND id IN (
  WITH RECURSIVE subtree(id) AS (
    SELECT ?2
    UNION ALL
    SELECT t.id FROM tree_entries t JOIN subtree s ON t.parent_id = s.id
  )
  SELECT id FROM subtree
)
```
Single statement, atomic, same `deleted_at` timestamp for the whole subtree by construction - which is exactly the invariant that makes `reclaim-space` (§8) simple too. Deleting the repository root (empty path) is a hard error.

---

## 8. `db backup` / `db restore` / `db compact`

`backup db backup [name]`: `VACUUM INTO` a new, timestamped file under `meta/backups/` (default name `repository_<yyyy-mm-dd_HH-MM-SS>.sqlite3`) - a single SQL statement that produces a complete, already-compacted, self-contained snapshot, safe to run against a live WAL-mode database (readers/the one writer are never blocked by it) without any of Scala's fragile-file-copy-of-a-possibly-open-file concern (`plainDbBackup` is a raw `Files.copy`, flagged in research as not obviously safe against H2's own concurrent-access guarantees). Always timestamped, never overwrites a prior backup - fixes Scala's flagged "plain backup silently clobbers itself every run" gap. No SQL-script/H2-migration-format duality to port either - `VACUUM INTO`'s output *is* a complete, directly-usable SQLite database file, so there's only one backup format, not two.

`backup db restore <file>`: copies the given backup file over the live `meta/repository.sqlite3`, and - important, since `VACUUM INTO`'s output has no WAL sidecars of its own but the *live* file being replaced might - removes any stale `repository.sqlite3-wal`/`-shm` at the destination first, so the next connection opens cleanly against the new file rather than pairing it with leftover WAL frames from the old one.

`backup db compact`: `PRAGMA incremental_vacuum` on the live write connection (§1).

No automatic retention/rotation for backups (matches Scala - flagged there as entirely absent, not something to invent from scratch here without being asked) - just documented as a known gap in the README section.

---

## 9. `reclaim-space`

`backup reclaim-space [--keep-days N]` (default 0, matching Scala's `keepDays` default), auto-runs `db backup` first unless `--no-backup` is passed (matching Scala's `reclaimSpace` always backing up first - the one Scala default worth keeping, since this is the one genuinely irreversible operation in the whole set - `del` alone never is).

Three plain statements - no orphan set-difference scan, no iterative unrooting fixed-point loop (Scala's approach, `db/maintenance.scala:216-253`, needed *only* because Scala has no reference counting and no atomicity guarantee on directory-delete recursion; we have both, by construction, from §7 and from the `ref_count` triggers already in place):
```sql
DELETE FROM tree_entries WHERE deleted_at IS NOT NULL AND deleted_at <= ?cutoff;
DELETE FROM contents WHERE ref_count = 0;
DELETE FROM chunks WHERE ref_count = 0;
```
The first statement is safe as one multi-row `DELETE` (not needing a per-row/leaf-first loop the way a naive port of Scala's approach might assume) because SQLite checks immediate (non-deferred) foreign keys once at the end of the whole statement, not per intermediate row - so a statement that removes an entire eligible subtree together (parent and children, all matching the same `deleted_at <= cutoff` condition since §7 guarantees a subtree's soft-delete always shares one timestamp) never trips the `parent_id` FK against another row being deleted in the same statement. Will add a unit test asserting this directly (delete a multi-level soft-deleted subtree in one call, assert no FK error and everything's gone) rather than just relying on the reasoning.

Reports counts purged (tree entries, contents, chunks) - the useful part of Scala's `freeAreas()` logging, without the part that's a full DB scan whose result is then thrown away except for its log line.

Physical byte-store space is **not** reclaimed (`store::LongTermStore` has no delete/truncate/hole-punch operation, matching Scala's own `LongTermStore` limitation exactly - this is not a regression, and building a free-list allocator for the store is real, separate future work, already flagged as out of scope when `store` was designed).

---

## Sequencing / commits

Each numbered item below is its own commit: `db` query/maintenance additions + the `cli` subcommand + its README section, verified with `cargo fmt --check`/`clippy -D warnings`/`test` before committing (same discipline as the `store` work). Given the size, this will take multiple rounds:

1. Shared plumbing (§2): `db/src/query.rs`, `cli/src/format.rs`, unit tests. No user-visible change, no README section yet.
2. `auto_vacuum = INCREMENTAL` pragma (§1) - small, standalone, lands before `db compact` needs it.
3. `stats` (§3)
4. `list` (§4)
5. `find` (§4)
6. `check` (§5)
7. `restore` real implementation (§6)
8. `del` (§7)
9. `db backup` / `db restore` / `db compact` (§8)
10. `reclaim-space` (§9)

Each command's README section is added to `rust/README.md` in a new `## Usage` block (subsections per command, mirroring the Scala README's prose-plus-example `### How To ...` style) as part of that command's own commit - not a separate documentation pass at the end.

## Verification

- `cargo fmt --check && cargo clippy -- -D warnings && cargo test` after every commit, workspace-wide.
- Each new `db` query/maintenance function gets unit tests against a temp repo (same pattern as `tree.rs`/`backup.rs`'s existing tests) - including the `del` recursive-subtree test and the `reclaim-space` multi-row-FK-safety test called out above.
- Each `cli` command gets at least one end-to-end integration test (temp repo + real files, run the command, assert output/DB/exit-code), same pattern as `store`'s existing `store::tests`.
- Manual smoke test per command against a real small repo once implemented, mirroring how `store` was smoke-tested (build a repo, run the command, inspect results).
