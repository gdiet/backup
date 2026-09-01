# Implement the read-only DB connection path DESIGN-METADATA-003 anticipates

**Why parked**: surfaced during the 2026-09-01 cross-boundary investigation (see
`agent-todos/network-fs-sqlite-reliability-docs.md`); it is a medium/large `crates/db`-core change,
and the developer wanted it captured as a TODO to weigh separately.
**Size**: medium/large - confirm scope with the developer before starting. Touches `crates/db`'s
connection model, which every other crate goes through.
**Opened**: 2026-09-01, native-Windows session on `3327`
**Context**: `crates/db/src/connection.rs` - `configure_write_connection`'s own doc comment already
spells out the intent: "A future read-only connection (once reads split off their own, per
DESIGN-METADATA-003) needs none of this except `busy_timeout` - the rest either only matters for
writes, or (for `journal_mode`) is a persistent, whole-database property already set once here."
`docs/design/metadata-storage.md` DESIGN-METADATA-003 ("reads and writes still share one
connection per Repository") and its note (~line 626) that a `SQLITE_OPEN_READ_ONLY` connection
cannot issue `PRAGMA journal_mode = WAL` even as a no-op. `crates/db/src/lib.rs` `Repository` (one
`Mutex<Connection>` today) and `open_repository`.

## Why now / what it buys

Every `dfs` command today - including read-only ones (`unlock`'s guard, `mount` without
`--read-write`) - opens a full write-mode connection: `PRAGMA journal_mode=WAL`,
`auto_vacuum=INCREMENTAL`, `foreign_keys=ON`, `synchronous=NORMAL`, plus running pending
migrations. On a filesystem where a WAL write-open fails (observed: WSL<->Windows 9p bridges ->
`database is locked` / `disk I/O error`), read-only operations fail even though they never needed
to write. A read-only connection path would let read-only `mount` (and any future read-only
tooling) work against such a repo.

## Sketch

- A read-only open: `Connection::open_with_flags(db_path, SQLITE_OPEN_READ_ONLY)`, then set only
  `busy_timeout`. Do **not** touch `journal_mode` / `auto_vacuum` / `foreign_keys` /
  `synchronous`. Do **not** run migrations (a read-only caller must not migrate; decide whether a
  schema-version mismatch on a read-only open is an error or is tolerated - probably error with an
  actionable "open once read-write to migrate" message).
- `Repository` gains a read-only construction path (`db::open_repository_read_only` or a flag),
  and read-side methods use it. Given the current single-`Mutex<Connection>` model, the smallest
  step is a separate `open_repository_read_only` returning a `Repository` whose connection is
  read-only, used by `mount` when `!read_write`; a fuller reads-split-from-writes design is the
  DESIGN-METADATA-003 end state.
- `crates/cli/src/mount.rs`: use the read-only open when `!read_write` (it already only acquires
  the `flock` write lock when `read_write`).
- Interaction with `agent-todos/unlock-should-not-open-the-db.md`: that one should still drop the
  DB open entirely for `unlock` (needs to work even when the DB is unreadable) - a read-only open
  is not enough there.
- Update DESIGN-METADATA-003 (and the `configure_write_connection` doc comment) once the split
  exists.

## Done

**Completed**: 2026-09-01, by Claude Code on the web session (branch `mount-read-write`) - the
developer confirmed this environment was fine for the change (a `crates/db`-core Rust change, no
special hardware needed) and gave the go-ahead directly.

Implemented per the sketch above, with two decisions the sketch left open:

- **Schema-version mismatch on a read-only open**: an error, not tolerated - a new
  `Error::SchemaNeedsMigration(PathBuf)`, checked via
  `rusqlite_migration::Migrations::pending_migrations` (itself just a `PRAGMA user_version` read,
  safe read-only) before ever reading `repository_settings`. Message points at opening the
  repository once with a write-capable operation first.
- **Mutation refusal**: `Repository` gained a `read_only: bool` field; `with_transaction` - the
  one choke point every mutating method already goes through - checks it first and returns a new
  `Error::ReadOnlyRepository` before ever touching the connection, rather than letting each write
  method rediscover SQLite's own `SQLITE_READONLY` independently.

`crates/db/src/connection.rs` gained `configure_read_only_connection` (sets only `busy_timeout`);
`crates/db/src/lib.rs` gained `open_repository_read_only` (`SQLITE_OPEN_READ_ONLY` +
`SQLITE_OPEN_NO_MUTEX` + `SQLITE_OPEN_URI`, matching `Connection::open`'s own default flags minus
`READ_WRITE`/`CREATE`) and a small `read_settings` helper factored out of `open_repository` to
avoid duplicating the `repository_settings` query between the two. `crates/cli/src/mount.rs`
dispatches to `open_repository_read_only` when `!read_write`, `open_repository` otherwise;
`crates/cli/src/dedup_fs.rs`'s `to_errno` gained arms for the two new `db::Error` variants
(`ReadOnlyRepository` -> `Errno::EROFS`, `SchemaNeedsMigration` folded into the existing
never-actually-reached catch-all, since `mount::try_run` fails at DB-open before `DedupFs` exists
either way).

One correction to this TODO's own citation, found via a throwaway empirical probe (per
`AGENTS.md`'s debugging discipline) before writing the real code: a `SQLITE_OPEN_READ_ONLY`
connection can actually still issue `PRAGMA journal_mode = WAL` successfully as a no-op
re-assertion when the database is already in WAL mode - the "cannot issue... even as a no-op"
claim in `configure_write_connection`'s old doc comment and DESIGN-METADATA-003's own text was
wrong. Doesn't change the outcome (`open_repository_read_only` still doesn't attempt it - nothing
to gain, every repository this crate creates is already durably WAL from `init_repository`
onward), but the doc comment and design doc both now state the corrected reasoning instead of
repeating the disproven claim.

Verified: 5 new `db`-crate tests (a fresh read-only open reading settings/the tree correctly, the
no-repository-here case, and the two red/green-verified critical paths - mutation refusal and the
schema-version check, each confirmed to actually fail its test when temporarily disabled, not just
pass vacuously). Full workspace suite green (`cargo build`/`fmt`/`clippy`/`test`/`doc`). Manually
smoke-tested the real `dfs` binary: a read-only mount against a normal repository reaches
`mountfs::preflight()` exactly as before (fails only on this container's missing `libfuse3`, an
unrelated environment limitation); a read-only mount against a deliberately unmigrated
`meta/repository.sqlite3` produces the actual actionable message end-to-end
(`error: the repository at ... needs a schema migration, which a read-only open cannot perform -
open it once with a write-capable operation (e.g. \`dfs mount --read-write\`) first`); a
`--read-write` mount against a normal repository still reaches preflight unaffected.

`docs/design/metadata-storage.md`'s DESIGN-METADATA-012 "Not yet built: a lighter configuration for
a genuinely read-only connection" subsection rewritten as "A lighter configuration..." with
`Status: implemented`, describing what actually shipped (including the corrected WAL-reassertion
finding) instead of the prior speculative sketch.

Not done, deliberately out of scope for this pass: `dedup_fs.rs`'s own test-setup helper
(`setup(read_write: bool)`) still opens a write-mode `Repository` for both its read-only and
read-write test scenarios - left as-is, since its `read_write=false` tests exercise `DedupFs`'s
own `require_read_write()` guard (which refuses before ever reaching `Repository`), not anything
`open_repository_read_only`-specific; the read-only-connection behavior itself is already covered
at the `db`-crate level above.
