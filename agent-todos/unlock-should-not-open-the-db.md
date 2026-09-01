# `dfs unlock` does a full write-mode DB open just as a "is this a repo?" guard

**Why parked**: found during the 2026-09-01 cross-boundary investigation (see
`agent-todos/network-fs-sqlite-reliability-docs.md`); developer wanted a TODO.
**Size**: small (self-contained; a lighter guard in one function)
**Opened**: 2026-09-01, native-Windows session on `3327`
**Context**: `crates/cli/src/unlock.rs` `try_run` - it calls `db::open_repository(repo_path)` and
immediately discards the result (`Ok(_repo) => {}`), purely as a guard, before calling
`db::unlock_stale_write_lock(repo_path)` (the actual `flock`-based stale-lock check,
DESIGN-MAINTENANCE-003 in `docs/design/repository-locking.md`). `db::open_repository`
(`crates/db/src/lib.rs` ~446) runs `configure_write_connection` (WAL pragma etc.) **and**
`migrations::migrations().to_latest(&mut conn)`.

## The problem

`unlock`'s job is a pure lock-file operation: check whether the write-lock file is genuinely stale
(no process holds the `flock`) and clear it if so. It does not need the database contents at all.
But the `open_repository` guard means `unlock`:

- runs schema migrations as a side effect of a maintenance command that has nothing to do with
  schema;
- **fails outright with `error: database is locked` / `error: disk I/O error` on any filesystem
  where a WAL write-open fails** (observed over WSL<->Windows 9p bridges) - which is exactly the
  situation where a user would reach for `dfs unlock` to recover a repo that seems stuck.

## Fix

Replace the `open_repository` guard in `unlock::try_run` with a lightweight existence check -
enough to answer "is this a repository directory?" without opening the DB. Options: check that
`repo_root/meta/` is a directory and `repo_root/meta/repository.sqlite3` exists (mirrors what
`open_repository` itself checks first for `NoRepositoryHere`), or add a `db` helper like
`db::looks_like_repository(path) -> bool` / a cheap `Error::NoRepositoryHere` check that does not
open a connection. Keep the existing actionable "no repository found at the default location ...
pass a path explicitly" message for the default-path case.

Note: if `agent-todos/read-only-db-connection-path.md` lands first, an even lighter option is a
read-only open - but a no-DB-open guard is strictly better here, since `unlock` should work even
when the DB file itself is inaccessible.
