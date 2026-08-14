# Verify `EMPTY_CONTENT_ID`'s seed row on repository open

**Status**: implemented (2026-08-14). Surfaced by inspecting `backup-repository/`'s actual schema
(not just its `user_version`) at the user's request, following `docs/plans/implemented/
empty-file-real-content-id.md`.

## What was found

`backup-repository/` (the shared local manual-test repository) was created before
`EMPTY_CONTENT_ID`'s seed row existed. Since `SCHEMA_V1` is a single, already-squashed migration
(see its own doc comment in `migrations.rs`), `rusqlite_migration`'s `to_latest` never re-runs it
for a database already at `user_version = 1` - which is every repository ever `init`ed, since
there's only one version. The seed `INSERT INTO contents (id, length, hash) VALUES (1, 0, ...)`
therefore silently never ran against this repository.

Worse than a missing row: `contents.id = 1` in that database was already taken by an ordinary,
pre-existing 101-byte content (from real, earlier test data) - `db::EMPTY_CONTENT_ID` being a
hardcoded `1` means `resolve_content` would confidently return `Ok(1)` for the next empty file,
silently aliasing it onto that unrelated 101-byte content instead of failing loudly. No foreign-key
violation, no error - just wrong data read back later, exactly the kind of bug that goes unnoticed
until someone actually looks.

## Proposal

Check `contents.id = EMPTY_CONTENT_ID` against its expected shape every time a repository is
opened (both `open_repository` and `open_repository_read_only`), right after confirming migrations
are current - cheap (one indexed primary-key lookup) and catches this class of problem immediately
and loudly, in every command, rather than only in whichever operation first happens to create an
empty file.

Three outcomes:
- Row exists with the expected `(length = 0, hash = <the known empty-input hash>)`: fine, proceed
  (the normal case for any repository actually `init`ed with current code).
- Row missing entirely: this repository predates the seed and nothing has collided with `id = 1`
  yet - still unsafe going forward, refuse to open.
- Row exists with a *different* `length`/`hash`: the actual collision case found in
  `backup-repository/` - refuse to open.

A new `Error::EmptyContentSeedMismatch` (or similar) variant, with a message explaining the
repository predates this schema's seed data and pointing at what to do about it (for a disposable
test repository: delete and re-`init`; there is no supported in-place migration for this, since no
real repository is known to exist yet - see `empty-file-real-content-id.md`'s own "Why no migration
was needed" section for that same reasoning applied to this exact case).

## Scope note

This is specifically a way to *detect* the problem loudly at open time, not a migration that fixes
it - deliberately: writing a real migration (insert a *new*, different id for the empty-content
row when `id = 1` is already taken, then repoint every existing `content_id IS NULL`-as-empty row
at it) is a bigger, separate piece of work that only matters once a real repository actually needs
it. Today, every known affected repository is disposable test data (see `empty-file-real-content-id.md`),
so "refuse to open, tell the user to delete and re-`init`" is the right-sized fix.

## Implementation

- `db::EMPTY_CONTENT_HASH` (`db/src/lib.rs`): the expected 20-byte hash as a plain Rust byte
  literal - kept separate from `migrations.rs`'s SQL hex literal (a `const &str` migration string
  can't interpolate a Rust constant), with a doc comment cross-referencing the two so they're
  understood to need to match.
- `verify_empty_content_seed(conn)`: looks up `contents.id = EMPTY_CONTENT_ID`, accepts only
  `(length = 0, hash = EMPTY_CONTENT_HASH)` exactly, otherwise returns the new
  `Error::EmptyContentSeedMismatch`.
- Called from both `open_repository` (after `to_latest`) and `open_repository_read_only` (after
  its own pending-migrations check) - every command that touches a repository goes through one of
  these two, so this can't be bypassed.

Verified: `cargo fmt --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test
--workspace`, `cargo doc --no-deps --workspace` all clean. New tests in `db/src/lib.rs`: a
correctly-`init`ed repository opens cleanly (both open paths); a repository with the seed row
deleted (simulating "predates the seed") is refused by both open paths; a repository with the seed
row present but holding different `length`/`hash` (the actual `backup-repository/` collision,
reproduced directly) is refused just as loudly, not treated as "close enough".
