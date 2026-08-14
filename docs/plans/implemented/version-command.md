# `backup version` command + richer schema-mismatch error

**Status**: implemented (2026-08-14).

## What was actually built, and where it differed from the plan

Followed the plan below closely; a few concrete deviations, all found while implementing rather
than anticipated:

- **Commit date uses `git show -s --format=%cs`** (short `YYYY-MM-DD`), not the plan's `%cI` (full
  ISO 8601 with time-of-day/offset). A time-of-day doesn't add anything actionable for "which
  commit is this" and just makes the one-line summary noisier - `%cs` alone already matches what
  VSCode/IntelliJ show.
- **No accessor was added to `rusqlite_migration::Migrations`** - it has no public way to ask "how
  many migrations do you have" (checked its actual source, not assumed). Used a separate, hand-
  maintained `pub const CURRENT_SCHEMA_VERSION: usize = 1` in `migrations.rs` instead (re-exported
  from `db/src/lib.rs`), documented as needing a manual bump alongside `migrations()` if a second
  migration is ever appended - acceptable since that's already a rare, deliberate event here (see
  `SCHEMA_V1`'s own doc comment on why there's only ever been one migration).
- **Found and fixed a real bug before it shipped**: naively wiring `Cli::command().version(...)` to
  the same `"backup 0.1.0 (...)"` string `version` prints doubled the app name - clap's `-V`/
  `--version` already prepends the command's own name (`backup`) to whatever `.version(...)` is set
  to. Split into `version_number()` (no `backup` prefix, what `.version()` gets) and `version_line()`
  (`format!("backup {}", version_number())`, what `version`'s own first `println!` uses) - both
  end up showing `backup 0.1.0 (857c46c5, 2026-08-14)`, just built from opposite ends.
- **Wired via the builder call `Cli::command().version(...)`**, not the plan's `#[command(version =
  ...)]` derive attribute - that attribute needs a literal/const expression, not a value assembled
  at runtime via `format!`; `main.rs` already builds `Cli::command()` explicitly as a two-step
  equivalent of `Cli::parse()` (for `usage_log`'s benefit), so there was already a natural place to
  chain `.version(...)` onto.
- `Command::version` needs a `&'static str`; `env!(...)`-only values would have been fine as
  `&'static str` directly, but the version string is assembled at runtime via `format!`, so it's an
  owned `String` - resolved with a one-time, deliberate `Box::leak`, harmless for a value computed
  once per process at startup.
- The dirty-working-tree flag noted as an open, non-blocking question below was left out - not
  revisited, no new information changed the original "nice-to-have, not needed" assessment.

Verification: `cargo fmt --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo
test --workspace`, `cargo doc --no-deps --workspace` all clean. New tests: `cli::version::tests`
(the version line includes `CARGO_PKG_VERSION`; the repository-schema line reports the actual
version for a freshly initialized repo, is best-effort/`unavailable` for a missing repo, and reports
both numbers side by side when the schema is too new); `db`'s two existing
`..._when_the_schema_is_too_new` tests extended to also assert `supported_version:
CURRENT_SCHEMA_VERSION`. Manually ran `backup version`, `backup --version`, `backup -V` against this
checkout.

---

**Original plan follows.**

## Motivation

There's currently no way to see, from a built binary, exactly which commit it was built from -
useful when comparing behavior across machines/checkouts (this project is routinely worked on from
several environments, see `AGENTS.md`'s "Working Across Environments"), and when a repository's
schema is incompatible with the binary trying to open it (`Error::SchemaTooNew`, see below) - the
current error message doesn't say what version *would* work, just that this one doesn't.

## Version info to show

- App version: already `0.1.0` (`CARGO_PKG_VERSION`, set in every crate's `Cargo.toml`) - no change
  needed, matches this project's actual pre-1.0 status.
- Git commit: short hash, 8 hex characters (`git rev-parse --short=8 HEAD`) - not git's own default
  of 7, chosen to match what this project's actual editors show (VSCode, IntelliJ both show 8).
- Commit date, not build date: `git show -s --format=%cI HEAD` (the commit's own timestamp, ISO
  8601) rather than "when this binary happened to be compiled" - two builds of the same commit
  should report the same date; a build-timestamp doesn't add information the hash doesn't already
  pin down, and actively hurts reproducible builds (two identical builds would otherwise differ).

Open question, not blocking: whether to also detect and flag a dirty working tree at build time
(`git status --porcelain`, e.g. append `-dirty` to the version string) - useful for catching "this
isn't actually what's in the commit" during local dev builds. Cheap to add alongside the rest if
wanted; leaving as a nice-to-have rather than deciding now.

## Mechanism: `cli/build.rs`, no new dependency

A new `cli/build.rs` shells out to `git` via `std::process::Command` (`rev-parse --short=8 HEAD`,
`show -s --format=%cI HEAD`) and emits `cargo:rustc-env=BACKUP_GIT_HASH=...` /
`cargo:rustc-env=BACKUP_GIT_COMMIT_DATE=...`, consumed in `cli` via `env!(...)` at compile time.
Considered and rejected: `vergen`/`shadow-rs` - both pull in more than this needs (`vergen`'s git
support specifically means either `vergen-git2`, a `libgit2`/C dependency this cross-platform
(Windows/WinFSP) workspace doesn't otherwise have, or `vergen-gitcl`, which does the same subprocess
call this `build.rs` does directly, just behind another crate). Two values, hand-rolled, no
dependency addition needed - fits this project's default stance on dependencies.

Must degrade gracefully (empty/`"unknown"` string, not a build failure) if `git` isn't on `PATH` or
`.git` isn't present - e.g. a build from a source tarball without history.

## `backup version` subcommand

New `Command::Version` variant (`cli/src/version.rs` or inline in `main.rs`, depending on eventual
size). Prints:
- App version + git hash + commit date (the one-line summary).
- The schema version this build supports (`db`'s own `migrations::migrations()` already knows
  this - see below, needs a small `pub` accessor).
- If `-r`/`--repo` resolves to an already-initialized repository (this arg is already `global =
  true` on `Cli`, so always available): that repository's actual current schema version too, for a
  direct side-by-side comparison. Best-effort - a missing/uninitialized repo at the given path
  shouldn't make `version` fail, just omit that line.

**Decided**: `clap::Parser`'s derive already wires up a bare `-V`/`--version` flag automatically
(currently just prints `CARGO_PKG_VERSION`, i.e. `backup 0.1.0`) - upgrade it via `#[command(version
= ...)]` to show the same one-line hash+date summary as `version`'s first line, so both spellings
agree (avoids two different answers to "what version is this" depending on which flag/command is
used).

## Schema-mismatch error: include what's actually supported

`Error::SchemaTooNew { db_version }` (`db/src/error.rs`) currently renders as:

```
this repository's database schema (version {db_version}) is newer than this version of `backup`
understands - please update `backup`
```

which never says *which* version would actually work. Fix is `db`-internal, no dependency on the
new `cli` build-info: `reject_if_schema_too_new` (`db/src/lib.rs`) already calls
`migrations::migrations()` to get `db_version` - the same call can report the max version this
build's `Migrations` actually knows about (currently `1`, i.e. `SCHEMA_V1`'s count). Add that as a
second field on `SchemaTooNew` (`db_version: usize, supported_version: usize`), and reword:

```
this repository's database schema (version {db_version}) is newer than what this build of `backup`
supports (schema version {supported_version}) - please update `backup`, or run `backup version` for
details
```

Doesn't need `cli`'s build-time git info at all - dependency direction is `cli` → `db`, not the
other way, so `db`'s own error type can't reach into `cli`'s `env!()` constants regardless; pointing
at `backup version` in the message is how a user gets from here to the hash/date if they want it.

## Verification plan

- `db`: new/updated test for `SchemaTooNew`'s message asserting both numbers appear (mirrors the
  existing `open_repository_also_fails_with_the_actual_version_when_the_schema_is_too_new`-style
  tests already in `db/src/lib.rs`).
- `cli`: a test invoking `version` and checking the output contains version/hash/date and the
  supported-schema line; one with `-r` pointed at a real initialized repo checking the repo's own
  schema-version line appears too.
- Full suite as usual before proposing a commit (`cargo fmt --check && cargo clippy --workspace
  --all-targets -- -D warnings && cargo test --workspace && cargo doc --no-deps --workspace`).
- Manually run `backup version` and `backup --version` once built, on at least this environment.
