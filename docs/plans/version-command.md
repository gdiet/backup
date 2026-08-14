# `backup version` command + richer schema-mismatch error

**Status**: proposed, not yet implemented.

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
