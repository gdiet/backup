# Usage log: track which features/flags actually get used

**Status**: implemented.

## Motivation

A year from now, being able to see which commands and optional flags were
actually exercised, to spot features nobody ever ended up using and
consider removing them ("feature bloat" avoidance) - not runtime
telemetry, not sent anywhere, purely a local, append-only record for the
repository's own owner to look back at.

## Format: plain, tab-separated text - not JSON

One line per invocation, tab-separated (matching the original "a text
file, one line per feature use" idea directly, rather than inventing more
structure than needed):

```
2026-08-09 14:32:10	restore	deleted,recursive
2026-08-09 14:35:00	mount	read-write,zero-fill-missing
2026-08-09 14:40:00	db backup
```

Columns: human-readable timestamp (reusing `format::format_timestamp_millis`,
already used elsewhere in this codebase - no new date/time dependency),
command (space-joined for nested subcommands, e.g. `db backup`), and a
comma-separated list of *optional* flags/options that were explicitly
passed (empty/omitted third column if none). No JSON: the workspace has no
`serde`/`serde_json` dependency today, and every field here is a
fixed-shape, code-controlled identifier (command names, clap arg ids) -
never arbitrary or user-supplied content - so there's nothing a JSON
library would actually buy over a plain tab-separated line, and hand
building one would mean either adding a new dependency or writing ad hoc
JSON-escaping code for a case that never needs escaping in the first
place.

**Deliberately not logged**: argument *values* (paths, ids, numbers). The
question this answers is "did I ever use `--deleted`", not "which id did I
pass to it" - values are either irrelevant to the feature-usage question
or (for paths) not something that belongs in a log file at all.

## Location: `meta/usage.log`

Alongside `meta/repository.sqlite3` and `meta/backups/` - per-repository,
not global, matching how every other piece of this tool's state is scoped
to a repository. Append-only, never rotated or truncated: even a heavy
user running several commands a day for a year produces at most a few
thousand short lines - negligible size, no reason to add rotation
complexity for a file this small.

## Which flags get logged: a generic mechanism, not per-command lists

Considered and rejected: hand-writing a `used_flags(&self) -> Vec<&str>`
method on every `*Args` struct (~15 of them). Rejected specifically
because the whole point of this feature is *long-term* accuracy - a
hand-maintained list silently goes stale the moment a new flag is added to
some command and its `used_flags` impl isn't updated to match, which is
exactly the kind of drift nobody notices until "in a year" arrives and the
log has been quietly wrong the whole time.

Instead: a single, generic mechanism built on `clap`'s own `ArgMatches`,
which already knows - for *any* command, without per-command code -
whether a given argument was actually present on the command line
(`ArgMatches::value_source(id) == Some(ValueSource::CommandLine)`, as
opposed to a default value). `main.rs` already calls the derive-generated
`Cli::parse()`; this changes that one call site to the two-step
equivalent (`Cli::command().get_matches()` then
`Cli::from_arg_matches(&matches)`, which is what `parse()` already does
internally) so the raw `ArgMatches` stays available afterward:

1. Walk `matches.subcommand()` recursively (handles both the top-level
   `Command` enum and `db`'s own nested `DbCommand`) to find the leaf
   subcommand's name chain (e.g. `["db", "backup"]`) and its `ArgMatches`.
2. Cross-reference against `Cli::command()`'s own `clap::Command` tree
   (walked the same way, `get_subcommands()`) to find which of the leaf's
   arg ids are positional (`Arg::is_positional()`) - these are excluded
   from the log (see "deliberately not logged" above: they're required
   inputs, not optional feature choices, e.g. `restore`'s `paths`).
3. Log every remaining id where `value_source(id) ==
   Some(ValueSource::CommandLine)` - this is generic across every current
   and future command/flag with zero per-command maintenance.

New module `cli/src/usage_log.rs`: `fn log_invocation(repo: &Path,
command_chain: &[&str], leaf_matches: &clap::ArgMatches, leaf_command:
&clap::Command)`, called once from `main()` right after parsing, before
dispatching to the matched command's handler.

## Timing and failure handling

Logged *before* the command runs, not after - so a flag's use is recorded
even if the process is later killed (relevant for `mount`, which can run
for hours/days; a log-after-completion design would silently lose the
entry for any session that doesn't exit cleanly). One exception: `init`
itself, where `meta/` doesn't exist yet at parse time - logged
*after* `db::init_repository` succeeds instead, the one place in this
design where "before" doesn't work.

Entirely best-effort: if the write fails for any reason (repository not
fully initialized yet, permissions, disk full), it's silently ignored -
never surfaces an error, never affects the command's exit code. This is
advisory bookkeeping, not something any command's correctness should ever
depend on.

## Explicitly not included

- **No opt-out flag.** Purely local, never transmitted anywhere, no
  runtime cost worth avoiding - nothing to opt out of. Can be added later
  if that judgment turns out to be wrong in practice.
- **No analysis/reporting command** (e.g. `backup usage-report`). The
  point is a plain-text file a human can `grep`/read directly in a year;
  building a summarizer now would be solving a problem that doesn't exist
  yet.

## One thing found during implementation

`ArgMatches::ids()` (the originally planned iteration source, see "Which
flags get logged" above) turned out to also yield a synthetic id named
after the flattened `Args` struct itself (e.g. `"RestoreArgs"`, from
`Command::Restore(RestoreArgs)`), not a real flag - clap's derive
generates this for enum variants wrapping a single `#[derive(Args)]`
struct. Fixed by iterating `leaf_command.get_arguments()` (the actual,
individually declared `Arg`s in the `Command` tree) instead of
`leaf_matches.ids()`, checking each one's `value_source` - same generic,
no-per-command-maintenance property as originally designed, just sourced
from the right place.

## Verification checklist

- [x] New tests in `cli/src/usage_log.rs`: a logged invocation appends
  exactly one well-formed line; positional args are excluded; a flag left
  at its default is excluded; an explicitly-passed flag is included; a
  missing `meta/` directory doesn't error, it's silently skipped.
- [x] Spot-checked real commands (`init`, `stats`, `db backup`, `store`,
  `restore --overwrite`) against a throwaway repository - each produced
  exactly the expected line, including the nested `db backup` chain and
  `restore`'s correctly-captured `overwrite` flag.
- [x] `cargo fmt --check && cargo clippy --workspace --all-targets -- -D
  warnings && cargo test --workspace && cargo doc --no-deps --workspace`.
- [x] Updated `README.md`.
