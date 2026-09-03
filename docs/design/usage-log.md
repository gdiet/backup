# Usage Log

## DESIGN-CLI-005: A generic, per-repository log of which commands and flags actually get used

Status: implemented

REQ-MAINTENANCE-005 in
[`../../requirements/functional/maintenance.md`](../../requirements/functional/maintenance.md)
wants a local, append-only record of which commands and optional flags actually get exercised, to
make an informed call later about removing something nobody uses. `crates/cli/src/usage_log.rs`
appends one line per invocation to `meta/usage.log`, alongside the repository's own metadata
database: a tab-separated timestamp, the invoked (sub)command's name, and a comma-separated list of
which optional flags were explicitly passed on the command line - never argument values (a path, an
id, a chunk-size number). The question this answers is "was `--reference` ever used", not "what was
passed to it" - a value belongs in this record even less than in most logs, since several of this
project's own flags take a real filesystem path as their value.

Kept per-repository rather than global, matching how every other piece of this tool's state is
scoped to a repository (REQ-OPERABILITY-002's mirrorability, in particular, already assumes nothing
relevant lives outside a repository's own directory). Entirely local and never transmitted anywhere
- there is nothing to opt out of, so no opt-out flag exists. No analysis or reporting command reads
this file back; the entire point is a plain-text file a human reads directly, sometime after enough
history has accumulated for the question to be worth asking - building a summarizer ahead of that
would be solving a problem that does not exist yet.

### Format: plain tab-separated text, not JSON

```
2026-09-03T14:32:10Z	restore	verify,best_effort
2026-09-03T14:35:00Z	mount	read_write
2026-09-03T14:40:00Z	ingest	reference
```

(The flag list uses each argument's `clap` id - its Rust field name, e.g. `best_effort` - rather
than the hyphenated `--best-effort` spelling a caller actually types; `clap` derives the two
independently, and the id is what `ArgMatches`/`Command` expose generically.)

No JSON: the workspace carries no `serde`/`serde_json` dependency today, and every field here is a
fixed-shape, code-controlled identifier (a command name, a `clap` argument id) - never arbitrary or
user-supplied content that would need escaping. Adding a dependency, or hand-rolling JSON-escaping
logic that never actually triggers, would buy nothing a plain tab-separated line does not already
give a human reading the file directly. The timestamp reuses `crate::time_format::format_time`
(shared with `dfs list`'s own directory listing) rather than a new formatting choice.

### Which flags get logged: derived from `clap`'s own `Command`/`ArgMatches`, not a hand-written list

The alternative - a hand-written `used_flags(&self) -> Vec<&str>` on each command's own argument
struct - was considered and rejected. This log's entire value is *long-term* accuracy: a
hand-maintained list silently drifts the moment a new flag is added somewhere and its entry is
forgotten, exactly the kind of gap nobody notices until the log is actually consulted, at which
point it has been quietly wrong for however long the gap existed.

Instead, `log_invocation` walks the `clap::Command` tree `Cli::command()` already produces
(`CommandFactory`, from the `derive(Parser)` already in place) against the `ArgMatches` the same
parse produced, and logs every declared, non-positional argument whose
`ArgMatches::value_source(id) == Some(ValueSource::CommandLine)` - present on the command line,
not merely defaulted. This is generic across every current and future command and flag, with no
per-command code to keep in sync. `main`'s own parse call becomes the two-step form
`Cli::command().get_matches()` then `Cli::from_arg_matches(&matches)` (what `Parser::parse()` does
internally) purely so `matches` stays available afterward for this walk; dispatch on the resulting
`Cli` value is unchanged.

This project's command set is flat today (no nested subcommand like a hypothetical `db backup`),
so a single `matches.subcommand()` call would suffice for now. `log_invocation` instead walks
however many subcommand levels are actually present, so a future nested subcommand does not
silently stop being logged correctly the day it is added - the same "must not silently go stale"
property the whole design exists for, applied to its own subcommand handling, not just its flag
list.

### Timing: before the command runs, except repository creation

Logged before dispatching to the matched command's own handler, so a flag's use is captured even if
the process is later killed - relevant for `dfs mount`, which can run for hours. One exception:
`dfs create-repo`, where `meta/` (the log's own location) does not exist until the repository does
- logged only once `db::init_repository` has actually succeeded, the one case where "before" is not
available at all.

### Failure handling: entirely best-effort

A write that fails for any reason (the repository does not exist yet for a command other than
`create-repo`, permissions, a full disk) is silently ignored - never surfaces an error, never
affects the invoked command's own exit code. This is advisory bookkeeping a command's correctness
must never depend on.
