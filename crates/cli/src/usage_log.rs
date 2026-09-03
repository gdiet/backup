//! Per-repository usage log - REQ-OPERABILITY-005, DESIGN-CLI-005 in `docs/design/usage-log.md`.
//! Appends one line per CLI invocation to `meta/usage.log`: timestamp, the invoked (sub)command's
//! name, and which optional flags were explicitly passed on the command line - never argument
//! values. Purely local, best-effort, never read back by this project's own code.

use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

use clap::{ArgMatches, Command, parser::ValueSource};

use crate::time_format::format_time;

const FILE_NAME: &str = "usage.log";

/// Appends one line to `meta_dir`'s usage log for one CLI invocation. `top` is the whole `Cli`
/// command's own [`Command`] tree (`Cli::command()`); `matches` its parsed [`ArgMatches`].
/// Silently does nothing if the write fails (repository not yet initialized, permissions, disk
/// full) - this is best-effort bookkeeping, never something a command's own success should depend
/// on.
pub fn log_invocation(meta_dir: &Path, top: &Command, matches: &ArgMatches, time_millis: i64) {
    let Some((name, leaf_command, leaf_matches)) = leaf(top, matches) else {
        return;
    };
    let flags = used_flags(leaf_command, leaf_matches).join(",");
    let line = format!("{}\t{name}\t{flags}\n", format_time(time_millis));

    let Ok(mut file) = OpenOptions::new()
        .create(true)
        .append(true)
        .open(meta_dir.join(FILE_NAME))
    else {
        return;
    };
    let _ = file.write_all(line.as_bytes());
}

/// Walks `matches`'s subcommand chain against `command`'s own tree, returning the leaf
/// subcommand's own name plus its [`Command`]/[`ArgMatches`] - `None` if `matches` names no
/// subcommand at all, or one `command` does not have (the latter should not happen for `matches`
/// actually produced by parsing against `command`). Only one level deep in this project's current
/// command set, but written as a walk - not just one `matches.subcommand()` call - so a future
/// nested subcommand (`db backup`-shaped, not present today) does not silently stop being logged
/// correctly.
fn leaf<'a>(
    command: &'a Command,
    matches: &'a ArgMatches,
) -> Option<(String, &'a Command, &'a ArgMatches)> {
    let mut current_command = command;
    let mut current_matches = matches;
    let mut name = None;
    while let Some((sub_name, sub_matches)) = current_matches.subcommand() {
        current_command = current_command
            .get_subcommands()
            .find(|c| c.get_name() == sub_name)?;
        current_matches = sub_matches;
        name = Some(match name {
            Some(prefix) => format!("{prefix} {sub_name}"),
            None => sub_name.to_string(),
        });
    }
    Some((name?, current_command, current_matches))
}

/// The optional flags/options explicitly passed on the command line for one leaf subcommand -
/// every declared, non-positional argument whose [`ArgMatches::value_source`] is
/// [`ValueSource::CommandLine`] (not a default value). Derived generically from `clap`'s own
/// `Command`/`ArgMatches`, rather than a hand-maintained per-command list, so a newly added flag
/// is covered automatically instead of the log silently going stale the moment someone forgets to
/// update a list kept in sync by hand.
fn used_flags(command: &Command, matches: &ArgMatches) -> Vec<String> {
    command
        .get_arguments()
        .filter(|arg| !arg.is_positional())
        .filter(|arg| matches.value_source(arg.get_id().as_str()) == Some(ValueSource::CommandLine))
        .map(|arg| arg.get_id().to_string())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::{CommandFactory, Parser, Subcommand};

    #[derive(Parser)]
    struct TestCli {
        #[command(subcommand)]
        command: TestCommands,
    }

    #[derive(Subcommand)]
    enum TestCommands {
        Restore {
            #[arg(long)]
            repo: Option<String>,
            #[arg(long)]
            overwrite: bool,
            #[arg(long)]
            verify: bool,
            path: String,
        },
    }

    fn parse(args: &[&str]) -> (Command, ArgMatches) {
        let top = TestCli::command();
        let matches = TestCli::command().get_matches_from(args);
        (top, matches)
    }

    #[test]
    fn logs_a_well_formed_line_with_the_command_name_and_no_flags() {
        let (top, matches) = parse(&["dfs", "restore", "a-path"]);
        let dir = tempfile::tempdir().unwrap();
        log_invocation(dir.path(), &top, &matches, 1_700_000_000_000);

        let contents = std::fs::read_to_string(dir.path().join(FILE_NAME)).unwrap();
        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(lines.len(), 1);
        let fields: Vec<&str> = lines[0].split('\t').collect();
        assert_eq!(fields.len(), 3);
        assert_eq!(fields[1], "restore");
        assert_eq!(fields[2], "", "no optional flag was passed");
    }

    #[test]
    fn positional_arguments_are_never_logged() {
        let (top, matches) = parse(&["dfs", "restore", "a-secret-path"]);
        let dir = tempfile::tempdir().unwrap();
        log_invocation(dir.path(), &top, &matches, 1_700_000_000_000);

        let contents = std::fs::read_to_string(dir.path().join(FILE_NAME)).unwrap();
        assert!(!contents.contains("a-secret-path"));
    }

    #[test]
    fn a_flag_left_at_its_default_is_not_logged() {
        let (top, matches) = parse(&["dfs", "restore", "--verify", "a-path"]);
        let dir = tempfile::tempdir().unwrap();
        log_invocation(dir.path(), &top, &matches, 1_700_000_000_000);

        let contents = std::fs::read_to_string(dir.path().join(FILE_NAME)).unwrap();
        assert!(!contents.contains("overwrite"));
    }

    #[test]
    fn an_explicitly_passed_flag_is_logged_by_its_own_id_never_its_value() {
        let (top, matches) = parse(&[
            "dfs",
            "restore",
            "--overwrite",
            "--repo",
            "/some/secret/path",
            "a-path",
        ]);
        let dir = tempfile::tempdir().unwrap();
        log_invocation(dir.path(), &top, &matches, 1_700_000_000_000);

        let contents = std::fs::read_to_string(dir.path().join(FILE_NAME)).unwrap();
        assert!(contents.contains("overwrite"));
        assert!(contents.contains("repo"));
        assert!(
            !contents.contains("/some/secret/path"),
            "an option's value must never be logged, only that it was passed"
        );
    }

    #[test]
    fn a_missing_meta_dir_is_silently_ignored() {
        let (top, matches) = parse(&["dfs", "restore", "a-path"]);
        let dir = tempfile::tempdir().unwrap();
        // Never created - simulates a repository that does not exist (yet).
        let missing = dir.path().join("does-not-exist");
        log_invocation(&missing, &top, &matches, 1_700_000_000_000);
        assert!(!missing.exists());
    }

    #[test]
    fn appending_more_than_one_invocation_keeps_every_line() {
        let dir = tempfile::tempdir().unwrap();
        for _ in 0..3 {
            let (top, matches) = parse(&["dfs", "restore", "a-path"]);
            log_invocation(dir.path(), &top, &matches, 1_700_000_000_000);
        }
        let contents = std::fs::read_to_string(dir.path().join(FILE_NAME)).unwrap();
        assert_eq!(contents.lines().count(), 3);
    }
}
