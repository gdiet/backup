//! Per-repository usage log (`meta/usage.log`, see
//! `docs/plans/implemented/usage-log.md`): one tab-separated line per
//! invocation recording which command and which *optional* flags were
//! explicitly used - not their values, not positional arguments - so a
//! year from now it's possible to see which features actually get used
//! and which never do. Purely local, append-only, best-effort: nothing
//! here is allowed to fail a command or print anything on its own.

use std::collections::HashSet;
use std::fs::OpenOptions;
use std::io::Write as _;
use std::path::Path;

use clap::{ArgMatches, Command, parser::ValueSource};

use crate::format::format_timestamp_millis;

/// Global/uninteresting arg ids excluded from every log line even though
/// they're technically present in every leaf `ArgMatches` (a `global =
/// true` arg like `--repo` is injected into every subcommand's matches by
/// clap) - `--repo`'s *value* is a path we deliberately never log, and its
/// mere presence doesn't represent a discrete feature choice the way
/// `--recursive`/`--deleted` do.
const EXCLUDED_IDS: &[&str] = &["repo", "help", "version"];

/// Finds the leaf subcommand actually invoked, walking both the built
/// `Command` tree and the parsed `ArgMatches` in lockstep (handles
/// arbitrary nesting depth - today just `db backup`/`db restore`/
/// `db compact`, but this doesn't hardcode that). Returns the dot-free
/// name chain (e.g. `["db", "backup"]`) plus the leaf's own `Command`
/// (for positional-arg introspection) and `ArgMatches` (for value
/// sources).
fn find_leaf<'a>(
    command: &'a Command,
    matches: &'a ArgMatches,
) -> (Vec<String>, &'a Command, &'a ArgMatches) {
    let mut names = Vec::new();
    let mut current_command = command;
    let mut current_matches = matches;
    while let Some((name, sub_matches)) = current_matches.subcommand() {
        names.push(name.to_string());
        current_command = current_command
            .get_subcommands()
            .find(|c| c.get_name() == name)
            .expect("a matched subcommand name must exist in the Command tree it was matched from");
        current_matches = sub_matches;
    }
    (names, current_command, current_matches)
}

/// Every optional flag/option explicitly passed on the command line for
/// the leaf subcommand actually invoked - excludes positional arguments
/// (required inputs, not feature choices - e.g. `restore`'s source paths)
/// and [`EXCLUDED_IDS`]. Generic across every current and future command:
/// nothing here needs updating when a new flag is added anywhere.
///
/// Iterates `leaf_command.get_arguments()` (the real, individually
/// declared args) rather than `leaf_matches.ids()`: for an enum variant
/// wrapping a single `#[derive(Args)]` struct (this codebase's usual
/// shape, e.g. `Command::Restore(RestoreArgs)`), clap's derive also
/// surfaces a synthetic group id named after the struct itself (e.g.
/// `"RestoreArgs"`) in `ids()` - not a real flag, and no `Arg` exists for
/// it in the `Command` tree, so this approach naturally excludes it
/// without needing to know that detail explicitly.
fn used_flags(leaf_command: &Command, leaf_matches: &ArgMatches) -> Vec<String> {
    let excluded: HashSet<&str> = EXCLUDED_IDS.iter().copied().collect();
    let mut flags: Vec<String> = leaf_command
        .get_arguments()
        .filter(|arg| !arg.is_positional())
        .map(|arg| arg.get_id().as_str())
        .filter(|id| !excluded.contains(id))
        .filter(|id| leaf_matches.value_source(id) == Some(ValueSource::CommandLine))
        .map(str::to_string)
        .collect();
    flags.sort();
    flags
}

/// Appends one line to `<repo>/meta/usage.log` for the command actually
/// invoked (found from `command`/`matches`, see [`find_leaf`]). Silently
/// does nothing if that fails for any reason (repository not fully set up
/// yet, permissions, disk full) - this is advisory bookkeeping, not
/// something any command's correctness should depend on.
pub fn log_invocation(repo: &Path, command: &Command, matches: &ArgMatches) {
    let (names, leaf_command, leaf_matches) = find_leaf(command, matches);
    if names.is_empty() {
        return;
    }
    let flags = used_flags(leaf_command, leaf_matches);
    let _ = append_line(repo, &names.join(" "), &flags);
}

fn append_line(repo: &Path, command_chain: &str, flags: &[String]) -> std::io::Result<()> {
    let log_path = db::meta_dir(repo).join("usage.log");
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)?;
    writeln!(
        file,
        "{}\t{command_chain}\t{}",
        format_timestamp_millis(now_millis()),
        flags.join(","),
    )
}

fn now_millis() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::{CommandFactory, Parser};

    #[derive(Parser)]
    struct TestCli {
        #[arg(short = 'r', long, default_value = "repo", global = true)]
        repo: String,
        #[command(subcommand)]
        command: TestCommand,
    }

    #[derive(clap::Subcommand)]
    enum TestCommand {
        Restore(RestoreArgs),
        Db(DbArgs),
    }

    #[derive(clap::Args)]
    struct RestoreArgs {
        #[arg(long)]
        overwrite: bool,
        #[arg(long)]
        deleted: Option<i64>,
        paths: Vec<String>,
    }

    #[derive(clap::Args)]
    struct DbArgs {
        #[command(subcommand)]
        command: DbSubcommand,
    }

    #[derive(clap::Subcommand)]
    enum DbSubcommand {
        Backup,
    }

    fn parse(args: &[&str]) -> (Command, ArgMatches) {
        let command = TestCli::command();
        let matches = command.clone().get_matches_from(args);
        (command, matches)
    }

    #[test]
    fn excludes_positionals_and_global_repo_but_includes_explicit_flags() {
        let (command, matches) = parse(&[
            "backup",
            "--repo",
            "somewhere",
            "restore",
            "--overwrite",
            "--deleted",
            "5",
            "a.txt",
        ]);
        let (names, leaf_command, leaf_matches) = find_leaf(&command, &matches);
        assert_eq!(names, vec!["restore"]);
        let flags = used_flags(leaf_command, leaf_matches);
        assert_eq!(flags, vec!["deleted", "overwrite"]);
    }

    #[test]
    fn excludes_a_flag_left_at_its_default() {
        let (command, matches) = parse(&["backup", "restore", "a.txt"]);
        let (_, leaf_command, leaf_matches) = find_leaf(&command, &matches);
        assert_eq!(used_flags(leaf_command, leaf_matches), Vec::<String>::new());
    }

    #[test]
    fn walks_nested_subcommands_to_the_leaf() {
        let (command, matches) = parse(&["backup", "db", "backup"]);
        let (names, leaf_command, leaf_matches) = find_leaf(&command, &matches);
        assert_eq!(names, vec!["db", "backup"]);
        assert_eq!(used_flags(leaf_command, leaf_matches), Vec::<String>::new());
    }

    #[test]
    fn log_invocation_appends_a_well_formed_line_and_is_best_effort_on_a_missing_repo() {
        let temp_dir = tempfile::tempdir().unwrap();
        let repo_root = temp_dir.path().join("repo");
        db::init_repository(
            &repo_root,
            &db::RepositorySettings::new(12, db::Chunking::Cdc).unwrap(),
        )
        .unwrap();

        let (command, matches) = parse(&["backup", "restore", "--overwrite", "a.txt"]);
        log_invocation(&repo_root, &command, &matches);

        let log_path = db::meta_dir(&repo_root).join("usage.log");
        let contents = std::fs::read_to_string(&log_path).unwrap();
        let line = contents.trim_end();
        let parts: Vec<&str> = line.split('\t').collect();
        assert_eq!(parts.len(), 3);
        assert_eq!(parts[1], "restore");
        assert_eq!(parts[2], "overwrite");

        // A second invocation appends, doesn't overwrite.
        let (command, matches) = parse(&["backup", "db", "backup"]);
        log_invocation(&repo_root, &command, &matches);
        let contents = std::fs::read_to_string(&log_path).unwrap();
        assert_eq!(contents.lines().count(), 2);

        // A repository that was never initialized: no meta/ dir, no panic,
        // no line written anywhere.
        let missing_repo = temp_dir.path().join("does-not-exist");
        let (command, matches) = parse(&["backup", "restore", "a.txt"]);
        log_invocation(&missing_repo, &command, &matches);
        assert!(!missing_repo.exists());
    }
}
