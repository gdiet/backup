use std::path::Path;
use std::process::ExitCode;

use clap::Args;

#[derive(Args)]
pub struct UndeleteArgs {
    /// Id of the deleted entry to reactivate, as shown by `backup deleted`.
    id: i64,

    /// Also reactivate every descendant that was deleted in the same
    /// operation (shares the exact same deletion timestamp) - the reverse
    /// of `del --recursive`'s own cascade. Without this, a reactivated
    /// directory's descendants stay deleted (find them with `backup
    /// deleted <the now-active directory>`).
    #[arg(long)]
    recursive: bool,

    /// Reactivate at a different repository path instead of the entry's
    /// original one. Use this if the original location is now occupied by
    /// an active entry (reactivating there fails otherwise, rather than
    /// silently renaming). The target's parent directory must already
    /// exist. Only the named entry itself moves - a `--recursive`
    /// reactivation's descendants keep their existing structure underneath
    /// it.
    #[arg(long)]
    to: Option<String>,
}

pub fn run_undelete(repo: &Path, args: UndeleteArgs) -> ExitCode {
    let repository = match db::open_repository(repo) {
        Ok(r) => r,
        Err(err) => {
            eprintln!(
                "error: failed to open repository at {}: {err}",
                repo.display()
            );
            return ExitCode::FAILURE;
        }
    };
    let conn = match repository.open_write_connection() {
        Ok(c) => c,
        Err(err) => {
            eprintln!("error: failed to open the metadata database: {err}");
            return ExitCode::FAILURE;
        }
    };

    match db::is_deleted(&conn, args.id) {
        Ok(None) => {
            eprintln!("error: no entry with id {}", args.id);
            return ExitCode::FAILURE;
        }
        Ok(Some(false)) => {
            eprintln!(
                "error: entry {} exists but is not currently deleted",
                args.id
            );
            return ExitCode::FAILURE;
        }
        Ok(Some(true)) => {}
        Err(err) => {
            eprintln!("error: {err}");
            return ExitCode::FAILURE;
        }
    }

    let relocate_to = match &args.to {
        None => None,
        Some(to_path) => {
            let (parent_path, name) = split_last_component(to_path);
            if name.is_empty() {
                eprintln!("error: --to path '{to_path}' has no final component");
                return ExitCode::FAILURE;
            }
            match db::resolve_path(&conn, parent_path) {
                Ok(Some(parent)) if parent.kind == db::EntryKind::Dir => Some((parent.id, name)),
                Ok(Some(_)) => {
                    eprintln!("error: '{parent_path}' is not a directory");
                    return ExitCode::FAILURE;
                }
                Ok(None) => {
                    eprintln!("error: target parent directory '{parent_path}' does not exist");
                    return ExitCode::FAILURE;
                }
                Err(err) => {
                    eprintln!("error: {err}");
                    return ExitCode::FAILURE;
                }
            }
        }
    };

    match db::undelete(&conn, args.id, args.recursive, relocate_to) {
        Ok(count) => {
            let entries = if count == 1 { "entry" } else { "entries" };
            println!("Reactivated {count} {entries}.");
            ExitCode::SUCCESS
        }
        Err(db::Error::AlreadyExists { name, .. }) => {
            eprintln!(
                "error: an active entry named '{name}' already exists at the target location; \
                 pass --to to reactivate elsewhere"
            );
            ExitCode::FAILURE
        }
        Err(err) => {
            eprintln!("error: {err}");
            ExitCode::FAILURE
        }
    }
}

/// Splits a repository path into `(parent, last component)` - e.g.
/// `"docs/a.txt"` -> `("docs", "a.txt")`, `"a.txt"` -> `("", "a.txt")`.
fn split_last_component(path: &str) -> (&str, &str) {
    match path.rfind('/') {
        Some(i) => (&path[..i], &path[i + 1..]),
        None => ("", path),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn init_repo() -> (tempfile::TempDir, PathBuf) {
        let temp_dir = tempfile::tempdir().unwrap();
        let repo_root = temp_dir.path().join("repo");
        db::init_repository(
            &repo_root,
            &db::RepositorySettings::new(12, db::Chunking::Cdc).unwrap(),
        )
        .unwrap();
        (temp_dir, repo_root)
    }

    fn args(id: i64) -> UndeleteArgs {
        UndeleteArgs {
            id,
            recursive: false,
            to: None,
        }
    }

    #[test]
    fn fails_for_a_missing_id() {
        let (_temp_dir, repo_root) = init_repo();
        assert_eq!(run_undelete(&repo_root, args(999)), ExitCode::FAILURE);
    }

    #[test]
    fn fails_for_an_id_that_is_not_deleted() {
        let (_temp_dir, repo_root) = init_repo();
        let repository = db::open_repository(&repo_root).unwrap();
        let conn = repository.open_write_connection().unwrap();
        conn.execute(
            "INSERT INTO tree_entries (id, parent_id, name, time, kind) VALUES (1, 0, 'a.txt', 0, 'file')",
            (),
        )
        .unwrap();
        drop(conn);

        assert_eq!(run_undelete(&repo_root, args(1)), ExitCode::FAILURE);
    }

    #[test]
    fn reactivates_a_deleted_file() {
        let (_temp_dir, repo_root) = init_repo();
        let repository = db::open_repository(&repo_root).unwrap();
        let conn = repository.open_write_connection().unwrap();
        conn.execute(
            "INSERT INTO tree_entries (id, parent_id, name, time, kind, deleted_at) VALUES (1, 0, 'a.txt', 0, 'file', 1000)",
            (),
        )
        .unwrap();
        drop(conn);

        assert_eq!(run_undelete(&repo_root, args(1)), ExitCode::SUCCESS);

        let conn = db::open_repository(&repo_root)
            .unwrap()
            .open_read_connection()
            .unwrap();
        assert!(db::resolve_path(&conn, "a.txt").unwrap().is_some());
    }

    #[test]
    fn fails_cleanly_on_conflict_and_to_resolves_it() {
        let (_temp_dir, repo_root) = init_repo();
        let repository = db::open_repository(&repo_root).unwrap();
        let conn = repository.open_write_connection().unwrap();
        conn.execute(
            "INSERT INTO tree_entries (id, parent_id, name, time, kind, deleted_at) VALUES (1, 0, 'a.txt', 0, 'file', 1000)",
            (),
        )
        .unwrap();
        conn.execute(
            "INSERT INTO tree_entries (parent_id, name, time, kind) VALUES (0, 'a.txt', 0, 'file')",
            (),
        )
        .unwrap();
        drop(conn);

        assert_eq!(run_undelete(&repo_root, args(1)), ExitCode::FAILURE);

        let mut with_to = args(1);
        with_to.to = Some("a-recovered.txt".to_string());
        assert_eq!(run_undelete(&repo_root, with_to), ExitCode::SUCCESS);

        let conn = db::open_repository(&repo_root)
            .unwrap()
            .open_read_connection()
            .unwrap();
        assert!(
            db::resolve_path(&conn, "a-recovered.txt")
                .unwrap()
                .is_some()
        );
    }
}
