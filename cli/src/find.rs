use std::path::Path;
use std::process::ExitCode;

use clap::Args;

#[derive(Args)]
pub struct FindArgs {
    /// Pattern to search for: case-insensitive match against each entry's
    /// full path, anywhere within it. `*` matches any run of characters
    /// (including none), `?` matches exactly one character.
    pattern: String,
}

pub fn run_find(repo: &Path, args: FindArgs) -> ExitCode {
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
    let conn = match repository.open_read_connection() {
        Ok(c) => c,
        Err(err) => {
            eprintln!("error: failed to open the metadata database: {err}");
            return ExitCode::FAILURE;
        }
    };
    let entries = match db::subtree_entries_with_paths(&conn, 0) {
        Ok(entries) => entries,
        Err(err) => {
            eprintln!("error: {err}");
            return ExitCode::FAILURE;
        }
    };

    let mut matches = 0u64;
    for entry in &entries {
        if glob_contains(&entry.path, &args.pattern) {
            println!("{}", entry.path);
            matches += 1;
        }
    }

    // Matches the `grep` convention: 0 means at least one match was found, 1
    // means the search ran fine but found nothing - a distinguishable,
    // scriptable result, unlike the Scala tool this replaces (which always
    // exits 0, so a script has no way to tell "no matches" from "matched
    // everything" without parsing stdout).
    if matches > 0 {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

/// Case-insensitive match of `pattern` (with `*`/`?` wildcards) anywhere
/// within `text`, as if `pattern` were implicitly wrapped in `*...*`.
fn glob_contains(text: &str, pattern: &str) -> bool {
    let text: Vec<char> = text.to_lowercase().chars().collect();
    let mut wrapped = vec!['*'];
    wrapped.extend(pattern.to_lowercase().chars());
    wrapped.push('*');
    glob_match(&text, &wrapped)
}

/// Whole-string glob match: `*` matches any run of characters (including
/// none), `?` matches exactly one character, anything else must match
/// literally. Classic two-pointer backtracking algorithm (as used for
/// `fnmatch`-style matching).
fn glob_match(text: &[char], pattern: &[char]) -> bool {
    let (mut ti, mut pi) = (0usize, 0usize);
    let mut star: Option<usize> = None;
    let mut star_text_pos = 0usize;

    while ti < text.len() {
        if pi < pattern.len() && (pattern[pi] == '?' || pattern[pi] == text[ti]) {
            ti += 1;
            pi += 1;
        } else if pi < pattern.len() && pattern[pi] == '*' {
            star = Some(pi);
            star_text_pos = ti;
            pi += 1;
        } else if let Some(star_pi) = star {
            pi = star_pi + 1;
            star_text_pos += 1;
            ti = star_text_pos;
        } else {
            return false;
        }
    }
    while pi < pattern.len() && pattern[pi] == '*' {
        pi += 1;
    }
    pi == pattern.len()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn glob_contains_matches_wildcards_case_insensitively_as_a_substring() {
        assert!(glob_contains("sub/a.txt", "a.txt"));
        assert!(glob_contains("sub/a.txt", "A.TXT"));
        assert!(glob_contains("sub/a.txt", "sub/*"));
        assert!(glob_contains("sub/a.txt", "*.txt"));
        assert!(glob_contains("sub/a.txt", "a?txt"));
        assert!(glob_contains("jre/bin/java", "jre/bin/java"));
        assert!(glob_contains("a/jre/bin/java/x", "jre/bin/java"));
        assert!(!glob_contains("sub/a.txt", "b.txt"));
        assert!(
            glob_contains("sub/a.txt", "a.tx"),
            "substring semantics: 'a.tx' is a literal substring of 'a.txt'"
        );
        assert!(!glob_contains("sub/a.txt", "xyz"));
    }

    #[test]
    fn glob_contains_treats_percent_and_underscore_as_literal_not_sql_wildcards() {
        // Regression check for the exact bug class the Scala tool this
        // replaces has (unescaped SQL LIKE wildcards): we never build a LIKE
        // pattern from user input at all, so `%`/`_` are always literal here.
        assert!(glob_contains("100%_done.txt", "100%_done.txt"));
        assert!(!glob_contains("100X_done.txt", "100%_done.txt"));
    }

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

    #[test]
    fn run_find_matches_and_exit_code_reflects_whether_anything_was_found() {
        let (_temp_dir, repo_root) = init_repo();
        let repository = db::open_repository(&repo_root).unwrap();
        let conn = repository.open_write_connection().unwrap();
        let sub_id = db::insert_directory(&conn, 0, "sub", 0).unwrap();
        conn.execute(
            "INSERT INTO tree_entries (parent_id, name, time, kind) VALUES (?1, 'a.txt', 0, 'file')",
            [sub_id],
        )
        .unwrap();
        drop(conn);

        assert_eq!(
            run_find(
                &repo_root,
                FindArgs {
                    pattern: "*.txt".to_string()
                }
            ),
            ExitCode::SUCCESS
        );
        assert_eq!(
            run_find(
                &repo_root,
                FindArgs {
                    pattern: "*.md".to_string()
                }
            ),
            ExitCode::FAILURE
        );
    }
}
