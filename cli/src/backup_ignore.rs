//! Per-directory `.backupignore` rule parsing and matching, used by
//! `store::walk_and_create_dirs` (private to that module, so not a doc link
//! here). See `docs/plans/backupignore.md` for the full design - including
//! why the directory-skip check here is
//! deliberately stricter than the Scala tool this ports (a bug there lets an
//! unconsumed multi-segment rule skip a whole directory it was only meant to
//! partially filter).

use std::fs;
use std::path::Path;

/// One `.backupignore` line, split on `/`. Each element of `segments` is raw
/// pattern text for one path component, matched via [`wildcard_match`].
/// `dir_only` records whether the source line ended with `/`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IgnoreRule {
    pub(crate) segments: Vec<String>,
    pub(crate) dir_only: bool,
}

/// Matches `text` against `pattern` in full (implicitly anchored at both
/// ends): `*` matches any run of characters (including none), `?` matches
/// exactly one character, every other character in `pattern` is literal.
pub(crate) fn wildcard_match(pattern: &str, text: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let t: Vec<char> = text.chars().collect();
    let (mut pi, mut ti) = (0usize, 0usize);
    // Backtrack point for the most recent `*`: how far into the pattern/text
    // it sits, and how much of the text it currently claims to have consumed
    // (grown one character at a time on backtrack, classic greedy-then-retry
    // wildcard matching).
    let mut star: Option<usize> = None;
    let mut star_match = 0usize;

    while ti < t.len() {
        if pi < p.len() && (p[pi] == '?' || p[pi] == t[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < p.len() && p[pi] == '*' {
            star = Some(pi);
            star_match = ti;
            pi += 1;
        } else if let Some(si) = star {
            pi = si + 1;
            star_match += 1;
            ti = star_match;
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == '*' {
        pi += 1;
    }
    pi == p.len()
}

/// Parses `.backupignore` content into rules: lines are trimmed, blank lines
/// and lines starting with `#` are dropped, remaining lines are split on `/`
/// into per-segment wildcard patterns (see [`IgnoreRule`]).
pub(crate) fn parse_ignore_rules(content: &str) -> Vec<IgnoreRule> {
    content
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(|line| {
            let dir_only = line.ends_with('/');
            let stripped = if dir_only {
                &line[..line.len() - 1]
            } else {
                line
            };
            IgnoreRule {
                segments: stripped.split('/').map(str::to_owned).collect(),
                dir_only,
            }
        })
        .collect()
}

/// What a directory's own `.backupignore` file (if any) says.
pub(crate) enum OwnIgnoreFile {
    /// No `.backupignore` in this directory.
    Absent,
    /// An empty `.backupignore` - the whole directory is skipped, no rules
    /// to parse.
    Empty,
    /// A non-empty, readable `.backupignore`, already parsed.
    Rules(Vec<IgnoreRule>),
}

/// Reads and parses `dir`'s own `.backupignore`, if any. An unreadable (but
/// present) file is treated the same as [`OwnIgnoreFile::Absent`] - silent
/// fail-open, matching the Scala tool's behavior.
pub(crate) fn read_own_ignore_file(dir: &Path) -> OwnIgnoreFile {
    let path = dir.join(".backupignore");
    match fs::metadata(&path) {
        Ok(meta) if meta.len() == 0 => OwnIgnoreFile::Empty,
        Ok(_) => match fs::read_to_string(&path) {
            Ok(content) => OwnIgnoreFile::Rules(parse_ignore_rules(&content)),
            Err(_) => OwnIgnoreFile::Absent,
        },
        Err(_) => OwnIgnoreFile::Absent,
    }
}

/// Does any inherited rule, fully consumed down to a single directory-marked
/// segment, match `name`? If so the directory (and everything under it) is
/// skipped outright. Deliberately restricted to single-segment rules - see
/// the module doc comment for why an unrestricted check (as Scala has) is a
/// bug, not a feature.
pub(crate) fn matches_dir_skip(inherited: &[IgnoreRule], name: &str) -> bool {
    inherited.iter().any(|rule| {
        rule.dir_only && rule.segments.len() == 1 && wildcard_match(&rule.segments[0], name)
    })
}

/// Does any inherited rule, fully consumed down to a single file-marked
/// segment, match `name`?
pub(crate) fn matches_file_skip(inherited: &[IgnoreRule], name: &str) -> bool {
    inherited.iter().any(|rule| {
        !rule.dir_only && rule.segments.len() == 1 && wildcard_match(&rule.segments[0], name)
    })
}

/// Builds the ignore scope handed to a directory named `name`'s own children,
/// given the scope inherited from its parent and its own freshly-parsed
/// rules: an inherited multi-segment rule whose next segment matches `name`
/// propagates (with that segment stripped) alongside `name`'s own rules.
/// Rules fully consumed by this match (nothing left after stripping) are
/// dropped - they were already applied by [`matches_dir_skip`]/
/// [`matches_file_skip`] at this level, not meant to propagate further.
pub(crate) fn child_scope(
    inherited: &[IgnoreRule],
    own: &[IgnoreRule],
    name: &str,
) -> Vec<IgnoreRule> {
    inherited
        .iter()
        .filter(|rule| wildcard_match(&rule.segments[0], name))
        .filter_map(|rule| {
            let remaining = rule.segments[1..].to_vec();
            if remaining.is_empty() {
                None
            } else {
                Some(IgnoreRule {
                    segments: remaining,
                    dir_only: rule.dir_only,
                })
            }
        })
        .chain(own.iter().cloned())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wildcard_star_matches_any_run_including_empty() {
        assert!(wildcard_match("a*b", "ab"));
        assert!(wildcard_match("a*b", "aXYZb"));
        assert!(wildcard_match("*", ""));
        assert!(wildcard_match("*", "anything"));
        assert!(wildcard_match("*.log", "app.log"));
        assert!(!wildcard_match("*.log", "app.txt"));
    }

    #[test]
    fn wildcard_question_mark_matches_exactly_one_char() {
        assert!(wildcard_match("a?c", "abc"));
        assert!(!wildcard_match("a?c", "ac"));
        assert!(!wildcard_match("a?c", "abbc"));
    }

    #[test]
    fn wildcard_literal_dot_is_not_special() {
        assert!(wildcard_match("a.b", "a.b"));
        assert!(!wildcard_match("a.b", "aXb"));
    }

    #[test]
    fn wildcard_match_is_anchored_to_the_full_string() {
        assert!(!wildcard_match("a?bc.d", "Xa?bc.d"));
        assert!(!wildcard_match("a?bc.d", "a?bc.dX"));
    }

    #[test]
    fn parse_ignore_rules_skips_comments_and_blank_lines() {
        let content = "# comment\n\n  \ntemp/\nlog*/*.log\n  .backupignore  \n";
        let rules = parse_ignore_rules(content);
        assert_eq!(
            rules,
            vec![
                IgnoreRule {
                    segments: vec!["temp".to_owned()],
                    dir_only: true,
                },
                IgnoreRule {
                    segments: vec!["log*".to_owned(), "*.log".to_owned()],
                    dir_only: false,
                },
                IgnoreRule {
                    segments: vec![".backupignore".to_owned()],
                    dir_only: false,
                },
            ]
        );
    }

    #[test]
    fn matches_dir_skip_ignores_unconsumed_multi_segment_rules() {
        // The Scala bug this deliberately does not reproduce: a multi-segment
        // rule's first segment must not skip the directory outright, even if
        // it happens to match.
        let rules = vec![IgnoreRule {
            segments: vec!["log*".to_owned(), "*.log".to_owned()],
            dir_only: false,
        }];
        assert!(!matches_dir_skip(&rules, "logs"));
    }

    #[test]
    fn matches_dir_skip_matches_a_terminal_dir_only_rule() {
        let rules = vec![IgnoreRule {
            segments: vec!["temp".to_owned()],
            dir_only: true,
        }];
        assert!(matches_dir_skip(&rules, "temp"));
        assert!(!matches_file_skip(&rules, "temp"));
    }

    #[test]
    fn matches_file_skip_matches_a_terminal_file_rule() {
        let rules = vec![IgnoreRule {
            segments: vec!["*.log".to_owned()],
            dir_only: false,
        }];
        assert!(matches_file_skip(&rules, "app.log"));
        assert!(!matches_dir_skip(&rules, "app.log"));
    }

    #[test]
    fn child_scope_propagates_remaining_segments_into_a_matching_directory() {
        let inherited = vec![IgnoreRule {
            segments: vec!["log*".to_owned(), "*.log".to_owned()],
            dir_only: false,
        }];
        let scope = child_scope(&inherited, &[], "logs");
        assert_eq!(
            scope,
            vec![IgnoreRule {
                segments: vec!["*.log".to_owned()],
                dir_only: false,
            }]
        );
        assert!(matches_file_skip(&scope, "app.log"));
        assert!(!matches_file_skip(&scope, "app.txt"));
    }

    #[test]
    fn child_scope_drops_rules_that_do_not_match_or_are_fully_consumed() {
        let inherited = vec![
            IgnoreRule {
                segments: vec!["other".to_owned()],
                dir_only: true,
            },
            IgnoreRule {
                segments: vec!["logs".to_owned()],
                dir_only: true,
            },
        ];
        // "other" doesn't match "logs"; "logs" matches but is a single
        // segment, so it's fully consumed (already applied by
        // matches_dir_skip at this level) and must not propagate further.
        assert!(child_scope(&inherited, &[], "logs").is_empty());
    }

    #[test]
    fn child_scope_combines_propagated_and_own_rules() {
        let inherited = vec![IgnoreRule {
            segments: vec!["src".to_owned(), "*.tmp".to_owned()],
            dir_only: false,
        }];
        let own = vec![IgnoreRule {
            segments: vec!["target".to_owned()],
            dir_only: true,
        }];
        let scope = child_scope(&inherited, &own, "src");
        assert!(matches_file_skip(&scope, "a.tmp"));
        assert!(matches_dir_skip(&scope, "target"));
    }
}
