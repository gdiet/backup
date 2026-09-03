//! `.backupignore` exclusion rules - REQ-INGEST-002 in `requirements/functional/ingest.md`. Used
//! by `crate::ingest` while walking a source tree: each directory's own rule file, combined with
//! rules propagated down from ancestor directories, decides which of that directory's entries are
//! skipped.

/// One parsed `.backupignore` rule line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rule {
    /// Pattern segments in order. All but the last name a subdirectory to descend into first;
    /// the last is matched against an entry directly inside the directory reached that way.
    segments: Vec<String>,
    /// Whether the terminal segment matches a directory only (the rule's source line ended in
    /// `/`) - a bare match otherwise applies to a file or a directory alike.
    directory_only: bool,
}

/// The result of parsing one directory's `.backupignore` file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackupIgnore {
    /// The file was empty: the directory holding it, and everything beneath it, is excluded
    /// wholesale, with no need for an explicit rule inside it.
    ExcludeWholeDirectory,
    Rules(Vec<Rule>),
}

/// Parses one `.backupignore` file's contents. A file that is empty (or holds only whitespace)
/// yields [`BackupIgnore::ExcludeWholeDirectory`]; otherwise each non-empty, non-comment
/// (`#`-prefixed) line becomes one [`Rule`].
pub fn parse(content: &str) -> BackupIgnore {
    if content.trim().is_empty() {
        return BackupIgnore::ExcludeWholeDirectory;
    }
    let rules = content
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(parse_rule)
        .collect();
    BackupIgnore::Rules(rules)
}

/// Parses one non-empty, non-comment rule line into its segments, splitting on `/` - a trailing
/// `/` at the very end of the line marks [`Rule::directory_only`] rather than producing a final
/// empty segment.
fn parse_rule(line: &str) -> Rule {
    let (body, directory_only) = match line.strip_suffix('/') {
        Some(body) => (body, true),
        None => (line, false),
    };
    let segments = body.split('/').map(str::to_string).collect();
    Rule {
        segments,
        directory_only,
    }
}

/// Matches `name` against `pattern`, where `*` matches any run of characters (including none) and
/// `?` matches exactly one character; matched case-sensitively (REQ-MOUNT-010), with every other
/// character - a literal `.` included - matched literally.
fn matches_segment(pattern: &str, name: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let n: Vec<char> = name.chars().collect();
    let (mut pi, mut ni) = (0usize, 0usize);
    let mut star: Option<(usize, usize)> = None; // (pattern index just past '*', name index it was tried at)

    while ni < n.len() {
        if pi < p.len() && (p[pi] == '?' || p[pi] == n[ni]) {
            pi += 1;
            ni += 1;
        } else if pi < p.len() && p[pi] == '*' {
            star = Some((pi + 1, ni));
            pi += 1;
        } else if let Some((star_pi, star_ni)) = star {
            pi = star_pi;
            ni = star_ni + 1;
            star = Some((star_pi, ni));
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == '*' {
        pi += 1;
    }
    pi == p.len()
}

/// The exclusion rules in effect while listing one directory's own children: rules propagated
/// down from an ancestor's multi-segment rules (their already-matched leading segments
/// consumed), combined with that directory's own `.backupignore` rules.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EffectiveRules {
    rules: Vec<Rule>,
}

impl EffectiveRules {
    /// The empty rule set - for a source tree's own root, which has no ancestor to propagate
    /// rules down from.
    pub fn none() -> Self {
        Self { rules: Vec::new() }
    }

    /// Combines this level's propagated rules with `own`, the rules just parsed from the current
    /// directory's own `.backupignore` (if it has one).
    pub fn combined_with(&self, own: &[Rule]) -> Self {
        let mut rules = self.rules.clone();
        rules.extend(own.iter().cloned());
        Self { rules }
    }

    /// Whether `name`, an entry directly inside this directory, is excluded - `is_dir` decides
    /// whether a directory-only rule (a trailing `/` in its source line) applies to it.
    pub fn excludes(&self, name: &str, is_dir: bool) -> bool {
        self.rules.iter().any(|rule| {
            rule.segments.len() == 1
                && (is_dir || !rule.directory_only)
                && matches_segment(&rule.segments[0], name)
        })
    }

    /// The rule set that propagates into a subdirectory named `name`: every multi-segment rule
    /// whose first segment matches it, with that first segment consumed - ready for the caller to
    /// [`Self::combined_with`] that subdirectory's own `.backupignore` rules (if it has one) once
    /// parsed.
    pub fn propagate_into(&self, name: &str) -> Self {
        let rules = self
            .rules
            .iter()
            .filter(|rule| rule.segments.len() > 1 && matches_segment(&rule.segments[0], name))
            .map(|rule| Rule {
                segments: rule.segments[1..].to_vec(),
                directory_only: rule.directory_only,
            })
            .collect();
        Self { rules }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rule(pattern: &str) -> Rule {
        parse_rule(pattern)
    }

    #[test]
    fn matches_segment_matches_a_plain_literal() {
        assert!(matches_segment("abc", "abc"));
        assert!(!matches_segment("abc", "abcd"));
        assert!(!matches_segment("abc", "ab"));
    }

    #[test]
    fn matches_segment_question_mark_matches_exactly_one_character() {
        assert!(matches_segment("a?c", "abc"));
        assert!(!matches_segment("a?c", "ac"));
        assert!(!matches_segment("a?c", "abbc"));
    }

    #[test]
    fn matches_segment_star_matches_any_run_including_empty() {
        assert!(matches_segment("a*c", "ac"));
        assert!(matches_segment("a*c", "abc"));
        assert!(matches_segment("a*c", "abbbbc"));
        assert!(!matches_segment("a*c", "ab"));
    }

    #[test]
    fn matches_segment_combines_wildcards_like_the_scala_reference_cases() {
        let pattern = "a?b*c.d";
        assert!(matches_segment(pattern, "aXbc.d"));
        assert!(matches_segment(pattern, "aXbXXXc.d"));
        assert!(
            !matches_segment(pattern, "abc.d"),
            "? must match a character"
        );
        assert!(!matches_segment(pattern, "aXbcXd"), ". is not a wildcard");
        assert!(!matches_segment(pattern, "XaXbc.d"), "no unexpected prefix");
        assert!(!matches_segment(pattern, "aXbc.dX"), "no unexpected suffix");
    }

    #[test]
    fn parse_treats_an_empty_or_whitespace_only_file_as_exclude_whole_directory() {
        assert_eq!(parse(""), BackupIgnore::ExcludeWholeDirectory);
        assert_eq!(parse("   \n\n  "), BackupIgnore::ExcludeWholeDirectory);
    }

    #[test]
    fn parse_skips_comments_and_blank_lines() {
        let content = "# a comment\n\nlog/*.log\n   \n# another\ntemp/\n";
        assert_eq!(
            parse(content),
            BackupIgnore::Rules(vec![rule("log/*.log"), rule("temp/")])
        );
    }

    #[test]
    fn parse_rule_splits_multi_segment_rules_keeping_the_trailing_slash_on_the_last_segment() {
        assert_eq!(
            parse_rule("log/*.log"),
            Rule {
                segments: vec!["log".to_string(), "*.log".to_string()],
                directory_only: false,
            }
        );
        assert_eq!(
            parse_rule("temp/"),
            Rule {
                segments: vec!["temp".to_string()],
                directory_only: true,
            }
        );
        assert_eq!(
            parse_rule("d/*e?f/"),
            Rule {
                segments: vec!["d".to_string(), "*e?f".to_string()],
                directory_only: true,
            }
        );
    }

    #[test]
    fn effective_rules_excludes_a_single_segment_rule_matching_a_file_or_directory() {
        let rules = EffectiveRules::none().combined_with(&[rule("*.tmp")]);
        assert!(rules.excludes("a.tmp", false));
        assert!(rules.excludes("a.tmp", true));
        assert!(!rules.excludes("a.txt", false));
    }

    #[test]
    fn effective_rules_directory_only_rule_does_not_exclude_a_same_named_file() {
        let rules = EffectiveRules::none().combined_with(&[rule("temp/")]);
        assert!(rules.excludes("temp", true));
        assert!(!rules.excludes("temp", false));
    }

    #[test]
    fn effective_rules_propagates_multi_segment_rules_into_the_named_subdirectory_only() {
        let rules = EffectiveRules::none().combined_with(&[rule("log/*.log")]);
        assert_eq!(
            rules.propagate_into("other"),
            EffectiveRules::none(),
            "a non-matching subdirectory gets nothing propagated into it"
        );
        let in_log = rules.propagate_into("log");
        assert_eq!(in_log.rules, vec![rule("*.log")]);
        assert!(in_log.excludes("a.log", false));
        assert!(!in_log.excludes("a.txt", false));
        // The propagated rule only ever applied inside "log" - a sibling directory of the same
        // name one level down does not inherit it a second time.
        assert_eq!(in_log.propagate_into("log"), EffectiveRules::none());
    }

    #[test]
    fn effective_rules_combines_propagated_and_own_rules_at_each_level() {
        let root = EffectiveRules::none().combined_with(&[rule("log/*.log"), rule("*.tmp")]);
        let in_log = root.propagate_into("log");
        // *.tmp is not itself propagated (it is a single-segment rule, scoped to the root only),
        // so it must not exclude anything inside "log".
        assert!(!in_log.excludes("a.tmp", false));
        assert!(in_log.excludes("a.log", false));
    }

    #[test]
    fn effective_rules_own_rules_from_a_deeper_directory_apply_alongside_propagated_ones() {
        let root = EffectiveRules::none().combined_with(&[rule("log/*.log")]);
        let in_log = root.propagate_into("log").combined_with(&[rule("*.old")]);
        assert!(
            in_log.excludes("a.log", false),
            "propagated rule still applies"
        );
        assert!(in_log.excludes("a.old", false), "own rule also applies");
    }
}
