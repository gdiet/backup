# `store`: `.backupignore` per-directory ignore rules

## Context

Scala's `.backupignore` (`BackupTool.process`/`ignoreRulesFromFile`/`deriveIgnoreRules`/
`createWildcardPattern` in `scala/src/main/scala/dedup/BackupTool.scala:106-167`, documented
in `scala/README.md:207-226`) is a per-directory ignore mechanism, checked during the source
tree walk:

- An **empty** `.backupignore` file in a directory skips that whole directory (fast path, no
  rules to parse).
- A **non-empty** `.backupignore` is a `.gitignore`-ish rule list: one glob per line (`*`/`?`
  wildcards only, no `**`, no negation, no character classes), `#` comments, blank lines
  ignored, a trailing `/` marks a directory-only rule.
- Rules apply to the directory they're found in *and* propagate into subdirectories: a
  multi-segment rule (e.g. `log*/*.log`) has its remaining segments carried down into any
  child directory matching its next segment.
- `.backupignore` is **not** auto-excluded - like any other file, it's only left out of the
  backup if the user adds an explicit rule for it. The shipped Scala example
  (`scala/README.md:220-221`) does exactly that:
  ```
  # Do not store the .backupignore itself in the backup.
  .backupignore
  ```
  Rust keeps this behavior unchanged - same convention, same opt-in.
- No CLI flag - purely a filesystem convention, same as Scala.

`cli/src/store.rs`'s `walk_and_create_dirs` (line 465) has no ignore mechanism at all today -
every readable file/directory under a source is backed up. It already threads one
`HashMap<PathBuf, i64>` (`dir_ids`) through a flat `WalkDir` iteration to resolve parent ids
without recursion; a second, parallel map threaded the same way is the natural place to hook
in `.backupignore` scopes (see Design below).

### A bug in Scala's directory-skip check, not being replicated

Scala's directory-skip check (`BackupTool.scala:125`) is:

```scala
else if ignore.flatMap(_.headOption).exists(s"$name/".matches) then
  log.trace(s"Ignored directory due to rules: $source"); 0L
```

This tests the **head segment of every active rule**, regardless of how many segments still
remain in that rule. Contrast with the file check three lines earlier
(`BackupTool.scala:116`), which correctly restricts to **fully-consumed, single-segment**
rules only:

```scala
if ignore.collect { case List(rule) => rule }.exists(name.matches) then ...
```

Because `*` compiles to regex `.*`, which readily absorbs the `/` appended for the directory
check, an *unconsumed* multi-segment rule can trigger the directory-skip check purely by
coincidence. Concretely, the README's own example rule `log*/*.log` - intended to mean "inside
any `log*`-named subdirectory, don't store `*.log` files, but keep everything else" - instead
causes the **entire** `logs/` directory (and everything under it) to be skipped the moment
`process` visits it, because `"logs/"` fully matches the compiled pattern for `log*`'s head
segment. Verified by reproducing the compiled-pattern semantics (`\Qlog\E.*\Q\E`, i.e.
effectively `^log.*$` after Java's literal-quoting) against a regex engine:

```
target="logs/" pattern="^log.*$" -> IsMatch = True   # entire directory wrongly skipped
target="a/"    pattern="^a$"     -> IsMatch = False  # non-wildcard segment: correctly not skipped
```

This is a latent bug, not a documented feature - `BackupToolSpec.scala` only unit-tests the
pure helper functions (`createWildcardPattern`, `deriveIgnoreRules`, `ignoreRulesFromFile`),
never an end-to-end multi-segment-rule-into-a-matching-subdirectory walk, so it was never
caught. The Rust port **fixes** this rather than reproducing it: the directory-skip check is
restricted to fully-consumed (single remaining segment), directory-marked rules only -
mirroring the restriction the file check already gets right. Multi-segment rules only ever
*propagate* into a directory whose name matches their next segment; they never skip that
directory outright.

## Design

### Matching: no new dependency

The pattern language is intentionally just `*` (any run of characters, including none) and `?`
(exactly one character) - everything else is literal, no character classes, no escaping, no
`**`. A small hand-rolled two-pointer wildcard matcher (the standard "Wildcard Matching"
algorithm, ~20 lines) covers this exactly and needs no crate. Pulling in `regex` or `globset`
would be overkill here and would change matching semantics for filenames containing `[`, `]`,
`{`, `}`, or backslashes (glob/regex metacharacters that this pattern language treats as
plain literals, same as Scala's hand-rolled `\Q...\E`-quoting does) - a hand-rolled matcher
avoids that class of surprise entirely, and needs no `cargo add`.

```rust
/// Matches `text` against `pattern` in full (implicitly anchored at both ends),
/// where `*` matches any run of characters (including none) and `?` matches
/// exactly one character; every other character in `pattern` is literal.
fn wildcard_match(pattern: &str, text: &str) -> bool
```

### Rule representation

```rust
/// One `.backupignore` line, split on `/`. Each element of `segments` is raw
/// pattern text for one path component, matched via `wildcard_match`.
/// `dir_only` records whether the source line ended with `/` (Scala instead
/// splices a literal trailing `/` onto the last segment's *pattern text* and
/// relies on the match target also getting `/` appended for directory checks -
/// see Context above for why that trick is what causes the directory-skip bug;
/// keeping `dir_only` as an explicit field sidesteps the whole bug class).
struct IgnoreRule {
    segments: Vec<String>,
    dir_only: bool,
}
```

### Per-directory scope, threaded through the existing flat walk

`walk_and_create_dirs` already keys a `HashMap<PathBuf, i64>` (`dir_ids`) by directory path,
populated as `WalkDir` yields entries (parents always before children). Add a second map built
the same way:

```rust
let mut ignore_scopes: HashMap<PathBuf, Vec<IgnoreRule>> = HashMap::new();
```

- The scope for `source` itself (depth 0) starts as `vec![]` - no inherited rules, matching
  Scala's `process(..., ignore = Seq())` at the top of each source.
- Switch the walk from `for entry in WalkDir::new(source)` to
  `let mut walker = WalkDir::new(source).into_iter(); while let Some(entry) = walker.next() { ... }`
  so a directory can be skipped *before* descending into it via `walker.skip_current_dir()` -
  needed both for the empty-file fast path and to avoid uselessly listing an ignored
  subtree (Scala's recursive design skips before recursing; the flat walk should too).
- On each **directory** entry (after the existing name-conflict handling):
  1. Look up the parent's scope from `ignore_scopes` (empty if absent - only possible for
     `source` itself).
  2. **Skip check** (the bug-fixed version of Scala's check): does any inherited rule with
     exactly one remaining segment, `dir_only = true`, match this directory's name via
     `wildcard_match`? If so: `warn`-count it, `walker.skip_current_dir()`, `continue` -
     do not create a `tree_entries` row, do not read this directory's own `.backupignore`.
  3. Otherwise, read `dir.join(".backupignore")`:
     - missing -> no own rules.
     - present, zero-length -> same as the skip check above (empty-file fast path):
       `walker.skip_current_dir()`, `continue`, no `tree_entries` row.
     - present, non-empty, readable -> parse into `Vec<IgnoreRule>` (unreadable is treated the
       same as "no own rules" - silent fail-open, matching Scala's
       `if ignoreFile.isFile && ignoreFile.canRead then ... else Seq()`).
  4. **Propagate**: for each inherited rule whose *next* segment matches this directory's name
     (`wildcard_match(&rule.segments[0], name)` - no trailing-slash trick needed, `dir_only`
     is carried on the struct, not encoded into the pattern text), drop the matched head
     segment; keep the rule (with its remaining segments) only if segments are still left
     afterwards.
  5. Combine propagated-inherited rules with this directory's own freshly-parsed rules; store
     under this directory's path in `ignore_scopes` for its children to look up. Proceed with
     the existing `insert_directory`/`dir_ids` logic unchanged.
- On each **file** entry: look up the parent's scope; **file check**: does any inherited rule
  with exactly one remaining segment, `dir_only = false`, match this file's name via
  `wildcard_match`? If so, count it and skip (don't push into `files`). Otherwise proceed as
  today.

### Parsing `.backupignore`

Mirrors Scala's `ignoreRulesFromFile`/`deriveIgnoreRules`: read the file, split into lines,
`.trim()` each, drop blank lines and lines starting with `#`, then for each remaining line:
`dir_only = line.ends_with('/')`; strip that trailing `/` *before* splitting on `/` (Rust's
`str::split('/')` on `"temp/"` yields `["temp", ""]` - an empty trailing segment - unlike
Scala's `String.split` which drops trailing empty strings by default; stripping the suffix
first avoids reproducing that mismatch) - so `segments = "temp".split('/').collect()`,
`dir_only = true`.

### Integration point

All of the above lives in `cli/src/store.rs`, most naturally as a new sibling module
(`cli/src/backup_ignore.rs`, `pub(crate)` items: `wildcard_match`, `IgnoreRule`,
`read_backupignore`/scope-building helpers) kept separate from `walk_and_create_dirs` itself
for testability, with `walk_and_create_dirs` gaining the `ignore_scopes` map and the
skip/propagate logic described above.

### CLI / config

None - pure filesystem convention, exactly like Scala. No new flag.

### Tests

- `backup_ignore.rs` unit tests for `wildcard_match` (mirrors Scala's
  `BackupToolSpec`'s `createWildcardPattern` cases): `*` matches zero-or-more, `?` matches
  exactly one, a literal `.` in a name is not a wildcard, full-string anchoring (extra
  prefix/suffix characters don't match).
- `backup_ignore.rs` unit tests for rule parsing: comment/blank-line filtering, trailing-`/`
  `dir_only` detection, multi-segment splitting.
- `cli/src/store.rs` integration tests (alongside the existing `run_store_*` tests), building a
  small temp source tree:
  - an empty `.backupignore` in a subdirectory -> that subdirectory produces zero
    `tree_entries` rows (not even the directory itself).
  - a populated `.backupignore` with a plain file rule and a dir-only rule -> the matching file
    and the matching directory (and everything under it) are absent from `tree_entries`.
  - the multi-segment propagating case (`log*/*.log`) -> the `logs/` directory *is* backed up,
    its non-`.log` files are present, only the `.log` files are excluded - this is exactly the
    case that demonstrates the Scala bug fix from Context above.
  - a `.backupignore` that excludes itself (matching the shipped Scala example) -> absent from
    `tree_entries`.

## Suggested sequencing

1. `wildcard_match` + unit tests.
2. `IgnoreRule` + parsing (`read_backupignore`) + unit tests.
3. Wire `ignore_scopes` into `walk_and_create_dirs`: skip-check, empty-file fast path,
   propagate, combine - switching to `WalkDir::into_iter()`/`skip_current_dir()`.
4. Integration tests in `store.rs`.

## Verification

- `cargo fmt --check && cargo clippy -- -D warnings && cargo test && cargo doc --no-deps`
  across the workspace, per `AGENTS.md`.
- Manual smoke test via the `run` skill: a temp repo, a small real source tree with a
  `.backupignore` (empty-file case and rule-file case), inspect `tree_entries` via `sqlite3`.
- Once shipped, move this file under `docs/plans/implemented/` and trim the `.backupignore`
  section out of `docs/plans/backup-reference-and-ignore.md` (keep that file for the
  still-unplanned reference-based-backup feature only).
