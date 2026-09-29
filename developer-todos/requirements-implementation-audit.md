# Audit whether every requirement's `Status` still matches reality

**Noted**: 2026-09-29, after finishing the Scala-repository migration tool and noticing along the
way that its own design doc had drifted from actual `Status` values (several `DESIGN-MIGRATION-...`
entries were still `decided` after the code implementing them had already shipped), and that
REQ-STORAGE-005 is `agreed` but has no implementation anywhere despite sounding, from its title
alone, like something `dfs reclaim`/`dfs db-compact` might already cover (they do not - see
`developer-todos/store-defragmentation.md`).
**Size**: medium to large - a full pass over every file under `requirements/`, confirm with the
developer before starting given the likely scope.
**Context**: `requirements/README.md`'s status scheme (`agreed`/`should`/`must` etc.) and directory
layout; `.claude/rules/design-docs.md`'s parallel `Status:` convention for `docs/design/`.

The developer's own request: go through `requirements/` systematically and check, requirement by
requirement, whether its current `Status` line still reflects reality - not just take each file's
own claim at face value. Concretely, for each `REQ-...` entry:

- If it reads as `agreed`/`should`/`must` with no obvious implementation, confirm that is actually
  still true (grep for the feature, check whether a `dfs` command or `db`/`store`/`cdc` API already
  covers it) rather than assuming an old, stale `Status` line.
- If a linked `docs/design/...` decision exists, cross-check that design doc's own `Status` too -
  the migration-tool experience this session showed these two can drift independently of each
  other and of the actual code.
- Flag (do not necessarily fix in the same pass) every requirement whose `Status` turns out wrong,
  with enough detail (file, line, what was found instead) for a follow-up session to correct it.

Not urgent, but worth doing once as a baseline sanity check rather than continuing to accumulate
requirements whose documented state nobody has re-verified against the actual, current code.
