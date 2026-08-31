# Consider date-substitution/existence-assertion syntax for ingest target paths

**Why parked**: found during the same Scala-comparison sweep as the other two items opened
alongside this one - a CLI-ergonomics feature idea, not something to add unilaterally.

**Size**: medium (a CLI syntax decision, plus it touches REQ-INGEST-001's target-path handling)
**Opened**: 2026-08-31, by Claude Code on the web session (branch
`claude/cloud-environment-tests-flwko1`)
**Context**: REQ-INGEST-001 in [`../requirements/functional/ingest.md`](../requirements/functional/ingest.md)

Nothing in `requirements/functional/ingest.md` or `cli-commands.md` currently addresses target-path
ergonomics for a scripted/scheduled ingest run beyond a plain literal path. Two small, low-cost
conveniences worth considering:

- Date-substitution placeholders in the target path (e.g. a `[yyyy-MM-dd]` segment resolved against
  the current date at run time), so a scheduled backup job can write into a fresh, self-naming
  target location each run without the calling script having to compute and pass a fully-resolved
  path itself.
- A per-path-segment existence assertion prefix, distinguishing "this segment must already exist"
  from "create it if missing" - catching a scripted run that silently starts creating a deep,
  unintended directory tree because a typo'd parent path did not exist, versus one that genuinely
  wants to populate a fresh dated directory each time.

Neither is required for REQ-INGEST-001 to function - a caller can already compute a timestamped
path itself before invoking ingest, and target paths already require existing parents by default.
This is purely about whether the convenience is worth a dedicated requirement, or whether "the
caller's script does that" is the intended, deliberately minimal answer.

If pursued, this is a `requirements/` addition (likely to `ingest.md` or `cli-commands.md`),
following the usual `Status: draft` → `agreed` process - including deciding the actual syntax
independently rather than copying it from elsewhere.
