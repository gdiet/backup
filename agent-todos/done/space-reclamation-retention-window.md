# Should space reclamation support a minimum-age retention window for soft-deleted entries?

**Why parked**: found during the same Scala-comparison sweep as
`ingest-reference-mismatch-safety-check.md` - a real product decision, not something to settle
unilaterally while reviewing.

**Size**: medium (changes REQ-STORAGE-004's scope, and whatever CLI surface eventually implements
space reclamation)
**Opened**: 2026-08-31, by Claude Code on the web session (branch
`claude/cloud-environment-tests-flwko1`)
**Context**: REQ-STORAGE-004 in [`../requirements/functional/storage.md`](../requirements/functional/storage.md)
(space reclamation), REQ-TREE-002 in [`../requirements/functional/tree.md`](../requirements/functional/tree.md)
(soft delete)

REQ-STORAGE-004 says storage no longer referenced by any file can be reclaimed, but says nothing
about *when* a soft-deleted entry (REQ-TREE-002) becomes eligible - whether running reclamation
purges every currently soft-deleted, unreferenced entry unconditionally each time it is invoked, or
whether the operation itself takes a minimum-age parameter (e.g. "only entries deleted more than N
days ago") so a routine/scheduled reclaim run can still preserve a recent-deletion recovery window
without the operator having to time reclaim runs around it manually.

Worth deciding explicitly: does REQ-STORAGE-004 (or REQ-CLI-004, or a new requirement) need this
retention-window concept, or is "reclaim purges everything currently eligible, the operator decides
when to run it" the intended, simpler model? Either is defensible - this just was not yet a
deliberate choice anywhere in `requirements/`.

If pursued, this is a `requirements/` change (likely amending REQ-STORAGE-004's own text, or a new
requirement it points to for the exact parameter), following the usual `Status: draft` → `agreed`
process.

**Done**: the developer confirmed this should become a real requirement. Amended REQ-STORAGE-004 in
`requirements/functional/storage.md` directly (still `Status: draft`) with a caller-chosen minimum
age, defaulting to reclaiming immediately when none is given.
