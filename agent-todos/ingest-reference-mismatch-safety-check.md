# Sanity-check a supplied reference against the actual ingest sources

**Why parked**: found while doing a full comparative sweep of the Scala reference implementation
against our current `requirements/`/`docs/design/` for gaps, at the developer's request - a
genuinely new candidate requirement, not something this task should decide and implement on its
own.

**Size**: medium (a real product decision - whether to add this at all, and if so, where the
threshold/override lives)
**Opened**: 2026-08-31, by Claude Code on the web session (branch
`claude/cloud-environment-tests-flwko1`)
**Context**: REQ-INGEST-003 in [`../requirements/functional/ingest.md`](../requirements/functional/ingest.md)
(accelerated re-ingest via reference)

REQ-INGEST-003 lets a caller supply an earlier backup as a reference so a same-named/same-size/
same-modified-time source file can be linked to existing content without re-reading it. Nothing in
that requirement (or elsewhere) addresses what happens if the supplied reference does not actually
correspond to the sources being backed up - e.g. an operator's script passes the wrong path, or a
reference from years ago that no longer resembles the current source tree at all. Every file would
then simply fail the time+size match one by one and fall back to a full read - correct, but with no
signal that something is probably configured wrong, only a silently slower run than expected.

Worth deciding: should ingest sanity-check a supplied reference before trusting it - comparing the
reference's own top-level listing against the sources' own top-level listing and refusing (or at
least warning loudly) if too few entries match, with an explicit override to force the reference
through anyway when a caller doesn't need or want that check? This is a genuine safety net against
a misconfigured or stale `--reference` silently doing nothing useful, not just an edge case -
finding "that this reference plainly does not match" long after the fact is much less useful than
being told immediately.

If pursued, this belongs as a new `REQ-INGEST-...` entry (or an addition to REQ-INGEST-003's own
text if the difference stays crisp) in `requirements/functional/ingest.md`, following this
project's usual `Status: draft` → `agreed` process - not something to decide unilaterally here.
