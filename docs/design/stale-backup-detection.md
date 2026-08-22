# Stale Backup Detection After Reclamation Or Compaction

Open design question for REQ-MAINTENANCE-007 in
[`../../requirements/functional/maintenance.md`](../../requirements/functional/maintenance.md):
restoring a metadata backup must warn the user if an operation that may have physically relocated
or invalidated stored bytes has run against the repository since that backup was taken. How the
repository actually detects this - and what "since that backup was taken" gets compared against -
is not yet decided.

## Not yet decided: how staleness is detected

No design decision recorded here yet - this file exists so the question is not lost between now
and when REQ-MAINTENANCE-001/002 (metadata backup/restore) and REQ-STORAGE-004/005 (reclamation/
compaction) are actually implemented, since none of the operations REQ-MAINTENANCE-007 is about
exist yet to evaluate a concrete approach against.

One candidate approach, worth evaluating fresh against this project's actual implementation rather
than assumed correct: a generation counter held in `repository_settings` (see DESIGN-METADATA-009
in [`metadata-schema-with-contents-table.md`](metadata-schema-with-contents-table.md) for that
table), bumped whenever an operation runs that may have physically relocated or invalidated stored
bytes; a metadata backup stamps the counter's value at backup time; restore compares the stamped
value against the repository's current one and warns (not refuses) on a mismatch. Not weighed
against alternatives yet, and not assumed to be the answer - named here only so the option is not
lost before this gets picked up.

This gets a `DESIGN-...` ID once an approach is actually decided, per this directory's own
[`README.md`](README.md) ID scheme ("assign IDs only once a decision is actually settled") - not
before.
