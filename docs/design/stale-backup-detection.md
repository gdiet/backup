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

## Reclamation is the natural arming point, not the eventual overwrite

Reclamation and compaction affect stored bytes differently, but that difference argues for arming
the warning at reclamation time, not against it:

- **Reclamation** only marks a byte range as reusable - it does not itself alter any byte already
  on disk (see REQ-STORAGE-004's own text). But it is the *only* moment that makes a previously
  live, possibly-backed-up byte range eligible for reuse at all - the risk to an existing metadata
  backup is created right there, even though the actual overwrite (if it ever happens) comes later,
  from a separate, ordinary write with no reason to know or care which backups exist.
- **Compaction** actively relocates still-live content as part of the operation itself (see
  REQ-STORAGE-005) - its staleness effect is immediate, at the moment compaction runs.

Both, then, are best treated as arming events in their own right: bumping a shared counter (or
equivalent) at reclamation time itself, not deferred until some later write happens to reuse a
given range. Tracking staleness at the finer granularity of "was this specific reclaimed range
actually reused yet" was the alternative considered here, and is worth naming explicitly as
rejected, not merely undecided: it would require the ordinary store/write path to check, on every
write, whether the range it is about to use might be referenced by some outstanding metadata
backup - a real, ongoing coupling between two otherwise-independent subsystems, taken on to reduce
how often a warning fires without needing to reduce anything else, since a false positive here (an
unnecessary warning) is cheap and a false negative (an undetected stale restore, silently resolving
to wrong bytes) is exactly what REQ-MAINTENANCE-007 exists to prevent. Arming at reclamation time
gets that asymmetry right for free, with a single, simple, well-isolated hook - the fine-grained
alternative would spend real design and implementation complexity buying back a cost (an
occasional, harmless extra warning) this project has no particular reason to minimize.

This gets a `DESIGN-...` ID once an approach is actually decided, per this directory's own
[`README.md`](README.md) ID scheme ("assign IDs only once a decision is actually settled") - not
before.
