# Stale Backup Detection After Reclamation Or Compaction

Open design question for REQ-MAINTENANCE-007 in
[`../../requirements/functional/maintenance.md`](../../requirements/functional/maintenance.md):
restoring a metadata backup must warn the user if an operation that may have physically relocated
or invalidated stored bytes has run against the repository since that backup was taken. How the
repository actually detects this - and what "since that backup was taken" gets compared against -
is not yet decided.

## Not yet decided: how staleness is detected

Status: idea

No design decision recorded here yet - this file exists so the question is not lost between now
and when REQ-MAINTENANCE-001/002 (metadata backup/restore) and REQ-STORAGE-004/005 (reclamation/
compaction) are actually implemented, since none of the operations REQ-MAINTENANCE-007 is about
exist yet to evaluate a concrete approach against.

One candidate approach, worth evaluating fresh against this project's actual implementation rather
than assumed correct: a generation counter held in `repository_settings` (see DESIGN-METADATA-009
in [`metadata-schema-with-contents-table.md`](metadata-schema-with-contents-table.md) for that
table), bumped whenever an operation runs that may have physically relocated or invalidated stored
bytes. A metadata backup stamps the counter's value at backup time. Restore compares the stamped
value against the repository's current one and warns (not refuses) on a mismatch. Not weighed
against alternatives yet, and not assumed to be the answer - named here only so the option is not
lost before this gets picked up.

### Reclamation is the natural arming point, not the eventual overwrite

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
rejected, not merely undecided. It would require the ordinary store/write path to check, on every
write, whether the range it is about to use might be referenced by some outstanding metadata backup
- a real, ongoing coupling between two otherwise-independent subsystems. This was taken on to
reduce how often a warning fires, without needing to reduce anything else: a false positive here (an
unnecessary warning) is cheap, and a false negative (an undetected stale restore, silently resolving
to wrong bytes) is exactly what REQ-MAINTENANCE-007 exists to prevent. Arming at reclamation time
gets that asymmetry right for free, with a single, simple, well-isolated hook - the fine-grained
alternative would spend real design and implementation complexity buying back a cost (an
occasional, harmless extra warning) this project has no particular reason to minimize.

### Purging a tree entry is also an arming point, on the same terms as reclamation

A purge (REQ-CLI-003's `--purge` case, or reached through the mount under REQ-MOUNT-007) performs
REQ-STORAGE-004's reclaim cascade immediately, scoped to the entry being purged (and, for a
directory, its descendants) - see REQ-CLI-003 in
[`../../requirements/functional/cli-commands.md`](../../requirements/functional/cli-commands.md).
Tracing the actual triggers in
[`metadata-schema-with-contents-table.md`](metadata-schema-with-contents-table.md) shows why this
decision matters here: deleting a `tree_entries` row only fires `tree_entries_ref_count_del`, which
decrements `contents.ref_count` - it does not by itself delete the `contents` row, or cascade down
through `content_chunks`/`chunks`/`chunk_extents`, even once that count reaches zero. Left alone, a
purge would leave every byte range it just orphaned neither freed nor even nominally eligible for
reuse until some later, separate operation walks `ref_count = 0` rows and performs that cascade.
Purge closes that gap itself: after removing the `tree_entries` row(s), it walks any `contents` row
whose `ref_count` it just dropped to zero and performs the same
`contents`/`content_chunks`/`chunks`/`chunk_extents` cascade the bulk reclaim sweep (REQ-STORAGE-004)
performs, just scoped to what that one purge actually orphaned rather than every aged soft-deleted
entry repository-wide - a targeted, small cascade per purge, rather than deferring that work to a
later, separate sweep.

This makes purge exactly as much an arming point for the staleness warning as reclamation is - the
same reasoning under "Reclamation is the natural arming point" above applies to it verbatim, since
purge now performs the identical kind of freeing work, just at a smaller, per-entry granularity
instead of a batched, repository-wide one. Any future implementation of the generation-counter idea
above must bump the counter from both operations, not from the bulk sweep alone.

### Each batch must bump the counter itself, atomically with its own freeing work

Because backup is read-only and REQ-MAINTENANCE-004 leaves it unblocked even while a mutating
reclamation, purge, or compaction run is in progress, a backup can capture a snapshot *during* such
a run, not just before or after it - including between two batches of a bulk reclaim sweep that
commits in several transactions rather than one giant one (likely, for the same
long-exclusive-lock reason REQ-MAINTENANCE-003 already rules out for compaction). Such a backup
stamps whatever counter value was current at that moment, even though some of the run's byte ranges
are already freed by then.

A single bump at the very end of the whole run - as the final batch's last statement, or as a
separate transaction committed right after it - would still be enough to catch *that* case: the
mechanism only has to guarantee `stamped != current` at restore time, not detect staleness the
instant it happens, and the counter is monotonically increasing, so any backup stamped before the
eventual bump still commits necessarily holds a value strictly lower than every `current` value
read afterward, restore included.

What a single end-of-run bump does not survive is the run never reaching its end at all: if the
process loses power (or is killed) after batch A, B, and C have committed - durably, per SQLite's
own guarantees - but before batch D or the planned final bump, and the run is never resumed or
retried, the counter permanently stays at its pre-run value even though A, B, and C's byte ranges
are already freed and durably so. A backup taken before this run then compares as `stamped ==
current` at restore time forever after - not delayed detection, but no detection at all, exactly
the false negative REQ-MAINTENANCE-007 exists to prevent. Deferring the bump to a later transaction
than the freeing work it covers reopens, one level down, the identical gap a naive "bump sometime
after freeing" design has at the level of a single write - it just takes a differently-timed crash
to hit it.

The fix is for every transaction that performs any freeing (or relocating) write to also bump the
counter itself, in that same transaction - never in a separate one, however soon after. Whatever
prefix of a run's batches actually ends up committed, by however abrupt an interruption, the
counter then already reflects exactly that prefix's worth of freed bytes, with no window in which
committed freeing work outruns the counter that is supposed to flag it.

Whether a metadata backup should be allowed to run at all while a mutating session (e.g. a
read-write mount) is active is a separate, still-open question - not decided here. It is already
tracked as an explicit hedge in REQ-MAINTENANCE-001 ("not guaranteed here as an unconditional
property, left to design to determine how far it holds in practice") in
[`../../requirements/functional/maintenance.md`](../../requirements/functional/maintenance.md). One
relevant fact for that question: REQ-MAINTENANCE-004's own concurrent-access safety already rules
out a reclamation run overlapping a mutating mount session in the first place - only one mutating
operation runs against a repository at a time - so the counter can only ever change before or after
such a session, never during it. A backup taken mid-session, itself read-only and so unblocked
under REQ-MAINTENANCE-004, simply captures whatever value was already in effect for that session's
entire duration.

This gets a `DESIGN-...` ID once an approach is actually decided, per this directory's own
[`README.md`](README.md) ID scheme ("assign IDs only once a decision is actually settled") - not
before.
