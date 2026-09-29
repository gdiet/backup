# Repository Migration

Requirements for adopting an existing repository created by a predecessor implementation. See
[`../../migration/`](../../migration/) for the concrete migration path and feature-parity tracking
this enables.

### REQ-MIGRATION-001: Preserve the complete tree, including deletion history
Status: agreed
Importance: must

Migrating an existing repository carries over its entire tree, including soft-deleted entries —
not only the currently active files.

Rationale: recoverability of deleted-but-not-yet-purged history is a property users of the
predecessor repository already relied on; migration should not be the event that quietly loses it.

### REQ-MIGRATION-002: No wholesale recopy of stored content
Status: agreed
Importance: should

Migration does not require rewriting or recopying already-stored byte content wholesale — it may
read existing bytes as needed to derive new metadata, but does not need to duplicate storage to
adopt it. This assumes the predecessor's stored-byte layout is directly usable, or convertible in
place, by this implementation's storage backend; establishing that compatibility concretely is
[`../../migration/from-scala.md`](../../migration/from-scala.md)'s responsibility, not decided
here.

Rationale: a migration that copies every stored byte would cost time and temporary disk space
proportional to the entire repository's size, for data that is already sitting on disk correctly.

### REQ-MIGRATION-003: Safely resumable after failure
Status: agreed
Importance: should

A migration that fails or is interrupted partway through can be re-run from scratch without manual
cleanup, and without risk to the source repository's original data.

Rationale: a multi-step migration over a potentially large repository will occasionally be
interrupted (power loss, a killed process) — recovering from that should be as simple as trying
again.

### REQ-MIGRATION-004: Compare several CDC target sizes from one read of the source
Status: agreed
Importance: should

Migrating into more than one candidate `--cdc-target-size-bits` value does not cost one full read
of the source repository's stored content per value compared — reading the source once and
producing several migrated repositories, one per value, is sufficient.

Rationale: choosing a target size for a large, one-time migration benefits from comparing the
resulting deduplication ratio across a few candidate values on the operator's own real data before
committing to one — REQ-MIGRATION-002's own "no wholesale recopy" concern applies just as much to
reading the same multi-terabyte source repeatedly for this comparison as it does to copying it.
