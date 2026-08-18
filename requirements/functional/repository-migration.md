# Repository Migration

Requirements for adopting an existing repository created by a predecessor implementation. See
[`../../migration/`](../../migration/) for the concrete migration path and feature-parity tracking
this enables.

### REQ-MIGRATION-001: Preserve the complete tree, including deletion history
Status: draft
Importance: must

Migrating an existing repository carries over its entire tree, including soft-deleted entries —
not only the currently active files.

Rationale: recoverability of deleted-but-not-yet-purged history is a property users of the
predecessor repository already relied on; migration should not be the event that quietly loses it.

### REQ-MIGRATION-002: No wholesale recopy of stored content
Status: draft
Importance: should

Migration does not require rewriting or recopying already-stored byte content wholesale — it may
read existing bytes as needed to derive new metadata, but does not need to duplicate storage to
adopt it.

Rationale: a migration that copies every stored byte would cost time and temporary disk space
proportional to the entire repository's size, for data that is already sitting on disk correctly.

### REQ-MIGRATION-003: Safely resumable after failure
Status: draft
Importance: should

A migration that fails or is interrupted partway through can be re-run from scratch without manual
cleanup, and without risk to the source repository's original data.

Rationale: a multi-step migration over a potentially large repository will occasionally be
interrupted (power loss, a killed process) — recovering from that should be as simple as trying
again.
