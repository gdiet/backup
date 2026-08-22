# Consider AUTOINCREMENT for tree_entries.id

**Noted**: 2026-08-22, during the Mount/deleted-entry-view design discussion (REQ-MOUNT-008): its
disambiguation scheme leans on `tree_entries.id` being unique enough to serve as the guaranteed
fallback suffix.
**Size**: small - a one-word schema change plus checking whether anything already assumes bare
`INTEGER PRIMARY KEY` behavior.
**Context**: `docs/design/metadata-schema-with-contents-table.md`'s `tree_entries` schema;
REQ-MOUNT-008 in `requirements/functional/mount.md`.

`tree_entries.id` is currently a bare `INTEGER PRIMARY KEY` (SQLite rowid alias), not
`INTEGER PRIMARY KEY AUTOINCREMENT`. Without `AUTOINCREMENT`, SQLite picks a new rowid as
"current max + 1" - if a row holding the table's current maximum id is ever hard-deleted (a purge)
before any later row is inserted, the next insert can reuse that same id. In practice this needs an
unlucky ordering (a purge of the literal highest-ever-allocated id, with no intervening insert) and
is unlikely in a continuously-used repository, but it is not actually impossible the way
`AUTOINCREMENT`'s own `sqlite_sequence`-backed bookkeeping would guarantee.

When picked up: change `tree_entries`'s `id` column to `INTEGER PRIMARY KEY AUTOINCREMENT` in the
schema (free before the first release, per "Pre-release: a single, freely rewritten `v1` migration"
in `docs/design/metadata-storage.md`), and check whether any other schema/design text currently
assumes bare rowid-reuse behavior.
