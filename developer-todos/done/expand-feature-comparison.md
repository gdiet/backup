# Expand migration/feature-comparison.md to actually cover every Scala feature

**Noted**: 2026-08-22, while doing db-crate/repo-init work - noticed `migration/feature-comparison.md`
has exactly one row today, nowhere near tracking every Scala feature.
**Size**: large - confirm with the developer before starting. A real, deliberate cataloging pass
over the whole Scala feature set, not a quick addition.
**Context**: `migration/feature-comparison.md`; AGENTS.md's "Successor Status And Migration"
section, which treats this file as a release gate: "before this implementation is declared a
release-ready successor, every row must be in a deliberate, explained state - no silently missing
features."

Go through the Scala implementation's actual feature set (its `README.md`, `fsc` subcommands, and
behavior - not just this Rust implementation's own `requirements/`) and add a row per feature,
each marked implemented / planned / explicitly not planned with a one-line reason for the latter,
per the file's existing format. Deliberately not attempted alongside the repo-init session that
noticed the gap - this is a large, separate effort spanning the whole Scala feature set, not
something to squeeze in alongside an unrelated implementation task.

## Done (2026-09-03)

Read every `fsc` subcommand and separate `@main` in `.local/scala/src/main/scala/dedup/` (`Main.scala`,
`db/maintenance.scala`, `FSTools.scala`, `BackupTool.scala`) and cross-referenced each against this
project's own `requirements/` (commit `613afd91`). Added 14 new rows covering `check`, `db-backup`/
`db-restore`, `db-compact`, `del`, `find`, `stats`, `reclaimSpace`, `blacklist`, mount's `gui`/
`copyWhenMoving`/`dbBackup` options, mount's default mountpoint (not planned), graceful interrupt,
and `dbMigrateStep1`/`dbMigrateStep2` (not planned). Two genuine gaps found along the way (graceful
interrupt, default mountpoint) were also added to `requirements/open-questions.md`. Every Scala
feature is now accounted for in a deliberate, explained state, per AGENTS.md's release-gate wording
this todo's own "Context" section quotes.
