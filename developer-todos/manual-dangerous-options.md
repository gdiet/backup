# Document the other potentially dangerous options in the manual

**Noted**: 2026-10-05, while planning REQ-MOUNT-005 (`dfs mount --best-effort`) and the new
`docs/manual.md`.
**Size**: small to medium - one manual section, plus one-line consequence statements in the clap
help of each option.
**Context**: `docs/manual.md` (to be created with REQ-MOUNT-005's work, which documents
`--best-effort` first); `developer-todos/cli-quickstart-help-text.md`.

The developer's own request: the manual gets a "Potentially dangerous options" section. The first
entry is `--best-effort` (`dfs mount` and `dfs restore`). Document the other options that can
destroy or corrupt data in the same section:

- `--assume-read-only-medium` (`mount`, `restore`, others): undefined behavior and possible
  corruption if the repository is modified by another process.
- `--purge` (`mount`): permanently purges entries.
- `dfs reclaim` with its default `--min-age-days 0`: purges every soft-deleted entry at once, the
  manual's "Reclaim space" section already says so.
- `--force-reference` (`ingest`): skips the likeness check of the reference.
- Any further option found while going through `dfs <command> --help` for every command.

Each clap help text keeps a one-line statement of the consequence. The manual explains the details.
