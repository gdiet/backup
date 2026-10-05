# Decide how commands take the repository path

**Noted**: 2026-10-05, while reading `docs/manual.md`.
**Size**: small to medium - the decision is the developer's, the change itself is mechanical.
**Context**: REQ-CLI-005 and REQ-CLI-006 in `requirements/functional/cli-commands.md`;
`crates/cli/src/main.rs`; `docker/samba-mount/entrypoint.sh` (calls `dfs create-repo "$REPO"`).

Found: `dfs create-repo` and `dfs unlock` take the repository path as a positional `[PATH]`. Every
other command takes `--repository <path>`. `dfs migrate-scala-repo` requires `--repository` and has
no default.

Options to decide between:

1. Keep it. A positional path suits `create-repo` (the thing being created) and `unlock`. Document
   the rule in the manual, which it already does.
2. Use `--repository` everywhere, including `create-repo` and `unlock`. One rule for every
   command. A breaking change of the command line, acceptable while the tool is alpha. Callers to
   update: the Docker entrypoint, scripts, documentation.
3. Accept both forms for `create-repo` and `unlock`. Compatible, but two ways to say the same thing
   (compare REQ-OPERABILITY-007).

Recommendation of the session that found it: option 2, because "every command takes
`--repository`" is the easiest rule to remember.
