# Claude Code Agent Setup: Findings And Decisions

Written 2026-08-21 as an audit of this project's Claude Code setup (Desktop app on Windows,
session in WSL2, working directory holding several independent sibling repositories) against
official documentation (code.claude.com/docs). Closed once its items were resolved - either done,
reverted by a deliberate decision, or spun off into their own tracked item. Kept as the reference
record for the decisions below that are worth finding again, not as an active plan. Directory names
below still say `rust2/` - the working directory has since been renamed to `rust/` (same content,
same branch), read `rust2/` as `rust/` throughout.

## Why sessions must start from the repository itself, not the shared parent

Claude Code walks up the directory tree from the working directory, loading every `CLAUDE.md` file
it finds along the way, at session start. A session rooted at the shared parent directory
(`~/privat/bdev/`) instead of the repository loaded that parent's own `CLAUDE.md` - generic
workspace orientation, not this project's conventions - while the repository's own `AGENTS.md` (a
subdirectory file relative to that root) never auto-loaded at all. This is why starting sessions
from inside the repository, not the shared parent, matters: it is the only way this project's own
conventions actually reach the session by default.

## PreToolUse Hooks For Git Safety: Tried, Then Reverted

An actual implementation across several sessions, then a deliberate reversal. Recorded here in
full so the same idea does not get re-attempted from scratch without first knowing what was
already found.

**What was built**: `.claude/hooks/git-safety-guard.sh` (Bash) and
`.claude/hooks/git-safety-guard.ps1` (PowerShell), registered as `PreToolUse` hooks in
`.claude/settings.json`, matching force-push, `git reset --hard`, `--no-verify`/`--no-gpg-sign`,
`git clean -f...`, `branch -D`, and discarding all uncommitted changes. Deliberately used
`permissionDecision: "escalate"`, not `"deny"`, specifically to preserve the ability to make a
deliberate, explicit exception (matching AGENTS.md's own "...unless explicitly instructed"
framing) rather than an unconditional block.

**What went wrong**:

- A live test (Desktop-App session, native Windows, `permission_mode: "auto"`) found `escalate`
  computed the correct decision but never actually surfaced as a confirmation prompt - the tool
  call just proceeded, silently, as if no hook existed at all. See
  `agent-todos/done/git-safety-hooks-not-actually-enforcing.md` for the full test transcript and
  reasoning.
- The same investigation separately found `git-safety-guard.sh` silently no-oped on a
  Git-for-Windows Bash that lacks `jq` (fixed at the time - fail closed, escalate-on-missing-`jq`
  instead of silent pass-through - before the eventual reversal below, but this bug is moot now
  that the hook itself is gone).
- A follow-up test, using a disposable local bare repository added as an extra git remote
  specifically to make this safe to repeat, confirmed directly: switching to
  `permissionDecision: "deny"` *does* reliably block the tool call in that same `auto`-mode
  Desktop-App session - the failure was specific to `escalate`, not to `PreToolUse` hooks in this
  harness/mode generally.

**Why it was reverted anyway**: `deny` blocks unconditionally, with no way to make the
"...unless explicitly instructed" exception AGENTS.md's own rule always allowed for - not even by
the developer explicitly asking, from within the same Claude Code session (only by running the
command in a separate, real terminal outside Claude Code entirely). `auto` permission mode matters
enough to the developer that giving up any deliberate-exception path was judged not worth it, so
the hooks and their `.claude/settings.json` registration were removed rather than kept as an
unconditional `deny`.

**If this idea comes up again**: the constraint to actually solve for is "reliable enforcement
that still allows a deliberate, explicit exception, in `auto` permission mode" - `escalate` does
not achieve this (confirmed non-functional there, as of this writing), `deny` achieves the
enforcement half but not the exception half. Check whether that trade-off has changed (a newer
Claude Code version, a different permission mode, a different harness) before re-implementing the
same approach from scratch.

## Working across sibling repositories

Starting sessions from inside the repository (above) already solves most of the multi-repo
problem: the sibling repositories (other implementations, plus a shared local test repository)
stop being part of the loaded context by default, since nothing else is an ancestor of the working
directory. For deliberate, one-off cross-repo work (e.g. comparing this project's schema against a
previous implementation's), grant access explicitly and temporarily - `claude --add-dir ../rust` at
launch, or `/add-dir ../rust` mid-session - rather than starting from the shared parent by default.
Neither loads the other repository's own memory files unless
`CLAUDE_CODE_ADDITIONAL_DIRECTORIES_CLAUDE_MD=1` is also set - a reasonable default, since another
implementation's conventions are not this project's (see AGENTS.md's "This Is A Rewrite, Not A
Port" and "Relationship To Other Implementations").

## AGENTS.md split into CLAUDE.md, path-scoped rules, and a skill

This document originally proposed splitting `AGENTS.md` (then 24KB, well past the ~200-line-per-
file guidance for reliable adherence) into a root `CLAUDE.md`, path-scoped `.claude/rules/*.md`
files, and per-directory `CLAUDE.md` files. That proposal has since been carried out - see
[`.claude/rules/requirements.md`](../.claude/rules/requirements.md),
[`.claude/rules/design-docs.md`](../.claude/rules/design-docs.md),
[`.claude/rules/rust-code-quality.md`](../.claude/rules/rust-code-quality.md),
[`mountfs/CLAUDE.md`](../mountfs/CLAUDE.md), and
[`.claude/skills/attributed-commits/`](../.claude/skills/attributed-commits/) directly, rather than
the now-historical reasoning that led to them. The confirmed mechanism behind all of it: a
subdirectory's own `CLAUDE.md` loads "at launch when started from that directory, or on demand when
Claude reads a file there" - real, documented, production-supported behavior, not something needing
further live testing.

## Everything else this audit raised

Confirmed and acted on without needing further tracking here: the repository already sits inside
WSL's own filesystem (not reached across the Windows/WSL boundary); the `fewer-permission-prompts`
skill has been run once (see `developer-todos/revisit-permission-allowlist-in-a-month.md` for the
planned re-check); a Rust code-intelligence plugin was evaluated far enough to find `rust-analyzer`
is not yet installed on this machine, tracked separately in
`agent-todos/evaluate-rust-code-intelligence-plugin.md`.
