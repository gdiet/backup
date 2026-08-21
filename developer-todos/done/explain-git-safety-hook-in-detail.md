# Explain the git-safety-guard hook in detail

**Noted**: 2026-08-21, during a session that added `PreToolUse` hooks enforcing hard git-safety
rules deterministically (commits `abec542f`, `344b5118`).
**Size**: medium (a real walkthrough, not a one-line answer)
**Context**: `.claude/hooks/git-safety-guard.sh` (Bash), `.claude/hooks/git-safety-guard.ps1`
(PowerShell), registered in `.claude/settings.json` under `PreToolUse`. Also see
`docs/agent-setup-plan.md` item 6 and `agent-todos/test-powershell-git-safety-hook.md` (the
PowerShell side is not yet live-verified).

The developer's own wording: "schreib mal als developer-todo auf, dass ich diesen hook genauer
erklärt haben möchte" - explicitly deferred, not to be explained now.

What a future session should cover when picking this up:
- What a `PreToolUse` hook is and why it exists as a separate enforcement layer from
  `AGENTS.md`/`CLAUDE.md` prose (context vs. enforced configuration - see
  code.claude.com/docs/en/memory).
- Exactly which git command patterns the two scripts match (force-push, `reset --hard`,
  `--no-verify`/`--no-gpg-sign`, `git clean -f...`, `branch -D`, discarding all uncommitted
  changes via `checkout .`/`restore .`) and why each one was picked.
- Why `permissionDecision: "escalate"` was chosen over `"deny"` - forces a confirmation prompt
  regardless of permission mode, rather than an unconditional block with no override.
- The `matcher: "Bash"` vs. `matcher: "PowerShell"` split and why two separate scripts exist
  (different shells, same rules) rather than one.
- Walk through the actual JSON input/output shape the hook receives and returns, ideally with the
  test commands already used in this session (see the commit messages and the `agent-todos` file
  above for example invocations).

## Done

**Completed**: 2026-08-21, moot rather than actually explained - the hooks this item was about were
removed before the explanation happened. A live test found `escalate` never surfaced a confirmation
prompt in the developer's actual `auto`-permission-mode Desktop-App setup, and the working
alternative (`deny`) removes the deliberate-exception path the developer relies on `auto` mode to
keep - see `docs/agent-setup-plan.md`, "PreToolUse Hooks For Git Safety: Tried, Then Reverted", for
the full account, and `agent-todos/done/git-safety-hooks-not-actually-enforcing.md` for the test
transcript. Nothing left to explain in detail now that the scripts themselves are gone.
