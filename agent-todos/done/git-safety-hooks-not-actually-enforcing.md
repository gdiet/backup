# The git-safety-guard PreToolUse hooks do not currently enforce anything in practice

**Needs**: further investigation on a machine/session where `jq` is installed and/or
`permission_mode` is not `"auto"`, to isolate which of the two findings below is fixable by
changing this repository versus which is a property of the harness/session that this repository
cannot control from `.claude/settings.json` alone.
**Size**: medium (the findings below are conclusive for this session, but confirming root cause
and deciding a fix strategy needs a developer decision, not a quick agent-only fix)
**Opened**: 2026-08-21, by a Claude Code Desktop-App session on native Windows.
**Context**: `agent-todos/done/test-powershell-git-safety-hook.md` (the PowerShell-hook
verification this followed on from), `.claude/hooks/git-safety-guard.sh`,
`.claude/hooks/git-safety-guard.ps1`, `.claude/settings.json`, `docs/agent-setup-plan.md` item 6
(whose status table currently claims the Bash hook is "live-tested" - the finding below shows that
claim does not hold and the table needs correcting once this is resolved).

## Finding 1: the Bash-side hook is a silent no-op on this machine (missing `jq`)

`.claude/hooks/git-safety-guard.sh` parses its stdin with `jq -r '.tool_input.command // empty'`.
This machine's Bash tool runs Git for Windows' bundled MinGW64/MSYS2 bash (confirmed via its
`PATH`, e.g. `/mingw64/bin:/usr/bin:...`), which does not include `jq`, and no other installation
of it is on `PATH` either. Running the script by hand confirms this directly:

```
$ echo '{"tool_input":{"command":"git push --force origin main"}}' | bash .claude/hooks/git-safety-guard.sh; echo "EXIT: $?"
.claude/hooks/git-safety-guard.sh: line 16: jq: command not found
EXIT: 0
```

Because the script has no `set -e`, the failed `jq` call leaves `command` empty, which hits
`[ -z "$command" ] && exit 0` - the script exits cleanly with no output, indistinguishable from "no
match found," for every single command, not just git commands (the `Bash` matcher in
`.claude/settings.json` is not scoped to git). This has been true for every Bash tool call made in
this session, confirmed by nobody having seen a hook error in this entire conversation.

Adding `set -euo pipefail` to the script (tested, then reverted - the file is back to its committed
state) does make the script exit immediately with status 127 instead of silently falling through -
but see Finding 2 below: this did not produce any visible error to the developer either, so by
itself `set -e` does not fix the actual problem, only the internal correctness of the script's
exit code.

## Finding 2: PreToolUse hook signals are not surfacing to the user in this session at all

Independently of Finding 1, the PowerShell-side hook (`git-safety-guard.ps1`, which has no external
dependency - it uses only built-in `ConvertFrom-Json`) was confirmed, by temporarily adding debug
logging (reverted afterward), to:

- actually be invoked live by the harness for every PowerShell tool call in this session, and
- receive and correctly parse the real `git push --force ...` command used in this test, e.g.:

```json
{"session_id":"...","permission_mode":"auto","hook_event_name":"PreToolUse","tool_name":"PowerShell","tool_input":{"command":"git push --force hooktest rust2-claude", ...}, ...}
```

Given this input, the script's existing logic would have matched the force-push rule and emitted a
correct `escalate` decision (verified independently via direct manual invocation with the same
command in `agent-todos/done/test-powershell-git-safety-hook.md`). Yet no confirmation dialog was
observed by the developer when this exact command was run live through the PowerShell tool against
a disposable local bare repo (`git init --bare` in the scratchpad, added as a second remote
`hooktest`, specifically to make this safe to repeat without touching the real GitHub origin).

The same absence of any visible signal was also confirmed for a hard script failure: temporarily
adding `set -euo pipefail` to `git-safety-guard.sh` (Finding 1) and triggering it with a harmless
`echo` command (any Bash command triggers the matcher) produced no visible error either.

The one concrete, session-specific fact captured in the hook's own stdin payload is
`"permission_mode":"auto"`. The working hypothesis is that this harness/session does not surface a
`PreToolUse` hook's `escalate` decision (or a hook script's failure) as an actual confirmation
prompt while `permission_mode` is `"auto"` - contradicting the hook's own
`permissionDecisionReason` text ("Confirm explicitly with the developer... regardless of
permission_mode") and the general Claude Code hook documentation's stated semantics. This has not
been tested under a non-`"auto"` permission mode, nor in a non-Desktop-App harness, so it is not
yet established whether this is specific to `"auto"` mode, to this harness, or both.

## Net effect

As of this writing, neither `git-safety-guard.sh` nor `git-safety-guard.ps1` provides any actual
enforcement in this session type: the Bash hook never runs its own logic at all (Finding 1), and
even when the PowerShell hook runs its logic correctly and decides to escalate, that decision does
not reach the developer (Finding 2). The advisory prose in `AGENTS.md`'s Git Safety Protocol is,
for the moment, the only thing actually governing agent behavior around force-push et al. in
sessions like this one - exactly the gap `docs/agent-setup-plan.md` item 6 set out to close.

To make progress on this: confirm whether `permission_mode != "auto"` changes Finding 2's outcome,
and decide whether the Bash hook should stop depending on `jq` (e.g. rewritten with pure
`grep`/`bash` pattern matching, mirroring the `.ps1` script's dependency-free approach) regardless
of Finding 2's resolution, since a hook that silently never runs is strictly worse than one that at
least tries.

## Developer's leaning, as of this writing

The developer's initial reaction to these findings is to lean toward dropping this
`PreToolUse`-hook approach entirely rather than fixing it - a hook that silently fails to enforce
anything (in the one session type it has actually been exercised in so far) may be worse than no
hook at all, since it creates false confidence that these commands are gated when they are not.
This is a leaning, not yet a decision: the developer has not ruled out fixing Finding 1 (the `jq`
dependency) or investigating Finding 2 further (whether a non-`"auto"` permission mode actually
does surface `escalate`). Whichever way this goes, `docs/agent-setup-plan.md` item 6's status
entry and this file need updating to reflect the actual outcome once decided.

## Done

**Completed**: 2026-08-21, by the WSL2/Linux Claude Desktop-App session Finding 2 called for
(non-`"auto"` mode was not available to test, but the same `"auto"`-mode session that opened this
file could reach Finding 2's open question a different way: a disposable local bare repo, added as
an extra git remote, made it safe to test `permissionDecision: "deny"` instead of `"escalate"` for
real). Result: `deny` *does* reliably block the tool call in `"auto"` mode - the failure is specific
to `escalate`, not to `PreToolUse` hooks in this harness/mode generally. Finding 1 (the `jq`
dependency) was also fixed at the time (fail closed on missing `jq` instead of silently passing
through).

Net decision, made by the developer once both facts were in hand: revert rather than switch to
`deny`. `deny` blocks unconditionally, with no way to make the "...unless explicitly instructed"
exception AGENTS.md's own rule always allowed for, and `auto` permission mode (which the developer
relies on) matters enough that losing that exception path was not an acceptable trade. Both hook
scripts and their `.claude/settings.json` registration were removed.

Full account, including the trade-off this ran into and what to check before trying again:
`docs/agent-setup-plan.md`, "PreToolUse Hooks For Git Safety: Tried, Then Reverted".
