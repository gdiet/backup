# Verify the PowerShell git-safety-guard hook actually fires on native Windows

**Needs**: a real, human-opened Windows terminal (PowerShell or a Claude Code session using the
PowerShell shell tool) - not an agent-spawned WSL shell. From this WSL2 session,
`powershell.exe` is reachable via PATH but every invocation fails with "Exec format error":
`/proc/sys/fs/binfmt_misc/` has no `WSLInterop` registration here, the same limitation already
documented in `agent-todos/done/test-graceful-absence-of-libfuse3-winfsp.md` for a Windows
`.exe`. `cmd.exe`/`powershell.exe` resolve on `PATH` but cannot actually be executed from this
particular agent shell.
**Size**: small (five-minute manual/agent check from a real terminal)
**Opened**: 2026-08-21, by Claude Desktop/WSL2 session.
**Context**: `.claude/hooks/git-safety-guard.ps1`, registered in `.claude/settings.json` under
the `PowerShell` matcher, added because the developer pointed out that sessions on this project
sometimes run directly on native Windows (not just WSL2), where Claude Code's shell tool is
"PowerShell" instead of "Bash" when Git for Windows isn't installed - the existing
`git-safety-guard.sh` (Bash matcher) never fires there. See `docs/agent-setup-plan.md` item 6.

The `.ps1` script mirrors `git-safety-guard.sh`'s checks (force-push, `reset --hard`,
`--no-verify`/`--no-gpg-sign`, `git clean -f...`, `branch -D`, discarding all uncommitted
changes) using PowerShell's `-match` operator instead of `grep -E`, and reads/writes the same
`hookSpecificOutput`/`permissionDecision` JSON shape. It has only been reviewed by reading, never
actually executed - unlike the Bash version, which was piped a dozen sample commands and checked
directly.

To finish this from a real Windows terminal (native PowerShell, or WSL with working interop):

```powershell
$body = '{"tool_input":{"command":"git push --force origin main"}}'
$body | powershell.exe -NoProfile -ExecutionPolicy Bypass -File .claude\hooks\git-safety-guard.ps1
```

Expected: a JSON object with `hookSpecificOutput.permissionDecision` equal to `"escalate"` and a
`permissionDecisionReason` mentioning force-push. Repeat with a harmless command (e.g. `git
status`) and confirm there is no output and exit code 0. Ideally also confirm end-to-end inside
an actual Claude Code session on native Windows (without Git for Windows) by attempting a
force-push and observing that Claude Code prompts for confirmation regardless of permission mode.

Not urgent - the Bash-side hook already covers the WSL2 sessions this project is mostly worked
from; this closes the gap for the native-Windows-without-Git-Bash case specifically.

## Done

**Completed**: 2026-08-21, by a Claude Code session using the PowerShell shell tool directly on
native Windows (the exact environment this TODO needed - no WSL interop involved).

Ran both commands from the TODO verbatim:

```
$body = '{"tool_input":{"command":"git push --force origin main"}}'
$body | powershell.exe -NoProfile -ExecutionPolicy Bypass -File .claude\hooks\git-safety-guard.ps1
```

Output:

```json
{
    "hookSpecificOutput":  {
                               "hookEventName":  "PreToolUse",
                               "permissionDecision":  "escalate",
                               "permissionDecisionReason":  "Matched a hard git-safety rule: force-push (--force/-f) rewrites remote history on a shared origin. Confirm explicitly with the developer before running this - even if a plan or an earlier message already approved the broader task."
                           }
}
```

Then with a harmless command (`git status`): no output, exit code 0 - confirmed via
`$LASTEXITCODE`.

Also checked `.claude/settings.json` and confirmed the `PowerShell` matcher is correctly wired to
invoke `git-safety-guard.ps1` with the same args pattern as the manual test.

Did not attempt the "ideally" end-to-end step (triggering a real force-push attempt through a live
Claude Code session to watch it prompt) - that would mean actually issuing
`git push --force origin main` against this shared repo as a test, which is the exact
hard-to-reverse action the hook exists to gate; not worth the risk when the direct script
invocation already demonstrates the hook's logic fires correctly and matches the JSON shape Claude
Code expects. The escalate/no-op behavior above is sufficient confirmation that the script itself
is correct; whether the harness wires PreToolUse hook JSON output into an actual permission prompt
is harness plumbing already exercised by the Bash-side hook (`git-safety-guard.sh`), which uses the
identical `hookSpecificOutput`/`permissionDecision` shape and is known to work end-to-end.
