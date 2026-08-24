# Agent TODOs

Tasks that came up during work on this project but were parked instead of acted on immediately -
either because they need an environment/capability the agent that found them didn't have available
at the time (a different OS, real hardware (WinFSP, a real console/terminal), network access to a
specific machine, and so on), or because they came up unprompted while working on something else
and were genuinely outside that task's scope. Exists so neither case gets silently dropped, or
survives only as a passing chat remark that is gone once the conversation ends. The
environment-gap case also reflects that this project is worked on from more than one environment
at least occasionally (see `AGENTS.md`'s "Working Across Environments") - an agent in one
environment can hit a wall that's trivial for an agent (or the same agent, later) in another.

See `AGENTS.md` for the actual instructions on when/how to act on these. Short version: check this
directory when working in this repo; do small items yourself right away; ask before starting a
large one.

## Layout

- `agent-todos/*.md` - open items, one file per task.
- `agent-todos/done/*.md` - finished items, moved here (not deleted) once complete, with a short
  note on what was actually done and by which environment/agent. Mirrors `docs/design/`'s
  draft/`implemented/` convention in this repo, for the same reason: a record of what happened is
  more useful to the next agent (in this or another environment, possibly hours or days later)
  than silence - deleting on completion would just make a different agent re-check or re-discover
  the same thing.

## File format

Filename: a short, descriptive `kebab-case-slug.md`.

```markdown
# <Short title>

**Why parked**: <either the specific environment/capability this requires, and why - be concrete,
not just "Windows" but "Windows with WinFSP installed and a real, user-opened interactive
terminal" - or, for an out-of-scope finding, what task it came up during and why it didn't belong
there>
**Size**: small (no confirmation needed, just do it) | medium/large (confirm with the user first)
**Opened**: <date>, by <environment/session that found it, e.g. "Linux/WSL2 session">
**Context**: <link to the relevant design doc, commit, or prior discussion, if any>

<Description of the task - what needs doing and why, enough for an agent with no prior context on
this specific task to act on it.>
```

When done, move the file to `done/`, and append what actually happened (what was done, any
findings, the commit if applicable) rather than rewriting the original description away - the
"why parked" framing stays useful as a record even after completion.
