# Revisit whether to re-run fewer-permission-prompts

**Noted**: 2026-08-21, during a session that ran the `fewer-permission-prompts` skill for the
first time (commit `bdda1f17`, `.claude/settings.json`'s `permissions.allow`).
**Size**: small (re-run the skill, compare against what's already allowlisted, ask the developer
about anything new)
**Context**: developer asked whether Claude proactively suggests re-running this, or whether the
initiative always has to come from them - answer at the time: no automatic background re-check
exists, so this file is the deliberate stand-in for that until/unless the developer sets up a
`/schedule` routine for it instead.

The developer's own wording: "schreib mal ein developer-todo, zur Vorlage in einem Monat" - bring
this up again in about a month (around 2026-09-21), not before, and not left indefinitely either.

When picked up: re-run the `fewer-permission-prompts` skill, compare the resulting candidate list
against what's already in `.claude/settings.json`'s `permissions.allow`, and ask the developer
about anything genuinely new (new recurring commands, not just the same cargo/git patterns already
allowlisted). If nothing has meaningfully changed, say so briefly and move this file to `done/`
rather than re-opening it again immediately.
