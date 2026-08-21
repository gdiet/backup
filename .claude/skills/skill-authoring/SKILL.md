---
name: skill-authoring
description: Write a new Claude Code skill (SKILL.md) or review/improve an existing one in this repo's .claude/skills/, following current best practices for triggering reliably. Use when creating a .claude/skills/*/SKILL.md, or asked to review, tighten, or fix a skill's description or triggering.
---

# Writing And Reviewing Skills

Verify against current docs before finalizing, not memory - this is a fast-moving product and
exact mechanics (budget sizes, field names) can drift:
[code.claude.com/docs/en/skills](https://code.claude.com/docs/en/skills).

## The `description` decides whether the skill triggers - get this right first

Claude picks a skill by model judgment over `description` (+ `when_to_use`, a separate frontmatter
field appended to it), not keyword/pattern matching. Every skill's description is always in the
candidate pool each turn - "evaluation frequency" isn't the lever; specificity is.

- **Lead with the concrete nouns/verbs a real request would contain** - the specific tool, file
  type, or action - not a generic framing that could apply to several skills at once.
- **Cut generic boilerplate** ("after confirming with the developer", "if needed") - it doesn't
  distinguish this skill from others, and it burns a hard budget: 1,536 characters per skill
  (`description` + `when_to_use` combined), plus a shared listing budget (~1% of the context
  window) that drops least-used skills' descriptions first when it overflows.
- **Move "how it behaves once invoked" into the body**, not the description - the body only loads
  on actual invocation, so it costs nothing until then.
- Triggers too often despite a specific description → `disable-model-invocation: true` (manual
  only, via `/name`). Never triggers → check the description actually contains the words a real
  request would use, per the docs' own troubleshooting section.

## Where a skill belongs vs. the alternatives

- `AGENTS.md`/`CLAUDE.md`: facts/conventions needed in *every* session regardless of task.
- `.claude/rules/*.md` (`paths:` frontmatter): standing conventions scoped to a file-path pattern.
- A skill can also carry its own `paths:` frontmatter field to auto-load only when Claude touches
  matching files - the same mechanism, just packaged as a skill instead of a rule when it's more
  procedure than fact.
- A skill (no `paths:`): a *procedure* needed only occasionally, invoked by name or by relevance
  judgment - not every session.

One fact lives in exactly one of these - don't duplicate the same content across a skill and
`AGENTS.md`/a rule.

## Keep the body itself concise

Once loaded, a skill's content stays in context for the rest of the session - every line is a
recurring cost. State what to do, not why or how it works internally.

## Self-check before committing

- Does the description lead with words the actual request would use?
- Would a generic, unrelated request plausibly also match it? If so, narrow it.
- Is anything in the description purely explanatory rather than trigger-relevant? Move it to the
  body.
