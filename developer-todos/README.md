# Developer TODOs

Items the developer hands over mid-conversation - things worth looking at or acting on later, but
not worth derailing whatever is currently being worked on for. Exists because a note left only in
chat does not survive past that conversation, and the developer would otherwise have to remember to
bring the item up again themselves.

Distinct from `agent-todos/`: those are opened by an agent, blocked on an environment/capability it
does not currently have. These are opened by the developer, not blocked on anything in particular -
just deferred on purpose.

See `AGENTS.md` for the actual instructions on when/how to act on these. Short version: check this
directory when starting work in this repo, same as `agent-todos/`; do small items yourself once
picked up, ask before starting a large one.

## Layout

- `developer-todos/*.md` - open items, one file per task.
- `developer-todos/done/*.md` - finished items, moved here (not deleted) once complete, with a
  short note on what was actually done. Same reasoning as `agent-todos/done/`: a record of what
  happened is more useful to the next agent than silence.

## File format

Filename: a short, descriptive `kebab-case-slug.md`.

```markdown
# <Short title>

**Noted**: <date>, during <brief context - what conversation/task this came up in, if useful>
**Size**: small (no confirmation needed, just do it) | medium/large (confirm with the user first)
**Context**: <link to the relevant design doc, commit, or prior discussion, if any>

<The developer's own wording for the item, plus enough surrounding context for an agent with no
memory of the conversation this came from to actually act on it - the developer's original
phrasing alone may be too terse on its own.>
```

When done, move the file to `done/`, and append what actually happened rather than rewriting the
original description away - the original wording stays useful as a record even after completion.
