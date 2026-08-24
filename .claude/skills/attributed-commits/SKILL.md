---
name: attributed-commits
description: Determine the Generated-By trailer (role - author or co-author), the commit's git Author identity, and the effective git identity, before creating any git commit in this repository. Use before every "git commit" invocation here, not only when the developer mentions attribution explicitly.
---

# Attributing Agent-Authored Commits

This repository is on GitHub (a shared origin - see AGENTS.md's "Relationship To Other
Implementations"), and the developer wants agent-authored work distinguishable from their own.
Whenever you are the sole author or a co-author of a commit, add a trailer alongside the standard
`Co-Authored-By:` trailer (keep that one too - GitHub parses it specifically to show co-author
avatars):

```
Generated-By: <agent-name> (harness: <harness>; model: <model>; role: author|co-author)
```

- `role: co-author` - a human genuinely reviewed *this specific change* before it was committed:
  read the diff/content, asked questions about it, requested revisions, or otherwise engaged with
  what was actually produced. `role: author` - the agent produced the change from a short or
  high-level instruction and no such review happened before commit. Authorizing a commit ("ja",
  "go ahead") is not the same as reviewing its content - the common plan-summary → "ja" →
  implement → verify → commit cycle is `role: author` by default, even though a human is present
  and explicitly permitted the commit (permission to commit is always required either way,
  regardless of role). Only use `co-author` when review of the actual content demonstrably
  happened.
- **Determine this from context you already have - do not ask.** Decide `author` vs. `co-author`
  from what already happened in the conversation; if nothing indicates real review, default
  straight to `author`, silently, with no confirmation question. Only surface a question here if
  the developer's own words already asked for review/confirmation on this change.
- If you do need to show a change for review, do not paste a raw unified diff into chat - present
  a prose summary grouped by file/concern, or point the developer at a proper diff tool.
- `harness` is the interface this session is running through (terminal CLI, Desktop app, web app,
  an IDE extension, etc.) - not reliably inferable, so ask for it rather than guessing, *unless*
  your system prompt already tells you this session is running in a managed, ephemeral remote
  execution environment (a cloud container, reclaimed after inactivity or session end). In that
  case, use `Claude Code on the web` without asking - that names the execution backend, not
  whichever client the developer happens to be viewing the session through (Desktop app, browser,
  mobile, a GitHub Action, ...); the same cloud session can be opened from any of those and the
  harness stays the same.

## Distinct Git Author Identity For Agent-Authored Commits

When `role: author` applies, also make the commit's actual git Author identity reflect that, not
just the trailer: set `GIT_AUTHOR_NAME`/`GIT_AUTHOR_EMAIL` as env vars scoped to that one `git
commit` invocation only, e.g.:

```
GIT_AUTHOR_NAME="Claude Sonnet 5" GIT_AUTHOR_EMAIL="noreply@anthropic.com" git commit -m "..."
```

Use the same name/email already used in the `Co-Authored-By` trailer. Never do this by editing
global (`~/.gitconfig`) or even this repo's local git config - it must only ever apply to the
single `git commit` invocation it is set for. Leave `GIT_COMMITTER_NAME`/`GIT_COMMITTER_EMAIL`
unset so the ambient (human) identity is used for Committer - GitHub then displays "X authored, Y
committed" when the two differ.

When `role: co-author` applies, leave the git Author identity alone (ambient/human) - only the
trailer changes.

**Ask early, not at commit time.** As soon as it looks like a commit will eventually be wanted in
this session, resolve the harness once, before you are mid-commit - either by recognizing the
remote-execution-environment case above, or, otherwise, by asking:

> "Über welche Oberfläche läuft diese Session?" ("Which interface is this session running
> through?") - options along the lines of: Terminal-CLI · Desktop-App · Web-App (claude.ai) ·
> VSCode-Extension · JetBrains-Extension · (something else, free text)

List the IDE options separately, not bundled as "VSCode/JetBrains". The harness describes how this
session actually executes - in a detected remote execution environment that is "Claude Code on the
web" regardless of which client opened or is viewing it; otherwise it is that client itself. Do not
cache the answer anywhere durable either way - just hold onto it for the rest of the current
session once it is resolved.

## Verify Git Identity Before Committing (Privacy)

Cheap and worth doing before any commit: check the effective git identity (`git config user.name` /
`git config user.email`) that commits in this session would actually use.

The developer's human commits on this project should use a privacy-preserving identity like `gdiet
<gdiet@users.noreply.github.com>` unless explicitly requested otherwise - regardless of what a
given machine/environment happens to have configured globally.

If the effective git identity does not match: fix it scoped to this repo only if it is obvious how
(`git config --local user.name "gdiet"` / `git config --local user.email
"gdiet@users.noreply.github.com"`), never touching global config. Only escalate to asking the
developer if something is genuinely ambiguous (e.g. local config already holds a different,
seemingly intentional override).
