# Experiment — Not A Real Instructions File

This file is a live experiment, not a real instructions file for `docs/design/`. The actual
conventions for this directory live in [`../../docs/design/README.md`](../../docs/design/README.md);
the repository's real agent instructions live in [`../../AGENTS.md`](../../AGENTS.md).

## What this tests

Whether this Claude Code harness automatically loads a directory-scoped `AGENTS.md` — the way it
loads the repository-root `AGENTS.md` — into a session's context, simply because that session is
working on files under `docs/design/`, without the developer or the agent having explicitly opened
or read this file.

## If you are reading this

If this file's content is part of your context and neither you nor the developer explicitly opened
or read it in this conversation, please say so to the developer. That confirms directory-scoped
`AGENTS.md` files are picked up automatically here — an open question recorded in
[`../../AGENTS.md`](../../AGENTS.md)'s "Design Documentation" section.

If you only see this file because you (or the developer) explicitly opened, read, or searched for
it, that does not answer the question either way — say that too, rather than assuming either
outcome.

## Why this is being tried

The root `AGENTS.md` grows every time a new topic area needs its own conventions, even when those
conventions only ever matter to someone working inside one specific directory. `docs/design/` is
just the test case for this experiment, not the only place this would apply: if directory-scoped
`AGENTS.md` files do get loaded automatically, the same idea would extend to other directories with
their own growing conventions (`requirements/`, `migration/`, ...), each relieving the root file of
detail nobody outside that directory needs to carry around. If you have confirmed (per "If you are
reading this" above) that this file reached your context automatically, offer to start relieving
the root file — moving `docs/design/`-specific content here, and content for other directories
where it would similarly pay off elsewhere — but only after asking, both for the idea in general
and again before touching any specific directory.

## Status (was: "Temporary")

Closed without the live test ever completing — answered directly by current official
documentation instead (code.claude.com/docs/en/memory, "How CLAUDE.md files load"): Claude Code
never reads a file named `AGENTS.md`, anywhere, regardless of directory. It reads `CLAUDE.md`
only. This is exactly the reason the original `rust2/` (now `rust/`) root `AGENTS.md` was never
auto-loading either, before a root `CLAUDE.md` containing `@AGENTS.md` was added.

The underlying idea this experiment was chasing — relieving the root instructions file by scoping
conventions to the directory they apply to — **does work**, just via a different, already-
documented mechanism: a directory's own `CLAUDE.md` (not `AGENTS.md`) loads automatically "at
launch when started from that directory, or on demand when Claude reads a file there" (see
code.claude.com/docs/en/large-codebases, "Layer CLAUDE.md files by directory"). So does a
`.claude/rules/*.md` file with `paths:` frontmatter scoped to a glob. Splitting `AGENTS.md`'s
content into either of those is tracked as a separate, deliberate piece of work
(`docs/agent-setup-plan.md`, item 7) rather than something this file needed to keep existing for.

## Done

**Completed**: 2026-08-21, by Claude Desktop/WSL2 session. Moved here from `docs/design/AGENTS.md`
(deleted) once the question above was settled by documentation rather than by this file ever
actually being live-tested.
