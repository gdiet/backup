# Experiment — Not A Real Instructions File

This file is a live experiment, not a real instructions file for `docs/design/`. The actual
conventions for this directory live in [`README.md`](README.md); the repository's real agent
instructions live in [`../../AGENTS.md`](../../AGENTS.md).

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

## Status

Temporary. Once the question above is answered, this file gets deleted or completely rewritten
into whatever the actual convention turns out to be — nothing here should be treated as a lasting
rule.
