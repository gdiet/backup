---
name: rust-code-intelligence
description: Check and set up rust-analyzer plus a Claude Code Rust LSP plugin for code intelligence (go to definition, find references) on this machine. Use for dev environment setup or new-machine setup in this repo, when asked to check/install/set up rust-analyzer or code intelligence, or when symbol lookups seem to be falling back to grep/file reads instead of the language server.
---

# Rust Code Intelligence Setup

This project is Rust-heavy enough that a language-server-backed code intelligence plugin is worth
having (see [large-codebases.md](https://code.claude.com/docs/en/large-codebases.md#reduce-file-reads-with-code-intelligence)) -
symbol lookups go through `rust-analyzer` directly instead of grep/file-read chains. The
availability check below is inherently per-machine - run it fresh on each environment rather than
assuming an earlier finding (from this machine or another) still holds.

## 1. Check `rust-analyzer`

```bash
rustup component list | grep rust-analyzer
```

Look for the `(installed)` suffix. If it is missing, a `rust-analyzer` shim may still exist on
`PATH` (rustup places one there regardless) and will fail with `error: Unknown binary
'rust-analyzer' in official toolchain ...` if actually run - that error means "not installed",
not a real problem with the binary.

## 2. Check the Claude Code plugin

**`/plugin` is never callable by the agent itself, in any environment - Desktop app (WSL or native
Windows), VSCode extension, or a genuine Terminal-CLI session** - confirmed live in all four
(2026-08-21). `ToolSearch` for "plugin marketplace install", "plugin", or "marketplace" turns up
nothing in any of them (the native-Windows Desktop app is a partial exception: it surfaces
`ListPlugins`/`SearchPlugins`/`SuggestPluginInstall`, but that is the unrelated claude.ai
connector-plugin catalog, not a `/plugin`-equivalent). `/plugin` is a client-side REPL affordance
that the terminal intercepts from human keystrokes before it ever becomes a message to the model -
architecturally, not an agent-callable tool or action anywhere, not a session-type restriction that
a "more terminal-like" environment lifts.

The practical difference a genuine Terminal-CLI session *does* make: a human can be sitting at that
same terminal and type `/plugin install <name>@<marketplace>` themselves, interactively - and that
actually works and hot-loads into the running session immediately, no restart needed (confirmed:
after the developer typed `/plugin install rust-analyzer-lsp@claude-plugins-official` in this
session, a subsequent `ToolSearch` in the same turn found a new `LSP` tool). So: **if you are in a
genuine Terminal-CLI session and a human is present, ask them to type the `/plugin` command
themselves** rather than looking for a way to do it yourself - there isn't one. In the Desktop app
or the VSCode extension, report the `/plugin` unavailability and stop; do not keep retrying
`/plugin` variants (whether a GUI equivalent exists for a human in those two has not been checked
and is out of scope here - this skill only covers the agent-facing path).

There is also a separate, non-interactive escape hatch: the `claude` CLI binary itself (if on
`PATH`) has a `plugin` subcommand (`claude plugin install/list/marketplace ...`) that works
standalone via a plain shell command, in any environment where the binary is reachable. It runs as
a separate process and will not hot-load into, or become verifiable from, the session that invoked
it - useful only to prepare a *future* session, not to satisfy step 4 of this checklist, and (per
"always ask" in step 3) not to be used without asking first even for that.

Check whether the plugin is already installed: run `/plugin marketplace list` and `/plugin list`
(yourself if available - it never is - or ask the human to, or read `claude plugin list` via
`Bash` as a same-effect substitute for this read-only check specifically) to see whether a Rust LSP
plugin is already installed and enabled. Also check this repository's `.claude/settings.json` for
an `enabledPlugins` entry naming one, since a project can enable a plugin for everyone without it
being personally installed. Plugin naming may have changed since this skill was written - as of
2026-08-21 the correct name is `rust-analyzer-lsp` (found via
`~/.claude/plugins/marketplaces/claude-plugins-official/plugins/` on disk) - **not** `rust-lsp`,
the name used in Claude's own docs at the time this skill was first written. Check `/plugin
marketplace` (or `claude plugin marketplace list`) for the current name if this one is also gone by
the time you read this, rather than assuming no such plugin exists.

## 3. If either is missing

**Always ask the developer for explicit confirmation before installing anything** - every time
this runs, regardless of environment, even if a previous run on a different machine already got a
yes. This touches machine-level tooling (a rustup component, a Claude Code plugin), not just this
repository.

Once confirmed:

```bash
rustup component add rust-analyzer
```

Then get the plugin installed - in a genuine Terminal-CLI session, by asking the developer to type
`/plugin install rust-analyzer-lsp@claude-plugins-official` (or whatever name step 2 found to be
current) themselves, per step 2 above; there is no path for the agent to do this directly in any
environment.

## 4. Verify, do not just assume

After installing, confirm it actually improves symbol lookups on a real task in this repository
(a go-to-definition or find-references that previously would have needed grep/file reads) before
treating this as done - a successful install command is not the same as a working setup. Concretely,
run a few `LSP` operations against a real symbol in this repo (`hover` and `workspaceSymbol` are
reliable immediately after install; `findReferences` right after a fresh install may briefly return
"no references found" while rust-analyzer finishes indexing the workspace - retry after a moment
rather than concluding it is broken) and confirm the results are real (type layout info, doc
comments, actual reference lists) rather than an error or an empty fallback.

If the plugin could not be installed (no human available in a genuine Terminal-CLI session to type
`/plugin install`), this verification is not possible in Claude Code itself, regardless of whether
`rustup component add rust-analyzer` succeeded - installing the binary alone does not connect it to
Claude Code. Say so plainly rather than treating a successful `rustup component add` as if it
verified anything about Claude Code's own symbol lookups; the binary may still be worth installing
for other tooling (e.g. an editor's own `rust-analyzer` extension) even though this half of the
check stays open.
