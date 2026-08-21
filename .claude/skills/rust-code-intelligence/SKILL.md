---
name: rust-code-intelligence
description: Check whether this machine has a working Rust code-intelligence setup (the rust-analyzer language server plus a Claude Code LSP plugin), and set it up if not - after confirming with the developer, since installing anything here touches machine-level tooling, not just this repository. Use when starting Rust work in a new environment, when asked to check/install/set up rust-analyzer or code intelligence, or when symbol lookups (go to definition, find references) seem to be falling back to grep/file reads instead of the language server.
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

Run `/plugin marketplace list` and `/plugin list` (or equivalent - check what your Claude Code
version actually offers) to see whether a Rust LSP plugin is already installed and enabled. Also
check this repository's `.claude/settings.json` for an `enabledPlugins` entry naming a Rust LSP
plugin, since a project can enable one for everyone without it being personally installed. Plugin
naming may have changed since this skill was written - if `rust-lsp@claude-plugins-official`
(the name used in Claude's own docs at the time) is not found, check `/plugin marketplace` for the
current equivalent rather than assuming it does not exist.

## 3. If either is missing

**Always ask the developer for explicit confirmation before installing anything** - every time
this runs, regardless of environment, even if a previous run on a different machine already got a
yes. This touches machine-level tooling (a rustup component, a Claude Code plugin), not just this
repository.

Once confirmed:

```bash
rustup component add rust-analyzer
```

Then install the plugin (`/plugin install <name>@<marketplace>`, using whatever step 2 found to be
current), adding the marketplace first with `/plugin marketplace add ...` if it is not already
configured.

## 4. Verify, do not just assume

After installing, confirm it actually improves symbol lookups on a real task in this repository
(a go-to-definition or find-references that previously would have needed grep/file reads) before
treating this as done - a successful install command is not the same as a working setup.
