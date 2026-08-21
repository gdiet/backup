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

**`/plugin` is unavailable in a Claude Desktop WSL session** (confirmed live, both from the agent
side - no such tool - and from the developer's own terminal: `/plugin isn't available in this
environment`) - matches the docs' own "Plugins aren't available in WSL sessions" note for Desktop.
If you are in exactly that setup, skip straight to reporting this and stop; do not keep retrying
`/plugin` variants. Retry the actual install from a Terminal-CLI session or a native-Windows
Desktop session instead, where `/plugin` should work normally.

Where `/plugin` *is* available: run `/plugin marketplace list` and `/plugin list` to see whether a
Rust LSP plugin is already installed and enabled. Also check this repository's
`.claude/settings.json` for an `enabledPlugins` entry naming one, since a project can enable a
plugin for everyone without it being personally installed. Plugin naming may have changed since
this skill was written - as of 2026-08-21 the correct name is `rust-analyzer-lsp` (found via
`~/.claude/plugins/marketplaces/claude-plugins-official/plugins/` on disk, since `/plugin` itself
was unavailable to check this the intended way) - **not** `rust-lsp`, the name used in Claude's own
docs at the time this skill was first written. Check `/plugin marketplace` for the current name if
this one is also gone by the time you read this, rather than assuming no such plugin exists.

## 3. If either is missing

**Always ask the developer for explicit confirmation before installing anything** - every time
this runs, regardless of environment, even if a previous run on a different machine already got a
yes. This touches machine-level tooling (a rustup component, a Claude Code plugin), not just this
repository.

Once confirmed:

```bash
rustup component add rust-analyzer
```

Then install the plugin (`/plugin install rust-analyzer-lsp@claude-plugins-official`, or whatever
name step 2 found to be current) - only where `/plugin` is actually available, per step 2 above.

## 4. Verify, do not just assume

After installing, confirm it actually improves symbol lookups on a real task in this repository
(a go-to-definition or find-references that previously would have needed grep/file reads) before
treating this as done - a successful install command is not the same as a working setup.
