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

**`/plugin` is unavailable in the Claude Desktop app (WSL or native Windows) and in the VSCode
extension alike** - confirmed live in all three. The WSL Desktop case matches the docs' own
"Plugins aren't available in WSL sessions" note; the native-Windows Desktop and VSCode-extension
cases do not match any documented restriction, so this looks like a "not a Terminal-CLI session"
gap rather than one tied to a specific platform or app. Concretely, in none of the three is there a
`/plugin`-equivalent tool - a `ToolSearch` for "plugin marketplace install" finds nothing at all in
the VSCode extension, and in the native-Windows Desktop app turns up only
`ListPlugins`/`SearchPlugins`/`SuggestPluginInstall`, the unrelated claude.ai connector-plugin
catalog. `~/.claude/plugins/` may or may not exist on disk depending on whether a Terminal-CLI
session on that same machine has already added a marketplace there (it existed in the VSCode-
extension case below, on a machine where a prior WSL Desktop session had already run
`rustup component add rust-analyzer` - but that does not mean `/plugin` itself became available;
plugin state on disk and the in-session `/plugin` tool are independent). If you are in a Desktop
session or the VSCode extension, skip straight to reporting this and stop; do not keep retrying
`/plugin` variants. There is a separate, non-interactive escape hatch worth knowing about but not
using without asking first: the `claude` CLI binary itself (if on `PATH`) has a `plugin` subcommand
(`claude plugin install/list/marketplace ...`) that works standalone via a plain shell command, in
any environment where the binary is reachable - confirmed working via `Bash` in the VSCode-extension
case below. This does not substitute for the in-session `/plugin` tool: it runs as a separate
process and will not hot-load into, or become verifiable from, the session that invoked it (plugins
load at session start) - useful only to prepare a future Terminal-CLI session, not to satisfy step 4
of this checklist. Retry the actual in-session install from a genuine Terminal-CLI session instead,
where `/plugin` is expected to work - this has not actually been confirmed yet in this repository as
of this writing; treat it as the next thing to verify, not an established fact.

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

If `/plugin` is unavailable (step 2), this verification is not possible in Claude Code itself,
regardless of whether `rustup component add rust-analyzer` succeeded - installing the binary alone
does not connect it to Claude Code. Say so plainly rather than treating a successful
`rustup component add` as if it verified anything about Claude Code's own symbol lookups; the
binary may still be worth installing for other tooling (e.g. an editor's own `rust-analyzer`
extension) even though this half of the check stays open.
