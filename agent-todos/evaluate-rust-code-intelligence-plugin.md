# Evaluate a Rust code-intelligence plugin

**Needs**: a Terminal-CLI or native-Windows Desktop session - `/plugin` is confirmed unavailable in
a Claude Desktop WSL session (both from the agent side and the developer's own terminal:
`/plugin isn't available in this environment`), matching the docs' "Plugins aren't available in
WSL sessions" note. The `rustup component` half of this item does not have that restriction and is
already done (see below).
**Size**: small
**Opened**: 2026-08-21, by a Claude Desktop/WSL2 session. Carried over from
`docs/agent-setup-plan.md` item 8 when that document was closed out and condensed.
**Context**: [large-codebases.md](https://code.claude.com/docs/en/large-codebases.md#reduce-file-reads-with-code-intelligence) -
a code intelligence plugin connects Claude Code to a language server (LSP) so it can jump to
definitions and find references directly instead of grep/file-read chains, worth it for a
Rust-heavy repository this size. Requires the language's language server binary installed locally.

For Rust, that is `rust-analyzer`. This whole check-and-install procedure is now a skill,
`.claude/skills/rust-code-intelligence/` - the "open question" this file originally carried
(one-off todo vs. repeatable skill) is resolved in favor of the skill, confirmed useful the very
first time it ran: it caught that the plugin's name had changed (`rust-lsp` → `rust-analyzer-lsp`)
and, this session, the WSL/Desktop `/plugin` unavailability documented in **Needs** above.

## Status (2026-08-21)

- **`rust-analyzer`**: installed on this machine (`rustup component add rust-analyzer`), confirmed
  working (`rust-analyzer --version` → `1.97.0`).
- **`rust-analyzer-lsp` plugin**: not installed - blocked by this environment's `/plugin`
  unavailability (see **Needs** above), not by anything specific to this repository. Pick this
  file back up in a Terminal-CLI or native-Windows session: run
  `/plugin install rust-analyzer-lsp@claude-plugins-official` (the marketplace itself,
  `claude-plugins-official`, is already configured locally). Confirm it actually improves symbol
  lookups on a real task in this repo before treating this as fully done, not just that the install
  command succeeded.
