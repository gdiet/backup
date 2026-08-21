# Evaluate a Rust code-intelligence plugin

**Needs**: a Terminal-CLI session, specifically. `/plugin` is now confirmed unavailable in the
Claude Desktop app on *both* WSL and native Windows (see 2026-08-21 update below) - this looks like
a Desktop-app-wide gap, not a WSL-specific one as first suspected. A Terminal-CLI session has not
yet been tried; that is the next thing to actually check, not an assumed fallback. The `rustup
component` half of this item does not have this restriction and is done on both machines tried so
far (see below).
**Size**: small
**Opened**: 2026-08-21, by a Claude Desktop/WSL2 session. Carried over from
`docs/agent-setup-plan.md` item 8 when that document was closed out and condensed.
**Context**: [large-codebases.md](https://code.claude.com/docs/en/large-codebases.md#reduce-file-reads-with-code-intelligence) -
a code intelligence plugin connects Claude Code to a language server (LSP) so it can jump to
definitions and find references directly instead of grep/file-read chains, worth it for a
Rust-heavy repository this size. Requires the language's language server binary installed locally.

For Rust, that is `rust-analyzer`. This whole check-and-install procedure is now a skill,
`.claude/skills/rust-code-intelligence/` - the "open question" this file originally carried
(one-off todo vs. repeatable skill) is resolved in favor of the skill, confirmed useful every time
it has run so far: it caught that the plugin's name had changed (`rust-lsp` → `rust-analyzer-lsp`),
the WSL `/plugin` unavailability, and now the native-Windows `/plugin` unavailability too.

## Status (2026-08-21, WSL2 session)

- **`rust-analyzer`**: installed on that machine's WSL2 side (`rustup component add
  rust-analyzer`), confirmed working (`rust-analyzer --version` → `1.97.0`).
- **`rust-analyzer-lsp` plugin**: not installed - blocked by that session's `/plugin`
  unavailability, believed at the time to be WSL-specific.

## Status (2026-08-21, native-Windows Desktop-App session, later the same day)

- **`/plugin` finding generalized**: also unavailable here, on native Windows, not just WSL. A
  `ToolSearch` for the plugin-marketplace mechanism only surfaces `ListPlugins`/`SearchPlugins`/
  `SuggestPluginInstall` - the unrelated claude.ai connector-plugin catalog - and
  `~/.claude/plugins/` does not exist on disk at all on this machine. No tool corresponding to
  `/plugin marketplace`/`/plugin install` exists in this harness. This rules out "WSL" as the
  specific cause and points to "Claude Desktop app" (either platform) instead - matches the pattern
  of other terminal-only slash commands (`/permissions`, `/config`, `/doctor`, `/hooks`) that are
  documented as unavailable outside a Terminal-CLI session.
- **`rust-analyzer`**: installed on this (separate, native Windows) machine/toolchain too
  (`rustup component add rust-analyzer`), confirmed working (`rust-analyzer --version` →
  `1.97.0`). Installed with the developer's explicit confirmation, per the skill's "always ask"
  rule, even though the WSL side already had a yes.
- **`rust-analyzer-lsp` plugin**: still not installed, for the same reason as the WSL session -
  `/plugin` unavailable, this time confirmed to be independent of WSL vs. native Windows.
- **Verification (skill step 4)**: not possible here either, and not attempted as if it were -
  installing the `rust-analyzer` binary alone does not connect it to Claude Code, so there was
  nothing to verify a symbol lookup against.
- **Remaining open question**: does `/plugin` actually work from a Terminal-CLI session (as
  opposed to the Desktop app)? Neither session tried has been a Terminal-CLI session; this is the
  next environment to actually test, not something to assume works from the docs alone.
