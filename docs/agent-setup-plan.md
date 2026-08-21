# Claude Code Agent Setup: Findings And Plan

Audits the current Claude Code setup for this project (Desktop app on Windows, session running
inside WSL2, working directory typically `rust2/` inside a parent directory holding several
independent sibling repositories) against current official documentation
(code.claude.com/docs), and lays out what to change. Written 2026-08-21; verify against current
docs before acting, since this is a fast-moving product.

Status: plan - not yet executed beyond what this document itself records.

## The central finding

**The session's actual root is `~/privat/bdev/` (the multi-repo parent), not `rust2/`.** The
original environment block at the start of this conversation reported the primary working
directory as `~/privat/bdev`, not `rust2/` - every command since has needed an explicit `cd` or
absolute path into `rust2/`. `~/privat/bdev/` is not itself a git repository (confirmed:
`git rev-parse --is-inside-work-tree` fails there), but it does hold a plain, ungit-tracked
`~/privat/bdev/CLAUDE.md` (9,962 bytes, last modified 2026-07-29 - before `rust2/` existed).

This one fact explains most of the other findings below:

- Claude Code walks up the directory tree from the working directory, loading every `CLAUDE.md`
  it finds along the way, at launch (see "How CLAUDE.md files load" in
  [memory.md](https://code.claude.com/docs/en/memory.md)). With the session rooted at
  `~/privat/bdev/`, that root `CLAUDE.md` is the file actually driving every session by default -
  and it is stale: it lists `rust/`, `go/`, `scala/`, `backup-repository/` as the workspace,
  never mentions `rust2/` at all, and states "Work happens in `rust/` unless told otherwise",
  which is no longer true.
- `rust2/AGENTS.md` (24KB) is a subdirectory file relative to that root, so it is not
  auto-loaded at all by the mechanism above - and separately, Claude Code's native memory
  system reads `CLAUDE.md`, never `AGENTS.md`, with no fallback (confirmed directly against
  current docs, not assumed). Its content has only ever entered this conversation because it was
  read explicitly, not because any built-in mechanism surfaced it.
- The `docs/design/AGENTS.md` live experiment (testing whether a directory-scoped `AGENTS.md`
  auto-loads) cannot succeed as designed, for the same reason: Claude Code does not read files
  named `AGENTS.md` anywhere, regardless of directory. See "Immediate fix" below.

## Question 1: Desktop app (Windows) + WSL2 session, improved step by step

Grounded in [desktop-wsl.md](https://code.claude.com/docs/en/desktop-wsl.md) and
[large-codebases.md](https://code.claude.com/docs/en/large-codebases.md).

1. **Start sessions from `rust2/` directly**, not from `~/privat/bdev/`. In the Desktop app's
   folder picker (WSL section), browse to and select `rust2/` itself. This is the single highest-
   leverage change - see "The central finding" above.
2. **Confirm the repository lives inside the WSL distribution's own filesystem**
   (e.g. `/home/georg/privat/bdev/rust2`, not a Windows path reached through
   `\\wsl.localhost\...` or `/mnt/c/...`). Working across that boundary is slow and breaks file
   watching per the docs; a quick `pwd`/`wslpath` check confirms this is already the case here.
3. **Know the current WSL-session feature gaps**: no integrated terminal, no connectors/plugins,
   no session forking, no file browser pane, no `@`-file suggestions in the composer (as of the
   docs snapshot above). The existing `wsl-windows-sync` skill already compensates for the
   shell-crossing case; this is a reason to keep it, not a bug to fix.
3. **Reduce permission-prompt friction**: the `fewer-permission-prompts` skill (already available
   in this environment) scans transcripts for common read-only Bash/MCP calls and writes a
   prioritized allowlist to `.claude/settings.json`. Cheap, concrete, worth running now and
   periodically thereafter.
4. **Convert the currently-prose-only safety rules into real enforcement.** AGENTS.md's Git
   Safety Protocol (never force-push, never skip hooks, never stage secrets, staging discipline)
   is, today, only ever advisory: per
   [memory.md](https://code.claude.com/docs/en/memory.md), "Claude treats \[CLAUDE.md/AGENTS.md]
   as context, not enforced configuration... To block an action regardless of what Claude decides,
   use a PreToolUse hook instead." A `PreToolUse` hook matching `git push --force`,
   `git reset --hard`, `--no-verify`, etc. would make the most safety-critical of these rules
   hold even if an instruction is missed or misread, rather than resting on the model reading and
   following prose correctly every time.
5. **Consider a Rust code-intelligence plugin** (`rust-lsp` or equivalent from the official
   plugin marketplace, see [large-codebases.md](https://code.claude.com/docs/en/large-codebases.md#reduce-file-reads-with-code-intelligence)) -
   symbol lookup via the language server instead of grep/file-read chains, worth it for a
   Rust-heavy repository this size. Needs the language server binary installed locally; verify
   before installing.
6. **Run `/doctor`'s CLAUDE.md-trim check once a `CLAUDE.md` exists** (see Question 3) to catch
   content the tool can already derive from the codebase (directory layouts, dependency lists)
   versus content actually worth keeping (pitfalls, rationale, conventions).

## Question 2: The multi-repo workspace

Largely resolved by "The central finding" above, not an independent problem: once sessions start
from `rust2/` directly, the sibling repositories (`rust/`, `go/`, `scala/`,
`backup-repository/`) stop being part of the loaded context by default. No dedicated official
guidance exists for "several independent sibling git repos in one parent directory" specifically
(the documented patterns - monorepo per-package layering, `additionalDirectories`/`--add-dir` -
are written for monorepo packages or deliberate one-off cross-repo access, not for this exact
shape), but they compose correctly once the starting directory is right:

- **Default**: start from `rust2/`. `rust2/`'s own `CLAUDE.md` (once it exists, see Question 3)
  loads; nothing from the sibling repositories does.
- **Deliberate cross-repo work** (e.g. comparing `rust2/`'s schema against `rust/db`'s, as this
  session did extensively): grant access explicitly and temporarily, either
  `claude --add-dir ../rust` at launch, or `/add-dir ../rust` mid-session, rather than starting
  from the shared parent by default. Neither loads `rust/`'s own memory files unless
  `CLAUDE_CODE_ADDITIONAL_DIRECTORIES_CLAUDE_MD=1` is also set - reasonable default, since
  `rust/`'s own conventions are not this project's conventions (see AGENTS.md's "This Is A
  Rewrite, Not A Port" and "Relationship To Other Implementations").
- **Fix the stale root file regardless**: `~/privat/bdev/CLAUDE.md` still exists and would load
  for any session that does start from the parent (deliberately or by habit) - worth correcting
  it to mention `rust2/` and its actual status, even if it stops being the default session root.
  This file is machine-local and outside `rust2/`'s own git history, so it is not something this
  plan can commit for you; flagged here as an action item instead of a "done" checkbox in the
  summary below.

## Question 3: Splitting AGENTS.md into scoped files

Now answerable directly from documentation rather than only from the (currently moot, see below)
live experiment. Grounded in
[memory.md](https://code.claude.com/docs/en/memory.md) and
[large-codebases.md](https://code.claude.com/docs/en/large-codebases.md).

**Confirmed**: a subdirectory's own `CLAUDE.md` loads on demand - "at launch when started from
that directory, or on demand when Claude reads a file there" - and content from sibling
subdirectories' `CLAUDE.md` files never enters context. This is real, documented,
production-supported behavior, not something needing further live testing.

**Immediate fix (independent of everything else on this page)**: `rust2/AGENTS.md` needs a
`rust2/CLAUDE.md` for any of it to load at all. Two documented options:

- `CLAUDE.md` containing `@AGENTS.md` (import syntax), with Claude-specific additions below it if
  ever needed. Works everywhere, including native Windows sessions.
- A symlink, `ln -s AGENTS.md CLAUDE.md`. Equivalent, but needs Administrator privileges or
  Developer Mode to create on native Windows - the import is the safer default here given this
  project is worked on from more than one environment.

Recommend the import. This alone makes `AGENTS.md`'s content actually auto-load for the first
time - independent of whether it is later split further.

**Worth doing regardless of the split question**: `rust2/AGENTS.md` is roughly 24KB, well past
the documented "target under 200 lines per CLAUDE.md file" guidance ("Longer files consume more
context and reduce adherence"). This is a second, independent motivation to split it beyond the
"reduce context overhead for scoped tasks" framing in the original question - the file may
already be too large for reliable adherence even in sessions that use all of it.

**Two mechanisms, not mutually exclusive, choose per case**:

| | Per-directory `CLAUDE.md` | `.claude/rules/*.md` with `paths:` frontmatter |
|---|---|---|
| File lives | Inside the target directory, alongside its code | Centrally under `rust2/.claude/rules/` |
| Loads when | Launched from that directory, or Claude reads a file there | Claude touches a file matching the glob |
| Best for | A whole subtree with its own concerns (e.g. `mountfs/`) | A cross-cutting concern tied to file *type* rather than location (e.g. anything under `requirements/functional/*.md`) |

A first-pass split for this repository, based on what AGENTS.md currently contains (subject to
revision once actually attempted):

- **Root `CLAUDE.md`** (imports `AGENTS.md` for now, or replaces it if the import proves
  sufficient): universal conventions that apply regardless of what is being touched - commit
  policy, dependency policy, "This Is A Rewrite, Not A Port", "Relationship To Other
  Implementations", verification-command summary, Interaction-With-The-Developer conventions.
- **`.claude/rules/requirements.md`**, `paths: ["requirements/**/*.md"]`: the REQ-ID scheme,
  status values, "self-check before adding a sentence" guidance - currently in AGENTS.md's
  "Requirements Documentation" section.
- **`.claude/rules/design-docs.md`**, `paths: ["docs/design/**/*.md"]`: the DESIGN-ID scheme,
  code-citation guidance - currently AGENTS.md's "Design Documentation" section.
- **`mountfs/CLAUDE.md`**: WinFSP/libfuse3-specific conventions, cross-platform verification
  steps - currently scattered through AGENTS.md's "Working Across Environments" and referenced
  design docs.
- Content that is really "how to do a specific, occasional task" rather than a standing
  convention (git-author-identity setup, the `Generated-By`/`Co-Authored-By` trailer mechanics)
  is arguably better as a [skill](https://code.claude.com/docs/en/skills) than a rule or
  `CLAUDE.md` entry - it loads only when actually committing, rather than in every session.

**Close the live experiment**: `docs/design/AGENTS.md` was testing something Claude Code cannot
do (auto-load a file named `AGENTS.md`, anywhere). Once the `CLAUDE.md` import above exists,
retire that experiment file - move it to `agent-todos/done/` or delete it, with a note that the
question was answered by documentation, not by the live test completing. Do not silently repeat
the same experiment with `CLAUDE.md` instead; the docs already answer it directly (see
"Confirmed" above).

## Summary: what to do, and where

| # | Item | Effort | Where |
|---|---|---|---|
| 1 | Add `rust2/CLAUDE.md` with `@AGENTS.md` | Trivial | This session, `rust2/` - can commit now |
| 2 | Close `docs/design/AGENTS.md` experiment, record the documented answer | Trivial | This session, `rust2/` |
| 3 | Always start sessions from `rust2/`, not `~/privat/bdev/` | No file change - a habit/folder-picker choice | Every future session |
| 4 | Fix `~/privat/bdev/CLAUDE.md` (mentions `rust2/`, corrects the `rust/`-is-active claim) | Small | Outside `rust2/`'s git history - do by hand or ask explicitly |
| 5 | Run the `fewer-permission-prompts` skill | Small | This session or any `rust2/` session |
| 6 | Add `PreToolUse` hooks for the hard safety rules (force-push, `--no-verify`, `reset --hard`) | Medium | This session, `rust2/.claude/settings.json` - worth a dedicated pass, not bundled silently into this one |
| 7 | Split `AGENTS.md` into root `CLAUDE.md` + `.claude/rules/*.md` + possibly `mountfs/CLAUDE.md` | Medium-large | This session or a follow-up - worth doing deliberately, not as a drive-by edit |
| 8 | Evaluate a Rust code-intelligence plugin | Small, but needs a local LSP binary | Confirm feasibility first |
| 9 | Confirm `rust2/` sits inside WSL's own filesystem, not reached across the Windows/WSL boundary | Trivial check | Any session |

Items 1-2 are safe, mechanical, and independently valuable regardless of what happens with items
6-7 - candidates to do immediately if the developer agrees. Items 6 and 7 change how the agent is
instructed going forward and deserve their own focused pass rather than being folded into this
audit.

## Prompts for a separate session (to verify item 3 and the `CLAUDE.md` fix independently)

This session's context already contains all of `AGENTS.md`'s content from having read it
directly, so it cannot cleanly demonstrate whether a *fresh* session picks it up automatically.
Two prompts for a new session (opened via the folder picker at `rust2/` directly, per item 3) to
verify independently, after item 1 is committed:

**A. Confirm `CLAUDE.md` loads without any prior context**:
> Run `/context` and tell me exactly which memory files are listed as loaded, and paste the first
> few lines of whichever one traces back to AGENTS.md.

**B. Confirm the sibling-repository boundary holds**:
> Without me telling you anything about the repository layout, tell me what you already know
> about this project's relationship to any sibling directories, and whether you have read
> anything outside the current working directory yet.
