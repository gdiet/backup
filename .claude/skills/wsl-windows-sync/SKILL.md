---
name: wsl-windows-sync
description: Sync a Windows-native checkout of this repo with its WSL clone on the same machine, run Linux-specific verification (real FUSE mount tests, cargo build/clippy/test as they'd actually run on Linux) via WSL, or run any single command that spans both a Windows-native shell (PowerShell/cmd) and a WSL/Linux shell (e.g. `wsl.exe ... -- bash -c '...'`, or reaching a WSL-built binary from PowerShell). Use whenever work touches both the Windows and WSL sides of this project, or hits shell-quoting/path oddities that only show up when a command crosses that boundary.
---

# Working Across Windows And WSL

On a machine with both a Windows-native checkout and a WSL clone of this repo, check
`.local/agent-environment.md` first for this environment's actual current paths (which directory
holds which checkout, whether one syncs from the other or both are worked in directly) - do not
assume a specific path or layout; it is machine-specific and can change. If that file does not
have them yet, locate both checkouts (e.g. search for a directory containing this repo's own
`AGENTS.md`, or check a Windows shell's `%USERPROFILE%` for a likely dev-projects folder) and
record what you found there before proceeding, so a later session on this same environment does
not have to rediscover it.

Linux-specific verification (real FUSE mount tests, `cargo build`/`clippy`/`test` as they'd
actually run on Linux) needs the WSL clone's own native filesystem, not the Windows checkout
reached via `/mnt/c/...` - `cargo` through the `/mnt/c` DrvFs/9p bridge is noticeably slower and
can surface filesystem quirks (permissions, locking, `/dev/fuse` access) that do not reflect real
Linux behavior, the whole point of testing on WSL in the first place.

This is one instance of a general rule, not a repo-checkout-specific one: treat `/mnt/c/...` as
read-mostly from a WSL shell - fine for fetching or referencing a file that only exists on the
Windows side, not a place to do real work. Any scratch files, generated output, or actual command
execution from within WSL belongs on WSL's own filesystem (e.g. under `~`), for the same DrvFs/9p
reasons as the repo checkout above - this also holds for work that has nothing to do with this
repository's build/test process, such as ad-hoc measurement scripts or temporary data. A build
artifact that genuinely needs to reach the other side (e.g. a WSL-cross-built `.exe` a native
Windows session has no toolchain to build itself) is the one deliberate exception - copy just that
one file across, through the shared remote's `git fetch`/`push` where the artifact is source-
controlled, or directly via `/mnt/c/...` when it is a gitignored build output.

Keep the two checkouts' git history in sync through the shared remote (`git fetch`/`push`/`merge
--ff-only`), never by copying source files directly. Which checkout is the actual working copy for
a given piece of work - where changes actually get made and committed - is a per-environment
convention, not a fixed rule this skill can state up front: check `.local/agent-environment.md` for
what sessions on this environment have actually been doing, or ask the developer if it is not
recorded there yet.

## Shell quoting across environments

Any single command line that gets parsed by more than one shell in sequence (Git
Bash/MSYS → `wsl.exe` → the WSL-side shell, or the reverse) is a recurring source of
subtle corruption - each layer's quoting/escaping rules differ, and content that's
literal to one layer can be syntax to the next. Two distinct ways this has bitten agents
on this project so far; expect more of the same shape rather than assuming these two are
exhaustive:

- **MSYS path conversion mangles WSL-only paths.** When invoking `wsl` from this (Git
  Bash/MSYS) shell with a WSL-only path (anything under `/home/...`, not `/mnt/c/...`)
  anywhere in the command - e.g. `wsl bash -lc '/home/georg/.../java -version'` - MSYS's
  automatic path conversion can silently mangle it into a bogus Windows path
  (`C:/Program Files/Git/home/...`), failing with a confusing `No such file or
  directory`. `/mnt/c/...` paths are unaffected (MSYS already special-cases them), so
  this only bites when a command touches something that only exists inside WSL, like a
  JRE unpacked under the Linux home directory. Fix: prefix the call with
  `MSYS_NO_PATHCONV=1`, e.g. `MSYS_NO_PATHCONV=1 wsl bash -lc '...'` - or sidestep the
  whole MSYS layer by running the `wsl` call from PowerShell/cmd instead of Git Bash.
- **Multi-layer quoting silently eats special characters, rather than erroring.** A
  command constructed as `wsl.exe -d <distro> -- bash -c '...big multi-line
  string...'` from Git Bash passes through Git Bash's own quoting, then `wsl.exe`'s
  argv-to-command-line reconstruction, then the WSL-side `bash -c` parse - and content
  that's meant to be inert *inside* that string (backticks in a Markdown code fence,
  parens, `$(...)`) can get interpreted as command substitution by one of the
  intermediate layers even when correctly quoted for the innermost shell (e.g. a
  heredoc using `<<"EOF"` to suppress expansion). The failure mode is worse than a
  parse error: the command "succeeds" but silently drops the offending text, so the
  bug surfaces later as corrupted file content, not an obvious crash. Prefer avoiding
  the nested-shell construction entirely for anything beyond a one-liner: write the
  content with a native file-editing tool against the WSL filesystem's UNC path
  (`\\wsl.localhost\<distro>\home\...`, writable from PowerShell/Windows tools) instead
  of embedding it in a quoted string passed through multiple shells. If a WSL-side
  shell command is unavoidable, keep it to simple, single-line, special-character-free
  invocations, or stage a script file first and execute that.

See also `julius-winfsp-ssh`'s own note on multi-layer shell escaping (bash → ssh → cmd
→ powershell) - same root cause, different chain of shells.
