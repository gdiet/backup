---
name: wsl-windows-sync
description: Sync the Windows checkout of this repo (backup, rust2/) with its WSL clone, run Linux-specific verification (real FUSE mount tests, cargo build/clippy/test as they'd actually run on Linux) via WSL, or run any single command that spans both a Windows-native shell (PowerShell/cmd) and a WSL/Linux shell (e.g. `wsl.exe ... -- bash -c '...'`, or reaching a WSL-built binary from PowerShell). Use whenever work touches both the Windows and WSL sides of this project, or hits shell-quoting/path oddities that only show up when a command crosses that boundary.
---

# Working Across Windows And WSL

The primary checkout is on the Windows filesystem (this repository's location under
`C:\...`). Linux-specific verification (real FUSE mount tests, `cargo build`/`clippy`/
`test` as they'd actually run on Linux) needs WSL - but run it against the native WSL
clone at `~/git/backup` (Linux `ext4`), not against the Windows checkout reached via
`/mnt/c/...`. `cargo` through the `/mnt/c` DrvFs/9p bridge is noticeably slower and can
surface filesystem quirks (permissions, locking, `/dev/fuse` access) that don't reflect
real Linux behavior - the whole point of testing on WSL in the first place.

Keep the two in sync through the shared remote, never by copying files directly:

- Make and commit all changes in the Windows checkout, same as always.
- After pushing from the Windows checkout, sync `~/git/backup` with:
  `git fetch origin <branch> && git merge --ff-only origin/<branch>`
  (fast-forward only - if that fails, something unexpected changed `~/git/backup`
  itself; investigate rather than force it).
- Verify `git status --short` is clean in `~/git/backup` right after syncing, before
  trusting any build/test output from it.
- Never make or commit changes directly in `~/git/backup` - it exists only to verify
  Linux behavior against what's already pushed, not as a second place to develop. If a
  Linux-only fix is needed, make it in the Windows checkout, push, then sync as above.

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
