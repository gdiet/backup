---
name: wsl-windows-sync
description: Sync the Windows checkout of this repo (backup, rust/) with its WSL clone, or run Linux-specific verification (real FUSE mount tests, cargo build/clippy/test as they'd actually run on Linux) via WSL. Use whenever work touches both the Windows and WSL sides of this project, or needs Linux-native verification of a change made in the Windows checkout.
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

When invoking `wsl` from this (Git Bash/MSYS) shell with a WSL-only path (anything
under `/home/...`, not `/mnt/c/...`) anywhere in the command - e.g.
`wsl bash -lc '/home/georg/.../java -version'` - MSYS's automatic path conversion can
silently mangle it into a bogus Windows path (`C:/Program Files/Git/home/...`), failing
with a confusing `No such file or directory`. `/mnt/c/...` paths are unaffected (MSYS
already special-cases them), so this only bites when a command touches something that
only exists inside WSL, like a JRE unpacked under the Linux home directory. Fix: prefix
the call with `MSYS_NO_PATHCONV=1`, e.g. `MSYS_NO_PATHCONV=1 wsl bash -lc '...'`.
