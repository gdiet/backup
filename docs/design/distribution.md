# Distribution And Default Repository Location

## DESIGN-CLI-003: Distributed as a portable download, not an installed package

Status: decided

`dfs` is obtained as a plain, self-contained binary download and run directly from wherever the
operator places it, rather than through a package manager or an installer that puts it in a fixed,
often system-owned location (`Program Files`, `/usr/...`, a `cargo install` toolchain directory). A
single, self-contained Rust binary needs no accompanying runtime or library directory alongside it
to make this work, so it can sit directly next to a repository - no wrapping app-folder structure
required, which is exactly what DESIGN-CLI-004 below relies on.

This directly enables REQ-CLI-006 in
[`../../requirements/functional/cli-commands.md`](../../requirements/functional/cli-commands.md)'s
default repository location, and matches a common, still-current personal-backup pattern: the
executable lives alongside the repository itself, often on the same removable/external drive the
repository is stored on, and travels with it between machines - REQ-OPERABILITY-002 in
[`../../requirements/non-functional/operability.md`](../../requirements/non-functional/operability.md)'s
"repository is portable, sync-able, machine-independent" goal, extended to the tool that manages
it.

Where the same repository (and therefore the same drive) may move between a Windows and a Linux
machine, bundling both platform builds together in one download is worth doing deliberately -
whichever machine the drive is currently plugged into then already has the right binary at hand,
without a separate platform-specific download.

### Alternative considered and rejected: an installer or system package (an MSI, a Linux distro package, `cargo install` as the recommended path)

Rejected as the primary, recommended distribution form: places the executable in a fixed,
often-unwritable system location unrelated to any particular repository, which would make
REQ-CLI-006's default either meaningless or actively broken (permission denied) for an operator
following the recommended install path. Nothing stops an operator from installing it that way
regardless - REQ-CLI-006's own fallback (a clear, actionable error, never a default silently
pointed at an unwritable location) covers that case without needing to prevent it outright.

## DESIGN-CLI-004: Default repository location - a `dedupfs-repository` sibling of the executable

Status: decided

REQ-CLI-006's default resolves via `std::env::current_exe()`, then that path's parent directory,
then a `dedupfs-repository` child of it - not the current working directory the command happens to
be invoked from, and not a subdirectory one level further up (the executable itself needs no
wrapping app folder - DESIGN-CLI-003 above - so the repository sits directly beside it).

### Alternative considered and rejected: relative to the current working directory

The assumption a portable, double-click-launched application can lean on - its working directory
is reliably wherever its own launcher sits - does not hold for a terminal-invoked CLI: `dfs` can be
run from any shell session, in any directory, so a working-directory-relative default would point
at a different, essentially arbitrary location depending on where the operator happened to be
`cd`'d into at the time, rather than reliably at "wherever `dfs` itself is".

### Alternative considered and rejected: an OS-appropriate application-data directory

Predictable and working-directory-independent, but semantically wrong for what a repository
actually is: the operator's real backed-up data, not application configuration or cache. Many other
backup tools deliberately exclude exactly this kind of OS-managed application-data location from
their own scope, and an operator checking a repository's size or moving it to different storage
would not intuitively look there either. It also does not support DESIGN-CLI-003's
travels-with-the-drive pattern at all, since it is tied to whichever machine's OS profile currently
holds it, not to the executable's own location.

### Not writable, or not actually holding a repository

Already REQ-CLI-006's own concern, not a gap this design leaves open: the command fails with a
clear, actionable message (REQ-OPERABILITY-004) asking for an explicit `--repo` argument - never a
raw permission error, and never a silent fallback to some other location.

## Known limitations

`std::env::current_exe()` carries a few documented platform-specific caveats (symlink resolution,
a moved-while-running binary on Linux appending a `(deleted)` marker) - not expected to matter for
the ordinary "run the binary where it sits" case this default targets, but worth being aware of if
a report ever comes in of the default resolving somewhere unexpected.

## Not yet implemented

- `create-repo`'s `path` and `mount`'s `repo` becoming optional, defaulting per REQ-CLI-006 when
  omitted.
- `create-repo`'s chunking arguments becoming optional, defaulting to CDC at a 20-bit target size
  per REQ-CLI-005 when neither `--cdc-target-size-bits` nor `--whole-file` is given.
- `--cdc-target-size-bits`'s `--help` text stating the setting is fixed for the repository's
  lifetime - a pre-existing gap noticed while working out this default, not caused by it, but
  worth fixing at the same time.
- The clear, actionable "path not writable" / "no repository found here, pass --repo explicitly"
  error REQ-CLI-006 and REQ-OPERABILITY-004 call for.
