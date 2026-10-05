# Distribution And Default Repository Location

## DESIGN-CLI-003: Distributed as a portable download, not an installed package

Status: implemented

`dfs` is obtained as a plain, self-contained binary download and run directly from wherever the
operator places it, rather than through a package manager or an installer that puts it in a fixed,
often system-owned location (`Program Files`, `/usr/...`, a `cargo install` toolchain directory). A
single, self-contained Rust binary needs no accompanying runtime or library directory alongside it
to make this work. So it can sit directly next to a repository, with no wrapping app-folder
structure required - exactly what DESIGN-CLI-004 below relies on.

This directly enables REQ-CLI-006 in
[`../../requirements/functional/cli-commands.md`](../../requirements/functional/cli-commands.md)'s
default repository location. It also matches a common, still-current personal-backup pattern: the
executable lives alongside the repository itself, often on the same removable/external drive the
repository is stored on, and travels with it between machines. This extends REQ-OPERABILITY-002 in
[`../../requirements/non-functional/operability.md`](../../requirements/non-functional/operability.md)'s
"repository is portable, sync-able, machine-independent" goal to the tool that manages it.

The download also carries the manual (DESIGN-CLI-008 below).

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

Status: implemented

REQ-CLI-006's default resolves via `std::env::current_exe()`, then that path's parent directory,
then a `dedupfs-repository` child of it - not the current working directory the command happens to
be invoked from, and not a subdirectory one level further up. The executable itself needs no
wrapping app folder (DESIGN-CLI-003 above), so the repository sits directly beside it.

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

### An actionable error only for a defaulted path, never a silent fallback

A *defaulted* path that turns out unusable gets a clear, actionable message pointing at passing
the path explicitly, instead of surfacing the raw underlying error (REQ-OPERABILITY-004) - never a
silent fallback to some other location. An explicitly-given path that turns out unusable gets the
plain underlying error instead - there is nothing more specific to tell an operator who already
made that choice themselves. This distinction belongs to the default-path mechanism itself, not to
any one command: what actually counts as "unusable" differs by what a command does with the path
(`create-repo`: cannot create a repository there; `mount`: nothing to open there), so each reports
its own natural error in that case - only the actionable-vs-plain policy is shared.

## DESIGN-CLI-008: The manual ships with every download and states the version it documents

Status: decided

The manual is [`../manual.md`](../manual.md). Each download of `dfs` includes a copy of it as a
plain Markdown file named `dfs-manual-<version>.md`. The file is readable in any text editor, so it
needs no viewer and no rendering tool. One manual covers both platform builds that a download
bundles (DESIGN-CLI-003).

The manual names the version it documents:

- In the repository, the manual is the development version. A fixed line at its top says so and
  says that it describes the `dfs` built from the same commit.
- A release build copies the manual from the tagged commit. It replaces only the line after the
  `release-version-line` marker with the version token that `--version` prints (DESIGN-CLI-001).
  Binary and manual therefore come from the same commit.

The development manual is not a substitute for the shipped one. It always describes the latest
commit, so it can differ from the binary an operator actually runs. The version line is the
safeguard. An operator who runs a released binary reads the manual that came with it.

A change that adds or alters user-visible behavior updates the manual in the same commit.

### Alternative considered and rejected: README.md as the manual

The README is the landing page of the repository. Its content (status, known limitations, system
requirements, notices) does not belong into a document that ships with a download, and the manual
grows with every command. A separate document keeps both short.

### Alternative considered and rejected: a version number in the development manual

The package version stays at one value for a long time. A number in the development manual would
claim a precision it does not have. The statement "describes the same commit" is accurate.

## Known limitations

`std::env::current_exe()` carries a few documented platform-specific caveats (symlink resolution,
a moved-while-running binary on Linux appending a `(deleted)` marker) - not expected to matter for
the ordinary "run the binary where it sits" case this default targets, but worth being aware of if
a report ever comes in of the default resolving somewhere unexpected.

## Verification

The default-path resolution itself (`default_repo_path_from` in `crates/cli/src/repo_path.rs`) is
covered directly, against several `exe_path` shapes, via the executable-path-as-a-plain-parameter
split this file's own reasoning above relies on for testability. Each command's own `try_run`
(`crates/cli/src/create_repo.rs`, `crates/cli/src/mount.rs`) covers its actionable-vs-plain error
message, specifically distinguishing a defaulted from an explicit unusable path; `create-repo`'s
also covers the defaulted and an explicit chunking choice each reported correctly on success.
