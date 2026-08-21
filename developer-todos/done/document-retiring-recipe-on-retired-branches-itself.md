# Add the retiring-branches recipe (and a .gitignore) to retired_branches itself

**Noted**: 2026-08-21, right after fake-merging `rust2-intermediate` and `rust` into
`retired_branches` locally (not yet pushed).
**Size**: small
**Context**: `main`'s `development-hints.md`, "Retiring Branches" section, documents the actual
recipe (`git checkout retired_branches && git merge -s ours --allow-unrelated-histories
origin/branch-to-retire && git push`, then delete the original branch) - but that recipe currently
only lives in `main`, not on `retired_branches` itself, where anyone actually looking at that
branch would most want to find it. `retired_branches`' own tree right now is whatever content
happened to be checked out when it was created (old retired Scala source files, an `.hgignore`),
purely an artifact of past fake-merges, not living content.

Developer's own wording: "Rezept für retire in retired-branches aufnehmen, gitignore für 'alles
andere' dort auch aufnehmen." Two asks:

- Copy (or reference) the retiring-branches recipe from `main`'s `development-hints.md` into
  `retired_branches` itself (e.g. a `README.md` at its root), so it is discoverable directly on
  the branch that needs it, not only in `main`. Note the `--allow-unrelated-histories` flag the
  documented recipe does not mention - needed whenever the branch being retired has no common
  ancestor with `retired_branches` (true for every Rust/Go rewrite-attempt branch so far, since
  they are all orphans relative to `main`'s Scala history that `retired_branches` itself descends
  from).
- Add a `.gitignore` to `retired_branches` "for everything else" - the developer's own phrasing,
  not further interpreted here. Plausible reading: since the branch's working tree is not meant to
  accumulate real content going forward (each retirement is a fake-merge, not a real commit to the
  tree), a broad `.gitignore` (e.g. `*`, with the recipe file itself as the one tracked exception)
  would keep it from accumulating clutter if anyone ever checks it out and works there by mistake.
  Confirm this reading with the developer before acting - "alles andere" could also mean something
  more specific than "everything untracked".

Do this before `retired_branches` is ever pushed with the two new fake-merge commits (retiring
`rust2-intermediate` and `rust`) already sitting on it locally, so the recipe/`.gitignore` land in
the same push rather than a separate follow-up.

## Done

**Completed**: 2026-08-21, on the local `retired_branches` branch (not yet pushed - lands in the
same push as the two fake-merges above). Added `README.md` (the recipe, plus the
`--allow-unrelated-histories` note, plus a note on tagging a branch before retiring it if the name
is wanted back) and `.gitignore` containing just `*` - the "broad, everything-untracked" reading
was confirmed correct by the developer. Also removed the old, tracked `.hgignore` (content just
`src/tryout`, a Mercurial-era leftover, now superseded by the `.gitignore`) in the same commit,
per the developer noticing it while this was in progress.
