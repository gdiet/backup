---
name: local-reference-worktrees
description: Set up or add to .local/ (git-excluded, this-machine-only) - a scratch area for read-only reference checkouts of this project's other branches or tags, kept as ordinary git worktrees so an old implementation (e.g. main for Scala, or a retired branch's tag like rust-1st-attempt) stays browsable on disk even after the main checkout moves elsewhere. Use to create .local/ on its own, or to add a worktree for a specific branch or tag into it (e.g. "check out branch X into .local as Y", or "check out tag X into .local as Y").
---

# Local Reference Worktrees

This project's history holds several implementations as branches of the same origin
(`main` for Scala, `go`/`go2`/`go3`, plus the active branch this checkout itself tracks) - a
previous Rust attempt is retired (tag `rust-1st-attempt`, merged into `retired_branches`, see that
branch's own `README.md`). Only one branch's files exist in the main working tree at a time -
`.local/` holds ordinary `git worktree` checkouts of the others, purely for
reading/grepping/comparing, never for editing or committing.

## `.local/` itself

`.local/` is machine-local and must never be committed or suggested to other clones - excluded via
`.git/info/exclude` (an untracked, per-clone ignore list), not a committed `.gitignore`.

To set up `.local/` on its own, without adding a worktree yet:

```bash
mkdir -p .local
grep -qxF '.local/' .git/info/exclude || echo '.local/' >> .git/info/exclude
```

Idempotent - safe to run even if `.local/` already exists and is already excluded.

## Adding a worktree

Given a name `X` and a worktree name `Y` ("check out branch/tag X into .local as Y"). `X` can be
either a branch or a tag - `git worktree add` accepts any commit-ish, not just branches.

1. Ensure `.local/` exists and is excluded (the commands above - safe to run every time).
2. Refuse if `.local/<Y>` already exists rather than overwriting it silently; ask the developer
   whether to reuse it, remove it first, or pick a different name.
3. Resolve `X`, in this order:
   - If `X` already exists as a local branch, use it directly - the worktree gets a normal,
     attached-HEAD checkout of that branch.
   - Otherwise, if `origin/X` exists as a remote branch, fetch it and create a local branch
     tracking it: `git fetch origin X && git worktree add .local/<Y> X` (git creates the local
     tracking branch `X` from `origin/X` automatically here, since `X` does not yet exist
     locally).
   - Otherwise, if `X` exists as a local tag, or `git fetch origin tag X` finds one, use it
     directly - `git worktree add .local/<Y> X` checks out a tag in **detached HEAD** state. That
     is expected, not an error to work around: a tag is an immutable pointer, there is nothing to
     track, and a purely read-only reference checkout has no need for a branch. Do not create a
     branch just to avoid detached HEAD.
   - If none of the above resolves, stop and report this rather than guessing at a branch or tag
     name.
4. `git worktree add .local/<Y> <X>` (skip if step 3 already ran the equivalent form).
5. Report the resulting path (`.local/<Y>`) back, and whether it is a branch or (detached-HEAD)
   tag checkout.

## Keeping a reference worktree current

Not automated by this skill - a worktree is a snapshot at creation time.

For a **branch** worktree, refresh with:

```bash
git -C .local/<Y> pull
```

For a **tag** worktree, `pull` does not apply (detached HEAD, no upstream branch to pull from) -
a tag is not expected to move. If the tag genuinely was reassigned to a different commit upstream,
re-fetch and re-check-out it explicitly instead:

```bash
git -C .local/<Y> fetch origin tag <tag-name> --force
git -C .local/<Y> checkout <tag-name>
```

## Removing a worktree

```bash
git worktree remove .local/<Y>
```

Refuses if the worktree has uncommitted changes - add `--force` only after confirming with the
developer that any changes there are safe to discard (a reference worktree is not expected to
carry work worth keeping, but do not assume that silently).
