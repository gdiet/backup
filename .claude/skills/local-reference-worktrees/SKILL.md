---
name: local-reference-worktrees
description: Set up or add to .local/ (git-excluded, this-machine-only) - a scratch area for read-only reference checkouts of this project's other branches, kept as ordinary git worktrees so an old implementation (e.g. the rust branch, or main for Scala) stays browsable on disk even after the main checkout moves to a different branch. Use to create .local/ on its own, or to add a worktree for a specific branch into it (e.g. "check out branch X into .local as Y").
---

# Local Reference Worktrees

This project's history holds several implementations as branches of the same origin
(`rust`, `main` for Scala, `go`/`go2`/`go3`, plus the active branch this checkout itself tracks).
Only one branch's files exist in the main working tree at a time - `.local/` holds ordinary
`git worktree` checkouts of the others, purely for reading/grepping/comparing, never for editing
or committing.

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

Given a branch name `X` and a worktree name `Y` ("check out branch X into .local as Y"):

1. Ensure `.local/` exists and is excluded (the commands above - safe to run every time).
2. Refuse if `.local/<Y>` already exists rather than overwriting it silently; ask the developer
   whether to reuse it, remove it first, or pick a different name.
3. Resolve the branch:
   - If `X` already exists as a local branch, use it directly.
   - Otherwise, if `origin/X` exists, fetch it and create a local branch tracking it:
     `git fetch origin X && git worktree add .local/<Y> X` (git creates the local tracking branch
     `X` from `origin/X` automatically here, since `X` does not yet exist locally).
   - If neither exists, stop and report this rather than guessing at a branch name.
4. `git worktree add .local/<Y> <X>` (skip if step 3 already ran the equivalent form).
5. Report the resulting path (`.local/<Y>`) back.

## Keeping a reference worktree current

Not automated by this skill - a worktree is a snapshot at creation time. To refresh one:

```bash
git -C .local/<Y> pull
```

## Removing a worktree

```bash
git worktree remove .local/<Y>
```

Refuses if the worktree has uncommitted changes - add `--force` only after confirming with the
developer that any changes there are safe to discard (a reference worktree is not expected to
carry work worth keeping, but do not assume that silently).
