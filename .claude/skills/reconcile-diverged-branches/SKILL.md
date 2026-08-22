---
name: reconcile-diverged-branches
description: Reconcile a local branch that has diverged from its remote counterpart (both have commits the other lacks). Use when `git status` reports branches have diverged, or when asked to rebase, reconcile, squash, or clean up local commits against a remote branch that moved - including without an interactive rebase.
---

# Reconcile Diverged Branches

Approach: rebuild history on a disposable scratch branch, auditing whether the remote's incoming
commits are content-relevant to local work beyond line-level git conflicts, replaying/regrouping
local commits via `cherry-pick` (never `rebase -i`, unsupported in this environment), verifying no
content is lost, then fast-forwarding the real branches only on explicit go-ahead.

## Never touch the real branches first

Work entirely on a disposable scratch branch until the result is verified and the user gives
explicit go-ahead to make it real. Investigating (read-only `git log`/`git diff`/`git show`) is
always safe; `git reset --hard`, `git rebase`, or force-pushing the actual branch is not, until
that go-ahead.

## 1. Investigate before touching anything

```bash
git fetch origin
git merge-base <local> origin/<local>
git log --oneline <merge-base>..<local>         # local-only commits
git log --oneline <merge-base>..origin/<local>  # incoming commits
git diff --stat <merge-base>..<local>
git diff --stat <merge-base>..origin/<local>
```

Check file-level overlap between the two ranges. No overlap does not mean no relevance - read the
incoming commits' actual content (`git show <commit>`), especially convention/rule changes (e.g.
`.claude/rules/`, a `docs/**/README.md`-style convention doc) that may apply to local work even
without touching the same lines. Report what is found before proceeding.

## 2. Rebuild on a scratch branch, never `rebase -i`

`git rebase -i` needs interactive input this environment does not support. Reconstruct manually:

```bash
git checkout -B <scratch> origin/<local>
git cherry-pick <commit1> <commit2> ...   # replay one at a time, stop on the first conflict
```

For a commit with an inadequate message (a raw "fixup", etc.): `git cherry-pick --no-commit <c>`,
then commit with a real message while preserving the original author's identity and date exactly
(read them first via `git show --format="%an <%ae> | %ad" -s <c>`):

```bash
GIT_AUTHOR_NAME="..." GIT_AUTHOR_EMAIL="..." GIT_AUTHOR_DATE="..." git commit -m "<real message>"
```

Preserving the original author does not mean skipping your own attribution: the git Author field
answers "who ran the commit / whose content this primarily is," `Co-Authored-By`/`Generated-By`
answers "who contributed to the content" - different questions. If you drafted or substantively
shaped the content (even though the human typed and committed it themselves), it still gets your
trailer, per the `attributed-commits` skill's own author/co-author determination - only skip the
trailer when the content is genuinely someone else's independent work you are merely relocating.

## 3. Combine commits, if asked, by content - not by mechanical squash

To regroup N commits into fewer, more meaningful ones: save each target file's *final* content to
a scratch location, reset the branch to the base, then re-stage and commit file-by-file (or
hunk-by-hunk - reset a file to its base copy, apply only the wanted change, stage, restore the full
final edit, repeat) in whatever grouping actually makes sense - not "commit 1 = everything up to
step 3 of the original session". Reuse step 2's author/date preservation for any commit whose
content originated as someone else's direct edit.

## 4. Verify losslessness before finishing

```bash
git diff --stat <old-local-head> <scratch>
```

Every changed file must be explainable as (a) part of the incoming remote changes, (b) part of the
original local-only changes, or (c) a new fix made during reconciliation - nothing else. Scan for
any deletion not matched by an equivalent addition elsewhere before calling it done.

## 5. Only after explicit approval, make it real

```bash
git push origin <scratch>:<local>   # fast-forward - should need no --force
git branch -f <local> <scratch>
git checkout <local>
git branch -d <scratch>
```

Confirm `git rev-parse <local> origin/<local>` match afterward.
