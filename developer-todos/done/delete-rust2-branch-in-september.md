# Delete branch `rust2` in September

**Noted**: 2026-08-21, handed over directly mid-conversation (not tied to any specific task in
progress at the time).
**Size**: medium - confirm with the developer before starting. Branch deletion is a destructive,
hard-to-reverse operation, and per `AGENTS.md`'s "Relationship To Other Implementations", this
repository's origin (`git@backup:gdiet/backup.git`) is shared with the other implementations
(Scala `main`, `rust`, `go`/`go2`/`go3`) - deleting a branch there acts on that shared remote, not
just a local one.
**Context**: none beyond the developer's own instruction below.

Developer's own wording: "Branch rust2 im September löschen" ("delete branch rust2 in September").
No exact date given - pick this up sometime in September 2026, not before, and not left
indefinitely either.

When picked up: confirm with the developer that `rust2` should still be deleted (things may have
changed since this note was written), then delete both the local branch (if a local copy exists on
whichever machine handles this) and the remote branch on `git@backup:gdiet/backup.git` - the local
`git branch -D` and the remote `git push origin --delete rust2` are separate, both destructive, and
both need the developer's go-ahead per `AGENTS.md`'s git safety rules, even though this note itself
already carries a prior instruction to do it.

## Done

**Completed**: 2026-09-01, by a Claude Code Desktop-App session on `julius`, with the developer's
explicit go-ahead in this session.

Before deleting, confirmed `origin/rust2`'s tip (`22f087be...`) is a full git ancestor of
`origin/rust`'s tip - nothing on `rust2` was unmerged content, so nothing is lost by removing the
ref itself. Deleted the local branch on this machine (`git branch -D rust2`) and the remote branch
(`git push origin --delete rust2`).

**If you are an agent (or a developer) who still has a local `rust2` branch on some other machine**:
it is safe to delete there too. To double-check your local copy has nothing this doesn't already
account for, compare your local `rust2` tip against the three commits `origin/rust2` actually ended
at before deletion:

```
22f087becb546352c743c4dca71469517ee74fcd | 2026-08-21 21:36:12 +0200 | gitignore: add /.vscode/, sort convention documented
ad86dd4b8abe2c8eb1572afcf1ad1f29ffa85b98 | 2026-08-21 21:34:43 +0200 | developer-todos: add the missing "Done" note (previous commit only did the rename)
07f19381166005dd828f58ad5de738c8c7532824 | 2026-08-21 21:32:36 +0200 | developer-todos: mark the retired_branches recipe/gitignore item done
```

If your local `rust2` tip matches `22f087be...` (or is an ancestor of it), it is safe to delete
outright - `git branch -D rust2` locally is enough, the remote ref is already gone. If your local
tip is *not* an ancestor of `22f087be...` (i.e. it has commits `origin/rust2` never saw), stop and
reconcile that content into `rust` first - see the `reconcile-diverged-branches` skill - rather than
deleting it outright, since that content would otherwise only ever have existed on that one local
machine.
