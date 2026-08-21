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
