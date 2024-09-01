# Hints For Developers Of This Project

## Scala

* Do not override `val`s that are referenced in the constructor. This avoids ugly order of initialization corner cases.
  <br>See e.g. https://docs.scala-lang.org/tutorials/FAQ/initialization-order.html

## GIT

```
git clone https://github.com/gdiet/backup.git
```
See also `git.conf`.

## Retiring branches

If a branch will not go to the main branch any time soon, consider doing a 'fake merge' to the `retired_branches` branch and then delete it. That way it will still be available 'just in case' while not cluttering the branch list:

    git checkout retired_branches
    git merge -s ours origin/branch-to-retire
    git push
    # now delete the remote branch branch-to-retire
