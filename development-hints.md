# Hints For Developers Of This Project

## Logging and Command Line Output

- When a process in started that might change data, this should be logged at info level.
- When a user expects confirmation of a successful operation, this should also be logged in Scala - the scripts do not need to print a "finished successfully" message.
- When a process terminates abnormally, this should be logged and printed to the console in Scala - the scripts do not need to print a "finished abnormally" message.

## Scala

* Do not override `val`s that are referenced in the constructor. This avoids ugly order of initialization corner cases.
  <br>See e.g. https://docs.scala-lang.org/tutorials/FAQ/initialization-order.html

## GIT

```
git clone https://github.com/gdiet/backup.git
```
See also `git.conf`.

## Retiring Branches

If a branch is not going to the main branch anytime soon, consider doing a 'fake merge' to the `retired_branches` branch and then delete it. That way it will still be available 'just in case' while not cluttering the branch list:

    git checkout retired_branches
    git merge -s ours origin/branch-to-retire
    git push
    # now delete the remote branch branch-to-retire
