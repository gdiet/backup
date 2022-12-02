# Hints For Developers Of This Project

## Scala

* Do not override `val`s. This avoids ugly order of initialization corner cases.
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

## Ubuntu 20.04 and build-app.sh

Java 17:

```
sudo apt install openjdk-17-jdk
```

SBT:

https://www.scala-sbt.org/download.html

```
echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | sudo tee /etc/apt/sources.list.d/sbt.list
echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | sudo tee /etc/apt/sources.list.d/sbt_old.list
curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | sudo apt-key add
sudo apt update
sudo apt install sbt
```

Tools:

```
sudo apt --assume-yes install curl jq zip binutils
```
