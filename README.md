# `retired_branches`

Landing point for retired branches - keeps them reachable without cluttering the branch list. To
find a retired branch's actual content: `git log --merges` - each merge commit's second parent is
that branch's original tip.

## Retiring a branch

```bash
# optionally tag, if you need a simpler way to find the last commit before retiring:
git tag <name> origin/<branch-to-retire>
git push origin <name>

git checkout retired_branches
git merge -s ours --allow-unrelated-histories origin/<branch-to-retire>
git push

# only now delete origin/<branch-to-retire>
```
