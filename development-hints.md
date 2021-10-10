# Hints For Developers Of This Project

## Retiring branches

If a branch will not go to the main branch any time soon, consider doing a 'fake merge' to the `retired-branches` branch and then delete it. That way it will still be available 'just in case' while not cluttering the branch list:

    git checkout retired-branches
    git merge -s ours origin/branch-to-retire
    git push
    # now delete the remote branch branch-to-retire
