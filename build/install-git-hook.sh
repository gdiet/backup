#!/bin/sh

# switch to the root of the repository
cd "$(git rev-parse --show-toplevel)" || exit 1

# install the pre-commit hook
ln -sf ../../build/pre-commit .git/hooks/pre-commit
echo "Installed pre-commit hook"
