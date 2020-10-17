#!/bin/sh

# Create version file
version=$(git log -1 | sed -n 's/commit //p' | cut -b 1-8) || exit 1
if git status | grep -q 'working tree clean'; then clean=''; else clean='+'; fi || exit 1
versionFile=$(date +%Y.%m.%d)-$version$clean.version || exit 1
echo building app $versionFile
touch $versionFile || exit 1

# Create HTML documentation
# Thanks to https://github.com/jfroche/docker-markdown and to the GitHub api
jq --slurp --raw-input '{"text": "\(.)", "mode": "markdown"}' < README.md | curl --data @- https://api.github.com/markdown > readme.html  || exit 1

docker build -t dedupfs-build . || exit 1

rm *.version
rm readme.html
