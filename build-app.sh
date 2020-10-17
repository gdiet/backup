#!/bin/sh

# Delete previous version of app if any
rm -f app.*

# Create version file here so we don't need git and the repo in the container
version=$(git log -1 | sed -n 's/commit //p' | cut -b 1-8) || exit 1
if git status | grep -q 'working tree clean'; then clean=''; else clean='+'; fi || exit 1
versionFile=$(date +%Y.%m.%d)-$version$clean.version || exit 1
echo building app $versionFile
touch $versionFile || exit 1

# Build and fetch the app
docker build -t dedupfs-build . || exit 1
docker create --name dedupfs-build dedupfs-build
docker cp dedupfs-build:/root/app.tar.gz .
docker cp dedupfs-build:/root/app.zip .
docker rm dedupfs-build

# Clean up
rm *.version || exit 1

echo
echo Created dedupfs app as app.tar.gz and app.zip
