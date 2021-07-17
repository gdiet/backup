#!/bin/sh

# sudo apt --assume-yes install curl jq zip

# Fetch JREs if necessary. Find newer releases here: https://adoptopenjdk.net/releases.html
jreFolder=jdk-11.0.11%2B9
jre=OpenJDK11U-jre_x64_windows_hotspot_11.0.11_9.zip
if [ ! -f "$jre" ]; then
  wget https://github.com/AdoptOpenJDK/openjdk11-binaries/releases/download/$jreFolder/$jre || exit 1
fi
jrex=OpenJDK11U-jre_x64_linux_hotspot_11.0.11_9.tar.gz
if [ ! -f "$jrex" ]; then
  wget https://github.com/AdoptOpenJDK/openjdk11-binaries/releases/download/$jreFolder/$jrex || exit 1
fi

# Delete previous version of app if any.
rm -f app.* ||  exit 1

# Read version from git.
version=$(git log -1 | sed -n 's/commit //p' | cut -b 1-8) || exit 1
if LANG=EN git status | grep -q 'working tree clean'; then clean=''; else clean='+'; fi || exit 1
versionFile=$(date +%Y.%m.%d)-$version$clean.version || exit 1
echo building app $versionFile

# Build the app. Note that the JAR file contains the resource files.
~/sbt/bin/sbt ';clean;update;createApp' || exit 1

# unpack JREs to app.
tar xfz $jrex || exit 1
mv jdk-*-jre target/app/jrex || exit 1
unzip -q $jre || exit 1
mv jdk-*-jre target/app/jre || exit 1

# Copy license and version file
cp LICENSE target/app/ || exit 1
touch target/app/$versionFile || exit 1

# Create HTML documentation
# Thanks to https://github.com/jfroche/docker-markdown and to the GitHub api
jq --slurp --raw-input '{"text": "\(.)", "mode": "markdown"}' < README.md \
| curl -s --data @- https://api.github.com/markdown > target/app/readme.html  || exit 1

# Final touches
chmod +x target/app/*
chmod -x target/app/LICENSE
chmod -x target/app/*.bat
chmod -x target/app/*.html
chmod -x target/app/*.version

# Pack apps
zip -rq ../../app.zip target/app
tar cfz ../../app.tar.gz target/app/*

echo
echo Created dedupfs app as app.tar.gz and app.zip
