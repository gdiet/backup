#!/bin/bash

# sudo apt --assume-yes install curl jq zip

# Read version from git.
version=$(git log -1 | sed -n 's/commit //p' | sed 1q | cut -b 1-8) || exit 1
if LANG=EN git status | grep -q 'working tree clean'; then clean=''; else clean='+'; fi || exit 1
versionString=$(date +%Y.%m.%d)-$version$clean || exit 1

# Possibly create release version
if [ "$1" ]; then release=$1; else release=$versionString; fi
echo "building app $versionString as release $release"
if [ "$1" ]; then
  echo
  read -r -s -p "RELEASE BUILD - Press enter to confirm..." _
  echo
  echo
fi

# Fetch JREs if necessary. Find newer releases here: https://adoptium.net/releases.html
jreFolder=jdk-17.0.3%2B7
jre=OpenJDK17U-jre_x64_windows_hotspot_17.0.3_7.zip
if [ ! -f "$jre" ]; then
  wget https://github.com/adoptium/temurin17-binaries/releases/download/$jreFolder/$jre || exit 1
fi
jreLinux=OpenJDK17U-jre_x64_linux_hotspot_17.0.3_7.tar.gz
if [ ! -f "$jreLinux" ]; then
  wget https://github.com/adoptium/temurin17-binaries/releases/download/$jreFolder/$jreLinux || exit 1
fi

# Delete previous version of app if any.
rm -rf dedupfs-* ||  exit 1

# Build the app. Note that the JAR file contains the resource files.
sbt ';clean;update;createApp' || exit 1

# unpack JREs to app.
tar xfz $jreLinux || exit 1
mv jdk-*-jre target/app/jrex || exit 1
unzip -q $jre || exit 1
mv jdk-*-jre target/app/jre || exit 1

# Copy license and version file
cp LICENSE target/app/ || exit 1
touch "target/app/$versionString.version" || exit 1

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
mv target/app "dedupfs-$release"
zip -rq "dedupfs-$release.zip" "dedupfs-$release"
tar cfz "dedupfs-$release.tar.gz" "dedupfs-$release"
rm -rf "dedupfs-$release"

echo
echo "Created dedupfs app as dedupfs-$release.tar.gz and dedupfs-$release.zip"
