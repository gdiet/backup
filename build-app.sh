#!/bin/bash

# sudo apt --assume-yes install curl jq zip binutils
# binutils package is needed for jlink

# Read version from git.
version=$(git log -1 2>/dev/null | sed -n 's/commit //p' | sed 1q | cut -b 1-8 ) || exit 1
if LANG=EN git status | grep -q 'working tree clean'; then clean=''; else clean='+'; fi || exit 1
versionString=$(date +%Y.%m.%d)-$version$clean || exit 1

# Possibly create release version.
if [ "$1" ]; then release=$1; else release=$versionString; fi
echo "building app $versionString as release $release"
if [ "$1" ]; then
  echo
  read -r -s -p "RELEASE BUILD - Press enter to confirm..." _
  echo
  echo
fi

# Fetch JDKs if necessary. Find newer releases here: https://adoptium.net/releases.html
jdkFolder=jdk-17.0.6%2B10
jdk=OpenJDK17U-jdk_x64_windows_hotspot_17.0.6_10.zip
if [ ! -f "$jdk" ]; then
  echo Load the Windows JDK
  wget -q --show-progress https://github.com/adoptium/temurin17-binaries/releases/download/$jdkFolder/$jdk || exit 1
fi
jdkLinux=OpenJDK17U-jdk_x64_linux_hotspot_17.0.6_10.tar.gz
if [ ! -f "$jdkLinux" ]; then
  echo Load the Linux JDK
  wget -q --show-progress https://github.com/adoptium/temurin17-binaries/releases/download/$jdkFolder/$jdkLinux || exit 1
fi

# Delete previous version of app if any.
rm dedupfs-* > /dev/null 2>&1

# Note that the JAR file contains the resource files.
echo Build the app
sbt ';clean;update;createApp' > /dev/null 2>&1 || exit 1

echo Unpack the Linux JDK
tar xfz $jdkLinux || exit 1
mv jdk-17.* target/jdk-linux || exit 1
echo Unpack the Windows JDK
unzip -q $jdk || exit 1
mv jdk-17.* target/jdk-windows || exit 1

# Create JREs
echo Collect Java modules information
modules=$(target/jdk-linux/bin/jdeps --print-module-deps --ignore-missing-deps --recursive --multi-release 17 --class-path="target/app/lib/*" --module-path="target/app/lib/*" target/app/lib/dedupfs_3-current.jar)
echo Build the minified Linux JRE
target/jdk-linux/bin/jlink --verbose --add-modules $modules --strip-debug --no-man-pages --no-header-files --compress=2 --output target/jre-linux > /dev/null
echo Build the minified Windows JRE
target/jdk-linux/bin/jlink --verbose --module-path target/jdk-windows/jmods --add-modules $modules --strip-debug --no-man-pages --no-header-files --compress=2 --output target/jre-windows > /dev/null

echo Prepare app folders

# Create system specific app folders.
cp -r target/app target/app-linux
cp -r target/app target/app-windows

# Final touches for Linux.
find target/app-linux -type f -name "*.bat" -delete
chmod +x target/app-linux/*
cp LICENSE target/app-linux/ || exit 1
touch "target/app-linux/$versionString.version" || exit 1
mv target/jre-linux target/app-linux/jrex

# Create HTML documentation.
# Thanks to https://github.com/jfroche/docker-markdown and to the GitHub api
jq --slurp --raw-input '{"text": "\(.)", "mode": "markdown"}' < README.md \
| curl -s --data @- https://api.github.com/markdown > target/app-linux/README.html  || exit 1
jq --slurp --raw-input '{"text": "\(.)", "mode": "markdown"}' < QUICKSTART.md \
| curl -s --data @- https://api.github.com/markdown > target/app-linux/QUICKSTART.html  || exit 1
jq --slurp --raw-input '{"text": "\(.)", "mode": "markdown"}' < SCHNELLSTART.md \
| curl -s --data @- https://api.github.com/markdown > target/app-linux/SCHNELLSTART.html  || exit 1

# Final touches for Windows.
find target/app-windows -type f ! -name "*.*" -delete
cp LICENSE target/app-windows/ || exit 1
touch "target/app-windows/$versionString.version" || exit 1
cp target/app-linux/*.html target/app-windows/
mv target/jre-windows target/app-windows/jre

echo Package apps
rm -r target/packaging > /dev/null 2>&1
mkdir target/packaging
cp target/app-linux/*.html target/packaging/
mv target/app-linux "target/packaging/dedupfs-$release-linux"
mv target/app-windows "target/packaging/dedupfs-$release-windows"
cd target/packaging || exit 1
tar cfz "../../dedupfs-$release-linux.tar.gz" "dedupfs-$release-linux" "README.html" "QUICKSTART.html" "SCHNELLSTART.html"
zip -rq "../../dedupfs-$release-windows.zip" "dedupfs-$release-windows" "README.html" "QUICKSTART.html" "SCHNELLSTART.html"
cd ../.. || exit 1

echo
echo "Created dedupfs app as dedupfs-$release-linux.tar.gz and dedupfs-$release-windows.zip"
