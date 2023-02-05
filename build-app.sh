#!/bin/bash

# When run on ubuntu-22.04-lts-jammy-wsl-amd64-wsl, the following additional packages are needed:
# sudo apt --assume-yes install jq zip
# Note: jlink needs the binutils package, but that is contained in the above ubuntu distribution.

# Read version from git.
version=$(git log -1 2>/dev/null | sed -n 's/commit //p' | sed 1q | cut -b 1-8 ) || exit 1
if LANG=EN git status | grep -q 'working tree clean'; then clean=''; else clean='+'; fi || exit 1
versionString="$(date +%Y.%m.%d)-$version$clean" || exit 1

# If $1 is set, use it as name of the release version.
if [ "$1" ]; then
  release="$1"
  echo "building app $versionString as release $release"
  echo
  read -r -s -p "RELEASE BUILD version $release - Press enter to confirm..." _
  echo
  echo
  # For release builds clean everything.
  rm -r .build > /dev/null 2>&1
else
  release="$versionString"
  echo "building app $versionString"
fi

# Create download and build directory if missing.
mkdir -p .build
mkdir -p .download

# Fetch SBT if missing. Find newer releases here: https://www.scala-sbt.org/download.html
sbtVersion="1.8.2"
sbtArchive="sbt-$sbtVersion.tgz"
if [ ! -f ".download/$sbtArchive" ]; then
  echo "Load SBT"
  wget -P .download -q --show-progress "https://github.com/sbt/sbt/releases/download/v$sbtVersion/$sbtArchive" || exit 1
fi

# Unpack SBT if missing.
sbtDir=".build/sbt-$sbtVersion"
if [ ! -d "$sbtDir" ]; then
  echo Unpack SBT
  mkdir -p "$sbtDir"
  tar xfz ".download/$sbtArchive" -C "$sbtDir" --strip-components=1 || exit 1
fi

# Fetch JDKs if missing. Find newer releases here: https://adoptium.net/releases.html
# Set version both in jdkBase and in jdkVersion.
jdkVersion="17.0.6_10"
jdkBase="https://github.com/adoptium/temurin17-binaries/releases/download/jdk-17.0.6%2B10"
jdkWindows="OpenJDK17U-jdk_x64_windows_hotspot_$jdkVersion.zip"
jdkLinux="OpenJDK17U-jdk_x64_linux_hotspot_$jdkVersion.tar.gz"
if [ ! -f ".download/$jdkWindows" ]; then
  echo Load the Windows JDK
  wget -P .download -q --show-progress "$jdkBase/$jdkWindows" || exit 1
fi
if [ ! -f ".download/$jdkLinux" ]; then
  echo Load the Linux JDK
  wget -P .download -q --show-progress "$jdkBase/$jdkLinux" || exit 1
fi

# Unpack JDKs if missing.
jdkDir=".build/jdk-$jdkVersion"
if [ ! -d "$jdkDir" ]; then
  echo Unpack the Linux JDK
  mkdir -p "$jdkDir"
  tar xfz ".download/$jdkLinux" -C "$jdkDir" --strip-components=1 || exit 1
fi
jdkDirWin="$jdkDir-windows"
if [ ! -d "$jdkDirWin" ]; then
  echo Unpack the Windows JDK
  mkdir -p "$jdkDirWin"
  unzip -q ".download/$jdkWindows" -d "$jdkDirWin-temp" || exit 1
  mv "$jdkDirWin-temp"/*/* "$jdkDirWin"
  rm -r "$jdkDirWin-temp"
fi

# Delete previous version of app if any.
rm dedupfs-* > /dev/null 2>&1

# Note that the JAR file contains the resource files.
echo Build the app
if [ "$1" ]; then clean=";clean"; else clean=""; fi
"$jdkDir/bin/java" -cp "$sbtDir/bin/sbt-launch.jar" -Xmx512M xsbt.boot.Boot "$clean;update;createApp" > /dev/null 2>&1 || exit 1

# Create JREs.
echo Collect Java modules information
modules=$("$jdkDir/bin/jdeps" --print-module-deps --ignore-missing-deps --recursive --multi-release 17 --class-path="target/app/lib/*" --module-path="target/app/lib/*" target/app/lib/dedupfs_3-current.jar)
echo Build the minified Linux JRE
"$jdkDir/bin/jlink" --verbose --add-modules "$modules" --strip-debug --no-man-pages --no-header-files --compress=2 --output target/jre-linux > /dev/null
echo Build the minified Windows JRE
"$jdkDir/bin/jlink" --verbose --module-path "$jdkDirWin/jmods" --add-modules "$modules" --strip-debug --no-man-pages --no-header-files --compress=2 --output target/jre-windows > /dev/null

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
# The perl commands translate .md links to .html links.
jq --slurp --raw-input '{"text": "\(.)", "mode": "markdown"}' < README.md \
| curl -s --data @- https://api.github.com/markdown \
| perl -pe 's/(<a href=".*?)\.md(">)/\1.html\2/g' > target/app-linux/README.html  || exit 1
jq --slurp --raw-input '{"text": "\(.)", "mode": "markdown"}' < QUICKSTART.md \
| curl -s --data @- https://api.github.com/markdown \
| perl -pe 's/(<a href=".*?)\.md(">)/\1.html\2/g' > target/app-linux/QUICKSTART.html  || exit 1
jq --slurp --raw-input '{"text": "\(.)", "mode": "markdown"}' < SCHNELLSTART.md \
| curl -s --data @- https://api.github.com/markdown \
| perl -pe 's/(<a href=".*?)\.md(">)/\1.html\2/g' > target/app-linux/SCHNELLSTART.html  || exit 1

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
