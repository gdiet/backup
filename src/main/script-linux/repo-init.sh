#!/bin/bash
cd "$(dirname "$0")" || exit 1
JAVAVERSION=$(java -version 2>&1 | head -1)
if [[ $JAVAVERSION != *"\"11.0."* ]]; then
  echo "This software has been tested on Java 11 only."
  echo "Detected $JAVAVERSION"
  echo "Exiting now."
  exit 1
fi
java -cp "lib/*" dedup.Server init
