#!/bin/bash
cd "$(dirname "$0")" || exit 1
JAVAVERSION=$(java -version 2>&1 | head -1)
if [[ $JAVAVERSION != *"\"11.0."* ]]; then
  echo "This software has been tested on Java 11 only."
  echo "Detected $JAVAVERSION"
  echo "Exiting now."
  exit 1
fi
DBERRORFILE=fsdb/dedupfs.trace.db
if [ -f "$DBERRORFILE" ]; then
  echo "Database trace file $DBERRORFILE found."
  echo "Check for database problems first."
  exit 1
fi
# Options: write copyWhenMoving repo=repositoryDirectory mount=mountPoint temp=tempFileDirectory
# Server needs at least ~400MB of memory.
java -cp "lib/*" -Dfile.encoding=UTF-8 dedup.ServerGui write
