#!/bin/bash
cd "$(dirname "$0")" || exit 1
JAVAVERSION=$(java -version 2>&1 | head -1)
if [[ $JAVAVERSION != *"\"11.0."* ]]; then
  echo "This software has been tested on Java 11 only."
  echo "Detected $JAVAVERSION"
  echo "Exiting now."
  exit 1
fi
DBFILE=fsdb/dedupfs.mv.db
if [ -f "$DBFILE" ]; then
  echo "Creating plain database backup: $DBFILE -> $DBFILE.backup"
  cp $DBFILE $DBFILE.backup
  TIMESTAMP=$(date +%Y-%m-%d_%H-%M)
  TARGET=fsdb/dedupfs_$TIMESTAMP.zip
  echo "Creating sql script database backup: $DBFILE -> $TARGET"
  java -cp "lib/*" org.h2.tools.Script -url "jdbc:h2:$(pwd)/fsdb/dedupfs" -script $TARGET -user sa -options compression zip
  chmod -w $TARGET
else
  echo "Database backup: Database file $DBFILE does not exist."
fi