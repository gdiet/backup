#!/bin/bash
cd "$(dirname "$0")" || exit 1
jre-linux/bin/java -cp "lib/*" dedup.Server init
