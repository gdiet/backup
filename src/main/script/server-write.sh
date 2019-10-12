#!/bin/bash
# Options: init write repo=repositoryDirectory mount=mountPoint
# "$(dirname "$0")"/openjdk-11u-11.0.4_11-jre/bin/java
java -cp "$(dirname "$0")"/lib/* dedup.Server write
