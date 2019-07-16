#!/bin/bash
java -cp "$(dirname "$0")"/lib/* dedup.Main $*
