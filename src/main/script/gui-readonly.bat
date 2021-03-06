@echo off
title Read DedupFS
rem Options for read:
rem mount=K:\                       | default: Windows J:\ Linux /tmp/mnt
rem repo=<repository directory>     | default: working directory
rem -DLOG_BASE=<log base directory> | mandatory for sensible logging
rem -Dfile.encoding=UTF-8           | necessary for FUSE operations
rem read needs at least 80MB RAM.
echo Starting the DedupFS server GUI now - it will run detached from the command line.
start "DedupFS" "%~dp0jre\bin\javaw" "-DLOG_BASE=%~dp0\" -Dfile.encoding=UTF-8 -cp "%~dp0lib\*" dedup.ServerGui %*
