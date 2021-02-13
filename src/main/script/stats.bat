@echo off
title Stats - DedupFS
rem Options for stats:
rem repo=<repository directory>     | default: working directory
rem -DLOG_BASE=<log base directory> | mandatory for sensible logging
"%~dp0jre\bin\java" "-DLOG_BASE=%~dp0\" -cp "%~dp0lib\*" dedup.Main stats %*
pause
