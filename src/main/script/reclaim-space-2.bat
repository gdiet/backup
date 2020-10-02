@echo off
title Reclaim Space 2 - DedupFS
rem Options for reclaim space 2:
rem repo=<repository directory>       | default: working directory
rem -DLOG_BASE=<log base directory>   | mandatory for sensible logging
"%~dp0jre\bin\javaw" "-DLOG_BASE=%~dp0\" -cp "%~dp0lib\*" dedup.Server reclaimspace2 %*
