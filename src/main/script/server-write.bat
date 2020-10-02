@echo off
rem Options for dbbackup:
rem repo=<repository directory>     | default: working directory
rem -DLOG_BASE=<log base directory> | mandatory for sensible logging
"%~dp0jre\bin\java" "-DLOG_BASE=%~dp0\" -cp "%~dp0lib\*" dedup.Server dbbackup %*

rem Options for write:
rem repo=<repository directory>     | default: working directory
rem -DLOG_BASE=<log base directory> | mandatory for sensible logging
rem -Dfile.encoding=UTF-8           | necessary for FUSE operations
rem write needs at least 128MB RAM.
start "DedupFS" "%~dp0jre\bin\javaw" "-DLOG_BASE=%~dp0\" -Dfile.encoding=UTF-8 -cp "%~dp0lib\*" dedup.ServerGui write temp=%TEMP%
