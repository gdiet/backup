@echo off
title DB Backup - DedupFS
rem Options for dbbackup:
rem repo=<repository directory>     | default: working directory
rem -DLOG_BASE=<log base directory> | mandatory for sensible logging
"%~dp0jre\bin\java" "-DLOG_BASE=%~dp0\" -cp "%~dp0lib\*" dedup.Server dbbackup %*
if errorlevel 1 (
    echo Database backup finished with error code %errorlevel%, exiting...
    pause
    exit /b 1
) else (
    echo Database backup finished.
)
