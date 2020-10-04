@echo off
call %~dp0db-backup.bat %*

if not errorlevel 1 (
    title Reclaim Space 1 - DedupFS
    rem Options for reclaim space 1:
    rem keepdays=<number of days to keep> | default: 0
    rem repo=<repository directory>       | default: working directory
    rem -DLOG_BASE=<log base directory>   | mandatory for sensible logging
    "%~dp0jre\bin\java" "-DLOG_BASE=%~dp0\" -cp "%~dp0lib\*" dedup.Server reclaimspace1 %*
    pause
)
