@echo off
title Reclaim Space 2 - DedupFS

echo.
echo This utility will compact the DedupFS data store. After this, older
echo database backups can't be fully applied anymore. If you do not want
echo that, simply close the console window where this utility is running.
echo.
pause
echo.

call "%~dp0db-backup.bat" %*

if not errorlevel 1 (
    title Reclaim Space 2 - DedupFS
    rem Options for reclaim space 2:
    rem repo=<repository directory>       | default: working directory
    rem -DLOG_BASE=<log base directory>   | mandatory for sensible logging
    "%~dp0jre\bin\java" "-DLOG_BASE=%~dp0\" -cp "%~dp0lib\*" dedup.Server reclaimspace2 %*
    pause
)
