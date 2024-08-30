@echo off
title DB Backup - DedupFS
call "%~dp0helpers\set-java.bat"
if errorlevel 1 exit /B %errorlevel%

echo.
echo This utility will create a plain file backup and a zipped script
echo backup of the DedupFS database.  If you do not want that, simply
echo close the console window where this utility is running.
echo.
pause
echo.
echo Starting database backup...

rem ### Parameters ###
rem # repo=<repository directory>        | default: '..' (parent of working directory)
%JAVA% "-DLOG_BASE=%~dp0log" -Xmx64m -cp "%~dp0lib\*;%~dp0lib-h2\*" dedup.fsc db-backup %*
if errorlevel 0 if not errorlevel 1 (
    echo Database backup finished successfully.
    pause
) else (
    echo Database backup finished with error code %errorlevel%, exiting...
    pause
    exit /b %errorlevel%
)
