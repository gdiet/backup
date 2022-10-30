@echo off
title DB Backup - DedupFS
call %~dp0helpers\set-java.bat
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
%JAVA% "-DLOG_BASE=%~dp0log" -Xmx64m -cp "%~dp0lib\*" dedup.fsc db-backup %*
if not errorlevel 0 (
    echo Database backup finished with error code %errorlevel%, exiting...
    pause
    exit /b 1
) else (
    echo Database backup finished.
    pause
)
