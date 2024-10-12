@echo off
title Reclaim Space - DedupFS
call "%~dp0helpers\set-java.bat"
if errorlevel 1 exit /B %errorlevel%

echo.
echo This utility will clean deleted file entries and orphan data entries from the
echo DedupFS database, so their disk space is released and can be used when storing
echo new files. If you do not want that, simply close the console window where this
echo utility is running.
echo.
pause
echo.
echo Starting database migration...

rem ### Parameters ###
rem # repo=<repository directory>       | default: '..' (parent of working directory)
rem # keepDays=<number of days to keep> | default: 0 - the 'keepDays=' prefix can be omitted
%JAVA% "-DLOG_BASE=%~dp0log" -Xmx256m -cp "%~dp0lib\*;%~dp0lib-h2\*" dedup.reclaimSpace %*
if errorlevel 0 if not errorlevel 1 (
    echo Reclaim space finished successfully.
    pause
) else (
    echo Reclaim space finished with error code %errorlevel%, exiting...
    pause
    exit /b %errorlevel%
)
