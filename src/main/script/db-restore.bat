@echo off
title DB Restore - DedupFS
call "%~dp0helpers\set-java.bat"
if errorlevel 1 exit /B %errorlevel%

echo.
echo This utility will restore a previous state of the DedupFS database.
echo This will overwrite the current database state.  If you do not want
echo that, simply close the console window where this utility is running.
echo.
pause
echo.
echo Starting database restore...

rem ### Parameters ###
rem # repo=<repository directory>        | default: '..' (parent of working directory)
rem # <database backup zip file name>    | default: none - for plain db file restore.
%JAVA% "-DLOG_BASE=%~dp0log" -Xmx512m -cp "%~dp0lib\*;%~dp0lib-h2\*" dedup.fsc db-restore %*
if errorlevel 0 if not errorlevel 1 (
    echo Database restore finished successfully.
    pause
) else (
    echo Database restore finished with error code %errorlevel%, exiting...
    pause
    exit /b %errorlevel%
)
