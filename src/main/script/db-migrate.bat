@echo off
title DB Migrate - DedupFS
call "%~dp0helpers\set-java.bat"
if errorlevel 1 exit /B %errorlevel%

echo.
echo This utility will update a DedupFS database created by the previous
echo major release to the current major release.  If you do not want
echo that, simply close the console window where this utility is running.
echo.
pause
echo.
echo Starting database migration...

%JAVA% "-DLOG_BASE=%~dp0log" -Xmx256m -cp "%~dp0lib\*;%~dp0lib-h2-previous\*" dedup.dbMigrateStep1
if errorlevel 0 if not errorlevel 1 (
    echo Database migration step 1 finished successfully.
) else (
    echo Database migration step 1 finished with error code %errorlevel%, exiting...
    pause
    exit /b %errorlevel%
)
%JAVA% "-DLOG_BASE=%~dp0log" -Xmx256m -cp "%~dp0lib\*;%~dp0lib-h2\*" dedup.dbMigrateStep2
if errorlevel 0 if not errorlevel 1 (
    echo Database migration step 2 finished successfully.
    pause
) else (
    echo Database migration step 2 finished with error code %errorlevel%, exiting...
    pause
    exit /b %errorlevel%
)
