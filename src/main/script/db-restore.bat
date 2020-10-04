@echo off
title DB Restore - DedupFS

echo.
echo This utility will restore a previous state of the DedupFS database.
echo This will overwrite the current database state.  If you do not want
echo that, simply close the console window where this utility is running.
echo.
pause
echo.

rem Options for dbrestore:
rem from=<script file>              | default: none (direct file restore)
rem repo=<repository directory>     | default: working directory
rem -DLOG_BASE=<log base directory> | mandatory for sensible logging
"%~dp0jre\bin\java" "-DLOG_BASE=%~dp0\" -cp "%~dp0lib\*" dedup.Server dbrestore %*
if errorlevel 1 (
    echo Database restore finished with error code %errorlevel%, exiting...
    pause
    exit /b 1
) else (
    echo Database restore finished.
    pause
)
