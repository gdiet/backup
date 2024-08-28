@echo off
call "%~dp0helpers\set-java.bat"
if errorlevel 1 exit /B %errorlevel%

%JAVA% "-DLOG_BASE=%~dp0log" -Xmx256m -cp "%~dp0lib\*;%~dp0lib-h2-previous\*" dedup.fsc db-migrate1
if not errorlevel 0 (
    echo Database migration step 1 finished with error code %errorlevel%, exiting...
    pause
    exit /b 1
) else (
    echo Database migration step 1 finished.
)
%JAVA% "-DLOG_BASE=%~dp0log" -Xmx256m -cp "%~dp0lib\*;%~dp0lib-h2\*"          dedup.fsc db-migrate2
if not errorlevel 0 (
    echo Database migration step 2 finished with error code %errorlevel%, exiting...
    pause
    exit /b 1
) else (
    echo Database migration step 2 finished.
    pause
)
