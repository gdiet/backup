@echo off
call "%~dp0helpers\set-java.bat"
if errorlevel 1 exit /B %errorlevel%

%JAVA% "-DLOG_BASE=%~dp0log" -Xmx256m -cp "%~dp0lib\*;%~dp0lib-h2-previous\*" dedup.fsc db-migrate1
rem clean up errorlevel handling everywhere else
if errorlevel 0 if not errorlevel 1 (
    echo Database migration step 1 finished.
) else (
    echo Database migration step 1 finished with error code %errorlevel%, exiting...
    pause
    exit /b %errorlevel%
)
%JAVA% "-DLOG_BASE=%~dp0log" -Xmx256m -cp "%~dp0lib\*;%~dp0lib-h2\*"          dedup.fsc db-migrate2
if errorlevel 0 if not errorlevel 1 (
    echo Database migration step 2 finished.
    pause
) else (
    echo Database migration step 2 finished with error code %errorlevel%, exiting...
    pause
    exit /b %errorlevel%
)
