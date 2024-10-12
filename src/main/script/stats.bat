@echo off
title Statistics - DedupFS
call "%~dp0helpers\set-java.bat"
if errorlevel 1 exit /B %errorlevel%

rem ### Parameters ###
rem # repo=<repository directory>     | default: '..' (parent of working directory)
%JAVA% "-DLOG_BASE=%~dp0log" -Xmx256m -Dlogback.configurationFile=logback-readonly.xml -cp "%~dp0lib\*;%~dp0lib-h2\*" dedup.stats %*
if errorlevel 0 if not errorlevel 1 (
    echo Statistics finished successfully.
    pause
) else (
    echo Statistics finished with error code %errorlevel%, exiting...
    pause
    exit /b %errorlevel%
)
