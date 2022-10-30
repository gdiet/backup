@echo off
title Reclaim Space 1 - DedupFS
call %~dp0helpers\set-java.bat
if errorlevel 1 exit /B %errorlevel%

rem ### Parameters ###
rem # repo=<repository directory>       | default: '..' (parent of working directory)
rem # keepDays=<number of days to keep> | default: 0 - the 'keepDays=' prefix can be omitted
%JAVA% "-DLOG_BASE=%~dp0log" -Xmx128m -cp "%~dp0lib\*" dedup.reclaimSpace %*
pause
