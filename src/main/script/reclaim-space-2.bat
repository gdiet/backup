@echo off
title Reclaim Space 1 - DedupFS
call %~dp0helpers\set-java.bat
if errorlevel 1 exit /B %errorlevel%

rem ### Parameters ###
rem # repo=<repository directory>       | default: '..' (parent of working directory)
%JAVA% "-DLOG_BASE=%~dp0log" -cp "%~dp0lib\*" dedup.reclaimSpace2 %*
pause
