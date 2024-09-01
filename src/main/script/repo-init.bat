@echo off
title Repo Init - DedupFS
call "%~dp0helpers\set-java.bat"
if errorlevel 1 exit /B %errorlevel%

rem ### Parameters ###
rem # repo=<repository directory>     | default: '..' (parent of working directory)
%JAVA% "-DLOG_BASE=%~dp0log" -Xmx64m -cp "%~dp0lib\*;%~dp0lib-h2\*" dedup.init %*
pause
