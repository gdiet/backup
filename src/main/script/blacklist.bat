@echo off
title Blacklisting - DedupFS
call %~dp0helpers\set-java.bat
if errorlevel 1 exit /B %errorlevel%

rem ### Parameters ###
rem # repo=<repository directory>                                     | default: '..' (parent of working directory)
rem # dfsBlacklist=<name of blacklist directory in dedup file system> | default: 'blacklist'
rem # deleteCopies=<delete all copies of blacklisted files?>          | default: 'false'
%JAVA% "-DLOG_BASE=%~dp0log" -cp "%~dp0lib\*" dedup.blacklist %*
pause
