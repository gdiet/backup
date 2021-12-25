@echo off
title Blacklisting - DedupFS
call %~dp0helpers\set-java.bat
if errorlevel 1 exit /B %errorlevel%

rem ### Parameters ###
rem # repo=<repository directory>                                     | default: '..' (parent of working directory)
rem # noDbBackup=true                                                 | default: false
rem # blacklistDir=<name of directory in repo with files to blacklist>| default: 'blacklist'
rem # deleteFiles=<delete files in blacklistDir when blacklisted?>    | default: 'false'
rem # dfsBlacklist=<name of blacklist directory in dedup file system> | default: 'blacklist'
rem # deleteCopies=<delete all copies of blacklisted files?>          | default: 'false'
%JAVA% "-DLOG_BASE=%~dp0log" -cp "%~dp0lib\*" dedup.blacklist %*
pause
