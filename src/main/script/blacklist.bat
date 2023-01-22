@echo off
title Blacklisting - DedupFS
call "%~dp0helpers\set-java.bat"
if errorlevel 1 exit /B %errorlevel%

rem ### Parameters ###
rem # repo=<repository directory>                                     | default: .. (parent of working directory)
rem # dbBackup=false                                                  | default: true
rem # blacklistDir=<name of directory in repo with files to blacklist>| default: blacklist
rem # deleteFiles=<delete files in blacklistDir when blacklisted?>    | default: true
rem # dfsBlacklist=<name of blacklist directory in dedup file system> | default: blacklist
rem # deleteCopies=<delete all copies of blacklisted files?>          | default: false
%JAVA% "-DLOG_BASE=%~dp0log" -Xmx128m -cp "%~dp0lib\*" dedup.blacklist %*
pause
