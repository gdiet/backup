@echo off
title DedupFS
call %~dp0helpers\set-javaw.bat
if errorlevel 1 exit /B %errorlevel%

rem ### Parameters ###
rem # repo=<repository directory> | default: .. (parent of working directory)
rem # mount=<mount point>         | default: /mnt/dedupfs
rem # readOnly=true               | default: false
rem # dbBackup=false              | default: true
rem # copyWhenMoving=true         | default: false
rem # gui=true                    | default: default: false, true in script - true to show server GUI
rem # temp=<temp dir>             | default: dedupfs-temp in the user's temp dir
echo Starting the DedupFS server GUI now - it will run detached from the command line.
start "DedupFS" %JAVAW% "-DLOG_BASE=%~dp0log" -Xmx512m -Dfile.encoding=UTF-8 -cp "%~dp0lib\*" dedup.mount gui=true %*
